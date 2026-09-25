"""
page/campanhas_supply.py
Página C - Supply: Avaliação Física, Cubagem de Bandejas, Cruzamento CD15 e Fechamento Operacional.
"""

from __future__ import annotations
import streamlit as st
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy import text
from services.campanha_service import (
    listar_campanhas,
    obter_campanha_por_id,
    obter_itens_campanha_com_detalhes,
    salvar_dimensoes_produto,
    obter_estruturas_e_bandejas,
    obter_mapa_capacidade_por_tipo_exposicao,
    salvar_fechamento_supply,
    finalizar_avaliacao_campanha,
    carregar_dados_produto_consolidado,
    expirar_campanhas_vencidas,
    excluir_campanha,
    obter_lista_compradores,
    LISTA_14_LOJAS
)
from services.campanha_calculo import (
    calcular_capacidade_exposicao,
    calcular_caixas_transferencia,
    apurar_disponibilidade_cd,
    calcular_dias_campanha
)
from services.exportacao_campanha import (
    gerar_excel_transferencia_operacional,
    gerar_excel_devolutiva_compras
)


def show_campanhas_supply_page(engine, base_data_path: str = "data"):
    st.title("📦 Avaliação e Cubagem Operacional — Supply")
    st.caption("Verificação de medidas físicas, cubagem por bandeja, confrontação de estoque no CD15 e fechamento de caixas.")

    usuario_atual = st.session_state.get("username", "supply")
    role_str = str(st.session_state.get("role", "")).strip().lower()
    cargo_str = str(st.session_state.get("cargo", "")).strip().lower()
    is_admin = role_str == "admin" or "admin" in cargo_str or usuario_atual in ["admin", "administrador"]

    # Atualiza expiração automática
    expirar_campanhas_vencidas(engine)

    lista_compradores = obter_lista_compradores(engine)

    # -------------------------------------------------------------------------
    # PAINEL DE FILTROS SUPERIORES DE CONSULTA (SUPPLY)
    # -------------------------------------------------------------------------
    with engine.connect() as conn:
        lojas_db = conn.execute(text("SELECT codigo, nome FROM lojas WHERE tipo = 'LOJA' AND ativa = TRUE ORDER BY codigo")).fetchall()
        lojas_list = [dict(r._mapping) for r in lojas_db]
        lojas_map = {l["codigo"]: l["nome"] for l in lojas_list}

    st.markdown("### 🔍 Filtros de Consulta")
    with st.container(border=True):
        col_sf1, col_sf2, col_sf3, col_sf4 = st.columns([1.5, 1.5, 1.5, 1.5])

        # 1. Filtro de Loja
        with col_sf1:
            opcoes_lojas_s = ["TODAS"] + [l["codigo"] for l in lojas_list]
            sel_loja_supply = st.selectbox(
                "Filtrar por Loja:",
                opcoes_lojas_s,
                format_func=lambda x: "🌟 Todas as Lojas (Consolidado)" if x == "TODAS" else f"{x} - {lojas_map.get(x, 'Loja ' + x)}",
                key="filtro_loja_supply_page"
            )

        # 2. Filtro de Período / Datas
        with col_sf2:
            hoje_s = datetime.now().date()
            col_sd1, col_sd2 = st.columns(2)
            d_ini_s = col_sd1.date_input("Vigência De:", value=hoje_s - pd.Timedelta(days=30), key="d_ini_supply_filtro")
            d_fim_s = col_sd2.date_input("Até:", value=hoje_s + pd.Timedelta(days=60), key="d_fim_supply_filtro")

        # 3. Filtro por Comprador
        with col_sf3:
            sel_comprador_supply = st.selectbox(
                "Filtrar por Comprador:",
                ["TODOS"] + lista_compradores,
                key="filtro_comprador_supply_page"
            )

        # 4. Filtro de Status
        with col_sf4:
            status_opcoes_s = ["ENVIADA_SUPPLY", "EM_AVALIACAO_SUPPLY", "PENDENCIA_COMPRAS", "FINALIZADA", "ATIVA", "RASCUNHO", "INATIVA"]
            status_default_s = ["ENVIADA_SUPPLY", "EM_AVALIACAO_SUPPLY", "PENDENCIA_COMPRAS", "FINALIZADA", "ATIVA"]
            status_selecionados_s = st.multiselect(
                "Status da Campanha:",
                status_opcoes_s,
                default=status_default_s,
                key="filtro_status_supply"
            )

    # Consulta no Banco com filtros
    where_supply = ["c.data_fim >= :d_ini", "c.data_inicio <= :d_fim"]
    params_supply: Dict[str, Any] = {
        "d_ini": d_ini_s,
        "d_fim": d_fim_s
    }

    if status_selecionados_s:
        where_supply.append("c.status = ANY(:status_list)")
        params_supply["status_list"] = status_selecionados_s

    if sel_loja_supply != "TODAS":
        where_supply.append("cl.loja_codigo = :loja_filtro")
        params_supply["loja_filtro"] = str(sel_loja_supply).zfill(3)

    if sel_comprador_supply != "TODOS":
        where_supply.append("ci.comprador = :comp_filtro")
        params_supply["comp_filtro"] = str(sel_comprador_supply).strip()

    where_sql_supply = " AND ".join(where_supply)

    sql_campanhas_supply = text(f"""
        SELECT DISTINCT
            c.id as campanha_id,
            c.codigo_campanha,
            c.nome as campanha_nome,
            c.data_inicio,
            c.data_fim,
            c.status,
            c.observacoes,
            COUNT(DISTINCT ci.produto_codigo) as total_skus,
            SUM(CASE WHEN c.status NOT IN ('INATIVA', 'CANCELADA') AND UPPER(COALESCE(te.nome, '')) != 'INATIVA' THEN COALESCE(cl.volume_final_supply, 0) ELSE 0 END) as total_unidades,
            SUM(CASE WHEN c.status NOT IN ('INATIVA', 'CANCELADA') AND UPPER(COALESCE(te.nome, '')) != 'INATIVA' THEN COALESCE(cl.caixas_transferencia, 0) ELSE 0 END) as total_caixas
        FROM campanhas c
        JOIN campanha_itens ci ON ci.campanha_id = c.id
        JOIN campanha_lojas cl ON cl.campanha_item_id = ci.id
        LEFT JOIN tipos_exposicao te ON te.id = cl.tipo_exposicao_id
        WHERE {where_sql_supply}
        GROUP BY c.id, c.codigo_campanha, c.nome, c.data_inicio, c.data_fim, c.status, c.observacoes
        ORDER BY c.data_inicio ASC, c.nome ASC
    """)

    with engine.connect() as conn:
        campanhas_encontradas = conn.execute(sql_campanhas_supply, params_supply).fetchall()

    if not campanhas_encontradas:
        st.info("Nenhuma campanha localizada para os filtros selecionados.")
        return

    # Métricas Gerais do Supply (Exclui inativas/canceladas)
    campanhas_ativas_supply = [c for c in campanhas_encontradas if c.status not in ('INATIVA', 'CANCELADA')]
    tot_skus_s = sum(c.total_skus or 0 for c in campanhas_ativas_supply)
    tot_un_s = sum(c.total_unidades or 0 for c in campanhas_ativas_supply)
    tot_cx_s = sum(c.total_caixas or 0 for c in campanhas_ativas_supply)

    col_sm1, col_sm2, col_sm3, col_sm4 = st.columns(4)
    col_sm1.metric("Campanhas Ativas / Filtradas", f"{len(campanhas_ativas_supply)} / {len(campanhas_encontradas)}")
    col_sm2.metric("Total de Produtos (SKUs)", tot_skus_s)
    col_sm3.metric("Volume Aprovado (Un)", f"{tot_un_s:,.0f} un")
    col_sm4.metric("Total de Caixas a Transferir", f"{tot_cx_s:,.0f} cx")

    st.markdown("---")

    # Seletor de Campanha
    col_cs_sel, col_cs_info = st.columns([3, 1])
    camp_dict = {f"{c.codigo_campanha} — {c.campanha_nome} [{c.status}]": c.campanha_id for c in campanhas_encontradas}
    sel_camp_label = col_cs_sel.selectbox("Selecione a Campanha para Avaliação / Cubagem:", list(camp_dict.keys()), key="sel_camp_supply")
    camp_id = camp_dict[sel_camp_label]

    camp = obter_campanha_por_id(engine, camp_id)
    if not camp:
        st.error("Campanha não encontrada.")
        return

    dias_camp = calcular_dias_campanha(camp["data_inicio"], camp["data_fim"])
    st.markdown(f"#### 🏷️ `{camp['codigo_campanha']}` — **{camp['nome']}** | Vigência: {camp['data_inicio'].strftime('%d/%m/%Y')} a {camp['data_fim'].strftime('%d/%m/%Y')} ({dias_camp} dias)")

    itens = obter_itens_campanha_com_detalhes(engine, camp_id)
    if not itens:
        st.warning("Esta campanha não possui itens cadastrados pelo Compras.")
        return

    # Progresso e Estatísticas da Avaliação
    total_itens = len(itens)
    itens_pendentes = sum(1 for it in itens if it.get("status_supply") == "PENDENTE")
    itens_avaliados = sum(1 for it in itens if it.get("status_supply") == "AVALIADO")
    itens_inativos = sum(1 for it in itens if it.get("status_supply") == "INATIVO")
    
    concluidos = itens_avaliados + itens_inativos
    pct = (concluidos / total_itens) * 100 if total_itens > 0 else 0

    c_p1, c_p2, c_p3, c_p4 = st.columns(4)
    c_p1.metric("Total de Produtos", total_itens)
    c_p2.metric("🟡 Pendentes de Avaliação", itens_pendentes)
    c_p3.metric("✅ Avaliados pelo Supply", itens_avaliados)
    c_p4.metric("⚪ Sem Lojas Ativas (Inativos)", itens_inativos)

    st.progress(pct / 100, text=f"Progresso da Análise: {concluidos} de {total_itens} produtos concluídos ({pct:.0f}%)")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # SELETOR DE ITENS COM DISCRIMINAÇÃO CLARA DE STATUS
    # -------------------------------------------------------------------------
    col_filtro_it, col_sel_it = st.columns([1, 2])
    filtro_status_item = col_filtro_it.radio(
        "Filtrar itens por status:",
        ["Todos", "🟡 Apenas Pendentes", "✅ Apenas Avaliados"],
        horizontal=True,
        key="filtro_status_item_supply"
    )

    if filtro_status_item == "🟡 Apenas Pendentes":
        itens_filtrados = [it for it in itens if it.get("status_supply") == "PENDENTE"]
    elif filtro_status_item == "✅ Apenas Avaliados":
        itens_filtrados = [it for it in itens if it.get("status_supply") == "AVALIADO"]
    else:
        itens_filtrados = itens

    if not itens_filtrados:
        st.info("Nenhum item corresponde ao filtro selecionado.")
        return

    # Criação dos rótulos descritivos no SelectBox
    itens_labels: Dict[str, Dict[str, Any]] = {}
    for it in itens_filtrados:
        st_supply = it.get("status_supply", "PENDENTE")
        qtd_atv = len(it.get("lojas_ativas", []))
        
        if st_supply == "AVALIADO":
            tag = f"✅ [AVALIADO ({qtd_atv} lj)]"
        elif st_supply == "INATIVO":
            tag = "⚪ [INATIVO (0 lj)]"
        else:
            tag = f"🟡 [PENDENTE ({qtd_atv} lj)]"

        label = f"{tag} {it['produto_codigo']} — {it['descricao_snapshot']} [Forn: {it.get('fornecedor') or 'N/D'} | Comp: {it.get('comprador') or 'N/D'}]"
        itens_labels[label] = it

    sel_item_label = col_sel_it.selectbox("Selecione o Produto para Analisar / Fechar:", list(itens_labels.keys()), key="sel_item_supply")
    item_atual = itens_labels[sel_item_label]
    lojas_ativas = item_atual.get("lojas_ativas", [])

    st.markdown(f"### 📦 `{item_atual['produto_codigo']}` — **{item_atual['descricao_snapshot']}**")
    st.caption(f"🏢 **Fornecedor:** {item_atual.get('fornecedor') or 'N/D'} | 👤 **Comprador:** {item_atual.get('comprador') or 'N/D'} | 🏷️ **Departamento:** {item_atual.get('departamento') or 'N/D'} | 🚚 **Embalagem Transf:** {item_atual['embalagem_transferencia']} un/cx | 📦 **Lojas Ativas Participantes:** {len(lojas_ativas)} de 14")

    # Se todas as lojas forem INATIVA
    if len(lojas_ativas) == 0:
        st.warning("⚪ **Produto Inativo:** Todas as 14 lojas foram configuradas como **INATIVA** pelo Compras para esta campanha. Nenhuma análise física ou abastecimento é necessário pelo Supply.")
        st.markdown("---")
        return

    # Carrega dados atualizados do parquet e dimensões
    prod_consolidado = carregar_dados_produto_consolidado(
        engine=engine,
        produto_codigo=item_atual["produto_codigo"],
        data_inicio=camp["data_inicio"],
        data_fim=camp["data_fim"],
        base_data_path=base_data_path
    )

    # -------------------------------------------------------------------------
    # SEÇÃO 1: CADASTRO E MEDIDAS FÍSICAS (cm)
    # -------------------------------------------------------------------------
    st.subheader(f"📐 1. Dimensões Físicas do Produto: `{item_atual['produto_codigo']}` — **{item_atual['descricao_snapshot']}**")
    
    dim_existente = prod_consolidado.get("dimensoes") or {}
    has_dim_cadastrada = prod_consolidado.get("dimensoes") is not None
    alt_p_padrao = float(dim_existente.get("altura_cm", 10.0) or 10.0)
    larg_p_padrao = float(dim_existente.get("largura_cm", 10.0) or 10.0)
    prof_p_padrao = float(dim_existente.get("profundidade_cm", 10.0) or 10.0)

    if has_dim_cadastrada:
        if item_atual.get("codigo_familia"):
            st.success(f"✅ **Dimensões Pré-carregadas do Cadastro:** Medidas sincronizadas a nível da Família `{item_atual.get('codigo_familia')}` — **{item_atual.get('descricao_familia', 'N/D')}**. Não é necessário alterá-las para os outros sabores.")
        else:
            st.success(f"✅ **Dimensões Pré-carregadas do Cadastro:** Medidas ({alt_p_padrao} x {larg_p_padrao} x {prof_p_padrao} cm) já salvas para este produto.")
    elif item_atual.get("codigo_familia"):
        st.info(f"👨‍👩‍👧‍👦 **Família `{item_atual.get('codigo_familia')}`:** Ao salvar as dimensões deste produto, todos os sabores da família serão atualizados automaticamente.")

    with st.container(border=True):
        col_d1, col_d2, col_d3, col_d4 = st.columns([1, 1, 1, 1])
        alt_p = col_d1.number_input("Altura (cm):", min_value=0.1, value=alt_p_padrao, step=0.5, key="alt_cm_inp")
        larg_p = col_d2.number_input("Largura (cm):", min_value=0.1, value=larg_p_padrao, step=0.5, key="larg_cm_inp")
        prof_p = col_d3.number_input("Profundidade (cm):", min_value=0.1, value=prof_p_padrao, step=0.5, key="prof_cm_inp")
        
        with col_d4:
            st.write("")
            st.write("")
            if st.button("💾 Salvar Dimensões", type="secondary", key="btn_salvar_dim"):
                suc_dim, msg_dim = salvar_dimensoes_produto(
                    engine=engine,
                    produto_codigo=item_atual["produto_codigo"],
                    altura_cm=alt_p,
                    largura_cm=larg_p,
                    profundidade_cm=prof_p,
                    usuario=usuario_atual,
                    propagar_familia=True
                )
                if suc_dim:
                    st.success(msg_dim)
                    st.rerun()
                else:
                    st.error(msg_dim)

    # -------------------------------------------------------------------------
    # SEÇÃO 2: CUBAGEM POR ESTRUTURA E BANDEJAS
    # -------------------------------------------------------------------------
    st.subheader("🗄️ 2. Parametrização e Simulação de Cubagem por Bandeja")
    
    emb_transf = max(1, int(item_atual["embalagem_transferencia"] or 1))
    tot_skus_fam_item = int(item_atual.get("total_skus_familia") or 1)
    if item_atual.get("codigo_familia") or tot_skus_fam_item > 1:
        st.info(f"👨‍👩‍👧‍👦 **Família de Exposição: `{item_atual.get('codigo_familia')}` — {item_atual.get('descricao_familia', 'N/D')}** | Este produto está parametrizado para compartilhar a estrutura física entre **{tot_skus_fam_item} SKUs**. A cubagem abaixo divide o espaço total proporcionalmente.")

    estruturas = obter_estruturas_e_bandejas(engine)
    dim_prod_dict = {"altura_cm": alt_p, "largura_cm": larg_p, "profundidade_cm": prof_p}

    if not estruturas:
        st.warning("Nenhuma estrutura física com bandejas cadastrada. Cadastre em Administração de Exposição.")
        capacidade_calculada_sku = 0
        estrutura_selecionada_id = None
        mapa_capacidades = {}
    else:
        est_dict = {f"{e['tipo_nome']} — {e['estrutura_nome']} ({len(e['bandejas'])} bandejas)": e for e in estruturas}
        col_c1, col_c2 = st.columns([2, 1])
        sel_est_label = col_c1.selectbox("Modelo Físico da Exposição Simulado:", list(est_dict.keys()), key="sel_est_cubagem")
        est_obj = est_dict[sel_est_label]
        estrutura_selecionada_id = est_obj["estrutura_id"]

        skus_compartilhados = col_c2.number_input(
            "Nº de SKUs Compartilhados:",
            min_value=1,
            max_value=max(30, tot_skus_fam_item),
            value=tot_skus_fam_item,
            step=1,
            help="Total de SKUs/sabores que dividem o mesmo móvel físico (ex: 5 sabores de Tang na mesma Ilha)."
        )

        # Mapa de capacidade para todos os tipos de exposição físicos
        mapa_capacidades = obter_mapa_capacidade_por_tipo_exposicao(
            engine=engine,
            produto_dimensoes=dim_prod_dict,
            embl_transferencia=emb_transf,
            total_skus=skus_compartilhados
        )

        # Capacidade Total da Estrutura Física (100% do móvel)
        cap_total_estrutura_un = calcular_capacidade_exposicao(
            bandejas=est_obj["bandejas"],
            produto_dimensoes=dim_prod_dict,
            total_skus=1
        )
        cap_total_estrutura_cx = int(cap_total_estrutura_un // emb_transf)

        # Capacidade Rateada por SKU (1 de N)
        capacidade_calculada_sku = calcular_capacidade_exposicao(
            bandejas=est_obj["bandejas"],
            produto_dimensoes=dim_prod_dict,
            total_skus=skus_compartilhados
        )
        cap_sku_cx = int(capacidade_calculada_sku // emb_transf)
        if capacidade_calculada_sku > 0 and cap_sku_cx == 0:
            cap_sku_cx = 1

        # Card de resultado da cubagem com UNIDADES e CAIXAS
        with st.container(border=True):
            col_res1, col_res2, col_res3, col_res4 = st.columns([1.2, 0.8, 1.3, 1.3])
            col_res1.metric("Estrutura Física", est_obj["estrutura_nome"])
            col_res2.metric("Bandejas", f"{len(est_obj['bandejas'])} níveis")
            col_res3.metric(
                "Capacidade Total do Móvel",
                f"{cap_total_estrutura_cx:,} cx",
                f"{cap_total_estrutura_un:,} un (Móvel Inteiro)"
            )
            taxa_rateio = f"Rateio {int(round(1/skus_compartilhados*100))}%" if skus_compartilhados > 1 else "Exclusivo"
            col_res4.metric(
                f"Capacidade por SKU (1 de {skus_compartilhados})",
                f"{cap_sku_cx:,} cx",
                f"{capacidade_calculada_sku:,} un ({taxa_rateio})"
            )

        # Tabela resumo comparativa dos tipos de exposição
        if mapa_capacidades:
            with st.expander("📊 Ver Comparativo de Capacidade em Todos os Tipos de Exposição (Ilha, Ponta, Meia Ponta, Orelha)"):
                df_comp_tipos = pd.DataFrame([
                    {
                        "Tipo de Exposição": info["tipo_nome"],
                        "Estrutura Padrão": info["estrutura_nome"],
                        "Bandejas": f"{info['bandejas_count']} níveis",
                        "Capacidade Total Móvel (cx)": f"{info['capacidade_total_cx']:,} cx",
                        "Capacidade Total Móvel (un)": f"{info['capacidade_total_un']:,} un",
                        f"Sugestão por SKU ({skus_compartilhados} sabores) [cx]": f"{info['capacidade_sku_cx']:,} cx",
                        f"Sugestão por SKU ({skus_compartilhados} sabores) [un]": f"{info['capacidade_sku_un']:,} un",
                    }
                    for tid, info in mapa_capacidades.items()
                    if info["tipo_nome"] != "INATIVA"
                ])
                st.dataframe(df_comp_tipos, use_container_width=True, hide_index=True)

    # -------------------------------------------------------------------------
    # SEÇÃO 3: ANÁLISE LOJA A LOJA E FECHAMENTO EM CAIXAS (SOMENTE LOJAS ATIVAS)
    # -------------------------------------------------------------------------
    st.subheader(f"🏪 3. Fechamento do Supply por Loja em CAIXAS ({len(lojas_ativas)} lojas participantes)")
    st.caption("💡 **Operação em Caixas Fechadas:** Digite a quantidade sugerida em **Caixas**. A sugestão do sistema é calculada automaticamente com base no **tipo de exposição de cada loja** e dividida proporcionalmente pela quantidade de SKUs da família.")

    fechamento_inputs = []
    total_caixas_previstas = 0
    total_unidades_efetivas = 0

    col_h1, col_h2 = st.columns(2)
    metade = (len(lojas_ativas) + 1) // 2

    for i, lj in enumerate(lojas_ativas):
        container_col = col_h1 if i < metade else col_h2
        with container_col:
            with st.container(border=True):
                lj_cod = lj["loja_codigo"]
                lj_nome = lj["loja_nome"]
                tipo_exp_id_lj = lj.get("tipo_exposicao_id")
                tipo_exp_definido = lj.get("tipo_exposicao_nome") or "Exposição Geral"
                
                # Identifica capacidade específica para a estrutura/tipo desta loja
                cap_info_tipo = mapa_capacidades.get(tipo_exp_id_lj, {})
                sug_cx_cubagem = cap_info_tipo.get("capacidade_sku_cx")
                sug_un_cubagem = cap_info_tipo.get("capacidade_sku_un")
                cap_tot_cx_tipo = cap_info_tipo.get("capacidade_total_cx")
                cap_tot_un_tipo = cap_info_tipo.get("capacidade_total_un")

                if sug_cx_cubagem is None:
                    sug_cx_cubagem = cap_sku_cx if 'cap_sku_cx' in locals() else 0
                    sug_un_cubagem = capacidade_calculada_sku if 'capacidade_calculada_sku' in locals() else 0
                    cap_tot_cx_tipo = cap_total_estrutura_cx if 'cap_total_estrutura_cx' in locals() else 0
                    cap_tot_un_tipo = cap_total_estrutura_un if 'cap_total_estrutura_un' in locals() else 0

                # Cabeçalho da loja com tipo de exposição SOMENTE LEITURA (definido pelo compras)
                st.write(f"🏬 **{lj_cod} - {lj_nome}**")
                st.markdown(f"🏷️ **Exposição Definida pelo Compras:** <span style='background-color:#0284c7;color:white;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:bold;'>{tipo_exp_definido}</span>", unsafe_allow_html=True)
                
                if cap_tot_cx_tipo and cap_tot_cx_tipo > 0:
                    st.caption(f"📐 **Cubagem do Tipo ({tipo_exp_definido}):** {cap_tot_cx_tipo} cx no móvel ({cap_tot_un_tipo:,} un) ➔ **Sugestão p/ este Sabor:** `{sug_cx_cubagem} cx` ({sug_un_cubagem:,} un) *(Rateio em {tot_skus_fam_item} SKUs)*")

                # Snapshot de dados da loja
                c_s1, c_s2, c_s3 = st.columns(3)
                c_s1.caption(f"Estoque Loja: **{float(lj['estoque_loja'] or 0):,.0f} un**")
                c_s2.caption(f"Venda Proj: **{float(lj['venda_projetada'] or 0):.0f} un**")
                c_s3.caption(f"Sug. Compras: **{int(lj['volume_comprador'] or 0)} un**")

                # Sugestão inicial em CAIXAS (acompanha a cubagem do tipo de exposição da loja rateado pela família)
                cx_salva = int(lj.get("caixas_transferencia") or 0)
                if cx_salva > 0:
                    cx_inicial = cx_salva
                elif sug_cx_cubagem and sug_cx_cubagem > 0:
                    cx_inicial = sug_cx_cubagem
                elif int(lj.get("volume_comprador") or 0) > 0:
                    cx_inicial = max(1, int(round(int(lj["volume_comprador"]) / emb_transf)))
                else:
                    cx_inicial = 0

                cx_final_supply = st.number_input(
                    f"Caixas de Transferência (cx) — Loja {lj_cod}:",
                    min_value=0,
                    value=cx_inicial,
                    step=1,
                    key=f"cx_supply_{item_atual['produto_codigo']}_{lj_cod}",
                    help=f"Informe a quantidade em caixas fechadas para transferência. Embalagem: {emb_transf} un/cx."
                )

                vol_efetivo = cx_final_supply * emb_transf
                total_caixas_previstas += cx_final_supply
                total_unidades_efetivas += vol_efetivo

                st.markdown(f"📦 **Caixas a Transferir:** `{cx_final_supply} cx` ➔ **Volume Efetivo:** `{vol_efetivo:,} unidades` ({cx_final_supply} cx × {emb_transf} un/cx)")

                fechamento_inputs.append({
                    "loja_codigo": lj_cod,
                    "volume_final_supply": vol_efetivo,
                    "caixas_transferencia": cx_final_supply
                })

    # -------------------------------------------------------------------------
    # SEÇÃO 4: CONFRONTAÇÃO COM ESTOQUE DO CD15
    # -------------------------------------------------------------------------
    st.markdown("---")
    st.subheader("🏢 4. Apuração de Saldo de CD15 e Rupturas")

    saldo_cd15_un = float(prod_consolidado["estoque_cd15"] or 0.0)
    saldo_cd15_cx = int(saldo_cd15_un // emb_transf)
    cx_atendidas, cx_falta, status_cd = apurar_disponibilidade_cd(total_caixas_previstas, saldo_cd15_cx)

    with st.container(border=True):
        col_cd1, col_cd2, col_cd3, col_cd4 = st.columns(4)
        col_cd1.metric("Estoque CD15 (Físico)", f"{saldo_cd15_cx:,.0f} cx", f"{saldo_cd15_un:,.0f} un")
        col_cd2.metric("Demanda das Lojas Ativas", f"{total_caixas_previstas:,.0f} cx", f"{total_unidades_efetivas:,.0f} un")
        col_cd3.metric("Transferência Viável", f"{cx_atendidas:,.0f} cx", delta="Atendido" if cx_atendidas > 0 else "Zerado")
        
        delta_color = "normal" if cx_falta == 0 else "inverse"
        col_cd4.metric("Falta no CD15 (Comprar)", f"{cx_falta:,.0f} cx", delta="Crítico" if cx_falta > 0 else "OK", delta_color=delta_color)

        if status_cd == "OK":
            st.success(f"✅ Estoque pleno no CD15 para cobrir 100% da demanda das {len(lojas_ativas)} lojas ativas.")
        elif status_cd == "PARCIAL":
            st.warning(f"⚠️ Estoque parcial no CD15. Há {saldo_cd15_cx} cx disponíveis para atender {total_caixas_previstas} cx necessárias. Foi gerada pendência de {cx_falta} cx para compras.")
        else:
            st.error(f"🚨 Estoque zerado no CD15! Necessário comprar {cx_falta} cx com o fornecedor.")

    # -------------------------------------------------------------------------
    # SEÇÃO 5: SALVAMENTO E EXPORTAÇÕES
    # -------------------------------------------------------------------------
    st.markdown("---")
    st.info("""
    💡 **Entenda a diferença entre as ações:**
    - **💾 Salvar Avaliação deste Item:** Grava a cubagem e caixas apenas do produto selecionado (`AVALIADO`). Permite salvar o progresso produto a produto.
    - **✅ Finalizar Avaliação de TODA a Campanha:** Valida se todos os produtos ativos do mix foram avaliados e conclui a campanha, liberando a consulta para as lojas e expedindo as devolutivas de compra para o CD15.
    """)

    cols_actions_s = st.columns([2, 2, 1] if is_admin else [2, 2])

    with cols_actions_s[0]:
        if st.button("💾 Salvar Avaliação deste Item", type="primary", use_container_width=True):
            suc_salv, msg_salv = salvar_fechamento_supply(
                engine=engine,
                campanha_id=camp_id,
                item_id=item_atual["item_id"],
                estrutura_exposicao_id=estrutura_selecionada_id,
                volume_calculado=capacidade_calculada_sku,
                fechamento_lojas=fechamento_inputs,
                estoque_cd15_total=saldo_cd15_un,
                usuario=usuario_atual
            )
            if suc_salv:
                st.success(msg_salv)
                st.rerun()
            else:
                st.error(msg_salv)

    with cols_actions_s[1]:
        if st.button("✅ Finalizar Avaliação de TODA a Campanha", use_container_width=True, help="Conclui a avaliação da campanha e gera o status final"):
            suc_fin, msg_fin = finalizar_avaliacao_campanha(engine, camp_id, usuario_atual)
            if suc_fin:
                st.success(msg_fin)
                st.rerun()
            else:
                st.error(msg_fin)

    if is_admin and len(cols_actions_s) > 2:
        with cols_actions_s[2]:
            with st.popover("🗑️ Excluir (Admin)"):
                st.error(f"⚠️ Atenção: Esta ação excluirá permanentemente a campanha `{camp['codigo_campanha']}` e todos os seus dados e testes.")
                if st.button("Confirmar Exclusão", type="primary", key="btn_del_camp_supply_admin"):
                    suc_d, msg_d = excluir_campanha(engine, camp_id, usuario_atual)
                    if suc_d:
                        st.success(msg_d)
                        st.rerun()
                    else:
                        st.error(msg_d)

    # Exportações
    st.markdown("---")
    st.subheader("📑 6. Exportações Operacionais")
    col_exp1, col_exp2 = st.columns(2)

    with col_exp1:
        excel_transf = gerar_excel_transferencia_operacional(engine, camp_id)
        st.download_button(
            label="📥 Baixar Excel Operacional de Transferência (Supply -> CD)",
            data=excel_transf,
            file_name=f"transferencia_campanha_{camp['codigo_campanha']}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    with col_exp2:
        excel_devolutiva = gerar_excel_devolutiva_compras(engine, camp_id)
        st.download_button(
            label="📑 Baixar Excel de Devolutiva de Compras (Faltas CD15)",
            data=excel_devolutiva,
            file_name=f"devolutiva_compras_{camp['codigo_campanha']}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

