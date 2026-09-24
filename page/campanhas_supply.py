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
    salvar_fechamento_supply,
    finalizar_avaliacao_campanha,
    carregar_dados_produto_consolidado,
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

    # Campanhas disponíveis para o Supply
    campanhas = listar_campanhas(
        engine=engine,
        status_filtro=["ENVIADA_SUPPLY", "EM_AVALIACAO_SUPPLY", "PENDENCIA_COMPRAS", "FINALIZADA", "ATIVA"]
    )

    if not campanhas:
        st.info("Nenhuma campanha aguardando avaliação ou finalizada no momento.")
        return

    camp_dict = {f"{c['codigo_campanha']} — {c['nome']} [{c['status']}]": c["id"] for c in campanhas}
    sel_camp_label = st.selectbox("Selecione a Campanha para Avaliação:", list(camp_dict.keys()), key="sel_camp_supply")
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

        label = f"{tag} {it['produto_codigo']} — {it['descricao_snapshot']} [{it.get('fornecedor') or 'GERAL'}]"
        itens_labels[label] = it

    sel_item_label = col_sel_it.selectbox("Selecione o Produto para Analisar / Fechar:", list(itens_labels.keys()), key="sel_item_supply")
    item_atual = itens_labels[sel_item_label]
    lojas_ativas = item_atual.get("lojas_ativas", [])

    st.markdown(f"### 📦 `{item_atual['produto_codigo']}` — **{item_atual['descricao_snapshot']}**")
    st.caption(f"🏢 **Fornecedor:** {item_atual.get('fornecedor') or 'GERAL'} | 🏷️ **Departamento:** {item_atual.get('departamento') or 'N/D'} | 🚚 **Embalagem Transf:** {item_atual['embalagem_transferencia']} un | 📦 **Lojas Ativas Participantes:** {len(lojas_ativas)} de 14")

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
    st.subheader(f"📐 1. Dimensões Físicas do Produto ({item_atual['produto_codigo']})")
    
    dim_existente = prod_consolidado.get("dimensoes") or {}
    alt_p_padrao = float(dim_existente.get("altura_cm", 10.0) or 10.0)
    larg_p_padrao = float(dim_existente.get("largura_cm", 10.0) or 10.0)
    prof_p_padrao = float(dim_existente.get("profundidade_cm", 10.0) or 10.0)

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
                    usuario=usuario_atual
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
    
    estruturas = obter_estruturas_e_bandejas(engine)
    if not estruturas:
        st.warning("Nenhuma estrutura física com bandejas cadastrada. Cadastre em Administração de Exposição.")
        capacidade_calculada_sku = 0
        estrutura_selecionada_id = None
    else:
        est_dict = {f"{e['tipo_nome']} — {e['estrutura_nome']} ({len(e['bandejas'])} bandejas)": e for e in estruturas}
        col_c1, col_c2 = st.columns([2, 1])
        sel_est_label = col_c1.selectbox("Modelo Físico da Exposição:", list(est_dict.keys()), key="sel_est_cubagem")
        est_obj = est_dict[sel_est_label]
        estrutura_selecionada_id = est_obj["estrutura_id"]

        skus_compartilhados = col_c2.number_input(
            "Nº de SKUs Compartilhados:",
            min_value=1,
            max_value=10,
            value=1,
            step=1,
            help="Ex: 3 sabores do mesmo produto dividindo a mesma ponta de gôndola."
        )

        dim_prod_dict = {"altura_cm": alt_p, "largura_cm": larg_p, "profundidade_cm": prof_p}
        capacidade_calculada_sku = calcular_capacidade_exposicao(
            bandejas=est_obj["bandejas"],
            produto_dimensoes=dim_prod_dict,
            total_skus=skus_compartilhados
        )

        # Card de resultado da cubagem
        with st.container(border=True):
            col_res1, col_res2, col_res3 = st.columns(3)
            col_res1.metric("Estrutura Física", est_obj["estrutura_nome"])
            col_res2.metric("Total de Bandejas", f"{len(est_obj['bandejas'])} níveis")
            col_res3.metric("Capacidade Física para este SKU", f"{capacidade_calculada_sku} unidades", delta="Calculado")

    # -------------------------------------------------------------------------
    # SEÇÃO 3: ANÁLISE LOJA A LOJA E FECHAMENTO DE VOLUME (SOMENTE LOJAS ATIVAS)
    # -------------------------------------------------------------------------
    st.subheader(f"🏪 3. Fechamento do Supply por Loja ({len(lojas_ativas)} lojas participantes)")
    st.caption("Ajuste o volume final por loja participante. Lojas 'INATIVA' foram excluídas automaticamente pois não terão oferta.")

    emb_transf = max(1, int(item_atual["embalagem_transferencia"] or 1))
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
                tipo_exp_definido = lj.get("tipo_exposicao_nome") or "Exposição Geral"
                
                # Cabeçalho da loja com tipo de exposição SOMENTE LEITURA (definido pelo compras)
                st.write(f"🏬 **{lj_cod} - {lj_nome}**")
                st.markdown(f"🏷️ **Exposição Definida pelo Compras:** <span style='background-color:#0284c7;color:white;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:bold;'>{tipo_exp_definido}</span>", unsafe_allow_html=True)

                # Snapshot de dados da loja
                c_s1, c_s2, c_s3 = st.columns(3)
                c_s1.caption(f"Estoque Loja: **{float(lj['estoque_loja'] or 0):,.0f}**")
                c_s2.caption(f"Venda Proj: **{float(lj['venda_projetada'] or 0):.0f}**")
                c_s3.caption(f"Sug. Compras: **{int(lj['volume_comprador'] or 0)}**")

                # Valor padrão: se volume_final_supply já foi preenchido, usa ele; senão usa a cubagem calculada ou sugestão de compras
                val_inicial = int(lj.get("volume_final_supply") or 0)
                if val_inicial == 0:
                    val_inicial = capacidade_calculada_sku if capacidade_calculada_sku > 0 else int(lj.get("volume_comprador") or 0)

                vol_final_supply = st.number_input(
                    f"Volume Final Supply (Unidades) - Loja {lj_cod}:",
                    min_value=0,
                    value=val_inicial,
                    step=1,
                    key=f"vol_final_supply_{item_atual['produto_codigo']}_{lj_cod}"
                )

                cx_calc, vol_efetivo = calcular_caixas_transferencia(vol_final_supply, emb_transf)
                total_caixas_previstas += cx_calc
                total_unidades_efetivas += vol_efetivo

                st.markdown(f"📦 **Caixas Transferência:** `{cx_calc} cx` (Vol Efetivo: `{vol_efetivo} un`)")

                fechamento_inputs.append({
                    "loja_codigo": lj_cod,
                    "volume_final_supply": vol_final_supply
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
    col_act_s1, col_act_s2 = st.columns(2)

    with col_act_s1:
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

    with col_act_s2:
        if st.button("✅ Finalizar Avaliação de TODA a Campanha", use_container_width=True, help="Conclui a avaliação da campanha e gera o status final"):
            suc_fin, msg_fin = finalizar_avaliacao_campanha(engine, camp_id, usuario_atual)
            if suc_fin:
                st.success(msg_fin)
                st.rerun()
            else:
                st.error(msg_fin)

    # Exportações
    st.markdown("---")
    st.subheader("📑 5. Exportações Operacionais")
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

