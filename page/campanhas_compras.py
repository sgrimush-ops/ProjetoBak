"""
page/campanhas_compras.py
Página A - Compras: Criação, Parametrização e Gestão de Campanhas de Exposição.
"""

from __future__ import annotations
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from services.campanha_service import (
    listar_campanhas,
    obter_campanha_por_id,
    criar_campanha,
    atualizar_campanha,
    inativar_campanha,
    replicar_campanha,
    carregar_dados_produto_consolidado,
    salvar_item_campanha_compras,
    remover_item_campanha,
    enviar_campanha_para_supply,
    obter_itens_campanha_com_detalhes,
    LISTA_14_LOJAS
)
from services.campanha_calculo import calcular_dias_campanha
from services.exportacao_campanha import gerar_excel_devolutiva_compras


def _obter_tipos_exposicao(engine) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, nome FROM tipos_exposicao WHERE ativo = TRUE ORDER BY nome")).fetchall()
        return [dict(r._mapping) for r in rows]


def _obter_lojas_cadastradas(engine) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT codigo, nome FROM lojas WHERE tipo = 'LOJA' AND ativa = TRUE ORDER BY codigo")).fetchall()
        return [dict(r._mapping) for r in rows]


def show_campanhas_compras_page(engine, base_data_path: str = "data"):
    st.title("🎯 Gestão de Campanhas de Exposição — Compras")
    st.caption("Criação de campanhas, definição de mix promocional e tipos de exposição por loja.")

    usuario_atual = st.session_state.get("username", "compras")

    tipos_exp = _obter_tipos_exposicao(engine)
    mapa_tipos_exp = {t["nome"]: t["id"] for t in tipos_exp}
    lista_nomes_tipos = list(mapa_tipos_exp.keys())

    # Índice padrão de "INATIVA"
    idx_inativa_padrao = lista_nomes_tipos.index("INATIVA") if "INATIVA" in lista_nomes_tipos else 0

    lojas_db = _obter_lojas_cadastradas(engine)
    lojas_map = {l["codigo"]: l["nome"] for l in lojas_db}

    # Abas Principais
    tab_ativa, tab_nova, tab_existentes, tab_devolutivas = st.tabs([
        "📌 Campanha Selecionada",
        "➕ Nova Campanha",
        "📂 Todas as Campanhas",
        "📋 Devolutivas de Compras"
    ])

    # =========================================================================
    # ABA 2: NOVA CAMPANHA
    # =========================================================================
    with tab_nova:
        st.subheader("Criar Nova Campanha")
        with st.form("form_nova_campanha", clear_on_submit=True):
            nome_camp = st.text_input("Nome da Campanha *", placeholder="Ex: Festival de Bebidas de Outubro 2026")
            col_d1, col_d2 = st.columns(2)
            d_ini = col_d1.date_input("Data Início *", value=date.today() + timedelta(days=7))
            d_fim = col_d2.date_input("Data Fim *", value=date.today() + timedelta(days=21))
            obs = st.text_area("Observações da Negociação / Fornecedor (Opcional)")

            btn_criar = st.form_submit_button("🚀 Criar Campanha (Rascunho)", type="primary")
            if btn_criar:
                sucesso, msg, nova_camp = criar_campanha(
                    engine=engine,
                    nome=nome_camp,
                    data_inicio=d_ini,
                    data_fim=d_fim,
                    usuario=usuario_atual,
                    observacoes=obs
                )
                if sucesso and nova_camp:
                    st.success(msg)
                    st.session_state["campanha_selecionada_id"] = nova_camp["id"]
                    st.rerun()
                else:
                    st.error(msg)

    # =========================================================================
    # ABA 3: TODAS AS CAMPANHAS
    # =========================================================================
    with tab_existentes:
        st.subheader("Campanhas Cadastradas")
        col_f1, col_f2 = st.columns([2, 1])
        status_filtro = col_f1.multiselect(
            "Filtrar por Status:",
            ["RASCUNHO", "ATIVA", "ENVIADA_SUPPLY", "EM_AVALIACAO_SUPPLY", "PENDENCIA_COMPRAS", "FINALIZADA", "INATIVA", "CANCELADA"],
            default=["RASCUNHO", "ATIVA", "ENVIADA_SUPPLY", "EM_AVALIACAO_SUPPLY", "PENDENCIA_COMPRAS"]
        )
        apenas_vigentes = col_f2.checkbox("Apenas Vigentes", value=False)

        campanhas_lista = listar_campanhas(engine, status_filtro=status_filtro, apenas_vigentes=apenas_vigentes)

        if not campanhas_lista:
            st.info("Nenhuma campanha encontrada para os filtros selecionados.")
        else:
            for c in campanhas_lista:
                with st.expander(f"🏷️ {c['codigo_campanha']} — {c['nome']} [{c['status']}]"):
                    st.write(f"**Vigência:** {c['data_inicio'].strftime('%d/%m/%Y')} até {c['data_fim'].strftime('%d/%m/%Y')} ({calcular_dias_campanha(c['data_inicio'], c['data_fim'])} dias)")
                    st.write(f"**Total de Produtos:** {c['total_itens']} | **Pendências de Compra:** {c['total_pendencias']}")
                    st.write(f"**Criado por:** {c['usuario_criacao']} em {c['data_criacao'].strftime('%d/%m/%Y %H:%M')}")
                    if c["observacoes"]:
                        st.caption(f"Obs: {c['observacoes']}")

                    col_act1, col_act2 = st.columns([1, 4])
                    if col_act1.button("Selecionar / Editar", key=f"sel_camp_{c['id']}", type="primary"):
                        st.session_state["campanha_selecionada_id"] = c["id"]
                        st.rerun()

    # =========================================================================
    # ABA 4: DEVOLUTIVAS DE COMPRAS (COM DOWNLOAD)
    # =========================================================================
    with tab_devolutivas:
        st.subheader("📋 Devolutivas de Faltas no CD15 (Necessidade de Compra)")
        st.caption("Itens apontados pelo Supply com saldo insuficiente no CD15 para atendimento das lojas da campanha.")

        camp_selecionada_id = st.session_state.get("campanha_selecionada_id")
        
        # Botões de Download da Devolutiva
        col_down1, col_down2 = st.columns(2)
        if camp_selecionada_id:
            with col_down1:
                excel_dev_camp = gerar_excel_devolutiva_compras(engine, camp_selecionada_id)
                st.download_button(
                    label="📑 Baixar Excel de Devolutiva da Campanha Selecionada",
                    data=excel_dev_camp,
                    file_name=f"devolutiva_compras_campanha_selecionada_{date.today().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                    use_container_width=True
                )

        with engine.connect() as conn:
            devolutivas_df = pd.read_sql(text("""
                SELECT 
                    cd.id,
                    c.codigo_campanha AS "Campanha",
                    cd.produto_codigo AS "Código",
                    cd.descricao_snapshot AS "Descrição",
                    COALESCE(ci.fornecedor, ci.comprador, 'GERAL') AS "Fornecedor",
                    cd.caixas_necessarias AS "Caixas Necessárias",
                    cd.caixas_cd_disponivel AS "Caixas CD15",
                    cd.caixas_falta AS "Caixas a Comprar",
                    ci.embalagem_compra AS "Emb Compra",
                    cd.situacao AS "Status",
                    cd.observacao AS "Observação",
                    cd.data_geracao AS "Data Alerta"
                FROM campanha_devolutivas cd
                JOIN campanhas c ON c.id = cd.campanha_id
                JOIN campanha_itens ci ON ci.campanha_id = cd.campanha_id AND ci.produto_codigo = cd.produto_codigo
                ORDER BY cd.data_geracao DESC
            """), conn)

        if devolutivas_df.empty:
            st.success("🎉 Nenhuma pendência de compra em aberto no momento.")
        else:
            with col_down2:
                # Download consolidado de todas as devoluções
                import io
                out_all_dev = io.BytesIO()
                with pd.ExcelWriter(out_all_dev, engine="openpyxl") as writer:
                    devolutivas_df.to_excel(writer, sheet_name="Todas_Devolutivas", index=False)
                st.download_button(
                    label="📥 Baixar Excel Consolidado de TODAS as Devolutivas",
                    data=out_all_dev.getvalue(),
                    file_name=f"todas_devolutivas_compras_{date.today().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

            st.markdown("---")
            st.dataframe(devolutivas_df, use_container_width=True)

            # Opção de resolver pendência
            st.markdown("---")
            st.markdown("#### Atualizar Status da Devolutiva")
            col_d_id, col_d_st, col_d_btn = st.columns([2, 2, 1])
            sel_dev_id = col_d_id.selectbox("Selecione o ID da Devolutiva:", devolutivas_df["id"].tolist())
            novo_st_dev = col_d_st.selectbox("Novo Status:", ["PENDENTE", "COMPRADO", "RESOLVIDO"])
            if col_d_btn.button("Atualizar Status", key="btn_up_dev"):
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE campanha_devolutivas 
                        SET situacao = :st, data_resolucao = NOW(), usuario_resolucao = :user
                        WHERE id = :id
                    """), {"st": novo_st_dev, "user": usuario_atual, "id": sel_dev_id})
                st.success("Devolutiva atualizada com sucesso!")
                st.rerun()

    # =========================================================================
    # ABA 1: CAMPANHA SELECIONADA
    # =========================================================================
    with tab_ativa:
        camp_id = st.session_state.get("campanha_selecionada_id")
        if not camp_id:
            st.info("Nenhuma campanha selecionada. Crie uma nova campanha na aba '➕ Nova Campanha' ou escolha uma existente em '📂 Todas as Campanhas'.")
            return

        camp = obter_campanha_por_id(engine, camp_id)
        if not camp:
            st.error("Campanha selecionada não foi encontrada no banco.")
            st.session_state["campanha_selecionada_id"] = None
            return

        # Cabeçalho da Campanha
        dias_camp = calcular_dias_campanha(camp["data_inicio"], camp["data_fim"])
        status_color = {
            "RASCUNHO": "#64748b",
            "ATIVA": "#0284c7",
            "ENVIADA_SUPPLY": "#d97706",
            "EM_AVALIACAO_SUPPLY": "#d97706",
            "PENDENCIA_COMPRAS": "#dc2626",
            "FINALIZADA": "#16a34a",
            "INATIVA": "#dc2626",
            "CANCELADA": "#dc2626"
        }.get(camp["status"], "#64748b")

        st.markdown(f"### 🏷️ `{camp['codigo_campanha']}` — {camp['nome']} <span style='background-color:{status_color};color:white;padding:3px 8px;border-radius:4px;font-size:12px;font-weight:bold;'>{camp['status']}</span>", unsafe_allow_html=True)
        st.write(f"**Vigência:** {camp['data_inicio'].strftime('%d/%m/%Y')} até {camp['data_fim'].strftime('%d/%m/%Y')} ({dias_camp} dias) | **Criado por:** {camp['usuario_criacao']}")

        # Ações na Campanha
        col_btn1, col_btn2, col_btn3, col_btn4, col_btn5 = st.columns([1.2, 1.2, 1.2, 1.5, 1.5])

        with col_btn1:
            with st.popover("✏️ Editar Capa"):
                novo_nome = st.text_input("Nome:", value=camp["nome"], key="edit_nome")
                col_e1, col_e2 = st.columns(2)
                novo_d_ini = col_e1.date_input("Início:", value=camp["data_inicio"], key="edit_d_ini")
                novo_d_fim = col_e2.date_input("Fim:", value=camp["data_fim"], key="edit_d_fim")
                nova_obs = st.text_area("Observações:", value=camp.get("observacoes") or "", key="edit_obs")
                if st.button("Salvar Alterações de Capa", type="primary"):
                    suc, msg = atualizar_campanha(engine, camp_id, novo_nome, novo_d_ini, novo_d_fim, usuario_atual, nova_obs)
                    if suc:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

        with col_btn2:
            if camp["status"] != "INATIVA":
                if st.button("⏸️ Inativar", help="Suspende a campanha"):
                    suc, msg = inativar_campanha(engine, camp_id, usuario_atual)
                    if suc:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

        with col_btn3:
            with st.popover("🔄 Replicar"):
                rep_nome = st.text_input("Novo Nome da Campanha:", value=f"Cópia de {camp['nome']}")
                col_r1, col_r2 = st.columns(2)
                rep_ini = col_r1.date_input("Nova Data Início:", value=date.today() + timedelta(days=7), key="rep_ini")
                rep_fim = col_r2.date_input("Nova Data Fim:", value=date.today() + timedelta(days=21), key="rep_fim")
                if st.button("Confirmar Replicação", type="primary"):
                    suc, msg, new_id = replicar_campanha(engine, camp_id, rep_nome, rep_ini, rep_fim, usuario_atual)
                    if suc and new_id:
                        st.success(msg)
                        st.session_state["campanha_selecionada_id"] = new_id
                        st.rerun()
                    else:
                        st.error(msg)

        with col_btn4:
            # Botão de Download da Devolutiva de Compras
            excel_dev_btn = gerar_excel_devolutiva_compras(engine, camp_id)
            st.download_button(
                label="📑 Baixar Devolutiva",
                data=excel_dev_btn,
                file_name=f"devolutiva_{camp['codigo_campanha']}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Baixa a planilha de faltas no CD15 e sugestão de compra"
            )

        with col_btn5:
            if camp["status"] in ["RASCUNHO", "ATIVA", "PENDENCIA_COMPRAS"]:
                if st.button("🚀 Enviar para Supply", type="primary", help="Envia os produtos e tipos de exposição ativos para conferência física do Supply"):
                    suc, msg = enviar_campanha_para_supply(engine, camp_id, usuario_atual)
                    if suc:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

        st.markdown("---")

        # =====================================================================
        # PRODUTOS DA CAMPANHA
        # =====================================================================
        st.subheader("📦 Mix de Produtos da Campanha")

        itens_campanha = obter_itens_campanha_com_detalhes(engine, camp_id)

        # Seção para Adicionar / Buscar Novo Produto no Parquet
        with st.expander("🔍 Adicionar Novo Produto ao Mix (Busca no ERP Consinco)", expanded=(len(itens_campanha) == 0)):
            col_b1, col_b2 = st.columns([3, 1])
            termo_busca = col_b1.text_input("Digite o Código Consinco ou Descrição do Produto:", key="busca_prod_termo")

            # Sugestão de produtos
            produto_encontrado_cod = None
            if termo_busca:
                parquet_path = "bdados/query.parquet"
                import os
                if os.path.exists(parquet_path):
                    try:
                        df_p = pd.read_parquet(parquet_path)
                        df_p.columns = [str(c).strip() for c in df_p.columns]
                        df_p_uniq = df_p.drop_duplicates(subset=["CODIGO_PRODUTO"])
                        
                        if termo_busca.isdigit():
                            filtro = df_p_uniq["CODIGO_PRODUTO"] == int(termo_busca)
                        else:
                            filtro = df_p_uniq["DESCRICAO_PRODUTO"].astype(str).str.contains(termo_busca, case=False, na=False)
                        
                        match_df = df_p_uniq[filtro].head(15)
                        if not match_df.empty:
                            opcoes_dict = {
                                f"{int(row['CODIGO_PRODUTO'])} - {row['DESCRICAO_PRODUTO']} [{row.get('FORNECEDOR') or row.get('COMPRADOR') or 'GERAL'}]": int(row['CODIGO_PRODUTO'])
                                for _, row in match_df.iterrows()
                            }
                            sel_label = st.selectbox("Selecione o Produto Encontrado:", list(opcoes_dict.keys()))
                            produto_encontrado_cod = opcoes_dict[sel_label]
                        else:
                            st.warning("Nenhum produto localizado com esse termo.")
                    except Exception as e:
                        st.error(f"Erro ao buscar no parquet: {e}")

            if produto_encontrado_cod:
                prod_data = carregar_dados_produto_consolidado(
                    engine=engine,
                    produto_codigo=produto_encontrado_cod,
                    data_inicio=camp["data_inicio"],
                    data_fim=camp["data_fim"],
                    base_data_path=base_data_path
                )

                # Card Resumo do Produto (Embalagens Somente Leitura)
                st.markdown("---")
                st.markdown(f"#### Detalhes do Produto: `{prod_data['produto_codigo']}` — **{prod_data['descricao']}**")
                st.caption(f"🏢 **Fornecedor:** {prod_data.get('fornecedor') or 'GERAL'} | 🏷️ **Departamento:** {prod_data.get('departamento') or 'N/D'} | 👤 **Comprador:** {prod_data.get('comprador') or 'N/D'}")
                
                c_m1, c_m2, c_m3, c_m4 = st.columns(4)
                c_m1.metric("Estoque Total Lojas", f"{prod_data['estoque_total_lojas']:,.0f} un")
                c_m2.metric("Estoque Disponível CD15", f"{prod_data['estoque_cd15']:,.0f} un")
                c_m3.metric("Venda Média Diária", f"{prod_data['venda_media_diaria_total']:.1f} un/dia")
                c_m4.metric(f"Venda Projetada ({dias_camp} dias)", f"{prod_data['venda_projetada_total']:.0f} un")

                # Exibição informativa das embalagens (Somente Leitura para Comprador)
                col_e1, col_e2 = st.columns(2)
                col_e1.info(f"📦 **Embalagem de Compra:** `{prod_data['embalagem_compra']}` un/cx *(Fixo do Cadastro ERP)*")
                col_e2.info(f"🚚 **Embalagem de Transferência:** `{prod_data['embalagem_transferencia']}` un/cx *(Fixo do Cadastro ERP)*")

                # Grid de Lojas para Compras com Padrão INATIVA
                st.markdown("##### 🏪 Definição de Exposição e Participação por Loja")
                st.info("💡 **Regra de Participação:** Todas as lojas iniciam com **INATIVA** (sem ponto extra e sem oferta). Selecione o tipo de exposição (ex: `ILHA`, `PONTA DE GÔNDOLA`) apenas nas lojas que participarão da campanha.")

                lojas_inputs = []
                col_g1, col_g2 = st.columns(2)
                metade = len(LISTA_14_LOJAS) // 2

                for i, lj_cod in enumerate(LISTA_14_LOJAS):
                    container_col = col_g1 if i < metade else col_g2
                    with container_col:
                        with st.container(border=True):
                            lj_nome = lojas_map.get(lj_cod, f"Loja {lj_cod}")
                            lj_detalhe = prod_data["lojas_detalhe"].get(lj_cod, {})
                            st_lj = lj_detalhe.get("estoque_loja", 0.0)
                            vp_lj = lj_detalhe.get("venda_projetada", 0.0)
                            
                            st.write(f"🏬 **{lj_cod} - {lj_nome}**")
                            st.caption(f"Estoque Atual: **{st_lj:,.0f} un** | Venda Proj: **{vp_lj:.0f} un**")
                            
                            tipo_sel = st.selectbox(
                                "Tipo de Exposição:",
                                options=lista_nomes_tipos,
                                index=idx_inativa_padrao,
                                key=f"tipo_exp_{prod_data['produto_codigo']}_{lj_cod}"
                            )
                            
                            is_inativa = tipo_sel.upper() == "INATIVA"
                            if is_inativa:
                                st.caption("⏸️ *Loja não participará do ponto extra / Não enviada ao Supply.*")
                                vol_sug = 0
                            else:
                                st.success(f"✅ **Ativa:** Exposição em `{tipo_sel}`")
                                vol_sug = st.number_input(
                                    "Sugestão Comprador (Unidades):",
                                    min_value=0,
                                    value=0,
                                    step=1,
                                    key=f"vol_sug_{prod_data['produto_codigo']}_{lj_cod}",
                                    help="Deixe 0 se desejar que o Supply calcule o volume com base na capacidade física da bandeja."
                                )

                            lojas_inputs.append({
                                "loja_codigo": lj_cod,
                                "tipo_exposicao_id": mapa_tipos_exp[tipo_sel],
                                "volume_comprador": vol_sug,
                                "estoque_loja": st_lj,
                                "estoque_cd": prod_data["estoque_cd15"],
                                "venda_media": lj_detalhe.get("venda_media", 0.0),
                                "venda_projetada": vp_lj
                            })

                if st.button("💾 Salvar Produto e Matriz de Lojas na Campanha", type="primary", key="btn_salvar_prod_matriz"):
                    suc, msg = salvar_item_campanha_compras(
                        engine=engine,
                        campanha_id=camp_id,
                        produto_codigo=prod_data["produto_codigo"],
                        descricao=prod_data["descricao"],
                        embalagem_compra=prod_data["embalagem_compra"],
                        embalagem_transferencia=prod_data["embalagem_transferencia"],
                        dados_lojas=lojas_inputs,
                        usuario=usuario_atual,
                        fornecedor=prod_data.get("fornecedor"),
                        departamento=prod_data.get("departamento"),
                        comprador=prod_data.get("comprador")
                    )
                    if suc:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

        # Exibição dos Itens Já Cadastrados com Ordenação por Fornecedor
        if not itens_campanha:
            st.info("Nenhum produto cadastrado nesta campanha ainda. Use o campo de busca acima para adicionar.")
        else:
            st.markdown("---")
            col_head_it, col_ord = st.columns([2, 2])
            col_head_it.markdown(f"#### Itens Cadastrados na Campanha ({len(itens_campanha)} produtos)")
            
            criterio_ord = col_ord.selectbox(
                "Ordenar lista de itens por:",
                ["🏢 Fornecedor (A-Z)", "🔢 Código do Produto", "📝 Descrição do Produto", "🏷️ Departamento", "🌟 Mais Lojas Ativas"],
                key="ord_itens_mix_compras"
            )

            # Aplicação da ordenação
            if "Fornecedor" in criterio_ord:
                itens_ordenados = sorted(itens_campanha, key=lambda x: str(x.get("fornecedor") or "").upper())
            elif "Código" in criterio_ord:
                itens_ordenados = sorted(itens_campanha, key=lambda x: int(x.get("produto_codigo") or 0))
            elif "Descrição" in criterio_ord:
                itens_ordenados = sorted(itens_campanha, key=lambda x: str(x.get("descricao_snapshot") or "").upper())
            elif "Departamento" in criterio_ord:
                itens_ordenados = sorted(itens_campanha, key=lambda x: str(x.get("departamento") or "").upper())
            elif "Lojas Ativas" in criterio_ord:
                itens_ordenados = sorted(itens_campanha, key=lambda x: len(x.get("lojas_ativas", [])), reverse=True)
            else:
                itens_ordenados = itens_campanha

            for it in itens_ordenados:
                qtd_ativas = len(it.get("lojas_ativas", []))
                qtd_inativas = len(it.get("lojas", [])) - qtd_ativas
                status_lojas_tag = f"🟢 {qtd_ativas} lojas ativas" if qtd_ativas > 0 else "⚪ 0 lojas ativas (INATIVO)"
                
                exp_label = f"📦 [{it.get('fornecedor') or 'GERAL'}] `{it['produto_codigo']}` — {it['descricao_snapshot']} | {status_lojas_tag}"
                
                with st.expander(exp_label):
                    c_act1, c_act2 = st.columns([4, 1])
                    c_act1.write(f"🏢 **Fornecedor:** {it.get('fornecedor') or 'GERAL'} | 🏷️ **Depto:** {it.get('departamento') or 'N/D'} | 📦 **Emb Compra:** {it['embalagem_compra']} un | 🚚 **Emb Transf:** {it['embalagem_transferencia']} un")
                    if c_act2.button("🗑️ Remover do Mix", key=f"del_prod_{it['produto_codigo']}", type="secondary"):
                        suc, msg = remover_item_campanha(engine, camp_id, it["produto_codigo"], usuario_atual)
                        if suc:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)

                    # Tabela Resumo das Lojas para este produto
                    lojas_it_df = pd.DataFrame([
                        {
                            "Loja": f"{l['loja_codigo']} - {l['loja_nome']}",
                            "Participação": "⏸️ INATIVA" if str(l["tipo_exposicao_nome"]).upper() == "INATIVA" else f"✅ {l['tipo_exposicao_nome']}",
                            "Estoque Loja": f"{float(l['estoque_loja'] or 0):,.0f} un",
                            "Venda Proj": f"{float(l['venda_projetada'] or 0):.0f} un",
                            "Sugestão Compras": f"{int(l['volume_comprador'] or 0)} un",
                            "Volume Supply": f"{int(l['volume_final_supply'] or 0)} un",
                            "Caixas Transf": f"{int(l['caixas_transferencia'] or 0)} cx",
                            "Status Estoque CD": l["status_estoque"]
                        }
                        for l in it["lojas"]
                    ])
                    st.dataframe(lojas_it_df, use_container_width=True, hide_index=True)

