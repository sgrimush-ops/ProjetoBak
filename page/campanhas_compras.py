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
    excluir_campanha,
    replicar_campanha,
    carregar_dados_produto_consolidado,
    salvar_item_campanha_compras,
    salvar_familia_campanha_compras,
    remover_item_campanha,
    enviar_campanha_para_supply,
    reenviar_campanha_para_supply,
    alterar_exposicao_lojas_item,
    atualizar_status_devolutiva_familia,
    obter_itens_campanha_com_detalhes,
    obter_lista_compradores,
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
    role_str = str(st.session_state.get("role", "")).strip().lower()
    cargo_str = str(st.session_state.get("cargo", "")).strip().lower()
    is_admin = role_str == "admin" or "admin" in cargo_str or usuario_atual in ["admin", "administrador"]

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

    lista_compradores = obter_lista_compradores(engine)

    # =========================================================================
    # ABA 3: TODAS AS CAMPANHAS (COM FILTROS AVANÇADOS)
    # =========================================================================
    with tab_existentes:
        st.subheader("📂 Consulta e Gestão de Campanhas")
        st.caption("Filtre campanhas por loja, período de vigência, comprador e status operacional.")

        # Painel de Filtros Superiores
        with st.container(border=True):
            col_f1, col_f2, col_f3, col_f4 = st.columns([1.5, 1.5, 1.5, 1.5])

            with col_f1:
                opcoes_lojas_c = ["TODAS"] + [l["codigo"] for l in lojas_db]
                sel_loja_compras = st.selectbox(
                    "Filtrar por Loja:",
                    opcoes_lojas_c,
                    format_func=lambda x: "🌟 Todas as Lojas (Consolidado)" if x == "TODAS" else f"{x} - {lojas_map.get(x, 'Loja ' + x)}",
                    key="filtro_loja_compras_tab"
                )

            with col_f2:
                hoje_c = date.today()
                col_cd1, col_cd2 = st.columns(2)
                d_ini_c = col_cd1.date_input("Vigência De:", value=hoje_c - timedelta(days=30), key="d_ini_compras_tab")
                d_fim_c = col_cd2.date_input("Até:", value=hoje_c + timedelta(days=60), key="d_fim_compras_tab")

            with col_f3:
                sel_comprador_c = st.selectbox(
                    "Filtrar por Comprador:",
                    ["TODOS"] + lista_compradores,
                    key="filtro_comprador_compras_tab"
                )

            with col_f4:
                status_opcoes_c = ["RASCUNHO", "ATIVA", "ENVIADA_SUPPLY", "EM_AVALIACAO_SUPPLY", "PENDENCIA_COMPRAS", "FINALIZADA", "INATIVA", "CANCELADA"]
                status_default_c = ["RASCUNHO", "ATIVA", "ENVIADA_SUPPLY", "EM_AVALIACAO_SUPPLY", "PENDENCIA_COMPRAS", "FINALIZADA"]
                status_selecionados_c = st.multiselect(
                    "Status da Campanha:",
                    status_opcoes_c,
                    default=status_default_c,
                    key="filtro_status_compras_tab"
                )

        # Consulta SQL com filtros avançados
        where_compras = ["c.data_fim >= :d_ini", "c.data_inicio <= :d_fim"]
        params_compras: Dict[str, Any] = {
            "d_ini": d_ini_c,
            "d_fim": d_fim_c
        }

        if status_selecionados_c:
            where_compras.append("c.status = ANY(:status_list)")
            params_compras["status_list"] = status_selecionados_c

        if sel_loja_compras != "TODAS":
            where_compras.append("cl.loja_codigo = :loja_filtro")
            params_compras["loja_filtro"] = str(sel_loja_compras).zfill(3)

        if sel_comprador_c != "TODOS":
            where_compras.append("ci.comprador = :comp_filtro")
            params_compras["comp_filtro"] = str(sel_comprador_c).strip()

        where_sql_compras = " AND ".join(where_compras)

        sql_campanhas_compras = text(f"""
            SELECT DISTINCT
                c.id as campanha_id,
                c.codigo_campanha,
                c.nome as campanha_nome,
                c.data_inicio,
                c.data_fim,
                c.status,
                c.usuario_criacao,
                c.data_criacao,
                c.observacoes,
                COUNT(DISTINCT ci.produto_codigo) as total_skus,
                (SELECT COUNT(*) FROM campanha_devolutivas cd WHERE cd.campanha_id = c.id AND cd.situacao = 'PENDENTE') as total_pendencias,
                (SELECT STRING_AGG(DISTINCT ci.comprador, ', ') FROM campanha_itens ci WHERE ci.campanha_id = c.id AND ci.comprador IS NOT NULL AND ci.comprador != '' AND ci.comprador != 'None') as compradores,
                SUM(CASE WHEN c.status NOT IN ('INATIVA', 'CANCELADA') AND UPPER(COALESCE(te.nome, '')) != 'INATIVA' THEN COALESCE(cl.volume_final_supply, 0) ELSE 0 END) as total_unidades,
                SUM(CASE WHEN c.status NOT IN ('INATIVA', 'CANCELADA') AND UPPER(COALESCE(te.nome, '')) != 'INATIVA' THEN COALESCE(cl.caixas_transferencia, 0) ELSE 0 END) as total_caixas
            FROM campanhas c
            LEFT JOIN campanha_itens ci ON ci.campanha_id = c.id
            LEFT JOIN campanha_lojas cl ON cl.campanha_item_id = ci.id
            LEFT JOIN tipos_exposicao te ON te.id = cl.tipo_exposicao_id
            WHERE {where_sql_compras}
            GROUP BY c.id, c.codigo_campanha, c.nome, c.data_inicio, c.data_fim, c.status, c.usuario_criacao, c.data_criacao, c.observacoes
            ORDER BY c.data_inicio ASC, c.nome ASC
        """)

        with engine.connect() as conn:
            campanhas_lista = conn.execute(sql_campanhas_compras, params_compras).fetchall()

        if not campanhas_lista:
            st.info("Nenhuma campanha localizada para os filtros selecionados.")
        else:
            # Métricas Gerais da Consulta (Exclui inativas/canceladas do faturamento e caixas)
            campanhas_ativas_count = sum(1 for c in campanhas_lista if c.status not in ('INATIVA', 'CANCELADA'))
            tot_skus_c = sum(c.total_skus or 0 for c in campanhas_lista if c.status not in ('INATIVA', 'CANCELADA'))
            tot_un_c = sum(c.total_unidades or 0 for c in campanhas_lista if c.status not in ('INATIVA', 'CANCELADA'))
            tot_cx_c = sum(c.total_caixas or 0 for c in campanhas_lista if c.status not in ('INATIVA', 'CANCELADA'))
            tot_pend_c = sum(c.total_pendencias or 0 for c in campanhas_lista if c.status not in ('INATIVA', 'CANCELADA'))

            cm_1, cm_2, cm_3, cm_4 = st.columns(4)
            cm_1.metric("Campanhas Ativas / Filtradas", f"{campanhas_ativas_count} / {len(campanhas_lista)}")
            cm_2.metric("Total de Produtos (SKUs)", tot_skus_c)
            cm_3.metric("Volume Aprovado (Un)", f"{tot_un_c:,.0f} un")
            cm_4.metric("Caixas a Transferir / Pendências", f"{tot_cx_c:,.0f} cx", f"{tot_pend_c} faltas CD" if tot_pend_c > 0 else "0 faltas")

            st.markdown("---")

            for c in campanhas_lista:
                d_ini_f = c.data_inicio.strftime("%d/%m/%Y")
                d_fim_f = c.data_fim.strftime("%d/%m/%Y")
                dias_f = (c.data_fim - c.data_inicio).days + 1
                status_tag = c.status

                with st.expander(f"🏷️ `{c.codigo_campanha}` — **{c.campanha_nome}** [{status_tag}] | Mix: {c.total_skus} prods | Pendências: {c.total_pendencias}"):
                    st.write(f"📅 **Vigência:** {d_ini_f} até {d_fim_f} ({dias_f} dias) | 👤 **Comprador(es):** `{c.compradores or 'N/D'}` | **Criado por:** {c.usuario_criacao} em {c.data_criacao.strftime('%d/%m/%Y %H:%M')}")
                    if c.observacoes:
                        st.caption(f"📌 Obs: {c.observacoes}")

                    col_cact1, col_cact2, col_cact3 = st.columns([1.5, 1.5, 3])
                    with col_cact1:
                        if st.button("🎯 Abrir / Editar no Compras", key=f"sel_camp_{c.campanha_id}", type="primary"):
                            st.session_state["campanha_selecionada_id"] = c.campanha_id
                            st.rerun()

                    with col_cact2:
                        excel_dev_c = gerar_excel_devolutiva_compras(engine, c.campanha_id)
                        st.download_button(
                            label="📑 Devolutiva de Faltas",
                            data=excel_dev_c,
                            file_name=f"devolutiva_{c.codigo_campanha}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"btn_dev_exp_{c.campanha_id}"
                        )

                    if is_admin:
                        with col_cact3:
                            with st.popover("🗑️ Excluir Campanha (Admin)"):
                                st.warning(f"Excluir permanentemente `{c.codigo_campanha}` e limpar todos os registros de teste?")
                                if st.button("Confirmar Exclusão", type="primary", key=f"del_camp_list_{c.campanha_id}"):
                                    suc_d, msg_d = excluir_campanha(engine, c.campanha_id, usuario_atual)
                                    if suc_d:
                                        st.success(msg_d)
                                        if st.session_state.get("campanha_selecionada_id") == c.campanha_id:
                                            st.session_state["campanha_selecionada_id"] = None
                                        st.rerun()
                                    else:
                                        st.error(msg_d)

    # =========================================================================
    # ABA 4: DEVOLUTIVAS DE COMPRAS (COM DOWNLOAD E FILTRO DE COMPRADOR)
    # =========================================================================
    with tab_devolutivas:
        st.subheader("📋 Devolutivas de Faltas no CD15 (Necessidade de Compra)")
        st.caption("Itens apontados pelo Supply com saldo insuficiente no CD15 para atendimento das lojas da campanha.")

        col_filtro_dev1, col_filtro_dev2 = st.columns([2, 2])
        with col_filtro_dev1:
            filtro_situacao_dev = st.radio(
                "Visualização das Devolutivas:",
                ["🟡 Apenas Pendentes (A Comprar)", "📋 Todas as Devolutivas (Histórico)"],
                horizontal=True,
                key="filtro_sit_dev_radio"
            )

        with col_filtro_dev2:
            sel_comprador_dev = st.selectbox(
                "Filtrar por Comprador:",
                ["TODOS"] + lista_compradores,
                key="filtro_comprador_devolutivas_tab"
            )

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

        where_dev_clauses = []
        params_dev: Dict[str, Any] = {}

        if "Apenas Pendentes" in filtro_situacao_dev:
            where_dev_clauses.append("cd.situacao = 'PENDENTE'")

        if sel_comprador_dev != "TODOS":
            where_dev_clauses.append("ci.comprador = :comp_filtro")
            params_dev["comp_filtro"] = str(sel_comprador_dev).strip()

        where_dev_sql = ("WHERE " + " AND ".join(where_dev_clauses)) if where_dev_clauses else ""

        with engine.connect() as conn:
            devolutivas_df = pd.read_sql(text(f"""
                SELECT 
                    cd.id,
                    c.id AS campanha_id,
                    c.codigo_campanha AS "Campanha",
                    ci.codigo_familia AS "Cód Família",
                    COALESCE(ci.descricao_familia, ci.descricao_snapshot) AS "Família / Grupo",
                    cd.produto_codigo AS "Código",
                    cd.descricao_snapshot AS "Descrição",
                    COALESCE(ci.comprador, 'N/D') AS "Comprador",
                    COALESCE(ci.fornecedor, 'SEM FORNECEDOR') AS "Fornecedor",
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
                {where_dev_sql}
                ORDER BY cd.data_geracao DESC, ci.comprador, ci.fornecedor
            """), conn, params=params_dev)

        if devolutivas_df.empty:
            if "Apenas Pendentes" in filtro_situacao_dev:
                st.success("🎉 Nenhuma pendência de compra em aberto para os filtros selecionados!")
            else:
                st.info("Nenhuma devolutiva registrada no histórico para os filtros selecionados.")
        else:
            with col_down2:
                import io
                out_all_dev = io.BytesIO()
                with pd.ExcelWriter(out_all_dev, engine="openpyxl") as writer:
                    devolutivas_df.to_excel(writer, sheet_name="Devolutivas_Compras", index=False)
                st.download_button(
                    label="📥 Baixar Planilha das Devolutivas Exibidas",
                    data=out_all_dev.getvalue(),
                    file_name=f"devolutivas_compras_{date.today().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

            st.markdown("---")
            # Tabela de exibição formatada
            cols_exibicao = [c for c in devolutivas_df.columns if c not in ["campanha_id"]]
            st.dataframe(devolutivas_df[cols_exibicao], use_container_width=True, hide_index=True)

            # Opção de resolver pendência agrupada por FAMÍLIA DE PRODUTOS
            st.markdown("---")
            st.markdown("#### 👨‍👩‍👧‍👦 Atualizar Status por Família de Produtos")
            st.caption("A manutenção de compras é realizada por Família de Produtos, atualizando todas as pendências dos SKUs irmãos em lote.")

            opcoes_familia_dict = {}
            for _, row in devolutivas_df.iterrows():
                cid = row["campanha_id"]
                camp_cod = row["Campanha"]
                cod_fam = row["Cód Família"]
                prod_cod = row["Código"]
                desc = row["Descrição"]
                fam_desc = row["Família / Grupo"]
                status_atual = row["Status"]
                cx_falta = int(row["Caixas a Comprar"] or 0)
                
                if pd.notna(cod_fam) and str(cod_fam).isdigit() and int(cod_fam) > 0:
                    chave = f"FAM_{cid}_{int(cod_fam)}"
                    if chave not in opcoes_familia_dict:
                        opcoes_familia_dict[chave] = {
                            "campanha_id": cid,
                            "codigo_familia": int(cod_fam),
                            "produto_codigo": None,
                            "tipo": "FAMILIA",
                            "nome": f"👨‍👩‍👧‍👦 Família {int(cod_fam)} — {fam_desc} [{camp_cod}]",
                            "itens_count": 0,
                            "total_cx": 0,
                            "ids": []
                        }
                    opcoes_familia_dict[chave]["itens_count"] += 1
                    opcoes_familia_dict[chave]["total_cx"] += cx_falta
                    opcoes_familia_dict[chave]["ids"].append(row["id"])
                else:
                    chave = f"PROD_{cid}_{prod_cod}"
                    if chave not in opcoes_familia_dict:
                        opcoes_familia_dict[chave] = {
                            "campanha_id": cid,
                            "codigo_familia": None,
                            "produto_codigo": prod_cod,
                            "tipo": "PRODUTO",
                            "nome": f"📦 Produto {prod_cod} — {desc} [{camp_cod}]",
                            "itens_count": 0,
                            "total_cx": 0,
                            "ids": []
                        }
                    opcoes_familia_dict[chave]["itens_count"] += 1
                    opcoes_familia_dict[chave]["total_cx"] += cx_falta
                    opcoes_familia_dict[chave]["ids"].append(row["id"])

            labels_fam_map = {}
            for k, val in opcoes_familia_dict.items():
                lbl = f"{val['nome']} — {val['itens_count']} item(ns) | {val['total_cx']} cx a comprar"
                labels_fam_map[lbl] = val

            if labels_fam_map:
                col_d_fam, col_d_st, col_d_btn = st.columns([2.8, 1.4, 1.4])
                sel_fam_label = col_d_fam.selectbox(
                    "Selecione a Família / Grupo da Devolutiva:",
                    list(labels_fam_map.keys()),
                    key="sel_dev_fam_box"
                )
                fam_selecionada = labels_fam_map[sel_fam_label]
                novo_st_dev = col_d_st.selectbox("Novo Status:", ["COMPRADO", "RESOLVIDO", "PENDENTE"], key="sel_novo_st_dev")
                
                with col_d_btn:
                    st.write("")
                    st.write("")
                    if st.button("💾 Atualizar Família", key="btn_up_dev_fam", type="primary", use_container_width=True):
                        suc_dev, msg_dev = atualizar_status_devolutiva_familia(
                            engine=engine,
                            campanha_id=fam_selecionada["campanha_id"],
                            novo_status=novo_st_dev,
                            usuario=usuario_atual,
                            codigo_familia=fam_selecionada["codigo_familia"],
                            produto_codigo=fam_selecionada["produto_codigo"],
                            devolutiva_ids=fam_selecionada["ids"]
                        )
                        if suc_dev:
                            st.success(msg_dev)
                            st.rerun()
                        else:
                            st.error(msg_dev)

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
        cols_btns = st.columns([1.2, 1.2, 1.2, 1.5, 1.5, 1.2] if is_admin else [1.2, 1.2, 1.2, 1.5, 1.5])

        with cols_btns[0]:
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

        with cols_btns[1]:
            if camp["status"] != "INATIVA":
                if st.button("⏸️ Inativar", help="Suspende a campanha"):
                    suc, msg = inativar_campanha(engine, camp_id, usuario_atual)
                    if suc:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

        with cols_btns[2]:
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

        with cols_btns[3]:
            # Botão de Download da Devolutiva de Compras
            excel_dev_btn = gerar_excel_devolutiva_compras(engine, camp_id)
            st.download_button(
                label="📑 Baixar Devolutiva",
                data=excel_dev_btn,
                file_name=f"devolutiva_{camp['codigo_campanha']}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Baixa a planilha de faltas no CD15 e sugestão de compra"
            )

        with cols_btns[4]:
            if camp["status"] in ["RASCUNHO", "ATIVA"]:
                if st.button("🚀 Enviar para Supply", type="primary", help="Envia os produtos e tipos de exposição ativos para conferência física do Supply"):
                    suc, msg = enviar_campanha_para_supply(engine, camp_id, usuario_atual)
                    if suc:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
            else:
                if st.button("🔄 Reenviar para o Supply", type="primary", help="Reenvia a campanha para o Supply após alterações ou renegociações de exposição"):
                    suc, msg = reenviar_campanha_para_supply(engine, camp_id, usuario_atual)
                    if suc:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

        if is_admin and len(cols_btns) > 5:
            with cols_btns[5]:
                with st.popover("🗑️ Excluir (Admin)"):
                    st.error(f"⚠️ Atenção: Esta ação excluirá permanentemente a campanha `{camp['codigo_campanha']}` e todos os seus dados e testes.")
                    if st.button("Confirmar Exclusão Permanente", type="primary", key="btn_del_camp_adm_top"):
                        suc_d, msg_d = excluir_campanha(engine, camp_id, usuario_atual)
                        if suc_d:
                            st.success(msg_d)
                            st.session_state["campanha_selecionada_id"] = None
                            st.rerun()
                        else:
                            st.error(msg_d)

        st.markdown("---")

        # =====================================================================
        # PRODUTOS DA CAMPANHA
        # =====================================================================
        st.subheader("📦 Mix de Produtos da Campanha")

        itens_campanha = obter_itens_campanha_com_detalhes(engine, camp_id)

        # Seção para Adicionar / Buscar Novo Produto no Parquet
        session_mix_key = f"mix_selecionado_camp_{camp_id}"

        with st.expander("🔍 Adicionar Novo Produto ao Mix (Busca por Código, Descrição ou Família)", expanded=(len(itens_campanha) == 0 or session_mix_key in st.session_state)):
            col_b1, col_b2 = st.columns([3, 1])
            termo_busca = col_b1.text_input(
                "Digite o Código Consinco, Descrição ou Família do Produto:",
                key="busca_prod_termo",
                placeholder="Ex: Tang, Capsula Dolce Gusto, Coca Cola, Sabão Omo, 38902, 26..."
            )

            # Sugestão de produtos
            if termo_busca and termo_busca.strip():
                parquet_path = "bdados/query.parquet"
                import os
                if os.path.exists(parquet_path):
                    try:
                        df_p = pd.read_parquet(parquet_path)
                        df_p.columns = [str(c).strip() for c in df_p.columns]
                        df_p_uniq = df_p.drop_duplicates(subset=["CODIGO_PRODUTO"])
                        
                        termo_limpo = termo_busca.strip()
                        if termo_limpo.isdigit():
                            num_val = int(termo_limpo)
                            filtro = (df_p_uniq["CODIGO_PRODUTO"] == num_val)
                            if "CODIGO_FAMILIA" in df_p_uniq.columns:
                                filtro = filtro | (df_p_uniq["CODIGO_FAMILIA"] == num_val)
                        else:
                            tokens = [t.strip().upper() for t in termo_limpo.split() if t.strip()]
                            desc_col = df_p_uniq["DESCRICAO_PRODUTO"].astype(str).str.upper()
                            fam_col = df_p_uniq["DESCRICAO_FAMILIA"].astype(str).str.upper() if "DESCRICAO_FAMILIA" in df_p_uniq.columns else desc_col
                            forn_col = df_p_uniq["FORNECEDOR"].astype(str).str.upper() if "FORNECEDOR" in df_p_uniq.columns else desc_col
                            
                            filtro = pd.Series(True, index=df_p_uniq.index)
                            for tok in tokens:
                                filtro = filtro & (desc_col.str.contains(tok, regex=False, na=False) | fam_col.str.contains(tok, regex=False, na=False) | forn_col.str.contains(tok, regex=False, na=False))
                        
                        match_df = df_p_uniq[filtro].head(40)
                        if not match_df.empty:
                            col_b2.caption(f"🎯 **{len(match_df)} produto(s)** encontrados")

                            with st.form(key=f"form_selecao_mix_multi_{camp_id}", clear_on_submit=False):
                                st.markdown("##### 📋 Selecione os produtos para agrupar na mesma exposição:")
                                st.info("💡 **Marcação sem recarregamento:** Marque todas as famílias e sabores que você deseja juntar na **mesma estrutura/ponta física**. Você pode marcar múltiplos itens livremente. Ao finalizar, clique no botão **🎯 Gravar Seleção de Produtos** abaixo.")

                                col_m1, col_m2 = st.columns(2)
                                metade_m = (len(match_df) + 1) // 2
                                chk_map = {}

                                for idx_m, (_, row_m) in enumerate(match_df.iterrows()):
                                    target_col = col_m1 if idx_m < metade_m else col_m2
                                    p_cod = int(row_m['CODIGO_PRODUTO'])
                                    desc = str(row_m['DESCRICAO_PRODUTO']).strip()
                                    forn = str(row_m.get('FORNECEDOR') or row_m.get('COMPRADOR') or 'GERAL').strip()
                                    fam_cod = row_m.get('CODIGO_FAMILIA')
                                    fam_desc = str(row_m.get('DESCRICAO_FAMILIA') or '').strip() if pd.notna(row_m.get('DESCRICAO_FAMILIA')) else ''
                                    
                                    fam_tag = f"[👨‍👩‍👧‍👦 Família {int(fam_cod)} - {fam_desc}]" if pd.notna(fam_cod) and int(fam_cod) > 0 else "[Sem Família]"
                                    
                                    with target_col:
                                        with st.container(border=True):
                                            chk_val = st.checkbox(
                                                f"**{p_cod}** — {desc}",
                                                key=f"chk_item_mix_{camp_id}_{p_cod}",
                                                help=f"{fam_tag} | Fornecedor: {forn}"
                                            )
                                            st.caption(f"🏷️ {fam_tag} | 🏢 {forn}")
                                            chk_map[p_cod] = chk_val

                                btn_gravar_selecao = st.form_submit_button(
                                    "🎯 Gravar Seleção de Produtos para Configurar Exposição",
                                    type="primary",
                                    use_container_width=True
                                )

                                if btn_gravar_selecao:
                                    skus_marcados = [pc for pc, is_chk in chk_map.items() if is_chk]
                                    if not skus_marcados:
                                        st.warning("⚠️ Marque ao menos um produto no box de seleção acima.")
                                    else:
                                        st.session_state[session_mix_key] = skus_marcados
                                        st.success(f"✅ {len(skus_marcados)} produto(s) selecionado(s) com sucesso!")
                                        st.rerun()

                        else:
                            st.warning(f"Nenhum produto localizado com o termo '{termo_busca}'. Tente buscar por palavras parciais ou outro código.")
                    except Exception as e:
                        st.error(f"Erro ao buscar no catálogo: {e}")

            # Painel de Configuração dos SKUs Selecionados
            skus_selecionados_atual = st.session_state.get(session_mix_key, [])
            if skus_selecionados_atual:
                prods_data_list = []
                for sc in skus_selecionados_atual:
                    p_info = carregar_dados_produto_consolidado(
                        engine=engine,
                        produto_codigo=sc,
                        data_inicio=camp["data_inicio"],
                        data_fim=camp["data_fim"],
                        base_data_path=base_data_path
                    )
                    prods_data_list.append(p_info)

                st.markdown("---")
                col_mix_head, col_mix_btn = st.columns([3, 1])
                col_mix_head.markdown(f"#### 📦 Exposição Conjunta: {len(prods_data_list)} SKUs Selecionados")
                with col_mix_btn:
                    if st.button("🗑️ Limpar Seleção", key=f"btn_limpar_sel_{camp_id}", type="secondary"):
                        del st.session_state[session_mix_key]
                        st.rerun()

                st.info(f"💡 **Rateio Proporcional no Supply:** Estes **{len(prods_data_list)} SKUs** compartilharão a mesma estrutura física de exposição (Ponta / Ilha). O Supply dividirá a capacidade do móvel proporcionalmente entre eles ({len(prods_data_list)} sabores).")

                # Lista resumida dos SKUs selecionados
                with st.container(border=True):
                    st.markdown("##### 🛒 Produtos do Mix Selecionado:")
                    for p in prods_data_list:
                        fam_str = f" [👨‍👩‍👧‍👦 Família {p['codigo_familia']} - {p.get('descricao_familia', '')}]" if p.get('codigo_familia') else ""
                        st.write(f"• `{p['produto_codigo']}` — **{p['descricao']}**{fam_str} | Estoque CD15: **{p['estoque_cd15']:,.0f} un** | Venda Proj: **{p['venda_projetada_total']:.0f} un**")

                    # Métricas agregadas do grupo
                    c_m1, c_m2, c_m3, c_m4 = st.columns(4)
                    c_m1.metric("Total SKUs no Móvel", f"{len(prods_data_list)} SKUs")
                    c_m2.metric("Estoque Total Lojas", f"{sum(p['estoque_total_lojas'] for p in prods_data_list):,.0f} un")
                    c_m3.metric("Estoque Disponível CD15", f"{sum(p['estoque_cd15'] for p in prods_data_list):,.0f} un")
                    c_m4.metric(f"Venda Projetada ({dias_camp} dias)", f"{sum(p['venda_projetada_total'] for p in prods_data_list):,.0f} un")

                # Grid de Lojas para Compras com Padrão INATIVA
                st.markdown("##### 🏪 Definição de Exposição e Participação por Loja (Aplicada ao Grupo)")
                st.info("💡 **Regra de Participação:** Selecione o tipo de exposição apenas nas lojas que participarão da campanha. A configuração será aplicada a todos os SKUs do grupo.")

                lojas_inputs = []
                col_g1, col_g2 = st.columns(2)
                metade = len(LISTA_14_LOJAS) // 2

                for i, lj_cod in enumerate(LISTA_14_LOJAS):
                    container_col = col_g1 if i < metade else col_g2
                    with container_col:
                        with st.container(border=True):
                            lj_nome = lojas_map.get(lj_cod, f"Loja {lj_cod}")
                            
                            # Soma dados dos produtos para esta loja
                            st_lj_total = sum(p["lojas_detalhe"].get(lj_cod, {}).get("estoque_loja", 0.0) for p in prods_data_list)
                            vp_lj_total = sum(p["lojas_detalhe"].get(lj_cod, {}).get("venda_projetada", 0.0) for p in prods_data_list)
                            
                            st.write(f"🏬 **{lj_cod} - {lj_nome}**")
                            st.caption(f"Estoque Total Mix: **{st_lj_total:,.0f} un** | Venda Proj Mix: **{vp_lj_total:.0f} un**")
                            
                            tipo_sel = st.selectbox(
                                "Tipo de Exposição:",
                                options=lista_nomes_tipos,
                                index=idx_inativa_padrao,
                                key=f"tipo_exp_mix_{camp_id}_{lj_cod}"
                            )
                            
                            is_inativa = tipo_sel.upper() == "INATIVA"
                            if is_inativa:
                                st.caption("⏸️ *Loja não participará do ponto extra / Não enviada ao Supply.*")
                                vol_sug = 0
                            else:
                                st.success(f"✅ **Ativa:** Exposição em `{tipo_sel}`")
                                vol_sug = st.number_input(
                                    "Sugestão Comprador (Unidades por SKU):",
                                    min_value=0,
                                    value=0,
                                    step=1,
                                    key=f"vol_sug_mix_{camp_id}_{lj_cod}",
                                    help="Volume sugerido por SKU. Deixe 0 para cálculo automático de capacidade física pelo Supply."
                                )

                            lojas_inputs.append({
                                "loja_codigo": lj_cod,
                                "tipo_exposicao_id": mapa_tipos_exp[tipo_sel],
                                "volume_comprador": vol_sug,
                                "estrutura_exposicao_id": None
                            })

                # Botão de Salvamento Final
                st.markdown("---")
                if st.button(f"💾 Salvar {len(prods_data_list)} Produtos e Matriz de Lojas na Campanha", type="primary", key=f"btn_salvar_mix_lote_{camp_id}", use_container_width=True):
                    suc, msg = salvar_familia_campanha_compras(
                        engine=engine,
                        campanha_id=camp_id,
                        produtos_familia=[{"produto_codigo": p["produto_codigo"], "descricao": p["descricao"]} for p in prods_data_list],
                        dados_lojas=lojas_inputs,
                        usuario=usuario_atual,
                        data_inicio=camp["data_inicio"],
                        data_fim=camp["data_fim"],
                        base_data_path=base_data_path
                    )
                    if suc:
                        if session_mix_key in st.session_state:
                            del st.session_state[session_mix_key]
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
                
                fam_tag = ""
                if it.get("codigo_familia"):
                    tot_fam = int(it.get("total_skus_familia") or 1)
                    fam_tag = f" | 👨‍👩‍👧‍👦 Família {it.get('codigo_familia')} ({tot_fam} SKUs)"
                
                exp_label = f"📦 [{it.get('fornecedor') or 'GERAL'}] `{it['produto_codigo']}` — {it['descricao_snapshot']}{fam_tag} | {status_lojas_tag}"
                
                with st.expander(exp_label):
                    c_act1, c_act2, c_act3 = st.columns([3, 1.4, 1.1])
                    c_act1.write(f"🏢 **Fornecedor:** {it.get('fornecedor') or 'GERAL'} | 🏷️ **Depto:** {it.get('departamento') or 'N/D'} | 📦 **Emb Compra:** {it['embalagem_compra']} un | 🚚 **Emb Transf:** {it['embalagem_transferencia']} un")
                    if it.get("codigo_familia"):
                        c_act1.caption(f"👨‍👩‍👧‍👦 **Família de Exposição:** `{it['codigo_familia']}` — {it.get('descricao_familia', 'N/D')} (Rateio em {it.get('total_skus_familia', 1)} SKUs)")
                    
                    with c_act2:
                        with st.popover("✏️ Alterar Exposição", help="Altera os tipos de exposição das lojas (ex: Ponta para Ilha)"):
                            st.markdown(f"#### 🏷️ Alterar Exposição — SKU `{it['produto_codigo']}`")
                            st.caption("Altere a estrutura de exposição por loja (ex: Ponta de Gôndola para Ilha).")
                            
                            chk_prop_fam = False
                            if it.get("codigo_familia"):
                                chk_prop_fam = st.checkbox(
                                    f"👨‍👩‍👧‍👦 Aplicar para TODOS os {it.get('total_skus_familia', 1)} SKUs da Família {it['codigo_familia']}",
                                    value=True,
                                    key=f"prop_fam_chk_{it['item_id']}"
                                )

                            novas_lojas_edit = []
                            for l_orig in it["lojas"]:
                                lj_c = l_orig["loja_codigo"]
                                lj_n = l_orig["loja_nome"]
                                tipo_nome_orig = str(l_orig.get("tipo_exposicao_nome") or "INATIVA").strip().upper()
                                idx_tipo_orig = lista_nomes_tipos.index(tipo_nome_orig) if tipo_nome_orig in lista_nomes_tipos else idx_inativa_padrao
                                
                                c_e1, c_e2 = st.columns([2, 1])
                                sel_t = c_e1.selectbox(
                                    f"Loja {lj_c}:",
                                    options=lista_nomes_tipos,
                                    index=idx_tipo_orig,
                                    key=f"edit_exp_{it['item_id']}_{lj_c}"
                                )
                                sug_v = c_e2.number_input(
                                    f"Sug. Compras (un):",
                                    min_value=0,
                                    value=int(l_orig.get("volume_comprador") or 0),
                                    step=1,
                                    key=f"edit_sug_{it['item_id']}_{lj_c}"
                                )
                                novas_lojas_edit.append({
                                    "loja_codigo": lj_c,
                                    "tipo_exposicao_id": mapa_tipos_exp[sel_t],
                                    "volume_comprador": sug_v
                                })

                            if st.button("💾 Salvar Alterações de Exposição", type="primary", key=f"btn_save_exp_{it['item_id']}"):
                                suc_ae, msg_ae = alterar_exposicao_lojas_item(
                                    engine=engine,
                                    campanha_id=camp_id,
                                    item_id=it["item_id"],
                                    dados_lojas=novas_lojas_edit,
                                    usuario=usuario_atual,
                                    propagar_familia=chk_prop_fam
                                )
                                if suc_ae:
                                    st.success(msg_ae)
                                    st.rerun()
                                else:
                                    st.error(msg_ae)

                    with c_act3:
                        if st.button("🗑️ Remover", key=f"del_prod_{it['produto_codigo']}", type="secondary"):
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

