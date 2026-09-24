"""
page/campanhas_loja.py
Página B - Loja: Consulta Operacional de Campanhas com Filtros Avançados para Lojas, Compras, Supply e Admin.
"""

from __future__ import annotations
import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
from sqlalchemy import text
from services.campanha_service import expirar_campanhas_vencidas, LISTA_14_LOJAS
from services.exportacao_campanha import (
    gerar_excel_consulta_loja,
    gerar_pdf_campanha_loja
)


def _obter_todas_lojas_db(engine) -> List[Dict[str, str]]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT codigo, nome FROM lojas WHERE tipo = 'LOJA' AND ativa = TRUE ORDER BY codigo")).fetchall()
        return [{"codigo": str(r[0]).zfill(3), "nome": str(r[1])} for r in rows]


def show_campanhas_loja_page(engine, base_data_path: str = "data"):
    st.title("🏪 Visão de Campanhas e Exposições de Loja")
    st.caption("Consulta dos planos de abastecimento e exposição para lojas, com filtros e exportação de PDF/Excel.")

    # 1. Identificar permissões do usuário
    role_str = str(st.session_state.get("role", "")).strip().lower()
    cargo_str = str(st.session_state.get("cargo", "")).strip().lower()
    lojas_usuario: List[str] = st.session_state.get("lojas_acesso", [])

    is_admin = role_str == "admin"
    is_compras = is_admin or "compras" in cargo_str or "comprador" in cargo_str
    is_supply = is_admin or "supply" in cargo_str or "abastecimento" in cargo_str
    has_global_access = is_admin or is_compras or is_supply

    todas_lojas = _obter_todas_lojas_db(engine)
    mapa_lojas = {l["codigo"]: l["nome"] for l in todas_lojas}

    # Se o usuário não tiver acesso global e nem lojas no perfil
    if not has_global_access and not lojas_usuario:
        st.warning("Seu usuário não possui nenhuma loja vinculada. Solicite acesso ao administrador.")
        return

    # Atualiza expiração automática
    expirar_campanhas_vencidas(engine)

    # -------------------------------------------------------------------------
    # PAINEL DE FILTROS SUPERIORES
    # -------------------------------------------------------------------------
    st.markdown("### 🔍 Filtros de Consulta")
    with st.container(border=True):
        col_f1, col_f2, col_f3 = st.columns([2, 2, 2])

        # 1. Filtro de Loja
        with col_f1:
            if has_global_access:
                opcoes_lojas = ["TODAS"] + [l["codigo"] for l in todas_lojas]
                sel_loja = st.selectbox(
                    "Filtrar por Loja:",
                    opcoes_lojas,
                    format_func=lambda x: "🌟 Todas as Lojas (Consolidado)" if x == "TODAS" else f"{x} - {mapa_lojas.get(x, 'Loja ' + x)}",
                    key="filtro_loja_page"
                )
            else:
                opcoes_lojas = [str(lj).zfill(3) for lj in lojas_usuario]
                if len(opcoes_lojas) > 1:
                    opcoes_lojas = ["TODAS"] + opcoes_lojas
                    sel_loja = st.selectbox(
                        "Selecione a Loja:",
                        opcoes_lojas,
                        format_func=lambda x: "🌟 Minhas Lojas (Todas)" if x == "TODAS" else f"{x} - {mapa_lojas.get(x, 'Loja ' + x)}",
                        key="filtro_loja_page"
                    )
                else:
                    sel_loja = opcoes_lojas[0]
                    st.info(f"🏬 Unidade: **{sel_loja} - {mapa_lojas.get(sel_loja, 'Loja')}**")

        # 2. Filtro de Período / Datas
        with col_f2:
            hoje = date.today()
            col_d1, col_d2 = st.columns(2)
            d_ini_filtro = col_d1.date_input("Vigência De:", value=hoje - timedelta(days=15), key="d_ini_loja_filtro")
            d_fim_filtro = col_d2.date_input("Até:", value=hoje + timedelta(days=45), key="d_fim_loja_filtro")

        # 3. Filtro de Status
        with col_f3:
            status_opcoes = ["FINALIZADA", "ATIVA", "EM_AVALIACAO_SUPPLY", "ENVIADA_SUPPLY", "PENDENCIA_COMPRAS", "INATIVA"]
            status_default = ["FINALIZADA", "ATIVA", "EM_AVALIACAO_SUPPLY"] if not has_global_access else ["FINALIZADA", "ATIVA", "EM_AVALIACAO_SUPPLY", "PENDENCIA_COMPRAS"]
            status_selecionados = st.multiselect(
                "Status da Campanha:",
                status_opcoes,
                default=status_default,
                key="filtro_status_loja"
            )

    # -------------------------------------------------------------------------
    # CONSULTA NO BANCO DE DADOS
    # -------------------------------------------------------------------------
    where_clauses = ["c.data_fim >= :d_ini", "c.data_inicio <= :d_fim"]
    params: Dict[str, Any] = {
        "d_ini": d_ini_filtro,
        "d_fim": d_fim_filtro
    }

    if status_selecionados:
        where_clauses.append("c.status = ANY(:status_list)")
        params["status_list"] = status_selecionados

    # Filtro de loja no SQL
    if sel_loja != "TODAS":
        where_clauses.append("cl.loja_codigo = :loja_filtro")
        params["loja_filtro"] = str(sel_loja).zfill(3)
    elif not has_global_access:
        # Usuário restrito selecionou "Minhas Lojas (Todas)"
        where_clauses.append("cl.loja_codigo = ANY(:lojas_permitidas)")
        params["lojas_permitidas"] = [str(lj).zfill(3) for lj in lojas_usuario]

    where_sql = " AND ".join(where_clauses)

    sql_campanhas = text(f"""
        SELECT DISTINCT
            c.id as campanha_id,
            c.codigo_campanha,
            c.nome as campanha_nome,
            c.data_inicio,
            c.data_fim,
            c.status,
            c.observacoes,
            COUNT(DISTINCT CASE WHEN UPPER(COALESCE(te.nome, '')) != 'INATIVA' THEN ci.produto_codigo END) as total_skus,
            SUM(CASE WHEN UPPER(COALESCE(te.nome, '')) != 'INATIVA' THEN COALESCE(cl.volume_final_supply, 0) ELSE 0 END) as total_unidades,
            SUM(CASE WHEN UPPER(COALESCE(te.nome, '')) != 'INATIVA' THEN COALESCE(cl.caixas_transferencia, 0) ELSE 0 END) as total_caixas
        FROM campanhas c
        JOIN campanha_itens ci ON ci.campanha_id = c.id
        JOIN campanha_lojas cl ON cl.campanha_item_id = ci.id
        LEFT JOIN tipos_exposicao te ON te.id = cl.tipo_exposicao_id
        WHERE {where_sql}
        GROUP BY c.id, c.codigo_campanha, c.nome, c.data_inicio, c.data_fim, c.status, c.observacoes
        ORDER BY c.data_inicio ASC, c.nome ASC
    """)

    with engine.connect() as conn:
        campanhas_encontradas = conn.execute(sql_campanhas, params).fetchall()

    if not campanhas_encontradas:
        st.info("Nenhuma campanha localizada para os filtros selecionados.")
        return

    # Filtro de Campanha Específica (Opcional)
    st.markdown("---")
    col_c_sel, col_c_info = st.columns([3, 1])
    
    camp_dict_map = {f"Todas ({len(campanhas_encontradas)} campanhas)": "TODAS"}
    for c in campanhas_encontradas:
        camp_dict_map[f"{c.codigo_campanha} — {c.campanha_nome} [{c.status}]"] = c.campanha_id

    sel_camp_escolhida = col_c_sel.selectbox("Filtrar por Campanha Específica:", list(camp_dict_map.keys()), key="filtro_camp_especifica")
    camp_escolhida_id = camp_dict_map[sel_camp_escolhida]

    # Lista final de campanhas a exibir
    campanhas_exibir = (
        campanhas_encontradas if camp_escolhida_id == "TODAS"
        else [c for c in campanhas_encontradas if c.campanha_id == camp_escolhida_id]
    )

    # Métricas Gerais
    total_cx_todas = sum(c.total_caixas or 0 for c in campanhas_exibir)
    total_un_todas = sum(c.total_unidades or 0 for c in campanhas_exibir)
    total_skus_todas = sum(c.total_skus or 0 for c in campanhas_exibir)

    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    col_m1.metric("Campanhas Filtradas", len(campanhas_exibir))
    col_m2.metric("Total de Produtos (SKUs)", total_skus_todas)
    col_m3.metric("Volume Aprovado (Un)", f"{total_un_todas:,.0f} un")
    col_m4.metric("Total de Caixas a Receber", f"{total_cx_todas:,.0f} cx")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # EXIBIÇÃO DETALHADA DAS CAMPANHAS E DOWNLOADS
    # -------------------------------------------------------------------------
    for camp in campanhas_exibir:
        cid = camp.campanha_id
        d_ini_fmt = camp.data_inicio.strftime("%d/%m/%Y")
        d_fim_fmt = camp.data_fim.strftime("%d/%m/%Y")
        dias_camp = (camp.data_fim - camp.data_inicio).days + 1

        with st.container(border=True):
            st.markdown(f"### 🏷️ `{camp.codigo_campanha}` — **{camp.campanha_nome}** `[{camp.status}]`")
            st.write(f"📅 **Vigência:** {d_ini_fmt} até {d_fim_fmt} ({dias_camp} dias) | **Mix:** {camp.total_skus} produtos | **Total Caixas:** {camp.total_caixas} cx")
            if camp.observacoes:
                st.caption(f"📌 Observações: {camp.observacoes}")

            # -----------------------------------------------------------------
            # CENÁRIO 1: UMA LOJA ESPECÍFICA SELECIONADA
            # -----------------------------------------------------------------
            if sel_loja != "TODAS":
                lj = str(sel_loja).zfill(3)
                loja_label = f"{lj} - {mapa_lojas.get(lj, 'Loja ' + lj)}"

                sql_itens_loja = text("""
                    SELECT 
                        ci.produto_codigo AS "Código",
                        ci.descricao_snapshot AS "Descrição do Produto",
                        COALESCE(te.nome, 'Geral') AS "Tipo de Exposição",
                        cl.volume_final_supply AS "Volume Aprovado (Un)",
                        cl.caixas_transferencia AS "Caixas a Receber"
                    FROM campanha_lojas cl
                    JOIN campanha_itens ci ON ci.id = cl.campanha_item_id
                    LEFT JOIN tipos_exposicao te ON te.id = cl.tipo_exposicao_id
                    WHERE ci.campanha_id = :cid 
                      AND cl.loja_codigo = :loja
                      AND UPPER(COALESCE(te.nome, '')) != 'INATIVA'
                    ORDER BY ci.produto_codigo
                """)

                with engine.connect() as conn:
                    itens_df = pd.read_sql(sql_itens_loja, conn, params={"cid": cid, "loja": lj})

                if itens_df.empty:
                    st.info(f"Nenhum produto programado para a {loja_label} nesta campanha.")
                else:
                    st.dataframe(itens_df, use_container_width=True, hide_index=True)

                    # Botões de exportação da loja
                    c_exp1, c_exp2 = st.columns(2)
                    with c_exp1:
                        pdf_bytes = gerar_pdf_campanha_loja(engine, cid, lj)
                        st.download_button(
                            label=f"📄 Baixar PDF de Conferência (Loja {lj})",
                            data=pdf_bytes,
                            file_name=f"campanha_{camp.codigo_campanha}_loja_{lj}.pdf",
                            mime="application/pdf",
                            key=f"btn_pdf_{cid}_{lj}",
                            use_container_width=True
                        )
                    with c_exp2:
                        excel_bytes = gerar_excel_consulta_loja(engine, cid, lj)
                        st.download_button(
                            label=f"📊 Baixar Planilha Excel (Loja {lj})",
                            data=excel_bytes,
                            file_name=f"campanha_{camp.codigo_campanha}_loja_{lj}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"btn_xlsx_{cid}_{lj}",
                            use_container_width=True
                        )

            # -----------------------------------------------------------------
            # CENÁRIO 2: TODAS AS LOJAS (CONSOLIDADO)
            # -----------------------------------------------------------------
            else:
                where_lojas_cons = ""
                params_cons: Dict[str, Any] = {"cid": cid}
                if not has_global_access:
                    where_lojas_cons = "AND cl.loja_codigo = ANY(:lojas_permitidas)"
                    params_cons["lojas_permitidas"] = [str(lj).zfill(3) for lj in lojas_usuario]

                sql_itens_todas = text(f"""
                    SELECT 
                        cl.loja_codigo AS "Loja",
                        l.nome AS "Nome da Loja",
                        ci.produto_codigo AS "Código",
                        ci.descricao_snapshot AS "Descrição do Produto",
                        COALESCE(te.nome, 'Geral') AS "Tipo de Exposição",
                        cl.volume_final_supply AS "Volume Aprovado (Un)",
                        cl.caixas_transferencia AS "Caixas a Receber"
                    FROM campanha_lojas cl
                    JOIN campanha_itens ci ON ci.id = cl.campanha_item_id
                    JOIN lojas l ON l.codigo = cl.loja_codigo
                    LEFT JOIN tipos_exposicao te ON te.id = cl.tipo_exposicao_id
                    WHERE ci.campanha_id = :cid 
                      AND UPPER(COALESCE(te.nome, '')) != 'INATIVA' {where_lojas_cons}
                    ORDER BY cl.loja_codigo, ci.produto_codigo
                """)

                with engine.connect() as conn:
                    itens_consolidados_df = pd.read_sql(sql_itens_todas, conn, params=params_cons)

                if itens_consolidados_df.empty:
                    st.info("Nenhum item alocado para as lojas selecionadas.")
                else:
                    st.dataframe(itens_consolidados_df, use_container_width=True, hide_index=True)

                    col_exp_cons1, col_exp_cons2 = st.columns([2, 2])
                    with col_exp_cons1:
                        excel_cons_bytes = gerar_excel_consulta_loja(engine, cid, None)
                        st.download_button(
                            label=f"📊 Baixar Excel Consolidado de Todas as Lojas (.xlsx)",
                            data=excel_cons_bytes,
                            file_name=f"campanha_{camp.codigo_campanha}_todas_lojas.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"btn_xlsx_cons_{cid}",
                            use_container_width=True
                        )

                    with col_exp_cons2:
                        with st.popover(f"📄 Baixar PDF Individual por Loja"):
                            loja_pdf_sel = st.selectbox(
                                "Selecione a Loja para Gerar o PDF:",
                                [l["codigo"] for l in todas_lojas],
                                format_func=lambda x: f"{x} - {mapa_lojas.get(x, 'Loja ' + x)}",
                                key=f"sel_pdf_lj_{cid}"
                            )
                            if loja_pdf_sel:
                                pdf_ind_bytes = gerar_pdf_campanha_loja(engine, cid, loja_pdf_sel)
                                st.download_button(
                                    label=f"📥 Download PDF da Loja {loja_pdf_sel}",
                                    data=pdf_ind_bytes,
                                    file_name=f"campanha_{camp.codigo_campanha}_loja_{loja_pdf_sel}.pdf",
                                    mime="application/pdf",
                                    key=f"btn_pdf_ind_{cid}_{loja_pdf_sel}",
                                    use_container_width=True
                                )
