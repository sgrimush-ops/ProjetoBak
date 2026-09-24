"""
page/campanhas_admin_exposicao.py
Página Administrativa: Cadastro de Tipos de Exposição, Estruturas Físicas, Medidas de Bandejas e Auditoria.
"""

from __future__ import annotations
import streamlit as st
import pandas as pd
from sqlalchemy import text
from typing import List, Dict, Any


def show_campanhas_admin_page(engine, base_data_path: str = "data"):
    st.title("⚙️ Administração de Exposições e Auditoria")
    st.caption("Parametrização dos modelos físicos de gôndolas, bandejas e visualização do histórico de auditoria.")

    tab_tipos, tab_estruturas, tab_bandejas, tab_auditoria = st.tabs([
        "🏷️ Tipos de Exposição",
        "🏗️ Estruturas Físicas",
        "📐 Dimensões de Bandejas",
        "📜 Histórico de Auditoria"
    ])

    # =========================================================================
    # ABA 1: TIPOS DE EXPOSIÇÃO
    # =========================================================================
    with tab_tipos:
        st.subheader("Tipos de Exposição Cadastrados")

        with engine.connect() as conn:
            tipos_df = pd.read_sql(text("SELECT id, nome, descricao, ativo FROM tipos_exposicao ORDER BY id"), conn)

        st.dataframe(tipos_df, use_container_width=True, hide_index=True)

        with st.expander("➕ Adicionar Novo Tipo de Exposição"):
            with st.form("form_novo_tipo_exp"):
                novo_nome = st.text_input("Nome do Tipo (ex: CHECKOUT, ILHA REFRIGERADA):").strip().upper()
                nova_desc = st.text_area("Descrição:")
                if st.form_submit_button("Salvar Tipo"):
                    if novo_nome:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO tipos_exposicao (nome, descricao, ativo)
                                    VALUES (:nome, :desc, TRUE)
                                    ON CONFLICT (nome) DO NOTHING;
                                """), {"nome": novo_nome, "desc": nova_desc})
                            st.success(f"Tipo '{novo_nome}' salvo com sucesso!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao salvar: {e}")
                    else:
                        st.warning("Informe o nome do tipo.")

    # =========================================================================
    # ABA 2: ESTRUTURAS FÍSICAS
    # =========================================================================
    with tab_estruturas:
        st.subheader("Estruturas Físicas de Exposição")

        with engine.connect() as conn:
            estruturas_df = pd.read_sql(text("""
                SELECT 
                    e.id, 
                    t.nome AS "Tipo de Exposição", 
                    e.nome AS "Nome da Estrutura", 
                    e.descricao AS "Descrição", 
                    e.ativo AS "Ativo",
                    (SELECT COUNT(*) FROM bandejas_exposicao b WHERE b.estrutura_exposicao_id = e.id AND b.ativa = TRUE) AS "Qtd Bandejas"
                FROM estruturas_exposicao e
                JOIN tipos_exposicao t ON t.id = e.tipo_exposicao_id
                ORDER BY e.id
            """), conn)

        st.dataframe(estruturas_df, use_container_width=True, hide_index=True)

        with st.expander("➕ Cadastrar Nova Estrutura"):
            with engine.connect() as conn:
                tipos_opts = conn.execute(text("SELECT id, nome FROM tipos_exposicao WHERE ativo = TRUE")).fetchall()
            tipos_dict = {r[1]: r[0] for r in tipos_opts}

            with st.form("form_nova_estrutura"):
                tipo_sel = st.selectbox("Tipo de Exposição Pai:", list(tipos_dict.keys()))
                nome_est = st.text_input("Nome da Estrutura (ex: Ponta Especial 5 Níveis):")
                desc_est = st.text_area("Descrição:")
                num_bandejas_iniciais = st.number_input("Criar automaticamente quantas bandejas padrão (150x50x25cm)?", min_value=1, max_value=12, value=6)

                if st.form_submit_button("Salvar Estrutura"):
                    if nome_est:
                        try:
                            with engine.begin() as conn:
                                eid = conn.execute(text("""
                                    INSERT INTO estruturas_exposicao (tipo_exposicao_id, nome, descricao, ativo)
                                    VALUES (:tid, :nome, :desc, TRUE)
                                    RETURNING id;
                                """), {"tid": tipos_dict[tipo_sel], "nome": nome_est, "desc": desc_est}).scalar()

                                for n in range(1, num_bandejas_iniciais + 1):
                                    conn.execute(text("""
                                        INSERT INTO bandejas_exposicao (estrutura_exposicao_id, numero_bandeja, largura_cm, profundidade_cm, altura_cm, ordem, ativa)
                                        VALUES (:eid, :num, 150.0, 50.0, 25.0, :num, TRUE)
                                    """), {"eid": eid, "num": n})

                            st.success(f"Estrutura '{nome_est}' criada com {num_bandejas_iniciais} bandejas!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao salvar: {e}")
                    else:
                        st.warning("Informe o nome da estrutura.")

    # =========================================================================
    # ABA 3: MEDIDAS DE BANDEJAS
    # =========================================================================
    with tab_bandejas:
        st.subheader("Bandejas e Dimensões por Estrutura")

        with engine.connect() as conn:
            est_rows = conn.execute(text("SELECT id, nome FROM estruturas_exposicao WHERE ativo = TRUE ORDER BY nome")).fetchall()
        est_map = {r[1]: r[0] for r in est_rows}

        if est_map:
            sel_est_bandeja = st.selectbox("Selecione a Estrutura para Gerenciar Bandejas:", list(est_map.keys()))
            eid_sel = est_map[sel_est_bandeja]

            with engine.connect() as conn:
                bandejas_df = pd.read_sql(text("""
                    SELECT id, numero_bandeja, largura_cm, profundidade_cm, altura_cm, ordem, ativa
                    FROM bandejas_exposicao
                    WHERE estrutura_exposicao_id = :eid
                    ORDER BY ordem, numero_bandeja
                """), conn, params={"eid": eid_sel})

            st.dataframe(bandejas_df, use_container_width=True, hide_index=True)

            with st.expander("➕ Adicionar Bandeja nesta Estrutura"):
                with st.form("form_nova_bandeja"):
                    col_b1, col_b2, col_b3, col_b4 = st.columns(4)
                    num_b = col_b1.number_input("Número da Bandeja:", min_value=1, value=len(bandejas_df) + 1)
                    larg_b = col_b2.number_input("Largura (cm):", min_value=1.0, value=150.0)
                    prof_b = col_b3.number_input("Profundidade (cm):", min_value=1.0, value=50.0)
                    alt_b = col_b4.number_input("Altura Útil (cm):", min_value=1.0, value=25.0)

                    if st.form_submit_button("Adicionar Bandeja"):
                        with engine.begin() as conn:
                            conn.execute(text("""
                                INSERT INTO bandejas_exposicao (estrutura_exposicao_id, numero_bandeja, largura_cm, profundidade_cm, altura_cm, ordem, ativa)
                                VALUES (:eid, :num, :larg, :prof, :alt, :num, TRUE)
                            """), {"eid": eid_sel, "num": num_b, "larg": larg_b, "prof": prof_b, "alt": alt_b})
                        st.success("Bandeja adicionada com sucesso!")
                        st.rerun()

    # =========================================================================
    # ABA 4: HISTÓRICO DE AUDITORIA
    # =========================================================================
    with tab_auditoria:
        st.subheader("Histórico Completo de Auditoria de Campanhas")
        st.caption("Rastreamento de todas as alterações, envios, replicações e finalizações.")

        with engine.connect() as conn:
            hist_df = pd.read_sql(text("""
                SELECT 
                    h.id,
                    c.codigo_campanha AS "Campanha",
                    h.usuario AS "Usuário",
                    h.data_hora AS "Data / Hora",
                    h.acao AS "Ação",
                    h.campo AS "Campo",
                    h.valor_anterior AS "Antes",
                    h.valor_novo AS "Depois",
                    h.detalhes AS "Detalhes"
                FROM campanha_historico h
                JOIN campanhas c ON c.id = h.campanha_id
                ORDER BY h.data_hora DESC
                LIMIT 100
            """), conn)

        if hist_df.empty:
            st.info("Nenhum evento registrado no histórico ainda.")
        else:
            st.dataframe(hist_df, use_container_width=True, hide_index=True)
