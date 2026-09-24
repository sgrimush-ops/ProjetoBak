"""
page/campanhas_admin_exposicao.py
Página Administrativa: Cadastro e Edição de Tipos de Exposição, Estruturas Físicas, Medidas de Bandejas e Auditoria.
"""

from __future__ import annotations
import os
import streamlit as st
import pandas as pd
from sqlalchemy import text
from typing import List, Dict, Any
from utils.timezone import now_brazil
from services.campanha_service import salvar_dimensoes_produto


def show_campanhas_admin_page(engine, base_data_path: str = "data"):
    st.title("⚙️ Administração de Exposições e Auditoria")
    st.caption("Parametrização dos modelos físicos de gôndolas, bandejas, medidas de produtos e histórico de auditoria.")

    usuario_atual = st.session_state.get("username", "admin")

    tab_tipos, tab_estruturas, tab_bandejas, tab_produtos_dim, tab_auditoria = st.tabs([
        "🏷️ Tipos de Exposição",
        "🏗️ Estruturas Físicas",
        "📐 Dimensões de Bandejas",
        "📦 Dimensões de Produtos",
        "📜 Histórico de Auditoria"
    ])

    # =========================================================================
    # ABA 1: TIPOS DE EXPOSIÇÃO
    # =========================================================================
    with tab_tipos:
        st.subheader("Tipos de Exposição")
        st.caption("Edite os nomes, descrições ou status diretamente na tabela e clique em Salvar.")

        with engine.connect() as conn:
            tipos_df = pd.read_sql(text("SELECT id, nome, descricao, ativo FROM tipos_exposicao ORDER BY id"), conn)

        col_config_tipos = {
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "nome": st.column_config.TextColumn("Nome do Tipo", required=True),
            "descricao": st.column_config.TextColumn("Descrição"),
            "ativo": st.column_config.CheckboxColumn("Ativo?", default=True)
        }

        edited_tipos = st.data_editor(
            tipos_df,
            column_config=col_config_tipos,
            use_container_width=True,
            hide_index=True,
            key="editor_tipos_exp"
        )

        if st.button("💾 Salvar Alterações nos Tipos de Exposição", type="primary", key="btn_salvar_tipos"):
            try:
                with engine.begin() as conn:
                    for _, r in edited_tipos.iterrows():
                        conn.execute(text("""
                            UPDATE tipos_exposicao
                            SET nome = :nome,
                                descricao = :desc,
                                ativo = :ativo
                            WHERE id = :id
                        """), {
                            "nome": str(r["nome"]).strip().upper(),
                            "desc": str(r.get("descricao") or ""),
                            "ativo": bool(r["ativo"]),
                            "id": int(r["id"])
                        })
                st.success("Tipos de exposição atualizados com sucesso!")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar alterações: {e}")

        st.markdown("---")
        with st.expander("➕ Adicionar Novo Tipo de Exposição"):
            with st.form("form_novo_tipo_exp"):
                novo_nome = st.text_input("Nome do Tipo (ex: CHECKOUT, ILHA REFRIGERADA):").strip().upper()
                nova_desc = st.text_area("Descrição:")
                if st.form_submit_button("Criar Tipo"):
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
        st.caption("Edite os nomes e descrições dos modelos físicos diretamente na tabela.")

        with engine.connect() as conn:
            estruturas_df = pd.read_sql(text("""
                SELECT 
                    e.id, 
                    t.nome AS "tipo_nome", 
                    e.nome, 
                    e.descricao, 
                    e.ativo,
                    (SELECT COUNT(*) FROM bandejas_exposicao b WHERE b.estrutura_exposicao_id = e.id AND b.ativa = TRUE) AS "qtd_bandejas"
                FROM estruturas_exposicao e
                JOIN tipos_exposicao t ON t.id = e.tipo_exposicao_id
                ORDER BY e.id
            """), conn)

        col_config_est = {
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "tipo_nome": st.column_config.TextColumn("Tipo de Exposição", disabled=True),
            "nome": st.column_config.TextColumn("Nome da Estrutura", required=True),
            "descricao": st.column_config.TextColumn("Descrição"),
            "ativo": st.column_config.CheckboxColumn("Ativo?", default=True),
            "qtd_bandejas": st.column_config.NumberColumn("Qtd Bandejas", disabled=True)
        }

        edited_est = st.data_editor(
            estruturas_df,
            column_config=col_config_est,
            use_container_width=True,
            hide_index=True,
            key="editor_estruturas"
        )

        if st.button("💾 Salvar Alterações nas Estruturas", type="primary", key="btn_salvar_est"):
            try:
                with engine.begin() as conn:
                    for _, r in edited_est.iterrows():
                        conn.execute(text("""
                            UPDATE estruturas_exposicao
                            SET nome = :nome,
                                descricao = :desc,
                                ativo = :ativo
                            WHERE id = :id
                        """), {
                            "nome": str(r["nome"]).strip(),
                            "desc": str(r.get("descricao") or ""),
                            "ativo": bool(r["ativo"]),
                            "id": int(r["id"])
                        })
                st.success("Estruturas atualizadas com sucesso!")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar: {e}")

        st.markdown("---")
        with st.expander("➕ Cadastrar Nova Estrutura"):
            with engine.connect() as conn:
                tipos_opts = conn.execute(text("SELECT id, nome FROM tipos_exposicao WHERE ativo = TRUE")).fetchall()
            tipos_dict = {r[1]: r[0] for r in tipos_opts}

            with st.form("form_nova_estrutura"):
                tipo_sel = st.selectbox("Tipo de Exposição Pai:", list(tipos_dict.keys()))
                nome_est = st.text_input("Nome da Estrutura (ex: Ponta Especial 5 Níveis):")
                desc_est = st.text_area("Descrição:")
                num_bandejas_iniciais = st.number_input("Criar automaticamente quantas bandejas padrão (150x50x25cm)?", min_value=1, max_value=12, value=6)

                if st.form_submit_button("Criar Estrutura"):
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
    # ABA 3: MEDIDAS DE BANDEJAS (EDITÁVEL)
    # =========================================================================
    with tab_bandejas:
        st.subheader("Bandejas e Dimensões por Estrutura")
        st.info("💡 **Edição Direta:** Altere as células de **Largura (cm)**, **Profundidade (cm)**, **Altura Útil (cm)**, **Ordem** ou **Ativa** diretamente na tabela abaixo e clique no botão **Salvar Alterações nas Bandejas**.")

        with engine.connect() as conn:
            est_rows = conn.execute(text("SELECT id, nome FROM estruturas_exposicao WHERE ativo = TRUE ORDER BY nome")).fetchall()
        est_map = {r[1]: r[0] for r in est_rows}

        if est_map:
            sel_est_bandeja = st.selectbox("Selecione a Estrutura para Gerenciar Bandejas:", list(est_map.keys()), key="sel_est_adm_bandeja")
            eid_sel = est_map[sel_est_bandeja]

            with engine.connect() as conn:
                bandejas_df = pd.read_sql(text("""
                    SELECT id, numero_bandeja, largura_cm, profundidade_cm, altura_cm, ordem, ativa
                    FROM bandejas_exposicao
                    WHERE estrutura_exposicao_id = :eid
                    ORDER BY ordem, numero_bandeja
                """), conn, params={"eid": eid_sel})

            col_config_bandejas = {
                "id": st.column_config.NumberColumn("ID", disabled=True),
                "numero_bandeja": st.column_config.NumberColumn("Nº Bandeja", min_value=1, step=1, required=True),
                "largura_cm": st.column_config.NumberColumn("Largura (cm)", min_value=1.0, step=0.5, format="%.2f", required=True),
                "profundidade_cm": st.column_config.NumberColumn("Profundidade (cm)", min_value=1.0, step=0.5, format="%.2f", required=True),
                "altura_cm": st.column_config.NumberColumn("Altura Útil (cm)", min_value=1.0, step=0.5, format="%.2f", required=True),
                "ordem": st.column_config.NumberColumn("Ordem", min_value=1, step=1),
                "ativa": st.column_config.CheckboxColumn("Ativa?", default=True)
            }

            edited_bandejas_df = st.data_editor(
                bandejas_df,
                column_config=col_config_bandejas,
                use_container_width=True,
                hide_index=True,
                key=f"editor_bandejas_{eid_sel}"
            )

            # Botão de Salvamento Direto
            col_b_save, col_b_info = st.columns([2, 4])
            with col_b_save:
                if st.button("💾 Salvar Alterações nas Bandejas", type="primary", key=f"btn_salvar_grid_bandejas_{eid_sel}"):
                    try:
                        with engine.begin() as conn:
                            for _, row in edited_bandejas_df.iterrows():
                                bid = row.get("id")
                                n_b = int(row.get("numero_bandeja", 1) or 1)
                                l_b = float(row.get("largura_cm", 150.0) or 150.0)
                                p_b = float(row.get("profundidade_cm", 50.0) or 50.0)
                                a_b = float(row.get("altura_cm", 25.0) or 25.0)
                                ordem_b = int(row.get("ordem", n_b) or n_b)
                                ativa_b = bool(row.get("ativa", True))

                                if pd.notna(bid) and int(bid) > 0:
                                    conn.execute(text("""
                                        UPDATE bandejas_exposicao
                                        SET numero_bandeja = :num,
                                            largura_cm = :larg,
                                            profundidade_cm = :prof,
                                            altura_cm = :alt,
                                            ordem = :ordem,
                                            ativa = :ativa
                                        WHERE id = :id AND estrutura_exposicao_id = :eid
                                    """), {
                                        "num": n_b,
                                        "larg": l_b,
                                        "prof": p_b,
                                        "alt": a_b,
                                        "ordem": ordem_b,
                                        "ativa": ativa_b,
                                        "id": int(bid),
                                        "eid": eid_sel
                                    })
                        st.success("✅ Medidas e dimensões das bandejas atualizadas com sucesso!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro ao atualizar bandejas: {e}")

            st.markdown("---")
            with st.expander("➕ Adicionar Nova Bandeja nesta Estrutura"):
                with st.form("form_nova_bandeja"):
                    col_b1, col_b2, col_b3, col_b4 = st.columns(4)
                    num_b = col_b1.number_input("Número da Bandeja:", min_value=1, value=len(bandejas_df) + 1)
                    larg_b = col_b2.number_input("Largura (cm):", min_value=1.0, value=150.0, step=0.5)
                    prof_b = col_b3.number_input("Profundidade (cm):", min_value=1.0, value=50.0, step=0.5)
                    alt_b = col_b4.number_input("Altura Útil (cm):", min_value=1.0, value=25.0, step=0.5)

                    if st.form_submit_button("Adicionar Bandeja"):
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO bandejas_exposicao (estrutura_exposicao_id, numero_bandeja, largura_cm, profundidade_cm, altura_cm, ordem, ativa)
                                    VALUES (:eid, :num, :larg, :prof, :alt, :num, TRUE)
                                """), {"eid": eid_sel, "num": num_b, "larg": larg_b, "prof": prof_b, "alt": alt_b})
                            st.success("Bandeja adicionada com sucesso!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao adicionar bandeja: {e}")

    # =========================================================================
    # ABA 4: DIMENSÕES FÍSICAS DOS PRODUTOS (cm)
    # =========================================================================
    with tab_produtos_dim:
        st.subheader("📦 Dimensões Físicas dos Produtos (cm)")
        st.caption("Consulte e altere as medidas de altura, largura e profundidade em centímetros. Alterações em qualquer produto de uma família são replicadas automaticamente para todos os SKUs da mesma família.")

        # Carrega mapa de descrições e famílias do parquet
        map_produtos: Dict[int, Dict[str, Any]] = {}
        parquet_path = "bdados/query.parquet"
        if os.path.exists(parquet_path):
            try:
                df_p = pd.read_parquet(parquet_path)
                df_p.columns = [str(c).strip() for c in df_p.columns]
                df_p_uniq = df_p.drop_duplicates(subset=["CODIGO_PRODUTO"])
                for _, rp in df_p_uniq.iterrows():
                    cod = int(rp["CODIGO_PRODUTO"])
                    desc = str(rp.get("DESCRICAO_PRODUTO", "")).strip()
                    fam_cod = rp.get("CODIGO_FAMILIA")
                    fam_desc = str(rp.get("DESCRICAO_FAMILIA", "")).strip() if pd.notna(rp.get("DESCRICAO_FAMILIA")) else ""
                    if pd.notna(fam_cod) and str(fam_cod).isdigit() and int(fam_cod) > 0:
                        fam_str = f"{int(fam_cod)} - {fam_desc}" if fam_desc else f"Família {int(fam_cod)}"
                    else:
                        fam_str = "Sem Família"
                    map_produtos[cod] = {
                        "descricao": desc,
                        "familia": fam_str,
                        "codigo_familia": int(fam_cod) if pd.notna(fam_cod) and str(fam_cod).isdigit() and int(fam_cod) > 0 else None
                    }
            except Exception:
                pass

        with engine.connect() as conn:
            prods_dim_df = pd.read_sql(text("""
                SELECT 
                    pd.id,
                    pd.produto_codigo,
                    pd.altura_cm,
                    pd.largura_cm,
                    pd.profundidade_cm,
                    pd.data_atualizacao,
                    pd.usuario_atualizacao
                FROM produto_dimensoes pd
                ORDER BY pd.produto_codigo
            """), conn)

        if not prods_dim_df.empty:
            prods_dim_df["descricao_produto"] = prods_dim_df["produto_codigo"].apply(
                lambda c: map_produtos.get(int(c), {}).get("descricao", f"Produto {c}")
            )
            prods_dim_df["familia_info"] = prods_dim_df["produto_codigo"].apply(
                lambda c: map_produtos.get(int(c), {}).get("familia", "Sem Família")
            )
            # Reorganizar colunas na ordem ideal de visualização
            cols_order = [
                "id", "produto_codigo", "descricao_produto", "familia_info",
                "altura_cm", "largura_cm", "profundidade_cm", "data_atualizacao", "usuario_atualizacao"
            ]
            prods_dim_df = prods_dim_df[[c for c in cols_order if c in prods_dim_df.columns]]

        col_config_pdim = {
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "produto_codigo": st.column_config.NumberColumn("Cód Consinco", disabled=True),
            "descricao_produto": st.column_config.TextColumn("Descrição do Produto", disabled=True, width="medium"),
            "familia_info": st.column_config.TextColumn("Família", disabled=True, width="medium"),
            "altura_cm": st.column_config.NumberColumn("Altura (cm)", min_value=0.1, step=0.5, format="%.2f", required=True),
            "largura_cm": st.column_config.NumberColumn("Largura (cm)", min_value=0.1, step=0.5, format="%.2f", required=True),
            "profundidade_cm": st.column_config.NumberColumn("Profundidade (cm)", min_value=0.1, step=0.5, format="%.2f", required=True),
            "data_atualizacao": st.column_config.DatetimeColumn("Última Atualização", disabled=True),
            "usuario_atualizacao": st.column_config.TextColumn("Usuário", disabled=True)
        }

        if prods_dim_df.empty:
            st.info("Nenhum produto com dimensões cadastradas no momento.")
        else:
            edited_pdim = st.data_editor(
                prods_dim_df,
                column_config=col_config_pdim,
                use_container_width=True,
                hide_index=True,
                key="editor_prods_dim"
            )

            if st.button("💾 Salvar Alterações nas Dimensões dos Produtos", type="primary", key="btn_salvar_pdim"):
                try:
                    for _, r in edited_pdim.iterrows():
                        salvar_dimensoes_produto(
                            engine=engine,
                            produto_codigo=int(r["produto_codigo"]),
                            altura_cm=float(r["altura_cm"]),
                            largura_cm=float(r["largura_cm"]),
                            profundidade_cm=float(r["profundidade_cm"]),
                            usuario=usuario_atual,
                            propagar_familia=True
                        )
                    st.success("✅ Dimensões dos produtos e famílias sincronizadas com sucesso!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao atualizar dimensões: {e}")

        st.markdown("---")
        with st.expander("➕ Cadastrar Dimensões de um Novo Produto"):
            col_busca, col_info_p = st.columns([1, 2])
            novo_pcod = col_busca.number_input("Código do Produto (Consinco):", min_value=1, step=1, key="novo_pcod_admin_dim")
            
            p_info = map_produtos.get(int(novo_pcod))
            if p_info:
                col_info_p.markdown(f"📦 **Descrição:** `{p_info['descricao']}`")
                col_info_p.caption(f"👨‍👩‍👧‍👦 **Família:** `{p_info['familia']}` *(As medidas serão salvas e propagadas automaticamente para toda a família)*")
            elif novo_pcod > 1:
                col_info_p.info("Código não localizado no catálogo local. O registro será criado normalmente.")

            with st.form("form_novo_prod_dim"):
                col_pd1, col_pd2, col_pd3 = st.columns(3)
                novo_palt = col_pd1.number_input("Altura (cm):", min_value=0.1, value=10.0, step=0.5)
                novo_plarg = col_pd2.number_input("Largura (cm):", min_value=0.1, value=10.0, step=0.5)
                novo_pprof = col_pd3.number_input("Profundidade (cm):", min_value=0.1, value=10.0, step=0.5)

                if st.form_submit_button("Salvar Dimensões"):
                    suc_salvar, msg_salvar = salvar_dimensoes_produto(
                        engine=engine,
                        produto_codigo=int(novo_pcod),
                        altura_cm=float(novo_palt),
                        largura_cm=float(novo_plarg),
                        profundidade_cm=float(novo_pprof),
                        usuario=usuario_atual,
                        propagar_familia=True
                    )
                    if suc_salvar:
                        st.success(msg_salvar)
                        st.rerun()
                    else:
                        st.error(msg_salvar)

    # =========================================================================
    # ABA 5: HISTÓRICO DE AUDITORIA
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
