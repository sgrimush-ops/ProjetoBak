import streamlit as st
import pandas as pd
import json
import os
from sqlalchemy import text
from utils.timezone import now_brazil
from utils.fornecedores_loader import (
    load_produtos_para_fornecedor,
    get_all_fornecedores_catalog,
    parse_fornecedores_acesso,
    format_fornecedores_summary,
)
from page import (
    resolve_pedidos_codigo_col,
    resolve_pedidos_descricao_col,
    resolve_pedidos_emb_col,
    has_table_column,
)

# --- Constantes ---
LISTA_LOJAS_PADRAO = [
    "001", "002", "003", "004", "005", "006", "007", "008",
    "011", "012", "013", "014", "016", "017", "018",
    "F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09",
    "F10", "F11", "M12", "M13", "ADM", "RH"
]


# --- Funções de Chamado ---
def create_fornecedor_ticket(engine, username, assunto, mensagem):
    """Cria um novo ticket de suporte/observação do fornecedor."""
    now = now_brazil()
    try:
        with engine.begin() as conn:
            query_ticket = text("""
                INSERT INTO contato_chamados (
                    usuario_username,
                    assunto,
                    data_criacao,
                    ultimo_update,
                    status
                )
                VALUES (:username, :assunto, :now, :now, 'Aguardando Retorno')
                RETURNING id;
            """)
            result = conn.execute(
                query_ticket,
                {"username": username, "assunto": assunto, "now": now},
            )
            new_ticket_id = result.scalar_one()

            query_msg = text("""
                INSERT INTO contato_mensagens (
                    chamado_id,
                    remetente_username,
                    mensagem,
                    data_envio
                )
                VALUES (:chamado_id, :username, :mensagem, :now)
            """)
            conn.execute(
                query_msg,
                {
                    "chamado_id": new_ticket_id,
                    "username": username,
                    "mensagem": mensagem,
                    "now": now,
                },
            )
        return True, new_ticket_id
    except Exception as e:
        return False, f"Erro ao criar chamado: {e}"


# --- Funções de Banco de Dados ---
def save_fornecedor_pedidos(engine, pedidos_df):
    """Salva pedidos do fornecedor na tabela pedidos_consolidados de forma resiliente."""
    try:
        from sqlalchemy import inspect
        try:
            insp = inspect(engine)
            cols_db = {c['name'].lower() for c in insp.get_columns("pedidos_consolidados")}
        except Exception:
            cols_db = _get_table_columns(engine, "pedidos_consolidados")

        df_real = pedidos_df.copy()

        # Compatibilidade com colunas legadas se existirem
        if cols_db:
            if "codigo" in cols_db and "codigo_interno" not in cols_db and "codigo_interno" in df_real.columns:
                df_real["codigo"] = df_real["codigo_interno"]

            if "embalagem" in cols_db and "embseparacao" not in cols_db and "embseparacao" in df_real.columns:
                df_real["embalagem"] = df_real["embseparacao"]

            if "embseparacao" in cols_db and "embseparacao" not in df_real.columns and "embalagem" in df_real.columns:
                df_real["embseparacao"] = df_real["embalagem"]

            cols_validas = [c for c in df_real.columns if c.lower() in cols_db and c.lower() != "id"]
            if cols_validas:
                df_real = df_real[cols_validas]

        # Garante remoção de colunas com nomes duplicados
        df_real = df_real.loc[:, ~df_real.columns.duplicated()]

        with engine.begin() as conn:
            df_real.to_sql(
                "pedidos_consolidados",
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
            )
        return True
    except Exception as e:
        st.error(f"Erro ao salvar os pedidos: {e}")
        return False


# --- Lógica da Página ---
def show_area_fornecedor(base_data_path: str = None):
    """
    Área principal do fornecedor / representante para digitar pedidos de mix.
    Conectado diretamente ao catálogo analítico query.parquet com digitação sem delay.
    """
    st.title("📦 Área do Fornecedor & Representante - Pedidos de Mix")

    from app import get_engine
    engine = get_engine()

    # Dados da sessão
    username = st.session_state.get("fornecedor_username", "")
    role = st.session_state.get("fornecedor_role", "fornecedor")
    lojas_acesso = st.session_state.get("fornecedor_lojas_acesso", [])
    
    # Normalização de lojas
    lojas_acesso_limpas = []
    for loja in lojas_acesso or []:
        l_str = str(loja).strip()
        if l_str.lower().startswith("loja_"):
            l_str = l_str[5:]
        if l_str and l_str not in lojas_acesso_limpas:
            lojas_acesso_limpas.append(l_str)

    if not lojas_acesso_limpas:
        lojas_acesso_limpas = list(LISTA_LOJAS_PADRAO)

    # Buscar dados completos do fornecedor no DB
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT empresa, role, fornecedores_acesso, lojas_acesso
                FROM fornecedores_users
                WHERE username = :username
            """)
            result = conn.execute(query, {"username": username.lower()}).fetchone()
            if result:
                empresa = result[0] or "Geral"
                role_db = result[1] or role
                forn_acesso_raw = result[2]
            else:
                empresa = "Geral"
                role_db = role
                forn_acesso_raw = "[]"
    except Exception as e:
        st.error(f"Erro ao buscar dados do fornecedor: {e}")
        return

    is_admin = (role_db == "admin_fornecedor") or (str(empresa).strip().lower() in ["administração", "administracao"])
    codigos_autorizados = parse_fornecedores_acesso(forn_acesso_raw)

    # Informações de cabeçalho
    catalog_df = get_all_fornecedores_catalog(base_data_path)
    resumo_fornecedores = format_fornecedores_summary(codigos_autorizados, catalog_df, max_display=3)
    
    col_h1, col_h2 = st.columns([3, 1])
    with col_h1:
        st.info(
            f"👤 **Usuário:** `{username}`\n\n"
            f"🏢 **Indústrias Vinculadas:** {resumo_fornecedores}"
        )
    with col_h2:
        if is_admin:
            st.success("👑 Perfil Administrador")

    # --- Seletor de Fornecedor Específico (se Admin ou se Representante de Múltiplos) ---
    filtro_forn_cod = None
    if is_admin and not codigos_autorizados:
        st.markdown("### 🏢 Seleção de Fornecedor (Modo Admin)")
        opcoes_forn = ["Todos os Fornecedores"] + catalog_df["label"].tolist()
        escolha_forn = st.selectbox(
            "Visualizar produtos de qual fornecedor?",
            opcoes_forn,
            index=0,
            key="admin_select_fornecedor_view"
        )
        if escolha_forn != "Todos os Fornecedores":
            cod_str = escolha_forn.split(" - ")[0]
            if cod_str.isdigit():
                filtro_forn_cod = int(cod_str)
    elif len(codigos_autorizados) > 1:
        st.markdown("### 🏢 Filtrar por Indústria / Marca")
        sub_catalog = catalog_df[catalog_df["cod_fornecedor"].isin(codigos_autorizados)]
        opcoes_rep = ["Todas as Minhas Marcas"] + sub_catalog["label"].tolist()
        escolha_rep = st.selectbox(
            "Filtrar produtos por marca:",
            opcoes_rep,
            index=0,
            key="rep_select_marca_view"
        )
        if escolha_rep != "Todas as Minhas Marcas":
            cod_str = escolha_rep.split(" - ")[0]
            if cod_str.isdigit():
                filtro_forn_cod = int(cod_str)

    # --- Seletor de Loja ---
    st.markdown("### 🏪 Loja Destino")
    if len(lojas_acesso_limpas) > 1:
        selected_loja = st.selectbox(
            "Selecione a loja para digitar pedidos:",
            lojas_acesso_limpas,
            index=0,
            key="forn_selected_loja",
        )
    else:
        selected_loja = lojas_acesso_limpas[0]
        st.success(f"Loja selecionada: **{selected_loja}**")

    if not selected_loja:
        st.info("Por favor, selecione uma loja para começar.")
        return

    # --- Carregar Mix do Fornecedor do query.parquet ---
    with st.spinner("Carregando mix de produtos do query.parquet..."):
        mix_df = load_produtos_para_fornecedor(
            fornecedor_codes=codigos_autorizados,
            empresa_fallback=empresa,
            is_admin=is_admin,
            base_data_path=base_data_path,
            filtro_fornecedor_selecionado=filtro_forn_cod,
            codigo_loja=selected_loja
        )

    if mix_df.empty:
        if not is_admin and not codigos_autorizados:
            st.warning(
                f"⚠️ Nenhum fornecedor/indústria vinculado ao usuário '{username}'.\n\n"
                "Solicite ao administrador para vincular as indústrias que você representa na aba **'Admin Fornecedores & Representantes'**."
            )
        else:
            st.warning(
                "Nenhum produto encontrado para a seleção atual no `query.parquet`."
            )
        return

    total_items = len(mix_df)

    # --- Filtros de Busca ---
    st.markdown("### 🔍 Pesquisar Itens")
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        filtro_codigo = st.text_input(
            "Filtrar por Código Consinco:",
            key="filtro_codigo"
        )
    with col_f2:
        filtro_desc = st.text_input(
            "Filtrar por Descrição do Produto:",
            key="filtro_desc"
        )
    with col_f3:
        filtro_ean = st.text_input(
            "Filtrar por EAN:",
            key="filtro_ean"
        )

    # Aplicar filtros
    filtered_df = mix_df.copy()
    if filtro_codigo:
        filtered_df = filtered_df[
            filtered_df["codigo_interno"].astype(str).str.contains(
                filtro_codigo.strip(), case=False, na=False
            )
        ]
    if filtro_desc:
        filtered_df = filtered_df[
            filtered_df["descricao"].astype(str).str.contains(
                filtro_desc.strip(), case=False, na=False
            )
        ]
    if filtro_ean:
        filtered_df = filtered_df[
            filtered_df["codigo_ean"].astype(str).str.contains(
                filtro_ean.strip(), case=False, na=False
            )
        ]

    total_filtered = len(filtered_df)

    if filtered_df.empty:
        st.warning("Nenhum produto encontrado com os filtros aplicados.")
        return

    # --- Configurações de Paginação & Visualização Rápida ---
    if "fornecedor_page" not in st.session_state:
        st.session_state.fornecedor_page = 0
    if "fornecedor_pedidos_salvos" not in st.session_state:
        st.session_state.fornecedor_pedidos_salvos = {}
    if "fornecedor_page_size" not in st.session_state:
        st.session_state.fornecedor_page_size = 30

    col_cfg1, col_cfg2, col_cfg3 = st.columns([1, 1, 2])
    with col_cfg1:
        opcoes_qtd_pag = [20, 30, 50, 100, 200]
        cur_size_idx = opcoes_qtd_pag.index(st.session_state.fornecedor_page_size) if st.session_state.fornecedor_page_size in opcoes_qtd_pag else 1
        page_size_escolhido = st.selectbox(
            "Itens por página:",
            opcoes_qtd_pag,
            index=cur_size_idx,
            key="sel_itens_por_pagina"
        )
        if page_size_escolhido != st.session_state.fornecedor_page_size:
            st.session_state.fornecedor_page_size = page_size_escolhido
            st.session_state.fornecedor_page = 0
            st.rerun()

    items_per_page = st.session_state.fornecedor_page_size
    total_pages = max(1, (total_filtered - 1) // items_per_page + 1)
    current_page = min(st.session_state.fornecedor_page, total_pages - 1)
    st.session_state.fornecedor_page = current_page

    with col_cfg2:
        opcoes_pags = list(range(1, total_pages + 1))
        pag_escolhida = st.selectbox(
            "Ir para a página:",
            opcoes_pags,
            index=current_page,
            key="sel_ir_para_pagina"
        ) - 1
        if pag_escolhida != current_page:
            st.session_state.fornecedor_page = pag_escolhida
            st.rerun()

    with col_cfg3:
        st.write("")
        st.write("")
        st.caption(f"📦 Total: **{total_filtered} produtos** | **Página {current_page + 1} de {total_pages}**")

    start_idx = current_page * items_per_page
    end_idx = min(start_idx + items_per_page, total_filtered)

    page_df = filtered_df.iloc[start_idx:end_idx].copy().reset_index(drop=True)

    # Restaura pedidos já salvos em memória
    page_df["Pedido (Cx)"] = 0
    for i, row in page_df.iterrows():
        cod_prod = int(row["codigo_interno"])
        key = f"{selected_loja}_{cod_prod}"
        if key in st.session_state.fornecedor_pedidos_salvos:
            page_df.at[i, "Pedido (Cx)"] = int(st.session_state.fornecedor_pedidos_salvos[key])

    # --- Formulário Isolado com Digitação em Tempo Real Sem Delay ---
    st.markdown("---")
    st.markdown("### 📝 Digite as Quantidades (Caixas)")
    st.caption("⚡ **Digitação rápida ativada:** Navegue entre as linhas com as setas ou Enter sem travamento. Clique em **Salvar** para registrar a página.")

    colunas_config = {
        "codigo_interno": st.column_config.NumberColumn(
            "Código Interno", disabled=True, format="%d"
        ),
        "descricao": st.column_config.TextColumn(
            "Produto", width="large", disabled=True
        ),
        "fornecedor_label": st.column_config.TextColumn(
            "Fornecedor / Indústria", width="medium", disabled=True
        ),
        "codigo_ean": st.column_config.TextColumn(
            "EAN", disabled=True
        ),
        "embalagem": st.column_config.NumberColumn(
            "Emb. (Un/Cx)", disabled=True, format="%d"
        ),
        "estoque_cd": st.column_config.NumberColumn(
            "Estoque CD15 (Cx)", disabled=True, format="%d"
        ),
        "estoque_loja": st.column_config.NumberColumn(
            f"Estoque Loja {selected_loja} (Cx)", disabled=True, format="%d"
        ),
        "Pedido (Cx)": st.column_config.NumberColumn(
            "Pedido (Cx)", min_value=0, step=1
        ),
    }

    cols_exibir = [
        "codigo_interno", "descricao", "fornecedor_label",
        "codigo_ean", "embalagem", "estoque_cd", "estoque_loja", "Pedido (Cx)"
    ]
    cols_existentes = [c for c in cols_exibir if c in page_df.columns]

    # Envolve o grid em um formulário para neutralizar o websocket delay
    with st.form(f"form_grid_{selected_loja}_{current_page}", clear_on_submit=False):
        edited_df = st.data_editor(
            page_df[cols_existentes],
            column_config=colunas_config,
            hide_index=True,
            use_container_width=True,
            key=f"forn_editor_{selected_loja}_{current_page}",
        )

        st.markdown("---")
        col_b1, col_b2, col_b3 = st.columns([1, 2, 1])
        
        with col_b1:
            btn_voltar = st.form_submit_button(
                "⬅️ Salvar e Página Anterior",
                disabled=(current_page == 0),
                use_container_width=True
            )
            
        with col_b2:
            btn_salvar_avancar = st.form_submit_button(
                "💾 Salvar Página e Avançar ➡️" if current_page < total_pages - 1 else "💾 Salvar Quantidades Desta Página",
                type="primary",
                use_container_width=True
            )

        with col_b3:
            btn_avancar = st.form_submit_button(
                "Salvar e Próxima ➡️",
                disabled=(current_page >= total_pages - 1),
                use_container_width=True
            )

    # Processamento do formulário
    if btn_voltar or btn_salvar_avancar or btn_avancar:
        salvos_count = 0
        for _, row_ed in edited_df.iterrows():
            cod_p = int(row_ed["codigo_interno"])
            qtd = int(row_ed.get("Pedido (Cx)", 0) or 0)
            key = f"{selected_loja}_{cod_p}"
            if qtd > 0:
                st.session_state.fornecedor_pedidos_salvos[key] = qtd
                salvos_count += 1
            elif key in st.session_state.fornecedor_pedidos_salvos:
                del st.session_state.fornecedor_pedidos_salvos[key]

        if btn_voltar and current_page > 0:
            st.session_state.fornecedor_page = current_page - 1
            st.rerun()
        elif (btn_salvar_avancar or btn_avancar) and current_page < total_pages - 1:
            st.session_state.fornecedor_page = current_page + 1
            st.rerun()
        else:
            st.success(f"✅ Quantidades desta página gravadas com sucesso ({salvos_count} itens preenchidos)!")
            st.rerun()

    # --- Resumo Geral de Pedidos Gravados & Envio para Aprovação ---
    st.markdown("---")
    st.markdown("### 🚀 Finalizar Envio para Aprovação")

    pedidos_loja_atual = {
        k: v for k, v in st.session_state.fornecedor_pedidos_salvos.items()
        if k.startswith(f"{selected_loja}_") and v > 0
    }
    
    total_pedidos_salvos = len(pedidos_loja_atual)
    
    if total_pedidos_salvos > 0:
        st.success(f"📊 Você possui **{total_pedidos_salvos} produtos** com quantidades salvas para a **Loja {selected_loja}**.")
        
        # Exibe resumo em tabela
        resumo_lista = []
        prod_map = mix_df.set_index("codigo_interno").to_dict(orient="index")
        for key, qtd in pedidos_loja_atual.items():
            _, cod_str = key.split("_", 1)
            cod_int = int(cod_str)
            pinfo = prod_map.get(cod_int, {})
            resumo_lista.append({
                "Loja": selected_loja,
                "Código": cod_int,
                "Produto": pinfo.get("descricao", "Produto"),
                "Fornecedor": pinfo.get("fornecedor_label", ""),
                "Emb.": pinfo.get("embalagem", 1),
                "Estoque CD": pinfo.get("estoque_cd", 0),
                "Estoque Loja": pinfo.get("estoque_loja", 0),
                "Pedido (Cx)": qtd,
                "Total Unidades": qtd * int(pinfo.get("embalagem", 1))
            })
            
        df_resumo = pd.DataFrame(resumo_lista)
        st.dataframe(df_resumo, hide_index=True, use_container_width=True)

        motivo_pedido = st.text_area(
            "📝 Observações do Pedido (Opcional):",
            placeholder="Ex: Campanha especial de vendas, reposição programada, queima de estoque...",
            key="motivo_pedido_input"
        )

        col_send, col_clear = st.columns([3, 1])
        with col_send:
            if st.button("📤 Enviar Pedido Completo para Aprovação", type="primary", use_container_width=True):
                pedidos_finais = []
                for item in resumo_lista:
                    cod_int = item["Código"]
                    qtd = item["Pedido (Cx)"]
                    pinfo = prod_map.get(cod_int, {})
                    
                    registro = {
                        "codigo_interno": str(cod_int),
                        "descricao": pinfo.get("descricao", "SEM DESCRIÇÃO"),
                        "codigo_ean": pinfo.get("codigo_ean", ""),
                        "embseparacao": int(pinfo.get("embalagem", 1)),
                        f"loja_{selected_loja}": qtd,
                        "total_cx": qtd,
                        "data_pedido": now_brazil(),
                        "usuario_pedido": username,
                        "status_aprovacao": "Pendente",
                        "status_item": "Ativo",
                        "origem_pedido": "Fornecedor",
                    }
                    
                    # Preenche colunas das demais lojas com 0
                    for loja_nome in LISTA_LOJAS_PADRAO:
                        cname = f"loja_{loja_nome}"
                        if cname not in registro:
                            registro[cname] = 0
                            
                    pedidos_finais.append(registro)

                if pedidos_finais:
                    df_pedidos_salvar = pd.DataFrame(pedidos_finais)
                    if save_fornecedor_pedidos(engine, df_pedidos_salvar):
                        if motivo_pedido.strip():
                            assunto = f"Pedido Fornecedor - Loja {selected_loja}"
                            mensagem = (
                                f"Pedido enviado pelo representante.\n\n"
                                f"**Representante:** {username}\n"
                                f"**Loja:** {selected_loja}\n"
                                f"**Total de Itens:** {len(pedidos_finais)}\n\n"
                                f"**Observações:**\n{motivo_pedido}"
                            )
                            create_fornecedor_ticket(engine, username, assunto, mensagem)

                        st.success(f"🎉 {len(pedidos_finais)} item(ns) enviados com sucesso para aprovação!")
                        st.balloons()
                        
                        # Limpa pedidos da loja enviada
                        for k in list(st.session_state.fornecedor_pedidos_salvos.keys()):
                            if k.startswith(f"{selected_loja}_"):
                                del st.session_state.fornecedor_pedidos_salvos[k]
                        st.session_state.fornecedor_page = 0
                        st.rerun()
                    else:
                        st.error("Erro ao gravar pedido no banco de dados. Tente novamente.")
                        
        with col_clear:
            if st.button("🗑️ Limpar Pedidos Digitados", use_container_width=True):
                for k in list(st.session_state.fornecedor_pedidos_salvos.keys()):
                    if k.startswith(f"{selected_loja}_"):
                        del st.session_state.fornecedor_pedidos_salvos[k]
                st.session_state.fornecedor_page = 0
                st.rerun()
    else:
        st.info("Preencha a coluna **'Pedido (Cx)'** nos produtos desejados acima e clique em **Salvar Página** para prosseguir.")
