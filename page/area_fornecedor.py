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
ITEMS_PER_PAGE = 30
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
    """Salva pedidos do fornecedor na tabela pedidos_consolidados."""
    try:
        pedidos_code_col = resolve_pedidos_codigo_col(engine)
        pedidos_desc_col = resolve_pedidos_descricao_col(engine)
        pedidos_emb_col = resolve_pedidos_emb_col(engine)

        rename_map = {}
        if pedidos_code_col != "codigo_interno":
            rename_map["codigo_interno"] = pedidos_code_col
        if pedidos_desc_col != "descricao":
            rename_map["descricao"] = pedidos_desc_col
        if pedidos_emb_col != "embalagem":
            rename_map["embalagem"] = pedidos_emb_col

        df_real = pedidos_df.rename(columns=rename_map).copy()

        # Garante embseparacao se existir na tabela
        if has_table_column(engine, "pedidos_consolidados", "embseparacao") and "embseparacao" not in df_real.columns:
            if "embalagem" in df_real.columns:
                df_real["embseparacao"] = df_real["embalagem"]

        # Compatibilidade com colunas legadas
        if has_table_column(engine, "pedidos_consolidados", "codigo") and "codigo" not in df_real.columns:
            code_col = pedidos_code_col if pedidos_code_col in df_real.columns else "codigo_interno"
            if code_col in df_real.columns:
                df_real["codigo"] = df_real[code_col]

        with engine.begin() as conn:
            df_real.to_sql(
                "pedidos_consolidados",
                con=conn,
                if_exists="append",
                index=False,
            )
        return True
    except Exception as e:
        st.error(f"Erro ao salvar os pedidos: {e}")
        return False


# --- Lógica da Página ---
def show_area_fornecedor(base_data_path: str = None):
    """
    Área principal do fornecedor / representante para digitar pedidos de mix.
    Conectado diretamente ao catálogo analítico query.parquet.
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
            f"👤 **Usuário:** `{username}` | **Empresa / Representação:** `{empresa}`\n\n"
            f"🏢 **Indústrias Vinculadas:** {resumo_fornecedores}"
        )
    with col_h2:
        if is_admin:
            st.success("👑 Perfil Administrador")

    # --- Seletor de Fornecedor Específico (se Admin ou se Representante de Múltiplos) ---
    filtro_forn_cod = None
    if is_admin and not codigos_autorizados:
        # Admin sem filtro fixo pode escolher qualquer indústria do catálogo
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
        # Representante multi-fornecedores pode filtrar por uma marca específica
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
            filtro_fornecedor_selecionado=filtro_forn_cod
        )

    if mix_df.empty:
        if not is_admin and not codigos_autorizados:
            st.warning(
                f"⚠️ Nenhum fornecedor/indústria vinculado ao usuário '{username}'.\n\n"
                "Solicite ao administrador para vincular as indústrias que você representa na aba **'Admin Fornecedores'**."
            )
        else:
            st.warning(
                f"Nenhum produto encontrado para a seleção atual no `query.parquet`."
            )
        return

    total_items = len(mix_df)
    st.success(f"📦 **Total de produtos disponíveis no seu mix:** {total_items}")

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

    if total_filtered != total_items:
        st.info(f"**Produtos filtrados:** {total_filtered} de {total_items}")

    # --- Paginação ---
    st.markdown("---")
    st.markdown("### 📝 Digite as Quantidades (Caixas)")

    if "fornecedor_page" not in st.session_state:
        st.session_state.fornecedor_page = 0
    if "fornecedor_pedidos_salvos" not in st.session_state:
        st.session_state.fornecedor_pedidos_salvos = {}

    current_page = st.session_state.fornecedor_page
    total_pages = max(1, (total_filtered - 1) // ITEMS_PER_PAGE + 1)
    
    if current_page >= total_pages:
        current_page = 0
        st.session_state.fornecedor_page = 0

    start_idx = current_page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, total_filtered)

    page_df = filtered_df.iloc[start_idx:end_idx].copy().reset_index(drop=True)

    # Restaura pedidos já salvos nesta página
    page_df["Pedido (Cx)"] = 0
    for i, row in page_df.iterrows():
        cod_prod = int(row["codigo_interno"])
        key = f"{selected_loja}_{cod_prod}"
        if key in st.session_state.fornecedor_pedidos_salvos:
            page_df.at[i, "Pedido (Cx)"] = st.session_state.fornecedor_pedidos_salvos[key]

    col_pg1, col_pg2 = st.columns([2, 1])
    with col_pg1:
        st.write(f"**Página {current_page + 1} de {total_pages}** (Exibindo {start_idx + 1} a {end_idx} de {total_filtered} itens)")

    # Editor de dados
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
        "Pedido (Cx)": st.column_config.NumberColumn(
            "Pedido (Cx)", min_value=0, step=1
        ),
    }

    cols_exibir = [
        "codigo_interno", "descricao", "fornecedor_label",
        "codigo_ean", "embalagem", "estoque_cd", "Pedido (Cx)"
    ]
    cols_existentes = [c for c in cols_exibir if c in page_df.columns]

    edited_df = st.data_editor(
        page_df[cols_existentes],
        column_config=colunas_config,
        hide_index=True,
        use_container_width=True,
        key=f"forn_editor_{selected_loja}_{current_page}",
    )

    # --- Botões de Navegação e Salvamento da Página ---
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])

    with col1:
        if current_page > 0:
            if st.button("⬅️ Página Anterior", use_container_width=True):
                for _, row_ed in edited_df.iterrows():
                    cod_p = int(row_ed["codigo_interno"])
                    qtd = int(row_ed.get("Pedido (Cx)", 0))
                    key = f"{selected_loja}_{cod_p}"
                    if qtd > 0:
                        st.session_state.fornecedor_pedidos_salvos[key] = qtd
                    elif key in st.session_state.fornecedor_pedidos_salvos:
                        del st.session_state.fornecedor_pedidos_salvos[key]
                st.session_state.fornecedor_page -= 1
                st.rerun()

    with col2:
        if st.button("💾 Salvar Página e Avançar", type="primary", use_container_width=True):
            salvos_nesta_pag = 0
            for _, row_ed in edited_df.iterrows():
                cod_p = int(row_ed["codigo_interno"])
                qtd = int(row_ed.get("Pedido (Cx)", 0))
                key = f"{selected_loja}_{cod_p}"
                if qtd > 0:
                    st.session_state.fornecedor_pedidos_salvos[key] = qtd
                    salvos_nesta_pag += 1
                elif key in st.session_state.fornecedor_pedidos_salvos:
                    del st.session_state.fornecedor_pedidos_salvos[key]

            if salvos_nesta_pag > 0:
                st.success(f"✅ {salvos_nesta_pag} item(ns) gravados na memória!")
            else:
                st.info("Nenhuma quantidade digitada nesta página.")

            if current_page < total_pages - 1:
                st.session_state.fornecedor_page += 1
                st.rerun()

    with col3:
        if current_page < total_pages - 1:
            if st.button("Próxima Página ➡️", use_container_width=True):
                for _, row_ed in edited_df.iterrows():
                    cod_p = int(row_ed["codigo_interno"])
                    qtd = int(row_ed.get("Pedido (Cx)", 0))
                    key = f"{selected_loja}_{cod_p}"
                    if qtd > 0:
                        st.session_state.fornecedor_pedidos_salvos[key] = qtd
                    elif key in st.session_state.fornecedor_pedidos_salvos:
                        del st.session_state.fornecedor_pedidos_salvos[key]
                st.session_state.fornecedor_page += 1
                st.rerun()

    # --- Resumo e Finalização ---
    st.markdown("---")
    st.markdown("### 🚀 Finalizar Envio para Aprovação")
    
    # Atualiza memória com dados visíveis antes de checar total
    for _, row_ed in edited_df.iterrows():
        cod_p = int(row_ed["codigo_interno"])
        qtd = int(row_ed.get("Pedido (Cx)", 0))
        key = f"{selected_loja}_{cod_p}"
        if qtd > 0:
            st.session_state.fornecedor_pedidos_salvos[key] = qtd
        elif key in st.session_state.fornecedor_pedidos_salvos:
            del st.session_state.fornecedor_pedidos_salvos[key]

    pedidos_loja_atual = {
        k: v for k, v in st.session_state.fornecedor_pedidos_salvos.items()
        if k.startswith(f"{selected_loja}_") and v > 0
    }
    
    total_pedidos_salvos = len(pedidos_loja_atual)
    
    if total_pedidos_salvos > 0:
        st.success(f"📊 Você possui **{total_pedidos_salvos} produtos** com quantidades preenchidas para a **Loja {selected_loja}**.")
        
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
                        "embalagem": int(pinfo.get("embalagem", 1)),
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
                                f"Pedido enviado pelo fornecedor/representante.\n\n"
                                f"**Empresa:** {empresa}\n"
                                f"**Usuário:** {username}\n"
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
        st.info("Preencha a coluna **'Pedido (Cx)'** nos produtos desejados acima para enviar.")
