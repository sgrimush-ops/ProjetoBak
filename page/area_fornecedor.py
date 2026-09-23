import streamlit as st
import pandas as pd
from sqlalchemy import text
from utils.timezone import now_brazil
from page import (
    resolve_mix_codigo_col,
    resolve_mix_descricao_col,
    resolve_mix_emb_col,
    resolve_pedidos_codigo_col,
    resolve_pedidos_descricao_col,
    resolve_pedidos_emb_col,
    has_table_column,
)

# --- Constantes ---
ITEMS_PER_PAGE = 30


# --- Funções de Chamado ---


def create_fornecedor_ticket(engine, username, assunto, mensagem):
    """Cria um novo ticket de fornecedor."""
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
                VALUES (:username, :assunto, :now, :now,
                    'Aguardando Retorno')
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


def get_fornecedor_mix(engine, empresa_fornecedor):
    """
    Busca produtos do mix que pertencem ao fornecedor.
    Filtra pela coluna 'origem' = empresa do fornecedor.
    """
    mix_code = resolve_mix_codigo_col(engine)
    mix_desc = resolve_mix_descricao_col(engine)
    mix_emb = resolve_mix_emb_col(engine)

    # Verifica se a coluna 'origem' existe
    has_origem = has_table_column(engine, "mix_produtos", "origem")

    if not has_origem:
        st.error(
            "A coluna 'origem' não existe na tabela mix_produtos. "
            "Por favor, atualize o arquivo mix.parquet com esta coluna."
        )
        return pd.DataFrame()

    query = text(f"""
        SELECT
            {mix_code} AS codigo_interno,
            {mix_desc} AS descricao,
            codigo_ean,
            estoque_cd,
            {mix_emb} AS embalagem,
            origem
        FROM mix_produtos
        WHERE LOWER(TRIM(origem)) = LOWER(TRIM(:empresa))
        ORDER BY {mix_desc}
    """)

    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={"empresa": empresa_fornecedor}
        )

    # Garante tipos corretos
    if "embalagem" in df.columns:
        df["embalagem"] = (
            pd.to_numeric(df["embalagem"], errors="coerce")
            .fillna(0)
            .astype(int)
        )

    if "estoque_cd" in df.columns:
        df["estoque_cd"] = (
            pd.to_numeric(df["estoque_cd"], errors="coerce")
            .fillna(0)
            .astype(int)
        )

    return df


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

        df_real = pedidos_df.rename(columns=rename_map)

        # Compatibilidade com colunas legadas
        try:
            if has_table_column(
                engine, "pedidos_consolidados", "codigo"
            ) and "codigo" not in df_real.columns:
                code_col = (
                    pedidos_code_col
                    if pedidos_code_col in df_real.columns
                    else "codigo_interno"
                )
                if code_col in df_real.columns:
                    df_real["codigo"] = df_real[code_col]
        except Exception:
            pass

        try:
            desc_source = None
            for cand in [
                pedidos_desc_col,
                "descricao",
                "produto",
                "nome_produto",
            ]:
                if cand and cand in df_real.columns:
                    desc_source = cand
                    break

            if desc_source:
                for legacy_col in ["produto", "nome_produto", "descricao"]:
                    if (
                        has_table_column(
                            engine, "pedidos_consolidados", legacy_col
                        )
                        and legacy_col not in df_real.columns
                    ):
                        df_real[legacy_col] = df_real[desc_source]
        except Exception:
            pass

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


def show_area_fornecedor():
    """
    Área principal do fornecedor para digitar pedidos do seu mix.
    """
    st.title("📦 Área do Fornecedor - Pedidos de Mix")

    # Obtém engine do session_state ou cria uma nova conexão
    from app import get_engine
    engine = get_engine()

    # Dados do fornecedor logado
    username = st.session_state.get("fornecedor_username", "")
    lojas_acesso = st.session_state.get("fornecedor_lojas_acesso", [])
    lojas_acesso = [
        (
            str(loja).strip()[5:]
            if str(loja).strip().lower().startswith("loja_")
            else str(loja).strip()
        )
        for loja in lojas_acesso
        if str(loja).strip()
    ]

    # Buscar empresa do fornecedor no banco
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT empresa
                FROM fornecedores_users
                WHERE username = :username
            """)
            result = conn.execute(query, {"username": username})
            data = result.fetchone()
            empresa = data[0] if data else None
    except Exception as e:
        st.error(f"Erro ao buscar dados do fornecedor: {e}")
        return

    if not empresa:
        st.error("Empresa não encontrada para este fornecedor.")
        return

    st.info(f"**Empresa:** {empresa} | **Usuário:** {username}")

    if not lojas_acesso:
        st.warning(
            "Você não tem lojas associadas. Contate o administrador."
        )
        return

    # --- Seletor de Loja ---
    if len(lojas_acesso) > 1:
        selected_loja = st.selectbox(
            "Selecione a loja para digitar pedidos:",
            lojas_acesso,
            index=None,
            placeholder="Escolha uma loja...",
        )
    else:
        selected_loja = lojas_acesso[0]
        st.success(f"Loja selecionada: **{selected_loja}**")

    if not selected_loja:
        st.info("Por favor, selecione uma loja para começar.")
        return

    # --- Carregar Mix do Fornecedor ---
    mix_df = get_fornecedor_mix(engine, empresa)

    if mix_df.empty:
        st.warning(
            f"Nenhum produto encontrado para a empresa '{empresa}'. "
            "Verifique se a coluna 'origem' no mix.parquet "
            "está preenchida corretamente."
        )
        return

    total_items = len(mix_df)
    st.success(f"**Total de produtos no seu mix:** {total_items}")

    # --- Filtros ---
    st.markdown("### 🔍 Filtros")
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        filtro_codigo = st.text_input(
            "Filtrar por Código Interno:",
            key="filtro_codigo"
        )
    with col_f2:
        filtro_ean = st.text_input(
            "Filtrar por EAN:",
            key="filtro_ean"
        )

    # Aplicar filtros
    filtered_df = mix_df.copy()
    if filtro_codigo:
        filtered_df = filtered_df[
            filtered_df["codigo_interno"].astype(str).str.contains(
                filtro_codigo, case=False, na=False
            )
        ]
    if filtro_ean:
        filtered_df = filtered_df[
            filtered_df["codigo_ean"].astype(str).str.contains(
                filtro_ean, case=False, na=False
            )
        ]

    total_filtered = len(filtered_df)

    if filtered_df.empty:
        st.warning("Nenhum produto encontrado com os filtros aplicados.")
        return

    st.info(f"**Produtos filtrados:** {total_filtered}")

    # --- Paginação ---
    st.markdown("---")
    st.markdown("### 📝 Digite seus Pedidos")

    # Inicializa estado de paginação e pedidos salvos
    if "fornecedor_page" not in st.session_state:
        st.session_state.fornecedor_page = 0
    if "fornecedor_pedidos_salvos" not in st.session_state:
        st.session_state.fornecedor_pedidos_salvos = {}

    current_page = st.session_state.fornecedor_page
    total_pages = (total_filtered - 1) // ITEMS_PER_PAGE + 1

    start_idx = current_page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, total_filtered)

    page_df = filtered_df.iloc[start_idx:end_idx].copy()

    # Restaura pedidos já salvos nesta página
    page_df["Pedido (Cx)"] = 0
    for idx in page_df.index:
        key = f"{selected_loja}_{idx}"
        if key in st.session_state.fornecedor_pedidos_salvos:
            page_df.at[idx, "Pedido (Cx)"] = (
                st.session_state.fornecedor_pedidos_salvos[key]
            )

    st.write(f"**Página {current_page + 1} de {total_pages}**")
    st.write(
        f"Exibindo itens {start_idx + 1} a {end_idx} "
        f"de {total_filtered}"
    )

    # Editor de dados
    edited_df = st.data_editor(
        page_df,
        column_config={
            "codigo_interno": st.column_config.TextColumn(
                "Código Interno", disabled=True
            ),
            "descricao": st.column_config.TextColumn(
                "Produto", width="large", disabled=True
            ),
            "codigo_ean": st.column_config.TextColumn(
                "EAN", disabled=True
            ),
            "embalagem": st.column_config.NumberColumn(
                "Emb. (Un/Cx)", disabled=True, format="%d"
            ),
            "estoque_cd": st.column_config.NumberColumn(
                "Estoque CD (Cx)", disabled=True, format="%d"
            ),
            "origem": st.column_config.TextColumn(
                "Empresa", disabled=True
            ),
            "Pedido (Cx)": st.column_config.NumberColumn(
                "Pedido (Cx)", min_value=0, step=1
            ),
        },
        hide_index=True,
        use_container_width=True,
        key=f"forn_editor_{selected_loja}_{current_page}",
    )

    # --- Botões de Navegação e Salvamento ---
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])

    with col1:
        if current_page > 0:
            if st.button("⬅️ Página Anterior", use_container_width=True):
                # Salva pedidos atuais antes de navegar
                for idx in edited_df.index:
                    qtd = edited_df.at[idx, "Pedido (Cx)"]
                    if qtd > 0:
                        key = f"{selected_loja}_{idx}"
                        st.session_state.fornecedor_pedidos_salvos[
                            key
                        ] = qtd
                st.session_state.fornecedor_page -= 1
                st.rerun()

    with col2:
        if st.button(
            "💾 Salvar Página e Continuar",
            type="primary",
            use_container_width=True
        ):
            # Salva pedidos da página atual
            saved_count = 0
            for idx in edited_df.index:
                qtd = edited_df.at[idx, "Pedido (Cx)"]
                if qtd > 0:
                    key = f"{selected_loja}_{idx}"
                    st.session_state.fornecedor_pedidos_salvos[key] = qtd
                    saved_count += 1

            if saved_count > 0:
                st.success(f"✅ {saved_count} pedido(s) salvos!")
            else:
                st.info("Nenhum pedido digitado nesta página.")

            # Vai para próxima página se houver
            if current_page < total_pages - 1:
                st.session_state.fornecedor_page += 1
                st.rerun()

    with col3:
        if current_page < total_pages - 1:
            if st.button("Próxima Página ➡️", use_container_width=True):
                # Salva pedidos atuais antes de navegar
                for idx in edited_df.index:
                    qtd = edited_df.at[idx, "Pedido (Cx)"]
                    if qtd > 0:
                        key = f"{selected_loja}_{idx}"
                        st.session_state.fornecedor_pedidos_salvos[
                            key
                        ] = qtd
                st.session_state.fornecedor_page += 1
                st.rerun()

    # --- Botão Final de Envio (só na última página) ---
    if current_page == total_pages - 1:
        st.markdown("---")
        st.markdown("### 🚀 Finalização")

        total_pedidos_salvos = len(
            st.session_state.fornecedor_pedidos_salvos
        )
        st.info(
            f"**Total de itens com pedidos salvos:** "
            f"{total_pedidos_salvos}"
        )

        if total_pedidos_salvos > 0:
            # Seção de chamado/observação
            st.markdown("#### 📝 Observações do Pedido (Opcional)")
            motivo_pedido = st.text_area(
                "Descreva o motivo do pedido ou adicione observações:",
                placeholder=(
                    "Ex: Reposição de estoque, promoção especial, "
                    "novo lançamento..."
                ),
                key="motivo_pedido"
            )

            if st.button(
                "📤 Enviar Todos os Pedidos para Aprovação",
                type="primary",
                use_container_width=True
            ):
                # Salva pedidos da página atual primeiro
                for idx in edited_df.index:
                    qtd = edited_df.at[idx, "Pedido (Cx)"]
                    if qtd > 0:
                        key = f"{selected_loja}_{idx}"
                        st.session_state.fornecedor_pedidos_salvos[
                            key
                        ] = qtd

                # Reconstrói DataFrame completo de pedidos
                pedidos_finais = []
                for key, qtd in (
                    st.session_state.fornecedor_pedidos_salvos.items()
                ):
                    parts = key.split("_", 1)
                    if len(parts) != 2:
                        continue
                    loja, idx_str = parts
                    idx = int(idx_str)

                    if loja != selected_loja:
                        continue

                    # Busca dados do produto no filtered_df
                    if idx in filtered_df.index:
                        produto = filtered_df.loc[idx]
                        pedidos_finais.append({
                            "codigo_interno": produto["codigo_interno"],
                            "descricao": produto["descricao"],
                            "codigo_ean": produto["codigo_ean"],
                            "embalagem": produto["embalagem"],
                            f"loja_{loja}": qtd,
                            "total_cx": qtd,
                            "data_pedido": now_brazil(),
                            "usuario_pedido": username,
                            "status_aprovacao": "Pendente",
                            "status_item": "Ativo",
                        })

                if pedidos_finais:
                    pedidos_df = pd.DataFrame(pedidos_finais)

                    # Preenche outras lojas com 0
                    LISTA_LOJAS = [
                        "001", "002", "003", "004", "005", "006", "007", "008",
                        "011", "012", "013", "014", "016", "017", "018",
                        "F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09",
                        "F10", "F11", "M12", "M13", "ADM", "RH"
                    ]
                    for loja in LISTA_LOJAS:
                        col_name = f"loja_{loja}"
                        if col_name not in pedidos_df.columns:
                            pedidos_df[col_name] = 0

                    # Salva no banco
                    if save_fornecedor_pedidos(engine, pedidos_df):
                        # Cria chamado se houver observação
                        if motivo_pedido.strip():
                            assunto = (
                                f"Pedido Fornecedor - Loja {selected_loja}"
                            )
                            mensagem = (
                                f"Pedido enviado para aprovação.\n\n"
                                f"**Observações:**\n{motivo_pedido}\n\n"
                                f"**Loja:** {selected_loja}\n"
                                f"**Total de itens:** "
                                f"{len(pedidos_finais)}"
                            )
                            create_fornecedor_ticket(
                                engine,
                                username,
                                assunto,
                                mensagem
                            )

                        st.success(
                            f"✅ {len(pedidos_finais)} pedido(s) "
                            f"enviado(s) para aprovação!"
                        )
                        st.balloons()

                        # Limpa pedidos salvos
                        st.session_state.fornecedor_pedidos_salvos = {}
                        st.session_state.fornecedor_page = 0
                        st.rerun()
                    else:
                        st.error("Erro ao enviar pedidos. Tente novamente.")
                else:
                    st.warning("Nenhum pedido válido para enviar.")
        else:
            st.warning(
                "Digite pelo menos um pedido antes de enviar para aprovação."
            )

    # --- Resumo de Pedidos Salvos ---
    if st.session_state.fornecedor_pedidos_salvos:
        st.markdown("---")
        st.markdown("### 📊 Resumo de Pedidos Salvos")

        resumo_items = []
        for key, qtd in st.session_state.fornecedor_pedidos_salvos.items():
            parts = key.split("_", 1)
            if len(parts) != 2:
                continue
            loja, idx_str = parts
            idx = int(idx_str)

            if idx in filtered_df.index:
                produto = filtered_df.loc[idx]
                resumo_items.append({
                    "Loja": loja,
                    "Código": produto["codigo_interno"],
                    "Produto": produto["descricao"],
                    "Qtde (Cx)": qtd
                })

        if resumo_items:
            resumo_df = pd.DataFrame(resumo_items)
            st.dataframe(resumo_df, hide_index=True, use_container_width=True)

            if st.button("🗑️ Limpar Todos os Pedidos Salvos"):
                st.session_state.fornecedor_pedidos_salvos = {}
                st.session_state.fornecedor_page = 0
                st.success("Pedidos salvos limpos!")
                st.rerun()
