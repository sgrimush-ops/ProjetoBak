import io
import os
import pandas as pd
import streamlit as st
from sqlalchemy import inspect, text
from utils.cargos import is_user_consumo_cd
from utils.timezone import now_brazil
from page.admin_uploads import _salvar_em_todas_as_pastas


# --- Funções de Chamado ---


def create_new_ticket(engine, username, assunto, mensagem):
    """Cria um novo ticket e a primeira mensagem."""
    now = now_brazil()
    try:
        with engine.begin() as conn:
            query_ticket = text(
                """
                INSERT INTO contato_chamados (
                    usuario_username,
                    assunto,
                    data_criacao,
                    ultimo_update,
                    status
                )
                VALUES (:username, :assunto, :now, :now, 'Aguardando Retorno')
                RETURNING id;
            """
            )
            result = conn.execute(
                query_ticket,
                {"username": username, "assunto": assunto, "now": now},
            )
            new_ticket_id = result.scalar_one()

            query_msg = text(
                """
                INSERT INTO contato_mensagens (
                    chamado_id, remetente_username, mensagem, data_envio
                )
                VALUES (:chamado_id, :username, :mensagem, :now)
            """
            )
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


# --- Funções de Carregamento e Gestão de Dados de Consumo ---


def _load_consumo_parquet():
    parquet_path = os.path.join("bdados", "consumo.parquet")
    if not os.path.exists(parquet_path):
        return None

    try:
        return pd.read_parquet(parquet_path)
    except Exception as e:
        st.error(f"Erro ao ler consumo.parquet local: {e}")
        return None


def _sync_consumo_table_to_parquet(engine, base_data_path=None):
    """Exporta a tabela consumo do banco para consumo.parquet e arquivos_sync."""
    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                text('SELECT codigo, "descricao consinco", embalagem FROM consumo ORDER BY codigo'),
                conn
            )
        if not df.empty:
            df["codigo"] = pd.to_numeric(df["codigo"], errors="coerce").fillna(0).astype(int)
            df["embalagem"] = pd.to_numeric(df["embalagem"], errors="coerce").fillna(1).astype(int)
            df["descricao consinco"] = df["descricao consinco"].fillna("").astype(str).str.strip()

            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            parquet_bytes = buffer.getvalue()

            _salvar_em_todas_as_pastas(parquet_bytes, "consumo.parquet", base_data_path=base_data_path)

            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO arquivos_sync (nome, conteudo, data_atualizacao)
                        VALUES ('consumo.parquet', :conteudo, NOW())
                        ON CONFLICT (nome) DO UPDATE 
                        SET conteudo = EXCLUDED.conteudo, data_atualizacao = NOW()
                    """),
                    {"conteudo": parquet_bytes}
                )
    except Exception as e:
        print(f"Erro ao sincronizar consumo.parquet: {e}")


def load_products_from_consumo_table(engine):
    """Carrega produtos exclusivamente da tabela consumo no banco."""
    inspector = inspect(engine)
    consumo_schema = None
    if inspector.has_table("consumo"):
        consumo_schema = None
    else:
        for schema in inspector.get_schema_names():
            if schema in ("information_schema", "pg_catalog"):
                continue
            if inspector.has_table("consumo", schema=schema):
                consumo_schema = schema
                break

    if consumo_schema is None and not inspector.has_table("consumo"):
        df = _load_consumo_parquet()
        if df is None:
            st.error(
                "Tabela `consumo` não encontrada no banco e "
                "consumo.parquet local inexistente. "
                "Cadastre os produtos ou carregue o arquivo em Admin Uploads."
            )
            return pd.DataFrame()
        st.warning(
            "Tabela `consumo` não encontrada no banco. "
            "Usando consumo.parquet local."
        )
    else:
        try:
            with engine.connect() as conn:
                if consumo_schema:
                    query = text(f'SELECT * FROM "{consumo_schema}".consumo')
                else:
                    query = text("SELECT * FROM consumo")
                df = pd.read_sql(query, conn)
        except Exception as e:
            df = _load_consumo_parquet()
            if df is not None:
                st.warning(
                    "Falha ao ler tabela consumo no banco. "
                    "Usando consumo.parquet local."
                )
            else:
                st.error(f"Erro ao carregar tabela consumo: {e}")
                return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    # Considera nomes padronizados do consumo
    required_columns = ["codigo", "descricao consinco", "embalagem"]
    for col in required_columns:
        if col not in df.columns:
            st.error(
                f"A tabela consumo precisa ter a coluna '{col}'. "
                "Verifique o arquivo consumo.parquet."
            )
            return pd.DataFrame()

    result = pd.DataFrame()
    result["codigo"] = pd.to_numeric(df["codigo"], errors="coerce")
    result["descricao consinco"] = df["descricao consinco"].astype(str).str.strip()
    result["embalagem"] = pd.to_numeric(df["embalagem"], errors="coerce")

    result = result.dropna(subset=["codigo", "embalagem"])
    result["codigo"] = result["codigo"].astype(int)
    result["embalagem"] = result["embalagem"].astype(int)
    result = result.drop_duplicates(subset=["codigo"], keep="first")

    return result.sort_values(by="codigo").reset_index(drop=True)


def add_consumo_product(engine, codigo: int, descricao: str, embalagem: int, base_data_path: str = None) -> tuple[bool, str]:
    """Adiciona um novo produto à tabela consumo no banco de dados com validação rigorosa."""
    try:
        cod_int = int(codigo)
        if cod_int <= 0:
            return False, "O código do produto deve ser um número inteiro positivo maior que zero."
    except (ValueError, TypeError):
        return False, "Código inválido. Digite um número inteiro."

    desc_clean = str(descricao or "").strip().upper()
    if not desc_clean:
        return False, "A descrição do produto é obrigatória."

    try:
        emb_int = int(embalagem)
        if emb_int <= 0:
            return False, "A embalagem deve ser um número inteiro maior ou igual a 1."
    except (ValueError, TypeError):
        return False, "Embalagem inválida. Digite um número inteiro."

    # Validação de código repetido
    try:
        with engine.connect() as conn:
            query_check = text('SELECT codigo, "descricao consinco" FROM consumo WHERE codigo = :cod')
            row = conn.execute(query_check, {"cod": cod_int}).fetchone()
            if row:
                return False, f"❌ O código {cod_int} já está cadastrado no banco de dados com a descrição '{row[1]}'. Não são permitidos códigos repetidos!"

        with engine.begin() as conn:
            query_ins = text('INSERT INTO consumo (codigo, "descricao consinco", embalagem) VALUES (:cod, :desc, :emb)')
            conn.execute(query_ins, {"cod": cod_int, "desc": desc_clean, "emb": emb_int})

        _sync_consumo_table_to_parquet(engine, base_data_path)
        st.cache_data.clear()
        return True, f"✅ Produto {cod_int} - {desc_clean} (Emb: {emb_int}) cadastrado com sucesso!"
    except Exception as e:
        return False, f"Erro ao inserir produto no banco de dados: {e}"


def delete_consumo_products(engine, codigos: list[int], base_data_path: str = None) -> tuple[bool, str]:
    """Exclui produtos da tabela consumo no banco de dados."""
    if not codigos:
        return False, "Nenhum produto selecionado para exclusão."

    cods_int = []
    for c in codigos:
        try:
            cods_int.append(int(c))
        except (ValueError, TypeError):
            pass

    if not cods_int:
        return False, "Códigos inválidos para exclusão."

    try:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM consumo WHERE codigo = ANY(:cods)"),
                {"cods": cods_int}
            )

        _sync_consumo_table_to_parquet(engine, base_data_path)
        st.cache_data.clear()
        return True, f"🗑️ {len(cods_int)} produto(s) excluído(s) com sucesso do banco de dados!"
    except Exception as e:
        return False, f"Erro ao excluir produto(s): {e}"


def search_product(df_produtos, search_term, search_type="codigo"):
    """Busca produto por código ou descrição."""
    if df_produtos.empty:
        return pd.DataFrame()

    if search_type == "codigo":
        try:
            cod = int(search_term)
            return df_produtos[df_produtos["codigo"] == cod]
        except ValueError:
            return pd.DataFrame()

    if search_type == "descricao" or search_type == "descricao_consinco":
        mask = df_produtos["descricao consinco"].str.contains(
            search_term, case=False, na=False
        )
        return df_produtos[mask]

    return pd.DataFrame()


def save_pedido_consolidado(engine, df_pedido):
    """Salva pedido no banco de dados."""
    try:
        with engine.begin() as conn:
            df_pedido.to_sql(
                "pedidos_consolidados",
                conn,
                if_exists="append",
                index=False,
                method="multi",
            )
        return True
    except Exception as e:
        st.error(f"Erro ao salvar pedido: {e}")
        return False


def get_last_item_order_30d(engine, username, codigo_produto):
    """Busca o último pedido do item nos últimos 30 dias."""
    try:
        query = text(
            """
            SELECT *
            FROM pedidos_consolidados
            WHERE usuario_pedido = :username
              AND codigo_interno = :codigo
              AND data_pedido >= (
                  CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'
              ) - INTERVAL '30 days'
            ORDER BY data_pedido DESC
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(
                query,
                conn,
                params={
                    "username": username,
                    "codigo": str(codigo_produto),
                },
            )

        if df.empty:
            return None

        return df.iloc[0].to_dict()
    except Exception:
        return None


def get_orders_history_30d(engine, username):
    """Retorna histórico de pedidos do usuário dos últimos 30 dias."""
    query = text(
        """
        SELECT
            id,
            codigo_interno,
            descricao,
            embseparacao,
            total_cx,
            TO_CHAR(data_pedido, 'DD/MM/YYYY HH24:MI') AS data_pedido,
            status_aprovacao
        FROM pedidos_consolidados
        WHERE usuario_pedido = :username
          AND COALESCE(origem_pedido, 'Pedido por Código (CD)') = 'Pedido de Consumo'
          AND data_pedido >= (
              CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'
          ) - INTERVAL '30 days'
        ORDER BY data_pedido DESC
        LIMIT 200
        """
    )

    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"username": username})


# --- Exportação de Dados para Excel e PDF ---


def gerar_excel_itens_consumo(df_itens: pd.DataFrame) -> bytes:
    """Gera arquivo Excel formatado com a lista de itens de consumo."""
    df_export = df_itens.copy()
    col_map = {
        "codigo": "Código Consinco",
        "descricao consinco": "Descrição do Produto",
        "embalagem": "Embalagem (Un/Cx)",
    }
    cols_exist = [c for c in ["codigo", "descricao consinco", "embalagem"] if c in df_export.columns]
    df_export = df_export[cols_exist].rename(columns=col_map)
    df_export.sort_values(by="Código Consinco", inplace=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_export.to_excel(writer, sheet_name="Itens_Consumo", index=False)
        worksheet = writer.sheets["Itens_Consumo"]

        # Ajuste automático da largura das colunas
        for col in worksheet.columns:
            max_len = 0
            col_letter = col[0].column_letter
            for cell in col:
                val_str = str(cell.value or "")
                if len(val_str) > max_len:
                    max_len = len(val_str)
            worksheet.column_dimensions[col_letter].width = max(max_len + 3, 12)

    return output.getvalue()


def gerar_pdf_itens_consumo(df_itens: pd.DataFrame) -> bytes:
    """Gera arquivo PDF formatado com checklist/catálogo de itens de consumo."""
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors

    df_export = df_itens.copy().sort_values(by="codigo").reset_index(drop=True)

    pdf_out = io.BytesIO()
    doc = SimpleDocTemplate(
        pdf_out,
        pagesize=A4,
        leftMargin=25,
        rightMargin=25,
        topMargin=25,
        bottomMargin=25,
    )
    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "DocTitle",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=13,
        leading=16,
        textColor=colors.HexColor("#1E3A8A"),
        alignment=1,
    )
    subtitle_style = ParagraphStyle(
        "DocSubTitle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8,
        leading=11,
        textColor=colors.HexColor("#64748B"),
        alignment=1,
    )
    cell_style = ParagraphStyle(
        "CellText",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#1E293B"),
    )
    cell_header = ParagraphStyle(
        "CellHeader",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=8,
        leading=10,
        textColor=colors.white,
    )

    elements = []
    elements.append(Paragraph("<b>CATÁLOGO DE ITENS DE CONSUMO - CD BAKLIZI</b>", title_style))
    elements.append(Spacer(1, 3))
    now_str = now_brazil().strftime("%d/%m/%Y às %H:%M")
    elements.append(Paragraph(f"Emissão: {now_str} | Total de Itens: {len(df_export)}", subtitle_style))
    elements.append(Spacer(1, 10))

    table_data = [[
        Paragraph("<b>Código</b>", cell_header),
        Paragraph("<b>Descrição do Produto</b>", cell_header),
        Paragraph("<b>Embalagem</b>", cell_header),
    ]]

    for _, row in df_export.iterrows():
        cod_val = str(row.get("codigo", ""))
        desc_val = str(row.get("descricao consinco", "")).strip()
        emb_val = str(row.get("embalagem", "1"))
        table_data.append([
            Paragraph(cod_val, cell_style),
            Paragraph(desc_val, cell_style),
            Paragraph(f"{emb_val} un/cx", cell_style),
        ])

    t = Table(table_data, colWidths=[70, 390, 85], repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1E3A8A")),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E2E8F0")),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    elements.append(t)
    doc.build(elements)
    return pdf_out.getvalue()


# --- Renderização da Gestão de Itens no Banco (Visualização Geral + Ações Admin/Consumo CD) ---


def _render_gestao_itens_consumo(engine, base_data_path, can_manage: bool = False):
    """Interface para visualização de itens com downloads (todos) e inclusão/exclusão (Admin / Consumo CD)."""
    st.markdown("### 📋 Catálogo de Itens de Consumo (Banco de Dados)")

    if can_manage:
        st.caption(
            "Você possui permissão para **incluir e excluir itens** no banco de dados. "
            "Produtos cadastrados ficam disponíveis no mesmo instante para pedidos de todas as lojas."
        )
    else:
        st.info(
            "ℹ️ **Modo de Consulta:** Você pode visualizar a relação de itens cadastrados no CD e realizar o "
            "download da lista completa em **Excel** ou **PDF** para conferência."
        )

    df_atual = load_products_from_consumo_table(engine)
    total_produtos = len(df_atual)

    # --- Formulário de Inclusão (Apenas Admins e Consumo CD) ---
    if can_manage:
        with st.expander("➕ **Incluir Novo Produto no Banco de Dados**", expanded=False):
            st.markdown(
                "Informe o **código do produto**, a **descrição** e a **embalagem** (campos obrigatórios). "
                "O sistema não aceita códigos repetidos."
            )

            with st.form("form_novo_produto_consumo", clear_on_submit=True):
                col_c1, col_c2, col_c3 = st.columns([1, 2, 1])
                with col_c1:
                    novo_codigo = st.number_input(
                        "Código do Produto (Consinco) *",
                        min_value=1,
                        step=1,
                        value=None,
                        placeholder="Ex: 35394",
                        help="Código numérico único do produto."
                    )
                with col_c2:
                    nova_descricao = st.text_input(
                        "Descrição do Produto *",
                        placeholder="Ex: CANETA COMPACTOR TOP 2000 UN",
                        help="Descrição conforme cadastrada no ERP Consinco."
                    )
                with col_c3:
                    nova_embalagem = st.number_input(
                        "Tipo de Embalagem (Un/Cx) *",
                        min_value=1,
                        step=1,
                        value=1,
                        help="Quantidade de unidades por embalagem/caixa."
                    )

                btn_cadastrar = st.form_submit_button(
                    "💾 Cadastrar Produto no Banco de Dados",
                    type="primary",
                    use_container_width=True
                )

                if btn_cadastrar:
                    if not novo_codigo or int(novo_codigo) <= 0:
                        st.error("❌ Por favor, informe um código de produto válido maior que 0.")
                    elif not nova_descricao or not str(nova_descricao).strip():
                        st.error("❌ Por favor, informe a descrição do produto.")
                    elif not nova_embalagem or int(nova_embalagem) <= 0:
                        st.error("❌ Por favor, informe a quantidade da embalagem (mínimo 1).")
                    else:
                        sucesso, msg = add_consumo_product(
                            engine=engine,
                            codigo=int(novo_codigo),
                            descricao=nova_descricao,
                            embalagem=int(nova_embalagem),
                            base_data_path=base_data_path
                        )
                        if sucesso:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)

        st.markdown("---")

    # --- Barra de Ações: Métricas & Downloads para Todos os Usuários ---
    col_m1, col_m2, col_m3 = st.columns([2, 1, 1])
    with col_m1:
        st.metric("Total de Itens Cadastrados no Banco", total_produtos)

    if not df_atual.empty:
        data_hoje = now_brazil().strftime("%d_%m_%Y")
        excel_bytes = gerar_excel_itens_consumo(df_atual)
        pdf_bytes = gerar_pdf_itens_consumo(df_atual)

        with col_m2:
            st.write("")
            st.download_button(
                label="📥 Baixar Excel (.xlsx)",
                data=excel_bytes,
                file_name=f"itens_consumo_cd_{data_hoje}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

        with col_m3:
            st.write("")
            st.download_button(
                label="📄 Baixar PDF (.pdf)",
                data=pdf_bytes,
                file_name=f"itens_consumo_cd_{data_hoje}.pdf",
                mime="application/pdf",
                use_container_width=True,
            )

    # --- Filtros de Busca ---
    if df_atual.empty:
        st.info("Nenhum produto cadastrado na base de consumo.")
        return

    col_b1, col_b2 = st.columns(2)
    with col_b1:
        busca_cod = st.text_input("Filtrar por Código:", key="gestao_busca_cod")
    with col_b2:
        busca_desc = st.text_input("Filtrar por Descrição:", key="gestao_busca_desc")

    df_view = df_atual.copy()
    if busca_cod:
        df_view = df_view[
            df_view["codigo"].astype(str).str.contains(busca_cod.strip(), case=False, na=False)
        ]
    if busca_desc:
        df_view = df_view[
            df_view["descricao consinco"].astype(str).str.contains(busca_desc.strip(), case=False, na=False)
        ]

    st.caption(f"Mostrando **{len(df_view)} de {total_produtos}** produtos cadastrados.")

    df_view_edit = df_view.copy().sort_values(by="codigo").reset_index(drop=True)

    if can_manage:
        df_view_edit["Excluir"] = False
        df_resultado_edit = st.data_editor(
            df_view_edit,
            column_config={
                "codigo": st.column_config.NumberColumn(
                    "Código Consinco", disabled=True, format="%d"
                ),
                "descricao consinco": st.column_config.TextColumn(
                    "Descrição Consinco", disabled=True, width="large"
                ),
                "embalagem": st.column_config.NumberColumn(
                    "Emb. (Un/Cx)", disabled=True, format="%d"
                ),
                "Excluir": st.column_config.CheckboxColumn(
                    "Excluir?", default=False
                ),
            },
            hide_index=True,
            use_container_width=True,
            key="gestao_consumo_editor",
        )

        if st.button("🗑️ Excluir Itens Selecionados", type="primary", key="btn_del_consumo"):
            codigos_para_excluir = df_resultado_edit[df_resultado_edit["Excluir"]]["codigo"].tolist()
            if codigos_para_excluir:
                ok, msg = delete_consumo_products(engine, codigos_para_excluir, base_data_path)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)
            else:
                st.warning("Selecione ao menos um produto marcando a caixa 'Excluir?' antes de clicar no botão.")
    else:
        # Modo Somente Leitura para usuários comuns
        st.dataframe(
            df_view_edit,
            column_config={
                "codigo": st.column_config.NumberColumn(
                    "Código Consinco", format="%d"
                ),
                "descricao consinco": st.column_config.TextColumn(
                    "Descrição Consinco", width="large"
                ),
                "embalagem": st.column_config.NumberColumn(
                    "Emb. (Un/Cx)", format="%d"
                ),
            },
            hide_index=True,
            use_container_width=True,
        )


# --- Formulário Principal de Pedido de Consumo ---


def _render_pedido_consumo_form(engine, base_data_path):
    """Renderiza a interface de pesquisa e digitação de pedidos de consumo para as lojas."""
    from app import LISTA_LOJAS
    lista_lojas_global = LISTA_LOJAS
    lojas_autorizadas = st.session_state.get("lojas_acesso", LISTA_LOJAS)
    _ = base_data_path

    if "consumo_searched_item" not in st.session_state:
        st.session_state.consumo_searched_item = None
    if "consumo_pedido_details" not in st.session_state:
        st.session_state.consumo_pedido_details = {}
    if "consumo_search_results" not in st.session_state:
        st.session_state.consumo_search_results = None

    df_produtos = load_products_from_consumo_table(engine)

    if df_produtos.empty:
        st.error("❌ Não foi possível carregar a base de consumo!")
        st.info("Cadastre os produtos na aba de Catálogo ou realize o upload em Admin Uploads.")
        return

    total_produtos = len(df_produtos)
    st.metric("Total de Produtos Disponíveis para Pedido", total_produtos)

    with st.form("consumo_search_form"):
        search_type = st.radio(
            "Tipo de busca:",
            ["Por Código", "Por Descrição"],
            horizontal=True,
        )

        if search_type == "Por Código":
            search_term = st.text_input(
                "Digite o código Consinco:",
                placeholder="Ex: 10480",
                max_chars=10,
            )
        else:
            search_term = st.text_input(
                "Digite parte da descrição do produto:",
                placeholder="Ex: CANETA",
            )

        submitted = st.form_submit_button("🔍 Buscar")

        if submitted and search_term:
            st.session_state.consumo_searched_item = None
            st.session_state.consumo_pedido_details = {}

            if search_type == "Por Código":
                search_mode = "codigo"
            else:
                search_mode = "descricao"

            results = search_product(df_produtos, search_term, search_mode)

            if not results.empty:
                if len(results) == 1:
                    st.session_state.consumo_searched_item = (
                        results.iloc[0].to_dict()
                    )
                    st.session_state.consumo_search_results = None
                else:
                    st.session_state.consumo_search_results = results
                    st.session_state.consumo_searched_item = None
            else:
                st.warning("❌ Nenhum produto encontrado com esse critério.")
                st.session_state.consumo_search_results = None

    if (
        st.session_state.consumo_search_results is not None
        and not st.session_state.consumo_search_results.empty
    ):
        st.markdown("### 📋 Resultados da Busca")
        st.info(
            f"Encontrados {len(st.session_state.consumo_search_results)} produtos. Selecione um:"
        )

        results_display = st.session_state.consumo_search_results.copy()
        results_display = results_display[[
            "codigo",
            "descricao consinco",
            "embalagem",
        ]]
        results_display.columns = [
            "Código",
            "Descrição Consinco",
            "Embalagem",
        ]

        selected_idx = st.selectbox(
            "Escolha o produto:",
            range(len(results_display)),
            format_func=lambda i: (
                f"{results_display.iloc[i]['Código']} - "
                f"{results_display.iloc[i]['Descrição Consinco']}"
            ),
            key="consumo_select_result",
        )

        if st.button("✅ Confirmar Seleção", key="consumo_confirm_select"):
            st.session_state.consumo_searched_item = (
                st.session_state.consumo_search_results.iloc[selected_idx].to_dict()
            )
            st.session_state.consumo_search_results = None
            st.rerun()

    if st.session_state.consumo_searched_item:
        item = st.session_state.consumo_searched_item
        codigo_produto = int(item["codigo"])
        username = st.session_state.get("username", "unknown")

        last_order = get_last_item_order_30d(engine, username, codigo_produto)
        default_qtd_por_loja = {}
        if last_order:
            st.info(
                "Último pedido deste item encontrado nos últimos 30 dias. "
                "As quantidades foram pré-preenchidas."
            )
            for key, value in last_order.items():
                if key.startswith("loja_"):
                    try:
                        default_qtd_por_loja[key.replace("loja_", "")] = int(
                            value or 0
                        )
                    except Exception:
                        default_qtd_por_loja[key.replace("loja_", "")] = 0
        else:
            st.warning(
                "Primeiro pedido deste item para você nos últimos 30 dias."
            )

        st.markdown("---")
        st.subheader(f"Produto Selecionado: {item['descricao consinco']}")

        col1, col2, col3 = st.columns(3)
        col1.metric("Código", codigo_produto)
        col2.metric("Descrição Consinco", item["descricao consinco"])
        col3.metric("Emb. (Un/Cx)", int(item["embalagem"]))

        st.markdown("---")
        st.subheader("Digite as quantidades por loja (em caixas):")
        st.info(
            "Você pode alterar as quantidades a qualquer momento antes de "
            "clicar em `Enviar para Aprovação`."
        )
        st.caption(
            "Somente lojas autorizadas para o seu usuário ficam disponíveis "
            "para pedido."
        )

        with st.form("consumo_pedido_form"):
            pedido_inputs = {}

            cols_per_row = 3
            cols = st.columns(cols_per_row)

            for idx, loja in enumerate(lojas_autorizadas):
                col_idx = idx % cols_per_row
                with cols[col_idx]:
                    pedido_inputs[loja] = st.number_input(
                        f"Loja {loja}",
                        min_value=0,
                        value=default_qtd_por_loja.get(loja, 0),
                        step=1,
                        key=f"consumo_loja_{loja}_{codigo_produto}",
                    )

            st.markdown("---")
            total_cx = sum(pedido_inputs.values())
            total_un = total_cx * int(item["embalagem"])

            col_total1, col_total2 = st.columns(2)
            col_total1.metric("Total de Caixas", total_cx)
            col_total2.metric("Total de Unidades", total_un)

            submitted_pedido = st.form_submit_button(
                "📤 Enviar para Aprovação",
                type="primary",
            )

            if submitted_pedido:
                if total_cx > 0:
                    st.session_state.consumo_pedido_details = {
                        "pedido_inputs": pedido_inputs,
                        "total_cx": total_cx,
                        "codigo_produto": codigo_produto,
                        "item": item,
                        "confirmar_pedido": True,
                    }
                    st.rerun()
                else:
                    st.warning(
                        "Nenhuma quantidade foi digitada. "
                        "O pedido não foi enviado."
                    )

        if st.session_state.consumo_pedido_details.get(
            "confirmar_pedido", False
        ):
            pedido_inputs = st.session_state.consumo_pedido_details[
                "pedido_inputs"
            ]
            total_cx = st.session_state.consumo_pedido_details["total_cx"]
            codigo_produto = st.session_state.consumo_pedido_details[
                "codigo_produto"
            ]
            item = st.session_state.consumo_pedido_details["item"]

            pedido_data = {
                "codigo_interno": [codigo_produto],
                "descricao": [item["descricao consinco"]],
                "codigo_ean": [""],
                "origem_pedido": ["Pedido de Consumo"],
                "embseparacao": [int(item["embalagem"])],
                "data_pedido": [now_brazil()],
                "usuario_pedido": [
                    st.session_state.get("username", "unknown")
                ],
                "status_item": ["Pendente"],
                "status_aprovacao": ["Pendente"],
                "total_cx": [total_cx],
            }

            lojas_nao_autorizadas = set(lista_lojas_global) - set(
                lojas_autorizadas
            )
            for loja in lojas_nao_autorizadas:
                pedido_inputs[loja] = 0

            for loja in lista_lojas_global:
                pedido_data[f"loja_{str(loja).lower()}"] = [
                    pedido_inputs.get(loja, 0)
                ]

            df_to_save = pd.DataFrame(pedido_data)

            if save_pedido_consolidado(engine, df_to_save):
                st.success("✅ Pedido enviado com sucesso para aprovação!")
                st.session_state.consumo_searched_item = None
                st.session_state.consumo_pedido_details = {}
                st.session_state.consumo_search_results = None
                st.rerun()

    st.markdown("---")
    st.subheader("📋 Histórico de Pedidos (Últimos 30 dias)")

    username = st.session_state.get("username", "unknown")
    try:
        df_historico = get_orders_history_30d(engine, username)

        if not df_historico.empty:
            st.info(
                f"Você tem {len(df_historico)} pedido(s) registrados "
                "nos últimos 30 dias."
            )

            st.dataframe(
                df_historico,
                column_config={
                    "id": None,
                    "codigo_interno": st.column_config.TextColumn(
                        "Código Consinco"
                    ),
                    "descricao": st.column_config.TextColumn(
                        "Produto", width="large"
                    ),
                    "embseparacao": st.column_config.NumberColumn(
                        "Emb. (Un/Cx)", format="%d"
                    ),
                    "total_cx": st.column_config.NumberColumn(
                        "Total CX", format="%d"
                    ),
                    "data_pedido": st.column_config.TextColumn("Data/Hora"),
                    "status_aprovacao": st.column_config.TextColumn(
                        "Status"
                    ),
                },
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("Você não tem histórico de pedidos nos últimos 30 dias.")
    except Exception as e:
        st.error(f"Erro ao buscar histórico de pedidos: {e}")

    st.markdown("---")
    with st.expander(
        "❔ Precisa de ajuda ou quer fazer uma observação? Abra um chamado."
    ):
        with st.form("chamado_form_consumo", clear_on_submit=True):
            mensagem = st.text_area(
                "Digite sua mensagem para o administrador:"
            )
            if st.form_submit_button("Enviar Chamado"):
                if mensagem:
                    username = st.session_state.get("username", "unknown")
                    assunto = "Chamado via Tela de Pedido de Consumo"
                    success, message = create_new_ticket(
                        engine, username, assunto, mensagem
                    )
                    if success:
                        st.success(
                            "Chamado enviado com sucesso! "
                            "Você pode acompanhar na tela de Contato."
                        )
                    else:
                        st.error(
                            "Não foi possível enviar o chamado: "
                            f"{message}"
                        )
                else:
                    st.warning(
                        "Por favor, digite uma mensagem antes de enviar."
                    )


# --- Página Principal ---


def show_pedido_consumo_page(engine, base_data_path):
    """Página de pedido de consumo com abas visíveis para todos os usuários."""
    st.title("📦 Pedido de Consumo")
    st.markdown("Sistema de pedidos alimentado pela tabela `consumo`.")

    is_admin = str(st.session_state.get("role", "")).strip().lower() == "admin"
    is_consumo_cd = is_user_consumo_cd(engine)
    can_manage_items = is_admin or is_consumo_cd

    tab_pedidos, tab_catalogo = st.tabs([
        "📦 Fazer Pedido de Consumo",
        "📋 Catálogo de Itens (Banco de Dados)"
    ])
    with tab_pedidos:
        _render_pedido_consumo_form(engine, base_data_path)
    with tab_catalogo:
        _render_gestao_itens_consumo(engine, base_data_path, can_manage=can_manage_items)

