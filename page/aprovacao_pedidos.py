import streamlit as st
import pandas as pd
from sqlalchemy import text
import io
from datetime import datetime, timedelta, date

# --- Configurações ---
LISTA_LOJAS = ["001", "002", "003", "004", "005", "006",
               "007", "008", "011", "012", "013", "014", "017", "018"]
COLUNAS_LOJAS_PEDIDO = [f"loja_{loja}" for loja in LISTA_LOJAS]


# ===========================================================
#   FUNÇÕES DE FORMATAÇÃO E CONSULTA
# ===========================================================

def formatar_tipos_df(df: pd.DataFrame) -> pd.DataFrame:
    """Formata tipos de dados e corrige valores numéricos."""
    int_cols_with_zero_fallback = COLUNAS_LOJAS_PEDIDO + ['total_cx']
    for col in int_cols_with_zero_fallback:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col], errors='coerce').fillna(0).astype(int)

    if 'embseparacao' in df.columns:
        df['embseparacao'] = pd.to_numeric(
            df['embseparacao'], errors='coerce').fillna(0).astype(int)

    # Garante que o código seja numérico para cruzamento com ofertas
    if 'codigo' in df.columns:
        df['codigo'] = pd.to_numeric(df['codigo'], errors='coerce').fillna(0).astype(int)

    return df

def get_offers_data(engine):
    """Busca ofertas ativas ou futuras (ignora passadas)."""
    today = date.today()
    query = text("""
        SELECT codigo, data_inicio, data_final
        FROM ofertas
        WHERE data_final >= :today
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={"today": today})
            # Remove duplicatas (caso haja) mantendo a última vigência
            df = df.drop_duplicates(subset=['codigo'], keep='last')
        return df
    except Exception:
        return pd.DataFrame(columns=['codigo', 'data_inicio', 'data_final'])

def get_pedidos_para_aprovacao(engine, date_start, date_end, only_pending: bool) -> pd.DataFrame:
    """Busca pedidos para a grade de aprovação, com filtros de data e status."""
    try:
        start_str = datetime.combine(
            date_start, datetime.min.time()).strftime('%Y-%m-%d %H:%M:%S')
        end_str = datetime.combine(
            date_end, datetime.max.time()).strftime('%Y-%m-%d %H:%M:%S')
        lojas_sql = ", ".join(COLUNAS_LOJAS_PEDIDO)

        query = text(f"""
            SELECT 
                id AS id_pedido, 
                TO_CHAR(data_pedido, 'DD/MM/YYYY HH24:MI') AS data_pedido_str, 
                usuario_pedido, 
                codigo, 
                produto, 
                embseparacao,
                {lojas_sql},
                total_cx,
                status_item,
                status_aprovacao
            FROM pedidos_consolidados
            WHERE data_pedido BETWEEN :start_str AND :end_str
        """)
        
        params = {"start_str": start_str, "end_str": end_str}

        if only_pending:
            query = text(str(query) + " AND status_aprovacao = 'Pendente'")
        
        query = text(str(query) + " ORDER BY data_pedido ASC")

        df_pedidos = pd.read_sql_query(query, con=engine, params=params)
        df_pedidos = formatar_tipos_df(df_pedidos)

        # --- MUDANÇA: Cruzar com Ofertas ---
        if not df_pedidos.empty:
            df_ofertas = get_offers_data(engine)
            if not df_ofertas.empty:
                # Merge (Left Join) para trazer info da oferta
                df_pedidos = pd.merge(df_pedidos, df_ofertas, on='codigo', how='left')
                
                # Formata as datas de oferta para string (DD/MM/YYYY)
                df_pedidos['inicio_oferta'] = pd.to_datetime(df_pedidos['data_inicio']).dt.strftime('%d/%m/%Y').fillna('-')
                df_pedidos['fim_oferta'] = pd.to_datetime(df_pedidos['data_final']).dt.strftime('%d/%m/%Y').fillna('-')
            else:
                df_pedidos['inicio_oferta'] = '-'
                df_pedidos['fim_oferta'] = '-'
        
        return df_pedidos

    except Exception as e:
        st.error(f"Erro ao buscar pedidos para aprovação: {e}")
        return pd.DataFrame()


def get_pedidos_aprovados_download(engine) -> pd.DataFrame:
    """Busca TODOS os pedidos 'Aprovados' para o download."""
    try:
        lojas_sql = ", ".join(COLUNAS_LOJAS_PEDIDO)
        
        query = text(f"""
            SELECT 
                id AS id_pedido, 
                TO_CHAR(data_pedido, 'DD/MM/YYYY HH24:MI') AS data_pedido_str, 
                usuario_pedido, 
                codigo, 
                produto, 
                embseparacao,
                {lojas_sql},
                total_cx,
                status_item
            FROM pedidos_consolidados
            WHERE status_aprovacao = 'Aprovado' 
            ORDER BY data_pedido ASC
        """)
        df = pd.read_sql_query(query, con=engine)
        df = formatar_tipos_df(df)
        return df
    except Exception as e:
        st.error(f"Erro ao buscar pedidos aprovados: {e}")
        return pd.DataFrame()


# ===========================================================
#   FUNÇÕES DE ATUALIZAÇÃO
# ===========================================================

def update_pedidos_aprovados(engine, df_editado_selecionado):
    """Atualiza o banco com quantidades editadas e aprova os itens."""
    try:
        data_aprovacao_dt = datetime.now()
        
        set_lojas_sql = ", ".join(
            [f"{col} = :{col}" for col in COLUNAS_LOJAS_PEDIDO])

        query = text(f"""
            UPDATE pedidos_consolidados
            SET 
                status_aprovacao = 'Aprovado',
                data_aprovacao = :data_aprovacao,
                total_cx = :total_cx,
                {set_lojas_sql}
            WHERE id = :id_pedido
        """)

        updates_list = []
        for _, row in df_editado_selecionado.iterrows():
            novas_lojas_vals = {col: int(pd.to_numeric(
                row[col], errors='coerce', downcast='integer')) for col in COLUNAS_LOJAS_PEDIDO}
            novo_total_cx = sum(novas_lojas_vals.values())
            
            params = {
                "data_aprovacao": data_aprovacao_dt,
                "total_cx": novo_total_cx,
                "id_pedido": row['id_pedido'],
                **novas_lojas_vals
            }
            updates_list.append(params)

        with engine.begin() as conn:
            conn.execute(query, updates_list)
            
        return True, f"{len(updates_list)} itens foram aprovados com sucesso."
    
    except Exception as e:
        return False, f"Erro ao atualizar o banco de dados: {e}"


def rejeitar_pedidos(engine, ids_pedidos: list):
    """Atualiza o status de uma lista de pedidos para 'Rejeitado'."""
    try:
        data_aprovacao_dt = datetime.now()

        query = text("""
            UPDATE pedidos_consolidados
            SET 
                status_aprovacao = 'Rejeitado',
                data_aprovacao = :data_aprovacao
            WHERE id IN :ids_list
        """)
        
        params = {
            "data_aprovacao": data_aprovacao_dt,
            "ids_list": tuple(ids_pedidos)
        }

        with engine.begin() as conn:
            conn.execute(query, params)
            
        return True, f"{len(ids_pedidos)} itens foram rejeitados."
    except Exception as e:
        return False, f"Erro ao rejeitar pedidos: {e}"


# ===========================================================
#   FUNÇÃO DE EXPORTAÇÃO
# ===========================================================

def to_excel(df: pd.DataFrame) -> bytes:
    """Exporta pedidos aprovados para Excel."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='PedidosAprovados')
        worksheet = writer.sheets['PedidosAprovados']
        for idx, col in enumerate(df):
            series = df[col]
            max_len = max(
                (series.astype(str).map(len).max() or len(str(series.name))),
                len(str(series.name))
            ) + 2
            worksheet.set_column(idx, idx, max_len)
    return output.getvalue()


# ===========================================================
#   PÁGINA PRINCIPAL
# ===========================================================

def show_aprovacao_page(engine, base_data_path):
    st.title("📋 Aprovação Detalhada de Pedidos")
    st.info(
        "Edite as quantidades, selecione os itens e clique em 'Aprovar' ou 'Rejeitar'.")
    st.subheader("1. Pedidos para Aprovação")
    st.markdown("#### Filtros de Visualização")

    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        data_inicio = st.date_input("Data Início", yesterday)
    with col2:
        data_fim = st.date_input("Data Fim", today)
    with col3:
        st.write("")
        ver_pendentes = st.checkbox(
            "Mostrar apenas Pedidos Pendentes", value=True)
    st.markdown("---")

    df_pedidos_filtrados = get_pedidos_para_aprovacao(
        engine, data_inicio, data_fim, ver_pendentes)

    if df_pedidos_filtrados.empty:
        st.success("Nenhum pedido encontrado para os filtros selecionados.")
    else:
        df_pedidos_filtrados['Selecionar'] = False

        # MUDANÇA: Adicionadas as novas colunas de oferta na lista de info
        colunas_info = [
            'Selecionar', 'id_pedido', 'data_pedido_str', 'usuario_pedido',
            'codigo', 'produto', 'inicio_oferta', 'fim_oferta', # <-- Novas Colunas
            'embseparacao', 'status_item', 'status_aprovacao'
        ]
        colunas_editaveis = COLUNAS_LOJAS_PEDIDO
        colunas_total = ['total_cx']

        colunas_existentes = [col for col in (
            colunas_info + colunas_editaveis + colunas_total) if col in df_pedidos_filtrados.columns]
        df_para_editar = df_pedidos_filtrados[colunas_existentes]

        column_config = {
            "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False),
            "id_pedido": None,
            "data_pedido_str": st.column_config.TextColumn("Data Pedido", disabled=True),
            "usuario_pedido": st.column_config.TextColumn("Usuário", disabled=True),
            "codigo": st.column_config.TextColumn("Código", disabled=True),
            "produto": st.column_config.TextColumn("Produto", width="medium", disabled=True),
            # MUDANÇA: Configuração das colunas de oferta
            "inicio_oferta": st.column_config.TextColumn("Início Oferta", disabled=True),
            "fim_oferta": st.column_config.TextColumn("Fim Oferta", disabled=True),
            "embseparacao": st.column_config.NumberColumn("Emb.", disabled=True, format="%d"),
            "status_item": st.column_config.TextColumn("Status Mix", disabled=True),
            "total_cx": st.column_config.NumberColumn("Total CX (Original)", disabled=True, format="%d"),
            "status_aprovacao": None
        }

        if not ver_pendentes:
            column_config["status_aprovacao"] = st.column_config.TextColumn(
                "Status", disabled=True)

        for col_loja in colunas_editaveis:
            column_config[col_loja] = st.column_config.NumberColumn(
                col_loja.replace("loja_", "Lj "), min_value=0, step=1, format="%d"
            )

        st.markdown("Edite as quantidades (em caixas) e selecione os itens:")
        df_editado = st.data_editor(
            df_para_editar,
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            num_rows="dynamic",
            key="editor_aprovacao"
        )
        st.markdown("---")

        df_selecionado = df_editado[df_editado['Selecionar'] == True]

        col_btn_1, col_btn_2, col_spacer = st.columns([1, 1, 3])

        with col_btn_1:
            if st.button("Aprovar Selecionados", type="primary"):
                if df_selecionado.empty:
                    st.warning("Nenhum item foi selecionado para aprovar.")
                else:
                    df_para_aprovar = df_selecionado[df_selecionado['status_aprovacao'] == 'Pendente']
                    if df_para_aprovar.empty:
                        st.warning(
                            "Nenhum item 'Pendente' foi selecionado para aprovar.")
                    else:
                        with st.spinner("Aprovando itens..."):
                            success, message = update_pedidos_aprovados(
                                engine, df_para_aprovar)
                            if success:
                                st.success(message)
                                st.rerun()
                            else:
                                st.error(message)

        with col_btn_2:
            if st.button("Rejeitar Selecionados"):
                if df_selecionado.empty:
                    st.warning("Nenhum item foi selecionado para rejeitar.")
                else:
                    df_para_rejeitar = df_selecionado[df_selecionado['status_aprovacao'] == 'Pendente']
                    ids_para_rejeitar = df_para_rejeitar['id_pedido'].tolist()
                    if not ids_para_rejeitar:
                        st.warning(
                            "Nenhum item 'Pendente' foi selecionado para rejeitar.")
                    else:
                        with st.spinner("Rejeitando itens..."):
                            success, message = rejeitar_pedidos(
                                engine, ids_para_rejeitar)
                            if success:
                                st.success(message)
                                st.rerun()
                            else:
                                st.error(message)

        if not ver_pendentes:
            st.info(
                "Para aprovar ou rejeitar pedidos, marque o filtro 'Mostrar apenas Pedidos Pendentes'.")

    st.markdown("---")

    st.subheader("2. Baixar Relatório de Pedidos Aprovados (Todos)")
    st.caption(
        "Esta seção baixa TODOS os pedidos aprovados, independente do filtro de data acima.")

    df_aprovados = get_pedidos_aprovados_download(engine)

    if df_aprovados.empty:
        st.info("Nenhum pedido aprovado encontrado para baixar.")
    else:
        st.markdown(
            f"Encontrados **{len(df_aprovados)}** itens aprovados no banco de dados.")
        excel_data = to_excel(df_aprovados)
        st.download_button(
            label="Baixar Aprovados (Excel)",
            data=excel_data,
            file_name=f"pedidos_aprovados_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )