import streamlit as st
import pandas as pd
import sqlite3
import io
from datetime import datetime, timedelta

# --- Configurações ---
PEDIDOS_DB_PATH = 'data/pedidos.db'
LISTA_LOJAS = ["001", "002", "003", "004", "005", "006",
               "007", "008", "011", "012", "013", "014", "017", "018"]
COLUNAS_LOJAS_PEDIDO = [f"loja_{loja}" for loja in LISTA_LOJAS]

# --- Funções Auxiliares ---

def formatar_tipos_df(df: pd.DataFrame) -> pd.DataFrame:
    """Função centralizada para formatar os tipos de dados do DataFrame."""
    int_cols_with_zero_fallback = COLUNAS_LOJAS_PEDIDO + ['total_cx']
    for col in int_cols_with_zero_fallback:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    if 'embalagem' in df.columns:
        df['embalagem'] = pd.to_numeric(df['embalagem'], errors='coerce').astype('Int64')
    return df

@st.cache_data(ttl=300) 
def get_pedidos_para_aprovacao(date_start, date_end, only_pending: bool) -> pd.DataFrame:
    """Busca pedidos para a grade de aprovação, com filtros de data e status."""
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        start_str = datetime.combine(date_start, datetime.min.time()).strftime('%Y-%m-%d %H:%M:%S')
        end_str = datetime.combine(date_end, datetime.max.time()).strftime('%Y-%m-%d %H:%M:%S')
        lojas_sql = ", ".join(COLUNAS_LOJAS_PEDIDO)
        
        query = f"""
            SELECT 
                id AS id_pedido, 
                STRFTIME('%d/%m/%Y %H:%M', data_pedido) AS data_pedido_str, 
                usuario_pedido, 
                codigo, 
                produto, 
                embalagem,
                {lojas_sql},
                total_cx,
                status_item,
                status_aprovacao
            FROM pedidos_consolidados
            WHERE data_pedido BETWEEN ? AND ?
        """
        params = [start_str, end_str]
        
        if only_pending:
            query += " AND status_aprovacao = 'Pendente'"
        query += " ORDER BY data_pedido ASC"
        
        df = pd.read_sql_query(query, conn, params=params)
        df = formatar_tipos_df(df)
        return df
    except Exception as e:
        st.error(f"Erro ao buscar pedidos para aprovação: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

@st.cache_data(ttl=300)
def get_pedidos_aprovados_download() -> pd.DataFrame:
    """Busca TODOS os pedidos 'Aprovados' para o download."""
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        lojas_sql = ", ".join(COLUNAS_LOJAS_PEDIDO)
        query = f"""
            SELECT 
                id AS id_pedido, 
                STRFTIME('%d/%m/%Y %H:%M', data_pedido) AS data_pedido_str, 
                usuario_pedido, 
                codigo, 
                produto, 
                embalagem,
                {lojas_sql},
                total_cx,
                status_item
            FROM pedidos_consolidados
            WHERE status_aprovacao = 'Aprovado' 
            ORDER BY data_pedido ASC
        """
        df = pd.read_sql_query(query, conn, params=('Aprovado',))
        df = formatar_tipos_df(df)
        return df
    except Exception as e:
        st.error(f"Erro ao buscar pedidos aprovados: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()


def update_pedidos_aprovados(df_editado_selecionado):
    """Atualiza o banco de dados com as quantidades editadas e aprova os itens."""
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        c = conn.cursor()
        data_aprovacao_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        set_lojas_sql = ", ".join([f"{col} = ?" for col in COLUNAS_LOJAS_PEDIDO])
        
        query = f"""
            UPDATE pedidos_consolidados
            SET 
                status_aprovacao = 'Aprovado',
                data_aprovacao = ?,
                total_cx = ?,
                {set_lojas_sql}
            WHERE id = ? 
        """
        
        updates_list = []
        for _, row in df_editado_selecionado.iterrows():
            id_pedido = row['id_pedido']
            novas_lojas_vals = [int(pd.to_numeric(row[col], errors='coerce', downcast='integer')) for col in COLUNAS_LOJAS_PEDIDO]
            novo_total_cx = sum(novas_lojas_vals)
            params = [data_aprovacao_str, novo_total_cx] + novas_lojas_vals + [id_pedido]
            updates_list.append(params)
            
        c.executemany(query, updates_list)
        conn.commit()
        return True, f"{len(updates_list)} itens foram aprovados com sucesso."
    except sqlite3.Error as e:
        if conn: conn.rollback()
        return False, f"Erro ao atualizar o banco de dados: {e}"
    finally:
        if conn: conn.close()

# --- NOVA FUNÇÃO PARA REJEITAR (DELETAR) ---
def rejeitar_pedidos(ids_pedidos: list):
    """Atualiza o status de uma lista de pedidos para 'Rejeitado'."""
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        c = conn.cursor()
        data_aprovacao_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Cria a string de placeholders (?, ?, ?)
        placeholders = ','.join(['?'] * len(ids_pedidos))
        
        query = f"""
            UPDATE pedidos_consolidados
            SET 
                status_aprovacao = 'Rejeitado',
                data_aprovacao = ?
            WHERE id IN ({placeholders}) 
        """
        
        params = [data_aprovacao_str] + ids_pedidos
        
        c.execute(query, params)
        conn.commit()
        return True, f"{len(ids_pedidos)} itens foram rejeitados."
    except sqlite3.Error as e:
        if conn: conn.rollback()
        return False, f"Erro ao rejeitar pedidos: {e}"
    finally:
        if conn: conn.close()
# --- FIM DA NOVA FUNÇÃO ---


def to_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='PedidosAprovados')
        for column in df:
            column_length = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            writer.sheets['PedidosAprovados'].set_column(col_idx, col_idx, column_length + 2)
    return output.getvalue()

# --- Lógica da Página ---
def show_aprovacao_page():
    st.title("📋 Aprovação Detalhada de Pedidos")
    st.info("Edite as quantidades, selecione os itens e clique em 'Aprovar' ou 'Rejeitar'.")
    st.subheader("1. Pedidos para Aprovação")
    st.markdown("#### Filtros de Visualização")
    
    today = datetime.now().date()
    yesterday = today - timedelta(days=1) 

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1: data_inicio = st.date_input("Data Início", yesterday)
    with col2: data_fim = st.date_input("Data Fim", today)
    with col3:
        st.write("") 
        st.write("") 
        ver_pendentes = st.checkbox("Mostrar apenas Pedidos Pendentes", value=True)
    st.markdown("---")
    
    df_pedidos_filtrados = get_pedidos_para_aprovacao(data_inicio, data_fim, ver_pendentes)

    if df_pedidos_filtrados.empty:
        st.success("Nenhum pedido encontrado para os filtros selecionados.")
    else:
        # --- MUDANÇA: Adiciona coluna 'Selecionar' ---
        df_pedidos_filtrados['Selecionar'] = False
        colunas_info = ['Selecionar', 'id_pedido', 'data_pedido_str', 'usuario_pedido', 'codigo', 'produto', 'embalagem', 'status_item', 'status_aprovacao']
        # --- FIM DA MUDANÇA ---
        
        colunas_editaveis = COLUNAS_LOJAS_PEDIDO
        colunas_total = ['total_cx']
        
        colunas_existentes = [col for col in (colunas_info + colunas_editaveis + colunas_total) if col in df_pedidos_filtrados.columns]
        df_para_editar = df_pedidos_filtrados[colunas_existentes]

        # Configuração da visualização das colunas
        column_config = {
            # --- MUDANÇA: Configura a coluna 'Selecionar' ---
            "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False),
            # --- FIM DA MUDANÇA ---
            "id_pedido": None, 
            "data_pedido_str": st.column_config.TextColumn("Data Pedido", disabled=True),
            "usuario_pedido": st.column_config.TextColumn("Usuário", disabled=True),
            "codigo": st.column_config.TextColumn("Código", disabled=True),
            "produto": st.column_config.TextColumn("Produto", width="medium", disabled=True),
            "embalagem": st.column_config.NumberColumn("Emb.", disabled=True, format="%d"), 
            "status_item": st.column_config.TextColumn("Status Mix", disabled=True),
            "total_cx": st.column_config.NumberColumn("Total CX (Original)", disabled=True, format="%d"),
            "status_aprovacao": None 
        }
        
        if not ver_pendentes:
             column_config["status_aprovacao"] = st.column_config.TextColumn("Status", disabled=True)

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
        
        # --- MUDANÇA: Botões de Aprovar e Rejeitar ---
        
        # Filtra as linhas que o usuário selecionou no checkbox
        df_selecionado = df_editado[df_editado['Selecionar'] == True]

        col_btn_1, col_btn_2, col_spacer = st.columns([1, 1, 3])

        with col_btn_1:
            if st.button("Aprovar Selecionados", type="primary"):
                if df_selecionado.empty:
                    st.warning("Nenhum item foi selecionado para aprovar.")
                else:
                    # Filtra apenas os pendentes (para segurança)
                    df_para_aprovar = df_selecionado[df_selecionado['status_aprovacao'] == 'Pendente']
                    if df_para_aprovar.empty:
                        st.warning("Nenhum item 'Pendente' foi selecionado para aprovar.")
                    else:
                        with st.spinner("Aprovando itens..."):
                            success, message = update_pedidos_aprovados(df_para_aprovar)
                            if success:
                                st.success(message)
                                get_pedidos_para_aprovacao.clear()
                                st.rerun()
                            else:
                                st.error(message)

        with col_btn_2:
            if st.button("Rejeitar Selecionados"):
                if df_selecionado.empty:
                    st.warning("Nenhum item foi selecionado para rejeitar.")
                else:
                    # Filtra apenas os pendentes (para segurança)
                    df_para_rejeitar = df_selecionado[df_selecionado['status_aprovacao'] == 'Pendente']
                    ids_para_rejeitar = df_para_rejeitar['id_pedido'].tolist()
                    
                    if not ids_para_rejeitar:
                        st.warning("Nenhum item 'Pendente' foi selecionado para rejeitar.")
                    else:
                        with st.spinner("Rejeitando itens..."):
                            success, message = rejeitar_pedidos(ids_para_rejeitar)
                            if success:
                                st.success(message)
                                get_pedidos_para_aprovacao.clear()
                                st.rerun()
                            else:
                                st.error(message)
        
        # Esconde o botão antigo se não estiver mostrando apenas pendentes
        if not ver_pendentes:
            st.info("Para aprovar ou rejeitar pedidos, marque o filtro 'Mostrar apenas Pedidos Pendentes'.")
        # --- FIM DAS MUDANÇAS ---


    st.markdown("---")
    
    st.subheader("2. Baixar Relatório de Pedidos Aprovados (Todos)")
    st.caption("Esta seção baixa TODOS os pedidos aprovados, independente do filtro de data acima.")
    
    df_aprovados = get_pedidos_aprovados_download()
    
    if df_aprovados.empty:
        st.info("Nenhum pedido aprovado encontrado para baixar.")
    else:
        st.markdown(f"Encontrados **{len(df_aprovados)}** itens aprovados no banco de dados.")
        excel_data = to_excel(df_aprovados)
        st.download_button(
            label="Baixar Aprovados (Excel)",
            data=excel_data,
            file_name=f"pedidos_aprovados_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    st.markdown("---")