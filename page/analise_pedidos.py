import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from typing import List
import io

# --- Configurações Iniciais ---
PEDIDOS_DB_PATH = 'data/pedidos.db'
# Lista de Lojas (deve ser idêntica à do app.py)
LISTA_LOJAS = ["001", "002", "003", "004", "005", "006", "007", "008", "011", "012", "013", "014", "017", "018"]
COLUNAS_LOJAS = [f"loja_{loja}" for loja in LISTA_LOJAS]

# --- Funções de Banco de Dados ---

@st.cache_data(ttl=60) # Cache de 1 minuto
def get_pedidos(data_inicio, data_fim):
    """Busca pedidos no banco de dados dentro do intervalo de datas."""
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        
        # Converte datas para o formato de string do SQLite (YYYY-MM-DD)
        # Adiciona +1 dia ao data_fim para incluir o dia inteiro
        data_inicio_str = data_inicio.strftime('%Y-%m-%d 00:00:00')
        data_fim_str = (data_fim + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')

        # Query SQL com todas as colunas
        query = f"""
            SELECT 
                id, 
                STRFTIME('%d/%m/%Y %H:%M', data_pedido) AS "Data Pedido",
                codigo AS "Código",
                produto AS "Produto", 
                status_item AS "Status Mix", 
                total_cx AS "Total CX", 
                status_aprovacao AS "Status Aprovação",
                usuario_pedido AS "Usuário",
                {', '.join(COLUNAS_LOJAS)} 
            FROM pedidos_consolidados
            WHERE data_pedido >= ? AND data_pedido < ?
            ORDER BY data_pedido DESC
        """
        
        df = pd.read_sql_query(query, conn, params=(data_inicio_str, data_fim_str))
        
        # --- CORREÇÃO: Força o campo "Código" a ser numérico limpo ---
        if "Código" in df.columns:
            df["Código"] = (
                df["Código"]
                .astype(str)
                .str.extract(r"(\d+)", expand=False)  # pega apenas os dígitos
                .fillna("")                           # evita NaN
                .apply(lambda x: int(x) if x.isdigit() else x)
            )
        # --- FIM DA CORREÇÃO ---
        # O DataFrame do Pandas precisa ter a coluna 'ID' (maiúscula)
        # para corresponder ao que o data_editor e a função update esperam.
        df.rename(columns={'id': 'ID'}, inplace=True)
        # --- FIM DA CORREÇÃO ---

        # Converte 'status_aprovacao' para um booleano para o checkbox
        df['Aprovado'] = df['Status Aprovação'].apply(lambda x: True if x == 'Aprovado' else False)
        
        # Renomeia as colunas de loja para exibição
        rename_map = {col: f"Loja {col.split('_')[-1]}" for col in COLUNAS_LOJAS}
        df.rename(columns=rename_map, inplace=True)
        
        return df

    except sqlite3.Error as e:
        st.error(f"Erro ao buscar pedidos: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def update_pedidos_db(df_modificado):
    """Atualiza o banco de dados com as alterações do data_editor."""
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        c = conn.cursor()
        
        data_aprovacao_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Renomeia as colunas de volta para o formato do DB (ex: "loja_001")
        rename_map_inverso = {f"Loja {col.split('_')[-1]}": col for col in COLUNAS_LOJAS}
        df_modificado_db = df_modificado.rename(columns=rename_map_inverso)

        for index, row in df_modificado_db.iterrows():
            
            # --- CORREÇÃO: RECALCULA O TOTAL_CX ---
            novo_total_cx = 0
            for col_loja in COLUNAS_LOJAS:
                # Trata valores NaN/None que podem vir do data_editor
                valor_loja = row.get(col_loja, 0)
                if pd.isna(valor_loja):
                    valor_loja = 0
                novo_total_cx += int(valor_loja)
            # --- FIM DA CORREÇÃO ---
            
            # Constrói o UPDATE dinamicamente
            set_clauses = []
            params = []
            
            # Adiciona colunas de loja
            for col in COLUNAS_LOJAS:
                valor_loja = row.get(col, 0)
                if pd.isna(valor_loja):
                    valor_loja = 0
                set_clauses.append(f"{col} = ?")
                params.append(int(valor_loja)) # Garante que é inteiro
            
            # Adiciona status de aprovação
            novo_status = 'Aprovado' if row['Aprovado'] else 'Pendente'
            set_clauses.append("status_aprovacao = ?")
            params.append(novo_status)
            
            # Adiciona o Total_CX recalculado
            set_clauses.append("total_cx = ?")
            params.append(novo_total_cx)
            
            # Adiciona data de aprovação se estiver sendo aprovado agora
            if novo_status == 'Aprovado':
                set_clauses.append("data_aprovacao = ?")
                params.append(data_aprovacao_str)
            
            # Adiciona o ID ao final dos parâmetros para o WHERE
            params.append(row['ID']) # <-- Esta linha estava falhando
            
            query = f"UPDATE pedidos_consolidados SET {', '.join(set_clauses)} WHERE id = ?"
            c.execute(query, tuple(params))
            
        conn.commit()
        return True

    except sqlite3.Error as e:
        st.error(f"Erro ao salvar alterações no banco de dados: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

# --- NOVA FUNÇÃO ---
def delete_pedidos_db(ids_to_delete: List[int]):
    """Deleta pedidos do banco de dados pelos IDs."""
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        c = conn.cursor()
        
        # Cria placeholders (?) para a lista de IDs
        placeholders = ', '.join(['?'] * len(ids_to_delete))
        query = f"DELETE FROM pedidos_consolidados WHERE id IN ({placeholders})"
        
        c.execute(query, tuple(ids_to_delete))
        conn.commit()
        return True

    except sqlite3.Error as e:
        st.error(f"Erro ao deletar pedidos do banco de dados: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
# --- FIM DA NOVA FUNÇÃO ---


# --- Função de Download (Excel) ---
def to_excel(df):
    """Converte DataFrame para um arquivo Excel em memória."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Pedidos')
    processed_data = output.getvalue()
    return processed_data

# --- Lógica da Página ---

def show_analise_pedidos_page():
    """Cria a interface da página de Análise e Aprovação de Pedidos."""
    
    st.title("⚖️ Análise e Aprovação de Pedidos")
    st.markdown("Filtre, edite as quantidades (CX) e aprove os pedidos para o CD.")
    
    # --- 1. Filtros de Data ---
    st.subheader("Filtros")
    
    hoje = datetime.now().date()
    data_inicio_default = hoje - timedelta(days=3) # Padrão: 3 dias atrás
    data_fim_default = hoje # Padrão: Hoje
    
    col1, col2 = st.columns(2)
    with col1:
        data_inicio = st.date_input("Data Inicial", value=data_inicio_default)
    with col2:
        data_fim = st.date_input("Data Final", value=data_fim_default)

    if data_inicio > data_fim:
        st.error("A Data Inicial não pode ser maior que a Data Final.")
        st.stop()

    # Filtro de status de aprovação
    ver_apenas_aprovados = st.checkbox("Ver apenas pedidos já aprovados")
    status_filtro = 'Aprovado' if ver_apenas_aprovados else None

    # --- 2. Carregar Dados ---
    df_pedidos = get_pedidos(data_inicio, data_fim)
    
    if df_pedidos.empty:
        st.info("Nenhum pedido encontrado para o período selecionado.")
        st.stop()
        
    # Aplica filtro de status se o checkbox estiver marcado
    if status_filtro:
        df_pedidos_filtrados = df_pedidos[df_pedidos['Status Aprovação'] == status_filtro].copy()
    else:
        df_pedidos_filtrados = df_pedidos.copy()

    # --- 3. Botões de Ação Global ---
    st.markdown("---")
    
    # --- CORREÇÃO: Lógica de Session State ---
    # Verifica se os filtros mudaram. Se sim, força o recarregamento do state
    filtro_mudou = (
        st.session_state.get('filtro_data_inicio_analise') != data_inicio or
        st.session_state.get('filtro_data_fim_analise') != data_fim or
        st.session_state.get('filtro_status_analise') != status_filtro
    )

    if 'pedidos_editados_df' not in st.session_state or filtro_mudou:
        # Se o state não existe OU os filtros mudaram, recarrega
        st.session_state.pedidos_editados_df = df_pedidos_filtrados.copy()
        st.session_state.filtro_data_inicio_analise = data_inicio
        st.session_state.filtro_data_fim_analise = data_fim
        st.session_state.filtro_status_analise = status_filtro
    # --- FIM DA CORREÇÃO ---


    col_b1, col_b2 = st.columns(2)

    # Botão para Aprovar Todos Visíveis
    with col_b1:
        if st.button("Marcar Todos como Aprovados", use_container_width=True):
            df_temp = st.session_state.pedidos_editados_df.copy()
            df_temp['Aprovado'] = True
            st.session_state.pedidos_editados_df = df_temp
            st.toast("Todos os itens na tela foram marcados. Clique em 'Salvar Alterações'.")

    # Botão para Salvar Alterações
    with col_b2:
        if st.button("Salvar Alterações no DB", type="primary", use_container_width=True):
            if update_pedidos_db(st.session_state.pedidos_editados_df):
                st.success("Alterações salvas no banco de dados!")
                get_pedidos.clear() # Limpa o cache
                st.rerun()
            else:
                st.error("Falha ao salvar as alterações.")

    # --- 4. Editor de Dados ---
    st.subheader("Pedidos")
    
    # --- CORREÇÃO: Configuração de Colunas ---
    # Define a configuração das colunas para o data_editor
    column_config = {}
    
    # Colunas Editáveis (Lojas e Aprovado)
    for col in COLUNAS_LOJAS:
        label = f"Loja {col.split('_')[-1]}"
        column_config[label] = st.column_config.NumberColumn(
            label=label,
            min_value=0,
            step=1
        )
    
    column_config["Aprovado"] = st.column_config.CheckboxColumn(
        label="Aprovado?",
        default=False
    )
    
    # Colunas Desabilitadas (Mas que DEVEM estar no output)
    # Garante que 'ID' (maiúsculo) esteja aqui
    colunas_nao_editaveis = ["ID", "Data Pedido", "Código", "Produto", "Status Mix", "Total CX", "Status Aprovação", "Usuário"]
    for col in colunas_nao_editaveis:
        if col in df_pedidos_filtrados.columns: # Verifica se a coluna existe
            column_config[col] = st.column_config.TextColumn(
                label=col,
                disabled=True
            )
    # --- FIM DA CORREÇÃO ---
    
    # Edição dos dados
    df_editado = st.data_editor(
        st.session_state.pedidos_editados_df,
        column_config=column_config,
        hide_index=True,
        use_container_width=True,
        num_rows="dynamic" # Permite que o editor cresça
    )

    # Atualiza o session state com as edições mais recentes
    st.session_state.pedidos_editados_df = df_editado
    
    # --- 5. NOVA SEÇÃO: Deletar Pedidos ---
    st.markdown("---")
    with st.expander("🗑️ Deletar Pedidos (Ação Permanente)"):
        st.warning("Use esta seção para remover pedidos corrompidos (Ex: com EAN no lugar do código) ou duplicados.")
        
        # Pega os IDs da tabela filtrada
        ids_disponiveis = df_pedidos_filtrados['ID'].tolist()
        
        ids_para_deletar = st.multiselect(
            "Selecione os IDs dos pedidos para deletar:",
            options=ids_disponiveis
        )
        
        if st.button("Deletar Pedidos Selecionados", type="primary"):
            if ids_para_deletar:
                if delete_pedidos_db(ids_para_deletar):
                    st.success("Pedidos selecionados foram deletados permanentemente!")
                    get_pedidos.clear()
                    st.rerun()
                else:
                    st.error("Falha ao deletar os pedidos.")
            else:
                st.info("Nenhum ID selecionado.")
    

    # --- 6. Download Excel ---
    st.markdown("---")
    st.subheader("Download")
    
    df_para_download = st.session_state.pedidos_editados_df
    
    # Filtra para aprovados se a opção de filtro estiver marcada
    if ver_apenas_aprovados:
        df_para_download = df_para_download[df_para_download['Aprovado'] == True]

    if not df_para_download.empty:
        excel_data = to_excel(df_para_download)
        st.download_button(
            label="Baixar Pedidos como Excel (.xlsx)",
            data=excel_data,
            file_name=f"pedidos_{data_inicio}_a_{data_fim}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.info("Nenhum dado selecionado para download.")