import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime

# Defina o caminho do seu banco de dados
DB_PATH = 'data/database.db'
# FILE_PATH foi removido pois não é mais necessário para KPIs

# --- Funções de KPI ---
# Usamos cache para que os números carreguem rápido

@st.cache_data(ttl=300) # Cache de 5 minutos
def get_kpi_users():
    """Busca o número total de usuários cadastrados."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        count = pd.read_sql_query("SELECT COUNT(username) as total FROM users", conn).iloc[0]['total']
        conn.close()
        return count
    except Exception:
        return 0

# --- REMOVIDA A FUNÇÃO get_kpi_items_hoje() ---
# A função que lia o WMS.xlsm foi removida para melhorar a performance.


# --- Função Principal da Página ---

def show_home_page():
    """Cria a interface da página inicial."""
    
    # 1. Título e Boas-Vindas
    st.title(f"Bem-vindo(a), {st.session_state.get('username', 'Usuário')}!")
    st.markdown("Este é o painel de controle do Sistema de Gestão de Estoque (WMS).")
    st.markdown("Estoque CD atualizado de seg a sab as 8:30hs, pedidos atualizados durante a tarde.")    
    st.markdown("---")

    # 2. KPIs (Métricas Principais)
    st.subheader("Resumo do Sistema")
    
    # KPI de Itens Movimentados foi removido.
    # Exibe apenas o total de usuários, sem usar colunas.
    st.metric(label="Total de Usuários Cadastrados", value=get_kpi_users())
        
    st.markdown("---")

    # 3. Atalhos Rápidos
    st.subheader("Acesso Rápido")
    st.markdown("Selecione uma das opções abaixo para navegar:")

    col1_nav, col2_nav = st.columns(2)

    with col1_nav:
        # Botão para ir para a Consulta
        if st.button("🔎 Consultar Estoque", use_container_width=True, type="primary"):
            st.session_state['current_page'] = "Consulta de Estoque"
            st.rerun()

    with col2_nav:
        # Botão para ir para a Análise
        if st.button("📈 Ver Análise de Evolução", use_container_width=True):
            st.session_state['current_page'] = "Análise de Evolução"
            st.rerun()

