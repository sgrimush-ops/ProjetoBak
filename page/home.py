import streamlit as st
# MUDANÇA: Removido sqlite3
from sqlalchemy import text # MUDANÇA: Adicionado import text
import pandas as pd
from datetime import datetime

# --- Funções de KPI ---
# MUDANÇA: Removido @st.cache_data, adicionado 'engine'
def get_kpi_users(engine):
    """Busca o número total de usuários cadastrados."""
    try:
        # MUDANÇA: Usando 'engine'
        with engine.connect() as conn:
            query = text("SELECT COUNT(username) as total FROM users")
            result = conn.execute(query)
            count = result.scalar_one_or_none() or 0
        return count
    except Exception as e:
        st.error(f"Erro ao buscar KPI de usuários: {e}")
        return 0

# --- Função Principal da Página ---

# MUDANÇA: Adicionado 'engine' e 'base_data_path'
def show_home_page(engine, base_data_path):
    """Cria a interface da página inicial."""
    
    # 1. Título e Boas-Vindas
    st.title(f"Bem-vindo(a), {st.session_state.get('username', 'Usuário')}!")
    st.markdown("Este é o painel de controle do Sistema de Gestão de Estoque do CD (Informações atualizadas as 8:30hs de seg a sab).")
    st.markdown("---")

    # 2. KPIs (Métricas Principais)
    st.subheader("Resumo do Sistema")
    
    # MUDANÇA: Passando 'engine'
    st.metric(label="Total de Usuários Cadastrados", value=get_kpi_users(engine))
        
    st.markdown("---")

    # 3. Atalhos Rápidos
    st.subheader("Acesso Rápido")
    st.markdown("Selecione uma das opções abaixo para navegar:")

    # --- MUDANÇA NA NAVEGAÇÃO ---
    # Esta lógica agora define 'st.session_state.page', que usaremos no app.py
    # para sincronizar a sidebar.
    
    lojas_do_usuario = st.session_state.get('lojas_acesso', [])
    
    if lojas_do_usuario:
        col1_nav, col2_nav, col3_nav = st.columns(3)
    else:
        col1_nav, col2_nav = st.columns(2)

    with col1_nav:
        # MUDANÇA: Define 'st.session_state.page' para o nome exato da página
        if st.button("🔎 Consultar Estoque CD", use_container_width=True):
            st.session_state['page'] = "Consulta de Estoque CD"
            st.rerun()

    with col2_nav:
        # MUDANÇA: Botão agora aponta para o Histórico de Transferência
        if st.button("📊 Ver Histórico de Transferência", use_container_width=True):
            st.session_state['page'] = "Histórico de Transferencia CD"
            st.rerun()
            
    if lojas_do_usuario:
        with col3_nav:
            # MUDANÇA: Define 'st.session_state.page' para o nome exato da página
            if st.button("🛒 Digitar Pedidos", use_container_width=True, type="primary"):
                st.session_state['page'] = "Digitar Pedidos"
                st.rerun()



