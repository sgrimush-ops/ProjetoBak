import streamlit as st
from sqlalchemy import text
import pandas as pd
from datetime import datetime, timedelta  # MUDANÇA: Importado o timedelta

# --- Funções de KPI ---

@st.cache_data(ttl=600) # Cache de 10 minutos
def get_kpi_users(engine):
    """Busca o número total de usuários cadastrados."""
    try:
        with engine.connect() as conn:
            query = text("SELECT COUNT(username) as total FROM users")
            result = conn.execute(query)
            count = result.scalar_one_or_none() or 0
        return count
    except Exception as e:
        st.error(f"Erro ao buscar KPI de usuários: {e}")
        return 0

# =========================================================
# MUDANÇA: NOVA FUNÇÃO PARA O GRÁFICO
# =========================================================
@st.cache_data(ttl=600) # Cache de 10 minutos
def get_approved_orders_chart(engine):
    """Busca o volume de pedidos aprovados nos últimos 30 dias."""
    try:
        # Define a data limite (30 dias atrás)
        date_limit = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
        
        # Query:
        # 1. Filtra por 'Aprovado'
        # 2. Filtra pela data de aprovação nos últimos 30 dias
        # 3. Agrupa pelo DIA da aprovação
        # 4. Soma o total de caixas (volume)
        query = text("""
            SELECT
                CAST(data_aprovacao AS DATE) AS "Dia",
                SUM(total_cx) AS "Volume (CX)"
            FROM pedidos_consolidados
            WHERE
                status_aprovacao = 'Aprovado'
                AND data_aprovacao >= :date_limit
            GROUP BY
                CAST(data_aprovacao AS DATE)
            ORDER BY
                "Dia" ASC
        """)
        
        df = pd.read_sql_query(query, con=engine, params={"date_limit": date_limit})
        
        # Define o 'Dia' como índice para o gráfico de barras
        if not df.empty:
            df = df.set_index('Dia')
            
        return df
    except Exception as e:
        st.error(f"Erro ao buscar dados do gráfico: {e}")
        return pd.DataFrame(columns=["Volume (CX)"])


# --- Função Principal da Página ---

def show_home_page(engine, base_data_path):
    """Cria a interface da página inicial."""
    
    # 1. Título e Boas-Vindas
    st.title(f"Bem-vindo(a), {st.session_state.get('username', 'Usuário')}!")
    st.markdown("Este é o painel de controle do Sistema de Gestão de Estoque (WMS).")
    
    # 2. MUDANÇA: Atalhos Rápidos (Movido para cima)
    st.subheader("Acesso Rápido")
    st.markdown("Selecione uma das opções abaixo para navegar:")
    
    lojas_do_usuario = st.session_state.get('lojas_acesso', [])
    
    if lojas_do_usuario:
        col1_nav, col2_nav, col3_nav = st.columns(3)
    else:
        col1_nav, col2_nav = st.columns(2)

    with col1_nav:
        if st.button("🔎 Consultar Estoque CD", use_container_width=True):
            st.session_state['page'] = "Consulta de Estoque CD"
            st.rerun()

    with col2_nav:
        # Corrigido para apontar para o Histórico
        if st.button("📊 Ver Histórico de Transferência", use_container_width=True):
            st.session_state['page'] = "Histórico de Transferencia CD"
            st.rerun()
            
    if lojas_do_usuario:
        with col3_nav:
            if st.button("🛒 Digitar Pedidos", use_container_width=True, type="primary"):
                st.session_state['page'] = "Digitar Pedidos"
                st.rerun()
    
    st.markdown("---")
    
    # 3. MUDANÇA: KPIs (Movido para o meio)
    st.subheader("Resumo do Sistema")
    st.metric(label="Total de Usuários Cadastrados", value=get_kpi_users(engine))
        
    st.markdown("---")

    # 4. MUDANÇA: Novo Gráfico de Pedidos Aprovados
    st.subheader("📦 Volume de Pedidos Aprovados (Últimos 30 dias)")
    
    # Busca os dados
    df_chart = get_approved_orders_chart(engine)
    
    if df_chart.empty:
        st.info("Nenhum pedido aprovado encontrado nos últimos 30 dias.")
    else:
        # Desenha o gráfico de barras
        st.bar_chart(df_chart)

