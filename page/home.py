import streamlit as st
from sqlalchemy import text
import pandas as pd
from datetime import datetime, timedelta


# =========================================================
# FUNÇÃO: GRÁFICO DE PEDIDOS APROVADOS
# =========================================================
@st.cache_data(ttl=600)  # Cache de 10 minutos
def get_approved_orders_chart(_engine):
    """Busca o volume de pedidos aprovados nos últimos 30 dias."""
    try:
        # Define a data limite (30 dias atrás)
        date_limit = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')

        # Query SQL
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

        # Executa a query e carrega os dados
        df = pd.read_sql_query(query, con=_engine, params={"date_limit": date_limit})

        if df.empty:
            return pd.DataFrame(columns=["Volume (CX)"])

        # Define 'Dia' como datetime
        df['Dia'] = pd.to_datetime(df['Dia']).dt.strftime('%d/%m')
        df = df.set_index('Dia')

        # Cria um range contínuo de 30 dias
        all_days = pd.date_range(start=datetime.now() - timedelta(days=30), end=datetime.now(), freq='D')

        # Reindexa o dataframe para incluir todos os dias (preenche dias vazios com 0)
        df = df.reindex(all_days, fill_value=0)
        df.index.name = 'Dia'

        return df

    except Exception as e:
        st.error(f"Erro ao buscar dados do gráfico: {e}")
        return pd.DataFrame(columns=["Volume (CX)"])


# =========================================================
# FUNÇÃO PRINCIPAL DA PÁGINA INICIAL
# =========================================================
def show_home_page(engine, base_data_path):
    """Cria a interface da página inicial."""
    
    # 1. Título e Boas-Vindas
    st.title(f"Bem-vindo(a), {st.session_state.get('username', 'Usuário')}!")
    st.markdown("Este é o painel de controle do Sistema de Gestão de Estoque (WMS).")
    
    # 2. Atalhos Rápidos
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
        if st.button("📊 Ver Histórico de Transferência", use_container_width=True):
            st.session_state['page'] = "Histórico de Transferencia CD"
            st.rerun()
            
    if lojas_do_usuario:
        with col3_nav:
            if st.button("🛒 Digitar Pedidos", use_container_width=True, type="primary"):
                st.session_state['page'] = "Digitar Pedidos"
                st.rerun()
    
    st.markdown("---")
    
    # 3. Gráfico de Pedidos Aprovados
    st.subheader("📦 Volume de Pedidos Aprovados (Últimos 30 dias)")
    
    df_chart = get_approved_orders_chart(engine)
    
    if df_chart.empty:
        st.info("Nenhum pedido aprovado encontrado nos últimos 30 dias.")
    else:
        # Exibe gráfico de barras com preenchimento de largura total
        st.bar_chart(df_chart, use_container_width=True, height=400)




