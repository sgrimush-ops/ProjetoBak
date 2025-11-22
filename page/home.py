import streamlit as st

# --- Função Principal da Página ---

def show_home_page(engine, base_data_path):
    """Cria a interface da página inicial."""

    st.title(f"Bem-vindo(a), {st.session_state.get('username', 'Usuário')}!")
    st.markdown("Este é o painel de controle do Sistema de Gestão de Estoque (WMS).")
    st.markdown("---") # Linha separadora

    st.subheader("Acesso Rápido")
    st.markdown("Selecione uma das opções abaixo para navegar:")

    lojas_do_usuario = st.session_state.get('lojas_acesso', [])

    if lojas_do_usuario:
        col1_nav, col2_nav, col3_nav = st.columns(3)
    else:
        col1_nav, col2_nav = st.columns(2)

    with col1_nav:
        if st.button("🔎 Consultar Estoque CD", use_container_width=True):
            st.session_state['page_key'] = "Consulta de Estoque CD" # Atualiza o page_key
            st.rerun()
            
    if lojas_do_usuario:
        with col3_nav:
            if st.button("🛒 Digitar Pedidos", use_container_width=True, type="primary"):
                st.session_state['page_key'] = "Digitar Pedidos" # Atualiza o page_key
                st.rerun()



