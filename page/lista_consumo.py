import streamlit as st
from utils.cargos import is_user_consumo_cd
from page.pedido_consumo import _render_gestao_itens_consumo


def show_lista_consumo_page(engine, base_data_path):
    """
    Página oficial 'Lista Consumo' no menu de navegação.
    Permite visualização e download em Excel/PDF para todos os usuários,
    e inclusão/exclusão no banco de dados para administradores e perfil 'consumo cd'.
    """
    st.title("📋 Lista Consumo")
    st.markdown(
        "Visualização e manutenção da base de dados de itens de consumo (`consumo.parquet` / PostgreSQL)."
    )

    is_admin = str(st.session_state.get("role", "")).strip().lower() == "admin"
    is_consumo_cd = is_user_consumo_cd(engine)
    can_manage_items = is_admin or is_consumo_cd

    _render_gestao_itens_consumo(engine, base_data_path, can_manage=can_manage_items)
