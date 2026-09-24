from dotenv import load_dotenv
load_dotenv()

import streamlit as st
from sqlalchemy import text, create_engine, event
import hashlib
import json
import os

# Import das lógicas das "aplicações"
from app import main_app as run_baklizi_app
from app import create_db_tables
from page.area_fornecedor import show_area_fornecedor
from page.admin_fornecedor import show_admin_fornecedor_page
from page.contato_fornecedor import show_contato_fornecedor_page


@st.cache_resource
def get_main_engine():
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        st.error("Erro: DATABASE_URL não encontrada.")
        st.stop()

    # Substituição necessária para Postgres no Render
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    try:
        db_engine = create_engine(
            db_url,
            connect_args={"sslmode": "require"},
            pool_size=5,
            max_overflow=2
        )

        @event.listens_for(db_engine, "connect")
        def _set_postgres_timezone(dbapi_connection, _connection_record):
            with dbapi_connection.cursor() as cursor:
                cursor.execute("SET TIME ZONE 'America/Sao_Paulo'")

        return db_engine
    except Exception as e:
        st.error(f"Erro ao criar conexão com BD: {e}")
        st.stop()


def make_hashes_fornecedor(password):
    return hashlib.sha256(str.encode(password)).hexdigest()


def check_hashes_fornecedor(password, hashed_text):
    return make_hashes_fornecedor(password) == hashed_text


def normalize_lojas_acesso(lojas_raw):
    lojas_norm = []
    for loja in lojas_raw or []:
        loja_str = str(loja).strip()
        if loja_str.lower().startswith("loja_"):
            loja_str = loja_str[5:]

        if loja_str.isdigit():
            loja_str = loja_str.zfill(3)

        if loja_str and loja_str not in lojas_norm:
            lojas_norm.append(loja_str)

    return lojas_norm


def check_fornecedor_login(engine, username, password):
    """Verifica as credenciais na tabela de fornecedores."""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT password, role
                FROM fornecedores_users
                WHERE username = :username
            """)
            result = conn.execute(query, {"username": username.lower()})
            data = result.fetchone()

        if data:
            hashed_password, role = data
            if check_hashes_fornecedor(password, hashed_password):
                return True, role
    except Exception as e:
        st.error(f"Erro ao verificar credenciais: {str(e)}")
    return False, None


def show_main_menu():
    """Exibe o menu principal de seleção de perfil."""
    st.title("Menu de acesso Baklizi")
    st.markdown("""
    Informe se é funcionário da empresa ou se é fornecedor/promotor.
    
    ***ATENÇÃO: liberado pedidos de consumo, horário comercial, de seg a sex e sab de manhã***.
    
    """)
    col1, col2 = st.columns(2)
    if col1.button("Sou Funcionário (Baklizi)", use_container_width=True):
        st.session_state['app_choice'] = 'baklizi'
        st.rerun()
    if col2.button("Sou Fornecedor/Promotor", use_container_width=True):
        st.session_state['app_choice'] = 'fornecedor'
        st.session_state['fornecedor_logged_in'] = False
        st.rerun()


def show_fornecedor_login(engine):
    """Exibe o formulário de login para fornecedores."""
    from page.admin_fornecedor import create_fornecedores_table
    create_fornecedores_table(engine)

    st.title("Login de Fornecedor/Promotor")
    username = st.text_input("Usuário:", key="forn_user").lower()
    password = st.text_input("Senha:", type="password", key="forn_pass")

    if st.button("Entrar", type="primary"):
        logged_in, role = check_fornecedor_login(engine, username, password)
        if logged_in:
            # Buscar lojas de acesso do fornecedor
            try:
                with engine.connect() as conn:
                    query = text("""
                        SELECT lojas_acesso
                        FROM fornecedores_users
                        WHERE username = :username
                    """)
                    result = conn.execute(query, {"username": username})
                    data = result.fetchone()
                    lojas_json = data[0] if data else None

                lojas = []
                if lojas_json:
                    try:
                        lojas = json.loads(lojas_json)
                    except json.JSONDecodeError:
                        lojas = []
            except Exception:
                lojas = []

            lojas = normalize_lojas_acesso(lojas)

            st.session_state['fornecedor_logged_in'] = True
            st.session_state['fornecedor_username'] = username
            st.session_state['fornecedor_role'] = role
            st.session_state['fornecedor_lojas_acesso'] = lojas
            st.rerun()
        else:
            st.error("Usuário ou senha de fornecedor inválidos.")


def fornecedor_area_main(engine):
    """Área principal para fornecedores logados."""
    username = st.session_state.get('fornecedor_username', '')
    role = st.session_state.get('fornecedor_role', 'fornecedor')
    st.sidebar.success(f"Logado: {username}")

    # Menu da área de fornecedor
    paginas_fornecedor = {
        "Página Inicial": lambda: show_area_fornecedor(),
        "Contato / Suporte": lambda: show_contato_fornecedor_page(engine),
    }
    if role == 'admin_fornecedor':
        paginas_fornecedor["Admin Fornecedores"] = (
            lambda: show_admin_fornecedor_page(engine)
        )

    page_choice = st.sidebar.radio(
        "Navegação Fornecedor:",
        list(paginas_fornecedor.keys())
    )

    # Executa a página escolhida
    paginas_fornecedor[page_choice]()


def main():
    st.set_page_config(page_title="Bakizi", layout="wide")

    if 'app_choice' not in st.session_state:
        st.session_state['app_choice'] = None

    if st.session_state['app_choice'] is None:
        show_main_menu()
    elif st.session_state['app_choice'] == 'baklizi':
        engine = get_main_engine()

        try:
            create_db_tables(engine)
        except Exception as e:
            st.error(f"Erro ao inicializar o banco de dados: {e}")
            st.stop()

        run_baklizi_app()

    elif st.session_state['app_choice'] == 'fornecedor':
        engine = get_main_engine()
        if not st.session_state.get('fornecedor_logged_in', False):
            show_fornecedor_login(engine)
        else:
            fornecedor_area_main(engine)

    # Botão de voltar unificado
    if st.session_state.get('app_choice') is not None:
        if st.sidebar.button("Voltar ao Menu Principal"):
            st.session_state.clear()
            st.rerun()


if __name__ == "__main__":
    main()
