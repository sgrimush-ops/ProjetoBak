import streamlit as st
import pandas as pd
import hashlib
from datetime import datetime
import json
import os
# MUDANÇA: Removido 'sqlite3' e 'psycopg2'
from sqlalchemy import create_engine, text  # MUDANÇA: Importado 'text'

# --- Importa as páginas ---
from page.home import show_home_page
from page.consulta import show_consulta_page
from page.ae import show_ae_page
from page.historico import show_historico_page
from page.pedidos import show_pedidos_page
from page.aprovacao_pedidos import show_aprovacao_page
from page.status import show_status_page
from page.admin_maint import show_admin_page
from page.admin_tools import show_admin_tools


# =========================================================
# CONFIGURAÇÕES INICIAIS
# =========================================================
st.set_page_config(page_title="Gestão de Produtos", layout="wide")

# MUDANÇA: Removido 'BASE_DATA_PATH', 'DB_PATH', 'PEDIDOS_DB_PATH'.
# Eles não são mais necessários com o Postgres.

LISTA_LOJAS = ["001", "002", "003", "004", "005", "006",
               "007", "008", "011", "012", "013", "014", "017", "018"]
COLUNAS_LOJAS_PEDIDO = [f"loja_{loja}" for loja in LISTA_LOJAS]


# =========================================================
# FUNÇÕES DE SEGURANÇA
# =========================================================
def make_hashes(password):
    return hashlib.sha256(password.encode()).hexdigest()


def check_hashes(password, hashed_text):
    return make_hashes(password) == hashed_text


# =========================================================
# MUDANÇA: CONEXÃO DE BANCO (APENAS POSTGRES)
# =========================================================
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    
    # MUDANÇA: Verificação para garantir que a URL existe
    if not db_url:
        st.error("Erro fatal: A variável de ambiente DATABASE_URL não foi encontrada.")
        st.stop()
        
    # MUDANÇA: O Render usa 'postgres://' mas SQLAlchemy prefere 'postgresql://'
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
        
    # MUDANÇA: Removido o 'else' que conectava ao SQLite
    return create_engine(db_url, connect_args={"sslmode": "require"})

# MUDANÇA: Criamos o 'engine' uma vez aqui para ser usado em todo o app
engine = get_engine()


# =========================================================
# CRIAÇÃO / MIGRAÇÃO DE TABELAS
# =========================================================
def create_db_tables():
    # MUDANÇA: Removidas conexões sqlite
    # MUDANÇA: Usando o 'engine' global do SQLAlchemy
    try:
        with engine.connect() as conn:
            # --- tabela de usuários ---
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password TEXT NOT NULL,
                    ultimo_acesso TIMESTAMP,
                    status_logado TEXT,
                    role TEXT DEFAULT 'user',
                    lojas_acesso TEXT
                )
            """))

            # --- tabela de pedidos ---
            lojas_sql_cols = ", ".join([f"loja_{loja} INTEGER DEFAULT 0" for loja in LISTA_LOJAS])
            # MUDANÇA: 'AUTOINCREMENT' é 'SERIAL' no Postgres, mas vamos usar o padrão
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS pedidos_consolidados (
                    id SERIAL PRIMARY KEY, 
                    codigo TEXT NOT NULL,
                    produto TEXT,
                    ean TEXT,
                    embseparacao INTEGER,
                    data_pedido TIMESTAMP,
                    data_aprovacao TIMESTAMP,
                    usuario_pedido TEXT,
                    status_item TEXT,
                    status_aprovacao TEXT DEFAULT 'Pendente',
                    total_cx INTEGER,
                    {lojas_sql_cols}
                )
            """))
            conn.commit() # MUDANÇA: Commit da transação
            
    except Exception as e:
        st.error(f"Erro ao inicializar o banco de dados: {e}")
        st.stop()


# =========================================================
# LOGIN E PERFIL DE USUÁRIO
# =========================================================
def check_login_and_get_roles(username, password):
    # MUDANÇA: Removida conexão sqlite
    # MUDANÇA: Usando o 'engine' global com 'text()'
    with engine.connect() as conn:
        query = text("SELECT password, role, lojas_acesso FROM users WHERE username = :username")
        result = conn.execute(query, {"username": username.lower()})
        data = result.fetchone()

    if data:
        hashed_password, role, lojas_acesso_json = data
        if check_hashes(password, hashed_password):
            lojas = []
            if lojas_acesso_json:
                try:
                    lojas = json.loads(lojas_acesso_json)
                except json.JSONDecodeError:
                    lojas = []
            return True, (role or "user"), lojas
    return False, "user", []


def update_user_status(username, status):
    # MUDANÇA: Removida conexão sqlite e try/except desnecessário
    # MUDANÇA: Usando 'engine.begin()' para auto-commit
    current_time = datetime.now() # MUDANÇA: Passando objeto datetime
    query = text("""
        UPDATE users 
        SET ultimo_acesso = :time, status_logado = :status 
        WHERE username = :username
    """)
    
    with engine.begin() as conn:
        conn.execute(query, {
            "time": current_time, 
            "status": status, 
            "username": username.lower()
        })


# =========================================================
# TELA DE LOGIN
# =========================================================
def login_page():
    st.title("🔐 Login do Sistema")
    username = st.text_input("Usuário:").lower()
    senha = st.text_input("Senha:", type="password")

    if st.button("Entrar", type="primary"):
        logged_in, role, lojas = check_login_and_get_roles(username, senha)
        if logged_in:
            st.session_state["logged_in"] = True
            st.session_state["username"] = username
            st.session_state["role"] = role
            st.session_state["lojas_acesso"] = lojas
            update_user_status(username, "LOGADO")
            st.rerun()
        else:
            st.error("Usuário ou senha inválidos.")

    st.stop()


# =========================================================
# MAIN APP
# =========================================================
def main():
    create_db_tables()

    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    if not st.session_state["logged_in"]:
        login_page()

    st.sidebar.success(f"Logado como: {st.session_state['username']}")

    if st.sidebar.button("Logout"):
        update_user_status(st.session_state["username"], "DESLOGADO")
        st.session_state.clear()
        st.session_state["logged_in"] = False
        st.rerun()

    # --- MENU LATERAL ---
    paginas_disponiveis = {
        "Home": show_home_page,
        "Consulta de Estoque CD": show_consulta_page,
        "Análise de Evolução Estoque CD": show_ae_page,
        "Histórico de Transferencia CD": show_historico_page,
    }

    if st.session_state.get("lojas_acesso"):
        paginas_disponiveis["Digitar Pedidos"] = show_pedidos_page

    if st.session_state.get("role") == "admin":
        paginas_disponiveis["Aprovação de Pedidos"] = show_aprovacao_page
        paginas_disponiveis["Status do Usuário"] = show_status_page
        paginas_disponiveis["Administração"] = show_admin_page
        paginas_disponiveis["Atualização de Dependências"] = show_admin_tools

    selected_page = st.sidebar.radio("Selecione a Página:", list(paginas_disponiveis.keys()))
    
    # MUDANÇA: Passando o 'engine' para todas as páginas
    # Você precisará atualizar seus arquivos na pasta 'page'
    # para aceitar este 'engine' e usá-lo.
    paginas_disponiveis[selected_page](engine=engine)


if __name__ == "__main__":
    main()
