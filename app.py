import streamlit as st
import pandas as pd
import hashlib
from datetime import datetime
import json
import os
import sqlite3
import psycopg2
from sqlalchemy import create_engine

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

BASE_DATA_PATH = os.environ.get("RENDER_DISK_PATH", "data")
os.makedirs(BASE_DATA_PATH, exist_ok=True)

DB_PATH = os.path.join(BASE_DATA_PATH, "database.db")
PEDIDOS_DB_PATH = os.path.join(BASE_DATA_PATH, "pedidos.db")

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
# CONEXÃO DINÂMICA DE BANCO
# =========================================================
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return create_engine(db_url, connect_args={"sslmode": "require"})
    else:
        return create_engine(f"sqlite:///{PEDIDOS_DB_PATH}")


# =========================================================
# CRIAÇÃO / MIGRAÇÃO DE TABELAS
# =========================================================
def create_db_tables():
    conn_users = sqlite3.connect(DB_PATH, timeout=10)
    conn_pedidos = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
    c_users = conn_users.cursor()
    c_pedidos = conn_pedidos.cursor()

    # --- tabela de usuários ---
    c_users.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            ultimo_acesso TIMESTAMP,
            status_logado TEXT,
            role TEXT DEFAULT 'user',
            lojas_acesso TEXT
        )
    """)

    # --- tabela de pedidos ---
    lojas_sql_cols = ", ".join([f"loja_{loja} INTEGER DEFAULT 0" for loja in LISTA_LOJAS])
    c_pedidos.execute(f"""
        CREATE TABLE IF NOT EXISTS pedidos_consolidados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    """)

    conn_users.commit()
    conn_pedidos.commit()
    conn_users.close()
    conn_pedidos.close()


# =========================================================
# LOGIN E PERFIL DE USUÁRIO
# =========================================================
def check_login_and_get_roles(username, password):
    conn = sqlite3.connect(DB_PATH, timeout=10)
    c = conn.cursor()
    c.execute("SELECT password, role, lojas_acesso FROM users WHERE username = ?", (username.lower(),))
    data = c.fetchone()
    conn.close()

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
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        c = conn.cursor()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE users SET ultimo_acesso=?, status_logado=? WHERE username=?",
                  (current_time, status, username.lower()))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erro ao atualizar status: {e}")
    finally:
        conn.close()


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

    # --- MENU LATERAL (formato original restaurado) ---
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
    paginas_disponiveis[selected_page]()


if __name__ == "__main__":
    main()
