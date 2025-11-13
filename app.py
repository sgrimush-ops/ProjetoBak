import streamlit as st
import pandas as pd
import hashlib
from datetime import datetime
import json
import os
from sqlalchemy import create_engine, text

# --- Importa as páginas ---
from page.home import show_home_page
from page.consulta_estoq_cd import show_consulta_page
from page.historico_cd import show_historico_page
from page.pedidos import show_pedidos_page
from page.aprovacao_pedidos import show_aprovacao_page
from page.status_usuarios import show_status_page
from page.admin_maint import show_admin_page
from page.admin_tools import show_admin_tools
from page.mudar_senha import show_mudar_senha_page

# CONFIGURAÇÕES INICIAIS
# =========================================================
st.set_page_config(page_title="Gestão de Produtos", layout="wide")

# O 'data' minúsculo é o fallback para rodar no seu PC local.
BASE_DATA_PATH = os.environ.get("RENDER_DISK_PATH", "data")
# Garante que o diretório exista (tanto no Render quanto local)
os.makedirs(BASE_DATA_PATH, exist_ok=True) 

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
        
    return create_engine(db_url, connect_args={"sslmode": "require"})

# MUDANÇA: Criamos o 'engine' uma vez aqui para ser usado em todo o app
engine = get_engine()


# =========================================================
# CRIAÇÃO / MIGRAÇÃO DE TABELAS
# =========================================================
def create_db_tables():
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
            conn.commit()
            
    except Exception as e:
        st.error(f"Erro ao inicializar o banco de dados: {e}")
        st.stop()


# =========================================================
# LOGIN E PERFIL DE USUÁRIO
# =========================================================
def check_login_and_get_roles(username, password):
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
# MUDANÇA: Adicionar esta função
# =========================================================
def check_if_first_run(engine):
    """Verifica se existe algum usuário no banco."""
    try:
        with engine.connect() as conn:
            query = text("SELECT COUNT(username) FROM users")
            result = conn.execute(query)
            count = result.scalar_one_or_none() or 0
        return count == 0
    except Exception as e:
        # Se a tabela não existir ainda (embora o create_db_tables deva rodar antes)
        if "does not exist" in str(e):
            return True
        st.error(f"Erro ao verificar contagem de usuários: {e}")
        return False # Assume que não é o first run se der erro
# =========================================================
# MAIN APP
# =========================================================
def main():
    create_db_tables()
    
    # MUDANÇA: Adiciona a verificação de primeiro acesso
    is_first_run = check_if_first_run(engine)

    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    # MUDANÇA: Nova lógica de roteamento
    # Se for o primeiro acesso, força a página de admin
    if is_first_run:
        st.warning("🚀 Bem-vindo! Detectamos que este é o primeiro acesso.")
        st.info("Por favor, crie o primeiro usuário administrador do sistema.")
        
        # 'show_admin_page' e 'BASE_DATA_PATH' vêm do topo do seu app.py
        show_admin_page(engine=engine, base_data_path=BASE_DATA_PATH)
        
        st.stop() # Para a execução aqui, não mostrando o login

    # Se não for o primeiro acesso, continua normal
    if not st.session_state["logged_in"]:
        login_page() # App normal, chama o login

    # --- O RESTO DA PÁGINA (SÓ RODA SE LOGADO) ---
    st.sidebar.success(f"Logado como: {st.session_state['username']}")

    if st.sidebar.button("Logout"):
        update_user_status(st.session_state["username"], "DESLOGADO")
        st.session_state.clear()
        st.session_state["logged_in"] = False
        st.rerun()

    # --- MENU LATERAL (formato original restaurado) ---
    paginas_disponiveis = {
        "Home": show_home_page,
        "Alterar Senha": show_mudar_senha_page,
        "Consulta de Estoque CD": show_consulta_page,
        "Histórico de Transferencia CD": show_historico_page,
    }

    if st.session_state.get("lojas_acesso"):
        paginas_disponiveis["Digitar Pedidos"] = show_pedidos_page

    if st.session_state.get("role") == "admin":
        paginas_disponiveis["Aprovação de Pedidos"] = show_aprovacao_page
        paginas_disponiveis["Status do Usuário"] = show_status_page
        paginas_disponiveis["Administração"] = show_admin_page
        paginas_disponiveis["Atualização de Dependências"] = show_admin_tools

    # MUDANÇA DE NAVEGAÇÃO: Lógica para sincronizar botões e sidebar
    page_list = list(paginas_disponiveis.keys())

    if "page" not in st.session_state:
        st.session_state.page = "Home"
    
    if st.session_state.page not in page_list:
        st.session_state.page = "Home"

    def update_sidebar_selection():
        st.session_state.page = st.session_state["sidebar_radio_key"]

    current_page_index = page_list.index(st.session_state.page)

    st.sidebar.radio(
        "Selecione a Página:", 
        page_list, 
        index=current_page_index,
        on_change=update_sidebar_selection,
        key="sidebar_radio_key"
    )
    
    paginas_disponiveis[st.session_state.page](engine=engine, base_data_path=BASE_DATA_PATH)


if __name__ == "__main__":
    main()



