import streamlit as st
import pandas as pd
import hashlib
from datetime import datetime, date
import json
import os
from sqlalchemy import create_engine, text

# --- Importa as páginas ---
from page.home import show_home_page
from page.consulta_estoq_cd import show_consulta_page
from page.pedidos import show_pedidos_page
from page.aprovacao_pedidos import show_aprovacao_page
from page.status_usuarios import show_status_page
from page.admin_maint import show_admin_page
from page.admin_tools import show_admin_tools
from page.mudar_senha import show_mudar_senha_page
from page.contato import show_contato_page
from page.upload_ofertas import show_upload_ofertas_page
from page.ver_ofertas import show_ver_ofertas_page

# =========================================================
# CONFIGURAÇÕES INICIAIS
# =========================================================
st.set_page_config(page_title="Gestão de Produtos", layout="wide")

BASE_DATA_PATH = os.environ.get("RENDER_DISK_PATH", "data")
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
# CONEXÃO DE BANCO (APENAS POSTGRES)
# =========================================================
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        st.error("Erro fatal: A variável de ambiente DATABASE_URL não foi encontrada.")
        st.stop()
        
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
        
    return create_engine(db_url, connect_args={"sslmode": "require"}, pool_size=10, max_overflow=5)

engine = get_engine()


# =========================================================
# CRIAÇÃO / MIGRAÇÃO DE TABELAS
# =========================================================
def create_db_tables():
    """
    Cria todas as tabelas necessárias e executa a limpeza de dados antigos.
    """
    try:
        with engine.begin() as conn: 
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

            # --- tabelas de "Contato" ---
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS contato_chamados (
                    id SERIAL PRIMARY KEY,
                    usuario_username TEXT REFERENCES users(username),
                    assunto TEXT,
                    data_criacao TIMESTAMP,
                    ultimo_update TIMESTAMP,
                    status TEXT DEFAULT 'Aguardando Retorno' 
                )
            """)) 
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS contato_mensagens (
                    id SERIAL PRIMARY KEY,
                    chamado_id INTEGER REFERENCES contato_chamados(id) ON DELETE CASCADE,
                    remetente_username TEXT,
                    mensagem TEXT,
                    data_envio TIMESTAMP
                )
            """))
            
            # --- tabela de OFERTAS ---
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ofertas (
                    id SERIAL PRIMARY KEY,
                    codigo INTEGER NOT NULL,
                    produto TEXT,
                    oferta NUMERIC(10, 2),
                    data_inicio DATE NOT NULL,
                    data_final DATE NOT NULL,
                    UNIQUE(codigo, data_inicio, data_final)
                )
            """))
            
            # --- Lógica de Auto-Deleção (Limpeza de 7 dias Contato) ---
            seven_days_ago = (datetime.now() - pd.Timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            
            conn.execute(text("""
                DELETE FROM contato_chamados 
                WHERE ultimo_update < :seven_days_ago
            """), {"seven_days_ago": seven_days_ago})
            
    except Exception as e:
        if "foreign key constraint" not in str(e) and "does not exist" not in str(e):
             st.error(f"Erro ao inicializar o banco de dados: {e}")

# =========================================================
# LOGIN E PERFIL DE USUÁRIO
# =========================================================
def check_login_and_get_roles(username, password):
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
    current_time = datetime.now()
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
# FUNÇÃO DE PRIMEIRO ACESSO (BOOTSTRAP)
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
        if "does not exist" in str(e): # Se a tabela 'users' ainda não foi criada
            return True
        st.error(f"Erro ao verificar contagem de usuários: {e}")
        return False

# =========================================================
# FUNÇÃO DE CONTAGEM DE MENSAGENS
# =========================================================
@st.cache_data(ttl=60) # Cache de 1 minuto
def get_unread_message_count(_engine, username, role):
    """Pega a contagem de mensagens não lidas para o usuário/admin."""
    query_str = ""
    params = {}
    
    if role == "admin":
        # Admin vê tickets 'Aguardando Retorno'
        query_str = "SELECT COUNT(id) FROM contato_chamados WHERE status = 'Aguardando Retorno'"
    else:
        # Usuário vê tickets 'Respondidos'
        query_str = "SELECT COUNT(id) FROM contato_chamados WHERE status = 'Respondido' AND usuario_username = :username"
        params = {"username": username}

    if not query_str:
        return 0

    try:
        with _engine.connect() as conn:
            result = conn.execute(text(query_str), params)
            count = result.scalar_one_or_none() or 0
        return count
    except Exception as e:
        # Se as tabelas ainda não existem, não falha
        if "does not exist" not in str(e):
            print(f"Erro ao buscar contagem de mensagens: {e}") 
        return 0

# =========================================================
# MAIN APP
# =========================================================
def main():
    create_db_tables()
    
    is_first_run = check_if_first_run(engine)

    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    if is_first_run:
        st.warning("🚀 Bem-vindo! Detectamos que este é o primeiro acesso.")
        st.info("Por favor, crie o primeiro usuário administrador do sistema.")
        show_admin_page(engine=engine, base_data_path=BASE_DATA_PATH)
        st.stop() 

    if not st.session_state["logged_in"]:
        login_page() 

    # --- O RESTO DA PÁGINA (SÓ RODA SE LOGADO) ---
    st.sidebar.success(f"Logado como: {st.session_state['username']}")

    if st.sidebar.button("Logout"):
        update_user_status(st.session_state["username"], "DESLOGADO")
        st.session_state.clear()
        st.session_state["logged_in"] = False
        st.rerun()

    # --- Lógica de Notificação de Contato ---
    username = st.session_state.get("username", "")
    role = st.session_state.get("role", "user")
    
    unread_count = get_unread_message_count(engine, username, role)
    
    contato_menu_label = "Contato"
    if unread_count > 0:
        contato_menu_label = f"Contato ({unread_count}) 🔴"

    # --- MENU LATERAL ---
    paginas_disponiveis_labels = {
        "Home": show_home_page,
        "Consulta de Estoque CD": show_consulta_page,
        # "Histórico de Transferencia CD": show_historico_page, # REMOVIDO
        "Ofertas Atuais": show_ver_ofertas_page,
        "Alterar Senha": show_mudar_senha_page,
        contato_menu_label: show_contato_page, 
    }

    if st.session_state.get("lojas_acesso"):
        paginas_disponiveis_labels["Digitar Pedidos"] = show_pedidos_page

    if st.session_state.get("role") == "mkt":
        paginas_disponiveis_labels["Upload Ofertas"] = show_upload_ofertas_page
    
    if st.session_state.get("role") == "admin":
        paginas_disponiveis_labels["Aprovação de Pedidos"] = show_aprovacao_page
        paginas_disponiveis_labels["Status do Usuário"] = show_status_page
        paginas_disponiveis_labels["Administração"] = show_admin_page
        paginas_disponiveis_labels["Atualização de Dependências"] = show_admin_tools
        if "Upload Ofertas" not in paginas_disponiveis_labels:
            paginas_disponiveis_labels["Upload Ofertas"] = show_upload_ofertas_page

    
    # --- Lógica de Navegação Atualizada ---
    page_list_labels = list(paginas_disponiveis_labels.keys())

    if "page_key" not in st.session_state:
        st.session_state.page_key = "Home"
    
    if st.session_state.page_key not in page_list_labels:
        if "Contato" in st.session_state.page_key:
            st.session_state.page_key = contato_menu_label 
        else:
            st.session_state.page_key = "Home" 

    def update_sidebar_selection():
        st.session_state.page_key = st.session_state["sidebar_radio_key"]

    current_page_index = page_list_labels.index(st.session_state.page_key)

    st.sidebar.radio(
        "Selecione a Página:", 
        page_list_labels, 
        index=current_page_index,
        on_change=update_sidebar_selection,
        key="sidebar_radio_key"
    )
    
    selected_page_func = paginas_disponiveis_labels[st.session_state.page_key]
    selected_page_func(engine=engine, base_data_path=BASE_DATA_PATH)


if __name__ == "__main__":
    main()


