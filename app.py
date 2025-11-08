import streamlit as st
import sqlite3
import pandas as pd
import hashlib
from datetime import datetime, timedelta
import json  # Para lidar com a lista de lojas

# --- Importa as Páginas ---
from page.home import show_home_page
from page.consulta import show_consulta_page
from page.ae import show_ae_page
from page.status import show_status_page
from page.admin_maint import show_admin_page
from page.historico import show_historico_page
from page.pedidos import show_pedidos_page
# from page.analise_pedidos import show_analise_pedidos_page # <-- 1. REMOVIDO
from page.aprovacao_pedidos import show_aprovacao_page  # <-- 1. ADICIONADO

# --- Configurações Globais ---
DB_PATH = 'data/database.db'
PEDIDOS_DB_PATH = 'data/pedidos.db'
LISTA_LOJAS = ["001", "002", "003", "004", "005", "006",
               "007", "008", "011", "012", "013", "014", "017", "018"]
COLUNAS_LOJAS_PEDIDO = [f"loja_{loja}" for loja in LISTA_LOJAS]

# --- Mapeamento de Páginas Base (Visíveis para todos) ---
PAGES = {
    "Home": show_home_page,
    "Consulta de Estoque": show_consulta_page,
    "Análise de Evolução": show_ae_page,
    "Histórico de Solicitações": show_historico_page,
    # Páginas de Pedidos e Admin são adicionadas dinamicamente
}

# A configuração da página deve ser a primeira chamada do Streamlit
st.set_page_config(
    page_title="Gestão de Produtos",
    layout="wide"  # Define o layout como "amplo"
)

# --- Funções de Segurança (Hashing) ---


def make_hashes(password):
    """Gera um hash SHA256 para a senha."""
    return hashlib.sha256(str.encode(password)).hexdigest()


def check_hashes(password, hashed_text):
    """Verifica se a senha fornecida corresponde ao hash salvo."""
    return make_hashes(password) == hashed_text

# --- Funções de Banco de Dados (Rastreamento e Login) ---


def create_db_tables():
    """
    Cria ou ATUALIZA as tabelas de 'users' (database.db) 
    e 'pedidos_consolidados' (pedidos.db).
    Esta função é segura para rodar múltiplas vezes (migração).
    """
    conn_users = None
    conn_pedidos = None

    # --- Colunas Esperadas (Para Migração) ---
    expected_user_cols = {
        "username": "TEXT PRIMARY KEY",
        "password": "TEXT NOT NULL",
        "ultimo_acesso": "TIMESTAMP",
        "status_logado": "TEXT",
        "role": "TEXT DEFAULT 'user'",
        "lojas_acesso": "TEXT"
    }

    lojas_sql_cols_dict = {
        f"loja_{loja}": "INTEGER DEFAULT 0" for loja in LISTA_LOJAS}
    expected_pedidos_cols = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "codigo": "TEXT NOT NULL",  # <-- ESSA COLUNA ESTAVA FALTANDO NO ERRO
        "produto": "TEXT",
        "ean": "TEXT",
        "embalagem": "INTEGER",
        "data_pedido": "TIMESTAMP",
        "data_aprovacao": "TIMESTAMP",
        "usuario_pedido": "TEXT",
        "status_item": "TEXT",
        **lojas_sql_cols_dict,  # Desempacota as colunas de loja aqui
        "total_cx": "INTEGER",
        "status_aprovacao": "TEXT DEFAULT 'Pendente'"
    }

    try:
        # 1. Conecta ao DB de Usuários
        conn_users = sqlite3.connect(DB_PATH, timeout=10)
        c_users = conn_users.cursor()

        # Cria a tabela de usuários (se não existir)
        c_users.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                ultimo_acesso TIMESTAMP,
                status_logado TEXT
            )
        ''')

        # Migração: Adiciona colunas faltantes à tabela 'users'
        c_users.execute("PRAGMA table_info(users)")
        existing_user_cols = [col[1] for col in c_users.fetchall()]

        for col, col_type in expected_user_cols.items():
            if col not in existing_user_cols:
                try:
                    c_users.execute(
                        f"ALTER TABLE users ADD COLUMN {col} {col_type}")
                    print(
                        f"Migração (Usuários): Adicionada coluna '{col}'")
                except sqlite3.OperationalError as e:
                    print(f"Aviso ao adicionar coluna '{col}': {e}")

        conn_users.commit()

        # 2. Conecta ao DB de Pedidos
        conn_pedidos = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        c_pedidos = conn_pedidos.cursor()

        # Cria a tabela de pedidos consolidados (se não existir)
        # Define apenas o 'id' inicialmente
        c_pedidos.execute('''
            CREATE TABLE IF NOT EXISTS pedidos_consolidados (
                id INTEGER PRIMARY KEY AUTOINCREMENT
            )
        ''')

        # Migração: Adiciona colunas faltantes à tabela 'pedidos_consolidados'
        c_pedidos.execute("PRAGMA table_info(pedidos_consolidados)")
        existing_pedidos_cols = [col[1] for col in c_pedidos.fetchall()]

        for col, col_type in expected_pedidos_cols.items():
            if col not in existing_pedidos_cols:
                try:
                    c_pedidos.execute(
                        f"ALTER TABLE pedidos_consolidados ADD COLUMN {col} {col_type}")
                    print(f"Migração (Pedidos): Adicionada coluna '{col}'")
                except sqlite3.OperationalError as e:
                    print(f"Aviso ao adicionar coluna '{col}': {e}")

        conn_pedidos.commit()

    except sqlite3.Error as e:
        st.error(f"Erro ao inicializar bancos de dados: {e}")
    finally:
        if conn_users:
            conn_users.close()
        if conn_pedidos:
            conn_pedidos.close()


def update_user_status(username, status):
    """Atualiza o status de login e o timestamp de acesso do usuário."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        c = conn.cursor()
        # Formato de data corrigido para %m (mês numérico)
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        c.execute(
            "UPDATE users SET ultimo_acesso = ?, status_logado = ? WHERE username = ?",
            (current_time, status, username.lower())  # Força minúsculas
        )
        conn.commit()
    except sqlite3.Error as e:
        # Evita mostrar erros de DB para o usuário final, mas loga no console do admin
        print(f"Erro ao atualizar status do usuário no DB: {e}")
    finally:
        if conn:
            conn.close()


def check_login_and_get_roles(username, password):
    """
    Verifica o login e retorna (True/False, role, lojas_acesso)
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        c = conn.cursor()

        # Busca usuário (forçando minúsculas)
        c.execute("SELECT password, role, lojas_acesso FROM users WHERE username = ?",
                  (username.lower(),))
        data = c.fetchone()  # Pega apenas um

        if data:
            hashed_password_from_db, role, lojas_acesso_json = data

            # 1. Verifica a senha
            if check_hashes(password, hashed_password_from_db):
                # 2. Prepara permissões
                lojas_acesso = []
                if lojas_acesso_json:
                    try:
                        lojas_acesso = json.loads(lojas_acesso_json)
                    except json.JSONDecodeError:
                        lojas_acesso = []

                # Garante que role nunca é None
                return True, (role or 'user'), lojas_acesso

        return False, 'user', []

    except sqlite3.Error as e:
        st.error(f"Erro de banco de dados: {e}")
        return False, 'user', []
    finally:
        if conn:
            conn.close()

# --- Lógica Principal da Aplicação ---


def main():
    """Função principal da aplicação."""

    # GARANTE que as tabelas (users e pedidos) estão prontas
    create_db_tables()

    # Inicializa o estado da sessão
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
        st.session_state['username'] = ''
        st.session_state['role'] = 'user'  # Padrão
        st.session_state['lojas_acesso'] = []  # Lojas que o usuário pode ver
        st.session_state['current_page'] = 'Home'  # Página padrão

    # 1. Se o usuário NÃO ESTIVER logado, mostra a tela de login
    if not st.session_state['logged_in']:
        # Título alterado conforme solicitação
        st.title("Sistema de Gestão de Produtos")
        st.subheader("Área de Login")

        # Força o username para minúsculas no input
        username = st.text_input("Nome de Usuário", key="login_user").lower()
        password = st.text_input("Senha", type="password", key="login_pass")

        if st.button("Fazer Login"):
            logged_in, role, lojas = check_login_and_get_roles(
                username, password)

            if logged_in:
                st.session_state['logged_in'] = True
                st.session_state['username'] = username.lower()  # Salva minúsculo
                st.session_state['role'] = role
                st.session_state['lojas_acesso'] = lojas
                update_user_status(username, 'LOGADO')  # ATUALIZA STATUS NO LOGIN
                st.rerun()
            else:
                st.warning("Nome de usuário ou senha incorretos.")

        st.markdown("---")
        st.info(
            "Para novos usuários ou problemas de senha, contate o Administrador do sistema.")

        return

    # 2. Se o usuário ESTIVER logado, mostra o menu de navegação
    st.sidebar.success(f"Logado como: {st.session_state['username']}")

    if st.sidebar.button("Logout"):
        update_user_status(st.session_state['username'],
                           'DESLOGADO')  # ATUALIZA STATUS NO LOGOUT

        # Limpa toda a sessão
        for key in st.session_state.keys():
            if key != 'login_mode':  # Preserva o modo de login
                del st.session_state[key]

        # Reseta o estado de login
        st.session_state['logged_in'] = False
        st.session_state['username'] = ''
        st.session_state['role'] = 'user'
        st.session_state['lojas_acesso'] = []
        st.session_state['current_page'] = 'Home'

        st.rerun()

    # --- LÓGICA DE NAVEGAÇÃO CONDICIONAL ---

    # Começa com as páginas base
    paginas_disponiveis = PAGES.copy()
    paginas_visiveis = list(paginas_disponiveis.keys())

    # Adiciona "Digitar Pedidos" se o usuário tiver acesso a lojas
    if st.session_state.get('lojas_acesso'):
        paginas_disponiveis["Digitar Pedidos"] = show_pedidos_page
        if "Digitar Pedidos" not in paginas_visiveis:
            # Adiciona depois do Histórico
            try:
                idx = paginas_visiveis.index(
                    "Histórico de Solicitações") + 1
                paginas_visiveis.insert(idx, "Digitar Pedidos")
            except ValueError:
                paginas_visiveis.append("Digitar Pedidos")

    # Adiciona páginas de Admin apenas se o role for 'admin'
    if st.session_state.get('role') == 'admin':
        # --- 2. MUDANÇA AQUI ---
        paginas_disponiveis["Aprovação de Pedidos"] = show_aprovacao_page
        paginas_disponiveis["Status do Usuário"] = show_status_page
        paginas_disponiveis["Administração"] = show_admin_page

        # Adiciona na ordem correta se não existirem
        # --- 3. MUDANÇA AQUI ---
        if "Digitar Pedidos" in paginas_visiveis and "Aprovação de Pedidos" not in paginas_visiveis:
            paginas_visiveis.insert(paginas_visiveis.index(
                "Digitar Pedidos") + 1, "Aprovação de Pedidos")
        elif "Aprovação de Pedidos" not in paginas_visiveis:
            paginas_visiveis.append("Aprovação de Pedidos")

        if "Status do Usuário" not in paginas_visiveis:
            paginas_visiveis.append("Status do Usuário")
        if "Administração" not in paginas_visiveis:
            paginas_visiveis.append("Administração")

    # Garante que a página atual seja válida para este usuário
    if st.session_state['current_page'] not in paginas_visiveis:
        st.session_state['current_page'] = "Home"

    # Seletor de Página na Sidebar
    selected_page = st.sidebar.radio(
        "Selecione a Página:",
        paginas_visiveis,  # <-- Usa a lista filtrada dinamicamente
        index=paginas_visiveis.index(st.session_state['current_page'])
    )

    # Atualiza o estado e executa a função da página
    st.session_state['current_page'] = selected_page
    paginas_disponiveis[selected_page]()

# --- Ponto de Entrada da Aplicação ---


if __name__ == "__main__":
    main()