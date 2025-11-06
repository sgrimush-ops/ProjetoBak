import streamlit as st
import sqlite3
import pandas as pd
import hashlib
from datetime import datetime, timedelta

# Importa as páginas
from page.home import show_home_page
from page.consulta import show_consulta_page
from page.ae import show_ae_page
from page.status import show_status_page
from page.admin_maint import show_admin_page
from page.historico import show_historico_page # <-- 1. ATUALIZAÇÃO: Nova importação

# Defina o caminho do seu banco de dados
DB_PATH = 'data/database.db'

# --- LISTA DE ADMINISTRADORES ---
# Adicione aqui os nomes de usuário que devem ter acesso de admin
ADMIN_USERS = ["admin", "rafael"]
# --- FIM DA LISTA ---

# --- Mapeamento de Páginas ---
PAGES = {
    # Define a Home como a primeira página
    "Home": show_home_page,
    "Consulta de Estoque": show_consulta_page,
    "Análise de Evolução": show_ae_page,
    "Histórico de Solicitações": show_historico_page # <-- 2. ATUALIZAÇÃO: Nova página
    # As páginas de Admin são adicionadas dinamicamente abaixo
}

# A configuração da página deve ser a primeira chamada do Streamlit
st.set_page_config(page_title="Consulta_WMS")

# --- Funções de Segurança (Hashing) ---

def make_hashes(password):
    """Gera um hash SHA256 para a senha."""
    return hashlib.sha256(str.encode(password)).hexdigest()

def check_hashes(password, hashed_text):
    """Verifica se a senha fornecida corresponde ao hash salvo."""
    return make_hashes(password) == hashed_text

# --- Funções de Banco de Dados (Rastreamento e Login) ---

def create_user_table_if_not_exists():
    """Cria a tabela de usuários com colunas de rastreamento se não existirem."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10) # <-- TIMEOUT
        c = conn.cursor()
        
        # Cria a tabela de usuários se não existir
        c.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                ultimo_acesso TIMESTAMP,
                status_logado TEXT
            )
        ''')
        
        # Remove a lógica de 'pending_requests'
        
        # Tenta adicionar as colunas de rastreamento (ignora se já existirem)
        try:
            c.execute("ALTER TABLE users ADD COLUMN ultimo_acesso TIMESTAMP")
            c.execute("ALTER TABLE users ADD COLUMN status_logado TEXT")
        except sqlite3.OperationalError:
            pass # Colunas já existem
            
        conn.commit()
    except sqlite3.Error as e:
        st.error(f"Erro ao criar/atualizar tabela de usuários: {e}")
    finally:
        if conn:
            conn.close()

def update_user_status(username, status):
    """Atualiza o status de login e o timestamp de acesso do usuário."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10) # <-- TIMEOUT
        c = conn.cursor()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        c.execute(
            "UPDATE users SET ultimo_acesso = ?, status_logado = ? WHERE username = ?",
            (current_time, status, username)
        )
        conn.commit()
    except sqlite3.Error as e:
        # Evita mostrar erros de DB para o usuário final, mas loga no console do admin
        print(f"Erro ao atualizar status do usuário no DB: {e}")
    finally:
        if conn:
            conn.close()

def check_login(username, password):
    """Verifica o usuário e o HASH da senha no banco de dados."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10) # <-- TIMEOUT
        c = conn.cursor()
        c.execute("SELECT password FROM users WHERE username = ?", (username,))
        data = c.fetchall()
        
        if data:
            hashed_password_from_db = data[0][0]
            return check_hashes(password, hashed_password_from_db)
        
        return False
        
    except sqlite3.Error as e:
        st.error(f"Erro de banco de dados: {e}")
        return False
    finally:
        if conn:
            conn.close()

# --- Lógica Principal da Aplicação ---

def main():
    """Função principal da aplicação."""
    
    # GARANTE que a tabela está pronta para rastrear o status
    create_user_table_if_not_exists()
    
    # Inicializa o estado da sessão
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
        st.session_state['username'] = ''
        st.session_state['current_page'] = 'Home' # Página padrão

    # 1. Se o usuário NÃO ESTIVER logado, mostra a tela de login
    if not st.session_state['logged_in']:
        st.title("Sistema de Consulta de Produtos")
        st.subheader("Área de Login")
        username = st.text_input("Nome de Usuário")
        password = st.text_input("Senha", type="password")

        if st.button("Fazer Login"):
            if check_login(username, password):
                st.session_state['logged_in'] = True
                st.session_state['username'] = username
                update_user_status(username, 'LOGADO') # <-- ATUALIZA STATUS NO LOGIN
                st.rerun()
            else:
                st.warning("Nome de usuário ou senha incorretos.")
        
        st.markdown("---")
        st.info("Para novos usuários ou problemas de senha, contate o Administrador do sistema.")
                
        return

    # 2. Se o usuário ESTIVER logado, mostra o menu de navegação e o conteúdo da página
    st.sidebar.success(f"Logado como: {st.session_state['username']}")
    
    if st.sidebar.button("Logout"):
        update_user_status(st.session_state['username'], 'DESLOGADO') # <-- ATUALIZA STATUS NO LOGOUT
        st.session_state['logged_in'] = False
        st.session_state['username'] = ''
        st.session_state['current_page'] = 'Home'
        st.rerun()

    # --- LÓGICA DE NAVEGAÇÃO CONDICIONAL ---
    paginas_a_exibir = list(PAGES.keys())
    
    # Adiciona as páginas de Admin apenas se o usuário estiver na lista ADMIN_USERS
    if st.session_state['username'] in ADMIN_USERS: # <-- Verifica a lista de admins
        # Adiciona as páginas de Admin ao dicionário e à lista
        PAGES["Status do Usuário"] = show_status_page
        PAGES["Administração"] = show_admin_page
        if "Status do Usuário" not in paginas_a_exibir:
            paginas_a_exibir.append("Status do Usuário")
        if "Administração" not in paginas_a_exibir:
            paginas_a_exibir.append("Administração")
            
    # Remove a página de Admin da lista se o usuário não for admin
    elif "Administração" in paginas_a_exibir:
         paginas_a_exibir.remove("Administração")
    elif "Status do Usuário" in paginas_a_exibir:
         paginas_a_exibir.remove("Status do Usuário")
    # --- FIM DA ALTERAÇÃO ---

    # Seletor de Página na Sidebar
    
    # Garante que a página atual seja válida
    if st.session_state['current_page'] not in paginas_a_exibir:
        st.session_state['current_page'] = "Home"
        
    selected_page = st.sidebar.radio(
        "Selecione a Página:",
        paginas_a_exibir, # <-- Usa a lista filtrada
        index=paginas_a_exibir.index(st.session_state['current_page'])
    )
    
    # Atualiza o estado e executa a função da página
    st.session_state['current_page'] = selected_page
    PAGES[st.session_state['current_page']]() # <-- 3. ATUALIZAÇÃO: Linha adicionada

# --- Ponto de Entrada da Aplicação ---

if __name__ == "__main__":
    main()
    