import streamlit as st
import sqlite3
import pandas as pd
import hashlib
from datetime import datetime, timedelta

# Importa as páginas existentes
from page.consulta import show_consulta_page
from page.ae import show_ae_page
from page.status import show_status_page
from page.admin_maint import show_admin_page
from page.home import show_home_page # <-- NOVA IMPORTAÇÃO

# Defina o caminho do seu banco de dados
DB_PATH = 'data/database.db'

# --- Mapeamento de Páginas ---
PAGES = {
    "Home": show_home_page, # <-- NOVA PÁGINA
    "Consulta de Estoque": show_consulta_page,
    "Análise de Evolução": show_ae_page
    # Páginas de Admin são adicionadas dinamicamente
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
    """Cria a tabela 'users' se não existir (tabela de pendentes removida)."""
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
                
        # Tenta adicionar as colunas de rastreamento (ignora se já existirem)
        try:
            c.execute("ALTER TABLE users ADD COLUMN ultimo_acesso TIMESTAMP")
            c.execute("ALTER TABLE users ADD COLUMN status_logado TEXT")
        except sqlite3.OperationalError:
            pass # Colunas já existem
            
        conn.commit()
    except sqlite3.Error as e:
        st.error(f"Erro ao criar/atualizar tabelas do DB: {e}")
    finally:
        if conn:
            conn.close()

def update_user_status(username, status):
    """Atualiza o status de login e o timestamp de acesso do usuário (APENAS no Login/Logout)."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10) # <-- TIMEOUT
        c = conn.cursor()
        
        # Garante que o formato %m (mês) está correto.
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
        
        c.execute(
            "UPDATE users SET ultimo_acesso = ?, status_logado = ? WHERE username = ?",
            (current_time, status, username)
        )
        conn.commit()
    except sqlite3.Error as e:
        # Não mostra erro na tela principal, apenas no log (para não poluir)
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
        
        # Retorna False se o usuário não for encontrado
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
    
    # GARANTE que as tabelas existem antes de qualquer operação
    create_user_table_if_not_exists()
    
    # Inicializa o estado da sessão
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
        st.session_state['username'] = ''
        st.session_state['current_page'] = 'Home' # <-- PÁGINA PADRÃO ALTERADA

    # 1. Se o usuário NÃO ESTIVER logado, mostra a tela de login simplificada
    if not st.session_state['logged_in']:
        
        st.title("Sistema de Consulta de Produtos")
        st.subheader("Área de Login")
        username = st.text_input("Nome de Usuário")
        password = st.text_input("Senha", type="password")

        if st.button("Fazer Login"):
            if check_login(username, password):
                st.session_state['logged_in'] = True
                st.session_state['username'] = username
                st.session_state['current_page'] = 'Home' # <-- Garante que vá para a Home
                update_user_status(username, 'LOGADO') # <-- ATUALIZA STATUS
                st.rerun()
            else:
                st.warning("Nome de usuário ou senha incorretos.")
        
        st.markdown("---")
        # Mensagem para novos usuários, conforme solicitado
        st.info("Para novos usuários ou recuperação de senha, contate o Administrador do sistema.")
                
        return

    # 2. Se o usuário ESTIVER logado, mostra o menu de navegação e o conteúdo da página
    st.sidebar.success(f"Logado como: {st.session_state['username']}")
    
    if st.sidebar.button("Logout"):
        update_user_status(st.session_state['username'], 'DESLOGADO') # <-- ATUALIZA STATUS
        st.session_state['logged_in'] = False
        st.session_state['username'] = ''
        st.session_state['current_page'] = 'Home' # <-- Reseta para a Home
        st.rerun()

    # --- LÓGICA DE NAVEGAÇÃO CONDICIONAL ---
    # Começa com as páginas base
    paginas_a_exibir = list(PAGES.keys())
    
    # Adiciona as páginas de Admin apenas se o usuário for 'admin'
    if st.session_state['username'] == "admin":
        
        # Adiciona "Status do Usuário" se for admin
        PAGES["Status do Usuário"] = show_status_page
        if "Status do Usuário" not in paginas_a_exibir:
            paginas_a_exibir.append("Status do Usuário")

        # Adiciona "Administração" se for admin
        PAGES["Administração"] = show_admin_page
        if "Administração" not in paginas_a_exibir:
            paginas_a_exibir.append("Administração")
            
    # Remove as páginas de admin se o usuário não for admin
    else:
         if "Status do Usuário" in paginas_a_exibir:
            paginas_a_exibir.remove("Status do Usuário")
         if "Administração" in paginas_a_exibir:
            paginas_a_exibir.remove("Administração")

    # Garante que a página padrão seja segura caso o usuário mude
    default_page_index = 0
    if st.session_state['current_page'] in paginas_a_exibir:
        default_page_index = paginas_a_exibir.index(st.session_state['current_page'])
    else:
        # Se a página atual não estiver disponível (ex: admin fez logout), vai para a Home
        st.session_state['current_page'] = 'Home'
        default_page_index = 0

    # Seletor de Página na Sidebar
    selected_page = st.sidebar.radio(
        "Selecione a Página:",
        paginas_a_exibir, # <-- Usa a lista filtrada
        index=default_page_index
    )
    
    # Atualiza o estado e executa a função da página
    st.session_state['current_page'] = selected_page
    PAGES[st.session_state['current_page']]()

# --- Ponto de Entrada da Aplicação ---

if __name__ == "__main__":
    main()
