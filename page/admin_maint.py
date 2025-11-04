import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import hashlib
from typing import Optional
import re # Para limpar o número de telefone

# Defina o caminho do seu banco de dados
DB_PATH = 'data/database.db'

# --- Funções Auxiliares de Hashing (Copiadas do app.py) ---

def make_hashes(password):
    """Gera um hash SHA256 para a senha."""
    return hashlib.sha256(str.encode(password)).hexdigest()

def check_hashes(password, hashed_text):
    """Verifica se a senha fornecida corresponde ao hash salvo."""
    return make_hashes(password) == hashed_text

# --- Funções de Manutenção do DB (Gestão de Usuários) ---

@st.cache_data # <-- ADICIONADO CACHE
def get_all_users():
    """Retorna todos os usuários para a tabela de gestão."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10) # <-- TIMEOUT
        df = pd.read_sql_query("SELECT username FROM users", conn)
        return df['username'].tolist()
    except sqlite3.Error as e:
        st.error(f"Erro ao carregar usuários: {e}")
        return []
    finally:
        if conn:
            conn.close()

def add_user(username, password):
    """Adiciona um novo usuário ao DB."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10) # <-- TIMEOUT
        c = conn.cursor()
        hashed_password = make_hashes(password)
        # Define o status inicial como DESLOGADO
        c.execute("INSERT INTO users (username, password, status_logado) VALUES (?, ?, ?)", 
                  (username, hashed_password, 'DESLOGADO'))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        st.error(f"Usuário '{username}' já existe.")
        return False
    except sqlite3.Error as e:
        st.error(f"Erro ao adicionar usuário: {e}")
        return False
    finally:
        if conn:
            conn.close()

def delete_user(username):
    """Remove um usuário do DB."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10) # <-- TIMEOUT
        c = conn.cursor()
        c.execute("DELETE FROM users WHERE username=?", (username,))
        conn.commit()
        return c.rowcount > 0
    except sqlite3.Error as e:
        st.error(f"Erro ao deletar usuário: {e}")
        return False
    finally:
        if conn:
            conn.close()

def update_password(username, new_password):
    """Altera a senha de um usuário existente."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10) # <-- TIMEOUT
        c = conn.cursor()
        hashed_password = make_hashes(new_password)
        c.execute("UPDATE users SET password=? WHERE username=?", (hashed_password, username))
        conn.commit()
        return c.rowcount > 0
    except sqlite3.Error as e:
        st.error(f"Erro ao alterar senha: {e}")
        return False
    finally:
        if conn:
            conn.close()

# --- Funções de Gestão de Solicitações REMOVIDAS ---

# --- Lógica de Exibição da Página ---

def show_admin_page():
    """Cria a interface do painel de administração."""
    st.title("🛡️ Painel de Administração de Usuários")
    st.markdown("Gerencie usuários, senhas e permissões.")
    
    # --- Bloco de Solicitações Pendentes REMOVIDO ---

    st.markdown("---")

    # 1. GESTÃO MANUAL DE USUÁRIOS
    st.subheader("Gestão Manual de Usuários")
    col_view, col_action = st.columns([1, 2])

    with col_view:
        users = get_all_users()
        st.write("**Usuários Existentes**")
        if users:
            st.dataframe(pd.DataFrame({'Usuário': users}))
        else:
            st.info("Nenhum usuário cadastrado.")

    # 2. SELEÇÃO DE AÇÃO (Adicionar, Alterar Senha, Excluir)
    with col_action:
        action = st.radio("Selecione a Ação Manual:", ("Adicionar Usuário", "Alterar Senha", "Excluir Usuário"))
    
        st.markdown("---")

        if action == "Adicionar Usuário":
            st.subheader("Adicionar Novo Usuário")
            with st.form("add_user_form", clear_on_submit=True):
                new_username = st.text_input("Nome de Usuário", key="new_user_name")
                new_password = st.text_input("Senha Inicial", type="password", key="new_user_pass")
                if st.form_submit_button("Criar Usuário"):
                    if new_username and new_password:
                        if add_user(new_username, new_password):
                            st.success(f"Usuário '{new_username}' criado com sucesso!")
                            get_all_users.clear() # Limpa o cache para atualizar a lista
                            st.rerun() 
                    else:
                        st.warning("Preencha todos os campos.")

        elif action == "Alterar Senha":
            st.subheader("Alterar Senha")
            with st.form("update_password_form", clear_on_submit=True):
                user_to_update = st.selectbox("Selecione o Usuário:", users, key="update_user_select")
                new_pass = st.text_input("Nova Senha", type="password", key="new_pass_input")
                
                if st.form_submit_button("Alterar Senha"):
                    if user_to_update and new_pass:
                        if update_password(user_to_update, new_pass):
                            st.success(f"Senha do usuário '{user_to_update}' alterada com sucesso!")
                        else:
                            st.error("Falha ao alterar senha (usuário não encontrado ou erro no DB).")
                    else:
                        st.warning("Selecione o usuário e digite a nova senha.")

        elif action == "Excluir Usuário":
            st.subheader("Excluir Usuário")
            st.warning("ATENÇÃO: A exclusão é permanente.")
            
            with st.form("delete_user_form"):
                # Filtra a lista de usuários para que o admin não possa se auto-excluir
                users_list = [u for u in users if u != st.session_state.get('username', 'admin')]
                user_to_delete = st.selectbox("Selecione o Usuário para Excluir:", users_list, key="delete_user_select", index=None)
                
                if st.form_submit_button("EXCLUIR PERMANENTEMENTE"):
                    if user_to_delete:
                        if delete_user(user_to_delete):
                            st.success(f"Usuário '{user_to_delete}' excluído com sucesso!")
                            get_all_users.clear() # Limpa o cache para atualizar a lista
                            st.rerun() 
                        else:
                            st.error("Falha ao excluir usuário.")
                    else:
                        st.warning("Selecione o usuário a ser excluído.")

