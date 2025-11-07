import streamlit as st
import sqlite3
import pandas as pd
import hashlib
import json # Para salvar a lista de lojas
from datetime import datetime

# --- Configurações Globais ---
DB_PATH = 'data/database.db'
# Lista de Lojas (deve ser idêntica à do app.py)
LISTA_LOJAS = ["001", "002", "003", "004", "005", "006", "007", "008", "011", "012", "013", "014", "017", "018"]
ROLES_DISPONIVEIS = ["user", "admin"]

# --- Funções Auxiliares de Hashing (Copiadas do app.py) ---

def make_hashes(password):
    """Gera um hash SHA256 para a senha."""
    return hashlib.sha256(str.encode(password)).hexdigest()

# --- Funções de Manutenção do DB (CRUD de Usuários) ---

@st.cache_data(ttl=60)
def get_all_users_details():
    """Busca todos os usuários, seus roles e lojas."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        # Busca todas as informações de permissão
        df = pd.read_sql_query("SELECT username, role, lojas_acesso FROM users", conn)
        
        # Converte o JSON de lojas em uma string legível
        def format_lojas(lojas_json):
            if not lojas_json:
                return "Nenhuma"
            try:
                lojas_list = json.loads(lojas_json)
                return ", ".join(lojas_list)
            except json.JSONDecodeError:
                return "Erro de Formato"
                
        df['lojas_acesso'] = df['lojas_acesso'].apply(format_lojas)
        df.rename(columns={'username': 'Usuário', 'role': 'Role', 'lojas_acesso': 'Lojas'}, inplace=True)
        return df
        
    except sqlite3.Error as e:
        st.error(f"Erro ao carregar usuários: {e}")
        return pd.DataFrame(columns=['Usuário', 'Role', 'Lojas'])
    finally:
        if conn:
            conn.close()

def add_new_user(username, password, role, lojas_acesso_list):
    """Adiciona um novo usuário completo ao DB."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        c = conn.cursor()
        hashed_password = make_hashes(password)
        lojas_acesso_json = json.dumps(lojas_acesso_list) # Converte lista para JSON text
        
        # Salva em minúsculas
        c.execute(
            "INSERT INTO users (username, password, role, lojas_acesso, status_logado) VALUES (?, ?, ?, ?, ?)", 
            (username.lower(), hashed_password, role, lojas_acesso_json, 'DESLOGADO')
        )
        conn.commit()
        get_all_users_details.clear() # Limpa o cache
        return True
    except sqlite3.IntegrityError:
        st.error(f"Erro: Usuário '{username.lower()}' já existe.")
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
        conn = sqlite3.connect(DB_PATH, timeout=10)
        c = conn.cursor()
        # Deleta em minúsculas
        c.execute("DELETE FROM users WHERE username=?", (username.lower(),))
        conn.commit()
        get_all_users_details.clear() # Limpa o cache
        return c.rowcount > 0
    except sqlite3.Error as e:
        st.error(f"Erro ao deletar usuário: {e}")
        return False
    finally:
        if conn:
            conn.close()

def update_user_permissions(username, role, lojas_acesso_list):
    """Atualiza o role e as lojas de um usuário."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        c = conn.cursor()
        lojas_acesso_json = json.dumps(lojas_acesso_list) # Converte lista para JSON text
        
        # Atualiza em minúsculas
        c.execute(
            "UPDATE users SET role=?, lojas_acesso=? WHERE username=?", 
            (role, lojas_acesso_json, username.lower())
        )
        conn.commit()
        get_all_users_details.clear() # Limpa o cache
        return c.rowcount > 0
    except sqlite3.Error as e:
        st.error(f"Erro ao alterar permissões: {e}")
        return False
    finally:
        if conn:
            conn.close()

def update_user_password(username, new_password):
    """Altera a senha de um usuário existente."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        c = conn.cursor()
        hashed_password = make_hashes(new_password)
        # Atualiza em minúsculas
        c.execute("UPDATE users SET password=? WHERE username=?", (hashed_password, username.lower()))
        conn.commit()
        # Não precisa limpar cache aqui, pois a lista de usuários não muda
        return c.rowcount > 0
    except sqlite3.Error as e:
        st.error(f"Erro ao alterar senha: {e}")
        return False
    finally:
        if conn:
            conn.close()

# --- Lógica de Exibição da Página ---

def show_admin_page():
    """Cria a interface do painel de administração."""
    st.title("🛡️ Painel de Administração")
    st.markdown("Gerencie usuários, funções (roles) e acesso às lojas.")
    
    if st.button("🔄 Atualizar Lista de Usuários"):
        get_all_users_details.clear()
        st.rerun()

    # 1. VISUALIZAÇÃO DOS USUÁRIOS
    st.subheader("Usuários Cadastrados")
    df_users = get_all_users_details()
    
    if df_users.empty:
        st.info("Nenhum usuário cadastrado.")
    else:
        st.dataframe(df_users, hide_index=True, use_container_width=True)

    st.markdown("---")

    # 2. ABAS DE AÇÃO
    tab1, tab2, tab3, tab4 = st.tabs(["Adicionar Usuário", "Gerenciar Acesso", "Alterar Senha", "Excluir Usuário"])

    # --- ABA 1: Adicionar Usuário ---
    with tab1:
        st.subheader("Adicionar Novo Usuário")
        with st.form("add_user_form", clear_on_submit=True):
            # Força minúsculas na entrada
            new_username = st.text_input("Novo Login (Username)", key="add_user").lower()
            new_password = st.text_input("Senha Inicial", type="password", key="add_pass")
            new_role = st.selectbox("Função (Role):", ROLES_DISPONIVEIS, index=0, key="add_role")
            
            # Caixa de seleção para lojas
            new_lojas = st.multiselect(
                "Quais lojas este usuário pode acessar? (Se for admin, pode deixar em branco)", 
                LISTA_LOJAS, 
                key="add_lojas"
            )
            
            if st.form_submit_button("Criar Usuário"):
                if not (new_username and new_password):
                    st.warning("Preencha pelo menos o Login e a Senha.")
                else:
                    if add_new_user(new_username, new_password, new_role, new_lojas): # new_username já está minúsculo
                        st.success(f"Usuário '{new_username}' criado com sucesso!")
                        st.rerun()

    # --- ABA 2: Gerenciar Acesso (Role e Lojas) ---
    with tab2:
        st.subheader("Gerenciar Acesso (Role e Lojas)")
        
        if df_users.empty:
            st.info("Nenhum usuário para gerenciar.")
        else:
            user_list = df_users['Usuário'].tolist()
            # Remove o admin atual da lista (não pode editar a si mesmo aqui)
            current_admin = st.session_state.get('username', 'admin').lower()
            
            if current_admin in user_list:
                user_list.remove(current_admin)
            
            user_to_manage = st.selectbox("Selecione o Usuário para gerenciar:", user_list, key="manage_user_select", index=None)
            
            if user_to_manage:
                # Busca o estado atual do usuário
                user_data = df_users[df_users['Usuário'] == user_to_manage].iloc[0]
                current_role_index = ROLES_DISPONIVEIS.index(user_data['Role']) if user_data['Role'] in ROLES_DISPONIVEIS else 0
                
                # Tenta carregar as lojas atuais dele
                try:
                    # Precisamos buscar os dados brutos do DB, não os formatados
                    conn = sqlite3.connect(DB_PATH, timeout=10)
                    c = conn.cursor()
                    c.execute("SELECT lojas_acesso FROM users WHERE username = ?", (user_to_manage.lower(),))
                    lojas_json_raw = c.fetchone()
                    conn.close()
                    if lojas_json_raw and lojas_json_raw[0]:
                        current_lojas = json.loads(lojas_json_raw[0])
                    else:
                        current_lojas = []
                except Exception as e:
                    current_lojas = []
                    print(f"Erro ao carregar lojas para {user_to_manage}: {e}")

                with st.form("manage_access_form"):
                    st.markdown(f"Editando **{user_to_manage}**")
                    
                    managed_role = st.selectbox(
                        "Nova Função (Role):", 
                        ROLES_DISPONIVEIS, 
                        index=current_role_index, 
                        key="manage_role"
                    )
                    
                    managed_lojas = st.multiselect(
                        "Novas Lojas que o usuário pode acessar:", 
                        LISTA_LOJAS, 
                        default=current_lojas,
                        key="manage_lojas"
                    )
                    
                    if st.form_submit_button("Salvar Alterações de Acesso"):
                        if update_user_permissions(user_to_manage, managed_role, managed_lojas): # user_to_manage já está minúsculo
                            st.success(f"Permissões de '{user_to_manage}' atualizadas!")
                            st.rerun()
                        else:
                            st.error("Falha ao salvar alterações.")

    # --- ABA 3: Alterar Senha ---
    with tab3:
        st.subheader("Alterar Senha de Usuário (Admin)")
        if df_users.empty:
            st.info("Nenhum usuário para gerenciar.")
        else:
            user_list_pass = df_users['Usuário'].tolist()
            user_to_update_pass = st.selectbox("Selecione o Usuário:", user_list_pass, key="update_pass_select", index=None)
            
            if user_to_update_pass:
                with st.form("update_password_form", clear_on_submit=True):
                    st.markdown(f"Alterando senha de **{user_to_update_pass}**")
                    new_pass = st.text_input("Nova Senha", type="password", key="new_pass_input")
                    
                    if st.form_submit_button("Confirmar Alteração de Senha"):
                        if new_pass:
                            if update_user_password(user_to_update_pass, new_pass): # user_to_update_pass já está minúsculo
                                st.success(f"Senha do usuário '{user_to_update_pass}' alterada!")
                            else:
                                st.error("Falha ao alterar senha.")
                        else:
                            st.warning("Digite a nova senha.")

    # --- ABA 4: Excluir Usuário ---
    with tab4:
        st.subheader("Excluir Usuário")
        st.warning("ATENÇÃO: A exclusão é permanente.")
        
        if df_users.empty:
            st.info("Nenhum usuário cadastrado.")
        else:
            user_list_del = df_users['Usuário'].tolist()
            # Força minúsculas na verificação
            current_admin_del = st.session_state.get('username', 'admin').lower()
            
            if current_admin_del in user_list_del:
                user_list_del.remove(current_admin_del) # Admin não pode se auto-excluir
            
            user_to_delete = st.selectbox("Selecione o Usuário para Excluir:", user_list_del, key="delete_user_select", index=None)

            if user_to_delete:
                if st.button(f"Confirmar Exclusão de {user_to_delete}", type="primary"):
                    if delete_user(user_to_delete): # user_to_delete já está minúsculo
                        st.success(f"Usuário '{user_to_delete}' excluído com sucesso!")
                        st.rerun()
                    else:
                        st.error("Falha ao excluir usuário.")