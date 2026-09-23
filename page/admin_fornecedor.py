import streamlit as st
from sqlalchemy import text
import pandas as pd
import hashlib
import json
from datetime import datetime

# --- Configurações Globais ---
LISTA_LOJAS_FORNECEDOR = [
    "001", "002", "003", "004", "005", "006", "007", "008",
    "011", "012", "013", "014", "016", "017", "018",
    "F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09",
    "F10", "F11", "M12", "M13", "ADM", "RH"
]  # Pode ser diferente no futuro
ROLES_FORNECEDOR = ["fornecedor", "admin_fornecedor"]

# --- Funções Auxiliares de Hashing ---
def make_hashes_fornecedor(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

# --- Funções de Manutenção do DB (CRUD de Fornecedores) ---
def create_fornecedores_table(engine):
    """Cria a tabela de fornecedores e adiciona admins iniciais se não existirem."""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fornecedores_users (
                    username TEXT PRIMARY KEY,
                    password TEXT NOT NULL,
                    empresa TEXT,
                    ultimo_acesso TIMESTAMP,
                    status_logado TEXT,
                    role TEXT DEFAULT 'fornecedor',
                    lojas_acesso TEXT
                )
            """))

            # Adicionar admins iniciais se não existirem
            admins = {
                "ale": make_hashes_fornecedor("7890"),
                "rafael": make_hashes_fornecedor("302010")
            }
            for admin_user, admin_pass in admins.items():
                result = conn.execute(text("SELECT 1 FROM fornecedores_users WHERE username = :user"), {"user": admin_user}).scalar()
                if not result:
                    conn.execute(text("""
                        INSERT INTO fornecedores_users (username, password, role, empresa, status_logado)
                        VALUES (:user, :pass, 'admin_fornecedor', 'Administração', 'DESLOGADO')
                    """), {"user": admin_user, "pass": admin_pass})
    except Exception as e:
        st.error(f"Erro ao inicializar banco de dados de fornecedores: {e}")

def get_all_fornecedores_details(engine):
    """Busca todos os fornecedores, seus roles e lojas."""
    try:
        df = pd.read_sql_query(text("SELECT username, empresa, role, lojas_acesso FROM fornecedores_users"), con=engine)
        
        def format_lojas(lojas_json):
            if not lojas_json: return "Nenhuma"
            try:
                lojas_list = json.loads(lojas_json)
                return ", ".join(lojas_list)
            except json.JSONDecodeError:
                return "Erro de Formato"
                
        df['lojas_acesso'] = df['lojas_acesso'].apply(format_lojas)
        df.rename(columns={'username': 'Usuário', 'empresa': 'Empresa', 'role': 'Role', 'lojas_acesso': 'Lojas'}, inplace=True)
        return df
        
    except Exception as e:
        st.error(f"Erro ao carregar fornecedores: {e}")
        return pd.DataFrame(columns=['Usuário', 'Empresa', 'Role', 'Lojas'])

def add_new_fornecedor(engine, username, password, role, empresa, lojas_acesso_list):
    """Adiciona um novo fornecedor ao DB."""
    try:
        hashed_password = make_hashes_fornecedor(password)
        lojas_acesso_json = json.dumps(lojas_acesso_list)
        
        query = text("""
            INSERT INTO fornecedores_users (username, password, role, empresa, lojas_acesso, status_logado) 
            VALUES (:username, :password, :role, :empresa, :lojas, :status)
        """)
        params = {
            "username": username.lower(), "password": hashed_password, "role": role, 
            "empresa": empresa, "lojas": lojas_acesso_json, "status": 'DESLOGADO'
        }
        
        with engine.begin() as conn:
            conn.execute(query, params)
        return True
    
    except Exception as e:
        st.error(f"Erro ao adicionar fornecedor: {'Usuário já existe.' if 'unique constraint' in str(e) else e}")
        return False

def delete_fornecedor(engine, username):
    """Remove um fornecedor do DB."""
    try:
        with engine.begin() as conn:
            result = conn.execute(text("DELETE FROM fornecedores_users WHERE username = :username"), {"username": username.lower()})
        return result.rowcount > 0
    except Exception as e:
        st.error(f"Erro ao deletar fornecedor: {e}")
        return False

def update_fornecedor_permissions(engine, username, role, empresa, lojas_acesso_list):
    """Atualiza o role, empresa e as lojas de um fornecedor."""
    try:
        lojas_acesso_json = json.dumps(lojas_acesso_list)
        query = text("UPDATE fornecedores_users SET role = :role, empresa = :empresa, lojas_acesso = :lojas WHERE username = :username")
        params = {"role": role, "empresa": empresa, "lojas": lojas_acesso_json, "username": username.lower()}
        
        with engine.begin() as conn:
            result = conn.execute(query, params)
        return result.rowcount > 0
    except Exception as e:
        st.error(f"Erro ao alterar permissões: {e}")
        return False

def update_fornecedor_password(engine, username, new_password):
    """Altera a senha de um fornecedor."""
    try:
        hashed_password = make_hashes_fornecedor(new_password)
        query = text("UPDATE fornecedores_users SET password = :password WHERE username = :username")
        params = {"password": hashed_password, "username": username.lower()}
        
        with engine.begin() as conn:
            result = conn.execute(query, params)
        return result.rowcount > 0
    except Exception as e:
        st.error(f"Erro ao alterar senha: {e}")
        return False

# --- Lógica de Exibição da Página ---
def show_admin_fornecedor_page(engine):
    """Cria a interface do painel de administração de fornecedores."""
    st.title("🛡️ Painel de Administração de Fornecedores")
    st.markdown("Gerencie acessos de fornecedores/promotores, suas empresas e lojas permitidas.")
    
    # Garante que a tabela e os admins existam
    create_fornecedores_table(engine)
    
    if st.button("🔄 Atualizar Lista de Fornecedores"):
        st.rerun()

    st.subheader("Fornecedores Cadastrados")
    df_fornecedores = get_all_fornecedores_details(engine)
    st.dataframe(df_fornecedores, hide_index=True, use_container_width=True)
    st.markdown("---")

    tab1, tab2, tab3, tab4 = st.tabs(["Adicionar Fornecedor", "Gerenciar Acesso", "Alterar Senha", "Excluir Fornecedor"])

    with tab1:
        st.subheader("Adicionar Novo Fornecedor")
        with st.form("add_fornecedor_form", clear_on_submit=True):
            new_username = st.text_input("Login (Username)", key="add_forn_user").lower()
            new_empresa = st.text_input("Empresa do Fornecedor", key="add_forn_empresa")
            new_password = st.text_input("Senha Inicial", type="password", key="add_forn_pass")
            new_role = st.selectbox("Função (Role):", ROLES_FORNECEDOR, index=0, key="add_forn_role")
            new_lojas = st.multiselect("Lojas que pode acessar:", LISTA_LOJAS_FORNECEDOR, key="add_forn_lojas")
            
            if st.form_submit_button("Criar Fornecedor"):
                if not all([new_username, new_password, new_empresa]):
                    st.warning("Preencha Login, Empresa e Senha.")
                else:
                    if add_new_fornecedor(engine, new_username, new_password, new_role, new_empresa, new_lojas):
                        st.success(f"Fornecedor '{new_username}' criado!")
                        st.rerun()

    with tab2:
        st.subheader("Gerenciar Acesso (Role, Empresa e Lojas)")
        if not df_fornecedores.empty:
            user_list = df_fornecedores['Usuário'].tolist()
            user_to_manage = st.selectbox("Selecione o Fornecedor:", user_list, key="manage_forn_select", index=None)
            
            if user_to_manage:
                user_data = df_fornecedores[df_fornecedores['Usuário'] == user_to_manage].iloc[0]
                current_role_index = ROLES_FORNECEDOR.index(user_data['Role']) if user_data['Role'] in ROLES_FORNECEDOR else 0
                
                with st.form("manage_fornecedor_form"):
                    st.write(f"Editando: **{user_to_manage}**")
                    manage_role = st.selectbox("Nova Função:", ROLES_FORNECEDOR, index=current_role_index)
                    manage_empresa = st.text_input("Empresa:", value=user_data.get('Empresa', ''))
                    
                    try:
                        with engine.connect() as conn:
                            lojas_raw = conn.execute(text("SELECT lojas_acesso FROM fornecedores_users WHERE username = :user"), {"user": user_to_manage.lower()}).scalar()
                        current_lojas = json.loads(lojas_raw) if lojas_raw else []
                    except (json.JSONDecodeError, Exception):
                        current_lojas = []

                    manage_lojas = st.multiselect("Novas Lojas:", LISTA_LOJAS_FORNECEDOR, default=current_lojas)
                    
                    if st.form_submit_button("Atualizar Acessos"):
                        if update_fornecedor_permissions(engine, user_to_manage, manage_role, manage_empresa, manage_lojas):
                            st.success("Permissões atualizadas.")
                            st.rerun()

    with tab3:
        st.subheader("Alterar Senha de Fornecedor")
        if not df_fornecedores.empty:
            user_list_pass = df_fornecedores['Usuário'].tolist()
            user_for_pass = st.selectbox("Selecione o Fornecedor:", user_list_pass, key="pass_forn_select", index=None)
            if user_for_pass:
                with st.form("change_pass_fornecedor_form", clear_on_submit=True):
                    new_pass = st.text_input(f"Nova senha para {user_for_pass}:", type="password")
                    if st.form_submit_button("Alterar Senha"):
                        if update_fornecedor_password(engine, user_for_pass, new_pass):
                            st.success("Senha alterada.")

    with tab4:
        st.subheader("Excluir Fornecedor")
        if not df_fornecedores.empty:
            user_list_del = df_fornecedores['Usuário'].tolist()
            current_admin = st.session_state.get('username', '').lower()
            if current_admin in user_list_del:
                user_list_del.remove(current_admin)
            
            user_to_delete = st.selectbox("Selecione o Fornecedor para EXCLUIR:", user_list_del, key="del_forn_select", index=None)
            if user_to_delete:
                st.warning(f"⚠️ **Atenção:** Esta ação é irreversível.")
                if st.button(f"Confirmar Exclusão de {user_to_delete}", type="primary"):
                    if delete_fornecedor(engine, user_to_delete):
                        st.success("Fornecedor excluído.")
                        st.rerun()
