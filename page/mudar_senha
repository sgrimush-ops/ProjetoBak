import streamlit as st
from sqlalchemy import text
import hashlib

# --- Funções de Hashing (copiadas do app.py/admin_maint.py) ---
# Precisamos delas aqui para verificar e criar as senhas.

def make_hashes(password):
    """Gera um hash SHA256 para a senha."""
    return hashlib.sha256(str.encode(password)).hexdigest()

def check_hashes(password, hashed_text):
    """Verifica se a senha bate com o hash salvo."""
    return make_hashes(password) == hashed_text

# --- Função de Banco de Dados ---

def update_user_password(engine, username, old_password, new_password):
    """Verifica a senha antiga e atualiza para a nova."""
    
    hashed_new_password = make_hashes(new_password)
    
    try:
        with engine.connect() as conn:
            # 1. Verificar a senha antiga
            query_check = text("SELECT password FROM users WHERE username = :username")
            result = conn.execute(query_check, {"username": username})
            data = result.fetchone()
            
            if not data:
                return False, "Usuário não encontrado."
                
            current_hashed_password = data[0]
            
            # Compara a senha "antiga" digitada com a do banco
            if not check_hashes(old_password, current_hashed_password):
                return False, "Senha atual incorreta."

            # 2. Se a senha antiga estiver correta, atualiza para a nova
            query_update = text("UPDATE users SET password = :new_password WHERE username = :username")
            
            # Usar .begin() para garantir o commit da transação
            with engine.begin() as trans_conn:
                 trans_conn.execute(query_update, {
                    "new_password": hashed_new_password,
                    "username": username
                })
            
            return True, "Senha alterada com sucesso!"

    except Exception as e:
        return False, f"Erro de banco de dados: {e}"

# --- Função Principal da Página ---

def show_mudar_senha_page(engine, base_data_path):
    """Cria a interface da página de alteração de senha."""
    
    st.title("🔑 Alterar Minha Senha")
    
    # Pega o usuário da sessão
    username = st.session_state.get("username")
    if not username:
        st.error("Erro: Usuário não está logado.")
        st.stop()
        
    st.info(f"Você está alterando a senha do usuário: **{username}**")

    # Usa um formulário para evitar recarregamentos
    with st.form("change_password_form", clear_on_submit=True):
        senha_atual = st.text_input("Senha Atual", type="password")
        nova_senha = st.text_input("Nova Senha", type="password")
        confirmar_senha = st.text_input("Confirmar Nova Senha", type="password")
        
        submitted = st.form_submit_button("Salvar Nova Senha")
        
        if submitted:
            if not senha_atual or not nova_senha or not confirmar_senha:
                st.warning("Por favor, preencha todos os campos.")
            elif nova_senha != confirmar_senha:
                st.error("A 'Nova Senha' e a 'Confirmação' não são iguais.")
            elif senha_atual == nova_senha:
                st.warning("A nova senha deve ser diferente da senha atual.")
            else:
                # Se tudo estiver OK, tenta atualizar no banco
                success, message = update_user_password(engine, username, senha_atual, nova_senha)
                if success:
                    st.success(message)
                else:
                    st.error(message)
