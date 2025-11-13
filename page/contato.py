import streamlit as st
from sqlalchemy import text
from datetime import datetime
import pandas as pd 

# =========================================================
# FUNÇÕES DE BANCO DE DADOS (Específicas do Contato)
# =========================================================

def get_user_tickets(engine, username):
    """Busca os tickets de um usuário específico."""
    query = text("""
        SELECT id, assunto, status, ultimo_update 
        FROM contato_chamados
        WHERE usuario_username = :username
        ORDER BY ultimo_update DESC
    """)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"username": username})
    return df

def get_admin_tickets(engine):
def get_user_tickets(engine, username):
    """Busca os tickets de um usuário específico."""
    query = text("""
        SELECT id, assunto, status, ultimo_update 
        FROM contato_chamados
        WHERE usuario_username = :username
        ORDER BY ultimo_update DESC
    """)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"username": username})
    return df

def get_admin_tickets(engine):
    """Busca todos os tickets para a visão do admin, separados por status."""
    query_open = text("""
        SELECT id, usuario_username, assunto, status, ultimo_update 
        FROM contato_chamados
        WHERE status = 'Aberto' OR status = 'Respondido'
        ORDER BY ultimo_update ASC
    """)
    query_closed = text("""
        SELECT id, usuario_username, assunto, status, ultimo_update 
        FROM contato_chamados
        WHERE status = 'Fechado'
        ORDER BY ultimo_update DESC
    """)
    with engine.connect() as conn:
        df_open = pd.read_sql_query(query_open, conn)
        df_closed = pd.read_sql_query(query_closed, conn)
    return df_open, df_closed

def create_new_ticket(engine, username, assunto, mensagem):
    """Cria um novo ticket e a primeira mensagem."""
    now = datetime.now()
    try:
        with engine.begin() as conn: # Inicia uma transação
            # 1. Cria o chamado
            query_ticket = text("""
                INSERT INTO contato_chamados (usuario_username, assunto, data_criacao, ultimo_update, status)
                VALUES (:username, :assunto, :now, :now, 'Aberto')
                RETURNING id;
            """)
            result = conn.execute(query_ticket, {"username": username, "assunto": assunto, "now": now})
            new_ticket_id = result.scalar_one()
            
            # 2. Insere a primeira mensagem
            query_msg = text("""
                INSERT INTO contato_mensagens (chamado_id, remetente_username, mensagem, data_envio)
                VALUES (:chamado_id, :username, :mensagem, :now)
            """)
            conn.execute(query_msg, {
                "chamado_id": new_ticket_id,
                "username": username,
                "mensagem": mensagem,
                "now": now
            })
        return True, new_ticket_id
    except Exception as e:
        return False, f"Erro ao criar chamado: {e}"

def get_ticket_messages(engine, ticket_id):
    """Busca todas as mensagens de um ticket específico."""
    query = text("""
        SELECT remetente_username, mensagem, data_envio 
        FROM contato_mensagens
        WHERE chamado_id = :ticket_id
        ORDER BY data_envio ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"ticket_id": ticket_id})
    return df

def add_message_to_ticket(engine, ticket_id, username, mensagem, new_status):
    """Adiciona uma nova mensagem e atualiza o status do ticket."""
    now = datetime.now()
    try:
        with engine.begin() as conn:
            # 1. Adiciona a mensagem
            query_msg = text("""
                INSERT INTO contato_mensagens (chamado_id, remetente_username, mensagem, data_envio)
                VALUES (:chamado_id, :username, :mensagem, :now)
            """)
            conn.execute(query_msg, {
                "chamado_id": ticket_id,
                "username": username,
                "mensagem": mensagem,
                "now": now
            })
            
            # 2. Atualiza o ticket
            query_ticket = text("""
                UPDATE contato_chamados
                SET status = :status, ultimo_update = :now
                WHERE id = :ticket_id
            """)
            conn.execute(query_ticket, {
                "status": new_status,
                "now": now,
                "ticket_id": ticket_id
            })
        return True
    except Exception as e:
        st.error(f"Erro ao enviar mensagem: {e}")
        return False

def close_ticket(engine, ticket_id):
    """Fecha um ticket (apenas admin)."""
    try:
        with engine.begin() as conn:
            query = text("UPDATE contato_chamados SET status = 'Fechado' WHERE id = :ticket_id")
            conn.execute(query, {"ticket_id": ticket_id})
        return True
    except Exception as e:
        st.error(f"Erro ao fechar chamado: {e}")
        return False

# =========================================================
# INTERFACE DA PÁGINA
# =========================================================

def show_chat_view(engine, ticket_id, role, username):
    """Mostra a interface de chat para um ticket selecionado."""
    
    # Botão para voltar
    if st.button("← Voltar para lista de chamados"):
        del st.session_state['selected_ticket_id']
        st.rerun()

    messages = get_ticket_messages(engine, ticket_id)
    
    # Exibe o histórico de chat
    for _, row in messages.iterrows():
        # Define o avatar (pessoa ou admin)
        avatar = "🧑‍💻" if row['remetente_username'] == username else "🛡️"
        with st.chat_message(row['remetente_username'], avatar=avatar):
            st.write(row['mensagem'])
            st.caption(f"Enviado em: {row['data_envio'].strftime('%d/%m/%Y %H:%M')}")

    # Input para nova mensagem
    prompt = st.chat_input("Digite sua resposta...")
    if prompt:
        # Define o novo status baseado em quem está respondendo
        new_status = "Respondido" if role == "admin" else "Aberto"
        
        if add_message_to_ticket(engine, ticket_id, username, prompt, new_status):
            st.rerun()
        else:
            st.error("Não foi possível enviar sua mensagem.")

# --- PÁGINA PRINCIPAL ---

def show_contato_page(engine, base_data_path):
    st.title("📞 Contato com a Administração")
    
    role = st.session_state.get("role", "user")
    username = st.session_state.get("username", "")

    # Se um ticket foi selecionado, mostra o chat
    if 'selected_ticket_id' in st.session_state:
        ticket_id = st.session_state['selected_ticket_id']
        show_chat_view(engine, ticket_id, role, username)
    
    # Senão, mostra a lista de tickets (visão de Admin ou Usuário)
    else:
        if role == "admin":
            st.subheader("Painel de Chamados (Admin)")
            df_open, df_closed = get_admin_tickets(engine)
            
            st.markdown("##### Chamados Ativos (Abertos / Respondidos)")
            if df_open.empty:
                st.info("Nenhum chamado ativo.")
            else:
                for _, row in df_open.iterrows():
                    col1, col2, col3, col4 = st.columns([1, 2, 1, 1])
                    col1.text(row['usuario_username'])
                    col2.text(row['assunto'])
                    col3.text(row['status'])
                    if col4.button("Ver", key=f"view_{row['id']}"):
                        st.session_state['selected_ticket_id'] = row['id']
                        st.rerun()

            with st.expander("Ver Chamados Fechados"):
                st.dataframe(df_closed, use_container_width=True)

        else:
            # --- VISÃO DO USUÁRIO ---
            st.subheader("Novo Chamado")
            with st.form("new_ticket_form", clear_on_submit=True):
                assunto = st.text_input("Assunto")
                mensagem = st.text_area("Sua Mensagem")
                
                if st.form_submit_button("Enviar Mensagem"):
                    if assunto and mensagem:
                        success, new_id = create_new_ticket(engine, username, assunto, mensagem)
                        if success:
                            st.session_state['selected_ticket_id'] = new_id
                            st.success("Chamado criado com sucesso!")
                            st.rerun()
                        else:
                            st.error(f"Erro: {new_id}")
                    else:
                        st.warning("Por favor, preencha o assunto e a mensagem.")
            
            st.markdown("---")
            st.subheader("Meus Chamados")
            df_user_tickets = get_user_tickets(engine, username)
            
            if df_user_tickets.empty:
                st.info("Você ainda não abriu nenhum chamado.")
            else:
                st.write("Clique em 'Ver' para abrir a conversa.")
                for _, row in df_user_tickets.iterrows():
                    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
                    col1.text(row['assunto'])
                    col2.text(row['status'])
                    col3.text(row['ultimo_update'].strftime('%d/%m/%Y'))
                    if col4.button("Ver", key=f"view_{row['id']}"):
                        st.session_state['selected_ticket_id'] = row['id']
                        st.rerun()
