import streamlit as st
import pandas as pd
import hashlib
import json
import os
from sqlalchemy import create_engine, text, event
from utils.cargos import bootstrap_cargos_catalog, is_user_consumo_cd
from utils.timezone import now_brazil

# --- Importa as páginas ---
from page.home import show_home_page
from page.aprovacao_pedidos import show_aprovacao_page
from page.status_usuarios import show_status_page
from page.admin_maint import show_admin_page
from page.mudar_senha import show_mudar_senha_page
from page.contato import show_contato_page
from page.admin_uploads import show_admin_uploads_page
from page.pedido_cd import show_pedidos_cd_page
from page.pedido_consumo import show_pedido_consumo_page
from page.solicitacao_acesso import show_solicitacao_acesso_page

# =========================================================
# CONFIGURAÇÕES INICIAIS
# =========================================================
st.set_page_config(page_title="Gestão de Produtos", layout="wide")

BASE_DATA_PATH = os.environ.get("RENDER_DISK_PATH", "data")
os.makedirs(BASE_DATA_PATH, exist_ok=True)

LISTA_LOJAS = [
    "001", "002", "003", "004", "005", "006", "007", "008",
    "011", "012", "013", "014", "016", "017", "018",
    "F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09",
    "F10", "F11", "M12", "M13", "ADM", "RH"
]

# =========================================================
# CONEXÃO DE BANCO
# =========================================================


@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        st.error("Erro fatal: DATABASE_URL não encontrada.")
        st.stop()
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    db_engine = create_engine(
        db_url,
        connect_args={"sslmode": "require"},
        pool_size=10,
        max_overflow=5,
    )

    @event.listens_for(db_engine, "connect")
    def _set_postgres_timezone(dbapi_connection, _connection_record):
        with dbapi_connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'America/Sao_Paulo'")

    return db_engine


engine = get_engine()

# =========================================================
# FUNÇÕES DE SEGURANÇA
# =========================================================


def make_hashes(password):
    # CORREÇÃO: Adicionando codificação explícita
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def check_hashes(password, hashed_text):
    return make_hashes(password) == hashed_text


def normalize_lojas_acesso(lojas_raw):
    lojas_norm = []
    for loja in lojas_raw or []:
        loja_str = str(loja).strip()
        if loja_str.lower().startswith("loja_"):
            loja_str = loja_str[5:]

        if loja_str.isdigit():
            loja_str = loja_str.zfill(3)

        if loja_str and loja_str not in lojas_norm:
            lojas_norm.append(loja_str)

    return lojas_norm


def check_login_and_get_roles(engine, username, password):
    # A definição aqui está correta (3 argumentos)
    with engine.connect() as conn:
        query = text(
            "SELECT password, role, lojas_acesso, cargo FROM users WHERE username = :username")
        result = conn.execute(query, {"username": username.lower()})
        data = result.fetchone()

    if data:
        hashed_password, role, lojas_acesso_json, cargo = data
        if check_hashes(password, hashed_password):
            lojas = []
            if lojas_acesso_json:
                try:
                    lojas = json.loads(lojas_acesso_json)
                except json.JSONDecodeError:
                    lojas = []
            lojas = normalize_lojas_acesso(lojas)
            return True, (role or "user"), lojas, (cargo or "")
    return False, "user", [], ""


def ensure_admin_test_access(engine, username, role, lojas):
    """Garante acesso total para o admin ale durante testes."""
    if str(username).strip().lower() != "ale" or str(role).strip().lower() != "admin":
        return lojas

    lojas_teste = list(LISTA_LOJAS)
    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE users SET lojas_acesso = :lojas WHERE username = :username"),
                {"lojas": json.dumps(lojas_teste), "username": "ale"},
            )
    except Exception:
        # Mesmo sem persistencia em banco, mantem acesso total em sessao.
        pass

    return lojas_teste


def update_user_status(username, status):
    try:
        current_time = now_brazil()
        query = text(
            "UPDATE users SET ultimo_acesso = :time, status_logado = :status WHERE username = :username")
        with engine.begin() as conn:
            conn.execute(query, {"time": current_time,
                         "status": status, "username": username.lower()})
    except Exception:
        pass


def update_user_last_access(username):
    """Atualiza o último acesso do usuário e garante status LOGADO durante interação."""
    try:
        current_time = now_brazil()
        query = text(
            "UPDATE users SET ultimo_acesso = :time, status_logado = 'LOGADO' "
            "WHERE username = :username")
        with engine.begin() as conn:
            conn.execute(query, {
                "time": current_time,
                "username": username.lower()
            })
    except Exception:
        pass


def cleanup_inactive_users():
    """Marca usuários como DESLOGADO se inativos por mais de 5 minutos"""
    try:
        query = text("""
            UPDATE users
            SET status_logado = 'DESLOGADO'
            WHERE status_logado = 'LOGADO'
            AND ultimo_acesso < (
                CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'
            ) - INTERVAL '5 minutes'
        """)
        with engine.begin() as conn:
            conn.execute(query)
    except Exception:
        pass


def cleanup_old_pedidos():
    """Remove TODOS os pedidos com mais de 30 dias - Execução diária"""
    try:
        query = text("""
            DELETE FROM pedidos_consolidados
            WHERE data_pedido < (
                CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'
            ) - INTERVAL '30 days'
        """)
        with engine.begin() as conn:
            conn.execute(query)
    except Exception:
        pass


def create_db_tables(engine):
    # Esta função agora aceita 1 argumento (engine)
    try:
        with engine.begin() as conn:
            # Tabela de sincronizacao de arquivos binarios
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS arquivos_sync (
                    nome VARCHAR(255) PRIMARY KEY,
                    conteudo BYTEA NOT NULL,
                    data_atualizacao TIMESTAMP NOT NULL DEFAULT NOW()
                );
            """))

            # Tabela de produtos customizados (PRIORIDADE MÁXIMA)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS produtos_custom (
                    cod_consinco INTEGER PRIMARY KEY,
                    descricao TEXT NOT NULL,
                    transicao INTEGER,
                    embalagem INTEGER NOT NULL CHECK (embalagem > 0),
                    status_mix CHAR(1) NOT NULL CHECK (status_mix IN ('A', 'S')),
                    data_criacao TIMESTAMP NOT NULL DEFAULT NOW(),
                    data_alteracao TIMESTAMP,
                    usuario_criacao TEXT NOT NULL,
                    usuario_alteracao TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_produtos_custom_status 
                ON produtos_custom(status_mix);
                
                COMMENT ON TABLE produtos_custom IS 
                'CRÍTICO: Produtos customizados que sobrescrevem o parquet. NUNCA deletar!';
            """))
            
            # Tabela de auditoria para produtos (backup automático)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS produtos_custom_audit (
                    id SERIAL PRIMARY KEY,
                    operacao VARCHAR(10) NOT NULL,
                    cod_consinco INTEGER NOT NULL,
                    descricao_old TEXT,
                    embalagem_old INTEGER,
                    status_mix_old CHAR(1),
                    data_operacao TIMESTAMP NOT NULL DEFAULT NOW(),
                    usuario_operacao TEXT
                );
            """))
            
            # ... comandos CREATE TABLE (os comandos estão OK)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password TEXT NOT NULL,
                    ultimo_acesso TIMESTAMP,
                    status_logado TEXT,
                    role TEXT DEFAULT 'user',
                    empresa TEXT DEFAULT 'Baklizi',
                    cargo TEXT,
                    lojas_acesso TEXT
                )
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS solicitacoes_acesso (
                    id SERIAL PRIMARY KEY,
                    nome TEXT NOT NULL,
                    cargo TEXT,
                    loja TEXT NOT NULL,
                    senha_sugerida TEXT NOT NULL,
                    username_sugerido TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'Pendente',
                    data_solicitacao TIMESTAMP NOT NULL,
                    data_analise TIMESTAMP,
                    admin_analise TEXT,
                    observacao TEXT
                )
            """))

            conn.execute(text("""
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS cargo TEXT
            """))

            conn.execute(text("""
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS empresa TEXT DEFAULT 'Baklizi'
            """))

            conn.execute(text("""
                UPDATE users
                SET empresa = 'Baklizi'
                WHERE empresa IS NULL OR BTRIM(empresa) = ''
            """))

            conn.execute(text("""
                ALTER TABLE solicitacoes_acesso
                ADD COLUMN IF NOT EXISTS empresa TEXT DEFAULT 'Baklizi'
            """))

            conn.execute(text("""
                UPDATE solicitacoes_acesso
                SET empresa = 'Baklizi'
                WHERE empresa IS NULL OR BTRIM(empresa) = ''
            """))

            lojas_sql_cols = ", ".join(
                [f"loja_{loja} INTEGER DEFAULT 0" for loja in LISTA_LOJAS])
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS pedidos_consolidados (
                    id SERIAL PRIMARY KEY, 
                    codigo_interno TEXT NOT NULL,
                    descricao TEXT,
                    codigo_ean TEXT,
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
            conn.execute(text("""
                ALTER TABLE pedidos_consolidados
                ADD COLUMN IF NOT EXISTS origem_pedido TEXT
            """))

            for loja in LISTA_LOJAS:
                conn.execute(text(f"""
                    ALTER TABLE pedidos_consolidados
                    ADD COLUMN IF NOT EXISTS loja_{loja} INTEGER DEFAULT 0
                """))

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

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ofertas (
                    id SERIAL PRIMARY KEY,
                    codigo_interno INTEGER NOT NULL,
                    descricao TEXT,
                    oferta NUMERIC(10, 2),
                    data_inicio DATE NOT NULL,
                    data_final DATE NOT NULL,
                    UNIQUE(codigo_interno, data_inicio, data_final)
                )
            """))

        bootstrap_cargos_catalog(engine)
    except Exception as e:
        # Melhoria: avisa se houver erro ao criar tabelas
        st.warning(f"Aviso: Falha ao tentar criar tabelas no BD. {e}")
        pass

def sincronizar_arquivos_do_banco(engine, base_data_path):
    """
    Verifica se há versões mais recentes dos arquivos no banco e faz o download.
    Roda apenas uma vez a cada carregamento ou quando o app acorda.
    """
    try:
        from page.admin_uploads import _resolver_pastas_destino
        with engine.connect() as conn:
            query = text("SELECT nome, data_atualizacao FROM arquivos_sync")
            arquivos_banco = conn.execute(query).fetchall()

        if not arquivos_banco:
            return

        pastas_destino = _resolver_pastas_destino(base_data_path)
        pasta_principal = pastas_destino[0] if pastas_destino else base_data_path
        os.makedirs(pasta_principal, exist_ok=True)

        for nome, data_banco in arquivos_banco:
            caminho_local = os.path.join(pasta_principal, nome)
            precisa_baixar = False
            
            if not os.path.exists(caminho_local):
                precisa_baixar = True
            else:
                # Usa timestamp de modificacao do arquivo local
                data_local_ts = os.path.getmtime(caminho_local)
                from datetime import timezone
                # Compara timestamp do arquivo com data do banco
                if data_banco.timestamp() > data_local_ts:
                    precisa_baixar = True

            if precisa_baixar:
                # Baixa o conteúdo do banco e salva
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT conteudo FROM arquivos_sync WHERE nome = :nome"), {"nome": nome}).fetchone()
                    if result and result[0]:
                        for pasta in pastas_destino:
                            os.makedirs(pasta, exist_ok=True)
                            with open(os.path.join(pasta, nome), "wb") as f:
                                f.write(result[0])
    except Exception as e:
        pass  # Evitar que quebre a inicializacao

# =========================================================
# NAVEGAÇÃO E LOGIN
# =========================================================


def login_page(engine):
    st.title("🔐 Login do Sistema")

    st.markdown("### Primeiro acesso?")
    st.caption(
        "Se você ainda não possui usuário, envie uma solicitação de acesso "
        "para aprovação do administrador."
    )
    if st.button("📝 Solicitar Novo Acesso"):
        st.session_state["show_access_request_page"] = True
        st.rerun()

    if st.session_state.get("show_access_request_page", False):
        st.markdown("---")
        show_solicitacao_acesso_page(engine, BASE_DATA_PATH)
        if st.button("← Voltar para Login"):
            st.session_state["show_access_request_page"] = False
            st.rerun()
        st.stop()

    st.markdown("---")
    username = st.text_input("Usuário:").lower()
    senha = st.text_input("Senha:", type="password")

    if st.button("Entrar", type="primary"):
        logged_in, role, lojas, cargo = check_login_and_get_roles(
            engine, username, senha)
        if logged_in:
            lojas = ensure_admin_test_access(engine, username, role, lojas)
            st.session_state["logged_in"] = True
            st.session_state["username"] = username
            st.session_state["role"] = role
            st.session_state["lojas_acesso"] = lojas
            st.session_state["cargo"] = cargo
            update_user_status(username, "LOGADO")
            st.rerun()
        else:
            st.error("Usuário ou senha inválidos.")
    st.stop()


def main_app():
    engine = get_engine()
    create_db_tables(engine)
    
    # Executa sincronização silenciosa de arquivos vindo do banco
    sincronizar_arquivos_do_banco(engine, BASE_DATA_PATH)

    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    try:
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
        if count == 0:
            st.warning("Primeiro acesso. Crie o usuário admin.")
            show_admin_page(engine=engine, base_data_path=BASE_DATA_PATH)
            st.stop()
    except:
        pass

    if not st.session_state["logged_in"]:
        login_page(engine)

    # --- ÁREA LOGADA ---
    # Limpa usuários inativos e atualiza o último acesso do usuário atual
    cleanup_inactive_users()
    cleanup_old_pedidos()
    update_user_last_access(st.session_state["username"])

    st.sidebar.success(f"Logado: {st.session_state['username']}")
    if st.sidebar.button("Logout"):
        update_user_status(st.session_state["username"], "DESLOGADO")
        st.session_state.clear()
        st.rerun()

    # Notificações
    username = st.session_state.get("username", "")
    role = st.session_state.get("role", "user")
    unread_count = get_unread_message_count(engine, username, role)
    contato_menu_label = "Contato"
    if unread_count > 0:
        contato_menu_label = f"Contato ({unread_count}) 🔴"

    # Menu
    paginas = {
        "Home": lambda: show_home_page(engine, BASE_DATA_PATH),
        "Alterar Senha": lambda: show_mudar_senha_page(engine, BASE_DATA_PATH),
        "Contato": lambda: show_contato_page(engine, BASE_DATA_PATH),
        "Solicitar Acesso": lambda: show_solicitacao_acesso_page(engine, BASE_DATA_PATH),
    }

    if st.session_state.get("lojas_acesso"):
        # Adiciona as páginas de pedido
        paginas["Pedido de Consumo"] = lambda: show_pedido_consumo_page(
            engine, BASE_DATA_PATH)
        paginas["Pedido por Código (CD)"] = lambda: show_pedidos_cd_page(
            engine, BASE_DATA_PATH)

    if st.session_state.get("role") == "admin" or is_user_consumo_cd(engine):
        paginas["Aprovação de Pedidos"] = lambda: show_aprovacao_page(
            engine, BASE_DATA_PATH)

    if st.session_state.get("role") == "admin":
        paginas["Status do Usuário"] = lambda: show_status_page(
            engine, BASE_DATA_PATH)
        paginas["Administração"] = lambda: show_admin_page(
            engine, BASE_DATA_PATH)
        paginas["Admin Uploads"] = lambda: show_admin_uploads_page(engine, BASE_DATA_PATH)

    # Seletor de Página
    page_labels = list(paginas.keys())
    if "page_key" not in st.session_state or st.session_state.page_key not in page_labels:
        st.session_state.page_key = "Home"

    # Validação e ajuste da página atual
    current_key = st.session_state.page_key
    if "Contato" in current_key:
        found_contact = next(
            (k for k in page_labels if "Contato" in k), "Home")
        if st.session_state.page_key != found_contact:
            st.session_state.page_key = found_contact
    elif st.session_state.page_key not in page_labels:
        st.session_state.page_key = "Home"

    def update_page():
        st.session_state.page_key = st.session_state.nav_radio

    try:
        current_index = page_labels.index(st.session_state.page_key)
    except ValueError:
        current_index = 0
        st.session_state.page_key = "Home"

    st.sidebar.radio(
        "Navegação:",
        page_labels,
        index=current_index,
        key="nav_radio",
        on_change=update_page
    )

    # --- EXECUÇÃO ---
    try:
        func = paginas[st.session_state.page_key]
        func()
    except Exception as e:
        st.error(f"Erro ao carregar página: {e}")


@st.cache_data(ttl=60)
def get_unread_message_count(_engine, username, role):
    query_str = ""
    params = {}
    if role == "admin":
        query_str = "SELECT COUNT(id) FROM contato_chamados WHERE status = 'Aguardando Retorno'"
    else:
        query_str = "SELECT COUNT(id) FROM contato_chamados WHERE status = 'Respondido' AND usuario_username = :username"
        params = {"username": username}

    if not query_str:
        return 0

    try:
        with _engine.connect() as conn:
            result = conn.execute(text(query_str), params)
            return result.scalar_one_or_none() or 0
    except Exception:
        return 0


if __name__ == "__main__":
    main_app()