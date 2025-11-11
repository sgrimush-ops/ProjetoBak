import streamlit as st
import os
import sqlite3

try:
    import psycopg2
except ImportError:
    psycopg2 = None


def resetar_banco():
    """Zera e recria a tabela pedidos_consolidados, compatível com o sistema atual."""
    sql_postgres = """
    DROP TABLE IF EXISTS pedidos_consolidados;
    CREATE TABLE pedidos_consolidados (
        id SERIAL PRIMARY KEY,
        codigo TEXT,
        produto TEXT,
        ean TEXT,
        embseparacao INTEGER DEFAULT 0,
        data_pedido TIMESTAMP,
        data_aprovacao TIMESTAMP,
        usuario_pedido TEXT,
        status_item TEXT,
        status_aprovacao TEXT DEFAULT 'Pendente',
        total_cx INTEGER DEFAULT 0,
        loja_001 INTEGER DEFAULT 0,
        loja_002 INTEGER DEFAULT 0,
        loja_003 INTEGER DEFAULT 0,
        loja_004 INTEGER DEFAULT 0,
        loja_005 INTEGER DEFAULT 0,
        loja_006 INTEGER DEFAULT 0,
        loja_007 INTEGER DEFAULT 0,
        loja_008 INTEGER DEFAULT 0,
        loja_011 INTEGER DEFAULT 0,
        loja_012 INTEGER DEFAULT 0,
        loja_013 INTEGER DEFAULT 0,
        loja_014 INTEGER DEFAULT 0,
        loja_017 INTEGER DEFAULT 0,
        loja_018 INTEGER DEFAULT 0
    );
    CREATE INDEX idx_pedidos_usuario ON pedidos_consolidados (usuario_pedido);
    CREATE INDEX idx_pedidos_data ON pedidos_consolidados (data_pedido);
    """

    sql_sqlite = """
    DROP TABLE IF EXISTS pedidos_consolidados;
    CREATE TABLE pedidos_consolidados (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT,
        produto TEXT,
        ean TEXT,
        embseparacao INTEGER DEFAULT 0,
        data_pedido TEXT,
        data_aprovacao TEXT,
        usuario_pedido TEXT,
        status_item TEXT,
        status_aprovacao TEXT DEFAULT 'Pendente',
        total_cx INTEGER DEFAULT 0,
        loja_001 INTEGER DEFAULT 0,
        loja_002 INTEGER DEFAULT 0,
        loja_003 INTEGER DEFAULT 0,
        loja_004 INTEGER DEFAULT 0,
        loja_005 INTEGER DEFAULT 0,
        loja_006 INTEGER DEFAULT 0,
        loja_007 INTEGER DEFAULT 0,
        loja_008 INTEGER DEFAULT 0,
        loja_011 INTEGER DEFAULT 0,
        loja_012 INTEGER DEFAULT 0,
        loja_013 INTEGER DEFAULT 0,
        loja_014 INTEGER DEFAULT 0,
        loja_017 INTEGER DEFAULT 0,
        loja_018 INTEGER DEFAULT 0
    );
    """

    if os.getenv("DATABASE_URL"):
        # 🔹 Modo Render (PostgreSQL)
        db_url = os.getenv("DATABASE_URL")
        if psycopg2 is None:
            st.error(
                "psycopg2 não instalado. Adicione 'psycopg2-binary' ao requirements.txt.")
            return
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            cur.execute(sql_postgres)
            conn.commit()
            cur.close()
            conn.close()
            st.success("✅ Banco PostgreSQL recriado com sucesso no Render!")
        except Exception as e:
            st.error(f"Erro ao recriar banco PostgreSQL: {e}")

    else:
        # 🔹 Modo local (SQLite)
        try:
            os.makedirs("data", exist_ok=True)
            conn = sqlite3.connect("data/pedidos.db")
            cur = conn.cursor()
            cur.executescript(sql_sqlite)
            conn.commit()
            conn.close()
            st.success("✅ Banco SQLite local recriado com sucesso!")
        except Exception as e:
            st.error(f"Erro ao recriar banco SQLite: {e}")


# 🔹 Interface Streamlit (visível só para admin)
def show_reset_db_page():
    if not st.session_state.get("is_admin", False):
        st.error(
            "❌ Acesso restrito: apenas administradores podem acessar esta página.")
        st.stop()

    st.title("🧰 Administração do Sistema")
    st.subheader("Recriar Banco de Pedidos (Reset)")

    st.warning(
        "⚠️ Esta ação apagará todos os pedidos e recriará a estrutura do banco.")
    if st.button("Recriar Banco Agora", type="primary"):
        resetar_banco()
