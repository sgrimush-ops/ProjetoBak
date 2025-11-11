import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import re
import os
import sqlite3
from sqlalchemy import create_engine, text

# --- Tenta importar psycopg2 (Render usa PostgreSQL) ---
try:
    import psycopg2
except ImportError:
    psycopg2 = None

# =========================================================
#  🔧 FUNÇÃO AUTOMÁTICA DE CONFIGURAÇÃO DO BANCO
# =========================================================


def atualizar_estrutura_banco():
    """Garante que a tabela pedidos_consolidados tenha todas as colunas corretas."""

    is_sqlite = os.path.exists("data/pedidos.db")

    ddl_base = [
        """
        CREATE TABLE IF NOT EXISTS pedidos_consolidados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT,
            produto TEXT,
            ean TEXT,
            embseparacao INTEGER DEFAULT 0,
            data_pedido TIMESTAMP,
            data_aprovacao TIMESTAMP,
            usuario_pedido TEXT,
            status_item TEXT,
            status_aprovacao TEXT DEFAULT 'Pendente',
            total_cx INTEGER DEFAULT 0
        );
        """
    ]

    # Colunas das lojas (001 a 018, com saltos)
    lojas = ["001", "002", "003", "004", "005", "006", "007",
             "008", "011", "012", "013", "014", "017", "018"]
    for loja in lojas:
        ddl_base.append(
            f"ALTER TABLE pedidos_consolidados ADD COLUMN IF NOT EXISTS loja_{loja} INTEGER DEFAULT 0;")

    # Executa os comandos no banco
    if is_sqlite:
        conn = sqlite3.connect("data/pedidos.db")
        cur = conn.cursor()
        for cmd in ddl_base:
            try:
                cur.execute(cmd.replace("IF NOT EXISTS", ""))
            except Exception as e:
                if "duplicate column name" not in str(e):
                    print(f"⚠️ Erro SQLite executando: {cmd}\n{e}")
        conn.commit()
        conn.close()
        print("✅ Estrutura SQLite atualizada com sucesso!")
    else:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            print("❌ Variável DATABASE_URL não encontrada.")
            return
        if psycopg2 is None:
            print(
                "❌ psycopg2 não instalado. Adicione 'psycopg2-binary' no requirements.txt.")
            return

        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        for cmd in ddl_base:
            try:
                cur.execute(cmd)
            except Exception as e:
                print(f"⚠️ Erro PostgreSQL executando: {cmd}\n{e}")
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Estrutura PostgreSQL atualizada com sucesso!")


# Executa a atualização automática
atualizar_estrutura_banco()

# =========================================================
#  ⚙️ CONEXÃO DINÂMICA (LOCAL / RENDER)
# =========================================================


def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return create_engine(db_url, connect_args={'sslmode': 'require'})
    else:
        return create_engine("sqlite:///data/pedidos.db")


# =========================================================
#  🧩 CONSTANTES E MAPEAMENTOS
# =========================================================
MIX_FILE_PATH = 'data/__MixAtivoSistema.xlsx'
HIST_FILE_PATH = 'data/historico_solic.xlsm'
WMS_FILE_PATH = 'data/WMS.xlsm'
PEDIDOS_DB_PATH = 'data/pedidos.db'

LISTA_LOJAS = ["001", "002", "003", "004", "005", "006",
               "007", "008", "011", "012", "013", "014", "017", "018"]

COLS_MIX_MAP = {
    'CODIGOINT': 'Codigo',
    'CODIGOEAN': 'EAN',
    'DESCRICAO': 'Produto',
    'LOJA': 'Loja',
    'EmbSeparacao': 'embseparacao',
    'PPCX': 'PPCX',
    'EICX': 'EICX',
    'CapCX': 'CapCX',
    'ltmix': 'Mix',
}

# =========================================================
#  📂 FUNÇÕES DE LEITURA DE DADOS
# =========================================================


def load_mix_data(file_path: str):
    try:
        df = pd.read_excel(file_path, dtype=str)
        df.rename(columns=COLS_MIX_MAP, inplace=True)
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(
            0).astype(int).astype(str)
        df['embseparacao'] = pd.to_numeric(
            df['embseparacao'].astype(str).str.split(
                ',').str[0].str.split('.').str[0].str.strip(),
            errors='coerce'
        ).fillna(0).astype(int)
        df['Loja'] = df['Loja'].astype(str).str.zfill(3)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar Mix: {e}")
        return pd.DataFrame()


def load_historico_data(file_path: str):
    try:
        df = pd.read_excel(file_path, dtype=str)
        df.rename(columns={'CODIGOINT': 'Codigo', 'LOJA': 'Loja',
                  'EmbSeparacao': 'embseparacao', 'DtSolicitacao': 'Data'}, inplace=True)
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(
            0).astype(int).astype(str)
        df['embseparacao'] = pd.to_numeric(
            df['embseparacao'].astype(str).str.split(
                ',').str[0].str.split('.').str[0].str.strip(),
            errors='coerce'
        ).fillna(0).astype(int)
        df['Loja'] = df['Loja'].astype(str).str.zfill(3)
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        return df
    except Exception as e:
        st.error(f"Erro ao carregar Histórico: {e}")
        return pd.DataFrame()


def load_wms_data(file_path: str):
    try:
        df = pd.read_excel(file_path, sheet_name='WMS', usecols=[
                           'codigo', 'Qtd', 'datasalva'])
        df.rename(columns={'codigo': 'Codigo', 'Qtd': 'Qtd_CD',
                  'datasalva': 'Data'}, inplace=True)
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(
            0).astype(int).astype(str)
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        return df
    except Exception as e:
        st.error(f"Erro ao carregar WMS: {e}")
        return pd.DataFrame(columns=['Codigo', 'Qtd_CD', 'Data'])

# =========================================================
#  💾 SALVAR PEDIDO NO BANCO
# =========================================================


def save_order_to_db(pedido_final: list[dict]):
    try:
        engine = get_engine()
        conn = engine.raw_connection()
        c = conn.cursor()

        data_pedido = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        usuario = st.session_state.get('username', 'desconhecido')
        placeholders = ", ".join(["?"] * len(LISTA_LOJAS))
        cols_lojas = ", ".join([f"loja_{l}" for l in LISTA_LOJAS])

        query = f"""
            INSERT INTO pedidos_consolidados (
                codigo, produto, ean, embseparacao,
                data_pedido, data_aprovacao, usuario_pedido,
                status_item, {cols_lojas}, total_cx, status_aprovacao
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, {placeholders}, ?, ?)
        """

        for item in pedido_final:
            vals_lojas = [item.get(f"loja_{l}", 0) for l in LISTA_LOJAS]
            emb_val = int(pd.to_numeric(
                item.get("embseparacao", 0), errors="coerce") or 0)
            params = (
                item["Codigo"], item["Produto"], item["EAN"], emb_val,
                data_pedido, None, usuario, item["Status"],
                *vals_lojas, item["Total_CX"], "Pendente"
            )
            c.execute(query, params)

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

# =========================================================
#  📊 HISTÓRICO DE PEDIDOS
# =========================================================


@st.cache_data(ttl=60)
def get_recent_orders_display(username: str) -> pd.DataFrame:
    try:
        engine = get_engine()
        dt_lim = (datetime.now() - timedelta(days=3)
                  ).strftime('%Y-%m-%d 00:00:00')
        q = text("""
            SELECT codigo AS "Cód", produto AS "Produto",
                   embseparacao AS "Emb", total_cx AS "Total",
                   status_aprovacao AS "Status",
                   data_pedido AS "Data"
            FROM pedidos_consolidados
            WHERE usuario_pedido = :username
              AND data_pedido >= :dt_lim
            ORDER BY data_pedido DESC
        """)
        df = pd.read_sql_query(q, con=engine, params={
                               "username": username, "dt_lim": dt_lim})
        df["Emb"] = pd.to_numeric(
            df["Emb"], errors='coerce').fillna(0).astype(int)
        return df
    except Exception as e:
        st.error(f"Erro ao ler histórico: {e}")
        return pd.DataFrame()

# =========================================================
#  🧭 INTERFACE PRINCIPAL
# =========================================================


def show_pedidos_page():
    st.title("🛒 Digitação de Pedidos")

    if 'pedido_atual' not in st.session_state:
        st.session_state.pedido_atual = []

    df_mix = load_mix_data(MIX_FILE_PATH)
    df_hist = load_historico_data(HIST_FILE_PATH)
    df_wms = load_wms_data(WMS_FILE_PATH)

    if df_mix.empty:
        st.warning("Falha ao carregar o Mix de Produtos.")
        st.stop()

    lojas_user = st.session_state.get('lojas_acesso', [])
    if not lojas_user:
        st.warning("Sem acesso a lojas.")
        st.stop()

    st.subheader("1. Buscar Produto")
    df_mix_user = df_mix[df_mix['Loja'].isin(lojas_user)].copy()

    tab1, tab2, tab3 = st.tabs(["Por Produto", "Por Código", "Por EAN"])
    prod_sel = None

    with tab1:
        busca_nome = st.text_input("Nome do Produto:")
        if busca_nome:
            res = df_mix_user[df_mix_user['Produto'].str.contains(
                busca_nome, case=False, na=False)]
            unicos = res.drop_duplicates(subset=['Codigo'])
            unicos['Show'] = unicos['Produto'] + \
                " (Cód: " + unicos['Codigo'] + ")"
            sel = st.selectbox(
                "Selecione:", ["Selecione..."] + unicos['Show'].tolist())
            if sel != "Selecione...":
                cod = re.search(r'\(Cód: (.*?)\)', sel).group(1)
                prod_sel = df_mix[df_mix['Codigo'] == cod].iloc[0]

    with tab2:
        busca_cod = st.text_input("Código:")
        if busca_cod:
            res = df_mix[df_mix['Codigo'] == busca_cod.strip()]
            if not res.empty:
                prod_sel = res.iloc[0]
            else:
                st.warning("Código não encontrado.")

    with tab3:
        busca_ean = st.text_input("EAN:")
        if busca_ean:
            res = df_mix[df_mix['EAN'] == busca_ean.strip()]
            if not res.empty:
                prod_sel = res.iloc[0]
            else:
                st.warning("EAN não encontrado.")

    st.markdown("---")

    if prod_sel is not None:
        st.subheader("2. Distribuir Quantidades (Caixas)")
        cod = prod_sel['Codigo']
        emb = int(prod_sel.get('embseparacao', 0))
        st.info(
            f"**Item:** {prod_sel['Produto']} (Cód: {cod}) | **Emb:** {emb} un/cx")

        with st.form("form_qty"):
            qtys, total = {}, 0
            cols = st.columns(min(len(lojas_user), 3))
            for i, loja in enumerate(lojas_user):
                q = cols[i % len(cols)].number_input(
                    f"Loja {loja}", min_value=0, step=1, key=f"q_{cod}_{loja}")
                if q > 0:
                    qtys[f"loja_{loja}"] = q
                    total += q

            if st.form_submit_button("Adicionar ao Pedido"):
                if total > 0:
                    st.session_state.pedido_atual.append({
                        "Codigo": cod, "Produto": prod_sel["Produto"],
                        "EAN": prod_sel["EAN"], "embseparacao": emb,
                        "Status": "Ativo", "Total_CX": total, **qtys
                    })
                    st.success("Item adicionado!")
                else:
                    st.warning("Digite ao menos uma quantidade.")

    st.markdown("---")
    st.subheader("3. Pedido Atual")
    if st.session_state.pedido_atual:
        df_ped = pd.DataFrame(st.session_state.pedido_atual)
        st.dataframe(df_ped, hide_index=True, use_container_width=True)
        c1, c2 = st.columns(2)
        if c1.button("Salvar Pedido", type="primary"):
            if save_order_to_db(st.session_state.pedido_atual):
                st.success("Salvo com sucesso!")
                st.session_state.pedido_atual = []
                get_recent_orders_display.clear()
                st.rerun()
            else:
                st.error("Erro ao salvar.")
        if c2.button("Limpar"):
            st.session_state.pedido_atual = []
            st.rerun()
    else:
        st.info("Carrinho vazio.")

    st.markdown("---")
    st.subheader("4. Histórico Recente")
    df_rec = get_recent_orders_display(st.session_state.get('username', ''))
    if not df_rec.empty:
        st.dataframe(df_rec, hide_index=True, use_container_width=True)
    else:
        st.info("Sem pedidos recentes.")
