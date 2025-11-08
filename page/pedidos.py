import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import sqlite3
import json
import re
import os

# --- Configurações Iniciais ---
MIX_FILE_PATH = 'data/__MixAtivoSistema.xlsx'
HIST_FILE_PATH = 'data/historico_solic.xlsm'
WMS_FILE_PATH = 'data/WMS.xlsm'
PEDIDOS_DB_PATH = 'data/pedidos.db' 

LISTA_LOJAS = ["001", "002", "003", "004", "005", "006",
               "007", "008", "011", "012", "013", "014", "017", "018"]

# --- Nomes das Colunas (Mix) ---
COLS_MIX_MAP = {
    'CODIGOINT': 'Codigo',       # Col A (Int 7)
    'CODIGOEAN': 'EAN',          # Col B (Str)
    'DESCRICAO': 'Produto',      # Col C (Str)
    'LOJA': 'Loja',              # Col D (Str 3 digits)
    'EmbSeparacao': 'Embalagem', # Col E (Int)
    'PPCX': 'PPCX',              # Col F (Float 1 decimal)
    'EICX': 'EICX',              # Col G (Float 1 decimal)
    'CapCX': 'CapCX',            # Col H (Float 1 decimal)
    'ltmix': 'Mix',              # Col I (Str)
}

# --- Nomes das Colunas (Histórico) ---
COL_HIST_CODIGO = 'CODIGOINT'      
COL_HIST_LOJA = 'LOJA'
COL_HIST_EMBALAGEM_HIST = 'EmbSeparacao'
COL_HIST_EST_LOJA = 'EstCX'       
COL_HIST_PED_LOJA = 'PedCX'       
COL_K = 'Vd1sem-CX'                 
COL_L = 'Vd2sem-CX'                 
COL_O = 'CobEstq+Ped'              
COL_HIST_DATA = 'DtSolicitacao'    

# --- Nomes das Colunas (WMS) ---
COL_WMS_CODIGO = 'codigo'        
COL_WMS_QTD = 'Qtd'              
COL_WMS_DATA = 'datasalva'       

# --- CARREGAMENTO E TRATAMENTO DE DADOS ---

@st.cache_data
def load_mix_data(file_path: str) -> Optional[pd.DataFrame]:
    """
    Carrega o Mix com tratamento RIGOROSO de tipos de dados.
    """
    try:
        cols_to_use = list(COLS_MIX_MAP.keys())
        # Lê tudo como string primeiro
        df = pd.read_excel(file_path, sheet_name=0, usecols=cols_to_use, dtype=str)
        df.rename(columns=COLS_MIX_MAP, inplace=True)

        # Coluna A: CODIGOINT (Inteiro, máx 7 dígitos)
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(0).astype(int)
        df = df[(df['Codigo'] > 0) & (df['Codigo'].astype(str).str.len() <= 7)]
        df['Codigo'] = df['Codigo'].astype(str) # Converte para string para merges

        # Colunas B, C, D, I: Strings (Texto)
        for col_str in ['EAN', 'Produto', 'Mix']:
             df[col_str] = df[col_str].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df['Loja'] = df['Loja'].astype(str).str.split('.').str[0].str.zfill(3)

        # --- CORREÇÃO DEFINITIVA DA EMBALAGEM ---
        # Coluna E: EmbSeparacao (Inteiro)
        # Pega o primeiro item antes de qualquer vírgula ou ponto
        df['Embalagem'] = df['Embalagem'].astype(str).str.split(',').str[0].str.split('.').str[0].str.strip()
        df['Embalagem'] = pd.to_numeric(df['Embalagem'], errors='coerce').fillna(0).astype(int)
        # --- FIM DA CORREÇÃO ---

        # Colunas F, G, H: Decimais (1 casa)
        for col_float in ['PPCX', 'EICX', 'CapCX']:
            if col_float in df.columns:
                df[col_float] = pd.to_numeric(df[col_float], errors='coerce').fillna(0.0).round(1)

        return df

    except FileNotFoundError:
        st.error(f"Arquivo de Mix '{file_path}' não encontrado.")
        return None
    except Exception as e:
        st.error(f"Erro crítico ao tratar o arquivo de Mix: {e}")
        return None

@st.cache_data
def load_wms_data(file_path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_excel(file_path, sheet_name='WMS', usecols=[COL_WMS_CODIGO, COL_WMS_QTD, COL_WMS_DATA])
        df.rename(columns={COL_WMS_CODIGO: 'Codigo', COL_WMS_QTD: 'Qtd_CD', COL_WMS_DATA: 'Data'}, inplace=True)
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(0).astype(int).astype(str)
        df = df[df['Codigo'] != '0'] 
        df.dropna(subset=['Data'], inplace=True)
        if df.empty: return pd.DataFrame(columns=['Codigo', 'Qtd_CD', 'Data'])
        latest_date = df['Data'].dt.date.max()
        return df[df['Data'].dt.date == latest_date].copy()
    except Exception as e:
        st.error(f"Erro ao carregar WMS: {e}")
        return pd.DataFrame(columns=['Codigo', 'Qtd_CD', 'Data'])

@st.cache_data
def load_historico_data(file_path: str) -> Optional[pd.DataFrame]:
    try:
        cols = [COL_HIST_CODIGO, COL_HIST_LOJA, COL_HIST_DATA, COL_HIST_EST_LOJA, COL_HIST_PED_LOJA, COL_K, COL_L, COL_O, COL_HIST_EMBALAGEM_HIST]
        df = pd.read_excel(file_path, sheet_name=0, usecols=cols, dtype=str) # Lê tudo como str
        df.rename(columns={COL_HIST_CODIGO: 'Codigo', COL_HIST_LOJA: 'Loja', COL_HIST_DATA: 'Data', 
                           COL_HIST_EST_LOJA: 'Estoque_Loja', COL_HIST_PED_LOJA: 'Pedidos_Loja', 
                           COL_HIST_EMBALAGEM_HIST: 'Embalagem', COL_K: 'Vd1', COL_L: 'Vd2', COL_O: 'Cob'}, inplace=True)

        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(0).astype(int).astype(str)
        df = df[df['Codigo'] != '0']
        df['Loja'] = df['Loja'].astype(str).str.split('.').str[0].str.zfill(3)
        
        # --- CORREÇÃO DEFINITIVA DA EMBALAGEM (HISTÓRICO) ---
        df['Embalagem'] = df['Embalagem'].astype(str).str.split(',').str[0].str.split('.').str[0].str.strip()
        df['Embalagem'] = pd.to_numeric(df['Embalagem'], errors='coerce').fillna(0).astype(int)
        # --- FIM DA CORREÇÃO ---

        for col in ['Estoque_Loja', 'Pedidos_Loja', 'Vd1', 'Vd2', 'Cob']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        return df.sort_values('Data', ascending=False).drop_duplicates(subset=['Codigo', 'Loja'])
    except Exception as e:
        st.error(f"Erro ao ler Histórico: {e}")
        return pd.DataFrame()

def get_cd_stock_in_caixas(df_wms_latest, df_mix_full, df_hist_full, product_code=None):
    if df_wms_latest.empty: return 0
    embalagem_map = df_mix_full[['Codigo', 'Embalagem']].drop_duplicates(subset=['Codigo'])
    if not df_hist_full.empty and 'Embalagem' in df_hist_full.columns:
        embalagem_map_hist = df_hist_full[['Codigo', 'Embalagem']].drop_duplicates(subset=['Codigo'])
        embalagem_map = embalagem_map.merge(embalagem_map_hist, on='Codigo', how='left', suffixes=('_mix', '_hist'))
        embalagem_map['Embalagem'] = embalagem_map['Embalagem_mix'].where(embalagem_map['Embalagem_mix'] > 0, embalagem_map['Embalagem_hist'])
    else:
        if 'Embalagem_mix' in embalagem_map.columns: embalagem_map.rename(columns={'Embalagem_mix': 'Embalagem'}, inplace=True)
    if 'Embalagem' not in embalagem_map.columns: embalagem_map['Embalagem'] = 0
    wms_stock = df_wms_latest.groupby('Codigo')['Qtd_CD'].sum().reset_index()
    df_merged = wms_stock.merge(embalagem_map[['Codigo', 'Embalagem']], on='Codigo', how='left')
    df_merged['Embalagem'] = df_merged['Embalagem'].fillna(0).astype(int)
    df_merged['Estoque_CD_Caixas'] = df_merged.apply(lambda x: int(x['Qtd_CD'] / x['Embalagem']) if x['Embalagem'] > 0 else 0, axis=1)
    if product_code:
        product_code_str = str(product_code) # Compara string com string
        item = df_merged[df_merged['Codigo'] == product_code_str]
        return item['Estoque_CD_Caixas'].sum() if not item.empty else 0
    return 0

def save_order_to_db(pedido_final: List[dict]):
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        c = conn.cursor()
        data_pedido = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        usuario = st.session_state.get('username', 'desconhecido')
        placeholders = ", ".join(["?"] * len(LISTA_LOJAS))
        cols_lojas = ", ".join([f"loja_{l}" for l in LISTA_LOJAS])

        query = f"""
            INSERT INTO pedidos_consolidados (
                codigo, produto, ean, embalagem, data_pedido, data_aprovacao,
                usuario_pedido, status_item, {cols_lojas}, total_cx, status_aprovacao
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, {placeholders}, ?, ?) 
        """
        for item in pedido_final:
            vals_lojas = [item.get(f"loja_{l}", 0) for l in LISTA_LOJAS]
            params = (item['Codigo'], item['Produto'], item['EAN'], item['Embalagem'],
                      data_pedido, None, usuario, item['Status'], *vals_lojas,
                      item['Total_CX'], 'Pendente')
            c.execute(query, params)
        conn.commit()
        return True
    except sqlite3.Error as e:
        st.error(f"Erro ao salvar: {e}")
        return False
    finally:
        if conn: conn.close()

# --- Lógica da Página ---
def show_pedidos_page():

    # --- MENSAGEM DE TESTE ---
    st.error("TESTE: RODANDO SCRIPT ATUALIZADO (v11_23_AM)")
    # --- FIM DA MENSAGEM DE TESTE ---

    st.title("🛒 Digitação de Pedidos")
    if 'pedido_atual' not in st.session_state: st.session_state.pedido_atual = []

    df_mix = load_mix_data(MIX_FILE_PATH)
    df_hist = load_historico_data(HIST_FILE_PATH)
    df_wms = load_wms_data(WMS_FILE_PATH)

    if df_mix is None: 
        st.warning("Falha ao carregar o Mix de Produtos. A página não pode continuar.")
        st.stop()
    if df_hist is None: df_hist = pd.DataFrame()
    if df_wms is None: df_wms = pd.DataFrame()

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
            res = df_mix_user[df_mix_user['Produto'].str.contains(busca_nome, case=False, na=False)]
            unicos = res.drop_duplicates(subset=['Codigo'])
            unicos['Show'] = unicos['Produto'] + " (Cód: " + unicos['Codigo'] + ")"
            opts = ["Selecione..."] + unicos['Show'].tolist()
            sel = st.selectbox("Selecione:", opts, key="sel_nome")
            if sel != "Selecione...":
                 cod = re.search(r'\(Cód: (.*?)\)', sel).group(1)
                 prod_sel = df_mix[df_mix['Codigo'] == cod].iloc[0]

    with tab2:
        busca_cod = st.text_input("Código:")
        if busca_cod:
            # Compara string com string
            res = df_mix[df_mix['Codigo'] == busca_cod.strip()]
            if not res.empty: prod_sel = res.iloc[0]
            else: st.warning("Código não encontrado.")

    with tab3:
        busca_ean = st.text_input("EAN:")
        if busca_ean:
            res = df_mix[df_mix['EAN'] == busca_ean.strip()]
            if not res.empty: prod_sel = res.iloc[0]
            else: st.warning("EAN não encontrado.")

    st.markdown("---")

    if prod_sel is not None:
        st.subheader("2. Distribuir Quantidades (Caixas)")
        cod, emb = prod_sel['Codigo'], prod_sel['Embalagem']
        est_cd = get_cd_stock_in_caixas(df_wms, df_mix, df_hist, cod)
        st.info(f"**Item:** {prod_sel['Produto']} (Cód: {cod}) | **Emb:** {emb} un/cx | **Estoque CD:** {est_cd} CX")

        with st.form("form_qty"):
            qtys, total = {}, 0
            # Define o número de colunas (máximo 3)
            num_cols = min(len(lojas_user), 3)
            if num_cols == 0: num_cols = 1 # Evita erro
            
            cols = st.columns(num_cols)
            for i, loja in enumerate(lojas_user):
                col = cols[i % num_cols]
                mix_loja = df_mix_user[(df_mix_user['Codigo'] == cod) & (df_mix_user['Loja'] == loja)]
                hist_loja = df_hist[(df_hist['Codigo'] == cod) & (df_hist['Loja'] == loja)]
                
                status = "Ativo" if not mix_loja.empty and mix_loja.iloc[0]['Mix'] == 'A' else "Suspenso"
                info = f"**Mix: {status}**"
                
                if not hist_loja.empty:
                    hl = hist_loja.iloc[0]
                    info += f" (Ref: {hl['Data'].strftime('%d/%m/%Y')})\n"
                    info += f"Est: {hl['Estoque_Loja']:.1f} | Ped: {hl['Pedidos_Loja']:.0f}\n"
                    info += f"Vd1: {hl['Vd1']:.1f} | Vd2: {hl['Vd2']:.1f} | Cob: {hl['Cob']:.1f}"
                else:
                    info += " (Sem histórico)"

                label = f"Loja {loja} ({status})"
                q = col.number_input(label, min_value=0, step=1, key=f"q_{cod}_{loja}")
                col.caption(info)
                if q > 0: qtys[f"loja_{loja}"] = q; total += q

            if st.form_submit_button("Adicionar ao Pedido"):
                if total > 0:
                    st.session_state.pedido_atual.append({
                        "Codigo": cod, "Produto": prod_sel['Produto'], "EAN": prod_sel['EAN'],
                        "Embalagem": emb, "Status": status, "Total_CX": total, **qtys
                    })
                    st.success("Item adicionado!")
                else: st.warning("Digite ao menos uma quantidade.")

    st.markdown("---")
    st.subheader("3. Pedido Atual")
    if st.session_state.pedido_atual:
        df_ped = pd.DataFrame(st.session_state.pedido_atual)
        cols_show = ['Codigo', 'Produto', 'Embalagem', 'Status', 'Total_CX'] + [c for c in df_ped.columns if c.startswith('loja_')]
        st.dataframe(df_ped[cols_show], hide_index=True, use_container_width=True, 
                     column_config={"Embalagem": st.column_config.NumberColumn(format="%d")})
        
        c1, c2 = st.columns(2)
        if c1.button("Salvar Pedido", type="primary"):
            if save_order_to_db(st.session_state.pedido_atual):
                st.success("Salvo!")
                st.session_state.pedido_atual = []
                # Limpa os caches de dados
                load_mix_data.clear()
                load_historico_data.clear()
                load_wms_data.clear()
                get_recent_orders_display.clear()
                st.rerun()
            else: st.error("Erro ao salvar.")
        if c2.button("Limpar"):
            st.session_state.pedido_atual = []
            st.rerun()
    else: st.info("Carrinho vazio.")

    st.markdown("---")
    st.subheader("4. Histórico Recente")
    df_rec = get_recent_orders_display(st.session_state.get('username', ''))
    if not df_rec.empty:
        st.dataframe(df_rec, hide_index=True, use_container_width=True,
                     column_config={"Embalagem": st.column_config.NumberColumn(format="%d")})
    else: st.info("Sem pedidos recentes.")

# No seu page/pedidos.py, substitua a função 'get_recent_orders_display'
# (que está no final do arquivo) por esta:

@st.cache_data(ttl=60)
def get_recent_orders_display(username: str) -> pd.DataFrame:
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        data_limite = datetime.now() - timedelta(days=3)
        data_limite_str = data_limite.strftime('%Y-%m-%d 00:00:00')
        
        # Query original
        query = """
                SELECT
                    STRFTIME('%d/%m/%Y %H:%M', data_pedido) AS "Data Pedido",
                    codigo AS "Código",
                    produto AS "Produto",
                    embalagem AS "Embalagem",
                    status_item AS "Status Mix",
                    total_cx AS "Total CX",
                    status_aprovacao AS "Status Aprovação"
                FROM pedidos_consolidados
                WHERE usuario_pedido = ? AND data_pedido >= ?
                ORDER BY data_pedido DESC
        """
        df = pd.read_sql_query(query, conn, params=(username, data_limite_str))
        # --- CORREÇÃO DEFINITIVA DA EMBALAGEM ---
        if "Embalagem" in df.columns:
            df["Embalagem"] = df["Embalagem"].astype(str).str.split(',').str[0].str.split('.').str[0].str.strip()
            df["Embalagem"] = pd.to_numeric(df["Embalagem"], errors='coerce').fillna(0).astype(int)
        # --- FIM DA CORREÇÃO ---
            
        return df
    except sqlite3.Error as e:
        st.error(f"Erro ao buscar histórico de pedidos: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()