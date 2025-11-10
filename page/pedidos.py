import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import sqlite3
import json
import re
import os # <-- 1. ADICIONE ESTA LINHA

# --- Configurações Iniciais ---
# Arquivos Excel ficam na pasta 'data' do código
MIX_FILE_PATH = 'data/__MixAtivoSistema.xlsx'
HIST_FILE_PATH = 'data/historico_solic.xlsm'
WMS_FILE_PATH = 'data/WMS.xlsm'

# Banco de dados .db vai para o Disco Persistente
BASE_DATA_PATH = os.environ.get('RENDER_DISK_PATH', 'data')
os.makedirs(BASE_DATA_PATH, exist_ok=True) # Garante que a pasta exista
PEDIDOS_DB_PATH = os.path.join(BASE_DATA_PATH, 'pedidos.db') 

LISTA_LOJAS = ["001", "002", "003", "004", "005", "006",
               "007", "008", "011", "012", "013", "014", "017", "018"]

# --- Nomes das Colunas (Mix) ---
COL_MIX_CODIGO = 'CODIGOINT'
COL_MIX_EAN = 'CODIGOEAN'
COL_MIX_PRODUTO = 'DESCRICAO'
COL_MIX_EMBALAGEM = 'EmbSeparacao'
COL_MIX_MIX = 'ltmix'
COL_MIX_LOJA = 'LOJA'

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

# --- Carregamento de Dados ---

@st.cache_data
def load_wms_data(file_path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_excel(
            file_path,
            sheet_name='WMS',
            usecols=[COL_WMS_CODIGO, COL_WMS_QTD, COL_WMS_DATA]
        )
        df.rename(columns={
            COL_WMS_CODIGO: 'Codigo',
            COL_WMS_QTD: 'Qtd_CD',
            COL_WMS_DATA: 'Data'
        }, inplace=True)

        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df['Codigo'] = df['Codigo'].astype(str).str.split('.').str[0]
        df.dropna(subset=['Data', 'Codigo'], inplace=True)

        if df.empty:
            return pd.DataFrame(columns=['Codigo', 'Qtd_CD', 'Data'])

        latest_date = df['Data'].dt.date.max()
        df_latest = df[df['Data'].dt.date == latest_date].copy()
        return df_latest
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo WMS ({file_path}): {e}")
        return pd.DataFrame(columns=['Codigo', 'Qtd_CD', 'Data'])


@st.cache_data
def load_mix_data(file_path: str) -> Optional[pd.DataFrame]:
    try:
        colunas_para_ler = [
            COL_MIX_CODIGO, COL_MIX_EAN, COL_MIX_PRODUTO,
            COL_MIX_EMBALAGEM, COL_MIX_MIX, COL_MIX_LOJA
        ]
        df = pd.read_excel(file_path, sheet_name=0, usecols=colunas_para_ler)
        df.rename(columns={
            COL_MIX_CODIGO: 'Codigo',
            COL_MIX_EAN: 'EAN',
            COL_MIX_PRODUTO: 'Produto',
            COL_MIX_EMBALAGEM: 'Embalagem',
            COL_MIX_MIX: 'Mix',
            COL_MIX_LOJA: 'Loja'
        }, inplace=True)

        df['Codigo'] = df['Codigo'].astype(str).str.split('.').str[0]
        df.dropna(subset=['Codigo'], inplace=True)
        df['EAN'] = df['EAN'].astype(str).str.split('.').str[0]
        df['Loja'] = df['Loja'].astype(str).str.zfill(3)
        df['Produto'] = df['Produto'].astype(str)
        df['Embalagem'] = pd.to_numeric(df['Embalagem'], errors='coerce').fillna(0).astype(int)
        return df
    except Exception as e:
        st.error(f"Erro ao ler o arquivo de Mix: {e}")
        return None


@st.cache_data
def load_historico_data(file_path: str) -> Optional[pd.DataFrame]:
    try:
        colunas_para_ler = [
            COL_HIST_CODIGO, COL_HIST_LOJA, COL_HIST_DATA, COL_HIST_EST_LOJA,
            COL_HIST_PED_LOJA, COL_K, COL_L, COL_O,
            COL_HIST_EMBALAGEM_HIST
        ]
        df = pd.read_excel(file_path, sheet_name=0, usecols=colunas_para_ler)
        df.rename(columns={
            COL_HIST_CODIGO: 'Codigo',
            COL_HIST_LOJA: 'Loja',
            COL_HIST_DATA: 'Data',
            COL_HIST_EST_LOJA: 'Estoque_Loja',
            COL_HIST_PED_LOJA: 'Pedidos_Loja',
            COL_HIST_EMBALAGEM_HIST: 'Embalagem',
            COL_K: 'Vd1',
            COL_L: 'Vd2',
            COL_O: 'Cob'
        }, inplace=True)

        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df['Codigo'] = df['Codigo'].astype(str).str.split('.').str[0]
        df.dropna(subset=['Data', 'Codigo'], inplace=True)
        df['Loja'] = df['Loja'].astype(str).str.zfill(3)
        df['Embalagem'] = pd.to_numeric(df['Embalagem'], errors='coerce').fillna(0).astype(int)
        cols_metricas = ['Estoque_Loja', 'Pedidos_Loja', 'Vd1', 'Vd2', 'Cob']
        for col in cols_metricas:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df = df.sort_values('Data', ascending=False).drop_duplicates(subset=['Codigo', 'Loja'])
        return df
    except Exception as e:
        st.error(f"Erro ao ler o arquivo de Histórico: {e}")
        return pd.DataFrame()


def get_cd_stock_in_caixas(df_wms_latest, df_mix_full, df_hist_full, product_code=None):
    if df_wms_latest.empty:
        return 0
    embalagem_map = df_mix_full[['Codigo', 'Embalagem']].drop_duplicates(subset=['Codigo'])
    if not df_hist_full.empty and 'Embalagem' in df_hist_full.columns:
        embalagem_map_hist = df_hist_full[['Codigo', 'Embalagem']].drop_duplicates(subset=['Codigo'])
        embalagem_map = embalagem_map.merge(
            embalagem_map_hist, on='Codigo', how='left', suffixes=('_mix', '_hist'))
        embalagem_map['Embalagem'] = embalagem_map['Embalagem_mix'].where(
            embalagem_map['Embalagem_mix'] > 0, embalagem_map['Embalagem_hist']
        )
    else:
        if 'Embalagem_mix' in embalagem_map.columns:
            embalagem_map = embalagem_map.rename(columns={'Embalagem_mix': 'Embalagem'})
    if 'Embalagem' not in embalagem_map.columns:
         embalagem_map['Embalagem'] = 0
    wms_stock_units = df_wms_latest.groupby('Codigo')['Qtd_CD'].sum().reset_index()
    df_merged = wms_stock_units.merge(embalagem_map[['Codigo', 'Embalagem']], on='Codigo', how='left')
    df_merged['Embalagem'] = df_merged['Embalagem'].fillna(0)
    df_merged['Estoque_CD_Caixas'] = df_merged.apply(
        lambda row: (row['Qtd_CD'] / row['Embalagem']) if row['Embalagem'] > 0 else 0, axis=1
    )
    df_merged['Estoque_CD_Caixas'] = df_merged['Estoque_CD_Caixas'].apply(
        lambda x: int(x) if pd.notna(x) else 0)
    if product_code:
        df_item = df_merged[df_merged['Codigo'] == product_code]
        if df_item.empty:
            return 0
        return df_item['Estoque_CD_Caixas'].sum()
    return 0

# --- CORREÇÃO AQUI ---
def save_order_to_db(pedido_final: List[dict]):
    """Salva o pedido consolidado no banco de dados 'pedidos.db'."""
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        c = conn.cursor()

        data_pedido_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # No seu app.py, o usuário logado está em 'username'
        usuario = st.session_state.get('username', 'desconhecido')

        loja_cols_names = [f"loja_{loja}" for loja in LISTA_LOJAS]
        loja_cols_placeholders = ", ".join(["?"] * len(LISTA_LOJAS))

        # Query CORRIGIDA para bater com a estrutura do seu app.py
        # Adiciona 'data_aprovacao' (que será Nula)
        query = f"""
            INSERT INTO pedidos_consolidados (
                codigo, produto, ean, embalagem, data_pedido, data_aprovacao,
                usuario_pedido, status_item,
                {", ".join(loja_cols_names)}, total_cx, status_aprovacao
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, {loja_cols_placeholders}, ?, ?) 
        """

        for item in pedido_final:
            qtys_lojas = []
            for loja in LISTA_LOJAS:
                qtys_lojas.append(item.get(f"loja_{loja}", 0))

            # Parâmetros CORRIGIDOS para bater com a query
            params = (
                item['Codigo'],       # 1. codigo
                item['Produto'],      # 2. produto
                item['EAN'],          # 3. ean
                item['Embalagem'],    # 4. embalagem (int)
                data_pedido_str,      # 5. data_pedido
                None,                 # 6. data_aprovacao (fica Nulo ao criar)
                usuario,              # 7. usuario_pedido
                item['Status'],       # 8. status_item
                *qtys_lojas,          # 9. ...lojas...
                item['Total_CX'],     # 10. total_cx
                'Pendente'            # 11. status_aprovacao
            )
            c.execute(query, params)

        conn.commit()
        return True

    except sqlite3.Error as e:
        st.error(f"Erro ao salvar pedido no banco de dados: {e}")
        if conn:
            conn.rollback()
        return False

    finally:
        if conn:
            conn.close()
# --- FIM DA CORREÇÃO ---

# --- Lógica da Página ---
def show_pedidos_page():
    st.title("🛒 Digitação de Pedidos")

    if 'pedido_atual' not in st.session_state:
        st.session_state.pedido_atual = []

    df_mix_full = load_mix_data(MIX_FILE_PATH)
    df_hist_full = load_historico_data(HIST_FILE_PATH)
    df_wms = load_wms_data(WMS_FILE_PATH)

    if df_mix_full is None: st.stop()
    if df_hist_full is None: df_hist_full = pd.DataFrame()
    if df_wms is None: df_wms = pd.DataFrame()

    # No seu app.py, as lojas estão em 'lojas_acesso'
    lojas_do_usuario = st.session_state.get('lojas_acesso', []) 
    if not lojas_do_usuario:
        st.warning("Seu usuário não tem acesso a nenhuma loja. Contate o administrador.")
        st.stop()

    st.subheader("1. Buscar Produto")
    df_mix_usuario = df_mix_full[df_mix_full['Loja'].isin(lojas_do_usuario)].copy()
    tab1, tab2, tab3 = st.tabs(["Buscar por Produto", "Buscar por Código", "Buscar por EAN"])
    produto_selecionado = None

    with tab1:
        produto_busca = st.text_input("Digite o nome do Produto:")
        if produto_busca:
            resultados_parciais = df_mix_usuario[
                df_mix_usuario['Produto'].str.contains(produto_busca, case=False, na=False)
            ]
            opcoes_unicas = resultados_parciais.drop_duplicates(subset=['Codigo'])
            opcoes_unicas['Display'] = opcoes_unicas['Produto'] + \
                " (Cód: " + opcoes_unicas['Codigo'].astype(str) + ")"
            lista_opcoes = ["Selecione um item..."] + opcoes_unicas['Display'].tolist()
            item_selecionado_display = st.selectbox(
                "Selecione o produto na lista:", lista_opcoes, key="sel_prod_desc")
            if item_selecionado_display and item_selecionado_display != "Selecione um item...":
                try:
                    codigo_extraido = re.search(r'\(Cód: (.*?)\)', item_selecionado_display).group(1)
                    produto_selecionado = df_mix_full[df_mix_full['Codigo'] == codigo_extraido].iloc[0]
                except (AttributeError, ValueError, IndexError):
                    st.error("Não foi possível selecionar o item.")

    with tab2:
        codigo_busca = st.text_input("Digite o Código do Produto:")
        if codigo_busca:
            try:
                resultados = df_mix_full[df_mix_full['Codigo'] == codigo_busca] 
                if not resultados.empty:
                    produto_selecionado = resultados.iloc[0]
                else: st.warning("Código não encontrado no Mix.")
            except (ValueError, IndexError): st.warning("Erro ao buscar código.")

    with tab3:
        ean_busca = st.text_input("Digite o Código EAN:")
        if ean_busca:
            try:
                resultados = df_mix_full[df_mix_full['EAN'] == ean_busca]
                if not resultados.empty:
                    produto_selecionado = resultados.iloc[0]
                else: st.warning("EAN não encontrado no Mix.")
            except (ValueError, IndexError): st.warning("Por favor, digite um EAN válido.")

    st.markdown("---")

    if produto_selecionado is not None:
        st.subheader("2. Distribuir Quantidades (em Caixas)")
        codigo_str = produto_selecionado['Codigo'] 
        embalagem = produto_selecionado['Embalagem']
        estoque_cd_caixas = get_cd_stock_in_caixas(df_wms, df_mix_full, df_hist_full, codigo_str) 

        st.info(f"""
            **Item:** {produto_selecionado['Produto']} (Cód: {codigo_str}) | 
            **Embalagem:** {embalagem} un/cx | 
            **Estoque CD (Atual):** {estoque_cd_caixas} Caixas
        """)

        with st.form(key="form_digitar_quantidades"):
            quantidades_digitadas = {}
            total_cx_item = 0
            st.markdown("**Digite as quantidades (CX) para cada loja:**")
            num_colunas_grade = 3
            linhas_lojas = [lojas_do_usuario[i:i + num_colunas_grade]
                            for i in range(0, len(lojas_do_usuario), num_colunas_grade)]
            status_item_geral = "Suspenso"

            for linha in linhas_lojas:
                cols = st.columns(num_colunas_grade)
                for i, loja in enumerate(linha):
                    col = cols[i]
                    df_loja_item_mix = df_mix_usuario[
                        (df_mix_usuario['Codigo'] == codigo_str) & (df_mix_usuario['Loja'] == loja)
                    ]
                    df_loja_item_hist = df_hist_full[
                        (df_hist_full['Codigo'] == codigo_str) & (df_hist_full['Loja'] == loja)
                    ]
                    info_loja_str = ""
                    status_item_loja = "Suspenso"

                    if not df_loja_item_mix.empty:
                        if df_loja_item_mix.iloc[0]['Mix'] == 'A':
                            status_item_loja = "Ativo"
                            status_item_geral = "Ativo"

                    if not df_loja_item_hist.empty:
                        latest_data_row = df_loja_item_hist.iloc[0]
                        data_ref = latest_data_row['Data'].strftime('%d/%m/%Y')
                        est_loja = latest_data_row['Estoque_Loja']
                        vd1 = latest_data_row['Vd1']
                        vd2 = latest_data_row['Vd2']
                        cob = latest_data_row['Cob']
                        ped_cx = latest_data_row['Pedidos_Loja']
                        info_loja_str += f"**Status Mix: {status_item_loja} (Ref: {data_ref})** \n"
                        info_loja_str += f"Est. Loja: {est_loja:.1f} | Ped. Loja: {ped_cx:.0f} \n"
                        info_loja_str += f"Vd1: {vd1:.1f} | Vd2: {vd2:.1f} | Cob: {cob:.1f}"
                    else:
                        info_loja_str += f"**Status Mix: {status_item_loja}** (Sem dados históricos)"

                    label_loja = f"Loja {loja}"
                    if status_item_loja == "Suspenso": label_loja += " (Suspenso)"
                    qty = col.number_input(
                        label_loja, min_value=0, step=1, key=f"qty_{codigo_str}_{loja}"
                    )
                    col.caption(info_loja_str)
                    if qty > 0:
                        quantidades_digitadas[f"loja_{loja}"] = qty
                        total_cx_item += qty

            if st.form_submit_button("Adicionar Item ao Pedido"):
                if total_cx_item > 0:
                    item_data = {
                        "Codigo": codigo_str, 
                        "Produto": produto_selecionado['Produto'],
                        "EAN": produto_selecionado['EAN'],
                        "Embalagem": embalagem,
                        "Status": status_item_geral,
                        "Total_CX": total_cx_item
                    }
                    item_data.update(quantidades_digitadas)
                    st.session_state.pedido_atual.append(item_data)
                    st.success(f"{produto_selecionado['Produto']} adicionado ao pedido!")
                else: st.warning("Nenhuma quantidade foi digitada.")

    st.markdown("---")
    st.subheader("3. Pedido Atual")

    if not st.session_state.pedido_atual:
        st.info("Nenhum item no pedido ainda.")
    else:
        df_pedido = pd.DataFrame(st.session_state.pedido_atual)
        colunas_info = ['Codigo', 'Produto', 'Embalagem', 'Status', 'Total_CX']
        colunas_loja = [
            f"loja_{loja}" for loja in lojas_do_usuario if f"loja_{loja}" in df_pedido.columns]
        df_pedido_display = df_pedido[colunas_info + colunas_loja]
        col_config_pedido = {"Embalagem": st.column_config.NumberColumn(format="%d")}
        st.dataframe(
            df_pedido_display, hide_index=True, use_container_width=True, column_config=col_config_pedido
        )
        col_final1, col_final2 = st.columns(2)
        with col_final1:
            if st.button("Salvar Pedido no Sistema", type="primary"):
                if save_order_to_db(st.session_state.pedido_atual):
                    st.success("Pedido salvo com sucesso!")
                    st.session_state.pedido_atual = []
                    if 'get_recent_orders_display' in globals() or 'get_recent_orders_display' in locals():
                        get_recent_orders_display.clear()
                    st.rerun()
                else: st.error("Falha ao salvar o pedido.")
        with col_final2:
            if st.button("Limpar Pedido Atual"):
                st.session_state.pedido_atual = []
                st.rerun()

    st.markdown("---")
    st.subheader("4. Seus Pedidos Recentes (Últimos 3 dias)")

    @st.cache_data(ttl=60)
    def get_recent_orders_display(username: str) -> pd.DataFrame:
        conn = None
        try:
            conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
            data_limite = datetime.now() - timedelta(days=3)
            data_limite_str = data_limite.strftime('%Y-%m-%d 00:00:00')
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
            return df
        except sqlite3.Error as e:
            st.error(f"Erro ao buscar histórico de pedidos: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    username_atual = st.session_state.get('username', 'desconhecido')
    df_recentes = get_recent_orders_display(username_atual)
    if df_recentes.empty:
        st.info("Nenhum pedido recente encontrado para seu usuário.")
    else:
        col_config_hist = {"Embalagem": st.column_config.NumberColumn(format="%d")}
        st.dataframe(
            df_recentes, hide_index=True, use_container_width=True, column_config=col_config_hist
        )