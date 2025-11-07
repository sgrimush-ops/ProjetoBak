import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import sqlite3
import json
import re
import os # Necessário para os paths dos arquivos

# --- Configurações Iniciais ---
MIX_FILE_PATH = 'data/__MixAtivoSistema.xlsx'
HIST_FILE_PATH = 'data/historico_solic.xlsm' # <-- ARQUIVO ADICIONAL
WMS_FILE_PATH = 'data/WMS.xlsm'

PEDIDOS_DB_PATH = 'data/pedidos.db'
LISTA_LOJAS = ["001", "002", "003", "004", "005", "006", "007", "008", "011", "012", "013", "014", "017", "018"]

# --- Nomes das Colunas (Mix) ---
COL_MIX_CODIGO = 'CODIGOINT'
COL_MIX_EAN = 'CODIGOEAN'
COL_MIX_PRODUTO = 'DESCRICAO' 
COL_MIX_EMBALAGEM = 'EmbSeparacao'
COL_MIX_MIX = 'ltmix' # <-- MANTIDO (Coluna I)
COL_MIX_LOJA = 'LOJA'

# --- Nomes das Colunas (Histórico) ---
COL_HIST_CODIGO = 'CODIGOINT'      # Coluna A
COL_HIST_LOJA = 'LOJA'             # Coluna D (Assumindo que também existe no histórico)
COL_HIST_EMBALAGEM_HIST = 'EmbSeparacao' # Coluna E (Usada para fallback do CD)
COL_HIST_EST_LOJA = 'EstCX'        # Coluna G
COL_HIST_PED_LOJA = 'PedCX'        # Coluna H
# COL_HIST_ESTQ_PED = 'EStq+Ped'     # <-- REMOVIDO PERMANENTEMENTE (Coluna J)
COL_K = 'Vd1sem-CX'                 # Coluna K (Nome corrigido)
COL_L = 'Vd2sem-CX'                 # Coluna L (Nome corrigido)
COL_O = 'CobEstq+Ped'              # Coluna O
COL_HIST_DATA = 'DtSolicitacao'    # Coluna R

# --- Nomes das Colunas (WMS) ---
COL_WMS_CODIGO = 'codigo'        # Coluna A
COL_WMS_QTD = 'Qtd'              # Coluna E
COL_WMS_DATA = 'datasalva'       # Coluna I


# --- Carregamento de Dados ---

@st.cache_data
def load_wms_data(file_path: str) -> Optional[pd.DataFrame]:
    """Carrega dados do WMS (estoque do CD) e retorna apenas o dia mais recente."""
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
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce')
        df.dropna(subset=['Data', 'Codigo'], inplace=True)
        df['Codigo'] = df['Codigo'].astype(int)
        
        if df.empty:
            st.warning("Arquivo WMS não contém dados válidos.")
            return pd.DataFrame(columns=['Codigo', 'Qtd_CD', 'Data'])
            
        latest_date = df['Data'].dt.date.max()
        df_latest = df[df['Data'].dt.date == latest_date].copy()
        
        return df_latest
        
    except FileNotFoundError:
        st.warning(f"Arquivo WMS ('{file_path}') não encontrado. Estoque do CD será 0.")
        return pd.DataFrame(columns=['Codigo', 'Qtd_CD', 'Data'])
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo WMS ({file_path}): {e}")
        return pd.DataFrame(columns=['Codigo', 'Qtd_CD', 'Data'])

@st.cache_data
def load_mix_data(file_path: str) -> Optional[pd.DataFrame]:
    """Carrega o arquivo de mix de produtos."""
    try:
        # Define as colunas a serem lidas do Mix
        colunas_para_ler = [
            COL_MIX_CODIGO, COL_MIX_EAN, COL_MIX_PRODUTO, 
            COL_MIX_EMBALAGEM, COL_MIX_MIX, COL_MIX_LOJA
        ]
        
        df = pd.read_excel(file_path, sheet_name=0, usecols=colunas_para_ler)
        
        # Renomeia colunas para uso interno
        df.rename(columns={
            COL_MIX_CODIGO: 'Codigo',
            COL_MIX_EAN: 'EAN',
            COL_MIX_PRODUTO: 'Produto',
            COL_MIX_EMBALAGEM: 'Embalagem',
            COL_MIX_MIX: 'Mix',
            COL_MIX_LOJA: 'Loja'
        }, inplace=True)
        
        # Converte tipos de dados
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce')
        df.dropna(subset=['Codigo'], inplace=True)
        df['Codigo'] = df['Codigo'].astype(int)
        df['EAN'] = df['EAN'].astype(str).str.split('.').str[0]
        df['Loja'] = df['Loja'].astype(str).str.zfill(3)
        df['Produto'] = df['Produto'].astype(str)
        df['Embalagem'] = pd.to_numeric(df['Embalagem'], errors='coerce')

        return df
    except FileNotFoundError:
        st.error(f"Arquivo de Mix '{file_path}' não encontrado. O admin precisa fazer o upload.")
        return None
    except Exception as e:
        st.error(f"Erro ao ler o arquivo de Mix: {e}")
        return None

@st.cache_data
def load_historico_data(file_path: str) -> Optional[pd.DataFrame]:
    """Carrega o arquivo de histórico de dados das lojas."""
    try:
        # Define as colunas a serem lidas do Histórico
        # --- COLUNA J (ESTQ+PED) REMOVIDA ---
        colunas_para_ler = [
            COL_HIST_CODIGO, COL_HIST_LOJA, COL_HIST_DATA, COL_HIST_EST_LOJA,
            COL_HIST_PED_LOJA, COL_K, COL_L, COL_O,
            COL_HIST_EMBALAGEM_HIST 
        ]
        
        df = pd.read_excel(file_path, sheet_name=0, usecols=colunas_para_ler)
        
        # Renomeia colunas para uso interno
        df.rename(columns={
            COL_HIST_CODIGO: 'Codigo',
            COL_HIST_LOJA: 'Loja',
            COL_HIST_DATA: 'Data',
            COL_HIST_EST_LOJA: 'Estoque_Loja',
            COL_HIST_PED_LOJA: 'Pedidos_Loja',
            # 'Estq_Ped_Loja' REMOVIDO
            COL_HIST_EMBALAGEM_HIST: 'Embalagem', 
            COL_K: 'Vd1',
            COL_L: 'Vd2',
            COL_O: 'Cob'
        }, inplace=True)
        
        # Converte tipos de dados
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce')
        df.dropna(subset=['Data', 'Codigo'], inplace=True)
        
        df['Codigo'] = df['Codigo'].astype(int)
        df['Loja'] = df['Loja'].astype(str).str.zfill(3)
        df['Embalagem'] = pd.to_numeric(df['Embalagem'], errors='coerce')

        # Converte colunas de métricas para numérico, tratando falhas
        cols_metricas = ['Estoque_Loja', 'Pedidos_Loja', 'Vd1', 'Vd2', 'Cob'] # 'Estq_Ped_Loja' REMOVIDO
        for col in cols_metricas:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Otimização: Retorna apenas os dados mais recentes por item/loja
        df = df.sort_values('Data', ascending=False).drop_duplicates(subset=['Codigo', 'Loja'])

        return df
    except FileNotFoundError:
        st.warning(f"Arquivo de Histórico ('{file_path}') não encontrado. Dados da loja não serão exibidos.")
        return pd.DataFrame() # Retorna DF vazio
    except Exception as e:
        st.error(f"Erro ao ler o arquivo de Histórico: {e}")
        return pd.DataFrame() # Retorna DF vazio


# --- NOVA FUNÇÃO (Copiada de 'historico.py') ---
def get_cd_stock_in_caixas(df_wms_latest, df_mix_full, df_hist_full, product_code=None):
    """Calcula o estoque do CD em caixas."""
    
    # Se o WMS falhou ao carregar, retorna 0
    if df_wms_latest.empty:
        return 0
        
    # 1. Cria o mapa de embalagens (Código -> Embalagem) do arquivo Mix
    embalagem_map = df_mix_full[['Codigo', 'Embalagem']].dropna(subset=['Embalagem']).drop_duplicates(subset=['Codigo'])
    
    # 2. Cria um mapa de fallback do histórico (se o histórico foi carregado)
    if not df_hist_full.empty and 'Embalagem' in df_hist_full.columns:
        embalagem_map_hist = df_hist_full[['Codigo', 'Embalagem']].dropna(subset=['Embalagem']).drop_duplicates(subset=['Codigo'])
        # Combina com o mapa do histórico para preencher embalagens faltantes
        embalagem_map = embalagem_map.merge(embalagem_map_hist, on='Codigo', how='left', suffixes=('_mix', '_hist'))
        # Prioriza a embalagem do Mix, mas usa a do Histórico se a do Mix for nula
        embalagem_map['Embalagem'] = embalagem_map['Embalagem_mix'].fillna(embalagem_map['Embalagem_hist'])
    else:
        # Se o histórico falhou ou não tem a coluna, usa apenas o mapa do mix
        if 'Embalagem_mix' in embalagem_map.columns:
            embalagem_map = embalagem_map.rename(columns={'Embalagem_mix': 'Embalagem'})
        
    
    # 3. Agrega o estoque do WMS (em unidades)
    wms_stock_units = df_wms_latest.groupby('Codigo')['Qtd_CD'].sum().reset_index()
    
    # 4. Combina o estoque WMS com o mapa de embalagens (merge em int)
    df_merged = wms_stock_units.merge(embalagem_map[['Codigo', 'Embalagem']], on='Codigo', how='left')
    
    # 5. Trata falhas (se embalagem for Nula ou 0, estoque em caixas é 0)
    df_merged['Embalagem'] = df_merged['Embalagem'].replace(0, pd.NA)
    
    # 6. Converte unidades para caixas
    df_merged['Estoque_CD_Caixas'] = df_merged.apply(
        lambda row: (row['Qtd_CD'] / row['Embalagem']) if pd.notna(row['Embalagem']) else 0,
        axis=1
    )
    
    # Arredonda para baixo (floor)
    df_merged['Estoque_CD_Caixas'] = df_merged['Estoque_CD_Caixas'].apply(lambda x: int(x) if pd.notna(x) else 0)
    
    # 7. Se um código de produto foi fornecido, filtra por ele
    if product_code:
        df_item = df_merged[df_merged['Codigo'] == product_code]
        if df_item.empty:
            return 0 # Produto existe no histórico mas não no WMS
        return df_item['Estoque_CD_Caixas'].sum()
    
    return 0 # Retorna 0 se nenhum código for passado


def save_order_to_db(pedido_final: List[dict]):
    """Salva o pedido consolidado no banco de dados 'pedidos.db'."""
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        c = conn.cursor()

        data_pedido_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        usuario = st.session_state.get('username', 'desconhecido')
        
        # Prepara as colunas de loja dinamicamente
        loja_cols_names = [f"loja_{loja}" for loja in LISTA_LOJAS]
        loja_cols_placeholders = ", ".join(["?"] * len(LISTA_LOJAS))
        
        query = f"""
            INSERT INTO pedidos_consolidados (
                codigo, produto, ean, embalagem, data_pedido, usuario_pedido, status_item,
                {", ".join(loja_cols_names)}, total_cx, status_aprovacao
            ) VALUES (?, ?, ?, ?, ?, ?, ?, {loja_cols_placeholders}, ?, ?) 
        """

        for item in pedido_final:
            qtys_lojas = []
            for loja in LISTA_LOJAS:
                qtys_lojas.append(item.get(f"loja_{loja}", 0))
            
            # --- ORDEM CORRIGIDA ---
            params = (
                item['Codigo'],       # 1. codigo
                item['Produto'],      # 2. produto
                item['EAN'],          # 3. ean
                item['Embalagem'],    # 4. embalagem
                data_pedido_str,      # 5. data_pedido
                usuario,              # 6. usuario_pedido
                item['Status'],       # 7. status_item
                *qtys_lojas,          # 8. ...lojas...
                item['Total_CX'],     # 9. total_cx
                'Pendente'            # 10. status_aprovacao
            )
            # --- FIM DA CORREÇÃO ---
            c.execute(query, params)
            
        conn.commit()
        return True

    except sqlite3.Error as e:
        st.error(f"Erro ao salvar pedido no banco de dados: {e}")
        if conn:
            conn.rollback() # Desfaz a transação em caso de erro
        return False

    finally:
        if conn:
            conn.close()

# --- Lógica da Página ---

def show_pedidos_page():
    """Cria a interface da página de Pedidos."""
    
    st.title("🛒 Digitação de Pedidos")

    if 'pedido_atual' not in st.session_state:
        st.session_state.pedido_atual = [] 

    # --- Carrega TODOS os arquivos de dados ---
    df_mix_full = load_mix_data(MIX_FILE_PATH)
    df_hist_full = load_historico_data(HIST_FILE_PATH)
    df_wms = load_wms_data(WMS_FILE_PATH)
    
    # O Mix é CRÍTICO. Se ele falhar, o app para.
    if df_mix_full is None:
        st.stop()
        
    # Garante que os dataframes opcionais não sejam 'None'
    if df_hist_full is None:
        df_hist_full = pd.DataFrame()
    if df_wms is None:
        df_wms = pd.DataFrame()

    lojas_do_usuario = st.session_state.get('lojas_acesso', [])
    if not lojas_do_usuario:
        st.warning("Seu usuário não tem acesso a nenhuma loja. Contate o administrador.")
        st.stop()
        
    # --- 1. BUSCA DE PRODUTO ---
    st.subheader("1. Buscar Produto")

    df_mix_usuario = df_mix_full[df_mix_full['Loja'].isin(lojas_do_usuario)].copy()
    
    tab1, tab2, tab3 = st.tabs(["Buscar por Produto", "Buscar por Código", "Buscar por EAN"])

    produto_selecionado = None
    
    with tab1:
        produto_busca = st.text_input("Digite o nome do Produto:") 
        if produto_busca:
            # Filtra o mix pela descrição (usando a coluna 'Produto' renomeada)
            resultados_parciais = df_mix_usuario[
                df_mix_usuario['Produto'].str.contains(produto_busca, case=False, na=False)
            ]
            opcoes_unicas = resultados_parciais.drop_duplicates(subset=['Codigo'])
            opcoes_unicas['Display'] = opcoes_unicas['Produto'] + " (Cód: " + opcoes_unicas['Codigo'].astype(str) + ")"
            
            lista_opcoes = ["Selecione um item..."] + opcoes_unicas['Display'].tolist()
            item_selecionado_display = st.selectbox("Selecione o produto na lista:", lista_opcoes, key="sel_prod_desc")
            
            if item_selecionado_display and item_selecionado_display != "Selecione um item...":
                try:
                    codigo_extraido = int(re.search(r'\(Cód: (\d+)\)', item_selecionado_display).group(1))
                    # Pega a primeira linha (qualquer loja) SÓ para obter os dados mestre (Nome, Emb, EAN)
                    produto_selecionado = df_mix_full[df_mix_full['Codigo'] == codigo_extraido].iloc[0]
                except (AttributeError, ValueError, IndexError):
                    st.error("Não foi possível selecionar o item.")
    
    with tab2:
        codigo_busca = st.text_input("Digite o Código do Produto:")
        if codigo_busca:
            try:
                codigo_int = int(codigo_busca) # Valida se é número
                resultados = df_mix_full[df_mix_full['Codigo'] == codigo_int]
                if not resultados.empty:
                    produto_selecionado = resultados.iloc[0]
                else:
                    st.warning("Código não encontrado no Mix.")
            except (ValueError, IndexError):
                st.warning("Por favor, digite um código numérico válido.")
                
    with tab3:
        ean_busca = st.text_input("Digite o Código EAN:")
        if ean_busca:
            try:
                resultados = df_mix_full[df_mix_full['EAN'] == ean_busca]
                if not resultados.empty:
                    produto_selecionado = resultados.iloc[0]
                else:
                    st.warning("EAN não encontrado no Mix.")
            except (ValueError, IndexError):
                st.warning("Por favor, digite um EAN válido.")

    st.markdown("---")

    # --- 2. GRADE DE DIGITAÇÃO ---
    if produto_selecionado is not None:
        st.subheader("2. Distribuir Quantidades (em Caixas)")
        
        codigo_int = produto_selecionado['Codigo']
        embalagem = produto_selecionado['Embalagem']
        
        # --- CÁLCULO DO ESTOQUE CD (EM CAIXAS) ---
        estoque_cd_caixas = get_cd_stock_in_caixas(df_wms, df_mix_full, df_hist_full, codigo_int)
        
        st.info(f"""
            **Item:** {produto_selecionado['Produto']} (Cód: {codigo_int}) | 
            **Embalagem:** {embalagem} un/cx | 
            **Estoque CD (Atual):** {estoque_cd_caixas} Caixas
        """)

        with st.form(key="form_digitar_quantidades"):
            
            quantidades_digitadas = {}
            total_cx_item = 0
            
            st.markdown("**Digite as quantidades (CX) para cada loja:**")
            
            num_colunas_grade = 3 
            linhas_lojas = [lojas_do_usuario[i:i + num_colunas_grade] for i in range(0, len(lojas_do_usuario), num_colunas_grade)]
            
            status_item_geral = "Suspenso"
            
            for linha in linhas_lojas:
                cols = st.columns(num_colunas_grade)
                for i, loja in enumerate(linha):
                    col = cols[i]
                    
                    # --- BUSCA DE DADOS DA LOJA (Item 9 da solicitação) ---
                    # Pega a linha de dados *mais recente* para este item/loja
                    df_loja_item_mix = df_mix_usuario[
                        (df_mix_usuario['Codigo'] == codigo_int) & 
                        (df_mix_usuario['Loja'] == loja)
                    ]
                    
                    df_loja_item_hist = df_hist_full[
                        (df_hist_full['Codigo'] == codigo_int) &
                        (df_hist_full['Loja'] == loja)
                    ]
                    
                    info_loja_str = ""
                    status_item_loja = "Suspenso"
                    
                    if not df_loja_item_mix.empty:
                        if df_loja_item_mix.iloc[0]['Mix'] == 'A': # 'A' = Ativo
                            status_item_loja = "Ativo"
                            status_item_geral = "Ativo"
                    
                    if not df_loja_item_hist.empty:
                        # Pega a linha única (já filtramos pelo mais recente no load)
                        latest_data_row = df_loja_item_hist.iloc[0]
                        
                        # Prepara as informações de exibição
                        data_ref = latest_data_row['Data'].strftime('%d/%m/%Y')
                        est_loja = latest_data_row['Estoque_Loja']
                        ped_cx = latest_data_row['Pedidos_Loja']
                        # estq_ped = latest_data_row['Estq_Ped_Loja'] # REMOVIDO
                        vd1 = latest_data_row['Vd1']
                        vd2 = latest_data_row['Vd2']
                        cob = latest_data_row['Cob']

                        info_loja_str += f"**Status Mix: {status_item_loja} (Ref: {data_ref})** \n"
                        info_loja_str += f"Est. Loja: {est_loja} | Ped. Loja: {ped_cx} \n" # <-- 'E+P' Removido
                        info_loja_str += f"Vd1: {vd1} | Vd2: {vd2} | Cob: {cob}"

                    else:
                        info_loja_str += f"**Status Mix: {status_item_loja}** (Sem dados históricos)"
                    # --- FIM DA BUSCA DE DADOS ---

                    label_loja = f"Loja {loja}"
                    if status_item_loja == "Suspenso":
                        label_loja += " (Suspenso)"
                        
                    qty = col.number_input(
                        label_loja,
                        min_value=0, 
                        step=1, 
                        key=f"qty_{codigo_int}_{loja}"
                    )
                    # Exibe as informações da loja (Item 9)
                    col.caption(info_loja_str)

                    if qty > 0:
                        quantidades_digitadas[f"loja_{loja}"] = qty
                        total_cx_item += qty
            
            # Botão de adicionar ao carrinho
            if st.form_submit_button("Adicionar Item ao Pedido"):
                if total_cx_item > 0:
                    item_data = {
                        "Codigo": codigo_int, # <-- CORRIGIDO: Chave é 'Codigo' (Maiúsculo)
                        "Produto": produto_selecionado['Produto'],
                        "EAN": produto_selecionado['EAN'],
                        "Embalagem": embalagem,
                        "Status": status_item_geral, 
                        "Total_CX": total_cx_item
                    } 
                    item_data.update(quantidades_digitadas) # Adiciona as quantidades por loja
                    st.session_state.pedido_atual.append(item_data)
                    st.success(f"{produto_selecionado['Produto']} adicionado ao pedido!")
                else:
                    st.warning("Nenhuma quantidade foi digitada.")



    st.markdown("---")
    st.subheader("3. Pedido Atual")

    if not st.session_state.pedido_atual:
        st.info("Nenhum item no pedido ainda.")
    else:
        df_pedido = pd.DataFrame(st.session_state.pedido_atual)

        # Reordena as colunas para melhor visualização
        colunas_info = ['Codigo', 'Produto', 'Status', 'Total_CX']
        colunas_loja = [f"loja_{loja}" for loja in lojas_do_usuario if f"loja_{loja}" in df_pedido.columns]
        df_pedido_display = df_pedido[colunas_info + colunas_loja]
        st.dataframe(df_pedido_display, hide_index=True, use_container_width=True)

        col_final1, col_final2 = st.columns(2)

        with col_final1:
            if st.button("Salvar Pedido no Sistema", type="primary"):
                if save_order_to_db(st.session_state.pedido_atual):
                    st.success("Pedido salvo com sucesso!")
                    st.session_state.pedido_atual = [] 
                    # Limpa o cache do histórico recente (se existir)
                    # Adiciona verificação se a função existe
                    if 'get_recent_orders_display' in globals() or 'get_recent_orders_display' in globals():
                        get_recent_orders_display.clear()
                    st.rerun()
                else:
                    st.error("Falha ao salvar o pedido.")
        with col_final2:
            if st.button("Limpar Pedido Atual"):
                st.session_state.pedido_atual = []
                st.rerun()

    # --- 4. HISTÓRICO RECENTE (COMO NO CÓDIGO ANTERIOR) ---
    st.markdown("---")
    st.subheader("4. Seus Pedidos Recentes (Últimos 3 dias)")
    
    # Define a função de histórico recente aqui
    @st.cache_data(ttl=60) # Cache de 1 minuto
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
            if conn:
                conn.close()
    
    username_atual = st.session_state.get('username', 'desconhecido')
    df_recentes = get_recent_orders_display(username_atual)
    
    if df_recentes.empty:
        st.info("Nenhum pedido recente encontrado para seu usuário.")
    else:
        st.dataframe(df_recentes, hide_index=True, use_container_width=True)
    
    if df_recentes.empty:
        st.info("Nenhum pedido recente encontrado.")
    else:

        st.dataframe(df_recentes, use_container_width=True)
