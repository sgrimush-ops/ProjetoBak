import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import sqlite3
import json
import re
import os # Necessário para os paths dos arquivos

# --- Configurações Iniciais ---
# Caminho corrigido para .xlsx e no diretório data/
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
COL_HIST_LOJA = 'LOJA'           # Coluna B
COL_HIST_ESTOQUE_LOJA = 'EstCX' # Coluna G
COL_HIST_PEDIDOS = 'PedCX'     # Coluna H
COL_HIST_DATA = 'DtSolicitacao'  # Coluna R

# --- Nomes das Colunas (WMS) ---
COL_WMS_CODIGO = 'codigo'
COL_WMS_QTD = 'Qtd'
COL_WMS_DATA = 'datasalva'


# --- Funções de Carregamento de Dados ---

@st.cache_data(ttl=timedelta(hours=6))
def load_mix_data(file_path: str) -> Optional[pd.DataFrame]:
    """Carrega o arquivo principal do Mix de Produtos."""
    if not os.path.exists(file_path):
        st.error(f"Erro Crítico: Arquivo Mix não encontrado em '{file_path}'")
        return None
    try:
        # Revertido para ler o arquivo Excel (.xlsx)
        # Por padrão, o pandas lê a primeira aba, o que deve funcionar.
        df = pd.read_excel(file_path)
        
        # Converte colunas principais para tipos corretos
        df[COL_MIX_CODIGO] = df[COL_MIX_CODIGO].astype(str)
        df[COL_MIX_EAN] = df[COL_MIX_EAN].astype(str)
        df[COL_MIX_LOJA] = df[COL_MIX_LOJA].astype(str).str.zfill(3)
        return df
    except Exception as e:
        # Mensagem de erro corrigida para Excel
        st.error(f"Erro ao ler o arquivo Mix Excel '{file_path}': {e}")
        return None

@st.cache_data(ttl=timedelta(hours=6))
def load_hist_data(file_path: str) -> Optional[pd.DataFrame]:
    """Carrega o arquivo de histórico de solicitações."""
    if not os.path.exists(file_path):
        st.warning(f"Arquivo de Histórico não encontrado em '{file_path}'. Sugestões de pedidos podem ficar incompletas.")
        return None
    try:
        df = pd.read_excel(file_path, sheet_name='Base')
        # ... (seu pré-processamento, se necessário) ...
        return df
    except Exception as e:
        st.error(f"Erro ao ler o arquivo de Histórico '{file_path}': {e}")
        return None

@st.cache_data(ttl=timedelta(hours=6))
def load_wms_data(file_path: str) -> Optional[pd.DataFrame]:
    """Carrega o arquivo WMS para estoque do CD."""
    if not os.path.exists(file_path):
        st.warning(f"Arquivo WMS não encontrado em '{file_path}'. Sugestões de estoque CD não funcionarão.")
        return None
    try:
        df = pd.read_excel(file_path, sheet_name='WMS')
        # ... (seu pré-processamento, se necessário) ...
        return df
    except Exception as e:
        st.error(f"Erro ao ler o arquivo WMS '{file_path}': {e}")
        return None

# --- Funções de Busca (Mix) ---

def search_product(query: str, df_mix: pd.DataFrame, lojas_acesso: List[str]) -> List[Dict]:
    """Busca produtos no DataFrame Mix (otimizado para lojas de acesso)."""
    
    # 1. Filtra o Mix apenas para as lojas que o usuário pode ver
    df_lojas_acesso = df_mix[df_mix[COL_MIX_LOJA].isin(lojas_acesso)]
    
    # 2. Normaliza a query (remove caracteres especiais e deixa minúsculo)
    query_norm = re.sub(r'\W+', ' ', query.lower())
    
    # 3. Busca por Código (exato)
    # Remove duplicados (um produto por código, independente da loja)
    df_mix_unicos = df_lojas_acesso.drop_duplicates(subset=[COL_MIX_CODIGO])

    results_cod = df_mix_unicos[df_mix_unicos[COL_MIX_CODIGO] == query]
    if not results_cod.empty:
        return results_cod.to_dict('records')

    # 4. Busca por EAN (exato)
    results_ean = df_mix_unicos[df_mix_unicos[COL_MIX_EAN] == query]
    if not results_ean.empty:
        return results_ean.to_dict('records')
        
    # 5. Busca por Descrição (parcial)
    # Divide a query normalizada em palavras-chave
    keywords = query_norm.split()
    
    # Filtra o DataFrame: todas as palavras-chave devem estar na descrição
    df_result = df_mix_unicos.copy()
    df_result['Descricao_Norm'] = df_result[COL_MIX_PRODUTO].astype(str).str.lower()
    
    for key in keywords:
        df_result = df_result[df_result['Descricao_Norm'].str.contains(key, na=False)]
        
    # Limita a 20 resultados para performance
    return df_result.head(20).to_dict('records')

# --- Funções de Sugestão ---

def get_sugestoes_lojas(loja: str, produto_selecionado: Dict, df_hist: Optional[pd.DataFrame], df_wms: Optional[pd.DataFrame]) -> Dict:
    """Busca sugestões de estoque e pedido para uma loja específica."""
    sugestoes = {
        'Estoque Loja (CX)': 0,
        'Média Pedido (CX)': 0,
        'Estoque CD (CX)': 0
    }
    
    try:
        codigo_int = produto_selecionado[COL_MIX_CODIGO]
        embalagem = produto_selecionado.get(COL_MIX_EMBALAGEM, 1)
        if embalagem == 0: embalagem = 1 # Evita divisão por zero

        # --- Sugestão do Histórico (Estoque Loja e Média Pedido) ---
        # Adicionada verificação se df_hist não é None
        if df_hist is not None:
            df_hist_prod_loja = df_hist[
                (df_hist[COL_HIST_CODIGO] == codigo_int) &
                (df_hist[COL_HIST_LOJA] == loja)
            ]
            
            if not df_hist_prod_loja.empty:
                # Pega o registro mais recente
                df_hist_prod_loja[COL_HIST_DATA] = pd.to_datetime(df_hist_prod_loja[COL_HIST_DATA], errors='coerce')
                df_recent = df_hist_prod_loja.sort_values(by=COL_HIST_DATA, ascending=False).iloc[0]
                
                sugestoes['Estoque Loja (CX)'] = df_recent.get(COL_HIST_ESTOQUE_LOJA, 0)
                
                # Calcula a média de pedidos dos últimos X dias (ex: 90 dias)
                data_limite = datetime.now() - timedelta(days=90)
                df_media = df_hist_prod_loja[df_hist_prod_loja[COL_HIST_DATA] >= data_limite]
                
                if not df_media.empty:
                    sugestoes['Média Pedido (CX)'] = round(df_media[COL_HIST_PEDIDOS].mean(), 1)

        # --- Sugestão do WMS (Estoque CD) ---
        # Adicionada verificação se df_wms não é None
        if df_wms is not None:
            df_wms_prod = df_wms[df_wms[COL_WMS_CODIGO] == codigo_int]
            if not df_wms_prod.empty:
                # Pega o estoque mais recente do WMS
                df_wms_prod[COL_WMS_DATA] = pd.to_datetime(df_wms_prod[COL_WMS_DATA], errors='coerce')
                df_wms_recent = df_wms_prod.sort_values(by=COL_WMS_DATA, ascending=False).iloc[0]
                
                estoque_und = df_wms_recent.get(COL_WMS_QTD, 0)
                sugestoes['Estoque CD (CX)'] = round(estoque_und / embalagem, 1)

    except Exception as e:
        # Não quebra a aplicação, apenas loga o erro no console
        print(f"Erro ao calcular sugestões para {codigo_int} / Loja {loja}: {e}")
    
    return sugestoes

# --- Funções do Banco de Dados (Salvar Pedido) ---

def save_pedido_items(items: List[Dict], username: str) -> bool:
    """Salva uma lista de itens de pedido no banco de dados consolidado."""
    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        c = conn.cursor()
        now = datetime.now()

        # Colunas base (excluindo ID e lojas)
        base_cols = [
            "codigo", "produto", "ean", "embalagem", "total_cx", 
            "usuario_pedido", "data_pedido", "status_item", "status_aprovacao"
        ]
        
        # Adiciona colunas de loja dinamicamente
        all_cols = base_cols + [f"loja_{loja}" for loja in LISTA_LOJAS]
        
        # Cria a string de placeholders (?, ?, ...)
        placeholders = ", ".join(["?"] * len(all_cols))
        query = f"INSERT INTO pedidos_consolidados ({', '.join(all_cols)}) VALUES ({placeholders})"

        for item in items:
            # Pega o status do mix (ltmix) do item
            status_mix = item.get(COL_MIX_MIX, "N/A") # Pega o 'ltmix' salvo no item

            # Prepara os valores na ordem correta
            valores = [
                item.get("codigo"),
                item.get("produto"),
                item.get("ean"),
                item.get("embalagem"),
                item.get("total_cx", 0),
                username,
                now,
                status_mix,     # status_item (Salva o 'ltmix')
                "Pendente"      # status_aprovacao
            ]
            
            # Adiciona os valores das lojas
            for loja in LISTA_LOJAS:
                # Adiciona o valor da loja se existir no item, senão 0
                valores.append(item.get(f"loja_{loja}", 0))
            
            # Executa a query
            c.execute(query, tuple(valores))

        conn.commit()
        return True
    
    except sqlite3.Error as e:
        st.error(f"Erro ao salvar o pedido no banco de dados: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

# --- Função Principal da Página ---

def show_pedidos_page():
    st.title("Digitar Pedidos")

    # --- 1. Carregamento de Dados ---
    with st.spinner("Carregando dados do Mix... (Isso pode levar um momento na primeira vez)"):
        df_mix_full = load_mix_data(MIX_FILE_PATH)
    
    # Se o Mix falhar, a página não pode continuar
    if df_mix_full is None:
        st.error("Não foi possível carregar o arquivo Mix. A página não pode ser exibida.")
        st.info(f"Verifique se o arquivo '{MIX_FILE_PATH}' existe e está no formato correto.")
        return

    # Carrega os arquivos opcionais (para sugestões)
    df_hist_sugestoes = load_hist_data(HIST_FILE_PATH)
    df_wms_sugestoes = load_wms_data(WMS_FILE_PATH)

    # --- 2. Verificação de Permissões ---
    lojas_acesso = st.session_state.get('lojas_acesso', [])
    username = st.session_state.get('username', 'desconhecido')
    
    if not lojas_acesso:
        st.error("Você não tem permissão para digitar pedidos. Contate um administrador.")
        st.info(f"Usuário: {username} | Lojas: Nenhuma")
        return

    st.info(f"Você está digitando pedidos para as lojas: {', '.join(lojas_acesso)}")

    # --- 3. Inicialização do Estado da Sessão ---
    if 'pedido_items' not in st.session_state:
        st.session_state.pedido_items = []
    if 'selected_product' not in st.session_state:
        st.session_state.selected_product = None
    if 'search_query' not in st.session_state:
        st.session_state.search_query = ""
        
    # --- 4. Formulário de Busca e Adição ---
    
    # --- 4a. Busca do Produto ---
    st.subheader("1. Buscar Produto")
    
    # Se um produto já foi selecionado, esconde a busca
    if st.session_state.selected_product is None:
        query = st.text_input(
            "Buscar por Código, EAN ou Descrição:", 
            key="search_query_input",
            help="Digite o código exato (INT ou EAN) ou parte da descrição."
        )
        
        # Atualiza o estado da busca
        st.session_state.search_query = query

        if len(st.session_state.search_query) > 2:
            search_results = search_product(st.session_state.search_query, df_mix_full, lojas_acesso)
            
            if search_results:
                st.write("Resultados da Busca:")
                # Mostra os resultados como botões
                cols = st.columns(4)
                for i, prod in enumerate(search_results):
                    col = cols[i % 4]
                    label = f"{prod[COL_MIX_CODIGO]} - {prod[COL_MIX_PRODUTO][:30]}..."
                    if col.button(label, key=f"prod_btn_{prod[COL_MIX_CODIGO]}", use_container_width=True):
                        st.session_state.selected_product = prod
                        st.session_state.search_query = "" # Limpa a busca
                        st.rerun() # Recarrega para mostrar o formulário de quantidade
            else:
                st.info("Nenhum produto encontrado com esse termo.")
                
    # --- 4b. Formulário de Quantidade (se um produto foi selecionado) ---
    else:
        prod_selecionado = st.session_state.selected_product
        
        st.subheader("2. Definir Quantidades")
        st.success(f"Produto selecionado: **{prod_selecionado[COL_MIX_PRODUTO]}** (Cód: {prod_selecionado[COL_MIX_CODIGO]})")
        
        # Botão para cancelar a seleção
        if st.button("X Cancelar Seleção (Buscar outro produto)"):
            st.session_state.selected_product = None
            st.rerun()

        with st.form("form_add_quantidade", clear_on_submit=True):
            embalagem = prod_selecionado.get(COL_MIX_EMBALAGEM, 1)
            st.metric("Embalagem de Separação (Unidades)", embalagem)
            
            st.markdown("---")
            st.write("**Quantidades por Loja (em Caixas):**")
            
            lojas_quantidades = {}
            # Exibe 4 lojas por linha
            num_cols = 4 
            cols_lojas = st.columns(num_cols)
            
            for i, loja in enumerate(lojas_acesso):
                with cols_lojas[i % num_cols]:
                    # Busca sugestões para esta loja e produto
                    sugestoes = get_sugestoes_lojas(loja, prod_selecionado, df_hist_sugestoes, df_wms_sugestoes)
                    
                    # Formata o help_text
                    help_txt = (
                        f"Estoque Loja: {sugestoes['Estoque Loja (CX)']} CX\n"
                        f"Média Pedido (90d): {sugestoes['Média Pedido (CX)']} CX\n"
                        f"Estoque CD: {sugestoes['Estoque CD (CX)']} CX"
                    )
                    
                    qtd = st.number_input(
                        f"Loja {loja}", 
                        min_value=0, 
                        step=1, 
                        key=f"loja_input_{loja}",
                        help=help_txt
                    )
                    lojas_quantidades[f"loja_{loja}"] = qtd
            
            # --- 4c. Botão de Adicionar ---
            submitted = st.form_submit_button("Adicionar Produto ao Pedido", type="primary")

            if submitted:
                # Calcula o total de caixas
                total_cx = sum(lojas_quantidades.values())
                
                if total_cx == 0:
                    st.warning("Nenhuma quantidade inserida. O produto não foi adicionado.")
                else:
                    # Cria o dicionário do item
                    item = {
                        "codigo": st.session_state.selected_product.get(COL_MIX_CODIGO),
                        "produto": st.session_state.selected_product.get(COL_MIX_PRODUTO),
                        "ean": st.session_state.selected_product.get(COL_MIX_EAN),
                        "embalagem": st.session_state.selected_product.get(COL_MIX_EMBALAGEM, 1),
                        COL_MIX_MIX: st.session_state.selected_product.get(COL_MIX_MIX, "N/A"), # Salva o status do mix
                        "total_cx": total_cx,
                        **lojas_quantidades # Adiciona as quantidades das lojas
                    }
                    
                    # Adiciona ao ESTADO DA SESSÃO
                    st.session_state.pedido_items.append(item)
                    st.success(f"Produto '{item['codigo']}' adicionado ao pedido!")
                    
                    # Limpa a seleção para permitir a próxima busca
                    st.session_state.selected_product = None
                    st.rerun()

    # --- 5. Exibição do Pedido Atual ---
    if st.session_state.pedido_items:
        st.markdown("---")
        st.subheader("3. Revisar Pedido Atual")
        
        # Constrói o DataFrame para exibição
        df_pedido = pd.DataFrame(st.session_state.pedido_items)
        
        # Organiza as colunas
        colunas_info = ["codigo", "produto", "ean", "embalagem", COL_MIX_MIX, "total_cx"]
        # Garante que apenas colunas que realmente existem no DF sejam chamadas
        colunas_info_visiveis = [col for col in colunas_info if col in df_pedido.columns]
        
        colunas_loja_visiveis = [f"loja_{loja}" for loja in lojas_acesso if f"loja_{loja}" in df_pedido.columns]
        
        st.dataframe(df_pedido[colunas_info_visiveis + colunas_loja_visiveis])
        
        total_caixas_pedido = sum(item['total_cx'] for item in st.session_state.pedido_items)
        st.metric("Total de Caixas no Pedido", total_caixas_pedido)
        
        # --- 5b. Botões de Salvar e Limpar ---
        col1, col2 = st.columns([1, 0.3])
        
        with col1:
            if st.button("✅ SALVAR PEDIDO E ENVIAR", type="primary"):
                if save_pedido_items(st.session_state.pedido_items, username):
                    st.success("Pedido salvo com sucesso no banco de dados!")
                    # Limpa o pedido do estado da sessão
                    st.session_state.pedido_items = []
                    st.rerun() # Recarrega a página
                else:
                    st.error("Ocorreu um erro ao salvar o pedido.")
        
        with col2:
            if st.button("❌ Limpar Pedido Atual"):
                # Limpa o pedido do estado da sessão
                st.session_state.pedido_items = []
                st.warning("Pedido limpo. Nenhum dado foi salvo.")
                st.rerun() # Recarrega a página

    else:
        st.info("Seu pedido está vazio. Adicione produtos usando o formulário de busca acima.")

    # --- 6. Histórico Recente ---
    st.markdown("---")
    st.subheader("Seus Pedidos nos Últimos 3 Dias")
    
    @st.cache_data(ttl=120) # Cache de 2 minutos
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
        st.info("Nenhum pedido recente encontrado.")
    else:
        st.dataframe(df_recentes, use_container_width=True)