import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple
import os
import re # Para a limpeza de strings na busca

# --- Configurações Iniciais ---
HIST_FILE_PATH = 'data/historico_solic.xlsm'
WMS_FILE_PATH = 'data/WMS.xlsm'

# --- Nomes das Colunas (Conforme sua descrição) ---
# Arquivo 'historico_solic.xlsm'
COL_HIST_CODIGO = 'CODIGOINT' # Coluna A
COL_HIST_EMBALAGEM = 'EmbSeparacao' # Coluna E
COL_HIST_ESTOQUE_LOJA = 'EstCX' # Coluna G
COL_HIST_PEDIDOS = 'PedCX' # Coluna H
COL_HIST_DATA = 'DtSolicitacao' # Coluna R
COL_HIST_DESCRICAO = 'Produto' # <-- CORRIGIDO

# Arquivo 'WMS.xlsm'
COL_WMS_CODIGO = 'codigo' # Coluna A
COL_WMS_QTD = 'Qtd' # Coluna E
COL_WMS_DATA = 'datasalva' # Coluna I

# Lista de meses para o seletor
MESES_DISPONIVEIS = {
    "Janeiro": 1, "Fevereiro": 2, "Março": 3, "Abril": 4, "Maio": 5, "Junho": 6,
    "Julho": 7, "Agosto": 8, "Setembro": 9, "Outubro": 10, "Novembro": 11, "Dezembro": 12
}
# Mapeamento inverso para encontrar o nome do mês a partir do número
MESES_INVERSO = {v: k for k, v in MESES_DISPONIVEIS.items()}


# --- Funções de Carregamento de Dados (Cacheadas) ---

@st.cache_data
def load_wms_data(file_path: str) -> Optional[pd.DataFrame]:
    """Carrega dados do WMS (estoque do CD)."""
    try:
        # Carrega apenas as colunas necessárias
        df = pd.read_excel(
            file_path,
            sheet_name='WMS',
            usecols=[COL_WMS_CODIGO, COL_WMS_QTD, COL_WMS_DATA]
        )
        
        # Renomeia colunas para consistência
        df.rename(columns={
            COL_WMS_CODIGO: 'Codigo',
            COL_WMS_QTD: 'Qtd_CD',
            COL_WMS_DATA: 'Data'
        }, inplace=True)
        
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        
        # --- CORREÇÃO: Converter Código para numérico ---
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce')
        df.dropna(subset=['Data', 'Codigo'], inplace=True)
        df['Codigo'] = df['Codigo'].astype(int)
        # --- FIM DA CORREÇÃO ---
        
        if df.empty:
            st.error("Arquivo WMS não contém dados válidos nas colunas esperadas.")
            return None
            
        # Pega a data mais recente disponível no WMS
        latest_date = df['Data'].dt.date.max()
        
        # Filtra o WMS para conter *apenas* o estoque do último dia
        df_latest = df[df['Data'].dt.date == latest_date].copy()
        
        return df_latest
        
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo WMS ({file_path}): {e}")
        return None

@st.cache_data
def load_hist_data(file_path: str) -> Optional[pd.DataFrame]:
    """Carrega dados do Histórico de Solicitações (Lojas)."""
    try:
        # Assume que a primeira planilha (index 0) é a correta
        df = pd.read_excel(
            file_path,
            sheet_name=0, 
            usecols=[
                COL_HIST_CODIGO, COL_HIST_DESCRICAO, COL_HIST_EMBALAGEM,
                COL_HIST_ESTOQUE_LOJA, COL_HIST_PEDIDOS, COL_HIST_DATA
            ]
        )
        
        df.rename(columns={
            COL_HIST_CODIGO: 'Codigo',
            COL_HIST_DESCRICAO: 'Descricao',
            COL_HIST_EMBALAGEM: 'Embalagem',
            COL_HIST_ESTOQUE_LOJA: 'Estoque_Lojas',
            COL_HIST_PEDIDOS: 'Pedidos',
            COL_HIST_DATA: 'Data'
        }, inplace=True)
        
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')

        # --- CORREÇÃO: Converter Código para numérico ---
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce')
        df.dropna(subset=['Data', 'Codigo'], inplace=True)
        df['Codigo'] = df['Codigo'].astype(int)
        # --- FIM DA CORREÇÃO ---
        
        # Garante que colunas de soma sejam numéricas, tratando falhas
        df['Estoque_Lojas'] = pd.to_numeric(df['Estoque_Lojas'], errors='coerce').fillna(0)
        df['Pedidos'] = pd.to_numeric(df['Pedidos'], errors='coerce').fillna(0)
        df['Embalagem'] = pd.to_numeric(df['Embalagem'], errors='coerce')
        
        return df
        
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo Histórico ({file_path}): {e}")
        return None

# --- Função de Lógica Principal ---

def get_cd_stock_in_caixas(df_wms_latest, df_hist_full, product_code=None):
    """
    Calcula o estoque do CD em caixas, usando o mapa de embalagens do arquivo histórico.
    Retorna 0 se a embalagem não for encontrada (conforme solicitado).
    """
    
    # 1. Cria o mapa de embalagens (Código -> Embalagem) do histórico
    embalagem_map = df_hist_full[['Codigo', 'Embalagem']].drop_duplicates(subset=['Codigo'])
    
    # 2. Agrega o estoque do WMS (em unidades)
    wms_stock_units = df_wms_latest.groupby('Codigo')['Qtd_CD'].sum().reset_index()
    
    # 3. Combina o estoque WMS com o mapa de embalagens (merge em int)
    df_merged = wms_stock_units.merge(embalagem_map, on='Codigo', how='left')
    
    # 4. Trata falhas (pedido do usuário: se falhar, é zero)
    # Preenche embalagens nulas ou zero com 'None' para forçar o 'else'
    df_merged['Embalagem'] = df_merged['Embalagem'].replace(0, pd.NA)
    
    # 5. Converte unidades para caixas
    df_merged['Estoque_CD_Caixas'] = df_merged.apply(
        lambda row: (row['Qtd_CD'] / row['Embalagem']) if pd.notna(row['Embalagem']) else 0,
        axis=1
    )
    
    # 6. Se um código de produto foi fornecido, filtra por ele (como int)
    if product_code:
        df_item = df_merged[df_merged['Codigo'] == product_code] # Compara como int
        if df_item.empty:
            return 0 # Produto existe no histórico mas não no WMS
        return df_item['Estoque_CD_Caixas'].sum()
    
    # 7. Se não houver filtro, retorna o estoque total do CD
    return df_merged['Estoque_CD_Caixas'].sum()


def show_historico_page():
    """Cria a interface da página de Histórico de Solicitações."""
    
    st.title("📊 Histórico de Solicitações vs. Estoques")

    # --- Carregamento de Dados ---
    df_hist_full = load_hist_data(HIST_FILE_PATH)
    df_wms_latest = load_wms_data(WMS_FILE_PATH)

    if df_hist_full is None or df_wms_latest is None:
        st.error("Falha ao carregar um ou mais arquivos de dados. Verifique os caminhos e as abas.")
        return

    # --- LÓGICA DE DATA PADRÃO ---
    # Por padrão, usa a data mais recente do arquivo histórico
    default_date = df_hist_full['Data'].max()
    if pd.isna(default_date):
        st.error("Não foi possível encontrar uma data válida no arquivo histórico.")
        return
        
    ano_atual = default_date.year
    mes_atual_num = default_date.month
    mes_atual_nome = MESES_INVERSO.get(mes_atual_num, "Janeiro")
    
    anos_disponiveis = sorted(df_hist_full['Data'].dt.year.unique(), reverse=True)
    if ano_atual not in anos_disponiveis:
        anos_disponiveis.insert(0, ano_atual)
        
    lista_meses_nomes = list(MESES_DISPONIVEIS.keys())
    
    # Define o índice padrão (posição na lista) para o mês/ano
    try:
        index_ano = anos_disponiveis.index(ano_atual)
    except ValueError:
        index_ano = 0
    try:
        index_mes = lista_meses_nomes.index(mes_atual_nome)
    except ValueError:
        index_mes = 0

    # --- Seletores de Data ---
    st.subheader("Selecione o Período")
    col1, col2 = st.columns(2)
    with col1:
        ano_selecionado = st.selectbox(
            "Selecione o Ano", 
            anos_disponiveis, 
            index=index_ano
        )
    with col2:
        mes_selecionado = st.selectbox(
            "Selecione o Mês", 
            lista_meses_nomes, 
            index=index_mes
        )
    
    mes_num = MESES_DISPONIVEIS[mes_selecionado]

    # --- FILTRAGEM DE DATAS ---
    df_mensal = df_hist_full[
        (df_hist_full['Data'].dt.year == ano_selecionado) &
        (df_hist_full['Data'].dt.month == mes_num)
    ]

    if df_mensal.empty:
        st.warning(f"Não há dados para {mes_selecionado} de {ano_selecionado}.")
        return

    st.markdown("---")

    # --- FILTRO POR PRODUTO ESPECÍFICO ---
    st.subheader("Filtro por Produto (Opcional)")
    
    tab1, tab2 = st.tabs(["Buscar por Descrição", "Buscar por Código"])
    
    codigo_para_filtrar = None
    item_selecionado_display = None

    # ABA 1: Busca por Descrição (Autocomplete)
    with tab1:
        # Usa 'Descricao' (o nome renomeado e sem acento)
        # Converte o código (int) para string APENAS para exibição
        df_mensal['Display'] = df_mensal['Descricao'].astype(str) + " (Código: " + df_mensal['Codigo'].astype(str) + ")"
        
        descricao_busca = st.text_input("Digite a descrição ou parte dela:")
        
        if descricao_busca:
            # Usa 'Descricao' (o nome renomeado e sem acento)
            resultados_parciais = df_mensal[df_mensal['Descricao'].str.contains(descricao_busca, case=False, na=False)]
            opcoes_unicas = resultados_parciais.drop_duplicates(subset=['Codigo'])
            lista_opcoes = ["Selecione um item..."] + opcoes_unicas['Display'].tolist()
        else:
            lista_opcoes = ["Digite algo para buscar..."]

        item_selecionado_display = st.selectbox("Selecione o produto na lista:", lista_opcoes)
        
        if item_selecionado_display and item_selecionado_display not in ["Selecione um item...", "Digite algo para buscar..."]:
            try:
                # --- CORREÇÃO: Extrai como INT ---
                codigo_para_filtrar = int(re.search(r'\(Código: (\d+)\)', item_selecionado_display).group(1))
            except (AttributeError, ValueError):
                st.error("Não foi possível extrair o código do item selecionado.")

    # ABA 2: Busca por Código Direto
    with tab2:
        codigo_busca_direta = st.text_input("Ou digite o Código (apenas números):")
        if codigo_busca_direta:
            try:
                # --- CORREÇÃO: Converte para INT ---
                codigo_para_filtrar = int(codigo_busca_direta) # Usa int
            except ValueError:
                st.warning("Código deve conter apenas números.")

    st.markdown("---")
    
    # --- PREPARAÇÃO DOS DADOS PARA O GRÁFICO ---
    
    df_grafico = None
    
    if codigo_para_filtrar:
        # 1. ANÁLISE POR ITEM
        st.subheader(f"Análise: {item_selecionado_display or codigo_para_filtrar}")
        
        df_item_hist = df_mensal[df_mensal['Codigo'] == codigo_para_filtrar] # Compara como int
        if df_item_hist.empty:
            st.warning("Produto não encontrado nos dados históricos do mês.")
            return

        # Agrupa os dados do item por dia
        df_grafico = df_item_hist.groupby(df_item_hist['Data'].dt.date).agg(
            Pedidos_Item=('Pedidos', 'sum'),
            Estoque_Lojas_Item=('Estoque_Lojas', 'sum')
        ).reset_index()
        
        # Calcula o Estoque CD (já em caixas) para este item
        estoque_cd_item = get_cd_stock_in_caixas(df_wms_latest, df_hist_full, codigo_para_filtrar)
        df_grafico['Estoque_CD_Item'] = estoque_cd_item

        # Renomeia para o gráfico
        df_grafico.rename(columns={'Data': 'Dia'}, inplace=True)
        df_grafico = df_grafico.set_index('Dia')

    else:
        # 2. ANÁLISE TOTAL
        st.subheader(f"Análise Total - {mes_selecionado}/{ano_selecionado}")
        
        # Agrupa os dados totais por dia
        df_grafico = df_mensal.groupby(df_mensal['Data'].dt.date).agg(
            Total_Pedidos=('Pedidos', 'sum'),
            Total_Estoque_Lojas=('Estoque_Lojas', 'sum')
        ).reset_index()
        
        # Calcula o Estoque CD Total (já em caixas)
        estoque_cd_total = get_cd_stock_in_caixas(df_wms_latest, df_hist_full)
        df_grafico['Total_Estoque_CD'] = estoque_cd_total
        
        # Renomeia para o gráfico
        df_grafico.rename(columns={'Data': 'Dia'}, inplace=True)
        df_grafico = df_grafico.set_index('Dia')

    # Exibe o gráfico de linhas
    st.line_chart(df_grafico)
    
    # Exibe a tabela de dados do gráfico (opcional, mas útil)
    with st.expander("Ver dados da tabela"):
        st.dataframe(df_grafico)