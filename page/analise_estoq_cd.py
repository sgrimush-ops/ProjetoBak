import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple
import os
import re # Para limpar o número de telefone

# --- Configurações Iniciais ---
# MUDANÇA: Removido FILE_PATH
COLUNA_DESCRICAO = 'Produto' 
COLUNA_CODIGO = 'codigo'

# Lista de meses para o seletor
MESES_DISPONIVEIS = {
    "Janeiro": 1, "Fevereiro": 2, "Março": 3, "Abril": 4, "Maio": 5, "Junho": 6,
    "Julho": 7, "Agosto": 8, "Setembro": 9, "Outubro": 10, "Novembro": 11, "Dezembro": 12
}
# Mapeamento inverso
MESES_INVERSO = {v: k for k, v in MESES_DISPONIVEIS.items()}


# --- Funções de Carregamento e Pré-Processamento ---

@st.cache_data
def load_data(file_path: str) -> Optional[pd.DataFrame]:
    """Carrega dados do arquivo Excel."""
    try:
        # Use a aba correta (assumindo 'WMS')
        return pd.read_excel(file_path, sheet_name='WMS')
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo {file_path}. Verifique o caminho e a aba. Erro: {e}")
        return None

def preprocess_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Preprocessa o DataFrame, garantindo que as colunas de data e quantidade existam."""
    df = df.copy()
    
    # 1. Checa colunas essenciais
    if 'datasalva' not in df.columns or 'Qtd' not in df.columns:
        st.error("Colunas 'datasalva' e/ou 'Qtd' não encontradas.")
        return None

    # 2. Converte datas
    df['datasalva'] = pd.to_datetime(df['datasalva'], errors='coerce')
    df.dropna(subset=['datasalva', 'Qtd'], inplace=True)
    df['Data_Dia'] = df['datasalva'].dt.date
    
    # 3. Garante que 'Qtd' é numérica
    df['Qtd'] = pd.to_numeric(df['Qtd'], errors='coerce')
    
    return df

# --- Função Principal de Análise ---

# MUDANÇA: Adicionado 'engine' e 'base_data_path'
def show_ae_page(engine, base_data_path):
    st.title("📈 Evolução de Estoque Mensal")

    # MUDANÇA: Definindo o caminho do arquivo dinamicamente
    file_path = os.path.join(base_data_path, "WMS.xlsm")

    # MUDANÇA: Usando o 'file_path' dinâmico
    df_raw = load_data(file_path)
    if df_raw is None:
        st.error(f"Arquivo 'WMS.xlsm' não encontrado em '{base_data_path}'. Faça o upload na página de Admin.")
        return

    df_processed = preprocess_data(df_raw)
    if df_processed is None:
        return

    # --- LÓGICA DE DATA PADRÃO ---
    hoje = datetime.now()
    ano_atual = hoje.year
    mes_atual_num = hoje.month
    
    mes_atual_nome = MESES_INVERSO.get(mes_atual_num, "Janeiro")
    
    anos_disponiveis = sorted(df_processed['datasalva'].dt.year.unique(), reverse=True)
    if ano_atual not in anos_disponiveis:
        anos_disponiveis.insert(0, ano_atual) 

    try:
        index_ano = anos_disponiveis.index(ano_atual)
    except ValueError:
        index_ano = 0
        
    lista_meses_nomes = list(MESES_DISPONIVEIS.keys())
    try:
        index_mes = lista_meses_nomes.index(mes_atual_nome)
    except ValueError:
        index_mes = 0
    # --- FIM DA LÓGICA DE DATA PADRÃO ---

    
    # --- ENTRADAS DO USUÁRIO ---
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
    df_mensal = df_processed[
        (df_processed['datasalva'].dt.year == ano_selecionado) &
        (df_processed['datasalva'].dt.month == mes_num)
    ]

    if df_mensal.empty:
        st.warning(f"Não há dados para {mes_selecionado} de {ano_selecionado}.")
        return

    st.markdown("---")

    # --- FILTRO POR PRODUTO ESPECÍFICO ---
    st.subheader("Filtro por Produto")
    
    tab1, tab2 = st.tabs(["Buscar por Descrição", "Buscar por Código"])
    
    codigo_para_filtrar = None

    # ABA 1: Busca por Descrição (Autocomplete)
    with tab1:
        # Cria a coluna "display" para o selectbox
        df_mensal[COLUNA_DESCRICAO] = df_mensal[COLUNA_DESCRICAO].astype(str)
        df_mensal[COLUNA_CODIGO] = df_mensal[COLUNA_CODIGO].astype(str).str.split('.').str[0]
        
        df_mensal['Display'] = df_mensal[COLUNA_DESCRICAO] + " (Código: " + df_mensal[COLUNA_CODIGO] + ")"
        
        descricao_busca = st.text_input("Digite a descrição ou parte dela:")
        
        if descricao_busca:
            resultados_parciais = df_mensal[df_mensal[COLUNA_DESCRICAO].str.contains(descricao_busca, case=False, na=False)]
            opcoes_unicas = resultados_parciais.drop_duplicates(subset=[COLUNA_CODIGO])
            lista_opcoes = ["Selecione um item..."] + opcoes_unicas['Display'].tolist()
        else:
            lista_opcoes = ["Digite algo para buscar..."]

        item_selecionado_display = st.selectbox("Selecione o produto na lista:", lista_opcoes)
        
        if item_selecionado_display and item_selecionado_display != "Selecione um item..." and item_selecionado_display != "Digite algo para buscar...":
            try:
                codigo_para_filtrar = int(re.search(r'\(Código: (\d+)\)', item_selecionado_display).group(1))
            except (AttributeError, ValueError):
                st.error("Não foi possível extrair o código do item selecionado.")

    # ABA 2: Busca por Código Direto
    with tab2:
        codigo_busca_direta = st.text_input("Ou digite o Código (apenas números):")
        if codigo_busca_direta:
            try:
                codigo_para_filtrar = int(codigo_busca_direta)
            except ValueError:
                st.warning("Código deve conter apenas números.")

    st.markdown("---")
    
    # --- ANÁLISE E EXIBIÇÃO DO GRÁFICO ---
    
    if codigo_para_filtrar:
        # Filtra pelo produto específico
        df_item = df_mensal[df_mensal[COLUNA_CODIGO].astype(int) == codigo_para_filtrar]
        
        if df_item.empty:
            st.warning(f"Nenhum produto encontrado com o código {codigo_para_filtrar} no mês selecionado.")
        else:
            # Agrupa e soma a quantidade para o produto específico
            estoque_item_dia = df_item.groupby('Data_Dia')['Qtd'].sum().reset_index()
            estoque_item_dia.columns = ['Data', 'Estoque Item']
            
            descricao = df_item[COLUNA_DESCRICAO].iloc[0]
            st.subheader(f"Evolução: {descricao}")
            
            st.line_chart(
                estoque_item_dia,
                x='Data',
                y='Estoque Item',
                use_container_width=True
            )
    
    else:
        # Se nenhum filtro for aplicado, mostra o estoque total
        st.subheader(f"Estoque Total - {mes_selecionado}/{ano_selecionado}")
        
        estoque_total_dia = df_mensal.groupby('Data_Dia')['Qtd'].sum().reset_index()
        estoque_total_dia.columns = ['Data', 'Estoque Total']

        st.line_chart(
            estoque_total_dia,
            x='Data',
            y='Estoque Total',
            use_container_width=True
        )
