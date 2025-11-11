import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple
import os
import re

# --- Configurações Iniciais ---
# (Os caminhos serão definidos na função principal)

# --- Nomes das Colunas (Conforme sua descrição) ---
COL_HIST_CODIGO = 'CODIGOINT'
COL_HIST_EMBALAGEM = 'EmbSeparacao'
COL_HIST_ESTOQUE_LOJA = 'EstCX'
COL_HIST_PEDIDOS = 'PedCX'
COL_HIST_DATA = 'DtSolicitacao'
COL_HIST_DESCRICAO = 'Produto'

COL_WMS_CODIGO = 'codigo'
COL_WMS_QTD = 'Qtd'
COL_WMS_DATA = 'datasalva'

MESES_DISPONIVEIS = {
    "Janeiro": 1, "Fevereiro": 2, "Março": 3, "Abril": 4, "Maio": 5, "Junho": 6,
    "Julho": 7, "Agosto": 8, "Setembro": 9, "Outubro": 10, "Novembro": 11, "Dezembro": 12
}
MESES_INVERSO = {v: k for k, v in MESES_DISPONIVEIS.items()}


# --- Funções de Carregamento de Dados (Cacheadas) ---

@st.cache_data
def load_wms_data(file_path: str) -> Optional[pd.DataFrame]:
    """Carrega DADOS COMPLETOS do WMS (estoque do CD)."""
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
        df['Qtd_CD'] = pd.to_numeric(df['Qtd_CD'], errors='coerce').fillna(0)
        
        # MUDANÇA: Retorna o DataFrame completo, não apenas o último dia
        return df
        
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo WMS ({file_path}): {e}")
        return None

@st.cache_data
def load_hist_data(file_path: str) -> Optional[pd.DataFrame]:
    """Carrega dados do Histórico de Solicitações (Lojas)."""
    try:
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
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce')
        df.dropna(subset=['Data', 'Codigo'], inplace=True)
        df['Codigo'] = df['Codigo'].astype(int)
        
        df['Estoque_Lojas'] = pd.to_numeric(df['Estoque_Lojas'], errors='coerce').fillna(0)
        df['Pedidos'] = pd.to_numeric(df['Pedidos'], errors='coerce').fillna(0)
        df['Embalagem'] = pd.to_numeric(df['Embalagem'], errors='coerce')
        
        return df
        
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo Histórico ({file_path}): {e}")
        return None

# --- Função Principal da Página ---

def show_historico_page(engine, base_data_path):
    st.title("📊 Histórico de Transferência (Completo)")

    # --- Definir Caminhos e Carregar Dados ---
    hist_file_path = os.path.join(base_data_path, "historico_solic.xlsm")
    wms_file_path = os.path.join(base_data_path, "WMS.xlsm")

    df_hist_full = load_hist_data(hist_file_path)
    df_wms_full = load_wms_data(wms_file_path) # MUDANÇA: Carrega o WMS completo

    if df_hist_full is None or df_wms_full is None:
        st.error("Falha ao carregar um ou mais arquivos de dados. Verifique os uploads.")
        return

    # --- Criar Mapa de Embalagens ---
    # Usamos isso para converter o estoque do CD de Unidades para Caixas
    embalagem_map = df_hist_full[df_hist_full['Embalagem'] > 0][['Codigo', 'Embalagem']]
    embalagem_map = embalagem_map.drop_duplicates(subset=['Codigo']).set_index('Codigo')

    # --- Lógica de Data Padrão ---
    default_date = df_hist_full['Data'].max()
    if pd.isna(default_date):
        st.error("Não foi possível encontrar uma data válida no arquivo histórico.")
        return
        
    ano_atual = default_date.year
    mes_atual_num = default_date.month
    mes_atual_nome = MESES_INVERSO.get(mes_atual_num, "Janeiro")
    anos_disponiveis = sorted(df_hist_full['Data'].dt.year.unique(), reverse=True)
    
    try: index_ano = anos_disponiveis.index(ano_atual)
    except ValueError: index_ano = 0
    
    lista_meses_nomes = list(MESES_DISPONIVEIS.keys())
    try: index_mes = lista_meses_nomes.index(mes_atual_nome)
    except ValueError: index_mes = 0

    # --- Seletores de Data ---
    st.subheader("Selecione o Período")
    col1, col2 = st.columns(2)
    with col1:
        ano_selecionado = st.selectbox("Selecione o Ano", anos_disponiveis, index=index_ano)
    with col2:
        mes_selecionado = st.selectbox("Selecione o Mês", lista_meses_nomes, index=index_mes)
    
    mes_num = MESES_DISPONIVEIS[mes_selecionado]

    # --- Filtrar AMBOS os DataFrames pelo período ---
    df_hist_mensal = df_hist_full[
        (df_hist_full['Data'].dt.year == ano_selecionado) &
        (df_hist_full['Data'].dt.month == mes_num)
    ]
    df_wms_mensal = df_wms_full[
        (df_wms_full['Data'].dt.year == ano_selecionado) &
        (df_wms_full['Data'].dt.month == mes_num)
    ]

    if df_hist_mensal.empty:
        st.warning(f"Não há dados no Histórico para {mes_selecionado} de {ano_selecionado}.")
        return

    st.markdown("---")

    # --- Filtro por Produto ---
    st.subheader("Filtro por Produto (Opcional)")
    tab1, tab2 = st.tabs(["Buscar por Descrição", "Buscar por Código"])
    
    codigo_para_filtrar = None
    item_selecionado_display = "Digite algo para buscar..."

    with tab1:
        df_hist_mensal['Display'] = df_hist_mensal['Descricao'].astype(str) + " (Código: " + df_hist_mensal['Codigo'].astype(str) + ")"
        descricao_busca = st.text_input("Digite a descrição ou parte dela:")
        
        if descricao_busca:
            resultados_parciais = df_hist_mensal[df_hist_mensal['Descricao'].str.contains(descricao_busca, case=False, na=False)]
            opcoes_unicas = resultados_parciais.drop_duplicates(subset=['Codigo'])
            lista_opcoes = ["Selecione um item..."] + opcoes_unicas['Display'].tolist()
        else:
            lista_opcoes = ["Digite algo para buscar..."]

        item_selecionado_display = st.selectbox("Selecione o produto na lista:", lista_opcoes)
        
        if item_selecionado_display and item_selecionado_display not in ["Selecione um item...", "Digite algo para buscar..."]:
            try:
                codigo_para_filtrar = int(re.search(r'\(Código: (\d+)\)', item_selecionado_display).group(1))
            except (AttributeError, ValueError):
                st.error("Não foi possível extrair o código do item selecionado.")

    with tab2:
        codigo_busca_direta = st.text_input("Ou digite o Código (apenas números):")
        if codigo_busca_direta:
            try:
                codigo_para_filtrar = int(codigo_busca_direta)
                # Pega o nome do item para o título
                nome_item = df_hist_full[df_hist_full['Codigo'] == codigo_para_filtrar]['Descricao'].iloc[0]
                item_selecionado_display = f"{nome_item} (Código: {codigo_para_filtrar})"
            except (ValueError, IndexError):
                st.warning("Código não encontrado no histórico.")

    st.markdown("---")
    
    # --- PREPARAÇÃO DOS DADOS PARA O GRÁFICO ---
    
    if codigo_para_filtrar:
        # 1. ANÁLISE POR ITEM
        st.subheader(f"Análise: {item_selecionado_display}")
        
        # --- Lógica do Histórico (Lojas/Pedidos) ---
        df_item_hist = df_hist_mensal[df_hist_mensal['Codigo'] == codigo_para_filtrar]
        df_lojas_grafico = df_item_hist.groupby(df_item_hist['Data'].dt.date).agg(
            Pedidos_Item=('Pedidos', 'sum'),
            Estoque_Lojas_Item=('Estoque_Lojas', 'sum')
        ).reset_index().rename(columns={'Data': 'Dia'})

        # --- Lógica do WMS (Estoque CD) ---
        df_item_wms = df_wms_mensal[df_wms_mensal['Codigo'] == codigo_para_filtrar]
        df_cd_grafico = df_item_wms.groupby(df_item_wms['Data'].dt.date).agg(
            Estoque_CD_Unidades=('Qtd_CD', 'sum')
        ).reset_index().rename(columns={'Data': 'Dia'})

        # Converter CD para Caixas
        try:
            embalagem = embalagem_map.loc[codigo_para_filtrar, 'Embalagem']
            if pd.notna(embalagem) and embalagem > 0:
                df_cd_grafico['Estoque_CD_Item'] = df_cd_grafico['Estoque_CD_Unidades'] / embalagem
            else:
                df_cd_grafico['Estoque_CD_Item'] = 0 # Embalagem não encontrada ou é 0
        except KeyError:
            st.warning(f"Não foi encontrada embalagem para o código {codigo_para_filtrar}. Estoque CD será 0.")
            df_cd_grafico['Estoque_CD_Item'] = 0
            
        # --- Junção dos Dados ---
        if not df_cd_grafico.empty:
            df_cd_grafico = df_cd_grafico[['Dia', 'Estoque_CD_Item']]
            df_final_grafico = pd.merge(df_lojas_grafico, df_cd_grafico, on='Dia', how='outer')
        else:
            df_final_grafico = df_lojas_grafico.copy()
            df_final_grafico['Estoque_CD_Item'] = 0 # Nenhum dado de WMS para este item/mês
            
        df_final_grafico = df_final_grafico.set_index('Dia')
        
    else:
        # 2. ANÁLISE TOTAL (Lógica simplificada, pode não ser 100% precisa sem item)
        st.subheader(f"Análise Total - {mes_selecionado}/{ano_selecionado}")
        
        df_final_grafico = df_hist_mensal.groupby(df_hist_mensal['Data'].dt.date).agg(
            Total_Pedidos=('Pedidos', 'sum'),
            Total_Estoque_Lojas=('Estoque_Lojas', 'sum')
        ).reset_index().rename(columns={'Data': 'Dia'}).set_index('Dia')
        
        st.info("Selecione um item para ver a análise completa (incluindo Estoque CD).")

    # Exibe o gráfico de linhas
    if not df_final_grafico.empty:
        st.line_chart(
            df_final_grafico,
            x='Dia',
            y=['Estoque_CD_Item', 'Estoque_Lojas_Item', 'Pedidos_Item'], # Reordenado para cor
            color=["#0000FF", "#FFA500", "#008000"], # MUDANÇA: Azul, Laranja, Verde para Pedidos_Item
            use_container_width=True
        )
        
        with st.expander("Ver dados da tabela"):
            st.dataframe(df_final_grafico)
    else:
        st.warning("Nenhum dado encontrado para exibir no gráfico.")

