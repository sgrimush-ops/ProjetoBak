import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple
import os

# --- Configurações e Path ---
FILE_PATH = 'data/WMS.xlsm'
# IMPORTANTE: Coloque o nome EXATO da sua coluna de Descrição.
COLUNA_DESCRICAO = 'Produto' 
COLUNA_ENDERECO = 'Endereço'


# --- Funções de Cache e Helpers ---

@st.cache_resource(ttl=timedelta(hours=24))
def get_today():
    """Retorna a data atual e força o cache a expirar a cada 24h."""
    return datetime.now().date()

@st.cache_data
def load_data(file_path: str, mod_time: float) -> Optional[pd.DataFrame]:
    """Carrega dados do arquivo Excel especificado."""
    try:
        # Altere 'WMS' para o nome da aba correta se necessário
        return pd.read_excel(file_path, sheet_name='WMS')
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo: {e}")
        return None

def preprocess_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Pré-processa o DataFrame limpando colunas e manipulando datas."""
    df = df.copy()
    
    # Validação de colunas necessárias
    if 'datasalva' not in df.columns or 'codigo' not in df.columns or 'Qtd' not in df.columns:
        st.error("Colunas essenciais (datasalva, codigo, Qtd) não encontradas.")
        return None
    if COLUNA_DESCRICAO not in df.columns:
        st.error(f"Coluna de descrição ('{COLUNA_DESCRICAO}') não encontrada.")
        return None

    df.dropna(axis=1, how='all', inplace=True)

    colunas_para_remover = ['Lote', 'Almoxarifado']
    df.drop(columns=[col for col in colunas_para_remover if col in df.columns], inplace=True)

    df['datasalva'] = pd.to_datetime(df['datasalva'], errors='coerce')
    df.dropna(subset=['datasalva'], inplace=True)
    df['datasalva_formatada'] = df['datasalva'].dt.date
    
    # Converte 'Qtd' para garantir a soma correta
    df['Qtd'] = pd.to_numeric(df['Qtd'], errors='coerce') 
    
    # Garante que a coluna 'codigo' é int, resolvendo o problema de comparação
    df['codigo'] = df['codigo'].fillna(0).astype(int)
    
    return df

# --- Função Principal de Exibição ---

def show_consulta_page():
    """Cria a interface da página de consulta de produtos com busca por descrição."""
    st.title("Consulta de Itens por Descrição/Código")

    # 1. Carregamento e Pré-processamento
    try:
        mod_time = os.path.getmtime(FILE_PATH)
    except FileNotFoundError:
        st.error(f"Arquivo '{FILE_PATH}' não encontrado. Verifique se o caminho está correto.")
        return
        
    df_raw = load_data(FILE_PATH, mod_time)
    if df_raw is None:
        return
    df_processed = preprocess_data(df_raw)
    if df_processed is None:
        return

    # 2. Filtragem de Data (Mesma lógica de antes)
    hoje = get_today() 
    df_hoje = df_processed[df_processed['datasalva_formatada'] == hoje]

    if df_hoje.empty:
        st.warning(f"Não há informações para a data de hoje ({hoje.strftime('%d/%m/%Y')}).")
        st.info("Por favor, selecione uma data para pesquisar.")
        data_pesquisa = st.date_input("Escolha a data da pesquisa:", value=hoje)
        df_filtrado = df_processed[df_processed['datasalva_formatada'] == data_pesquisa]
    else:
        df_filtrado = df_hoje
    
    if df_filtrado.empty:
        st.info("Nenhum dado encontrado para a data selecionada.")
        return
        
    st.markdown("---")
    st.write(f"Dados exibidos para a data: **{df_filtrado['datasalva_formatada'].iloc[0].strftime('%d/%m/%Y')}**")

    # --- CAMPOS DE BUSCA ---
    st.subheader("Buscar Item")
    
    col_busca_desc, col_busca_cod = st.columns(2)

    with col_busca_desc:
        # Campo de texto para digitar a descrição (o "autocomplete")
        termo_busca = st.text_input("Digite a descrição ou parte dela:")

    with col_busca_cod:
        # Campo de texto para buscar diretamente pelo código
        codigo_direto = st.text_input("Ou digite o Código (apenas números):")

    item_selecionado_code = None
    
    if codigo_direto and codigo_direto.isdigit():
        # 1. Se o usuário digitar um código diretamente
        item_selecionado_code = int(codigo_direto)
        termo_busca = None # Ignora a busca por descrição
        
    elif termo_busca:
        # 2. Se o usuário estiver digitando a descrição (Autocomplete)
        
        # Converte para minúsculas e remove acentos para facilitar a busca (Opcional, mas recomendado)
        df_filtrado['Descrição_Lower'] = df_filtrado[COLUNA_DESCRICAO].astype(str).str.lower()
        termo_lower = termo_busca.lower()
        
        # Filtra a coluna de descrição que contém o termo
        mask = df_filtrado['Descrição_Lower'].str.contains(termo_lower, na=False)
        resultados_parciais = df_filtrado[mask].sort_values(by=COLUNA_DESCRICAO, ascending=True)

        # Remove duplicatas, mantendo a descrição única com seu código
        opcoes_unicas = resultados_parciais.drop_duplicates(subset=['codigo'])
        
        # Cria uma lista de strings formatadas: "DESCRIÇÃO (Código: 123456)"
        lista_opcoes = opcoes_unicas.apply(
            lambda row: f"{row[COLUNA_DESCRICAO]} (Código: {row['codigo']})", 
            axis=1
        ).tolist()
        
        if lista_opcoes:
            # Exibe o dropdown para seleção (funciona como o autocomplete)
            escolha = st.selectbox(
                "Selecione o produto na lista:",
                options=[''] + lista_opcoes,
                index=0
            )
            
            if escolha:
                # Extrai o código do final da string selecionada
                try:
                    # Encontra o valor do código dentro do parênteses
                    # E garante que o valor seja convertido para INT, corrigindo o erro.
                    code_str = escolha.split('(Código: ')[1].strip(')')
                    item_selecionado_code = int(float(code_str)) # Conversão segura (str -> float -> int)
                except Exception as e:
                    st.error(f"Erro ao processar o código selecionado: {e}") 
                    pass 
        else:
            st.warning("Nenhum produto encontrado com o termo digitado.")

    # --- EXIBIÇÃO FINAL DO RESULTADO ---

    if item_selecionado_code:
        # Filtra o DataFrame filtrado por data usando o código final
        resultados_finais = df_filtrado[df_filtrado['codigo'] == item_selecionado_code]

        if not resultados_finais.empty:
            st.write("### Resultado da Busca")
            
            # Exibe Descrição
            descricao_produto = resultados_finais[COLUNA_DESCRICAO].iloc[0]
            st.markdown(f"#### {descricao_produto}")

            # Sumariza a quantidade
            total_quantidade = resultados_finais['Qtd'].sum()
            st.metric(label="Total de Quantidade", value=f"{total_quantidade:,.0f}")
            
            # Pega os endereços
            if COLUNA_ENDERECO in resultados_finais.columns:
                enderecos_encontrados = resultados_finais[COLUNA_ENDERECO].unique()
                st.write("### Endereços")
                for endereco in enderecos_encontrados:
                    st.write(f"- {endereco}")
            else:
                st.warning(f"Coluna '{COLUNA_ENDERECO}' não encontrada para exibição.")
            
            st.write("---")
            st.dataframe(resultados_finais)
        else:
            st.warning(f"Nenhum item encontrado com o código {item_selecionado_code} na data exibida.")
    
    # Se nada foi buscado ou selecionado, mostra a planilha inteira (filtrada por data)
    elif not termo_busca and not codigo_direto:
        st.write("### Planilha do Dia (Primeiras Linhas)")
        st.dataframe(df_filtrado.head(10)) # Exibe apenas as 10 primeiras linhas para performance
        
