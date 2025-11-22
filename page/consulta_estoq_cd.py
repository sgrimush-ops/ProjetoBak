import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple
import os

# --- Configurações e Path ---
COLUNA_DESCRICAO = 'Produto' 
COLUNA_ENDERECO = 'Endereço'

# --- Funções de Cache e Helpers ---

@st.cache_resource(ttl=timedelta(hours=24))
def get_today():
    """Retorna a data atual e força o cache a expirar a cada 24h."""
    return datetime.now().date()

@st.cache_data
def load_data(base_path_no_ext: str, mod_time: float) -> Optional[pd.DataFrame]:
    """
    Carrega dados do arquivo Parquet.
    'mod_time' é usado para invalidar o cache automaticamente quando o arquivo muda.
    """
    parquet_path = f"{base_path_no_ext}.parquet"
    
    # Tenta ler apenas o Parquet (Rápido)
    if os.path.exists(parquet_path):
        try:
            return pd.read_parquet(parquet_path)
        except Exception as e:
            st.error(f"Erro ao ler arquivo Parquet: {e}")
            return None
    else:
        # Se o arquivo não existir (ex: primeiro uso), retorna None silenciosamente
        # para ser tratado na interface principal
        return None

def preprocess_wms_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Pré-processa o DataFrame do WMS."""
    df = df.copy()
    
    # Validação de colunas necessárias
    if 'datasalva' not in df.columns or 'codigo' not in df.columns or 'Qtd' not in df.columns:
        st.error("Colunas essenciais do WMS (datasalva, codigo, Qtd) não encontradas.")
        return None

    df.dropna(axis=1, how='all', inplace=True)

    colunas_para_remover = ['Lote', 'Almoxarifado']
    df.drop(columns=[col for col in colunas_para_remover if col in df.columns], inplace=True)

    df['datasalva'] = pd.to_datetime(df['datasalva'], errors='coerce')
    df.dropna(subset=['datasalva'], inplace=True)
    df['datasalva_formatada'] = df['datasalva'].dt.date
    
    # Converte 'Qtd' para garantir a soma correta
    df['Qtd'] = pd.to_numeric(df['Qtd'], errors='coerce').fillna(0)
    
    # Garante que a coluna 'codigo' é int
    df['codigo'] = df['codigo'].fillna(0).astype(int)
    
    return df

def preprocess_mix_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Pré-processa o DataFrame do Mix para pegar a embalagem."""
    df = df.copy()
    # Mapeamento esperado do Mix
    cols_map = {'CODIGOINT': 'codigo', 'EmbSeparacao': 'embalagem'}
    
    # Renomeia se encontrar as colunas originais
    df.rename(columns={k:v for k,v in cols_map.items() if k in df.columns}, inplace=True)
    
    if 'codigo' not in df.columns or 'embalagem' not in df.columns:
        # Se não achar, retorna vazio mas não para o app (pode ser que o mix não tenha subido ainda)
        return pd.DataFrame(columns=['codigo', 'embalagem'])
        
    df['codigo'] = pd.to_numeric(df['codigo'], errors='coerce').fillna(0).astype(int)
    
    # Tratamento da embalagem (pode vir como string "12,00")
    df['embalagem'] = pd.to_numeric(
        df['embalagem'].astype(str).str.split(',').str[0].str.split('.').str[0].str.strip(),
        errors='coerce'
    ).fillna(1).astype(int) # Default 1 para evitar divisão por zero
    
    # Remove duplicatas (um código pode aparecer em várias lojas, pegamos a primeira embalagem que é igual)
    df = df[['codigo', 'embalagem']].drop_duplicates(subset=['codigo'])
    
    return df

# --- Função Principal de Exibição ---

def show_consulta_page(engine, base_data_path):
    """Cria a interface da página de consulta de produtos com busca por descrição."""
    st.title("Consulta de Itens por Descrição/Código")

    # 1. Definir Caminhos
    wms_base_path = os.path.join(base_data_path, "WMS")
    mix_base_path = os.path.join(base_data_path, "__MixAtivoSistema")

    # 2. Obter Data de Modificação (Para Cache Inteligente)
    try:
        # Se o arquivo não existir, definimos mod_time como 0.0
        wms_mod = os.path.getmtime(f"{wms_base_path}.parquet") if os.path.exists(f"{wms_base_path}.parquet") else 0.0
        mix_mod = os.path.getmtime(f"{mix_base_path}.parquet") if os.path.exists(f"{mix_base_path}.parquet") else 0.0
    except Exception:
        wms_mod, mix_mod = 0.0, 0.0

    # 3. Carregar Dados (Passando o mod_time para garantir atualização)
    df_wms_raw = load_data(wms_base_path, wms_mod)
    
    if df_wms_raw is None:
        st.error(f"Arquivo 'WMS.parquet' não encontrado. Faça o upload na página de Administração.")
        return

    df_wms = preprocess_wms_data(df_wms_raw)
    if df_wms is None:
        return

    # Carrega Mix
    df_mix_raw = load_data(mix_base_path, mix_mod)
    
    # Prepara o Mix (se existir)
    if df_mix_raw is not None:
        df_mix = preprocess_mix_data(df_mix_raw)
    else:
        df_mix = pd.DataFrame(columns=['codigo', 'embalagem'])

    # 3. Filtragem de Data
    hoje = get_today() 
    df_hoje = df_wms[df_wms['datasalva_formatada'] == hoje]

    if df_hoje.empty:
        st.warning(f"Não há informações para a data de hoje ({hoje.strftime('%d/%m/%Y')}).")
        st.info("Por favor, selecione uma data para pesquisar.")
        data_pesquisa = st.date_input("Escolha a data da pesquisa:", value=hoje)
        df_filtrado = df_wms[df_wms['datasalva_formatada'] == data_pesquisa]
    else:
        df_filtrado = df_hoje
    
    if df_filtrado.empty:
        st.info("Nenhum dado encontrado para a data selecionada.")
        return
        
    # --- CRUZAMENTO COM MIX ---
    # Adiciona a informação de embalagem ao dataframe filtrado
    if not df_mix.empty:
        df_filtrado = pd.merge(df_filtrado, df_mix, on='codigo', how='left')
        df_filtrado['embalagem'] = df_filtrado['embalagem'].fillna(1) # Se não achar, assume emb 1
    else:
        df_filtrado['embalagem'] = 1

    st.markdown("---")
    st.write(f"Dados exibidos para a data: **{df_filtrado['datasalva_formatada'].iloc[0].strftime('%d/%m/%Y')}**")

    # --- CAMPOS DE BUSCA ---
    st.subheader("Buscar Item")
    
    col_busca_desc, col_busca_cod = st.columns(2)

    with col_busca_desc:
        termo_busca = st.text_input("Digite a descrição ou parte dela:")

    with col_busca_cod:
        codigo_direto = st.text_input("Ou digite o Código (apenas números):")

    item_selecionado_code = None
    
    if codigo_direto and codigo_direto.isdigit():
        item_selecionado_code = int(codigo_direto)
        termo_busca = None 
        
    elif termo_busca:
        if COLUNA_DESCRICAO not in df_filtrado.columns:
             st.error(f"Coluna '{COLUNA_DESCRICAO}' não encontrada no WMS.")
             return

        df_filtrado['Descrição_Lower'] = df_filtrado[COLUNA_DESCRICAO].astype(str).str.lower()
        termo_lower = termo_busca.lower()
        
        mask = df_filtrado['Descrição_Lower'].str.contains(termo_lower, na=False)
        resultados_parciais = df_filtrado[mask].sort_values(by=COLUNA_DESCRICAO, ascending=True)

        opcoes_unicas = resultados_parciais.drop_duplicates(subset=['codigo'])
        
        lista_opcoes = opcoes_unicas.apply(
            lambda row: f"{row[COLUNA_DESCRICAO]} (Código: {row['codigo']})", 
            axis=1
        ).tolist()
        
        if lista_opcoes:
            escolha = st.selectbox(
                "Selecione o produto na lista:",
                options=[''] + lista_opcoes,
                index=0
            )
            
            if escolha:
                try:
                    code_str = escolha.split('(Código: ')[1].strip(')')
                    item_selecionado_code = int(float(code_str))
                except Exception as e:
                    st.error(f"Erro ao processar o código selecionado: {e}") 
                    pass 
        else:
            st.warning("Nenhum produto encontrado com o termo digitado.")

    # --- EXIBIÇÃO FINAL DO RESULTADO ---

    if item_selecionado_code:
        resultados_finais = df_filtrado[df_filtrado['codigo'] == item_selecionado_code].copy()

        if not resultados_finais.empty:
            st.write("### Resultado da Busca")
            
            descricao_produto = resultados_finais[COLUNA_DESCRICAO].iloc[0]
            emb_produto = int(resultados_finais['embalagem'].iloc[0])
            
            st.markdown(f"#### {descricao_produto}")
            st.caption(f"Embalagem: {emb_produto} un/cx")

            # Cálculos
            total_unidades = resultados_finais['Qtd'].sum()
            total_caixas = total_unidades / emb_produto
            
            # Exibe Métricas lado a lado
            col_metric1, col_metric2 = st.columns(2)
            col_metric1.metric(label="Total (Unidades)", value=f"{total_unidades:,.0f}")
            col_metric2.metric(label="Total (Caixas)", value=f"{total_caixas:,.1f} CX")
            
            # Calcula caixas para cada linha da tabela também
            resultados_finais['Qtd (Caixas)'] = (resultados_finais['Qtd'] / resultados_finais['embalagem']).round(1)

            if COLUNA_ENDERECO in resultados_finais.columns:
                enderecos_encontrados = resultados_finais[COLUNA_ENDERECO].unique()
                st.write("### Endereços")
                for endereco in enderecos_encontrados:
                    st.write(f"- {endereco}")
            else:
                # st.warning(f"Coluna '{COLUNA_ENDERECO}' não encontrada para exibição.")
                pass
            
            st.write("---")
            
            # Reordena colunas para mostrar as Caixas perto da Qtd
            cols_to_show = [c for c in resultados_finais.columns if c not in ['datasalva', 'datasalva_formatada', 'Descrição_Lower', 'embalagem']]
            # Tenta colocar 'Qtd (Caixas)' logo após 'Qtd'
            if 'Qtd' in cols_to_show and 'Qtd (Caixas)' in cols_to_show:
                cols_to_show.remove('Qtd (Caixas)')
                idx_qtd = cols_to_show.index('Qtd')
                cols_to_show.insert(idx_qtd + 1, 'Qtd (Caixas)')
                
            st.dataframe(resultados_finais[cols_to_show], hide_index=True)
        else:
            st.warning(f"Nenhum item encontrado com o código {item_selecionado_code} na data exibida.")
    
    elif not termo_busca and not codigo_direto:
        st.write("### Planilha do Dia (Primeiras Linhas)")
        # Calcula caixas para o preview também
        df_preview = df_filtrado.head(10).copy()
        df_preview['Qtd (Caixas)'] = (df_preview['Qtd'] / df_preview['embalagem']).round(1)
        
        cols_to_show = [c for c in df_preview.columns if c not in ['datasalva', 'datasalva_formatada', 'Descrição_Lower', 'embalagem']]
        if 'Qtd' in cols_to_show and 'Qtd (Caixas)' in cols_to_show:
            cols_to_show.remove('Qtd (Caixas)')
            idx_qtd = cols_to_show.index('Qtd')
            cols_to_show.insert(idx_qtd + 1, 'Qtd (Caixas)')
            
        st.dataframe(df_preview[cols_to_show], hide_index=True)
