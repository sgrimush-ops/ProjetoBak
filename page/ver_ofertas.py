import streamlit as st
import pandas as pd
from sqlalchemy import text
from datetime import datetime, date

# =========================================================
# FUNÇÕES DE PROCESSAMENTO
# =========================================================

def processar_upload(engine, df, data_inicio, data_final):
    """
    Processa o DataFrame, valida e faz o "upsert" no banco de dados.
    """
    
    # O DataFrame já chega aqui com as colunas certas ['codigo', 'produto', 'oferta']
    df_processado = df.copy()
    
    # 2. Limpeza e Validação dos Dados
    try:
        # Codigo: Remove não numéricos, preenche com 0, converte para int
        df_processado['codigo'] = pd.to_numeric(df_processado['codigo'], errors='coerce').fillna(0).astype(int)
        # Oferta: Converte para numérico (float), arredonda para 2 casas
        df_processado['oferta'] = pd.to_numeric(df_processado['oferta'], errors='coerce').fillna(0).round(2)
        # Produto: Converte para string e limpa espaços
        df_processado['produto'] = df_processado['produto'].astype(str).str.strip()
        
        # Adiciona as datas
        df_processado['data_inicio'] = data_inicio
        df_processado['data_final'] = data_final
        
        # Remove linhas onde o código é 0 (inválido ou linha vazia)
        df_processado = df_processado[df_processado['codigo'] != 0]
        
    except Exception as e:
        st.error(f"Erro ao processar os tipos de dados do arquivo: {e}")
        return False, 0, 0
        
    if df_processado.empty:
        st.warning("Nenhum dado válido encontrado no arquivo após a limpeza.")
        return False, 0, 0

    # 3. Lógica de UPSERT no Banco de Dados (PostgreSQL)
    # Se o código+datas já existir, atualiza o preço e nome.
    upsert_query = text("""
        INSERT INTO ofertas (codigo, produto, oferta, data_inicio, data_final)
        VALUES (:codigo, :produto, :oferta, :data_inicio, :data_final)
        ON CONFLICT (codigo, data_inicio, data_final) 
        DO UPDATE SET
            oferta = EXCLUDED.oferta,
            produto = EXCLUDED.produto
        WHERE 
            ofertas.oferta IS DISTINCT FROM EXCLUDED.oferta
            OR ofertas.produto IS DISTINCT FROM EXCLUDED.produto
    """)
    
    records = df_processado.to_dict('records')
    
    try:
        with engine.begin() as conn:
            result = conn.execute(upsert_query, records)
            total_afetado = result.rowcount 
            
        return True, total_afetado, len(records)
        
    except Exception as e:
        st.error(f"Erro ao salvar dados no banco: {e}")
        return False, 0, 0

# =========================================================
# INTERFACE DA PÁGINA
# =========================================================

def show_upload_ofertas_page(engine, base_data_path):
    st.title("🚀 Upload de Ofertas (Marketing)")
    
    st.info("Faça o upload do arquivo (.xls ou .xlsx). O sistema lerá as colunas A, B e E automaticamente.")

    # 1. Seleção de Data
    st.subheader("1. Defina a Vigência da Oferta")
    today = datetime.now().date()
    col1, col2 = st.columns(2)
    data_inicio = col1.date_input("Data de Início", value=today)
    data_final = col2.date_input("Data Final", value=today)

    if data_final < data_inicio:
        st.error("A 'Data Final' não pode ser anterior à 'Data de Início'.")
        st.stop()

    # 2. Upload do Arquivo
    st.subheader("2. Selecione o Arquivo")
    st.markdown("""
    O sistema lerá as seguintes colunas pela **posição**:
    - **Coluna A (1ª)** -> Código
    - **Coluna B (2ª)** -> Produto
    - **Coluna E (5ª)** -> Preço de Oferta
    """)
    
    uploaded_file = st.file_uploader("Escolha um arquivo Excel", type=["xls", "xlsx"])

    if uploaded_file:
        try:
            # MUDANÇA: Lê sem cabeçalho (header=None) para pegar a linha 1 como dados
            # Lê apenas colunas A(0), B(1) e E(4)
            df = pd.read_excel(uploaded_file, header=None, usecols=[0, 1, 4])
            
            # MUDANÇA: Verifica se a primeira linha é cabeçalho (texto) ou dado (número)
            if not df.empty:
                primeira_celula = df.iloc[0, 0] # Coluna A, Linha 0
                
                # Tenta converter para número. Se falhar, é texto (cabeçalho) -> Remove a linha
                try:
                    float(primeira_celula)
                    # É número, então não tem cabeçalho, mantém tudo.
                except (ValueError, TypeError):
                    # É texto (ex: "Produto"), então é cabeçalho -> Remove a linha 0
                    df = df.iloc[1:].reset_index(drop=True)

            # Renomeia as colunas para o padrão interno
            df.columns = ['codigo', 'produto', 'oferta']
                
        except Exception as e:
            st.error(f"Erro ao ler o arquivo: {e}")
            if "xlrd" in str(e):
                st.error("Erro de dependência: O arquivo é .xls antigo. Certifique-se que 'xlrd' está instalado no servidor.")
            st.stop()

        # Preview dos dados que serão importados
        with st.expander("Pré-visualização dos dados (Primeiras 5 linhas)"):
            st.dataframe(df.head())

        if st.button(f"Processar Ofertas", type="primary"):
            with st.spinner("Processando e salvando..."):
                success, total_afetado, total_tentado = processar_upload(engine, df, data_inicio, data_final)
                
            if success:
                st.success(f"Sucesso! {total_afetado} registros inseridos/atualizados (de {total_tentado} lidos).")
            else:
                st.error("Ocorreu um erro durante o processamento.")
