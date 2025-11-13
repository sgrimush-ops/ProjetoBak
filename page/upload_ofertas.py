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
    'Upsert' = Insere se for novo, Atualiza o preço se (codigo, data_inicio, data_final) já existir.
    """
    
    # 1. Definir o nome das colunas a serem lidas (A, B, E)
    col_map = {
        'produto': 'codigo',   # Coluna A
        'Descrição': 'produto',  # Coluna B
        'Vlr. Venda': 'oferta'   # Coluna E
    }
    
    # Verifica se as colunas esperadas existem no upload
    cols_necessarias = list(col_map.keys())
    if not all(col in df.columns for col in cols_necessarias):
        st.error(f"Erro: O arquivo enviado não contém as colunas esperadas: {', '.join(cols_necessarias)}")
        return False, 0, 0
    
    # Renomeia e seleciona apenas as colunas que importam
    df_renomeado = df[cols_necessarias].rename(columns=col_map)
    
    # 2. Limpeza e Validação dos Dados
    try:
        # Codigo: Remove não numéricos, preenche com 0, converte para int
        df_renomeado['codigo'] = pd.to_numeric(df_renomeado['codigo'], errors='coerce').fillna(0).astype(int)
        # Oferta: Converte para numérico (float), arredonda para 2 casas
        df_renomeado['oferta'] = pd.to_numeric(df_renomeado['oferta'], errors='coerce').fillna(0).round(2)
        # Produto: Converte para string
        df_renomeado['produto'] = df_renomeado['produto'].astype(str)
        
        # Adiciona as datas
        df_renomeado['data_inicio'] = data_inicio
        df_renomeado['data_final'] = data_final
        
        # Remove linhas onde o código é 0 (inválido)
        df_renomeado = df_renomeado[df_renomeado['codigo'] != 0]
        
    except Exception as e:
        st.error(f"Erro ao processar os tipos de dados do arquivo: {e}")
        return False, 0, 0
        
    if df_renomeado.empty:
        st.warning("Nenhum dado válido encontrado no arquivo após a limpeza.")
        return False, 0, 0

    # 3. Lógica de UPSERT no Banco de Dados (PostgreSQL)
    # Esta query é complexa, mas faz exatamente o que você pediu:
    # - ON CONFLICT: Se (codigo, data_inicio, data_final) já existir...
    # - DO UPDATE SET: ...atualize o preço (oferta) E QUANDO o preço for diferente.
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
    
    # Converte o DataFrame para uma lista de dicionários para o SQLAlchemy
    records = df_renomeado.to_dict('records')
    
    inseridos = 0
    atualizados = 0 # SQLAlchemy não nos diz facilmente quantos foram atualizados vs inseridos
                    # em um upsert, mas sabemos o total.
    
    try:
        with engine.begin() as conn:
            result = conn.execute(upsert_query, records)
            # rowcount nos diz quantas linhas foram afetadas (inseridas + atualizadas)
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
    
    st.info("Faça o upload do arquivo de ofertas (.csv ou .xlsx) e defina o período de vigência.")

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
    O arquivo deve conter **exatamente** os seguintes cabeçalhos nas colunas:
    - Coluna A: **produto** (Será o `codigo`)
    - Coluna B: **Descrição** (Será o `produto`)
    - Coluna E: **Vlr. Venda** (Será a `oferta`)
    """)
    
    uploaded_file = st.file_uploader("Escolha um arquivo (.csv ou .xlsx)", type=["csv", "xlsx"])

    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
        except Exception as e:
            st.error(f"Erro ao ler o arquivo: {e}")
            st.stop()

        if st.button(f"Processar {uploaded_file.name}", type="primary"):
            with st.spinner("Processando e salvando ofertas..."):
                success, total_afetado, total_tentado = processar_upload(engine, df, data_inicio, data_final)
                
            if success:
                st.success(f"Upload concluído! {total_afetado} de {total_tentado} registros foram inseridos ou atualizados.")
                st.info("Registros duplicados (com o mesmo preço) foram ignorados.")
            else:
                st.error("Ocorreu um erro durante o processamento.")
