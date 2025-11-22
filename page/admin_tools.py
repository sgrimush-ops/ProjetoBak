import streamlit as st
import os
import pandas as pd

def save_file_as_parquet(uploaded_file, target_path_no_ext):
    """
    Lê o arquivo Excel enviado e salva uma versão otimizada .parquet.
    Retorna True se sucesso.
    """
    try:
        # Lê o Excel (processo lento, mas feito apenas uma vez no upload)
        if uploaded_file.name.endswith('.csv'):
             df = pd.read_csv(uploaded_file)
        else:
             df = pd.read_excel(uploaded_file)
             
        # Salva como Parquet (processo ultra rápido para leitura futura)
        parquet_path = f"{target_path_no_ext}.parquet"
        df.to_parquet(parquet_path, index=False)
        return True
    except Exception as e:
        st.error(f"Erro ao converter para Parquet: {e}")
        return False

def show_admin_tools(engine, base_data_path):
    st.title("🔧 Ferramentas de Admin: Upload de Arquivos")
    st.info(f"Os arquivos são convertidos automaticamente para formato acelerado (.parquet).")

    # --- Upload do WMS ---
    st.subheader("1. Upload do WMS (Estoque CD)")
    uploaded_wms = st.file_uploader("Selecione o WMS.xlsm", type=["xlsm", "xlsx"])
    
    if uploaded_wms:
        if st.button("Salvar WMS"):
            with st.spinner("Processando e otimizando WMS..."):
                # Salva original
                file_path = os.path.join(base_data_path, "WMS.xlsm")
                with open(file_path, "wb") as f:
                    f.write(uploaded_wms.getbuffer())
                
                # Salva versão rápida
                target_base = os.path.join(base_data_path, "WMS") # Vai virar WMS.parquet
                if save_file_as_parquet(uploaded_wms, target_base):
                    st.success("WMS atualizado e otimizado com sucesso!")

    # --- Upload do Histórico ---
    st.subheader("2. Upload do Histórico de Solicitações")
    uploaded_hist = st.file_uploader("Selecione o historico_solic.xlsm", type=["xlsm", "xlsx"])
    
    if uploaded_hist:
        if st.button("Salvar Histórico"):
            with st.spinner("Processando e otimizando Histórico... (Isso pode demorar um pouco)"):
                # Salva original
                file_path = os.path.join(base_data_path, "historico_solic.xlsm")
                with open(file_path, "wb") as f:
                    f.write(uploaded_hist.getbuffer())
                
                # Salva versão rápida
                target_base = os.path.join(base_data_path, "historico_solic")
                if save_file_as_parquet(uploaded_hist, target_base):
                    st.success("Histórico atualizado e otimizado com sucesso!")

    # --- Upload do Mix ---
    st.subheader("3. Upload do Mix Ativo")
    uploaded_mix = st.file_uploader("Selecione o __MixAtivoSistema.xlsx", type=["xlsx", "xls"])
    
    if uploaded_mix:
        if st.button("Salvar Mix"):
            with st.spinner("Processando e otimizando Mix..."):
                # Salva original
                file_path = os.path.join(base_data_path, "__MixAtivoSistema.xlsx")
                with open(file_path, "wb") as f:
                    f.write(uploaded_mix.getbuffer())
                
                # Salva versão rápida
                target_base = os.path.join(base_data_path, "__MixAtivoSistema")
                if save_file_as_parquet(uploaded_mix, target_base):
                    st.success("Mix atualizado e otimizado com sucesso!")