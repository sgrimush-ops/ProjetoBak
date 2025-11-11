import streamlit as st
import os

# MUDANÇA: Esta função deve ter os mesmos argumentos que as outras
def show_admin_tools(engine, base_data_path):
    st.title("🔧 Ferramentas de Admin: Upload de Arquivos")
    st.info(f"Os arquivos serão salvos em: {base_data_path}")

    # --- Upload do WMS ---
    st.subheader("Upload do WMS")
    uploaded_wms = st.file_uploader("Selecione o arquivo WMS.xlsm", type="xlsm")
    
    if uploaded_wms:
        # Define o caminho completo onde o arquivo será salvo
        file_path = os.path.join(base_data_path, "WMS.xlsm")
        try:
            # Salva o arquivo no disco persistente
            with open(file_path, "wb") as f:
                f.write(uploaded_wms.getbuffer())
            st.success("Arquivo WMS.xlsm atualizado com sucesso!")
        except Exception as e:
            st.error(f"Falha ao salvar o arquivo: {e}")

    # --- Upload do Histórico ---
    st.subheader("Upload do Histórico de Solicitações")
    uploaded_hist = st.file_uploader("Selecione o arquivo historico_solic.xlsm", type="xlsm")
    
    if uploaded_hist:
        file_path = os.path.join(base_data_path, "historico_solic.xlsm")
        try:
            with open(file_path, "wb") as f:
                f.write(uploaded_hist.getbuffer())
            st.success("Arquivo historico_solic.xlsm atualizado com sucesso!")
        except Exception as e:
            st.error(f"Falha ao salvar o arquivo: {e}")

    # --- Upload do Mix ---
    st.subheader("Upload do Mix Ativo")
    uploaded_mix = st.file_uploader("Selecione o arquivo __MixAtivoSistema.xlsx", type="xlsx")
    
    if uploaded_mix:
        file_path = os.path.join(base_data_path, "__MixAtivoSistema.xlsx")
        try:
            with open(file_path, "wb") as f:
                f.write(uploaded_mix.getbuffer())
            st.success("Arquivo __MixAtivoSistema.xlsx atualizado com sucesso!")
        except Exception as e:
            st.error(f"Falha ao salvar o arquivo: {e}")

