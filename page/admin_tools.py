import streamlit as st
import os
import pandas as pd
from datetime import datetime

# Função auxiliar para formatar a data do arquivo
def get_file_info(file_path):
    if os.path.exists(file_path):
        # Pega a data de modificação (timestamp)
        mod_time = os.path.getmtime(file_path)
        # Converte para string legível
        return datetime.fromtimestamp(mod_time).strftime('%d/%m/%Y às %H:%M:%S')
    return "Ainda não enviado"

def save_file_as_parquet(uploaded_file, target_path_no_ext):
    """
    Lê o arquivo Excel enviado e salva uma versão otimizada .parquet.
    Retorna True se sucesso.
    """
    try:
        # Reseta o ponteiro do arquivo para garantir leitura desde o início
        uploaded_file.seek(0)
        
        if uploaded_file.name.endswith('.csv'):
             df = pd.read_csv(uploaded_file)
        else:
             df = pd.read_excel(uploaded_file)
             
        # Salva como Parquet
        parquet_path = f"{target_path_no_ext}.parquet"
        df.to_parquet(parquet_path, index=False)
        return True
    except Exception as e:
        st.error(f"Erro ao converter para Parquet: {e}")
        return False

def process_automatic_upload(uploaded_file, file_path_ext, base_path_no_ext, file_key):
    """
    Gerencia o upload automático: Salva, Converte e Atualiza a tela.
    """
    if uploaded_file:
        # Cria um ID único para este upload (nome + tamanho) para evitar reprocessamento
        file_id = f"{uploaded_file.name}_{uploaded_file.size}"
        
        # Se este arquivo exato ainda não foi processado nesta sessão
        if st.session_state.get(f"processed_{file_key}") != file_id:
            
            progress_bar = st.progress(0, text="Iniciando upload...")
            
            try:
                # 1. Salva o arquivo original (Excel/CSV)
                progress_bar.progress(30, text="Salvando arquivo original...")
                with open(file_path_ext, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                # 2. Converte e Salva o Parquet
                progress_bar.progress(60, text="Gerando arquivo otimizado (.parquet)...")
                if save_file_as_parquet(uploaded_file, base_path_no_ext):
                    
                    # Marca como processado para não entrar em loop
                    st.session_state[f"processed_{file_key}"] = file_id
                    
                    progress_bar.progress(100, text="Concluído!")
                    st.success("Arquivo atualizado e otimizado com sucesso!")
                    
                    # Força recarregamento para atualizar a data na tela
                    st.rerun() 
            except Exception as e:
                st.error(f"Erro no processamento: {e}")
            finally:
                progress_bar.empty()

def show_admin_tools(engine, base_data_path):
    st.title("🔧 Ferramentas de Admin: Upload de Arquivos")
    st.info("Basta arrastar os arquivos. A conversão para .parquet e atualização de data são automáticas.")

    # --- 1. WMS ---
    st.subheader("1. WMS (Estoque CD)")
    wms_path = os.path.join(base_data_path, "WMS.xlsm")
    wms_base = os.path.join(base_data_path, "WMS")
    
    # Mostra a data atual do arquivo no disco
    st.caption(f"📅 Última atualização: **{get_file_info(wms_path)}**")
    
    uploaded_wms = st.file_uploader("Selecione o WMS.xlsm", type=["xlsm", "xlsx"], key="wms_uploader")
    process_automatic_upload(uploaded_wms, wms_path, wms_base, "wms")

    st.markdown("---")

    # --- 2. Histórico ---
    st.subheader("2. Histórico de Solicitações")
    hist_path = os.path.join(base_data_path, "historico_solic.xlsm")
    hist_base = os.path.join(base_data_path, "historico_solic")
    
    st.caption(f"📅 Última atualização: **{get_file_info(hist_path)}**")
    
    uploaded_hist = st.file_uploader("Selecione o historico_solic.xlsm", type=["xlsm", "xlsx"], key="hist_uploader")
    process_automatic_upload(uploaded_hist, hist_path, hist_base, "hist")

    st.markdown("---")

    # --- 3. Mix ---
    st.subheader("3. Mix Ativo")
    mix_path = os.path.join(base_data_path, "__MixAtivoSistema.xlsx")
    mix_base = os.path.join(base_data_path, "__MixAtivoSistema")
    
    st.caption(f"📅 Última atualização: **{get_file_info(mix_path)}**")
    
    uploaded_mix = st.file_uploader("Selecione o __MixAtivoSistema.xlsx", type=["xlsx", "xls"], key="mix_uploader")
    process_automatic_upload(uploaded_mix, mix_path, mix_base, "mix")
                # Salva versão rápida
                target_base = os.path.join(base_data_path, "__MixAtivoSistema")
                if save_file_as_parquet(uploaded_mix, target_base):

                    st.success("Mix atualizado e otimizado com sucesso!")
