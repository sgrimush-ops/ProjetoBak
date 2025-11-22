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
             # Tenta ler Excel (xls, xlsx, xlsm)
             # O pandas detecta automaticamente o formato se as bibliotecas (xlrd, openpyxl) estiverem instaladas
             df = pd.read_excel(uploaded_file)
             
        # Salva como Parquet (Formato de alta performance)
        parquet_path = f"{target_path_no_ext}.parquet"
        df.to_parquet(parquet_path, index=False)
        
        # (Opcional) Salva também o original como backup se desejar, 
        # mas o sistema agora prioriza ler o .parquet
        # original_ext = os.path.splitext(uploaded_file.name)[1]
        # with open(f"{target_path_no_ext}{original_ext}", "wb") as f:
        #     uploaded_file.seek(0)
        #     f.write(uploaded_file.getbuffer())
            
        return True
    except Exception as e:
        st.error(f"Erro ao converter para Parquet: {e}")
        if "xlrd" in str(e):
             st.error("Dica: Para arquivos .xls antigos, certifique-se de que 'xlrd' está no requirements.txt")
        return False

def process_automatic_upload(uploaded_file, base_path_no_ext, file_key):
    """
    Gerencia o upload automático: Converte para Parquet e Atualiza a tela.
    """
    if uploaded_file:
        # Cria um ID único para este upload (nome + tamanho) para evitar reprocessamento contínuo
        file_id = f"{uploaded_file.name}_{uploaded_file.size}"
        
        # Se este arquivo exato ainda não foi processado nesta sessão
        if st.session_state.get(f"processed_{file_key}") != file_id:
            
            progress_container = st.empty()
            progress_bar = progress_container.progress(0, text="Iniciando upload...")
            
            try:
                # 1. Leitura e Conversão
                progress_bar.progress(30, text="Lendo arquivo e convertendo para Parquet...")
                
                # Salva diretamente como Parquet (otimizado)
                if save_file_as_parquet(uploaded_file, base_path_no_ext):
                    
                    progress_bar.progress(100, text="Concluído!")
                    
                    # Marca como processado para não entrar em loop
                    st.session_state[f"processed_{file_key}"] = file_id
                    
                    st.toast(f"Arquivo {file_key.upper()} atualizado e otimizado com sucesso!", icon="✅")
                    
                    # Força recarregamento para atualizar a data na tela imediatamente
                    st.rerun() 
                    
            except Exception as e:
                st.error(f"Erro no processamento: {e}")
            finally:
                progress_container.empty()

def show_admin_tools(engine, base_data_path):
    st.title("🔧 Ferramentas de Admin: Upload de Arquivos")
    st.info("Basta arrastar os arquivos. O sistema converterá automaticamente para o formato acelerado (.parquet).")

    # --- 1. WMS ---
    st.subheader("1. WMS (Estoque CD)")
    wms_base = os.path.join(base_data_path, "WMS") # Caminho base sem extensão
    wms_parquet = wms_base + ".parquet"
    
    if os.path.exists(wms_parquet):
        st.caption(f"📅 Última atualização: **{get_file_info(wms_parquet)}** (Formato Otimizado)")
    else:
        st.caption("⚠️ Arquivo otimizado não encontrado.")
    
    uploaded_wms = st.file_uploader("Selecione o WMS (xls, xlsx, xlsm)", type=["xlsm", "xlsx", "xls"], key="wms_uploader")
    process_automatic_upload(uploaded_wms, wms_base, "wms")

    st.markdown("---")

    # --- 2. Histórico ---
    st.subheader("2. Histórico de Solicitações")
    hist_base = os.path.join(base_data_path, "historico_solic")
    hist_parquet = hist_base + ".parquet"
    
    if os.path.exists(hist_parquet):
        st.caption(f"📅 Última atualização: **{get_file_info(hist_parquet)}** (Formato Otimizado)")
    else:
        st.caption("⚠️ Arquivo otimizado não encontrado.")
    
    uploaded_hist = st.file_uploader("Selecione o Histórico (xls, xlsx, xlsm)", type=["xlsm", "xlsx", "xls"], key="hist_uploader")
    process_automatic_upload(uploaded_hist, hist_base, "hist")

    st.markdown("---")

    # --- 3. Mix ---
    st.subheader("3. Mix Ativo")
    mix_base = os.path.join(base_data_path, "__MixAtivoSistema")
    mix_parquet = mix_base + ".parquet"
    
    if os.path.exists(mix_parquet):
        st.caption(f"📅 Última atualização: **{get_file_info(mix_parquet)}** (Formato Otimizado)")
    else:
        st.caption("⚠️ Arquivo otimizado não encontrado.")
    
    uploaded_mix = st.file_uploader("Selecione o Mix (xls, xlsx)", type=["xlsx", "xls"], key="mix_uploader")
    process_automatic_upload(uploaded_mix, mix_base, "mix")
