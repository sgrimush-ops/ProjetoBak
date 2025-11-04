import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# Defina o caminho do seu banco de dados
DB_PATH = 'data/database.db'
# Define o tempo limite de inatividade (em minutos)
INACTIVITY_LIMIT_MINUTES = 5

@st.cache_data(ttl=60) # Cache de 1 minuto para esta função
def get_user_status_df():
    """
    Busca usuários no DB, calcula o status (com cores) e ordena a lista.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        df_users = pd.read_sql_query("SELECT username, ultimo_acesso, status_logado FROM users", conn)
    except sqlite3.Error as e:
        st.error(f"Erro ao carregar usuários: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

    if df_users.empty:
        return pd.DataFrame()

    agora = pd.to_datetime(datetime.now())
    
    # 1. Converte datas, tratando erros (como '2025-m-04')
    df_users['ultimo_acesso_dt'] = pd.to_datetime(df_users['ultimo_acesso'], errors='coerce')
    
    # 2. Calcula o tempo em segundos
    # Preenche NaT (datas nulas ou inválidas) com um valor muito alto (ex: 10 anos em segundos)
    # para que eles caiam para o final da lista (status "Vermelho").
    tempo_total_segundos = (agora - df_users['ultimo_acesso_dt']).dt.total_seconds().fillna(315360000)
    df_users['Tempo_Segundos'] = tempo_total_segundos
    
    # 3. Define Limites de Tempo
    limite_ativo_seg = INACTIVITY_LIMIT_MINUTES * 60
    limite_recente_seg = 24 * 60 * 60 # 24 horas

    # 4. Define Cor e Chave de Ordenação
    # Chave 1: Ativo (Verde)
    # Chave 2: Inativo Recente (Preto)
    # Chave 3: Inativo Antigo (Vermelho)
    
    df_users['Sort_Key'] = 3 # Padrão: Vermelho/Antigo
    df_users['Cor'] = "red"
    df_users['Status_Desc'] = "Inativo (> 24h)"

    # Inativo Recente (Preto)
    recente_mask = (df_users['Tempo_Segundos'] < limite_recente_seg)
    df_users.loc[recente_mask, 'Sort_Key'] = 2
    df_users.loc[recente_mask, 'Cor'] = "black"
    df_users.loc[recente_mask, 'Status_Desc'] = "Inativo (< 24h)"

    # Ativo (Verde)
    ativo_mask = (df_users['status_logado'] == 'LOGADO') & (df_users['Tempo_Segundos'] < limite_ativo_seg)
    df_users.loc[ativo_mask, 'Sort_Key'] = 1
    df_users.loc[ativo_mask, 'Cor'] = "green"
    df_users.loc[ativo_mask, 'Status_Desc'] = f"Ativo (< {INACTIVITY_LIMIT_MINUTES}m)"

    # 5. Formata colunas para exibição
    df_users['ultimo_acesso_str'] = df_users['ultimo_acesso_dt'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna("Nenhuma Atividade")
    df_users['Tempo_Formatado'] = df_users['Tempo_Segundos'].apply(
        lambda x: f"{int(x // 60)}m {int(x % 60)}s" if x < 315360000 else "N/A"
    )

    # 6. Ordena o DataFrame (Chave de Ordenação primeiro, depois pelo tempo)
    df_users = df_users.sort_values(by=['Sort_Key', 'Tempo_Segundos'], ascending=[True, True])
    
    return df_users

def show_status_page():
    """Cria a interface da página de status."""
    st.title("📊 Status dos Usuários Ativos")
    st.markdown(f"Usuários considerados ativos se acessaram nos últimos **{INACTIVITY_LIMIT_MINUTES} minutos**.")

    if st.button("🔄 Atualizar Status"):
        get_user_status_df.clear() # Limpa o cache desta função
        st.rerun()

    df_status = get_user_status_df()
    
    st.markdown("---")
    
    # --- NOVO DISPLAY COM CORES ---
    
    # Cabeçalho da Tabela
    col1, col2, col3 = st.columns([1.5, 2, 1.5])
    col1.markdown("**Usuário**")
    col2.markdown("**Último Acesso**")
    col3.markdown("**Status**")

    st.markdown("<hr style='margin-top:0px; margin-bottom:10px;'>", unsafe_allow_html=True)

    if not df_status.empty:
        # Itera pelas linhas do DataFrame ordenado
        for index, row in df_status.iterrows():
            cor = row['Cor']
            
            # Define o texto de status (se estiver ativo, mostra 'Ativo', senão mostra o tempo)
            if row['Sort_Key'] == 1:
                status_texto = "Ativo"
            else:
                status_texto = row['Tempo_Formatado']

            col1_disp, col2_disp, col3_disp = st.columns([1.5, 2, 1.5])
            
            # Aplica a cor usando HTML/Markdown
            col1_disp.markdown(f"<span style='color: {cor};'>{row['username']}</span>", unsafe_allow_html=True)
            col2_disp.markdown(f"<span style='color: {cor};'>{row['ultimo_acesso_str']}</span>", unsafe_allow_html=True)
            col3_disp.markdown(f"<span style='color: {cor};'>**{status_texto}**</span>", unsafe_allow_html=True)
            
    else:
        st.info("Nenhum usuário encontrado no banco de dados.")