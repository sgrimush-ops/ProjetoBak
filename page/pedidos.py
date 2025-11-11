import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import re
import os
from sqlalchemy import create_engine, text
import numpy as np # MUDANÇA: Necessário para arredondamento

# =========================================================
#  🧩 CONSTANTES E MAPEAMENTOS
# =========================================================
MIX_FILE_PATH = 'data/__MixAtivoSistema.xlsx'
HIST_FILE_PATH = 'data/historico_solic.xlsm'
WMS_FILE_PATH = 'data/WMS.xlsm'

LISTA_LOJAS = ["001", "002", "003", "004", "005", "006",
               "007", "008", "011", "012", "013", "014", "017", "018"]

# Mapeamento do Mix (como antes)
COLS_MIX_MAP = {
    'CODIGOINT': 'Codigo', 'CODIGOEAN': 'EAN', 'DESCRICAO': 'Produto',
    'LOJA': 'Loja', 'EmbSeparacao': 'embseparacao'
}

# MUDANÇA: Mapeamento do Histórico (incluindo colunas G, H, I, J, K)
COLS_HIST_MAP = {
    'CODIGOINT': 'Codigo',
    'LOJA': 'Loja',
    'DtSolicitacao': 'Data',
    'EstCX': 'Estoque_G',      # Coluna G: Estoque atual
    'PedCX': 'Pedido_H',      # Coluna H: Ultimo pedido
    'Vd1sem-CX': 'Venda_I',   # Coluna I: Venda 1 sem
    'Vd2sem-CX': 'Venda_J',   # Coluna J: Venda 2 sem
    'VM30dCX': 'Venda_K',     # Coluna K: Venda Média 30d
}

# MUDANÇA: Mapeamento do WMS
COLS_WMS_MAP = {
    'codigo': 'Codigo',
    'Qtd': 'Qtd_CD',
    'datasalva': 'Data'
}

# =========================================================
#  📂 FUNÇÕES DE LEITURA DE DADOS (COM CACHE)
# =========================================================
@st.cache_data
def load_mix_data(file_path: str):
    """Carrega dados do Mix de produtos."""
    try:
        df = pd.read_excel(file_path, dtype=str)
        df.rename(columns=COLS_MIX_MAP, inplace=True)
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(
            0).astype(int)
        df['embseparacao'] = pd.to_numeric(
            df['embseparacao'].astype(str).str.split(
                ',').str[0].str.split('.').str[0].str.strip(),
            errors='coerce'
        ).fillna(0).astype(int)
        df['Loja'] = df['Loja'].astype(str).str.zfill(3)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar Mix: {e}")
        return pd.DataFrame()

@st.cache_data
def load_historico_data(file_path: str):
    """MUDANÇA: Carrega dados do Histórico, incluindo colunas G a K."""
    try:
        # Define as colunas que queremos ler
        use_cols = list(COLS_HIST_MAP.keys())
        df = pd.read_excel(file_path, sheet_name=0, usecols=use_cols)
        
        # Renomeia
        df.rename(columns=COLS_HIST_MAP, inplace=True)

        # Limpa e converte tipos
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(0).astype(int)
        df['Loja'] = df['Loja'].astype(str).str.zfill(3)
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        
        # Converte todas as colunas de métrica (G-K) para número, tratando erros
        metric_cols = ['Estoque_G', 'Pedido_H', 'Venda_I', 'Venda_J', 'Venda_K']
        for col in metric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        df.dropna(subset=['Data'], inplace=True)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar Histórico: {e}")
        return pd.DataFrame()

@st.cache_data
def load_wms_data(file_path: str):
    """MUDANÇA: Carrega dados do WMS e filtra pelo último dia de upload."""
    try:
        df = pd.read_excel(file_path, sheet_name='WMS', usecols=COLS_WMS_MAP.keys())
        df.rename(columns=COLS_WMS_MAP, inplace=True)
        
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(0).astype(int)
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df['Qtd_CD'] = pd.to_numeric(df['Qtd_CD'], errors='coerce').fillna(0)
        df.dropna(subset=['Data'], inplace=True)

        # MUDANÇA: Filtra apenas pela data mais recente do arquivo
        latest_date = df['Data'].max()
        df_latest = df[df['Data'] == latest_date]
        return df_latest
        
    except Exception as e:
        st.error(f"Erro ao carregar WMS: {e}")
        return pd.DataFrame(columns=['Codigo', 'Qtd_CD', 'Data'])

# =========================================================
#  💾 SALVAR PEDIDO NO BANCO (Sem alterações)
# =========================================================
def save_order_to_db(engine, pedido_final: list[dict]):
    try:
        data_pedido = datetime.now()
        usuario = st.session_state.get('username', 'desconhecido')
        cols_lojas = ", ".join([f"loja_{l}" for l in LISTA_LOJAS])
        params_lojas = ", ".join([f":loja_{l}" for l in LISTA_LOJAS])

        query = text(f"""
            INSERT INTO pedidos_consolidados (
                codigo, produto, ean, embseparacao,
                data_pedido, data_aprovacao, usuario_pedido,
                status_item, {cols_lojas}, total_cx, status_aprovacao
            ) VALUES (
                :codigo, :produto, :ean, :embseparacao,
                :data_pedido, :data_aprovacao, :usuario_pedido,
                :status_item, {params_lojas}, :total_cx, :status_aprovacao
            )
        """)

        params_list = []
        for item in pedido_final:
            vals_lojas = {f"loja_{l}": item.get(
                f"loja_{l}", 0) for l in LISTA_LOJAS}
            emb_val = int(pd.to_numeric(
                item.get("embseparacao", 0), errors="coerce") or 0)
            
            params_list.append({
                "codigo": item["Codigo"], "produto": item["Produto"], "ean": item["EAN"],
                "embseparacao": emb_val, "data_pedido": data_pedido, "data_aprovacao": None,
                "usuario_pedido": usuario, "status_item": item["Status"],
                **vals_lojas, "total_cx": item["Total_CX"], "status_aprovacao": "Pendente"
            })

        with engine.begin() as conn:
            conn.execute(query, params_list)
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

# =========================================================
#  📊 HISTÓRICO DE PEDIDOS (Sem alterações)
# =========================================================
def get_recent_orders_display(engine, username: str) -> pd.DataFrame:
    try:
        dt_lim = (datetime.now() - timedelta(days=3)
                  ).strftime('%Y-%m-%d 00:00:00')
        q = text("""
            SELECT codigo AS "Cód", produto AS "Produto",
                   embseparacao AS "Emb", total_cx AS "Total",
                   status_aprovacao AS "Status",
                   data_pedido AS "Data"
            FROM pedidos_consolidados
            WHERE usuario_pedido = :username
              AND data_pedido >= :dt_lim
            ORDER BY data_pedido DESC
        """)
        df = pd.read_sql_query(q, con=engine, params={
                               "username": username, "dt_lim": dt_lim})
        df["Emb"] = pd.to_numeric(
            df["Emb"], errors='coerce').fillna(0).astype(int)
        return df
    except Exception as e:
        st.error(f"Erro ao ler histórico: {e}")
        return pd.DataFrame()

# =========================================================
#  🧭 INTERFACE PRINCIPAL
# =========================================================
def show_pedidos_page(engine, base_data_path):
    st.title("🛒 Digitação de Pedidos")

    if 'pedido_atual' not in st.session_state:
        st.session_state.pedido_atual = []

    # MUDANÇA: Definindo os caminhos
    mix_file_path = os.path.join(base_data_path, "__MixAtivoSistema.xlsx")
    hist_file_path = os.path.join(base_data_path, "historico_solic.xlsm")
    wms_file_path = os.path.join(base_data_path, "WMS.xlsm")
    
    # Carrega todos os dados (funções cacheadas)
    df_mix = load_mix_data(mix_file_path)
    df_hist = load_historico_data(hist_file_path)
    df_wms = load_wms_data(wms_file_path) # df_wms já vem filtrado pelo último dia

    if df_mix.empty:
        st.warning("Falha ao carregar o Mix de Produtos.")
        st.stop()

    lojas_user = st.session_state.get('lojas_acesso', [])
    if not lojas_user:
        st.warning("Sem acesso a lojas.")
        st.stop()

    st.subheader("1. Buscar Produto")
    df_mix_user = df_mix[df_mix['Loja'].isin(lojas_user)].copy()

    tab1, tab2, tab3 = st.tabs(["Por Produto", "Por Código", "Por EAN"])
    prod_sel = None

    with tab1:
        busca_nome = st.text_input("Nome do Produto:")
        if busca_nome:
            res = df_mix_user[df_mix_user['Produto'].str.contains(
                busca_nome, case=False, na=False)]
            unicos = res.drop_duplicates(subset=['Codigo'])
            unicos['Show'] = unicos['Produto'] + \
                " (Cód: " + unicos['Codigo'].astype(str) + ")"
            sel = st.selectbox(
                "Selecione:", ["Selecione..."] + unicos['Show'].tolist())
            if sel != "Selecione...":
                cod = int(re.search(r'\(Cód: (\d+)\)', sel).group(1))
                prod_sel = df_mix[df_mix['Codigo'] == cod].iloc[0]

    with tab2:
        busca_cod = st.text_input("Código:")
        if busca_cod:
            try:
                cod = int(busca_cod.strip())
                res = df_mix[df_mix['Codigo'] == cod]
                if not res.empty:
                    prod_sel = res.iloc[0]
                else:
                    st.warning("Código não encontrado.")
            except ValueError:
                st.warning("Código deve ser numérico.")

    with tab3:
        busca_ean = st.text_input("EAN:")
        if busca_ean:
            res = df_mix[df_mix['EAN'] == busca_ean.strip()]
            if not res.empty:
                prod_sel = res.iloc[0]
            else:
                st.warning("EAN não encontrado.")

    st.markdown("---")

    if prod_sel is not None:
        st.subheader("2. Distribuir Quantidades (Caixas)")
        
        # Converte tipos para a lógica
        cod = int(prod_sel['Codigo'])
        emb = int(prod_sel.get('embseparacao', 0))

        # --- MUDANÇA: 1. LÓGICA DO ESTOQUE CD (BARRA AZUL) ---
        stock_cd_units = df_wms[df_wms['Codigo'] == cod]['Qtd_CD'].sum()
        stock_display = "Esta em falta"
        
        if emb > 0 and stock_cd_units > 0:
            stock_cd_cases = int(stock_cd_units // emb)
            if stock_cd_cases > 0:
                stock_display = f"{stock_cd_cases:,.0f} CX"
        
        st.info(f"**Item:** {prod_sel['Produto']} (Cód: {cod}) | **Emb:** {emb} un/cx | **Estoque CD:** {stock_display}")
        
        # --- MUDANÇA: 2. PREPARAR DADOS HISTÓRICOS PARA O ITEM ---
        # Filtra o histórico (do último dia) para este item
        if not df_hist.empty:
            latest_hist_date = df_hist['Data'].max()
            df_hist_item = df_hist[
                (df_hist['Codigo'] == cod) & 
                (df_hist['Data'] == latest_hist_date)
            ]
            # Cria um "mapa" Loja -> Dados para busca rápida
            hist_item_map = df_hist_item.set_index('Loja').to_dict('index')
            data_atualizacao = latest_hist_date.strftime('%d/%m/%Y')
        else:
            hist_item_map = {}
            data_atualizacao = "N/A"

        with st.form("form_qty"):
            qtys, total = {}, 0
            cols = st.columns(min(len(lojas_user), 3))
            
            # --- MUDANÇA: 3. LÓGICA DE SUGESTÃO E CAPTION ---
            for i, loja in enumerate(lojas_user):
                col_render = cols[i % len(cols)]
                
                # Valores padrão
                sugestao_int = 0
                caption_text = f"Sem dados históricos (Atu: {data_atualizacao})"
                
                # Busca dados históricos da loja
                if loja in hist_item_map:
                    row = hist_item_map[loja]
                    est_g = row['Estoque_G']
                    ped_h = row['Pedido_H']
                    vd_i = row['Venda_I']
                    vd_j = row['Venda_J']
                    vm_k = row['Venda_K']
                    
                    # Calcular sugestão: (Venda Média / 7 dias * 4 dias de previsão) - Estoque Atual
                    sugestao_float = (vm_k / 7 * 4) - est_g
                    
                    # Arredonda para o inteiro mais próximo
                    sugestao_int = int(np.round(sugestao_float)) 
                    
                    if sugestao_int < 1:
                        sugestao_int = 0 # Não sugere menos que 1 caixa
                    
                    # Formata o caption
                    caption_text = (
                        f"Est: {est_g:.1f} | Ult.Ped: {ped_h:.0f} | "
                        f"Vd1: {vd_i:.1f} | Vd2: {vd_j:.1f} | VM30: {vm_k:.1f} | "
                        f"(Atu: {data_atualizacao})"
                    )

                # Renderiza o campo de número com a sugestão pré-preenchida
                q = col_render.number_input(
                    f"Loja {loja}", 
                    min_value=0, 
                    step=1, 
                    value=sugestao_int,  # <-- SUGESTÃO AQUI
                    key=f"q_{cod}_{loja}"
                )
                
                # Renderiza o caption
                col_render.caption(caption_text)
                
                if q > 0:
                    qtys[f"loja_{loja}"] = q
                    total += q

            if st.form_submit_button("Adicionar ao Pedido"):
                if total > 0:
                    st.session_state.pedido_atual.append({
                        "Codigo": str(cod), "Produto": prod_sel["Produto"],
                        "EAN": prod_sel["EAN"], "embseparacao": emb,
                        "Status": "Ativo", "Total_CX": total, **qtys
                    })
                    st.success("Item adicionado!")
                else:
                    st.warning("Digite ao menos uma quantidade.")

    st.markdown("---")
    st.subheader("3. Pedido Atual")
    if st.session_state.pedido_atual:
        df_ped = pd.DataFrame(st.session_state.pedido_atual)
        st.dataframe(df_ped, hide_index=True, use_container_width=True)
        c1, c2 = st.columns(2)
        if c1.button("Salvar Pedido", type="primary"):
            if save_order_to_db(engine, st.session_state.pedido_atual):
                st.success("Salvo com sucesso!")
                st.session_state.pedido_atual = []
                st.rerun()
            else:
                st.error("Erro ao salvar.")
        if c2.button("Limpar"):
            st.session_state.pedido_atual = []
            st.rerun()
    else:
        st.info("Carrinho vazio.")

    st.markdown("---")
    st.subheader("4. Histórico Recente")
    df_rec = get_recent_orders_display(engine, st.session_state.get('username', ''))
    if not df_rec.empty:
        st.dataframe(df_rec, hide_index=True, use_container_width=True)
    else:
        st.info("Sem pedidos recentes.")
