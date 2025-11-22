import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import json
import re
import os
from sqlalchemy import create_engine, text
import numpy as np

# =========================================================
#  🧩 CONSTANTES E MAPEAMENTOS
# =========================================================

LISTA_LOJAS = ["001", "002", "003", "004", "005", "006",
               "007", "008", "011", "012", "013", "014", "017", "018"]

COLS_MIX_MAP = {
    'CODIGOINT': 'Codigo', 'CODIGOEAN': 'EAN', 'DESCRICAO': 'Produto',
    'LOJA': 'Loja', 'EmbSeparacao': 'embseparacao'
}

COLS_HIST_MAP = {
    'CODIGOINT': 'Codigo', 'LOJA': 'Loja', 'DtSolicitacao': 'Data',
    'EstCX': 'Estoque_G', 'PedCX': 'Pedido_H', 'Vd1sem-CX': 'Venda_I',
    'Vd2sem-CX': 'Venda_J', 'VM30dCX': 'Venda_K',
}

COLS_WMS_MAP = {
    'codigo': 'Codigo', 'Qtd': 'Qtd_CD', 'datasalva': 'Data'
}

# =========================================================
#  📂 FUNÇÕES DE LEITURA DE DADOS (OTIMIZADAS)
# =========================================================

def load_data_optimized(parquet_path, excel_path, usecols_map=None, dtype=None):
    """Tenta ler Parquet (rápido), cai para Excel (lento) se necessário."""
    if os.path.exists(parquet_path):
        # Leitura ultra-rápida
        df = pd.read_parquet(parquet_path)
        if usecols_map:
            cols_to_keep = [c for c in usecols_map.keys() if c in df.columns]
            df = df[cols_to_keep]
    else:
        # Fallback para Excel
        if excel_path.endswith('.csv'):
             df = pd.read_csv(excel_path)
        else:
             sheet = 'WMS' if 'WMS' in excel_path else 0
             cols = list(usecols_map.keys()) if usecols_map else None
             df = pd.read_excel(excel_path, sheet_name=sheet, usecols=cols, dtype=dtype)
    return df

@st.cache_data
def load_mix_data(base_path_no_ext: str, mod_time: float):
    """Carrega dados do Mix (Prioriza Parquet)."""
    parquet_path = f"{base_path_no_ext}.parquet"
    excel_path = f"{base_path_no_ext}.xlsx"
    
    try:
        df = load_data_optimized(parquet_path, excel_path, dtype=str)
        cols_renomear = {k:v for k,v in COLS_MIX_MAP.items() if k in df.columns}
        df.rename(columns=cols_renomear, inplace=True)
        
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(0).astype(int)
        
        if 'embseparacao' in df.columns:
             df['embseparacao'] = pd.to_numeric(
                df['embseparacao'].astype(str).str.split(',').str[0].str.strip(),
                errors='coerce'
            ).fillna(0).astype(int)
            
        df['Loja'] = df['Loja'].astype(str).str.zfill(3)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar Mix: {e}")
        return pd.DataFrame()

@st.cache_data
def load_historico_data(base_path_no_ext: str, mod_time: float):
    """Carrega dados do Histórico (Prioriza Parquet)."""
    parquet_path = f"{base_path_no_ext}.parquet"
    excel_path = f"{base_path_no_ext}.xlsm" 
    
    try:
        df = load_data_optimized(parquet_path, excel_path, usecols_map=COLS_HIST_MAP)
        cols_renomear = {k:v for k,v in COLS_HIST_MAP.items() if k in df.columns}
        df.rename(columns=cols_renomear, inplace=True)

        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(0).astype(int)
        df['Loja'] = df['Loja'].astype(str).str.zfill(3)
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        
        metric_cols = ['Estoque_G', 'Pedido_H', 'Venda_I', 'Venda_J', 'Venda_K']
        for col in metric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        df.dropna(subset=['Data'], inplace=True)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar Histórico: {e}")
        return pd.DataFrame()

@st.cache_data
def load_wms_data(base_path_no_ext: str, mod_time: float):
    """Carrega dados do WMS (Prioriza Parquet)."""
    parquet_path = f"{base_path_no_ext}.parquet"
    excel_path = f"{base_path_no_ext}.xlsm"
    
    try:
        df = load_data_optimized(parquet_path, excel_path, usecols_map=COLS_WMS_MAP)
        cols_renomear = {k:v for k,v in COLS_WMS_MAP.items() if k in df.columns}
        df.rename(columns=cols_renomear, inplace=True)
        
        df['Codigo'] = pd.to_numeric(df['Codigo'], errors='coerce').fillna(0).astype(int)
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df['Qtd_CD'] = pd.to_numeric(df['Qtd_CD'], errors='coerce').fillna(0)
        df.dropna(subset=['Data'], inplace=True)

        if not df.empty:
            latest_date = df['Data'].max()
            df_latest = df[df['Data'] == latest_date]
            return df_latest
        return df
        
    except Exception as e:
        st.error(f"Erro ao carregar WMS: {e}")
        return pd.DataFrame(columns=['Codigo', 'Qtd_CD', 'Data'])

@st.cache_data(ttl=300)
def load_active_offers(_engine):
    """Busca ofertas do banco de dados que estão ativas hoje OU no futuro."""
    today = date.today()
    query = text("""
        SELECT codigo, oferta, data_inicio, data_final
        FROM ofertas
        WHERE data_final >= :today
    """)
    try:
        with _engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"today": today})
        
        if not df.empty:
            # Remove duplicatas mantendo a última inserção
            df = df.drop_duplicates(subset=['codigo'], keep='last').set_index('codigo')
        return df
    except Exception as e:
        # Em caso de erro (ex: tabela não existe ainda), retorna vazio sem quebrar
        return pd.DataFrame()

# =========================================================
#  💾 SALVAR PEDIDO
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
#  📊 HISTÓRICO DE PEDIDOS
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

    # Caminhos base (sem extensão)
    mix_base = os.path.join(base_data_path, "__MixAtivoSistema")
    hist_base = os.path.join(base_data_path, "historico_solic")
    wms_base = os.path.join(base_data_path, "WMS")
    
    # Verifica modificação para quebrar cache
    def get_mod_time(base_path, ext):
        if os.path.exists(f"{base_path}.parquet"):
            return os.path.getmtime(f"{base_path}.parquet")
        elif os.path.exists(f"{base_path}.{ext}"):
             return os.path.getmtime(f"{base_path}.{ext}")
        return 0.0

    try:
        mix_mod = get_mod_time(mix_base, "xlsx")
        hist_mod = get_mod_time(hist_base, "xlsm")
        wms_mod = get_mod_time(wms_base, "xlsm")
    except Exception:
        mix_mod, hist_mod, wms_mod = 0.0, 0.0, 0.0
    
    df_mix = load_mix_data(mix_base, mix_mod)
    df_hist = load_historico_data(hist_base, hist_mod)
    df_wms = load_wms_data(wms_base, wms_mod) 
    df_ofertas = load_active_offers(engine)

    if df_mix.empty:
        st.warning("Falha ao carregar o Mix de Produtos.")
        st.stop()

    lojas_user = st.session_state.get('lojas_acesso', [])
    if not lojas_user:
        st.warning("Sem acesso a lojas.")
        st.stop()

    st.subheader("1. Buscar Produto")
    df_mix_user = df_mix[df_mix['Loja'].isin(lojas_user)].copy()

    tab_cod, tab_prod, tab_ean = st.tabs(["Por Código", "Por Produto", "Por EAN"])
    prod_sel = None

    with tab_cod:
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

    with tab_prod:
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
                cod_str = re.search(r'\(Cód: (\d+)\)', sel).group(1)
                cod = int(cod_str)
                prod_sel = df_mix[df_mix['Codigo'] == cod].iloc[0]

    with tab_ean:
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
        
        cod = int(prod_sel['Codigo'])
        emb = int(prod_sel.get('embseparacao', 0))

        # Estoque CD
        stock_cd_units = df_wms[df_wms['Codigo'] == cod]['Qtd_CD'].sum()
        stock_display = "Esta em falta"
        
        if emb > 0 and stock_cd_units > 0:
            stock_cd_cases = int(stock_cd_units // emb)
            if stock_cd_cases > 0:
                stock_display = f"{stock_cd_cases:,.0f} CX"
        
        st.info(f"**Item:** {prod_sel['Produto']} (Cód: {cod}) | **Emb:** {emb} un/cx | **Estoque CD:** {stock_display}")
        
        # Ofertas
        try:
            today = date.today()
            if not df_ofertas.empty and cod in df_ofertas.index:
                oferta_data = df_ofertas.loc[cod] 
                if isinstance(oferta_data, pd.DataFrame):
                     oferta_data = oferta_data.iloc[-1]
                
                preco = f"R$ {oferta_data['oferta']:.2f}"
                inicio = oferta_data['data_inicio']
                fim = oferta_data['data_final']
                
                inicio_str = inicio.strftime('%d/%m')
                fim_str = fim.strftime('%d/%m/%Y')
                
                if today >= inicio:
                    st.success(f"🛍️ **OFERTA ATIVA:** Este item está em promoção por **{preco}** (Vigência: de {inicio_str} até {fim_str})")
                else:
                    st.warning(f"📣 **OFERTA FUTURA:** Este item entrará em promoção por **{preco}** (Vigência: de {inicio_str} até {fim_str})")
        except Exception as e:
            pass 

        # Dados Históricos
        if not df_hist.empty:
            latest_hist_date = df_hist['Data'].max()
            df_hist_item_raw = df_hist[
                (df_hist['Codigo'] == cod) & 
                (df_hist['Data'] == latest_hist_date)
            ]
            df_hist_item = df_hist_item_raw.drop_duplicates(subset=['Loja'], keep='first')
            hist_item_map = df_hist_item.set_index('Loja').to_dict('index')
            data_atualizacao = latest_hist_date.strftime('%d/%m/%Y')
        else:
            hist_item_map = {}
            data_atualizacao = "N/A"

        with st.form("form_qty"):
            qtys, total = {}, 0
            cols = st.columns(min(len(lojas_user), 3))
            
            for i, loja in enumerate(lojas_user):
                col_render = cols[i % len(cols)]
                
                sugestao_int = 0
                caption_text = f"Sem dados (Atu: {data_atualizacao})"
                
                if loja in hist_item_map:
                    row = hist_item_map[loja]
                    est_g = row['Estoque_G']
                    ped_h = row['Pedido_H']
                    vd_i = row['Venda_I']
                    vd_j = row['Venda_J']
                    vm_k = row['Venda_K']
                    
                    sugestao_float = (vm_k / 7 * 4) - est_g
                    sugestao_int = int(np.round(sugestao_float)) 
                    
                    if sugestao_int < 1:
                        sugestao_int = 0 
                    
                    caption_text = (
                        f"Est: {est_g:.1f} | Ult.Ped: {ped_h:.0f} | "
                        f"Vd1: {vd_i:.1f} | Vd2: {vd_j:.1f} | VM30: {vm_k:.1f} | "
                        f"(Atu: {data_atualizacao})"
                    )

                q = col_render.number_input(
                    f"Loja {loja}", 
                    min_value=0, 
                    step=1, 
                    value=sugestao_int,
                    key=f"q_{cod}_{loja}"
                )
                
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
