import streamlit as st
import pandas as pd
import os
from sqlalchemy import text
from datetime import datetime, timedelta, date
import io
from utils.timezone import now_brazil, today_brazil
from utils.cargos import is_user_consumo_cd

# --- Configurações ---
LISTA_LOJAS = [
    "001", "002", "003", "004", "005", "006",
    "007", "008", "011", "012", "013", "014", "016", "017", "018",
    "F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09",
    "F10", "F11", "M12", "M13", "ADM", "RH"
]
COLUNAS_LOJAS_PEDIDO = [f"loja_{str(loja).lower()}" for loja in LISTA_LOJAS]
ORIGEM_CONSUMO = "Pedido de Consumo"
ORIGEM_CD_GERAL = "Pedido por Código (CD)"
ORIGEM_CD15 = "CD15"
ORIGEM_CD16 = "CD16"
FILTRO_LOJA_TODAS = "Todas"
FILTRO_LOJA_FREE_SHOPS = "Somente Free Shops (F*)"
FILTRO_LOJA_RH = "Somente RH"
FILTRO_LOJA_ADM = "Somente ADM"
OPCOES_ORIGEM_FILTRO = [
    "Todas",
    ORIGEM_CONSUMO,
    ORIGEM_CD15,
    ORIGEM_CD16,
    ORIGEM_CD_GERAL,
]
OPCOES_FILTRO_LOJA = [
    FILTRO_LOJA_TODAS,
    FILTRO_LOJA_FREE_SHOPS,
    FILTRO_LOJA_RH,
    FILTRO_LOJA_ADM,
]


# --- Funções Auxiliares ---


def formatar_tipos_df(df: pd.DataFrame) -> pd.DataFrame:
    """Formata tipos de dados e corrige valores numéricos."""
    int_cols = COLUNAS_LOJAS_PEDIDO + [
        "total_cx",
        "embseparacao",
        "embalagem",
        "codigo_interno",
    ]
    
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    
    return df


def load_products_from_parquet():
    """Carrega produtos do arquivo parquet."""
    parquet_path = os.path.join("bdados", "query.parquet")
    
    if not os.path.exists(parquet_path):
        return pd.DataFrame()
    
    try:
        df = pd.read_parquet(parquet_path)
        df = df.drop_duplicates(subset=['CODIGO_PRODUTO'])
        
        # Mapear colunas novas
        column_mapping = {
            'CODIGO_PRODUTO': 'cod_consinco',
            'DESCRICAO_PRODUTO': 'descricao',
            'EMBL_TRANSFERENCIA': 'Emb'
        }
        
        df.rename(columns=column_mapping, inplace=True)
        
        # Garantir colunas minimas
        if 'cod_consinco' not in df.columns:
            raise ValueError("Coluna 'cod_consinco' (CODIGO_PRODUTO) não encontrada")
        if 'descricao' not in df.columns:
            df['descricao'] = 'SEM DESCRIÇÃO'
            
        if 'Emb' not in df.columns:
            if 'EMBL_COMPRA' in df.columns:
                df['Emb'] = df['EMBL_COMPRA']
            else:
                df['Emb'] = 1
                
        df['Emb'] = df['Emb'].fillna(1).replace(0, 1)
        
        df['Mix'] = 'A'
        df['cod_consinco'] = df['cod_consinco'].astype(int)
        
        return df
    except Exception:
        return pd.DataFrame()


def get_product_info(df_produtos, codigo):
    """Retorna informações do produto do parquet."""
    if df_produtos.empty:
        return None
    
    try:
        cod = int(codigo)
        result = df_produtos[df_produtos['cod_consinco'] == cod]
        if not result.empty:
            return result.iloc[0].to_dict()
    except:
        pass
    return None


def get_origens_cd():
    """Retorna origens que pertencem ao fluxo de pedidos CD."""
    return [ORIGEM_CD_GERAL, ORIGEM_CD15, ORIGEM_CD16]


def formatar_origem_pedido(origem):
    """Normaliza origem para exibição e filtros locais."""
    origem_normalizada = str(origem).strip()

    if not origem_normalizada or origem_normalizada.lower() == "none":
        origem_normalizada = ORIGEM_CD_GERAL

    if origem_normalizada == ORIGEM_CONSUMO:
        return f"🛒 {ORIGEM_CONSUMO}"

    return f"📦 {origem_normalizada}"


def adicionar_ids_selecionados(ids_atuais, novos_ids):
    """Acumula ids selecionados sem duplicar."""
    return list(dict.fromkeys([*ids_atuais, *novos_ids]))


def aplicar_filtro_origem_sql(query_base, origem_filtro):
    """Aplica filtro de origem na query SQL."""
    if origem_filtro == ORIGEM_CONSUMO:
        return (
            query_base
            + f" AND COALESCE(p.origem_pedido, '{ORIGEM_CD_GERAL}') "
            + f"= '{ORIGEM_CONSUMO}'"
        )

    if origem_filtro == ORIGEM_CD15:
        return (
            query_base
            + f" AND COALESCE(p.origem_pedido, '{ORIGEM_CD_GERAL}') "
            + f"= '{ORIGEM_CD15}'"
        )

    if origem_filtro == ORIGEM_CD16:
        return (
            query_base
            + f" AND COALESCE(p.origem_pedido, '{ORIGEM_CD_GERAL}') "
            + f"= '{ORIGEM_CD16}'"
        )

    if origem_filtro == ORIGEM_CD_GERAL:
        origens_cd_sql = ", ".join(
            [f"'{origem}'" for origem in get_origens_cd()]
        )
        return (
            query_base
            + f" AND COALESCE(p.origem_pedido, '{ORIGEM_CD_GERAL}') "
            + f"IN ({origens_cd_sql})"
        )

    return query_base


def filtrar_pedidos_por_tipo_loja(df: pd.DataFrame, filtro_loja: str) -> pd.DataFrame:
    """Filtra os pedidos com base nas lojas preenchidas em cada item."""
    if df.empty or filtro_loja == FILTRO_LOJA_TODAS:
        return df

    colunas_free_shops = [
        f"loja_{loja.lower()}" for loja in LISTA_LOJAS if str(loja).upper().startswith("F")
    ]

    if filtro_loja == FILTRO_LOJA_FREE_SHOPS:
        colunas_alvo = [col for col in colunas_free_shops if col in df.columns]
    elif filtro_loja == FILTRO_LOJA_RH:
        colunas_alvo = ["loja_rh"] if "loja_rh" in df.columns else []
    elif filtro_loja == FILTRO_LOJA_ADM:
        colunas_alvo = ["loja_adm"] if "loja_adm" in df.columns else []
    else:
        return df

    if not colunas_alvo:
        return pd.DataFrame(columns=df.columns)

    mascara = (df[colunas_alvo].fillna(0).sum(axis=1) > 0)
    return df.loc[mascara].copy()


def get_colunas_exclusivas_consumo():
    """Retorna colunas de lojas exclusivas para pedidos de consumo."""
    colunas_f = [
        col for col in COLUNAS_LOJAS_PEDIDO
        if col.replace("loja_", "").upper().startswith("F")
    ]
    colunas_fixas = [
        col for col in ["loja_adm", "loja_rh"] if col in COLUNAS_LOJAS_PEDIDO
    ]
    return [*colunas_f, *colunas_fixas]


def get_pedidos_para_aprovacao(
    engine,
    date_start,
    date_end,
    only_pending: bool,
    origem_filtro: str = "Todas",
) -> pd.DataFrame:
    """Busca pedidos para aprovação."""
    try:
        start_str = datetime.combine(date_start, datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
        end_str = datetime.combine(date_end, datetime.max.time()).strftime("%Y-%m-%d %H:%M:%S")
        lojas_sql = ", ".join(COLUNAS_LOJAS_PEDIDO)
        
        query = text(
            f"""
            SELECT
                p.id AS id_pedido,
                TO_CHAR(p.data_pedido, 'DD/MM/YYYY HH24:MI') AS data_pedido_str,
                p.usuario_pedido,
                COALESCE(u.empresa, 'Baklizi') AS empresa,
                COALESCE(u.lojas_acesso, '') AS loja_usuario,
                p.codigo_interno,
                p.descricao,
                p.embseparacao,
                p.{lojas_sql},
                p.total_cx,
                COALESCE(
                    p.origem_pedido,
                    'Pedido por Código (CD)'
                ) AS origem_pedido,
                p.status_item,
                p.status_aprovacao
            FROM pedidos_consolidados p
            LEFT JOIN users u ON LOWER(u.username) = LOWER(p.usuario_pedido)
            WHERE p.data_pedido BETWEEN :start_str AND :end_str
        """
        )
        
        params = {"start_str": start_str, "end_str": end_str}
        
        if only_pending:
            query = text(str(query) + " AND status_aprovacao = 'Pendente'")

        if origem_filtro == ORIGEM_CONSUMO:
            query = text(
                str(query)
                + f" AND COALESCE(origem_pedido, '{ORIGEM_CD_GERAL}') "
                + f"= '{ORIGEM_CONSUMO}'"
            )
        elif origem_filtro == ORIGEM_CD15:
            query = text(
                str(query)
                + f" AND COALESCE(origem_pedido, '{ORIGEM_CD_GERAL}') "
                + f"= '{ORIGEM_CD15}'"
            )
        elif origem_filtro == ORIGEM_CD16:
            query = text(
                str(query)
                + f" AND COALESCE(origem_pedido, '{ORIGEM_CD_GERAL}') "
                + f"= '{ORIGEM_CD16}'"
            )
        elif origem_filtro == ORIGEM_CD_GERAL:
            origens_cd_sql = ", ".join([f"'{origem}'" for origem in get_origens_cd()])
            query = text(
                str(query)
                + f" AND COALESCE(origem_pedido, '{ORIGEM_CD_GERAL}') "
                + f"IN ({origens_cd_sql})"
            )
        
        query = text(str(query) + " ORDER BY data_pedido ASC")
        
        df_pedidos = pd.read_sql_query(query, con=engine, params=params)
        df_pedidos = formatar_tipos_df(df_pedidos)
        
        return df_pedidos
    
    except Exception as e:
        st.error(f"Erro ao buscar pedidos: {e}")
        return pd.DataFrame()


def update_pedidos_aprovados(engine, df_editado_selecionado):
    """Atualiza o banco com quantidades editadas e aprova os itens."""
    try:
        df_para_salvar = df_editado_selecionado.copy()
        colunas_exclusivas_consumo = [
            col for col in get_colunas_exclusivas_consumo()
            if col in df_para_salvar.columns
        ]

        if colunas_exclusivas_consumo:
            if "origem_pedido" in df_para_salvar.columns:
                mascara_nao_consumo = (
                    df_para_salvar["origem_pedido"].fillna(ORIGEM_CD_GERAL)
                    .astype(str)
                    .str.strip()
                    != ORIGEM_CONSUMO
                )
            else:
                origem_series = pd.Series(
                    [ORIGEM_CD_GERAL] * len(df_para_salvar),
                    index=df_para_salvar.index,
                )
                mascara_nao_consumo = (
                    origem_series
                    .astype(str)
                    .str.strip()
                    != formatar_origem_pedido(ORIGEM_CONSUMO)
                )

            if mascara_nao_consumo.any():
                df_para_salvar.loc[
                    mascara_nao_consumo,
                    colunas_exclusivas_consumo,
                ] = 0

            colunas_soma = [
                col for col in COLUNAS_LOJAS_PEDIDO if col in df_para_salvar.columns
            ]
            if colunas_soma:
                df_para_salvar["total_cx"] = (
                    df_para_salvar[colunas_soma].sum(axis=1).astype(int)
                )

        data_aprovacao_dt = now_brazil()
        set_lojas_sql = ", ".join([f"{col} = :{col}" for col in COLUNAS_LOJAS_PEDIDO])
        
        query = text(
            f"""
            UPDATE pedidos_consolidados
            SET
                {set_lojas_sql},
                total_cx = :total_cx,
                data_aprovacao = :data_aprovacao,
                status_aprovacao = 'Aprovado'
            WHERE id = :id_pedido
        """
        )
        
        with engine.begin() as conn:
            for _, row in df_para_salvar.iterrows():
                params = {"id_pedido": row["id_pedido"], "data_aprovacao": data_aprovacao_dt}
                params["total_cx"] = row["total_cx"]
                
                for col in COLUNAS_LOJAS_PEDIDO:
                    params[col] = row[col]
                
                conn.execute(query, params)
        
        return True
    except Exception as e:
        st.error(f"Erro ao aprovar pedidos: {e}")
        return False


def reprovar_pedidos(engine, ids_list):
    """Reprova pedidos (altera status para 'Reprovado')."""
    try:
        query = text(
            """
            UPDATE pedidos_consolidados
            SET status_aprovacao = 'Reprovado', data_aprovacao = :data_aprovacao
            WHERE id = ANY(:ids)
        """
        )
        
        with engine.begin() as conn:
            conn.execute(query, {"ids": ids_list, "data_aprovacao": now_brazil()})
        
        return True
    except Exception as e:
        st.error(f"Erro ao reprovar pedidos: {e}")
        return False


def ensure_download_aprovados_table(engine):
    """Garante tabela de log de downloads de aprovados."""
    query = text(
        """
        CREATE TABLE IF NOT EXISTS downloads_aprovados_log (
            id SERIAL PRIMARY KEY,
            data_download TIMESTAMP NOT NULL,
            usuario TEXT
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(query)


def registrar_download_aprovados(engine, usuario):
    """Registra que o Excel de aprovados foi baixado."""
    try:
        ensure_download_aprovados_table(engine)
        query = text(
            """
            INSERT INTO downloads_aprovados_log (data_download, usuario)
            VALUES (:data_download, :usuario)
            """
        )
        with engine.begin() as conn:
            conn.execute(
                query,
                {
                    "data_download": now_brazil(),
                    "usuario": usuario,
                },
            )
        return True
    except Exception:
        return False


def get_intervalo_hoje():
    """Retorna início e fim do dia atual no horário do Brasil."""
    agora = now_brazil()
    inicio_dia = datetime.combine(agora.date(), datetime.min.time())
    fim_dia = datetime.combine(agora.date(), datetime.max.time())
    return inicio_dia, fim_dia


def get_alerta_download_pendente(engine):
    """Retorna status de aprovados do dia sem download registrado."""
    inicio_dia, fim_dia = get_intervalo_hoje()

    query_aprov = text(
        """
        SELECT
            COUNT(*) AS total_itens,
            MAX(data_aprovacao) AS ultima_aprovacao
        FROM pedidos_consolidados
        WHERE status_aprovacao = 'Aprovado'
          AND data_aprovacao BETWEEN :inicio_dia AND :fim_dia
        """
    )

    try:
        with engine.connect() as conn:
            row_aprov = conn.execute(
                query_aprov,
                {"inicio_dia": inicio_dia, "fim_dia": fim_dia},
            ).fetchone()

        total_itens = int((row_aprov.total_itens or 0) if row_aprov else 0)
        ultima_aprovacao = row_aprov.ultima_aprovacao if row_aprov else None

        if total_itens == 0:
            return {
                "pendente": False,
                "total_itens": 0,
                "ultima_aprovacao": None,
            }

        ensure_download_aprovados_table(engine)
        query_download = text(
            "SELECT MAX(data_download) AS ultimo_download "
            "FROM downloads_aprovados_log "
            "WHERE data_download BETWEEN :inicio_dia AND :fim_dia"
        )
        with engine.connect() as conn:
            row_down = conn.execute(
                query_download,
                {"inicio_dia": inicio_dia, "fim_dia": fim_dia},
            ).fetchone()

        ultimo_download = row_down.ultimo_download if row_down else None
        pendente = (
            ultimo_download is None
            or (
                ultima_aprovacao is not None
                and ultimo_download < ultima_aprovacao
            )
        )

        return {
            "pendente": pendente,
            "total_itens": total_itens,
            "ultima_aprovacao": ultima_aprovacao,
        }
    except Exception:
        return {
            "pendente": False,
            "total_itens": 0,
            "ultima_aprovacao": None,
        }


def gerar_payload_excel_aprovados_dia(engine, origem_filtro="Todas"):
    """Monta o Excel com os pedidos aprovados no dia."""
    inicio_dia, fim_dia = get_intervalo_hoje()

    query_sql = f"""
        SELECT
            p.id,
            TO_CHAR(p.data_pedido, 'DD/MM/YYYY') AS data_pedido,
            TO_CHAR(p.data_pedido, 'HH24:MI') AS hora_digitacao,
            TO_CHAR(p.data_aprovacao, 'DD/MM/YYYY') AS data_aprovacao,
            p.usuario_pedido,
            COALESCE(u.empresa, 'Baklizi') AS empresa,
            p.codigo_interno,
            p.descricao,
            p.embseparacao,
            p.{", p.".join(COLUNAS_LOJAS_PEDIDO)},
            p.total_cx
        FROM pedidos_consolidados p
        LEFT JOIN users u ON LOWER(u.username) = LOWER(p.usuario_pedido)
        WHERE p.status_aprovacao = 'Aprovado'
          AND p.data_aprovacao BETWEEN :inicio_dia AND :fim_dia
        """
    query_sql = aplicar_filtro_origem_sql(query_sql, origem_filtro)
    query_sql += " ORDER BY p.data_aprovacao DESC"
    query = text(query_sql)

    df_aprovados = pd.read_sql_query(
        query,
        con=engine,
        params={"inicio_dia": inicio_dia, "fim_dia": fim_dia},
    )

    if df_aprovados.empty:
        return None, "Nenhum pedido aprovado hoje para o filtro selecionado."

    lojas_presentes = [
        col for col in COLUNAS_LOJAS_PEDIDO if col in df_aprovados.columns
    ]

    for col in lojas_presentes:
        df_aprovados[col] = pd.to_numeric(
            df_aprovados[col], errors="coerce"
        ).fillna(0)

    df_lojas = df_aprovados.melt(
        id_vars=[
            "id",
            "data_pedido",
            "hora_digitacao",
            "usuario_pedido",
            "empresa",
            "codigo_interno",
            "descricao",
            "embseparacao",
        ],
        value_vars=lojas_presentes,
        var_name="loja_col",
        value_name="qtd_cx_loja",
    )

    df_lojas = df_lojas[df_lojas["qtd_cx_loja"] > 0].copy()

    if df_lojas.empty:
        return (
            None,
            "Nenhum item com quantidade por loja nos pedidos aprovados hoje para o filtro selecionado.",
        )

    df_lojas["loja"] = df_lojas["loja_col"].astype(str).str.replace(
        "loja_", "", regex=False
    )

    def formatar_usuarios(series_usuarios):
        contagem = (
            series_usuarios.astype(str)
            .str.strip()
            .replace("", "unknown")
            .value_counts()
        )
        return ", ".join(
            [
                f"{usuario} ({qtd})" if qtd > 1 else usuario
                for usuario, qtd in contagem.items()
            ]
        )

    def formatar_empresas(series_empresas):
        empresas = sorted(
            {
                str(empresa).strip()
                for empresa in series_empresas
                if str(empresa).strip()
            }
        )
        return ", ".join(empresas) if empresas else "Baklizi"

    df_export = (
        df_lojas.groupby(
            [
                "data_pedido",
                "hora_digitacao",
                "loja",
                "codigo_interno",
                "descricao",
                "embseparacao",
            ],
            as_index=False,
        )
        .agg(
            total_cx=("qtd_cx_loja", "sum"),
            usuarios_pedido=("usuario_pedido", formatar_usuarios),
            empresas=("empresa", formatar_empresas),
            qtd_lancamentos=("id", "count"),
        )
        .sort_values(
            by=["data_pedido", "hora_digitacao", "loja", "descricao"],
            ascending=[False, False, True, True],
        )
    )

    df_export = df_export.rename(
        columns={
            "data_pedido": "Data Pedido",
            "hora_digitacao": "Hora Digitação",
            "loja": "Loja",
            "codigo_interno": "Código Consinco",
            "descricao": "Descrição",
            "embseparacao": "Emb",
            "total_cx": "Total CX",
            "usuarios_pedido": "Usuários",
            "empresas": "Empresa",
            "qtd_lancamentos": "Qtd Lançamentos",
        }
    )

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_export.to_excel(
            writer,
            sheet_name="Pedidos Aprovados",
            index=False,
        )

    output.seek(0)

    sufixo_arquivo = "todos"
    if origem_filtro == ORIGEM_CONSUMO:
        sufixo_arquivo = "consumo"
    elif origem_filtro == ORIGEM_CD15:
        sufixo_arquivo = "cd15"
    elif origem_filtro == ORIGEM_CD16:
        sufixo_arquivo = "cd16"
    elif origem_filtro == ORIGEM_CD_GERAL:
        sufixo_arquivo = "cd"

    payload = {
        "bytes": output.getvalue(),
        "nome": (
            f"pedidos_aprovados_{sufixo_arquivo}_"
            f"{inicio_dia.strftime('%Y%m%d')}.xlsx"
        ),
        "total_itens": int(df_export.shape[0]),
    }
    return payload, None


# --- Página Principal ---


def show_aprovacao_page(engine, base_data_path):
    """Página de aprovação de pedidos."""
    
    st.title("✅ Aprovação de Pedidos")
    st.markdown("Aprovar ou reprovar pedidos enviados pelos usuários")
    
    # Carregar produtos
    df_produtos = load_products_from_parquet()
    
    # --- Filtros ---
    st.markdown("### 🔍 Filtros")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        date_start = st.date_input(
            "Data Início:",
            value=today_brazil() - timedelta(days=7),
            key="date_start_aprov"
        )
    
    with col2:
        date_end = st.date_input(
            "Data Fim:",
            value=today_brazil(),
            key="date_end_aprov"
        )
    
    with col3:
        only_pending = st.checkbox("Apenas Pendentes", value=True)

    is_consumo_cd = is_user_consumo_cd(engine)
    opcoes_origem = [ORIGEM_CONSUMO] if is_consumo_cd else OPCOES_ORIGEM_FILTRO

    with col4:
        origem_filtro = st.selectbox(
            "Origem:",
            opcoes_origem,
            index=0,
            key="origem_filtro_aprov",
        )

    with col5:
        filtro_loja = st.selectbox(
            "Lojas no pedido:",
            OPCOES_FILTRO_LOJA,
            index=0,
            key="filtro_loja_aprov",
        )
    
    # Buscar pedidos
    df_pedidos = get_pedidos_para_aprovacao(
        engine,
        date_start,
        date_end,
        only_pending,
        origem_filtro,
    )
    df_pedidos = filtrar_pedidos_por_tipo_loja(df_pedidos, filtro_loja)
    
    if df_pedidos.empty:
        st.info("Nenhum pedido encontrado no período selecionado.")
    else:
        st.markdown(f"### 📊 {len(df_pedidos)} pedido(s) encontrado(s)")

        if not df_produtos.empty:
            df_pedidos['Status Mix'] = df_pedidos['codigo_interno'].apply(
                lambda x: get_product_info(df_produtos, x)['Mix'] if get_product_info(df_produtos, x) else 'N/A'
            )
            df_pedidos['Status Mix'] = df_pedidos['Status Mix'].map({'A': '✅ Ativo', 'S': '⚠️ Suspenso', 'N/A': '❓ N/A'})
        else:
            df_pedidos['Status Mix'] = '❓ N/A'

        df_pedidos.insert(0, "Selecionar", False)

        if "origem_pedido" in df_pedidos.columns:
            df_pedidos["Origem"] = df_pedidos["origem_pedido"].apply(formatar_origem_pedido)
        else:
            df_pedidos["Origem"] = formatar_origem_pedido(ORIGEM_CD_GERAL)

        cols_exibicao = [
            "Selecionar", "id_pedido", "data_pedido_str", "usuario_pedido",
            "empresa", "loja_usuario", "Origem", "codigo_interno", "descricao", "Status Mix",
            "embseparacao", "total_cx",
            "status_aprovacao"
        ] + COLUNAS_LOJAS_PEDIDO
        cols_exibicao = [col for col in cols_exibicao if col in df_pedidos.columns]
        df_para_editar = df_pedidos[cols_exibicao].copy()

        selection_key = "aprovacao_selected_ids"
        if selection_key not in st.session_state:
            st.session_state[selection_key] = []

        ids_visiveis = df_para_editar["id_pedido"].tolist()
        selecionados_atuais = set(st.session_state.get(selection_key, []))
        selecionados_atuais = selecionados_atuais.intersection(ids_visiveis)
        st.session_state[selection_key] = list(selecionados_atuais)

        ids_selecionados = st.session_state[selection_key]

        st.markdown("### ⚡ Seleção Rápida")
        if is_consumo_cd:
            col_marcar, col_desmarcar, col_consumo = st.columns(3)

            with col_marcar:
                if st.button("☑️ Marcar Todos", use_container_width=True):
                    st.session_state[selection_key] = ids_visiveis.copy()

            with col_desmarcar:
                if st.button("⬜ Desmarcar Todos", use_container_width=True):
                    st.session_state[selection_key] = []

            with col_consumo:
                if st.button("🛒 Marcar Consumo", use_container_width=True):
                    ids_consumo = df_para_editar.loc[
                        df_para_editar["Origem"] == formatar_origem_pedido(ORIGEM_CONSUMO),
                        "id_pedido",
                    ].tolist()
                    st.session_state[selection_key] = adicionar_ids_selecionados(
                        ids_selecionados,
                        ids_consumo,
                    )
        else:
            col_marcar, col_desmarcar, col_cd15, col_cd16, col_consumo = st.columns(5)

            with col_marcar:
                if st.button("☑️ Marcar Todos", use_container_width=True):
                    st.session_state[selection_key] = ids_visiveis.copy()

            with col_desmarcar:
                if st.button("⬜ Desmarcar Todos", use_container_width=True):
                    st.session_state[selection_key] = []

            with col_cd15:
                if st.button("📦 Marcar CD15", use_container_width=True):
                    ids_cd15 = df_para_editar.loc[
                        df_para_editar["Origem"] == formatar_origem_pedido(ORIGEM_CD15),
                        "id_pedido",
                    ].tolist()
                    st.session_state[selection_key] = adicionar_ids_selecionados(
                        ids_selecionados,
                        ids_cd15,
                    )

            with col_cd16:
                if st.button("📦 Marcar CD16", use_container_width=True):
                    ids_cd16 = df_para_editar.loc[
                        df_para_editar["Origem"] == formatar_origem_pedido(ORIGEM_CD16),
                        "id_pedido",
                    ].tolist()
                    st.session_state[selection_key] = adicionar_ids_selecionados(
                        ids_selecionados,
                        ids_cd16,
                    )

            with col_consumo:
                if st.button("🛒 Marcar Consumo", use_container_width=True):
                    ids_consumo = df_para_editar.loc[
                        df_para_editar["Origem"] == formatar_origem_pedido(ORIGEM_CONSUMO),
                        "id_pedido",
                    ].tolist()
                    st.session_state[selection_key] = adicionar_ids_selecionados(
                        ids_selecionados,
                        ids_consumo,
                    )

        df_para_editar["Selecionar"] = df_para_editar["id_pedido"].isin(
            st.session_state[selection_key]
        )

        st.markdown("---")

        column_config = {
            "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False),
            "id_pedido": None,
            "data_pedido_str": st.column_config.TextColumn("Data/Hora", disabled=True),
            "usuario_pedido": st.column_config.TextColumn("Usuário", disabled=True, width="small"),
            "empresa": st.column_config.TextColumn("Empresa", disabled=True, width="small"),
            "loja_usuario": st.column_config.TextColumn("Lojas de Acesso", disabled=True, width="small"),
            "Origem": st.column_config.TextColumn(
                "Origem",
                disabled=True,
                width="medium"
            ),
            "codigo_interno": st.column_config.NumberColumn("Cód. Consinco", disabled=True, format="%d"),
            "descricao": st.column_config.TextColumn("Produto", disabled=True, width="large"),
            "Status Mix": st.column_config.TextColumn("Mix", disabled=True, width="small"),
            "embseparacao": st.column_config.NumberColumn("Emb", disabled=True, format="%d"),
            "total_cx": st.column_config.NumberColumn("Total CX", format="%d"),
            "status_aprovacao": st.column_config.TextColumn("Status", disabled=True, width="small"),
        }

        for col in COLUNAS_LOJAS_PEDIDO:
            if col in df_para_editar.columns:
                loja_num = col.replace("loja_", "")
                column_config[col] = st.column_config.NumberColumn(
                    loja_num, min_value=0, step=1, format="%d"
                )

        df_editado = st.data_editor(
            df_para_editar,
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            key="editor_aprovacao_v2"
        )

        selecionados_editor = df_editado.loc[
            df_editado["Selecionar"] == True, "id_pedido"
        ].tolist()
        st.session_state[selection_key] = selecionados_editor

        for idx in df_editado.index:
            soma = sum(df_editado.loc[idx, col] for col in COLUNAS_LOJAS_PEDIDO if col in df_editado.columns)
            df_editado.loc[idx, "total_cx"] = soma

        st.markdown("---")
        col_aprovar, col_reprovar = st.columns(2)

        with col_aprovar:
            if st.button("✅ Aprovar Selecionados", type="primary", use_container_width=True):
                selecionados = df_editado[df_editado["Selecionar"] == True]

                if selecionados.empty:
                    st.warning("Nenhum pedido selecionado.")
                else:
                    if update_pedidos_aprovados(engine, selecionados):
                        st.success(f"✅ {len(selecionados)} pedido(s) aprovado(s) com sucesso!")
                        st.rerun()

        with col_reprovar:
            if st.button("❌ Reprovar Selecionados", use_container_width=True):
                selecionados = df_editado[df_editado["Selecionar"] == True]

                if selecionados.empty:
                    st.warning("Nenhum pedido selecionado.")
                else:
                    ids_reprovar = selecionados["id_pedido"].tolist()
                    if reprovar_pedidos(engine, ids_reprovar):
                        st.success(f"❌ {len(selecionados)} pedido(s) reprovado(s)!")
                        st.rerun()
    
    # --- Download de Pedidos Aprovados ---
    st.markdown("---")
    st.subheader("📥 Download de Pedidos Aprovados")

    if "aprovacao_excel_payload" not in st.session_state:
        st.session_state["aprovacao_excel_payload"] = None

    origem_download = st.selectbox(
        "Filtrar download por origem:",
        opcoes_origem,
        index=0,
        key="origem_download_aprov",
    )

    alerta_download = get_alerta_download_pendente(engine)
    if alerta_download["pendente"]:
        st.warning(
            "⚠️ Existem "
            f"{alerta_download['total_itens']} item(ns) aprovado(s) "
            "hoje sem download registrado."
        )

    payload_dia, erro_payload_dia = gerar_payload_excel_aprovados_dia(
        engine,
        origem_download,
    )
    if payload_dia:
        st.caption(
            f"{payload_dia['total_itens']} linha(s) disponíveis para exportação hoje."
        )
    else:
        st.session_state["aprovacao_excel_payload"] = None

    if st.button(
        "Gerar Excel de Pedidos Aprovados do Dia",
        disabled=payload_dia is None,
    ):
        try:
            st.session_state["aprovacao_excel_payload"] = payload_dia
            st.success("Excel do dia gerado e pronto para download.")
        except Exception as e:
            st.error(f"Erro ao gerar Excel: {e}")

    if payload_dia is None and erro_payload_dia:
        st.info(erro_payload_dia)

    payload_excel = st.session_state.get("aprovacao_excel_payload")
    if payload_excel:
        baixou = st.download_button(
            label="📥 Baixar Pedidos Aprovados (Excel)",
            data=payload_excel["bytes"],
            file_name=payload_excel["nome"],
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_aprovados_excel"
        )
        if baixou:
            usuario = st.session_state.get("username", "unknown")
            if registrar_download_aprovados(engine, usuario):
                st.success("Download registrado com sucesso.")
                st.session_state["aprovacao_excel_payload"] = None
