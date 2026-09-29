"""
Módulo de carregamento e gestão de dados para Fornecedores e Representantes.
Conecta os produtos do query.parquet, estoque CD15, EANs e customizações.
"""
import os
import json
import streamlit as st
import pandas as pd
from sqlalchemy import text


def _normalizar_caminho(caminho):
    return os.path.abspath(os.path.normpath(caminho))


def find_parquet_file(filename: str, base_data_path: str = None) -> str | None:
    """Busca um arquivo parquet em locais conhecidos (Render Disk, bdados local, etc)."""
    candidatos = []
    
    if base_data_path:
        candidatos.append(os.path.join(base_data_path, "bdados", filename))
        candidatos.append(os.path.join(base_data_path, filename))
        
    render_disk = os.environ.get("RENDER_DISK_PATH")
    if render_disk:
        candidatos.append(os.path.join(render_disk, "bdados", filename))
        candidatos.append(os.path.join(render_disk, filename))
        
    # Local do projeto
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidatos.append(os.path.join(base_dir, "bdados", filename))
    candidatos.append(os.path.join("bdados", filename))
    candidatos.append(filename)
    
    for c in candidatos:
        if os.path.exists(c) and os.path.isfile(c):
            return _normalizar_caminho(c)
            
    return None


def parse_fornecedores_acesso(raw_val) -> list[int]:
    """Converte valores brutos (JSON, lista, string delimitada) em lista de inteiros (códigos de fornecedor)."""
    if raw_val is None:
        return []
    if isinstance(raw_val, list):
        res = []
        for x in raw_val:
            try:
                res.append(int(x))
            except (ValueError, TypeError):
                pass
        return res
    if isinstance(raw_val, (int, float)):
        return [int(raw_val)]
    
    raw_str = str(raw_val).strip()
    if not raw_str:
        return []
        
    try:
        data = json.loads(raw_str)
        if isinstance(data, list):
            res = []
            for x in data:
                try:
                    res.append(int(x))
                except (ValueError, TypeError):
                    pass
            return res
        elif isinstance(data, (int, float)):
            return [int(data)]
    except Exception:
        pass
        
    # Tenta quebrar por vírgula, ponto-e-vírgula ou espaço
    cleaned = raw_str.replace(";", ",").replace("\n", ",").replace(" ", ",")
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    res = []
    for p in parts:
        try:
            res.append(int(p))
        except (ValueError, TypeError):
            pass
    return sorted(list(set(res)))


@st.cache_data(ttl=300)
def get_all_fornecedores_catalog(base_data_path: str = None) -> pd.DataFrame:
    """
    Lê query.parquet e retorna todos os fornecedores únicos cadastrados no ERP Consinco.
    Colunas: ['cod_fornecedor', 'nome_fornecedor', 'label', 'total_skus']
    """
    parquet_path = find_parquet_file("query.parquet", base_data_path)
    if not parquet_path or not os.path.exists(parquet_path):
        return pd.DataFrame(columns=["cod_fornecedor", "nome_fornecedor", "label", "total_skus"])
        
    try:
        df = pd.read_parquet(parquet_path)
        if "COD_FORNECEDOR" not in df.columns or "FORNECEDOR" not in df.columns:
            return pd.DataFrame(columns=["cod_fornecedor", "nome_fornecedor", "label", "total_skus"])
            
        df_forn = df[["COD_FORNECEDOR", "FORNECEDOR", "CODIGO_PRODUTO"]].dropna(subset=["COD_FORNECEDOR", "FORNECEDOR"]).copy()
        df_forn["cod_fornecedor"] = pd.to_numeric(df_forn["COD_FORNECEDOR"], errors="coerce").fillna(0).astype(int)
        df_forn["nome_fornecedor"] = df_forn["FORNECEDOR"].astype(str).str.strip()
        
        # Filtra registros inválidos
        df_forn = df_forn[df_forn["cod_fornecedor"] > 0]
        
        # Agrupa para obter o total de SKUs por fornecedor
        agrupado = df_forn.groupby(["cod_fornecedor", "nome_fornecedor"])["CODIGO_PRODUTO"].nunique().reset_index()
        agrupado.rename(columns={"CODIGO_PRODUTO": "total_skus"}, inplace=True)
        
        agrupado["label"] = agrupado.apply(
            lambda r: f"{r['cod_fornecedor']} - {r['nome_fornecedor']} ({r['total_skus']} SKUs)",
            axis=1
        )
        agrupado.sort_values(by=["nome_fornecedor", "cod_fornecedor"], inplace=True)
        return agrupado
    except Exception as e:
        st.error(f"Erro ao carregar catálogo de fornecedores: {e}")
        return pd.DataFrame(columns=["cod_fornecedor", "nome_fornecedor", "label", "total_skus"])


def get_fornecedores_options_dict(base_data_path: str = None) -> dict[int, str]:
    """Retorna dicionário {cod_fornecedor: label} para selects rápidos."""
    df = get_all_fornecedores_catalog(base_data_path)
    if df.empty:
        return {}
    return dict(zip(df["cod_fornecedor"], df["label"]))


def format_fornecedores_summary(codigos: list[int], catalog_df: pd.DataFrame = None, max_display: int = 3) -> str:
    """Retorna uma string resumida amigável dos fornecedores vinculados."""
    if not codigos:
        return "Nenhum fornecedor vinculado"
        
    if catalog_df is None or catalog_df.empty:
        catalog_df = get_all_fornecedores_catalog()
        
    cod_map = dict(zip(catalog_df["cod_fornecedor"], catalog_df["nome_fornecedor"]))
    
    nomes = []
    for cod in codigos:
        nome = cod_map.get(cod)
        if nome:
            nomes.append(f"{cod} - {nome}")
        else:
            nomes.append(str(cod))
            
    total = len(nomes)
    if total <= max_display:
        return f"[{total}] " + ", ".join(nomes)
    else:
        preview = ", ".join(nomes[:max_display])
        return f"[{total}] {preview} e mais {total - max_display}..."


@st.cache_data(ttl=60)
def load_produtos_para_fornecedor(
    fornecedor_codes: list[int] = None,
    empresa_fallback: str = None,
    is_admin: bool = False,
    base_data_path: str = None,
    filtro_fornecedor_selecionado: int = None
) -> pd.DataFrame:
    """
    Carrega e formata produtos do query.parquet com estoque CD15, EAN e embalagem correta.
    Filtra pelos fornecedores autorizados para o usuário/representante.
    """
    parquet_path = find_parquet_file("query.parquet", base_data_path)
    if not parquet_path or not os.path.exists(parquet_path):
        return pd.DataFrame()
        
    try:
        df_raw = pd.read_parquet(parquet_path)
    except Exception as e:
        st.error(f"Erro ao ler query.parquet: {e}")
        return pd.DataFrame()
        
    if df_raw.empty:
        return pd.DataFrame()
        
    # Normaliza colunas
    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    
    if "CODIGO_PRODUTO" not in df_raw.columns:
        st.error("Coluna 'CODIGO_PRODUTO' não encontrada em query.parquet")
        return pd.DataFrame()
        
    # Converte tipos base
    df_raw["CODIGO_PRODUTO"] = pd.to_numeric(df_raw["CODIGO_PRODUTO"], errors="coerce").fillna(0).astype(int)
    if "COD_FORNECEDOR" in df_raw.columns:
        df_raw["COD_FORNECEDOR"] = pd.to_numeric(df_raw["COD_FORNECEDOR"], errors="coerce").fillna(0).astype(int)
    else:
        df_raw["COD_FORNECEDOR"] = 0
        
    if "FORNECEDOR" not in df_raw.columns:
        df_raw["FORNECEDOR"] = "SEM FORNECEDOR"
    else:
        df_raw["FORNECEDOR"] = df_raw["FORNECEDOR"].fillna("").astype(str).str.strip()

    # --- APLICAÇÃO DE FILTROS DE FORNECEDOR ---
    df_filtrado = df_raw.copy()
    
    # 1. Se foi selecionado um fornecedor específico na tela (ex: admin ou representante com múltiplos)
    if filtro_fornecedor_selecionado and filtro_fornecedor_selecionado > 0:
        df_filtrado = df_filtrado[df_filtrado["COD_FORNECEDOR"] == int(filtro_fornecedor_selecionado)]
    elif fornecedor_codes and len(fornecedor_codes) > 0:
        # 2. Usuário possui códigos de fornecedores explicitamente liberados
        cods_set = set(int(c) for c in fornecedor_codes if int(c) > 0)
        df_filtrado = df_filtrado[df_filtrado["COD_FORNECEDOR"].isin(cods_set)]
    elif empresa_fallback and str(empresa_fallback).strip().lower() not in ["administração", "administracao", "baklizi", "admin", ""]:
        # 3. Fallback para compatibilidade por nome/empresa
        emp_clean = str(empresa_fallback).strip()
        if emp_clean.isdigit():
            df_filtrado = df_filtrado[df_filtrado["COD_FORNECEDOR"] == int(emp_clean)]
        else:
            df_filtrado = df_filtrado[df_filtrado["FORNECEDOR"].str.contains(emp_clean, case=False, na=False)]
    elif not is_admin:
        # Usuário regular sem fornecedores configurados
        return pd.DataFrame()
        
    if df_filtrado.empty:
        return pd.DataFrame()
        
    # --- DEDUPLICAÇÃO DE PRODUTOS ---
    df_produtos = df_filtrado.drop_duplicates(subset=["CODIGO_PRODUTO"]).copy()
    
    # --- APURAÇÃO DE ESTOQUE NO CD15 (CODIGO_EMPRESA == 15 ou 015) ---
    if "CODIGO_EMPRESA" in df_raw.columns and "QUANTIDADE_DISPONIVEL" in df_raw.columns:
        mask_cd15 = df_raw["CODIGO_EMPRESA"].astype(str).str.strip().isin(["15", "015", "CD15", "CD 15"])
        df_cd15_calc = df_raw[mask_cd15].groupby("CODIGO_PRODUTO")["QUANTIDADE_DISPONIVEL"].sum().reset_index()
        df_cd15_calc.rename(columns={"QUANTIDADE_DISPONIVEL": "estoque_cd"}, inplace=True)
        df_produtos = df_produtos.merge(df_cd15_calc, on="CODIGO_PRODUTO", how="left")
        df_produtos["estoque_cd"] = pd.to_numeric(df_produtos["estoque_cd"], errors="coerce").fillna(0).astype(int)
    else:
        df_produtos["estoque_cd"] = 0
        
    # --- EMBALAGEM ---
    # Prioridade: EMBL_TRANSFERENCIA > EMBL_COMPRA > 1
    if "EMBL_TRANSFERENCIA" in df_produtos.columns:
        df_produtos["embalagem"] = pd.to_numeric(df_produtos["EMBL_TRANSFERENCIA"], errors="coerce")
    else:
        df_produtos["embalagem"] = None
        
    if "EMBL_COMPRA" in df_produtos.columns:
        df_produtos["embalagem"] = df_produtos["embalagem"].fillna(pd.to_numeric(df_produtos["EMBL_COMPRA"], errors="coerce"))
        
    df_produtos["embalagem"] = df_produtos["embalagem"].fillna(1).replace(0, 1).astype(int)
    
    # --- EAN (ean_dun.parquet) ---
    ean_path = find_parquet_file("ean_dun.parquet", base_data_path)
    if ean_path and os.path.exists(ean_path):
        try:
            df_ean = pd.read_parquet(ean_path)
            if "CODIGO_PRODUTO" in df_ean.columns and "EAN_DUN" in df_ean.columns:
                df_ean["CODIGO_PRODUTO"] = pd.to_numeric(df_ean["CODIGO_PRODUTO"], errors="coerce").fillna(0).astype(int)
                df_ean_clean = df_ean[df_ean["CODIGO_PRODUTO"] > 0].drop_duplicates(subset=["CODIGO_PRODUTO"])[["CODIGO_PRODUTO", "EAN_DUN"]]
                df_produtos = df_produtos.merge(df_ean_clean, on="CODIGO_PRODUTO", how="left")
                df_produtos["codigo_ean"] = df_produtos["EAN_DUN"].fillna("").astype(str)
            else:
                df_produtos["codigo_ean"] = ""
        except Exception:
            df_produtos["codigo_ean"] = ""
    else:
        df_produtos["codigo_ean"] = ""

    # Padroniza colunas de saída
    df_produtos.rename(columns={
        "CODIGO_PRODUTO": "codigo_interno",
        "DESCRICAO_PRODUTO": "descricao",
        "COD_FORNECEDOR": "cod_fornecedor",
        "FORNECEDOR": "fornecedor",
        "DEPARTAMENTO": "departamento",
        "COMPRADOR": "comprador"
    }, inplace=True)
    
    if "descricao" not in df_produtos.columns:
        df_produtos["descricao"] = "SEM DESCRIÇÃO"
        
    df_produtos["fornecedor_label"] = df_produtos.apply(
        lambda r: f"{r['cod_fornecedor']} - {r['fornecedor']}",
        axis=1
    )
    
    colunas_ordenadas = [
        "codigo_interno", "descricao", "fornecedor_label", "cod_fornecedor",
        "fornecedor", "codigo_ean", "embalagem", "estoque_cd"
    ]
    cols_existentes = [c for c in colunas_ordenadas if c in df_produtos.columns]
    
    df_final = df_produtos[cols_existentes].sort_values(by=["fornecedor", "descricao"]).reset_index(drop=True)
    return df_final
