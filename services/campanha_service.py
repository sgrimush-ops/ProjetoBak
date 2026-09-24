"""
services/campanha_service.py
Camada de Serviços e Regras de Negócio para Gestão de Campanhas.
"""

from __future__ import annotations
import os
import uuid
import json
import logging
from datetime import date, datetime
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
from sqlalchemy import text
from utils.timezone import now_brazil
from services.campanha_calculo import (
    calcular_dias_campanha,
    calcular_venda_media_diaria,
    calcular_venda_projetada,
    calcular_caixas_transferencia,
    apurar_disponibilidade_cd,
)

logger = logging.getLogger(__name__)

LISTA_14_LOJAS = [
    "001", "002", "003", "004", "005", "006", "007", "008",
    "011", "012", "013", "014", "017", "018"
]


def gerar_codigo_campanha(engine) -> str:
    """
    Gera um código único e legível no formato CMP-YYYY-XXXXXX baseado no maior sequencial.
    """
    ano = now_brazil().year
    with engine.connect() as conn:
        max_seq = conn.execute(text("""
            SELECT MAX(CAST(SUBSTRING(codigo_campanha FROM '[0-9]+$') AS INTEGER))
            FROM campanhas 
            WHERE codigo_campanha LIKE :prefix
        """), {"prefix": f"CMP-{ano}-%"}).scalar()
        
    proximo = (max_seq or 0) + 1
    return f"CMP-{ano}-{proximo:06d}"


def registrar_historico(
    engine,
    campanha_id: str,
    usuario: str,
    acao: str,
    campo: Optional[str] = None,
    valor_anterior: Optional[str] = None,
    valor_novo: Optional[str] = None,
    detalhes: Optional[str] = None,
    conn=None
) -> None:
    """
    Grava um registro na tabela campanha_historico para auditoria completa.
    """
    sql = text("""
        INSERT INTO campanha_historico (campanha_id, usuario, data_hora, acao, campo, valor_anterior, valor_novo, detalhes)
        VALUES (:cid, :user, :dta, :acao, :campo, :v_ant, :v_novo, :det)
    """)
    params = {
        "cid": campanha_id,
        "user": usuario or "sistema",
        "dta": now_brazil(),
        "acao": acao,
        "campo": campo,
        "v_ant": str(valor_anterior) if valor_anterior is not None else None,
        "v_novo": str(valor_novo) if valor_novo is not None else None,
        "det": detalhes
    }
    if conn is not None:
        conn.execute(sql, params)
    else:
        with engine.begin() as c:
            c.execute(sql, params)


def expirar_campanhas_vencidas(engine) -> int:
    """
    Inativa campanhas com data_fim anterior à data atual.
    """
    hoje = now_brazil().date()
    with engine.begin() as conn:
        res = conn.execute(text("""
            UPDATE campanhas
            SET status = 'INATIVA',
                usuario_atualizacao = 'sistema (expiracao)',
                data_atualizacao = NOW()
            WHERE data_fim < :hoje
              AND status IN ('RASCUNHO', 'ATIVA', 'ENVIADA_SUPPLY', 'EM_AVALIACAO_SUPPLY', 'PENDENCIA_COMPRAS', 'FINALIZADA')
            RETURNING id;
        """), {"hoje": hoje})
        expiradas = [r[0] for r in res.fetchall()]
        for cid in expiradas:
            registrar_historico(
                engine,
                campanha_id=cid,
                usuario="sistema",
                acao="EXPIRACAO_AUTOMATICA",
                campo="status",
                valor_novo="INATIVA",
                detalhes=f"Campanha expirada automaticamente em {hoje}",
                conn=conn
            )
    return len(expiradas)


def obter_lista_compradores(engine) -> List[str]:
    """
    Retorna a lista ordenada de todos os compradores únicos presentes no banco e no catálogo parquet.
    """
    compradores_set = set()
    
    # 1. Busca no banco de dados (campanha_itens)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT comprador 
                FROM campanha_itens 
                WHERE comprador IS NOT NULL AND TRIM(comprador) != '' AND TRIM(comprador) != 'None'
            """)).fetchall()
            for r in rows:
                if r[0] and str(r[0]).strip():
                    compradores_set.add(str(r[0]).strip())
    except Exception as e:
        logger.warning(f"Erro ao buscar compradores no banco: {e}")

    # 2. Complementa com o parquet
    parquet_path = "bdados/query.parquet"
    if os.path.exists(parquet_path):
        try:
            df = pd.read_parquet(parquet_path)
            df.columns = [str(c).strip() for c in df.columns]
            if "COMPRADOR" in df.columns:
                comps_parquet = df["COMPRADOR"].dropna().unique().tolist()
                for cp in comps_parquet:
                    cp_str = str(cp).strip()
                    if cp_str and cp_str not in ["None", "nan", "N/D", "GERAL", ""]:
                        compradores_set.add(cp_str)
        except Exception as e:
            logger.warning(f"Erro ao ler compradores do parquet: {e}")

    return sorted(list(compradores_set))


def listar_campanhas(
    engine,
    status_filtro: Optional[List[str]] = None,
    apenas_vigentes: bool = False,
    comprador_filtro: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Retorna a lista de campanhas cadastradas com filtros de status, vigência e comprador.
    """
    expirar_campanhas_vencidas(engine)
    where_clauses = []
    params: Dict[str, Any] = {}

    if status_filtro:
        where_clauses.append("c.status = ANY(:status_list)")
        params["status_list"] = status_filtro

    if apenas_vigentes:
        hoje = now_brazil().date()
        where_clauses.append("c.data_fim >= :hoje")
        params["hoje"] = hoje

    if comprador_filtro and comprador_filtro not in ["TODOS", "Todos", ""]:
        where_clauses.append("c.id IN (SELECT ci.campanha_id FROM campanha_itens ci WHERE ci.comprador = :comp_filtro)")
        params["comp_filtro"] = str(comprador_filtro).strip()

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    sql = text(f"""
        SELECT 
            c.id, c.codigo_campanha, c.nome, c.data_inicio, c.data_fim, 
            c.status, c.usuario_criacao, c.data_criacao, c.usuario_atualizacao, 
            c.data_atualizacao, c.campanha_origem_id, c.observacoes,
            (SELECT COUNT(*) FROM campanha_itens ci WHERE ci.campanha_id = c.id) as total_itens,
            (SELECT COUNT(*) FROM campanha_devolutivas cd WHERE cd.campanha_id = c.id AND cd.situacao = 'PENDENTE') as total_pendencias,
            (SELECT STRING_AGG(DISTINCT ci.comprador, ', ') FROM campanha_itens ci WHERE ci.campanha_id = c.id AND ci.comprador IS NOT NULL AND ci.comprador != '' AND ci.comprador != 'None') as compradores
        FROM campanhas c
        {where_sql}
        ORDER BY c.data_criacao DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r._mapping) for r in rows]


def obter_campanha_por_id(engine, campanha_id: str) -> Optional[Dict[str, Any]]:
    """
    Retorna os dados completos da capa de uma campanha.
    """
    sql = text("""
        SELECT 
            id, codigo_campanha, nome, data_inicio, data_fim, 
            status, usuario_criacao, data_criacao, usuario_atualizacao, 
            data_atualizacao, campanha_origem_id, observacoes
        FROM campanhas
        WHERE id = :cid
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"cid": campanha_id}).fetchone()
        return dict(row._mapping) if row else None


def criar_campanha(
    engine,
    nome: str,
    data_inicio: date,
    data_fim: date,
    usuario: str,
    observacoes: Optional[str] = None
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Cria uma nova campanha com status RASCUNHO.
    """
    if not nome or not nome.strip():
        return False, "O nome da campanha é obrigatório.", None

    if data_fim < data_inicio:
        return False, "A data final não pode ser anterior à data inicial.", None

    cid = str(uuid.uuid4())
    cod = gerar_codigo_campanha(engine)
    agora = now_brazil()

    sql = text("""
        INSERT INTO campanhas (
            id, codigo_campanha, nome, data_inicio, data_fim, 
            status, usuario_criacao, data_criacao, usuario_atualizacao, 
            data_atualizacao, observacoes
        )
        VALUES (
            :id, :cod, :nome, :d_ini, :d_fim, 
            'RASCUNHO', :user, :agora, :user, 
            :agora, :obs
        )
    """)

    try:
        with engine.begin() as conn:
            conn.execute(sql, {
                "id": cid,
                "cod": cod,
                "nome": nome.strip(),
                "d_ini": data_inicio,
                "d_fim": data_fim,
                "user": usuario,
                "agora": agora,
                "obs": observacoes
            })
            registrar_historico(
                engine,
                campanha_id=cid,
                usuario=usuario,
                acao="CRIACAO",
                detalhes=f"Campanha {cod} criada por {usuario}",
                conn=conn
            )
        camp = obter_campanha_por_id(engine, cid)
        return True, f"Campanha {cod} criada com sucesso!", camp
    except Exception as e:
        logger.error(f"Erro ao criar campanha: {e}")
        return False, f"Erro no banco de dados: {e}", None


def atualizar_campanha(
    engine,
    campanha_id: str,
    nome: str,
    data_inicio: date,
    data_fim: date,
    usuario: str,
    observacoes: Optional[str] = None,
    status: Optional[str] = None
) -> Tuple[bool, str]:
    """
    Atualiza os dados de cabeçalho da campanha.
    """
    if data_fim < data_inicio:
        return False, "A data final não pode ser anterior à data inicial."

    atual = obter_campanha_por_id(engine, campanha_id)
    if not atual:
        return False, "Campanha não encontrada."

    agora = now_brazil()
    status_final = status or atual["status"]

    sql = text("""
        UPDATE campanhas
        SET nome = :nome,
            data_inicio = :d_ini,
            data_fim = :d_fim,
            status = :status,
            usuario_atualizacao = :user,
            data_atualizacao = :agora,
            observacoes = :obs
        WHERE id = :cid
    """)

    try:
        with engine.begin() as conn:
            conn.execute(sql, {
                "cid": campanha_id,
                "nome": nome.strip(),
                "d_ini": data_inicio,
                "d_fim": data_fim,
                "status": status_final,
                "user": usuario,
                "agora": agora,
                "obs": observacoes
            })
            registrar_historico(
                engine,
                campanha_id=campanha_id,
                usuario=usuario,
                acao="EDICAO_CAPA",
                detalhes=f"Atualização da capa da campanha {atual['codigo_campanha']}",
                conn=conn
            )
        return True, "Campanha atualizada com sucesso!"
    except Exception as e:
        logger.error(f"Erro ao atualizar campanha: {e}")
        return False, f"Erro ao atualizar: {e}"


def inativar_campanha(engine, campanha_id: str, usuario: str) -> Tuple[bool, str]:
    """
    Inativa uma campanha.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE campanhas
                SET status = 'INATIVA',
                    usuario_atualizacao = :user,
                    data_atualizacao = NOW()
                WHERE id = :cid
            """), {"cid": campanha_id, "user": usuario})
            registrar_historico(
                engine,
                campanha_id=campanha_id,
                usuario=usuario,
                acao="INATIVACAO",
                campo="status",
                valor_novo="INATIVA",
                detalhes=f"Campanha inativada por {usuario}",
                conn=conn
            )
        return True, "Campanha inativada com sucesso!"
    except Exception as e:
        return False, f"Erro ao inativar campanha: {e}"


def excluir_campanha(engine, campanha_id: str, usuario: str) -> Tuple[bool, str]:
    """
    Exclui permanentemente uma campanha e todos os seus registros associados
    (devolutivas, matriz de lojas, itens e historico). Utilizado por Administradores para limpeza de testes.
    """
    try:
        with engine.begin() as conn:
            # 1. Exclui devolutivas
            conn.execute(text("DELETE FROM campanha_devolutivas WHERE campanha_id = :cid"), {"cid": campanha_id})
            # 2. Exclui lojas dos itens
            conn.execute(text("""
                DELETE FROM campanha_lojas 
                WHERE campanha_item_id IN (SELECT id FROM campanha_itens WHERE campanha_id = :cid)
            """), {"cid": campanha_id})
            # 3. Exclui itens
            conn.execute(text("DELETE FROM campanha_itens WHERE campanha_id = :cid"), {"cid": campanha_id})
            # 4. Exclui historico
            conn.execute(text("DELETE FROM campanha_historico WHERE campanha_id = :cid"), {"cid": campanha_id})
            # 5. Exclui capa da campanha
            conn.execute(text("DELETE FROM campanhas WHERE id = :cid"), {"cid": campanha_id})

        return True, f"Campanha excluída permanentemente pelo administrador {usuario}."
    except Exception as e:
        logger.error(f"Erro ao excluir campanha {campanha_id}: {e}")
        return False, f"Erro ao excluir campanha: {e}"


def replicar_campanha(
    engine,
    campanha_origem_id: str,
    novo_nome: str,
    nova_data_inicio: date,
    nova_data_fim: date,
    usuario: str
) -> Tuple[bool, str, Optional[str]]:
    """
    Clona integralmente a estrutura de itens e lojas da campanha de origem com novo ID e código.
    """
    origem = obter_campanha_por_id(engine, campanha_origem_id)
    if not origem:
        return False, "Campanha de origem não encontrada.", None

    novo_id = str(uuid.uuid4())
    novo_cod = gerar_codigo_campanha(engine)
    agora = now_brazil()

    try:
        with engine.begin() as conn:
            # 1. Cria nova capa
            conn.execute(text("""
                INSERT INTO campanhas (
                    id, codigo_campanha, nome, data_inicio, data_fim,
                    status, usuario_criacao, data_criacao, usuario_atualizacao,
                    data_atualizacao, campanha_origem_id, observacoes
                )
                VALUES (
                    :id, :cod, :nome, :d_ini, :d_fim,
                    'RASCUNHO', :user, :agora, :user,
                    :agora, :origem_id, :obs
                )
            """), {
                "id": novo_id,
                "cod": novo_cod,
                "nome": novo_nome.strip(),
                "d_ini": nova_data_inicio,
                "d_fim": nova_data_fim,
                "user": usuario,
                "agora": agora,
                "origem_id": campanha_origem_id,
                "obs": f"Replicada a partir de {origem['codigo_campanha']}"
            })

            # 2. Clona itens
            itens_origem = conn.execute(text("""
                SELECT id, produto_codigo, descricao_snapshot, embalagem_compra, embalagem_transferencia
                FROM campanha_itens
                WHERE campanha_id = :origem_id
            """), {"origem_id": campanha_origem_id}).fetchall()

            for it in itens_origem:
                novo_item_id = conn.execute(text("""
                    INSERT INTO campanha_itens (
                        campanha_id, produto_codigo, descricao_snapshot, embalagem_compra, embalagem_transferencia
                    )
                    VALUES (:cid, :pcod, :desc, :e_comp, :e_transf)
                    RETURNING id
                """), {
                    "cid": novo_id,
                    "pcod": it.produto_codigo,
                    "desc": it.descricao_snapshot,
                    "e_comp": it.embalagem_compra,
                    "e_transf": it.embalagem_transferencia
                }).scalar()

                # Clona matriz de lojas com sugestão de compras zerada/preservada
                conn.execute(text("""
                    INSERT INTO campanha_lojas (
                        campanha_item_id, loja_codigo, tipo_exposicao_id, estrutura_exposicao_id,
                        volume_comprador, volume_calculado, volume_final_supply, caixas_transferencia,
                        volume_transferencia, estoque_loja, estoque_cd, venda_media, venda_projetada,
                        status_estoque, caixas_falta, quantidade_falta
                    )
                    SELECT 
                        :novo_item_id, loja_codigo, tipo_exposicao_id, estrutura_exposicao_id,
                        volume_comprador, 0, 0, 0, 0, 0, 0, 0, 0, 'OK', 0, 0
                    FROM campanha_lojas
                    WHERE campanha_item_id = :item_origem_id
                """), {"novo_item_id": novo_item_id, "item_origem_id": it.id})

            registrar_historico(
                engine,
                campanha_id=novo_id,
                usuario=usuario,
                acao="REPLICACAO",
                detalhes=f"Campanha replicada de {origem['codigo_campanha']}",
                conn=conn
            )

        return True, f"Campanha {novo_cod} replicada com sucesso a partir de {origem['codigo_campanha']}!", novo_id
    except Exception as e:
        logger.error(f"Erro ao replicar campanha: {e}")
        return False, f"Erro ao replicar: {e}", None


# =============================================================================
# BUSCA E SNAPSHOTS DE PRODUTOS NO PARQUET + BD
# =============================================================================

def carregar_dados_produto_consolidado(
    engine,
    produto_codigo: int,
    data_inicio: date,
    data_fim: date,
    base_data_path: str = "data"
) -> Dict[str, Any]:
    """
    Lê os dados operacionais do parquet para o produto especificado (estoques, vendas, embalagens)
    e une com as dimensões cadastradas no banco.
    """
    parquet_path = "bdados/query.parquet"
    dias_camp = calcular_dias_campanha(data_inicio, data_fim)

    resultado: Dict[str, Any] = {
        "produto_codigo": int(produto_codigo),
        "descricao": "PRODUTO NÃO ENCONTRADO",
        "embalagem_compra": 1,
        "embalagem_transferencia": 1,
        "estoque_total_lojas": 0.0,
        "estoque_cd15": 0.0,
        "venda_media_diaria_total": 0.0,
        "venda_projetada_total": 0.0,
        "dimensoes": None,
        "lojas_detalhe": {}
    }

    # 1. Dimensões no banco
    with engine.connect() as conn:
        dim = conn.execute(text("""
            SELECT altura_cm, largura_cm, profundidade_cm 
            FROM produto_dimensoes 
            WHERE produto_codigo = :pcod
        """), {"pcod": int(produto_codigo)}).fetchone()
        if dim:
            resultado["dimensoes"] = {
                "altura_cm": float(dim.altura_cm),
                "largura_cm": float(dim.largura_cm),
                "profundidade_cm": float(dim.profundidade_cm)
            }

    # 2. Dados no parquet
    import os
    if os.path.exists(parquet_path):
        try:
            df = pd.read_parquet(parquet_path)
            df.columns = [str(c).strip() for c in df.columns]
            df_prod = df[df["CODIGO_PRODUTO"] == int(produto_codigo)]

            if not df_prod.empty:
                primeiro = df_prod.iloc[0]
                resultado["descricao"] = str(primeiro.get("DESCRICAO_PRODUTO", "")).strip()
                depto = str(primeiro.get("DEPARTAMENTO", "")).strip()
                comp = str(primeiro.get("COMPRADOR", "")).strip()
                resultado["departamento"] = depto if depto and depto != "None" else "GERAL"
                resultado["comprador"] = comp if comp and comp != "None" else ""

                # Captura Família
                cod_fam = primeiro.get("CODIGO_FAMILIA")
                desc_fam = primeiro.get("DESCRICAO_FAMILIA")
                if pd.notna(cod_fam) and str(cod_fam).isdigit() and int(cod_fam) > 0:
                    resultado["codigo_familia"] = int(cod_fam)
                    resultado["descricao_familia"] = str(desc_fam).strip() if pd.notna(desc_fam) else resultado["descricao"]
                else:
                    resultado["codigo_familia"] = None
                    resultado["descricao_familia"] = None

                # Captura fornecedor e código do fornecedor
                cod_forn = primeiro.get("COD_FORNECEDOR")
                nome_forn = primeiro.get("FORNECEDOR") or primeiro.get("RAZAO_FORN_PRINCIPAL")

                if pd.notna(nome_forn) and str(nome_forn).strip() and str(nome_forn).strip().upper() not in ["SEM FORNECEDOR PRINCIPAL", "GERAL", "N/D", "NONE", "NAN"]:
                    if pd.notna(cod_forn) and str(cod_forn).strip() and str(cod_forn) not in ["0", "None", "nan"]:
                        try:
                            resultado["fornecedor"] = f"{int(float(cod_forn))} - {str(nome_forn).strip()}"
                        except Exception:
                            resultado["fornecedor"] = f"{str(cod_forn).strip()} - {str(nome_forn).strip()}"
                    else:
                        resultado["fornecedor"] = str(nome_forn).strip()
                else:
                    resultado["fornecedor"] = "SEM FORNECEDOR PRINCIPAL"
                
                emb_c = primeiro.get("EMBL_COMPRA", 1)
                emb_t = primeiro.get("EMBL_TRANSFERENCIA", 1)
                resultado["embalagem_compra"] = int(emb_c) if pd.notna(emb_c) and int(emb_c) > 0 else 1
                resultado["embalagem_transferencia"] = int(emb_t) if pd.notna(emb_t) and int(emb_t) > 0 else resultado["embalagem_compra"]

                venda_total_30d = 0.0

                for _, row in df_prod.iterrows():
                    emp = int(row.get("CODIGO_EMPRESA", 0))
                    emp_str = str(emp).zfill(3)
                    estoque = float(row.get("QUANTIDADE_DISPONIVEL", 0.0) or 0.0)
                    venda_p = float(row.get("QTD_VENDIDA_PERIODO", 0.0) or 0.0)
                    dias_pesq = float(row.get("DIAS_PESQUISA", 30.0) or 30.0)
                    v_med = calcular_venda_media_diaria(venda_p, dias_pesq)
                    v_proj = calcular_venda_projetada(v_med, dias_camp)

                    if emp == 15 or emp_str == "015":
                        resultado["estoque_cd15"] = estoque
                    elif emp_str in LISTA_14_LOJAS:
                        resultado["estoque_total_lojas"] += estoque
                        venda_total_30d += venda_p
                        resultado["lojas_detalhe"][emp_str] = {
                            "estoque_loja": estoque,
                            "venda_media": v_med,
                            "venda_projetada": v_proj
                        }

                resultado["venda_media_diaria_total"] = calcular_venda_media_diaria(venda_total_30d, 30.0)
                resultado["venda_projetada_total"] = calcular_venda_projetada(resultado["venda_media_diaria_total"], dias_camp)

                # Busca outros SKUs irmãos da mesma família se houver CODIGO_FAMILIA
                resultado["skus_familia"] = []
                if resultado["codigo_familia"]:
                    df_fam = df[df["CODIGO_FAMILIA"] == resultado["codigo_familia"]]
                    df_fam_uniq = df_fam.drop_duplicates(subset=["CODIGO_PRODUTO"])
                    for _, rf in df_fam_uniq.iterrows():
                        p_cod = int(rf["CODIGO_PRODUTO"])
                        st_comp = str(rf.get("STATUS_COMPRA", "Ativo")).strip()
                        ec = int(rf.get("EMBL_COMPRA", 1) or 1)
                        et = int(rf.get("EMBL_TRANSFERENCIA", 1) or ec)
                        resultado["skus_familia"].append({
                            "produto_codigo": p_cod,
                            "descricao": str(rf.get("DESCRICAO_PRODUTO", "")).strip(),
                            "status_compra": st_comp,
                            "embalagem_compra": ec,
                            "embalagem_transferencia": et,
                            "is_selecionado_padrao": st_comp.lower() in ["ativo", "a"]
                        })

                    # Se este SKU individual ainda não tem dimensões, herda da família (qualquer outro SKU irmão já cadastrado)
                    if resultado["dimensoes"] is None and resultado["skus_familia"]:
                        cods_fam = [int(s["produto_codigo"]) for s in resultado["skus_familia"]]
                        with engine.connect() as conn:
                            dim_fam = conn.execute(text("""
                                SELECT altura_cm, largura_cm, profundidade_cm 
                                FROM produto_dimensoes 
                                WHERE produto_codigo = ANY(:cods)
                                ORDER BY data_atualizacao DESC
                                LIMIT 1
                            """), {"cods": cods_fam}).fetchone()
                            if dim_fam:
                                resultado["dimensoes"] = {
                                    "altura_cm": float(dim_fam.altura_cm),
                                    "largura_cm": float(dim_fam.largura_cm),
                                    "profundidade_cm": float(dim_fam.profundidade_cm)
                                }

        except Exception as e:
            logger.error(f"Erro ao ler parquet para produto {produto_codigo}: {e}")

    # Garante que todas as 14 lojas estejam presentes
    for lj in LISTA_14_LOJAS:
        if lj not in resultado["lojas_detalhe"]:
            resultado["lojas_detalhe"][lj] = {
                "estoque_loja": 0.0,
                "venda_media": 0.0,
                "venda_projetada": 0.0
            }

    return resultado


def salvar_item_campanha_compras(
    engine,
    campanha_id: str,
    produto_codigo: int,
    descricao: str,
    embalagem_compra: int,
    embalagem_transferencia: int,
    dados_lojas: List[Dict[str, Any]],
    usuario: str,
    fornecedor: Optional[str] = None,
    departamento: Optional[str] = None,
    comprador: Optional[str] = None,
    codigo_familia: Optional[int] = None,
    descricao_familia: Optional[str] = None,
    total_skus_familia: int = 1
) -> Tuple[bool, str]:
    """
    Adiciona ou atualiza um item na campanha e sua matriz de 14 lojas pelo Comprador.
    """
    try:
        with engine.begin() as conn:
            # 1. Upsert no item
            item_row = conn.execute(text("""
                INSERT INTO campanha_itens (
                    campanha_id, produto_codigo, descricao_snapshot, fornecedor, departamento, comprador,
                    codigo_familia, descricao_familia, total_skus_familia,
                    embalagem_compra, embalagem_transferencia
                )
                VALUES (:cid, :pcod, :desc, :forn, :dep, :comp, :cod_fam, :desc_fam, :tot_skus, :e_c, :e_t)
                ON CONFLICT (campanha_id, produto_codigo) DO UPDATE
                SET descricao_snapshot = EXCLUDED.descricao_snapshot,
                    fornecedor = COALESCE(EXCLUDED.fornecedor, campanha_itens.fornecedor),
                    departamento = COALESCE(EXCLUDED.departamento, campanha_itens.departamento),
                    comprador = COALESCE(EXCLUDED.comprador, campanha_itens.comprador),
                    codigo_familia = COALESCE(EXCLUDED.codigo_familia, campanha_itens.codigo_familia),
                    descricao_familia = COALESCE(EXCLUDED.descricao_familia, campanha_itens.descricao_familia),
                    total_skus_familia = EXCLUDED.total_skus_familia,
                    embalagem_compra = EXCLUDED.embalagem_compra,
                    embalagem_transferencia = EXCLUDED.embalagem_transferencia
                RETURNING id;
            """), {
                "cid": campanha_id,
                "pcod": int(produto_codigo),
                "desc": descricao,
                "forn": fornecedor or "FORNECEDOR DIVERSOS",
                "dep": departamento or "",
                "comp": comprador or "",
                "cod_fam": codigo_familia,
                "desc_fam": descricao_familia,
                "tot_skus": max(1, total_skus_familia),
                "e_c": int(embalagem_compra or 1),
                "e_t": int(embalagem_transferencia or 1)
            }).fetchone()

            item_id = item_row[0]

            # 2. Upsert nas 14 lojas
            for d in dados_lojas:
                lj = str(d["loja_codigo"]).zfill(3)
                tipo_exp_id = d.get("tipo_exposicao_id")
                est_exp_id = d.get("estrutura_exposicao_id")
                vol_comp = d.get("volume_comprador")
                est_lj = float(d.get("estoque_loja", 0.0) or 0.0)
                est_cd = float(d.get("estoque_cd", 0.0) or 0.0)
                v_med = float(d.get("venda_media", 0.0) or 0.0)
                v_proj = float(d.get("venda_projetada", 0.0) or 0.0)

                conn.execute(text("""
                    INSERT INTO campanha_lojas (
                        campanha_item_id, loja_codigo, tipo_exposicao_id, estrutura_exposicao_id,
                        volume_comprador, estoque_loja, estoque_cd, venda_media, venda_projetada, atualizado_em
                    )
                    VALUES (
                        :item_id, :loja, :tipo_exp, :est_exp,
                        :vol_c, :est_l, :est_cd, :v_med, :v_proj, NOW()
                    )
                    ON CONFLICT (campanha_item_id, loja_codigo) DO UPDATE
                    SET tipo_exposicao_id = EXCLUDED.tipo_exposicao_id,
                        estrutura_exposicao_id = EXCLUDED.estrutura_exposicao_id,
                        volume_comprador = EXCLUDED.volume_comprador,
                        estoque_loja = EXCLUDED.estoque_loja,
                        estoque_cd = EXCLUDED.estoque_cd,
                        venda_media = EXCLUDED.venda_media,
                        venda_projetada = EXCLUDED.venda_projetada,
                        atualizado_em = NOW();
                """), {
                    "item_id": item_id,
                    "loja": lj,
                    "tipo_exp": tipo_exp_id,
                    "est_exp": est_exp_id,
                    "vol_c": vol_comp,
                    "est_l": est_lj,
                    "est_cd": est_cd,
                    "v_med": v_med,
                    "v_proj": v_proj
                })

            # Atualiza status da campanha para ATIVA se estava em RASCUNHO
            conn.execute(text("""
                UPDATE campanhas
                SET status = 'ATIVA',
                    usuario_atualizacao = :user,
                    data_atualizacao = NOW()
                WHERE id = :cid AND status = 'RASCUNHO'
            """), {"cid": campanha_id, "user": usuario})

            registrar_historico(
                engine,
                campanha_id=campanha_id,
                usuario=usuario,
                acao="SALVAR_ITEM",
                detalhes=f"Item {produto_codigo} salvo na campanha por {usuario}",
                conn=conn
            )

        return True, f"Produto {produto_codigo} salvo com sucesso na campanha!"
    except Exception as e:
        logger.error(f"Erro ao salvar item na campanha: {e}")
        return False, f"Erro ao salvar produto: {e}"


def salvar_familia_campanha_compras(
    engine,
    campanha_id: str,
    produtos_familia: List[Dict[str, Any]],
    dados_lojas: List[Dict[str, Any]],
    usuario: str,
    data_inicio: date,
    data_fim: date,
    base_data_path: str = "data"
) -> Tuple[bool, str]:
    """
    Salva em lote múltiplos produtos pertencentes à mesma família de exposição.
    """
    if not produtos_familia:
        return False, "Nenhum produto da família selecionado."

    total_skus = len(produtos_familia)
    sucessos = 0
    erros = []

    for p in produtos_familia:
        p_cod = p["produto_codigo"]
        prod_data = carregar_dados_produto_consolidado(
            engine=engine,
            produto_codigo=p_cod,
            data_inicio=data_inicio,
            data_fim=data_fim,
            base_data_path=base_data_path
        )

        # Monta matriz de lojas para este SKU específico
        lojas_sku = []
        for dl in dados_lojas:
            lj_c = dl["loja_codigo"]
            lj_det = prod_data["lojas_detalhe"].get(lj_c, {})
            lojas_sku.append({
                "loja_codigo": lj_c,
                "tipo_exposicao_id": dl.get("tipo_exposicao_id"),
                "estrutura_exposicao_id": dl.get("estrutura_exposicao_id"),
                "volume_comprador": dl.get("volume_comprador", 0),
                "estoque_loja": lj_det.get("estoque_loja", 0.0),
                "estoque_cd": prod_data["estoque_cd15"],
                "venda_media": lj_det.get("venda_media", 0.0),
                "venda_projetada": lj_det.get("venda_projetada", 0.0)
            })

        suc, msg = salvar_item_campanha_compras(
            engine=engine,
            campanha_id=campanha_id,
            produto_codigo=p_cod,
            descricao=prod_data["descricao"],
            embalagem_compra=prod_data["embalagem_compra"],
            embalagem_transferencia=prod_data["embalagem_transferencia"],
            dados_lojas=lojas_sku,
            usuario=usuario,
            fornecedor=prod_data.get("fornecedor"),
            departamento=prod_data.get("departamento"),
            comprador=prod_data.get("comprador"),
            codigo_familia=prod_data.get("codigo_familia"),
            descricao_familia=prod_data.get("descricao_familia"),
            total_skus_familia=total_skus
        )
        if suc:
            sucessos += 1
        else:
            erros.append(f"{p_cod}: {msg}")

    if sucessos > 0:
        return True, f"Família cadastrada com sucesso! {sucessos} produtos inseridos na campanha compartilhando a exposição."
    else:
        return False, f"Falha ao salvar família: {'; '.join(erros)}"


def remover_item_campanha(engine, campanha_id: str, produto_codigo: int, usuario: str) -> Tuple[bool, str]:
    """
    Remove um produto da campanha.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM campanha_itens
                WHERE campanha_id = :cid AND produto_codigo = :pcod
            """), {"cid": campanha_id, "pcod": int(produto_codigo)})

            registrar_historico(
                engine,
                campanha_id=campanha_id,
                usuario=usuario,
                acao="REMOVER_ITEM",
                detalhes=f"Item {produto_codigo} removido da campanha por {usuario}",
                conn=conn
            )
        return True, f"Item {produto_codigo} removido com sucesso."
    except Exception as e:
        return False, f"Erro ao remover item: {e}"


def enviar_campanha_para_supply(engine, campanha_id: str, usuario: str) -> Tuple[bool, str]:
    """
    Valida e despacha a campanha de Compras para o Supply (muda status para ENVIADA_SUPPLY).
    """
    with engine.connect() as conn:
        total_itens = conn.execute(text("""
            SELECT COUNT(*) FROM campanha_itens WHERE campanha_id = :cid
        """), {"cid": campanha_id}).scalar() or 0

        if total_itens == 0:
            return False, "A campanha não possui nenhum produto cadastrado."

        # Verifica se há lojas com tipo de exposição não selecionado
        lojas_sem_exp = conn.execute(text("""
            SELECT COUNT(*) 
            FROM campanha_lojas cl
            JOIN campanha_itens ci ON ci.id = cl.campanha_item_id
            WHERE ci.campanha_id = :cid AND cl.tipo_exposicao_id IS NULL
        """), {"cid": campanha_id}).scalar() or 0

        if lojas_sem_exp > 0:
            return False, f"Existem {lojas_sem_exp} loja(s) sem tipo de exposição selecionado."

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE campanhas
                SET status = 'ENVIADA_SUPPLY',
                    usuario_atualizacao = :user,
                    data_atualizacao = NOW()
                WHERE id = :cid
            """), {"cid": campanha_id, "user": usuario})

            registrar_historico(
                engine,
                campanha_id=campanha_id,
                usuario=usuario,
                acao="ENVIO_SUPPLY",
                campo="status",
                valor_novo="ENVIADA_SUPPLY",
                detalhes=f"Campanha enviada para análise do Supply por {usuario}",
                conn=conn
            )
        return True, "Campanha enviada para o Supply com sucesso!"
    except Exception as e:
        return False, f"Erro ao enviar para o Supply: {e}"


# =============================================================================
# OPERAÇÕES DO SUPPLY: DIMENSÕES, CUBAGEM E FECHAMENTO
# =============================================================================

def salvar_dimensoes_produto(
    engine,
    produto_codigo: int,
    altura_cm: float,
    largura_cm: float,
    profundidade_cm: float,
    usuario: str,
    propagar_familia: bool = True
) -> Tuple[bool, str]:
    """
    Salva ou atualiza as dimensões físicas de um produto na tabela produto_dimensoes.
    Se o produto fizer parte de uma família e propagar_familia for True, replica automaticamente
    as dimensões para todos os outros SKUs da mesma família.
    """
    if any(d <= 0 for d in [altura_cm, largura_cm, profundidade_cm]):
        return False, "Todas as dimensões em cm devem ser maiores que zero."

    # Identifica família do produto para propagação
    produtos_para_salvar = [int(produto_codigo)]
    desc_familia_info = ""

    if propagar_familia:
        parquet_path = "bdados/query.parquet"
        import os
        if os.path.exists(parquet_path):
            try:
                df = pd.read_parquet(parquet_path)
                df.columns = [str(c).strip() for c in df.columns]
                row_prod = df[df["CODIGO_PRODUTO"] == int(produto_codigo)]
                if not row_prod.empty:
                    cod_fam = row_prod.iloc[0].get("CODIGO_FAMILIA")
                    if pd.notna(cod_fam) and str(cod_fam).isdigit() and int(cod_fam) > 0:
                        df_fam = df[df["CODIGO_FAMILIA"] == int(cod_fam)]
                        skus_irmaos = df_fam["CODIGO_PRODUTO"].dropna().unique().tolist()
                        produtos_para_salvar = [int(p) for p in skus_irmaos]
                        desc_fam_nome = str(row_prod.iloc[0].get("DESCRICAO_FAMILIA") or f"Família {cod_fam}").strip()
                        desc_familia_info = f" (aplicado a todos os {len(produtos_para_salvar)} SKUs da Família {cod_fam} — {desc_fam_nome})"
            except Exception as e:
                logger.warning(f"Erro ao buscar família para propagação de dimensões: {e}")

    sql = text("""
        INSERT INTO produto_dimensoes (
            produto_codigo, altura_cm, largura_cm, profundidade_cm, 
            data_atualizacao, usuario_atualizacao
        )
        VALUES (:pcod, :alt, :larg, :prof, NOW(), :user)
        ON CONFLICT (produto_codigo) DO UPDATE
        SET altura_cm = EXCLUDED.altura_cm,
            largura_cm = EXCLUDED.largura_cm,
            profundidade_cm = EXCLUDED.profundidade_cm,
            data_atualizacao = NOW(),
            usuario_atualizacao = EXCLUDED.usuario_atualizacao;
    """)

    try:
        with engine.begin() as conn:
            for pcod in produtos_para_salvar:
                conn.execute(sql, {
                    "pcod": int(pcod),
                    "alt": float(altura_cm),
                    "larg": float(largura_cm),
                    "prof": float(profundidade_cm),
                    "user": usuario
                })
        return True, f"Dimensões salvas com sucesso para o produto {produto_codigo}{desc_familia_info}!"
    except Exception as e:
        return False, f"Erro ao salvar dimensões: {e}"


def obter_estruturas_e_bandejas(engine, tipo_exposicao_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Retorna as estruturas e suas bandejas ativas.
    """
    where_sql = "WHERE e.ativo = TRUE"
    params = {}
    if tipo_exposicao_id:
        where_sql += " AND e.tipo_exposicao_id = :tipo_id"
        params["tipo_id"] = int(tipo_exposicao_id)

    sql = text(f"""
        SELECT 
            e.id as estrutura_id, e.tipo_exposicao_id, e.nome as estrutura_nome,
            t.nome as tipo_nome,
            b.id as bandeja_id, b.numero_bandeja, b.largura_cm, b.profundidade_cm, b.altura_cm, b.ordem, b.ativa
        FROM estruturas_exposicao e
        JOIN tipos_exposicao t ON t.id = e.tipo_exposicao_id
        LEFT JOIN bandejas_exposicao b ON b.estrutura_exposicao_id = e.id AND b.ativa = TRUE
        {where_sql}
        ORDER BY e.nome, b.ordem, b.numero_bandeja
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    estruturas_dict: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        eid = r.estrutura_id
        if eid not in estruturas_dict:
            estruturas_dict[eid] = {
                "estrutura_id": eid,
                "tipo_exposicao_id": r.tipo_exposicao_id,
                "estrutura_nome": r.estrutura_nome,
                "tipo_nome": r.tipo_nome,
                "bandejas": []
            }
        if r.bandeja_id is not None:
            estruturas_dict[eid]["bandejas"].append({
                "bandeja_id": r.bandeja_id,
                "numero_bandeja": r.numero_bandeja,
                "largura_cm": float(r.largura_cm),
                "profundidade_cm": float(r.profundidade_cm),
                "altura_cm": float(r.altura_cm),
                "ordem": r.ordem,
                "ativa": r.ativa
            })

    return list(estruturas_dict.values())


def sincronizar_metadados_itens_parquet(engine, campanha_id: Optional[str] = None) -> int:
    """
    Sincroniza departamento, comprador, fornecedor e família dos itens da campanha a partir do query.parquet.
    """
    parquet_path = "bdados/query.parquet"
    import os
    if not os.path.exists(parquet_path):
        return 0

    where_cid = "WHERE (ci.fornecedor IS NULL OR ci.fornecedor IN ('GERAL', 'N/D', '', 'FORNECEDOR DIVERSOS') OR ci.fornecedor = ci.comprador OR ci.departamento IS NULL OR ci.departamento IN ('N/D', '') OR ci.codigo_familia IS NULL)"
    params = {}
    if campanha_id:
        where_cid += " AND ci.campanha_id = :cid"
        params["cid"] = campanha_id

    try:
        with engine.connect() as conn:
            itens_pend = conn.execute(text(f"""
                SELECT id, produto_codigo, fornecedor, departamento, comprador, codigo_familia, descricao_familia 
                FROM campanha_itens ci
                {where_cid}
            """), params).fetchall()

        if not itens_pend:
            return 0

        df = pd.read_parquet(parquet_path)
        df.columns = [str(c).strip() for c in df.columns]
        df_uniq = df.drop_duplicates(subset=["CODIGO_PRODUTO"])
        map_prod = {int(r["CODIGO_PRODUTO"]): r for _, r in df_uniq.iterrows()}

        atualizados = 0
        with engine.begin() as conn:
            for it in itens_pend:
                pcod = int(it.produto_codigo)
                if pcod in map_prod:
                    row = map_prod[pcod]
                    depto = str(row.get("DEPARTAMENTO", "")).strip() or "GERAL"
                    comp = str(row.get("COMPRADOR", "")).strip() or ""
                    
                    cod_fam = row.get("CODIGO_FAMILIA")
                    desc_fam = row.get("DESCRICAO_FAMILIA")
                    cod_fam_val = int(cod_fam) if pd.notna(cod_fam) and str(cod_fam).isdigit() and int(cod_fam) > 0 else None
                    desc_fam_val = str(desc_fam).strip() if pd.notna(desc_fam) else None

                    cod_forn = row.get("COD_FORNECEDOR")
                    nome_forn = row.get("FORNECEDOR") or row.get("RAZAO_FORN_PRINCIPAL")

                    if pd.notna(nome_forn) and str(nome_forn).strip() and str(nome_forn).strip().upper() not in ["SEM FORNECEDOR PRINCIPAL", "GERAL", "N/D", "NONE", "NAN"]:
                        if pd.notna(cod_forn) and str(cod_forn).strip() and str(cod_forn) not in ["0", "None", "nan"]:
                            try:
                                forn = f"{int(float(cod_forn))} - {str(nome_forn).strip()}"
                            except Exception:
                                forn = f"{str(cod_forn).strip()} - {str(nome_forn).strip()}"
                        else:
                            forn = str(nome_forn).strip()
                    else:
                        forn = "SEM FORNECEDOR PRINCIPAL"

                    conn.execute(text("""
                        UPDATE campanha_itens
                        SET fornecedor = :forn,
                            departamento = :dep,
                            comprador = :comp,
                            codigo_familia = COALESCE(:cod_fam, codigo_familia),
                            descricao_familia = COALESCE(:desc_fam, descricao_familia)
                        WHERE id = :id
                    """), {
                        "forn": forn,
                        "dep": depto,
                        "comp": comp,
                        "cod_fam": cod_fam_val,
                        "desc_fam": desc_fam_val,
                        "id": it.id
                    })
                    atualizados += 1

        return atualizados
    except Exception as e:
        logger.error(f"Erro ao sincronizar metadados dos itens com parquet: {e}")
        return 0


def obter_itens_campanha_com_detalhes(engine, campanha_id: str) -> List[Dict[str, Any]]:
    """
    Carrega todos os itens da campanha e sua matriz de distribuição por loja.
    """
    sincronizar_metadados_itens_parquet(engine, campanha_id)

    sql_itens = text("""
        SELECT 
            ci.id as item_id, ci.produto_codigo, ci.descricao_snapshot,
            COALESCE(ci.fornecedor, 'SEM FORNECEDOR') AS fornecedor,
            COALESCE(ci.departamento, 'GERAL') AS departamento,
            COALESCE(ci.comprador, '') AS comprador,
            ci.codigo_familia, ci.descricao_familia, COALESCE(ci.total_skus_familia, 1) AS total_skus_familia,
            ci.embalagem_compra, ci.embalagem_transferencia,
            pd.altura_cm, pd.largura_cm, pd.profundidade_cm
        FROM campanha_itens ci
        LEFT JOIN produto_dimensoes pd ON pd.produto_codigo = ci.produto_codigo
        WHERE ci.campanha_id = :cid
        ORDER BY COALESCE(ci.fornecedor, 'ZZZ'), ci.produto_codigo
    """)

    sql_lojas = text("""
        SELECT 
            cl.id as loja_rel_id, cl.campanha_item_id, cl.loja_codigo,
            l.nome as loja_nome,
            cl.tipo_exposicao_id, te.nome as tipo_exposicao_nome,
            cl.estrutura_exposicao_id, ee.nome as estrutura_nome,
            cl.volume_comprador, cl.volume_calculado, cl.volume_final_supply,
            cl.caixas_transferencia, cl.volume_transferencia,
            cl.estoque_loja, cl.estoque_cd, cl.venda_media, cl.venda_projetada,
            cl.status_estoque, cl.caixas_falta, cl.quantidade_falta
        FROM campanha_lojas cl
        JOIN lojas l ON l.codigo = cl.loja_codigo
        LEFT JOIN tipos_exposicao te ON te.id = cl.tipo_exposicao_id
        LEFT JOIN estruturas_exposicao ee ON ee.id = cl.estrutura_exposicao_id
        JOIN campanha_itens ci ON ci.id = cl.campanha_item_id
        WHERE ci.campanha_id = :cid
        ORDER BY cl.loja_codigo
    """)

    with engine.connect() as conn:
        itens_rows = conn.execute(sql_itens, {"cid": campanha_id}).fetchall()
        lojas_rows = conn.execute(sql_lojas, {"cid": campanha_id}).fetchall()

    lojas_por_item: Dict[int, List[Dict[str, Any]]] = {}
    for lr in lojas_rows:
        iid = lr.campanha_item_id
        if iid not in lojas_por_item:
            lojas_por_item[iid] = []
        lojas_por_item[iid].append(dict(lr._mapping))

    resultado = []
    for it in itens_rows:
        item_dict = dict(it._mapping)
        item_dict["lojas"] = lojas_por_item.get(it.item_id, [])
        
        # Filtra lojas ativas (diferentes de 'INATIVA')
        lojas_ativas = [l for l in item_dict["lojas"] if str(l.get("tipo_exposicao_nome") or "").upper() != "INATIVA"]
        item_dict["lojas_ativas"] = lojas_ativas
        item_dict["total_lojas_ativas"] = len(lojas_ativas)

        if len(lojas_ativas) == 0:
            item_dict["status_supply"] = "INATIVO"
        elif all(int(l.get("volume_final_supply") or 0) > 0 for l in lojas_ativas):
            item_dict["status_supply"] = "AVALIADO"
        else:
            item_dict["status_supply"] = "PENDENTE"
        resultado.append(item_dict)

    return resultado


def salvar_fechamento_supply(
    engine,
    campanha_id: str,
    item_id: int,
    estrutura_exposicao_id: Optional[int],
    volume_calculado: int,
    fechamento_lojas: List[Dict[str, Any]],
    estoque_cd15_total: float,
    usuario: str
) -> Tuple[bool, str]:
    """
    Grava as decisões finais do Supply para o item em todas as lojas,
    calcula as caixas, confronta com o estoque do CD15 e gera/atualiza devolutivas.
    """
    try:
        with engine.begin() as conn:
            # Busca dados do item
            item = conn.execute(text("""
                SELECT produto_codigo, descricao_snapshot, embalagem_compra, embalagem_transferencia
                FROM campanha_itens
                WHERE id = :iid
            """), {"iid": item_id}).fetchone()

            if not item:
                return False, "Item da campanha não encontrado."

            emb_transf = max(1, int(item.embalagem_transferencia or 1))
            total_caixas_necessarias = 0

            # 1. Atualiza matriz de lojas
            for fl in fechamento_lojas:
                lj = str(fl["loja_codigo"]).zfill(3)
                if "caixas_transferencia" in fl and fl["caixas_transferencia"] is not None:
                    cx_transf = max(0, int(fl["caixas_transferencia"]))
                    vol_transf = cx_transf * emb_transf
                    vol_final = vol_transf
                else:
                    vol_final = max(0, int(fl.get("volume_final_supply", 0) or 0))
                    cx_transf, vol_transf = calcular_caixas_transferencia(vol_final, emb_transf)
                
                total_caixas_necessarias += cx_transf

                conn.execute(text("""
                    UPDATE campanha_lojas
                    SET estrutura_exposicao_id = :est_id,
                        volume_calculado = :vol_calc,
                        volume_final_supply = :vol_final,
                        caixas_transferencia = :cx_transf,
                        volume_transferencia = :vol_transf,
                        atualizado_em = NOW()
                    WHERE campanha_item_id = :iid AND loja_codigo = :loja
                """), {
                    "est_id": estrutura_exposicao_id,
                    "vol_calc": volume_calculado,
                    "vol_final": vol_final,
                    "cx_transf": cx_transf,
                    "vol_transf": vol_transf,
                    "iid": item_id,
                    "loja": lj
                })

            # 2. Apuração do CD15
            estoque_cd_cx = int(float(estoque_cd15_total or 0.0) // emb_transf)
            cx_atendidas, cx_falta, status_est = apurar_disponibilidade_cd(total_caixas_necessarias, estoque_cd_cx)

            # Atualiza status_estoque nas lojas deste item
            conn.execute(text("""
                UPDATE campanha_lojas
                SET status_estoque = :status_est,
                    caixas_falta = :cx_falta,
                    quantidade_falta = :qtd_falta
                WHERE campanha_item_id = :iid
            """), {
                "status_est": status_est,
                "cx_falta": cx_falta,
                "qtd_falta": cx_falta * emb_transf,
                "iid": item_id
            })

            # 3. Gerenciar tabela campanha_devolutivas
            if cx_falta > 0:
                conn.execute(text("""
                    INSERT INTO campanha_devolutivas (
                        campanha_id, produto_codigo, descricao_snapshot, 
                        caixas_necessarias, caixas_cd_disponivel, caixas_falta, situacao, observacao
                    )
                    VALUES (:cid, :pcod, :desc, :cx_nec, :cx_disp, :cx_falta, 'PENDENTE', :obs)
                    ON CONFLICT (id) DO NOTHING;
                """), {
                    "cid": campanha_id,
                    "pcod": item.produto_codigo,
                    "desc": item.descricao_snapshot,
                    "cx_nec": total_caixas_necessarias,
                    "cx_disp": estoque_cd_cx,
                    "cx_falta": cx_falta,
                    "obs": f"Falta de {cx_falta} cx no CD15 para atender a demanda total de {total_caixas_necessarias} cx."
                })
            else:
                # Remove pendências resolvidas caso já existissem
                conn.execute(text("""
                    DELETE FROM campanha_devolutivas
                    WHERE campanha_id = :cid AND produto_codigo = :pcod
                """), {"cid": campanha_id, "pcod": item.produto_codigo})

            # Atualiza status da campanha para EM_AVALIACAO_SUPPLY
            conn.execute(text("""
                UPDATE campanhas
                SET status = 'EM_AVALIACAO_SUPPLY',
                    usuario_atualizacao = :user,
                    data_atualizacao = NOW()
                WHERE id = :cid AND status = 'ENVIADA_SUPPLY'
            """), {"cid": campanha_id, "user": usuario})

            registrar_historico(
                engine,
                campanha_id=campanha_id,
                usuario=usuario,
                acao="AVALIACAO_SUPPLY",
                detalhes=f"Avaliação Supply concluída para produto {item.produto_codigo}. Caixas: {total_caixas_necessarias}, Status CD: {status_est}",
                conn=conn
            )

        return True, f"Avaliação do produto {item.produto_codigo} salva com sucesso!"
    except Exception as e:
        logger.error(f"Erro ao salvar fechamento do supply: {e}")
        return False, f"Erro ao salvar avaliação: {e}"


def finalizar_avaliacao_campanha(engine, campanha_id: str, usuario: str) -> Tuple[bool, str]:
    """
    Finaliza formalmente a campanha pelo Supply, alterando seu status para FINALIZADA.
    """
    with engine.connect() as conn:
        # Verifica se há pendências de lojas ativas sem volume definido
        sem_vol = conn.execute(text("""
            SELECT COUNT(*) 
            FROM campanha_lojas cl
            JOIN campanha_itens ci ON ci.id = cl.campanha_item_id
            LEFT JOIN tipos_exposicao te ON te.id = cl.tipo_exposicao_id
            WHERE ci.campanha_id = :cid 
              AND UPPER(COALESCE(te.nome, '')) != 'INATIVA'
              AND (cl.volume_final_supply IS NULL OR cl.volume_final_supply < 0)
        """), {"cid": campanha_id}).scalar() or 0

        if sem_vol > 0:
            return False, f"Existem {sem_vol} registro(s) de lojas sem volume final definido."

        # Verifica se há pendências em aberto
        pendencias = conn.execute(text("""
            SELECT COUNT(*) 
            FROM campanha_devolutivas
            WHERE campanha_id = :cid AND situacao = 'PENDENTE'
        """), {"cid": campanha_id}).scalar() or 0

    status_destino = "PENDENCIA_COMPRAS" if pendencias > 0 else "FINALIZADA"

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE campanhas
                SET status = :st,
                    usuario_atualizacao = :user,
                    data_atualizacao = NOW()
                WHERE id = :cid
            """), {"cid": campanha_id, "st": status_destino, "user": usuario})

            registrar_historico(
                engine,
                campanha_id=campanha_id,
                usuario=usuario,
                acao="FINALIZACAO_SUPPLY",
                campo="status",
                valor_novo=status_destino,
                detalhes=f"Avaliação finalizada por {usuario}. Status final: {status_destino}",
                conn=conn
            )

        msg = "Campanha finalizada com sucesso! Pronta para visualização nas lojas." if status_destino == "FINALIZADA" else "Campanha avaliada, porém enviada para PENDÊNCIA DE COMPRAS devido a falta de estoque no CD15."
        return True, msg
    except Exception as e:
        return False, f"Erro ao finalizar campanha: {e}"
