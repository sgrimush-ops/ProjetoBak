"""
services/campanha_calculo.py
Motor de Cálculo, Cubagem e Regras Matemáticas do Módulo de Campanhas.
Isolado para garantir 100% de reuso e integridade entre Compras, Supply e Lojas.
"""

from __future__ import annotations
import math
from datetime import date, datetime
from typing import Dict, List, Tuple, Union


def calcular_dias_campanha(data_inicio: Union[date, datetime, str], data_fim: Union[date, datetime, str]) -> int:
    """
    Calcula o número inclusivo de dias de uma campanha.
    Exemplo: 01/10 a 15/10 -> (15 - 1) + 1 = 15 dias.
    """
    if isinstance(data_inicio, str):
        data_inicio = datetime.strptime(data_inicio[:10], "%Y-%m-%d").date()
    elif isinstance(data_inicio, datetime):
        data_inicio = data_inicio.date()

    if isinstance(data_fim, str):
        data_fim = datetime.strptime(data_fim[:10], "%Y-%m-%d").date()
    elif isinstance(data_fim, datetime):
        data_fim = data_fim.date()

    if data_fim < data_inicio:
        return 0

    return (data_fim - data_inicio).days + 1


def calcular_venda_media_diaria(qtd_vendida_periodo: float, dias_pesquisa: float = 30.0) -> float:
    """
    Calcula a venda média diária com base na venda acumulada e período de pesquisa.
    """
    qtd = max(0.0, float(qtd_vendida_periodo or 0.0))
    dias = max(1.0, float(dias_pesquisa or 30.0))
    return round(qtd / dias, 4)


def calcular_venda_projetada(venda_media_diaria: float, dias_campanha: int) -> float:
    """
    Calcula a venda projetada no período da campanha: Venda Média * Dias.
    """
    venda_media = max(0.0, float(venda_media_diaria or 0.0))
    dias = max(0, int(dias_campanha or 0))
    return round(venda_media * dias, 2)


def calcular_capacidade_bandeja(
    largura_b: float,
    profundidade_b: float,
    altura_b: float,
    largura_p: float,
    profundidade_p: float,
    altura_p: float
) -> int:
    """
    Calcula a capacidade física (unidades) de uma bandeja individual para um produto.
    Frentes = floor(Largura Bandeja / Largura Produto)
    Fila    = floor(Profundidade Bandeja / Profundidade Produto)
    Camadas = floor(Altura Bandeja / Altura Produto)
    Capacidade = Frentes * Fila * Camadas
    """
    if any(dim <= 0 for dim in [largura_b, profundidade_b, altura_b, largura_p, profundidade_p, altura_p]):
        return 0

    frentes = int(largura_b // largura_p)
    fila = int(profundidade_b // profundidade_p)
    camadas = int(altura_b // altura_p)

    return max(0, frentes * fila * camadas)


def calcular_capacidade_exposicao(
    bandejas: List[Dict[str, float]],
    produto_dimensoes: Dict[str, float],
    total_skus: int = 1
) -> int:
    """
    Calcula a capacidade total física de uma estrutura de exposição para um SKU específico,
    fazendo o rateio inteiro (floor) por bandeja quando múltiplos SKUs compartilham a mesma estrutura.
    """
    if not bandejas or not produto_dimensoes:
        return 0

    larg_p = float(produto_dimensoes.get("largura_cm", 0.0))
    prof_p = float(produto_dimensoes.get("profundidade_cm", 0.0))
    alt_p = float(produto_dimensoes.get("altura_cm", 0.0))

    if larg_p <= 0 or prof_p <= 0 or alt_p <= 0:
        return 0

    skus = max(1, int(total_skus or 1))
    capacidade_total_sku = 0

    for b in bandejas:
        if not b.get("ativa", True):
            continue
        larg_b = float(b.get("largura_cm", 0.0))
        prof_b = float(b.get("profundidade_cm", 0.0))
        alt_b = float(b.get("altura_cm", 0.0))

        cap_bandeja_total = calcular_capacidade_bandeja(
            largura_b=larg_b,
            profundidade_b=prof_b,
            altura_b=alt_b,
            largura_p=larg_p,
            profundidade_p=prof_p,
            altura_p=alt_p
        )

        cap_por_sku = cap_bandeja_total // skus
        capacidade_total_sku += cap_por_sku

    return capacidade_total_sku


def calcular_caixas_transferencia(volume_final: int, embl_transferencia: int = 1) -> Tuple[int, int]:
    """
    Converte o volume final de unidades em caixas fechadas de transferência e apura o volume efetivo.
    Retorna: (caixas_necessarias, volume_efetivo_unidades)
    Exemplo: 42 un com emb 24 -> 2 caixas (48 un).
    """
    emb = max(1, int(embl_transferencia or 1))
    vol = max(0, int(volume_final or 0))

    if vol == 0:
        return 0, 0

    caixas = math.ceil(vol / emb)
    volume_efetivo = caixas * emb
    return caixas, volume_efetivo


def apurar_disponibilidade_cd(caixas_necessarias: int, estoque_cd_cx: int) -> Tuple[int, int, str]:
    """
    Verifica a disponibilidade de estoque no CD (em caixas).
    Retorna: (caixas_atendidas, caixas_falta_comprar, status_estoque)
    Status: 'OK' | 'PARCIAL' | 'SEM_ESTOQUE_CD'
    """
    nec = max(0, int(caixas_necessarias or 0))
    disp = max(0, int(estoque_cd_cx or 0))

    if nec == 0:
        return 0, 0, "OK"

    if disp >= nec:
        return nec, 0, "OK"
    elif disp > 0:
        falta = nec - disp
        return disp, falta, "PARCIAL"
    else:
        return 0, nec, "SEM_ESTOQUE_CD"
