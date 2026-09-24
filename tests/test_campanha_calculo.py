"""
tests/test_campanha_calculo.py
Testes unitários para o motor de cálculo e cubagem de campanhas.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import date
from services.campanha_calculo import (
    calcular_dias_campanha,
    calcular_venda_media_diaria,
    calcular_venda_projetada,
    calcular_capacidade_bandeja,
    calcular_capacidade_exposicao,
    calcular_caixas_transferencia,
    apurar_disponibilidade_cd,
)


def test_dias_campanha():
    # 01/10/2026 a 15/10/2026 -> 15 dias inclusivos
    assert calcular_dias_campanha(date(2026, 10, 1), date(2026, 10, 15)) == 15
    assert calcular_dias_campanha("2026-10-01", "2026-10-15") == 15
    assert calcular_dias_campanha("2026-10-05", "2026-10-05") == 1
    assert calcular_dias_campanha("2026-10-15", "2026-10-01") == 0


def test_venda_projetada():
    # 156 unidades em 30 dias -> 5.2 un/dia * 15 dias = 78.0 unidades
    venda_media = calcular_venda_media_diaria(156.0, 30.0)
    assert round(venda_media, 1) == 5.2
    venda_proj = calcular_venda_projetada(venda_media, 15)
    assert venda_proj == 78.0


def test_capacidade_bandeja_e_exposicao():
    # Exemplo do briefing:
    # Produto: Nescau 350g (10 x 10 x 10 cm)
    # Bandeja: 150 x 50 x 20 cm
    # Frentes: 15, Fila: 5, Camadas: 2 -> Total = 150 unidades
    cap = calcular_capacidade_bandeja(150, 50, 20, 10, 10, 10)
    assert cap == 150

    # Com 3 SKUs na mesma bandeja -> 50 unidades por SKU
    bandejas = [{"largura_cm": 150, "profundidade_cm": 50, "altura_cm": 20, "ativa": True}]
    prod_dim = {"largura_cm": 10, "profundidade_cm": 10, "altura_cm": 10}
    cap_sku = calcular_capacidade_exposicao(bandejas, prod_dim, total_skus=3)
    assert cap_sku == 50

    # Estrutura com 6 bandejas iguais -> 6 * 50 = 300 unidades por SKU
    bandejas_6 = [{"largura_cm": 150, "profundidade_cm": 50, "altura_cm": 20, "ativa": True} for _ in range(6)]
    cap_sku_6 = calcular_capacidade_exposicao(bandejas_6, prod_dim, total_skus=3)
    assert cap_sku_6 == 300


def test_caixas_transferencia():
    # Volume final = 42 un, embalagem = 24 un -> 2 cx (volume efetivo 48 un)
    cx, vol_efetivo = calcular_caixas_transferencia(42, 24)
    assert cx == 2
    assert vol_efetivo == 48

    # Volume 0 -> 0 cx, 0 vol
    assert calcular_caixas_transferencia(0, 24) == (0, 0)

    # Volume 24 com emb 24 -> 1 cx, 24 un
    assert calcular_caixas_transferencia(24, 24) == (1, 24)


def test_apurar_disponibilidade_cd():
    # Cenário A: Estoque Pleno (Disp 10 >= Nec 5) -> 5 atendidas, 0 falta, 'OK'
    atendidas, falta, status = apurar_disponibilidade_cd(5, 10)
    assert atendidas == 5 and falta == 0 and status == "OK"

    # Cenário B: Estoque Parcial (Disp 3 < Nec 5) -> 3 atendidas, 2 falta, 'PARCIAL'
    atendidas, falta, status = apurar_disponibilidade_cd(5, 3)
    assert atendidas == 3 and falta == 2 and status == "PARCIAL"

    # Cenário C: Estoque Zerado (Disp 0 < Nec 5) -> 0 atendidas, 5 falta, 'SEM_ESTOQUE_CD'
    atendidas, falta, status = apurar_disponibilidade_cd(5, 0)
    assert atendidas == 0 and falta == 5 and status == "SEM_ESTOQUE_CD"


if __name__ == "__main__":
    test_dias_campanha()
    test_venda_projetada()
    test_capacidade_bandeja_e_exposicao()
    test_caixas_transferencia()
    test_apurar_disponibilidade_cd()
    print("Todos os testes unitários do motor de cálculo passaram com sucesso!")
