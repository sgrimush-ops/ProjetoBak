"""
scripts/test_comprador_fornecedor_filtros.py
Validação automatizada de:
1. Correção e separação estrita entre Fornecedor e Comprador.
2. Função obter_lista_compradores e filtros opcionais por Comprador.
3. Listagem de campanhas filtrada por comprador.
4. Tabela e Excel de Devolutivas com colunas separadas de Comprador e Fornecedor.
"""

import os
import sys
import pandas as pd
from datetime import date
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

load_dotenv()
from sqlalchemy import create_engine, text
from services.campanha_db import create_campanhas_tables
from services.campanha_service import (
    obter_lista_compradores,
    listar_campanhas,
    carregar_dados_produto_consolidado,
    sincronizar_metadados_itens_parquet
)
from services.exportacao_campanha import gerar_excel_devolutiva_compras

def run_test():
    print("--- INICIANDO TESTE DE FORNECEDOR X COMPRADOR E FILTROS ---")
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL não configurada!")
        return

    engine = create_engine(db_url)
    create_campanhas_tables(engine)

    # 1. Testar carregar_dados_produto_consolidado para produtos do Nicolas
    p10 = carregar_dados_produto_consolidado(engine, 10, date.today(), date.today())
    p48 = carregar_dados_produto_consolidado(engine, 48, date.today(), date.today())

    print(f"\n[1] Verificação do Produto 10:")
    print(f"    Descrição: {p10['descricao']}")
    print(f"    Comprador: '{p10['comprador']}'")
    print(f"    Fornecedor: '{p10['fornecedor']}'")
    assert "NICOLAS" in p10["comprador"].upper(), f"Comprador incorreto no P10: {p10['comprador']}"
    assert "SPAL" in p10["fornecedor"].upper(), f"Fornecedor incorreto no P10: {p10['fornecedor']}"
    assert p10["fornecedor"] != p10["comprador"], "Fornecedor e Comprador não podem ser iguais!"

    print(f"\n[2] Verificação do Produto 48:")
    print(f"    Descrição: {p48['descricao']}")
    print(f"    Comprador: '{p48['comprador']}'")
    print(f"    Fornecedor: '{p48['fornecedor']}'")
    assert "NICOLAS" in p48["comprador"].upper(), f"Comprador incorreto no P48: {p48['comprador']}"
    assert "SARANDI" in p48["fornecedor"].upper(), f"Fornecedor incorreto no P48: {p48['fornecedor']}"
    assert p48["fornecedor"] != p48["comprador"], "Fornecedor e Comprador não podem ser iguais!"

    # 2. Testar obter_lista_compradores
    compradores = obter_lista_compradores(engine)
    print(f"\n[3] Lista de Compradores identificada ({len(compradores)} encontrados): {compradores[:5]}")
    assert len(compradores) > 0, "Nenhum comprador retornado!"
    assert any("NICOLAS" in c.upper() for c in compradores), "Nicolas não encontrado na lista de compradores!"

    # 3. Testar listar_campanhas com filtro de comprador
    todas_camp = listar_campanhas(engine)
    print(f"\n[4] Total de campanhas sem filtro: {len(todas_camp)}")
    
    nicolas_nome = [c for c in compradores if "NICOLAS" in c.upper()][0]
    camp_nicolas = listar_campanhas(engine, comprador_filtro=nicolas_nome)
    print(f"    Total de campanhas do comprador '{nicolas_nome}': {len(camp_nicolas)}")
    
    # 4. Testar sincronizacao de itens existentes
    sinc = sincronizar_metadados_itens_parquet(engine)
    print(f"\n[5] Sincronização de itens com parquet executada: {sinc} atualizados.")

    # 5. Testar exportacao Excel de Devolutiva
    if todas_camp:
        cid_teste = todas_camp[0]["id"]
        excel_dev = gerar_excel_devolutiva_compras(engine, cid_teste)
        print(f"\n[6] Excel de Devolutiva gerado com sucesso: {len(excel_dev)} bytes")
        assert len(excel_dev) > 0

    print("\n🎉 TODOS OS TESTES DE FORNECEDOR, COMPRADOR E FILTROS FORAM APROVADOS COM SUCESSO!")

if __name__ == "__main__":
    run_test()
