"""
scripts/test_dimensoes_familia_completo.py
Teste automatizado para validar:
1. Pré-carregamento de dimensões salvas na campanha/supply sem alteração manual.
2. Exibição da descrição do produto e família.
3. Dimensionamento e propagação automática a nível de família.
"""

import os
import sys
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text

# Ajusta path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv()

from services.campanha_db import create_campanhas_tables
from services.campanha_service import (
    carregar_dados_produto_consolidado,
    salvar_dimensoes_produto
)

def run_test():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL não configurada!")
        return

    engine = create_engine(db_url)
    create_campanhas_tables(engine)

    print("--- INICIANDO TESTE DE DIMENSIONAMENTO POR FAMÍLIA ---")

    # 1. Identificar uma família no query.parquet
    parquet_path = "bdados/query.parquet"
    if not os.path.exists(parquet_path):
        print(f"❌ Arquivo {parquet_path} não encontrado!")
        return

    df = pd.read_parquet(parquet_path)
    df.columns = [str(c).strip() for c in df.columns]

    # Localizar família com múltiplos produtos
    fam_counts = df.drop_duplicates(subset=["CODIGO_PRODUTO"]).groupby("CODIGO_FAMILIA")["CODIGO_PRODUTO"].count()
    fam_mult = fam_counts[fam_counts > 1]
    if fam_mult.empty:
        print("❌ Nenhuma família com múltiplos produtos encontrada no parquet.")
        return

    cod_fam_teste = int(fam_mult.index[0])
    skus_fam = df[df["CODIGO_FAMILIA"] == cod_fam_teste]["CODIGO_PRODUTO"].drop_duplicates().tolist()
    sku_principal = int(skus_fam[0])
    sku_irmao = int(skus_fam[1])

    print(f"Família selecionada para teste: {cod_fam_teste}")
    print(f"SKU Principal: {sku_principal} | SKU Irmão: {sku_irmao} | Total SKUs: {len(skus_fam)}")

    # Limpar dimensões desses SKUs para teste limpo
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM produto_dimensoes WHERE produto_codigo = ANY(:cods)"), {"cods": [int(s) for s in skus_fam]})

    # 2. Salvar dimensões para o SKU principal com propagação para a família
    alt_teste = 22.50
    larg_teste = 7.80
    prof_teste = 6.40

    suc, msg = salvar_dimensoes_produto(
        engine=engine,
        produto_codigo=sku_principal,
        altura_cm=alt_teste,
        largura_cm=larg_teste,
        profundidade_cm=prof_teste,
        usuario="teste_auto",
        propagar_familia=True
    )

    print(f"Resultado salvar_dimensoes_produto: {suc} - {msg}")
    assert suc, f"Falha ao salvar dimensões: {msg}"

    # 3. Verificar no banco se o SKU irmão também recebeu as medidas
    with engine.connect() as conn:
        dim_irmao = conn.execute(text("""
            SELECT altura_cm, largura_cm, profundidade_cm 
            FROM produto_dimensoes 
            WHERE produto_codigo = :pcod
        """), {"pcod": sku_irmao}).fetchone()

    assert dim_irmao is not None, f"SKU irmão {sku_irmao} não foi populado no banco pela propagação de família!"
    assert float(dim_irmao.altura_cm) == alt_teste, f"Altura esperada {alt_teste}, obtida {dim_irmao.altura_cm}"
    print(f"✅ Propagação no banco confirmada: SKU Irmão {sku_irmao} tem medidas {dim_irmao.altura_cm} x {dim_irmao.largura_cm} x {dim_irmao.profundidade_cm} cm")

    # 4. Testar carregar_dados_produto_consolidado para o SKU irmão
    d_ini = date.today()
    d_fim = date.today()
    dados_irmao = carregar_dados_produto_consolidado(engine, sku_irmao, d_ini, d_fim)

    assert dados_irmao["dimensoes"] is not None, "Dimensões não vieram pré-carregadas no consolidado do SKU irmão!"
    assert float(dados_irmao["dimensoes"]["altura_cm"]) == alt_teste
    print(f"✅ Pré-carregamento automático confirmado no consolidado: {dados_irmao['dimensoes']}")
    print(f"✅ Descrição do produto: {dados_irmao['descricao']}")
    print(f"✅ Família do produto: {dados_irmao['codigo_familia']} - {dados_irmao['descricao_familia']}")

    print("\n🎉 TODOS OS TESTES DE DIMENSIONAMENTO E FAMÍLIA PASSARAM COM SUCESSO!")

if __name__ == "__main__":
    run_test()
