"""
scripts/test_familia_completo.py
Teste ponta a ponta da dinâmica de Família de Produtos (SEQFAMILIA) e rateio no ProjetoBak_Sincronizador.
"""

import os
import sys
from datetime import date, timedelta
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
    criar_campanha,
    carregar_dados_produto_consolidado,
    salvar_familia_campanha_compras,
    obter_itens_campanha_com_detalhes,
    salvar_fechamento_supply,
    obter_estruturas_e_bandejas,
    LISTA_14_LOJAS
)
from services.campanha_calculo import calcular_capacidade_exposicao
from services.exportacao_campanha import (
    gerar_excel_transferencia_operacional,
    gerar_excel_devolutiva_compras
)

def run_tests():
    print(">>> Iniciando Testes da Dinamica de Familia de Produtos...")
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL não encontrada.")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    engine = create_engine(db_url, connect_args={"sslmode": "require"})
    create_campanhas_tables(engine)

    with engine.connect() as conn:
        tipo_inativa_id = conn.execute(text("SELECT id FROM tipos_exposicao WHERE UPPER(nome) = 'INATIVA'")).scalar()
        tipo_ilha_id = conn.execute(text("SELECT id FROM tipos_exposicao WHERE UPPER(nome) = 'ILHA'")).scalar()

    assert tipo_inativa_id is not None, "Tipo INATIVA não encontrado!"
    assert tipo_ilha_id is not None, "Tipo ILHA não encontrado!"

    hoje = date.today()
    d_ini = hoje + timedelta(days=7)
    d_fim = hoje + timedelta(days=21)

    # 1. Cria Campanha de Teste
    suc, msg, camp = criar_campanha(
        engine=engine,
        nome="Campanha Teste Familia Tang e Frisco 2026",
        data_inicio=d_ini,
        data_fim=d_fim,
        usuario="teste_agente",
        observacoes="Validacao da dinamica de SEQFAMILIA e cubagem compartilhada"
    )
    assert suc and camp is not None, f"Falha ao criar campanha: {msg}"
    camp_id = camp["id"]
    print(f"[OK] Campanha criada: {camp['codigo_campanha']} (ID: {camp_id})")

    # 2. Testa carregar produto com familia do parquet (ex: produto com codigo_familia)
    import pandas as pd
    df_p = pd.read_parquet("bdados/query.parquet")
    df_fam_ex = df_p[df_p["CODIGO_FAMILIA"] == 26] # REFRE PO TANG 18G
    skus_tang = df_fam_ex.drop_duplicates(subset=["CODIGO_PRODUTO"])["CODIGO_PRODUTO"].tolist()
    print(f"SKUs encontrados para familia 26 (Tang): {skus_tang[:5]}")
    assert len(skus_tang) > 0, "Nenhum SKU encontrado para a familia 26!"

    prod_cod_tang = int(skus_tang[0])
    prod_data = carregar_dados_produto_consolidado(
        engine=engine,
        produto_codigo=prod_cod_tang,
        data_inicio=d_ini,
        data_fim=d_fim
    )

    print(f"[OK] Produto {prod_cod_tang} carregado:")
    print(f"     Descricao: {prod_data['descricao']}")
    print(f"     Codigo Familia: {prod_data['codigo_familia']}")
    print(f"     Descricao Familia: {prod_data['descricao_familia']}")
    print(f"     Fornecedor: {prod_data.get('fornecedor')}")
    print(f"     Total SKUs Familia encontrados: {len(prod_data.get('skus_familia', []))}")
    assert prod_data["codigo_familia"] == 26, "Codigo da familia incorreto!"
    assert len(prod_data.get("skus_familia", [])) >= 2, "Deveria ter encontrado multiplos SKUs irmaos!"

    # 3. Seleciona 4 sabores da familia para cadastrar na mesma Ilha
    sabores_escolhidos = prod_data["skus_familia"][:4]
    print(f"Sabores selecionados para cadastrar em lote: {[s['descricao'] for s in sabores_escolhidos]}")

    # Monta matriz de lojas com Lojas 001, 002, 003 ativas em ILHA e as outras INATIVA
    matriz_lojas = []
    for lj in LISTA_14_LOJAS:
        if lj in ["001", "002", "003"]:
            matriz_lojas.append({
                "loja_codigo": lj,
                "tipo_exposicao_id": tipo_ilha_id,
                "volume_comprador": 0
            })
        else:
            matriz_lojas.append({
                "loja_codigo": lj,
                "tipo_exposicao_id": tipo_inativa_id,
                "volume_comprador": 0
            })

    # 4. Salva a familia em lote
    suc_fam, msg_fam = salvar_familia_campanha_compras(
        engine=engine,
        campanha_id=camp_id,
        produtos_familia=sabores_escolhidos,
        dados_lojas=matriz_lojas,
        usuario="comprador_teste",
        data_inicio=d_ini,
        data_fim=d_fim
    )
    print(f"[OK] salvar_familia_campanha_compras: {suc_fam} -> {msg_fam}")
    assert suc_fam, f"Falha ao salvar familia: {msg_fam}"

    # 5. Valida itens gravados no banco
    itens_camp = obter_itens_campanha_com_detalhes(engine, camp_id)
    print(f"[OK] Total de itens retornados com detalhes: {len(itens_camp)}")
    assert len(itens_camp) == 4, f"Esperava 4 itens na campanha, obteve {len(itens_camp)}"

    for it in itens_camp:
        assert it["codigo_familia"] == 26, "Codigo de familia divergente no item!"
        assert it["total_skus_familia"] == 4, "Total de SKUs da familia divergente no item!"
        assert len(it["lojas_ativas"]) == 3, "Deveria ter exatamente 3 lojas ativas!"
        assert it["status_supply"] == "PENDENTE", "Status deveria ser PENDENTE antes da avaliacao do supply"

    # 6. Simula cubagem no Supply rateando pelos 4 SKUs da familia
    estruturas = obter_estruturas_e_bandejas(engine)
    assert len(estruturas) > 0, "Deveria haver estruturas cadastradas!"
    est_ilha = estruturas[0]
    
    dim_tang = {"altura_cm": 15.0, "largura_cm": 10.0, "profundidade_cm": 1.0}
    cap_total_estrutura = calcular_capacidade_exposicao(est_ilha["bandejas"], dim_tang, total_skus=1)
    cap_rateada_sku = calcular_capacidade_exposicao(est_ilha["bandejas"], dim_tang, total_skus=4)
    print(f"[OK] Capacidade Total da Estrutura: {cap_total_estrutura} un | Capacidade por SKU (Rateio 4): {cap_rateada_sku} un")
    assert cap_rateada_sku > 0, "Capacidade rateada deve ser maior que zero!"

    # 7. Simula fechamento do Supply para cada um dos 4 sabores
    for idx_s, it in enumerate(itens_camp):
        fechamento_lojas = []
        for lj in it["lojas_ativas"]:
            vol_final = cap_rateada_sku
            if idx_s == 0 and lj["loja_codigo"] == "001":
                vol_final = 0 # Supply zerou para Loja 001 por ter estoque de sobra deste sabor
            fechamento_lojas.append({
                "loja_codigo": lj["loja_codigo"],
                "volume_final_supply": vol_final
            })

        suc_sup, msg_sup = salvar_fechamento_supply(
            engine=engine,
            campanha_id=camp_id,
            item_id=it["item_id"],
            estrutura_exposicao_id=est_ilha["estrutura_id"],
            volume_calculado=cap_rateada_sku,
            fechamento_lojas=fechamento_lojas,
            estoque_cd15_total=100.0,
            usuario="supply_teste"
        )
        assert suc_sup, f"Falha ao salvar fechamento supply: {msg_sup}"

    print("[OK] Fechamento do Supply salvo para todos os 4 sabores da familia com sucesso!")

    # 8. Gera Exportacoes Excel
    bytes_transf = gerar_excel_transferencia_operacional(engine, camp_id)
    assert len(bytes_transf) > 1000, "Arquivo de transferencia Excel vazio ou invalido!"
    print(f"[OK] Excel de Transferencia Operacional gerado com {len(bytes_transf)} bytes")

    bytes_dev = gerar_excel_devolutiva_compras(engine, camp_id)
    assert len(bytes_dev) > 1000, "Arquivo de Devolutiva Excel vazio ou invalido!"
    print(f"[OK] Excel de Devolutiva de Compras gerado com {len(bytes_dev)} bytes")

    print("\n=======================================================")
    print(">>> TODOS OS TESTES DE FAMILIA DE PRODUTOS PASSARAM! <<<")
    print("=======================================================")

if __name__ == "__main__":
    run_tests()
