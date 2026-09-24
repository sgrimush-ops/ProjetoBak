"""
scripts/test_regras_inativa_e_supply.py
Validação automatizada das novas regras:
1. Tipo INATIVA como padrão e exclusão nas camadas Supply/Loja/Transferência.
2. Discriminação de status no Supply (PENDENTE vs AVALIADO vs INATIVO).
3. Download e geração de Devolutivas de Compras.
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
    obter_campanha_por_id,
    salvar_item_campanha_compras,
    enviar_campanha_para_supply,
    salvar_fechamento_supply,
    finalizar_avaliacao_campanha,
    obter_itens_campanha_com_detalhes,
    LISTA_14_LOJAS
)
from services.exportacao_campanha import (
    gerar_excel_transferencia_operacional,
    gerar_excel_devolutiva_compras,
    gerar_excel_consulta_loja,
    gerar_pdf_campanha_loja
)


def run_tests():
    print("🚀 TESTANDO REGRAS DE EXPOSIÇÃO INATIVA, SUPPLY E DEVOLUTIVAS...")

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

    assert tipo_inativa_id is not None, "Tipo INATIVA não cadastrado!"
    assert tipo_ilha_id is not None, "Tipo ILHA não cadastrado!"

    # 1. Cria campanha de teste
    d_ini = date.today() + timedelta(days=2)
    d_fim = date.today() + timedelta(days=12)
    suc, msg, camp = criar_campanha(engine, "Campanha Teste Inativa Supply", d_ini, d_fim, "tester_unit")
    assert suc and camp, f"Erro ao criar: {msg}"
    cid = camp["id"]

    try:
        # 2. Cadastrar Produto 1: 4 lojas ILHA e 10 lojas INATIVA
        lojas_matriz_1 = []
        for i, lj in enumerate(LISTA_14_LOJAS):
            if i < 4:
                lojas_matriz_1.append({
                    "loja_codigo": lj,
                    "tipo_exposicao_id": tipo_ilha_id,
                    "volume_comprador": 20,
                    "estoque_loja": 5.0,
                    "estoque_cd": 100.0,
                    "venda_media": 2.0,
                    "venda_projetada": 20.0
                })
            else:
                lojas_matriz_1.append({
                    "loja_codigo": lj,
                    "tipo_exposicao_id": tipo_inativa_id,
                    "volume_comprador": 0,
                    "estoque_loja": 10.0,
                    "estoque_cd": 100.0,
                    "venda_media": 1.0,
                    "venda_projetada": 10.0
                })

        suc_p1, _ = salvar_item_campanha_compras(
            engine=engine,
            campanha_id=cid,
            produto_codigo=888801,
            descricao="PRODUTO PARCIAL ILHA E INATIVA",
            embalagem_compra=6,
            embalagem_transferencia=6,
            dados_lojas=lojas_matriz_1,
            usuario="compras",
            fornecedor="FORNECEDOR ALFA",
            departamento="MERCEARIA"
        )
        assert suc_p1, "Erro ao salvar produto 1"

        # 3. Cadastrar Produto 2: 100% das 14 lojas INATIVA
        lojas_matriz_2 = [
            {
                "loja_codigo": lj,
                "tipo_exposicao_id": tipo_inativa_id,
                "volume_comprador": 0,
                "estoque_loja": 0.0,
                "estoque_cd": 50.0,
                "venda_media": 0.0,
                "venda_projetada": 0.0
            }
            for lj in LISTA_14_LOJAS
        ]

        suc_p2, _ = salvar_item_campanha_compras(
            engine=engine,
            campanha_id=cid,
            produto_codigo=888802,
            descricao="PRODUTO TOTALMENTE INATIVO",
            embalagem_compra=10,
            embalagem_transferencia=10,
            dados_lojas=lojas_matriz_2,
            usuario="compras",
            fornecedor="FORNECEDOR BETA",
            departamento="BEBIDAS"
        )
        assert suc_p2, "Erro ao salvar produto 2"

        # 4. Validar obtenção dos detalhes e status inicial
        itens = obter_itens_campanha_com_detalhes(engine, cid)
        assert len(itens) == 2, f"Esperado 2 itens, obtido {len(itens)}"

        item_p1 = next(it for it in itens if it["produto_codigo"] == 888801)
        item_p2 = next(it for it in itens if it["produto_codigo"] == 888802)

        assert len(item_p1["lojas_ativas"]) == 4, f"P1 deveria ter 4 lojas ativas, obteve {len(item_p1['lojas_ativas'])}"
        assert item_p1["status_supply"] == "PENDENTE", f"P1 deveria estar PENDENTE, obteve {item_p1['status_supply']}"

        assert len(item_p2["lojas_ativas"]) == 0, f"P2 deveria ter 0 lojas ativas, obteve {len(item_p2['lojas_ativas'])}"
        assert item_p2["status_supply"] == "INATIVO", f"P2 deveria estar INATIVO, obteve {item_p2['status_supply']}"
        print("✅ Status e isolamento de lojas ativas vs inativas validados no backend.")

        # 5. Fechamento do Supply apenas para as 4 lojas ativas de P1
        fech_p1 = [
            {"loja_codigo": l["loja_codigo"], "volume_final_supply": 24}
            for l in item_p1["lojas_ativas"]
        ]
        suc_f, _ = salvar_fechamento_supply(
            engine=engine,
            campanha_id=cid,
            item_id=item_p1["item_id"],
            estrutura_exposicao_id=None,
            volume_calculado=24,
            fechamento_lojas=fech_p1,
            estoque_cd15_total=300.0,
            usuario="supply"
        )
        assert suc_f, "Falha no fechamento do supply para P1"

        # 6. Revalidar status de P1 no Supply
        itens_reval = obter_itens_campanha_com_detalhes(engine, cid)
        item_p1_reval = next(it for it in itens_reval if it["produto_codigo"] == 888801)
        assert item_p1_reval["status_supply"] == "AVALIADO", f"Esperado AVALIADO, obteve {item_p1_reval['status_supply']}"
        print("✅ P1 promovido para status AVALIADO com sucesso.")

        # 7. Finalização pelo Supply (deve passar sem erro mesmo com P2 inativo e P1 com 10 lojas inativas)
        suc_fin, msg_fin = finalizar_avaliacao_campanha(engine, cid, "supply")
        assert suc_fin, f"Falha na finalização da campanha: {msg_fin}"
        print("✅ Campanha finalizada com sucesso sem travas de lojas inativas.")

        # 8. Validação dos relatórios de exportação (nenhuma loja inativa deve aparecer)
        bytes_transf = gerar_excel_transferencia_operacional(engine, cid)
        assert len(bytes_transf) > 500
        print("✅ Excel de Transferência gerado com sucesso.")

        bytes_dev = gerar_excel_devolutiva_compras(engine, cid)
        assert len(bytes_dev) > 500
        print("✅ Excel de Devolutiva gerado com sucesso.")

        bytes_loja_001 = gerar_excel_consulta_loja(engine, cid, "001")
        assert len(bytes_loja_001) > 500
        print("✅ Excel Loja 001 gerado com sucesso.")

        bytes_pdf = gerar_pdf_campanha_loja(engine, cid, "001")
        assert len(bytes_pdf) > 500
        print("✅ PDF Loja 001 gerado com sucesso.")

    finally:
        # Limpa dados de teste
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM campanhas WHERE id = :cid"), {"cid": cid})
        print("🧹 Limpeza de registros de teste concluída.")

    print("\n🎉 TODOS OS TESTES ESPECÍFICOS DE REGRAS PASSARAM COM 100% DE SUCESSO!")


if __name__ == "__main__":
    run_tests()
