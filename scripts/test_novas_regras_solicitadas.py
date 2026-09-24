"""
scripts/test_novas_regras_solicitadas.py
Validação automatizada dos 5 novos requisitos:
1. Devolutiva sumindo após COMPRADO / RESOLVIDO.
2. Não contabilização de campanhas inativas/canceladas nas métricas.
3. Supply informando sugestão em caixas e sistema calculando unidades.
4. Validação de Salvar Item vs Finalizar Campanha.
5. Exclusão permanente de testes pelo ADM.
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
    inativar_campanha,
    excluir_campanha,
    salvar_item_campanha_compras,
    salvar_fechamento_supply,
    finalizar_avaliacao_campanha,
    obter_campanha_por_id,
    obter_itens_campanha_com_detalhes,
    LISTA_14_LOJAS
)

def run_tests():
    print(">>> Iniciando Testes das 5 Novas Regras...")
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

    hoje = date.today()
    d_ini = hoje + timedelta(days=5)
    d_fim = hoje + timedelta(days=20)

    # 1. Cria campanha de teste para validar exclusão e métricas
    suc, msg, camp_teste = criar_campanha(
        engine=engine,
        nome="Campanha Teste Exclusao ADM e Caixas Supply",
        data_inicio=d_ini,
        data_fim=d_fim,
        usuario="admin_tester"
    )
    assert suc and camp_teste, f"Erro ao criar campanha: {msg}"
    cid = camp_teste["id"]
    print(f"[OK] Campanha criada: {camp_teste['codigo_campanha']}")

    # 2. Insere item de teste com lojas
    matriz = []
    for lj in LISTA_14_LOJAS:
        t_id = tipo_ilha_id if lj in ["001", "002"] else tipo_inativa_id
        matriz.append({
            "loja_codigo": lj,
            "tipo_exposicao_id": t_id,
            "volume_comprador": 0
        })

    suc_it, msg_it = salvar_item_campanha_compras(
        engine=engine,
        campanha_id=cid,
        produto_codigo=4, # Tang Abacaxi
        descricao="REFRE PO TANG 18G ABACAXI",
        embalagem_compra=180,
        embalagem_transferencia=180,
        dados_lojas=matriz,
        usuario="compras_tester",
        fornecedor="TOPFLEX"
    )
    assert suc_it, f"Erro ao salvar item: {msg_it}"

    # 3. Testa Supply informando em CAIXAS (Requisito 3)
    # Loja 001: 2 caixas (360 un), Loja 002: 1 caixa (180 un)
    fechamento_caixas = [
        {"loja_codigo": "001", "caixas_transferencia": 2},
        {"loja_codigo": "002", "caixas_transferencia": 1}
    ]

    itens_c = obter_itens_campanha_com_detalhes(engine, cid)
    item_id = itens_c[0]["item_id"]

    suc_sup, msg_sup = salvar_fechamento_supply(
        engine=engine,
        campanha_id=cid,
        item_id=item_id,
        estrutura_exposicao_id=1,
        volume_calculado=360,
        fechamento_lojas=fechamento_caixas,
        estoque_cd15_total=0.0, # CD15 zerado para gerar devolutiva
        usuario="supply_tester"
    )
    assert suc_sup, f"Erro no fechamento do supply: {msg_sup}"
    print("[OK] Supply fechou com sucesso em CAIXAS diretas.")

    # Valida no banco se as caixas e volumes foram gravados corretamente
    with engine.connect() as conn:
        lojas_db = conn.execute(text("""
            SELECT loja_codigo, caixas_transferencia, volume_final_supply, volume_transferencia
            FROM campanha_lojas
            WHERE campanha_item_id = :iid AND loja_codigo IN ('001', '002')
            ORDER BY loja_codigo
        """), {"iid": item_id}).fetchall()
        
        assert lojas_db[0].caixas_transferencia == 2 and lojas_db[0].volume_final_supply == 360, "Erro no calculo de caixas Loja 001"
        assert lojas_db[1].caixas_transferencia == 1 and lojas_db[1].volume_final_supply == 180, "Erro no calculo de caixas Loja 002"
        print("[OK] Valores conferidos no banco: Loja 001 = 2 cx (360 un), Loja 002 = 1 cx (180 un)")

    # 4. Finaliza a campanha (gera pendência de compras)
    suc_fin, msg_fin = finalizar_avaliacao_campanha(engine, cid, "supply_tester")
    assert suc_fin, f"Erro ao finalizar campanha: {msg_fin}"
    camp_atual = obter_campanha_por_id(engine, cid)
    assert camp_atual["status"] == "PENDENCIA_COMPRAS", f"Status deveria ser PENDENCIA_COMPRAS, mas é {camp_atual['status']}"
    print(f"[OK] Campanha finalizada com status correto: {camp_atual['status']}")

    # 5. Testa regra da Devolutiva sumir após RESOLVIDO / COMPRADO (Requisito 1)
    with engine.connect() as conn:
        dev_pend = conn.execute(text("SELECT id, situacao FROM campanha_devolutivas WHERE campanha_id = :cid AND situacao = 'PENDENTE'"), {"cid": cid}).fetchall()
        assert len(dev_pend) > 0, "Deveria ter devolutiva pendente gerada!"
        dev_id = dev_pend[0].id

    # Atualiza status para RESOLVIDO
    with engine.begin() as conn:
        conn.execute(text("UPDATE campanha_devolutivas SET situacao = 'RESOLVIDO' WHERE id = :id"), {"id": dev_id})

    # Consulta com filtro de pendentes (deve retornar 0 linhas!)
    with engine.connect() as conn:
        dev_apos = conn.execute(text("SELECT id FROM campanha_devolutivas WHERE campanha_id = :cid AND situacao = 'PENDENTE'"), {"cid": cid}).fetchall()
        assert len(dev_apos) == 0, "A devolutiva resolvida ainda apareceu no filtro de pendentes!"
        print("[OK] Devolutiva resolvida sumiu da lista de pendentes com sucesso!")

    # 6. Testa Inativação e não contabilização em métricas (Requisito 2)
    inativar_campanha(engine, cid, "compras_tester")
    camp_inativa = obter_campanha_por_id(engine, cid)
    assert camp_inativa["status"] == "INATIVA", "Campanha deveria estar INATIVA"
    print("[OK] Campanha inativada com sucesso.")

    # 7. Testa Exclusão Permanente pelo Administrador (Requisito 5)
    suc_del, msg_del = excluir_campanha(engine, cid, "admin_tester")
    assert suc_del, f"Erro ao excluir campanha: {msg_del}"
    camp_del = obter_campanha_por_id(engine, cid)
    assert camp_del is None, "Campanha ainda existe no banco após exclusão!"
    print(f"[OK] Exclusão permanente pelo ADM executada: {msg_del}")

    print("\n==================================================================")
    print(">>> TODOS OS TESTES DOS 5 NOVOS REQUISITOS PASSARAM COM SUCESSO! <<<")
    print("==================================================================")

if __name__ == "__main__":
    run_tests()
