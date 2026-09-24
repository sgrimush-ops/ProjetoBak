"""
scripts/test_campanhas_e2e.py
Script de teste ponta a ponta (E2E) do ciclo de vida completo do Módulo de Campanhas.
"""

import os
import sys
from datetime import date, timedelta
from dotenv import load_dotenv

# Configura path e encoding
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
    salvar_dimensoes_produto,
    obter_estruturas_e_bandejas,
    salvar_fechamento_supply,
    finalizar_avaliacao_campanha,
    replicar_campanha,
    obter_itens_campanha_com_detalhes,
    LISTA_14_LOJAS
)
from services.exportacao_campanha import (
    gerar_excel_transferencia_operacional,
    gerar_excel_devolutiva_compras,
    gerar_excel_consulta_loja,
    gerar_pdf_campanha_loja
)


def run_e2e_tests():
    print("🚀 INICIANDO TESTES END-TO-END (E2E) DO MÓDULO DE CAMPANHAS...")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL não configurada.")

    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    engine = create_engine(db_url, connect_args={"sslmode": "require"})

    # 1. Garantir tabelas
    print("\n[1/7] Criando/Verificando tabelas no PostgreSQL...")
    create_campanhas_tables(engine)
    print("✅ Tabelas e dados iniciais verificados.")

    # 2. Criar Campanha E2E
    print("\n[2/7] Criando Campanha de Teste...")
    d_ini = date.today() + timedelta(days=5)
    d_fim = date.today() + timedelta(days=20)
    nome_camp = f"Campanha Teste E2E Auto - {d_ini.strftime('%d/%m')}"

    sucesso, msg, camp = criar_campanha(
        engine=engine,
        nome=nome_camp,
        data_inicio=d_ini,
        data_fim=d_fim,
        usuario="admin_tester",
        observacoes="Teste de validação automatizada"
    )
    assert sucesso and camp is not None, f"Falha ao criar campanha: {msg}"
    cid = camp["id"]
    cod_camp = camp["codigo_campanha"]
    print(f"✅ Campanha criada: {cod_camp} (ID: {cid})")

    # 3. Vincular Produto e 14 Lojas (Compras)
    print("\n[3/7] Vinculando Produto e Matriz das 14 Lojas (Compras)...")
    with engine.connect() as conn:
        tipo_pg = conn.execute(text("SELECT id FROM tipos_exposicao WHERE nome = 'PONTA DE GÔNDOLA'")).scalar()

    lojas_matriz = [
        {
            "loja_codigo": lj,
            "tipo_exposicao_id": tipo_pg,
            "volume_comprador": 30,
            "estoque_loja": 15.0,
            "estoque_cd": 120.0,
            "venda_media": 3.0,
            "venda_projetada": 45.0
        }
        for lj in LISTA_14_LOJAS
    ]

    prod_cod = 999901
    prod_desc = "PRODUTO TESTE E2E CHOCOLATE 200G"
    suc_item, msg_item = salvar_item_campanha_compras(
        engine=engine,
        campanha_id=cid,
        produto_codigo=prod_cod,
        descricao=prod_desc,
        embalagem_compra=12,
        embalagem_transferencia=12,
        dados_lojas=lojas_matriz,
        usuario="compras_tester"
    )
    assert suc_item, f"Falha ao salvar item: {msg_item}"
    print(f"✅ Item {prod_cod} e matriz de 14 lojas salvos com sucesso.")

    # 4. Enviar para o Supply
    print("\n[4/7] Enviando Campanha para o Supply...")
    suc_envio, msg_envio = enviar_campanha_para_supply(engine, cid, "compras_tester")
    assert suc_envio, f"Falha ao enviar para supply: {msg_envio}"
    camp_atual = obter_campanha_por_id(engine, cid)
    assert camp_atual["status"] == "ENVIADA_SUPPLY", "Status divergente."
    print("✅ Campanha em status ENVIADA_SUPPLY.")

    # 5. Salvar Dimensões e Cubagem no Supply
    print("\n[5/7] Configurando Dimensões Físicas e Fechamento no Supply...")
    suc_dim, msg_dim = salvar_dimensoes_produto(
        engine=engine,
        produto_codigo=prod_cod,
        altura_cm=15.0,
        largura_cm=10.0,
        profundidade_cm=5.0,
        usuario="supply_tester"
    )
    assert suc_dim, f"Falha ao salvar dimensões: {msg_dim}"

    estruturas = obter_estruturas_e_bandejas(engine)
    est_id = estruturas[0]["estrutura_id"] if estruturas else None

    itens_detalhes = obter_itens_campanha_com_detalhes(engine, cid)
    item_id = itens_detalhes[0]["item_id"]

    fechamento_lojas = [
        {"loja_codigo": lj, "volume_final_supply": 36}
        for lj in LISTA_14_LOJAS
    ]

    suc_fech, msg_fech = salvar_fechamento_supply(
        engine=engine,
        campanha_id=cid,
        item_id=item_id,
        estrutura_exposicao_id=est_id,
        volume_calculado=48,
        fechamento_lojas=fechamento_lojas,
        estoque_cd15_total=600.0,
        usuario="supply_tester"
    )
    assert suc_fech, f"Falha no fechamento do supply: {msg_fech}"

    suc_fin, msg_fin = finalizar_avaliacao_campanha(engine, cid, "supply_tester")
    assert suc_fin, f"Falha ao finalizar campanha: {msg_fin}"
    print(f"✅ Fechamento do Supply concluído. Resultado: {msg_fin}")

    # 6. Testar Geração de Todos os 4 Arquivos de Exportação
    print("\n[6/7] Gerando e validando os 4 relatórios de exportação...")
    excel_transf = gerar_excel_transferencia_operacional(engine, cid)
    assert len(excel_transf) > 500, "Arquivo de transferência inválido."
    print(f"   -> Excel Operacional Transferência gerado ({len(excel_transf)} bytes)")

    excel_dev = gerar_excel_devolutiva_compras(engine, cid)
    assert len(excel_dev) > 500, "Arquivo de devolutivas inválido."
    print(f"   -> Excel Devolutivas Compras gerado ({len(excel_dev)} bytes)")

    excel_loja = gerar_excel_consulta_loja(engine, cid, "001")
    assert len(excel_loja) > 500, "Arquivo de consulta de loja inválido."
    print(f"   -> Excel Consulta Loja 001 gerado ({len(excel_loja)} bytes)")

    pdf_loja = gerar_pdf_campanha_loja(engine, cid, "001")
    assert len(pdf_loja) > 500, "PDF da loja inválido."
    print(f"   -> PDF Conferência Loja 001 gerado ({len(pdf_loja)} bytes)")

    # 7. Testar Replicação
    print("\n[7/7] Testando Replicação de Campanha...")
    suc_rep, msg_rep, rep_id = replicar_campanha(
        engine=engine,
        campanha_origem_id=cid,
        novo_nome=f"Cópia Replicada de {nome_camp}",
        nova_data_inicio=d_fim + timedelta(days=1),
        nova_data_fim=d_fim + timedelta(days=15),
        usuario="admin_tester"
    )
    assert suc_rep and rep_id is not None, f"Falha ao replicar: {msg_rep}"
    camp_rep = obter_campanha_por_id(engine, rep_id)
    assert camp_rep["campanha_origem_id"] == cid, "Origem não vinculada."
    print(f"✅ Campanha replicada com sucesso: {camp_rep['codigo_campanha']} (Origem: {cod_camp})")

    # Limpeza dos dados de teste
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM campanhas WHERE id IN (:cid1, :cid2)"), {"cid1": cid, "cid2": rep_id})
        conn.execute(text("DELETE FROM produto_dimensoes WHERE produto_codigo = :pcod"), {"pcod": prod_cod})
    print("\n🧹 Registros de teste limpos do banco de dados.")

    print("\n🎉 TODOS OS TESTES E2E PASSARAM COM 100% DE SUCESSO!")


if __name__ == "__main__":
    run_e2e_tests()
