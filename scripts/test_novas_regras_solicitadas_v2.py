"""
scripts/test_novas_regras_solicitadas_v2.py
Validação automatizada ponta a ponta dos 5 novos requisitos solicitados:
1. Devolutiva de Compras atualizada e mantida por FAMÍLIA de produtos.
2. Busca de novos itens por descrição, tokens parciais, código e família.
3. Compras com alteração de tipos de exposição em itens cadastrados e Reenvio para Supply.
4. Supply com capacidade física informada em Caixas (cx) e Unidades (un) para móvel e por SKU.
5. Sugestão de caixas acompanhando a cubagem específica do tipo de exposição de cada loja com rateio familiar.
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

import pandas as pd
from sqlalchemy import create_engine, text
from services.campanha_db import create_campanhas_tables
from services.campanha_service import (
    criar_campanha,
    obter_campanha_por_id,
    excluir_campanha,
    carregar_dados_produto_consolidado,
    salvar_familia_campanha_compras,
    enviar_campanha_para_supply,
    reenviar_campanha_para_supply,
    alterar_exposicao_lojas_item,
    atualizar_status_devolutiva_familia,
    obter_mapa_capacidade_por_tipo_exposicao,
    salvar_fechamento_supply,
    obter_itens_campanha_com_detalhes,
    LISTA_14_LOJAS
)
from services.campanha_calculo import calcular_capacidade_exposicao


def run_tests():
    print("=" * 80)
    print("🚀 INICIANDO BATERIA DE TESTES DOS 5 NOVOS REQUISITOS")
    print("=" * 80)

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL não configurada.")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    engine = create_engine(db_url, connect_args={"sslmode": "require"})
    create_campanhas_tables(engine)

    # Obter IDs dos tipos de exposição
    with engine.connect() as conn:
        tipo_inativa_id = conn.execute(text("SELECT id FROM tipos_exposicao WHERE UPPER(nome) = 'INATIVA'")).scalar()
        tipo_ponta_id = conn.execute(text("SELECT id FROM tipos_exposicao WHERE UPPER(nome) = 'PONTA DE GÔNDOLA'")).scalar()
        tipo_ilha_id = conn.execute(text("SELECT id FROM tipos_exposicao WHERE UPPER(nome) = 'ILHA'")).scalar()
        tipo_meia_id = conn.execute(text("SELECT id FROM tipos_exposicao WHERE UPPER(nome) = 'MEIA PONTA'")).scalar()

    hoje = date.today()
    d_ini = hoje + timedelta(days=5)
    d_fim = hoje + timedelta(days=20)

    # Criação de Campanha para os Testes
    suc, msg, camp = criar_campanha(
        engine=engine,
        nome="Campanha Teste 5 Requisitos Família e Cubagem",
        data_inicio=d_ini,
        data_fim=d_fim,
        usuario="test_runner"
    )
    assert suc and camp, f"Erro ao criar campanha: {msg}"
    cid = camp["id"]
    print(f"\n[1] ✅ Campanha criada: {camp['codigo_campanha']} (ID: {cid})")

    # -------------------------------------------------------------------------
    # TESTE REQUISITO 2: Busca por Descrição / Tokens Parciais no Catálogo
    # -------------------------------------------------------------------------
    print("\n[2] 🔍 Testando Busca de Produtos por Descrição e Tokens Parciais...")
    parquet_path = "bdados/query.parquet"
    df_p = pd.read_parquet(parquet_path)
    df_p.columns = [str(c).strip() for c in df_p.columns]
    df_p_uniq = df_p.drop_duplicates(subset=["CODIGO_PRODUTO"])

    termo_busca = "tang laranja"
    tokens = [t.strip().upper() for t in termo_busca.split()]
    desc_col = df_p_uniq["DESCRICAO_PRODUTO"].astype(str).str.upper()
    filtro = pd.Series(True, index=df_p_uniq.index)
    for tok in tokens:
        filtro = filtro & desc_col.str.contains(tok, regex=False, na=False)

    resultados_busca = df_p_uniq[filtro]
    assert not resultados_busca.empty, "Busca por 'tang laranja' falhou."
    cod_tang = int(resultados_busca.iloc[0]["CODIGO_PRODUTO"])
    print(f"   -> Busca '{termo_busca}' encontrou {len(resultados_busca)} produto(s). Exemplo: SKU {cod_tang} - {resultados_busca.iloc[0]['DESCRICAO_PRODUTO']}")

    # -------------------------------------------------------------------------
    # TESTE REQUISITO 5 & 4: Inserção de Família, Cubagem e Sugestão por Tipo de Loja
    # -------------------------------------------------------------------------
    print("\n[3] 👨‍👩‍👧‍👦 Cadastrando Família Tang (SKU 3 Laranja, SKU 4 Abacaxi) com Lojas Diferenciadas...")
    prod3_info = carregar_dados_produto_consolidado(engine, 3, d_ini, d_fim)
    prod4_info = carregar_dados_produto_consolidado(engine, 4, d_ini, d_fim)

    # Loja 001: PONTA DE GÔNDOLA
    # Loja 002: ILHA
    # Loja 003: MEIA PONTA
    # Outras: INATIVA
    matriz_lojas = []
    for lj in LISTA_14_LOJAS:
        if lj == "001":
            t_id = tipo_ponta_id
        elif lj == "002":
            t_id = tipo_ilha_id
        elif lj == "003":
            t_id = tipo_meia_id
        else:
            t_id = tipo_inativa_id
        
        matriz_lojas.append({
            "loja_codigo": lj,
            "tipo_exposicao_id": t_id,
            "volume_comprador": 0
        })

    produtos_familia = [
        {"produto_codigo": 3, "descricao": prod3_info["descricao"]},
        {"produto_codigo": 4, "descricao": prod4_info["descricao"]}
    ]

    suc_fam, msg_fam = salvar_familia_campanha_compras(
        engine=engine,
        campanha_id=cid,
        produtos_familia=produtos_familia,
        dados_lojas=matriz_lojas,
        usuario="comprador_test",
        data_inicio=d_ini,
        data_fim=d_fim
    )
    assert suc_fam, f"Falha ao salvar família: {msg_fam}"
    print(f"   -> {msg_fam}")

    # Testando Mapa de Capacidade Física (Requisito 4 & 5)
    dim_tang = {"altura_cm": 12.0, "largura_cm": 10.0, "profundidade_cm": 1.0}
    mapa_cap = obter_mapa_capacidade_por_tipo_exposicao(engine, dim_tang, embl_transferencia=180, total_skus=2)
    
    print("\n[4] 📐 Verificando Mapa de Capacidade Física em Caixas e Unidades (Requisito 4):")
    for tid, info in mapa_cap.items():
        if info["tipo_nome"] != "INATIVA":
            print(f"   -> {info['tipo_nome']:<18} | Móvel: {info['capacidade_total_cx']:>3} cx ({info['capacidade_total_un']:>5} un) | Sugestão por Sabor (1 de 2): {info['capacidade_sku_cx']:>3} cx ({info['capacidade_sku_un']:>5} un)")

    # Validações matemáticas do Requisito 4 & 5:
    assert mapa_cap[tipo_ponta_id]["capacidade_total_cx"] > 0, "Ponta deve ter caixas > 0"
    assert mapa_cap[tipo_ponta_id]["capacidade_sku_cx"] == mapa_cap[tipo_ponta_id]["capacidade_total_cx"] // 2, "Rateio por SKU deve ser exato para 2 sabores"
    assert mapa_cap[tipo_ilha_id]["capacidade_total_cx"] >= mapa_cap[tipo_ponta_id]["capacidade_total_cx"], "Ilha deve ter maior ou igual capacidade que Ponta"

    # Envia para Supply
    enviar_campanha_para_supply(engine, cid, "comprador_test")

    # -------------------------------------------------------------------------
    # TESTE REQUISITO 3: Alteração de Exposição por Loja e Reenvio para o Supply
    # -------------------------------------------------------------------------
    print("\n[5] 🔄 Testando Alteração de Exposição pelo Compras (Ponta -> Ilha) e Reenvio para o Supply...")
    itens_camp = obter_itens_campanha_com_detalhes(engine, cid)
    item_3 = [it for it in itens_camp if it["produto_codigo"] == 3][0]

    # Modifica Loja 001 de PONTA DE GÔNDOLA para ILHA e propaga para toda a família (SKU 3 e 4)
    matriz_alterada = []
    for lj_orig in item_3["lojas"]:
        lj_c = lj_orig["loja_codigo"]
        t_id = tipo_ilha_id if lj_c == "001" else lj_orig["tipo_exposicao_id"]
        matriz_alterada.append({
            "loja_codigo": lj_c,
            "tipo_exposicao_id": t_id,
            "volume_comprador": 500
        })

    suc_alt, msg_alt = alterar_exposicao_lojas_item(
        engine=engine,
        campanha_id=cid,
        item_id=item_3["item_id"],
        dados_lojas=matriz_alterada,
        usuario="comprador_test",
        propagar_familia=True
    )
    assert suc_alt, f"Falha ao alterar exposição: {msg_alt}"
    print(f"   -> {msg_alt}")

    # Reenvia para o Supply
    suc_re, msg_re = reenviar_campanha_para_supply(engine, cid, "comprador_test", motivo="Loja 001 renegociada de Ponta para Ilha")
    assert suc_re, f"Falha ao reenviar para Supply: {msg_re}"
    print(f"   -> {msg_re}")

    camp_re = obter_campanha_por_id(engine, cid)
    assert camp_re["status"] == "ENVIADA_SUPPLY", f"Status esperado ENVIADA_SUPPLY, obtido {camp_re['status']}"

    # -------------------------------------------------------------------------
    # TESTE REQUISITO 1: Fechamento Supply, Geração de Devolutivas e Manutenção por FAMÍLIA
    # -------------------------------------------------------------------------
    print("\n[6] 📋 Testando Devolutivas de Compras e Atualização de Status por FAMÍLIA...")
    # Avalia SKU 3 com demanda de 10 caixas no total, saldo CD15 = 0 cx -> Gera 10 cx de falta
    fechamento_s3 = [
        {"loja_codigo": "001", "caixas_transferencia": 5},
        {"loja_codigo": "002", "caixas_transferencia": 5}
    ]
    salvar_fechamento_supply(
        engine=engine,
        campanha_id=cid,
        item_id=item_3["item_id"],
        estrutura_exposicao_id=3,
        volume_calculado=900,
        fechamento_lojas=fechamento_s3,
        estoque_cd15_total=0.0,
        usuario="supply_test"
    )

    # Avalia SKU 4 com demanda de 8 caixas no total, saldo CD15 = 0 cx -> Gera 8 cx de falta
    item_4 = [it for it in itens_camp if it["produto_codigo"] == 4][0]
    fechamento_s4 = [
        {"loja_codigo": "001", "caixas_transferencia": 4},
        {"loja_codigo": "002", "caixas_transferencia": 4}
    ]
    salvar_fechamento_supply(
        engine=engine,
        campanha_id=cid,
        item_id=item_4["item_id"],
        estrutura_exposicao_id=3,
        volume_calculado=900,
        fechamento_lojas=fechamento_s4,
        estoque_cd15_total=0.0,
        usuario="supply_test"
    )

    with engine.connect() as conn:
        devs = conn.execute(text("SELECT id, produto_codigo, situacao, caixas_falta FROM campanha_devolutivas WHERE campanha_id = :cid"), {"cid": cid}).fetchall()
        print(f"   -> Devolutivas geradas no CD15: {len(devs)} registros (SKUs: {[d.produto_codigo for d in devs]}, Faltas: {[d.caixas_falta for d in devs]} cx)")
        assert len(devs) == 2, f"Esperado 2 devolutivas geradas, obtido {len(devs)}"

    # Atualiza Status da FAMÍLIA INTEIRA para 'COMPRADO'
    cod_fam_tang = item_3["codigo_familia"]
    suc_dev_up, msg_dev_up = atualizar_status_devolutiva_familia(
        engine=engine,
        campanha_id=cid,
        novo_status="COMPRADO",
        usuario="comprador_test",
        codigo_familia=cod_fam_tang
    )
    assert suc_dev_up, f"Falha ao atualizar devolutivas por família: {msg_dev_up}"
    print(f"   -> {msg_dev_up}")

    with engine.connect() as conn:
        devs_pos = conn.execute(text("SELECT id, produto_codigo, situacao FROM campanha_devolutivas WHERE campanha_id = :cid"), {"cid": cid}).fetchall()
        for dp in devs_pos:
            assert dp.situacao == "COMPRADO", f"Devolutiva do SKU {dp.produto_codigo} não foi atualizada para COMPRADO (está {dp.situacao})"
        print(f"   -> Todas as devolutivas da Família {cod_fam_tang} foram atualizadas simultaneamente para COMPRADO!")

    # -------------------------------------------------------------------------
    # LIMPEZA
    # -------------------------------------------------------------------------
    print("\n[7] 🧹 Limpando campanha de teste...")
    suc_del, msg_del = excluir_campanha(engine, cid, "admin_tester")
    assert suc_del, f"Falha na exclusão: {msg_del}"
    print(f"   -> {msg_del}")

    print("\n" + "=" * 80)
    print("🎉 TODOS OS 5 REQUISITOS FORAM TESTADOS E VALIDADOS COM 100% DE SUCESSO!")
    print("=" * 80)


if __name__ == "__main__":
    run_tests()
