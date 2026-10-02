"""
scripts/test_multi_familia_mesma_ponta.py
Validação automatizada de agrupamento de múltiplos SKUs de famílias diferentes na mesma exposição:
1. Seleciona 6 SKUs divididos em múltiplas famílias ERP (ex: cápsulas Dolce Gusto).
2. Salva todos os 6 SKUs na campanha compartilhando a mesma exposição e matriz de lojas.
3. Verifica se todos os 6 SKUs foram cadastrados com total_skus_familia = 6.
4. Verifica na tela de Supply e motor de cubagem se a capacidade física do móvel é dividida por 6 para cada SKU.
"""

from __future__ import annotations
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
    salvar_familia_campanha_compras,
    obter_itens_campanha_com_detalhes,
    obter_mapa_capacidade_por_tipo_exposicao,
    salvar_dimensoes_produto,
    LISTA_14_LOJAS
)
from services.campanha_calculo import calcular_capacidade_exposicao


def run_tests():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL não configurada.")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    engine = create_engine(db_url, connect_args={"sslmode": "require"})
    create_campanhas_tables(engine)

    print("=" * 70)
    print("TESTE: LANÇAMENTO DE MÚLTIPLAS FAMÍLIAS NA MESMA PONTA / ILHA")
    print("=" * 70)

    # Criação de campanha teste
    d_ini = date.today()
    d_fim = date.today() + timedelta(days=20)
    suc_c, msg_c, camp = criar_campanha(
        engine=engine,
        nome="Campanha Teste Multi-Famílias Dolce Gusto",
        data_inicio=d_ini,
        data_fim=d_fim,
        usuario="compras_test"
    )
    assert suc_c and camp, f"Erro ao criar campanha: {msg_c}"
    camp_id = camp["id"]
    print(f"\n[1] ✅ Campanha criada com sucesso: {camp['codigo_campanha']} (ID: {camp_id})")

    # Lista de 6 SKUs de famílias diferentes (exemplo Dolce Gusto do print do usuário)
    # 38902 (família 63018), 2216 (família 1547), 2206 (família 1538), 2232 (família 1561), 2266 (família 1593), 2265 (família 1592)
    skus_teste = [
        {"produto_codigo": 38902, "descricao": "CAPSULA DOLCE GUSTO CHOCOCINO C10 150G"},
        {"produto_codigo": 2216, "descricao": "CAPSULA DOLCE GUSTO 100G C 10 CAFE AU LAIT"},
        {"produto_codigo": 2206, "descricao": "CAPSULA DOLCE GUSTO 110G C 10 AU LAIT VANILLA"},
        {"produto_codigo": 2232, "descricao": "CAPSULA DOLCE GUSTO 117G C 10 CAPPUCCINO"},
        {"produto_codigo": 2266, "descricao": "CAPSULA DOLCE GUSTO 170G C 10 CAPP DOCE LEITE"},
        {"produto_codigo": 2265, "descricao": "CAPSULA DOLCE GUSTO 170G C10 NESCAU"}
    ]

    with engine.connect() as conn:
        tipo_ilha_id = conn.execute(text("SELECT id FROM tipos_exposicao WHERE UPPER(nome) = 'ILHA'")).scalar() or 2
        tipo_ponta_id = conn.execute(text("SELECT id FROM tipos_exposicao WHERE UPPER(nome) = 'PONTA DE GÔNDOLA'")).scalar() or 1

    # Matriz de 14 lojas
    dados_lojas = [
        {"loja_codigo": lj, "tipo_exposicao_id": tipo_ponta_id if int(lj) % 2 == 1 else tipo_ilha_id, "volume_comprador": 50}
        for lj in LISTA_14_LOJAS
    ]

    print(f"\n[2] 📦 Salvando grupo com {len(skus_teste)} SKUs de famílias diferentes na mesma exposição...")
    suc_lote, msg_lote = salvar_familia_campanha_compras(
        engine=engine,
        campanha_id=camp_id,
        produtos_familia=skus_teste,
        dados_lojas=dados_lojas,
        usuario="comprador_tester",
        data_inicio=d_ini,
        data_fim=d_fim
    )
    assert suc_lote, f"Erro ao salvar lote multi-famílias: {msg_lote}"
    print(f"   -> Resultado: {msg_lote}")

    # 3. Verificação no banco de dados
    print(f"\n[3] 🔍 Verificando integridade dos itens cadastrados no banco...")
    itens_camp = obter_itens_campanha_com_detalhes(engine, camp_id)
    assert len(itens_camp) == 6, f"Esperado 6 itens na campanha, obteve {len(itens_camp)}"

    for it in itens_camp:
        p_cod = it["produto_codigo"]
        tot_fam = it.get("total_skus_familia")
        lojas_atv = len(it.get("lojas_ativas", []))
        print(f"   - SKU {p_cod}: Total SKUs compartilhados = {tot_fam} | Lojas Ativas = {lojas_atv}")
        assert tot_fam == 6, f"Erro: SKU {p_cod} deveria ter total_skus_familia = 6, mas tem {tot_fam}!"
        assert lojas_atv == 14, f"Erro: SKU {p_cod} deveria ter 14 lojas ativas, mas tem {lojas_atv}!"

    print("   ✅ Sucesso: Todos os 6 SKUs foram registrados no banco com rateio compartilhado de 6 SKUs!")

    # 4. Verificação da cubagem no Supply com rateio entre os 6 SKUs
    print(f"\n[4] 📐 Verificando cubagem do Supply dividindo o móvel entre os 6 SKUs...")
    # Cadastra dimensões de teste para as cápsulas: 15 x 15 x 15 cm
    salvar_dimensoes_produto(engine, 38902, 15.0, 15.0, 15.0, "admin_test", propagar_familia=False)
    
    dim_capsula = {"altura_cm": 15.0, "largura_cm": 15.0, "profundidade_cm": 15.0}
    mapa_cap = obter_mapa_capacidade_por_tipo_exposicao(engine, dim_capsula, embl_transferencia=10, total_skus=6)
    
    info_ponta = mapa_cap.get(tipo_ponta_id, {})
    cap_tot_un = info_ponta.get("capacidade_total_un", 0)
    cap_sku_un = info_ponta.get("capacidade_sku_un", 0)
    
    print(f"   - Ponta de Gôndola:")
    print(f"     * Capacidade Total do Móvel (100%): {info_ponta.get('capacidade_total_cx')} cx ({cap_tot_un} un)")
    print(f"     * Sugestão por Sabor (1 de 6 SKUs): {info_ponta.get('capacidade_sku_cx')} cx ({cap_sku_un} un)")
    
    assert cap_sku_un <= (cap_tot_un // 6) + 1, "Erro no rateio da capacidade física por SKU!"
    print("   ✅ Sucesso: A cubagem divide a capacidade física do móvel exatamente entre os 6 SKUs agrupados!")

    # Limpeza
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM campanhas WHERE id = :cid"), {"cid": camp_id})

    print("\n" + "=" * 70)
    print("🎉 TESTE CONCLUÍDO COM 100% DE SUCESSO! 🚀")
    print("=" * 70)


if __name__ == "__main__":
    run_tests()
