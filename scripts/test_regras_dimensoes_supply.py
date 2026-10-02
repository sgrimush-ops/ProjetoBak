"""
scripts/test_regras_dimensoes_supply.py
Validação automatizada das regras de dimensões no Supply e Admins Exposição:
1. Produto sem cadastro em produto_dimensoes retorna 0,00 cm e 0 caixas calculadas/sugeridas.
2. Ao atualizar o banco de dados (Admins Exposição), a cubagem calcula e sugere automaticamente para itens pendentes.
3. Valores já modificados e salvos pelo Supply permanecem preservados e não são refatorados.
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
    carregar_dados_produto_consolidado,
    salvar_dimensoes_produto,
    obter_mapa_capacidade_por_tipo_exposicao,
    criar_campanha,
    salvar_familia_campanha_compras,
    obter_itens_campanha_com_detalhes,
    salvar_fechamento_supply,
    obter_estruturas_e_bandejas,
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
    print("INICIANDO TESTES DE REGRAS DE DIMENSÕES E FECHAMENTO DO SUPPLY")
    print("=" * 70)

    # 1. Testar produto com dimensões limpas / zeradas no banco
    prod_teste = 3682  # MIST LACTEA COND TRIANGULO TP 395G
    d_ini = date.today()
    d_fim = date.today() + timedelta(days=15)

    with engine.begin() as conn:
        # Garante que não haja dimensões prévias para o teste 1
        conn.execute(text("DELETE FROM produto_dimensoes WHERE produto_codigo = :pcod"), {"pcod": prod_teste})

    # Consulta consolidada
    dados_sem_dim = carregar_dados_produto_consolidado(engine, prod_teste, d_ini, d_fim)
    dim_dict = dados_sem_dim.get("dimensoes")

    print(f"\n[1] Testando Produto {prod_teste} sem cadastro no banco:")
    print(f"    Dimensões retornadas: {dim_dict}")
    
    # Se dimensoes for None, alt_p deve ser 0
    alt_p = float(dim_dict.get("altura_cm", 0.0) or 0.0) if dim_dict else 0.0
    larg_p = float(dim_dict.get("largura_cm", 0.0) or 0.0) if dim_dict else 0.0
    prof_p = float(dim_dict.get("profundidade_cm", 0.0) or 0.0) if dim_dict else 0.0
    
    assert alt_p == 0.0 and larg_p == 0.0 and prof_p == 0.0, "Erro: dimensões deveriam ser 0 quando não cadastradas no banco!"

    dim_zerada = {"altura_cm": 0.0, "largura_cm": 0.0, "profundidade_cm": 0.0}
    mapa_cap_zerado = obter_mapa_capacidade_por_tipo_exposicao(engine, dim_zerada, embl_transferencia=24, total_skus=1)
    
    for tid, info in mapa_cap_zerado.items():
        assert info["capacidade_total_un"] == 0, f"Erro: capacidade total deveria ser 0, deu {info['capacidade_total_un']}"
        assert info["capacidade_total_cx"] == 0, f"Erro: capacidade cx deveria ser 0, deu {info['capacidade_total_cx']}"
        assert info["capacidade_sku_un"] == 0, f"Erro: capacidade sku un deveria ser 0, deu {info['capacidade_sku_un']}"
        assert info["capacidade_sku_cx"] == 0, f"Erro: capacidade sku cx deveria ser 0, deu {info['capacidade_sku_cx']}"

    print("    ✅ Sucesso: Com dimensões ausentes no banco, capacidades e caixas resultam estritamente em 0!")

    # 2. Testar inserção das dimensões no banco (simulando Admins Exposição)
    print(f"\n[2] Cadastrando dimensões no banco via Admins Exposição:")
    suc_dim, msg_dim = salvar_dimensoes_produto(
        engine=engine,
        produto_codigo=prod_teste,
        altura_cm=12.0,
        largura_cm=6.0,
        profundidade_cm=3.0,
        usuario="admin_teste",
        propagar_familia=True
    )
    assert suc_dim, f"Falha ao salvar dimensões: {msg_dim}"
    print(f"    Dimensões salvas: 12.0 x 6.0 x 3.0 cm ({msg_dim})")

    dados_com_dim = carregar_dados_produto_consolidado(engine, prod_teste, d_ini, d_fim)
    dim_salva = dados_com_dim.get("dimensoes")
    assert dim_salva is not None, "Erro: dimensões não foram carregadas após inserção no banco!"
    assert float(dim_salva["altura_cm"]) == 12.0
    assert float(dim_salva["largura_cm"]) == 6.0
    assert float(dim_salva["profundidade_cm"]) == 3.0

    mapa_cap_calculado = obter_mapa_capacidade_por_tipo_exposicao(engine, dim_salva, embl_transferencia=24, total_skus=1)
    print(f"    Mapa de Capacidade Calculado com Medidas Reais:")
    for tid, info in mapa_cap_calculado.items():
        print(f"    - Tipo {info['tipo_nome']}: {info['capacidade_total_cx']} cx ({info['capacidade_total_un']} un) | SKU: {info['capacidade_sku_cx']} cx ({info['capacidade_sku_un']} un)")
        if info["tipo_nome"] != "INATIVA":
            assert info["capacidade_total_un"] > 0, "Capacidade deveria ser maior que zero com dimensões cadastradas!"

    print("    ✅ Sucesso: Após atualizar o banco, cálculos de cubagem e sugestões passam a ser gerados automaticamente!")

    # 3. Testar preservação de caixas já modificadas e salvas pelo Supply
    print(f"\n[3] Testando preservação de valores já salvos pelo Supply:")
    
    # Cria uma campanha de teste
    suc_c, msg_c, camp = criar_campanha(
        engine=engine,
        nome="Campanha Teste Supply Dimensões",
        data_inicio=d_ini,
        data_fim=d_fim,
        usuario="test_runner"
    )
    assert suc_c and camp, f"Erro ao criar campanha: {msg_c}"
    camp_id = camp["id"]

    with engine.connect() as conn:
        tipo_ilha_id = conn.execute(text("SELECT id FROM tipos_exposicao WHERE UPPER(nome) = 'ILHA'")).scalar() or 2

    # Salva item na campanha
    produtos_fam = [{"produto_codigo": prod_teste, "descricao": "MIST LACTEA TESTE"}]
    dados_lojas = [{"loja_codigo": lj, "volume_comprador": 100, "tipo_exposicao_id": tipo_ilha_id} for lj in LISTA_14_LOJAS]
    suc_fam, msg_fam = salvar_familia_campanha_compras(
        engine=engine,
        campanha_id=camp_id,
        produtos_familia=produtos_fam,
        dados_lojas=dados_lojas,
        usuario="compras_teste",
        data_inicio=d_ini,
        data_fim=d_fim
    )
    assert suc_fam, f"Erro ao adicionar família: {msg_fam}"

    itens_detalhes = obter_itens_campanha_com_detalhes(engine, camp_id)
    assert len(itens_detalhes) > 0
    item_id = itens_detalhes[0]["item_id"]

    # Supply define e salva manualmente 15 caixas para a loja 001
    fechamento = [{"loja_codigo": "001", "caixas_transferencia": 15, "volume_final_supply": 15 * 24}]
    suc_salv, msg_salv = salvar_fechamento_supply(
        engine=engine,
        campanha_id=camp_id,
        item_id=item_id,
        estrutura_exposicao_id=None,
        volume_calculado=100,
        fechamento_lojas=fechamento,
        estoque_cd15_total=5000.0,
        usuario="supply_teste"
    )
    assert suc_salv, f"Erro ao salvar supply: {msg_salv}"

    # Recarrega itens
    itens_pos_salv = obter_itens_campanha_com_detalhes(engine, camp_id)
    loja_001 = next(l for l in itens_pos_salv[0]["lojas_ativas"] if l["loja_codigo"] == "001")
    
    # Aplica a exata lógica de inicialização da tela
    is_item_avaliado = (itens_pos_salv[0].get("status_supply") == "AVALIADO")
    cx_salva = int(loja_001.get("caixas_transferencia") or 0)
    vol_salvo = int(loja_001.get("volume_final_supply") or 0)
    sug_cx_cubagem = mapa_cap_calculado.get(tipo_ilha_id, {}).get("capacidade_sku_cx", 0)

    if is_item_avaliado or cx_salva > 0 or vol_salvo > 0:
        cx_inicial = cx_salva
    elif sug_cx_cubagem > 0:
        cx_inicial = sug_cx_cubagem
    else:
        cx_inicial = 0

    print(f"    Valor salvo pelo Supply: {cx_salva} cx")
    print(f"    Sugestão da Cubagem para Ilha: {sug_cx_cubagem} cx")
    print(f"    Valor carregado no campo de input: {cx_inicial} cx")

    assert cx_inicial == 15, f"Erro: Esperado 15 cx salvas pelo Supply, mas obteve {cx_inicial} cx!"
    print("    ✅ Sucesso: O valor modificado e salvo pelo Supply (15 cx) foi 100% preservado e não sobrescrito pela cubagem!")

    # Limpeza
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM campanhas WHERE id = :cid"), {"cid": camp_id})

    print("\n" + "=" * 70)
    print("TODOS OS TESTES FORAM CONCLUÍDOS COM SUCESSO! 🚀")
    print("=" * 70)


if __name__ == "__main__":
    run_tests()
