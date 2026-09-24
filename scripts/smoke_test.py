#!/usr/bin/env python3
"""
Smoke tests automatizados para páginas Streamlit sem UI real.

O teste stubba o módulo `streamlit`, importa as páginas e exercita:
- Resolvers de colunas em `page/__init__.py`
- Conversão de tipos em `aprovacao_pedidos.formatar_tipos_df`

Uso:
  /workspaces/ProjetoBak/.venv/bin/python scripts/smoke_test.py
Retorno:
  Código 0 em sucesso; >0 em falhas.
"""

from __future__ import annotations

import sys
import os
import types
import traceback


class _StreamlitStub:
    def __init__(self):
        # column_config stubs
        self.column_config = types.SimpleNamespace(
            NumberColumn=lambda *a, **k: None,
            TextColumn=lambda *a, **k: None,
            DateColumn=lambda *a, **k: None,
            CheckboxColumn=lambda *a, **k: None,
        )
        # session_state stub
        self.session_state = {}

    def __getattr__(self, name):
        # cache_data decorator → identidade
        if name == "cache_data":
            def deco(*dargs, **dkwargs):
                def wrapper(func):
                    return func
                return wrapper
            return deco

        # st.stop / st.rerun como no-op
        if name in {"stop", "rerun"}:
            def _no_op(*a, **k):
                return None
            return _no_op

        # Demais funções: no-op
        def _f(*a, **k):
            return None

        return _f


def _install_streamlit_stub():
    sys.modules["streamlit"] = _StreamlitStub()


def main() -> int:
    try:
        # Garantir que o diretório do projeto esteja no PYTHONPATH
        repo_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..")
        )
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)

        _install_streamlit_stub()

        # Imports de páginas
        import importlib

        modules = [
            "page.__init__",
            "page.home",
            "page.pedido_cd",
            "page.aprovacao_pedidos",
            "page.status_usuarios",
            "page.campanhas_compras",
            "page.campanhas_supply",
            "page.campanhas_loja",
            "page.campanhas_admin_exposicao",
            "services.campanha_calculo",
            "services.campanha_db",
            "services.campanha_service",
            "services.exportacao_campanha",
        ]

        for m in modules:
            importlib.import_module(m)
            print(f"import_ok {m}")

        # Exercitar resolvers com SQLite (retornam defaults)
        from sqlalchemy import create_engine
        engine = create_engine("sqlite:///:memory:")
        from page import (
            resolve_ofertas_codigo_col,
            resolve_mix_codigo_col,
            resolve_mix_descricao_col,
            resolve_mix_emb_col,
            resolve_pedidos_codigo_col,
            resolve_pedidos_descricao_col,
            resolve_pedidos_emb_col,
        )

        assert resolve_ofertas_codigo_col(engine) == "codigo_interno"
        assert resolve_mix_codigo_col(engine) == "codigo_interno"
        assert resolve_mix_descricao_col(engine) == "descricao"
        assert resolve_mix_emb_col(engine) == "embalagem"
        assert resolve_pedidos_codigo_col(engine) == "codigo_interno"
        assert resolve_pedidos_descricao_col(engine) == "descricao"
        assert resolve_pedidos_emb_col(engine) == "embalagem"
        print("resolvers_ok")

        # Teste de conversão de tipos em aprovacao_pedidos
        import pandas as pd
        from page.aprovacao_pedidos import formatar_tipos_df

        df = pd.DataFrame(
            {
                "loja_001": ["1", "2"],
                "loja_002": ["0", None],
                "total_cx": ["3", "4"],
                "embalagem": ["5", "6"],
                "codigo_interno": ["123", "456"],
            }
        )
        res = formatar_tipos_df(df)
        assert str(res["loja_001"].dtype) == "int64"
        assert str(res["total_cx"].dtype) == "int64"
        assert str(res["embalagem"].dtype) == "int64"
        assert str(res["codigo_interno"].dtype) == "int64"
        print("format_ok")

        # Teste verificação de cargo consumo cd
        from utils.cargos import is_user_consumo_cd

        assert not is_user_consumo_cd(session_state={"role": "admin", "cargo": "consumo cd"})
        assert is_user_consumo_cd(session_state={"role": "user", "cargo": "consumo cd"})
        assert is_user_consumo_cd(session_state={"role": "consumo cd", "cargo": ""})
        assert is_user_consumo_cd(session_state={"role": "user", "cargo": "Consumo CD"})
        assert not is_user_consumo_cd(session_state={"role": "user", "cargo": "comprador"})
        print("cargo_consumo_cd_ok")

        # Teste da tela de status dos usuários e exclusão em lote
        from page.status_usuarios import get_user_status_df, delete_users_batch
        from sqlalchemy import text as _text
        with engine.begin() as conn:
            conn.execute(_text("""
                CREATE TABLE users (
                    username TEXT PRIMARY KEY,
                    empresa TEXT,
                    cargo TEXT,
                    ultimo_acesso TEXT,
                    status_logado TEXT,
                    lojas_acesso TEXT
                )
            """))
            conn.execute(_text("""
                INSERT INTO users (username, empresa, cargo, ultimo_acesso, status_logado, lojas_acesso) VALUES
                ('u_nunca', 'Baklizi', 'gerente', NULL, 'DESLOGADO', '["001"]'),
                ('u_65dias', 'Baklizi', 'caixa', '2025-01-01 10:00:00', 'DESLOGADO', '["001"]'),
                ('u_online', 'Baklizi', 'admin', '2099-01-01 10:00:00', 'LOGADO', '["001"]')
            """))

        df_s = get_user_status_df(engine)
        assert len(df_s) == 3
        categorias = dict(zip(df_s['username'], df_s['Categoria']))
        assert categorias['u_nunca'] == "Nunca Acessou"
        assert categorias['u_65dias'] == "Inativo (60+ dias)"
        assert df_s.loc[df_s['username'] == 'u_nunca', 'Elegivel_Exclusao'].values[0] == True
        assert df_s.loc[df_s['username'] == 'u_65dias', 'Elegivel_Exclusao'].values[0] == True

        deletados = delete_users_batch(engine, ['u_nunca', 'u_65dias'])
        assert deletados == 2
        print("status_usuarios_ok")

        from page.pedido_cd import get_cd15_stock_from_parquet
        stock_val = get_cd15_stock_from_parquet(3938)
        if stock_val is not None:
            assert isinstance(stock_val, (int, float))
            print("estoque_query_parquet_ok")

        from page.home import get_query_parquet_last_update
        last_up = get_query_parquet_last_update()
        assert last_up is not None and "às" in last_up
        print("home_last_update_ok")

        print("SMOKE_ALL_OK")
        return 0
    except Exception:
        traceback.print_exc()
        print("SMOKE_FAILED")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
