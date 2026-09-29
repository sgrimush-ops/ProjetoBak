import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from page.admin_fornecedor import get_all_fornecedores_details, create_fornecedores_table
from utils.fornecedores_loader import parse_fornecedores_acesso, format_fornecedores_summary

print("1. Testando parse_fornecedores_acesso com variados tipos:")
casos = [
    None,
    np.nan,
    float('nan'),
    "NaN",
    "none",
    "null",
    "[]",
    "",
    [15134, np.nan, None, "15520"],
    "15134, 15520, NaN",
    15134,
    15134.0,
    "[15134, 15520]"
]

for c in casos:
    res = parse_fornecedores_acesso(c)
    print(f"  Input: {repr(c)} -> Output: {res}")

print("\n2. Testando get_all_fornecedores_details com DB SQLite em memoria:")
engine = create_engine('sqlite:///:memory:')
create_fornecedores_table(engine)

with engine.begin() as conn:
    conn.execute(text("INSERT INTO fornecedores_users (username, password, empresa, role, lojas_acesso, fornecedores_acesso, status_logado) VALUES ('user_null', '123', NULL, 'fornecedor', NULL, NULL, NULL)"))
    conn.execute(text("INSERT INTO fornecedores_users (username, password, empresa, role, lojas_acesso, fornecedores_acesso, status_logado) VALUES ('user_empty', '123', '', 'fornecedor', '[]', '[]', 'DESLOGADO')"))
    conn.execute(text("INSERT INTO fornecedores_users (username, password, empresa, role, lojas_acesso, fornecedores_acesso, status_logado) VALUES ('user_valid', '123', 'Nestle', 'fornecedor', '[\"001\"]', '[15134, 15520]', 'LOGADO')"))

df = get_all_fornecedores_details(engine)
print("DF Resultante:")
print(df)
print("\nSUCESSO: Nenhum erro de conversao ocorreu!")
