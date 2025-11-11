from sqlalchemy import create_engine, text
import os

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("❌ DATABASE_URL não encontrada. Configure no Render.")
    exit()

engine = create_engine(DATABASE_URL, connect_args={'sslmode': 'require'})

sql_script = """
CREATE TABLE IF NOT EXISTS pedidos_consolidados (
    id SERIAL PRIMARY KEY,
    codigo TEXT NOT NULL,
    produto TEXT NOT NULL,
    ean TEXT,
    embseparacao INTEGER DEFAULT 0,
    data_pedido TIMESTAMP NOT NULL,
    data_aprovacao TIMESTAMP NULL,
    usuario_pedido TEXT,
    status_item TEXT,
    loja_001 INTEGER DEFAULT 0,
    loja_002 INTEGER DEFAULT 0,
    loja_003 INTEGER DEFAULT 0,
    loja_004 INTEGER DEFAULT 0,
    loja_005 INTEGER DEFAULT 0,
    loja_006 INTEGER DEFAULT 0,
    loja_007 INTEGER DEFAULT 0,
    loja_008 INTEGER DEFAULT 0,
    loja_011 INTEGER DEFAULT 0,
    loja_012 INTEGER DEFAULT 0,
    loja_013 INTEGER DEFAULT 0,
    loja_014 INTEGER DEFAULT 0,
    loja_017 INTEGER DEFAULT 0,
    loja_018 INTEGER DEFAULT 0,
    total_cx INTEGER DEFAULT 0,
    status_aprovacao TEXT DEFAULT 'Pendente'
);

CREATE INDEX IF NOT EXISTS idx_pedidos_codigo ON pedidos_consolidados (codigo);
CREATE INDEX IF NOT EXISTS idx_pedidos_usuario ON pedidos_consolidados (usuario_pedido);
CREATE INDEX IF NOT EXISTS idx_pedidos_data ON pedidos_consolidados (data_pedido);
CREATE INDEX IF NOT EXISTS idx_pedidos_status ON pedidos_consolidados (status_aprovacao);
"""

with engine.begin() as conn:
    conn.execute(text(sql_script))

print("✅ Banco PostgreSQL criado/configurado com sucesso!")
