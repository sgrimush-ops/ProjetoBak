import sqlite3
import psycopg2
import os
import pandas as pd

# Caminho local do banco SQLite
sqlite_path = "data/database.db"

# Conexão SQLite
sqlite_conn = sqlite3.connect(sqlite_path)
df = pd.read_sql_query("SELECT * FROM usuarios", sqlite_conn)

# Conexão PostgreSQL (Render)
db_url = os.getenv("DATABASE_URL")
pg_conn = psycopg2.connect(db_url)
cur = pg_conn.cursor()

# Cria a tabela no Render (se não existir)
cur.execute("""
CREATE TABLE IF NOT EXISTS usuarios (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    senha TEXT NOT NULL,
    perfil TEXT DEFAULT 'usuario'
);
""")

# Insere usuários
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO usuarios (username, senha, perfil)
        VALUES (%s, %s, %s)
        ON CONFLICT (username) DO NOTHING;
    """, (row['username'], row['senha'], row.get('perfil', 'usuario')))

pg_conn.commit()
cur.close()
pg_conn.close()
sqlite_conn.close()

print("✅ Usuários migrados com sucesso para o Render!")
