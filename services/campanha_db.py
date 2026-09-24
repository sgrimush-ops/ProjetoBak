"""
services/campanha_db.py
Gerenciador de Banco de Dados e DDLs para o Módulo de Campanhas de Exposição e Abastecimento.
"""

from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def create_campanhas_tables(engine):
    """
    Cria as 10 tabelas relacionais do módulo de campanhas e insere os dados iniciais.
    Totalmente idempotente (IF NOT EXISTS e ON CONFLICT DO NOTHING).
    """
    try:
        with engine.begin() as conn:
            # 1. TABELA DE LOJAS E CENTROS DE DISTRIBUIÇÃO
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS lojas (
                    id SERIAL PRIMARY KEY,
                    codigo VARCHAR(10) UNIQUE NOT NULL,
                    nome VARCHAR(100) NOT NULL,
                    tipo VARCHAR(20) NOT NULL DEFAULT 'LOJA',
                    ativa BOOLEAN NOT NULL DEFAULT TRUE,
                    criado_em TIMESTAMP NOT NULL DEFAULT NOW()
                );
            """))

            # Carga inicial das 14 lojas e CDs
            conn.execute(text("""
                INSERT INTO lojas (codigo, nome, tipo, ativa)
                VALUES 
                    ('001', 'Loja 001 - Matriz', 'LOJA', TRUE),
                    ('002', 'Loja 002', 'LOJA', TRUE),
                    ('003', 'Loja 003', 'LOJA', TRUE),
                    ('004', 'Loja 004', 'LOJA', TRUE),
                    ('005', 'Loja 005', 'LOJA', TRUE),
                    ('006', 'Loja 006', 'LOJA', TRUE),
                    ('007', 'Loja 007', 'LOJA', TRUE),
                    ('008', 'Loja 008', 'LOJA', TRUE),
                    ('011', 'Loja 011', 'LOJA', TRUE),
                    ('012', 'Loja 012', 'LOJA', TRUE),
                    ('013', 'Loja 013', 'LOJA', TRUE),
                    ('014', 'Loja 014', 'LOJA', TRUE),
                    ('017', 'Loja 017', 'LOJA', TRUE),
                    ('018', 'Loja 018', 'LOJA', TRUE),
                    ('015', 'CD 15 - Distribuição Principal', 'CD', TRUE),
                    ('016', 'CD 16 - Distribuição Secundário', 'CD', TRUE)
                ON CONFLICT (codigo) DO NOTHING;
            """))

            # 2. TIPOS E ESTRUTURAS DE EXPOSIÇÃO FÍSICA
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS tipos_exposicao (
                    id SERIAL PRIMARY KEY,
                    nome VARCHAR(50) UNIQUE NOT NULL,
                    descricao TEXT,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE
                );
            """))

            conn.execute(text("""
                INSERT INTO tipos_exposicao (nome, descricao, ativo)
                VALUES 
                    ('INATIVA', 'Loja não participante da exposição / sem oferta', TRUE),
                    ('PONTA DE GÔNDOLA', 'Exposição em cabeceira de gôndola completa', TRUE),
                    ('MEIA PONTA', 'Exposição em metade da cabeceira de gôndola', TRUE),
                    ('ILHA', 'Exposição central no corredor em formato de ilha/pallet', TRUE),
                    ('ORELHA', 'Exposição lateral acoplada à gôndola', TRUE)
                ON CONFLICT (nome) DO NOTHING;
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS estruturas_exposicao (
                    id SERIAL PRIMARY KEY,
                    tipo_exposicao_id INTEGER NOT NULL REFERENCES tipos_exposicao(id) ON DELETE CASCADE,
                    nome VARCHAR(100) NOT NULL,
                    descricao TEXT,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS bandejas_exposicao (
                    id SERIAL PRIMARY KEY,
                    estrutura_exposicao_id INTEGER NOT NULL REFERENCES estruturas_exposicao(id) ON DELETE CASCADE,
                    numero_bandeja INTEGER NOT NULL,
                    largura_cm NUMERIC(10, 2) NOT NULL CHECK (largura_cm > 0),
                    profundidade_cm NUMERIC(10, 2) NOT NULL CHECK (profundidade_cm > 0),
                    altura_cm NUMERIC(10, 2) NOT NULL CHECK (altura_cm > 0),
                    ordem INTEGER NOT NULL DEFAULT 1,
                    ativa BOOLEAN NOT NULL DEFAULT TRUE
                );
            """))

            # Carga de estruturas e bandejas padrão se não houver estruturas cadastradas
            res_est = conn.execute(text("SELECT COUNT(*) FROM estruturas_exposicao")).scalar()
            if res_est == 0:
                # Obter id de PONTA DE GÔNDOLA
                id_pg = conn.execute(text("SELECT id FROM tipos_exposicao WHERE nome = 'PONTA DE GÔNDOLA'")).scalar()
                if id_pg:
                    id_est = conn.execute(text("""
                        INSERT INTO estruturas_exposicao (tipo_exposicao_id, nome, descricao, ativo)
                        VALUES (:tipo_id, 'Ponta de Gôndola Padrão A (6 Bandejas)', 'Estrutura padrão de 6 níveis com 150cm de largura', TRUE)
                        RETURNING id;
                    """), {"tipo_id": id_pg}).scalar()

                    for n in range(1, 7):
                        conn.execute(text("""
                            INSERT INTO bandejas_exposicao (estrutura_exposicao_id, numero_bandeja, largura_cm, profundidade_cm, altura_cm, ordem, ativa)
                            VALUES (:est_id, :num, 150.0, 50.0, 25.0, :num, TRUE)
                        """), {"est_id": id_est, "num": n})

                # Meia ponta padrão
                id_mp = conn.execute(text("SELECT id FROM tipos_exposicao WHERE nome = 'MEIA PONTA'")).scalar()
                if id_mp:
                    id_est_mp = conn.execute(text("""
                        INSERT INTO estruturas_exposicao (tipo_exposicao_id, nome, descricao, ativo)
                        VALUES (:tipo_id, 'Meia Ponta Padrão (6 Bandejas)', 'Estrutura de 6 níveis com 75cm de largura', TRUE)
                        RETURNING id;
                    """), {"tipo_id": id_mp}).scalar()

                    for n in range(1, 7):
                        conn.execute(text("""
                            INSERT INTO bandejas_exposicao (estrutura_exposicao_id, numero_bandeja, largura_cm, profundidade_cm, altura_cm, ordem, ativa)
                            VALUES (:est_id, :num, 75.0, 50.0, 25.0, :num, TRUE)
                        """), {"est_id": id_est_mp, "num": n})

                # Ilha padrão
                id_il = conn.execute(text("SELECT id FROM tipos_exposicao WHERE nome = 'ILHA'")).scalar()
                if id_il:
                    id_est_il = conn.execute(text("""
                        INSERT INTO estruturas_exposicao (tipo_exposicao_id, nome, descricao, ativo)
                        VALUES (:tipo_id, 'Ilha / Pallet Padrão (1 Nível)', 'Estrutura aberta para pallet (120x100x120cm)', TRUE)
                        RETURNING id;
                    """), {"tipo_id": id_il}).scalar()

                    conn.execute(text("""
                        INSERT INTO bandejas_exposicao (estrutura_exposicao_id, numero_bandeja, largura_cm, profundidade_cm, altura_cm, ordem, ativa)
                        VALUES (:est_id, 1, 120.0, 100.0, 120.0, 1, TRUE)
                    """), {"est_id": id_est_il})

                # Orelha padrão
                id_or = conn.execute(text("SELECT id FROM tipos_exposicao WHERE nome = 'ORELHA'")).scalar()
                if id_or:
                    id_est_or = conn.execute(text("""
                        INSERT INTO estruturas_exposicao (tipo_exposicao_id, nome, descricao, ativo)
                        VALUES (:tipo_id, 'Orelha Lateral (4 Níveis)', 'Exposição vertical lateral compacta', TRUE)
                        RETURNING id;
                    """), {"tipo_id": id_or}).scalar()

                    for n in range(1, 5):
                        conn.execute(text("""
                            INSERT INTO bandejas_exposicao (estrutura_exposicao_id, numero_bandeja, largura_cm, profundidade_cm, altura_cm, ordem, ativa)
                            VALUES (:est_id, :num, 40.0, 30.0, 20.0, :num, TRUE)
                        """), {"est_id": id_est_or, "num": n})

            # 3. DIMENSÕES FÍSICAS DOS PRODUTOS (cm)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS produto_dimensoes (
                    id SERIAL PRIMARY KEY,
                    produto_codigo INTEGER UNIQUE NOT NULL,
                    altura_cm NUMERIC(10, 2) NOT NULL CHECK (altura_cm > 0),
                    largura_cm NUMERIC(10, 2) NOT NULL CHECK (largura_cm > 0),
                    profundidade_cm NUMERIC(10, 2) NOT NULL CHECK (profundidade_cm > 0),
                    data_atualizacao TIMESTAMP NOT NULL DEFAULT NOW(),
                    usuario_atualizacao VARCHAR(100)
                );
                CREATE INDEX IF NOT EXISTS idx_produto_dimensoes_cod ON produto_dimensoes(produto_codigo);
            """))

            # 4. CAMPANHAS (CAPA)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS campanhas (
                    id VARCHAR(50) PRIMARY KEY,
                    codigo_campanha VARCHAR(30) UNIQUE NOT NULL,
                    nome VARCHAR(150) NOT NULL,
                    data_inicio DATE NOT NULL,
                    data_fim DATE NOT NULL,
                    status VARCHAR(30) NOT NULL DEFAULT 'RASCUNHO' 
                        CHECK (status IN (
                            'RASCUNHO', 
                            'ATIVA', 
                            'ENVIADA_SUPPLY', 
                            'EM_AVALIACAO_SUPPLY', 
                            'PENDENCIA_COMPRAS', 
                            'FINALIZADA', 
                            'INATIVA', 
                            'CANCELADA'
                        )),
                    usuario_criacao VARCHAR(100) NOT NULL,
                    data_criacao TIMESTAMP NOT NULL DEFAULT NOW(),
                    usuario_atualizacao VARCHAR(100),
                    data_atualizacao TIMESTAMP NOT NULL DEFAULT NOW(),
                    campanha_origem_id VARCHAR(50) REFERENCES campanhas(id) ON DELETE SET NULL,
                    observacoes TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_campanhas_status ON campanhas(status);
                CREATE INDEX IF NOT EXISTS idx_campanhas_datas ON campanhas(data_inicio, data_fim);
            """))

            # 5. ITENS DA CAMPANHA (SKUs)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS campanha_itens (
                    id SERIAL PRIMARY KEY,
                    campanha_id VARCHAR(50) NOT NULL REFERENCES campanhas(id) ON DELETE CASCADE,
                    produto_codigo INTEGER NOT NULL,
                    descricao_snapshot VARCHAR(255) NOT NULL,
                    fornecedor VARCHAR(150),
                    departamento VARCHAR(100),
                    comprador VARCHAR(100),
                    embalagem_compra INTEGER NOT NULL DEFAULT 1,
                    embalagem_transferencia INTEGER NOT NULL DEFAULT 1,
                    criado_em TIMESTAMP NOT NULL DEFAULT NOW(),
                    UNIQUE(campanha_id, produto_codigo)
                );
                CREATE INDEX IF NOT EXISTS idx_campanha_itens_camp ON campanha_itens(campanha_id);

                ALTER TABLE campanha_itens ADD COLUMN IF NOT EXISTS fornecedor VARCHAR(150);
                ALTER TABLE campanha_itens ADD COLUMN IF NOT EXISTS departamento VARCHAR(100);
                ALTER TABLE campanha_itens ADD COLUMN IF NOT EXISTS comprador VARCHAR(100);
            """))

            # 6. MATRIZ DE DISTRIBUIÇÃO POR LOJA
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS campanha_lojas (
                    id SERIAL PRIMARY KEY,
                    campanha_item_id INTEGER NOT NULL REFERENCES campanha_itens(id) ON DELETE CASCADE,
                    loja_codigo VARCHAR(10) NOT NULL REFERENCES lojas(codigo),
                    tipo_exposicao_id INTEGER REFERENCES tipos_exposicao(id),
                    estrutura_exposicao_id INTEGER REFERENCES estruturas_exposicao(id),
                    volume_comprador INTEGER DEFAULT 0,
                    volume_calculado INTEGER DEFAULT 0,
                    volume_final_supply INTEGER DEFAULT 0,
                    caixas_transferencia INTEGER DEFAULT 0,
                    volume_transferencia INTEGER DEFAULT 0,
                    estoque_loja NUMERIC(12, 2) DEFAULT 0,
                    estoque_cd NUMERIC(12, 2) DEFAULT 0,
                    venda_media NUMERIC(12, 2) DEFAULT 0,
                    venda_projetada NUMERIC(12, 2) DEFAULT 0,
                    status_estoque VARCHAR(30) DEFAULT 'OK' CHECK (status_estoque IN ('OK', 'PARCIAL', 'SEM_ESTOQUE_CD')),
                    caixas_falta INTEGER DEFAULT 0,
                    quantidade_falta NUMERIC(12, 2) DEFAULT 0,
                    atualizado_em TIMESTAMP NOT NULL DEFAULT NOW(),
                    UNIQUE(campanha_item_id, loja_codigo)
                );
                CREATE INDEX IF NOT EXISTS idx_campanha_lojas_item ON campanha_lojas(campanha_item_id);
                CREATE INDEX IF NOT EXISTS idx_campanha_lojas_loja ON campanha_lojas(loja_codigo);
            """))

            # 7. PENDÊNCIAS E DEVOLUTIVAS PARA COMPRAS
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS campanha_devolutivas (
                    id SERIAL PRIMARY KEY,
                    campanha_id VARCHAR(50) NOT NULL REFERENCES campanhas(id) ON DELETE CASCADE,
                    produto_codigo INTEGER NOT NULL,
                    descricao_snapshot VARCHAR(255) NOT NULL,
                    fornecedor VARCHAR(150),
                    caixas_necessarias INTEGER NOT NULL,
                    caixas_cd_disponivel INTEGER NOT NULL,
                    caixas_falta INTEGER NOT NULL,
                    situacao VARCHAR(30) NOT NULL DEFAULT 'PENDENTE' CHECK (situacao IN ('PENDENTE', 'RESOLVIDO', 'COMPRADO')),
                    observacao TEXT,
                    data_geracao TIMESTAMP NOT NULL DEFAULT NOW(),
                    data_resolucao TIMESTAMP,
                    usuario_resolucao VARCHAR(100)
                );
                CREATE INDEX IF NOT EXISTS idx_campanha_devolutivas_camp ON campanha_devolutivas(campanha_id);

                ALTER TABLE campanha_devolutivas ADD COLUMN IF NOT EXISTS fornecedor VARCHAR(150);
            """))

            # 8. AUDITORIA E HISTÓRICO DE MUTAÇÕES
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS campanha_historico (
                    id SERIAL PRIMARY KEY,
                    campanha_id VARCHAR(50) NOT NULL REFERENCES campanhas(id) ON DELETE CASCADE,
                    usuario VARCHAR(100) NOT NULL,
                    data_hora TIMESTAMP NOT NULL DEFAULT NOW(),
                    acao VARCHAR(50) NOT NULL,
                    campo VARCHAR(100),
                    valor_anterior TEXT,
                    valor_novo TEXT,
                    detalhes TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_campanha_historico_camp ON campanha_historico(campanha_id);
            """))

        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabelas de campanhas: {e}")
        raise e
