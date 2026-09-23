# 01 - ARQUITETURA E MODELAGEM DE BANCO DE DADOS (PostgreSQL)

Este documento descreve a modelagem relacional, constraints, índices e comandos SQL DDL para criar e manter o Módulo de Campanhas de Exposição e Abastecimento no PostgreSQL.

---

## 🏛️ Filosofia de Dados: Parquet vs PostgreSQL

- **`bdados/query.parquet` (Leitura Analítica Diária):**
  - Contém a foto operacional atualizada diariamente do ERP Consinco com: estoques de lojas, estoques de CDs, vendas acumuladas de 30 dias (`QTD_VENDIDA_30D`) e embalagens.
  - **Nunca gravar ou alterar o arquivo Parquet com dados de campanhas.**
- **PostgreSQL (Persistência Transacional & Snapshots):**
  - Armazena todas as campanhas, decisões do comprador, decisões do supply, medidas físicas de bandejas e produtos, histórico de auditoria e pendências de compra.
  - **Gravação de Snapshots:** Ao vincular um produto a uma campanha, salvar a `descricao_snapshot`, `embalagem_compra` e `embalagem_transferencia` para que mudanças futuras no cadastro do ERP não corrompam campanhas passadas.

---

## 📜 Script Completo DDL (PostgreSQL)

```sql
-- ============================================================================
-- 1. TABELA DE LOJAS E CENTROS DE DISTRIBUIÇÃO
-- ============================================================================
CREATE TABLE IF NOT EXISTS lojas (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(10) UNIQUE NOT NULL, -- Ex: '001', '002', '015', '016'
    nome VARCHAR(100) NOT NULL,
    tipo VARCHAR(20) NOT NULL DEFAULT 'LOJA', -- 'LOJA' ou 'CD'
    ativa BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Carga inicial das 14 lojas operacionais e CDs
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

-- ============================================================================
-- 2. TIPOS E ESTRUTURAS DE EXPOSIÇÃO FÍSICA
-- ============================================================================
CREATE TABLE IF NOT EXISTS tipos_exposicao (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(50) UNIQUE NOT NULL, -- 'PONTA DE GÔNDOLA', 'MEIA PONTA', 'ILHA', 'ORELHA'
    descricao TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO tipos_exposicao (nome, descricao, ativo)
VALUES 
    ('PONTA DE GÔNDOLA', 'Exposição em cabeceira de gôndola completa', TRUE),
    ('MEIA PONTA', 'Exposição em metade da cabeceira de gôndola', TRUE),
    ('ILHA', 'Exposição central no corredor em formato de ilha/pallet', TRUE),
    ('ORELHA', 'Exposição lateral acoplada à gôndola', TRUE)
ON CONFLICT (nome) DO NOTHING;

CREATE TABLE IF NOT EXISTS estruturas_exposicao (
    id SERIAL PRIMARY KEY,
    tipo_exposicao_id INTEGER NOT NULL REFERENCES tipos_exposicao(id) ON DELETE CASCADE,
    nome VARCHAR(100) NOT NULL, -- Ex: 'Ponta de Gôndola Padrão A (6 Bandejas)'
    descricao TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS bandejas_exposicao (
    id SERIAL PRIMARY KEY,
    estrutura_exposicao_id INTEGER NOT NULL REFERENCES estruturas_exposicao(id) ON DELETE CASCADE,
    numero_bandeja INTEGER NOT NULL, -- 1 a 6
    largura_cm NUMERIC(10, 2) NOT NULL CHECK (largura_cm > 0),
    profundidade_cm NUMERIC(10, 2) NOT NULL CHECK (profundidade_cm > 0),
    altura_cm NUMERIC(10, 2) NOT NULL CHECK (altura_cm > 0),
    ordem INTEGER NOT NULL DEFAULT 1,
    ativa BOOLEAN NOT NULL DEFAULT TRUE
);

-- ============================================================================
-- 3. DIMENSÕES FÍSICAS DOS PRODUTOS (cm)
-- ============================================================================
CREATE TABLE IF NOT EXISTS produto_dimensoes (
    id SERIAL PRIMARY KEY,
    produto_codigo INTEGER UNIQUE NOT NULL, -- Código Consinco
    altura_cm NUMERIC(10, 2) NOT NULL CHECK (altura_cm > 0),
    largura_cm NUMERIC(10, 2) NOT NULL CHECK (largura_cm > 0),
    profundidade_cm NUMERIC(10, 2) NOT NULL CHECK (profundidade_cm > 0),
    data_atualizacao TIMESTAMP NOT NULL DEFAULT NOW(),
    usuario_atualizacao VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_produto_dimensoes_cod ON produto_dimensoes(produto_codigo);

-- ============================================================================
-- 4. CAMPANHAS (CAPA)
-- ============================================================================
CREATE TABLE IF NOT EXISTS campanhas (
    id VARCHAR(50) PRIMARY KEY, -- UUID técnico ou identificador único
    codigo_campanha VARCHAR(30) UNIQUE NOT NULL, -- Ex: 'CMP-2026-000184'
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

-- ============================================================================
-- 5. ITENS DA CAMPANHA (SKUs)
-- ============================================================================
CREATE TABLE IF NOT EXISTS campanha_itens (
    id SERIAL PRIMARY KEY,
    campanha_id VARCHAR(50) NOT NULL REFERENCES campanhas(id) ON DELETE CASCADE,
    produto_codigo INTEGER NOT NULL,
    descricao_snapshot VARCHAR(255) NOT NULL,
    embalagem_compra INTEGER NOT NULL DEFAULT 1,
    embalagem_transferencia INTEGER NOT NULL DEFAULT 1,
    criado_em TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(campanha_id, produto_codigo)
);

CREATE INDEX IF NOT EXISTS idx_campanha_itens_camp ON campanha_itens(campanha_id);

-- ============================================================================
-- 6. MATRIZ DE DISTRIBUIÇÃO POR LOJA
-- ============================================================================
CREATE TABLE IF NOT EXISTS campanha_lojas (
    id SERIAL PRIMARY KEY,
    campanha_item_id INTEGER NOT NULL REFERENCES campanha_itens(id) ON DELETE CASCADE,
    loja_codigo VARCHAR(10) NOT NULL REFERENCES lojas(codigo),
    tipo_exposicao_id INTEGER REFERENCES tipos_exposicao(id),
    estrutura_exposicao_id INTEGER REFERENCES estruturas_exposicao(id),
    
    -- Volumes
    volume_comprador INTEGER DEFAULT 0, -- Sugestão Compras (pode ser 0/null)
    volume_calculado INTEGER DEFAULT 0, -- Sugestão calculada pela cubagem
    volume_final_supply INTEGER DEFAULT 0, -- Decisão do Supply
    
    -- Caixas e Transferências
    caixas_transferencia INTEGER DEFAULT 0,
    volume_transferencia INTEGER DEFAULT 0, -- caixas * emb_transferencia
    
    -- Snapshots operacionais no momento da análise
    estoque_loja NUMERIC(12, 2) DEFAULT 0,
    estoque_cd NUMERIC(12, 2) DEFAULT 0,
    venda_media NUMERIC(12, 2) DEFAULT 0,
    venda_projetada NUMERIC(12, 2) DEFAULT 0,
    
    -- Status do estoque e faltas
    status_estoque VARCHAR(30) DEFAULT 'OK' CHECK (status_estoque IN ('OK', 'PARCIAL', 'SEM_ESTOQUE_CD')),
    caixas_falta INTEGER DEFAULT 0,
    quantidade_falta NUMERIC(12, 2) DEFAULT 0,
    
    atualizado_em TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(campanha_item_id, loja_codigo)
);

CREATE INDEX IF NOT EXISTS idx_campanha_lojas_item ON campanha_lojas(campanha_item_id);
CREATE INDEX IF NOT EXISTS idx_campanha_lojas_loja ON campanha_lojas(loja_codigo);

-- ============================================================================
-- 7. PENDÊNCIAS E DEVOLUTIVAS PARA COMPRAS
-- ============================================================================
CREATE TABLE IF NOT EXISTS campanha_devolutivas (
    id SERIAL PRIMARY KEY,
    campanha_id VARCHAR(50) NOT NULL REFERENCES campanhas(id) ON DELETE CASCADE,
    produto_codigo INTEGER NOT NULL,
    descricao_snapshot VARCHAR(255) NOT NULL,
    caixas_necessarias INTEGER NOT NULL,
    caixas_cd_disponivel INTEGER NOT NULL,
    caixas_falta INTEGER NOT NULL, -- Quantidade a comprar
    situacao VARCHAR(30) NOT NULL DEFAULT 'PENDENTE' CHECK (situacao IN ('PENDENTE', 'RESOLVIDO', 'COMPRADO')),
    observacao TEXT,
    data_geracao TIMESTAMP NOT NULL DEFAULT NOW(),
    data_resolucao TIMESTAMP,
    usuario_resolucao VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_campanha_devolutivas_camp ON campanha_devolutivas(campanha_id);

-- ============================================================================
-- 8. AUDITORIA E HISTÓRICO DE MUTAÇÕES
-- ============================================================================
CREATE TABLE IF NOT EXISTS campanha_historico (
    id SERIAL PRIMARY KEY,
    campanha_id VARCHAR(50) NOT NULL REFERENCES campanhas(id) ON DELETE CASCADE,
    usuario VARCHAR(100) NOT NULL,
    data_hora TIMESTAMP NOT NULL DEFAULT NOW(),
    acao VARCHAR(50) NOT NULL, -- 'CRIACAO', 'EDICAO', 'REPLICACAO', 'ENVIO_SUPPLY', 'FINALIZACAO', etc.
    campo VARCHAR(100),
    valor_anterior TEXT,
    valor_novo TEXT,
    detalhes TEXT
);

CREATE INDEX IF NOT EXISTS idx_campanha_historico_camp ON campanha_historico(campanha_id);
```

---

## 🔒 Regras de Integridade e Isolamento Transacional
1. **Remoção em Cascata:** Se uma campanha for excluída (ou cancelada em rascunho), seus itens, matriz de lojas e histórico são limpos de forma consistente via `ON DELETE CASCADE`.
2. **Imutabilidade de Decisões:** Quando o status muda para `FINALIZADA`, triggers ou validações na camada de serviço bloqueiam modificações nos volumes sem um estorno formal de status.
3. **Auditoria Contínua:** Toda ação de edição de volume, envio ou finalização registra um evento na tabela `campanha_historico`.
