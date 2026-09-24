# 📐 Plano de Implementação: Família de Produtos (`SEQFAMILIA`) e Rateio de Exposição

## 🎯 1. Objetivo da Funcionalidade

Permitir que o cadastro de produtos em campanhas reconheça automaticamente quando um SKU pertence a uma **Família de Produtos** (ex: *Refresco Tang com 5 sabores* dividindo uma mesma ilha ou ponta de gôndola), realizando:
1. **Agrupamento Inteligente no Compras:** Inclusão conjunta de todos os SKUs ativos da família com seleção do mix participante;
2. **Cálculo Físico Automático de Capacidade Dividida:** A cubagem da estrutura de exposição (ex: 100 caixas na Ilha) é dividida proporcionalmente pelo número de SKUs participantes (ex: 20 caixas por sabor);
3. **Ajuste Fino por Estoque Loja a Loja no Supply:** O Supply visualiza o estoque e giro de cada sabor individualmente por loja, podendo zerar ou diminuir o abastecimento dos sabores que a loja já possui em estoque excedente;
4. **Devolutiva Precisa para Compras:** O fechamento de compras e faltas no CD15 reflete exatamente a necessidade líquida de cada SKU irmão, evitando compras de sabores com saldo em loja.

---

## 🏗️ 2. Arquitetura de Dados e Banco PostgreSQL

### Alterações no Esquema de Banco (`services/campanha_db.py`):
1. Adicionar colunas em `campanha_itens`:
   - `codigo_familia INTEGER` (Código `SEQFAMILIA` vindo do ERP Consinco);
   - `descricao_familia VARCHAR(255)` (Descrição da Família);
   - `familia_grupo_id VARCHAR(50)` (Identificador de agrupamento da família na campanha);
2. Criar índices para busca rápida por família: `idx_campanha_itens_familia`.

---

## 🔄 3. Fluxo Operacional Ponta a Ponta

1. **Comprador busca produto no Catálogo:** (ex: Tang Abacaxi)
2. **Sistema detecta Família:** Localiza `SEQFAMILIA` e lista todos os SKUs irmãos ativos no `query.parquet`.
3. **Seleção de Mix:** Comprador escolhe incluir a família toda ou selecionar os sabores específicos participantes.
4. **Definição de Exposição:** Comprador seleciona as lojas ativas para a Ilha/Ponta uma única vez para o grupo familiar.
5. **Avaliação no Supply:**
   - O Supply recebe os SKUs agrupados por Família.
   - O campo de cubagem `Nº de SKUs Compartilhados` assume automaticamente a quantidade de sabores da família.
   - A capacidade total calculada (ex: 100 cx) é rateada igualmente (20 cx por sabor).
   - O Supply ajusta os volumes por loja conforme o estoque local de cada sabor.
6. **Cruzamento CD15 e Devolutivas:**
   - O CD15 confronta individualmente cada SKU.
   - A Devolutiva para Compras é gerada apenas com as faltas reais de cada sabor.

---

## 📝 4. Detalhamento dos Módulos a Modificar

### 1. `services/campanha_service.py`
- **`carregar_dados_produto_consolidado`:**
  - Extrair `CODIGO_FAMILIA` e `DESCRICAO_FAMILIA` do `query.parquet`.
- **`buscar_skus_familia(engine, codigo_familia, ...)`:**
  - Método para listar todos os produtos irmãos que compartilham o mesmo `CODIGO_FAMILIA` e possuem `STATUS_COMPRA = 'Ativo'`.
- **`salvar_familia_campanha_compras(...)`:**
  - Salvar todos os SKUs da família em lote, aplicando a mesma configuração de lojas e exposição.

### 2. `page/campanhas_compras.py` (Visão Comprador)
- Ao selecionar um produto que possui família com múltiplos SKUs:
  - Exibir card informativo: *`👨‍👩‍👧‍👦 Família de Produtos Detectada: {N} SKUs ativos no catálogo`*.
  - Permitir selecionar/desmarcar sabores específicos antes de salvar.
  - Ao salvar, inclui automaticamente os SKUs selecionados com o rateio pré-configurado.

### 3. `page/campanhas_supply.py` (Visão Supply)
- Na Seção de Cubagem:
  - O campo `Nº de SKUs Compartilhados` é preenchido automaticamente com a quantidade de SKUs ativos daquela família no ponto extra.
  - A capacidade calculada por SKU é obtida por:
    $$\text{Capacidade por SKU} = \left\lfloor \frac{\text{Capacidade Total da Estrutura}}{\text{Total de SKUs na Família}} \right\rfloor$$
- Na Seção Loja a Loja:
  - Exibir visão lado a lado ou agrupada dos sabores da família para a loja.
  - Permitir que o Supply veja o estoque de cada sabor e ajuste o volume (ex: zerar o sabor que já está cheio na loja).

### 4. `services/exportacao_campanha.py`
- Relatórios de transferência e consulta de loja agrupados por Fornecedor e Família.
- Devolutiva de Compras com discriminação sabor a sabor das faltas no CD15.

---

## 🚀 5. Próximo Passo

Assim que você atualizar o arquivo `bdados/query.parquet` com as novas colunas e avisar aqui no chat:
1. Validaremos a estrutura das novas colunas no parquet;
2. Executaremos a migração do banco PostgreSQL;
3. Implementaremos a lógica no backend e nas páginas de Compras e Supply;
4. Executaremos a suíte de testes e sincronizaremos via `sub_commit.py`.
