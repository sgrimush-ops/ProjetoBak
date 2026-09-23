# 04 - EXPORTAÇÕES, RELATÓRIOS E INTEGRAÇÕES

Este documento define a especificação dos arquivos gerados pelo sistema, garantindo conformidade com os processos operacionais do ERP Totvs Consinco e necessidades das equipes de Compras, Supply e Lojas.

---

## 📑 1. Tipos de Exportação Distintos

O sistema produz **4 tipos de arquivos independentes**, que não devem ser confundidos:

| Tipo de Arquivo | Usuário Alvo | Formato | Finalidade |
| :--- | :--- | :--- | :--- |
| **Excel Operacional de Transferência** | Supply / CD | `.xlsx` | Lançamento oficial de transferências CD $\rightarrow$ Lojas no ERP. |
| **Excel Devolutiva de Compras** | Comprador | `.xlsx` | Relatório de SKUs com falta de saldo no CD e caixas a comprar. |
| **Excel de Consulta da Loja** | Gerente de Loja | `.xlsx` | Consulta simplificada dos itens e volumes da própria loja. |
| **PDF Operacional da Loja** | Equipe de Reposição | `.pdf` | Impressão física para checagem e montagem da gôndola. |

---

## 📊 2. Layouts dos Arquivos Excel

### 2.1 Excel Operacional de Transferência (Supply $\rightarrow$ CD)
- **Engine:** `pandas.ExcelWriter` com `openpyxl`.
- **Nome do Arquivo:** `transferencia_campanha_[CODIGO_CAMPANHA]_[DATA].xlsx`
- **Aba:** `Transferencias_CD`
- **Colunas:**
  1. `Código Consinco` (Numérico inteiro)
  2. `Descrição do Produto` (Texto)
  3. `Loja Destino` (Ex: `001`, `002`, ..., `018`)
  4. `Quantidade em Caixas` (Inteiro)
  5. `Embalagem Transferência` (Inteiro)
  6. `Volume Total em Unidades` (Inteiro)
  7. `Tipo de Exposição` (Texto, ex: `Ponta de Gôndola`)
  8. `Campanha` (Código da Campanha)

### 2.2 Excel de Devolutiva para Compras (Supply $\rightarrow$ Compras)
- **Nome do Arquivo:** `devolutiva_compras_[CODIGO_CAMPANHA]_[DATA].xlsx`
- **Aba:** `Pendencias_Compra`
- **Colunas:**
  1. `Código Consinco`
  2. `Descrição do Produto`
  3. `Caixas Totais Necessárias`
  4. `Caixas Disponíveis CD15`
  5. `Caixas Faltantes (Comprar)`
  6. `Embalagem Compra`
  7. `Sugestão Pedido Fornecedor (Caixas)`
  8. `Situação` (`Pendente`, `Em Cotação`, `Comprado`)

### 2.3 Excel de Consulta da Loja (Loja)
- **Nome do Arquivo:** `campanha_[CODIGO_CAMPANHA]_loja_[CODIGO_LOJA].xlsx`
- **Aba:** `Itens_Campanha`
- **Colunas:**
  1. `Campanha`
  2. `Vigência Início`
  3. `Vigência Fim`
  4. `Código Produto`
  5. `Descrição`
  6. `Tipo de Exposição`
  7. `Volume Final (Unidades)`
  8. `Caixas a Receber`

---

## 📄 3. Especificação do PDF Operacional da Loja

- **Formato:** Página A4 (Retrato ou Paisagem).
- **Cabeçalho:** Logotipo Baklizi + Nome da Campanha + Identificação da Loja + Período de Vigência.
- **Tabela Principal:**
  - Código | Descrição | Exposição | Volume Unidades | Caixas | Checkbox Conferência Física $[\ ]$
- **Rodapé:** Data e hora de emissão + Assinatura do Encarregado de Loja.
- **Geração Técnica:** Geração HTML/CSS formatado para impressão nativa do navegador (`window.print()`) ou renderização em PDF via biblioteca Python em memória.
