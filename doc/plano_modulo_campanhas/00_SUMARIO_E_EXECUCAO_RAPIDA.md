# 00 - SUMÁRIO E GUIA DE EXECUÇÃO RÁPIDA

Este documento orienta a execução técnica e cronológica do Módulo de Campanhas de Exposição e Abastecimento no `ProjetoBak_Sincronizador`.

---

## 📅 Roteiro de Execução por Fases (Passo a Passo)

```
FASE 1: Diagnóstico e Validação da Conexão
   │
   ▼
FASE 2: Criação das Tabelas PostgreSQL e Cargas Iniciais
   │
   ▼
FASE 3: Implementação do Motor de Cálculo e Cubagem (services/campanha_calculo.py) + Testes
   │
   ▼
FASE 4: Desenvolvimento da Página A - Compras (page/campanhas_compras.py)
   │
   ▼
FASE 5: Desenvolvimento da Página C - Supply (page/campanhas_supply.py)
   │
   ▼
FASE 6: Desenvolvimento dos Serviços de Exportação (services/exportacao_campanha.py)
   │
   ▼
FASE 7: Desenvolvimento da Página B - Loja (page/campanhas_loja.py)
   │
   ▼
FASE 8: Telas Administrativas, Auditoria, Expiração Automática e Menu do app.py
```

---

## 🛠️ Detalhamento das Ações em Cada Fase

### Fase 1 — Diagnóstico e Base
- Verificar conectividade com a `DATABASE_URL` (PostgreSQL).
- Verificar presença e integridade do arquivo `bdados/query.parquet`.
- Garantir que as bibliotecas necessárias (`openpyxl`, `pyarrow`, `pandas`, `sqlalchemy`, `streamlit`) estejam funcionais.

### Fase 2 — Banco de Dados Relacional
- Criar a função `create_campanhas_tables(engine)` que executa os DDLs contidos no documento `01_ARQUITETURA_E_MODELAGEM_BD.md`.
- Garantir a criação das 10 tabelas:
  1. `lojas` (14 lojas participantes cadastradas + CDs).
  2. `tipos_exposicao` (Ponta de Gôndola, Meia Ponta, Ilha, Orelha, etc.).
  3. `estruturas_exposicao` (Modelos físicos).
  4. `bandejas_exposicao` (Dimensões de cada nível/bandeja).
  5. `produto_dimensoes` (Dimensões em cm dos SKUs).
  6. `campanhas` (Capa com código, período, status e auditoria).
  7. `campanha_itens` (SKUs vinculados com snapshots de embalagens e descrição).
  8. `campanha_lojas` (Matriz de distribuição item x loja).
  9. `campanha_devolutivas` (Pendências de compra geradas pelo Supply).
  10. `campanha_historico` (Auditoria de alterações).
- Inserir carga inicial padrão de lojas e tipos de exposição.

### Fase 3 — Motor de Cálculo e Cubagem Isolado
- Criar `services/campanha_calculo.py` contendo:
  - `calcular_dias_campanha(data_inicio, data_fim) -> int`
  - `calcular_venda_projetada(venda_media_diaria, dias) -> float`
  - `calcular_capacidade_bandeja(largura_b, prof_b, alt_b, larg_p, prof_p, alt_p) -> int`
  - `calcular_capacidade_exposicao(bandejas, produto_dimensoes, total_skus) -> int`
  - `calcular_caixas_transferencia(volume_final, embl_transferencia) -> tuple[int, int]`
  - `apurar_disponibilidade_cd(caixas_necessarias, estoque_cd_cx) -> tuple[int, int, str]`
- Criar script de teste unitário isolado para validar todos os cálculos matemáticos com os dados de exemplo do briefing.

### Fase 4 — Página A: Compras
- Criar `page/campanhas_compras.py`:
  - Seletor de campanhas ativas/rascunho ou criação de nova campanha (`CMP-YYYY-XXXXXX`).
  - Campo de pesquisa inteligente de produtos no `query.parquet` (código ou descrição).
  - Cards de resumo de estoque (Lojas vs CD15), venda média e venda projetada.
  - Tabela interativa para preenchimento do tipo de exposição e volume sugerido para as 14 lojas.
  - Botões de ação: Salvar Rascunho, Inativar Campanha, Replicar Campanha e Enviar para Supply.
  - Painel de Devolutivas de Compras (acompanhamento de itens com falta de estoque no CD).

### Fase 5 — Página C: Supply
- Criar `page/campanhas_supply.py`:
  - Seletor de campanhas enviadas pelo Compras (`ENVIADA_SUPPLY`, `EM_AVALIACAO_SUPPLY`).
  - Painel de parametrização de dimensões do produto (altura, largura, profundidade em cm) com salvamento em `produto_dimensoes`.
  - Painel de cubagem física por estrutura/bandeja e divisão por SKUs.
  - Tabela de revisão com colunas de sugestão do comprador, sugestão calculada e input do **Volume Final Supply**.
  - Cálculo automático de caixas e cruzamento com estoque do CD15 (`QUANTIDADE_DISPONIVEL`).
  - Identificação de ruptura, marcação de status (`OK`, `PARCIAL`, `SEM_ESTOQUE_CD`) e geração de registros em `campanha_devolutivas`.
  - Card de resumo e botão de "Finalizar Avaliação".

### Fase 6 — Módulo de Exportações
- Criar `services/exportacao_campanha.py`:
  - `gerar_excel_transferencia_operacional(campanha_id)`: Planilha oficial para lançamento de transferências no ERP.
  - `gerar_excel_devolutiva_compras(campanha_id)`: Planilha com a lista de SKUs e caixas a comprar.
  - `gerar_excel_consulta_loja(campanha_id, loja_id)`: Planilha de consulta para a equipe de loja.
  - `gerar_pdf_campanha_loja(campanha_id, loja_id)`: Documento formatado para impressão da loja.

### Fase 7 — Página B: Loja
- Criar `page/campanhas_loja.py`:
  - Identificação automática da loja do usuário logado via `st.session_state["lojas_acesso"]`.
  - Consulta restrita às campanhas ativas e vigentes daquela loja.
  - Visualização dos itens, tipos de exposição e volumes finais aprovados pelo Supply.
  - Botões de exportação (PDF, Excel de consulta e impressão em tela).

### Fase 8 — Telas de Apoio, Expiração e Menu do Sistema
- Criar `page/campanhas_admin_exposicao.py` para administradores gerenciarem tipos de exposição, estruturas e medidas de bandejas.
- Implementar rotina de expiração automática (campanhas com `data_fim < hoje` são tratadas como `INATIVA`).
- Atualizar o menu de navegação em `app.py` integrando os acessos por cargo (`Compras`, `Supply`, `Admin` e `Loja`).
- Criar tabela de auditoria `campanha_historico` acionada em todas as mutações relevantes.

---

## ✅ Checklist de Testes de Aceitação

| # | Teste | Procedimento | Critério de Sucesso |
|---|---|---|---|
| **1** | Permissões | Logar como Loja 001 | Só visualiza itens da Loja 001; não vê dados de CD nem outras lojas. |
| **2** | Busca Parquet | Digitar código ou descrição na Página A | Carrega instantaneamente descrição, estoque das 14 lojas, saldo CD15 e vendas. |
| **3** | Cubagem | Informar produto 10x10x10 cm em bandeja 150x50x20 cm com 3 SKUs | Capacidade calculada: 150 un totais / 50 un por SKU. |
| **4** | Caixas | Volume final = 42 un, embalagem transferência = 24 un | Caixas calculadas = 2 cx (volume real 48 un). |
| **5** | Falta no CD | Necessidade = 5 cx, Estoque CD15 = 3 cx | Transfere 3 cx, aponta 2 cx para compra e gera pendência para Compras. |
| **6** | Replicação | Clicar em "Replicar Campanha" | Gera nova campanha em rascunho com novo ID, clona itens e exige novas datas. |
| **7** | Exportação | Gerar Excel Operacional no Supply | Arquivo `.xlsx` gerado sem erros e com colunas compatíveis com o ERP. |
