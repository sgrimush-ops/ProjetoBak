# PLANO MESTRE DE IMPLEMENTAÇÃO: MÓDULO DE CAMPANHAS DE EXPOSIÇÃO E ABASTECIMENTO

> **Documento de Especificação e Guia de Execução Sob Demanda**  
> **Sistema:** `ProjetoBak_Sincronizador`  
> **Status:** Pronto para Aplicação Direta quando acionado pelo usuário.

---

## 🚀 Como Executar este Plano
Quando chegar a hora de implantar, basta enviar o comando:
```text
Execute o plano de implementação do Módulo de Campanhas localizado em doc/plano_modulo_campanhas/
```

O agente executor deve seguir rigorosamente as 8 fases definidas neste plano de forma incremental.

---

## 📑 Estrutura da Documentação do Plano
Para facilitar a leitura e o desenvolvimento modular, este plano está dividido nos seguintes documentos na pasta `doc/plano_modulo_campanhas/`:

1. [`00_SUMARIO_E_EXECUCAO_RAPIDA.md`](file:///c:/Users/usr/Downloads/Equipes_Agentes/Aplicativos/ProjetoBak_Sincronizador/doc/plano_modulo_campanhas/00_SUMARIO_E_EXECUCAO_RAPIDA.md) - Roteiro de desenvolvimento por fases, critérios de conclusão e checklist de testes.
2. [`01_ARQUITETURA_E_MODELAGEM_BD.md`](file:///c:/Users/usr/Downloads/Equipes_Agentes/Aplicativos/ProjetoBak_Sincronizador/doc/plano_modulo_campanhas/01_ARQUITETURA_E_MODELAGEM_BD.md) - Scripts SQL (DDL) completos das 10 tabelas PostgreSQL, constraints, índices e carga inicial.
3. [`02_ESPECIFICACAO_TELAS_E_FLUXOS.md`](file:///c:/Users/usr/Downloads/Equipes_Agentes/Aplicativos/ProjetoBak_Sincronizador/doc/plano_modulo_campanhas/02_ESPECIFICACAO_TELAS_E_FLUXOS.md) - Especificação detalhada das 3 telas principais (A - Compras, B - Loja, C - Supply) e telas de apoio.
4. [`03_MOTOR_CALCULO_E_CUBAGEM.md`](file:///c:/Users/usr/Downloads/Equipes_Agentes/Aplicativos/ProjetoBak_Sincronizador/doc/plano_modulo_campanhas/03_MOTOR_CALCULO_E_CUBAGEM.md) - Fórmulas matemáticas de projeção de vendas, cubagem por bandeja, rateio de SKUs e conferência de CDs.
5. [`04_EXPORTACOES_E_INTEGRACOES.md`](file:///c:/Users/usr/Downloads/Equipes_Agentes/Aplicativos/ProjetoBak_Sincronizador/doc/plano_modulo_campanhas/04_EXPORTACOES_E_INTEGRACOES.md) - Layouts e especificações técnicas de todos os relatórios e planilhas de importação/exportação.

---

## 🎯 Resumo dos Requisitos do Projeto

### 1. Objetivos do Módulo
Permitir a gestão completa do ciclo de vida de campanhas comerciais de exposição e abastecimento no ERP Totvs Consinco / ProjetoBak:
- **Compras (Página A):** Cadastra campanha, seleciona SKUs no `query.parquet`, define tipo de exposição por loja (Ponta de Gôndola, Meia Ponta, Ilha, Orelha), sugere volume inicial e envia para Supply.
- **Loja (Página B):** Visualização restrita aos itens da sua própria loja logada, com exibição de volumes finais, período de validade e exportação de relatórios.
- **Supply (Página C):** Avaliação de dimensões físicas do produto (cm), cálculo de capacidade de bandejas na exposição física, ajuste do volume final, conversão para caixas fechadas de transferência, apuração de estoque do CD15, emissão de devolutivas para compras em caso de falta e geração do arquivo Excel Operacional de Transferência.

### 2. Regras de Ouro
1. **Não Quebrar Funcionalidades Existentes:** As rotinas de pedidos de CD, aprovações, consumo e uploads mantêm-se 100% inalteradas.
2. **Parquet vs Banco Relacional:** O `query.parquet` continua sendo a fonte diária de leitura analítica (estoques e vendas). Campanhas, decisões, volumes e histórico residem exclusivamente nas tabelas transacionais do PostgreSQL.
3. **Preservação Histórica (Snapshot):** Uma campanha fechada nunca deve ser alterada retroativamente por viradas diárias do parquet.
4. **Isolamento de Camadas:** Fórmulas matemáticas isoladas em `services/campanha_calculo.py` para garantir reuso idêntico entre Compras e Supply.
