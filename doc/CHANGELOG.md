# Histórico de Atualizações - ProjetoBak

## 📅 24/09/2026 - Versão 2.1.0 - MÓDULO DE CAMPANHAS DE EXPOSIÇÃO E ABASTECIMENTO

### 🎯 Gestão de Campanhas (Compras) — `page/campanhas_compras.py`
- ✅ Criação e edição de campanhas com gerador automático de código único (`CMP-YYYY-XXXXXX`) e status rastreável (`RASCUNHO`, `ATIVA`, `ENVIADA_SUPPLY`, `EM_AVALIACAO_SUPPLY`, `PENDENCIA_COMPRAS`, `FINALIZADA`, `INATIVA`).
- ✅ Busca inteligente de produtos integrada ao catálogo do ERP Consinco (`bdados/query.parquet`).
- ✅ Cards analíticos com estoque consolidado de lojas, saldo CD15, venda média diária e projeção de vendas para o período da campanha.
- ✅ Parametrização da matriz de 14 lojas com seleção de tipo de exposição (Ponta de Gôndola, Meia Ponta, Ilha, Orelha) e sugestão inicial de volume.
- ✅ Painel de acompanhamento e resolução de Devolutivas de Compras (faltas de estoque no CD15).
- ✅ Funcionalidade de Replicação de Campanhas com novo período e clonagem estrutural de mix.

### 📦 Avaliação Física e Fechamento Operacional (Supply) — `page/campanhas_supply.py`
- ✅ Cadastro e persistência de dimensões físicas em centímetros (altura, largura, profundidade) na tabela `produto_dimensoes`.
- ✅ Motor de cubagem por bandeja e estrutura física com rateio inteligente entre múltiplos SKUs compartilhados.
- ✅ Grid de fechamento loja a loja com cálculo automatizado de caixas fechadas de transferência (`calcular_caixas_transferencia`).
- ✅ Confrontação em tempo real com o estoque disponível do CD15 (`QUANTIDADE_DISPONIVEL`), classificação de ruptura (`OK`, `PARCIAL`, `SEM_ESTOQUE_CD`) e geração automática de registros de pendência em `campanha_devolutivas`.

### 🏪 Visualização Operacional da Loja — `page/campanhas_loja.py`
- ✅ Consulta restrita e segura às campanhas vigentes para a loja vinculada ao usuário logado (`st.session_state["lojas_acesso"]`).
- ✅ Exibição de produtos, tipo de exposição, volume aprovado e caixas a receber (com dados de custos e CDs estritamente ocultados).
- ✅ Exportação de documento PDF formatado com checklist para impressão e conferência em prancheta (`ReportLab`).
- ✅ Exportação de planilha Excel simplificada para consulta do gerente de loja.

### 📑 Motor de Exportações — `services/exportacao_campanha.py`
- ✅ Geração de Excel Operacional de Transferência (`Supply -> CD`) formatado para digitação no ERP Consinco.
- ✅ Geração de Excel de Devolutivas de Compras com caixas necessárias, disponíveis e faltantes.
- ✅ Geração de Excel e PDF para conferência de lojas físicas.

### 🏛️ Arquitetura e Banco de Dados Relacional — `services/campanha_db.py` & `services/campanha_service.py`
- ✅ Criação idempotente das 10 tabelas relacionais no PostgreSQL: `lojas`, `tipos_exposicao`, `estruturas_exposicao`, `bandejas_exposicao`, `produto_dimensoes`, `campanhas`, `campanha_itens`, `campanha_lojas`, `campanha_devolutivas`, `campanha_historico`.
- ✅ Rotina de expiração automática de campanhas vencidas e auditoria contínua de mutações em `campanha_historico`.
- ✅ Cargas iniciais de 14 lojas, CDs, tipos de exposição e estruturas/bandejas padrão.

---

## 📅 23/02/2026 - Versão 2.0.4 - AJUSTES DE PEDIDO CD E APROVAÇÃO

### 📦 Pedido por Código (CD)
- ✅ Seleção de CD abastecedor obrigatória antes do envio (`CD15` ou `CD16`)
- ✅ Seleção de CD alterada para clique único (sem lista suspensa)
- ✅ Origem do pedido CD passa a ser gravada em `origem_pedido` como `CD15` ou `CD16`
- ✅ Lista de pendentes da tela CD exibe `Origem/CD`
- ✅ Aviso na tela quando o mesmo usuário lança o mesmo item novamente no mesmo dia

### ✅ Aprovação de Pedidos
- ✅ Correção do botão `Marcar Todos` para persistir seleção corretamente até aprovar/reprovar
- ✅ Filtro de origem "Pedido por Código (CD)" inclui pedidos legados e novos (`Pedido por Código (CD)`, `CD15`, `CD16`)
- ✅ Coluna de origem exibe o CD abastecedor quando aplicável

### 📥 Exportação de aprovados (Excel)
- ✅ Download limitado a pedidos aprovados nos últimos 5 minutos
- ✅ Nome padrão do arquivo alterado para `pedido.xlsx`
- ✅ Exportação consolidada por dia + loja + item, somando quantidades duplicadas
- ✅ Inclusão de usuários participantes com contagem por repetição (ex.: `usuario (2)`)

### 🧾 Upload/Leitura de `con5cod.parquet`
- ✅ Normalização de nomes de colunas com espaços/quebras (`strip`) para evitar falha de mapeamento
- ✅ Compatibilidade ampliada para variações de cabeçalho (`Código Produto`/`Codigo Produto`, `Empresa : Produto`/`Empresa: Produto`)
- ✅ Correção de leitura de descrição na tela de Pedido CD

### 🔧 Arquivos impactados
- `page/admin_uploads.py`
- `page/aprovacao_pedidos.py`
- `page/pedido_cd.py`
- `utils/produtos_loader.py`
- `bdados/con5cod.parquet`

## 📅 19/02/2026 - Versão 2.0.3 - AJUSTES DE LOJA E PEDIDO DE CONSUMO

### 🏬 Acesso a lojas
- ✅ Incluída a loja `016` nas listas de acesso do sistema (usuários e fornecedores)
- ✅ Ajustada normalização de lojas legadas para exibição no formato `016` (sem prefixo `loja_`)

### 📦 Pedido de Consumo
- ✅ Histórico da tela de consumo agora exibe apenas pedidos com origem `Pedido de Consumo`
- ✅ Removido botão `Limpar busca` na seção de pedido rápido por setor para eliminar erro de interface

### 🗄️ Banco de dados
- ✅ Adicionada rotina de migração automática para garantir criação de colunas de loja faltantes (incluindo `loja_016`) em bases já existentes

### 🔧 Arquivos impactados
- `app.py`
- `main.py`
- `page/admin_maint.py`
- `page/admin_fornecedor.py`
- `page/area_fornecedor.py`
- `page/pedido_cd.py`
- `page/pedido_consumo.py`
- `page/aprovacao_pedidos.py`
- `page/solicitacao_acesso.py`

## 📅 18/02/2026 - Versão 2.0.2 - LIMPEZA DE RECURSOS OBSOLETOS

### 🧹 Remoções
- ❌ Removidos scripts obsoletos de verificação:
  - `tools/verify_produtos_integrity.py`
  - `tools/verify_brt_timestamps.py`
- ❌ Removida exposição detalhada de stack trace no login de fornecedor (`main.py`)

### 📝 Documentação atualizada
- `README.md`
- `doc/README_PRINCIPAL.md`
- `tools/README.md`
- `doc/COMO_OBTER_DATABASE_URL.md`

## 📅 18/02/2026 - Versão 2.0.1 - PADRONIZAÇÃO DE HORÁRIO (BRT)

### 🕒 Correção de timezone
- Padronização de gravações de data/hora para `America/Sao_Paulo`
- Ajuste de telas de pedidos, aprovação, contato e status para usar horário de Brasília
- Ajuste de consultas temporais (ex.: últimos 30 dias) com referência explícita em BRT

### 🗄️ Banco de dados
- Conexões do sistema e scripts agora configuram timezone da sessão PostgreSQL para `America/Sao_Paulo`
- Redução de divergências entre horário exibido e horário gravado

### 🧪 Nova ferramenta de homologação
- ✅ `tools/verify_brt_timestamps.py`
  - Verifica timezone da sessão
  - Compara relógio do banco com referência Brasília
  - Inspeciona últimos timestamps em tabelas críticas

### 📝 Documentação atualizada
- `README.md`
- `doc/README_PRINCIPAL.md`
- `tools/README.md`

## 📅 02/02/2026 - Versão 2.0.0 - REESTRUTURAÇÃO COMPLETA

### 🔄 MUDANÇAS CRÍTICAS - BREAKING CHANGES

#### Nova Estrutura de Códigos de Produto
- **Código Principal:** Migrado para `cod_consinco` (sistema Consinco)
- **Arquivo de Dados:** `bdados/con5cod.parquet`
- **Estrutura de Colunas:**
  - `cod_consinco` - Código principal do produto (novo)
  - `descricao` - Descrição atualizada do produto
  - `transicao` - Código antigo do sistema anterior (legado)
  - `Mix` - Status do produto: A (Ativo) ou S (Suspenso)
  - `Emb` - Quantidade da embalagem (essencial)

### ✨ Novas Funcionalidades

#### Nova Página: Consulta de Mix 🔍
- **Arquivo:** `page/consulta_mix.py`
- **Funcionalidades:**
  - Busca por código Consinco
  - Busca por descrição do produto
  - Filtro automático de produtos ativos (Mix = A)
  - Visualização de produtos suspensos
  - Filtros por quantidade de embalagem
  - Exportação para CSV
  - Estatísticas do mix (ativos/suspensos)
- **Acesso:** Disponível para todos os usuários logados

### 🗑️ Páginas Removidas
As seguintes páginas foram **completamente removidas** do sistema:
- ❌ `page/consulta_cd.py` - Substituída por `consulta_mix.py`
- ❌ `page/dashboard_online.py` - Funcionalidade descontinuada
- ❌ `page/upload_ofertas.py` - Sistema de ofertas descontinuado
- ❌ `page/gestao_promo.py` - Gestão de promoções descontinuada
- ❌ `page/ver_ofertas.py` - Visualização de ofertas descontinuada

### 🧹 Scripts e Ferramentas Removidos
**Scripts obsoletos removidos:**
- ❌ `scripts/cleanup_old_ofertas.py`
- ❌ `scripts/cleanup_old_pedidos_aprovados.py`
- ❌ `scripts/deploy_migrations_and_cleanup.sh`

**Ferramentas obsoletas removidas:**
- ❌ `tools/check_and_fix_ofertas.py`
- ❌ `tools/diagnose_ofertas.py`
- ❌ `tools/fix_ofertas_interactive.py`
- ❌ `tools/fix_ofertas_now.py`
- ❌ `tools/fix_ofertas_quick.sh`
- ❌ `tools/fix_ofertas_streamlit.py`
- ❌ Backups antigos de migração

### 🆕 Nova Ferramenta Adicionada
**Script de Limpeza do Banco de Dados:**
- ✅ `tools/cleanup_database_v2.py` - Limpeza completa e segura do BD
  - Remove dados obsoletos do sistema de ofertas
  - Limpa pedidos aprovados antigos (configurável)
  - Remove tabelas obsoletas
  - Cria backup automático antes de modificar
  - Modo dry-run para teste seguro
  - Otimiza banco com VACUUM ANALYZE
- ✅ `doc/GUIA_LIMPEZA_BD.md` - Documentação completa de uso

**Documentação obsoleta removida:**
- ❌ `doc/CORRECAO_OFERTAS.md`
- ❌ `doc/SOLUCAO_DASHBOARD_NAO_ATUALIZA.md`
- ❌ `doc/SOLUCAO_RAPIDA.md`
- ❌ `doc/README_TOOLS.md`
- ❌ `doc/RELATORIO_CALCULOS.md`

### 🔧 Arquivos Modificados
- `app.py` - Imports e menu atualizados
- `scripts/smoke_test.py` - Testes atualizados
- `doc/README_PRINCIPAL.md` - Documentação atualizada
- Menu simplificado para refletir novas funcionalidades

### 📝 Menu Atualizado

#### Para Todos os Usuários:
- Home
- **Consulta de Mix** (NOVO)
- Alterar Senha
- Contato

#### Para Usuários com Acesso a Lojas:
- Pedido por Código (CD)

#### Para Administradores:
- Aprovação de Pedidos
- Status do Usuário
- Administração
- Admin Uploads

### ⚠️ Impactos e Migrações Necessárias

#### Código de Produto
- Todos os produtos agora devem usar `cod_consinco`
- Código antigo disponível na coluna `transicao` para referência
- **Ação Requerida:** Atualizar integrações e relatórios que usam códigos antigos

#### Sistema de Ofertas
- Funcionalidade de ofertas foi descontinuada
- Tabela `ofertas` no banco de dados ainda existe mas não é mais usada
- **Ação Requerida:** Migrar para novo sistema de promoções (se aplicável)

#### Consulta de Estoque
- Sistema de consulta de estoque foi substituído
- Nova consulta foca apenas no mix de produtos
- **Ação Requerida:** Revisar fluxos de trabalho que dependiam da consulta antiga

---

## 📅 02/01/2026 - Versão 1.5.0

### ✨ Novas Funcionalidades

#### Scanner de Código de Barras 📱
- **Página:** Pedido por Código (CD)
- **Descrição:** Leitura automática de códigos EAN através da câmera do dispositivo
- **Tecnologia:** pyzbar + Streamlit camera_input
- **Formatos suportados:** EAN-13, EAN-8, UPC-A, UPC-E, Code 128, QR Code
- **Benefícios:**
  - Elimina erros de digitação
  - Agiliza processo de pedidos
  - Otimizado para dispositivos móveis
  - Mantém opção de digitação manual

### 📝 Arquivos Adicionados
- `scripts/test_barcode_scanner.py` - Script de teste e validação
- `doc/SCANNER_CODIGO_BARRAS.md` - Documentação técnica completa
- `doc/CHANGELOG.md` - Histórico de versões

### 🔧 Arquivos Modificados
- `requirements.txt` - Adicionado `pyzbar==0.1.9`
- `page/pedido_cd.py` - Implementação do scanner
- `README.md` - Atualização com novas funcionalidades
- `doc/README_PRINCIPAL.md` - Estrutura e funcionalidades atualizadas
- `doc/README_TOOLS.md` - Inclusão do script de teste

### 📦 Dependências Atualizadas
```diff
+ pyzbar==0.1.9
```

### 🗑️ Funcionalidades Descontinuadas
- **Área de Fornecedores** - Páginas existem mas não estão mais no menu:
  - `page/area_fornecedor.py` (não referenciado)
  - `page/admin_fornecedor.py` (não referenciado)
  - `page/contato_fornecedor.py` (não referenciado)
- **Digitar Pedidos (Legado)** - Removido junto com `pedidos.py`

### 🎯 Melhorias de Documentação
- ✅ Documentação principal atualizada com estrutura completa do projeto
- ✅ Lista de funcionalidades reorganizada por perfil de usuário
- ✅ Novo guia técnico do scanner com troubleshooting
- ✅ Remoção de referências a funcionalidades obsoletas

---

## Versões Anteriores

### Versão 1.4.x
- Sistema de contato e chamados
- Dashboard online
- Gestão de promoções
- Consulta de estoque e mix

### Versão 1.3.x
- Aprovação de pedidos
- Upload de ofertas
- Admin uploads

### Versão 1.2.x
- Migrações de banco de dados
- Scripts de limpeza automática
- Ferramentas de diagnóstico

### Versão 1.1.x
- Sistema base de pedidos
- Gerenciamento de usuários
- Autenticação e autorização

### Versão 1.0.x
- Versão inicial do sistema

---

**Mantido por:** Equipe Bakizi  
**Última atualização:** 23/02/2026
