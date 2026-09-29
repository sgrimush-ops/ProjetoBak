# ProjetoBak - Versão 2.2.0

Sistema de gestão de produtos, pedidos, campanhas de exposição e integração com fornecedores e representantes (ERP Consinco).

## Estrutura do projeto

```
ProjetoBak/
├── app.py                   # Aplicação principal (funcionários)
├── main.py                  # Menu principal e acesso fornecedor/representante
├── requirements.txt         # Dependências Python
├── README.md                # Documentação principal
├── ProjetoPY.code-workspace # Configuração do workspace
│
├── bdados/                  # Base de dados analítica e cadastral
│   ├── query.parquet         # Catálogo oficial Consinco (36k SKUs, estoques e lojas)
│   ├── ean_dun.parquet       # Mapeamento oficial de código de barras / DUN
│   └── con5cod.parquet       # Base de apoio Consinco
│
├── page/                    # Módulos de páginas
│   ├── home.py               # Página inicial
│   ├── pedido_cd.py          # Pedidos por código (CD15 / CD16)
│   ├── pedido_consumo.py     # Pedidos de consumo
│   ├── aprovacao_pedidos.py  # Aprovação de pedidos (admin)
│   ├── campanhas_compras.py  # Gestão de campanhas (compras)
│   ├── campanhas_supply.py   # Fechamento e cubagem de campanhas (supply)
│   ├── campanhas_loja.py     # Visualização e checklists de campanhas por loja
│   ├── admin_uploads.py      # Gerenciamento de uploads (admin)
│   ├── admin_maint.py        # Administração de usuários
│   ├── status_usuarios.py    # Status de usuários online
│   ├── area_fornecedor.py    # Área de digitação rápida para fornecedores/representantes
│   ├── admin_fornecedor.py   # Gestão de carteiras e representantes de fornecedores
│   └── mudar_senha.py        # Alteração de senha
│
├── services/                # Serviços de negócio e exportação
│   ├── campanha_service.py   # Lógica e cálculos de campanhas
│   ├── campanha_db.py        # Modelagem relacional de campanhas
│   └── exportacao_campanha.py# Gerador de planilhas e relatórios PDF
│
├── utils/                   # Utilitários compartilhados
│   ├── fornecedores_loader.py# Catálogo, estoques CD/loja e carteiras de fornecedores
│   ├── timezone.py           # Relógio padrão de Brasília
│   └── cargos.py             # Permissões e papéis de acesso
│
└── doc/                     # Documentação completa
    ├── README_PRINCIPAL.md  # Este arquivo
    ├── CHANGELOG.md         # Histórico completo de versões
    └── plano_modulo_campanhas/# Especificações completas do módulo de campanhas
```

## Funcionalidades principais

### Consulta de mix de produtos
- Busca por codigo Consinco e descricao
- Filtros por embalagem e status
- Exportacao para CSV

### Sistema de pedidos
- Pedido por codigo (CD)
- Pedido de consumo
- Aprovacao de pedidos (admin)
- Controle por lojas
- Seleção obrigatória de CD abastecedor no Pedido por Código (`CD15` ou `CD16`)
- Aviso de relançamento no mesmo dia para o mesmo item no Pedido por Código

### Gestao de usuarios
- Perfis: user e admin
- Cargo do usuario
- Controle de acesso por loja
- Status online em tempo real
- Sistema de chamados/suporte

### Area de fornecedores
- Login separado para fornecedor/promotor
- Pagina inicial e contato/suporte
- Administracao de fornecedores (admin_fornecedor)

### Dados
- Base Consinco em bdados/con5cod.parquet
- Colunas: cod_consinco, descricao, transicao, Mix, Emb
- Normalizacao automatica de cabecalhos com espacos/quebras de linha no carregamento do parquet

### Exportacao de aprovados
- Download considera apenas pedidos aprovados nos ultimos 5 minutos
- Arquivo padrao: `pedido.xlsx`
- Consolidacao por dia + loja + item com soma de quantidades
- Coluna de usuarios consolidada com contagem por repeticao (quando aplicavel)

## Comandos uteis

### Executar a aplicacao
```bash
streamlit run main.py
```

### Testes automatizados
```bash
python3 scripts/smoke_test.py
```

### Verificar dados
```bash
python3 -c "import pandas as pd; df = pd.read_parquet('bdados/con5cod.parquet'); print(df.info())"
```

## Documentacao adicional

- [CHANGELOG.md](CHANGELOG.md) - Historico de versoes
- [MIGRACAO_CONSINCO.md](MIGRACAO_CONSINCO.md) - Guia de migracao (legado)
- [README_MIGRATIONS.md](README_MIGRATIONS.md) - Migrações (legado)
- [GUIA_LIMPEZA_BD.md](GUIA_LIMPEZA_BD.md) - Limpeza do banco
- [COMO_OBTER_DATABASE_URL.md](COMO_OBTER_DATABASE_URL.md) - Configuracao do banco

**Versao:** 2.0.4
**Ultima atualizacao:** 23/02/2026
