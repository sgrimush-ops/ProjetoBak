# 03 - MOTOR DE CÁLCULO, CUBAGEM E REGRAS MATEMÁTICAS

Este documento consolida todas as formulações matemáticas e lógicas de processamento que devem ser implementadas no módulo isolado `services/campanha_calculo.py`.

---

## 📐 1. Projeção de Vendas no Período da Campanha

### 1.1 Venda Média Diária
A venda diária é obtida a partir do acumulado dos últimos 30 dias registrado no `query.parquet`:
$$\text{Venda Média Diária} = \frac{\text{QTD\_VENDIDA\_30D}}{30.0}$$

### 1.2 Duração Inclusiva da Campanha
A contagem de dias é inclusiva:
$$\text{Dias de Campanha} = (\text{Data Fim} - \text{Data Início}) + 1$$
*Exemplo:* De 01/10/2026 a 15/10/2026: $(15 - 1) + 1 = 15 \text{ dias}$.

### 1.3 Venda Projetada
$$\text{Venda Projetada} = \text{Venda Média Diária} \times \text{Dias de Campanha}$$
*Exemplo:* $5.2 \text{ un/dia} \times 15 \text{ dias} = 78 \text{ unidades}$.

---

## 🗄️ 2. Algoritmo de Cubagem Física por Bandeja e Rateio de SKUs

Uma exposição (como uma Ponta de Gôndola) é composta por múltiplas bandejas independentes $b \in \{1, 2, \dots, N\}$, cada uma com suas próprias dimensões físicas:
- $L_b$: Largura da bandeja em cm
- $P_b$: Profundidade da bandeja em cm
- $A_b$: Altura útil da bandeja em cm

O produto possui dimensões físicas em centímetros:
- $L_p$: Largura do produto em cm
- $P_p$: Profundidade do produto em cm
- $A_p$: Altura do produto em cm

### 2.1 Capacidade por Bandeja Individual
Para cada bandeja $b$:
$$\text{Frentes (Largura)} = \left\lfloor \frac{L_b}{L_p} \right\rfloor$$
$$\text{Fila (Profundidade)} = \left\lfloor \frac{P_b}{P_p} \right\rfloor$$
$$\text{Camadas (Altura)} = \left\lfloor \frac{A_b}{A_p} \right\rfloor$$
$$\text{Capacidade Total da Bandeja } b = \text{Frentes} \times \text{Fila} \times \text{Camadas}$$

### 2.2 Rateio entre Múltiplos SKUs
Se $K$ SKUs diferentes (ex: 3 sabores do mesmo produto) forem alocados juntos na mesma exposição:
$$\text{Capacidade por SKU na Bandeja } b = \left\lfloor \frac{\text{Capacidade Total da Bandeja } b}{K} \right\rfloor$$

### 2.3 Capacidade Total Física da Exposição
$$\text{Capacidade Total Física (SKU)} = \sum_{b=1}^{N} \text{Capacidade por SKU na Bandeja } b$$

### 📊 Exemplo Prático de Cubagem:
- **Produto:** Nescau 350g ($10 \times 10 \times 10 \text{ cm}$)
- **Bandeja 1:** $150 \times 50 \times 20 \text{ cm}$
  - Frentes: $\lfloor 150/10 \rfloor = 15$
  - Fila: $\lfloor 50/10 \rfloor = 5$
  - Camadas: $\lfloor 20/10 \rfloor = 2$
  - Total Bandeja 1: $15 \times 5 \times 2 = 150 \text{ unidades}$.
- **Com 3 SKUs na mesma bandeja:**
  - Capacidade SKU: $\lfloor 150 / 3 \rfloor = 50 \text{ unidades}$.

---

## 📦 3. Conversão para Caixas Fechadas de Transferência

O abastecimento e transferência ocorrem estritamente em caixas fechadas, utilizando a embalagem de transferência ($E = \text{EMBL\_TRANSFERENCIA}$).

### 3.1 Quantidade de Caixas
$$\text{Caixas Necessárias} = \left\lceil \frac{\text{Volume Final Supply}}{E} \right\rceil$$

### 3.2 Volume Efetivo Transferido (Unidades)
$$\text{Volume Transferência (Unidades)} = \text{Caixas Necessárias} \times E$$

### 📊 Exemplo de Arredondamento:
- $\text{Volume Final Supply} = 42 \text{ un}$
- $\text{Embalagem Transferência } E = 24 \text{ un/cx}$
- $\text{Caixas} = \lceil 42 / 24 \rceil = 2 \text{ caixas}$
- $\text{Volume Efetivo} = 2 \times 24 = 48 \text{ unidades}$

---

## 🏢 4. Apuração de Saldo de CD e Ruptura (CD15)

Seja $C_{total}$ a soma das caixas necessárias para todas as 14 lojas e $S_{cd}$ o saldo de estoque disponível no CD15 (obtido de `QUANTIDADE_DISPONIVEL` do parquet convertido em caixas):

### 4.1 Cenário A: Estoque Pleno ($S_{cd} \ge C_{total}$)
- $\text{Caixas Atendidas} = C_{total}$
- $\text{Caixas Faltantes para Compra} = 0$
- $\text{Status de Estoque} = \text{'OK'}$

### 4.2 Cenário B: Estoque Parcial ($0 < S_{cd} < C_{total}$)
- $\text{Caixas Atendidas} = S_{cd}$
- $\text{Caixas Faltantes para Compra} = C_{total} - S_{cd}$
- $\text{Status de Estoque} = \text{'PARCIAL'}$
- Gera registro de pendência na tabela `campanha_devolutivas`.

### 4.3 Cenário C: Estoque Zerado ($S_{cd} = 0$)
- $\text{Caixas Atendidas} = 0$
- $\text{Caixas Faltantes para Compra} = C_{total}$
- $\text{Status de Estoque} = \text{'SEM\_ESTOQUE\_CD'}$
- Gera registro de pendência urgente na tabela `campanha_devolutivas`.
