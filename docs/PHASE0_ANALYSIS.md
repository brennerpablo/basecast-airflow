# PHASE0_ANALYSIS — o que os dados entregam de verdade (antes de codar as features)

> Handoff para o Claude Code, sessão A (`basecast-airflow`). Esta fase entra **antes da A0** do
> `docs/FINAL_SPRINT.md`. Leia os dois arquivos.
>
> **Objetivo:** responder 7 perguntas cujas respostas mudam o plano. Cada uma termina em uma decisão:
> segue o plano, aciona o plano B ou corta.
>
> **Timebox total: 3h20, com parada obrigatória.** Isto não é uma exploração aberta: gráfico que não muda
> nenhuma decisão fica de fora.
>
> **As sessões C e B não esperam.** Seguem o bloco 1 do `FINAL_SPRINT.md` (contrato com fixtures,
> usuário demo, `SourceFooter`, tela de backtest).

## 0. Decisão antes de abrir os dados — [PERGUNTAR]

Como o score de contas vai se relacionar com os parceiros atuais da Base (Bandera EC, GVEC, CoServ,
Farmers EC, Austin Energy)? Pergunte ao Pablo qual das duas opções vale antes de começar.

- **Validação (padrão):**
  - a fase 0 não olha onde os parceiros caem;
  - os pesos são fixados e commitados sem essa informação;
  - a posição dos parceiros é revelada só na A2;
  - no vídeo pode ser chamado de validação.
- **Look-alike:**
  - o score é aprendido a partir dos parceiros e pode olhar para eles à vontade;
  - no vídeo **não** pode ser chamado de validação.

Na opção de validação, a trava fica no próprio código:
- a leitura de `base_public_facts` fica num módulo separado (`models/partners.py`);
- nenhum script de `analysis/` importa esse módulo;
- a sessão A não consulta essa tabela durante a fase 0.

## 1. Setup (timebox 15 min)

- **Pasta:** `analysis/` em `basecast-airflow`, com um script por pergunta, `q1_backtest.py` …
  `q7_organic_peak.py`.
  - Formato `.py` com células `# %%`, não `.ipynb`.
  - `analysis/_common.py` traz `read_sql(query) -> pl.DataFrame`.
- **Conexão:** use o papel `basecast_reader` (somente leitura) no Cloud SQL de produção. A fase 0 não
  grava nada no banco.
- **Reaproveitamento:** a lógica que vai virar modelo nasce direto em `basecast_pipelines/models/`, e
  os scripts de análise só chamam essas funções. Exemplos:
  - `models/survival.py`: `load_events()`, `fit_cif()`;
  - `models/backtest.py`: `actual_summer_peaks()`, `match_forecasts()`;
  - `models/accounts.py`: `build_universe()`, `build_signals()`.
- **Fora da fase 0:** a parte da A0 que é CLI, `etl_run`, registry e escrita de marts fica para depois.
- **Figuras:** vão em `analysis/out/` (gitignored). Os números vão para `docs/analysis/findings.md`
  (commitado, modelo no §4).

## 2. As perguntas

### Q1. Quanto as previsões oficiais erram? (timebox 30 min)

**Entradas:** `official_forecasts` e `ercot_monthly_peaks`.

**Calcular:**
1. **Catálogo de métricas.** Liste os valores distintos de (`product`, `metric`, `scenario`,
   `region_type`) com contagem e escolha a série base de cada produto.
2. **Pico real de verão por ano.**
   - Use a mesma definição de pico que a previsão usa (horário × 15 min; coincidente × não coincidente).
   - Em 2026, informe o valor, a data e a hora do pico e o `data_as_of`.
3. **Erro.** Calcule o erro de cada safra × ano-alvo e resuma por horizonte (n, média %, mediana, mínimo
   e máximo).
4. **Preliminar de 2026.** Compare os ~112 GW com a faixa de 90,5–98 GW e com o pico real de 2026.

**Decisão:**
- Sinal consistente por horizonte (a mesma direção na maioria das safras) → a narrativa é "a previsão
  oficial erra X% para cima/baixo a Y anos".
- Sinal misto → a narrativa fica no preliminar de 2026 e na dispersão das safras.
- A1 segue nos dois casos.

### Q2. O universo de contas fecha? (timebox 30 min)

**Entradas:** `puct_ccn_territories`, `county_utility_overlap_puct`, `eia861_utility` e `eia861_sales`.

**Calcular:**
1. **Contagens do universo:**
   - co-ops e munis da ERCOT no PUCT (tipo coop/muni, ISO inclui ERCOT);
   - co-ops e munis no EIA-861 (ownership Cooperative/Municipal, BA ERCO).
2. **Crosswalk.** Rode nome normalizado + rapidfuzz e mostre a distribuição dos scores.
   - Gere o rascunho de `config/utility_crosswalk.yaml`.
   - **[PERGUNTAR]** Liste os casos com score < 90 para o Pablo revisar.
3. **Presença dos parceiros.** Confirme que as 5 contas parceiras **existem** no universo, casando só
   pelo nome. Não calcule nada sobre elas.

**Decisão:**
- ≥ 80% das co-ops e munis do PUCT casadas (score ≥ 90 ou depois da revisão) → o universo PUCT ∩ EIA
  segue.
- Menos que isso → plano B: universo só do PUCT. Os sinais do EIA-861 saem do score e ficam só os sinais
  por condado.

### Q3. Os sinais das contas prestam? (timebox 30 min, sem olhar os parceiros)

**Entradas:** as da Q2, mais `census_permits_county`, `census_housing_county`, `census_population_county`,
`tceq_data_center_sites` e `ltlf_forecasts` (placeholder do crescimento de pico da zona).

**Calcular, para cada sinal do A2:**
1. **Cobertura.** Percentual do universo com valor, distribuição (mínimo, p10, mediana, p90, máximo) e
   fração de zeros.
2. **EIA-861.**
   - anos disponíveis por utility;
   - releases early marcados;
   - saltos ano a ano acima de ±50% (fusão, troca de id ou erro), sinalizados.
3. **Alvarás.** Condados sem reporte e a diferença entre o valor imputado e o reportado (`_rep`).
4. **Redundância.** Correlação entre os sinais, para achar pares que medem a mesma coisa (ex.: população
   × alvarás).

**Decisão:**
- **Entra no score** o sinal com cobertura ≥ 80% do universo que não seja quase constante.
- **Redundância:** par com correlação > 0,8 → fica um dos dois, ou os dois dividem o peso.
- **Pesos:**
  - proponha os pesos em `config/account_score.yaml` com uma linha de justificativa por sinal;
  - **[PERGUNTAR]** o Pablo aprova;
  - commite o arquivo antes da A2.

### Q4. Os data centers novos caem em território de co-op? (timebox 20 min)

**Entradas:** `tceq_data_center_sites`, `county_utility_overlap_puct`, `census_population_county` e
`tx_counties`.

**Calcular:**
1. **Sites.** Selecione os sites com primeira licença desde 2025-01-01. A referência nos decisions é 38
   sites em 27 condados. Reporte à parte os que têm `has_undated_affiliation`.
2. **Classificação por tipo de território** (co-op / muni / IOU), de duas formas:
   - pelo tipo com maior área no condado;
   - ponderado pelo `county_share`.

   A atribuição é por condado, não por ponto; diga isso no resultado.
3. **Metro × não metro.**
   - Descubra como a decisão anterior chegou a "34 fora das metrópoles" e reproduza.
   - Se não for reproduzível, use densidade demográfica (população do PEP ÷ área de terra) com um limiar
     explícito, registrado no findings.

**Decisão:**
- ≥ 60% dos sites (ponderado) em território de co-op e a maioria fora das metrópoles → o achado entra
  no vídeo.
- Senão → fica só como sinal no score, fora do roteiro.

### Q5. As séries de grandes cargas sustentam uma razão de realização? (timebox 30 min)

**Entradas:** `large_load_chart_values` (Gemini, `verified=false`) e `large_load_headlines`.

**Calcular:**
1. **Inventário.** Liste os valores distintos de (data do deck, gráfico/série, dimensão). Encontre, em
   cada deck desde mai/2023, a série de MW por ano previsto de entrada em serviço e a de MW por status.
   Conte quantos decks têm cada uma.
2. **Consistência.**
   - o total de cada deck bate com o headline do mesmo deck? Tolerância de ±5%;
   - a soma das barras por ano bate com o total do gráfico?
3. **Checagem manual.** Gere `docs/large-load-spot-check.md` com 5 valores por série e o link do slide.
   **[PERGUNTAR]** O Pablo confere.
4. **Razão preliminar.** Para os anos-alvo 2024 e 2025 (e 2026 parcial), compare o prometido acumulado
   de cada safra com o aprovado para energizar ou energizado depois. Documente as definições: as duas
   séries são estoques.

**Decisão:**
- ≥ 6 decks com a série de entrada em serviço e totais consistentes (≤ 5%) → realização estimada, com
  faixa (A3 como planejado).
- Senão → plano B da A3: três cenários explícitos, e a tela diz "cenário, não estimado".

### Q6. A fila de geração aguenta um modelo de sobrevivência? (timebox 30 min)

**Entradas:** `gis_project_events` e `tx_counties`.

**Calcular:**
1. **Composição da coorte** (`first_seen_month >= 2018-08`):
   - contagem por `exit_status` × fuel group;
   - fração de `dropped`;
   - fração de `exit_inferred`;
   - distribuição de `months_missing`.
2. **Curvas.** Rode um Aalen–Johansen rápido por landmark (entrada, `fis_approved`, `ia_signed`) × fuel
   group e salve as curvas de CIF.
3. **Sanidade.** Em cada fuel, CIF(IA assinado) > CIF(FIS aprovado) > CIF(entrada) aos 36 meses.
4. **Tamanho dos estratos.** Estrato com n < 30 ou menos de 10 eventos → junte com outro (ex.: gas +
   other).
5. **Sensibilidade do `dropped`.** Compare `dropped` como evento competidor × censurado pela diferença
   da CIF em 36 meses.
6. **Condados.** Taxa de casamento de `county` (nome) com `county_fips`. Meta ≥ 98%; liste os que não
   casarem.

**Decisão:**
- Ordenação coerente e estratos com tamanho suficiente → Aalen–Johansen na A4.
- Senão → taxas por coorte.
- Sensibilidade do `dropped` > 10 pp → o app mostra as duas versões, ou a tela explica a escolha.

### Q7. Clima + tendência explicam o pico por zona? (timebox 30 min)

**Entradas:** `ercot_load_hourly_wz` e `weather_hourly_wz` (ERA5).

**Calcular:**
1. **Alinhamento de horário.**
   - `ts_utc` é o fim do intervalo hour-ending;
   - o Open-Meteo vem em GMT;
   - agregue os dias em America/Chicago.
2. **Série de pico.** Pico anual de verão (jun–set) por weather zone e do ERCOT total, de 2003 a 2025.
3. **Variável de clima.** Máxima da média de 3 dias de temperatura da zona em jun–set. Teste também com
   ponto de orvalho, se o tempo deixar.
4. **Ajuste.** `pico ~ ano + temperatura` por zona, com R² e desvio dos resíduos.
5. **Hold-out.** Ajuste até 2022 e preveja 2023–2025 com o clima real. Calcule o MAPE por zona e do
   ERCOT total.
6. **Quebras.** Procure quebras estruturais nos resíduos. FWEST tende a ter uma, por grandes cargas e
   petróleo.

**Decisão** (limiares padrão; o Pablo pode ajustar):
- MAPE do ERCOT total no hold-out ≤ 5% → A3 como planejado.
- Zona com MAPE > 10% → a zona aparece marcada, ou só o total do ERCOT é mostrado.
- MAPE do ERCOT total > 5% → plano B da A3: faixa pelo desvio dos resíduos, sem simulação de clima.

## 3. Encerramento (timebox 15 min)

1. Preencha `docs/analysis/findings.md` (modelo abaixo) e commite. As figuras ficam em `analysis/out/`.
2. **[PERGUNTAR]** Revisão de 10 minutos com o Pablo: ele lê o findings e confirma as decisões.
3. Registre no `docs/decisions.md` cada plano B acionado e cada limiar adotado.
4. Siga para a A0 do `FINAL_SPRINT.md`. Todo número que for para o vídeo começa no findings e passa
   depois para o `docs/video-numbers.md`.

## 4. Modelo do `docs/analysis/findings.md`

```markdown
# Phase 0 findings — <data/hora>

Partner mode: validation | look-alike (decided by Pablo at <hora>)

| Q | Result (numbers) | Decision | FINAL_SPRINT task affected | Verified |
|---|---|---|---|---|
| Q1 | 2026 summer peak = … MW on … (data_as_of …); preliminary 112 GW error = …%; mean error by horizon: 1y …, 3y …, 5y … | narrative: … | A1 | file |
| Q2 | PUCT universe = …; EIA = …; matched ≥90 = …%; reviewed = … | go / plan B | A2 | — |
| Q3 | signals kept: …; dropped: … (why); weights proposed/approved at commit … | … | A2 | — |
| Q4 | … of … sites in co-op territory (weighted …%); outside metro: … | in video / signal only | A2, video | — |
| Q5 | decks with in-service series = …; consistency = …; spot-check: …/… ok; realization 2024 = …, 2025 = … | estimated / scenarios | A3 | human-checked? |
| Q6 | cohort n = …; dropped = …%; CIF36 IA-signed solar = …; ordering ok? …; county match = …% | AJ / cohort rates | A4 | — |
| Q7 | R² by zone …; hold-out MAPE ERCOT = …%; zones > 10%: … | go / plan B | A3 | — |

## Notes
- …
```
