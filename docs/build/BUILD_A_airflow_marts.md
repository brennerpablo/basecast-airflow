# BUILD_A — Sessão A (`basecast-airflow`): a camada de marts

> Leia antes: `BUILD_00_overview.md` (arquitetura, defaults das decisões pendentes, prioridades, trava de
> validação), `CLAUDE.md`, a seção "Start here" e a fila de revisão de `docs/analysis/findings.md`,
> `docs/analysis/marts-proposal.md` §1 (regras de todo mart) e `docs/analysis/x16_models_review.md`. Para cada mart,
> leia a seção indicada do doc de análise e o script `analysis/` correspondente: **o script é a implementação de
> referência da cadeia.**

Objetivo: transformar as cadeias das análises em tabelas `public.mart_*` pequenas, tipadas e reprodutíveis. A API
(sessão C) serve essas tabelas, e o app (sessão B) as mostra. Hoje `basecast_pipelines/models/` tem a lógica,
`models/db.py` só lê, e nada escreve marts.

## A-M0. Preparação (P0, antes de qualquer mart)

1. **`git status` primeiro.** O working tree tem arquivos parciais de X17–X23 não commitados (findings, Notes). Não
   os inclua em commits; pergunte ao Pablo o que fazer com eles. O X16 também avisou que outros agentes editavam
   `peak_forecast.py` e `triggers.py`: confira se há diffs locais nesses arquivos antes de mexer.
2. **Corrija os 4 bugs abertos do X16**, cada um com teste em `tests/models/` usando a entrada sintética que o X16 usou
   para demonstrá-lo. O bug 1 (Uri) e o bug 4 (`DC_`) já foram corrigidos em `9c62100`.
   - **Bug 2**, `survival.EVENT_COLUMNS` / `queue_adjusted.truncate_at`:
     - inclua `ia_first_month`, `synchronization_first_month`, `fis_first_month`, `cod_first_month` e `cancelled_month`
       nas colunas carregadas;
     - `truncate_at` passa a levantar erro quando falta uma coluna de guarda, em vez de pular.
   - **Bug 3**, `peak_forecast` (convenção de data): adote fim de mês, como o X16 sugere:
     - `dec_index = Y*12+12`;
     - meses de status em `month_index + 1`;
     - `SUMMER_POINT = 7.0`.
   - **Bugs 5 e 6**, `triggers.score_accounts` / `percentile_ranks`:
     - `pl.when(den > 0)`;
     - `nulls_last=True` na ordenação;
     - com n ≤ 1, `percentile_ranks` devolve null.
3. **Rode de novo** `analysis/x2_adjusted_queue.py`, `x7_peak_forecast.py` e `x12_weather_normalized_load.py`, e
   confira os valores que o X16 previu:

   | Script | Deve dar |
   |---|---|
   | X2 | backtest −13,2 / +9,3 / +1,4%; fila de hoje igual (38.689 / 70.394 MW) |
   | X7, deck de março | 2027 = 92.820 [89.292–96.420] MW; 2030 = 111.328 [104.041–123.287] MW; MAPE 3,3% |
   | X7, deck de junho | 2027 ≈ 101.805 MW; 2030 ≈ 132.800 MW |
   | X12 | 2,2%/ano em 2010–21; 2022 com +4,4% (z 3,9); 2022–25 com 3,9–5,7 DP |

   Se algum valor divergir, pare e avise. Se bater, atualize os números nos docs `x2_*`, `x7_*` e `x12_*`, no
   `findings.md` ("Start here" e as linhas X2, X7 e X12) e nas linhas B1, B3 e D1 de `video-candidates.md`, e
   registre em `docs/decisions.md`.
4. **R6.** Corrija a linha do contexto no `CLAUDE.md` deste repo. Em vez de "demand record of ~87–91 GW", escreva:
   recorde de 85.508 MW (2023-08-10) até 2026-07-22, quando o pico chegou a 91,1 GW (preliminar).

## A-M1. A camada de marts (P0)

**Pacote `basecast_pipelines/marts/`:**
- `core.py`, com:
  - um dataclass `Mart`: `name`, `key`, `build`, `inputs` (as tabelas que lê), `caveats` (códigos do contrato v2),
    `checks`;
  - um `MartContext`: `as_of: date`, o `config` e um cache de frames compartilhados dentro do mesmo run, como o
    universo de contas e os eventos do GIS.
- Um módulo por grupo de telas: `accounts.py`, `explorer.py`, `forecast.py`, `backtest.py`, `geo.py`.

**Regras de construção:**
- **Datas.** Passe `as_of` explicitamente para toda função de modelo. Vários módulos têm um `AS_OF` fixo como default;
  num build diário ele envelheceria os gatilhos e as datas de expiração.
- **Leitura:** `models.db.read_sql` (papel `basecast_reader`).
- **Escrita:**
  - Postgres `public.mart_<name>`, via `BASECAST_DB_URL` (papel `basecast_writer`);
  - substituição atômica numa transação (tabela nova, depois troca), para que a API nunca leia uma tabela pela metade;
  - tipos reais (número nunca como texto);
  - toda linha com `model_version` (sha curto do commit + versão do mart), `as_of` e `built_at` (UTC).
- **Grant.** Confira que `basecast_reader` recebe SELECT nas tabelas novas (default privileges). Se não receber, dê o
  GRANT logo depois de criar a tabela.
- **Registry.** `processing.registry.publish` apaga as linhas que não estão na sua lista de módulos. Registre os marts
  **dentro** desse publish (por exemplo, `source_id = "marts"`); senão o próximo `basecast process` apaga o registro.
  Para a linhagem de `/data/flow`, o registry precisa de um campo `inputs`. Isso muda o contrato §6: combine com a
  sessão C antes, e a decisão fica registrada no `decisions.md` do get-data.
- **etl_run.** Uma linha por build, com `stage = "model"` (valor novo). Combine com C e B se `/pipeline/runs`,
  `/data/flow` e `/ops` aceitam esse estágio. Eventos da linha: linhas escritas, checks que passaram e falharam,
  duração.

**CLI:**
- `basecast marts list`;
- `basecast marts build [NAME...] [--all] [--as-of YYYY-MM-DD] [--dry-run]`. O `--dry-run` escreve Parquet em
  `data/marts_dry/` e não toca no Postgres;
- `basecast marts check`.

**Config `config/marts.yaml`.** Todos os switches da tabela §3 do overview, cada um com
`status: default_pending_review`, mais `validation.revealed: false` e `validation.held_out_names` (os 5 nomes, movidos
de `analysis/q3_signals.py`).

**Checks de ouro.** Cada mart declara os números que precisa reproduzir, com o `as_of` do doc de origem (listas
abaixo). Se um check falha, o run termina `partial` e grava o evento. Nunca publique em silêncio um número que diverge
do doc.

**Trava de validação:**
- estenda `tests/test_partner_lock.py` para `basecast_pipelines/marts/`. A única exceção futura é
  `marts/validation.py` (A-M8);
- retenha as contas por nome **antes** de qualquer sinal, exatamente como `analysis/q3_signals.py`
  (`accounts.find_names`), com um assert de que os 5 nomes foram achados;
- adicione um check: com `validation.revealed: false`, `mart_accounts` tem 107 linhas e nenhuma coluna
  `is_base_partner`.

**Dependências.** Os modelos usam numpy, e o casamento de nomes usa rapidfuzz. Os dois estão só no grupo `analysis`.
No Mac, rode com `uv run --group analysis basecast marts ...`. A ida para a VM fica na A-M7.

**Pronto:**
- `basecast marts build --all` roda limpo a partir do Mac;
- os checks passam, ou cada divergência está explicada no `decisions.md`;
- os marts aparecem em `/tables` e `/data`;
- `uv run pytest` passa.

## A-M2. Geometria do mapa (P0, rápido; destrava a sessão B)

- **Condados:** `basecast export-geo --out-dir ../basecast-app/public/geo/` gera `tx-counties.geojson` a partir de
  `tx_counties` (PostGIS):
  - simplificação com `ST_SimplifyPreserveTopology` em EPSG:3083, depois de volta para 4326;
  - coordenadas com 5 casas;
  - propriedades: `county_fips` (string de 5 dígitos), `county_name`, `weather_zone` (de `county_weather_zone`, null
    fora da ERCOT) e `in_ercot`.
- **Zonas:** `ercot-weather-zones.geojson`, com os condados `in_ercot` dissolvidos por zona.
- **Tamanho:** abaixo de 1 MB cada arquivo. GeoJSON evita uma dependência no cliente. O KICKOFF A1 pedia TopoJSON
  (< 2 MB); decida no plano.
- **Commit:** a sessão B faz o commit no app.

## A-M3. Marts de contas (P0)

Referências: `analysis/x5_triggers.py`, `x9_account_diagnosis.py`, `q3_signals.py`, `x4_eia861_short_form.py`,
`x10_muni_places.py`, `x13_gt_large_load.py`. Docs: X5 ("Useful"), X9 §1 e §4, marts-proposal §5.

| Mart | Grão | Como |
|---|---|---|
| `mart_accounts` | conta (`account_id` = `ccn_no`) | Cadeia: `accounts.build_universe` → retenção → `build_signals` → `triggers.score_accounts` (pesos de `scoring.weights_set`) → `score_tier` → os 8 construtores de eventos → `active_events` → `next_action` → `diagnosis.action_changes_on`. Colunas da lista do X9 §4, mais `gt` (X13), `rank_within_type` (R14/X10), `weights_set` e `weights_status` |
| `mart_account_events` | conta × evento | Colunas do marts-proposal §5.2. Com `munis.place_permit_trigger: true`, inclui o gatilho de alvarás por cidade do X10 (`muni_places.place_permit_surge_events`) |
| `mart_account_detail` | conta, com `payload` jsonb | Detalhe pronto: `diagnosis.assemble` → `to_jsonable`, no formato do X9 §4, com os gatilhos resumidos (ativos + `context_summary` + `history_count`). Veja a nota abaixo |
| `mart_account_counties` | conta × condado | `diagnosis.account_counties` com `min_share = 0.01`. Serve o filtro `/accounts?county=` e o drill-down do mapa (X14 §6) |
| `mart_tsp_large_load_requests` (P1) | TSP × ano | X13 "Useful": `tsp`, `entity_type`, `group`, `year`, `mw`, `share`, `filed_date`, `source_ref`, `verified = false` |

**Nota sobre `mart_account_detail`.** O marts-proposal separa o detalhe em `account_facts`, `account_counties` e
`account_eia_series`. Pré-computar o JSON por conta poupa a API de remontar o diagnóstico e cabe no prazo. As tabelas
separadas ficam como P1 se alguma tela precisar filtrar por fato.

**O payload do detalhe também leva:**
- **(P1) Card do fornecedor (X13):** o caminho 2026 → 2030 → 2032 do G&T, a participação dele no RFI, o número de
  membros e os rótulos "requests, not forecasts" e "no location below the TSP".
- **(P1) Munis (X10):** os fatos de cidade, com `note` "city" e o selo de encaixe.
- **(P1) Oferta de 4CP (X3 + X15):**
  - a linha 4CP da zona;
  - janela 15:45–17:45, com cerca de 55 dias de despacho por verão;
  - US$ 68.547 por MW-ano à tarifa de 2025 (Docket 57491, final) e US$ 75.527 à de 2026 (Docket 59080, pendente),
    faturados no ano seguinte ao verão;
  - ressalva: é custo evitado pela co-op, não receita da Base, e o kW por casa da frota não foi verificado;
  - a carga 4CP da própria conta entra como lacuna (dado privado).

**Checks de ouro** (as_of 2026-09-26, pesos Q3, sem os extras do X10, igual ao X5):
- 107 contas;
- 46 com gatilho forte ativo;
- ações 25 call now / 12 nurture / 34 watch / 36 hold;
- a #1 é New Braunfels Utilities, com `action_changes_on` = 2026-10-22;
- cerca de 6.014 eventos.

Depois de passar, ligue os extras do X10 (R15) e registre as contagens novas no `decisions.md`.

## A-M4. Marts do Explorer (P0, exceto a última linha)

| Mart | Grão | Como | Checks de ouro |
|---|---|---|---|
| `mart_county_acquisition` | condado (204 da ERCOT) | X14 §6. Cadeia: `acquisition_zones.channel_split` → construtores de `accounts.py` sobre `county_links` → `score_counties` → `channel_priority` → `drivers` → `legend_classes`. As quebras da legenda, os pesos e o tilt vão numa linha de metadados (tabela `mart_meta` ou colunas repetidas; decida no plano). A fila de geração entra como contexto por join na API, sem cópia | 204 condados; 161 parceria / 39 retail-direct / 4 misto |
| `mart_queue_adjusted_county` | mês do relatório × condado × estrato | X2 "Pipeline": `truncate_at` → `fit_stage_curves(..., fit_stages("entry_ia_sm"))` → `build_queue` → `score` → `aggregate`. Colunas do marts-proposal §2.1, com `large_gas_mw` (R10). Horizontes dez/2027 e dez/2028. Anos seguintes só como P2 e com o caveat `beyond_backtested_window` | 1.810 projetos; 438.262 MW brutos; 38.689 MW (2027) e 70.394 MW (2028) ajustados |
| `mart_queue_project_scores` | mês × `inr` | marts-proposal §2.2 | 1.810 linhas |
| `mart_data_center_sites_new` | site TCEQ (RN) | Q4, marts-proposal §2.3, com as regras R7 do config | 38 sites em 27 condados |
| `mart_zone_layers` (P1) | zona × medida | Três fontes: participações do excesso coincidente e razão mín/máx 2019 × 2026 (X1); alocação das grandes cargas `a2e_stock`, `pipeline_2032`, `u_share`, com central/low/high e `verified = false` (X11); os 10 condados que a ERCOT nomeia, como pontos (X11 `county_pressure`) | valores dos docs X1 e X11 |

## A-M5. Marts do Forecast (P0 pico e grandes cargas; P1 o resto)

- **`mart_peak_forecast`** (P0; X7 §4; marts-proposal §3.1):
  - variantes `deck_pre_batch_zero` (default), `deck_latest` e `approvals_pace` (só P50);
  - camadas `organic`, `large_load`, `unattributed` e `total`;
  - regiões: ERCOT e as 8 zonas. As zonas usam a alocação do X11 (R16) e levam a coluna de rótulo;
  - anos 2027–2030 (R13);
  - colunas de entrada: data do deck, fator, faixa da razão, estoque aprovado e `verified`;
  - seed fixa (`simulate(..., seed=7)`), para o resultado ser determinístico;
  - checks: os valores X7 corrigidos da A-M0.
- **`mart_official_peak_lines`** (P0): as previsões oficiais de pico por produto, safra e ano-alvo, lidas de
  `official_forecasts`. Inclui LTLF 2025 (ERCOT-adjusted e TSP-provided) e CDR dez/2025, para ERCOT e zonas. São as
  linhas de comparação do gráfico.
- **Grandes cargas** (P0; marts-proposal §3.2–3.4):
  - `mart_large_load_realization`, `mart_large_load_in_service` e `mart_large_load_monthly`;
  - antes de datar picos observados pelos headlines, corrija o parser de `large_load_headlines`: os decks de jan e
    fev/2026 imprimem "January 2025" para o pico de 3.977 MW (X6, nota 7).
- **`mart_annotations`** (P1): eventos datados vindos de `config/annotations.yaml`, cada um com a URL da fonte (a
  pausa de 2026-08-03 tem a URL no findings, Notes). O intake do Batch Zero em abr/2026 só entra com fonte primária;
  sem ela fica "not verified".
- **`mart_queue_stage_curves`** (P1; §3.6): inclui exportar as curvas por etapa do X2, que ainda não foram exportadas.
- **Carga normalizada** (P1; X12 §5):
  - `mart_load_normalized_monthly` e `mart_load_normalized_annual`, com os metadados do X12;
  - checks: os valores corrigidos da A-M0.
- **`mart_organic_peak_history`** (P1; Q7, marts-proposal §3.5): inclui o cartão do modelo e as zonas marcadas
  (NORTH, FWEST, SCENT).
- **4CP** (P1; §3.7):
  - `mart_four_cp_intervals`, `mart_four_cp_zone`, `mart_four_cp_dispatch_curve` e `mart_four_cp_scarcity`;
  - `mart_four_cp_rates` vem de config, com docket e status (R11 fechado pelo X15);
  - a curva de despacho leva o rótulo "optimistic: observed ERA5 weather".

## A-M6. Marts do Backtest (P0)

| Mart | Fonte | Checks de ouro |
|---|---|---|
| `mart_peak_backtest` | marts-proposal §4.1, incluindo as linhas de ablação `basecast_organic_only` e o `leak_note` de cada linha | 18 células; MAPE 3,3% × LTLF 5,1% e CDR 4,8%; orgânico sozinho 10,5% |
| `mart_official_forecast_errors` | §4.2 | 354 linhas |
| `mart_actual_summer_peaks` | §4.3 | 2026 = 91.134 MW (HE 18, 2026-07-22, `final = false`); 2023 = 85.508 MW |
| `mart_queue_backtest` | §4.4 | −13,2 / +9,3 / +1,4% depois da A-M0; Spearman por condado 0,64–0,66 (ajustada) × 0,43–0,45 (bruta) |

## A-M7. Agendamento (P1)

- **DAG:** um só, `dag_marts_incremental`, fino:
  - roda diariamente depois das fontes da manhã (os marts inteiros levam poucos minutos, como nas análises);
  - pool `heavy_process`;
  - a docstring começa com `"""Airflow DAG: ...` (a descoberta do Airflow 3 exige isso).
- **Imagem:** mova numpy e rapidfuzz para `[project].dependencies` **e** `deploy/airflow/requirements.txt` juntos
  (`tests/test_deploy.py` exige que os dois andem juntos).
- **Leitura na VM:** configure `BASECAST_READER_DSN` na VM, pelo secret `AIRFLOW_ENV_FILE`. O default de
  `models/db.py` aponta para o proxy do Mac.
- **Push:** só com nenhum run longo vivo (veja o overview §7).

## A-M8. Revelação da validação (a "A2" do findings; só com o "vai" do Pablo)

1. Rode só depois que o Pablo aprovar os pesos (R1) **e** disser para revelar.
2. Commite primeiro os pesos aprovados e anote o hash.
3. Crie `marts/validation.py`, o único código novo autorizado a importar `models/partners.py`. Ponha na lista de
   exceções do teste de trava e exponha pelo comando `basecast validation reveal`.
4. **O método da revelação vai no plano, para o Pablo aprovar antes.** Por exemplo: pontuar as 5 contra a
   distribuição congelada das 107, com os mesmos pesos e o mesmo `as_of`.
5. O resultado vai para `docs/analysis/a2_validation.md`, com número, fonte e ressalvas.
6. Depois disso, `validation.revealed: true`: os marts passam a ter 112 contas e o campo `is_base_partner`. Avise as
   sessões C e B.

Antes do "vai", não rode, não estime e não antecipe nada sobre a posição dos parceiros.

## A-M9. Parser do EIA-861S (P1)

Siga a especificação do X4: parsear `Short_Form_<year>.xlsx` e as delivery companies em `eia861_sales`. Enquanto isso
não existe, os marts podem ler o 861S direto dos zips brutos por `eia861_short_form`, como o X4 fez. O parser serve
para o `/data` mostrar a tabela e para a série EIA do diagnóstico sair de uma tabela.

## A-M10. Adapters privados simulados (P2)

- Implemente `FleetDataSource` e `UtilityDataSource` em `basecast_pipelines/adapters/`: interface mais uma
  implementação simulada determinística, com seed.
- Saída: `mart_account_private_facts`, no mesmo formato de Fact, com `simulated = true` e `source` =
  "UtilityDataSource (simulated)".
- Isso nunca alimenta score, ranking, gatilhos, previsão nem backtest (marts-proposal §7).
- Nenhuma conta fora do universo pontuado recebe camada simulada.
