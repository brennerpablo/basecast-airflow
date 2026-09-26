# BUILD_C — Sessão C (`basecast-get-data`): contrato v2 e endpoints dos módulos

> Leia antes: `BUILD_00_overview.md`, `CLAUDE.md`, `docs/data-contract.md` (§1–§5 são rascunho; §6 e §7 estão
> implementados e ficam como estão) e, em `../basecast-airflow/docs/analysis/`:
> - `marts-proposal.md`: as tabelas de colunas de cada mart e as diferenças contra o contrato atual;
> - `x9_account_diagnosis.md` §4: o formato da API de contas;
> - `x14_acquisition_zones.md` §6: o que o mapa precisa;
> - `x12_weather_normalized_load.md` §5 e as seções "Useful" de X2, X3, X7, X11 e X13.

Hoje esta API só serve `/lake/*`, `/tables/*` e `/pipeline/runs`. Os recursos de produto (§1–§5 do contrato) não
existem, nem como fixture. Esta sessão escreve o contrato v2, faz a API ler os marts da sessão A em memória e expõe
um endpoint tipado por recurso.

## C-1. Contrato v2 (P0, primeiro: a sessão B depende dele)

Reescreva §1–§5 do `docs/data-contract.md`. Pydantic em `schemas/<recurso>.py`, `openapi.json` exportado e o teste
de desatualização passando. Registre no `docs/decisions.md` cada mudança que afeta mais de um repo.

**Mudanças de convenção:**

- **Contas:** a chave passa a ser `account_id` = PUCT `ccn_no` (string). O `utility_id` do EIA vira o atributo
  `eia_utility_id`.
- **Envelope:** `meta` ganha três campos:
  - `model_version`, ao lado de `data_as_of`, `simulated` e `sources`, que já existem;
  - `verified: bool`, falso se algum valor da resposta foi lido por máquina e não conferido;
  - `caveats: CaveatCode[]`, uma lista fechada no contrato, cada código com o texto padrão que o app mostra. Lista
    inicial:

    | Código | Texto padrão (resumo) | Onde |
    |---|---|---|
    | `machine_read_unverified` | valores lidos por máquina dos gráficos da ERCOT, não conferidos por humano | grandes cargas, forecast, backtest |
    | `preliminary_actuals` | jul–ago/2026 ainda não liquidados | pico real 2026, backtest |
    | `weights_pending_review` | pesos do score propostos, aguardando revisão | contas |
    | `band_uncalibrated` | a faixa cobriu 56% das células do backtest (10 de 18), não 80% | forecast |
    | `beyond_backtested_window` | horizonte além dos 24 meses testados | fila ajustada |
    | `allocated_statewide` | grandes cargas estaduais alocadas às zonas | forecast por zona |
    | `by_county_not_point` | atribuição por condado, não por ponto | data centers, gatilhos |
    | `by_area_not_homes` | canais medidos por área de terra, não por residências | mapa de aquisição |
    | `requests_not_forecasts` | pedidos dos TSPs, não previsões; sem localização abaixo do TSP | card do G&T |
    | `policy_pause_2026` | aprovações pausadas pela ERCOT em 2026-08-03 | grandes cargas 2026 |
    | `optimistic_weather` | usa o clima ERA5 observado | curva de despacho 4CP |
    | `simulated` | dado de adapter privado simulado | P2 |

- **Saem do contrato** (não têm fonte; marts-proposal §3.1 e §5.1): `first_deficit_year`, `deficit_mw_p50_next_3y`,
  `generation_added_mw`, previsões com `region_type=county`, e `region_type=utility` (só volta no P2, simulado).
  `is_base_partner` também sai até a A-M8.

**Endpoints recomendados** (ajuste os nomes no plano, mas mantenha um endpoint tipado por recurso):

| Recurso | Endpoint | Mart(s) | Prioridade |
|---|---|---|---|
| Contas | `GET /accounts`, com filtros `type`, `tier`, `next_action`, `trigger`, `zone`, `gt`, `county`, `q`, `sort` e `rank_scope=all\|within_type` | `mart_accounts` (+ `mart_account_counties` para `county`) | P0 |
| | `GET /accounts/export.csv`, com os mesmos filtros | idem | P0 |
| | `GET /accounts/{account_id}`: o detalhe do X9 §4 | `mart_account_detail` | P0 |
| | `GET /accounts/{account_id}/events?since=&trigger=&page=` | `mart_account_events` | P0 |
| Explorer | `GET /geo/counties?horizon=2027\|2028&stratum=all\|solar\|storage\|wind\|gas_other`: uma linha por condado (254; os não ERCOT com prioridade null), juntando aquisição, fila e a contagem de data centers. `meta` leva quebras da legenda, pesos e horizontes | `mart_county_acquisition`, `mart_queue_adjusted_county`, `mart_data_center_sites_new` | P0 |
| | `GET /geo/counties/{county_fips}`: detalhamento do condado (sinais e percentis, drivers e drags, fila por estrato, top projetos, data centers, co-ops e munis que cobrem ≥ 1% do condado) | os mesmos + `mart_queue_project_scores` + `mart_account_counties` | P0 |
| | `GET /queue/projects?county=&stratum=&stage=&sort=&limit=` | `mart_queue_project_scores` | P0 |
| | `GET /geo/zones?measure=` | `mart_zone_layers` | P1 |
| Forecast | `GET /forecasts/peak?region=ERCOT\|<zona>&variant=`: `series[]` (ano, p10, p50, p90), `layers[]` (ano, camada, p10/p50/p90), `official[]` (produto, safra, ano, MW), `inputs{}`, `variants[]` | `mart_peak_forecast`, `mart_official_peak_lines` | P0 |
| | `GET /forecasts/large-load`: `realization[]`, `in_service[]`, `monthly[]`, `annotations[]` | `mart_large_load_*`, `mart_annotations` | P0 |
| | `GET /forecasts/queue-curves?stratum=&stage=` | `mart_queue_stage_curves` | P1 |
| | `GET /load/normalized?region=&grain=month\|year` | `mart_load_normalized_*` | P1 |
| | `GET /four-cp`: intervalos, zonas, curva de despacho, escassez e tarifas | `mart_four_cp_*` | P1 |
| Backtest | `GET /backtest/peak?as_of=`: as células, os escores por era (antes e depois de a ERCOT incluir as grandes cargas dos TSPs), a ablação e os `leak_note`. `meta` lista as 8 datas | `mart_peak_backtest`, `mart_actual_summer_peaks` | P0 |
| | `GET /backtest/official-errors`: a matriz safra × ano-alvo | `mart_official_forecast_errors` | P0 |
| | `GET /backtest/queue` | `mart_queue_backtest` | P0 |
| Insights | `GET /insights`: cards `{id, value, unit, caption, caveats, source_doc, link}` | vários | P1 |

**Regras de `/insights`:**
- entram só linhas de `video-candidates.md` das classes A e B;
- cada card leva a ressalva obrigatória da sua linha;
- os valores saem dos marts, nunca de texto fixo;
- os dois "~438 GW" nunca aparecem sem dizer de qual fila são.

**Fixtures:**
- pequenas, feitas à mão a partir das tabelas de colunas do marts-proposal, com `meta.simulated: true`;
- ficam em `basecast_get_data/data/fixtures/` e existem para a sessão B trabalhar antes dos marts;
- use valores plausíveis, mas **nunca** números que pareçam os reais do vídeo.

**Pronto:**
- `/docs` mostra todos os recursos P0;
- `openapi.json` está atualizado;
- a sessão B consegue rodar `npm run api:generate`.

## C-2. Loader de marts (P0)

- **Pacote `basecast_get_data/marts/`:**
  - um registry dos marts conhecidos (nome → schema esperado);
  - leitura de `public.mart_*` via `basecast_reader` para Polars na subida;
  - refresh em background, no mesmo padrão do índice do lake (a cada 10 min, compara `built_at` e `model_version`);
  - cada carga grava `mart.load` no `ops.log` (o evento já está no contrato §7).
- **Modos:**
  - `DATA_MODE=fixtures|marts` (a variável já existe no `.env.example`);
  - em modo `marts`, um endpoint cujo mart não existe responde **503** `{"detail": "mart_not_built", "mart": "<nome>"}`
    e nunca cai para fixture em silêncio;
  - o app trata o 503 como estado vazio.
- **`meta`:** vem das linhas do mart. `model_version` e `data_as_of` saem de `as_of`; `verified` é falso se alguma
  linha tem `verified = false`; os caveats vêm dos marts usados.
- **Desempenho:**
  - todos os marts cabem em poucos MB (o Cloud Run tem 2 GiB);
  - filtros e joins em Polars;
  - meta de < 100 ms por request em memória;
  - o pool de conexões fica pequeno (2–3; `decisions.md` do get-data).
- **Trava de validação.** Enquanto a A-M8 não rodar, testes garantem três coisas:
  - nenhum schema tem `is_base_partner`;
  - `/accounts` devolve exatamente as linhas do mart;
  - um `account_id` que não está no mart responde 404 igual a qualquer id desconhecido, sem dica de que existe.

## C-3 a C-6. Endpoints por módulo (P0; na ordem contas → explorer → forecast → backtest)

- Um router por recurso.
- Testes com marts em miniatura, recortados dos reais depois que a sessão A os publicar. É o mesmo padrão de
  `tests/fixtures/lake`.
- **Contas:**
  - o detalhe serve o `payload` de `mart_account_detail` como está, validado pelo schema;
  - o CSV sai das mesmas linhas da lista, com as colunas do X9 §4 e `signals` achatado.
- **Explorer:**
  - `GET /geo/counties` devolve os 254 condados num só payload, e o mapa alterna as camadas no cliente;
  - a fila entra por join em `county_fips`, nunca copiada para dentro do mart de aquisição (X14);
  - a lista de contas de um condado vem de `mart_account_counties` e só tem contas do universo pontuado.
- **Forecast:**
  - `variant` default = `config` da sessão A (R13), repassado em `meta`;
  - as zonas levam o caveat `allocated_statewide`.
- **Backtest:** `as_of` inválido responde 422 com a lista das datas válidas.

## C-7. Contexto do produto (P0, 2 minutos)

Corrija a linha "demand record of ~87–91 GW" no `CLAUDE.md` deste repo (R6; texto certo no overview §6).

## C-8. Depois do P0

- **P1:** `inputs` no `/tables` para a linhagem de `/data/flow`, combinado com a sessão A (é o registry dela) e com o
  app (o `graph.ts` já espera o campo).
- **P2:** `account_private_facts` simulado no detalhe, com `meta.simulated: true` e `coverage.resolution` =
  "territory (simulated)".
- **P2:** webhook de gatilhos. Fica fora do MVP a menos que o Pablo peça.
