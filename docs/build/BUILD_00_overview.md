# BUILD_00 — Construção dos módulos do basecast (visão geral)

> Handoff para o Claude Code. Escrito em 2026-09-26, a partir de `basecast-airflow@9c62100`,
> `basecast-get-data@4287b6b` e `basecast-app@adf7545`. Entrega do hackathon: 2026-09-27 (vídeo de 5 min + código).
>
> Três sessões em paralelo, uma por repo, como na fase 0:
> - **Sessão A** (`basecast-airflow`): marts. Leia também `BUILD_A_airflow_marts.md`.
> - **Sessão C** (`basecast-get-data`): contrato v2 e endpoints. Leia também `BUILD_C_get_data_api.md`.
> - **Sessão B** (`basecast-app`): telas. Leia também `BUILD_B_app_screens.md`.
>
> Os quatro arquivos ficam em `basecast-airflow/docs/build/`. Os repos irmãos estão em `~/Documents/repos/basecast/`,
> e as análises em `basecast-airflow/docs/analysis/`.

## 0. Como usar

- As regras de trabalho continuam as do `CLAUDE.md` de cada repo: planeje e mostre o plano ao Pablo, implemente só
  depois da aprovação, trabalhe só no `main` e rode os checks antes de cada push.
- **Numeração.** Este handoff usa ids próprios (`A-M*`, `C-*`, `B-*`). Os "A0–A4" citados no `findings.md` vêm do
  `FINAL_SPRINT.md` e equivalem a:

  | findings | aqui |
  |---|---|
  | A0 (marts) | A-M1 |
  | A1 (backtest) | A-M6, C-6, B-6 |
  | A2 (contas e revelação) | A-M3, A-M8 |
  | A3 (forecast) | A-M5 |
  | A4 (fila) | A-M4 |

  As tarefas A0–A9 do `KICKOFF.md` §4 são outra lista (ingestão) e já estão feitas.
- **Onde está cada coisa.** O hub é `docs/analysis/findings.md`. A lista de marts por tela é
  `docs/analysis/marts-proposal.md`, mas ela foi escrita no X8 e **não cobre X10–X15**. Este handoff completa essa
  lacuna.

## 1. O que vamos construir

O produto responde a uma pergunta da Base: *onde e quando a demanda cresce de verdade, e quais co-ops e munis
abordar, com que oferta*. Quatro telas carregam isso. Em ordem de importância para a demo:

| Tela | O que mostra | Análises que a sustentam |
|---|---|---|
| `/accounts`, `/accounts/[id]` | 107 co-ops e munis ranqueadas, com gatilhos datados ("why now"), próxima ação com data de validade, diagnóstico com fonte em cada número e a oferta de 4CP em dólares | Q2, Q3, X4, X5, X9, X10, X13, X3 + X15 |
| `/explorer` | Mapa por condado: prioridade de aquisição com dois canais (retail-direct × parceria), fila de geração bruta × ajustada e data centers novos | X14, X2, Q4, X1, X11 |
| `/forecast` | Pico em três camadas (orgânico, grandes cargas realizadas, carga plana não atribuída) com P10/P50/P90 contra LTLF e CDR; grandes cargas prometidas × aprovadas; curvas da fila; carga normalizada por clima; 4CP | X7, Q5, X1, X11, X12, Q6, X2, X3 |
| `/backtest` | O preliminar de 112 GW contra o real de 91,1 GW; o nosso modelo refeito em 8 datas passadas; a fila refeita em 3 snapshots | Q1, X7, X2 §3 |

**Outras features que as análises identificaram** (entram como P1, veja §4):

| Feature | Onde aparece | Fonte |
|---|---|---|
| Oferta de 4CP em dólares por MW (janela 15:45–17:45, tarifa 2025 final e 2026 pendente) | `/accounts/[id]`, aba 4CP do `/forecast` | X3, X15 |
| Card "seu fornecedor atacadista" (exposição do G&T às grandes cargas) e filtro por G&T | `/accounts`, `/accounts/[id]` | X13 |
| "Call now até <data>": a data em que a ação expira sem evento novo | `/accounts` | X9 |
| Fatos de cidade para munis e gatilho de alvarás por cidade | `/accounts/[id]` | X10 |
| Camadas por zona: carga plana chegou (excesso e razão mín/máx) e grandes cargas alocadas | `/explorer` | X1, X11 |
| Série de carga normalizada por clima ("o pico normalizado subiu todo ano") | `/forecast` | X12 |
| Página de insights com os números do vídeo, cada um com a sua ressalva | nova, `/insights` | `video-candidates.md` |
| Linhagem dos marts em `/data/flow` (o app já desenha, falta o `inputs`) | `/data` | marts-proposal §6 |
| Export CSV das contas | `/accounts` | KICKOFF §1, X9 |
| Dados privados simulados (`FleetDataSource`, `UtilityDataSource`) como camada sobreposta | `/accounts/[id]` | KICKOFF §1, marts-proposal §7 |

## 2. Arquitetura

```
tabelas processadas (Postgres public, BigQuery)
  → basecast_pipelines/models/*        lógica existente, exercitada nas análises
  → basecast_pipelines/marts/*         NOVO: as mesmas cadeias que os scripts analysis/ executam
  → Postgres public.mart_*             escrito por basecast_writer, lido por basecast_reader
  → get-data carrega em memória (Polars) → endpoints tipados (contrato v2)
  → BFF do app (route handlers) → telas
```

Seis princípios valem para as três sessões:

1. **Os scripts `analysis/qN_*.py` e `analysis/xN_*.py` são a implementação de referência.** Cada mart porta a cadeia
   do seu script; não reescreva a lógica. Se o mart não reproduz o número do doc, pare e avise.
2. **Nenhum número fica escrito à mão no app.** Todo número vem de um mart, pela API. Isso vale também para os
   números do vídeo mostrados em `/insights`.
3. **Proveniência em tudo.** Toda linha de mart carrega `model_version` e `as_of`. Valores lidos por máquina dos
   decks de grandes cargas carregam `verified = false`. A API repassa isso em `meta`, e o app mostra fonte e data em
   cada card.
4. **Ressalvas como códigos.** A API manda `meta.caveats` (lista fixa de códigos no contrato v2), e o app transforma
   cada código em selo e tooltip. Assim a ressalva obrigatória nunca depende de alguém lembrar de escrevê-la.
5. **A trava de validação vale no produto inteiro**, não só em `analysis/`. Veja §7.
6. **"Simulated"** marca tudo que vem dos adapters privados ou de fixtures, por linha e no envelope, e nunca alimenta
   score, ranking, gatilhos, previsão ou backtest (marts-proposal §7).

## 3. Decisões pendentes e o default que o build usa

O Pablo ainda não revisou a fila do `findings.md`. O build **não espera**: usa o default abaixo, deixa cada escolha
num switch de `config/marts.yaml` (sessão A) e marca o que está pendente com o caveat certo. O único item que
**bloqueia** é o R1, porque a revelação dos parceiros (A-M8) só roda depois dele.

| R | Assunto | Default no build | Switch / efeito |
|---|---|---|---|
| **R1** | Pesos do score (Q3 × X4) | Q3 (`config/account_score.yaml` como está). X4 fica implementado como alternativa, e o Claude recomenda X4 | `scoring.weights_set: q3 \| x4`; caveat `weights_pending_review`. **Bloqueia A-M8** |
| R2 | 5 linhas do crosswalk abaixo de 90 | aceitar (proposta do Q2) | `config/utility_crosswalk.yaml` |
| R3 | Spot-check dos 10 valores dos decks | `verified = false` em tudo; o modo segue "estimado com faixa" | `large_load.mode: estimated \| scenarios`. Mais de 1 erro em 10 → `scenarios` |
| R4 | Modelo da fila | Aalen–Johansen com a etapa semi-Markov de entrada, ponderado por MW (variante primária do X2); evento = COD; corte em < 10 em risco | `queue.model: entry_ia_sm \| cohort` |
| R5 | Faixa e definição de "orgânico" | ajuste até 2019; faixa = RMSE dos erros um ano à frente (X7) | `forecast.organic_fit_end: 2019` |
| R6 | Redação e contexto | "preliminary long-term forecast"; "about 21 GW"; recorde 85,5 GW até 22/07/2026 | corrigir a linha do contexto nos três `CLAUDE.md` |
| R7 | Data centers (Q4) | metrópole = lista de 13 condados; matches só por NAICS entram, com toggle; os 7 fora da ERCOT ficam marcados e fora das contagens ERCOT | `dc_sites.metro_rule`, `dc_sites.include_naics_only` |
| R9 | Camada não atribuída | camada separada (~4,5 GW em 2026), sem dizer "roda dia e noite" | `forecast.unattributed_layer: separate` |
| R10 | Gás novo grande e faixa da fila | coluna `large_gas_mw` + selo na UI; sem P10/P90 no mapa (nota de credibilidade com o erro do backtest) | — |
| R11 | Rótulo do 4CP e US$ | **fechado pelo X15**: a janela vale; tarifa US$ 68,547/kW-ano (2025, final) e US$ 75,527 (2026, pendente) | tarifas em config, com docket |
| R12 | `gen_storage_ia` forte ou contexto | forte (25 call-now; seriam 17 sem ele) | `triggers.gen_storage_ia: strong \| context` |
| R13 | Deck que dirige o forecast | três variantes; default = deck de março/2026 (antes do Batch Zero); para em 2030; faixa rotulada "não calibrada" | `forecast.default_variant` |
| R14 | Chave e parceiros | `account_id` = `ccn_no`; sem `is_base_partner` até A-M8; munis ranqueadas também dentro do tipo | — |
| R15 | Munis (X10) | escopo estreito: fatos de cidade no diagnóstico + gatilho de alvarás por cidade; score igual | `munis.place_facts`, `munis.place_permit_trigger` |
| R16 | Grandes cargas por zona (X11) | forma do Batch Zero e f = 0,72 para NORTH em LZ_WEST; substitui as participações fixas do X7 | `large_load.zone_allocation: x11 \| x7_fixed` |
| R19 | G&T (X13) | fato de contexto; munis supridas pela LCRA ficam com o fato e sem o gatilho; LCRA como poder público (22,9%) | — |
| R20 | Zonas de aquisição (X14) | a leitura do X14 (prioridade por condado × canal, forma multiplicativa, canais por área) | — |
| R8, R17, R18 | Limiares, marts, série normalizada | como no findings | — |

## 4. Prioridades e linha de corte

**P0: a demo não existe sem isso.**
- **A:**
  - A-M0: bugs abertos do X16 e números atualizados;
  - A-M1: camada de marts;
  - A-M2: geo do mapa;
  - A-M3: marts de contas;
  - A-M4: aquisição, fila e data centers;
  - A-M5: pico e grandes cargas;
  - A-M6: backtest.
- **C:** C-1 a C-6: contrato v2, loader e endpoints de contas, explorer, forecast (pico e grandes cargas) e backtest.
- **B:** B-1 a B-6: base compartilhada, `/accounts`, `/accounts/[id]`, `/explorer` (camadas 1–3), `/forecast` aba
  Peak e aba Large loads, `/backtest`.

**P1: entra se o P0 estiver no ar.**
- As demais abas do `/forecast`: fila, carga normalizada, 4CP.
- Camadas por zona no mapa.
- Card do G&T, oferta de 4CP e fatos de cidade das munis.
- `/insights`.
- DAG dos marts (A-M7) e parser 861S (A-M9).
- `inputs` para a linhagem em `/data/flow`.

**P2: fora se faltar tempo.**
- Adapters privados simulados (A-M10, B-8).
- Webhook.
- Horizontes da fila além de dez/2028.
- Camada de sensibilidade ao clima.

**A-M8 (revelação da validação)** não tem prioridade de código: roda assim que o Pablo aprovar os pesos (R1), porque
é o momento-chave do vídeo.

## 5. Sequência e pontos de sincronização

1. **Primeiras horas, em paralelo.**
   - A: A-M0, A-M1 e A-M2. A-M2 é rápido e destrava o mapa do B.
   - C: C-1. O contrato v2 com fixtures destrava o B.
   - B: B-1, mais o esqueleto das telas contra as fixtures do C.
2. **Sync 1: contrato v2 publicado.** C exporta o `openapi.json`, e B roda `npm run api:generate`. A partir daqui,
   qualquer mudança de contrato passa pelo C (doc, Pydantic e `openapi.json` juntos).
3. **Marts entrando.** A constrói na ordem A-M3 → A-M4 → A-M5 (pico e grandes cargas) → A-M6. A cada mart pronto,
   C troca o endpoint de fixture para mart (`DATA_MODE=marts`), e B valida a tela com dado real.
4. **Sync 2: todos os marts P0 no Postgres, API em modo marts em produção, telas P0 navegáveis sem erro.**
5. P1 na ordem de §4.
6. **Quando o Pablo aprovar R1:** A-M8, depois C e B ligam `is_base_partner`.

## 6. Números: armadilhas conhecidas

- **Números antigos no findings.** O X16 achou bugs, e a A-M0 corrige e atualiza os docs. Até lá, use:

  | Onde | Antigo | Correto |
  |---|---|---|
  | X2, backtest | "dentro de 13%" | −13,2 / +9,3 / +1,4% |
  | X7, deck de março | 2027 = 93,0 GW; 2030 = 111,6 GW | 2027 ≈ 92,8 GW; 2030 ≈ 111,3 GW (MAPE 3,3% se mantém) |
  | X12 | 2,1%/ano em 2010–21; 2022 com +5,0% | 2,2%/ano em 2010–21; 2022 com +4,4% (z 3,9); 2022–25 com 3,9–5,7 DP |

- **Dois "~438 GW".** Um é a fila de **geração** (ago/2026), o outro a de **grandes cargas** (mai/2026). Todo rótulo
  diz qual é.
- **Recorde.** Era 85.508 MW (2023-08-10) até 2026-07-22, quando o pico chegou a 91.134 MW (preliminar, não liquidado).
  A linha "demand record of ~87–91 GW" nos `CLAUDE.md` está errada.
- **"Errou por mais de 20 GW".** A margem acima de 20 GW é só 866 MW. Diga "about 21 GW".
- **Pausa da ERCOT em 2026-08-03.** A ERCOT pausou as aprovações de data centers e cripto ≥ 75 MW. Isso explica o
  estoque aprovado parado em 8,8–8,9 GW. A razão parcial de 2026 (0,28) é efeito de política, não taxa de realização.
- **"not verified" nunca vira fato na UI.** Aparece com selo.

## 7. Regras que não podem quebrar

- **Trava de validação.** Enquanto `validation.revealed` for `false`:
  - nenhum código fora de `models/partners.py` (e, na A-M8, `marts/validation.py`) lê `base_public_facts`,
    `config/base_public_facts.yaml` ou `models/partners.py`;
  - os marts de contas têm 107 contas, com as 5 retidas por nome **antes** de qualquer cálculo, como em
    `analysis/q3_signals.py`;
  - a API e o app não têm campo `is_base_partner`;
  - uma conta retida responde 404, igual a qualquer id desconhecido.

  Ninguém calcula, estima ou comenta a posição dos parceiros antes da A-M8.
- **Pushes.** Cada push no `main` do airflow redeploya a VM e reinicia o scheduler; isso matou um run longo 9 vezes
  num dia. Agrupe commits e confira que nenhum run longo está vivo antes de subir. Push no app deploya na Vercel; na
  API, no Cloud Run.
- **Working tree do airflow.** Tem arquivos parciais de X17–X23 não commitados. Não os inclua em commits sem perguntar.
- **Não invente URL, coluna nem número.** O que não foi confirmado fica "not verified".
- **O contrato é do `basecast-get-data`.** A sessão A propõe; a sessão C decide e registra.

## 8. Prompts de abertura

- **Sessão A:**

  > Leia `docs/build/BUILD_00_overview.md` e `docs/build/BUILD_A_airflow_marts.md`. Comece pela A-M0 e me mostre o
  > plano antes de implementar.

- **Sessão C:**

  > Leia `../basecast-airflow/docs/build/BUILD_00_overview.md` e
  > `../basecast-airflow/docs/build/BUILD_C_get_data_api.md`. Comece pela C-1 (contrato v2 com fixtures) e me mostre
  > o plano antes de implementar.

- **Sessão B:**

  > Leia `../basecast-airflow/docs/build/BUILD_00_overview.md` e
  > `../basecast-airflow/docs/build/BUILD_B_app_screens.md`. Comece pela B-1 e me mostre o plano antes de implementar.
