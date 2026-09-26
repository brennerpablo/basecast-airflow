# BUILD_B — Sessão B (`basecast-app`): as telas dos módulos

> Leia antes: `BUILD_00_overview.md` e `CLAUDE.md`. Dê atenção especial às regras de abas na mesma janela
> (`screen-tabs.test.ts`), ao padrão do BFF em `src/lib/get-data/` e ao aviso de que esta versão do Next.js tem
> mudanças que quebram código (leia o guia em `node_modules/next/dist/docs/`). Leia também o contrato v2 da sessão C
> e, em `../basecast-airflow/docs/analysis/`:
> - as seções "Useful for the core features" de X1, X2, X3, X5, X7, X11, X12 e X13;
> - `x9_account_diagnosis.md` §1–§2 (três diagnósticos de exemplo);
> - `x14_acquisition_zones.md` §6 (legenda e cliques do mapa);
> - `video-candidates.md`, para a redação e as ressalvas.

Hoje as páginas `/explorer`, `/forecast`, `/backtest`, `/accounts` e `/accounts/[id]` são placeholders. Não há
biblioteca de mapa instalada nem `public/geo/`. O texto da interface é em inglês.

## Regras que valem para todas as telas

- **Dados:** só pela API, via BFF (`withSession`, `x-request-id`), com os tipos gerados do `openapi.json`
  (`npm run api:generate`). Nenhum número de dado fica escrito no código, nem os do vídeo.
- **Proveniência:**
  - cada card mostra fonte, `data_as_of` e `model_version` num rodapé (`<Provenance>`);
  - valores com `verified = false` levam o selo "Machine-read, not verified";
  - valores simulados levam "Simulated" **ao lado do número**, não só num banner.
- **Ressalvas:** `meta.caveats` vira selos com tooltip (`<CaveatBadges>`). O texto padrão de cada código vem do
  contrato v2, e as telas não reescrevem ressalvas por conta própria.
- **Estados:**
  - carregando: skeleton;
  - vazio;
  - 503 `mart_not_built`: "This view is being rebuilt" com o nome do mart, **sem quebrar a página**;
  - erro: o `error.tsx` existente.
- **URL:** o estado das telas (camada, horizonte, filtros, aba, variante, `as_of`) fica na URL (`nuqs`), seguindo as
  regras de abas. Navegação sempre por link.
- **Gráficos:** use `src/components/components-app/charts` e recharts, que já estão instalados. Só adicione
  biblioteca nova para o mapa.
- **Números:** MW com separador de milhar abaixo de 10 GW, GW com uma casa acima disso, percentuais com uma casa,
  datas em America/Chicago.
- **Trava de validação:** nenhuma marca de parceiro até a sessão A rodar a A-M8. O app nunca calcula ranking; mostra o
  que a API manda.

## B-1. Base compartilhada (P0)

- **BFF:**
  - uma função por endpoint novo em `src/lib/get-data/`, o único lugar que chama o get-data;
  - route handlers em `src/app/api/<recurso>/...`;
  - o CSV das contas passa em streaming pelo BFF.
- **Componentes:** `<Provenance>`, `<CaveatBadges>`, `<VerifiedBadge>`, `<SimulatedBadge>`, formatadores de MW/GW e um
  `<StatCard>` (valor, unidade, legenda, caveats, link).
- **Menu:** `MAIN_MENU` na ordem Insights (P1), Accounts, Explorer, Forecast, Backtest, Data, Ops. Accounts sobe,
  porque é o núcleo da demo.
- **Contexto do produto:** corrija a linha "demand record of ~87–91 GW" no `CLAUDE.md` deste repo (R6; texto certo no
  overview §6).

## B-2. `/accounts` (P0)

Tabela (`DataTable`; são 107 linhas):

- **Colunas:**
  - rank;
  - nome;
  - tipo;
  - tier;
  - chip da próxima ação, com cor por ação;
  - "until <date>" quando `action_changes_on` existe (X9: 5 dos 25 call-now expiram em 90 dias);
  - top trigger, com idade ("data-center permit · 38 days");
  - chips dos gatilhos ativos;
  - G&T;
  - zona;
  - medidores;
  - barra do score;
  - ícones das flags (`short_form`, `apportionment_*`, `eia_break`), com tooltip.
- **Filtros:** tipo, tier, próxima ação, gatilho, zona, G&T e busca. Mais o `county` que chega do Explorer
  (`/accounts?county=<fips>`), mostrado como chip removível.
- **Toggle "Rank munis within type"** (X10/R14): pede `rank_scope=within_type`.
- **Export CSV** com os filtros atuais.
- **Banner** "Score weights pending review" quando vier o caveat `weights_pending_review`.

## B-3. `/accounts/[id]` (P0; os cards marcados P1 entram depois)

Blocos na ordem do X9 §1. Cada escalar é um Fact `{value, unit, source, as_of, note}`: tooltip com a fonte, e `null`
aparece como lacuna, nunca como zero.

1. **Header:** tipo, CCN, G&T, id do EIA, condados, km², medidores e crescimento, vendas, preço e tendência.
2. **Next-action card:**
   - a ação;
   - a regra que disparou, em palavras ("Tier A + data-center permit → call now");
   - o gatilho líder;
   - a oferta e os talking points;
   - a expiração ("Call now until 2026-10-22 unless a new event lands").
3. **Why now:**
   - linha do tempo dos eventos, com a janela de 12 meses sombreada;
   - gatilhos de contexto recolhidos numa linha por tipo, com a contagem;
   - "Show full history" pagina `/accounts/{id}/events`;
   - cada evento mostra condado e exposição, ou "by name", e a referência da fonte.
4. **Score breakdown:** barras de percentil por sinal, com peso configurado, peso usado e contribuição. As
   contribuições somam o score, e o selo `weights_pending_review` aparece aqui.
5. **Territory:**
   - condados (expostos ≥ 20% ou "context: home county" para munis);
   - data centers próximos;
   - fila bruta × ajustada (dez/2027 e dez/2028);
   - perspectiva da zona: LTLF × agora, intensidade do 4CP e hora do pico próprio contra a hora do 4CP.
6. **(P1) Wholesale supplier card (X13):**
   - caminho 2026 → 2030 → 2032 do G&T em barras pequenas;
   - participação no RFI e número de membros;
   - selo `requests_not_forecasts`.
7. **(P1) 4CP offer card (X3 + X15):**
   - janela 15:45–17:45 e cerca de 55 dias de despacho por verão;
   - dólares por MW-ano nas tarifas de 2025 (final) e 2026 (pendente);
   - a frase "avoided cost for the co-op, not Base revenue";
   - a carga 4CP da própria conta aparece como "Private data (UtilityDataSource)", uma lacuna explicada.
8. **(P1) City facts para munis (X10),** com o selo de encaixe.
9. **EIA series:** clientes, medidores e preço por ano, com o early release marcado.
10. **Data gaps:** a lista `gaps` da API.

## B-4. `/explorer` (P0: camadas 1–3; P1: camada 4)

- **Mapa:**
  - adicione `maplibre-gl` com versão fixa;
  - carregue `public/geo/tx-counties.geojson` (gerado pela sessão A na A-M2; você faz o commit) com
    `promoteId: "county_fips"`;
  - coroplético por `feature-state`, contornos das weather zones (`ercot-weather-zones.geojson`);
  - basemap sem token (OpenFreeMap) ou só os polígonos.
- **Dados:** uma chamada a `GET /geo/counties` traz tudo, e as camadas alternam no cliente. Tooltip no hover, painel
  no clique.
- **Camadas** (`layer` na URL):
  1. **Acquisition priority** (default, X14 §6):
     - o canal dá o matiz (retail-direct, partnership, mixed) e a classe de prioridade dá a luminosidade, com as
       quebras vindas de `meta`;
     - condados fora da ERCOT em cinza hachurado;
     - toggle All / Retail-direct list / Partnership list, com os condados fora da lista esmaecidos;
     - selo `by_area_not_homes`.
  2. **Generation queue** (X2):
     - métrica Raw MW / Adjusted MW / Ratio / Rank change;
     - horizonte Dec 2027 / Dec 2028;
     - filtro de estrato;
     - nota de credibilidade com o erro do backtest e o Spearman (valores da API);
     - selo para o gás novo grande.
     - O rótulo diz sempre **"generation queue"**.
  3. **New data centers** (Q4): contagem por condado desde 2025, toggle "include NAICS-only matches", selo
     `by_county_not_point`.
  4. **(P1) Grid layers by zone** (X1/X11): participação no excesso de carga plana, grandes cargas aprovadas (alocadas)
     e pipeline de 2032, mais os condados que a ERCOT nomeia como pontos. Não espalhe o pipeline pelos alvarás.
- **Painel do condado:**
  - prioridade como "market 0.89 × grid 0.82";
  - drivers e drags;
  - participações dos canais "by area";
  - fila por estrato e top projetos (`/queue/projects`), com a probabilidade de COD e a data do desenvolvedor para
    comparação;
  - data centers;
  - co-ops e munis que cobrem o condado, com link para `/accounts?county=<fips>`.

## B-5. `/forecast` (P0: abas Peak e Large loads; P1: as outras)

Abas com `urlParam="tab"`:

- **Peak** (X7):
  - barras ou áreas empilhadas por ano, 2027–2030 (organic / large load / unattributed), com a faixa P10–P90 no total;
  - linhas do LTLF 2025 (ERCOT-adjusted e TSP-provided) e do CDR dez/2025;
  - toggle de variante: Before Batch Zero (default) / Latest deck / Approvals pace;
  - seletor de região: ERCOT e as 8 zonas, com as zonas marcadas `allocated_statewide`;
  - painel de entradas (data do deck, fator, faixa da razão, estoque aprovado), marcado "machine-read";
  - selo `band_uncalibrated`.
- **Large loads** (Q5, X7):
  - prometido × aprovado por safra do deck;
  - faixa da razão de realização;
  - estoque aprovado mês a mês e pico observado;
  - anotações da API (pausa de 2026-08-03 e as demais com fonte).
- **(P1) Generation queue** (Q6/X2): curvas de COD e de desistência por estrato e etapa, cortadas onde há menos de 10
  projetos em risco.
- **(P1) Normalized load** (X12):
  - real × normalizado mensal por zona;
  - pico de verão com a faixa normal;
  - o contraste "raw fell in 7 years, normalized rose every year", com números da API.
- **(P1) 4CP** (X3):
  - calendário dos intervalos;
  - cobertura da janela;
  - curva dias de despacho × taxa de acerto (selo `optimistic_weather`);
  - deslocamento da escassez (o pico de carga líquida foi para HE 20–21);
  - tarifas.

## B-6. `/backtest` (P0)

- **Leque de 2026:**
  - o preliminar de 112 GW, rotulado "preliminary long-term forecast";
  - a faixa de 90,5–98 GW da ERCOT;
  - o real de 91,1 GW (`preliminary_actuals`);
  - o nosso modelo.
- **Slider de `as_of`** pelas 8 datas: o nosso P50 e a faixa contra a safra oficial daquela data e o real.
- **Tabela de escores por era,** mostrando também a era em que a ERCOT foi melhor, que é o que dá credibilidade.
  Junto, a ablação (só orgânico × método completo) e o `leak_note` de cada linha, visível.
- **Matriz de erros das safras oficiais** (Q1).
- **Card do backtest da fila** (X2): previsto × construído × datas dos desenvolvedores.

## B-7. `/insights` (P1)

Cards de `GET /insights`, cada um com valor, legenda, ressalva obrigatória e link para a tela que o sustenta. É a
página de abertura do vídeo. Nenhum número fica escrito no código.

## B-8. Camada privada simulada (P2)

Em `/accounts/[id]`, uma seção "Private data" com os Facts do `UtilityDataSource` e do `FleetDataSource`, cada valor
com "Simulated" ao lado. `coverage.resolution` passa a "territory (simulated)". Nada disso muda score, gatilho ou
ação.

## Pronto (cada push)

- `typecheck`, `lint`, `test` e `build` passam.
- As telas P0 abrem sem erro em modo marts e em modo fixtures.
- Um mart faltando mostra o estado vazio, não quebra a página.
- Cada número tem fonte e data a um hover de distância.
