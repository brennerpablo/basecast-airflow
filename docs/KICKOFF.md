# KICKOFF — Base Power × AITX Hackathon (Austin, 25–27 set 2026)

> Documento de partida para o Claude Code. Leia inteiro antes de começar.
> Projeto: **basecast**, em três repositórios: `basecast-airflow`, `basecast-get-data` e `basecast-app`.
> Não use "Novi" em código, pacotes ou branding: Novi Labs é uma empresa real de Austin.
> "Novi para Energia" é só a analogia do pitch.

## 0. Como trabalhar com este documento

1. Este mesmo arquivo vai em `docs/KICKOFF.md` de cada um dos três repositórios.
2. Primeira tarefa em cada repositório: criar um `CLAUDE.md` na raiz com as seções 1, 2 e 7 e a parte
   da seção 3 que descreve aquele repositório. As tarefas das seções 4, 5 e 6 ficam só no KICKOFF.
3. Há três frentes: **A. mineração de dados local** (`basecast-airflow`), **B. base do frontend**
   (`basecast-app`) e **C. esqueleto da API** (`basecast-get-data`, pequena). Planeje cada frente
   antes de codar e me mostre o plano.
4. Código, comentários, README e nomes em inglês. Conversa comigo em português.
5. Na dúvida sobre formato de arquivo da ERCOT: leia cabeçalhos dinamicamente, nunca assuma nomes
   de colunas. O que não foi confirmado fica marcado como "não verificado".
6. Registre decisões em `docs/decisions.md` do repositório afetado (uma linha por decisão: data,
   decisão, motivo). Decisões que afetam mais de um repositório vão no `basecast-get-data`, que é
   o dono do contrato.

## 1. Contexto do produto

A Base Power é uma empresa de energia de Austin: instala baterias nas casas (continua dona delas),
vende energia no varejo e opera a frota como usina virtual. Ganha dinheiro de três bolsos: morador,
mercado atacadista da ERCOT e concessionárias (cooperativas e municipais que compram capacidade).

**Problema:** o Texas planeja a rede com filas de interconexão infladas. Em jan/2026 a ERCOT
acompanhava ~232,5 GW de grandes cargas, só 3,8% com aprovação para energizar, contra um recorde de
consumo de ~87–91 GW. A própria previsão preliminar oficial de 2026 (~112 GW) errou o pico do mesmo
ano em mais de 20 GW.

**Produto:** prevê quanto das filas (grandes cargas e geração) realmente sai do papel, onde e quando,
converte isso em previsão de **pico em MW** por região e ano (P10/P50/P90) e traduz em decisões para a
Base, principalmente para o time de parcerias: quais cooperativas procurar, quando e com qual proposta.

**Módulos:**
1. **Explorer:** mapa do Texas por condado; fila bruta vs. ajustada; zonas prioritárias de aquisição.
2. **Forecast:** sobrevivência por projeto na fila de geração; fluxo agregado por estágio nas grandes
   cargas; séries temporais de carga normalizadas pelo clima; backtest.
3. **Inteligência comercial (núcleo da demo):** contas priorizadas, gatilhos, diagnóstico por
   cooperativa, próxima ação por regras. Somente leitura; exportação CSV/webhook.
4. **Adapters de dados privados:** `FleetDataSource` (frota da Base, simulada) e `UtilityDataSource`
   (pedidos de grandes cargas que a própria cooperativa recebeu). Dados públicos dão visão por zona;
   dados privados levam o diagnóstico ao território.

**Como seremos julgados:** vídeo de 5 min + código. Completude sem crash, profundidade técnica,
encaixe na trilha (Open Grid Data é a principal), insight não óbvio, usabilidade, performance.

## 2. Decisões de arquitetura (fechadas)

- **Três repositórios, mesmo desenho do Fundsys:** `basecast-airflow` (ingestão e modelos),
  `basecast-get-data` (API) e `basecast-app` (frontend). Nada de monorepo.
- **Agora:** a mineração roda local no Mac mini (Apple Silicon). Depois sobe para um projeto GCP
  pessoal em `us-central1` (a API da ERCOT bloqueia acesso de fora dos EUA) e Vercel pessoal.
- **Lake:** raw imutável `raw/source=<id>/dt=<data do snapshot>/<arquivo original>` + Parquet tipado
  `parquet/<dataset>/dt=<data>/part-*.parquet`. O layout local é idêntico ao do bucket GCS, então
  "subir" = `gcloud storage rsync` + load no BigQuery. Nada de reescrever código na subida.
- **Armazém (depois):** BigQuery, tabelas particionadas por dia. Sem Cloud SQL (não há escrita de usuário).
- **Pipelines (`basecast-airflow`):** cada fonte é um módulo Python puro com
  `run(*, storage, http, since=None, until=None)`, executável sozinho pela CLI. Os DAGs do Airflow
  (depois, numa VM com Docker Compose e LocalExecutor) serão finos e só chamam esses `run()`.
  Padrões herdados do Fundsys: fábrica de DAG `full` / `incremental`, `etl_run` (uma linha por
  execução com status, duração, eventos), idempotência (reprocessar não duplica).
- **API (`basecast-get-data`):** FastAPI, depois no Cloud Run. Mesmo nome do serviço do Fundsys, mas
  **um endpoint tipado por recurso** (não o dispatch por `process`), Pydantic, OpenAPI gerando o
  client TypeScript do app. Marts pequenos carregados em memória (Polars) para os sliders
  responderem em ms.
- **Frontend (`basecast-app`):** Next.js na Vercel, base do Fundsys. O navegador só fala com o BFF
  (route handlers); o token da API fica no servidor. TanStack Query. MapLibre para o mapa.
- **Tempo:** gravar tudo em UTC; preservar hora local e flag de horário de verão das fontes (a ERCOT
  repete uma hora em novembro e usa "hour ending"); crons e exibição em `America/Chicago`.
- **Fora do MVP:** preços de serviços ancilares (série pós-RTC+B tem < 1 ano), divulgações de 60 dias,
  outages, previsão de preço de curto prazo, multi-tenant, escrita de usuário.

## 3. Estrutura dos repositórios

### `basecast-airflow` (frente A)
```
basecast-airflow/
├── CLAUDE.md
├── README.md
├── .env.example              # nunca commitar .env
├── pyproject.toml            # Python 3.12, uv
├── catalog.yaml              # extraído do catálogo da ERCOT (tarefa A0)
├── data/                     # lake local (gitignored): raw/, parquet/, _runs/
├── basecast_pipelines/
│   ├── common/               # storage (local|gcs), http client, manifest, etl_run, schemas
│   ├── sources/              # um módulo por fonte, cada um com run()
│   │   ├── ercot/            # gis.py, load_archive.py, ltlf.py, cdr.py, large_load.py, ...
│   │   ├── census/           # permits.py, housing.py, geo.py
│   │   ├── weather/          # open_meteo.py
│   │   └── territories/      # eia_atlas.py
│   ├── adapters/             # fleet_data_source.py, utility_data_source.py (interfaces + simulados)
│   └── cli.py                # `uv run basecast run <source> [--since --until]`, `basecast inventory`
├── dags/                     # vazio por enquanto; DAGs finos depois
├── tests/                    # pytest com fixtures pequenas (amostras reais recortadas)
└── docs/
    ├── KICKOFF.md
    ├── ercot-data-catalog.md # copiar do arquivo de catálogo que vou fornecer
    ├── data-inventory.md     # gerado por `basecast inventory`
    └── decisions.md
```
Dependências sugeridas (fixar versões): `polars`, `pyarrow`, `httpx`, `tenacity`, `pydantic`,
`openpyxl`, `pyxlsb`, `xlrd`, `pdfplumber`, `geopandas`, `shapely`, `python-dotenv`, `typer`, `pytest`.
`gridstatus` como apoio (fixar versão; métodos variam entre versões).

### `basecast-get-data` (frente C)
```
basecast-get-data/
├── CLAUDE.md
├── README.md
├── pyproject.toml            # Python 3.12, uv, FastAPI
├── basecast_get_data/
│   ├── main.py
│   ├── routers/              # um router por recurso (counties, forecasts, backtest, accounts, runs)
│   ├── schemas/              # modelos Pydantic = o contrato
│   └── data/                 # por enquanto: fixtures JSON; depois: leitura dos marts
├── openapi.json              # exportado a cada mudança; o app gera o client a partir dele
├── tests/
└── docs/
    ├── KICKOFF.md
    ├── data-contract.md      # seção 6; dono do contrato entre dados e frontend
    └── decisions.md
```

### `basecast-app` (frente B)
```
basecast-app/
├── CLAUDE.md
├── README.md
├── src/ (ou app/)            # base do Fundsys: layout, auth, components-app, tema
├── lib/api/                  # client TypeScript gerado do openapi.json do get-data
├── public/geo/               # TopoJSON dos condados (gerado na tarefa A1)
└── docs/
    ├── KICKOFF.md
    └── decisions.md
```

## 4. Frente A — Mineração de dados local (`basecast-airflow`)

### Princípios
- **Raw é imutável.** Nunca sobrescrever. Cada download gera um `_manifest.json` ao lado: URL, id do
  documento/Report Type ID, `fetched_at` (UTC), sha256, bytes, status HTTP. Rodar de novo com o mesmo
  sha256 = pular (idempotência).
- **Parquet tipado:** números como números, datas como datas, timestamps em UTC, mais colunas
  `source_file` e `ingested_at`. Nada de guardar número como texto (dor conhecida do Fundsys).
- **HTTP educado:** um client compartilhado com token bucket (API da ERCOT: limite de 30 req/min;
  usar no máximo ~20), backoff exponencial com jitter, retry em 429/5xx, renovação de token sem
  assumir 1h fixa, User-Agent identificável. Se falhar repetidamente, **parar e avisar**: a ERCOT já
  suspendeu chaves por alta taxa de falhas.
- **Credenciais** em `.env`: `ERCOT_API_USERNAME`, `ERCOT_API_PASSWORD`, `ERCOT_SUBSCRIPTION_KEY`
  (cadastro gratuito no portal de desenvolvedor da ERCOT). Muitos arquivos também baixam direto do
  ercot.com sem login: prefira esse caminho quando existir.
- **etl_run local:** cada execução grava uma linha em `data/_runs/etl_run.parquet`
  (source, started_at, finished_at, status, rows, files, error, eventos).
- Downloads acima de ~1 GB no total de uma fonte: me pergunte antes.

### Tarefas, em ordem (cada uma com critério de pronto)

**A0. Fundação.** Repo, ambiente `uv`, abstração de storage (`file://` agora, `gs://` depois),
client HTTP, manifest, etl_run, CLI. Extrair o bloco YAML do catálogo da ERCOT para `catalog.yaml`
e o texto para `docs/ercot-data-catalog.md`.
*Pronto:* `uv run basecast run --help` funciona; testes da fundação passam.

**A1. Geografia (primeiro, porque tudo depende disso).**
- Baixar o Load Profiling Guide Appendix D (aba **ZipToZone**: CEP → weather zone).
- Baixar a tabela de relacionamento ZCTA ↔ condado do Census (2020).
- Construir `county_weather_zone`: condado (FIPS de 5 dígitos, string) → weather zone pela regra da
  maior área, com colunas de participação e proveniência.
- Baixar fronteiras dos condados do Texas (Census TIGER/cartographic boundary), simplificar e salvar
  como TopoJSON leve para o mapa (254 condados). Esse arquivo vai para `basecast-app/public/geo/`.
*Pronto:* 254 condados com zona atribuída; relatório de casos ambíguos; arquivo do mapa < 2 MB.

**A2. Fila de geração: relatório GIS (PG7-200-ER, Report Type ID 15933).**
Pode começar em paralelo com A1, já na primeira noite: a página da ERCOT só mantém ~7 anos e o
limite de requisições torna o download lento.
- Listar e baixar todos os arquivos mensais disponíveis, de 2019 até hoje. Testar a listagem via API
  `archive/PG7-200-ER` e o download em lote. Ignorar o arquivo de custos PG7-201-ER.
- Parser por aba procurando a linha de cabeçalho com "INR" (a posição varia). Deduplicar por
  INR + data do snapshot (o mesmo projeto aparece em mais de uma aba).
- Saídas: `gis_snapshots` (inr, snapshot_date, sheet, county, fuel, capacity_mw, datas de marcos
  como encontradas, status) e `gis_project_events` (entrada, estudos, acordo assinado,
  operação/comissionamento, cancelamento/inativo): a base da análise de sobrevivência.
*Pronto:* contagem por snapshot; checagem de continuidade dos INR; **relatório com os nomes reais
das colunas de marcos** (pendência em aberto).

**A3. Carga horária por weather zone (2003 → hoje).**
- Baixar os arquivos anuais da página de Hourly Load Data Archives (xls até 2015, zip depois) e
  completar os dias mais recentes com NP6-345-CD.
- Normalizar para formato longo: `ts_utc`, `hour_ending_local`, `dst_flag`, `weather_zone`, `mw`.
  Tratar "24:00" e a hora duplicada no fim do horário de verão.
- O arquivo do ano corrente é sobrescrito todo mês pela ERCOT: por isso o snapshot imutável.
*Pronto:* série horária contínua por zona desde abr/2003; relatório de lacunas.

**A4. Previsões oficiais: LTLF e CDR.**
- LTLF 2025: `Summer-and-Winter-Peaks.xlsx` e a planilha mensal de pico/energia. O xlsb horário
  (~46 MB) é opcional.
- CDR: dezembro de 2025 mais duas ou três edições antigas, com um mapa de parser por edição (o layout
  das abas muda). Incluir o "Generation Resource Capacity Forecast" de maio de 2026.
- Guardar também os números da previsão preliminar de 2026 citados no catálogo (fonte: PUCT 58777),
  marcados como entrada manual com a fonte.
*Pronto:* tabela `official_forecasts` (vintage, fonte, ano-alvo, estação, métrica, mw).

**A5. Grandes cargas: decks mensais em PDF.**
- Descobrir e baixar todos os "Large Load Interconnection Status Update" disponíveis (páginas de
  reuniões do TAC/LLWG e do board), de 2024 até hoje.
- Extrair com `pdfplumber` onde houver tabelas. Onde só houver gráficos, gerar um CSV-modelo para
  extração assistida por LLM, com a coluna `verified` para conferência humana.
- Saída: `large_load_status` (report_date, status_bucket, load_zone, project_type, tsp, mw,
  source_page, extraction_method, verified).
*Pronto:* ≥ 12 snapshots mensais ou um relatório explicando a lacuna; **responder quantos decks
existem e se algum traz tabelas** (pendência em aberto).

**A6. Census.** Building Permits Survey por condado (Texas) e ACS de moradias próprias
unifamiliares por condado.
*Pronto:* tabelas por condado e ano.

**A7. Clima.** Temperatura horária histórica (Open-Meteo) desde 2003 para pontos representativos de
cada weather zone. Documentar os pontos e os pesos escolhidos.
*Pronto:* série por zona alinhada com A3.

**A8. Territórios das concessionárias.** Camada "Electric Retail Service Territories" do EIA Energy
Atlas (HIFLD Open foi desativado), filtrada para o Texas. Calcular localmente, com geopandas, a
tabela condado × concessionária com a fração de área. Montar a lista de cooperativas e municipais
dentro da ERCOT.
*Pronto:* tabela de sobreposição; lista de contas; **avaliar se o recorte por transmissora dos decks
de grandes cargas ajuda a aproximar cooperativas** (pendência em aberto).

**A9. Se sobrar tempo.** Preços históricos por hub e load zone (NP6-785-ER, NP4-180-ER) e MORA.

**Entregável da frente A:** `docs/data-inventory.md` gerado por `basecast inventory`: por dataset,
arquivos, linhas, intervalo de datas, tamanho, checagens e as respostas às pendências.

## 5. Frente B — Base do frontend (`basecast-app`, reaproveitando o Fundsys)

**B0. Copiar a base do app do Fundsys:** layout/shell, auth, `components-app`, tema, setup do
TanStack Query e o padrão de BFF nos route handlers.
Remover: escopo de tenant, RBAC, Prisma e módulos de domínio, branding do Fundsys e qualquer
referência a clientes ou lógica regulatória. Nada de dados do Fundsys.
**Auth:** manter simples. Os jurados precisam entrar sem atrito: conta de demonstração ou um modo
público somente leitura controlado por variável de ambiente. Documentar no README.

**B1. Navegação e páginas (com placeholders):**
- `/explorer`: mapa
- `/forecast` e `/backtest`
- `/accounts` e `/accounts/[id]`: inteligência comercial
- `/data`: fontes, última atualização e histórico de `etl_run` (mostra a robustez do pipeline)

**B2. Acesso a dados:** o BFF chama o `basecast-get-data` (rodando local com fixtures, ver frente C)
usando o client TypeScript gerado do `openapi.json`. Token só no servidor, nunca `NEXT_PUBLIC_`.
Sem mocks dentro do app: as fixtures ficam na API, e o app já nasce falando com o contrato real.

**B3. Mapa:** MapLibre GL com os condados do Texas (TopoJSON gerado em A1), coropleto por FIPS via
feature-state, tooltip, legenda e alternância de métrica. Basemap sem token (ex.: OpenFreeMap) ou
só os polígonos.

**B4. Visual:** seguir os componentes e gráficos do Fundsys, com identidade própria do basecast.

*Pronto:* app roda local e em preview na Vercel; todas as páginas navegáveis com dados das
fixtures; mapa renderiza os 254 condados.

## 6. Frente C — Esqueleto da API e contrato (`basecast-get-data`, pequena)

**C0. Contrato primeiro.** Escrever `docs/data-contract.md` e os modelos Pydantic correspondentes:
- **Chaves:** `county_fips` (string de 5 dígitos), `weather_zone` (COAST, EAST, FWEST, NORTH, NCENT,
  SOUTH, SCENT, WEST), `load_zone`, `utility_id` (ID do EIA), `inr`, datas em ISO 8601 UTC, MW como float.
- **Recursos:** métricas por condado (Explorer), séries de previsão com P10/P50/P90, séries de
  backtest (oficial, ajustado, realizado, nosso modelo), lista e detalhe de contas (gatilhos, déficit
  por ano, próxima ação com evidência) e execuções de pipeline.

**C1. Endpoints com fixtures.** Um router por recurso, devolvendo fixtures JSON realistas e
rotuladas como tal. Exportar `openapi.json` a cada mudança (script ou teste que falha se estiver
desatualizado). Auth por token Bearer simples.

**C2. Depois (fora do escopo agora):** trocar as fixtures pela leitura dos marts, com cache em
memória.

*Pronto:* `uv run` sobe a API local; `/docs` mostra todos os recursos; o app gera o client e
consome as fixtures.

## 7. Regras gerais

- Planejar antes de codar; commits pequenos e descritivos; nunca commitar `data/` nem `.env`.
- Testes com fixtures pequenas recortadas de arquivos reais, principalmente para os parsers.
- Rotular como **simulado** tudo o que vier dos adapters de dados privados e das fixtures.
- Não inventar URLs, IDs ou colunas; confirmar na fonte ou marcar "não verificado".
- Mudou o contrato? Atualizar `data-contract.md`, os modelos Pydantic e o `openapi.json` juntos, e
  regenerar o client no app.
- Fora do escopo agora: deploy do Airflow, carga no BigQuery, modelos, Fleet API mock, lógica do
  módulo comercial, leitura real de marts na API.

## 8. Pendências a responder durante o trabalho

- [ ] Nomes reais das colunas de marcos do GIS (A2)
- [ ] Quantos decks de grandes cargas existem e se trazem tabelas (A5)
- [ ] Se o recorte por transmissora ajuda a aproximar cooperativas (A8)
- [ ] Qual cooperativa usar na demo (depois de A8)
