# Scraping runbook — basecast-airflow

> Roteiro prático da frente A do `docs/KICKOFF.md`. Os princípios de lá valem aqui (raw imutável,
> manifest, Parquet tipado, HTTP educado, etl_run). Este arquivo diz **o que baixar, de onde, como
> descobrir os arquivos e o que sair de cada fonte**.
>
> Fontes de verdade no repositório:
> - `docs/ercot-data-catalog.md`: o catálogo completo, com contexto e ressalvas.
> - `catalog.yaml`: as mesmas entradas em formato estruturado, mais as fontes fora da ERCOT.
>
> Regra de ouro: **descubra os links nas páginas indicadas; não construa URLs.** Os exemplos de URL
> abaixo vêm do catálogo e servem para reconhecer o padrão. Tudo que o catálogo marca como
> "não verificado" continua não verificado até você confirmar e registrar em `docs/decisions.md`.

## 0. Antes de começar

- **Conta na API da ERCOT:** cadastro gratuito no portal de desenvolvedor. A autenticação usa um ID
  token (Azure B2C) mais o header `Ocp-Apim-Subscription-Key`. O token expira: renove sem assumir
  1h fixa. A API bloqueia acesso de fora dos EUA; o Mac mini em Austin atende.
- **Limites da API** (página "API limitations" do portal): até 30 requisições por minuto (acima disso,
  HTTP 429) e até 1.000 arquivos por download de histórico. Use no máximo ~20 req/min. A ERCOT já
  suspendeu uma chave por alta taxa de falhas: se as falhas se repetirem, **pare e me avise**.
- **Retenção:** a busca por linhas na API só vai até 30/nov/2023. Histórico mais antigo vem de
  arquivos (arquivo da API, páginas do ercot.com, produtos anuais "-ER").
- **Espaço em disco:** o núcleo (P0) deve ficar abaixo de ~3 GB. Os arquivos de cenários climáticos
  do LTLF (66–78 MB cada, um por weather zone) são opcionais.
- **IDs de fonte** (prefixo do raw `raw/source=<id>/dt=<data>/`): usar os da tabela da seção 1.

## 1. Mapa das fontes

| Ordem | Tarefa | Source id (raw) | Entrada no `catalog.yaml` | Prioridade | Saídas principais |
|---|---|---|---|---|---|
| 1 | A2 | `ercot_gis` | `gis_report` | P0 | `gis_snapshots`, `gis_project_events` |
| 2 | A1 | `ercot_ziptozone` | `geo_ziptozone` | P0 | `ercot_zip_weather_zone` |
| 3 | A1 | `census_zcta_county` | `census_zcta_county` | P0 | `census_zcta_county`, `county_weather_zone` |
| 4 | A1 | `census_tx_counties_geo` | `census_tx_counties_geo` | P0 | `tx_counties.topo.json` |
| 5 | A3 | `ercot_native_load` | `hourly_load_archive` | P0 | `ercot_load_hourly_wz` |
| 6 | A3 | `ercot_load_wz_daily` | `actual_load_weather_zone` | P0 | complemento de `ercot_load_hourly_wz` |
| 7 | A4 | `ercot_ltlf` | `ltlf` | P0 | `official_forecasts` |
| 8 | A4 | `ercot_cdr` | `cdr` | P0 | `official_forecasts` |
| 9 | A4 | `manual_official_figures` | — | P0 | `official_forecasts` (entradas manuais com fonte) |
| 10 | A5 | `ercot_large_load_decks` | `large_load_status` | P0 | `large_load_status`, `large_load_headlines` |
| 11 | A6 | `census_bps` | `census_bps` | P0 | `census_permits_county` |
| 12 | A6 | `census_acs` | `census_acs_housing` | P1 | `census_housing_county` |
| 13 | A7 | `open_meteo` | `open_meteo_weather` | P0 | `weather_hourly_wz` |
| 14 | A8 | `eia_territories` | `utility_territories` | P1 | `utility_territories`, `county_utility_overlap` |
| 15 | A8 | `eia_861` | `eia_861_utilities` | P0 | `utilities_ercot` (lista de contas) |
| 16 | A9 | `ercot_spp_hist` | `spp_hist_hub_lz` | P1 | `prices_rtm_hub_lz`, `prices_dam_hub_lz` |
| 17 | A9 | `ercot_mora` | `mora` | P1 | `mora_outlook` |

**Fontes adicionadas em 26/set/2026** (pesquisa noturna, ver `docs/source-research-2026-09.md`; já com
módulo e raw baixado):

| Source id | Entrada no `catalog.yaml` | Para quê |
|---|---|---|
| `puct_ccn_territories` | `puct_ccn_service_areas` | polígonos oficiais (não-oficiais segundo a PUCT) de cooperativas, municipais e IOUs, com CCN e G&T |
| `puct_directories` | `puct_utility_directories`, `puct_pgc_facilities` | lista de contas (CCN = `PrimaryIDNo`); usinas por território anfitrião |
| `puct_filings` | `puct_ltlf_tsp_rfi_2026`, `puct_interchange_filings` | pedidos de grandes cargas por transmissora e por tipo, 2026–2032 (58777 item 38) |
| `ercot_tpit` | `ercot_tpit` | projetos de transmissão por condado e data, 2009–2026 |
| `ercot_rtp` | `ercot_rtp_public` | carga submetida pelas TSPs vs. previsão da ERCOT por weather zone, 2014–2025 |
| `ercot_settlement_points` | `settlement_points_mapping` | mapeamento das zonas NOIE (LZ_AEN, LZ_CPS, LZ_LCRA, LZ_RAYBN); janela de 31 dias |
| `ercot_mp_list` | `ercot_mp_list` | TDSPs registradas na ERCOT |
| `ercot_members` | `ercot_members` | membros por segmento, 2013–2026 |
| `ercot_demand_energy` | `demand_energy_report` | picos mensais com hora, por load zone e weather zone, 2008–2026 |
| `ercot_fuel_mix` | `fuel_mix` | geração por fonte a cada 15 min, 2007–2026 |
| `eia_860m` | `eia_860m` | geradores planejados/operando (último mês + dezembros) |
| `eia_860` | `eia_860_annual` | dono da rede de transmissão/distribuição de cada usina |
| `pudl` | `pudl_release`, `pudl_ferc714_forecast` | previsões oficiais de pico da ERCOT 2006–2025 (FERC 714) e changelog do EIA-860M |
| `noaa_ghcnh` | `noaa_ghcnh` | temperatura horária sem restrição de licença, 12 aeroportos |
| `census_pep` | `census_pep_county` | população por condado 2020–2025 |
| `bls_qcew` | `bls_qcew_county` | empregos em data centers (NAICS 518210) por condado |
| `tceq_air_permits` | `tceq_central_registry` | licenças de ar (geradores de backup de data centers) por condado |
| `tx_comptroller` | `cpa_local_dev_agreements`, `cpa_data_centers`, `cpa_jeti` | incentivos fiscais a grandes projetos por condado |

**Fora do MVP** (não baixar agora): `as_dam_mcpc`, `as_rt_mcpc`, `rt_price_adders`, `disclosure_60d`,
`outages`, `demand_energy_report`, `spp_rt`/`spp_dam` diários, `load_forecast_7day`,
`gis_interconnection_costs`. `fuel_mix` e `solar_wind_actual_forecast` ficam para depois do A9: são
úteis para a carga líquida, mas a API só tem linhas desde dez/2023.

## 2. Roteiro por fonte (na ordem de execução)

### 1) `ercot_gis` — fila de geração (A2) — **começar na primeira noite**
- **Onde:** página do produto `https://www.ercot.com/mp/data-products/data-product-details?id=pg7-200-er`
  (EMIL `PG7-200-ER`, Report Type ID `15933`). xlsx mensal, publicado no começo do mês seguinte.
- **Descobrir arquivos, nesta ordem:**
  1. Listagem legada do MIS, sem login: `mis.ercot.com/misapp/GetReports.do?reportTypeId=15933`
     (padrão usado por versões antigas do gridstatus).
  2. Arquivo da API: `GET https://api.ercot.com/api/public-reports/archive/PG7-200-ER` (padrão
     verificado para outros produtos, **não testado para este**), depois download em lote por
     `docId` (POST, até 1.000 por vez).
- **Janela:** a página mostra ~7 anos (2.555 dias); os arquivos começam em 2019. Baixe tudo agora,
  antes que os mais antigos saiam da janela.
- **Ignorar:** o arquivo de custos `PG7-201-ER` (Report Type ID 27292), que aparece na mesma série
  desde mar/2026.
- **Raw:** `raw/source=ercot_gis/dt=<mês do relatório>/<nome original>.xlsx` + manifest.
- **Parse:**
  - Abas conhecidas: "Project Details - Large Gen", "Project Details - Small Gen",
    "Inactive Projects", "Cancellation Update" e a de atualização de comissionamento.
  - Em cada aba, procure a linha de cabeçalho que contém "INR" (a posição varia).
  - Guarde primeiro tudo em formato longo e bruto, `gis_rows_raw` (snapshot, aba, linha,
    nome_da_coluna, valor), antes de normalizar. Isso permite reprocessar quando entendermos os
    cabeçalhos.
  - Normalizado: `gis_snapshots` com `inr`, `snapshot_date`, `sheet`, `county`, `fuel`,
    `capacity_mw` e as datas de marcos como forem encontradas.
  - Deduplicar por INR + snapshot (o mesmo projeto aparece em mais de uma aba).
- **Eventos para a sobrevivência (`gis_project_events`):** entrada = "Screening Study Started"; estudo
  = datas de FIS; acordo = IA assinado; sucesso = aprovação para sincronizar / operação comercial
  (projetos passam pela atualização de comissionamento e depois somem); falha = aparece em
  "Cancellation Update" ou "Inactive Projects". Os nomes exatos dessas colunas **não foram
  verificados**.
- **Checagens:** contagem por snapshot; continuidade dos INR entre meses; **relatório de variação
  de cabeçalhos em todos os snapshots** (responde a pendência); detalhe de projetos pequenos
  (< 10 MW) só existe desde jan/2022.
- **Conferência cruzada:** `Ercot().get_interconnection_queue()` do gridstatus lê o arquivo atual.
  Compare com o seu parse do último snapshot.

### 2) `ercot_ziptozone` — CEP → weather zone (A1)
- **Onde:** Load Profiling Guide, Appendix D (Profile Decision Tree), versão de 30/abr/2024:
  `https://www.ercot.com/files/docs/2024/04/30/Appendix_D_Profile_Decision_Tree_050124.xlsx`.
  Confira em `https://www.ercot.com/mktrules/guides/loadprofiling/current` se há versão mais nova.
- **Parse:** aba **ZipToZone**; leia os cabeçalhos dinamicamente.
- **Saída:** `ercot_zip_weather_zone` (zip, weather_zone, versão do arquivo).

### 3) `census_zcta_county` — ZCTA → condado (A1)
- **Onde:** página de Relationship Files do Census, arquivo 2020 ZCTA → county (não verificado).
- **Saída:** `census_zcta_county` com as áreas de terra.
- **Construir `county_weather_zone`:** para cada condado, some a área de terra dos ZCTAs por weather
  zone e escolha a maior (regra da maioria da área). Guarde as participações e a proveniência.
  Atenção: CEP não é ZCTA; reporte os CEPs do ZipToZone sem ZCTA correspondente.
- **Checagem:** os 254 condados com zona; lista de condados ambíguos (participação < 60%) e dos que
  ficam fora da ERCOT.

### 4) `census_tx_counties_geo` — fronteiras dos condados (A1)
- **Onde:** Cartographic Boundary Files do Census, condados em 1:500k, filtrar `STATEFP=48`
  (não verificado).
- **Saídas:** a geometria completa (para o cálculo de sobreposição no passo 14) e um TopoJSON
  simplificado `tx_counties.topo.json` (< 2 MB) com `county_fips` e nome. Esse arquivo vai para
  `basecast-app/public/geo/`.

### 5) `ercot_native_load` — carga horária histórica (A3)
- **Onde:** `https://www.ercot.com/gridinfo/load/load_hist`. Exemplos do padrão:
  `/files/docs/2026/02/10/Native_Load_2026.zip`, `/files/docs/2025/02/11/Native_Load_2025.zip`,
  `/files/docs/2015/10/22/2014_ercot_hourly_load_data.xls`. **Colete os links da página.**
  O texto da página ainda diz "1995–2016", mas os arquivos vão até 2026.
- **Intervalo:** 2003 → hoje. As 8 weather zones só existem desde abr/2003; antes eram 11 áreas de
  controle (pular para o MVP). Não existe arquivo de 2001.
- **Formatos:** xls até 2015, zip desde 2016.
- **Snapshot:** o arquivo do ano corrente é sobrescrito todo mês (por volta do dia 9). Cada download
  é um novo `dt=`, nunca sobrescrito.
- **Parse:** colunas esperadas `Hour Ending, COAST, EAST, FWEST, NORTH, NCENT, SOUTH, SCENT, WEST,
  ERCOT` (**não verificado**: detecte). Normalize para formato longo: `ts_utc`, `hour_ending_local`,
  `dst_flag`, `weather_zone`, `mw`. Trate "24:00" e a hora 2 duplicada no fim do horário de verão
  (às vezes há coluna de DST).
- **Checagens:** série contínua; soma das zonas ≈ total ERCOT; relatório de lacunas.

### 6) `ercot_load_wz_daily` — ponte até ontem (A3)
- **Onde:** `NP6-345-CD` (Actual System Load by Weather Zone, RTID 13101), arquivo diário do dia
  anterior. Página: `https://www.ercot.com/mp/data-products/data-product-details?id=NP6-345-CD`.
- **Uso:** preencher do último mês do arquivo anual até ontem. Alternativa pronta:
  `Ercot().get_load_by_weather_zone(date)` do gridstatus.
- **Saída:** mesmas colunas de `ercot_load_hourly_wz`, com `source` identificando a origem.

### 7) `ercot_ltlf` — Long-Term Load Forecast (A4)
- **Onde:** `https://www.ercot.com/gridinfo/load/forecast` e as páginas por ano
  `/gridinfo/load/forecast/2013` … `/2025` (não há link de 2015).
- **Baixar da edição 2025:**
  - `2025-ERCOT-Monthly-Peak-Demand-and-Energy-Forecast.xlsx`
  - `Summer-and-Winter-Peaks.xlsx` (picos coincidentes e não coincidentes por zona, 2025–2031)
  - `ERCOT-Peak-Demand-Scenarios.xlsx`
  - `2025_LTLF_Report.docx` (referência)
  - Opcional: `ErcotAdjustedForecast.xlsb` e `TSP-Provided-Hourly-Forecast.xlsb` (~46 MB; `pyxlsb`)
- **Edições antigas (para o backtest):** das páginas por ano, baixar as planilhas de pico das edições
  mais recentes que existirem (ex.: 2018–2024).
- **Edição 2026:** ainda não estava na página. Reverifique a página; se não houver arquivo, use as
  entradas manuais do passo 9.

### 8) `ercot_cdr` — Capacity, Demand and Reserves (A4)
- **Onde:** `https://www.ercot.com/gridinfo/resource` e as páginas `/gridinfo/resource/2000` … `/2025`.
- **Baixar:**
  - Dezembro de 2025:
    `/files/docs/2025/12/19/CapacityDemandandReservesReport_December2025.xlsx` (13 MB)
  - Duas ou três edições de dezembro anteriores (links nas páginas por ano; preferir as "Revised"
    quando existirem)
  - Maio de 2026 não foi publicado. No lugar dele, o "Generation Resource Capacity Forecast":
    `/files/docs/2026/05/18/Generation_Resource_Forecast_May2026.xlsx`
- **Parse:** um mapa de parser por edição (arquivo de configuração: edição → aba → localização da
  tabela de previsão de pico por ano e estação). O layout muda entre edições.
- **Ressalvas:** quebras de metodologia (ELCC para armazenamento, a previsão de 2025 adotando cargas
  informadas pelas transmissoras), reedições "Revised" e definições de estação.

### 9) `manual_official_figures` — números oficiais sem arquivo tabular (A4)
Entradas manuais em `config/manual_official_figures.yaml`, cada uma com fonte e data, carregadas em
`official_forecasts` com `method=manual`:
- Previsão preliminar da LTLF 2026 (PUCT Projeto 58777, protocolada em 15/abr/2026): 278.003 MW em
  2029 e 367.790 MW em 2032.
- Pico do verão de 2026: ~112.000 MW na previsão preliminar; faixa de ~90.500 a 98.000 MW estimada pela
  própria ERCOT (carta no Projeto 58777, Item 38).
- Grandes cargas: ~232.500 MW acompanhados e 8.786 MW aprovados para energizar (ERCOT Monthly de
  jan/2026, dados de 21/jan/2026); 9.042 MW aprovados, com pico não simultâneo observado de 4.004 MW
  e simultâneo de 3.522 MW em mar/2026 (deck do TAC de mar/2026, versão atualizada de 26/mar; a versão de
  12/mar trazia 3.883 e 3.801 MW rotulados "March 2025"). Conferido em 26/set/2026: ver
  `config/manual_official_figures.yaml`.
- Recorde de consumo de jul/2026: registrar os valores de cada fonte com a métrica usada (horária
  integrada vs. instantânea), sem escolher um.

### 10) `ercot_large_load_decks` — fila de grandes cargas (A5)
- **O que existe:** só dados agregados, em PDF. Não há lista pública de projetos (o LLI-# é interno).
- **Onde procurar:**
  - Decks mensais "Large Load Interconnection Status Update" do time de integração para TAC/LLWG.
    Exemplo: `https://www.ercot.com/files/docs/2026/03/12/March-TAC-Report.pdf` (13/mar/2026).
  - Decks do board "Interconnection and Grid Analysis Update" (abr/2026 Item 9, mai/2026 Item 8).
  - Recapitulações "ERCOT Monthly".
  - Página do processo: `https://www.ercot.com/services/rq/large-load-integration`.
  - Os decks antigos ficam nas páginas das reuniões: navegue pelas reuniões mensais do TAC e do
    board de 2024 a 2026 e colete os PDFs cujo título contenha "Large Load".
- **Extrair:**
  - Primeiro o fácil e valioso: os números de manchete de cada deck e de cada ERCOT Monthly (MW
    acompanhados, aprovados, energizados, picos observados) em `large_load_headlines`
    (report_date, metric, mw, source_url, page).
  - Depois o detalhe por estágio: `pdfplumber` onde houver tabela. Onde só houver gráfico,
    renderize a página em PNG e gere um CSV-modelo para extração assistida por LLM, com
    `verified=false` até conferência humana.
- **Saída `large_load_status`:** report_date, status_bucket, load_zone, project_type, tsp, mw,
  source_url, page, extraction_method, verified, definition_version.
- **Ressalvas:**
  - As definições dos estágios mudaram com PGRR115 e com o Batch Zero (PGRR145): guarde
    `definition_version`.
  - MW não aprovados são reclassificados como "No Studies Submitted".
  - Transmissoras com menos de 5 clientes aparecem agrupadas em "Other".
  - A partir de 2027, só cargas com acordo de interconexão assinado entram na previsão oficial.
- **Checagens:** tabela de cobertura por mês; **responder quantos decks existem e se algum traz
  tabelas** (pendência).

### 11) `census_bps` — licenças de construção (A6)
- **Onde:** página de dados da Building Permits Survey do Census, arquivos por condado (não verificado).
- **Saída `census_permits_county`:** county_fips, ano (e mês, se houver), unidades por tipo de
  estrutura (1 unidade, 2–4, 5+). Guardar todas as categorias.

### 12) `census_acs` — moradias (A6, P1)
- **Onde:** Census Data API, ACS 5 anos, tabela de posse × unidades na estrutura (confirmar o ID,
  ex.: B25032). Chave de API opcional.
- **Saída `census_housing_county`:** casas próprias unifamiliares por condado. Use uma única edição
  da ACS por análise.

### 13) `open_meteo` — clima (A7)
- **Onde:** `https://archive-api.open-meteo.com/v1/archive`, `temperature_2m` horária, desde
  2003-01-01, em UTC. Pedidos por ano e por ponto, com pausa entre eles.
- **Pontos:** `config/weather_points.yaml` com 2 a 4 cidades representativas por weather zone e seus
  pesos. Documente a escolha em `docs/decisions.md`.
- **Saída `weather_hourly_wz`:** ts_utc, weather_zone, temp_c (média ponderada), mais a tabela por
  ponto.

### 14) `eia_territories` — territórios das concessionárias (A8)
- **Onde:** camada "Electric Retail Service Territories" do EIA Energy Atlas:
  `https://atlas.eia.gov/maps/geoplatform::electric-retail-service-territories-2/about`
  (ArcGIS FeatureServer: GeoJSON/JSON). O HIFLD Open foi desativado em 26/ago/2025; o arquivo
  alternativo está no DataLumos (projeto 239091, snapshot de 30/set/2024).
- **Baixar:** consultar o FeatureServer filtrando o Texas (nomes de campos não verificados),
  paginando os resultados. Guardar as páginas GeoJSON cruas.
- **Saídas:**
  - `utility_territories`: polígonos e atributos.
  - `county_utility_overlap`, calculada com geopandas numa projeção de área igual (EPSG:5070):
    county_fips, utility_id, participação na área do condado, participação na área da concessionária.

### 15) `eia_861` — lista de contas (A8)
- **Onde:** página do EIA-861, arquivos anuais (não verificado).
- **Saída `utilities_ercot`:** concessionárias da ERCOT com tipo de propriedade (cooperativa,
  municipal, outra), ligadas a `utility_territories` pelo ID do EIA.
- **Referências para conferência:** o Texas tem 67 cooperativas de distribuição, 9 de geração e
  transmissão e 72 municipais, nem todas na ERCOT. As load zones LZ_AEN, LZ_CPS, LZ_LCRA e LZ_RAYBN
  identificam as maiores áreas municipais e cooperativas.
- **Checagem:** avaliar se o recorte por transmissora dos decks de grandes cargas ajuda a aproximar
  cooperativas (pendência).

### 16) `ercot_spp_hist` — preços históricos por hub e load zone (A9, P1)
- **Onde:** `NP6-785-ER` (tempo real, RTID 13061) e `NP4-180-ER` (day-ahead, RTID 13060), arquivos
  anuais desde 2011. Alternativa: `get_rtm_spp(year)` e `get_dam_spp(year)` do gridstatus.
- **Ressalvas:** o RTC+B mudou a formação de preço em 5/dez/2025 (fim do adicional ORDC); correções
  de preço existem; intervalos repetidos no horário de verão.

### 17) `ercot_mora` — Monthly Outlook for Resource Adequacy (A9, P1)
- **Onde:** página de Resource Adequacy, pares PDF + xlsx (`MORA_[Mês][Ano].xlsx`), desde a edição
  de dez/2023. Podem existir versões revisadas.

## 3. Ordem de trabalho

**Primeira noite:**
1. Fundação (A0 do KICKOFF), já com o `catalog.yaml` e o catálogo que vêm neste repositório.
2. Disparar o backfill do GIS (passo 1) em segundo plano: é o download mais lento, por causa do
   limite de requisições.
3. Em paralelo, a geografia (passos 2 a 4).
4. Depois, a carga horária (passos 5 e 6).

**Dia seguinte:**
5. Previsões oficiais (passos 7 a 9).
6. Grandes cargas (passo 10): tarefa mais manual; uma pessoa acompanha a extração.
7. Census, clima e territórios (passos 11 a 15).
8. Se sobrar tempo: passos 16 e 17.

## 4. Acompanhamento

Atualize esta tabela a cada fonte concluída (ou gere pelo `basecast inventory`):

| Source id | Status | Arquivos | Linhas | Intervalo | Observações |
|---|---|---|---|---|---|
| ercot_gis | raw baixado (2026-09-25) | 227 (202 MB) | — | relatórios mai/2014 → ago/2026 | listagem sem login (abr/2019+) + 60 GIS antigos das páginas por ano; inclui 72 relatórios de baterias co-localizadas |
| ercot_ziptozone | raw baixado | 1 | — | versão 2024-04-30 | ainda é a versão vigente |
| census_zcta_county | raw baixado | 2 (6,6 MB) | — | 2020 | arquivo nacional + PDF de layout |
| census_tx_counties_geo | raw baixado | 1 (11 MB) | — | vintage 2025 | shapefile nacional 1:500k; filtrar STATEFP=48 no parse |
| ercot_native_load | raw baixado | 24 (34 MB) | — | 2003 → 2026 | arquivo de 2026 é sobrescrito pela ERCOT todo mês |
| ercot_load_wz_daily | raw baixado | 32 | — | 2026-08-25 → 2026-09-25 | listagem só guarda ~31 dias: rodar com frequência |
| ercot_ltlf | raw baixado | 88 (871 MB) | — | edições 2013 → 2025 | inclui xlsb horários e os 8 cenários climáticos; LTLF 2026 ainda não publicada |
| ercot_cdr | raw baixado | 74 (38 MB) | — | edições 2000 → dez/2025 + Gen. Resource Forecast mai/2026 | só planilhas (PDFs opcionais) |
| manual_official_figures | config criado | — | — | — | `config/manual_official_figures.yaml`, tudo `verified: false` |
| ercot_large_load_decks | raw baixado | 64 (68 MB) | — | jan/2024 → set/2026 | 24 decks de status (último: LLWG 2026-06-19), 24 board updates, 7 ERCOT Monthly, 9 outros |
| census_bps | raw baixado | 356 (62 MB) | — | anual 1990 → 2025; mensal 2000-01 → 2026-08 | arquivos nacionais |
| census_acs | raw baixado | 2 (86 MB) | — | ACS 5 anos 2020–2024 | summary file por tabela (sem chave de API) |
| open_meteo | raw baixado | 288 (71.7 MB) | — | 2003 → 2026-09 | ERA5, 12 pontos; ritmo 3 req/min; plano gratuito só não comercial |
| eia_territories | raw baixado | 14 (38 MB) | — | snapshot HIFLD 2025-08-21 | 256 polígonos no bbox do Texas (cópia ORNL; página do EIA Atlas deu 404) |
| eia_861 | raw baixado | 13 (57 MB) | — | 2013 → 2024 + 2025 early release | |
| ercot_spp_hist | raw baixado | 34 (230 MB) | — | 2010 → 2026 | RTM e DAM por hub/load zone |
| ercot_mora | raw baixado | 74 (47 MB) | — | edições dez/2023 → nov/2026 | xlsx + pdf |
| puct_ccn_territories | raw baixado (2026-09-26) | 12 (33 MB) | — | edição 2026-06-29 | 68 coop, 72 muni, 8 IOU |
| puct_directories | raw baixado | 11 | — | diário | coop, muni, iou, pgc, pgc_facility, agg |
| puct_filings | raw baixado | 564 (578 MB) | — | 58777 (40 itens), 59772 (18), 58481 (217) | todos os documentos com arquivo público; tabela por TSP no slide 5 do `Attachment A.pptx` (58777-38) |
| ercot_tpit | raw baixado | 2 (20 MB) | — | 2009 → jul/2026 | ERCOT reaproveita a URL antiga |
| ercot_rtp | raw baixado | 5 (36 MB) | — | RTP 2014 → 2025 | |
| ercot_settlement_points | raw baixado | 4 | — | 2026-08-26 → 2026-09-23 | janela de 31 dias: rodar semanalmente |
| ercot_mp_list | raw baixado | 1 | — | 2026-09-25 | só o último arquivo fica listado |
| ercot_members | raw baixado | 14 | — | 2013 → 2026 | |
| ercot_demand_energy | raw baixado | 22 (4 MB) | — | 2008 → 2026 | arquivo do ano corrente é sobrescrito todo mês |
| ercot_fuel_mix | raw baixado | 3 (56 MB) | — | 2007 → 2026 | |
| eia_860m | raw baixado | 12 (107 MB) | — | dez/2015 → ago/2026 | opção `all_months=true` para os ~130 meses |
| eia_860 | raw baixado | 1 (24 MB) | — | 2025 final | |
| pudl | raw baixado | 8 (25 MB) | — | release v2026.9.0 | CC-BY-4.0: citar Catalyst Cooperative |
| noaa_ghcnh | raw baixado | 288 (296 MB) | — | 2003 → 2026-09 | parquet por estação e ano |
| census_pep | raw baixado | 1 | — | 2020 → 2025 | Latin-1 |
| bls_qcew | raw baixado | 12 | — | 2014 → 2025 (anual) | muitos condados suprimidos |
| tceq_air_permits | raw baixado | 16 (359 MB) | 690.719 | diário | só programa AIRNSR; bate com a contagem da API |
| tx_comptroller | raw baixado | 9 (8 MB) | — | diário | Ch. 312/380 CSV + registros de data centers e JETI (HTML) |

## 5. Pendências que este trabalho deve responder

- [ ] Nomes reais das colunas de marcos do GIS e como variam entre snapshots (passo 1)
- [~] Quantos decks de grandes cargas existem e se algum traz tabelas (passo 10): 24 decks de status de jan/2024 a jun/2026 (PDF/PPTX, alguns dentro dos zips do TAC); nenhum encontrado após jun/2026. Tabelas: verificar no parse.
- [ ] Se o recorte por transmissora ajuda a aproximar cooperativas (passo 15)
- [x] URLs do Census e do EIA-861 confirmados e registrados no `catalog.yaml` (passos 3, 4, 11, 12, 15)
- [ ] Qual cooperativa usar na demo (depois do passo 15)
