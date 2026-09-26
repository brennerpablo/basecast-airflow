# X4 — EIA-861 short form (861S) for the account universe (2026-09-26)

Script: `analysis/x4_eia861_short_form.py` (`uv run --group analysis python analysis/x4_eia861_short_form.py`).
Logic: `basecast_pipelines/models/eia861_short_form.py`, tested in `tests/models/test_eia861_short_form.py`.
Outputs (gitignored): `analysis/out/x4_short_form_layout.csv`, `x4_short_form_tx.csv`, `x4_coverage_by_year.csv`,
`x4_signals.csv`, `x4_signal_summary.csv`, `x4_signal_pairs.csv`, `x4_size_ranks.csv`, `x4_scores.csv`,
`x4_jumps.csv`. No figure.

The short form is read straight from the raw zips in the local lake (`data/raw/source=eia_861/dt=2026-09-25/`),
and the long form comes from `eia861_sales` (read-only). No pipeline, parser or database change.

**Validation lock.** The five partner accounts were held out before any number was computed, with the same code
as `analysis/q3_signals.py`. Every count, statistic, rank and example below covers **107 accounts** (48 co-ops,
59 munis). No EIA id is shared by two of them.

## 1. What the short form carries

**Files.** Every zip from 2013 to 2024 has a `Short_Form_<year>` workbook except 2019. That year every utility
filed the long form, and 42 Texas rows in `eia861_sales` 2019 carry `short_form = true`. The files are `.xls` in
2013–2014 and `.xlsx` afterwards, plus `Short_Form_2025_Data_Early_Release.xlsx`. Each has one sheet, `861S`.

**Columns.** There are four header layouts. The headers were read from the files, not assumed.

| Years | Columns |
|---|---|
| 2013–2014 | Data Year, Utility Number, Utility Name, State, BA_CODE, Total Revenue (Thousand Dollars), Total Sales (MWh), Total Customers, Water Heater, Net Metering, Demand Side Management, Time Based Programs |
| 2015–2016 | the same plus **Ownership** after Utility Name |
| 2017–2024 | `BA_CODE` becomes **BA Code**; 2022 names the water-heater column `w_heater` |
| 2025 early release | the 2017+ layout, plus a note row above the header and a leading note column on every row |

- The short form has totals only. It has no sector split, part, service type or data type (observed or imputed).
  EIA writes a missing value as `.`.
- Each utility has one row per state, and no Texas utility-year appears twice.
- **No utility-year sits in both forms** (0 overlaps against `eia861_sales`).

| Rows | 2013–2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 ER |
|---|---|---|---|---|---|---|---|---|
| US | 1,114–1,122 | — | 1,732 | 1,723 | 1,727 | 1,718 | 1,724 | 1,614 |
| Texas | 50 each | — | 62 | 61 | 62 | 62 | 61 | 57 |
| Texas without customers | 0–2 | — | 2 | 0 | 1 | 1 | 1 | 0 |

In 2024 the Texas rows are 55 Municipal and 6 Cooperative. Ownership is blank in 2013–2014.

**What it adds to the 107 accounts.** Counts are accounts with positive customers:

| | Long form only (today) | Long + short | Added by 861S |
|---|---|---|---|
| All, 2024 | 61 (57.0%) | **106 (99.1%)** | 45 |
| Co-ops, 2024 | 45 / 48 (93.8%) | 48 / 48 (100%) | 3 |
| Munis, 2024 | 16 / 59 (27.1%) | 58 / 59 (98.3%) | 42 |

The one gap is City of Waelder (muni). Its 2024 short-form row is blank, and it last reported customers in 2016.
Sales, revenue and average price have the same 2024 coverage as customers (106).

**Coverage by year, long + short combined** (accounts with customers > 0; sales and revenue within ±1 of this):

| Year | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 ER |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| All (of 107) | 107 | 107 | 106 | 107 | 105 | 105 | 103 | 105 | 106 | 106 | 106 | 106 | 96 |
| Co-ops (of 48) | 48 | 48 | 48 | 48 | 48 | 48 | 48 | 48 | 48 | 48 | 48 | 48 | 44 |
| Munis (of 59) | 59 | 59 | 58 | 59 | 57 | 57 | 55 | 57 | 58 | 58 | 58 | 58 | 52 |
| of which from 861S | 39 | 39 | 38 | 39 | 37 | 37 | 0 | 45 | 46 | 46 | 46 | 45 | 42 |

Four munis have no 2019 row, so the 2019 → 2024 growth rates cover 103 accounts: Bartlett (no 2018–2019),
Hempstead (no 2017 or 2019), Robstown (no 2019) and Waelder (only 2013, 2014 and 2016).

## 2. The combined signals

Definitions (`eia861_short_form.utility_signals`, final releases only):

- `eia_customers` = **wires customers**: bundled customers from the long or short form, plus delivery-only
  customers from `Delivery_Companies_<year>.xlsx` (see §4).
- `eia_customer_cagr` runs 2019 → 2024.
- `eia_price` = bundled revenue ÷ bundled sales (thousand USD / MWh = USD/kWh). `eia_price_cagr` is its
  2019 → 2024 growth.

| Signal | Coverage | Min | p10 | Median | p90 | Max | Co-op median | Muni median |
|---|---|---|---|---|---|---|---|---|
| eia_customers | **99.1%** (106) | 222 | 1,681 | 13,059 | 75,869 | 962,272 | 23,385 | 3,012 |
| eia_customer_cagr | **96.3%** (103) | −3.8% | −0.04% | 1.40% | 4.14% | 10.6% | 2.11% | 1.09% |
| eia_price (USD/kWh) | 99.1% | 0.048 | 0.088 | 0.118 | 0.140 | 0.168 | 0.115 | 0.119 |
| eia_price_cagr | 96.3% | −10.7% | −1.8% | 2.6% | 6.9% | 10.3% | 2.8% | 2.4% |
| eia_res_sales_share | 57.0% (long form only) | 0.044 | 0.174 | 0.488 | 0.731 | 0.881 | — | — |

- Coverage by type: co-ops 100% on all four new signals; munis 98.3% on customers and price and 93.2% on the CAGRs.
- Eleven accounts lost meters between 2019 and 2024: 10 munis and 1 co-op.
- The −44.5% minimum CAGR in Q3 was Lubbock's retail-choice break. Counting delivery-only customers moves Lubbock
  to +0.72% a year (§4).

**Correlation with the Q3 signals** (Spearman, accounts that have both):

| Pair | ρ | n |
|---|---|---|
| eia_customers × eia_revenue_kusd | 0.979 | 106 |
| eia_customers × eia_sales_mwh | 0.971 | 106 |
| eia_customers × population | 0.876 | 106 |
| eia_customers × owner_sf_homes | **0.870** | 106 |
| eia_customers × permit_units | 0.861 | 105 |
| eia_customer_cagr × permit_units | 0.720 | 102 |
| eia_customer_cagr × eia_customers | 0.666 | 103 |
| eia_customer_cagr × owner_sf_homes | 0.662 | 103 |
| eia_customer_cagr × permits_per_1k | 0.518 | 102 |
| eia_customer_cagr × pop_growth | **0.430** | 103 |
| eia_customers × pop_growth | 0.116 | 106 |

- **By type**, eia_customer_cagr × pop_growth is 0.75 among co-ops but 0.42 among munis. Among co-ops, meter
  growth tracks size (ρ 0.86 with owner_sf_homes): the big suburban co-ops grow fastest.
- Q3 saw ρ 0.68 between eia_customer_cagr and pop_growth on the 61 long-form accounts. With the munis in, it drops
  to 0.43, so meter growth is a different signal from area-apportioned population growth, not a copy of it.

**Muni size bias.** Each account gets a percentile rank within the 106 (the score method) under each size signal:

| Type | n | Customers per apportioned owner-SF home, median [p10–p90] | Median rank, owner_sf_homes | Median rank, eia_customers | In the top half: owner_sf → eia |
|---|---|---|---|---|---|
| Co-op | 48 | 1.26 [0.63–1.91] | 0.750 | 0.675 | 45 → 40 |
| Muni | 58 | **31.1 [3.9–118.7]** | 0.278 | 0.278 | 9 → 14 |

- **The scale bias is real and large.** Area apportionment gives a muni about 25 times fewer homes per meter than a
  co-op (medians 31.1 vs 1.26).
- **The rank bias is small.** Munis really are small: the median muni has 3,012 meters and the median co-op
  23,385. The munis' median rank does not move (0.278). Five more munis reach the top half, and 6 munis move ≥ 30
  percentile points (5 up), against 3 co-ops (all down).
- So `eia_customers` fixes the measurement, but the order changes only a little. Within each type the two size
  signals agree at ρ ≈ 0.77.

**Price as a "rate pressure" candidate.**

- It carries information the other signals do not. Every |ρ| with the public signals is ≤ 0.24, and eia_price ×
  eia_price_cagr is 0.58.
- The price level is confounded by customer mix: ρ 0.60 with the residential sales share on the 61 long-form
  accounts. The short form cannot correct for mix because it has no sector split.
- The top quintile of eia_price_cagr (≥ 5.1% a year) holds 14 co-ops and 7 munis.
- No price jump beyond ±50% falls inside 2019–2024 (§4).

## 3. The Q3 rule on the new signals, and the proposed weights

The rule: coverage ≥ 80%, not near-constant (mode < 90%), and one signal per cluster with ρ > 0.8.

- **Pass:** eia_customers, eia_sales_mwh, eia_revenue_kusd, eia_customer_cagr, eia_price and eia_price_cagr, plus
  every public signal from Q3.
- **Fail:** eia_res_sales_share, at 57.0%. The short form has no sector split, so no EIA-861 fix can lift it.
- **Size cluster:** eia_customers, eia_sales_mwh, eia_revenue_kusd, population, owner_sf_homes and permit_units
  (ρ 0.86–0.98). Only one member is weighted: **eia_customers replaces owner_sf_homes**, because it counts real
  meters instead of homes apportioned by area.
- eia_customer_cagr stays below 0.8 with every weighted signal: 0.67 with eia_customers, 0.52 with permits_per_1k
  and 0.43 with pop_growth.

**Proposed weights.** `config/account_score.yaml` is not edited. The proposal follows the rule already written in
the YAML's `pending` block before this data existed: eia_customers takes owner_sf_homes' weight, and
eia_customer_cagr, once at ≥ 80%, takes 0.10 from pop_growth.

| Signal | Q3 (current) | **X4 (proposed)** | Why |
|---|---|---|---|
| pop_growth | 0.25 | **0.15** | Still the broadest growth signal (100%), now sharing growth with real meter growth |
| owner_sf_homes | 0.20 | **0** | Replaced; stays as a diagnosis fact ("≈ N owner-occupied single-family homes") |
| eia_customers | — | **0.20** | Real meters (99.1%); fixes the 25× scale error for munis |
| eia_customer_cagr | — | **0.10** | Meters added 2019 → 2024 (96.3%); the most direct growth measure; ρ 0.43 with pop_growth |
| permits_per_1k | 0.15 | 0.15 | unchanged |
| dc_sites | 0.15 | 0.15 | unchanged |
| zone_peak_cagr | 0.15 | 0.15 | unchanged (placeholder until A3) |
| owner_sf_share | 0.10 | 0.10 | unchanged |
| eia_price, eia_price_cagr | — | **0** (pending) | Pass the rule, but see below |

- The script checks the proposal: the weights sum to 1, every weighted signal passes, and no weighted pair is
  redundant. The largest |ρ| between two weighted signals is 0.731 (pop_growth × permits_per_1k), then 0.666
  (eia_customers × eia_customer_cagr).
- **Effect on the order** (107 accounts, no partners):
  - Spearman between the Q3 and X4 scores is 0.939.
  - The median position change is 5 places (p90 17, max 43).
  - The top 10 keep 8 of 10 accounts and the top 25 keep 23 of 25.
  - Munis in the top 25 go from 7 to 8. The median position of co-ops goes from 45.5 to 39.5; that of munis stays
    at 67.
- **Why price stays at 0 for now.**
  - Its direction is not verified: rising rates could make a co-op more open to a capacity partner, or signal
    costs outside Base's reach (for example Uri securitization or fuel pass-through).
  - The price level is confounded by customer mix.
  - Proposed use: a diagnosis chip and a "rate pressure" trigger (top-quintile eia_price_cagr). It could enter
    later at 0.05, taken from zone_peak_cagr once A3 replaces that placeholder (review item 3).

## 4. Jumps beyond ±50% (combined series, final releases, 107 accounts)

There are 8 utilities with 11 flags. **None falls at a long ↔ short form switch.**

| Utility | Type | Measure | Years | Before → after | Change | Read |
|---|---|---|---|---|---|---|
| Lubbock Power & Light | muni | customers | 2023 → 2024 | 110,848 → 5,614 | −94.9% | Delivery-only customers, see below |
| Lubbock Power & Light | muni | sales | 2023 → 2024 | 2.53 → 0.59 TWh | −76.7% | same |
| Lighthouse Electric Coop | co-op | sales | 2022 → 2023 | 261 → 440 GWh | +68.9% | cause not verified (same as Q3) |
| Jackson Electric Coop | co-op | sales | 2013 → 2014 | 305 → 561 GWh | +84.0% | outside the window |
| Bartlett | muni | sales | 2016 → 2017; 2020 → 2021 | 9.0 → 30.2 GWh; 10.3 → 18.9 GWh | +234%; +83% | 861S; no 2018–2019 rows; cause not verified |
| Moulton | muni | sales, price | 2015 → 2016 | 11.8 → 594.4 GWh | +4,947% (price −98%) | the 2016 value is ~50× the years around it (9–12 GWh): a filing error, not verified |
| Goldsmith | muni | sales | 2017 → 2018 | 3.3 → 6.0 GWh | +79% | 861S, small base |
| Castroville | muni | price | 2016 → 2017 | 0.068 → 0.137 USD/kWh | +101% | 861S, cause not verified |
| Farmersville | muni | price | 2013 → 2014 | 0.062 → 0.120 USD/kWh | +92% | 861S, cause not verified |

**Inside the 2019 → 2024 CAGR window**, only Lubbock is flagged, on customers and sales. No price jump falls in
the window.

**Lubbock is confirmed from EIA's own files.**

- `Delivery_Companies_2024.xlsx` (in the 2024 zip, not parsed today) lists City of Lubbock (id 11292) with
  **105,080 delivery-only customers** (part C, service type Delivery) and 1.97 TWh.
- Bundled service fell to 5,614 customers and 0.59 TWh.
- Wires customers were 110,694 in 2024 against 110,848 in 2023 (−0.1%). Sales were 2.56 TWh against 2.53 TWh
  (+1.2%).
- Lubbock is not in the 2020–2023 delivery files.
- In the 2025 early release it has no bundled row and 109,490 delivery customers.
- This fits a move to retail choice in 2024. The program and its start date are not verified at the source; the
  EIA data only shows the switch to delivery service.
- The long-form rule (drop part C customers, `accounts.eia_totals`) is right for state totals but wrong for the
  size of a wires utility, so X4 counts wires customers. Lubbock's 2019 → 2024 CAGR is then **+0.72% a year**.

**Nueces Electric Cooperative** is the only other universe utility in the delivery files. It had 2,058 delivery
customers in 2020 and 1,245 in 2024, which lifts its 2024 count from 52,603 to 53,848. Its CAGR stays on bundled
customers (4.17% a year): its 2019 delivery count is not in the local zips, because 2019's `Sales_Ult_Cust` has
no Texas part C rows.

## Proposed findings row

| X4 | 861S read from the raw zips (2013–2018, 2020–2024, 2025 ER; none in 2019; 4 header layouts; totals only). 107 accounts (partners held out): eia_customers 57.0% → **99.1%** (munis 27.1% → 98.3%; +45 accounts), eia_customer_cagr 2019→2024 **96.3%**, price 99.1%. eia_customers × owner_sf_homes ρ 0.87 (replace); CAGR × pop_growth ρ 0.43. Muni homes under-apportioned ~25× (customers per SF home 31.1 vs 1.26), but munis' median rank is unchanged (0.278; munis are small). Lubbock 2024 = retail choice in EIA's own files (105,080 delivery-only + 5,614 bundled = 110,694 wires, −0.1%). Proposed weights: pop_growth .15, eia_customers .20, eia_customer_cagr .10, permits_per_1k .15, dc_sites .15, zone_peak_cagr .15, owner_sf_share .10 (ρ Q3 vs X4 score 0.94) | parser change for 861S + Delivery_Companies; weights pending Pablo | A2 | — |

## Proposed decisions.md lines

- 2026-09-26 — `eia_861` also loads `Short_Form_<year>` (861S) and `Delivery_Companies_<year>` into
  `eia861_sales` (spec in `docs/analysis/x4_eia861_short_form.md`). — X4: with them, EIA customers cover 106 of
  the 107 non-partner accounts (99.1%, munis 98.3%) instead of 57%, and Lubbock's 2024 break (retail choice) is
  explained by 105,080 delivery-only customers.
- 2026-09-26 — Account size is counted as wires customers (bundled, long or short form, plus delivery-only), not as
  the long-form bundled count. — Part C customers are dropped for state totals, but they are the wires utility's
  meters; without them Lubbock falls 95% in 2024 and its 5-year CAGR reads −44.5% instead of +0.7%.
- 2026-09-26 — Account score (proposed, pending Pablo): eia_customers 0.20 replaces owner_sf_homes;
  eia_customer_cagr enters at 0.10, taken from pop_growth (0.25 → 0.15); the other weights are unchanged. — The
  rule Q3 wrote down before the data: both pass (99.1% and 96.3% coverage), eia_customers × owner_sf_homes
  ρ = 0.87 (one of them), eia_customer_cagr × pop_growth ρ = 0.43; fixed with the partners held out.
- 2026-09-26 — Average revenue per kWh and its 2019 → 2024 growth enter the account diagnosis ("rate pressure"
  chip and trigger), not the score. — They pass the rule and are independent (|ρ| ≤ 0.24), but their direction for
  a Base partnership is not verified, and the price level mixes in customer mix (ρ 0.60 with the residential share).

## Review items for Pablo

1. **[PERGUNTAR] Approve the X4 weights** (table in §3) before A2 reveals the partners, or keep Q3's. The change
   is small (score ρ 0.94, top-25 keeps 23 of 25).
2. **Wires customers vs bundled.** X4 adds delivery-only customers (it changes Lubbock and Nueces only). The default
   is to adopt it. The alternative is to keep bundled and flag Lubbock as broken, as Q3 proposed.
3. **Price in the score?** The default is 0 (diagnosis and trigger only). Option: 0.05 for eia_price_cagr, taken
   from zone_peak_cagr once A3 lands.
4. **The muni size result changes the story.** Q3 review item 3 assumed munis rank low because of the area
   apportionment. X4 shows they rank low because they are small (median 3,012 meters). If Base wants munis
   ranked on their own terms, the answer is ranking within type (option b of Q3), not better data.
5. **Waelder** has no customers after 2016 (its 2024 861S row is blank). Keep it in the universe with a "no EIA
   data" badge (default), or drop it?
6. **Moulton 2016** (594 GWh for a town that sells about 11 GWh) is outside the CAGR window. Leave it as filed
   (default) or null it in the diagnosis time series.

## Useful for the core features (/accounts diagnosis)

- **Size and trend chips for every account**, co-ops and munis alike: "N meters (2024), +X% a year since 2019",
  "Y GWh sold", "average rate Z ¢/kWh, +W% a year since 2019". Before X4 this worked for 57% of accounts; now it
  works for 106 of 107.
- **Per-account time series 2013–2024** of customers, sales, revenue and price from one combined table, with the
  form marked. A small sparkline on `/accounts/[id]` needs no new source.
- **Triggers:** a top-quintile price CAGR ("rate pressure", 21 accounts at ≥ 5.1% a year) and negative meter
  growth (11 accounts, 10 of them munis: shrinking towns, a weaker case).
- **Data-quality badges:** "retail choice since 2024 (delivery-only customers)" for Lubbock, "last EIA data 2016"
  for Waelder, and "filing outlier" for Moulton 2016. These support the robust-pipeline story.
- **Program flags from the 861S** (net metering, demand-side management, time-based programs, Y/N) exist for the
  short-form utilities only. Not analysed here; possibly a "has no DSM program" hook for the pitch (not verified).

## Parser change spec (for later; not done here)

- **File:** `basecast_pipelines/parsers/territories/eia_861.py`, dataset `eia861_sales` (`by_file`, one zip per
  data year). `parse_sales` adds two readers to the `Sales_Ult_Cust` rows:
  1. **Short form**, member `^Short_Form_\d{4}\w*\.xlsx?$`, one sheet (`861S`).
     - Find the header by `utility number` + `customers`. Map columns by label: `revenue` → `revenue_thousand_usd`,
       `sales` → `sales_mwh`, `customers` → `customers`, `ownership`, `ba code`/`BA_CODE` → `ba_code`, `state`.
     - `.` → null; keep Texas only.
     - Rows go out as `sector = 'total'`, `short_form = true`, `part`, `service_type` and `data_type` null.
     - A missing member (the 2019 zip) is not an error.
     - `models/eia861_short_form.parse_short_form` is this reader, typed and tested, and can move over as is.
  2. **Delivery companies**, member `^Delivery_Companies_\d{4}\w*\.xlsx?$` (from the 2020 zip).
     - Its three-row header is the `Sales_Ult_Cust` block, so `_sales_sheet` reads it unchanged.
     - Rows keep part C and service type Delivery, for every sector.
- **Program flags** (`net_metering`, `demand_side_management`, `time_based_programs`; `Water Heater` is `.` in
  every Texas row of 2013, 2018, 2022 and 2024, the years checked) do not fit `eia861_sales`. Either skip them, or put them in a small
  `eia861_short_form_programs` table (utility_id, data_year, early_release, three booleans).
- **Consumers.**
  - `accounts.eia_totals` picks up the short-form rows without a change (sector total, not part C), and keeps
    dropping delivery customers.
  - Add a wires option there (bundled + part C customers, and the rule for a utility that already had delivery
    customers in the first delivery year) before `eia_customers` enters the score. `add_delivery` in the X4 module
    has the rule and its test.
- **Checks after reprocessing:**
  - 61 Texas short-form rows for 2024 and 57 for the 2025 early release.
  - 0 utility-years in both forms.
  - Waelder 2024 present with null totals.
  - Lubbock 2024 part C = 105,080 customers.
- **Re-run:** `basecast process` over the 13 zips. That each zip's rows are replaced on re-run (`by_file`) is not
  verified here.
