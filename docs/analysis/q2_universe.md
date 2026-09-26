# Q2 — Does the account universe close? (2026-09-26)

Spec: `docs/PHASE0_ANALYSIS.md` §2 Q2. Script: `analysis/q2_universe.py`
(`uv run --group analysis python analysis/q2_universe.py`). Logic: `basecast_pipelines/models/accounts.py`, tested in
`tests/models/test_accounts.py`. Output: the draft `config/utility_crosswalk.yaml` (112 rows) and
`analysis/out/q2_crosswalk.csv` (gitignored). No figure: the score table below is the whole distribution.

Partner mode is **validation**: step 3 only checks that five names exist. Nothing else is computed for them.

## 1. Universe counts

**PUCT CCN layers** (`puct_ccn_territories`, 148 territories): co-op or muni, with ERCOT in `iso_rto`.

| Type | ERCOT (any) | ERCOT only | Mixed ISO | Outside ERCOT |
|---|---|---|---|---|
| Co-op (distribution) | **52** | 39 | 13 (9 ERCOT+SPP, 2 ERCOT+MISO, 1 ERCOT+MISO+SPP, 1 ERCOT+WECC) | 16 |
| Muni | **60** | 60 | 0 | 12 |
| **Universe** | **112** | 99 | 13 | 28 |

`ccn_no` is unique within the 112, so it is the `account_id`. Mixed co-ops keep their whole territory, including the
non-ERCOT part.

**EIA-861** (`eia861_utility`, Texas rows). Co-op = ownership Cooperative; muni = Municipal or Political Subdivision;
BA ERCO = ERCO among the balancing authorities of the utility's Texas sales.

| Data year | Co-ops (BA ERCO) | Munis (BA ERCO) | Political subdivisions (BA ERCO) | ERCOT co-ops + munis |
|---|---|---|---|---|
| 2018 | 68 (46) | 25 (21) | 1 (0) | 67 |
| 2019 | 69 (46) | 66 (54) | 1 (0) | 100 |
| 2024 (latest final) | 63 (43) | 17 (17) | 2 (0) | **60** |
| 2025 (early release) | 55 (39) | 14 (14) | 2 (0) | 53 |

**Why only 17 munis since 2020:** EIA moved small utilities to the short form (861S, `Short_Form_<year>.xlsx`), which
the `eia_861` parser does not read (it reads `Sales_Ult_Cust`, `Utility_Data` and `Service_Territory`). The short-form
utilities still appear in `Service_Territory`, with their ids and counties. 2019 is the one year in the table where
the short-form munis also sit in `Utility_Data`. The raw 2024 zip in the local lake has 61 Texas rows in
`Short_Form_2024.xlsx`:

| 861S 2024, Texas | ERCO | SWPP | MISO |
|---|---|---|---|
| Cooperative | 3 | 3 | 0 |
| Municipal | 43 | 4 | 8 |

So EIA-861 2024 has **106 ERCOT co-ops and munis**: 60 long form plus 46 short form only.

## 2. Crosswalk PUCT ↔ EIA-861

Method (`accounts.match_utilities`):

- Names are normalized: abbreviations expanded (`Coop` → cooperative, `Elec` → electric, `Assn` → association),
  legal suffixes and EIA's state tag (`- (TX)`, `(OK)`) dropped. Then they are cut to their distinctive words
  (`Bartlett City of` and `City of Bartlett - (TX)` both become `bartlett`).
- Names are compared with rapidfuzz `token_sort_ratio`, **within the same type only**.
- Candidates: all 152 Texas EIA-861 ids typed co-op or muni. Ownership comes from any year; for 9 ids with none in
  the parsed tables, the type comes from the name.
- County check: `county_overlap` is the share of the PUCT territory's area that falls in counties the EIA utility
  lists in its latest `Service_Territory`.
- `auto` = score ≥ 90 and county_overlap > 0. Name ties go to the larger overlap, then to names without a foreign
  state tag.

Distribution of the best score per account:

| Score | Accounts |
|---|---|
| [95, 100] | **107** |
| [90, 95) | 0 |
| [80, 90) | 0 |
| [70, 80) | 0 |
| [50, 70) | 3 |
| [0, 50) | 2 |

- **107 auto (95.5%)**: 49 of 52 co-ops and 58 of 60 munis. Every auto row has a county overlap of 0.81 or more. In
  all 107, the best name candidate is also the candidate with the largest county overlap.
- Two name ties were settled by the counties: `Farmers Electric Coop, Inc - (TX)` over the `(NM)` utility, and
  `Tri-County Electric Coop, Inc (TX)` over the `(OK)` one. Both have overlap 1.00.
- **5 review rows**, all brand names that share no word with the EIA filing. The county check finds the right
  utility for each one (overlap ≥ 0.998), and Claude proposes accepting all five (see Review items).
- Reverse check: all 60 long-form EIA 2024 ERCOT co-ops and munis are taken by a PUCT account.

## 3. Partner presence (names only)

| Name (hard-coded) | In the PUCT ERCOT universe | In the matched universe |
|---|---|---|
| Bandera Electric Cooperative | yes | yes |
| Guadalupe Valley Electric Cooperative (GVEC) | yes | yes |
| CoServ | yes | yes |
| Farmers Electric Cooperative | yes | yes |
| Austin Energy | yes | yes |

"(GVEC)" was dropped from the name before matching. Nothing else was computed for these accounts.

## What the matched ids carry

| | Accounts | Long-form sales 2024 (`eia861_sales`) | Only in 861S 2024 (not parsed) |
|---|---|---|---|
| Co-ops | 52 | 49 | 3 |
| Munis | 60 | 17 | 43 |
| **Total** | 112 | **66 (58.9%)** | 46 |

If the parser also read `Short_Form_2024`, every matched id would have a row. City of Waelder's row has blank totals,
though, so 111 of 112 would have numbers.

## Decision

**Go: the universe is PUCT ∩ EIA = 112 accounts** (52 co-ops, 60 munis). 95.5% match with score ≥ 90, and 100%
after the proposed review, against the 80% bar.

Caveat for Q3: matching an id does not bring the data with it. Only 58.9% of the universe has EIA-861 sales in the
database today, because the short form is not loaded.

## Proposed findings row

| Q2 | PUCT universe = 112 (52 co-ops + 60 munis in ERCOT); EIA-861 2024 ERCOT co-ops/munis = 106 (60 long form + 46 short form only, 861S not parsed); matched ≥90 = 107/112 (95.5%); after review = 112/112 (5 proposed accepts, pending Pablo); EIA sales in DB for 66/112 (58.9%) | go (PUCT ∩ EIA) | A2 | — |

## Review items for Pablo

1. **[PERGUNTAR] Crosswalk rows with score < 90.** Claude's default is to accept all five (they are in the YAML with
   `status: review`, `decision: accept`), and they are already used:

   | ccn_no | PUCT name | Proposed EIA utility (id) | Name score | County overlap | Reason |
   |---|---|---|---|---|---|
   | 30036 | Comanche Electric Cooperative Association | Comanche County Elec Coop Assn (4295) | 69.6 | 1.00 | EIA adds "County" |
   | 30040 | PenTex Energy | Cooke County Elec Coop Assn (4262) | 33.3 | 1.00 | PenTex Energy is the new name of Cooke County ECA (rename not verified at the source) |
   | 30047 | CoServ Electric Cooperative, Inc. | Denton County Elec Coop, Inc (5078) | 31.6 | 1.00 | CoServ is Denton County Electric Cooperative's trade name |
   | 30022 | BTU Rural Electric Division | City of Bryan - (TX) (2442) | 25.0 | 1.00 | BTU is the City of Bryan's utility. EIA files one id; whether it also covers the city system (not in the PUCT muni layer) is not verified |
   | 30031 | CPS Energy | City of San Antonio - (TX) (16604) | 14.3 | 1.00 | CPS Energy is owned by the City of San Antonio |

2. **Parser follow-up (not done here, outside this task's files).** `eia_861` should also load
   `Short_Form_<year>.xlsx` into `eia861_sales`. It has total revenue, sales and customers by utility, with no
   sector split. Without it, EIA-861 size signals cover 57% of the universe; with it, 99% (Q3).
3. **Confirm the auto rule:** score ≥ 90 **and** at least one shared county. It passed all 107 name matches (minimum
   overlap 0.81).

## Proposed decisions.md lines

- 2026-09-26 — Account universe = the 112 ERCOT co-ops (52) and munis (60) of the PUCT CCN layers (`account_id` =
  `ccn_no`). Each is matched to an EIA-861 id by normalized name + rapidfuzz `token_sort_ratio` within type,
  confirmed by the `Service_Territory` counties (auto = score ≥ 90 and a shared county); draft in
  `config/utility_crosswalk.yaml`. — Q2: 107/112 auto (95.5%) and 112/112 after 5 reviewed brand-name cases clear the
  80% bar.
- 2026-09-26 — `eia_861` should also parse `Short_Form_<year>.xlsx` (861S) into `eia861_sales`. — Since 2020, 46 of
  the 106 ERCOT co-ops and munis in EIA-861 file only the short form; without it, 41% of the account universe has no
  EIA sales or customers.

## Useful for the core features

- Each /accounts row gets a stable key (`ccn_no`) that ties three things together: the PUCT polygon (map and
  territory counties), the EIA-861 id (meters, MWh, revenue) and the PUCT directory (website, address).
- `county_overlap` and the account × county links from `county_utility_overlap_puct` feed the Explorer
  drill-down: from a county to the co-ops and munis that serve it, and back.
- The crosswalk YAML is small and reviewable. Pablo's edits to the five review rows flow into `build_universe()`
  without code changes.
- The 13 mixed-ISO co-ops can carry an "ERCOT + SPP/MISO" badge. Their signals cover the whole territory.
