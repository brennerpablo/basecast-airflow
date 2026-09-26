# X13 — Large-load requests through the G&T co-ops

Exploration after X11. Run on 2026-09-26 against production Postgres (`basecast_reader`, read-only):
`puct_tsp_large_load_requests` (58777 item 38, filed 2026-04-15, PPTX and PDF copies agree), `puct_ccn_territories`
(`gt_cooperative`), `ercot_members`, `county_utility_overlap_puct`, `eia861_sales`, `tceq_data_center_sites`,
`large_load_chart_values` (Batch Zero Update, Sep 2026), plus the EIA-861 short forms in the local lake (X4).

- Script: `analysis/x13_gt_large_load.py` (`uv run --group analysis python analysis/x13_gt_large_load.py`, ~20 s)
- Model code: `basecast_pipelines/models/gt_large_load.py` (pure functions + thin loaders; `accounts.py`, `triggers.py`,
  `eia861_short_form.py`, `large_load.py` imported unchanged)
- Tests: `tests/models/test_gt_large_load.py` (synthetic, no DB; 11 tests, pass with `tests/test_partner_lock.py`)
- Outputs (gitignored) in `analysis/out/`: `x13_rfi_by_tsp.csv`, `x13_rfi_entity_type.csv`, `x13_coop_share.csv`,
  `x13_segment_check.csv`, `x13_sales_vs_rfi.csv`, `x13_gt_members.csv`, `x13_member_allocation.csv`,
  `x13_named_counties.csv`, `x13_exposure_facts.csv`, `x13_coverage.csv`

**Validation lock.** The five partner accounts are held out of the account universe before anything account-level is
computed (107 analysed accounts, as in Q3). TSP-level totals over the whole RFI are filed numbers, not account-level.
Member lists, allocations, coverage and rankings below are over the 107 only; allocation denominators exclude the
held-out accounts, so each member share is an upper bound.

**What the RFI is.** MW of large-load requests submitted to each transmission service provider (TSP), cumulative by
year of service, 2026–2032. They are **requests, not forecasts** (Q5: ~0.32–0.35 of promised MW reached approval to
energize; X7 uses ~0.19). A TSP is the transmission owner at the interconnection point, **not the retail utility**:
MW "at a G&T" is not the same as MW "in co-op retail territory". The RFI gives **no location below the TSP**.

## 1. The RFI by entity type (Q1)

Entity types: IOU wires (Oncor, AEP, TNMP, CenterPoint), transmission-only IOUs (WETT, Lone Star), public power (CPS
Energy as a muni, LCRA TSC), G&T co-ops (Brazos, Golden Spread, Rayburn, STEC), unknown ("Aggregate of TSPs with
< 3 sites"). No distribution co-op files as a named TSP; any would sit in the aggregate (not verified). Cross-check
against ERCOT's 2026 membership segments (`ercot_members`): 11 of 12 named TSPs agree; **LCRA** sits in ERCOT's
"cooperative" segment but is classified here as public power (a river authority; `x13_segment_check.csv`). The TSP
rows sum to the filed total within 1 MW every year.

MW requested (cumulative):

| Entity type | TSPs | 2026 | 2027 | 2028 | 2029 | **2030** | 2031 | 2032 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| IOU wires | Oncor, AEP, TNMP, CenterPoint | 9,301 | 34,917 | 76,445 | 108,440 | **136,376** | 154,346 | 169,267 |
| Transmission-only IOU | WETT, Lone Star | 300 | 2,800 | 5,500 | 6,650 | **7,850** | 8,700 | 9,150 |
| **G&T co-op** | Brazos, Golden Spread, Rayburn, STEC | 1,005 | 6,491 | 22,459 | 37,473 | **45,734** | 51,228 | 52,594 |
| Distribution co-op | none named | 0 | 0 | 0 | 0 | **0** | 0 | 0 |
| Public power | LCRA | 400 | 876 | 1,887 | 3,980 | **4,422** | 5,232 | 5,697 |
| Muni | CPS Energy | 141 | 141 | 508 | 626 | **1,070** | 1,270 | 1,600 |
| Unknown | aggregate of TSPs with < 3 sites | 297 | 307 | 1,617 | 3,491 | **4,091** | 4,691 | 4,691 |
| **Total** | | 11,444 | 45,532 | 108,416 | 160,660 | **199,543** | 225,467 | 242,999 |

By G&T: Brazos 0.1 → 26.6 (2030) → 32.2 GW (2032); Golden Spread 0.9 → 16.6 → 17.4 GW; Rayburn 0 → 1.8 → 2.0 GW;
STEC 0 → 0.8 → 1.1 GW.

Shares by group, with the co-op readings:

| Group | 2026 | 2027 | 2028 | 2029 | **2030** | 2032 |
|---|---:|---:|---:|---:|---:|---:|
| IOU (wires + transmission-only) | 83.9% | 82.8% | 75.6% | 71.6% | **72.3%** | 73.4% |
| **Co-op (G&T)** | 8.8% | 14.3% | 20.7% | 23.3% | **22.9%** | 21.6% |
| Co-op incl. LCRA (ERCOT's segment) | 12.3% | 16.2% | 22.5% | 25.8% | **25.1%** | 24.0% |
| Co-op over known TSPs (aggregate out) | 9.0% | 14.4% | 21.0% | 23.8% | **23.4%** | 22.1% |
| Public power (CPS + LCRA) | 4.7% | 2.2% | 2.2% | 2.9% | **2.8%** | 3.0% |
| Unknown | 2.6% | 0.7% | 1.5% | 2.2% | **2.1%** | 1.9% |

- **The co-op share grows with the horizon:** 8.8% of 2026 requests, ~23% from 2029. The G&T requests are
  back-loaded: MW 2030 / MW 2027 = Golden Spread 8.0×, Rayburn 7.9×, STEC 6.8×, Brazos 6.5×, vs Oncor 4.3×, AEP 3.6×,
  TNMP 2.6×. Co-op G&Ts take **25.5% of the MW added 2027 → 2030** (39.2 of 154.0 GW).
- **Against who sells ERCOT's energy today** (EIA-861 2024, long form, balancing authority ERCO, parts A + D):
  co-ops 12.3% of retail MWh, munis/state 12.2%, retail marketers in IOU wires areas 72.5%, unknown ownership 3.0%.

| Group | ERCOT retail sales 2024 | RFI requests 2030 | Ratio |
|---|---:|---:|---:|
| IOU | 72.5% | 72.3% | 1.0× |
| **Co-op** | **12.3%** | **22.9%** | **1.9×** |
| Public power | 12.2% | 2.8% | 0.2× |
| Unknown | 3.0% | 2.1% | — |

  Co-op G&Ts carry about twice their share of today's energy; public power about a fifth of it (X10's "cities grow onto
  land their muni does not serve" fits, not verified as the cause). The EIA baseline leaves out short-form utilities
  (small) and was not checked against ERCOT's own energy total.
- **Q4 comparison.** Q4 put 46.6% of the 38 new data-center sites (weighted by county land) in co-op territory. The
  RFI puts 22.9% of the requested MW with co-op TSPs (25.1% with LCRA). They measure different things (air-permit
  sites × county land vs MW × transmission owner), and neither reaches Q4's 60% bar.

## 2. G&T → member co-ops (Q2)

From PUCT's `gt_cooperative` (the distribution co-op's wholesale supplier; `;` = two suppliers). Analysed accounts
only; `x13_gt_members.csv` lists them.

| RFI TSP | 2030 MW | Analysed accounts | Co-ops | Munis | Two suppliers | Members (analysed) |
|---|---:|---:|---:|---:|---:|---|
| Brazos | 26,576 | 15 | 15 | 0 | 2 | Bartlett, Comanche, Fort Belknap, HILCO, Hamilton County, Heart of Texas, J-A-C, Mid-South, Navarro County, Navasota Valley, PenTex, South Plains, Tri-County, United, Wise |
| Golden Spread | 16,580 | 9 | 9 | 0 | 1 | Big Country, Coleman County, Concho Valley, Greenbelt, Lighthouse, Lyntegar, South Plains, Southwest Texas, Taylor |
| LCRA | 4,422 | 47 | 7 | 40 | 2 | co-ops: Bluebonnet, Central Texas, Fayette, Hamilton County, Pedernales, Rio Grande, San Bernard; 40 munis (Boerne, Georgetown, Kerrville, New Braunfels, San Marcos, Seguin, … and North Texas munis such as Bridgeport, Sanger, Whitesboro) |
| Rayburn | 1,761 | 3 | 3 | 0 | 0 | Fannin County, Grayson-Collin, Trinity Valley |
| STEC | 817 | 9 | 9 | 0 | 1 | Jackson, Karnes, Magic Valley, Medina, Nueces, San Bernard, San Patricio, Victoria, Wharton County |
| CPS (its own TSP) | 1,070 | 1 | 0 | 1 | 0 | CPS Energy |

- 81 of the 107 analysed accounts have a supplier in the RFI. The other 26: 7 co-ops supplied by East Texas and/or
  Northeast Texas Electric Coop (not RFI TSPs), 1 co-op and 18 munis with no supplier in PUCT's field.
- **LCRA for munis is a supply link, not a transmission link.** PUCT lists LCRA as the supplier of munis far from
  LCRA TSC's area (Bridgeport, Sanger, Whitesboro, Seymour), so LCRA TSC's RFI filing says little about them.
- Golden Spread also supplies co-ops outside ERCOT (SPP). Whether its RFI MW include non-ERCOT loads is **not verified**.

### How the G&T's MW could reach members, and why the answer is "not at all"

Four weights over the analysed members of each G&T (2030 MW; `x13_member_allocation.csv`). A member with two suppliers
brings half its weight to each (assumption).

| G&T | Largest member: equal / area / EIA customers 2024 / new DC sites since 2025 | Spearman area~customers, area~DC, customers~DC | Median max/min (equal, area, customers) |
|---|---|---|---:|
| Brazos | (all 1,898 MW) / United 3,340 / Tri-County 7,033 / Bartlett 7,636 | 0.56, 0.31, 0.33 | 2.1× |
| Golden Spread | (all 1,951) / Southwest Texas 3,295 / South Plains 4,254 / Big Country 6,544 | 0.13, 0.17, −0.28 | 1.9× |
| LCRA | (all 96) / Rio Grande 2,724 / Pedernales 2,139 / Rio Grande 2,445 | 0.79, 0.36, 0.39 | 126× |
| Rayburn | (all 587) / Trinity Valley 1,016 / Grayson-Collin 858 / Trinity Valley 1,612 | 0.5, 0.5, −0.5 | 2.2× |
| STEC | (all 96) / Medina 325 / Magic Valley 353 / Medina 815 | 0.53, 0.73, 0.23 | 2.3× |

- The member with the most MW changes with the weight for every G&T, and one member's MW moves by up to 10× (Bartlett:
  764 by area, 770 by customers, 7,636 by DC sites). The DC-site weight gives nothing to 5 of 15 Brazos members and
  41 of 47 LCRA accounts.
- **Member size is the wrong weight:** large loads go where land and transmission are, not where meters are.
- **"Territory in the counties where the G&T's loads are"** needs load locations the RFI lacks. The nearest public
  evidence is ERCOT's own named counties (Batch Zero Update, Sep 2026, MW in 2032; analysed accounts covering ≥ 20%):

| County | ERCOT MW (deck) | Analysed co-op land (raw county_share) | Covering co-ops (supplier) |
|---|---:|---:|---|
| Jones | 7,000 base + studied | 98% | Big Country 0.77, Taylor 0.21 (Golden Spread) |
| Childress | 6,750 base + studied | 87% | South Plains 0.63 (Brazos + Golden Spread) |
| Haskell | 2,689 base | 99% | Big Country 0.74 (Golden Spread), Tri-County 0.26 (Brazos) |
| Ellis | 2,330 base | 72% | Navarro County 0.39, HILCO 0.29 (Brazos) |
| Cameron | 12,235 base + studied | 50% | Magic Valley 0.50 (STEC) |
| Dallas, Harris, Brazoria | 5,713 / 5,672 / 2,357 base | ≤ 5% | none ≥ 20% |

  Jones, Childress and Haskell (16.4 GW in the deck, mixing base + studied for the first two with base for Haskell) are
  almost all Golden Spread / Brazos member land, and Big Country
  leads Golden Spread under the DC-site weight too: two independent sources agree there. But Cameron shows the limit:
  it is half Magic Valley (STEC) land, yet STEC requested only 1.1 GW by 2032, so Cameron's 12 GW must sit mostly with
  another TSP (not verified which). **County land does not tell who carries the load.**
- **Recommendation: do not allocate.** Show the G&T's figure as the G&T's, and let county evidence (permits, ERCOT's
  named counties) speak for itself on the member page.

## 3. The per-account fact and the trigger (Q3)

**Fact** (`exposure_facts`, `x13_exposure_facts.csv`, one row per account and supplier):
"Your wholesale supplier Brazos Electric Power Coop reported 26.6 GW of large-load requests to be in service by 2030
(32.2 GW by 2032; 13.3% of the ERCOT-wide RFI), up from 0.1 GW in 2026. Requests, not forecasts; no location below the
TSP." Fields: `gt`, `tsp`, `via` (g&t / self), `n_gt`, `mw_2026`, `mw_near`, `mw_far`, `share_near`, `n_accounts`.

Coverage over the 107:

| | Co-ops (48) | Munis (59) | Total |
|---|---:|---:|---:|
| Supplier in the RFI (fact shown) | 40 | 41 | **81** |
| Supplier ≥ 1 GW for 2030 (X5's `tsp_large_load`) | 32 | 41 | **73** |
| Same, without LCRA-supplied munis (proposed) | 32 | 1 (CPS) | **33** |

- The fact takes only 9 distinct values; one value (LCRA alone) covers 45 accounts (42%), Brazos 13, Golden Spread 8,
  STEC 8, Rayburn 3, plus 4 single-account values (CPS, three two-supplier co-ops).
- Against X5's ranking (`x13_coverage.csv`): Brazos members rank high (9 of 15 tier A and call now; median rank 20 of
  107 for the 13 with Brazos alone), Rayburn's 3 higher still (median 4), Golden Spread (57.5), STEC (58) and LCRA (68) lower. Spearman of rank vs
  supplier MW is −0.17: the fact adds little ranking information the score does not already carry.
- **As a strong trigger** it would raise call-now from 25 to 33 and move 38 accounts from hold → watch or watch →
  nurture; 38 of the 46 changes come from accounts linked only through LCRA, the weakest link.

**Strength: context.** One filing (2026-04-15) shared by up to 45 accounts, no location, requests rather than
approvals, and 38 of the 46 action changes would hang on the LCRA link (PUCT records wholesale supply; the RFI row is
LCRA TSC's transmission). It belongs in the talking points ("your
G&T faces 26.6 GW of requests; its capacity and 4CP costs reach your rates"), not in the next action. For munis
supplied by LCRA, show the fact as "wholesale supplier" context without the trigger.

**Does it change Q4?** No. Q4's "signal only" stands: the co-op share of requested MW (22.9%) is lower than Q4's site
share (46.6%) and far from 60%. What X13 adds is a different, G&T-level story: co-op G&Ts carry about twice their share
of ERCOT's energy, and their share of the wave grows after 2027.

## Headline for the video (candidate, with caveats)

"Co-ops sell one in eight of ERCOT's retail megawatt-hours, but their G&Ts hold nearly one in four of the megawatts
requested for 2030: 46 GW, half of this summer's record peak. Brazos alone reported 26.6 GW, from 0.1 GW in 2026."

Caveats to say or show: requests, not forecasts (a third or less has historically reached approval); a TSP is not
the retail utility; LCRA counted as public power (as co-op: 25%); EIA baseline from the long form; Golden Spread's
filing may include non-ERCOT loads (not verified); 2.1% of requests sit with unnamed small TSPs. "Half of this
summer's record peak" = 45.7 / 91.1 GW. Checked only against the stored table (PPTX = PDF), not re-read from the raw
file as X6 did.

## Proposed decisions.md lines

- 2026-09-26 — The PUCT TSP RFI (58777 item 38) TSPs are typed IOU wires (Oncor, AEP, TNMP, CenterPoint), transmission-only IOU (WETT, Lone Star), public power (CPS Energy, LCRA), G&T co-op (Brazos, Golden Spread, Rayburn, STEC) and unknown (the aggregate of TSPs with < 3 sites); LCRA counts as public power although ERCOT seats it in its cooperative segment. — X13: 11 of 12 named TSPs agree with ERCOT's 2026 membership segment; counting LCRA as co-op moves the 2030 co-op share only from 22.9% to 25.1%.
- 2026-09-26 — A G&T's requested MW are not allocated to its member co-ops; each member shows the G&T's figure as a context fact. — X13: the RFI has no location below the TSP, and area, EIA-customer and data-center weights disagree on the largest member for every G&T (Brazos: three different members; one member moves up to 10×); Cameron is half STEC-member land yet STEC requested 1.1 GW against Cameron's 12.2 GW.
- 2026-09-26 — `tsp_large_load` stays a context trigger and no longer fires for munis linked only through LCRA's wholesale supply (73 → 33 accounts); they keep the fact, labelled "wholesale supplier". — X13: PUCT's `gt_cooperative` is the power supplier, not the TSP (it lists LCRA for Sanger and Whitesboro); as a strong trigger it would add 8 call-nows and 38 of its 46 action changes would come from the LCRA link.
- 2026-09-26 — Q4 stays "signal only"; the co-op line for the pitch, if used, is the G&T-level ratio (22.9% of 2030 requests vs 12.3% of 2024 ERCOT retail sales), not a county claim. — X13: co-op TSPs hold less of the requested MW (22.9%) than Q4's site share (46.6%), but about twice their share of today's energy.

## Review items for Pablo

1. **LCRA** as public power (default) or co-op (ERCOT's segment)? 22.9% vs 25.1% for 2030.
2. **LCRA-supplied munis:** drop them from the `tsp_large_load` trigger (default proposed, 73 → 33) or keep X5's rule?
   This is a change to X5's trigger in `models/triggers.py` (not made here).
3. **The video line.** Use "one in eight vs one in four"? It rests on requests, on TSP ≠ retail utility, and on an EIA
   baseline that was not checked against ERCOT's energy total.
4. **Golden Spread's scope.** It supplies SPP co-ops too; whether its 17.4 GW include non-ERCOT loads is not verified.
   If some do, the co-op share is overstated (all of Golden Spread out of both sides: 15.9% for 2030).
5. **The aggregate of TSPs with < 3 sites** (4.1 GW in 2030): which TSPs (munis such as Garland or Denton, a
   distribution co-op?) is not in the filing's table. It is the only place distribution co-ops could appear.
6. **A G&T channel.** Brazos alone links 15 analysed members (9 tier A). Whether Base wants to approach a G&T for its
   members, rather than co-op by co-op, is a business call; the data supports a G&T rollup either way.

## Useful for the core features

- **/accounts/[id]:** a "Your wholesale supplier" card with the fact sentence, the G&T's 2026 → 2030 → 2032 path as a
  small bar series, its share of the RFI, the number of fellow members, and the labels "requests, not forecasts" and
  "no location below the TSP". Next to it, the county evidence (data-center permits, ERCOT's named counties with MW)
  that does have a location.
- **/accounts:** a "supplier" column and a G&T filter / grouping (Brazos 15, Golden Spread 9, STEC 9, Rayburn 3, LCRA 47
  analysed accounts), with call-now counts per G&T.
- **Explorer or Forecast:** a stacked bar "who carries the requests" by entity type and year (the §1 table), next to
  the 2024 retail-sales split. Not a map layer: the RFI has no geography below the TSP.
- **Talking point / offer angle:** "Your G&T faces N GW of large-load requests; its capacity and transmission (4CP)
  costs flow into your wholesale rate. Distributed storage on your members' homes is capacity you own."
- **API:** mart `tsp_large_load_requests` (grain TSP × year: `tsp`, `entity_type`, `group`, `year`, `mw`, `share`,
  `filed_date`, `source_ref` = `58777:38`, `verified=false`) and `account_supplier` (grain account × supplier:
  `account_id`, `gt`, `tsp`, `via`, `n_gt`), from which the fact is a join. Both are tiny.
