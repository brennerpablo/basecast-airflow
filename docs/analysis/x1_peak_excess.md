# X1 — Is the recent summer-peak excess over weather + trend the large loads showing up?

Exploration after phase 0. Run on 2026-09-26 against production Postgres (`basecast_reader`): `ercot_load_hourly_wz`
through operating day 2026-09-25, `weather_hourly_wz` through 2026-09-20 23:00 UTC, `large_load_chart_values`
(Gemini-read, `verified=false`), `county_weather_zone` and `tceq_data_center_sites`.

- Script: `analysis/x1_peak_excess.py` (`uv run --group analysis python analysis/x1_peak_excess.py`, ~12 s)
- Model code: `basecast_pipelines/models/peak_excess.py` (pure functions on top of Q7's `weather_load.py` and Q5's
  `large_load.py`, both unchanged); tests: `tests/models/test_peak_excess.py` (synthetic, no DB)
- Figures and tables (gitignored): `analysis/out/x1_excess_by_zone.png`, `x1_ercot_excess_vs_decks.png`,
  `x1_min_max_by_month.png`, `x1_excess_peak.csv`, `x1_excess_coincident.csv`, `x1_shape_excess.csv`,
  `x1_deck_vs_excess.csv`, `x1_energized_by_month.csv`

Definitions:

- **Pre-break model** = Q7's `peak ~ 1 + (year − 2014) + t_mean_3d`, fit per zone on the summers **2003–2019**, then
  fed each later summer's actual weather. ERCOT weather weights are the 2003–2019 summer energy shares.
- **Excess** = actual − pre-break prediction.
- **Non-coincident excess**: each zone's own summer peak.
- **Coincident excess**: each zone's MW in the hour of the ERCOT system peak. Every zone uses the same regressors
  (year + ERCOT weather), so the zone excesses add up exactly to the ERCOT excess.
- **Shape stats**: June 1 – September 20 of every year (2026's weather ends on Sep 20). They are the mean daily
  minimum (`dmin`), the mean daily maximum (`dmax`), the load factor (mean / peak) and the average day's min/max
  ratio. `dmin` and `dmax` each get their own pre-break model, fit on 2003–2019 against the summer mean temperature.
- **Flatness** = dmin excess / dmax excess. Organic growth scales the whole daily curve, so the ratio sits near the
  baseline min/max ratio (~0.61 for ERCOT). A flat 24-hour block lifts both lines by the same MW, so the ratio
  sits near 1.

## 1. Which zones carry the excess?

### Non-coincident peak excess over the pre-break model (MW; % in the last row)

| Year | COAST | EAST | FWEST | NCENT | NORTH | SCENT | SOUTH | WEST | **ERCOT** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2020 | −147 | 402 | 826 | −556 | 218 | 107 | −333 | −43 | 391 |
| 2021 | 125 | 325 | 1,403 | −490 | 187 | −380 | −121 | −70 | 912 |
| 2022 | 128 | 433 | 1,605 | −396 | 924 | 873 | −50 | −125 | 3,208 |
| 2023 | 1,634 | 671 | 2,626 | 235 | 1,055 | 1,695 | −151 | −15 | 6,933 |
| 2024 | 1,096 | 485 | 3,422 | 975 | 1,361 | 2,070 | 103 | −14 | 8,105 |
| 2025 | 939 | 465 | 4,515 | 839 | 1,752 | 1,260 | 176 | 20 | 7,054 |
| 2026 (partial) | 1,508 | 722 | 4,324 | 1,586 | 1,896 | 2,162 | 422 | 698 | **11,173** |
| 2026 % | +6.6 | +27.8 | +98.2 | +5.6 | +216.4 | +15.7 | +6.0 | +31.5 | **+14.0** |
| Training residual SD | 413 | 52 | 369 | 403 | 208 | 239 | 94 | 62 | 852 |

- The pre-break model puts ERCOT at 78.5 / 77.1 / 76.6 / 80.0 GW for 2023–2026. Actual was 85.5 / 85.2 / 83.7 /
  91.1 GW, an **excess of 6.9 / 8.1 / 7.1 / 11.2 GW** (8.8% / 10.5% / 9.2% / 14.0%). 2020–2021 sit inside ~1 SD, and
  the break shows from 2022 (+3.2 GW, 3.8 SD).
- 2026 is larger than Q7's +7.0 GW because the pre-break trend is 856 MW/yr, against 1,107 MW/yr for Q7's
  2003–2025 fit.
- **Sensitivity, fit on 2010–2019:** ERCOT's excess is 5.75 GW (2025) and 8.8 GW (2026). NORTH's 2026 excess is
  1,316 MW (+90%), not 1,896 MW (+216%). NORTH's 2003–2019 trend is negative (−54 MW/yr) only because of the
  2009 drop (§3).

### Coincident (at the ERCOT peak hour, additive) — MW

| Year (peak hour, CDT) | COAST | EAST | FWEST | NCENT | NORTH | SCENT | SOUTH | WEST | **ERCOT** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2022 (Jul 20, HE 17) | 185 | 384 | 1,386 | −113 | 872 | 881 | −264 | −122 | 3,208 |
| 2023 (Aug 10, HE 18) | 1,703 | 499 | 2,102 | −231 | 1,047 | 1,964 | −82 | −70 | 6,933 |
| 2024 (Aug 20, HE 18) | 1,292 | 446 | 2,349 | 814 | 1,099 | 2,293 | −162 | −26 | 8,105 |
| 2025 (Aug 18, HE 18) | 653 | 454 | 2,681 | 864 | 890 | 1,874 | −100 | −263 | 7,054 |
| 2026 (Jul 22, HE 18) | 2,068 | 630 | 3,068 | 1,400 | 1,313 | 2,363 | 102 | 230 | 11,173 |
| **Share, mean 2023–26** | 17.2% | 6.1% | **30.7%** | 8.6% | 13.1% | **25.5%** | −0.7% | −0.4% | 100% |

**What it changes for the product:** the excess is concentrated. FWEST, SCENT, COAST and NORTH carry 87% of it at
the system peak hour, and SOUTH and WEST (before 2026) carry none. The Explorer map and the account ranking can use
these shares as the "where is non-organic load landing" layer at zone level. NORTH is small in MW (~1.1 GW
coincident) but the largest in % (+90% to +216%).

## 2. Is it large load? Decks and the shape test

### Shape: the excess is flat (weather-light test)

ERCOT, excess over each pre-break model (MW):

| Year | Peak | Average-day max (dmax) | Average-day min (dmin) | Flatness (dmin/dmax excess) |
|---|---:|---:|---:|---:|
| 2021 | 912 | 1,982 | 2,060 | 1.04 |
| 2022 | 3,208 | 4,165 | 4,355 | 1.05 |
| 2023 | 6,933 | 7,088 | 7,383 | 1.04 |
| 2024 | 8,105 | 7,008 | 7,781 | 1.11 |
| 2025 | 7,054 | 8,922 | 9,817 | 1.10 |
| 2026 | 11,173 | 12,277 | 13,332 | 1.09 |

The dmin model's training residual SD is 794 MW. The baseline min/max ratio is 0.61.

- **The overnight minimum rose as much as the daily maximum, or slightly more.** Organic, weather-driven growth
  would give a flatness near 0.61. The observed 1.04–1.11 is the fingerprint of 24-hour load.
- ERCOT's summer load factor went from 0.703 (2019) to **0.765** (2025 and 2026). The average day's min/max ratio
  went from 0.614 to 0.681.
- **Caveat:** the in-sample years are also ~1 (their residuals are noise that moves both lines), so the ratio
  only means something when the excess is many SDs (2022+).
- **Second caveat:** a flatness above 1 also fits behind-the-meter solar shaving the daily maximum, or flexible
  loads (crypto) curtailing in peak hours. Neither is verified here, and both would make the flat share look
  larger than it is.

By zone (dmax excess / dmin excess, MW):

| Zone | 2025 | 2026 | Reads as |
|---|---|---|---|
| FWEST | 4,195 / 3,964 | 4,165 / 4,063 | flat (min/max ratio 0.84 → 0.90) |
| SCENT | 1,471 / 1,483 | 2,253 / 2,115 | flat |
| NCENT | 515 / 683 | 1,848 / 1,866 | flat |
| COAST | 1,412 / 1,686 | 1,568 / 2,131 | flat, min-heavy |
| WEST | 15 / 33 | 549 / 539 | flat (new in 2026) |
| SOUTH | 13 / 388 | 256 / 787 | min-heavy (night load rising, peak not) |
| NORTH | 1,467 / 994 | 1,490 / 902 | **peakier than flat** |
| EAST | 449 / 261 | 583 / 393 | peakier than flat |

### Versus the ERCOT large-load decks (Gemini-read, spot-check pending: "not verified")

"Observed simultaneous peak" is the approved large loads' combined monthly peak, from the "Loads Approved to
Energize – Observations" charts. A2E is the approved-to-energize stock. Both are taken in the month of the ERCOT
summer peak.

| Summer | ERCOT peak excess | dmin excess | Deck: observed simultaneous | Non-simultaneous | A2E stock | Simultaneous ÷ peak excess |
|---|---:|---:|---:|---:|---:|---:|
| 2023 (Aug) | 6.93 GW | 7.38 | 1.97 | 2.35 | 3.74 | 28% |
| 2024 (Aug) | 8.10 | 7.78 | 2.82 | 3.17 | 5.48 | 35% |
| 2025 (Aug) | 7.05 | 9.82 | 3.73 | 4.06 | 7.50 | 53% |
| 2026 (Jul) | 11.17 | 13.33 | 3.70 (latest month in the decks: Mar 2026) | 4.01 | 8.93 (Jun 2026) | 33% (stale) |

- **A flat block does fit the excess's shape. The large loads ERCOT observes explain only a third to a half of
  it.** The rest is 3.3–5.3 GW in 2023–2025 and ~7.5 GW in 2026 against a stale deck reading.
- Even if the whole approved stock ran at the peak hour, it would cover 54% / 68% / 106% / 80% of the peak excess.
- **Observed at peak ÷ approved stock is stable: 0.525 / 0.514 / 0.498 (mean 0.51)** in Aug 2023/2024/2025. The
  non-simultaneous ratio is 0.63 / 0.58 / 0.54. Q5 cited ~0.65 from the in-service chart's "observed energized"
  segment, which is a different series. For the peak forecast the simultaneous 0.51 is the right factor. Whether
  "simultaneous" means coincident with the ERCOT peak or only among the loads themselves is not verified. If it is
  the latter, 0.51 is an upper bound at the system peak hour.
- **By region:** the decks split the approved stock into LZ_WEST and "Other". LZ_WEST ≈ FWEST + WEST + NORTH; the
  mapping of load zone to weather zones is not verified.
  - 2025: FWEST + WEST + NORTH carry a dmin excess of 5.0 GW and a coincident excess of 3.3 GW. The deck's LZ_WEST
    observed (non-simultaneous) is 2.1–2.6 GW (Aug–Nov 2025 decks) and its approved stock 4.06 GW.
  - 2026: those three zones carry a dmin excess of 5.5 GW and a coincident excess of 4.6 GW, against a deck
    LZ_WEST of 1.9–2.4 GW observed and 5.05–5.19 GW approved.
  - Rest of ERCOT in 2025: dmin excess ≈ 9.8 − 5.0 = 4.8 GW (approximate; non-coincident mins do not add exactly),
    against a deck "Other" of 1.5–1.7 GW observed and 3.44 GW approved.
  - Both regions come to roughly half. The unexplained flat load is not only in the Permian.
- **Data issue found:** the May 2026 deck's monthly axes were read as 2023-07 … 2024-05 but carry the 2025-07 …
  2026-05 values. Q5's `a2e_by_month` keeps them, e.g. A2E Dec 2023 = 8,786 MW against 4,479 MW in the
  contemporaneous decks. X1 drops any monthly value dated more than 16 months before its deck
  (`peak_excess.drop_misdated_months`). Q5's Dec 2024 and Dec 2025 realized values sit outside that range. The
  2023–mid-2024 part of Q5's monthly series is wrong (see the review items).

**What it changes for the product:** the "large-load layer" cannot be the whole excess. The decks' observed large
loads are about half of the flat growth. A forecast of organic trend plus the decks would under-shoot by 3–5 GW,
and one of Q7's full-sample trend plus the decks would double-count (§4).

## 3. NORTH, WEST and FWEST

### NORTH 2009 drop

- **When:** monthly YoY of NORTH's mean daily minimum and maximum turns negative in **Oct 2008** (−155 / −293 MW)
  and deepens from **Jan 2009** (−294 / −441). It holds through summer 2009 (−270 to −300 on the min, −565 to −663
  on the max) and returns to ~0 by Jan 2010. That makes it a one-time level shift between Oct 2008 and Jan 2009.
- **Shape:** it is **proportional, not flat**. The max fell about twice as much as the min, so a group of
  customers left NORTH, not one industrial load.
- **Receiving zone:** a scan of zone shares did not isolate one. It is confounded by a COAST disruption in
  Sep 2008 (Hurricane Ike, not verified from the DB).
- **Cause (zone boundary/meter reassignment, or a utility leaving):** not verified.
- **Consequence:** fit NORTH's organic trend on 2010+ only.

### NORTH 2022+ jump

- **2022 step:** YoY turns positive in Dec 2021 (+95 on the min), then +234 / +382 (min / max) in Jan 2022, and
  stays +160 to +360 on the min through 2022.
- **2023:** flat Jan–Mar 2023, then a second rise from mid-2023. The largest 28-day step in the daily minimum is
  +220 MW on 2023-06-19. YoY is +200 to +320 through 2024 and +76 to +317 in 2025.
- **Summer 2026 is below 2025:** YoY on the min is −117 (Jun), −229 (Jul) and −166 (Aug); the Jul 1–Sep 20 hourly
  profile averages −56 MW.
- **Shape:** peakier than flat (2026 dmax excess 1,490 vs. dmin 902). That does not match a 24-hour data center.
  It could match flexible or seasonal load (not verified).
- **DB source:** `tceq_data_center_sites` has **"GALAXY HELIOS I", Dickens County (NORTH), first affiliation
  2022-07-19**. That is consistent in time with the 2022 ramp. Its type and MW are not verified.
- **Later NORTH sites in the same table:** Thelma (Haskell, 2025-08), Journey (Haskell, 2026-02), Platon
  (Wilbarger, 2026-05) and Clocktower (Hardeman, 2026-07).

### WEST 2026 (+698 MW non-coincident, +31.5%)

- **When:** YoY of the mean daily minimum turns positive in **Oct 2025** (+95), then +145 (Nov–Dec 2025), +96 to
  +119 (Jan–Feb 2026), +238 to +267 (Mar–May), **+400 (Jun), +513 (Jul), +671 (Aug) and +732 (Sep, through the
  25th)**. It is still ramping.
- The largest 28-day step in the daily minimum is **+295 MW around 2026-06-07**.
- **Shape:** Jul 1–Sep 20 2026 minus 2025 is **+619 to +730 MW in every hour of the day** (mean +675), and the
  2026 excess is 549 MW on the dmax against 539 MW on the dmin. That is a flat block, the large-load signature.
- **Candidates:** `tceq_data_center_sites` lists WEST sites Longhorn Data Center (Taylor, 2024-08-15, active and
  pending), Barber Lake Data Center Project (Mitchell, 2025-08-27), Woodrow Data Center & Power Plant (Nolan,
  2026-05-18), and two pending sites (Tom Green, Fisher; 2026). Which one draws the MW is not verified.

### FWEST stalled in 2026 (a finding, not asked)

- YoY of the daily minimum was +0.5 to +1.2 GW a year through 2022–2025. It fell to +64 MW (Jan 2026), +240 to
  +640 MW (Feb–Jun) and **+14 MW (Sep 2026)**.
- The Jul–Sep profile 2026 minus 2025 averages +3 MW. FWEST's 2026 excess (4.3 GW) equals 2025's (4.5 GW).
- Cause not verified.

**What it changes for the product:**

- A rising daily **minimum**, year over year, is a working "large load arrived" trigger at zone level. WEST
  showed it 8 months before its summer peak moved.
- TCEQ data-center permits can serve the Commercial module as a leading indicator. The Taylor County permit
  (2024-08) came ~22 months before WEST's step, if that site is the source (not verified).
- NORTH is the counter-example (peaky, falling in 2026): not every excess is a data center.

## 4. Implication for the Forecast: organic vs. large load without double counting

- **Don't stack the large-load layer on Q7's full-sample model.** Its 2003–2025 trend (1,107 MW/yr) sits
  **4.2 GW** above the pre-break trend (856 MW/yr) in 2026, **4.7 GW** in 2028 and **5.2 GW** in 2030, all at
  2026 weather. That much large load is already inside the trend and would be counted twice, more each year.
- **Proposed split:** `peak(Y) = organic(Y) + LL(Y) + U`.
  - **organic**: the pre-break model per zone (fit ≤ 2019; NORTH 2010–2019) at P50 weather. It holds the
    residual-SD band.
  - **LL** (large-load layer): **0.51 × the approved-to-energize stock** at the peak month (2023–2025:
    0.50–0.53). New approvals come from Q5's realization ratio, and the zone split from the decks' LZ_WEST / Other
    breakdown.
  - **U** (unattributed flat load): the excess the decks don't see. It was 5.0 / 5.3 / 3.3 GW in 2023 / 2024 /
    2025, **mean 4.5 GW**. Held **flat** (not trended, not weather-scaled), shown as its own layer with the
    2023–2025 range as its band. Label: "flat load not in ERCOT's large-load observations (smaller loads, oil & gas
    electrification, industrial) — composition not verified".
- **2026 check** (pseudo out-of-sample: factor and U from 2023–2025, A2E from the Jun 2026 deck):
  - The proposed split gives **80.0 + 4.6 + 4.5 = 89.1 GW** against **91.1 GW** actual (**−2.3%**).
  - Q7's full-sample model alone gives 84.2 GW (−7.6%).
  - Q7's full-sample model + LL gives 88.8 GW (−2.6%). It is close today, but its double count grows 0.25 GW/yr.
- **Zone allocation of LL + U:** the coincident shares of §1 (FWEST 31%, SCENT 26%, COAST 17%, NORTH 13%,
  NCENT 9%, EAST 6%) until the decks give more than LZ_WEST / Other.

## Headline for the video (candidate)

"Weather and the pre-2020 trend put Texas's 2026 summer peak at about 80 GW; it hit 91. The extra 11 GW runs day
and night — the overnight minimum rose as much as the peak — and ERCOT's own large-load reports account for only a
third to a half of it."

Caveats to say or show:

- The "80 GW" is our model fit on 2003–2019, not an ERCOT figure. Q7's full-sample fit says 84 GW.
- The deck values are machine-read (spot-check pending), and the latest observed month is Mar 2026.
- Behind-the-meter solar may exaggerate the flatness.
- What the unattributed ~4.5 GW is made of is not verified.

## Proposed decisions.md lines

- 2026-09-26 — The Forecast's organic layer is the pre-break model (peak ~ year + weather, fit on summers ≤ 2019; NORTH on 2010–2019), not the 2003–2025 fit. — X1: the full-sample trend (1,107 vs. 856 MW/yr) already absorbs 4.2 GW of the 2022–2025 step in 2026 (5.2 GW by 2030), which the large-load layer would count again.
- 2026-09-26 — Large-load layer at the summer peak = 0.51 × the approved-to-energize stock, plus a separate flat "unattributed" layer U = 4.5 GW held constant (band 3.3–5.3 GW). — X1: observed simultaneous peak ÷ A2E in the August peak month was 0.53 / 0.51 / 0.50 in 2023–2025; the flat excess over the pre-break model exceeded the observed large loads by 3.3–5.3 GW; this split puts 2026 at 89.1 GW vs. 91.1 GW actual (−2.3%).
- 2026-09-26 — Monthly large-load deck series drop values dated more than 16 months before their deck. — The May 2026 deck's monthly axes were machine-read as 2023-07…2024-05 but carry 2025–2026 values (A2E Dec 2023 read as 8,786 MW vs. 4,479 MW in the contemporaneous decks).
- 2026-09-26 — Zone-level "large load arrived" trigger = sustained positive YoY in the zone's mean daily minimum load. — X1: the excess since 2022 is flat (dmin excess ≈ dmax excess, ratio 1.04–1.11 vs. 0.61 for organic growth); WEST's minimum rose from Oct 2025, eight months before its summer peak.

## Review items for Pablo

1. **Q5 monthly A2E series is contaminated for 2023-07 … 2024-05** by the mis-dated May 2026 deck. Q5's realized
   Dec 2024 (6,297 MW) and Dec 2025 (8,786 MW) are outside the range, so its ratios should stand. Any chart of the
   monthly A2E line should use `peak_excess.a2e_by_month_checked`. Confirm before the Forecast uses the series.
2. **Accept U as its own layer?** The alternative is to fold it into "large load" by using the whole A2E stock
   (factor ~1.0 instead of 0.51), which overstates what ERCOT observes. U's composition is not verified.
3. **"Simultaneous" definition.** Does ERCOT's "observed simultaneous peak" mean at the system peak hour, or only
   across the loads? It decides whether 0.51 is the contribution at the peak or an upper bound. Not verified.
4. **Flatness vs. behind-the-meter solar.** Flatness above 1 could partly be rooftop solar shaving the daily max.
   Worth one check against ERCOT's distributed-solar estimate before the video claims "runs day and night".
5. **NORTH 2009** (a proportional level shift, Oct 2008–Jan 2009) and **NORTH 2022** (the "GALAXY HELIOS I" TCEQ
   record, 2022-07): causes not verified. NORTH's 2026 summer is below 2025.
6. **WEST 2026:** a flat ~0.7 GW block, still ramping in Sep 2026. Don't name a site or company in the video; the
   TCEQ records only show permits.
7. **FWEST stall in 2026** (YoY min +14 MW in Sep) deserves a sentence in the Forecast: the Permian's
   contribution to the excess stopped growing this year. Cause not verified.

## Useful for the core features

- **Forecast screen:** three stacked layers per zone and year: organic (pre-break, P50 ± residual band), large
  load (0.51 × A2E, Q5 realization for new approvals) and unattributed flat (4.5 GW ERCOT, allocated by coincident
  shares). 2026 check: 89.1 vs. 91.1 GW. This is the non-double-counting answer to Q7's review item 3.
- **Explorer map:** zone shares of the coincident excess (FWEST 31%, SCENT 26%, COAST 17%, NORTH 13%) and the
  change in the average day's min/max ratio (WEST 0.63 → 0.72, FWEST 0.84 → 0.90, SOUTH 0.61 → 0.70, 2019 → 2026)
  as a "flat load arrived" metric.
- **Commercial intelligence triggers:** YoY in the zone's mean daily minimum, e.g. WEST +95 MW (Oct 2025) → +732
  MW (Sep 2026). Pair it with new TCEQ data-center permits in the zone's counties as the leading signal. Both
  are rule-based and explainable.
- **Backtest screen:** the pre-break model plus the decks' observed large loads under-shoots 2023–2025 by 3.3–5.3
  GW. That is an honest picture of how much of Texas's new load is invisible to the official large-load tracking.
- **Video:** the figure `analysis/out/x1_ercot_excess_vs_decks.png` (bars: peak excess and night-minimum excess;
  line: the decks' observed large loads). The gap between the line and the bars is the story.
