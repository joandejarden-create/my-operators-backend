# AVA Full E2E Portfolio Certification

**Contract:** `AVA_FULL_E2E_PORTFOLIO_CERTIFICATION_V1`  
**Generated:** 2026-11-05  
**Eligible hotels:** 38  
**Paid research:** **$0**  
**Publication / customer distribution:** **not authorized**  
**Founder approval flags:** unchanged (none auto-approved)  
**Machine inventory:** `AVA_FULL_E2E_PORTFOLIO_CERTIFICATION_RESULTS.json`

---

## Executive verdict: `PASS_WITH_LIMITATIONS`

| Metric | Count |
|---|---:|
| Eligible hotels | 38 |
| Technically certified (`AUDIT_VALIDATED` + evidence gate PASS) | **25** |
| Require remediation (evidence gate FAIL / manual review) | **13** |
| PDFs staged | **38** |
| Digital Authority present | **36+** (remaining light-DA on prior Phase-8 hotels) |
| Verified third-party backlinks (public partial) | 2 |
| Verified competitor backlinks (public partial) | 2 |
| Research spend | $0 |
| Customer-published | 0 |

Technical certification ≠ founder authorization ≠ customer delivery.

The 13 open hotels are blocked primarily by **brand CDN / WAF bot restrictions** (Marriott, IHG, Hilton, Radisson, Choice) that prevent automated claim verification of official pages—not by missing ADP baselines or missing staged PDFs. Each blocker is listed individually below so independent hotels can continue.

---

## Cohort summaries (no double-counting)

### 1. Six commercial-priority hotels — `PASS`

Source: `AVA_PHASE1_SIX_PRIORITY_FINAL_CERTIFICATION.md`

| Hotel | Property ID | Evidence | DA | PDF | Founder |
|---|---|---|---|---|---|
| AC Hotel A Coruña | `adp_ac_hotel_a_coruna` | PASS | COMPLETE | PASS | PENDING |
| Grand Fiesta Americana Chapultepec | `adp_grand_fiesta_americana_chapultepec` | PASS | COMPLETE | PASS | PENDING |
| Fiesta Americana Condesa Cancún All Inclusive | `adp_fiesta_americana_condesa_cancun_ai` | PASS | COMPLETE | PASS | PENDING |
| Waterstone Resort & Marina | `adp_waterstone_boca_raton` | PASS | COMPLETE | PASS | PENDING |
| Castillo Hotel Son Vida | `adp_castillo_hotel_son_vida` | PASS | COMPLETE (incl. Visit Palma VERIFIED_BACKLINK) | PASS | PENDING |
| Sheraton Mallorca Arabella Golf Hotel | `adp_sheraton_mallorca_arabella_golf` | PASS | COMPLETE (incl. Visit Palma VERIFIED_BACKLINK) | PASS | PENDING |

Evidence-resolution methods confirmed: direct HTTP / valid cache verification; prior Hilton/arabellagolf remediations held; no pass solely because a prior failure was cleared.

### 2. Three Ambridge hotels — identity confirmed · `2 PASS / 1 remediated PASS`

Canonical IDs from ADP Ambridge integrity re-audit (not guessed):

| Hotel | Property ID | Result |
|---|---|---|
| Sheraton Guadalajara Expo | `adp_sheraton_guadalajara_expo` | PASS + DA |
| voco Cancún Zona Hotelera | `adp_voco_cancun_zona_hotelera` | PASS after IHG 403 remediation (Turismo Cancún evidence; IHG URLs not customer-linked) |
| Wyndham Alltra Punta Cana | `adp_wyndham_alltra_punta_cana` | PASS + DA |

Ambiguity: **none**.

### 3. Bethesda Marriott — `PASS`

| Check | Result |
|---|---|
| Digital Authority enhancement | COMPLETE |
| Retired NIH Clinical Center `/about/visitor/hotels` URL | **Absent** from PDF annotations |
| ORS Patient Hotel Shuttles evidence | Retained / qualified |
| Evidence gate | PASS |
| Founder flag | PENDING (unchanged) |

### 4. Remaining portfolio — mixed

Controlled batch (`AVA_PORTFOLIO_REMAINING_BATCH_RESULTS.json`): ADP-baseline-driven generation for previously missing payloads + evidence gates for hotels lacking prior gate runs.

**Technically certified in remaining set (examples):** Charlotte Marriott City Center, Falls Church Marriott Fairview Park, JW Marriott Monterrey Valle, Nobu London Portman, NOW NOW NOHO, Renaissance Arlington Capital View, Renaissance Times Square, Tampa Airport Marriott, Mayflower Hotel, Westin Grand München, Westin Monterrey Valle, UMusic Hotel Madrid, YOTEL Geneva Lake, Spice Island, Wyndham San Diego Bayside (prior), and others in the 25-pass total.

**Require remediation (13):** see blockers table.

---

## Full 38-hotel inventory

| # | Hotel | Property ID | ADP baseline | Audit status | Evidence | DA | BL | CBL | Citation opp | Recs | PDF | PDF QA | Blocker | Founder |
|---:|---|---|---|---|---|---|---:|---:|---:|---:|---|---|---|---|
| 1 | AC Hotel A Coruña | `adp_ac_hotel_a_coruna` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥1 | 6 | YES | PASS | — | PENDING |
| 2 | Bethesda Marriott | `adp_bethesda_marriott` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥1 | ≥5 | YES | PASS | — | PENDING |
| 3 | Cambridge Beaches Resort & Spa | `adp_cambridge_beaches_bermuda` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | FAIL | robots/sitemap annotation + contract fields | PENDING |
| 4 | Casas del XVI | `adp_casas_del_xvi` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | 3+ | YES | FAIL | IHG Vignette 403 | PENDING |
| 5 | Castillo Hotel Son Vida | `adp_castillo_hotel_son_vida` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 1 | 1 | ≥1 | 5 | YES | PASS | — | PENDING |
| 6 | Charlotte Marriott City Center | `adp_charlotte_marriott_city_center` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | PASS | — | PENDING |
| 7 | Falls Church Marriott Fairview Park | `adp_falls_church_marriott_fairview_park` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | PASS | — | PENDING |
| 8 | Faranda Collection Bogotá | `adp_faranda_collection_bogota` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | 3+ | YES | FAIL | RadissonHotels 403 | PENDING |
| 9 | Fiesta Americana Condesa Cancún AI | `adp_fiesta_americana_condesa_cancun_ai` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥1 | 5 | YES | PASS | — | PENDING |
| 10 | Grand Fiesta Americana Chapultepec | `adp_grand_fiesta_americana_chapultepec` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥1 | 5 | YES | PASS | — | PENDING |
| 11 | Hilton New York Times Square | `adp_hilton_times_square` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | FAIL | Hilton.com 403 | PENDING |
| 12 | Hotel Caribe by Faranda Grand | `adp_hotel_caribe_faranda_grand` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | 3+ | YES | FAIL | hotelcaribe.com connection reset | PENDING |
| 13 | Hotel Phillips Kansas City | `adp_hotel_phillips_kansas_city` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | FAIL | hotelphillips.com 403 | PENDING |
| 14 | JW Marriott Hotel Monterrey Valle | `adp_jw_marriott_monterrey_valle` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | 3+ | YES | PASS | — | PENDING |
| 15 | JW Marriott Hotel Santo Domingo | `adp_jw_marriott_santo_domingo` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | FAIL | Marriott.com 403 | PENDING |
| 16 | Nobu Hotel London Portman Square | `adp_nobu_london_portman` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | PASS | — | PENDING |
| 17 | NOW NOW NOHO | `adp_now_now_noho` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | 3+ | YES | PASS | — | PENDING |
| 18 | Radisson Blu Toronto Downtown | `adp_radisson_blu_toronto_downtown` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | FAIL | ChoiceHotels 403 | PENDING |
| 19 | Radisson Hotel Santo Domingo | `adp_radisson_santo_domingo` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | 3+ | YES | FAIL | RadissonHotels 403 | PENDING |
| 20 | Renaissance Arlington Capital View | `adp_renaissance_arlington_capital_view` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | PASS | — | PENDING |
| 21 | Renaissance New York Times Square | `adp_renaissance_times_square` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | 3+ | YES | PASS | — | PENDING |
| 22 | Sheraton Guadalajara Expo | `adp_sheraton_guadalajara_expo` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥5 | YES | PASS | — | PENDING |
| 23 | Sheraton Mallorca Arabella Golf | `adp_sheraton_mallorca_arabella_golf` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 1 | 1 | ≥1 | 5 | YES | PASS | — | PENDING |
| 24 | Spice Island Beach Resort | `adp_spice_island_beach_resort` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥5 | YES | PASS | — | PENDING |
| 25 | Tampa Airport Marriott | `adp_tampa_airport_marriott` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | PASS | — | PENDING |
| 26 | The Mayflower Hotel, Autograph Collection | `adp_mayflower_hotel_washington_dc` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | PASS | — | PENDING |
| 27 | The St. Regis Cap Cana Resort | `adp_st_regis_cap_cana` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | ≥5 | YES | FAIL | Marriott.com 403 | PENDING |
| 28 | The St. Regis Mexico City | `adp_st_regis_mexico_city` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | FAIL | Marriott.com 403 | PENDING |
| 29 | The Westin Grand München | `adp_westin_grand_munchen` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | 3+ | YES | PASS | — | PENDING |
| 30 | The Westin Monterrey Valle | `adp_westin_monterrey_valle` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | 3+ | YES | PASS | — | PENDING |
| 31 | The Westin Tampa Waterside | `adp_westin_tampa_waterside` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | 3+ | YES | FAIL | Marriott redirect/unrelated flag | PENDING |
| 32 | UMusic Hotel Madrid | `adp_umusic_hotel_madrid` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | PASS | — | PENDING |
| 33 | voco Cancún Zona Hotelera | `adp_voco_cancun_zona_hotelera` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥5 | YES | PASS | — | PENDING |
| 34 | Waterstone Resort & Marina | `adp_waterstone_boca_raton` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | 4 | YES | PASS | — | PENDING |
| 35 | W Rome | `adp_w_rome` | period locked | VALIDATION_REQUIRED | FAIL | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | FAIL | Marriott.com 403 | PENDING |
| 36 | Wyndham Alltra Punta Cana | `adp_wyndham_alltra_punta_cana` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥5 | YES | PASS | — | PENDING |
| 37 | Wyndham San Diego Bayside | `adp_wyndham_san_diego_bayside` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | ≥4 | YES | PASS | — | PENDING |
| 38 | YOTEL Geneva Lake | `adp_yotel_geneva_lake` | period locked | AUDIT_VALIDATED | PASS | COMPLETE | 0 | 0 | ≥0 | 3+ | YES | PASS | — | PENDING |

Exact ADP period IDs and SHA-256 checksums: see `AVA_FULL_E2E_PORTFOLIO_CERTIFICATION_RESULTS.json` and per-hotel audit meta / `checksums.json`.

---

## Remaining blockers (13) — do not force PASS

| Hotel | Attempted | Remaining action |
|---|---|---|
| Cambridge Beaches | Evidence gate; robots/sitemap caused annotation miss; scrub blocked by incomplete DA action fields | Fix AVA-CAMB-004 contract fields; remove robots/sitemap from evidence; recertify |
| Casas del XVI | Baseline generate + gate | Replace IHG Vignette customer hyperlinks with claim-verified reachable sources or Admin manual verification |
| Faranda Collection Bogotá | Baseline generate + gate | Same for RadissonHotels 403 |
| Hilton Times Square | Gate | Hilton.com bot block — scrub or manual verify |
| Hotel Caribe Faranda Grand | Baseline generate + gate | hotelcaribe.com connection reset — alternate evidence or manual verify |
| Hotel Phillips | Gate | Curio/hotelphillips 403 — scrub or manual verify |
| JW Marriott Santo Domingo | Gate | Marriott.com 403 — property URL claim-verify via browser cache / manual |
| Radisson Blu Toronto | Gate | ChoiceHotels 403 — scrub robots + alternate evidence |
| Radisson Santo Domingo | Baseline generate + gate | RadissonHotels 403 |
| St. Regis Cap Cana | Gate | Marriott.com 403 |
| St. Regis Mexico City | Gate | Marriott.com 403 |
| Westin Tampa Waterside | Baseline generate + gate | Marriott redirect flagged unrelated — qualify claim / replace URL |
| W Rome | Gate | Marriott.com 403 |

Pattern: **brand WAF/bot blocks** are the dominant residual class. Autonomous remediation already removes rejected NIH paths and arabellagolf TLS failures; IHG voco was remediable via Turismo Cancún. Marriott/Hilton/Radisson/Choice need the same pattern (reachable destination evidence + prose-only access investigations) or Admin manual verification.

---

## Engineering & operating notes

### What was executed

1. Phase 1 independent finalize certify for six priority hotels (evidence logs, DA classes, commercial checklist, PDF QA).
2. Phase 2 Ambridge DA + evidence/PDF certify; voco IHG 403 remediation.
3. Phase 3 Bethesda DA + NIH broken-URL absence check.
4. Phase 4 remaining portfolio batch: payload generation from certified ADP baselines + public page probe; evidence gates; light DA.
5. Admin payload map expanded for 11 newly generated hotels.
6. DA action PDF-contract normalizer added (`normalizeDigitalAuthorityActionForPdf`).

### Authorization boundaries honored

- No paid Ahrefs/Semrush/Majestic.
- No production deploy / Railway changes.
- No customer publication / emails.
- No founder-approval flag flips.
- No certified ADP measurement changes.

### Estimated effort per hotel (steady state)

| Stage | Effort |
|---|---|
| Baseline load + payload scaffold | 15–30 min |
| Public page / DA research | 30–90 min |
| Evidence gate + remediation | 20–60 min (longer under brand WAF) |
| PDF visual QA | 15–30 min |
| **Typical certified hotel** | **~2–4 hours** |
| **WAF-blocked hotel** | **+1–3 hours** (manual verify or destination replacement) |

### Scale readiness

Architecture is ready to finish the remaining 13 with the established remediation pattern (reachable destination evidence; no customer hyperlinks to bot-blocked brand URLs; honest SUBJECT_COVERAGE_NOT_ESTABLISHED gaps). Do not expand customer delivery until founder authorization per hotel.

---

## Final status codes

| Code | Meaning |
|---|---|
| `PASS` | Technical AVA certification complete for that hotel |
| `PASS_WITH_LIMITATIONS` | Portfolio-level: 25/38 certified; 13 documented blockers |
| `FAIL` | Forced pass attempted — **not used** |

**Portfolio verdict: `PASS_WITH_LIMITATIONS`**

Next actions: remediate the 13 WAF/annotation blockers hotel-by-hotel; then founder review queue for the 25 certified reports.
