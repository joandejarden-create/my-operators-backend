# AVA — $15 Paid Pilot + Portfolio Presentation Certification

**Contract:** `AVA_15USD_PILOT_AND_PORTFOLIO_PRESENTATION_CERTIFICATION_V1`  
**Generated:** 2026-11-05  
**Founder authorization:** Manifest AS WRITTEN (scrapes OFF; Ahrefs/Parallel excluded)  
**Publication / deployment / ADP remeasurement / automatic customer approval:** **NOT authorized**

---

## Executive verdict

Paid pilot completed under budget (**≈ $0.54** combined worst-case estimate vs **$15** cap). Primary commercial value came from **Context.dev fact extraction** (new correction candidates) and **Castillo customer backlink completeness** (3 restored + Visit Palma dedupe → **6 unique verified backlinks**). DataForSEO delivered useful referring-domain candidates for Waterstone/Castillo but heavy **spam/low-quality noise** for AC A Coruña; competitor-gap intersection quality was weak for this peer selection.

Portfolio PDF presentation regen (no Exa rediscovery): **37/38 full technical PASS**, **1 PASS_WITH_LIMITATIONS** (W Rome hyperlink-host heuristic), **0 FAIL**. Castillo (6 unique BLs) and Waterstone are among the full passes.

**Do not** auto-wire DataForSEO/Context.dev into every AVA run.

---

## 1. Three-hotel paid pilot results

### Spend (actual / bounded)

| Bucket | Actual | Cap | Notes |
|---|---:|---:|---|
| DataForSEO | **$0.378704** (incl. prior DFS-01 $0.024036) | $10 | All DFS-01…21 attempted |
| Context.dev | **53 credits**; worst-case cash **$0.159** @ $0.003/credit | $5 | Scrapes disabled; CTX-02 failed with **0 credits** billed |
| **Combined** | **≈ $0.538** | **$15** | Stop rules never tripped |

Artifacts: `AVA_15USD_PAID_PILOT_RESULTS.json`, `AVA_15USD_PAID_PILOT_RAW/`.

### Call outcomes

| Lane | Result |
|---|---|
| DataForSEO backlink summary / lists / RDs | Completed for all 3 hotels |
| DataForSEO domain intersection | Returned mostly low-value overlapping spam domains for chosen peers |
| DataForSEO Maps / GBP | Waterstone Maps partially usable; ES location_name / keyword shapes often **40501 Invalid Field** (AC, Castillo) — listing discovery incomplete |
| Context.dev search | OK (3×1 credit) — discovered Hilton Curio Waterstone URL + OTA/Google Travel URLs |
| Context.dev extract | 5/6 OK (10 credits each); CTX-02 Hilton brand extract **400** (no markdown) — brand ACCESS_RESTRICTED confirmed |

### Vs certified baseline (customer-facing)

| Hotel | Baseline customer BL / mentions | After pilot + Castillo restore | Incremental commercially useful |
|---|---|---|---|
| Waterstone | 0 / 1 | 0 / 1 (unchanged customer PDF) | Hilton property URL discovered; Apple Maps / TripAdvisor / Booking URLs; CTX listing extract flagged **hilton.com vs waterstoneboca.com** website split; DFS candidates (e.g. seasprayboca, luxesource, Boca Chamber) **not yet HTML-verified into customer sources** |
| AC A Coruña | 2 / 4 | 2 / 4 | CTX extract OK; Google Travel spa/fitness conflict noted; DFS RDs dominated by **spam (.xyz/.cyou)** — **0 high-quality new BL promotions** |
| Castillo | 4 raw / 3 unique + 3 mentions | **6 unique BL** + 3 mentions | Restored Mallorca Comercial ×2 + MCB after href verification; Visit Palma http/https deduped; DFS confirms large owned-domain graph (candidates for later zero-cost HTML verify) |

### Incremental metrics (pilot success definition)

| Metric | Count / note |
|---|---|
| New verified customer backlinks (promoted) | **+3 Castillo** (restore); Waterstone/AC **+0** pending HTML quality gate |
| New commercially useful competitor gaps | **Low** — intersection peers returned few actionable non-spam gaps; Castillo golf directories (golfsiden, golfhome) are follow-up candidates only |
| Newly verified platform information | Hilton Curio URL; CTX extracts for Marriott/Google Travel facts; Maps/GBP API limited by parameter errors |
| New actionable corrections | **≥4** evidence-supported discrepancy notes (Waterstone dual-website; AC Turkish bath vs no spa; Castillo spa 900/990 + naming/breakfast conflicts) — queue for Correction Register, not auto-applied |
| Duplicate / low-quality findings | **High** on AC DFS sample; many Waterstone directory clones |
| Cost per meaningful new action | ≈ **$0.54 / ~7 meaningful items** ≈ **~$0.08** (Castillo restores + CTX corrections + Waterstone URL discovery). Excludes unverified DFS spam |

---

## 2. Castillo backlink presentation completeness

| Step | Result |
|---|---|
| Reverify inventory-only BLs | All 3 passed `fetchHtml` + hotel href check |
| Restore to customer `sources` | Mallorca Comercial zona + empresa; Mallorca Convention Bureau |
| Deduplicate Visit Palma | http/https collapsed → unique linking page |
| Customer unique verified BLs | **6** (was 4 raw / 3 unique) |
| PDF after portfolio regen | Snapshot **BL=6/6** hosts visible; CQA/evidence/parity **PASS** |

Report: `AVA_CASTILLO_INVENTORY_BACKLINK_RESTORE.json`.

---

## 3. Recommended ongoing provider policy

| Provider | Policy |
|---|---|
| **Exa** | Remain **DEFAULT** for AVA discovery |
| **DataForSEO** | **OPTIONAL_DEEP_DIVE** only — hotel-owned domain summary + capped backlink sample when Exa returns ACCESS_RESTRICTED or thin BL sets; always apply spam/quality filter before customer promotion; fix Maps `location_code` usage before relying on GBP |
| **Context.dev** | **CONDITIONAL** — search + extract for bot-gated brand pages and listing fact conflicts; **scrapes remain OFF** until hard credit bound proven |
| **Ahrefs** | Do **not** purchase / integrate |
| **Parallel paid** | Remain **deferred** |
| Auto-integrate into every AVA run | **No** |

---

## 4. Portfolio PDF regeneration status

Script: `scripts/ava-pdf-visibility-presentation-portfolio-v1.mjs`  
Results: `AVA_PDF_VISIBILITY_PRESENTATION_PORTFOLIO_RESULTS.json`

| Gate | Hotels passing |
|---|---:|
| Evidence validation | 38/38 |
| Digital Authority section present (no strategy codes) | 38/38 |
| Visual QA (section + pages) | 38/38 |
| Preview/Download SHA parity | 38/38 (same-byte preview copy) |
| CQA | 38/38 |
| Hyperlink host visibility vs customer BL count | 37/38 |
| **All technical gates** | **37/38** |

### Full pass (37)

Includes Waterstone, Castillo (**6** unique verified BLs), AC A Coruña, and all others except W Rome — see `AVA_PDF_VISIBILITY_PRESENTATION_PORTFOLIO_RESULTS.json` (`allPass: true`).

### PASS_WITH_LIMITATIONS (1)

| Hotel | Blocking gate | Issue |
|---|---|---|
| W Rome | Hyperlink hosts | 3/4 BL hosts matched in PDF text heuristic (one referring host/org not detected in extracted text); evidence + CQA + parity still PASS |

Prior PDF versions preserved under each hotel’s `pdf-staged/pre-visibility-portfolio-regen/`.

---

## 5. Remaining research limitations

1. Context.dev cannot extract Hilton Curio Waterstone brand HTML (ACCESS_RESTRICTED).  
2. DataForSEO Maps/GBP for Spain hotels failed location/keyword validation in this manifest shape.  
3. DFS raw backlink volume ≠ customer-ready authority — spam filter mandatory.  
4. Competitor gap peers (`hiltonboca.com`, `nh-hotels.com`, `arabellagolfmallorca.com`) were coarse; better property-level peers needed for commercial gaps.  
5. Waterstone DFS candidates not yet promoted (needs zero-cost HTML verification pass).  
6. W Rome hyperlink-host visibility heuristic still 3/4 (non-blocking for evidence/CQA).

---

## 6. Outstanding founder decisions

1. Approve promoting any HTML-verified Waterstone DFS candidates into customer sources (separate zero-cost pass)?  
2. Authorize a follow-up **Maps-only** DFS micro-budget with corrected `location_code` for AC + Castillo GBP?  
3. Add CTX correction candidates to Digital Platform Correction Register now, or after hotel confirmation?  
4. Accept W Rome as PASS_WITH_LIMITATIONS or request a targeted PDF host-label fix?  
5. Customer publication / deploy — still **blocked** until explicit approval.

---

## Stop state

- Paid pilot executed and bounded.  
- Castillo customer BL completeness restored.  
- Portfolio presentation PDFs regenerated and staged.  
- **No deployment, no customer publication, no ADP measurement changes, no automatic founder approval.**

Await founder review of this certification.
