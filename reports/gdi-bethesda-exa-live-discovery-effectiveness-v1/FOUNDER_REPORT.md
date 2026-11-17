# FOUNDER REPORT — Bethesda Marriott Live Exa Discovery Effectiveness V1

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Run ID:** `gdi_e2e_14seg_mwladic1_ff9898`  
**Verdict:** `FULL_E2E_EXECUTION_COMPLETE` (`DEEP_RESEARCH_COMPLETED`)  
**Actual Exa spend:** **$3.20** of $15.00 hard ceiling (remaining $11.80)  
**Publication:** none · Airtable writes: 0 · Apify: not orchestrated

## Live execution outcome

Live Exa AI executed successfully through the canonical Full E2E research entry after safety gates in PR #348.

- Preflight status before spend: **LIVE_READY**
- Provider: Exa Agent only (discovery + deep seed)
- Discovery: **14/14 segments LIVE** (`freshRetrievalCells=14`, `resumedCells=0`)
- Candidates extracted (raw): **165**
- Deep-researched / processed under `maxCandidates=20`: **20**
- Paid deep research operations settled: **18**
- Certificate: `budgetsRespected=true`, `outputsIngested=true` (evidence-based, not hard-coded)

## Segment coverage

All fourteen canonical segments received **LIVE_EXECUTED** discovery. Deep research completed for 13 segments; `preferred_lodging_partnerships` deep status **SKIPPED** (duplicate / no paid deep retained).

No discovery segment was `FIXTURE_ONLY` or `SKIPPED_BUDGET`. Budget headroom remained $11.80 after the controlled campaign.

## Novelty vs 67 published baseline

Baseline package: `gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1` · fingerprint `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f` · **67** CUSTOMER_PUBLISHED IDs (unchanged).

| Evaluation label | Count |
|---|---|
| NEW_UNIQUE | 3 |
| EXISTING_ENRICHED | 0 |
| DUPLICATE | 1 |
| REJECTED | 16 |
| FURTHER_RESEARCH | 0 |

### NEW_UNIQUE (watchlist-grade)

1. **Amentum Holdings / Amentum** — `corporate_accounts_scaling` — WATCHLIST_RESEARCH · 2 contacts investigated · **1 sales-ready**
2. **American Political Science Association** — `government_public_sector` — WATCHLIST_RESEARCH · contacts thin
3. **SAME Foundation** — `social_special_events` — WATCHLIST_RESEARCH · contacts thin

### DUPLICATE

- **Rock Creek Mansion** — matches baseline `gdi_plp_live_fa2063dc1195`

### Commercial qualification

Most processed rows dispositioned **EXCLUDE** (16/20). Three retained as **WATCHLIST_RESEARCH** (not CONFIRMED_DEMAND / RELIABLE_INFERRED_DEMAND). Speculative demand was not promoted.

Sales-ready contacts on processed set: **2** (Amentum + Enabled Intelligence; latter EXCLUDE).

## Main discovery weaknesses

1. High extract volume (165) but low commercially retained yield after qualification.
2. Missions still surface large national orgs / national events that fail Bethesda catchment (AHA, NFL, AEA, NASPA, etc.).
3. Contact relevance remains weak outside one corporate scaling hit.
4. `maxCandidates=20` intentionally capped deep spend — remaining raw candidates were not deep-researched in this campaign.
5. Preferred-lodging rediscovery of Rock Creek Mansion burned budget without novelty.

## Top five recommendations

1. Tighten Bethesda/DMV catchment + negative filters in Exa discovery missions (especially associations, sports, corporate meetings).
2. Prefer early corporate-expansion / workforce / project signals over national conference calendars.
3. Improve contact-relevance verification before counting sales-ready.
4. Dedupe against published inventory **before** paid deep seed.
5. Raise per-segment candidate diversity and strengthen local Montgomery County meeting/catering missions.

## Cost efficiency

- Actual spend: **$3.20**
- Cost per NEW_UNIQUE (watchlist): **$1.07**
- Cost per sales-ready NEW_UNIQUE: **$3.20**
- Cost per paid deep op: **$0.10** (medium effort quote settled)

**Customer inventories were not modified.**
