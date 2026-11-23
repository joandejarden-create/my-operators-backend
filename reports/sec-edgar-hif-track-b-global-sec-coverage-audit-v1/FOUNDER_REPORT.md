# Track B — Global SEC / CMBS / Contractual Coverage Audit V1

**As of:** 2026-11-23T09:27:49.104Z  
**Publication:** CUSTOMER_BLOCKED · **Research cost:** $0  
**Branch:** feat/hif-v6-track-b-international-source-expansion-v1 · **PR:** #556 (update)

## A. Existing SEC research capabilities

- EDGAR eligible-filing inventory builder (`data/sec-edgar/eligible-filings/`)
- CIK registry V24 (`32` companies / `31` with CIK)
- Checkpointed research runners (Marriott/Hilton historical V2 + universal V24)
- Exhibit / EX-10 enumeration + HTML schedule extractors
- CMBS 424B pilot (contract-timing-cmbs-v1)
- Coverage ledger V2/V3 contracts
- Track B global waves + ISE + CALA follow-up persistence via `hif-track-b-global-waves-persist.mjs`
- Free EDGAR only in this pass (no new paid auth)

## B. Actual registrant and filing coverage

| Metric | Evidence |
|---|---|
| Registry V24 companies | 32 |
| Registry with CIK | 31 |
| Eligible index companies | 27 |
| Eligible filings DISCOVERED | **11940** |
| Checkpoint docs status=ok | **50** |
| Checkpoint docs error | 2 |
| Local SEC cache files | 72 |
| CORE_SIX eligible / processed | 3863 / 20 |

## C. Discovered vs processed vs commercially assessed

| Stage | Count / status |
|---|---|
| DISCOVERED | 11940 |
| RETRIEVED | 52 |
| EXTRACTED | 50 |
| HOTEL_MATCHED | PARTIAL — prior waves; not re-counted in this audit (see Track C / HPC gates) |
| COMMERCIALLY_ASSESSED | 34 prior global-wave dossiers + Track B ISE/CALA waves (separate reports) |
| PERSISTED | Prior waves via foundation/global-waves persist; this audit batch persists separately |
| UNPROCESSED | **11890** |

Reconcile prior claims:
- **11,940** → CONFIRMED DISCOVERED inventory (not analyzed).
- **29 CIKs** → approximate; live evidence shows **31** registry-with-CIK / **27** index companies.
- **CMBS hundreds** → overstated vs **57** verified hotel-level dates (pilot) + **21** unresolved CMBS dossiers in global waves.

## D. Historical commercial findings (this gap test)

Nine APLE hotels completed Marriott→third-party manager transitions in **January 2026** (footnotes 4/5/6 on YE2025 10-K property schedule): Huntington (6 CA), Dimension (1 CA), Crestline (2 FL). Closed MCT events — franchise continues; hotel-level franchise expiry still undisclosed.

## E. Forward-looking commercial findings

RHP Marriott HMAs with **year-only** expirations: Gaylord×4 (2047), Rockies (2053), JW Hill Country (2060), **JW Desert Ridge (2027)** near-term with five automatic 10-year renewals if performance thresholds met, Inn at Opryland (2034), AC Hotel (2040). APLE portfolio: ~81% short-term variable HMAs (structural flexibility — not hotel-specific open MCT).

## F. Unprocessed high-value cohorts

See `COVERAGE_INVENTORY.json` → `highValueGaps`. Priority next: PEB/CLDT/SVC named EX-10; Ashford EX-10 over mass 8-K; Host/Sunstone/DRH named schedules. Summit YE2025 = negative schedule.

## G. Incremental discovery yield

- Filings analyzed this pass: **2** (APLE + RHP 10-K; Summit negative reviewed)
- Persist-eligible dossiers: **18**
- Staged (non-persist): **1**
- Named hotels with commercial facts: **18**

## H. Source hyperlinks and evidence integrity

- APLE: https://www.sec.gov/Archives/edgar/data/1418121/000119312526064008/aple-20251231.htm
- RHP: https://www.sec.gov/Archives/edgar/data/1040829/000110465926019035/rhp-20251231x10k.htm
- Claim-level snippets on each dossier; year-only → no fake ISO dates.

## I. Airtable persistence / idempotency

Executed on batch dir `reports/sec-edgar-hif-track-b-global-sec-coverage-audit-v1`:

| Mode | Creates | No-ops | Failures | Inventory | Findings | Contracts | Milestones |
|---|---|---|---|---|---|---|---|
| DRY_RUN | 54 | 0 | 0 | 18 | 18 | 18 | 0 |
| APPLY | 54 | 0 | 0 | 18 | 18 | 18 | 0 |
| Re-APPLY (idempotency) | 0 | 54 | 0 | — | — | — | 0 |

No new schema. CUSTOMER_BLOCKED. No Verified BCT/MCT auto-promotion. Year-only → **0** milestone ISO rows.

## J. Research cost and future cost

- This pass: **$0**
- Paid proposal (optional later): sec-api.io bulk exhibits ~$50–150/mo only if free EDGAR rate limits block named EX-10 queue — **not required now**.

## K. Track A handoff

See `TRACK_A_HANDOFF.json`. Questions center on closed MCT vs residual BCT, and Desert Ridge 2027 auto-renewal realism. **Do not auto-promote to Verified BCT/MCT.**

## L. Track C handoff

See `TRACK_C_HANDOFF.json`. All gap-test hotels `PENDING_HPC_SEARCH`.

## M. Integration Controller handoff

Update open PR **#556** on current branch. IC owns merge. No scheduler / Surfe / customer publication. Preserve dirty `tmp/*`.

## N. Saturation assessment

**NOT_SATURATED** — extracted ≈ **0.419%** of discovered eligible filings. Productive coverage remains in named owner schedules and EX-10 exhibits, not bulk filing exhaustion.

---

### Persistence command
```bash
node scripts/hif-track-b-global-waves-persist.mjs --batch-dir=reports/sec-edgar-hif-track-b-global-sec-coverage-audit-v1
node scripts/hif-track-b-global-waves-persist.mjs --batch-dir=reports/sec-edgar-hif-track-b-global-sec-coverage-audit-v1 --apply
```
