# Bethesda 88 — Opportunity Title Quality Repair V1

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Inventory:** 59 canonical + 29 historical = **88**  
**Paid research:** `$0`  
**Canonical `opportunities.json` writes:** **NOT APPLIED** (dry-run patches only)

## Verdict

All 88 titles were audited against a reusable title-quality contract. Historical research-stage titles were repaired via protected merge. Canonical title-only patches await founder approval. Meeting join-instruction contamination is quarantined from customer publication by the shared gate (no silent share-pack rewrite).

## Defect counts (pre-repair audit)

| Cause | Count |
|---|---:|
| RESEARCH_STATUS_IN_TITLE | 9 |
| SPECULATIVE_OVERFLOW | 7 |
| UNCONFIRMED_DESTINATION | 7 |
| OVERLONG | 2 |
| SOURCE_NAV_CONTAMINATION | 2 |
| INTERNAL_WORKFLOW | 1–2 |
| RAW_SOURCE_TEXT | 1 |
| MEETING_CREDENTIALS / TITLE_SOURCE_INVALID | 1 |

Defective rows: **28** · Proposed changes: **33** (includes evidence-grounded refinements)

## Known examples → repaired titles

| ID | Defect | Proposed title |
|---|---|---|
| `gdi_opp_amwa_2028_annual` | destination TBD | AMWA 113th Annual Meeting 2028 |
| `gdi_opp_umd_alumni_weekend_watch` | research monitoring language; 2027 dates unverified | University of Maryland alumni & family weekend — 2027 dates not yet announced |
| `gdi_opp_series_if_prompted_enter_meeting_id_*` | meeting join instructions | NIDA Clinical Trials Network — unidentified virtual session (TITLE_SOURCE_INVALID) |
| `gdi_opp_nih_research_festival_2026` | speculative overflow in title | NIH Research Festival 2026 |

UMD: Family Weekend 2027 dates are **not yet determined** ([familyweekend.umd.edu](https://familyweekend.umd.edu/)). Do not invent alumni-weekend dates.  
NIH Research Festival 2026 is the official festival title ([researchfestival.nih.gov/2026](https://researchfestival.nih.gov/2026)); lodging thesis stays in demand copy, not the title.

## Root cause

1. **Series / page extraction** promoted raw `<title>` text (nav chrome, virtual-meeting join instructions) into `opportunity.title` without a shared quality gate.  
2. **Pilot-seed** authored research-status and speculative overflow clauses directly into titles.  
3. PDF had a partial NCIF sanitizer; store/UI/customer-surface did not share the same contract.

## Pipeline fix (reusable — not a 4-title map)

- `lib/group-demand-intelligence/opportunity-title-quality-v1.js` — classify / sanitize / redact / customer-safe / publication block  
- Wired into:
  - `customer-surface-revalidation-v1.js` (blocks credential/nav titles from customer-active)
  - `gdi-live-evidence-yield-v1.js` (rejects join-instruction titles at extraction)
  - `gdi-pdf-report-data-v1.js` (shared customer-safe display)
  - Pilot-seed source titles corrected
- Bethesda orchestrator: `research/bethesda-88-title-quality-repair-v1.js`

## Persistence

| Layer | Action |
|---|---|
| Historical 29 | Title-only corrections **applied** via protected research-stage merge + checkpoint |
| Canonical 59 | **22 title-only dry-run patches** in `03-canonical-59-title-dry-run-patches.json` |
| Woman's Club contact | Preserved (`Diane Robertson`, store SHA `a298d6b8…`) |
| Contacts / Exa / categories / need dates / shares | Unchanged |
| Sales Ready / publication | No promotion; TITLE_SOURCE_INVALID forces non-customer |

## Publication exposure

- Meeting-credential title: **not** customerFacing; **not** found in share registries.  
- Until canonical patches are approved, customer surfaces must use `customerSafeOpportunityTitle()` / `titleBlocksCustomerPublication()`.  
- Customer-facing titles that still need store rewrite after approval include AMWA 2028, UMD weekend watch, NIH Research Festival, and other pilot-seed defects listed in the dry-run patch file.

## Integrity

- Union **88** · Canonical **59** · Historical **29** · Scaling Companies **5** intact  
- Store SHA unchanged by this run (`a298d6b8…`)  
- No paid providers · No unapproved canonical writes · No automatic customer publication

## Commands

```bash
npm run gdi:bethesda-88-title-quality-repair-dry-run
npm run gdi:bethesda-88-title-quality-repair
npm run test:gdi-bethesda-88-title-quality-repair-v1
```
