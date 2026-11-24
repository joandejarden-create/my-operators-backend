# Founder report — Restore existing GDI WHO + HOW (PR #585 addendum)

**Status:** STOP before Integration Controller merge  
**Exa spend this addendum:** $0  
**Findymail / Surfe / Airtable / CURRENT / publish:** none  

## Regression found

Canonical Full E2E ran **WHO** (`BUYER_WHO_RESEARCH` + `CONTACT_RESEARCH` via `runRestoredContactInvestigationPassAsync`) but **bypassed** existing public-first **HOW** (`runPublicContactDiscoveryWorkflow`).

Broken connection: `original-full-e2e-research-chain-v1.js` after `CONTACT_RESEARCH` never called HOW.

## Restored (no replacements)

| Stage | Existing function | Status |
|---|---|---|
| WHO identification | `ensureBuyerWhoResearchExecuted` | active |
| Person classify + secondary | `runRestoredContactInvestigationPassAsync` | active |
| Min-1 / target-2 | `evaluateNamedContactCoverageStandard` | active |
| HOW public channels | `runPublicContactDiscoveryWorkflow` | **wired after WHO** |
| Channel verify | `assessPublicContactVerification` | active |
| Optional Surfe/Findymail | buyer-contact-enrichment / providers | **off** (refused in HOW stage) |

## Separate gates

- **WHO_COMPLETE** — ≥1 verified named person (email not required)
- **WHO_SECONDARY_COMPLETE** — 2nd independently verified person
- **HOW_COMPLETE** — public channel research executed to terminal standard (found or documented exhausted/attempted)
- **SALES_READY** — independent; unchanged

## Design Hotels — 18 finalized

| Metric | Count |
|---|---:|
| Primary named person present | **18** |
| Secondary named person present | **0** |
| Public email found/verified | **0** |
| Public phone found/verified | **0** |
| Professional LinkedIn verified | **0** |
| No verified channel found | **18** |
| Sales Ready | **0** |
| WHO_COMPLETE | **18** |
| HOW_COMPLETE | **0** (HOW_NOT_ATTEMPTED in prior recovery — honest) |

## 110 unresolved (outside STAGED)

All **110** remain outside finalized STAGED.  
**WHO incomplete** (no verified named person for finalization) ⇒ **HOW incomplete / skipped** for those candidates. Evidence preserved; standard not lowered.

## Tests

`npm run test:gdi-who-how-contact-intelligence-restore-v1` → **CERTIFICATION_PASS** (10/10)  
Mocked providers only; $0 Exa.

## Acceptance

Existing WHO and HOW intelligence restored with separate completeness gates; HOW executes automatically after WHO in canonical Full E2E (new hotel, portfolio, leisure, resume, replay). Fail-closed on missing mandatory WHO identity. Sales Ready criteria preserved.
