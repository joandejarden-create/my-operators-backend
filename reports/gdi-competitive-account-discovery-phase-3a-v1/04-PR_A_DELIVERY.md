# CAD Phase 3A PR-A — Delivery note

**Auth:** `FOUNDER_GO AUTHORIZE_GDI_CAD_PHASE_3A_PR_A`  
**Scope:** contract + classifier + tests + docs  
**Mutations:** Airtable OFF · STAGED OFF · CURRENT OFF · paid OFF · Bethesda freeze ON · Wyndham freeze ON

## Files

| Path | Role |
|---|---|
| `lib/.../gdi-competitive-account-discovery-contract-v1.js` | Enums, programKey, sanitize sourcing window, validate, dedupe helper |
| `lib/.../gdi-competitive-account-discovery-classifier-v1.js` | Deterministic classifier + attach (pure) |
| `scripts/test-gdi-competitive-account-discovery-v1.mjs` | Regressions |
| `docs/ai-build-system/GDI_COMPETITIVE_ACCOUNT_DISCOVERY_V1.md` | Contract doc |
| `package.json` | `test:gdi-competitive-account-discovery-v1` |

## Classifier summary

- Reuses `extractCompetitorHotelFromResearchEvidence` + `isEventVenueOnlyMention`
- Classes: HISTORICAL / EMERGING / FQO_CANDIDATE
- Attainability + CONFIRMED|INFERRED|UNKNOWN sourcing windows
- Explicit `fqoPass: false`, `persistenceAuthorized: false`, `qualificationTransferred: false`

## Remaining gaps for PR-B

1. Read-only Bethesda 68 + Wyndham CLEAN 11 / HOLD scoring report using this classifier  
2. Counts: Historical vs Emerging vs FQO_CANDIDATE (not FQO PASS)  
3. Duplicate/enrich recommendations by `programKey`  
4. Still no Airtable / pointer / paid mutations  

Do not start PR-B until this PR is merged by Integration Controller.
