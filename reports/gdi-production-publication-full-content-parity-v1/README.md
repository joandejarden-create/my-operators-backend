# GDI Production Publication Full Content Parity V1

**Status:** `FOUNDER_GO_REQUIRED_FOR_DEPLOYMENT`  
**Hotel:** Spice Island Beach Resort (`recKRJjcPnb4tVDDS`)  
**Customer ready:** **No** (production share still lean)

## Root cause

1. Durable published snapshots stored only lean fields (title / whyThisHotel / next step / primary contact / sources).
2. Production `opportunities.json` does **not** contain the seven certified IDs, and production has no Spice local-shadow overlay — so registry snaps are the sole payload under `strictPublishedOnly`.
3. Share sanitize retained `scalingDrawerModel` but dropped `eventsDrawerModel` / shared-drawer **sections**, and did not flatten `commercialIntelligenceBrief.establishedFactsAndUncertainties` / qualification questions onto customer fields.
4. Client fallback then substituted organization title for “What is happening” and left Potential / Why Act Now / Facts empty.

## Local repair completed (no production deploy)

- Canonical contract: `lib/.../gdi-customer-facing-published-snapshot-v1.js`
- Publication content gate on publish/update
- Share sanitize + drawer builders + UI fallbacks retain commercial body
- Staging registry snapshots rebuilt to **v2** for all seven IDs
- Registry-only (no local-shadow) share simulation **PASS**
- Local browser share shows full Hangers drawer; production still lean (contrast screenshot)

## Preserve

- Seven approved IDs unchanged  
- Classifications: **1 Sales Ready / 6 Account Development**  
- Aventa remains unpublished  

## Key artifacts

| File | Purpose |
|------|---------|
| `00-executive-verdict.json` | Stop gate |
| `01-root-cause.json` | Drop-stage map |
| `02-missing-field-inventory.json` | Pre-repair missing fields |
| `03-source-to-publication-mapping.json` | Alias map |
| `04-seven-opportunity-parity.json` | Field-by-field parity |
| `05-publication-gate-results.json` | Gate + staging updates |
| `07-registry-only-share-verify.json` | Production-like verify |
| `08-production-still-lean-hangers.json` | Live prod probe |
| `09-github-railway-deployment-source-status.json` | PR #136 / Railway |
| `10-recommended-production-repair-sequence.json` | Deploy sequence |
| `11-regression-test-results.json` | Tests + browser |
| `enriched-snapshots/*.json` | Volume update payloads |
| `screenshots/` | Local fixed vs prod lean |

## Commands

```bash
node scripts/test-gdi-publication-full-content-contract-v1.mjs
node scripts/gdi-production-publication-full-content-parity-v1.mjs
node scripts/gdi-verify-enriched-registry-share-local-v1.mjs
```
