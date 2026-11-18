# Phase 1.3 — Gate results

## Automated

| Gate | Result |
|------|--------|
| `test:radar-host-hotel-explorer-wiring-v1` | PASS |
| `test:radar-hotel-explorer-progressive-enrichment-v1` | PASS |
| `test:radar-app-opportunity-radar-he-wiring-v1` *(new)* | PASS |
| `test:radar-internal-preview-hotels-v1` *(new)* | PASS |
| `assert:hotel-explorer-share-assets` | PASS |
| `test:adp-published-store-durability-v1` | PASS |
| `test:ai-demand-positioning-published-foundation` | PASS |
| `test:adp-phase1-radar-he-gates` (composite, updated) | PASS |
| `test:adp-permanent-full-e2e-certification-v1` Layer A | PASS |
| Permanence Layer B | BLOCKED_MISSING_AUTHORITATIVE_EVIDENCE (expected) |

## Fixture LF (Phase 1.2 retained)

All offline permanence JSON fixtures remain `i/lf w/lf` with unchanged MANIFEST checksums.

## Live API checks (local 8091)

| Check | Result |
|-------|--------|
| `GET /api/brand-presence` Bethesda | Absent (15692) |
| `GET /api/brand-presence/internal-preview-hotels` (authorized local bypass admin) | Bethesda Marriott, `internalPreviewOnly:true`, `customerVisible:false` |
| Query-alone authorization doctrine (unit) | `authorizedByQueryAlone:false` |

## Browser

See `PHASE1_3_BROWSER_VALIDATION.md`.
