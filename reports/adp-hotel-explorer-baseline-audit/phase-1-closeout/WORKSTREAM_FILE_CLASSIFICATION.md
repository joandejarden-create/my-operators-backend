# Phase 1 closeout — workstream file classification

Explicit staging only. No `git add .`.

## PR #364 — Hotel Explorer Phase 1 (COMMIT)

| Path | Why |
|------|-----|
| `.gitattributes` | Windows LF fixture correction (1.2) |
| `api/brand-presence.js` | Identity aliases + HPC by-id fallback |
| `api/brand-presence-internal-preview.js` | Authorized Bethesda internal-preview API |
| `lib/hotel-intelligence/radar-internal-preview-hotels-v1.js` | HPC→DTO merge + auth (owner-app latch for local bypass) |
| `middleware/requireDealalityUser.js` | Local DEV_AUTH_BYPASS admin parity (production-blocked) |
| `public/brand-presence-mapping.js` | Authorized internal-preview client merge |
| `public/deal-capture-radar-with-ranked-list.html` | HE primary wiring + auth-helper + cache bust |
| `server.js` | Register authenticated internal-preview route |
| `package.json` | HE gates only (`radar-app-opportunity…`, `radar-internal-preview…`, phase1 composite) |
| `scripts/test-radar-app-opportunity-radar-he-wiring-v1.mjs` | App route→HE gate |
| `scripts/test-radar-internal-preview-hotels-v1.mjs` | Bethesda auth/merge gate |
| `reports/.../phase-1.2/` | Validation evidence |
| `reports/.../phase-1.3/` | Bethesda bridge + architecture evidence |
| `reports/.../phase-1-closeout/` | Closeout decision + screenshots |

## Hold local — Phase 1.3A/1.3B census migration (DO NOT COMMIT to #364)

| Path | Why |
|------|-----|
| `data/hotel-identity/` | Identity ledger prototype |
| `lib/hotel-identity/` | Ledger + eligibility + shadow modules |
| `scripts/seed-canonical-hotel-identity-ledger-v1.mjs` | Ledger seed |
| `scripts/run-census-dual-source-shadow-v1.mjs` | Shadow CLI |
| `scripts/test-canonical-hotel-identity-ledger-v1.mjs` | 1.3b gate |
| `scripts/test-radar-eligibility-authorization-model-v1.mjs` | 1.3b gate |
| `scripts/test-census-dual-source-shadow-v1.mjs` | 1.3b gate |
| `reports/.../phase-1.3a/` | Census reconciliation / migration analysis |
| `reports/.../phase-1.3b/` | Shadow + migration planning |

Preserved as untracked local files for the next PR. Not discarded.

## Excluded noise

- `reports/adp-six-hotel-e2e/.../DURABILITY_PROBE.json` — reverted
- Customer-surface contract JSON churn from local gate runs — reverted
