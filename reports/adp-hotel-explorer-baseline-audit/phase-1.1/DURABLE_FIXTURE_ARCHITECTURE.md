# Phase 1.1 — Durable fixture architecture (Layer A + Layer B)

## Layer A — Deterministic offline CI (implemented)

**Root:** `fixtures/ai-demand-positioning/permanent-full-e2e-offline/`

| File | Role |
|------|------|
| `MANIFEST.json` | Schema, checksums, expected outcomes, `notProductionProof: true` |
| `property-profile.json` | Sanitized synthetic hotel profile |
| `observations.json` | 32 synthetic observations (8 scenarios × 4 providers) |
| `baked-package.json` | Intentional mismatch / 78 contamination for negative parity |
| `prepared-package.json` | Corrected unpublished package (`published: false`) |
| `README.md` | Human notes |

**Gate:** `npm run test:adp-permanent-full-e2e-certification-v1`  
Runs Layer A always. Does **not** copy production durable packages into Git.

**Coverage:** provenance/checksums, lifecycle no-skip, provider mismatch correction, runtime-flag ignore, legacy 78, missing text fail-closed, invalid frozen eligibility, manifest cert artifact, prepared package cert, HTTP 200 ≠ sufficient.

## Layer B — Authoritative durable evidence (documented, separate from PR CI)

See `PHASE1_1_LAYER_B_DURABLE_EVIDENCE_PROCESS.md`.

- Requires gitignored `data/adp-durable-published/**`
- Optional hard fail: `ADP_PERMANENCE_REQUIRE_LAYER_B=1` → EXIT 2 when missing
- Measured-dates: `PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json` (not fabricated)

## Persistence fixtures

Synthetic longitudinal mock store: `fixtures/ai-demand-positioning/longitudinal-synthetic/history-store/` (local mock only).
