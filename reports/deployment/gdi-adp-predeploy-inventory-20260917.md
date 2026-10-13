# Pre-deploy inventory — GDI/ADP/Decision & Outcome (2026-09-17)

Branch: `deploy/adp-final-trust-closure-20260910`  
Canonical intelligence base (Railway verified): `appa2cE7FTRmIbB32`  
Wrong base guard: `assertNotLegacyMvpCanonicalBase` → rejects `appvtnDurnMSjINP6`

## Classification

### READY_TO_DEPLOY

| Area | Paths |
|---|---|
| Decision & Outcome | `api/decision-outcomes.js`, `lib/decision-outcomes/**`, `server.js` routes, schema/verify scripts, `docs/decision-outcomes-airtable-fields.md` |
| GDI product | `api/group-demand-intelligence.js`, `lib/group-demand-intelligence/**` (Airtable persist, territory, capacity, future-cycle, WHO bridge, Parallel gate), configs for 3 hotels, hotel data packs, UI/CSS/HTML |
| ADP product | `api/ai-demand-positioning.js`, ADP UI decision/outcome controls, monthly-review admin API + `lib/.../monthly-review/**`, share HTML |
| Boot-critical coexist | `api/contact-intelligence.js`, `api/admin-adp-monthly-reviews.js` (already imported by production `server.js` but previously untracked — must ship) |
| HPC identity | `lib/hotel-census/adp-gdi-canonical-identity.js`, alias map, completeness evidence, steward/complete scripts |
| Deploy safety | `.railwayignore` lean exclusions (+ `artifacts/`), `.env.example` intelligence base docs, `package.json` scripts |

Railway env already set: `AIRTABLE_INTELLIGENCE_BASE_ID`, `AIRTABLE_DECISION_OUTCOME_BASE_ID`, `AIRTABLE_GDI_BASE_ID`, `ADP_AIRTABLE_BASE_ID` → `appa2cE7FTRmIbB32`; `GDI_OPPORTUNITIES_PERSISTENCE=airtable`.

### LOCAL_ONLY_EVAL (do not treat as live SoT)

- `artifacts/astra-*`, `data/astra-dev-comparison/**`
- `data/group-demand-intelligence/evals/**`, `research-artifacts/**`, webhound corpora
- Contact-intelligence benchmark verdict docs / experimental CI runners not required for ADP/GDI surfaces
- Frozen first-run JSON freezes under evals (post-wave only)

### DO_NOT_DEPLOY as behavior source

- Hotel-specific hardcodes / Webhound-as-required discovery for Hotels #4+
- Eval freezes as live opportunity SoT (Airtable is SoT)

### NEEDS_REVIEW before GDI expansion waves

- Native blind opportunity discovery not production-wired (`runGroupDemandResearch` still needs `seedCandidates`)
- Cutover doc: do not start Hotel #4 until Native (+ gated Parallel) discovery lanes addressed
- See `reports/group-demand-intelligence/remaining-adp-hotels-for-gdi.md` (12 remaining)

## Coexist / route wipe prevention

1. Asset assert via `npm run assert:production-assets` before upload  
2. Keep Hotel Explorer / Brand / Operator / Brand AI / GDI / ADP share markers in `server.js` + required files  
3. Ship previously-missing API modules that `server.js` already imports  
4. Lean `.railwayignore` so Cloudflare 413 cannot truncate upload mid-surface  
5. Postdeploy smoke on all share static paths  

## GDI selector

`listGdiSelectableHotels()` — data-driven from HPC + config + opportunity dataset. No per-hotel UI hardcodes in this deploy.
