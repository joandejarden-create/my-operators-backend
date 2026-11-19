# Brand Explorer Phase 2C — Curio independent reconstruction

**Contract:** `BE_PHASE2C_CURIO_RECONSTRUCTION_V1`  
**Target:** Curio Collection by Hilton (`receQkxgjlezsc1xg`)  
**Status:** Evidence package for founder review — **STOP before bulk population**

## What this proves

The reusable evidence→slot factory can generate a staged ten-tab candidate **without**:

- Reading production Presentation rows as input
- Using `fixtures/brand-explorer-presentation-curio-*.json` as content source
- Writing Airtable
- Paid research
- Brand-specific hardcoded Curio marketing copy packs

Identity remains Brand Basics `rec…`. HPC is read-only. Existing Presentation is captured as a **frozen baseline** for comparison only.

## Commands

```bash
npm run test:be-phase2c-curio-reconstruction-v1
node --env-file=<env> scripts/be-phase2c-curio-reconstruction-v1.mjs
# preview: public/brand-explorer-phase2c-preview/index.html
```

## Artifacts

Under `reports/brand-explorer/phase2c-curio-reconstruction/`:

1. `01-baseline-snapshot.json` — read-only production Presentation + Basics  
2. `02-independent-evidence-pack.json` — non-Presentation sources only  
3. `03-independent-candidate.json` — staged candidate rows  
4. `04-population-matrix.json` — field-level matrix  
5. `05-quality-audit.json`  
6. `06-baseline-comparison.json`  
7. `07-render-preview-note.json`  
9. `09-paid-research-gate.json`  
10. `10-acceptance-report.md` / `.json`

## Honest limits

- Canonical `brand-explorer:build` still has **materialization disabled** — Phase 2C is the reconstruction/research stage evidence path.
- Many economics / gallery / momentum / standards-table slots remain `RESEARCH_REQUIRED`.
- Isolated candidate HTML preview is shipped; **full atelier component mount = NOT_VALIDATED**.
- Live HPC for Curio is `LIVE_UNVERIFIED` unless independently certified.

## Scaling gate

Do **not** recommend factory scale-up until research gaps are source-backed, atelier render is validated, and founder authorizes any paid research budget.