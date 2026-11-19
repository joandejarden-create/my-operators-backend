# Brand Explorer Phase 2D — Curio Quality Recovery

**Contract:** `BE_PHASE2D_CURIO_QUALITY_RECOVERY_V1`  
**Goal:** Improve the universal factory so an independent Curio reconstruction reaches high applicable completeness without Presentation/fixture copy-in, paid research, or production writes.

## What changed

1. **Production template inventory** (`lib/brand-explorer/be-phase2d-template-inventory-v1.js`)  
   Reconciles ~229 Presentation rows → unique slot keys + multi-row collections + atelier references. Applicable denominator excludes only legacy freeform and parse artifacts (trailing-dot aliases). Does **not** shrink the denominator to inflate scores.

2. **Gap diagnosis** for Phase 2C’s 72 `RESEARCH_REQUIRED` slots into:
   - Evidence exists but extraction failed  
   - Evidence exists but generator mapping failed  
   - Public research needed  
   - Private/proprietary required  
   - Optional / inapplicable / undisclosed  

3. **Canonical research process** (`be-phase2d-research-process-v1.js`) — one brand-agnostic stage list (Basics → official → PI → HPC → FDD diligence → loyalty → peers → momentum → imagery → paid gate).

4. **Enhanced universal generator** (`be-phase2d-curio-quality-recovery-v1.js`) — evidence→slot mapping for inventory-applicable fields; FDD public disclosure for investment framing; diligence guidance when confidential figures unavailable; no fee % invention.

5. **Real atelier overlay** — `beInternalPreview=1&beCandidateOverlay=1` loads `public/brand-explorer-phase2d-preview/curio-candidate.json` into `brand-explorer-atelier-from-api.js` (not a standalone HTML substitute).

## Commands

```bash
npm run test:be-phase2d-curio-quality-recovery-v1
npm run be-phase2d-curio-quality-recovery-v1
```

Atelier validation URL (local server):

`/brand-explorer-combined.html?brand=curio-collection&beInternalPreview=1&beCandidateOverlay=1`

## Hard stops (unchanged)

- No mass population  
- No paid research activation without founder gate  
- No production Airtable Presentation writes  
- No publication  
