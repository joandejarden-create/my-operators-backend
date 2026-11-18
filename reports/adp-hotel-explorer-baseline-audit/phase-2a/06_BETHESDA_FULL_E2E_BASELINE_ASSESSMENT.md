# Phase 2A — Bethesda Marriott Full E2E baseline assessment

| Field | Value |
|-------|-------|
| Property | Bethesda Marriott |
| ADP ID | `adp_bethesda_marriott` |
| HPC | `recLuxvwwxID7U2B8` |
| Canonical 31 | **Included** |
| Customer visible policy (local ledger) | `force_internal` / `COMPATIBILITY_ONLY` |

## Existing artifacts (no fresh paid research)

| Artifact | Status |
|----------|--------|
| Published manifest | **Live** — `data/ai-demand-positioning/published/adp_bethesda_marriott/manifest.json` |
| Latest period | `adp_period_adp_bethesda_marriott_20261002142154_62428a` (published 2026-10-02) |
| Report JSON | Present |
| Evidence JSON | Present (~1.3MB; displacement + missing-by-intent structures) |
| Prior runtime | Also `...20260909091016_9f3a60` |
| Demand capture rate (manifest) | 76.2 / 4 providers |
| Census record on manifest | `recLuxvwwxID7U2B8` |

## Can it pass full workflow **without** fresh paid research?

| Concern | Assessment |
|---------|------------|
| Identity intake | Resolvable via HPC + ADP id; ledger marks compatibility-only — founder policy needed for customer visibility |
| Report regeneration from existing period | Likely possible offline from published/runtime packages |
| Customer-surface certification | **No** — 5× `TERRITORY_SUBJECT_RATES` failures locked to Bethesda |
| Layer B durability proof | **No** — durable `current/` missing for portfolio gate (not Bethesda-specific alone) |
| BPP commercial READY | Not claimed; BPP failures are portfolio-wide, Bethesda not the sole driver |
| Overwrite risk | Do **not** overwrite authoritative observations; refresh only with auth |

### Exact refresh needs (if founder wants surface PASS)

1. **Territory subject rates:** either pin certification expectations to the locked certified period **or** authorize a governed re-cert / remeasure for a new period (may imply paid monitoring). Do not flip constants.  
2. **Identity customer policy:** decide whether Bethesda remains internal-only while on HPC compatibility path.  
3. **Layer B mount:** portfolio durable manifests (Hilton reference path) for permanence Layer B — separate from Bethesda content quality.  
4. **No fabrication** of territory rates or evidence to clear gates.

## Commercial usefulness (existing customer package — Phase 2A read)

Answers against founder questions (qualitative; Phase 2B deep rewrite out of scope):

1. **AI visibility** — Present via demand capture / provider counts; executive surfaces exist.  
2. **Who outperforms / scenarios** — Competitive / displacement structures exist in evidence; surface gates still fail territory rates.  
3. **Why competitors recommended** — Partially via displacement support sets; completeness gated by evidence-link failures elsewhere in portfolio.  
4. **Missing/weak attributes** — Reality-gap / attribute modules exist; Explorer enrichment is a **separate** workstream (do not duplicate).  
5. **What to do** — Action intelligence exists in product; Bethesda-specific action quality not re-authored here.  
6. **How we’ll know improvement** — Longitudinal/runtime periods exist; formal comparability requires certification authority (matrix currently absent).

**Shortcomings → Phase 2B backlog** (see `09_PHASE2B_ENHANCEMENT_BACKLOG.md`).
