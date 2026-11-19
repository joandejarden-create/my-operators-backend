# Brand Explorer Phase 2G — Founder acceptance package

**STOP for founder review.** Curio and Ascend are **not** approved for publication or mass population.

## PR / governance

| Item | Status |
|---|---|
| PR #453 (Phase 2F) | OPEN · founder-rejected · CI green but **behind main** · **do not merge** until defects addressed via IC |
| Phase 2G branch | `feat/be-phase2g-parity-comparison-v1` (includes main sync locally; ships paired comparison + anti-padding) |

## Local URL

```text
http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1
```

Admin Resources → Brand Explorer Comparison (unchanged auth).

## What changed

1. **Comparison architecture** — one page-level scroll; Brand → Tab → Section → Subsection paired rows; next pair starts below taller column; collection match by title/signature; Missing/New empty counterparts.
2. **Anti-padding gates** — repeated sentences, near-duplicate bodies, internal meta language, generic Proof N titles fail closed.
3. **Sanitize pipeline** — strips generator meta; clears duplicates into research gaps (no re-padding).
4. **Subsection parity contract** — `configs/brand-explorer-subsection-parity-contract-v1.json`.

## Explicit non-claims

- Automated score = 1.0 is **not** acceptance.
- Research gaps remain for Ascend imagery and any cleared duplicate economics fields.
- Production Presentation was **not** copied into candidates.

## Evidence

- RCA: `docs/brand-explorer/BE_PHASE2G_REPEATED_CONTENT_RCA.md`
- Scorecards / registers: `reports/brand-explorer/phase2g-parity/`
- Tests: `npm run test:be-phase2g-parity-comparison-v1`

Airtable writes: **0** · Paid research: **0** · Mass population: **none**
