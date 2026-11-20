# Dealality Insight — Customer-Facing Advisory Quality

**Status:** BROWSER_VERIFIED (enforcement + Curio RIGHT proposal) · **NOT** founder-approved for publication  
**Date:** 2026-11-19  
**Stream:** Brand Explorer (`ws-brand-explorer`)  
**Curio / Ascend:** **NOT APPROVED**  
**Airtable writes:** 0  
**Gate 2:** Still **BLOCKED** until Gate 1 founder approval

## Founder failure

Dealality View on Market Presence / candidate editorial rendered internal research language as customer copy, including:

- `HPC Open hotels matched=…; keys=…`
- `certState=LIVE_UNVERIFIED_INVENTORY`
- steward-confirmation / “no fabricated property names” workflow instructions

Root cause: phase2 candidates stored HPC/certification dumps in `footprint.editorial*`, and the cumulative assembler treated longer dumps as non-regressive improvements over shorter clean CURRENT notes.

## Fix

| Layer | Change |
| --- | --- |
| Utility | `lib/brand-explorer/be-dealality-insight-advisory-v1.js` + browser mirror |
| Assembler gate | Candidates with internal language never replace customer-facing slots |
| RIGHT advisory | Strip leaks; apply curated assessment packs (Curio + Ascend) |
| Structure | Overall · Opportunity · Advantages · Fit · Commercial · Perspective |
| Atelier | Never paint internal language into customer panels; render assessment sections |
| Issues panel | Rejected internal dumps held out as incomplete/research evidence |
| Contract | `fixedCollections.dealalityInsightAdvisory` |
| Lesson | `BE-QL-DEALALITY-INSIGHT-001` |
| Gate | `npm run test:be-dealality-insight-advisory-v1` |

## Compare Studio evidence

URL: `http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&brand=receQkxgjlezsc1xg`

| Side | Observed |
| --- | --- |
| LEFT Dealality View | Saved short footprint note (unchanged) |
| RIGHT Dealality View | Owner-facing soft-collection advisory — **no HPC/certState** |
| RIGHT Insight | Six assessment sections populated; LEFT shows Missing in existing for new slots |
| Highlight OFF | Clean customer-facing proposed copy |

Screenshots:

- `screenshots/01-curio-dealality-view-LEFT-vs-RIGHT.png`
- `screenshots/02-curio-dealality-insight-assessment-RIGHT.png`

## Ascend

Same universal enforcement + Ascend advisory pack (Choice / soft-collection framing). No Curio copy reuse. Validated in automated gate.

## Explicit non-claims

- Not FOUNDER_APPROVED / not publication-ready
- No production Airtable writes / no mass population
- Gate 2 content reconstruction remains blocked until Gate 1 approval
- Curio and Ascend remain **NOT APPROVED**
