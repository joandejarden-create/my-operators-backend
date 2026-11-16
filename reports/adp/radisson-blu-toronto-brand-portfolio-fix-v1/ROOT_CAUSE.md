# ROOT CAUSE

## Why Toronto rendered “Portfolio measurement is being established”

Exact root cause (two layers):

1. **API resolver precedence gap** — `api/ai-demand-positioning.js#resolveBrandPortfolioPosition` preferred only:
   - customer-published BPP pack
   - env-gated local preview packs
   - then fell through to `buildBrandPortfolioPositionPayload(profile)` → `AWAITING_FIRST_MONITORING`

   It **did not** load the already-complete founder-staged Loyalty BPP embedded in durable published-current (`BPP_COMPARISON_CERTIFIED` / `STAGED_COMPARISON_CERTIFIED`, 4 KPIs, 6 ranking rows).

2. **SPA readiness (defense-in-depth)** — `isBrandPortfolioReady` previously accepted only `CUSTOMER_READY` / `PERIOD_2_CERTIFIED_LOCAL`, rejecting `STAGED_COMPARISON_CERTIFIED` even if a READY payload arrived.

Toronto had a valid LOYALTY_BPP period (`…308ba1`, 28×4=112) and a populated durable BPP payload, but the live report API replaced that embedded section with the empty awaiting shell.

**Not caused by:** Core/Loyalty period mixup in metric math, missing Loyalty observations, or Toronto-specific calculation shortcuts.

## Fix

Shared (not Toronto-only):
- `loadMeasurementAvailableBrandPortfolio` resolves Loyalty staged pack → durable embedded BPP → staged owner embedded BPP when measurement is renderable without customer publication.
- Wired into `resolveBrandPortfolioPosition` before awaiting fallback.
- SPA readiness aligned with `bppComparisonCertifiedIsRenderable` (accepts `STAGED_COMPARISON_CERTIFIED` / `BPP_COMPARISON_CERTIFIED`).
- Report response stamps `corePeriodId` + `portfolioPeriodId`.
