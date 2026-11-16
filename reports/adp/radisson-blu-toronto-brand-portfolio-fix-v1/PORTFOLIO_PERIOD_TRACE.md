# PORTFOLIO PERIOD TRACE

| Layer | Module / function | Toronto result |
|------|-------------------|----------------|
| UI | `public/js/ai-demand-positioning/ai-demand-positioning.js#renderBrandPortfolio` / `isBrandPortfolioReady` | READY path after fix |
| Report API | `getAiDemandPositioningReport` → `resolveBrandPortfolioPosition` | measurement-available BPP |
| Period resolver | `resolveReportPeriodIds` | core=adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b · portfolio=adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1 |
| Portfolio loader | `loadMeasurementAvailableBrandPortfolio` | source=loyalty_bpp_comparable_cohort_rank |
| Eligibility | `bppComparisonCertifiedIsRenderable` + staged comparison states | true |
| Fallback | `buildBrandPortfolioPositionPayload` awaiting copy | **not used** |

Loyalty period: `adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1` · family LOYALTY_BPP · scenarios 28 · observations 112
Core period: `adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b` (not used for portfolio metrics)
