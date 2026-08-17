# Brand AI Visibility — Phase 3C.1 Discoverability / Referral / Business Impact Foundation

**Status:** Foundation PASS  
**Composite scores:** NO  
**Live analytics/log connections:** NO

## Product chain

AI Visibility → Discoverability → Referral → Business Impact

## Commands

```bash
npm run test:ai-visibility-phase3c1
npm run ai-visibility:phase3c1-dry-run
node scripts/ai-visibility-phase3c1-dry-run.mjs --bounded-live --max-brands=1
```

## Key modules

| Module | Purpose |
|---|---|
| `discoverability-taxonomy.js` | Product definitions, no composite score |
| `discoverability-data-states.js` | MEASURED / CONNECTION_REQUIRED / etc. |
| `discoverability-dimensions.js` | v1 dimensions, priority pages, metrics |
| `brand-url-governance.js` | URL source priority, gaps |
| `ai-crawler-registry.js` | Governed crawler identities |
| `robots-parser.js` | Deterministic robots.txt parser |
| `public-crawl-checks.js` | HTTP, canonical, noindex, HTML checks |
| `public-check-engine.js` | Bounded public check orchestrator |
| `referral-intelligence.js` | Referrer classification |
| `business-impact.js` | Qualified actions, attribution |
| `discoverability-contracts.js` | Read contracts, capability matrix |
| `discoverability-adapters.js` | Log + analytics adapter seams |
| `discoverability-read-service.js` | UI read payloads |
| `future-discoverability.js` | Executive/Detail UI integration |

## UI integration

- **Executive Summary:** `discoverabilityBusinessImpact` compact section
- **Detailed View:** `discoverabilityBusinessImpact` modules
- Backward compat: `openAiDiscoverability` alias retained

## Next recommended phase

**PHASE_3C2_PUBLIC_DISCOVERABILITY_BASELINE**
