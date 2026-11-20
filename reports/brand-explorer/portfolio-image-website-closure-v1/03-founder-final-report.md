# Brand Explorer — Permanent Image & Website Quality Closure

Generated: 2026-11-20  
Contract: `BE_FACTORY_2_0_IMAGE_WEBSITE_CLOSURE_V1`  
PR: #491 · Research spend: **$0**

## Founder scoreboard

| Metric | Value |
|---|---:|
| Brands audited (frozen Factory 2 payloads) | 230 |
| Comparison Studio registry | **233 / 233 ALIGNED** |
| Brand websites verified (Active/Live Airtable) | **68 / 68 (0 parent-homepage issues)** |
| Brand website display rule (path-preserving label) | **SHIPPED** |
| Gallery 6/6 before → after | **111 → 150** |
| Gallery gaps before → after | **119 → 80** |
| Scenario 3/3 before → after | **110 → 159** |
| Scenario gaps before → after | **120 → 71** |
| Durable image pools written | 123 |
| Candidates patched this closure | 93 + 47 gap-pass |
| Fee-verification gaps (policy — disclosure exhibits) | **230 unchanged** |
| Fully quality-certified (fee + images + browser) | **0** (fee + remaining imagery + :8080 cert) |
| Still needing remediation (image soft-block) | **80 gallery / 71 scenario** |
| Actual :8080 browser certification | **NOT COMPLETE** (factory tooling + payloads updated; full ten-tab visual cert outstanding) |
| Research costs | **$0** |

## Permanent factory changes

1. **Brand Website** — `active-brand-website-corrections.js` + Airtable Brand Basics corrections; atelier `brandWebsiteDisplayLabel` never collapses brand paths to bare parent host; depth hydrate refuses parent homepage as `sources.brandPage`.
2. **Hotel image discovery** — `be-factory-2-hotel-image-discovery-v1.js` ($0 HTTP): harvest same-candidate assets, durable pools, official brand/hotel `og:image` / `img` CDN cues; same-brand + hotel-photography gates; soft-block when insufficient.
3. **Pool wiring** — `loadGalleryCardsFromFixture` prefers `data/brand-explorer/factory-2-0/image-pools/<slug>.json` before static fixtures.
4. **Gap assessment** — depth runner now flags `scenario_images_pending` and brand-website parent homepage.

## Remaining blockers (honest)

1. **80 brands** still lack six verified distinct hotel gallery images after free HTTP discovery (bot blocks, missing official property pages, or affinity soft-rejects). Certification blocked — **no invented photos**.
2. **71 brands** still lack three scenario images (often same discovery shortfall).
3. **Fee amounts** remain `fee_amounts_unverified_pending_disclosure_exhibit` for Factory 2.0 brands — disclosure exhibits required; never fabricate.
4. **Authenticated :8080 ten-tab visual certification** for all remediated candidates still required for `PORTFOLIO_AUDIT_COMPLETE`.
5. Production Presentation writeback remains **OFF**.

## Verdict

- `COMPARISON_STUDIO_INVENTORY: ALIGNED`
- Brand website permanent rule: **ENFORCED** (Basics + rendering + factory hydrate)
- Image factory automation: **SHIPPED** (partial portfolio fill; soft-blocks preserved)
- `PORTFOLIO_AUDIT_COMPLETE: **NO**`
