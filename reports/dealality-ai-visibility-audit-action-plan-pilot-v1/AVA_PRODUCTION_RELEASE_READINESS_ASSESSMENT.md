# AVA Production Release Readiness Assessment

Generated: 2026-11-05T21:53:16.463Z

**Readiness only — no deployment or publication performed.**

## Code revision

| Item | Value |
|---|---|
| Branch | `feat/gdi-spice-published-inventory-registry-v1` |
| Commit | `e2ae6c7d3b8a98dfd006d8660aba7e4764efe672` |
| Short | `e2ae6c7d3` |
| Dirty working tree | **true** |

Uncommitted paths (truncated):

```
M .railwayignore
 M AGENTS.md
 M api/admin-adp-monthly-reviews.js
 M api/admin-external-client-links.js
 M api/ai-demand-positioning.js
 M api/group-demand-intelligence.js
 M api/me.js
 M config/client-share/adp-share-registry/active-tokens.json
 M config/client-share/adp-share-registry/revoked-token-ids.json
 M config/client-share/gdi-share-registry/active-tokens.json
 M config/client-share/gdi-share-registry/revoked-token-ids.json
 M config/client-share/production-share-contract-tokens.json
 M config/group-demand-intelligence/hotels/recLuxvwwxID7U2B8.json
 M config/group-demand-intelligence/hotels/recgMYovrrZDJMqzX.json
 M config/production-required-assets.json
 M data/ai-demand-positioning/published/adp_ac_hotel_a_coruna/manifest.json
 M data/ai-demand-positioning/published/adp_bethesda_marriott/evidence-adp_period_adp_bethesda_marriott_20261002142154_62428a.json
 M data/ai-demand-positioning/published/adp_bethesda_marriott/manifest.json
 M data/ai-demand-positioning/published/adp_bethesda_marriott/report-adp_period_adp_bethesda_marriott_20261002142154_62428a.json
 M data/ai-demand-positioning/published/adp_cambridge_beaches_bermuda/evidence-adp_period_adp_cambridge_beaches_bermuda_20260903010829_044437.json
 M data/ai-demand-positioning/published/adp_cambridge_beaches_bermuda/manifest.json
 M data/ai-demand-positioning/published/adp_cambridge_beaches_bermuda/report-adp_period_adp_cambridge_beaches_bermuda_20260903010829_044437.json
 M data/ai-demand-positioning/published/adp_casas_del_xvi/manifest.json
 M data/ai-demand-positioning/published/adp_castillo_hotel_son_vida/manifest.json
 M data/ai-demand-positioning/published/adp_faranda_collection_bogota/manifest.json
 M data/ai-demand-positioning/published/adp_hilton_times_square/manifest.json
 M data/ai-demand-positioning/published/adp_hotel_caribe_faranda_grand/manifest.json
 M data/ai-demand-positioning/published/adp_hotel_phillips_kansas_city/evidence-adp_period_adp_hotel_phillips_kansas_city_20260903003324_88084d.json
 M data/ai-demand-positioning/published/adp_hotel_phillips_kansas_city/manifest.json
 M data/ai-demand-positioning/published/adp_hotel_phillips_kansas_city/report-adp_period_adp_hotel_phillips_kansas_city_20260903003324_88084d.json
 M data/ai-demand-positioning/published/adp_jw_marriott_monterrey_valle/manifest.json
 M data/ai-demand-positioning/published/adp_jw_marriott_santo_domingo/manifest.json
 M data/ai-demand-positioning/published/adp_mayflower_hotel_washington_dc/evidence-adp_period_adp_mayflower_hotel_washington_dc_20261013213022_f9bdd0.json
 M data/ai-demand-positioning/published/adp_mayflower_hotel_washington_dc/manifest.json
 M data/ai-demand-positioning/published/adp_mayflower_hotel_washington_dc/report-adp_period_adp_mayflower_hotel_washington_dc_20261013213022_f9bdd0.json
 M data/ai-demand-positioning/published/adp_radisson_santo_domingo/manifest.json
 M data/ai-demand-positioning/published/adp_renaissance_times_square/evidence-adp_period_adp_renaissance_times_square_20260902235427_7d74ca.json
 M data/ai-demand-positioning/published/adp_renaissance_times_square/manifest.json
 M data/ai-demand-positioning/published/adp_renaissance_times_square/report-adp_period_adp_renaissance_times_square_20260902235427_7d74ca.json
 M data/ai-demand-positioning/published/adp_sheraton_mallorca_arabella_golf/manifest.json
 M data/ai-demand-positioning/published/adp_spice_island_beach_resort/manifest.json
 M data/ai-demand-positioning/published/adp_waterstone_boca_raton/manifest.json
 M data/dealality-report-archive/index.json
 M data/group-demand-intelligence/hotels/recG66DQJKP2c0UNh/opportunities.json
 M data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/feedback.json
 M data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json
 M data/group-demand-intelligence/hotels/recgMYovrrZDJMqzX/opportunities.json
 M data/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities.json
 M fixtures/ai-demand-positioning/ac-hotel-a-coruna-property-profile.json
 M fixtures/ai-demand-positioning/bethesda-marriott-property-profile.json
 M fixtures/ai-demand-positioning/cambridge-beaches-bermuda-property-profile.json
 M fixtures/ai-demand-positioning/casas-del-xvi-property-profile.json
 M fixtures/ai-demand-positioning/castillo-hotel-son-vida-property-profile.json
 M fixtures/ai-demand-positioning/census-links-v1.json
 M fixtures/ai-demand-positioning/faranda-collection-bogota-property-profile.json
 M fixtures/ai-demand-positioning/hilton-times-square-property-profile.json
 M fixtures/ai-demand-positioning/hotel-caribe-faranda-grand-property-profile.json
 M fixtures/ai-demand-positioning/hotel-phillips-kansas-city-property-profile.json
 M fixtures/ai-demand-positioning/jw-marriott-monterrey-valle-property-profile.json
 M fixtures/ai-demand-positioning/jw-marriott-santo-domingo-property-profile.json
 M fixtures/ai-demand-positioning/mayflower-hotel-washington-dc-property-profile.json
 M fixtures/ai-demand-positioning/nobu-london-portman-property-profile.json
 M fixtures/ai-demand-positioning/now-now-noho-property-profile.json
 D fixtures/ai-demand-positioning/published/adp_smoke_test_synthetic/evidence-adp_period_smoke_synthetic_001.json
 D fixtures/ai-demand-positioning/published/adp_smoke_test_synthetic/manifest.json
 D fixtures/ai-demand-positioning/published/adp_smoke_test_synthetic/report-adp_period_smoke_synthetic_001.json
 M fixtures/ai-demand-positioning/radisson-santo-domingo-property-profile.json
 M fixtures/ai-demand-positioning/renaissance-times-square-property-profile.json
 M fixtures/ai-demand-positioning/sheraton-mallorca-arabella-golf-property-profile.json
 M fixtures/ai-demand-positioning/spice-island-beach-resort-property-profile.json
 M fixtures/ai-demand-positioning/st-regis-cap-cana-property-profile.json
 M fixtures/ai-demand-positioning/st-regis-mexico-city-property-profile.json
 M fixtures/ai-demand-positioning/w-rome-property-profile.json
 M fixtures/ai-demand-positioning/waterstone-property-profile.json
 M fixtures/ai-demand-positioning/westin-grand-munchen-property-profile.json
 M fixtures/ai-demand-positioning/westin-monterrey-valle-property-profile.json
 M fixtures/ai-demand-positioning/westin-tampa-waterside-property-profile.json
 M fixtures/ai-demand-positioning/wyndham-san-diego-bayside-property-profile.json
 M fixtures/ai-demand-positioning/yotel-geneva-lake-property-profile.json
 M fixtures/hotel-census/adp-gdi-hotel-alias-map-v1.json
 M lib/admin/report-external-client-links-v1.js
 M lib/ai-demand-positioning/airtable-published-report.js
 M lib/ai-demand-positioning/brand-portfolio/brand-portfolio-affiliation-mapping-v1.js
 M lib/ai-demand-positioning/brand-portfolio/brand-portfolio-peer-set-v1.js
 M lib/ai-demand-positioning/brand-portfolio/build-bpp-customer-ready-payload-v1.js
 M lib/ai-demand-positioning/brand-portfolio/build-brand-portfolio-position-payload-v1.js
 M lib/ai-demand-positioning/budget/authorization-ledger-v1.js
 M lib/ai-demand-positioning/census-link-registry.js
 M lib/ai-demand-positioning/customer/canonical-presence-per-observation-v1.js
 M lib/ai-demand-positioning/customer/competitive-ranking-overall-view-v1.js
 M lib/ai-demand-positioning/customer/customer-entity-resolution-v1.js
 M lib/ai-demand-positioning/customer/missing-evidence-v1.js
 M lib/ai-demand-positioning/customer/owner-payload.js
 M lib/ai-demand-positioning/customer/positive-evidence-v1.js
 M lib/ai-demand-positioning/customer/resolve-displacement-evidence-v1.js
 M lib/ai-demand-positioning/data-model.js
 M lib/ai-demand-positioning/execution/multi-provider-runner.js
 M lib/ai-demand-positioning/execution/response-parser.js
 M lib/ai-demand-positioning/execution/shared-first-property-baseline-period-001-v1.js
 M lib/ai-demand-positioning/executive-read-v3/section-composer-v3.js
```

## Artifact posture

| Channel | Status |
|---|---|
| Staged V4 portfolio | 38 PDFs under `portfolio-v4-local-staging/` |
| Final manifest | `AVA_38_HOTEL_FINAL_MANIFEST.json` (SHA-256 per hotel) |
| Published channel | Unchanged (true) |
| Share tokens / approvals | Not modified |

## Controlled release plan (future authorization)

1. Commit **AVA-only** changes (V4 renderer/CSS, DA customer copy, CQA V4 alignment, validation scripts/reports). Exclude unrelated ADP/GDI diffs on this branch if present.
2. Tag certified commit as `AVA_V4_PORTFOLIO_CERTIFIED`.
3. Backup production `data/ava-data/pdfs/*/published.pdf` + meta before any write.
4. Deploy that exact revision.
5. Transfer 38 staged V4 PDFs to durable storage / Admin **draft** channel; verify SHA-256 vs final manifest.
6. Keep **published** customer PDFs unchanged until separate founder publication approval.
7. Rollback: restore published backups + previous code revision.

## Explicitly not done

Production deploy · Customer publish · Share-token rotation · Approval changes · Paid research · ADP/GDI measurement changes
