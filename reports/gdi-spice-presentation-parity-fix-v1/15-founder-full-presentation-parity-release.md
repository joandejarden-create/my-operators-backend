# GDI Universal Full Presentation Parity — Release Auth Package

**Status intent:** `GDI_UNIVERSAL_FULL_PRESENTATION_PARITY_READY_FOR_DEPLOY_AUTH`  
**Live production still diverges until authorized deploy + republish.**  
**Do not overwrite Spice published snapshot without founder authorization.**

## A. Root cause

Prior `PRODUCTION_GDI_FULL_PARITY_PASS` compared a **tile/drawer field subset** (priority, sales action, quick filters, primary/secondary names in API DTOs). That is now explicitly `TILE_DRAWER_FIELD_SUBSET_PASS` — not full parity.

Actual customer-visible defects came from **competing derivation after publish**:

1. **Publish freeze used list projection**, which omits `scalingTwoContactPresentation`. Production share therefore fell into the client fallback renderer.
2. **`looksLikePersonNameLocal` was ASCII-only** — `A'yesha Gaetán` failed on accented `á`, so the UI nullified the verified primary and showed *Named event contact not publicly identified yet* while still asking to be directed to A'yesha.
3. **`share-app.js` passed `whoContactHtml: whoShouldSalesContactHtml`**, causing recursive re-entry that appended **Adrienne Brown twice**.
4. Locked snaps did not freeze the **complete** customer presentation object (two-contact slots, narrative advantages/limitations, presentation hash).

## B. A'yesha correction (approved local / freeze dry-run)

| Field | Corrected value |
|---|---|
| Classification | `ACCOUNT_DEVELOPMENT` — intermediary relationship development; no verified live couple brief / RFP / room block |
| Demand stage | `RELATIONSHIP_DEVELOPMENT` |
| Priority | `WATCHLIST` |
| Sales action | `Engage Planner` |
| Primary contact | **A'yesha Gaetán** — Certified Wedding Planner and Travel Designer (named commercial entry point; email/authority not verified) |
| Secondary contact | **Adrienne Brown** once — Owner and Lead Travel Advisor, Breathless Getaways |
| Sourcing | Partnership / account-development path; no confirmed hotel-sourcing programme |
| Recommended action | Develop intermediary relationship first; determine whether an active Grenada brief exists; **no production forecast / inventory hold** until a live programme exists |
| Presentation hash (freeze) | `d5de6559a5a94b3913e0b32a1788664c0719f1a74b6b394d55daf1ae2658b82d` |

## C. Seven-opportunity freeze dry-run (local → frozen object)

All seven candidates `ok` with integrity pass and frozen `scalingTwoContactPresentation`. See `14-freeze-dry-run-seven.json`.

Live local-vs-prod HTTP cert remains **FAIL** on all seven until republish (prod missing frozen two-contact + some narrative fields). See `13-full-presentation-parity-cert.json`.

## D. Universal implementation (no hotel fork)

| Module | Change |
|---|---|
| `dealality-gdi-ui.js` | Unicode `looksLikePersonNameLocal`; block whoFn self-recursion |
| `share-app.js` | `whoContactHtml: null` |
| `gdi-customer-presentation-integrity-v1.js` | Dedupe + blocking contradiction gates |
| `gdi-full-presentation-parity-v1.js` | Full-object hash + compare; subset label |
| `gdi-one-click-hotel-publish-v1.js` | Detail project → integrity → freeze complete object + rebuild two-contact |
| `gdi-customer-facing-published-snapshot-v1.js` | Persist two-contact + contactName/role |
| `full-e2e-publication-quality-v1.js` | `requireFullPresentationIntegrity` |
| `full-e2e-contract-v1.js` | Lesson `full_presentation_object_parity` |

**Competing paths eliminated:** share whoFn recursion; ASCII name blanking; list-only freeze omitting two-contact; subset PASS labeled as full.

## E. Production readiness

### Required before founder auth

- [x] Unit: `npm run test:gdi-full-presentation-parity-v1`
- [x] Unit: `npm run test:gdi-locked-published-presentation-parity-v1`
- [x] Freeze dry-run seven: `14-freeze-dry-run-seven.json` ok
- [ ] Clean PR merge + deploy SHA
- [ ] Founder authorization token for deploy + Spice republish

### Publication replacement (only after founder auth)

1. Deploy merged SHA to Railway production.
2. Backup current Spice volume snapshots (`adp-durable-published`).
3. Dry-run one-click publish from approved local seven.
4. Apply publish with same share token `gdishare…mw3olaq5` / hotel `recKRJjcPnb4tVDDS`.
5. Re-run `npm run gdi:spice-full-presentation-parity-cert` — require `FULL_PARITY_PASS` × 7.
6. Browser verify A'yesha Who to Contact: primary A'yesha once, Adrienne once, no unidentified ceiling.
7. Confirm Bethesda / Waterstone inventories unchanged.

### Rollback

1. Restore volume backup from step 2.
2. Redeploy previous Railway revision if UI regression.
3. Share token unchanged; customer URL preserved.

### Explicitly not done in this package

- No production write / republish
- No paid research
- No ADP / AVA changes
- No Bethesda / Waterstone inventory changes
