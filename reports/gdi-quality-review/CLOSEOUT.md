# GDI drawer / history / inventory / quality closeout

Date: 2026-10-15  
Branch: `gdi/drawer-history-inventory-quality-v1`  
Base: `origin/main` @ `fed35cc7`

## Why the prior “fixed and verified” claim did not match the user page

1. A **dirty hotfix** deploy (`ce6f4a9` / `gdi-drawer-contact-v3`) briefly served drawer CSS/JS from a non-main worktree.
2. Clean-main redeploy `fed35cc7` (`gdi-history-eligibility-v2`, `hotfixOverride: false`) restored PR **#84** / merge **`23ed5b8d`** drawer assets that were already ancestors of main — but verification was not re-done on the **same** production opportunity the user had open.
3. HCAOA still showed **occasion prose in “Why this hotel”** because `sanitizeOpportunityForCustomerPresentation` fell back `hotelOpportunityThesis` / `summaryWhyHotel` → `summaryWhat` when assessment prose was stripped — which looked like a “withheld vs 63” narrative conflict even though the score grid was already provisional-correct.

## Affected opportunity (production)

| Field | Value |
|-------|-------|
| Hotel | The Mayflower Hotel, Autograph Collection (`rec0eTEnCqlsGJJt9`) |
| Opportunity | **HCAOA Annual Meeting 2027** `gdi_opp_prospect_mv8fnmjz_832ce0cf` |
| Entry | Authenticated `/group-demand-intelligence.html?hotelId=rec0eTEnCqlsGJJt9` → View Details |
| Pre-fix assets | `dealality-gdi-ui.js?v=gdi-history-eligibility-v2` (deployed SHA `fed35cc7`) |
| Renderer | `DealalityGdiUi.intelligenceDetailHtml` → `hotelFitAssessmentSectionHtml` + `whyThisHotelOpeningHtml` |

### Score grid (already on `fed35cc7` before this correction)

- One **Overall Hotel Fit 63 (provisional)** inside the grid
- Physical / Commercial / Historical → **Needs verification** (not blank numerics)
- Evidence confidence **36 / 100**
- History footer: Last research **History unavailable** · Run **Status unavailable** · Last published **Oct 15, 2026** (Oct 29 excluded)

### Residual defect fixed in this release

- Sanitize **v2**: never fall back hotel-thesis fields to occasion summary; clear stale withheld `fitExplanation` from sales surfaces.
- Association drawer uses `whyThisHotelOpeningHtml` for Why this hotel.
- Scoped Airtable repair cleared withheld assessment prose on **7** provisional Mayflower rows (backup under `narrative-repair/backups/`, not committed).
- CSS: slightly taller score summaries / wrap so Contactability does not clip at zoom.

## Oct 29 research date

Root cause documented in `reports/gdi-history-eligibility-v2/OCT29_ROOT_CAUSE.md`: simulated **+2 weeks** dry-run weekly refresh recovered into the activity ledger. Eligibility **v2** excludes dry-run / test-clock / materially future stamps from customer history without clamping. Production Mayflower/SD after `fed35cc7`: **History unavailable** for Last Research; publication **Oct 15**.

## Inventory 35→30 / 46→38

Record-level table: [`INVENTORY_RECONCILIATION_RECORD_LEVEL.md`](./INVENTORY_RECONCILIATION_RECORD_LEVEL.md).

Summary: different **count definitions** (broad facing vs customer list gate), not deletions. All delta IDs remain in canonical store. Live SD list may show **45** when PE preferred-lodging rows re-enter visibility — still not a silent restore of unsupported rows.

## Quality commands (clean checkout)

```bash
npm ci
npm run gdi:quality:quick
# optional:
npm run gdi:quality:full
npm run test:gdi-customer-presentation-sanitize-v2
node scripts/test-gdi-drawer-hotel-fit-display-v1.mjs
node scripts/test-gdi-hotel-activity-history-eligibility-v1.mjs
```

Actual `gdi:quality:quick` on this branch: **PASS** (integrity fixtures, hotel-fit presentation, buyer-contact, sanitize-v2, drawer-fit, history-eligibility, scoped prettier).

## Remaining gaps

- San Diego live list **45** vs freeze **38** — explained as PE visibility drift; no eligibility code change in this PR.
- Share-token crypto verify still environment-dependent.
- Narrative-repair backups contain full opportunity JSON — kept local / untracked on purpose.

## Release SHAs (2026-10-15 closeout)

| Item | SHA / ID |
|------|----------|
| Merge (squash to main) | `d0c61133` (PR #92) |
| Product commit (pre-squash on branch) | `459bcee9` |
| Quality tooling commit (pre-squash) | `cc41b857` |
| Cursor rule commit (pre-squash) | `d9b0014e` |
| Deployed | `d0c61133` � Railway `8af3285a` SUCCESS � assets `gdi-drawer-history-inventory-v1` |
| Hotfix override | false |

Production HCAOA after deploy: Why this hotel = not-yet-established (no occasion/withheld copy); Overall 63 provisional once; dims Needs verification; evidence 36/100; history History unavailable / Oct 15 published.
