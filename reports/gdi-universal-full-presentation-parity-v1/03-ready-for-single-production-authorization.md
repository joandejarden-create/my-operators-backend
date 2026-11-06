# READY_FOR_SINGLE_PRODUCTION_AUTHORIZATION

## Release candidate

| Item | Value |
|---|---|
| PR | https://github.com/joandejarden-create/my-operators-backend/pull/146 |
| Branch | `fix/gdi-approved-presentation-css` |
| Status gate | `READY_FOR_SINGLE_PRODUCTION_AUTHORIZATION` |
| Production write | **NOT executed** — awaiting one founder authorization |

Exact deploy SHA is the PR HEAD after the full-content parity commit lands (do not assume `a9dffc47` if HEAD advanced).

## Engineering gates (passed)

- Recursive full-content schema discovery from approved local detail objects (2430 paths across Spice seven).
- Freeze round-trip: approved local presentation object ↔ frozen `approvedCustomerFacingDetail` — **7/7 PASS**.
- Hydrate locked snapshot → same approved object — **7/7 PASS**.
- A'yesha: ACCOUNT_DEVELOPMENT, Engage Planner, WATCHLIST, primary A'yesha Gaetán once, Adrienne Brown once.
- CARPHA: Contact Buyer / Medium; Savor: Build Account / Medium.
- Inventory: 7 opportunities, 1 Sales Ready, 6 Account Development, Medium 2, Watch 5.
- Unit: `test:gdi-full-content-parity-v1`, prior locked/presentation parity tests.

## Live production (not yet)

Live share still fails full-content compare until authorized deploy + republish (missing frozen complete object / two-contact on current volume snaps).

## Single authorization scope

One founder authorization covering:

1. Merge/deploy PR #146 exact HEAD SHA to Railway production.
2. Backup production GDI registry + Spice snapshots.
3. Publish the seven complete locked Spice opportunities from approved local.
4. Preserve share token `gdisht_spice_island_mw3olaq5` / existing customer URL.
5. Re-run `npm run gdi:universal-100-percent-full-content-parity-cert` until `GDI_UNIVERSAL_100_PERCENT_FULL_CONTENT_PARITY_PASS`.
6. Browser-verify all seven tiles, drawers, filters, CSV; desktop + mobile.
7. Confirm Bethesda and Waterstone inventories unchanged.
8. Rollback volume backup + prior Railway revision on critical failure.

Excluded: paid research, ADP/AVA, external customer send, Bethesda/Waterstone content changes.

## Permanent standard

RESEARCH → REVIEW → APPROVE COMPLETE PRESENTATION OBJECT → PUBLISH IMMUTABLE OBJECT (`approvedCustomerFacingDetail`) → AUTOMATIC FULL CONTENT VERIFICATION → EXISTING CUSTOMER LINK UPDATED.

No subset field PASS may be labeled 100% full-content parity.
