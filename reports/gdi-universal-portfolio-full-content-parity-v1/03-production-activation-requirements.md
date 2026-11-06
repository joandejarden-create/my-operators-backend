# GDI Universal Portfolio Full-Content Parity — Production Activation

## What this release ships (code only)

- One canonical presentation contract (`gdi-canonical-customer-presentation-contract-v1`)
- `verifyGdiHotelFullContentParity(hotelId, options)` — hotel-scoped recursive parity
- Portfolio discovery + audit (no manual hotel shortlist)
- One-click publish **pre-pointer** full-content gate + post-activation verify → `VERIFIED` only on pass
- GDI Admin **Full Content Parity** column + Verify / View Differences actions
- CI tests: `npm run test:gdi-portfolio-full-content-parity-v1` (+ existing survival / full-content tests)
- Browser harness: `npm run gdi:portfolio-browser-full-content-parity` (requires share token map; never rotates tokens)

## What this release does **not** do without authorization

- No production deploy
- No production inventory writes / republish of Bethesda, Waterstone, or peers
- No share token mint/rotate
- No silent replacement of legacy published content

## Read-only audit result (local durable work inventory)

See `01-portfolio-audit.md`.

- Only **Spice Island Beach Resort** is on the durable published-inventory registry with schema-ready snapshots (7 / VERIFIED).
- Peer hotels with ACTIVE share tokens but **no** canonical registry snapshots are classified **NOT_VERIFIABLE** / migration candidates (`LEGACY_SHARE_WITHOUT_CANONICAL_REGISTRY`).
- They are **not** counted as production full-content PASS.

## Exact activation sequence (authorized founder only)

1. Deploy this branch to Railway (lean-stage / existing GDI release path).
2. Confirm PR #146 / SHA `7603fe138` + cert follow-up `2841abbd2` remain ancestors.
3. Preserve existing share tokens (Spice `gdisht_spice_island_mw3olaq5`; Bethesda / Waterstone unchanged).
4. Run read-only portfolio audit against production volume mount:
   ```bash
   GDI_PUBLISHED_INVENTORY_ROOT=<prod-volume> npm run gdi:portfolio-full-content-parity-audit
   ```
5. Optional live share verify (capability JWTs via env map — never commit):
   ```bash
   GDI_SHARE_TOKEN_MAP_JSON='{"recKRJjcPnb4tVDDS":"gdishare.v1...."}' \
     npm run gdi:portfolio-full-content-parity-audit -- --live-share --prod-base https://my-operators-backend-production.up.railway.app
   ```
6. Peer inventory safety (counts only; not full-content VERIFIED until migrated):
   - Bethesda Marriott: expect **31** published opportunities unchanged
   - Waterstone Resort & Marina: expect **28** published opportunities unchanged
7. Migration of legacy share hotels onto the canonical freeze path requires **separate** authorized per-hotel one-click publish with complete approved objects — never force-republish the portfolio to “create” parity.

## Success status

`GDI_UNIVERSAL_PORTFOLIO_FULL_CONTENT_PARITY_PASS` applies when every **published + verifiable** hotel passes complete production customer-facing content parity.

Unpublished and NOT_VERIFIABLE hotels must remain reported separately.
