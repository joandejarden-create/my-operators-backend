# Sheraton Guadalajara Expo — Local Founder Preview Handoff

## Exact local URL

```
http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recCAGqkERSXgiku4&preview=staged
```

Also works (package-first default; STAGED used when no CURRENT):

```
http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recCAGqkERSXgiku4
```

Live research browse (not the staged customer package):

```
http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recCAGqkERSXgiku4&live=1
```

## Server / package identity

| Item | Value |
|------|--------|
| Local server git SHA (at verification) | `ec6afcf2cde4dfefc827e2dcaad9d2cd3b213c8f` (+ uncommitted preview fixes in PR) |
| Hotel | Sheraton Guadalajara Expo (`recCAGqkERSXgiku4`) |
| Package version | `gdi_pub_recCAGqkERSXgiku4_20261113_full_e2e_v1` |
| Fingerprint | `60f2d7ef732ac9578e33e159988092b90f49ec8372b8b174c7630296240e6211` |
| Preview source | **STAGED** (not CUSTOMER_PUBLISHED) |
| Opportunity count | **62** |

## Browser verification

- All **62** cards present (`Showing 62 of 62`)
- All **62** drawers opened with content
- Fingerprint match confirmed via API
- No Mode A/B banner in normal UI
- No silent Bethesda fallback
- Screenshots: `screenshots/01-overview.png`, `02-first-drawer.png`, `03-tour.png`
- Evidence: `BROWSER_FOUNDER_PREVIEW_VERIFICATION.json`

## Protections

- Bethesda CURRENT unchanged (67 ops, distinct fingerprint)
- Sheraton remains STAGED / founder-review — **no** CUSTOMER_PUBLISHED
- No external share link issued
- Explicit `?preview=staged` fails closed with `STAGED_PUBLICATION_PREVIEW_UNAVAILABLE` if package missing
- Explicit `?hotelId=` no longer silently remaps to catalog[0]

## Restart note

If the local API returns `count: 0` with `listScope: CUSTOMER_FACING`, restart `node server.js` on this branch so it loads current STAGED package resolution.
