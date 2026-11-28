# Root cause — invisible Find Email & Phone buttons on :8080

## Diagnosis (Playwright + asset probe)

| Check | Result on canonical :8080 (`ws-gdi`) |
|---|---|
| Process | `node C:\Dev\dealality\ws-gdi\server.js` (PID was 44560) |
| Git HEAD | `6f47e1f80` — **behind** `origin/main` `5c0cb0dd5` |
| Served JS | `/js/hotel-contact-intelligence.js?v=16` — **no** `gdi-list-contact-details--with-action`, **no** `data-hci-hybrid-enrich` |
| Served CSS | `/css/hotel-contact-intelligence.css?v=18` — **no** V2 action-slot rules |
| DOM `[data-hci-contact-details-cell]` | **0** |
| DOM `[data-hci-enrich-slot]` | **0** |
| DOM `[data-hci-hybrid-enrich]` | **0** |
| Find Email & Phone buttons | **0** |

## Why emails/phones still appeared

Pre-#695 Contact Intelligence still hydrates Airtable channels into the directory. The **old** enrich path only injects an action when **both** email and phone are missing, and never as a right-aligned control inside Contact Details. So saved channels show, but the founder-approved V2 button never mounts.

## Not the primary cause (ruled out for :8080)

- CSS-hidden V2 button (V2 markup was never served)
- Clipping of a rendered V2 button (nothing to clip)
- Older renderer override of V2 on the same tip (tip never loaded)

## Secondary hardening (this PR)

On builds that *do* include V2 (#695+):

- Contact Details `td` uses `overflow: visible` so the button is not clipped by `td { overflow: hidden }`
- Enrich action slot is `flex-shrink: 0` with explicit `visibility: visible`
- Cache-bust CSS `?v=19` / JS `?v=17`
- Browser gates assert **nonzero visible boxes**, not source-only presence
