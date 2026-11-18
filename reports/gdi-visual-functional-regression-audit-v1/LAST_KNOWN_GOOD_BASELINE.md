# Last known good GDI visual baseline

## Authoritative reference

| Item | Value |
|---|---|
| Hotel reference | Sheraton Guadalajara Expo (`recCAGqkERSXgiku4`) |
| Historical repair | SVG revenue-stream repair in `reports/gdi-sheraton-need-dates-svg-cmi-v1/` |
| Commit family | `6b78c8f81` / package attach restore path |
| STAGED visual contract | Gold active (`#fdb52a` / `--secondary--color-5`) vs muted gray |
| Expected Guadalajara counts | **43** guestroom-active · **25** catering-active |

## Visual system (not a redesign)

CSS in `public/css/group-demand-intelligence.css` already encodes the design:

- `.gdi-revenue-stream-icon--active` → `color: var(--secondary--color-5)` (`#fdb52a`)
- `.gdi-revenue-stream-icon--muted` → muted gray
- Need-date calendar states keep distinct colors (matches / potential / not-assessed / no-match)

The regression was **not** removal of gold CSS tokens. Active icons were never being classified as `active` on Research Preview for class+sales-key inventories whose thesis prose had been scrubbed.

## Surfaces that must share the same icon meaning

1. Research Preview (live FS mirror / forceLive)
2. Staged Preview (immutable PACKAGE + read-time attach)
3. Published Inventory (locked package bytes)

Authorization differences remain legitimate. Icon color meaning must not diverge.
