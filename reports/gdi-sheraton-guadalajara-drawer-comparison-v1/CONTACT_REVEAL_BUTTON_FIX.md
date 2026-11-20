# FIX — Inactive Find Email & Phone in comparison preview

**Status:** READY FOR FOUNDER VISUAL REVIEW  
**PR:** #462 OPEN — no merge / deploy / live Surfe spend without authorization

## Root cause

The contact-card renderer correctly emitted `[data-gdi-contact-cta]` buttons, but the **click handler lived only on the main GDI page** (`app.js` → `#gdiDrawerBody`).

The Sheraton side-by-side page loaded `dealality-gdi-ui.js` + `drawer-comparison-app.js` only — **no enrichment event binding**. Clicks were inert HTML.

Not an inert snapshot of a different renderer; same `twoContactSlotHtml`, missing initialization.

## Fix (one shared workflow)

| File | Change |
| --- | --- |
| `public/js/group-demand-intelligence/gdi-contact-reveal-client-v1.js` | **New** canonical client: bind clicks, mock/live fetch, toast, session-only card update, L/R isolation via `data-gdi-cmp-panel` |
| `public/js/group-demand-intelligence/app.js` | Use shared binder (removed duplicate inline handler) |
| `public/js/group-demand-intelligence/drawer-comparison-app.js` | Bind shared client on `#gdiCmpPanels` + Dealality toast |
| `public/group-demand-intelligence.html` | Load reveal client script |
| `public/group-demand-intelligence-drawer-comparison.html` | Load reveal client; panel attrs `before`/`after`; cache `v11` |
| `api/group-demand-intelligence.js` | Resolve comparison-inventory opportunities; allow local mock when worktree + `useMock` (never spends Surfe) |

## Behavior

1. Verified public channels remain preferred (RIGHT EUROGUSS already shows recovered email/phone → no Surfe CTA).
2. LEFT still shows Find Email & Phone when channels missing.
3. Click → canonical `/contacts/reveal` with opportunity id + person name.
4. Mock: `?gdiContactRevealMock=1` on worktree server (or `GDI_CONTACT_REVEAL_MOCK=1`) — **no paid calls**.
5. Live Surfe still requires signed-in user + `authorizeLiveProvider` — not enabled in this smoke.
6. Toast: “Retrieving contact details…”
7. Results update **only the clicked card** (LEFT mock did not mutate RIGHT public channels).
8. Surfe/mock PII remains session-only (`persisted: false`).

## Browser smoke (mocked)

URL:  
http://localhost:8095/group-demand-intelligence-drawer-comparison.html?hotelId=recCAGqkERSXgiku4&opportunityId=paid3_565ce4588e6d&gdiContactRevealMock=1&v=cmp11reveal

Observed:

- Button responds
- LEFT shows `alex.buyer@fixture.example` + `+1-202-555-0199` (session only)
- RIGHT keeps `gissella.palma@nmna.us` (public recovery) — no cross-mutation
- Screenshot: `screenshots/07-contact-reveal-mock-left.png` (copy if present)

## Live Surfe

**Not run.** Requires separate founder authorization before spending credits.

## STOP

Awaiting founder visual review. No merge, deploy, or live paid enrichment without approval.
