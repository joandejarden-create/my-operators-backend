# GDI Guided Tour Production Visual Regression — Root Cause

**Status:** Root cause confirmed with production DOM geometry (pre-fix).  
**Package preserved:** `gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1`  
**Fingerprint (unchanged):** `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`

## What customers saw (step 6/10)

At “Open Opportunity Details” on the Railway Bethesda share URL:

- Most of the viewport was covered by an empty dark field.
- The Opportunity Details drawer sat mid-page (upper-right fragment), not the normal right rail.
- The coachmark floated disconnected from a usable highlight target.
- Next/Back still worked — prior certifications treated that as PASS.

## Exact causal chain

1. Tour opens the Opportunity Details `<dialog>` via the normal customer path (`showModal()`), which places the dialog in the **browser top layer**.
2. Popover coachmarks cannot receive reliable clicks above a modal dialog, so the tour **demotes** the drawer: `close()` → `show()` (non-modal).
3. Leaving the top layer drops the dialog out of modal layout. Without an explicit fixed right-rail pin, the open non-modal `<dialog>` reflows as `position: absolute` with a **large positive `top`** (production measured **`y ≈ 416`** at 1440×900).
4. The guided-tour highlight uses a transparent cutout + `box-shadow: 0 0 0 9999px` scrim. Default highlight height was capped at **~40vh**.
5. With the drawer mid-page and only a short cutout, most of the viewport (including most of the drawer) sits under the scrim → **blank dark overlay**. Viewport center `elementFromPoint` resolves to `html.adp-gt-active` rather than report content.

## Production evidence (step 6, before fix)

From `00-REPRO-prod.json`:

| Measurement | Value |
|-------------|-------|
| Drawer `#gdiShareDrawer` | `x=863 y=416 w=560 h=900`, `position:absolute`, `topLayer:false`, `open:true` |
| Tour root | `z-index:10050`, full viewport |
| Center sample | `HTML.adp-gt-active` |
| Asset cache | Production HTML still served `gdi-howto-read-v1-1e` |

The blank region is **not** a separate full-screen div. It is the **tour highlight scrim** over a mis-positioned non-modal drawer + short cutout.

## Fix (shared customer UI — V1_1g)

1. **CSS** `.gdi-drawer--tour-nonmodal[open]` — pin fixed right rail (`top/right/bottom:0`, `height:100vh`, `z-index:10040`) whenever the tour demotes the dialog.
2. **Tour** — always add that class when demoting / patching `showModal` during an active tour; clear on exit.
3. **Engine** — drawer targets get near-full viewport highlight height (no 40vh cap); coachmark for tall right-rail highlights is forced into the left column with ≥28px clearance.
4. **Cache bust** — share + founder HTML load `?v=gdi-howto-read-v1-1g`.

## What was ruled out

- Not a different local vs production tour implementation (same files; production was **stale `v1-1e`**).
- Not approved package content drift (fingerprint unchanged).
- Not “Next broken” — navigation worked while layout failed.
- Not arbitrary z-index stacking alone — without fixed positioning, demoted dialog geometry is wrong regardless of z-index.
