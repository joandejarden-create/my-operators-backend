# AI Admin table presentation — More menu layering fix

## Root cause

Sticky Actions cells use `position: sticky; right: 0; z-index: 3`, so **each cell is its own stacking context**. The More menu lived inside that cell as `position: absolute; z-index: 40`. Raising the menu z-index could not escape the cell context, so later sticky Actions cells painted over the open menu.

## Shared fix

| Change | Detail |
|--------|--------|
| Portal menu | My Deals–style `position: fixed` menu appended to `document.body` (`openMoreMenu` / `closeMoreMenu` in `admin-ai-demand-table.js`) |
| Positioning | Anchored to trigger; flips upward near viewport bottom; clamped horizontally; repositions on scroll/resize |
| Interaction | One menu at a time; Escape + outside click close; focus returns to trigger; keyboard arrow navigation on items |
| Row height | Trigger is a button (not `<details>`), so opening does not change row height or select the row |
| Actions | Icon primary controls + vertical-ellipsis More (tooltips + `aria-label`); secondary actions in the portal menu |
| Handlers | Menu item clicks proxy through the row `.ada-row-actions` wrap so existing `data-act` handlers keep working |
| Copy | “Select this page” replaces “page select ≠ all filtered” |
| Metrics | Ready/Watch / archive notes moved into compact `<details class="ada-metric-disclosure">` |

Applied via shared `DealalityAiAdminTable.actionsHtml` on **AI Demand Reviews**, **GDI Reports**, and **Report Archive**. **ADP Action Plan** does not use row More menus; its preview table is wrapped in `ada-table-shell` to contain horizontal overflow.

## Verification

| Check | Result |
|-------|--------|
| Unit | `node scripts/test-admin-ai-demand-table-v1.mjs` PASS |
| Browser | `node scripts/playwright-ai-admin-more-menu-layering-v1.mjs` PASS — `elementFromPoint` hits menu; item click fires row act |
| Viewports | First / middle / last rows; horizontal + vertical scroll; narrow; ~150–200% scale |
| Before/after | `before-open-menu-obscured.png` vs `after-open-menu-unobscured.png` (+ last/narrow/zoom shots) |
| Scope | No GDI research, qualification, Airtable writes, or count changes |

## Release references

| Item | Value |
|------|-------|
| Branch | `gdi/ai-admin-table-menu-layering` |
| PR | [#104](https://github.com/joandejarden-create/my-operators-backend/pull/104) merged |
| Deploy SHA | `8148eed9618f8f43326dbe3564c157b57de32edd` |
| Railway | `1371c41d-764c-4664-9184-e70810bd4a9d` SUCCESS |
| Local `:8080` | Served `admin-ai-demand-table.js` includes `openMoreMenu` / `ada-more-trigger` |
| Production asset | `https://…/js/admin-ai-demand-table.js` — `hasPortal=true`, `hasTrigger=true`, no legacy `ada-more__menu` |

## Remaining verification blocker

Authenticated production AI Admin **interactive** open-menu screenshot (live Reviews/GDI tab with Memberstack admin session) was not captured in this pass. Fixture Playwright proof + production static asset SHA alignment confirm the released implementation; an authenticated browser open-menu shot remains a follow-up if required for visual sign-off.
