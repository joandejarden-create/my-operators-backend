# GDI — How to Read This Report V1.1

## Verdict

The Opportunities walkthrough now follows the page top-to-bottom, stays on Opportunities (never opens Commercial Priorities), and opens a real Opportunity Details drawer with ADP coachmarks above the native dialog via the Popover API.

## Final step sequence

| # | Step | Target |
|---|------|--------|
| 1 | Start with the Opportunities overview | Opportunity list |
| 2 | Select your hotel and filters | Filter toolbar |
| 3 | Understand Market Scope | All / Hotel / Competitive |
| 4 | Read the opportunity cards | First opportunity card |
| 5 | Understand opportunity readiness | Card pills / contact signals |
| 6 | Open Opportunity Details | Native drawer (auto-opened) |
| 7 | Evidence and recommended action | Drawer evidence / next step |
| 8 | Hotel Validation / Action / Outcome | Drawer feedback controls |
| 9 | Export and continue working the list | Export CSV |
| 10 | How to use GDI | Finish card |

Empty inventory (e.g. Castillo Son Vida sales-team view) shortens automatically (~7 steps) by skipping missing card/drawer targets — no crash, no forced drawer open.

## Drawer interaction approach

- Opens the existing drawer through the normal View Details click path.
- Tour root uses `popover="manual"` + `showPopover()` so coachmarks share the browser top layer with `<dialog showModal()>`, without z-index hacks.
- Demo opportunity is scored dynamically from the authorized card inventory (title, contact, revenue icons).
- Tour closes only drawers it opened; prior open state is restored on exit when applicable.
- Demo does not save validation / action / outcome fields.
- Engine waits for deferred targets (`deferTargetCheck`) so drawer steps are not skipped before open.
- Transition lock + fail-safe prevents Next/Back from abandoning mid-open drawer loads.

## Browser verification

| Check | Result |
|-------|--------|
| Bethesda Marriott top→bottom order | Pass |
| Commercial Priorities never opened | Pass |
| Drawer auto-opens at step 6 | Pass |
| Coachmarks above drawer (Popover) | Pass |
| Evidence + validation highlighted | Pass |
| Back/Next across drawer steps | Pass |
| Exit restores usable Opportunities page | Pass |
| Wyndham San Diego Bayside second hotel | Pass (demo `gdi_pe_295cfef5822e63c3`) |
| Castillo Son Vida empty inventory | Pass (shortened 7-step tour) |
| No opportunity / filter / permission mutation | Pass |

## Screenshot evidence

Under `reports/gdi-guided-report-tour-v1/screenshots/`:

- `gdi-howto-v1-1-step-overview.png`
- `gdi-howto-v1-1-step-filters.png`
- `gdi-howto-v1-1-step-card-readiness.png`
- `gdi-howto-v1-1-step-drawer-open.png`
- `gdi-howto-v1-1-step-drawer-evidence.png`
- `gdi-howto-v1-1-step-drawer-validation.png`
- `gdi-howto-v1-1-step-finish.png`
- `gdi-howto-v1-1-mobile-finish.png` (390×844)

## Mobile verification

Finish callout remains readable on a 390×844 viewport; highlight + Next/Back/Exit controls stay usable. Drawer steps still rely on drawer-body scroll for tall content.

## Tests

```bash
npm run test:gdi-guided-report-tour-v1
```

PASS — Opportunities-only V1.1 contract, Popover/drawer targets, no Commercial Priorities navigation.

## Limitations

- Callout placement on very tall drawer sections may cover part of the highlighted block on narrow viewports; drawer scroll remains available on those steps.
- Shared engine Popover path is enabled for GDI; ADP’s existing guide module is unchanged.
- Empty-inventory hotels skip card/drawer teaching steps (by design) once filters/market/export/finish targets resolve.
