# Leak Audit Cover Parity QA

**Date:** 2026-09-07  
**Verdict:** Cover is a text-swapped Hotel Intelligence cover template (shared BAS/HID shell).

## 1. Source template reused

| Item | Path |
|------|------|
| Cover HTML builder (SoT) | `public/js/hotel-intelligence-dossier.js` → `buildCoverHtml()` |
| Cover CSS (layout + pattern) | `public/css/brand-alignment-snapshot.css` (`.brand-alignment-snapshot .bas-cover-*`) |
| Cover CSS (screen/print fidelity) | `public/css/hotel-intelligence-dossier.css` (`.hid-cover-page`, `.hid-print-host--flow .hid-cover-page`) |
| Leak Audit shell | `public/adp-leak-audit-report.html` |
| Leak Audit renderer (tokens only) | `public/js/adp-leak-audit-report.js` |

## 2. Shared classes / components reused

Exact HID cover class list:

- `bas-cover-page bas-book-page-surface bas-avoid-break hid-cover-page`
- `bas-cover-geometric`
- `bas-cover-confidential`
- `bas-cover-block`
- `bas-cover-doc-type`
- `bas-cover-title`
- `bas-cover-location`
- `bas-cover-accent-line`
- `bas-cover-sub`
- `bas-cover-date` (×2 metadata lines)
- `bas-cover-disclaimer`
- `bas-cover-hero` → `bas-cover-logo-block` → `bas-cover-logo-img`
- `hid-cover-page__foot` → `hid-chapter__foot-left` / `hid-chapter__foot-right`

Host activation classes (required for BAS scoped CSS):

- `brand-alignment-snapshot`
- `hotel-intelligence-dossier`
- `hid-reader` (body)
- `hid-print-host hid-print-host--flow` (`#alaReport`)
- `data-cover-template="hotel-intelligence-dossier"`
- `data-ala-cover-variant="intelligence"`

## 3. Text tokens replaced (HID → Leak Audit)

| HID field | Leak Audit token / copy |
|-----------|-------------------------|
| Top confidential | `Dealality AI Demand Positioning · Confidential · For recipient only` |
| Doc type | `Limited AI Demand Leak Audit` |
| Title | `Cambridge Beaches Resort & Spa` (same `bas-cover-title` + `max-width: 14ch` wrap) |
| Location | `Sandys Parish, Bermuda` |
| Subline | `AI Visibility · Demand Territories · Competitor Displacement · Action Priorities` |
| Meta line 1 | `Providers 4 · Sample findings 12 · Action items 3` |
| Meta line 2 | `Generated September 7, 2026 · Limited diagnostic` |
| Disclaimer | Limited diagnostic / ADP reading logic / no causation copy |
| Foot left | `Confidential · For recipient only · © 2026 Dealality` |
| Foot right | `Page 1 of 3` |

## 4. Layout issues fixed

1. Removed leak-audit-only cover CSS (custom padding, fixed card height, left-biased logo, restyled foot).
2. Linked `hotel-intelligence-dossier.css` and activated BAS/HID host classes so geometric pattern + typography tokens apply.
3. Restored absolute lower-left disclaimer + lower-right logo hero from BAS.
4. Kept DOM cover footer visible for Leak Audit PDF (HID print hides foot for Playwright chrome; Leak Audit overrides that one rule only).
5. Forced cover `min-height: var(--hid-page-h, 1123px)` + `height: auto` so BAS `height: 100%` cannot collapse the sheet.
6. Cover page-break remains; pages 2–3 stay on the ADP navy diagnostic surface.

## 5. Measured screen QA (2026-09-07)

Against `/adp-leak-audit/sample` with HID CSS loaded:

| Check | Result |
|-------|--------|
| `hotel-intelligence-dossier.css` linked | yes |
| Cover height | **1123px** (`--hid-page-h`) |
| Geometric layer present | `.bas-cover-geometric` |
| Logo lower-right | ~41px inset from right/bottom, width 140px |
| Foot visible | `Confidential · For recipient only · © 2026 Dealality` + `Page 1 of 3` |
| Meta copy | `Providers 4 · Sample findings 12 · Action items 3` |
| Custom cover classes absent | no `drs-cover-shell`, no `ala-adp-cover` |

## 6. Remaining visual differences (intentional / known)

1. **Footer visibility in print:** HID PDF uses Playwright footer and hides `.hid-cover-page__foot`. Leak Audit shows the same DOM foot for print/PDF because this free diagnostic does not use the Playwright footer pipeline.
2. **Pages 2–3:** Still ADP navy diagnostic pages (out of scope for this cover-only pass).
3. **Portfolio sample** (`/adp-leak-audit/sample-portfolio`) is not on this unified HID cover shell yet.

## 7. How to verify

1. Hard-refresh `/adp-leak-audit/sample` (CSS `?v=leak5` / `?v=6`).
2. Compare side-by-side with a Hotel Intelligence dossier cover (same geometric navy, title scale, logo lower-right, disclaimer lower-left, foot).
3. Confirm cover fills the A4 host width at 1123px sheet height and is not a centered inner card.
