# Brand Alignment vs Operator Alignment — print & cover parity

Reference deal: `recIeGRZP21udmTnt`. Cache bust: `?v=oas-print-flatten` (standalone + My Deals).

## What was wrong (root cause)

OAS diverged from BAS in three ways that broke PDF/cover:

1. **Separate print DOM** — `buildPrintHtml()` + `renderPrintCover()` built `oas-print-cover-page` instead of cloning the on-screen book.
2. **OAS-only print CSS** — fixed `273mm` box, absolute footer band, and `oas-print-flow` overrides fought `brand-alignment-snapshot.css` (blank page 2, shifted cover text).
3. **Screen cover** — same HTML as BAS, but print hacks and non-clone path meant fixes never matched what you saw in the iframe.

BAS iframe is correct because it only uses **clone + shared BAS print CSS**.

## Side-by-side comparison

| Area | Brand Alignment (correct) | Operator Alignment (now) |
|------|---------------------------|---------------------------|
| Root classes | `brand-alignment-snapshot` (+ embed/full) | `operator-alignment-snapshot brand-alignment-snapshot` (+ same flags) |
| Cover HTML | `bas-cover-page`, `bas-cover-block`, `bas-cover-disclaimer`, `bas-cover-hero` + CDN logo | **Same structure** (only doc type string differs) |
| Book | 3× `bas-book-page` in `bas-book-viewport` | **Same** |
| `printSnapshot()` | `cloneNode(true)` → `#bas-print-host` → `bas-print-active` → `print()` | **Same** (no `buildPrintHtml`, no page flatten) |
| Print CSS for cover/book | `brand-alignment-snapshot.css` `@media print` | **Same** (OAS print block = content-only: detail titles, hide extra companies) |
| Logo | `DEALALITY_LOGO_URL` CDN PNG | **Same constant** |
| OAS-only | — | Narrative/detail copy, company cards, light `oas-brief-card` on white pages |

## Cover HTML (identical shape)

Both use one `<section class="bas-cover-page …">` with:

- `bas-cover-confidential`
- `bas-cover-block` (title stack, centered via `flex: 1; justify-content: center`)
- `bas-cover-disclaimer` (absolute bottom-left)
- `bas-cover-hero` + `bas-cover-logo-img`

## Print pipeline (OAS)

```
User clicks Print
  → cloneNode(true)
  → flattenOasBookForPrint() — removes flip-book wrappers around narrative/detail
     (those wrappers caused blank PDF page 2 in Chrome; BAS cover is shorter so it did not show)
  → class oas-print-flattened + BAS print CSS
  → #bas-print-host → window.print()
```

Do **not** reintroduce: `buildPrintHtml`, `renderPrintCover`, `oas-print-cover-page`, or extra `bas-book-page` wrappers in the print clone.

## How to verify

1. Hard refresh OAS: `operator-alignment-snapshot.html?dealId=recIeGRZP21udmTnt` — Network must show `?v=oas-cover-onepage`.
2. **Screen:** cover hero text vertically centered in the book viewport (same as BAS iframe).
3. **Print:** Headers/footers **off**, Background graphics **on**.
4. **PDF:** Page 1 = full cover (title + disclaimer + logo); page 2 = narrative (no blank sheet); no duplicate disclaimer page.

## Remaining OAS-only print tweaks (intentional)

- **One-page cover (print only):** OAS location is often longer than BAS; Chrome splits `position:absolute` disclaimer to page 2. OAS uses in-flow flex footer on `.bas-cover-page` (273mm box) — screen still uses BAS absolute layout.
- Cover location line uses same dedupe as BAS (`market` contains `country` → show market only).
- Operator detail card title/meta sizing on white technical page
- Hide `oas-company-card--limited-extra` beyond print limits
- Light paper backgrounds for OAS summary/deal sections (does not affect cover)
