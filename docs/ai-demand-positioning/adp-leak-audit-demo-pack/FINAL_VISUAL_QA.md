# AI Demand Leak Audit — Final Visual QA

Date: 2026-09-07  
Surface: `/adp-leak-audit/sample` (shared renderer also used by `/adp-leak-audit/:reportId`)

## Page 1 — Cover

- Uses Hotel Intelligence / BAS cover shell (`bas-cover-page` + `hid-cover-page` + `bas-cover-geometric`).
- Print CSS locks cover to **297mm × 210mm** with `@page { margin: 0 }` so the navy geometric fills the PDF page (no white gap below).
- Logo remains lower-right (`bas-cover-hero` / `bas-cover-logo-img`).
- Footer (`hid-cover-page__foot`) forced visible: `Confidential · For recipient only · © 2026 Dealality` + **Page 1 of 3**.
- Removed `adp-mr-pdf-host` class so monthly-review CSS can no longer hide the cover foot or force a white host background.

## Page 2 — Executive Diagnostic

- Print helper line only: `Need help reading this? Use the How to Read guide in the web report.`
- Sections: Executive Summary → Executive Signal → Demand Area to Review → Competitors Showing Up Instead → Supporting Evidence + Scenarios Monitored.
- PDF contrast: body/card text forced to near-white (`#d1dbf9` / `#ffffff`) with `opacity: 1 !important` on navy `#080f25` / `#101935` surfaces. DRS paper ink (`#101935`) overridden on action/evidence cards.
- Competitors: two ADP cards (Rosewood, The Reefs) — Count / Demand area / What this may mean. No rank chrome, no displacement jargon, no blue body links.
- Supporting Evidence: three static cards; web-only single `View example` per card (hidden in PDF).
- Scenarios Monitored: ADP-style `aiv-kpi` card (`60 × 4 Providers`) beside evidence.
- Footer: **Page 2 of 3**.

## Page 3 — Action + Conversion

- Title: Recommended Dealality Action Items  
- Subtitle: Practical next steps Dealality can help prepare…
- Action labels: **DEALALITY CAN HELP PREPARE** (not “Dealality prepares”).
- Who Does the Work:
  - Dealality can help prepare
  - Hotel/operator approves
  - Hotel / Agency publishes → `Website · OTAs · Google Business Profile · TripAdvisor` (single short line)
  - Dealality monitors
- Next Step CTA text forced readable in PDF.
- Footer: **Page 3 of 3**.

## Web-only behaviors

- How to Read: ADP guided modal button `How to read this report` → 8 steps in report order.
- KPI info icons: `#aiv-info-icon` sprite + `.info-tooltip.aiv-col-info`; click opens tooltip; suppressed in PDF.
- Evidence drawer: ADP `dialog.aiv-drawer` styling; client-safe only.

## Gates

```bash
npm run test:adp-leak-audit-pdf-contrast-v1
npm run test:adp-leak-audit-pdf-three-page-v1
npm run test:adp-leak-audit-how-to-read-guide-v1
npm run test:adp-leak-audit-evidence-links-v1
npm run test:adp-leak-audit-client-report-safety-v1
npm run test:adp-leak-audit-demo-pack-v1
```

## Screenshot / print review checklist

| Check | Expected |
| --- | --- |
| Cover full height | Navy fills page; no white strip |
| Page 2 body text | Readable light text on navy cards |
| Page 3 actions / Who / Next | Readable; not empty-looking cards |
| Guide (web) | Opens; order matches report |
| KPI icons (web) | Open tooltips; no border overlap |
| Evidence links | Only under Supporting Evidence; one per card |
| Scenarios Monitored | Present beside evidence |
| Labels | “can help prepare” + “Hotel / Agency publishes” |
| Footers | Page 1/2/3 of 3 |
