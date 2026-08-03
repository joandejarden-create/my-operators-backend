# Old Home Ecosystem — Staging Publish + Hosted QA

**Recommendation: A. Ready for production approval**

Do not publish to production from this report. Stop after staging QA.

## 1. Unpublished-change audit

| Area | Result |
| --- | --- |
| Site lastPublished before this run | 2026-07-31T15:18:27.790Z |
| Pages updated after that | Only Old Home (68108c2a063eeb5d1bd7ae90, lastUpdated 2026-07-31T17:26:49.529Z) |
| Root / Home | lastUpdated 2026-07-30T13:46:47.675Z — already covered by prior publish |
| Site applied scripts | lastUpdated 2026-07-31T15:18:06.916Z — at/before prior publish |
| Site freeform head | No ecosystem CSS; GTM/FOUC/analytics only |
| Old Home page head | Testimonials CSS + dealality-old-home-ecosystem.v20260731i.css |
| CMS collections | Users / Insights — lastUpdated May 2025 / May 2026 |
| Webhooks | None |
| Expected package | Old Home ecosystem HTML + page-head CSS reference |
| Unrelated risk | None found that blocked staging publish |

## 2-5. Staging publish

| Field | Value |
| --- | --- |
| Target | mvp-deal-capture.webflow.io only |
| Request | customDomains: [], publishToWebflowSubdomain: true |
| API result | { customDomains: [], publishToWebflowSubdomain: true, publishScope: "site" } |
| Publish timestamp | 2026-07-31T17:29:13.149Z |
| Hosted URL | https://mvp-deal-capture.webflow.io/old-home |

## Production safety (post-publish)

| Domain | lastPublished |
| --- | --- |
| Site / staging subdomain | 2026-07-31T17:29:13.149Z |
| www.dealality.com | 2026-07-31T15:18:27.790Z (unchanged) |
| dealality.com | 2026-07-31T15:18:27.790Z (unchanged) |

Production /old-home still has old #eco-grid (no owner-advisor-led, no v20260731i). Production / unchanged.

## Screenshots

Artifacts under /opt/cursor/artifacts/ecosystem-staging-qa/:

- 01-desktop-1440-full-section.png
- 02-desktop-1280-full-section.png
- 10-tablet-900.png
- 11-mobile-390.png
- 13-zoom-200.png
- 03-central-owner-advisor-composition.png
- 06-connectors-labels-arrows.png
- 07-owner-control-band.png
- 08-process-strip.png
- 09-closing-panel.png
- 11-owners-lead-card.png / 11b-advisors-lead-card.png
- 04-brands-card.png / 05-capital-card.png / 12-zoom-150.png

## CSS / network / console

- Ecosystem CSS v20260731i.css HTTP 200
- No failed CSS/font requests for ecosystem
- No unstyled flash after oh-ready; no raw CSS/HTML in section
- Console noise pre-existing (globe CORS; landing-events CORS)
- No duplicate IDs; no horizontal overflow; cards not clipped

## Content verification (hosted)

- Eyebrow L/R, headline, supporting copy exact match
- Marker section#ecosystem[data-oh-ecosystem=owner-advisor-led]
- Stage + band + 3-step process + closing panel present
- Role hierarchy confirmed including Leads the process for owners

## Assessments

- Mockup parity: close enough for staging visual approval
- Role clarity: strong
- Section height: desktop ~1036px acceptable; mobile tall by design
- Production domains selected: No
- Root / touched: No

## Recommendation

A. Ready for production approval

Staging-only publish succeeded. Production domains were not selected. Stop here — no production publish.
