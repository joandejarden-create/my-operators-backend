# Brand Asset & PR Package Governance v1

Generated: 2026-07-07T18:40:28.898Z
Mode: **dry-run** · Airtable modified: **no**
Brand: Tribute Portfolio `recCvV0PuZOi8c3hC`

## 1. Governed profile status

- Text/governance Platform Ready: **yes**
- Validation Status: Company Published
- Display Label: —
- Approved PI sources: 6 · approved facts: 7
- Company Validated: null (unchanged)

## 2. Asset/media schema

- Schema fields found in Brand Setup: **yes** (4 media-related fields)
- Brand Explorer presentation slot images (overview cards, etc.) live in separate presentation tables — not scanned in v1.

- `Explorer Hero Data Source` (hero) — populated: true
- `Partner Intelligence - Brand Asset Registry` (media) — populated: true
- `Logo` (logo) — populated: true
- `Explorer Hero Verification` (hero) — populated: true

## 3. Current asset status

| Asset | Status | Notes |
|-------|--------|-------|
| Logo | Needs Usage Review | Logo attachment present in Brand Setup — source/rights not confirmed; do not treat as Source-Confirmed without review. |
| Hero | Mock/Demo | Hero is Mock/Demo — must not be used as governed Explorer hero. |
| Property/design images | Missing | No governed property/design/lifestyle image package in Brand Setup or PI. |
| PDF / attachments | Source-Confirmed | unable to determine — recommend future asset-governance audit (presentation slots / attachments) |
| PR / recent openings | Missing | unable to determine — check footprint/openings tables in future audit |

## 4. Local asset files

- Reference root: `G:\My Drive\Dealality™\Platform Design & Build\Brand Reference Material`
- Images: **0** · PDFs: **4**
- No local image files found
- Marriott International/fdd/Tribute Portfolio/2026-tribute-portfolio-fdd-3-31-2026.pdf (PDF)
- Marriott International/press/2026 Q1 earnings infographic.pdf (PDF)
- Marriott International/press/2025 Q4 earnings infographic.pdf (PDF)
- Marriott International/press/2025 Q3 earnings infographic.pdf (PDF)

## 5. Official image/logo candidates (HTML refs only — not downloaded)

- Logo candidates: **25**
- https://tribute-portfolio.marriott.com/wp-content/uploads/2025/03/tribute-black.svg
- https://tribute-portfolio.marriott.com/wp-content/themes/tribute/assets/img/tribute.svg
- https://tribute-portfolio.marriott.com/wp-content/themes/tribute/assets/img/tribute-black.svg
- https://tribute-portfolio.marriott.com/wp-content/uploads/2025/10/tribute-black.svg
- https://cache.marriott.com/Images/Mobile/MC_Logos/MarriottApple57x57.png
- Hero/property candidates: **15**
- https://tribute-portfolio.marriott.com/wp-content/uploads/2018/12/cropped-tributefavicon_512x512-1-32x32.jpg
- https://tribute-portfolio.marriott.com/wp-content/uploads/2018/12/cropped-tributefavicon_512x512-1-192x192.jpg
- https://tribute-portfolio.marriott.com/wp-content/uploads/2018/12/cropped-tributefavicon_512x512-1-180x180.jpg
- https://tribute-portfolio.marriott.com/wp-content/uploads/2018/12/cropped-tributefavicon_512x512-1-270x270.jpg
- https://tribute-portfolio.marriott.com/wp-content/uploads/2023/07/trbcl.1256725ws2880-1152-1536x614.jpg
- https://tribute-portfolio.marriott.com/wp-content/uploads/2023/07/trbcl.1256725ws2880-1152.jpg

## 6. PR / recent-opening candidates

- https://news.marriott.com/brands/tribute-portfolio — JS-shell: **high** · usability: Do Not Use
- **Rendered Source Capture v1 needed:** yes
- news.marriott.com/brands/tribute-portfolio returns near-zero readable text (JS-shell). PR links, recent openings, and press imagery require Rendered Source Capture v1 before asset governance can approve press assets.

## 7. Recommended Tribute asset package

| Priority | Type | Status | Action |
|----------|------|--------|--------|
| 1 | Logo | Needs Usage Review | Confirm existing Brand Setup logo against official Marriott tribute-portfolio.svg |
| 1 | Hero Image | Mock/Demo | Replace Mock/Demo hero with Marriott-controlled candidate after Rendered Source Capture |
| 2 | Hero Image | Candidate | Hero candidate from official page HTML — usage review required |
| 3 | Exterior / Property | Candidate | Property/design candidate — usage review before Explorer |
| 3 | Exterior / Property | Candidate | Property/design candidate — usage review before Explorer |
| 3 | Exterior / Property | Candidate | Property/design candidate — usage review before Explorer |
| 3 | Exterior / Property | Candidate | Property/design candidate — usage review before Explorer |
| 4 | PDF / Brochure | Source-Confirmed | FDD/brochure already in PI Source Library — text only; not Explorer hero/logo |
| 4 | PDF / Brochure | Source-Confirmed | FDD/brochure already in PI Source Library — text only; not Explorer hero/logo |
| 5 | Press Link | Do Not Use | Provenance only until Rendered Source Capture v1 |

## 8. Safe to use now

- Approved PI text facts (7) and governance trust chip (AI-Assisted Profile / Company Materials)
- Approved Source Library rows (6 Marriott-controlled sources) for provenance — text extraction only
- Local FDD PDF as factual/legal reference in Source Library — not for Explorer hero/logo display

## 9. Needs human / usage review

- Existing Brand Setup logo — confirm authoritative Marriott source and usage rights
- All image URL candidates parsed from HTML — usage/rights review before any Explorer display
- Any local image files — verify not outdated or third-party before use

## 10. Requires future tooling

- Rendered Source Capture v1 — required for news.marriott.com PR/openings (JS-shell)
- Asset download + rights registry — no approved image download workflow in repo
- Airtable asset governance fields — v1 is report-only; no asset status columns written
- Explorer hero/logo field writer — must not overwrite Mock/Demo hero without staged approval
- Presentation slot image governance — overview cards / property galleries not scanned in v1

## 11. Visual parity gap (Kimpton / Radisson Blu)

- Target: Verified logo, hero, 3–6 property/design images, PR/recent-opening links, governed asset statuses
- Tribute now: Text/governance Platform Ready; hero Mock/Demo; logo unconfirmed; no governed image package; PR not captured

**Remaining:**
- Confirm logo source and usage rights
- Replace Mock/Demo hero with Marriott-controlled candidate
- Capture 3–6 property/design/lifestyle images with rights metadata
- Rendered capture of Marriott newsroom for PR/recent openings
- Future v2: asset status fields + Explorer field writer with staging

## Does not do

- Download images or scrape OTA/booking-site images
- Overwrite Brand Setup logo, hero, image, or attachment fields
- Write Airtable or create new schema fields
- Set Company Validated or Company Validation Date
- Imply Marriott validated assets or profile
- Publish/display images without source and usage review
