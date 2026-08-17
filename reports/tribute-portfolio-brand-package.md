# Tribute Portfolio by Marriott — Full Brand Intelligence Package (Pilot)

Generated: 2026-07-07T08:15:08.068Z
Mode: **dry_run** · Airtable modified: **no**

## 1. Brand record resolution

| Field | Value |
|-------|-------|
| Record | `recCvV0PuZOi8c3hC` |
| Name | Tribute Portfolio |
| Resolved | **yes** (name match: true) |
| Parent | Marriott International, Inc. |

## 2. Existing Brand Explorer status

- Explorer active: **yes** (Brand Status: Active)
- Hero data source: **Mock Data for Presentation** · verification: **Demo — Not Brand-Verified**
- Existing PI sources: **0** · facts: **0**

## 3. Current profile completeness

- **Strong Existing Profile** (score 11)
- Strong existing Brand Setup content, but hero is Mock/Demo and content is not source-backed/governed.

## 4. Local files found

Company dir: `G:\My Drive\Dealality™\Platform Design & Build\Brand Reference Material\Marriott International`

- **[Tribute]** `Marriott International/fdd/Tribute Portfolio/2026-tribute-portfolio-fdd-3-31-2026.pdf` — 6086368 bytes · 1308878 chars · readable true
- `Marriott International/press/2026 Q1 earnings infographic.pdf` — 9124239 bytes · 4216 chars · readable true
- `Marriott International/press/2025 Q4 earnings infographic.pdf` — 19518216 bytes · 3832 chars · readable true
- `Marriott International/press/2025 Q3 earnings infographic.pdf` — 5250688 bytes · 3517 chars · readable true
- Images found: 0
- Recommended primary local doc: `Marriott International/fdd/Tribute Portfolio/2026-tribute-portfolio-fdd-3-31-2026.pdf`

## 5. Official web / source candidates

| Slot | Role | HTTP | Readable | JS-shell | Register? |
|------|------|------|----------|----------|-----------|
| consumer_page | consumer_page | 200 | 2834 | low | yes |
| bonvoy_page | consumer_page | 200 | 13581 | low | yes |
| development_page | development_page | failed | 0 | unreachable | no |
| press_hub | pr_opening | 200 | 17 | high | no |

## 6. Proposed source package

- Registerable sources: **6** · all company-controlled: **true**
- Consumer: true · Local PDF: true · Development: true · Press: false

- [web] consumer_page → **register_website_capture** — https://tribute-portfolio.marriott.com/
- [web] consumer_page → **register_website_capture** — https://www.marriott.com/loyalty.mi
- [web] development_page → **hold_unreachable** — https://development.marriott.com/our-brands/
- [web] pr_opening → **provenance_only_js_shell** — https://news.marriott.com/brands/tribute-portfolio
- [local] local_pdf → **register_local_document** — Marriott International/fdd/Tribute Portfolio/2026-tribute-portfolio-fdd-3-31-2026.pdf
- [local] consumer_page → **register_local_document** — Marriott International/brands/Tribute Portfolio/tribute portfolio brand page.html
- [local] development_page → **register_local_document** — Marriott International/development/Brand portfolio.html
- [local] development_page → **register_local_document** — Marriott International/development/Marriott development home.html

## 7. Proposed extraction fields

- `be.identity.brandName` (any) — source-backed: Directly stated.
- `be.identity.parentCompany` (local_pdf) — source-backed: FDD / brand page.
- `be.positioning.summary` (consumer_page) — source-backed · AI-draftable: Consumer brand page + FDD framing.
- `be.positioning.tagline` (consumer_page) — source-backed: Marriott brand tagline.
- `be.positioning.guestPromise` (consumer_page) — source-backed · AI-draftable: Consumer + Bonvoy page.
- `be.positioning.history` (local_pdf) — source-backed · AI-draftable: Launch year / history from FDD.
- `be.overview.developmentModel` (local_pdf) — source-backed · AI-draftable: FDD franchise/conversion model.
- `be.overview.whyValue` (development_page) — AI-draft · AI-draftable: Owner value proposition — AI-draft, human review.
- `be.overview.typicalUseCase` (consumer_page) — AI-draft · AI-draftable: Conversion / adaptive-reuse fit — AI-draft.
- `be.loyalty.programName` (consumer_page) — source-backed: Marriott Bonvoy.
- `be.footprint.geoIntro` (consumer_page) — AI-draft · AI-draftable: Regional relevance — AI-draft, verify.

## 8. Brand Setup completion plan

**Already populated (preserve, source-back):** 17
- Brand Name → `be.identity.brandName`
- Parent Company → `be.identity.parentCompany`
- Brand Positioning → `be.positioning.summary`
- Brand Tagline → `be.positioning.tagline`
- Brand Customer Promise → `be.positioning.guestPromise`
- Brand History → `be.positioning.history`
- Brand Architecture → `n/a`
- Brand Model → `be.overview.developmentModel`
- Brand Value Proposition → `be.overview.whyValue`
- Key Brand Differentiators → `be.overview.whyValue`
- Hotel Chain Scale → `n/a`
- Hotel Service Model → `n/a`
- Target Guest Segments → `be.overview.typicalUseCase`
- Guest Psychographics Description → `be.overview.typicalUseCase`
- Region Offered → `be.footprint.geoIntro`
- Brand Pillars → `be.positioning.summary`
- Sustainability Positioning → `n/a`

**Missing but source-supported:** 0

**AI-draftable (Pending review):** 5
- Owner / developer value proposition → `be.overview.whyValue`
- Conversion / adaptive-reuse fit → `be.overview.typicalUseCase`
- Owner considerations narrative → `be.overview.whyValue`
- Questions owners should ask → `be.overview.scenarios`
- Marriott Bonvoy relationship summary → `be.loyalty.programName`

**Human review required:**
- Economics / franchise fees (royalty, initial fee) — From FDD Item 5-7 — human review before display.
- Footprint counts (hotels / rooms / pipeline) — Verify against latest Marriott disclosure; numbers age quickly.
- Item 19 financial performance — FDD Item 19 — sensitive; human review, likely internal-only.

**Keep blank:**
- Company Validated (do not set)
- Company Validation Date (do not set)
- Any field implying Marriott reviewed/endorsed the profile

**Source gaps:**
- Recent openings / PR links: news.marriott.com is a JS shell (near-zero readable text); needs manual capture or rendered snapshot.
- Official development brand page: development.marriott.com candidates unreachable; rely on local development captures + FDD.
- Property / design imagery + verified hero: Explorer hero is Mock/Demo; no approved brand image-capture workflow in repo.

## 9. Asset package plan

- Logo: present_in_brand_setup (reuse_existing)
- Hero: mock_demo_not_verified (capture_verified_hero_future_asset_module)
- Property images: none_local
- Asset governance schema exists: **false** — Build asset-governance module (logo/hero/property image capture + rights + trust labeling) before downloading Marriott imagery.

## 10. Governance recommendation

- Recommended posture: **company_materials**
- Validation Status: Company Published
- Usage Permission: Platform Display Allowed
- External Display Status: Show Trust Label
- External chip: AI-Assisted Profile · Source Basis: Company Materials
- Company Validated: false / unchanged · Company Validation Date: unchanged
- Rationale: All registerable sources are Marriott-controlled (consumer site + Tribute FDD + development captures); qualifies for Company Materials basis and AI-Assisted Profile chip.

## 11. Ready for pipeline?

- **Yes** — ≥2 approved-able Marriott-controlled sources (incl. consumer page and/or Tribute FDD).

## 12. Risks & caveats

- Existing Brand Setup content is demo/mock (hero = Mock Data); it must be source-backed via staged extraction, not overwritten.
- Marriott consumer + newsroom pages are JS-heavy; readable text is thin — prefer the local Tribute FDD for factual extraction.
- development.marriott.com candidates were unreachable at probe time — use local Marriott development captures for provenance.
- FDD Item 19 / fee data is sensitive — human review; likely internal-only, not public trust-label display.
- No approved image/asset-governance workflow exists — do not download Marriott imagery yet.
- Do not imply Marriott validated the profile; Company Validated / Company Validation Date must remain unchanged.

## 13. Next commands

- `npm run tribute-portfolio-brand-package -- --dry-run`
- `npm run partner-reference:download -- --url "https://tribute-portfolio.marriott.com/" --company "Marriott International" --brand "Tribute Portfolio" --type website-capture --title "Tribute Portfolio consumer brand page" --brand-id recCvV0PuZOi8c3hC --dry-run`
- `Register 2026 Tribute FDD (local) via Source Library with Profile Type Brand, Source Origin FDD Library (dry-run first)`
- `After sources registered + approved: run stewardship → extraction → fact stewardship → governance publish (all dry-run first)`

## Does not do

- Overwrite existing Brand Setup content fields (staged review only)
- Register or approve sources in dry-run
- Extract, approve facts, or publish governance
- Set Company Validated or Company Validation Date
- Download images or scrape third-party / OTA pages
- Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema
