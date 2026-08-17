# Tribute Portfolio Package Pipeline v1

Generated: 2026-07-10T00:58:09.801Z
Mode: **dry-run** · Airtable modified: **no**
Brand: Tribute Portfolio `recCvV0PuZOi8c3hC`

## Executive summary

| Metric | Value |
|--------|-------|
| Current stage | Fact Stewardship Needed |
| Live sources | 9 (approved explorer: 9) |
| Live facts | 54 (approved: 25, pending: 7, held internal: 3) |
| Governance eligible | yes |
| Governed Platform Ready | yes |
| Next action | Fact Stewardship Needed |

### Apply command

```bash
npm run tribute-portfolio-package-pipeline -- --apply --approve-tribute-portfolio-package-pipeline
```

## A. Source registration

- To register: **0** · duplicates skipped: 6 · invalid: 0

**Provenance-only (never registered for extraction):**
- development_page (hold_unreachable) — https://development.marriott.com/our-brands/
- pr_opening (provenance_only_js_shell) — https://news.marriott.com/brands/tribute-portfolio

## B. Source stewardship

- Sources needing stewardship: **0**
- Tribute Portfolio — Marriott Bonvoy — Member Benefits (elite tiers & published benefits) → already_stewarded · Loyalty page — extraction scoped to Bonvoy relationship only.
- Tribute Portfolio — Official Tribute Portfolio consumer brand site → already_stewarded
- Tribute Portfolio — Captured Marriott premium-brands page (mentions Tribute) — optional consumer context. → already_stewarded
- Tribute Portfolio — Marriott brand-portfolio development capture — company-controlled development context. → already_stewarded
- Tribute Portfolio — Marriott development home capture — owner/developer provenance. → already_stewarded
- Tribute Portfolio — Marriott Bonvoy — Use Points / Redeem → already_stewarded · Loyalty page — extraction scoped to Bonvoy relationship only.
- Tribute Portfolio — Marriott Bonvoy — Earn & Use Points → already_stewarded · Loyalty page — extraction scoped to Bonvoy relationship only.
- Tribute Portfolio — 2026 Tribute FDD — parent, development model, franchise fees, Item 19 (secondary factual). → already_stewarded · FDD extraction allowed; economics/fees/Item 19/legal facts HELD at fact stewardship.
- Tribute Portfolio — Marriott Bonvoy loyalty page (guest promise / Bonvoy relationship) → already_stewarded · Loyalty page — extraction scoped to Bonvoy relationship only.

## C. Extraction (Pending facts only)

- Extraction enabled: **yes**
- Generic gap placeholders detected: **22**
- Target fact keys: `be.identity.brandName`, `be.identity.parentCompany`, `be.positioning.summary`, `be.positioning.guestPromise`, `be.positioning.tagline`, `be.overview.developmentModel`, `be.overview.whyValue`, `be.overview.typicalUseCase`, `be.loyalty.programName`, `be.footprint.geoIntro`, `be.economics.royaltyPct`, `be.economics.initialFranchiseFee`

## C2. Targeted extraction (source-backed Tribute patterns)

- Used: **yes** · reason: generic_extraction_produced_gap_placeholders
- Placeholder facts detected: **22**
- Proposed targeted facts: **0** (approvable: 0, human-review: 0)
- Created on apply: **0**
- Skipped duplicates: 9

## D. Fact stewardship

- Targeted path active: **yes**
- Proposed approve (from plan): 7
- Live pending decisions — approve: 0, hold internal: 2, hold review: 5, reject placeholders: 0
- Targeted facts live: 17 · approved targeted: 12
- Placeholder disposition: **rejected_superseded** (rejected: 22, still pending: 0)

**Held (FDD / economics / legal):**
- FDD Item 19 financial performance — Sensitive; internal-only, human review; no public trust-label display.
- Franchise fees / royalty / marketing fund — Economic terms — human review; do not publish externally as company-validated.
- Legal / franchise obligations — Legal terms — human review; no legal-advice statements; likely internal-only.

## E. Brand Setup completion draft (staging only — never written)

**Do not overwrite:**
- Brand Name
- Parent Company
- Brand Positioning
- Brand Tagline
- Brand Customer Promise
- Brand History
- Brand Architecture
- Brand Model
- Brand Value Proposition
- Key Brand Differentiators
- Hotel Chain Scale
- Hotel Service Model
- Target Guest Segments
- Guest Psychographics Description
- Region Offered
- Brand Pillars
- Sustainability Positioning
- Logo (present — preserve)
- Explorer Hero (Mock/Demo — do not replace in this step)
- Explorer Hero Data Source / Verification

**AI-drafted enhancements (Pending review):**
- Owner / developer value proposition → `be.overview.whyValue` (AI-draft, Pending review)
- Conversion / adaptive-reuse fit → `be.overview.typicalUseCase` (AI-draft, Pending review)
- Owner considerations narrative → `be.overview.whyValue` (AI-draft, Pending review)
- Questions owners should ask → `be.overview.scenarios` (AI-draft, Pending review)
- Marriott Bonvoy relationship summary → `be.loyalty.programName` (AI-draft, Pending review)
- Owner / developer considerations (ai_drafted_company_materials)
- Questions owners should ask (dealality_authored_guidance)
- Conversion / adaptive-reuse fit (ai_drafted_review)

## F. Governance publish

- Live readiness eligible: **yes**
- Target posture: Company Published / Platform Display Allowed / Show Trust Label / AI-Assisted Profile / Company Materials
- Company Validated + Company Validation Date: never written

### Readiness gate sequence

  1. sources_registered — 6 company-controlled sources
  2. sources_approved_for_explorer_and_extraction — stewardship approves Website Capture + FDD (extraction Yes when readable)
  3. facts_extracted_pending — 12 Pending facts; FDD fee/Item 19 Internal Only
  4. facts_approved_source_backed — 7 approvable; held 2
  5. readiness_clean_min_3_approved_facts — ≥3 approved publish-scope facts incl. identity + substantive
  6. governance_publish — Company Published / Platform Display Allowed / Show Trust Label / AI-Assisted Profile / Company Materials

## G. Verification

- Governed Platform Ready: **yes**
- Approved facts: 25 · approved explorer sources: 9
- Chip: **AI-Assisted Profile** · Basis: **Company Materials**
- Company Validated untouched: true · Company Validation Date untouched: true

## Asset / image gaps (future module)

- Official logo confirmation (existing Brand Setup logo present — confirm authoritative)
- Hero image candidate (replace Mock/Demo hero — future asset module)
- Property images
- Room / public-space / lifestyle images
- PR / recent-opening imagery

## PR / recent-opening gaps

- news.marriott.com is a JS shell (near-zero readable text) — provenance only; not extraction-eligible.
- Next: Rendered snapshot / manual capture of Tribute openings for a future PR/openings source; do not rely on live newsroom for extraction.

## Airtable modified

- **no**

## Does not do

- Rebuild Brand Explorer content or overwrite Brand Setup content/hero/image/logo fields
- Download images (asset governance is a future module)
- Auto-approve held/weak facts or FDD economics
- Publish FDD economics / Item 19 / legal detail externally
- Set Company Validated or Company Validation Date
- Imply Marriott validated the profile
- Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema
