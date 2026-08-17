# Tribute Portfolio — Source-Backed Package Apply Plan

Generated: 2026-07-07T08:15:09.127Z
Mode: **dry_run** · Airtable modified: **no**
Brand: Tribute Portfolio `recCvV0PuZOi8c3hC` · Completeness: **Strong Existing Profile** · PI sources: 0 · facts: 0

## 1. Source registration plan

- Ready to register: **6** · all valid: **true** · all company-controlled: **true**

| Role | Origin | PI Source Type | Profile type | Valid | Ref |
|------|--------|----------------|--------------|-------|-----|
| consumer_page | web | Website Capture | Company Website | yes | https://tribute-portfolio.marriott.com/ |
| consumer_page | web | Website Capture | Company Website | yes | https://www.marriott.com/loyalty.mi |
| local_pdf | local | FDD | Company PDF / Brochure | yes | Marriott International/fdd/Tribute Portfolio/2026-tribute-portfolio-fd |
| consumer_page | local | Website Capture | Company Website | yes | Marriott International/brands/Tribute Portfolio/tribute portfolio bran |
| development_page | local | Website Capture | Company Website | yes | Marriott International/development/Brand portfolio.html |
| development_page | local | Website Capture | Company Website | yes | Marriott International/development/Marriott development home.html |

**Provenance-only (not registered for extraction):**
- development_page (hold_unreachable) — https://development.marriott.com/our-brands/
- pr_opening (provenance_only_js_shell) — https://news.marriott.com/brands/tribute-portfolio

## 2. Ready to register (web / local)

- Web: 2 · Local: 4
- [web] Tribute Portfolio — Official Tribute Portfolio consumer brand site
- [web] Tribute Portfolio — Marriott Bonvoy loyalty page (guest promise / Bonvoy relationship)
- [local] Tribute Portfolio — 2026 Tribute FDD — parent, development model, franchise fees, Item 19 (secondary factual).
- [local] Tribute Portfolio — Captured Marriott premium-brands page (mentions Tribute) — optional consumer context.
- [local] Tribute Portfolio — Marriott brand-portfolio development capture — company-controlled development context.
- [local] Tribute Portfolio — Marriott development home capture — owner/developer provenance.

## 3-4. Extraction eligibility + proposed facts

- Extraction-eligible sources: 6
- Proposed facts: **12** (Pending) · approvable by source-backed stewardship: **7** · held: **2**

| Fact key | Source role | Extraction | Create as | Approvable | Held | Visibility |
|----------|-------------|------------|-----------|------------|------|------------|
| `be.identity.brandName` | any | Directly Stated | Pending | yes | no | Public |
| `be.identity.parentCompany` | local_pdf | Directly Stated | Pending | yes | no | Public |
| `be.positioning.summary` | consumer_page | Directly Stated | Pending | yes | no | Public |
| `be.positioning.guestPromise` | consumer_page | Directly Stated | Pending | yes | no | Public |
| `be.positioning.tagline` | consumer_page | Needs Confirmation | Pending | yes | no | Public |
| `be.overview.developmentModel` | local_pdf | Directly Stated | Pending | yes | no | Public |
| `be.overview.whyValue` | development_page | Inferred | Pending | no | no | Public |
| `be.overview.typicalUseCase` | consumer_page | Inferred | Pending | no | no | Public |
| `be.loyalty.programName` | consumer_page | Directly Stated | Pending | yes | no | Public |
| `be.footprint.geoIntro` | consumer_page | Needs Confirmation | Pending | no | no | Public |
| `be.economics.royaltyPct` | local_pdf | Directly Stated | Pending | no | **HELD** | Internal Only |
| `be.economics.initialFranchiseFee` | local_pdf | Directly Stated | Pending | no | **HELD** | Internal Only |

## 6. Human-review / held facts (FDD)

- **FDD Item 19 financial performance** — Sensitive; internal-only, human review; no public trust-label display.
- **Franchise fees / royalty / marketing fund** — Economic terms — human review; do not publish externally as company-validated.
- **Legal / franchise obligations** — Legal terms — human review; no legal-advice statements; likely internal-only.

## 7. Brand Setup completion draft (staging output only)

**Source-backable existing fields (17):**
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

**AI-drafted enhancements (Pending review):**
- Owner / developer value proposition → `be.overview.whyValue` (AI-draft, Pending review)
- Conversion / adaptive-reuse fit → `be.overview.typicalUseCase` (AI-draft, Pending review)
- Owner considerations narrative → `be.overview.whyValue` (AI-draft, Pending review)
- Questions owners should ask → `be.overview.scenarios` (AI-draft, Pending review)
- Marriott Bonvoy relationship summary → `be.loyalty.programName` (AI-draft, Pending review)
- Owner / developer considerations (ai_drafted_company_materials)
- Questions owners should ask (dealality_authored_guidance)
- Conversion / adaptive-reuse fit (ai_drafted_review)

**Human-review only:**
- Economics / franchise fees (royalty, initial fee) — From FDD Item 5-7 — human review before display.
- Footprint counts (hotels / rooms / pipeline) — Verify against latest Marriott disclosure; numbers age quickly.
- Item 19 financial performance — FDD Item 19 — sensitive; human review, likely internal-only.

## 8. Fields that should NOT be overwritten

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

## 9. Asset / image gaps

- Asset governance module: **does_not_exist_yet**
- Official logo confirmation (existing Brand Setup logo present — confirm authoritative)
- Hero image candidate (replace Mock/Demo hero — future asset module)
- Property images
- Room / public-space / lifestyle images
- PR / recent-opening imagery
- Asset governance is a FUTURE module — no image downloads/overwrites in this step; Mock/Demo hero preserved.

## 10. PR / recent-opening gaps

- news.marriott.com is a JS shell (near-zero readable text) — provenance only; not extraction-eligible.
- Next: Rendered snapshot / manual capture of Tribute openings for a future PR/openings source; do not rely on live newsroom for extraction.

## 11. Governance readiness path

- Current: **blocked_no_sources_registered_yet** → target **company_materials**
  1. sources_registered — 6 company-controlled sources
  2. sources_approved_for_explorer_and_extraction — stewardship approves Website Capture + FDD (extraction Yes when readable)
  3. facts_extracted_pending — 12 Pending facts; FDD fee/Item 19 Internal Only
  4. facts_approved_source_backed — 7 approvable; held 2
  5. readiness_clean_min_3_approved_facts — ≥3 approved publish-scope facts incl. identity + substantive
  6. governance_publish — Company Published / Platform Display Allowed / Show Trust Label / AI-Assisted Profile / Company Materials
- Do not publish until: readiness clean (≥3 approved facts + ≥1 approved Explorer source, no downgrade/conflict)
- Company Validated: false / unchanged (never set) · Company Validation Date: unchanged (never set)

## 12. Exact apply command sequence

```bash
# 1. Review this plan (dry-run). No Airtable writes yet.
# 2. Register sources (after review):
   npm run partner-reference:download -- --url "https://tribute-portfolio.marriott.com/" --company "Marriott International" --brand "Tribute Portfolio" --brand-id recCvV0PuZOi8c3hC --type website-capture --title "Tribute Portfolio — Official Tribute Portfolio consumer brand site" --apply --register
   npm run partner-reference:download -- --url "https://www.marriott.com/loyalty.mi" --company "Marriott International" --brand "Tribute Portfolio" --brand-id recCvV0PuZOi8c3hC --type website-capture --title "Tribute Portfolio — Marriott Bonvoy loyalty page (guest promise / Bonvoy relationship)" --apply --register
   Register local file via createPartnerSource (Local File Path="Marriott International/fdd/Tribute Portfolio/2026-tribute-portfolio-fdd-3-31-2026.pdf") — see register-radisson-blu-pdf-sources.mjs pattern (dry-run first)
   Register local file via createPartnerSource (Local File Path="Marriott International/brands/Tribute Portfolio/tribute portfolio brand page.html") — see register-radisson-blu-pdf-sources.mjs pattern (dry-run first)
   Register local file via createPartnerSource (Local File Path="Marriott International/development/Brand portfolio.html") — see register-radisson-blu-pdf-sources.mjs pattern (dry-run first)
   Register local file via createPartnerSource (Local File Path="Marriott International/development/Marriott development home.html") — see register-radisson-blu-pdf-sources.mjs pattern (dry-run first)
# 3. Re-run the package + apply-plan dry-run to auto-resolve registered source IDs:
   npm run tribute-portfolio-brand-package -- --dry-run
   npm run tribute-portfolio-package-apply-plan -- --dry-run
# 4. Steward sources → extract (Pending) → fact stewardship → governance publish (each dry-run BEFORE apply).
#    Use the existing stewardship/extraction/governance scripts; auto-resolver supplies the allowlist (no manual IDs).
```

## 13. Airtable modified

- **no** — dry-run planner only.

## Does not do

- Write to Airtable / register / extract / approve / publish in dry-run
- Overwrite Brand Setup content, hero, image, or logo fields
- Auto-approve facts (source-backed stewardship recommends; human approves)
- Publish FDD fee / Item 19 / legal detail externally without human review
- Set Company Validated or Company Validation Date
- Imply Marriott validated the profile
- Download images or change UI/scoring/BAS/OAS/OCS/Deal Readiness/schema
