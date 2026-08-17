# Brand Explorer Lifestyle / Affiliation Config v35B

Generated: 2026-07-14T13:04:11.632Z

## Guardrails
- No Airtable writes
- No active-profile approval
- No Company Validated changes
- Dry-run only for factory stages

## Brand config registration
| Priority | Brand | Slug | Config pass | Copy mode |
| --- | --- | --- | --- | --- |
| 1 | Design Hotels | `design-hotels` | PASS | affiliation_curation_platform |
| 2 | Small Luxury Hotels of the World | `small-luxury-hotels-of-the-world` | PASS | independent_luxury_consortium |
| 3 | Autograph Collection | `autograph-collection` | PASS | soft_brand_collection |
| 4 | Tribute Portfolio | `tribute-portfolio` | PASS | soft_brand_collection |
| 5 | Vignette Collection | `vignette-collection` | PASS | soft_brand_collection |
| 6 | MGallery Collection | `mgallery-collection` | PASS | soft_brand_collection |
| 7 | Handwritten Collection | `handwritten-collection` | PASS | soft_brand_collection |

**Registered:** 7/7

## Copy governance modes
### Affiliation / curation platform (`affiliation_curation_platform`)
- independent identity preservation
- design-led and culturally distinctive hotel fit
- architecture, storytelling, and local identity
- distribution and recognition value
- owner control and curation expectations

### Independent luxury consortium (`independent_luxury_consortium`)
- independent luxury and boutique hotel fit
- quality and guest-experience expectations
- affiliation and distribution value
- owner control
- luxury credibility without chain flag

### Soft-brand collection (`soft_brand_collection`)
- parent platform context
- soft-brand lifestyle positioning
- conversion and repositioning fit
- loyalty and distribution context
- standards and owner obligations with flexibility

## Factory dry-runs
### design-hotels
- Preflight: **FAIL**
- Asset pack ready: **NO**
- Approved sources: 0
- Blockers (5):
  - need_6_visible_gallery_imageUrl_got_0: 
  - no_visible_scenario_cards_in_api: 
  - atelier_hardcoded_scenario_fallback_risk:overview.scenario.1: 
  - atelier_hardcoded_scenario_fallback_risk:overview.scenario.2: 
  - atelier_hardcoded_scenario_fallback_risk:overview.scenario.3: 

### small-luxury-hotels-of-the-world
- Preflight: **FAIL**
- Asset pack ready: **NO**
- Approved sources: 0
- Blockers (5):
  - need_6_visible_gallery_imageUrl_got_0: 
  - no_visible_scenario_cards_in_api: 
  - atelier_hardcoded_scenario_fallback_risk:overview.scenario.1: 
  - atelier_hardcoded_scenario_fallback_risk:overview.scenario.2: 
  - atelier_hardcoded_scenario_fallback_risk:overview.scenario.3: 

### tribute-portfolio
- Preflight: **FAIL**
- Asset pack ready: **NO**
- Approved sources: 0
- Blockers (15):
  - missing_registry_row:footprint.openings: 
  - missing_registry_row:overview.scenario.3: 
  - missing_registry_row:overview.scenario.2: 
  - missing_registry_row:overview.scenario.1: 
  - missing_registry_row:overview.hero: 
  - missing_registry_row:materials.gallery.1: 
  - missing_registry_row:materials.gallery.2: 
  - missing_registry_row:materials.gallery.3: 

## Tribute Portfolio benchmark
- Passes v34D factory rules: **NO**
- Gallery 6-image: **PASS**
- Property examples (real hotel images): **PASS**
- Copy governance needed: **YES**
- Registry traceability missing: **YES**
- Staged apply recommendation: **copy_governance_and_registry_traceability_first**

## Build recommendations
### Design Hotels
- Proceed via: source_capture_first → affiliation_specific_copy_governance_setup → manual_source_library_seeding_after_official_page_capture → asset_pack_dry_run_after_sources_approved
- Rationale: Zero approved sources and no live gallery/property examples — generic factory config is registered but asset-pack cannot complete until official Design Hotels pages, property directory, and hotel photography are source-captured.

### Small Luxury Hotels of the World
- Proceed via: source_capture_first → legal_review_consortium_sensitivity → affiliation_specific_copy_governance_setup → manual_source_library_seeding → asset_pack_dry_run_after_sources_approved
- Rationale: SLH requires consortium-appropriate copy governance and official source library before asset-pack. Generic factory represents the brand model; source capture is the gating step.

### Tribute Portfolio (benchmark)
- Proceed via: copy_governance → registry_traceability → staged_apply_draft → founder_visual_review

## Source capture plans
# Source Capture Plan v35B

- Brand: **Design Hotels** (`design-hotels`)
- Record: `rec02zPClpWUTCyXM`
- Model: affiliation_curation_platform
- Mode: **dry-run** (no Airtable writes)
- Copy guidance: Affiliation / curation platform — no franchise-flag language.
- Build recommendation: **source_capture_first**

## Source categories
### Official brand / affiliation page — **needed**
- [consumer_page] https://www.designhotels.com/
- [affiliation_page] https://www.marriott.com/marriott-brands/design-hotels.mi

### Development or membership page — **needed**
- [development_affiliation] https://www.marriott.com/marriott-brands/design-hotels.mi

### Owner / hotelier-facing criteria — **needed**
- [owner_criteria] https://www.designhotels.com/about — Verify live path during capture

### Property collection directory — **needed**
- [property_directory] https://www.designhotels.com/hotels

### 6 gallery / property images — **blocked**
- Requires 6 hotel/property photography URLs from official property pages — no logos or lifestyle stock.

### 3 property examples with hotel images — **blocked**
- Select 3 curator-approved hotels from official directory with real property images.

### Standards / participation / quality expectations — **needed**
- [curation_standards] https://www.designhotels.com/about

### Distribution / loyalty / affiliation platform — **needed**
- [bonvoy_distribution] https://www.marriott.com/loyalty.mi

### Press or official brand resources — **optional**
- [press] https://news.marriott.com/

## Recommended sequence
1. source_capture_official_pages
1. approve_source_library_rows
1. select_three_property_examples_from_directory
1. probe_six_gallery_images_from_property_pages
1. register_brand_asset_registry_rows
1. affiliation_copy_governance_setup
1. asset_pack_dry_run

# Source Capture Plan v35B

- Brand: **Small Luxury Hotels of the World** (`small-luxury-hotels-of-the-world`)
- Record: `recjjSnY2opb8P4DG`
- Model: independent_luxury_consortium
- Mode: **dry-run** (no Airtable writes)
- Copy guidance: Independent luxury consortium — no parent-brand or franchise language.
- Build recommendation: **source_capture_first**

## Source categories
### Official brand / affiliation page — **needed**
- [consumer_page] https://www.slh.com/

### Development or membership page — **needed**
- [membership_page] https://www.slh.com/about-slh

### Owner / hotelier-facing criteria — **needed**
- [owner_criteria] https://www.slh.com/about-slh
- [membership_criteria] https://www.slh.com/join-slh — Verify live path

### Property collection directory — **needed**
- [hotel_finder] https://www.slh.com/hotels

### 6 gallery / property images — **blocked**
- 6 hotel photography URLs from official SLH property pages — no stock lifestyle imagery.

### 3 property examples with hotel images — **blocked**
- 3 independent luxury property examples with verifiable hotel images.

### Standards / participation / quality expectations — **needed**
- [quality_standards] https://www.slh.com/about-slh

### Distribution / loyalty / affiliation platform — **needed**
- [slh_club_distribution] https://www.slh.com/

### Press or official brand resources — **optional**

## Recommended sequence
1. source_capture_official_pages
1. legal_review_consortium_sensitivity
1. approve_source_library_rows
1. select_three_luxury_property_examples
1. probe_six_gallery_images
1. affiliation_copy_governance_setup
1. asset_pack_dry_run

# Source Capture Plan v35B

- Brand: **Tribute Portfolio** (`tribute-portfolio`)
- Record: `recCvV0PuZOi8c3hC`
- Model: lifestyle_conversion_brand
- Mode: **dry-run** (no Airtable writes)
- Copy guidance: Soft-brand collection — Marriott Tribute Portfolio lifestyle conversion framing.
- Build recommendation: **copy_governance_and_registry_first**

## Source categories
### Official brand / affiliation page — **present**
- [consumer_page] https://tribute-portfolio.marriott.com/
- [consumer_page] https://www.marriott.com/loyalty.mi

### Development or membership page — **present**
- [development_page] https://development.marriott.com/our-brands/

### Owner / hotelier-facing criteria — **present**

### Property collection directory — **present**

### 6 gallery / property images — **partial**
- 0/6 gallery imageUrl in live API

### 3 property examples with hotel images — **partial**
- 0/3 property examples with images

### Standards / participation / quality expectations — **present**

### Distribution / loyalty / affiliation platform — **present**
- [bonvoy] https://www.marriott.com/loyalty.mi

### Press or official brand resources — **present**

## Recommended sequence
1. copy_governance_pass
1. registry_traceability_approval
1. staged_apply_draft
1. founder_visual_review
1. active_approval

