# Brand Setup Child-Table Validation — Active-62 (Read-Only)

**Status:** `brand_setup_child_table_validation_62_readonly_complete_ready_for_remediation_queue`
**Generated:** 2026-08-08T09:15:16.745Z
**Freeze:** `frozen_62_active_public_full_baseline_quality_clean_flex_held` · unchanged=true
**Mode:** read-only · Airtable writes=false

## Verdict

Audited **10** Brand Setup child tables × **62** Active-62 brands. Fully covered brands: **60/62**. Remediation queue: **222** (high=30; missing=0, duplicate=2, orphan=3, held-stale=30, language high/med=21/0, mismatch=166). Non-Active inventory rows observed=1914 (reported, not auto-queued).

## Scope

- Active-62 only (frozen quality-clean baseline)
- Child tables only (not Presentation, not Basics parent, not Census)
- Excluded brand probes: radisson-collection, the-house-of-originals, four-points-flex-by-sheraton, morgans-originals

## Table summary

| Table | Records | Brands w/ row | Missing | Duplicate | Orphan | Stale | Language |
|------|--------:|-------------:|--------:|----------:|-------:|------:|---------:|
| Brand Setup - Brand Footprint | 256 | 62 | 0 | 0 | 0 | 194 | 0 |
| Brand Setup - Project Fit | 257 | 62 | 0 | 0 | 0 | 195 | 0 |
| Brand Setup - Portfolio & Performance | 256 | 62 | 0 | 2 | 0 | 196 | 0 |
| Brand Setup - Brand Standards | 257 | 62 | 0 | 0 | 0 | 195 | 0 |
| Brand Setup - Fee Structure | 257 | 62 | 0 | 0 | 1 | 194 | 0 |
| Brand Setup - Deal Terms | 256 | 62 | 0 | 0 | 0 | 194 | 21 |
| Brand Setup - Operational Support | 258 | 62 | 0 | 0 | 2 | 194 | 0 |
| Brand Setup - Legal Terms | 256 | 62 | 0 | 0 | 0 | 194 | 0 |
| Brand Setup - Loyalty & Commercial | 256 | 62 | 0 | 0 | 0 | 194 | 0 |
| Brand Setup - Sustainability & ESG | 256 | 62 | 0 | 0 | 0 | 194 | 0 |

## Confirmations

- `activeUniverseRemains62`: **true**
- `frozenBaselineUnchanged`: **true**
- `noBrandExplorerWrites`: **true**
- `noBrandSetupWrites`: **true**
- `noHotelPropertyCensusWrites`: **true**
- `noBrandStatusChanges`: **true**
- `noReleaseFieldChanges`: **true**
- `noCompanyValidatedOrBrandVerifiedWrites`: **true**

## Remediation queue (top 40)

- **[high]** mismatch · Brand Setup - Portfolio & Performance: child record links to multiple Brand Basics ids — reclu1Fgv5Leq25qn,recnVdGwNaaJNn0eH
- **[high]** mismatch · Brand Setup - Portfolio & Performance: child record links to multiple Brand Basics ids — rec2DDyPu38C6zDBC,recPAB0PgJyKE2v09
- **[high]** mismatch · Brand Setup - Portfolio & Performance: child record links to multiple Brand Basics ids — rec3nyARnkn97W9w6,recaayt9u7YYg8h7Y
- **[high]** mismatch · Brand Setup - Portfolio & Performance: child record links to multiple Brand Basics ids — recFLwYLMKLbXZFM6,recmKqo7M7mLZgRqQ
- **[high]** duplicate · country-inn-suites: duplicate Brand Setup - Portfolio & Performance records (2) — reccseFx1vp1SfnCq,recp3rK8H1uRMVBHT
- **[high]** duplicate · radisson-red: duplicate Brand Setup - Portfolio & Performance records (2) — rec5Sfb1oDC6ebrjn,recvFQCfrANNJfogo
- **[high]** orphan · Brand Setup - Fee Structure: orphan child record (no Brand Basics link) — The Unbound Collection
- **[high]** public_language_risk · pullman: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · city-express-by-marriott: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · ibis: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · towneplace-suites-by-marriott: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · courtyard-by-marriott: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · springhill-suites-by-marriott: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · sheraton: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · home2-suites-by-hilton: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · avid-hotels: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · trademark-collection-by-wyndham: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · fairmont-hotels-and-resorts: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · residence-inn-by-marriott: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · westin: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · marriott-hotels: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · studiores: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · homewood-suites-by-hilton: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · novotel: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · mercure: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · hilton-hotels-and-resorts: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · voco-hotels: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** public_language_risk · holiday-inn-express: process/internal language in Brand Setup - Deal Terms — qa_process
- **[high]** orphan · Brand Setup - Operational Support: orphan child record (no Brand Basics link)
- **[high]** orphan · Brand Setup - Operational Support: orphan child record (no Brand Basics link)
- **[medium]** mismatch · radisson-red: Brand Name text on child ≠ Basics — "Radisson RED  (Choice)" vs "Radisson RED by Choice"
- **[medium]** mismatch · so-hotels-and-resorts: Brand Name text on child ≠ Basics — "So" vs "SO/"
- **[medium]** mismatch · radisson-blu: Brand Name text on child ≠ Basics — "Radisson Blu (Choice)" vs "Radisson Blu by Choice"
- **[medium]** mismatch · country-inn-suites: Brand Name text on child ≠ Basics — "Country Inn & Suites by Radisson (Choice)" vs "Country Inn & Suites by Choice"
- **[medium]** mismatch · sheraton: Brand Name text on child ≠ Basics — "Sheraton Hotel" vs "Sheraton"
- **[medium]** mismatch · radisson-individuals-by-choice: Brand Name text on child ≠ Basics — "Radisson Individual (Choice)" vs "Radisson Individuals by Choice"
- **[medium]** stale_held_probe · Brand Setup - Brand Footprint: child linked to held/excluded brand probe — Under Review
- **[medium]** mismatch · marriott-hotels: Brand Name text on child ≠ Basics — "Marriott" vs "Marriott Hotels"
- **[medium]** mismatch · radisson: Brand Name text on child ≠ Basics — "Radisson (Choice)" vs "Radisson by Choice"
- **[medium]** mismatch · bunkhouse-hotels: Brand Name text on child ≠ Basics — "Bunkhouse" vs "Bunkhouse Hotels"
- … +182 more (see JSON)

## Outputs

- Coverage matrix: 620 cells in JSON `coverageMatrix`
- Missing / duplicate / orphan / stale / language / mismatch reports embedded in JSON

**Final status:** `brand_setup_child_table_validation_62_readonly_complete_ready_for_remediation_queue`

