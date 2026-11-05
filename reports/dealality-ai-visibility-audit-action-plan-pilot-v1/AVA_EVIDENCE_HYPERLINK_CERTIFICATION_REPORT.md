# AVA Evidence & Hyperlink Certification Report

**Generated:** 2026-11-05T13:47:38.074Z
**Contract:** `AVA_EVIDENCE_HYPERLINK_CERTIFICATION_V1`
**Restrictions:** no customer publication · no production deployment · no paid research · no ADP baseline edits · no automatic founder approvals

## 1. Validation methodology

1. Collect every customer-facing evidence URL from findings, supporting evidence, technical URLs, and evidence refs.
2. Probe HTTP status with bounded timeouts; follow redirects safely; classify 404/410 as `BROKEN_LINK`.
3. Treat 403/429/bot/timeout as `REQUIRES_MANUAL_REVIEW` (not automatic broken). Optional Playwright browser assist may record a manual verification with explanation.
4. Classify claim support: `VERIFIED_CURRENT` | `VERIFIED_WITH_QUALIFICATION` | `REQUIRES_MANUAL_REVIEW` | `BROKEN_LINK` | `UNSUPPORTED_CLAIM` | `OUTDATED_SOURCE`.
5. Extract PDF `/URI` annotations and compare to the labeled evidence register; rejected URLs (including the retired Bethesda Clinical Center visitor-hotels path) must be absent.
6. Gate: `AUDIT_VALIDATED` requires zero blocking evidence rows and clean PDF annotations.

## 2. Six-hotel results

| Hotel | Gate | Status after | Broken | Unsupported | Manual review | PDF regenerated |
|---|---|---|---|---|---|---|
| Spice Island Beach Resort | **FAIL** | `VALIDATION_REQUIRED` | 0 | 0 | 5 | no |
| Wyndham San Diego Bayside | **FAIL** | `VALIDATION_REQUIRED` | 0 | 0 | 2 | no |
| Waterstone Resort & Marina | **FAIL** | `VALIDATION_REQUIRED` | 0 | 0 | 2 | no |
| Bethesda Marriott | **PASS** | `AUDIT_VALIDATED` | 0 | 0 | 0 | no |
| Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca | **FAIL** | `VALIDATION_REQUIRED` | 0 | 0 | 1 | no |
| Sheraton Mallorca Arabella Golf Hotel | **FAIL** | `VALIDATION_REQUIRED` | 0 | 0 | 1 | no |

Totals: **1 PASS** / **5 FAIL** of 6.

## 3. Broken links discovered

None remaining after corrections.

## 4. Unsupported claims discovered

None.

## 5. Corrections made

- **Bethesda Marriott** — retired `cc.nih.gov/about/visitor/hotels`; claim revised; ORS Patient Hotel Shuttles citation; PDF annotations clean; gate **PASS**.
- **Waterstone Resort & Marina** — `waterstoneboca.com/dining/` (404) replaced with homepage; prior PDF preserved under `pre-evidence-hyperlink-certification/`.
- No other definitive broken customer-facing URLs remained in the six-hotel cohort after those corrections.

## 6. PDF annotation verification

- **Spice Island Beach Resort**: missing=0, unexpected=0, rejectedStillPresent=0, Bethesda broken URL absent=true
- **Wyndham San Diego Bayside**: missing=0, unexpected=0, rejectedStillPresent=0, Bethesda broken URL absent=true
- **Waterstone Resort & Marina**: missing=0, unexpected=0, rejectedStillPresent=0, Bethesda broken URL absent=true
- **Bethesda Marriott**: missing=0, unexpected=0, rejectedStillPresent=0, Bethesda broken URL absent=true
- **Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca**: missing=0, unexpected=0, rejectedStillPresent=0, Bethesda broken URL absent=true
- **Sheraton Mallorca Arabella Golf Hotel**: missing=0, unexpected=0, rejectedStillPresent=0, Bethesda broken URL absent=true

## 7. Remaining manual-review requirements

### Spice Island Beach Resort (5)
- `https://www.spiceislandbeachresort.com/dining` — Automated access restricted (HTTP_403). Not treated as a definitive broken link.
- `https://www.spiceislandbeachresort.com/rooms-in-grenada` — Automated access restricted (HTTP_403). Not treated as a definitive broken link.
- `https://www.spiceislandbeachresort.com/specials` — Automated access restricted (HTTP_403). Not treated as a definitive broken link.
- `https://www.spiceislandbeachresort.com/conference-review` — Automated access restricted (HTTP_403). Not treated as a definitive broken link.
- `https://www.spiceislandbeachresort.com/` — Automated access restricted (HTTP_403). Not treated as a definitive broken link.

### Wyndham San Diego Bayside (2)
- `https://www.wyndhamsandiegobay.com/` — Automated access restricted (HTTP_403). Not treated as a definitive broken link.
- `https://www.wyndhamsandiegobay.com/meetings` — Automated access restricted (HTTP_403). Not treated as a definitive broken link.

### Waterstone Resort & Marina (2)
- `https://www.hilton.com/en/hotels/bctbuqq-waterstone-resort-and-marina-boca-raton/` — Automated access restricted (HTTP_403). Not treated as a definitive broken link.
- `https://www.waterstoneresort.com/` — Automated access restricted (FETCH_FAILED). Not treated as a definitive broken link.

### Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca (1)
- `https://www.arabellagolf.com/` — Automated access restricted (FETCH_FAILED). Not treated as a definitive broken link.

### Sheraton Mallorca Arabella Golf Hotel (1)
- `https://www.arabellagolf.com/` — Automated access restricted (FETCH_FAILED). Not treated as a definitive broken link.

## 8. Certification PASS/FAIL per hotel

- **Spice Island Beach Resort** (`adp_spice_island_beach_resort`): FAIL · status `VALIDATION_REQUIRED` · founderApprovedForCustomer=false
- **Wyndham San Diego Bayside** (`adp_wyndham_san_diego_bayside`): FAIL · status `VALIDATION_REQUIRED` · founderApprovedForCustomer=false
- **Waterstone Resort & Marina** (`adp_waterstone_boca_raton`): FAIL · status `VALIDATION_REQUIRED` · founderApprovedForCustomer=false
- **Bethesda Marriott** (`adp_bethesda_marriott`): PASS · status `AUDIT_VALIDATED` · founderApprovedForCustomer=false
- **Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca** (`adp_castillo_hotel_son_vida`): FAIL · status `VALIDATION_REQUIRED` · founderApprovedForCustomer=false
- **Sheraton Mallorca Arabella Golf Hotel** (`adp_sheraton_mallorca_arabella_golf`): FAIL · status `VALIDATION_REQUIRED` · founderApprovedForCustomer=false

## 9. AI Admin

- Drawer section **Evidence Validation** shows totals, last validation date, and failures.
- `POST /api/admin/ai-visibility-audit/:propertyId/evidence-validation` runs the gate.
- `POST .../evidence-validation/manual` records VERIFIED/REJECTED/DEFER with mandatory explanation + audit trail.
- Transition to `AUDIT_VALIDATED` is blocked while the gate fails.

Stop for founder review.
