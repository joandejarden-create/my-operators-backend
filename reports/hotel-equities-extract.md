# Hotel Equities Narrow Extraction

Generated: 2026-07-06T13:38:02.083Z
Mode: **dry-run**
Source group: **pdf**
Operator: Hotel Equities (CALA) — `recWPKu5laVZxsvpn`

## Summary

- Source group: **pdf** (PDF enrichment — recxdPFckVzA3ckmN + rectqBTiGkq3hUlXa)
- Sources in scope: `recxdPFckVzA3ckmN`, `rectqBTiGkq3hUlXa`
- Target fact keys requested: 7
- Registry-supported keys: 5
- Registry-unsupported keys: 2
- HTML text clean (all sources): **yes**
- Existing facts (allowlisted sources): 0
- Clean facts that would be created on apply: **4**
- Skipped candidates: 12
- Duplicate warnings vs approved website facts: 1

## Publish scope strength (later)

- Substantive proposed facts: **4**
- Has `op.platform.offeredServices`: **no**
- Strong enough to add to publish scope later: **no — needs stronger candidates**
- Note: No op.platform.offeredServices candidate — review before publish-scope inclusion.

Dry-run uses read-only extraction preview — no Airtable writes unless --apply --approve-hotel-equities-extract.

## Target Fact Keys

| Priority | Field key | Registry supported | Display label |
|----------|-----------|-------------------|---------------|
| P0 | `op.platform.offeredServices` | yes | Offered Services |
| P0 | `op.snapshot.companyDescription` | yes | Company Description |
| P0 | `op.snapshot.primaryServiceModel` | yes | Primary Service Model |
| P0 | `op.markets.regionsSupported` | yes | Regions Supported |
| P1 | `op.brand.familiesOperated` | yes | Brand Families Operated |
| P1 | `op.ownerValueProposition` | **no** | — |
| P1 | `op.operatingModel` | **no** | — |

## Unsupported Registry Keys

- `op.ownerValueProposition`
- `op.operatingModel`

## Duplicate warnings (approved website facts)

- `op.snapshot.primaryServiceModel` from `recxdPFckVzA3ckmN`: duplicate_of_approved_website_fact — "Hotel Management"

_Approved website facts used for duplicate check: 5_

## Source: HE CALA Marketing Presentation March 2026 (`recxdPFckVzA3ckmN`)

- Validation: pass
- Extraction quality: substantive_pdf_text (11211 chars, kind=pdf)
- HTML text clean: yes
- Document kind: pdf (11211 chars)
- Classifier role: overview_en
- Raw candidates (target keys): 5
- Clean candidates (this source): 2
- Skipped (this source): 3

### Clean candidates

#### `op.markets.regionsSupported` (P0)

- Value: His global perspective and leadership are key assets in strengthening Hotel Equities’ footprint in these fast-growing hospitality markets.
- Evidence: His global perspective and leadership are key assets in strengthening Hotel Equities’ footprint in these fast-growing hospitality markets.
- Extraction: Inferred from Context · Confidence: Low

#### `op.brand.familiesOperated` (P1)

- Value: Marriott, Hilton, IHG, Choice
- Evidence: Mentioned brands: Marriott, Hilton, IHG, Choice
- Extraction: Directly Stated · Confidence: Medium

### Skipped candidates

- `op.snapshot.companyDescription`: pdf_deck_slide_noise — "308 United States Hotels 41 Canada Hotels 17 Caribbean & Latin America Hotels 85 F&B Outlets 64 Franchise partner Brands"
- `op.platform.offeredServices`: pdf_deck_slide_noise — "308 United States Hotels 41 Canada Hotels 17 Caribbean & Latin America Hotels 85 F&B Outlets 64 Franchise partner Brands"
- `op.snapshot.primaryServiceModel`: duplicate_of_approved_website_fact — "Hotel Management"

## Source: Caribbean & Latin America Hospitality Company — Hotel Equities (`rectqBTiGkq3hUlXa`)

- Validation: pass
- Extraction quality: substantive_pdf_text (3895 chars, kind=pdf)
- HTML text clean: yes
- Document kind: pdf (3895 chars)
- Classifier role: public_web
- Raw candidates (target keys): 5
- Clean candidates (this source): 2
- Skipped (this source): 3

### Clean candidates

#### `op.markets.regionsSupported` (P0)

- Value: Expanding Caribbean and Latin America Footprint HE enters the CALA market with a clear mission: to fill the long-standing gap in third-party hospitality management for owners in the Caribbean and Latin American regions.
- Evidence: Expanding Caribbean and Latin America Footprint HE enters the CALA market with a clear mission: to fill the long-standing gap in third-party hospitality management for owners in the Caribbean and Latin American regions.
- Extraction: Inferred from Context · Confidence: Low

#### `op.brand.familiesOperated` (P1)

- Value: Hilton
- Evidence: Mentioned brands: Hilton
- Extraction: Directly Stated · Confidence: Medium

### Skipped candidates

- `op.snapshot.companyDescription`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.snapshot.primaryServiceModel`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.platform.offeredServices`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."

## Proposed Facts (global, after dedupe)

| Source | Field key | Value preview |
|--------|-----------|---------------|
| HE CALA Marketing Presentation March 2026 | `op.markets.regionsSupported` | His global perspective and leadership are key assets in strengthening Hotel Equities’ footprint in t |
| HE CALA Marketing Presentation March 2026 | `op.brand.familiesOperated` | Marriott, Hilton, IHG, Choice |
| Caribbean & Latin America Hospitality Company — Hotel Equities | `op.markets.regionsSupported` | Expanding Caribbean and Latin America Footprint HE enters the CALA market with a clear mission: to f |
| Caribbean & Latin America Hospitality Company — Hotel Equities | `op.brand.familiesOperated` | Hilton |

## Would Write On Apply

- Pending fact rows: 4
- Sources patched (Status → Extracted): 2

**Does not write:**
- Approved for Explorer Use
- Approved for Extraction (not auto-set)
- Human Review Status = Approved (facts remain Pending)
- Operator Setup profile governance fields
- Company Validated / Company Validation Date
- External Display Status / Show Trust Label
- Published Explorer Fields
- Gap facts / Not confirmed placeholders
- All operator PI sources (allowlist only)

## Post-Apply Recommendation

```bash
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run --recompute
```

_Manually review extracted facts before any approval._
