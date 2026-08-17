# Curio Clean Re-Extraction

Generated: 2026-07-06T11:21:02.072Z
Mode: **apply**
Brand: Curio Collection by Hilton — `receQkxgjlezsc1xg`

## Summary

- Allowed source IDs: `recy2pyEahF9UUsEk`, `recL1qfHCOAUZr9Rz`
- Target fact keys: 10
- Registry-supported keys: 7
- Registry-unsupported keys: 3
- True extraction preview: **yes**
- Existing fact contamination warnings: 0
- Facts that would be created on apply: 5

Dry-run uses read-only document extraction preview (`extractFromBrandSourceDocument`) — no Airtable fact writes in dry-run.

## Target Fact Keys

| Priority | Field key | Registry supported | Display label |
|----------|-----------|-------------------|---------------|
| P0 | `be.identity.brandName` | yes | Brand Name |
| P0 | `be.identity.parentCompany` | yes | Parent Company |
| P0 | `be.positioning.summary` | yes | Brand Positioning |
| P1 | `be.positioning.tagline` | yes | Brand Tagline |
| P1 | `be.positioning.guestPromise` | yes | Brand Customer Promise |
| P1 | `be.overview.typicalUseCase` | yes | Typical Use Case |
| P2 | `be.development.conversionRelevance` | **no** | — |
| P2 | `be.development.ownerConsiderations` | **no** | — |
| P2 | `be.footprint.globalHotels` | yes | Global Hotel Count |
| P2 | `be.footprint.regionalPresence` | **no** | — |

## Existing Facts (by source / key)

- `recy2pyEahF9UUsEk` · `be.footprint.globalHotels` — 6 fact(s) `recD61KjKSlrboU76`, `recEB09MUDJAIMAXo`, `recF9DXScZrXcoS4D`, `recORMNCIWDC33viM`, `recr6hp6Q1UsM998j`, `recz6RQZcNA8DdSOJ`

## Source: 2025 US Curio FDD (`recy2pyEahF9UUsEk`)

- Validation: pass
- Document kind: pdf
- Classification role: fdd
- Raw candidates from extractor: 3

### Preview candidates

#### `be.identity.brandName` (P0)

- Extracted preview: Curio Collection by Hilton
- Evidence preview: …on Exhibit J - 2 under our StiR Creative Collective program (the “Restaurant Brands”), in the US to franchisees of Canopy by Hilton, Curio Collection by Hilton, Hilton Hotels and Resorts, Hi lton Gar
- Extraction: Directly Stated · Confidence: High

#### `be.identity.parentCompany` (P0)

- Extracted preview: Hilton Worldwide
- Evidence preview: …lton”). Hilton’s parent company is Hilton Worldwide Holdings Inc., a Delaware corporation formed on March 18, 2010 (NYSE: HLT) (“Hilton Worldwide”). The principal business address of both companies i
- Extraction: Directly Stated · Confidence: High

#### `be.footprint.globalHotels` (P2)

- Extracted preview: Not confirmed in available sources.
- Evidence preview: Not confirmed in available sources.
- Extraction: Needs Confirmation · Confidence: Low

## Source: Curio Collection Fact Sheet May 2026 (`recL1qfHCOAUZr9Rz`)

- Validation: pass
- Document kind: md
- Classification role: overview_en
- Raw candidates from extractor: 2

### Preview candidates

#### `be.identity.brandName` (P0)

- Extracted preview: Curio Collection by Hilton
- Evidence preview: # Curio Collection by Hilton — Fact Sheet (structured extract) **Source:** https://stories.hilton.com/curio-collection-by-hilton-fact-sheet…
- Extraction: Directly Stated · Confidence: High

#### `be.identity.parentCompany` (P0)

- Extracted preview: Hilton Worldwide
- Evidence preview: Fixed from brand registry (Hilton/press/Curio Collection Fact Sheet May 2026.md) for Curio Collection by Hilton
- Extraction: Directly Stated · Confidence: Medium

## Would Write On Apply

- Fact rows (clean): 5
- Fact rows blocked by contamination preview: 0
- Unsupported registry keys (skipped): `be.development.conversionRelevance`, `be.development.ownerConsiderations`, `be.footprint.regionalPresence`

**Does not write:**
- Approved for Explorer Use
- Human Review Status (facts remain Pending)
- Brand Basics profile governance fields
- Company Validated / Company Validation Date
- External Display Status / Show Trust Label

## Apply Result

- Run ID: `pi-curio-clean-11b9312f`
- Sources patched: 2
- Facts created: 5
- Skipped: 0

## Post-Apply Recommendation

```bash
npm run steward-partner-intelligence -- --entity-type brand --target-rec-id receQkxgjlezsc1xg --dry-run
```

_Manually review extracted facts before any approval._
