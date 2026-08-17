# Protected 27 PVQL Re-Green

Version: `27-protected-pvql-regreen-v1` · Generated: 2026-07-24T12:18:20.434Z
Applied: **true**

## Targets (3 protected brands only)

- `preferred-hotels-and-resorts`
- `radisson-individuals-by-choice`
- `small-luxury-hotels-of-the-world`

## Summary

| Metric | Count |
| --- | ---: |
| Brands | 3 |
| Needing fix | 3 |
| Patches | 3 |
| Projected clean | true |

## Patches

| Brand | Record ID | Field | Before | After | Reason |
| --- | --- | --- | --- | --- | --- |
| Preferred Hotels & Resorts | `recwl5JOYxlChuCAr` | Target Guest Segments | Luxury / Discerning, Leisure, Experience-Oriented, International Inbound | Experience-Oriented, Leisure, International Inbound | golden_generic_audience_prose_segment_adjacency |
| Radisson Individuals by Choice | `recRyvM8OmLlDj9G7` | Target Guest Segments | Luxury / Discerning, Leisure, Experience-Oriented | Experience-Oriented, Leisure | golden_generic_audience_prose_segment_adjacency |
| Small Luxury Hotels of the World | `recjjSnY2opb8P4DG` | Target Guest Segments | Luxury / Discerning, Leisure, Experience-Oriented, International Inbound | Experience-Oriented, Leisure, International Inbound | golden_generic_audience_prose_segment_adjacency |

## Protections

- Company Validated untouched
- Source Library untouched
- Registry untouched
- Brand Status untouched
- Release fields untouched
- Images untouched
- Wave 12 brands untouched
- No Presentation broad rewrites

## Wave 12 gate

**Wave 12 may resume at Stage 3 (source packs).** Stages 1–2 remain complete; Stages 3–11 were blocked only by this PVQL failure and are now unblocked.

## Post-apply validation (2026-07-24)

| Gate | Result |
| --- | --- |
| PVQL public-full-only | **PASS** (`overallPass=true`, publicFull=27) |
| Preferred / RI / SLH PVQL | **PASS** (`tab_factory_audit` + `generic_copy_scan`) |
| Tab-section quality audit | **PASS** |
| Recent momentum evidence | **PASS** |
| Mandatory release gates | **PASS** |
| OS release-readiness (dry-run) | **PASS** (read-only) |
| 27 baseline regression (tightened) | **PASS** |
| Wave 12 factory preflight | **PASS** (`protectedBaselineClean=true`, Active/Live=27, no Wave12 Active drift) |

## Per-brand reports

- `reports/brand-explorer-27-protected-pvql-regreen-preferred.md`
- `reports/brand-explorer-27-protected-pvql-regreen-radisson-individuals.md`
- `reports/brand-explorer-27-protected-pvql-regreen-slh.md`

