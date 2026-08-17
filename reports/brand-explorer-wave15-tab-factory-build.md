# Brand Explorer Wave 15 — Tab Factory Build

Generated: 2026-08-03T22:30:24.906Z
Dry-run: **false** · Applied: **true**
Scope: **hilton-hotels-and-resorts, homewood-suites-by-hilton, home2-suites-by-hilton, tru-by-hilton, doubletree-by-hilton, hampton-by-hilton, hilton-garden-inn, spark-by-hilton**
Presentation writes planned: **742**
Brand Basics writes planned: **8**
TGS validated: **true**
Blocked: **none**

## Post-apply acceptance notes

- Ready statement: `wave15_stage4_content_clean_ready_for_image_materialization`
- All eight remain **Under Review** / factory preview only (not Active/Live, no release fields)
- No images written (Stage 5)
- No Brand Status / release / CV / Source Library / Registry writes on Wave 15 targets
- Protected 54: Marriott Hotels Brand Name drift (`Marriott` → slug `marriott`) was restored to **Marriott Hotels** so freeze slug `marriott-hotels` reconciles; Wave 15 did not write that brand

### Accepted steward / Stage-5 gaps (not Stage 4 content blockers)

- `overview.scenario.1–3` fail rendered completeness / tab-factory audit **only** as “Scenario card missing image” — copy bodies are present (verified live); images deferred to Stage 5 materialization
- `snapshot.typical_keys:cleanly_unavailable` on some brands — Brand Basics portfolio/typical-keys steward gap, not Presentation copy
- Homewood / Home2 / Tru / Spark: International Reference openings/momentum (no verified CALA property yet)
- DoubleTree / Hampton / HGI: CALA openings use verified hilton.com hotel URLs; some momentum still cites trade press / Stories where dated openings lack primary confirmation

### Validation snapshot

- Golden content quality: PASS 8/8
- No-empty rendered components: PASS 8/8
- Tab-factory audit: ran for all 8; failFindings = missing scenario images only (accepted until Stage 5)
- Rendered completeness: same image + typical_keys gaps only

## Scope controls

- the-house-of-originals: excluded_from_stage4
- morgans-originals: not_created_not_modified
- so-hotels-and-resorts: undefined
- fairmont-hotels-and-resorts: undefined

## Brands

| Slug | Name | Rows | Pres writes | Basics writes | Blocked |
| --- | --- | ---: | ---: | ---: | --- |
| `hilton-hotels-and-resorts` | Hilton Hotels & Resorts | 95 | 95 | 1 | false |
| `homewood-suites-by-hilton` | Homewood Suites by Hilton | 93 | 93 | 1 | false |
| `home2-suites-by-hilton` | Home2 Suites by Hilton | 92 | 92 | 1 | false |
| `tru-by-hilton` | Tru by Hilton | 92 | 92 | 1 | false |
| `doubletree-by-hilton` | DoubleTree by Hilton | 93 | 93 | 1 | false |
| `hampton-by-hilton` | Hampton by Hilton | 92 | 92 | 1 | false |
| `hilton-garden-inn` | Hilton Garden Inn | 93 | 93 | 1 | false |
| `spark-by-hilton` | Spark by Hilton | 92 | 92 | 1 | false |
