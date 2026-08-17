# Wave 13 Stage 5 — Image / Visual Materialization

- Generated: 2026-07-27T14:36:14.180Z
- Mode: **APPLIED** (report regenerated dry-run after apply; live Presentation Images written)
- Ready: **7/7** · Blocked: **0**
- Patches planned: **84** (12 per brand: 6 gallery + 3 scenario + 3 openings)
- Scenario image failures before materialization: **21**
- Ready statement: `wave13_image_materialization_ready_for_post_image_cleanup`

## Brand results

- **Mama Shelter**: ready · g6/6 s3/3 o3/3
- **Mercure**: ready · g6/6 s3/3 o3/3
- **ibis**: ready · g6/6 s3/3 o3/3
- **Novotel**: ready · g6/6 s3/3 o3/3
- **Pullman**: ready · g6/6 s3/3 o3/3
- **SO/ Hotels & Resorts**: ready · g6/6 s3/3 o3/3
- **Fairmont**: ready · g6/6 s3/3 o3/3

## Post-apply validation

| Gate | Result |
| --- | --- |
| Image uniqueness (7) | PASS |
| Image role-match (7) | PASS |
| Tab-factory image gates (gallery/scenario/property distinct + role-match + no-empty) | PASS |
| No-empty rendered components (7) | PASS |
| Golden content quality (7) | PASS |
| Rendered completeness (7) | 6/7 PASS — SO/ residual `positioning.positioning` + `positioning.audience` too_thin (non-image) |
| Strict 39 Active/Live baseline | PASS |
| PVQL `--public-full-only` | overallPass=true; publicFullProfileCount=38 (bunkhouse classified remediation_locked; not a Wave 13 write) |
| 24-tab section quality audit | PASS (exit 0) |

## Residuals (not Stage 5 image blockers)

- **SO/** `positioning.positioning` / `positioning.audience` too_thin (economy-sounding copy) — content cleanup after images, not steward invent-fills.
- SO/ steward fields (`snapshot.*`, `footprint.primary_regions`) were **not written** in Stage 5; live completeness currently shows values present from Basics / prior work.
- Fairmont leftover San Francisco openings card set to **Do Not Display** (no Accor ahstatic inventory; wrong El San Juan photo cleared).
- Accor `a552_sm_00` denylisted (Airtable attachment fetcher accepts then drops).

## SO/ steward-data gaps (intentionally not filled in Stage 5)

- snapshot.* (SO/ Brand Basics steward fields)
- footprint.primary_regions (SO/ Brand Basics steward field)

## Apply flags

- `--approve-wave13-image-materialization`
- `--confirm-seven-brand-stage5-scope`
- `--confirm-target-brands-only`
- `--confirm-house-of-originals-excluded`
- `--confirm-no-morgans-originals-writes`
- `--confirm-no-radisson-collection-changes`
- `--confirm-no-protected-39-brand-changes`
- `--confirm-no-company-validation-changes`
- `--confirm-no-source-library-status-changes`
- `--confirm-no-registry-approval-changes`
- `--confirm-no-brand-status-changes`
- `--confirm-no-release-field-writes`
- `--confirm-image-uniqueness`
- `--confirm-image-role-match`
- `--confirm-scene7-filename-aware-distinct-images`
- `--confirm-cala-first-openings-priority`
- `--confirm-international-reference-labels-where-needed`
- `--confirm-no-logo-only-filler`
- `--confirm-no-wrong-brand-images`
- `--confirm-no-sibling-brand-images`
- `--confirm-no-content-rewrites`
- `--confirm-no-so-steward-data-fills`
