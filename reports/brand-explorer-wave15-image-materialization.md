# Wave 15 Stage 5 — Image / Visual Materialization

- Generated: 2026-08-04T16:45:13.453Z
- Mode: **APPLY**
- Ready: **8/8** · Blocked: **0**
- Patches planned: **96**
- Protected 54 identity preflight: **PASS**

## Brand results

- **Hilton Hotels & Resorts**: ready · g6/6 s3/3 o3/3
- **Homewood Suites by Hilton**: ready · g6/6 s3/3 o3/3
- **Home2 Suites by Hilton**: ready · g6/6 s3/3 o3/3
- **Tru by Hilton**: ready · g6/6 s3/3 o3/3
- **DoubleTree by Hilton**: ready · g6/6 s3/3 o3/3
- **Hampton by Hilton**: ready · g6/6 s3/3 o3/3
- **Hilton Garden Inn**: ready · g6/6 s3/3 o3/3
- **Spark by Hilton**: ready · g6/6 s3/3 o3/3

## Apply flags

- `--approve-wave15-image-materialization`
- `--confirm-eight-brand-stage5-scope`
- `--confirm-target-brands-only`
- `--confirm-protected-54-identity-preflight-passed`
- `--confirm-no-protected-54-brand-changes`
- `--confirm-no-marriott-hotels-writes`
- `--confirm-no-four-points-flex-writes`
- `--confirm-no-house-of-originals-writes`
- `--confirm-no-morgans-originals-writes`
- `--confirm-no-radisson-collection-changes`
- `--confirm-no-company-validation-changes`
- `--confirm-no-source-library-status-changes`
- `--confirm-no-registry-approval-changes`
- `--confirm-no-brand-status-changes`
- `--confirm-no-release-field-writes`
- `--confirm-no-content-rewrites`
- `--confirm-image-uniqueness`
- `--confirm-image-role-match`
- `--confirm-no-wrong-brand-images`
- `--confirm-no-sibling-brand-images`
- `--confirm-hilton-brand-family-separated`
- `--confirm-hilton-hotels-not-hilton-corporate`
- `--confirm-homewood-not-home2`
- `--confirm-home2-not-homewood-or-tru`
- `--confirm-tru-not-spark-or-hampton`
- `--confirm-spark-not-tru-or-hampton`
- `--confirm-cala-first-openings-priority`
- `--confirm-americas-reference-before-international-reference`
- `--confirm-property-url-matches-required-for-named-gallery`
- `--confirm-cleanly-unavailable-for-unsupported-property-images`

## Post-apply validation

| Gate | Result |
| --- | --- |
| Image uniqueness | PASS 8/8 |
| Image role-match | PASS 8/8 |
| Tab-factory audit | PASS (auditPass=true; steward `snapshot.typical_keys` only on DT/Hampton/HGI/Spark) |
| Rendered-field completeness | Scenario images cleared; remaining fails = `snapshot.typical_keys:cleanly_unavailable` only (steward, out of Stage 5 scope) |
| No-empty rendered components | PASS 8/8 |
| Golden content quality | PASS 8/8 |
| Protected 54 baseline | PASS |
| Global active semantic audit | Critical/High/Medium = 0 · Active = 54 |
| Brand Status (live) | All 8 remain **Under Review** |

## Hilton CDN ingest note

Hilton `hilton.com/im/` URLs must not go through wsrv (404). Bare Hilton CDN returns ~2KB thumbnails; Stage 5 appends `impolicy=crop&…rw=1600&rh=1067` so Airtable stores usable JPEGs (`toAirtableFetchableImageUrl`).

## Ready

`wave15_image_materialization_ready_for_post_image_cleanup`
