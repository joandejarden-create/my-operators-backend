# Wave 15 Stage 6 — Post-Image Cleanup

- Generated: 2026-08-04T20:38:55.847Z
- Mode: **APPLY**
- Ready: **`wave15_post_image_cleanup_ready_for_founder_review`**
- Patches planned: **8** · Writes: **8**
- Protected 54 identity preflight: **PASS**
- All eight Under Review: **PASS**

## Brand results

- **Hilton Hotels & Resorts** (`hilton-hotels-and-resorts`): patches=1 · typical_keys 200–1000 rooms → 120–450 rooms
- **Homewood Suites by Hilton** (`homewood-suites-by-hilton`): patches=1 · typical_keys 200–1000 rooms → 80–160 rooms · holds: international_reference_openings
- **Home2 Suites by Hilton** (`home2-suites-by-hilton`): patches=1 · typical_keys 200–1000 rooms → 90–160 rooms · holds: international_reference_openings
- **Tru by Hilton** (`tru-by-hilton`): patches=1 · typical_keys 85–120 rooms → 40–120 rooms · holds: international_reference_openings
- **DoubleTree by Hilton** (`doubletree-by-hilton`): patches=1 · typical_keys (blank) → 120–450 rooms
- **Hampton by Hilton** (`hampton-by-hilton`): patches=1 · typical_keys (blank) → 70–200 rooms
- **Hilton Garden Inn** (`hilton-garden-inn`): patches=1 · typical_keys (blank) → 90–250 rooms
- **Spark by Hilton** (`spark-by-hilton`): patches=1 · typical_keys (blank) → 60–120 rooms · holds: international_reference_openings

## snapshot.typical_keys handling

| Brand | Before | After | Handling |
| --- | --- | --- | --- |
| Hilton Hotels & Resorts | 200–1000 rooms | 120–450 rooms | typical_keys_stale_portfolio_mismatch |
| Homewood Suites by Hilton | 200–1000 rooms | 80–160 rooms | typical_keys_stale_portfolio_mismatch |
| Home2 Suites by Hilton | 200–1000 rooms | 90–160 rooms | typical_keys_stale_portfolio_mismatch |
| Tru by Hilton | 85–120 rooms | 40–120 rooms | typical_keys_stale_portfolio_mismatch |
| DoubleTree by Hilton | (blank) | 120–450 rooms | typical_keys_blank_cleanly_unavailable |
| Hampton by Hilton | (blank) | 70–200 rooms | typical_keys_blank_cleanly_unavailable |
| Hilton Garden Inn | (blank) | 90–250 rooms | typical_keys_blank_cleanly_unavailable |
| Spark by Hilton | (blank) | 60–120 rooms | typical_keys_blank_cleanly_unavailable |

## Protections

- No Brand Status / release / CV / Source / Registry / public restore writes
- No protected 54 / Marriott Hotels / Four Points Flex / House of Originals / Morgans / Radisson Collection writes
- No broad Presentation rewrites; no new image materialization
- All eight remain Under Review / factory preview

## Apply flags

- `--approve-wave15-post-image-content-cleanup`
- `--confirm-eight-brand-stage6-scope`
- `--confirm-target-brands-only`
- `--confirm-all-eight-remain-under-review`
- `--confirm-snapshot-typical-keys-handled`
- `--confirm-no-brand-status-changes`
- `--confirm-no-release-field-writes`
- `--confirm-no-company-validation-changes`
- `--confirm-no-source-library-status-changes`
- `--confirm-no-registry-approval-changes`
- `--confirm-no-public-restore-registry-changes`
- `--confirm-no-protected-54-brand-changes`
- `--confirm-no-marriott-hotels-writes`
- `--confirm-no-four-points-flex-writes`
- `--confirm-no-house-of-originals-writes`
- `--confirm-no-morgans-originals-writes`
- `--confirm-no-radisson-collection-changes`
- `--confirm-no-broad-rewrites`
- `--confirm-no-wrong-brand-images`
- `--confirm-no-sibling-brand-images`
- `--confirm-hilton-brand-family-separated`
- `--confirm-no-internal-source-language`
- `--confirm-no-raw-urls`
- `--confirm-recent-momentum-semantics-preserved`
- `--confirm-portfolio-mix-structured`
- `--confirm-openings-use-actual-property-names`
- `--confirm-geo-footprint-source-supported-or-cleanly-unavailable`

## Founder review note

Eight Wave 15 Hilton brands remain Under Review / factory preview. Stage 6 synced Project Fit room ranges into Portfolio so Typical Keys Range renders. Homewood / Home2 / Tru / Spark openings remain International Reference where CALA is unconfirmed. Do not promote Brand Status yet.

## Post-apply validation

| Gate | Result |
| --- | --- |
| Tab-factory audit | PASS (failFindings=0) |
| Rendered-field completeness | PASS 8/8 (typical_keys cleared) |
| No-empty | PASS 8/8 |
| Golden | PASS 8/8 |
| Recent Momentum evidence | PASS 8/8 (after Wave 15 `CALA_AVAILABLE_BY_SLUG` registration) |
| Image uniqueness | PASS 8/8 |
| Image role-match | PASS 8/8 |
| Protected 54 baseline | PASS (after Wave 14 `EXTRA_ACTIVE_IDENTITY_ANCHORS`) |
| Global active semantic | PASS · Active 54 · C/H/M=0 |

## typical_keys after sync

| Brand | Typical Keys |
| --- | --- |
| Hilton Hotels & Resorts | 120–450 rooms |
| Homewood Suites by Hilton | 80–160 rooms |
| Home2 Suites by Hilton | 90–160 rooms |
| Tru by Hilton | 40–120 rooms |
| DoubleTree by Hilton | 120–450 rooms |
| Hampton by Hilton | 70–200 rooms |
| Hilton Garden Inn | 90–250 rooms |
| Spark by Hilton | 60–120 rooms |

Ready: `wave15_post_image_cleanup_ready_for_founder_review`
