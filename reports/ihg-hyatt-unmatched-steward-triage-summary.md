# IHG + Hyatt unmatched steward triage

**Generated:** 2026-07-23  
**Mode:** Official sources only · fill-blank · no invented Property IDs

## Snapshot

| Brand | Website+Property ID cumulative | New this pass | Still unmatched |
|-------|-------------------------------:|--------------:|----------------:|
| IHG | 252 | 0 | 76 |
| Hyatt | 125 | 13 | 59 |

Amenities: IHG highlights already applied earlier; Hyatt still blocked (429/Kasada).

## IHG buckets (76)

| Bucket | Count | Next action |
|--------|------:|-------------|
| `pipeline_or_unopened` | 45 | Re-extract when hoteldetail appears |
| `name_variant_needs_human` | 11 | Confirm before bind |
| `alliance_not_on_ihg_directory` | 10 | Leave Six Senses/alliance stewarded |
| `census_duplicate_pid_claimed` | 7 | Deduplicate census rows |
| `missing_from_public_listing` | 2 | Monitor public directory |
| `no_destination_page_market` | 1 | St Kitts — sitemap only |

Reports: `reports/ihg-census-unmatched-steward-triage.csv` / `.json`

## Hyatt buckets (59)

| Bucket | Count | Next action |
|--------|------:|-------------|
| `classic_hyatt_missing_from_archive` | 19 | Steward-paste when live hyatt.com allows |
| `inclusive_missing_from_wayback` | 18 | Same |
| `census_duplicate_after_1to1` | 8 | Deduplicate |
| `nonstandard_or_unbound_name` | 8 | Confirm Affiliation first |
| `pipeline_or_unopened` | 5 | Revisit after opening |
| `hard_exclusion_name_conflict` | 1 | Keep Cariari ≠ Pinares |

Reports: `reports/hyatt-census-unmatched-steward-triage.csv` / `.json`

## This pass recovery

- **IHG:** No safe auto-apply (island destination pages 404; remaining need human/pipeline).
- **Hyatt:** +13 from Inclusive brand-align fix on slug-stripped hyatt.com URLs (CDX already exhausted).

## Change impact

**High** for the 13 Hyatt Website/Property ID writes. Rollback: clear fields for IDs in `reports/hyatt-census-enrichment-apply-log.csv` from this batch.
