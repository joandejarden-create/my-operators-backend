# Brand Explorer — Image Integrity V3 Founder Dashboard

Generated: 2026-11-21T00:36:05.261Z

## Canonical Airtable (live probe)

| Metric | Value |
|--------|------:|
| Active/Live brands | 68 |
| Raw required image rows | 603 |
| Unique brand+slot pairs | 603 |
| Duplicate slot conflicts | 16 |
| IMAGE_MISSING | 0 |
| IMAGE_SIGNED_URL_EXPIRED | 0 |
| IMAGE_SIBLING_BRAND | 0 |
| IMAGE_TECHNICALLY_VALID | 603 |
| Brands with all 9 present | 67 |
| Brands with gallery 6 present | 67 |
| Brands with scenario 3 present | 67 |

> Re-run `npm run be-factory-image-integrity-current-audit-v3 -- --probe` after remediation to refresh these counts.

## Remediation executed (this wave)

- Aloft Hotels: 9/9 content-API import + read-back
- Bunkhouse Hotels: 9/9 (initial 2 + retry 7) JPEG durable import + read-back
- Suburban Studios gallery.5: PDF replaced with same-brand gallery reuse + read-back
- Radisson Individuals gallery.6 + SLH scenario.1–2: webp→JPEG re-encode + read-back
- Successfully imported/repaired slots (apply reports): **22**
- Paid research spend: **$0**

## Shared Verified Hotel Image Library

| Metric | Value |
|--------|------:|
| Total assets | 21 |
| Unique content hashes | 21 |
| Brands represented | 4 |
| Rights Brand Explorer | 21 |
| Rights Hotel Explorer | 0 |
| Customer-facing | 0 |
| Awaiting review | 21 |

## Factory Proposed (prior audit snapshot)

| Metric | Value |
|--------|------:|
| Frozen candidates | null |
| Six gallery present | null |
| Three scenario present | null |
| Gallery gaps | 70 |
| Scenario gaps | 71 |

## Persistence / certification

- QUALITY_CERTIFIED (overall factory): **0** this wave (image technical repair ≠ full certification)
- AIRTABLE_PERSISTED (presentation writeback of certified candidates): **0** this wave
- Customer publication: **NOT_AUTO**

## Remaining blockers

- IMAGE_BRAND_VERIFIED / IMAGE_CERTIFIED require census-linked property verification + rights + browser cert
- 16 duplicate Presentation slot conflicts flagged for review (not deleted)
- Factory Proposed ephemeral CDN URLs still require Current rehydration path (V2)
- Authenticated browser certification pending post-merge 8080 sync for remediated brands
- Hotel Explorer display permission remains false until HE product approval per asset

## Contracts

- `BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3`
- `BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1` / `BE-QL-IMAGE-EXACT-BRAND-001`
- `BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1`
- `BE_FACTORY_DUPLICATE_SLOT_RESOLUTION_V1`
- `BE_HOTEL_EXPLORER_VERIFIED_IMAGE_REUSE_V1`
