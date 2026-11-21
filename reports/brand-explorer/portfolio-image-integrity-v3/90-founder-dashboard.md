# Brand Explorer — Image Integrity V3 Founder Dashboard

Generated: 2026-11-21T01:42:12.149Z

## Integration / sync

| Item | Value |
|------|-------|
| PR #502 | MERGED |
| Merge commit | `d6dd99ddc588835c67cf46c8a4089b19089878ec` |
| BE home | `C:\Dev\dealality\ws-brand-explorer` on `ws/brand-explorer` |
| HEAD == origin/main | true |
| Port 8080 root | `C:\Dev\dealality\ws-integrated-staging` |
| Gallery delivery fix on 8080 | true |
| Gallery delivery fix on origin/main | false |

## Canonical Airtable (live)

| Metric | Value |
|--------|------:|
| Active/Live brands | 68 |
| Brands with complete 9 image rows | 67 |
| Raw required image rows | 619 |
| Unique brand+slot pairs | 603 |
| Duplicate slot conflicts | 16 |
| IMAGE_TECHNICALLY_VALID (prior probe) | 603 |
| IMAGE_BRAND_VERIFIED (census) | 119 |
| IMAGE_BROWSER_VALIDATED brands | 59 |
| IMAGE_CERTIFIED brands | 0 |
| Sibling rejects | 0 |

### Missing Active/Live brand identity

- **SO/** (`recTJdPlr4mDs9app`)

## Exact-brand identity

| Metric | Value |
|--------|------:|
| Slots evaluated | 603 |
| Census IMAGE_BRAND_VERIFIED | 119 |
| Unverified / census pending | 484 |
| Sibling rejected | 0 |
| Title inferred | 12 |
| Pool matched | 496 |

> Matching parent company is insufficient. Exact verified brand affiliation is required.

## Rights staging

| Stage | Count |
|-------|------:|
| Internal research | 603 |
| Brand Explorer staged | 603 |
| Hotel Explorer blocked | 603 |
| Customer blocked | 603 |

## Authenticated browser certification (CURRENT)

| Metric | Value |
|--------|------:|
| Brands attempted | 68 |
| Fully browser-validated | 59 |
| Pass | 59 |
| Fail | 9 |

Fixtures: AC Hotels, Courtyard, Aloft, Bunkhouse — see `browser-current/*/CERT.json`.

## Shared Verified Hotel Image Library

| Metric | Value |
|--------|------:|
| Total assets | 624 |
| Unique content hashes | 624 |
| Brands represented | 67 |
| Rights Brand Explorer | 624 |
| Rights Hotel Explorer | 0 |
| Customer-facing | 0 |
| Awaiting review | 505 |

## Factory Proposed

| Metric | Value |
|--------|------:|
| Frozen payload files | 230 |
| Brands audited | 230 |
| Six gallery URLs | 160 |
| Three scenario URLs | 159 |
| Gallery gaps | 70 |
| Scenario gaps | 71 |
| Ephemeral CDN slots | 415 |

## Remediation executed

- Successfully imported/repaired slots (apply reports): **22**
- Paid research spend: **$0**

## Persistence / certification

- Browser+census-ready brands (not IMAGE_CERTIFIED): **7** (dazzler-by-wyndham, hampton-by-hilton, hilton-garden-inn, hilton-hotels-and-resorts, holiday-inn-express, preferred-hotels-and-resorts, voco-hotels)


- QUALITY_CERTIFIED: **0**
- IMAGE_CERTIFIED: **0**
- AIRTABLE_PERSISTED: **0**
- Customer publication: **NOT_AUTO**

## Remaining blockers

- 16 duplicate Presentation slot conflicts flagged (no deletes; primary selected)
- Exact-brand census verification incomplete: 119/603 IMAGE_BRAND_VERIFIED
- Authenticated browser CURRENT cert: 59/68 brands fully validated
- IMAGE_RIGHTS: HE display + customer publication remain blocked without explicit per-asset authorization
- QUALITY_CERTIFIED / AIRTABLE_PERSISTED require full content gates + IMAGE_CERTIFIED (not technical completeness alone)
- Gallery relative delivery-URL fix is live on 8080 (ws-integrated-staging overlay) but still uncommitted on origin/main — follow-up PR required
- Factory Proposed ephemeral CDN URLs still require Current rehydration path (V2) before Proposed browser cert

## Contracts

- `BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3`
- `BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1` / `BE-QL-IMAGE-EXACT-BRAND-001`
- `BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1`
- `BE_FACTORY_DUPLICATE_SLOT_RESOLUTION_V1`
- `BE_HOTEL_EXPLORER_VERIFIED_IMAGE_REUSE_V1`
