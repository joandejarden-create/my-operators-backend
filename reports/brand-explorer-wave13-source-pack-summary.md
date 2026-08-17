# Wave 13 — Official Source Pack Summary

Version: `wave13-source-packs-v1` · Generated: 2026-07-26T22:07:48.901Z
Dry-run: **true** · Airtable writes: **false**
Ready: `wave13_source_packs_ready_with_open_items`

## Gate

- Source: `same_session_reports`
- Reused fresh reports: **false**
- Preflight: `protected_39_live_clean_wave13_may_resume`
- Manifest: `wave13_manifest_ready_for_factory_preview_cohort_with_open_items`
- Cohort: `wave13_factory_preview_cohort_applied`

## Acceptance

| Check | Result |
| --- | --- |
| Packs created | 8/8 |
| Packs valid | 8/8 |
| Official brand pages | 8/8 |
| Brands with CALA examples | 6 |
| Brands International Reference only | 2 |
| SO/ Brand Basics created | **false** (recommendation only) |
| Fairmont renamed | **false** (documented only) |
| Protected 39 changes | **false** |
| May proceed to SO/ Brand Basics creation | **true** |
| May proceed to Stage 4 tab-factory-build | **false** |

**Stage 3 PASS (with open items)** — see Stage 4 blockers before content generation.

## Stage 4 blockers

- so-hotels-and-resorts missing Brand Basics — create in separate stage first
- the-house-of-originals official status likely superseded by Morgans Originals — founder/manual review required

## Per-brand packs

| Brand | Basics ID | Status | CALA avail | Props (CALA/Intl) | Momentum | TGS | Pack | Valid |
| --- | --- | --- | --- | ---: | ---: | --- | --- | --- |
| Mama Shelter | `recXCZCK05XXYX7Q8` | Under Review | pipeline | 1/1 | 2 | Experience-Oriented, Leisure, Bleisure | [`brand-explorer-wave13-source-pack-mama-shelter.md`](reports/brand-explorer-wave13-source-pack-mama-shelter.md) | PASS |
| Mercure | `recevrLJ3m6rIug3S` | Under Review | strong | 2/1 | 2 | Leisure, Bleisure, International Inbound | [`brand-explorer-wave13-source-pack-mercure.md`](reports/brand-explorer-wave13-source-pack-mercure.md) | PASS |
| ibis | `reclFXbpZ5XzLWbGP` | Under Review | strong | 2/0 | 2 | Leisure, Bleisure, International Inbound | [`brand-explorer-wave13-source-pack-ibis.md`](reports/brand-explorer-wave13-source-pack-ibis.md) | PASS |
| Novotel | `recQE2lSSSSyuUrMQ` | Under Review | strong | 2/0 | 2 | Bleisure, Leisure, International Inbound | [`brand-explorer-wave13-source-pack-novotel.md`](reports/brand-explorer-wave13-source-pack-novotel.md) | PASS |
| Pullman | `recFW9kfqKfOjv7Z1` | Under Review | strong | 1/1 | 2 | Bleisure, Experience-Oriented, International Inbound | [`brand-explorer-wave13-source-pack-pullman.md`](reports/brand-explorer-wave13-source-pack-pullman.md) | PASS |
| SO/ Hotels & Resorts | `null` | — | none_found | 0/2 | 2 | Experience-Oriented, International Inbound | [`brand-explorer-wave13-source-pack-so-hotels-and-resorts.md`](reports/brand-explorer-wave13-source-pack-so-hotels-and-resorts.md) | PASS |
| Fairmont | `recJhPaDVU3YUDQUt` | Under Review | strong | 1/1 | 2 | Experience-Oriented, International Inbound | [`brand-explorer-wave13-source-pack-fairmont-hotels-and-resorts.md`](reports/brand-explorer-wave13-source-pack-fairmont-hotels-and-resorts.md) | PASS |
| The House of Originals | `rec7ZPOVYsldGmNfx` | Under Review | none_found | 0/2 | 2 | Experience-Oriented, International Inbound | [`brand-explorer-wave13-source-pack-the-house-of-originals.md`](reports/brand-explorer-wave13-source-pack-the-house-of-originals.md) | PASS |

## Target Guest Segments rule

Do not combine Luxury / Discerning with Leisure (or Experience-Oriented adjacency that renders as generic audience prose). Prefer brand-specific Bleisure / Experience-Oriented / Leisure / International Inbound only when source-supported.

Recommendations are recorded for a later approved Brand Basics patch stage — **not written now**.

## Protections

- No Airtable writes
- No Presentation / Brand Status / release field writes
- No Company Validated / Source Library / Registry writes
- No protected 39 Presentation or Basics changes
- No SO/ Brand Basics creation
- No Fairmont rename
- No Wave 13 content generation in this stage

## Next

Stage 3 source packs complete (read-only). Create SO/ Brand Basics separately; resolve House of Originals manual review; then proceed to Stage 4 tab-factory-build for ready brands only.

```bash
npm run brand-explorer-wave13-factory -- --stage source-packs --dry-run
npm run brand-explorer-wave13-factory -- --stage source-packs --dry-run --reuse-fresh-reports
```

