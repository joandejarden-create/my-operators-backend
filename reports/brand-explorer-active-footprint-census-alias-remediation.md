# Brand Explorer — Census Alias Gap Remediation (Hidden 18)

Generated: 2026-08-03
Companion to `brand-explorer-active-footprint-census-alias-gaps.md`.
Read-only findings. No Airtable writes in this pass.

## Root cause

Hidden Footprint Metrics for these 18 brands is **not** missing Brand Alias rows in most cases.

- **17/18** already have Active alias rows, but `Alias / Source Brand Name` does not match Hotel Census `Affiliation` strings (exact match required) → `recordsMatched=0` → `fallbackRecommended=true` → Demo/Mock hero blocks MVP rescue → tables hidden.
- **1/18 (`SO/`)** has no alias row at all (`NO_ALIAS_FOR_REQUESTED_BRAND`).

## Tier A — Alias string mismatch (census inventory exists)

Safe next step: add/activate an additional Active alias Source pointing at the real Affiliation (keep Canonical = Brand Basics name). Dry-run before apply.

| Brand | Current matcher | Add Alias / Source | Census open | Note |
| --- | --- | --- | ---: | --- |
| Aloft Hotels | `Aloft Hotels` | `aloft Hotel` | 15 | Census spelling (lowercase a + singular Hotel) |
| avid hotels | `avid hotels` | `Avid` | 3 | Census short form |
| Kimpton Hotels | `Kimpton` | `Kimpton Hotels` | 7 | Matcher too short; census Affiliation is plural Hotels |
| Residence Inn by Marriott | `Residence Inn by Marriott` | `Residence Inn` | 9 | Strip by-operator suffix |
| Sheraton | `Sheraton` | `Sheraton Hotel` | 28 | Do **not** map `Four Points by Sheraton` |
| Voco Hotels | `Voco Hotels` | `voco` | 3 | Census lowercase short form |
| Moxy Hotels | `Moxy Hotels` | `MOXY` | 0 open / 5 rows | Pipeline-only today; alias still needed for future open unlock |

## Tier B — No safe census Affiliation in current Hotel Census

No trustworthy Affiliation hit in the current census set (or only wrong fuzzy hits). Do **not** invent Affiliation options.

Options: (1) ingest/backfill census properties with correct Affiliation, or (2) unlock via Footprint Data Status `Verified`/`Estimated` + clear Demo/Mock hero labels when Brand Setup figures are real.

| Brand | Slug | Parent | Near misses (do not auto-map) |
| --- | --- | --- | --- |
| Bunkhouse Hotels | `bunkhouse-hotels` | Hyatt Hotels Corporation | — |
| Even Hotels | `even-hotels` | InterContinental Hotels Group | Evenia (wrong brand) |
| Everhome Suites | `everhome-suites` | Choice Hotels International | ME (Melia — wrong) |
| Preferred Hotels & Resorts | `preferred-hotels-and-resorts` | Preferred Hotels & Resorts | Iberostar / Dreams / Secrets (wrong) |
| SO/ | `so-hotels-and-resorts` | AccorHotels | Sofitel (wrong — softitel ≠ SO/) |
| SpringHill Suites by Marriott | `springhill-suites-by-marriott` | Marriott International, Inc. | Marriott / City Express bleed only |
| StudioRes | `studiores` | Marriott International, Inc. | — |
| Suburban Studios | `suburban-studios` | Choice Hotels International | — |
| Tempo by Hilton | `tempo-by-hilton` | Hilton Worldwide | Hilton family bleed only |
| TownePlace Suites by Marriott | `towneplace-suites-by-marriott` | Marriott International, Inc. | Marriott family bleed only |
| WoodSpring Suites | `woodspring-suites` | Choice Hotels International | — |

### SO/ special

- Gap class: `alias_gap`
- No Brand Alias Mapping row; matcher falls back to exact `SO/` (0 census rows).
- Sofitel is **not** SO/ — do not map.
- Steward: confirm real census Affiliation for SO/ properties (if any), then create Canonical `SO/` + Source alias.

## Suggested order

1. Tier A alias expansions (Aloft, Avid, Kimpton, Residence Inn, Sheraton, voco, Moxy) — highest unlock ROI.
2. Re-run `node scripts/audit-brand-explorer-active-footprint-display-gate.mjs` — expect hidden count to drop (~18 → ~11).
3. Tier B: census backfill plan **or** MVP footprint verification path (no invented Affiliations).

## Reports

- `reports/brand-explorer-active-footprint-census-alias-gaps.md`
- `reports/brand-explorer-active-footprint-census-alias-gaps.json`
- `reports/brand-explorer-active-footprint-census-alias-fuzzy.json`
- Re-run diagnosis: `node scripts/diagnose-active-footprint-census-alias-gaps.mjs`
