# Brand Registry Resolution v1

**Status:** `production_census_brand_registry_resolution_v1_partial_remaining`
**Objective:** `brand-registry-resolution-v1`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Brand Setup writes:** false
**Brand Explorer writes:** false

## Session cumulative (before → after)

| Metric | Prior (source-confirmed-v2) | After brand-registry-v1 |
| --- | ---: | ---: |
| Unknown brands (not in official census registry) | 202 | 16 |
| Human Review Required | 24 | 16 |
| brand_code_unresolved | 20 | 0 |
| Clean Core pass (approx) | 773 | 784 |
| Excluded from Clean Core | 318 | 307 |
| Dirty partner labels | — | 16 |
| Brand Setup promotion candidates | — | 9 |

## What was written (Hotel Property Census only)

- Accor code remaps: BAN→Banyan Tree, ANG→Angsana, HYD→Hyde, MOD→Mondrian, SLS→SLS, SOU→Handwritten Collection
- Wyndham locale decode: Pt Br → Registry Collection (via URL slug, not locale)
- JOIA / Garner / Apartments / lifestyle brands normalized into official census registry
- Dirty partner labels stewarded (HR + Radar Hold): SAM, IHG Partner / Spnd, Choice Hotels, Marriott Bonvoy — Brand Unconfirmed
- No address / coords / phone / rooms / owner / operator / dates
- Brand Setup / Brand Explorer untouched

## Brand Setup promotion candidates (read-only)

See `reports/research-engine-v2/production-census-brand-setup-promotion-candidates.md`.

- **Banyan Tree** ×4 → `promote_to_brand_setup`
- **JOIA Iberostar** ×2 → `promote_to_brand_setup`
- **SLS** ×2 → `promote_to_brand_setup`
- **Mondrian** ×2 → `promote_to_brand_setup`
- **Apartments by Marriott Bonvoy** ×2 → `promote_to_brand_setup`
- **Angsana** ×1 → `promote_to_brand_setup`
- **TRIBE** ×1 → `promote_to_brand_setup`
- **Garner** ×1 → `promote_to_brand_setup`
- **Hyde** ×1 → `promote_to_brand_setup`

## Unresolved steward (no safe Brand write)

- SAM (Accor managed-by / By Accor) — dirty_partner_label
- IHG Partner / Spnd — dirty_partner_label
- Choice Hotels (generic partner path) — dirty_partner_label
- Marriott Bonvoy — Brand Unconfirmed — dirty_partner_label

## Chained source-confirmed-census-v2

- Ran automatically after brand-registry-resolution-v1
- Status: `production_census_source_confirmed_census_v2_partial_steward_remaining`
- Clean Core after chain: 784

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No address / coords / phone / rooms
- No owner/operator/date writes
- No opaque code guessing / no hotel-name-only brand inference
- Evidence-backed non-active brands reported in promotion pack, not forced into Active dictionary
- Unresolved dirty labels excluded from Clean Core

## Validation

- `npm run test:census-autopilot` — pass
- `npm run dealality:batch-learning-audit` — pass
