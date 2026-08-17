# Autopilot Brand Governance Scope Update

**Status:** `production_census_brand_governance_scope_v1`  
**Date:** 2026-08-07  
**Impact:** High (discovery/coverage scope, Census insert governance, Clean Core eligibility)

## Summary

Discovery and coverage reconciliation are no longer limited to Brand Setup Active/Live brands. Official parent-company inventory in the selected region is discovered and classified by **Brand Governance Status**. Owner-facing / public / Dealality product use remains Active/Live (or explicitly approved promotions) only.

## Brand Governance Status values

1. `active_brand_setup`
2. `evidence_backed_non_active_brand`
3. `brand_setup_promotion_candidate`
4. `dirty_partner_label`
5. `brand_code_unresolved`
6. `unsupported_or_ambiguous`

## Census save rules (evidence-backed non-active)

| Field | Value |
| --- | --- |
| Production Use Status | `Census Only / Not Owner-Facing` |
| Public Display Review Status | `Hold` |
| Radar Display Status | `Hold` |
| Human Review Required | `true` unless explicitly approved |

## Clean Core (non-active)

Allowed only when all of:

- Official source confirms brand (census official registry)
- Brand Family is canonical
- Property identity is clean
- Source URL is official (`https://…`)
- Record is `Census Only / Not Owner-Facing`

Human Review may remain true for steward visibility without blocking Clean Core on this path.

## Hard constraints (unchanged)

- Do **not** modify Brand Setup automatically
- Do **not** modify Brand Explorer
- Do **not** force non-active brands into an existing Active brand
- Do **not** drop official hotels because the brand is not currently Active
- Create/maintain Brand Setup **promotion decision pack** only (read-only vs Brand Setup)

## Modules

| Module | Role |
| --- | --- |
| `lib/research-engine-v2/census-brand-governance.js` | Classifier, Census-only fields, Clean Core eligibility, official inventory control list, promotion pack writer |
| `lib/research-engine-v2/census-autopilot-source-discovery.js` | Default official inventory discovery; governance on inserts |
| `lib/research-engine-v2/census-autopilot-coverage-reconciliation.js` | Official inventory control list + `requireBrandMatch: false` |
| `lib/research-engine-v2/census-brand-normalization.js` | Brand SoT: Active first; Census Only path for non-active official |
| `lib/research-engine-v2/census-map-contact-size-readiness.js` | Non-active Clean Core gate + HR exception |
| `lib/research-engine-v2/census-autopilot-apply-guard.js` | Scope `official-parent-inventory` (default without `--parent-company`) |
| `lib/research-engine-v2/census-autopilot-brand-registry-resolution-v1.js` | Promotion pack via governance writer |

## CLI scope

- Default (no parent): `--scope official-parent-inventory`
- Owner-facing enrichment still available: `--scope active-brand-setup`
- Parent missions: `--scope parent-company --parent-company …`

## Promotion pack

- `reports/research-engine-v2/production-census-brand-setup-promotion-candidates.json`
- `reports/research-engine-v2/production-census-brand-setup-promotion-candidates.md`

## Test

```bash
npm run test:census-brand-governance-scope
```

## Rollback

- Discovery: pass `discoverAllOfficialParents: false` / use `buildActiveBrandDiscoveryControlList`
- Scope: `--scope active-brand-setup`
- Revert `census-brand-governance.js` wiring in discovery / Clean Core / brand SoT

## Data contract snapshot

- **Tables:** Hotel Property Census only (writes); Brand Setup read-only for Active index
- **Field maps:** `MAP_FIRST_PASS` + insert allowlist (includes Public/Radar Hold fields)
- **Required for non-active Census save:** brand, official URL, Census Only status, Hold flags
- **Select options:** Production Use Status / Public Display / Radar Display — must exist in schema (existing Census enums)
- **Linked records:** none for governance fields
- **UI output:** N/A (Autopilot / Census backend)

## Regression checklist

- What could break: Clean Core count (non-Active without Census Only no longer pass brand SoT); discovery volume increases; inserts set Hold/HR
- Retest: source discovery controlled, coverage reconciliation dry-run, Clean Core gate, brand registry promotion pack
- Fields touched: Production Use Status, Public Display Review Status, Radar Display Status, Radar Display Reason, Human Review Required
