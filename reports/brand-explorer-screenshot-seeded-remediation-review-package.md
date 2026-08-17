# Brand Explorer Screenshot-Seeded Remediation Review Package v24A-R2

- Generated: 2026-07-09T12:41:37.930Z
- Mode: **dry-run** · Airtable: **no** · Images: **untouched**
- v24 score baseline: **16/100**
- Screenshot checks confirmed: **33/45**
- Comparable to Curio: **no**

## Human screenshot defects verified
- [1] **scenario_thin**: Tribute scenario avg depth lower than Curio (21/23/44 vs Curio)
- [2] **curio_bullet_depth**: Curio uses 4 line-broken bullets per column; Tribute pads empty <li>&nbsp;</li>
- [3] **bestAt_thin**: bestAt word counts: 23, 24, 29
- [3] **generic_bestAt**: Some bestAt copy reads as pillar labels repeated in body
- [4] **valueOwners_title_only**: All four valueOwners.scenario.* cards title-only
- [4] **lifecycle_thin**: lifecycle phase bodies short (22, 17, 8, 7, 10, 17)
- [5] **featured_truncated**: Featured Application uses brandPositioning.slice(0,220)
- [5] **openings_button_disabled**: View Recent Openings disabled — no footprint.openings rows
- [5] **openings_suppress**: Keep openings suppressed until dated source-backed PR rows exist
- [6] **generic_ladder**: Portfolio ladder shows Lower-scale/Mid-scale generics
- [6] **marriott_ladder_missing**: No Marriott static ladder mapping in frontend (unlike Hilton for Curio)
- [7] **loyalty_thin**: Tribute loyalty slots sparse vs Curio (proof 0 vs 6)
- [7] **loyalty_kpi_unsafe**: KPI cards show em-dash defaults — do not invent member/hotel counts
- [7] **loyalty_generic_fallback**: UI falls back to generic earn/redeem/elite/proof when slots empty
- [7] **loyalty_mechanics_incomplete**: earn/redeem presentation slots empty (v23 facts Pending)
- [8] **footprint_less_specific_than_curio**: Region narratives shorter than Curio CALA-specific examples
- [8] **footprint_counts_unsafe**: Do not add property/count claims without verified footprint metrics
- [9] **standards_placeholder**: standards.requirement table missing — placeholder visible
- [9] **standards_table_missing**: 0 structured standards.requirement rows
- [9] **no_fdd_table_invention**: be.standards.qualityAssuranceTheme is Internal Only Pending — not table-safe
- [10] **writer_default_sort**: 83 rows with index×10 Sort Order
- [10] **sort_order_visibility_impact**: Multi-row slots (loyalty.proof, standards.requirement, materials.file) may render out of intended order
- [11] **momentum_empty_placeholder**: Recent Momentum renders empty placeholder state
- [11] **momentum_suppress_when_unsourced**: Suppress Recent Momentum until dated source-backed rows exist
- [12] **portfolio_mix_thin_chip**: Portfolio Mix appears thin (1 row(s))
- [12] **portfolio_mix_needs_source_or_suppress**: Use source-backed mix context or suppress unsupported mix claims
- [13] **openings_empty_cards_critical**: Openings / Examples / Properties renders blank cards/disabled state (0 rows)
- [13] **openings_suppress_until_real_cards**: Suppress empty property/example cards until complete rows + approved assets exist
- [14] **demand_single_card**: Demand Scenario View shows 1 scenario row(s)
- [14] **demand_grid_needs_manual_review**: Additional demand scenarios require founder/manual review before expansion
- [15] **market_perception_generic**: Market Perception summary reads generic vs completed profiles
- [15] **market_perception_copy_upgrade_safe**: Safe brand-specific copy upgrade possible without new claims
- [16] **loyalty_hold_until_v23_review**: Hold loyalty mechanics until v23 loyalty facts are approved

## Safe copy proposals
### `overview.why_value`
- AI-drafted / pending founder review · Not company-validated · Not Marriott-validated
- Operator fit matters: strongest with teams that can deliver design-forward full-service or resort operations, Marriott systems cutover, and ongoing collection QA.
### `valueOwners.watchouts`
- AI-drafted / pending founder review · Not company-validated · Not Marriott-validated
- Collection affiliation is not a one-time reflag; owners should plan for ongoing QA, systems participation, and brand-standard upkeep through the hold period.
### `overview.differentiators.identity`
- AI-drafted / pending founder review · Not company-validated · Not Marriott-validated
- Soft-brand structure: independent hotel character with Marriott affiliation, systems, and quality expectations.
### `overview.differentiators.commercial`
- AI-drafted / pending founder review · Not company-validated · Not Marriott-validated
- Conversion and repositioning path: confirm development milestones, PIP expectations, approval steps, and commercial terms directly with Marriott for the specific asset.
### `overview.featured_application`
- AI-drafted / pending founder review · Not company-validated · Not Marriott-validated
- Independent boutique hotels with distinctive style and local flavor, supported by Marriott Bonvoy and Marriott commercial infrastructure without erasing the property's individuality.
### `overview.bestAt.1`
- AI-drafted / pending founder review · Not company-validated · Not Marriott-validated
- Strongest for independent or boutique full-service assets that already have local identity, design character, or a clear story worth preserving.
### `overview.bestAt.2`
- AI-drafted / pending founder review · Not company-validated · Not Marriott-validated
- A fit for resort and leisure-led destinations where experience, design, F&B, and sense of place can support a higher-touch operating model.

## Section safety gates
- Standards table now: **no**
- Loyalty complete now: **no**
- Footprint improve now: **yes**
- Portfolio Context now: **no**

## Next: **v24B_copy_cleanup_then_v24B-rowcreate_then_v24C_then_v24D_then_v24E**
```bash
npm run brand-explorer-screenshot-seeded-remediation-review-package -- --brand tribute-portfolio --dry-run
```