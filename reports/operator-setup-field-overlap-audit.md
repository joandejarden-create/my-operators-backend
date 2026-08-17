# Operator Setup Field Overlap Audit

| Cluster | Fields | Problem | Recommendation |
| ------- | ------ | ------- | -------------- |
| Platform caps | cap_profile_operational / commercial / transition | Phase D reused OE brands/structures across all three | NARROW each; blank if no field-specific evidence |
| Commercial cards | ownerEngagementNarrative, ov_card_*, specializations | Overlap + UI scaffold mixed with company truth | ov_card_* → presentation only; engagement → research/blank |
| Governance infra | infra_systems_technology, infra_asset_management_reporting, risk_* | Phase D wrote diligence hedges in all | Separate systems map vs reporting cadence vs risk programs |
| Engagement section | phase_d_* rows | Duplicate Platform/Commercial/Governance narratives | CLEAR Phase D rows; redesign section or derive |
| Leadership Platform | Team Market / Language / Governance Cadence | Markets OK as presence; languages provisional; cadence generic | NARROW to markets-only or redesign |

Fields too overlapping to populate reliably at Production scale without research:
- ownerEngagementNarrative vs infra_asset_management_reporting vs Engagement cadence rows
- ov_card_commercial vs cap_profile_commercial vs specializations
