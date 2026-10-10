# 002 — Baseline Metrics

Planning CALA universe: **~15000**
Production census (documented): **5956**
Audited current dataset (artifacts in checkout): **21**

```json
{
  "planning_universe_cala": 15000,
  "production_census_documented": 5956,
  "audited_current_dataset_size": 21,
  "deep_sample_size": 21,
  "deep_sample_note": "200–300 hotel deep sample NOT RELIABLY ACHIEVABLE — only hotels with stored ownership/contact artifacts exist in this checkout. Expanding to 240 would require inventing NO_RESEARCH rows without hotel identity store dump.",
  "TOTAL_HOTELS_AUDITED": 21,
  "HOTELS_WITH_ANY_OWNER_VALUE": 19,
  "HOTELS_WITH_ANY_EVIDENCED_OWNER": 4,
  "PROPERTY_OWNER_LEGAL_ENTITY_RESOLVED": 1,
  "OWNER_SPONSOR_GROUP_RESOLVED": 1,
  "ULTIMATE_PARENT_RESOLVED": "NOT RELIABLY MEASURABLE — no consistent ultimate_parent field in auditable stores",
  "CONTACTABLE_OWNER_ORG_RESOLVED": 2,
  "OWNER_PLUS_CONTACTABLE_ORG": 2,
  "OWNER_PLUS_RELEVANT_PERSON": 4,
  "OWNER_PLUS_PUBLIC_EMAIL": 2,
  "OWNER_PLUS_VERIFIED_EMAIL": "NOT RELIABLY MEASURABLE — verification_status not consistently stored outside KGPV showcase; do not treat inferred/showcase as verified",
  "OWNER_PLUS_DIRECT_BUSINESS_PHONE": "NOT RELIABLY MEASURABLE — phone role (direct vs corporate vs hotel) not typed in fixtures",
  "OWNER_PLUS_CORPORATE_PHONE": 1,
  "OWNER_PLUS_CONTACT_PAGE": "NOT RELIABLY MEASURABLE — contact-page flag not stored",
  "OWNER_PLUS_PROFILE_LINKEDIN": 3,
  "FULLY_OWNER_ACTIONABLE": 2,
  "STALE_OWNER_RECORDS": "NOT RELIABLY MEASURABLE — freshness timestamps sparse",
  "STALE_CONTACT_RECORDS": "NOT RELIABLY MEASURABLE — contact store empty",
  "CONFLICTED_OWNERSHIP": 7,
  "UNRESOLVED_OWNERSHIP": 8,
  "NO_OWNERSHIP_RESEARCH_ATTEMPTED_IN_AUDIT_SET": 0,
  "NO_OWNERSHIP_RESEARCH_ATTEMPTED_IN_PLANNING_UNIVERSE_ESTIMATE": 14979
}
```
