# 013 — Baseline KPI

```json
{
  "denominators": {
    "planning_universe": 15000,
    "audited_artifact_hotels": 21,
    "hotels_with_evidenced_owner": 4,
    "hotels_in_hotel_to_owner": 4
  },
  "OWNER_COVERAGE_RATE": {
    "value": 90.5,
    "numerator": 19,
    "denominator": 21,
    "denominator_def": "hotels appearing in any ownership/contact artifact in this checkout"
  },
  "OWNER_COVERAGE_RATE_VS_PLANNING_UNIVERSE": {
    "value": 0.1,
    "numerator": 19,
    "denominator": 15000,
    "denominator_def": "planning CALA ~15000"
  },
  "EVIDENCED_OWNER_RATE": {
    "value": 19,
    "numerator": 4,
    "denominator": 21
  },
  "EVIDENCED_OWNER_RATE_VS_PLANNING_UNIVERSE": {
    "value": 0,
    "numerator": 4,
    "denominator": 15000
  },
  "PROPCO_RESOLUTION_RATE": {
    "value": 4.8,
    "numerator": 1,
    "denominator": 21
  },
  "SPONSOR_RESOLUTION_RATE": {
    "value": 4.8,
    "numerator": 1,
    "denominator": 21
  },
  "CONTACTABLE_OWNER_ORG_RATE": {
    "value": 9.5,
    "numerator": 2,
    "denominator": 21
  },
  "OWNER_PERSON_RATE": {
    "value": 19,
    "numerator": 4,
    "denominator": 21
  },
  "OWNER_EMAIL_RATE": {
    "value": 9.5,
    "numerator": 2,
    "denominator": 21
  },
  "OWNER_VERIFIED_EMAIL_RATE": {
    "value": null,
    "note": "NOT RELIABLY MEASURABLE"
  },
  "OWNER_PHONE_RATE": {
    "value": 4.8,
    "numerator": 1,
    "denominator": 21
  },
  "OWNER_ACTIONABLE_RATE": {
    "value_on_audited_artifacts": 9.5,
    "value_on_planning_universe": 0,
    "numerator": 2,
    "denominator_audited": 21,
    "denominator_planning": 15000,
    "definition": "Hotel has evidenced owner AND at least one viable contact path (email/phone/showcase contact package)"
  }
}
```
