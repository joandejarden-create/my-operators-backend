# Bethesda validation reconciliation

Generated: 2026-09-17T16:21:33.764Z
Hotel: Bethesda Marriott (`recLuxvwwxID7U2B8`)
Canonical Decision base: `appa2cE7FTRmIbB32`

## Verdict

Mapping: **CORRECT**

## Counts

```json
{
  "validatedOpportunities": 1,
  "validationEvents": 72,
  "feedbackRows": 18,
  "uniqueFeedbackOpportunityIds": [
    "gdi_opp_amwa_2027_annual"
  ],
  "uniqueValidatedSubjectIds": [
    "gdi_opp_amwa_2027_annual"
  ],
  "amwaDecisionCount": 3,
  "amwaDecisionIds": [
    "dec_mu4qcpxz_5394a4c6",
    "dec_mu5kpxvb_cbfe3bb4",
    "dec_mu5kpw7x_d10847ae"
  ],
  "mapping": "CORRECT",
  "valueCounts": {
    "NEVER_SEEN": 36,
    "WORTH_PURSUING_NOW": 36
  }
}
```

## Interpretation

Legacy feedback.json rows all target a single opportunity (AMWA). Collapsing 18 feedback rows → 1 Decision is **correct**, not a mapping bug.

Do not infer “new” from absence in Contact Intelligence — hotel-confirmed validation values only.
