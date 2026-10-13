# ADP customer UI E2E

Generated: 2026-09-17T17:02:53.002Z
Host: http://127.0.0.1:8080
Finding: `adp_e2e_test_finding_v2` (TEST ONLY)
Hotel: Bethesda `recLuxvwwxID7U2B8`

## Result: PASS

### Root cause fixed (required for PASS)

Owner-app `authFetch` dropped `method` and `body`, so Hotel Feedback POSTs became GETs with empty bodies. Validation/action/outcome events were created without `validationValue` / `actionType`. Fixed in `public/js/ai-demand-positioning/ai-demand-positioning.js`. Airtable read path also recovers typed values from `eventValue` when raw JSON omitted undefined keys.

### Regression

`npm run playwright:adp-decision-outcome-feedback-e2e-v1`

Asserts: UI save → refresh persistence → Airtable eventValue AGREE / WEBSITE_CONTENT_UPDATED / TOO_EARLY_TO_MEASURE; single decision; no duplicate.

```json
{
  "uiLoaded": true,
  "validationSave": true,
  "validationPersistAfterRefresh": true,
  "validationPersistDetail": {
    "decisionId": "dec_mu5s2mcs_3e5fb9be",
    "validationValue": "AGREE",
    "eventCount": 1
  },
  "actionSave": true,
  "outcomeSave": true,
  "refreshOk": true,
  "subjectApi": {
    "ok": true,
    "decisionId": "dec_mu5s2mcs_3e5fb9be"
  },
  "airtable": {
    "decisionCount": 1,
    "eventCount": 3,
    "hotelOk": true,
    "moduleOk": true,
    "subjectOk": true,
    "hasValidation": true,
    "hasAction": true,
    "hasOutcome": true,
    "noDuplicateDecision": true,
    "validationValueOk": true,
    "actionValueOk": true,
    "outcomeValueOk": true
  },
  "uiQuality": {
    "feedbackCardPresent": true,
    "labelsFriendly": true,
    "consoleErrorCount": 0,
    "failedRequestCount": 0
  }
}
```

## Airtable read-back

Decision `dec_mu5s2mcs_3e5fb9be` — hotel `recLuxvwwxID7U2B8` — module ADP — subject `adp_e2e_test_finding_v2` — events VALIDATION(AGREE) + ACTION(WEBSITE_CONTENT_UPDATED) + OUTCOME(TOO_EARLY_TO_MEASURE).
