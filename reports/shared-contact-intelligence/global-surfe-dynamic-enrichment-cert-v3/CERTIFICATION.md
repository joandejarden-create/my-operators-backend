# Global Surfe Dynamic Enrichment Certification V3

**Classification:** `GLOBAL_SURFE_ENRICHMENT_VALIDATED`

## Bethesda missing-channel selection

| Person | Email | Phone | LinkedIn | Request | Surfe include |
|---|---|---|---|---|---|
| Jeff Mullen | true | false | true | `phone` | email=false mobile=true |
| Katherine Button | true | true | true | `none` | email=false mobile=false |
| Patrick Reece | true | true | true | `none` | email=false mobile=false |
| Meinrad Lang III | false | false | true | `both` | email=true mobile=true |
| Jenny Pyne | false | false | true | `both` | email=true mobile=true |

## Live Surfe

```json
{
  "status": "DEFERRED_TO_PRIOR_OR_CONFIRM",
  "priorLiveStatus": "PARTIAL_OR_FULL_SUCCESS",
  "priorEnrichmentId": "5b090384-c17e-4886-b916-eef78ffec3e4",
  "priorCreditsConsumed": {
    "email": 1,
    "mobile": 1
  },
  "priorRequestedIncludeMobile": true,
  "message": "Pass --confirm-live-spend to run one guarded Surfe enrich for the golden person."
}
```

## Provider root cause (golden)

{
  "incorrectIdentityInput": false,
  "correctedLinkedInField": "linkedinUrl",
  "incorrectRequestedChannelsHistorically": "HE previously defaulted include.mobile=false unless SURFE_INCLUDE_MOBILE; phone for golden person is in mobilePhones",
  "apiVsExtension": "API returns email+mobile when linkedinUrl + include.mobile=true; landlines free but unused for this person",
  "asyncHandling": "pollSurfePeopleEnrichment until COMPLETED",
  "responseParsing": "normalizeSurfePerson now reads emails[], top-level email, mobilePhones, landlines",
  "errorClassification": "pending ≠ no_result; Findymail miss ≠ Surfe miss",
  "priorLiveOutcome": "PARTIAL_OR_FULL_SUCCESS"
}
