# PowerShell → canonical execution trace

Generated: 2026-11-18T17:48:27.816Z
Baseline: `f4846df2ad2cd23b369c398c7ff9bd97c811f9f2`

## Verdict: **FIXED_AND_VERIFIED**

| Check | Result |
| --- | --- |
| Research/FullRun default to canonical | true |
| `-Legacy` opt-out present | true |
| Points at `gdi-canonical-full-e2e-entry-v1.mjs` | true |
| Refuses implicit paid flags | true |
| Logs canonical runner | true |
| Fixture canonical invocation ok | true |
| PowerShell smoke | {"attempted":true,"ok":true,"note":"PowerShell FullRun invoked canonical entry","exitCode":0,"stdoutTail":", \"remainingUsd\": null, \"budgetsRespected\": true, \"outputsIngested\": true, \"publicationAuthorized\": false, \"researchPersistence\": null, \"researchPersistenceLedgerPath\": null, \"outDir\": \"C:\\\\Dev\\\\dealality\\\\ws-gdi\\\\reports\\\\gdi-canonical-full-e2e-entry-v1\\\\recLuxvwwxID7U2B8\\\\2026-11-18T17-48-30-331Z\", \"summaryPath\": \"C:\\\\Dev\\\\dealality\\\\ws-gdi\\\\reports\\\\gdi-canonical-full-e2e-entry-v1\\\\recLuxvwwxID7U2B8\\\\2026-11-18T17-48-30-331Z\\\\canonical-full-e2e-summary.json\" } GDI canonical workflow finished.","stderrTail":""} |

## Behavior

1. `.scripts\gdi-workflow.ps1 -HotelId <id> -Action FullRun -DryRun` → Node `gdi-canonical-full-e2e-entry-v1.mjs --mode dry-run`.
2. Validate / SyncAirtable / Review / Publish / Verify remain on universal operational workflow.
3. Historical research-to-page path requires explicit `-Legacy`.
4. Paid Exa / Airtable apply never auto-authorized from PowerShell.

## Stages (canonical fixture)

```json
{
  "ok": true,
  "exitCode": 0,
  "mode": "fixture",
  "blocked": false,
  "stages": [
    "HOTEL_PROFILE",
    "DISCOVERY",
    "DEEP_RESEARCH",
    "CONTACT_RESEARCH",
    "COMMERCIAL_QUALIFICATION",
    "SALES_INTELLIGENCE_OUTPUT",
    "PERSISTENCE_AND_CONTINUATION"
  ]
}
```
