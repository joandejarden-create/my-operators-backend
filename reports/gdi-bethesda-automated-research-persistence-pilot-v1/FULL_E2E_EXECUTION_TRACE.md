# Full E2E execution trace

## Canonical sequence (certified)

1. Load existing hotel intelligence (canonical FS + research inventory)
2. Discover potential demand (Full E2E chain)
3. Research and qualify supporting evidence
4. Resolve canonical identities (opportunityId + hotelId)
5. Deduplicate against existing research + in-run composite keys
6. Identify new vs materially changed vs unchanged
7. Persist eligible research inventory changes (gated)
8. Read back and verify persistence
9. Maintain Weekly Change history (FS + payload + provenance fields)
10. Keep GDO qualification and publication independently gated

## Integration points

- Entry: `runCanonicalFullE2eResearch` → `persistFullE2eResearchInventory`
- Adapter: `upsertResearchInventoryRecord` (production) / injectable memory store (this pilot)
- Ledger: `research-inventory-persistence-ledger.json` on live runs
- Parallel orchestrator created: **false**
- Fail-closed by default: **true**
- Production auth locked during pilot: **true**

## Pass A actuals (completed operations)

```json
{
  "opportunitiesResearched": 5,
  "uniqueCandidates": 4,
  "newResearchOpportunities": 2,
  "materiallyUpdatedOpportunities": 1,
  "unchangedOpportunities": 1,
  "duplicatesResolved": 1,
  "exploratoryResearchOnly": 1,
  "airtable": {
    "CREATE": 2,
    "UPDATE": 1,
    "UNCHANGED": 1,
    "WOULD_CREATE": 0,
    "WOULD_UPDATE": 0,
    "FAILED": 0
  },
  "failedWrites": 0,
  "verifiedReadBacks": 3,
  "dryRun": true,
  "gdoWrites": 0,
  "publicationWrites": 0
}
```
