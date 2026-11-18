# Weekly Change results

Engine: `gdi-weekly-change-durability-v1`

| Check | Result |
| --- | --- |
| Research-touch without material field change | non-material / unchanged path exercised |
| Material contact/date change detected | true |
| Description-only rewrite non-material | true |

Storage (existing — no new Weekly Change table):

- filesystem.hotel.opportunities
- airtable.gdo.opportunityPayloadJson
- airtable.gdo.firstDiscoveredAt
- airtable.gdo.firstDiscoveredRunId
- airtable.gdo.lastMaterialChangeAt
- airtable.gdo.lastMaterialChangeRunId

Live Weekly Change fills after Authorization A + B against canonical records.
