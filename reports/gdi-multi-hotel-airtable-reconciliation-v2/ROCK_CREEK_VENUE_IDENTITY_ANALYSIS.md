# Rock Creek Mansion — venue identity analysis

**Record:** Private Event Venues `recf35Q9B6jpX3Ivm`  
**Phase:** Read-only — **no production write**

## Summary

One physical venue (Rock Creek Mansion, Bethesda). Two venue IDs on the same Airtable row: structured field ≠ embedded JSON. Downstream links use the **domain-based** ID only.

## Live Airtable state

| Field | Value |
|-------|-------|
| Structured Venue ID | `pev_42542bf58aac936b` |
| Venue Payload JSON `venueId` | `pev_53b689545914754d` |
| Name | Rock Creek Mansion |
| Domain / website | `rockcreekmansion.com` |
| Address | 5417 West Cedar Lane |
| City | Bethesda |
| On-site lodging | `NO_LODGING` |

## ID computation

| Method | ID | Matches |
|--------|-----|---------|
| `computeVenueId(name + officialDomain)` | `pev_53b689545914754d` | Historical payload, repo corpus, links |
| `computeVenueId(name + city Bethesda, no domain)` | `pev_42542bf58aac936b` | Post–PR #367 structured field |

## Linked records

| Table | Venue ID used | Count |
|-------|---------------|------:|
| Hotel Venue Fit | `pev_53b689545914754d` | 1 (`hvf_0ec589c6e19bbb`, hotel `recLuxvwwxID7U2B8`) |
| Private Event Signals | `pev_53b689545914754d` | 2 |
| Fits / signals on pilot ID | `pev_42542bf58aac936b` | **0** |

Name search: **one** venue row; no duplicate venue entity.

## Root cause

PR #367 (Bethesda four-record production pilot) updated the structured **Venue ID** to the geo-only recomputation. **Venue Payload JSON** was intentionally left on the domain-stable historical ID — partial update, not two venues.

## Conclusion

| Question | Answer |
|----------|--------|
| Same physical venue? | **Yes** |
| Canonical ID | `pev_53b689545914754d` |
| Pilot structured ID | Erroneous alias / rewrite — not a second venue |
| Integrity code | `EMBEDDED_JSON_STRUCTURED_ID_MISMATCH` |

Evidence: `ROCK_CREEK_IDENTITY_AUDIT.json`