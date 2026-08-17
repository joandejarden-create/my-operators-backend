# Arbor PI Stewardship Pilot

Generated: 2026-07-06T09:20:32.385Z
Mode: **apply**
Operator: Arbor Lodging (CALA) — `recF5Z87OAqFgndoq`

## Summary

- Steward source IDs in scope: 6
- All Arbor sources found: 7
- Facts linked to Arbor: 283
- Approved facts (current): 1
- Published row: `recyXqZU26sKrPVhZ` (found)
- Current package eligible: **false**
- Projected eligible after steward source updates: **true**

## Apply Result

- Sources updated: 6
- Facts updated: 0
- Skipped: 0

## Steward Sources (in scope)

### Arbor Lodging Regional Experience June 2026 (`rec83yK5rIkE7aTWx`)

| Field | Value |
|-------|-------|
| Status | Extracted |
| Approved for Explorer Use | No |
| Source Quality | Medium |
| Source Type | PDF |
| Region | CALA |
| Operator link | recF5Z87OAqFgndoq |

**Blockers:** approved_for_explorer_use_no

**Recommended updates:**
- `Approved for Explorer Use?` → **Yes** — Required for publish readiness

### ALM Arbor Lodging Overview Spanish Case Studies (`recg3p1cVgwVmZ9ot`)

| Field | Value |
|-------|-------|
| Status | Extracted |
| Approved for Explorer Use | No |
| Source Quality | Medium |
| Source Type | PDF |
| Region | CALA |
| Operator link | recF5Z87OAqFgndoq |

**Blockers:** approved_for_explorer_use_no

**Recommended updates:**
- `Approved for Explorer Use?` → **Yes** — Required for publish readiness

### ALM Arbor Lodging Overview English MX Team Case Studies (`recgadiUD9cdGmaqY`)

| Field | Value |
|-------|-------|
| Status | Extracted |
| Approved for Explorer Use | No |
| Source Quality | Medium |
| Source Type | PDF |
| Region | CALA |
| Operator link | recF5Z87OAqFgndoq |

**Blockers:** approved_for_explorer_use_no

**Recommended updates:**
- `Approved for Explorer Use?` → **Yes** — Required for publish readiness

### Arbor Lodging — Press releases (`reckn9Hgz1StOc4t1`)

| Field | Value |
|-------|-------|
| Status | Extracted |
| Approved for Explorer Use | No |
| Source Quality | Medium |
| Source Type | Press Release |
| Region | CALA |
| Operator link | recF5Z87OAqFgndoq |

**Blockers:** approved_for_explorer_use_no

**Recommended updates:**
- `Approved for Explorer Use?` → **Yes** — Required for publish readiness

### Arbor Lodging Experiencia Regional Junio 2026 (`recwa89aO43SS9uey`)

| Field | Value |
|-------|-------|
| Status | Extracted |
| Approved for Explorer Use | No |
| Source Quality | Medium |
| Source Type | PDF |
| Region | CALA |
| Operator link | recF5Z87OAqFgndoq |

**Blockers:** approved_for_explorer_use_no

**Recommended updates:**
- `Approved for Explorer Use?` → **Yes** — Required for publish readiness

### Hotel Investment Today — Arbor Lodging profile (`recyY5faXntjMFkZp`)

| Field | Value |
|-------|-------|
| Status | Found |
| Approved for Explorer Use | No |
| Source Quality | Medium |
| Source Type | Press Release |
| Region | CALA |
| Operator link | recF5Z87OAqFgndoq |

**Blockers:** source_status_not_ready:Found; approved_for_explorer_use_no

**Recommended updates:**
- `Approved for Explorer Use?` → **Yes** — Required for publish readiness
- `Status` → **Approved** — Move from Found/Captured after steward review

## Other Arbor-Linked Sources (not in steward ID list)

- `recM4HuV9r5Gz35P7` — Arbor Lodging — Platforms / CALA — status=Extracted, explorer=Yes

## Published Explorer Fields Row

```json
{
  "id": "recyXqZU26sKrPVhZ",
  "fieldName": "op.snapshot.companyName",
  "publishStatus": "Published",
  "stale": false,
  "overallSourceConfidence": "Medium",
  "operatorId": "recF5Z87OAqFgndoq",
  "approvedValuePreview": "Arbor Lodging"
}
```

Verify Publish Status and Stale? manually in Airtable — this script does not auto-patch published rows.

## Recommended Facts for Manual Review

Top 10 pending facts (not auto-approved unless --approve-fact-ids):

- `rec72Dej1lkTrEIpE` — op.snapshot.companyName — review=Pending — score=6
- `recGgP2uPwA3S83oc` — op.snapshot.primaryServiceModel — review=Pending — score=6
- `recIKJC6V4GhM8z9W` — op.snapshot.primaryServiceModel — review=Pending — score=6
- `recIwGt4uV2o6qCBN` — op.snapshot.totalProperties — review=Pending — score=6
- `recRZ7kce8XlwNetP` — op.snapshot.totalProperties — review=Pending — score=6
- `recWkl5jPCXVApyVS` — op.snapshot.primaryServiceModel — review=Pending — score=6
- `recX0fj9BPCNoL870` — op.snapshot.companyName — review=Pending — score=6
- `recXGM9tGqLpeTLp4` — op.snapshot.companyName — review=Pending — score=6
- `recZ7Ws1xvQ3ilzHY` — op.snapshot.totalRooms — review=Pending — score=6
- `recZJXA9Dyk0Vafjz` — op.snapshot.companyName — review=Pending — score=6

## Eligibility Preview

**Current blockers:** source:rec83yK5rIkE7aTWx:approved_for_explorer_use_no; source:recg3p1cVgwVmZ9ot:approved_for_explorer_use_no; source:recgadiUD9cdGmaqY:approved_for_explorer_use_no; source:reckn9Hgz1StOc4t1:approved_for_explorer_use_no; source:recwa89aO43SS9uey:approved_for_explorer_use_no; source:recyY5faXntjMFkZp:source_status_not_ready:Found; source:recyY5faXntjMFkZp:approved_for_explorer_use_no
**Projected blockers (after source steward apply):** none

## Never Updated By This Script

- Company Validated
- Company Validation Date
- Brand/Operator Setup profile governance fields
- Profile governance trust labels / External Display Status on Setup
- Scoring / snapshot fields
- Published Explorer Fields row (report only unless mapping verified)

## Next Step

Run: npm run audit-partner-intelligence-publish-readiness
