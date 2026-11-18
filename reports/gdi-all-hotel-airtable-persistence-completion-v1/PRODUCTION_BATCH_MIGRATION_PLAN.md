# Production batch migration plan

**STOP — no bulk writes authorized by this package.**

## Prerequisites (founder)

1. Schema decision on **GDI Hotel Research Opportunities**
2. Exact per-hotel create/update allowlists
3. Confirm YOTEL legacy IDs remain blocked
4. Confirm customer publication packages untouched
5. Backup/recovery readiness (see ROLLBACK_AND_RECOVERY.md)

## Proposed batches

See machine-readable section below. Default batch size **25**. Research inventory and qualified GDO are **separate** authorization streams.

```json
{
  "version": "gdi-complete-research-persistence-v1",
  "status": "AWAITING_FOUNDER_BATCH_AUTHORIZATION",
  "prerequisites": [
    "Approve or reject proposed GDI Hotel Research Opportunities table",
    "If approved, create table with required fields in base appa2cE7FTRmIbB32",
    "Authorize research inventory backfill batches by hotel",
    "Separately authorize qualified GDO create batches (non-empty create sets only)",
    "Rock Creek Venue ID one-field fix on separate allowlist"
  ],
  "hotels": [
    {
      "hotelId": "rec2PVBDavppGpenm",
      "hotelName": "AC Hotel A Coruña",
      "researchInventoryCreates": 39,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "rec35fExUxCClpOP6",
      "hotelName": "Hilton New York Times Square",
      "researchInventoryCreates": 45,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "rec82D9zpB8fede1I",
      "hotelName": "Castillo Hotel Son Vida",
      "researchInventoryCreates": 20,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recC2kbCaS48jFjFF",
      "hotelName": "Nobu Hotel London Portman Square",
      "researchInventoryCreates": 0,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recCAGqkERSXgiku4",
      "hotelName": "Sheraton Guadalajara Expo",
      "researchInventoryCreates": 43,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recFaxTEFF9ILHWC9",
      "hotelName": "The Westin Grand München",
      "researchInventoryCreates": 26,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recG66DQJKP2c0UNh",
      "hotelName": "Renaissance New York Times Square Hotel",
      "researchInventoryCreates": 29,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recGkME49yYuxQl0u",
      "hotelName": "NOW NOW NOHO",
      "researchInventoryCreates": 8,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recH9h3McgMNFQEla",
      "hotelName": "Radisson Blu Toronto Downtown",
      "researchInventoryCreates": 66,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 1,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recIwaP1etgx2g9nA",
      "hotelName": "Cambridge Beaches Resort & Spa",
      "researchInventoryCreates": 8,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recKRJjcPnb4tVDDS",
      "hotelName": "Spice Island Beach Resort",
      "researchInventoryCreates": 77,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recLuxvwwxID7U2B8",
      "hotelName": "Bethesda Marriott",
      "researchInventoryCreates": 59,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 1,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recUOyzOXn2Zdp98I",
      "hotelName": "Radisson Hotel Santo Domingo",
      "researchInventoryCreates": 16,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recXh4NgF8sDbi9cQ",
      "hotelName": "Wyndham Alltra Punta Cana",
      "researchInventoryCreates": 27,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "rece0or38cxo3Fymb",
      "hotelName": "W Rome",
      "researchInventoryCreates": 13,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recgMYovrrZDJMqzX",
      "hotelName": "Waterstone Resort & Marina",
      "researchInventoryCreates": 29,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recjhQdAUiSyxfCqE",
      "hotelName": "Sheraton Mallorca Arabella Golf Hotel",
      "researchInventoryCreates": 30,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recnPB3Q02NO8Ma1o",
      "hotelName": "Wyndham San Diego Bayside",
      "researchInventoryCreates": 82,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    },
    {
      "hotelId": "recrPQcZg7SFARRb2",
      "hotelName": "YOTEL Geneva Lake",
      "researchInventoryCreates": 42,
      "qualifiedGdoCreates": 0,
      "conflictsDeferred": 0,
      "recommendedBatchSize": 25,
      "rateLimit": "Airtable 5 rps soft; backoff on 429"
    }
  ],
  "totals": {
    "hotels": 19,
    "filesystemOpps": 659,
    "airtableGdos": 674,
    "researchPendingSchema": 659,
    "qualifiedGdoCreate": 0,
    "qualifiedGdoUnchanged": 509,
    "idOverlap": 511,
    "commerciallyQualifiedFs": 55,
    "conflicts": 2,
    "airtableOnly": 163,
    "forbiddenBlocked": 0
  },
  "rockCreekSeparateAllowlist": {
    "airtableRecordId": "recf35Q9B6jpX3Ivm",
    "field": "Venue ID",
    "after": "pev_53b689545914754d",
    "authorizedByThisPrompt": false
  }
}
```
