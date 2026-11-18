# Deduplication and shared intelligence

## In-run deduplication

Duplicate segment discovery for the same `hotelId::opportunityId` is collapsed before upsert.

Resolved in Pass A: **1**

```json
[
  {
    "hotelId": "recLuxvwwxID7U2B8",
    "opportunityId": "gdi_pilot_new_bethesda_research_001",
    "compositeKey": "recLuxvwwxID7U2B8::gdi_pilot_new_bethesda_research_001",
    "segment": "corporate_meetings_training",
    "reason": "DUPLICATE_COMPOSITE_KEY_IN_RUN"
  }
]
```

## Cross-hotel shared demand generator

| Field | Value |
|-------|-------|
| Shared `dgGeneratorId` | `dg_pilot_shared_synthetic_001` |
| Bethesda composite key | `recLuxvwwxID7U2B8::gdi_pilot_new_bethesda_research_001` |
| Peer composite key | `recPeerHotelSharedDg001::gdi_pilot_new_peer_hotel_research_001` |
| Bethesda CREATE | 1 |
| Peer CREATE | 1 |

### Rules

- Reuse dgGeneratorId for the same underlying demand activity across hotels
- Persist separate hotelId::opportunityId research inventory rows
- Keep hotel-specific fit, contacts, pursuit, sales feedback, and publication authority separate
- Never expose one hotel private sales notes to another hotel payload
