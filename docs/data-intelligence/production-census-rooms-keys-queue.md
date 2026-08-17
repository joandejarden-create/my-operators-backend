# Production Census Rooms / Keys Queue

**Status:** `production_census_rooms_keys_queue_ready`  
**Generated:** 2026-08-07T13:20:35.398Z  
**Queue:** `rooms_keys_missing`  
**Extractor:** production-census-rooms-keys-extractor-v1

## 1. Executive summary

Rooms / Keys is an **early** Census queue with **High-only** production writes. Mixed-use / units / residences stay Hold. VIC false-positive room counts (IHG `22`) are rejected.

| Metric | Value |
| --- | ---: |
| Scanned | 1205 |
| Eligible | 568 |
| Processed (limit) | 30 |
| Pages ok / blocked | 30 / 0 |
| Counts found | 0 |
| High proposals | 0 |
| Medium review | 0 |
| Low blocked | 30 |
| Hold | 0 |
| Updates if applied | 0 |

## 2. Current Rooms / Keys field status

```json
{
  "rooms_keys": true,
  "rooms_confidence": true,
  "rooms_source_url": true
}
```

Existing live fields: Rooms / Keys, Rooms Confidence, Rooms Source URL

## 3. Missing provenance fields (v1.1.4 plan)

Needs v1.1.4: **false**

Missing planned: none

```json
[
  {
    "name": "Rooms Source Type",
    "type": "singleSelect",
    "options": [
      "official_property_page",
      "official_brand_directory",
      "official_hotel_website",
      "official_press_release",
      "official_development_page",
      "trusted_secondary_source",
      "steward_review"
    ]
  },
  {
    "name": "Rooms Reviewed Date",
    "type": "date"
  },
  {
    "name": "Rooms Notes",
    "type": "multilineText"
  },
  {
    "name": "Rooms Confidence",
    "type": "singleSelect_option_add",
    "options_add": [
      "Hold"
    ],
    "note": "Hold added on live Rooms Confidence via v1.1.4 apply (typecast seed; Meta choices PATCH unsupported)"
  }
]
```

**This task does not create schema fields.**

## 4. Queue definition

- **Name:** rooms_keys_missing
- **Early:** yes
- **Write gate:** High confidence + official page + hotel-only count
- **Medium:** review / founder approval only
- **Low / Hold:** no write

## 5. Source rules

1. Official hotel property page  
2. Official brand/property directory  
3. Official hotel website  
4. Official press release  
5. Official development/company page  
6. Trusted secondary only as Medium (not High)

## 6. Mixed-use guardrails

Do not write when count may include residences, villas, apartments, vacation ownership, total units, pipeline masterplan, or mixed hotel+residences without a hotel-only split.

## 7. Commands

```bash
npm run census:queue-run -- --queue rooms_keys_missing --dry-run --limit 100
```

Apply later:

```bash
npm run census:queue-run -- --queue rooms_keys_missing --apply --limit 100 --confirm-targeted-queue-apply --confirm-rooms-keys-only --confirm-official-source-room-counts-only --confirm-no-mixed-use-unit-confusion --confirm-no-owner-operator-writes --confirm-no-date-writes --confirm-no-brand-explorer-writes
```

## 8. Learning system updates

- Reject VIC IHG `22` rooms false positive (`\\x22rooms` JS escape)
- Prefer `json_ld_numberOfRooms` and explicit hotel-room phrases
- Split "80 hotel rooms and 40 residences" → write 80 only
- Hold on "units" / including residences / planned pipeline counts

## 9. Sample High proposals

```json
[]
```

## 10. Recommended next step

Founder review High proposals; apply with confirm flags. Medium stays review-only.
