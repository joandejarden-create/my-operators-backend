# Wave 13 — SO/ Brand Basics creation

Generated: 2026-07-26T22:19:56.208Z
Dry-run: **false** · Created: **true**

## Planned / applied values

| Field | Value |
| --- | --- |
| Brand Name | `SO/` |
| Brand Status | `Under Review` |
| Parent Company | `AccorHotels` |
| Display alias (no Airtable field) | `SO/ Hotels & Resorts` |
| Code-side slug | `so-hotels-and-resorts` |
| Record ID | `recPCWbTmBPe5SMm0` |

## Validation

- PASS — create payload limited to allowed fields; Status Under Review

## Field mapping

- **brandName:** Brand Name
- **brandStatus:** Brand Status
- **parentCompany:** Parent Company
- **internalNotes:** Internal Notes
- **slugNote:** No Brand Basics Slug field in schema — Wave 13 slug remains code-side so-hotels-and-resorts via factory-preview identity + WAVE13_SLUGS.
- **displayAliasNote:** No dedicated display-alias field — alias SO/ Hotels & Resorts recorded in Internal Notes + factory-preview identity name.

## Sanitized payload preview

```json
{
  "table": "Brand Setup - Brand Basics",
  "fields": {
    "Brand Name": "SO/",
    "Brand Status": "Under Review",
    "Parent Company": "AccorHotels",
    "Internal Notes": "Created for Wave 13 factory preview; not public; not Active/Live; not company validated. Display alias (no dedicated Airtable field): SO/ Hotels & Resorts. Slug is code-side: so-hotels-and-resorts (Brand Basics has no Slug field)."
  }
}
```

## Error handling

- Validation error → refuse create; report failures (no Airtable call).
- API error → surface Airtable message; no partial Presentation/release writes.
- Network error → fail stage; retry after connectivity restore.

## Protections

- No Active/Live status
- No release fields
- No Company Validated / Source Library / Registry
- No Presentation / image writes
- No protected 39 brand changes
- No Morgans Originals record changes
