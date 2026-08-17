# Profile governance pilot values

Generated: 2026-07-06T08:44:08.210Z
Mode: **apply**
Base: `appvtnDurnMSjINP6`

## Targets

- Brand: `rec5KPgalPPAFl7UZ`
- Operator: `recZPHT2zqc8K6itx`

## Brand

- Status: **found**
- Record: `rec5KPgalPPAFl7UZ` — Best Western Plus (record_id)

### Fields unchanged

- `Validation Status`: "Company Published"
- `Usage Permission`: "Platform Display Allowed"
- `Source Type`: "Company PDF / Brochure"
- `Source Region`: "Global Reference"
- `Confidence Level`: "Medium"
- `External Display Status`: "Show Trust Label"
- `Last Reviewed Date`: "2026-07-06"
- `Company Validated`: false
- `Evidence Notes`: "Pilot governance value for Explorer trust label QA."
- `Missing Data Flags`: "Company validation not yet completed."
- `Internal Notes`: "Pilot value. Remove or update after QA."

- Write: **skipped** (no_changes)

### Expected normalized governance

```json
{
  "displayLabel": "Company Published",
  "displaySubtitle": "Last Reviewed: Jul 6, 2026 · Region: Global Reference · Confidence: Medium",
  "validationStatus": "Company Published",
  "usagePermission": "Platform Display Allowed",
  "externalDisplayStatus": "Show Trust Label",
  "lastReviewedDate": "2026-07-06",
  "sourceRegion": "Global Reference",
  "confidenceLevel": "Medium"
}
```
- Expected **displayLabel**: `Company Published`
- Expected **displaySubtitle**: `Last Reviewed: Jul 6, 2026 · Region: Global Reference · Confidence: Medium`

### Manual QA

1. Open Explorer: [`/brand-explorer-combined?id=rec5KPgalPPAFl7UZ`](/brand-explorer-combined?id=rec5KPgalPPAFl7UZ)
2. Confirm header trust chip shows expected label when `displayLabel` is set.
3. Optional API check: `/api/brand-library/brand?brandId=rec5KPgalPPAFl7UZ`

## Operator

- Status: **found**
- Record: `recZPHT2zqc8K6itx` — Viento Sur Gestión Hotelera (record_id)

### Fields updated

| Field | From | To |
|-------|------|-----|
| `Validation Status` | — | "Source-Informed" |
| `Usage Permission` | — | "Platform Display Allowed" |
| `Source Type` | "Imported sample data" | "Company Website" |
| `Source Region` | — | "CALA-Specific" |
| `Confidence Level` → `Data Confidence Level` | "Inferred" | "Medium" |
| `External Display Status` | — | "Show Trust Label" |
| `Last Reviewed Date` | — | "2026-07-06" |
| `Evidence Notes` | — | "Pilot governance value for Explorer trust label QA." |
| `Missing Data Flags` | — | "Company validation not yet completed." |
| `Internal Notes` | — | "Pilot value. Remove or update after QA." |

### Fields unchanged

- `Company Validated`: false

- Write: **applied**
- Airtable: updated 10 field(s)

### Expected normalized governance

```json
{
  "displayLabel": "Source-Informed",
  "displaySubtitle": "Last Reviewed: Jul 6, 2026 · Region: CALA-specific · Confidence: Medium",
  "validationStatus": "Source-Informed",
  "usagePermission": "Platform Display Allowed",
  "externalDisplayStatus": "Show Trust Label",
  "lastReviewedDate": "2026-07-06",
  "sourceRegion": "CALA-Specific",
  "confidenceLevel": "Medium"
}
```
- Expected **displayLabel**: `Source-Informed`
- Expected **displaySubtitle**: `Last Reviewed: Jul 6, 2026 · Region: CALA-specific · Confidence: Medium`

### Manual QA

1. Open Explorer: [`/operator-explorer-gold-mock.html?id=recZPHT2zqc8K6itx`](/operator-explorer-gold-mock.html?id=recZPHT2zqc8K6itx)
2. Confirm header trust chip shows expected label when `displayLabel` is set.
3. Optional API check: `/api/intake/third-party-operators/recZPHT2zqc8K6itx`

## Summary

- Brand write: skipped
- Operator write: applied
- Records modified: 1