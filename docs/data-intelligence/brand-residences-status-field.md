# Brand Residences Status Audit

- Generated: 2026-07-09T22:59:35.177Z
- Mode: **dry-run**
- Frontend renders indicator: **yes**
- Airtable modified: **no**

## API shape
```json
{
  "path": "brand.residences",
  "shape": {
    "status": "Yes | Case-by-Case | No | Not Confirmed",
    "notes": "string | null",
    "sourceUrl": "string | null",
    "reviewStatus": "Source-Backed | Founder-Reviewed | Needs Review | Not Confirmed"
  }
}
```

## Tribute current status
```json
{
  "status": "Not Confirmed",
  "notes": null,
  "sourceUrl": null,
  "reviewStatus": "Not Confirmed"
}
```

## Active brand audits
### Tribute Portfolio
- Status: **Not Confirmed** · Review: **Not Confirmed**
- [medium] Branded Residences Status is empty

### Curio Collection by Hilton
- Status: **Not Confirmed** · Review: **Not Confirmed**
- [medium] Branded Residences Status is empty

### Kimpton Hotels
- Status: **Not Confirmed** · Review: **Not Confirmed**
- [medium] Branded Residences Status is empty

### Radisson Blu by Choice
- Status: **Not Confirmed** · Review: **Not Confirmed**
- [medium] Branded Residences Status is empty

### Radisson by Choice
- Status: **Not Confirmed** · Review: **Not Confirmed**
- [medium] Branded Residences Status is empty

### Ascend Hotel Collection
- Status: **Not Confirmed** · Review: **Not Confirmed**
- [medium] Branded Residences Status is empty

## Apply command
```bash
npm run setup-brand-residences-status-fields -- --apply
```