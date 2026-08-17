# Brand Asset Download & Attachment Writer v6.1

Generated: 2026-07-09T12:02:31.776Z
Mode: **attachment-materialization-repair-apply** · Airtable modified: **yes**
Brand: Tribute Portfolio `recCvV0PuZOi8c3hC`
Text/governance Platform Ready: **yes**
Brand Setup media untouched: **yes**
Explorer media fields untouched: **yes**
Single-asset scope: **yes** (`recgq6wOvz5yOWPmY`)
Single-asset apply gate approved: **yes**

## 1. Summary

- Total Tribute asset records scanned: **1**
- Formally approved records found: **0**
- Records eligible for download: **0**
- Records excluded: **0**
- Dry-run validation passed: **yes**
- Ready for Explorer Media Promotion Writer v7: **no**
- Airtable content/direct upload available: **yes**

## 2. Formally approved records

None.

## 3. Records excluded and why

None.

## 4. Download plan

| Record | Asset | Local path |
|---|---|---|
| `recgq6wOvz5yOWPmY` | Ermita, Cartagena, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior) | `data/partner-intelligence/assets/tribute-portfolio/tribute-portfolio__value-driver-conversion__ermita-cartagena-a-tribute-portfolio-hotel.jpg` |

## 5. Attachment strategy

- Supported: **yes**
- Mode: Prefer Airtable content API uploadAttachment (bytes); fallback remote URL patch + reread verification
- Note: v6.1 considers patch success insufficient; materialization is true only when reread Attachment count > 0.
- Fallback: When direct byte upload is unavailable or exceeds 5MB, fallback to URL patch and confirm reread.

## 6. Dry-run validation details

- `recgq6wOvz5yOWPmY` — Ermita, Cartagena, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior): ok (ok; content-type=image/jpeg; size=394650)

## 7. Attachment Materialization Status

- `recgq6wOvz5yOWPmY` — Ermita, Cartagena, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior): attachment=missing (count=0); localFile=missing; sourceUrl=present

## 8. Root Cause

- Airtable URL attachment patch accepted, but Marriott image URLs did not materialize into Attachment on reread for 8 non-logo records.
- Remote URL ingestion by Airtable may fail asynchronously or be blocked by source host policies.
- Patch response success does not guarantee attachment ingestion completion.
- Materialization must be confirmed by reread Attachment count > 0.

## 9. Records Proposed For Repair

- `recgq6wOvz5yOWPmY` — Ermita, Cartagena, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior): Attachment missing; local file absent, will use source fetch bytes if available

## 10. Single-Asset Materialization Preflight

- Asset record: `recgq6wOvz5yOWPmY`
- Source URL: https://cache.marriott.com/content/dam/marriott-renditions/CTGTX/ctgtx-exterior-4545-hor-wide.jpg?output-quality=70&interpolation=progressive-bilinear&downsize=1336px:*
- Current attachment count: **0**
- Proposed action: materialize_attachment_on_registry_asset_only
- Can download/read: **yes**
- Can write attachment back: **yes**

## 11. Apply result

- Files downloaded: **1**
- Registry records updated: **1**
- Registry records repaired: **1**
- Records still not materialized: **0**
- Failed updates: **0**

## 12. Apply command

```bash
npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio --asset-record-id recxVPbTlsrP9v4bQ --repair-missing-attachments --apply --approve-brand-asset-download-attachments --approve-brand-asset-attachment-materialization-repair --approve-brand-asset-single-attachment-materialization
```

## 13. Notes

- Binary files are staged under data/partner-intelligence/assets/tribute-portfolio/ on apply only. Do not commit binaries unless explicitly approved.
