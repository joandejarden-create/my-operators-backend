# Partner Intelligence — Helena AI Outreach Intake (Airtable fields)

**Table name (exact):** `Partner Intelligence - Helena Outreach Intake`  
**Base:** Primary (`AIRTABLE_BASE_ID`)  
**Status:** Proposed.

**Purpose:** Track materials requested from and received via Helena AI outreach to brand development and hotel management company contacts. Received materials enter the same Partner Source Library → extraction → review → publish pipeline.

---

## Links

| Link field | Links to |
|------------|----------|
| **Parent Company** | Brand Basics (parent) |
| **Brand** | Brand Basics |
| **Operator / Management Company** | Operator Setup - Master |
| **Linked Source Record** | Partner Intelligence - Source Library (after upload) |

---

## Field specification

| Airtable field name | Type | Allowed values / notes |
|---------------------|------|------------------------|
| **Profile Type** | Single select | Brand, Operator |
| **Contact Name** | Single line | |
| **Contact Title** | Single line | |
| **Contact Email** | Email | |
| **Company** | Single line | |
| **Region** | Single line or select | |
| **Requested Materials** | Long text | e.g. "Current development brochure, CALA one-sheet, FDD excerpt" |
| **Received Materials** | Long text | Description of what was received |
| **Date Requested** | Date | |
| **Date Received** | Date | |
| **Source Origin** | Single select | Brand Provided, Operator Provided (auto when linked) |
| **Permission / Visibility Notes** | Long text | |
| **Confidentiality Notes** | Long text | |
| **Follow-up Needed** | Single select | Yes, No |
| **Suggested Follow-up Date** | Date | |
| **Uploaded to Partner Source Library?** | Single select | Yes, No |
| **Extraction Status** | Single select | Not Started, Ready for Extraction, Extracted, Needs Review |
| **Notes** | Long text | |

---

## Intake workflow

```
1. Create intake row when Helena sends outreach
2. On receipt: upload files to PARTNER_REFERENCE_ROOT/{Company}/
3. POST link-source → create Source Library row
   - Source Origin = Brand Provided | Operator Provided
   - Public/Private/Restricted from intake notes
   - Status = Captured
4. Set Uploaded to Partner Source Library? = Yes
5. Reviewer sets Approved for Extraction? on source
6. Standard extraction → facts → review → publish
7. Gap facts with Follow-up Question → next Helena cycle
```

---

## Gap list export for Helena

Filter Extracted Facts where:

- `Data Gap?` = Yes, OR
- `Human Review Status` = Needs More Source

Join to intake by Brand/Operator for outreach prioritization.

---

## API mapping

`MAP_PARTNER_HELENA` in `api/lib/partner-intelligence-field-map.js`.
