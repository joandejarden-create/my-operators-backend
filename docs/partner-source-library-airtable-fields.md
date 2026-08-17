# Partner Intelligence — Source Library (Airtable fields)

**Table name (exact):** `Partner Intelligence - Source Library`  
**Base:** Primary (`AIRTABLE_BASE_ID`)  
**Status:** Proposed — not created in Airtable until `scripts/ensure-partner-intelligence-tables.mjs --apply` with explicit approval.

**Purpose:** Canonical registry of every source document, webpage, PDF, deck, or internal note used to support brand, operator, or parent-company intelligence. Supports both brands and operators.

---

## Links (required context)

| Link field | Links to | Required |
|------------|----------|----------|
| **Parent Company** | `Brand Setup - Brand Basics` (filter: parent-company rows) OR future Parent Company table | One of Parent / Brand / Operator required |
| **Brand** | `Brand Setup - Brand Basics` | Optional if operator-only source |
| **Operator / Management Company** | `Operator Setup - Master` | Optional if brand-only source |
| **Related Contact** | `Users` or future Contacts table | Optional |
| **Duplicate Of** | Self-link → same table | Optional |

---

## Field specification

| Airtable field name | Type | Allowed values / notes |
|---------------------|------|------------------------|
| **Profile Type** | Single select | Brand, Operator, Parent Company, Service Provider, Other |
| **Region** | Single select or text | CALA, US, Europe, Global, etc. (align with Dealality region taxonomy) |
| **Country / Market** | Single line or multi-select | Free text or controlled list |
| **Source Title** | Single line | Required |
| **Source Type** | Single select | FDD, Development Brochure, Development Page, Brand Page, Operator Capability Deck, Owner Presentation, Portfolio Page, Case Study, Press Release, Investor Presentation, RFP Response, Internal Note, Website Capture, Other |
| **Source URL** | URL | Public sources; empty if file-only |
| **Source File** | Attachment | PDF, PPTX, DOCX, etc. |
| **File Type** | Single select | PDF, PPTX, DOCX, XLSX, HTML, PNG/JPG, TXT, Other |
| **Source Date** | Date | Document date if known |
| **Capture Date** | Date | When Dealality captured/archived |
| **Source Origin** | Single select | Public Web, Brand Provided, Operator Provided, Internal Upload, FDD Library, Press Release, RFP Response, Other |
| **Public / Private / Restricted** | Single select | Public, Private, Restricted |
| **Verified Source?** | Single select | Yes, No |
| **Source Quality** | Single select | High, Medium, Low |
| **Status** | Single select | Found, Captured, Classified, Extracted, Needs Review, Approved, Rejected, Stale |
| **Notes** | Long text | Internal |
| **Last Reviewed** | Date | |
| **Reviewed By** | Collaborator or link to Users | |
| **Approved for Extraction?** | Single select | Yes, No |
| **Approved for Explorer Use?** | Single select | Yes, No |
| **Confidentiality Notes** | Long text | |
| **Permission / Visibility Notes** | Long text | |
| **Extraction Run ID** | Single line | Latest run correlation id |
| **Local File Path** | Single line | Relative path under `PARTNER_REFERENCE_ROOT` (server-side only; not shown in public UI) |

---

## Source quality hierarchy (guidance for reviewers)

### Hotel brands

1. Current FDD → **High** (when verified current filing)
2. Current official development brochure → **High**
3. Official development page → **High/Medium**
4. Official brand page → **Medium**
5. Official press release → **Medium**
6. Investor presentation → **Medium**
7. Reputable trade publication → **Medium/Low**
8. Third-party PDF copy → **Low** (Verified Source = No)
9. Old or unverified source → **Low** + consider **Stale**

### Operators / HMCs

1. Current official capability deck → **High**
2. Current official owner/developer presentation → **High**
3. Official company website → **Medium**
4. Official portfolio page → **Medium**
5. Official case study → **Medium**
6. Official press release → **Medium**
7. Reputable trade publication → **Medium/Low**
8. Third-party profile → **Low**
9. Old or unverified → **Low** + **Stale**

**Auto-discovery defaults:** Status = **Found** or **Captured**; Verified Source = **No**; Approved for Extraction = **No** until reviewer action.

---

## Status workflow

```
Found → Captured → Classified → [Approved for Extraction? Yes] → Extracted → Needs Review
                                                              ↓
                                    Approved (for archive) / Rejected / Stale
```

---

## API mapping object keys

Central map: `api/lib/partner-intelligence-field-map.js` → `MAP_PARTNER_SOURCE`.

---

## Initial seed targets (Phase 8 discovery)

**Parent companies / brands:** Marriott, Hilton, Hyatt, IHG, Choice, Radisson/Choice International, Wyndham, Accor, BWH, Minor/NH, Sonesta.

**Operators:** Hotel Equities, Arbor Lodging, GHL, Aimbridge, Highgate, Remington, Pyramid Global Hospitality, Davidson Hospitality, Playa Hotels & Resorts, Driftwood, HEI Hotels & Resorts, Concord Hospitality, Valor Hospitality, GF Hotels & Resorts, PM Hotel Group, Schulte Hospitality, HVMG.

Folder layout under reference root: `{Parent Company Name}/` or `{Operator Name}/`.
