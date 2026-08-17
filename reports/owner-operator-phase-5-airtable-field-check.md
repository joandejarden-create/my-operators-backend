# Owner-Operator Phase 5 — Airtable Field Check

**Date:** 2026-06-04  
**Scope:** Company Profile (`tblItyfH6MlOnMKZ9`) Owner-Operator extension fields  
**Detection:** Code mapping + `createWithUnknownFieldFallback` / `updateWithUnknownFieldFallback` (unknown columns stripped on write; app does not crash)

---

## Summary

| Field | Expected type | Read mapping | Write mapping | Manual Airtable action |
|-------|---------------|--------------|---------------|------------------------|
| Company Type | Single select | **Active** (existing) | **Active** — includes `Hotel Owner - Operator` | Add option **Hotel Owner - Operator** if missing |
| Company Type Tags | Multiple select | **Active** | **Active** (skipped if column missing) | Create field + options per schema plan |
| Workspace Access | Multiple select | **Active** | **Active** (skipped if missing) | Create field + Owner, Operator, Brand, Admin |
| Core Profile Status | Single select | **Active** | **Active** (defaults only when blank) | Create field + status options |
| Owner Profile Status | Single select | **Active** | **Active** | Create field |
| Operator Profile Status | Single select | **Active** | **Active** | Create field |
| Developer Profile Status | Single select | **Active** | **Active** | Create field |
| Operating Model | Single select | **Active** | **Active** | Create field + options |
| Third-Party Management Availability | Single select | **Active** | **Active** | Create field + Yes/No/Selectively/Case-by-case/Unknown |
| Potential Conflict Flags | Multiple select | **Active** | **Active** | Create field (optional phase) |
| Competitive Sensitivity Notes | Long text | **Active** | **Active** | Create field (optional phase) |

**Found / not found at runtime:** The app cannot introspect Airtable schema without API metadata. On write, unknown field names are removed with a dev console warning; save continues with remaining fields.

---

## Field details

### Company Type

- **Expected type:** Single select  
- **Read:** `api/company-profile.js` → `airtableCompanyTypeToFormKey()`  
- **Write:** `COMPANY_TYPE_FORM_TO_AIRTABLE` + capability derivation in `lib/company-profile-owner-operator-fields.js`  
- **Notes:** Canonical value **`Hotel Owner - Operator`**. Form keys: `owner_operator`, `hotel_owner_operator`, `owner-operator`. Do not add Developer/Investor/Broker/Lender/Service Provider until options exist in Airtable.

### Company Type Tags

- **Expected type:** Multiple select  
- **Read/Write:** `MAP_CP_AIRTABLE.companyTypeTags`  
- **Notes:** Populated from Company Settings capability checkboxes.

### Workspace Access

- **Expected type:** Multiple select  
- **Read/Write:** `MAP_CP_AIRTABLE.workspaceAccess`  
- **Notes:** Not permission enforcement alone — used with `lib/company-workspace-access.js` and `/api/me`.

### Profile status fields

- **Expected type:** Single select each  
- **Write defaults:** `applyProfileStatusDefaults()` — never overwrites `Complete` or `Needs Review`  
- **Notes:** Owner-Operator → Owner + Operator `In Progress` when blank; other side `Not Applicable` where applicable.

### Operating Model / Third-Party Management Availability

- **Write:** Validated against allowed option lists in `lib/company-profile-owner-operator-fields.js`  
- **Defaults:** From capabilities; existing third-party value preserved (no silent overwrite).

### Potential Conflict Flags / Competitive Sensitivity Notes

- **Read/Write:** Mapped; UI not expanded in Phase 5 (API-ready).

---

## Before enabling full write mapping in production

1. Confirm **Hotel Owner - Operator** exists on **Company Type**.  
2. Create **Company Type Tags**, **Workspace Access**, **Operating Model**, **Third-Party Management Availability** (minimum for Owner-Operator workflows).  
3. Create profile status fields when module completion tracking is needed.  
4. Spot-check one save in staging — watch server logs for `ignored unknown Airtable fields`.

---

## Code references

- `lib/company-profile-owner-operator-fields.js` — central mapping + derivation  
- `api/company-profile.js` — prefill + `formToAirtableFields` + unknown-field fallback  
- `reports/owner-operator-airtable-schema-plan.md` — full schema intent
