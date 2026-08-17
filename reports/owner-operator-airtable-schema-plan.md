# Owner-Operator Airtable Schema Plan

**Date:** 2026-06-04  
**Prerequisite:** `reports/owner-operator-implementation-audit.md`  
**Base:** Dealality Airtable (same `AIRTABLE_BASE_ID` as app)  
**Apply method:** **Manual in Airtable UI** (recommended). This repo has no safe automated schema migration tool for select-option creation. Document field API names here; implement code mappings after UI confirmation.

---

## Tables in scope

| Table | Table ID (from repo) | Role |
|-------|----------------------|------|
| Company Profile | `tblItyfH6MlOnMKZ9` | Company classification, workspace access, module status, conflict |
| Users | `tbl6shiyz2wdUqE5F` | Member identity; optional workspace override |
| Operator Setup - Master | env / `NEW_BASE_MASTER_TABLE` in `api/lib/operator-setup-new-base-read.js` | Explorer list, deal-request scope, operator profile completeness |

---

## 1. Company Profile fields

### 1.1 Company Type (extend existing)

| Property | Value |
|----------|--------|
| **Field name (expected)** | `Company Type` |
| **Type** | Single select |
| **Canonical** | Yes — primary company classification |

**Keep existing options (confirmed in code):**

- Hotel Owner
- Hotel Management Company
- Hotel Brands (Franchise)
- Hospitality Consultants
- Other

**Add options (product-approved; deploy code mappings **before** enabling in production):**

| New option | Description (Airtable field description) |
|------------|------------------------------------------|
| **Owner-Operator** | Owns, develops, or controls hotel assets and operates hotels (own portfolio, affiliated-owned, and/or third-party). |
| Developer | *(optional phase)* Develops hotel assets; may or may not operate. |
| Investor | *(optional phase)* Invests in hotel assets. |
| Broker / Advisor | *(optional)* Transaction advisory; distinct from Hospitality Consultants if needed. |
| Lender | *(optional)* |
| Service Provider | *(optional)* |

**TODO:** Confirm live base does not already use different spellings for Owner/Developer.

**Code impact (deploy with option):**

- `api/company-profile.js` — `COMPANY_TYPE_FORM_TO_AIRTABLE` / reverse map
- `lib/company-type-normalize.js` — **new filter key** e.g. `OWNER_OPERATOR` (must **not** fall through to `HOTEL MGMT. COMPANY` via `includes("OPERATOR")`)

---

### 1.2 Company Type Tags (new)

| Property | Value |
|----------|--------|
| **Field name** | `Company Type Tags` |
| **Type** | Multiple select |
| **Canonical** | Yes — capability tags for derivation and display |

**Options:**

- Owns Hotels
- Develops Hotels
- Operates Own Portfolio
- Operates Affiliated-Owned Hotels
- Operates Third-Party Hotels
- Brand / Franchisor
- Capital Provider
- Asset Manager
- Broker
- Consultant / Advisor
- Service Provider
- Lender

**Derivation rule (app, not formula):** If tags include ownership/development **and** any operate tag → suggest `Company Type = Owner-Operator` + `Workspace Access` Owner + Operator.

---

### 1.3 Workspace Access (new)

| Property | Value |
|----------|--------|
| **Field name** | `Workspace Access` |
| **Type** | Multiple select |
| **Canonical** | Yes — **app permissions** (not classification) |

**Options:**

- Owner
- Operator
- Brand
- Admin

**Default suggestions (app on save):**

| Company Type | Suggested Workspace Access |
|--------------|----------------------------|
| Hotel Owner | Owner |
| Hotel Management Company | Operator |
| Hotel Brands (Franchise) | Brand |
| Owner-Operator | Owner, Operator |
| Hospitality Consultants | Owner *(or none — TODO product)* |

**TODO:** Confirm whether Advisor companies get Owner workspace for deal tools.

---

### 1.4 Profile status fields (new)

| Field name | Type | Options |
|------------|------|---------|
| `Core Profile Status` | Single select | Not Started, In Progress, Complete, Needs Review |
| `Owner Profile Status` | Single select | Not Started, In Progress, Complete, Needs Review, **Not Applicable** |
| `Operator Profile Status` | Single select | Not Started, In Progress, Complete, Needs Review, **Not Applicable** |
| `Developer Profile Status` | Single select | Not Started, In Progress, Complete, Needs Review, **Not Applicable** |

**App rules:**

- Owner-Operator with Owner module → `Owner Profile Status` ≠ N/A.
- Owner-Operator with Operator module → `Operator Profile Status` ≠ N/A; link completeness to Operator Setup submission + required Profile fields (see §4).

---

### 1.5 Operating Model (new)

| Property | Value |
|----------|--------|
| **Field name** | `Operating Model` |
| **Type** | Single select |

**Options:**

- Own-and-operate only
- Affiliated-owned hotels only
- Third-party management
- Mixed owner/operator model
- Asset-light management platform
- Franchisee/operator model
- Unknown / To Confirm

---

### 1.6 Third-Party Management Availability (new)

| Property | Value |
|----------|--------|
| **Field name** | `Third-Party Management Availability` |
| **Type** | Single select |

**Options:**

- Yes
- No
- Selectively
- Case-by-case
- Unknown / To Confirm

**Explorer / deal-request rule:** Eligible only for **Yes**, **Selectively**, **Case-by-case**.

---

### 1.7 Eligibility fields

**Option A — Formula fields (preferred if maintainable in Airtable)**

| Field name | Type | Logic summary |
|------------|------|----------------|
| `Operator Explorer Eligible` | Formula (checkbox) | `FIND("Operator", {Workspace Access})` AND (`{Operator Profile Status}` = Complete OR Needs Review) AND OR(third-party Yes/Selectively/Case-by-case) AND NOT hidden — **TODO:** link to Master Active via lookup |
| `Operator Deal Request Eligible` | Formula | Explorer eligible AND open to requests — **TODO:** add `Open to Operator Opportunities` if needed |
| `Operator Alignment Snapshot Eligible` | Formula | Operator profile complete threshold — may duplicate Explorer or stricter |

**Option B — Code-only (Phase 1 deploy)**

Compute in `lib/company-workspace-access.js` + Operator Master fields; store nothing in Airtable until formulas validated.

**Recommendation:** Start **Option B** in API list filter; add formulas after CALA pilot records stabilized.

---

### 1.8 Conflict / sensitivity (new)

| Field name | Type |
|------------|------|
| `Potential Conflict Flags` | Multiple select |
| `Competitive Sensitivity Notes` | Long text |

**Potential Conflict Flags options:**

- Owns competing hotels in market
- Operates competing hotels in market
- Brand conflict possible
- Requires NDA before disclosure
- Related-party ownership
- No known conflict
- To Be Reviewed

---

### 1.9 Ecosystem role (extend existing)

| Property | Value |
|----------|--------|
| **Field name** | `Company's role in the hotel ecosystem` |
| **Type** | Single select (existing) |

**Add option:**

- `Owner-Operator - We own, develop, or control hotel assets and we operate hotels`

**Keep existing Brand / Operator / Both / Owner / Advisor / Lender options.

**Canonical hierarchy (recommended):**

1. **Company Type** + **Workspace Access** → permissions & marketplace rules  
2. **Company Type Tags** → onboarding derivation  
3. **Ecosystem role** → Partner Directory / legacy filters (sync from Type or manual)

**TODO:** Product decision on deprecating ecosystem role for new signups.

---

## 2. Users table

| Field name | Type | Required? |
|------------|------|-----------|
| `Workspace Access` | Multiple select (same options as Company Profile) | **Optional override** |

**Inheritance rule (app):**

```
effectiveWorkspaceAccess = user.workspace_access.length
  ? user.workspace_access
  : company.workspace_access
```

**Keep unchanged:**

- Platform Role `fldd5eJ32P42i17kO`
- User Type `fldkRyBI486KKY6Ps`
- Company Profile link `fldDi6uBC4TvL5kbd`
- Operator Setup - Master link (per `MAP_OPERATOR_SCOPE`)

**Memberstack:** Do **not** duplicate users for Owner-Operator.

---

## 3. Operator Setup - Master (reference)

**Existing fields (code):**

- `company_name`
- `submission_status` (Active for Explorer `activeOnly=1`)
- Profile table: `primaryServiceModel`, service models, etc.

### 3.1 Source of truth decision

| Field | Recommended canonical | Sync |
|-------|----------------------|------|
| Operating Model | **Company Profile** | Optional copy to Master on operator setup save for Explorer display |
| Third-Party Management Availability | **Company Profile** | Same |
| Operator profile completeness | **Operator Setup** tables + `Operator Profile Status` on Company Profile | App computes status on save |

**Rationale:** Explorer list reads Master/Profile today; Company Profile is company-level truth for **whether** to show in marketplace. List API should join Company Profile by `company_name` or linked record **TODO:** confirm link field Master → Company Profile exists or add lookup.

---

## 4. Lookups / links (TODO confirm in Airtable)

- [ ] Operator Setup - Master → Company Profile (link or `company_name` match)
- [ ] Users → Company Profile (exists)
- [ ] Users → Operator Setup - Master (exists per phase-2 doc)
- [ ] Deals → Company Profile (for sponsor backfill)

---

## 5. Rollback plan

1. Hide new select options (Owner-Operator) in Airtable field settings without deleting rows.
2. Clear new fields on Company Profile (bulk edit) if needed.
3. Revert code deploy — list filter returns to `activeOnly` + Active status only.
4. `Workspace Access` empty → app falls back to `classifyRole` behavior documented in audit.

---

## 6. Deployment order (safe)

1. Ship **code** with mappings + helpers (supports new values but options not in Airtable yet).
2. Add Airtable fields (empty).
3. Enable **Owner-Operator** Company Type option.
4. Pilot 2–3 CALA companies (manual).
5. Run backfill report; bulk update after review.
6. Enable Explorer eligibility filter in production.

---

## 7. Environment variables (document in `.env.example` when implemented)

```bash
# Extend role token matching (resolve-user.js)
DEALITY_OWNER_ROLES=owner,hotel owner,hotel owners,owner-operator,owner operator
DEALITY_OPERATOR_ROLES=operator,management,mgmt,hotel management,owner-operator,owner operator

# Optional field names if Airtable labels differ
AIRTABLE_COMPANY_WORKSPACE_ACCESS_FIELD=Workspace Access
AIRTABLE_COMPANY_TYPE_TAGS_FIELD=Company Type Tags
AIRTABLE_COMPANY_OPERATING_MODEL_FIELD=Operating Model
AIRTABLE_COMPANY_THIRD_PARTY_MGMT_FIELD=Third-Party Management Availability
```

---

## 8. Data Contract Snapshot (Company Profile extension)

| Item | Value |
|------|--------|
| Table | Company Profile `tblItyfH6MlOnMKZ9` |
| Mapping module | `api/company-profile.js` + `lib/company-workspace-access.js` (proposed) |
| Required for Owner-Operator | Company Type, Workspace Access, Operating Model, Third-Party Management Availability |
| Optional | Tags, profile statuses, conflict flags |
| Linked | Users, Deals (sponsor), Operator Setup (by name/link — TBD) |

---

*Schema plan — confirm field names in Airtable UI before production edits.*
