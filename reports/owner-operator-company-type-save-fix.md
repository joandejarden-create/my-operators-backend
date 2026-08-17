# Owner-Operator Company Type Save Fix (Round 2)

**Date:** 2026-06-04  
**Issue:** Airtable **Company Profile > Company Type** still stored `owner_operator` after Round 1 mapping fix.

---

## Why Round 1 did not resolve manual QA

Round 1 added `toAirtableCompanyType()` and `sanitizeCompanyTypeFieldOnPayload()` inside `formToAirtableFields()` / `mergeOwnerOperatorExtensionFields()`. Unit tests passed, but QA still showed `owner_operator` in Airtable. Likely causes:

| # | Finding |
|---|---------|
| 1 | **No guard at the Airtable boundary** — Sanitization ran while building the payload, but **`createWithUnknownFieldFallback` / `updateWithUnknownFieldFallback` did not re-sanitize** immediately before `.create()` / `.update()`. Any regression or stray key could still reach Airtable. |
| 2 | **`typecast: true`** — Airtable can **create new single-select options** from arbitrary strings. If `owner_operator` ever reached the API (stale server, cached HTML, or lookup miss), typecast would **persist `owner_operator` as a valid option** and keep selecting it. |
| 3 | **Stray form keys on payload** — If `companyType` (form key) was ever copied onto the fields object alongside `"Company Type"`, typecast could treat unknown keys unpredictably. |
| 4 | **Capability tags as strings** — UI JSON sometimes sends tag labels (`"Owns Hotels"`) instead of capability ids; derivation was skipped while `companyType` stayed `owner_operator`. Mapping should still fix type — unless the running server had not been restarted with Round 1 code. |
| 5 | **Only write path** — Grep confirms **Company Profile writes only in `api/company-profile.js`** (`tblItyfH6MlOnMKZ9`). No other route writes `Company Type` on that table. |

**Exact write path:**  
`POST/PATCH /api/company-profile` → `parseCompanyProfileArrays` (server.js) → `createCompanyProfile` / `updateCompanyProfile` → `formToAirtableFields(req.body)` → `createWithUnknownFieldFallback` / `updateWithUnknownFieldFallback` → Airtable SDK.

Round 1 fixed mapping **inside** `formToAirtableFields` but did not harden the **last mile** before Airtable.

---

## Round 2 fix

### 1. `finalizeCompanyProfileFieldsForAirtableWrite()` (lib)

Called:

- End of `mergeOwnerOperatorExtensionFields`
- End of `formToAirtableFields`
- **`prepareCompanyProfileFieldsForAirtableWrite()`** before create/update
- **Again inside the retry loop** immediately before each Airtable call

Behavior:

- Maps `"Company Type"` with `toAirtableCompanyType()`
- **Deletes** stray keys: `companyType`, `company_type`, `derivedCompanyType`, `companyTypeKey`, `companyTypeDisplay`
- If value is still internal (`owner_operator`, etc.) → force **`Hotel Owner - Operator`**
- **Non-production:** throws if an internal key would still be written (`loud: true` on Airtable path)

### 2. `typecast: false` on Company Profile create/update

Prevents Airtable from auto-creating `owner_operator` as a select option. Valid values must match existing options (canonical **`Hotel Owner - Operator`** must exist in Airtable).

### 3. `isInternalCompanyTypeKey()` + stronger `pickFirstCompanyTypeInput`

Handles arrays and bracket-wrapped values.

### 4. `capabilitiesToTags()` accepts tag labels or capability ids

### 5. `server.js` / `server.upload-ready.js`

Deletes `req.body["Company Type"]` if a form accidentally posts the Airtable column name.

### 6. Dev log before write

`[company-profile] Airtable write Company Type: …` (non-production).

---

## Files changed

| File | Change |
|------|--------|
| `lib/company-profile-owner-operator-fields.js` | `finalizeCompanyProfileFieldsForAirtableWrite`, `isInternalCompanyTypeKey`, tag-aware `capabilitiesToTags` |
| `api/company-profile.js` | `prepareCompanyProfileFieldsForAirtableWrite`; finalize before every Airtable call; **`typecast: false`** |
| `server.js`, `server.upload-ready.js` | Strip `req.body["Company Type"]` |
| `scripts/test-company-profile-owner-operator.mjs` | Full save simulation + leak tests |
| `reports/owner-operator-company-type-save-fix.md` | This report |

---

## Before / after

| | Before | After |
|---|--------|--------|
| Payload at Airtable SDK | Could be `owner_operator` (typecast created option) | **`Hotel Owner - Operator`** only |
| Internal key on payload | Possible | **Removed / blocked** |
| typecast | `true` | **`false`** for Company Profile |

---

## Test results

```bash
node scripts/test-company-profile-owner-operator.mjs
```

| Case | Expected | Status |
|------|----------|--------|
| `companyType: "owner_operator"` + tag capabilities | `"Company Type": "Hotel Owner - Operator"` | Pass |
| `companyType: ["owner_operator"]` | Canonical Airtable value | Pass |
| Stray `companyType` on payload object | Stripped | Pass |
| `{ "Company Type": "owner_operator" }` finalize | Canonical | Pass |
| Never `"Company Type": "owner_operator"` | — | Pass |

---

## Manual QA instructions

1. **Restart the Node server** (local or redeploy Railway) so Round 2 code is running.
2. In Airtable **Company Type** field options, **delete or rename** the invalid option `owner_operator` (typecast may have created it).
3. Confirm the canonical option **`Hotel Owner - Operator`** exists (exact spelling).
4. Open **Company Settings** for Dealality Owner Demo.
5. Select capabilities (e.g. owns + operates third-party) — UI should show **Hotel Owner - Operator**.
6. **Save** the profile.
7. In Airtable, **Company Type** must be **`Hotel Owner - Operator`**.
8. It must **not** be `owner_operator`.
9. (Optional) Check server log: `[company-profile] Airtable write Company Type: Hotel Owner - Operator`.

If save fails with an invalid select error, add **`Hotel Owner - Operator`** to the Airtable field options (do not add `owner_operator`).

---

## Confirmation

- **Normalized key** `owner_operator` remains **internal** (hidden input / API only).
- **Airtable** stores **`Hotel Owner - Operator`** only.
- **Final payload guard** runs immediately before every Company Profile create/update.
