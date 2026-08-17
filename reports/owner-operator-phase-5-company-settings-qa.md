# Owner-Operator Phase 5 — Company Settings QA

**Date:** 2026-06-04  
**Change impact:** **High** (Company Profile Airtable writes, classification derivation)

---

## Files changed

| File | Change |
|------|--------|
| `lib/company-profile-owner-operator-fields.js` | **New** — field map, capability derivation, profile status defaults |
| `api/company-profile.js` | Read/write extension fields; prefill enrichment |
| `public/company-settings.html` | Capabilities, Operating Model, Third-Party Availability UI |
| `public/js/company-profile-capabilities.js` | **New** — browser derivation helper |
| `server.js` / `server.upload-ready.js` | Parse JSON array fields on company profile save |
| `scripts/test-company-profile-owner-operator.mjs` | **New** — mapping + derivation tests |
| `reports/owner-operator-phase-5-airtable-field-check.md` | Field checklist |
| `reports/owner-operator-phase-5-company-settings-qa.md` | This report |

**Not changed (per scope):** workspace switcher, Memberstack plans, production backfill, Operator Alignment Snapshot.

---

## Field mappings added

- Company Type Tags ← capability checkboxes  
- Workspace Access ← derived from tags  
- Operating Model, Third-Party Management Availability ← UI + smart defaults  
- Core / Owner / Operator / Developer Profile Status ← defaults on save when blank  
- Potential Conflict Flags, Competitive Sensitivity Notes ← API-ready (no new UI)

**Canonical Company Type write:** `Hotel Owner - Operator` (`owner_operator` form key).

---

## Fields requiring manual Airtable creation

See `reports/owner-operator-phase-5-airtable-field-check.md`. Minimum recommended before relying on writes:

1. `Hotel Owner - Operator` on **Company Type**  
2. **Company Type Tags**, **Workspace Access**  
3. **Third-Party Management Availability**, **Operating Model**

---

## Test results

```bash
node scripts/test-company-profile-owner-operator.mjs
node scripts/test-company-type-normalize.mjs
node scripts/test-company-workspace-access.mjs
```

| Test | Result |
|------|--------|
| `owner_operator` / `hotel_owner_operator` → Airtable `Hotel Owner - Operator` | Pass |
| Read `Hotel Owner - Operator` → `owner_operator` | Pass |
| Tags: Owns + Operates Own → Owner-Operator, workspaces Owner+Operator, third-party default No | Pass |
| Tags: Owns + Operates Third-Party → Owner-Operator, third-party default Yes | Pass |
| Tags: Operates Third-Party only → Hotel Management Company, Operator workspace | Pass |
| Tags: Owns only → Hotel Owner, Owner workspace | Pass |
| Existing Third-Party `Selectively` not overwritten | Pass |
| Missing Airtable columns — no throw (unknown field fallback) | Pass (existing pattern) |

---

## Regression — existing Company Settings

| Area | Risk | Retest |
|------|------|--------|
| Basics save (name, website, employees, HQ) | Low | Save company profile without touching capabilities |
| Regions / services / brands | Low | Unchanged paths |
| Ecosystem role select | Low | Still separate from Company Type |
| Logo upload | Low | POST/PATCH multipart |
| Readonly Company Type display | Medium | Prefill shows label; hidden sends form key |
| Company Type from account only (no capabilities) | Medium | Hidden `companyType` from prefill still saves |

---

## Confirmations

| Requirement | Status |
|-------------|--------|
| Hotel Owner - Operator Airtable mapping | Implemented |
| Workspace Access Owner + Operator for Owner-Operator | Derived on save when capabilities selected |
| Third-Party Management Availability mapping | UI + API; no silent overwrite of existing value |
| No workspace switcher | Not built |
| No production data migration | Not performed |
| No new Memberstack plans | Not added |

---

## Manual QA checklist

1. Open **Company Settings** — capability checkboxes and Operating Model / Third-Party fields visible.  
2. Load existing profile (`/api/company-profile/mine`) — no console errors; capabilities pre-check from tags if present.  
3. Select **Owns** + **Operates own portfolio** — Company Type display shows **Hotel Owner - Operator**; save succeeds.  
4. With existing Third-Party = **Selectively**, change only capabilities — Third-Party stays **Selectively**.  
5. Staging Airtable without new columns — save still works; check logs for ignored unknown fields.  
6. Ecosystem role + primary services validation unchanged.

---

## Deferred items

- Workspace switcher UI (`dealality_active_workspace`)  
- Potential Conflict Flags / Competitive Sensitivity Notes UI  
- Production Airtable schema rollout / backfill  
- Developer / Investor / Broker / Lender / Service Provider Company Type options until added in Airtable  
- Server returning `warnings` array to client on save (stub empty today)

---

## Rollback

Revert `lib/company-profile-owner-operator-fields.js` and related imports in `api/company-profile.js`; remove capability block from `company-settings.html`. Existing Company Type mapping in API remains if only partial revert.

---

## Data contract snapshot

| Item | Value |
|------|--------|
| Table | Company Profile `tblItyfH6MlOnMKZ9` |
| Mapping module | `MAP_CP_AIRTABLE` in `lib/company-profile-owner-operator-fields.js` |
| Required (unchanged) | Company Name, Website, Employees, HQ, Overview, Regions, Brands, Primary Services, Ecosystem Role |
| Optional (new) | Tags, Workspace Access, Operating Model, Third-Party Availability, profile statuses |
| Select sources | Constants in `lib/company-profile-owner-operator-fields.js` |
| Prefill shape | Extended `prefill` on GET prefill/mine |
