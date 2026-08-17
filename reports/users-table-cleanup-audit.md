# Users Table — Pilot Provisioning View Audit

**Date:** 2026-07-01  
**Base:** `appvtnDurnMSjINP6`  
**Table:** Users `tbl6shiyz2wdUqE5F`  
**Field count (live Meta API):** 97  

**Scope:** Read-only classification for a **dedicated Airtable view** named **Pilot Provisioning**.  
**Not in scope:** Deleting, renaming, or removing fields from the Users table globally; changing auth or provisioning code; altering non-provisioning operational views.

> **This audit classifies fields by whether they are needed in the Pilot Provisioning view. It does not determine whether a field is used elsewhere in Dealality. Many fields hidden from this view remain valid for other platform workflows** (onboarding, User Management, Partner Directory, brand/operator modules, dashboards, reporting, future Deal Room, region/profile logic, HO/HB intake, and deal setup).

**Confirmed context:**
- No `Workspace Access` on Users — SSOT is Company Profile → Workspace Access (`fldhZqzi0LskI0MpK`).
- Pilot deal access: Deals → Company Profile (required) + optional Deals → User_ID.
- Memberstack ID: `Unique_Webflow_ID` + `Slug` (mirror).
- Joan baseline passes `verify-pilot-user-by-email` with zero warnings.

**Classification key (view-focused):**
- **P — Pilot Provisioning view:** show in the narrow provisioning view  
- **H — Hide from Pilot Provisioning view only:** keep field and other views unchanged; may still be used elsewhere  
- **O — Optional in Pilot Provisioning view:** helpful but not required for invite readiness  
- **R — Reference only:** show read-only in provisioning view (IDs, formulas)  
- **F — Future product/schema review:** not a view decision; do not delete based on this audit alone  

**Do not:** delete fields · rename fields · remove fields from existing non-provisioning views · change auth behavior.

---

## Executive summary (view classification)

| Bucket | Approx. count | Meaning |
|--------|---------------|---------|
| **P — Show in Pilot Provisioning** | ~13 | Fields operators need to invite/verify a pilot user (see ordered list below) |
| **O — Optional in view** | ~0 | Company Name and Phone are shown in recommended list |
| **R — Reference in view** | ~2 | User_ID, Record_ID |
| **H — Hide from Pilot Provisioning only** | ~80 | Still used elsewhere; hide to reduce provisioning confusion |
| **F — Future product review** | ~0 actionable now | Long-term schema/product decisions — separate from this view |

**Three-way distinction (do not conflate):**
- **Workspace access / permissions** → Company Profile → Workspace Access  
- **User Type** → which Webflow/app pages are used (operational routing/display; **not** workspace SSOT)  
- **Contact Visibility** → whether the user/contact is shown in the platform (**not** workspace SSOT)  

**Top 5 fields to hide from Pilot Provisioning** (because they confuse *provisioning*, not because they are unused):
1. **Deals** (and related deal link columns) — reverse links; My Deals access is via Deals → Company Profile / User_ID  
2. **Deal Access** — User Management metadata; not a My Deals route gate today  
3. **Document Access** — User Management / future Deal Room metadata  
4. **HO/HB intake blocks** — onboarding/intake flows; hide from this view only  
5. **Brand/operator/favorites links** — module UIs; hide from this view only  

---

## Specific decisions (provisioning view only)

### 1. User Type (`fldkRyBI486KKY6Ps`)
| Question | Answer |
|----------|--------|
| Workspace access? | **No** — not workspace SSOT (Company Profile → Workspace Access). May contribute legacy inference only when CP WS is empty. |
| Operational use | **Yes** — determines which **Webflow/app pages** are used; Partner Directory; signup/MS fill-if-blank. |
| Pilot Provisioning view? | **Show** — operators must set/review for correct app page routing. |
| Do not delete/rename | **Yes** |

### 1b. Contact Visibility (`fld1ojjulh0kXnwYV`)
| Question | Answer |
|----------|--------|
| Workspace access? | **No** |
| Operational use | **Yes** — controls whether the user/contact is **shown in the platform**. |
| Pilot Provisioning view? | **Show** — operators must set/review for platform visibility. |
| Do not delete/rename | **Yes** |

### 2. Deal Access (`fldcJ1KOo3ZGhkhiY`)
| Question | Answer |
|----------|--------|
| My Deals gate? | **No** — `deal-record-access.js` uses Deals → Company Profile / User_ID. |
| Used elsewhere? | **Yes** — `api/user-management.js` (Company Settings admin UI). |
| Pilot Provisioning view? | **Hide from view only** — may become Deal Room / access metadata later. |

### 3. Document Access (`fldZkgnDnZQNBZged`)
| Question | Answer |
|----------|--------|
| Enforced on deal routes today? | **No** |
| Used elsewhere? | **Yes** — User Management CRUD; potential future Deal Room controls. |
| Pilot Provisioning view? | **Hide from view only** |

### 4. Deals on Users (`fldNCCtmyOtnJtmW5`)
| Question | Answer |
|----------|--------|
| Reverse link? | **Yes** |
| Grants My Deals access alone? | **No** |
| Used elsewhere? | **Yes** — dashboards, audits, historical reporting, reverse navigation. |
| Pilot Provisioning view? | **Hide** Deals and related deal link columns **from this view only** |

### 5. Role / Platform Role / Auth Role Hint / Memberstack Role Hint
| Field | In live base? | Pilot Provisioning view |
|-------|---------------|-------------------------|
| Platform Role | **No** | N/A — code fallback name only |
| Role | **No** | N/A |
| Auth Role Hint / Memberstack Role Hint | **No** | N/A |
| User Type | **Yes** | **Show** — Webflow/app page routing; not workspace SSOT |

### 6. Company Name on Users (`fldAQaiOiYZzqEzGG`)
| Question | Answer |
|----------|--------|
| Used elsewhere? | **Yes** — `/api/me`, signup, MS backfill, display. |
| Company SSOT for permissions? | **Company Profile** |
| Pilot Provisioning view? | **Optional** — label as signup/display hint, not permission source |

### 7. Brand / operator / favorites links on Users
Used by brand explorer, operator setup, favorites, and related modules.  
**Pilot Provisioning view:** hide from this view only — **do not delete**; keep in operational views.

### 8. HO / HB intake fields
May be used by owner/brand onboarding, intake, deal setup, or legacy forms (`api/me` reads HO - PI - Regions; brand dashboard).  
**Pilot Provisioning view:** hide all HO - * and HB - * **from this view only** — not a statement that intake is unused globally.

### 9. Memberstack / identity (show in Pilot Provisioning)
| Field | ID | Pilot Provisioning view |
|-------|-----|-------------------------|
| Email | fldBl7IXEscwkMhnZ | **Show** |
| Unique_Webflow_ID | flddTfp7oLdcPwBIC | **Show** (edit via link script only) |
| Slug | fldEgbHu5MvfyrxgE | **Show** (mirror) |
| Account Status | fldbihZblnprYFTUX | **Show** |
| Company Profile | fldDi6uBC4TvL5kbd | **Show** |
| First Name / Last Name | fldG5nb… / fldV0g… | **Show** |

---

## Pilot-critical field matrix

| Field | ID | Pilot view | Used elsewhere? | Pilot provisioning action |
|-------|-----|------------|-----------------|---------------------------|
| User_ID | fldUX9GvjFcIbuzAR | R | Yes | Show as reference; do not edit |
| Record_ID | fld8YSQaChTZCexeL | R | Yes | Show as reference; formula — do not edit |
| Unique_Webflow_ID | flddTfp7oLdcPwBIC | P | Yes | Show; edit only via link script |
| Slug | fldEgbHu5MvfyrxgE | P | Yes | Show; mirror MS id |
| Email | fldBl7IXEscwkMhnZ | P | Yes | Show |
| First Name / Last Name | fldG5nb… / fldV0g… | P | Yes | Show |
| Company Profile | fldDi6uBC4TvL5kbd | P | Yes | Show |
| Account Status | fldbihZblnprYFTUX | P | Yes | Show |
| Company Name | fldAQaiOiYZzqEzGG | O | Yes | Optional; signup/display hint |
| User Type | fldkRyBI486KKY6Ps | P | Yes | **Show** — app page routing; not workspace SSOT |
| Contact Visibility | fld1ojjulh0kXnwYV | P | Yes | **Show** — platform visibility; not workspace SSOT |
| Deal Access | fldcJ1KOo3ZGhkhiY | H | Yes (User Management) | Hide from view only |
| Document Access | fldZkgnDnZQNBZged | H | Yes (User Management / future Deal Room) | Hide from view only |
| Deals (+ related deal links) | fldNCCtmyOtnJtmW5 … | H | Yes (reporting/dashboards) | Hide from view only |
| Phone Number | fldRqaLX4bxkJzIik | O | Yes | Optional in view |
| Region / language fields | various | H | Yes (User Management, `/api/me`, directory) | Hide from view only |
| HO / HB intake | 28 fields | H | Yes (intake/onboarding flows) | Hide from view only |
| Brand/operator/favorites links | various | H | Yes (module UIs) | Hide from view only |
| Metrics / responsiveness badges | various | H | Yes (directory/reporting) | Hide from view only |
| Created / Last Modified | system | H | Yes | Hide from view (optional) |

---

## Grouped fields — hide from Pilot Provisioning view only

### Deal relationship links
Deals, Active Deals, Archived Deals, Deals Visited, Received Deals, Declined Deals, Deal Interactions, Deal Status History, Deal Actions, Deal Actions 2, Hotel Ownership, Outreach Setup — **may still be used** for dashboards, workflow, and reporting; **not** My Deals access gates.

### Region / language fields
Region checkboxes, Coverage Territories, Languages, HO - PI - Regions — **may still be used** by User Management, Partner Directory, `/api/me`, profile, or future access logic. Hide from Pilot Provisioning view only.

### HO intake (17 × `HO - `)
**May still be used** by owner onboarding, intake, or deal setup. Hide from Pilot Provisioning view only.

### HB intake (11 × `HB - `)
**May still be used** by brand onboarding or legacy signup. Hide from Pilot Provisioning view only.

### Partner directory / metrics
Closed Deals, Unique Brands (Deals), Submitted Bids, responsiveness_* , Brands Supported — **used** by Partner Directory and internal reporting. Hide from Pilot Provisioning view only.

### Feature module links
Brand Setup, favorites, Brand Deal Preferences, Operator Setup - Master, Partner Intelligence, Capital Setup links — **used** by respective platform sections. Hide from Pilot Provisioning view only.

---

## Code references (where Users fields are read — not view recommendations)

| Area | Users fields |
|------|----------------|
| `resolve-user.js` / `/api/me` | Email, MS ids, Company Profile, Account Status, User Type, HO - PI - Regions |
| `deal-record-access.js` | Deals table Company Profile + User_ID (not Users deal links) |
| `api/user-management.js` | Deal Access, Document Access, regions, profile |
| `partner-directory.js` | User Type, metrics, company fields |
| Pilot scripts | Email, MS ids, Account Status, Company Profile |

---

## Recommended Pilot Provisioning view (Airtable only)

**Create or update one dedicated view:** `Pilot Provisioning`  
Duplicate an existing grid view — **do not** change other operational views.

### Show in this view (recommended order)
1. User_ID  
2. Record_ID  
3. Unique_Webflow_ID  
4. Slug  
5. Email  
6. First Name  
7. Last Name  
8. Account Status  
9. User Type  
10. Contact Visibility  
11. Company Profile  
12. Company Name  
13. Phone Number  

### Hide from this view only (keep in other views)
- Deal Access, Document Access  
- All Deals* and deal workflow link fields  
- Region checkboxes, Coverage Territories, Languages, HO - PI - Regions  
- All HO - * and HB - * fields  
- Brand / operator / favorites module links  
- Partner directory metrics and responsiveness badges  
- Profile, Title, signup narrative fields (unless support needs them in this view)  
- Created, Last Modified *(optional hide)*  

**View description (recommended):**  
*Pilot invite provisioning only. Workspace access is controlled on Company Profile → Workspace Access. User Type controls which Webflow/app pages are used. Contact Visibility controls whether the user is shown in the platform. Fields hidden here may still be used elsewhere in Dealality.*

---

## Manual Airtable action plan

1. Duplicate an existing Users grid view → name **Pilot Provisioning**.  
2. Show only the fields listed under “Show in this view” above.  
3. Hide other columns **in this view only** — do not remove fields from the table or from other views.  
4. **Do not delete, rename, or globally hide any field.**  
5. Re-run Joan verifier after the view is saved (views-only change).  

---

## Future product review (separate from this view audit)

These are **not** Pilot Provisioning view instructions and **not** delete recommendations:
- Long-term schema consolidation (HO/HB on Users vs deal intake tables)  
- Company Name vs Company Profile alignment  
- Whether Deal Access / Document Access become enforced Deal Room controls  
- Code-only field names (Platform Role, Auth Role Hint) not present on live base  

Any future deletion requires formula/lookup dependency audit and explicit product sign-off.

---

## Tests after Pilot Provisioning view setup (views only)

```bash
node scripts/verify-pilot-user-by-email.mjs --email joan@aohospitalityadvisors.com
npm run test:batch1-route-auth
npm run test:batch2a-route-auth
node scripts/test-pilot-provisioning-validators.mjs
node scripts/test-memberstack-signup-path-alignment.mjs
```

---

## Code/docs cross-reference

- Runbook: **§2b Users Pilot Provisioning View** (`lib/support/owner-pilot-provisioning-runbook.js`)  
- Schema export: `reports/users-table-schema-export.json`  

Generated by Users Pilot Provisioning View Audit — read-only.
