# Commercial Acceptance — Airtable Field Spec

**Purpose:** Track public Terms acceptance and private Commercial Terms Schedule acceptance per member.

**Recommended base:** Main product base (`AIRTABLE_BASE_ID`) — same base as **Company Profile** and **Users**, so acceptances link to live member records.

**Recommended table name:** `Commercial Acceptances`

**Status:** Table created on Deal Capture MVP (`AIRTABLE_BASE_ID`).

**Table ID:** `tblznOWoTE0vF1dVG`  
**Env (optional):** `COMMERCIAL_ACCEPTANCES_TABLE_ID=tblznOWoTE0vF1dVG`

**Ensure script:**
```bash
node scripts/ensure-commercial-acceptances-table.mjs          # dry-run
node scripts/ensure-commercial-acceptances-table.mjs --apply  # create / add missing fields
```

**Related docs:**
- `docs/commercial-onboarding-process.md`
- `docs/templates/commercial-terms-schedule-founding-participant-prefilled.html`
- `public/terms.html`

---

## Table: Commercial Acceptances

One row per **acceptance event** (Terms-only or full Schedule). A member may have multiple rows over time (e.g., founding schedule → later paid schedule).

### Primary & identity

| Airtable field name | Type | Required | Notes |
|---------------------|------|----------|-------|
| `Acceptance ID` | Single line text | Yes | Primary human ID, e.g. `ACC-2026-0001` or `CTS-FOUND-001` |
| `Record Label` | Formula | — | `{Member Legal Name} & " — " & {Acceptance Type} & " (" & DATETIME_FORMAT({Accepted At}, 'YYYY-MM-DD') & ")"` |
| `Member Legal Name` | Single line text | Yes | Exact legal entity name on Schedule |
| `Company Profile` | Link → Company Profile | No | Link when member has product account |
| `Users` | Link → Users | No | Member Representative user record(s) |
| `Member Account ID` | Single line text | No | Internal slug / Webflow ID / CRM ID |

### Acceptance classification

| Airtable field name | Type | Required | Allowed options |
|---------------------|------|----------|-----------------|
| `Acceptance Type` | Single select | Yes | `Public Terms Only`, `Founding Schedule`, `Standard Schedule`, `Schedule Amendment`, `Paid Transition` |
| `Member Type` | Single select | Yes | `Owner Member`, `Brand Member`, `Operator Member`, `Advisor`, `Other` |
| `Billing Class` | Single select | Yes | `founding_complimentary`, `standard_owner`, `standard_brand`, `standard_operator`, `enterprise_custom`, `non_billing` |
| `Participation Label` | Single select | No | `Founding Participant`, `Pilot`, `Standard`, `Enterprise Custom` |

### Document versions

| Airtable field name | Type | Required | Notes |
|---------------------|------|----------|-------|
| `Terms Version` | Single line text | Yes | e.g. `2026-07-16` — matches `public/terms.html` Last Updated |
| `Schedule Version` | Single line text | No | e.g. `v1.0` |
| `Schedule Template` | Single select | No | `founding_participant_prefilled`, `standard_template`, `custom` |
| `Terms URL` | URL | No | `https://…/terms.html` at time of acceptance |

### Signatory & acceptance proof

| Airtable field name | Type | Required | Notes |
|---------------------|------|----------|-------|
| `Accepted By Name` | Single line text | Yes | Member Representative full name |
| `Accepted By Email` | Email | Yes | |
| `Accepted By Title` | Single line text | No | e.g. `Managing Director` |
| `Accepted At` | Date and time | Yes | UTC preferred; store timezone in notes if needed |
| `Acceptance Method` | Single select | Yes | `In-platform click`, `Email reply`, `DocuSign`, `PDF signature`, `Other` |
| `Acceptance Evidence` | Attachment | No | PDF, screenshot, DocuSign cert, email export |
| `Acceptance Evidence Notes` | Long text | No | Paste email reply text or DocuSign envelope ID |
| `IP Address` | Single line text | No | If captured from in-app accept |
| `User Agent` | Long text | No | If captured from in-app accept |

### Commercial term dates

| Airtable field name | Type | Required | Notes |
|---------------------|------|----------|-------|
| `Effective Date` | Date | Yes | Schedule effective date |
| `Initial Term End Date` | Date | No | For subscriptions / founding period end |
| `Founding End Date` | Date | No | Same as term end for founding rows |
| `Paid Transition Review Date` | Date | No | Reminder: 30 days before founding end |
| `Auto Renewal` | Checkbox | No | From Schedule |
| `Non-Renewal Notice Days` | Number | No | e.g. `30` |

### Fee snapshot (store what was agreed — not live billing)

| Airtable field name | Type | Required | Notes |
|---------------------|------|----------|-------|
| `List Subscription Annual USD` | Currency | No | Pre-discount / standard list price |
| `Subscription Annual USD` | Currency | No | **Net** after discount; `0` for founding |
| `Success Fee Waived` | Checkbox | No | True for founding / full waiver |
| `Upfront Submission Fee USD` | Currency | No | Usually `0` for owners |
| `List Per Key Rate USD` | Currency | No | Pre-discount per-key rate |
| `Per Key Rate USD` | Currency | No | **Net** after discount; null if waived / N/A |
| `List Minimum Success Fee USD` | Currency | No | Pre-discount minimum |
| `Minimum Success Fee USD` | Currency | No | **Net** after discount |
| `LOI Commitment Fee Pct` | Number (percent) | No | e.g. `80` |
| `Final Success Fee Pct` | Number (percent) | No | e.g. `20` |
| `Tail Period Months` | Number | No | `0` or blank if waived for founding |
| `Discount Applied` | Checkbox | No | True if any discount or waiver beyond list pricing |
| `Discount Type` | Single select | No | `Percent`, `Fixed USD`, `Full waiver`, `Custom mix`, `None` |
| `Discount Percent` | Number (percent) | No | e.g. `25` for 25% off |
| `Discount Amount USD` | Currency | No | Fixed dollar discount (annual or per deal, see notes) |
| `Discount Applies To` | Multiple select | No | Options: `Subscription`, `Success Fee`, `Per-key rate`, `Minimum fee`, `LOI Commitment Fee`, `Final Success Fee`, `Add-ons`, `First year only` |
| `Discount Duration` | Single select | No | `Entire Initial Term`, `First year only`, `Through fixed date`, `Until written notice` |
| `Discount Valid Through` | Date | No | If duration is fixed date |
| `Discount Code / Label` | Single line text | No | e.g. `FOUNDING-25`, `PILOT-50` |
| `Discount Reason` | Long text | No | Why discount was granted |
| `Discount Approved By` | Single line text | No | Internal approver |
| `Fee Notes` | Long text | No | Custom enterprise terms |

### Workflow & ops

| Airtable field name | Type | Required | Allowed options |
|---------------------|------|----------|-----------------|
| `Acceptance Status` | Single select | Yes | `Pending`, `Accepted`, `Superseded`, `Expired`, `Withdrawn` |
| `Platform Access Granted` | Checkbox | No | Check after Terms + Schedule complete |
| `Access Granted At` | Date and time | No | |
| `Granted By` | Collaborator / text | No | Internal ops owner |
| `Internal Notes` | Long text | No | Why complimentary, intro source, etc. |
| `Superseded By` | Link → Commercial Acceptances | No | Points to newer schedule row |
| `Previous Acceptance` | Link → Commercial Acceptances | No | Prior schedule this replaces |

### Contacts

| Airtable field name | Type | Required | Notes |
|---------------------|------|----------|-------|
| `Dealality Contact Email` | Email | No | Default `hello@aohospitalityadvisors.com` |
| `Member Representative Email` | Email | No | Duplicate of Accepted By Email if same |

---

## Suggested views

| View name | Filter / sort | Use |
|-----------|---------------|-----|
| **Pending acceptance** | Status = Pending | Outreach follow-up |
| **Discounted participants** | Billing Class = standard_*, Discount Applied = true | Active discounted deals |
| **Transition due 30d** | Founding End Date within next 30 days | Paid transition outreach |
| **Accepted — no access** | Status = Accepted, Platform Access Granted = false | Ops queue |
| **All by member** | Group by Member Legal Name | Member history |

---

## Minimum viable row (founding user)

Create one row when founding Schedule is accepted:

| Field | Example value |
|-------|----------------|
| Acceptance ID | `CTS-FOUND-001` |
| Member Legal Name | `Example Hospitality LLC` |
| Acceptance Type | `Founding Schedule` |
| Member Type | `Owner Member` |
| Billing Class | `founding_complimentary` |
| Terms Version | `2026-07-16` |
| Schedule Version | `v1.0` |
| Accepted By Name | `Jane Smith` |
| Accepted By Email | `jane@example.com` |
| Accepted At | `2026-07-16 15:00` |
| Acceptance Method | `Email reply` |
| Effective Date | `2026-07-16` |
| Founding End Date | `2027-07-16` |
| Subscription Annual USD | `0` |
| Success Fee Waived | ✓ |
| Discount Applied | ✓ |
| Discount Type | `Full waiver` |
| Discount Percent | `100` |
| Discount Reason | `Founding participant — complimentary access` |
| Acceptance Status | `Accepted` |
| Platform Access Granted | ✓ (after provisioning) |

**Optional second row** when they only accepted Terms first:

| Acceptance Type | `Public Terms Only` |
| Billing Class | `founding_complimentary` |
| Status | `Accepted` → later superseded by Founding Schedule row |

---

## Field mapping object (for future API/scripts)

When implementing writes from Node:

```javascript
// lib/commercial-acceptance/field-map.js (future)
export const map_commercial_acceptance_fields = {
  acceptanceId: "Acceptance ID",
  memberLegalName: "Member Legal Name",
  companyProfile: "Company Profile",
  users: "Users",
  acceptanceType: "Acceptance Type",
  memberType: "Member Type",
  billingClass: "Billing Class",
  termsVersion: "Terms Version",
  scheduleVersion: "Schedule Version",
  acceptedByName: "Accepted By Name",
  acceptedByEmail: "Accepted By Email",
  acceptedAt: "Accepted At",
  acceptanceMethod: "Acceptance Method",
  effectiveDate: "Effective Date",
  foundingEndDate: "Founding End Date",
  subscriptionAnnualUsd: "Subscription Annual USD",
  listSubscriptionAnnualUsd: "List Subscription Annual USD",
  successFeeWaived: "Success Fee Waived",
  listPerKeyRateUsd: "List Per Key Rate USD",
  perKeyRateUsd: "Per Key Rate USD",
  discountApplied: "Discount Applied",
  discountType: "Discount Type",
  discountPercent: "Discount Percent",
  discountAmountUsd: "Discount Amount USD",
  discountAppliesTo: "Discount Applies To",
  discountDuration: "Discount Duration",
  discountValidThrough: "Discount Valid Through",
  discountCodeLabel: "Discount Code / Label",
  discountReason: "Discount Reason",
  acceptanceStatus: "Acceptance Status",
  platformAccessGranted: "Platform Access Granted",
};
```

---

## Onboarding sequence → Airtable rows

| Step | Action | Airtable |
|------|--------|----------|
| 1 | User accepts public Terms | Create/update row: `Acceptance Type = Public Terms Only`, Status = Accepted |
| 2 | Send founding Schedule PDF | Create row: Status = **Pending** (or attach PDF to Pending row) |
| 3 | Member accepts Schedule | Update row: Status = Accepted, fill Accepted At / Method / Evidence |
| 4 | Provision access | Set Platform Access Granted = true, Access Granted At |
| 5 | Later: paid transition | New row: `Acceptance Type = Paid Transition`, link Previous Acceptance |

---

## Regression / data safety notes

- Do not store payment card data in this table.
- Attachments may contain confidential pricing — restrict base permissions.
- `Billing Class` and fee snapshot fields are **historical record** of what was agreed; changing standard pricing in templates does not retroactively change old rows.
- When a founding user converts to paid, **do not delete** the founding row — set Status = `Superseded` and link `Superseded By`.

---

## Manual setup checklist

- [ ] Create table `Commercial Acceptances` in product base
- [ ] Add fields from spec (or start with Minimum viable subset)
- [ ] Create views: Pending, Founding participants, Transition due 30d
- [ ] Link to Company Profile when record exists
- [ ] Restrict edit access to ops/founder roles
- [ ] Test one founding row end-to-end with prefilled HTML schedule

---

*Last updated: 2026-07-16*
