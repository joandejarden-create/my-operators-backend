# SEC EDGAR — Hotel Discovery Airtable Schema Proposal (Founder Review)

**Status:** DESIGN ONLY — not created  
**Program:** SEC EDGAR Contract / Historical Intelligence (PR #492)  
**Canonical HPC base:** `appCCUsuGsE1ifoLk`  
**Decision required before any Meta API table/field creation**

## Problem

Filesystem inventory (`data/sec-edgar/discovery-inventory/hotels.json`) is useful for processing but is **not** durable enough as the permanent unmatched-hotel research store (branch cleanup, disk loss, multi-workspace sync).

Existing HI destinations assume an **HPC hotel link**:

| Destination | Can store unmatched hotels? |
|---|---|
| HI Graph Evidence | No — field evidence is hotel-linked |
| HI Hotel–Org Relationships | No — requires HPC Hotel link |
| HI Organizations | Yes — orgs only, not properties |
| HI Ownership Research Runs | Partial — run ledger only |
| Hotel Property Census | **Do not auto-create** from SEC candidates |

## Minimal proposed extension (smallest useful)

### New table: `HI Hotel Discovery Inventory`

**Purpose:** Durable unmatched (and matched) property identities from SEC research, independent of HPC coverage.

| Field | Type | Notes |
|---|---|---|
| Discovery Property ID | singleLineText | `prop_…` / `disc_sec_…` stable id |
| Hotel Name (normalized) | singleLineText | |
| Hotel Name (raw) | singleLineText | Preserve extraction |
| Historical Names / Aliases | longText or multipleSelect | |
| City | singleLineText | |
| State / Region | singleLineText | |
| Country | singleLineText | |
| Address | singleLineText | When verified |
| Brand Family | singleSelect | MARRIOTT / HILTON / HYATT / OTHER / UNKNOWN |
| Historical Brand | singleLineText | |
| Current Brand | singleLineText | When verified |
| Identity Class | singleSelect | VERIFIED_PROPERTY / PROBABLE_PROPERTY / AMBIGUOUS_PROPERTY / … |
| Evidence Confidence | singleSelect | HIGH / MEDIUM / LOW |
| SEC Registrant | singleLineText | Filing registrant — **not** auto owner |
| Registrant CIK | singleLineText | |
| Source Filing URL | url | |
| Filing Date | date | |
| HPC Match Status | singleSelect | DISCOVERED_NOT_IN_HPC / MATCHED / AMBIGUOUS / … |
| HPC Hotel | multipleRecordLinks → Hotel Property Census | Empty until matched |
| Last Researched At | dateTime | |
| First Discovered At | dateTime | |
| Persistence Status | singleSelect | INVENTORY_ONLY / PERSISTED_VERIFIED / … |
| Notes / Quarantine Reason | longText | |

### Optional linked table: `HI Discovery Contract Findings`

| Field | Type | Notes |
|---|---|---|
| Discovery Property | link → HI Hotel Discovery Inventory | |
| Agreement Type | singleSelect | |
| Milestone Class | singleSelect | VERIFIED_EXPIRATION / … |
| Historical Applicability | singleSelect | HISTORICALLY_VERIFIED / CURRENTLY_VERIFIED / … |
| Primary Date | singleLineText | Preserve original date string |
| Snippet | longText | |
| Source URL | url | |
| Accession | singleLineText | |

### Reuse without new fields

- **HI Organizations** — create/link owners, managers, franchisors when **explicitly** evidenced (not registrant-by-default).
- **HI Hotel–Org Relationships** — only after HPC match.
- **HI Graph Evidence** — only after HPC match + verified milestone.
- **HI Ownership Research Runs** — continue as run/cost ledger.

## Migration plan (after founder approval)

1. Dry-run create table via ensure script (new, dedicated).
2. Backfill from `data/sec-edgar/discovery-inventory/hotels.json` **VERIFIED_PROPERTY + PROBABLE_PROPERTY** only.
3. Keep raw evidence JSON as immutable research archive.
4. Wire reconciliation: when HPC gains hotel → link Discovery Inventory → promote evidence.

## Explicit non-goals (this proposal)

- No auto-create of HPC hotels from SEC candidates  
- No customer publication  
- No new HPC contract-expiration columns (still use HI Graph Evidence after match)

## Founder decision checklist

- [ ] Approve table name `HI Hotel Discovery Inventory` on HPC base  
- [ ] Approve field list above (or mark removals)  
- [ ] Approve optional findings child table vs JSON attachment on parent  
- [ ] Authorize ensure-script `--apply`  
- [ ] Confirm no production customer surfaces read this table  

**Until approved:** continue filesystem + git-tracked inventory; do not discard unmatched research.
