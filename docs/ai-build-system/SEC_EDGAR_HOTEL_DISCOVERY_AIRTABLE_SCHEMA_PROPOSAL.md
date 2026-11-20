# SEC EDGAR — Hotel Discovery Airtable Schema Proposal (Founder Review)

**Status:** APPROVED + APPLIED (V2.3) — tables created on HPC base  
**Program:** SEC EDGAR Contract / Historical Intelligence (PR #492)  
**Canonical HPC base:** `appCCUsuGsE1ifoLk`  
**Applied tables:** `HI Hotel Discovery Inventory` (`tblx77Nmh56h4xa9z`), `HI Discovery Findings` (`tblGjTWNm8G4UOF1e`)  
**Graph Evidence reuse:** DO_NOT_REUSE_FOR_UNMATCHED — see contract `discovery-inventory-airtable-contract-v23.js`

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

### Linked table: `HI Discovery Findings` (general-purpose — applied)

Prefer this over a contract-only child table. Supports ownership, management, brand, contractual, transaction, and other findings as discrete rows (not JSON blobs).

| Field | Type | Notes |
|---|---|---|
| Finding ID | singleLineText | Primary |
| Discovery Property | link → HI Hotel Discovery Inventory | |
| Finding Type | singleSelect | CONTRACT_MILESTONE / OWNERSHIP / MANAGEMENT / … |
| Field Name | singleLineText | |
| Value Observed | multilineText | |
| Agreement Type | singleSelect | where relevant |
| Historical Applicability | singleSelect | |
| Effective / Event Date | date | |
| Reporting Date | date | |
| Source URL | url | |
| SEC Accession | singleLineText | |
| Registrant CIK | singleLineText | |
| Evidence Snippet | multilineText | |
| Confidence | singleSelect | |
| Verification Status | singleSelect | |
| Related Organization | link → HI Organizations | optional |
| Related Organization Name | singleLineText | when org not yet linked |
| Superseded By | singleLineText | |
| HPC Promotion Status | singleSelect | NOT_PROMOTED / ELIGIBLE / … |

### Reuse without new fields

- **HI Organizations** — create/link owners, managers, franchisors when **explicitly** evidenced (not registrant-by-default).
- **HI Hotel–Org Relationships** — only after HPC match.
- **HI Graph Evidence** — only after HPC match + verified milestone.
- **HI Ownership Research Runs** — continue as run/cost ledger.

## Expected initial record counts (order-of-magnitude)

| Store | Expected after V2.2 DiamondRock-quality pass | Notes |
|---|---|---|
| HI Hotel Discovery Inventory | ~40–2,000 rows | Start with VERIFIED + PROBABLE only; grow with CORE_SIX |
| HI Discovery Contract Findings | ~0–5,000 | Only dated milestones; many portfolio rows have no date |
| HI Discovery Org Mentions (optional) | ~100–10,000 | Manager/franchisor/owner mentions before HPC link |
| Filesystem raw evidence | retain all | Immutable; source of truth until Airtable approved |

## Optional: `HI Discovery Org Mentions` (unmatched relationships)

For owner/operator/franchisor evidence **before** HPC match (HI Hotel–Org Relationships requires HPC link):

| Field | Type | Notes |
|---|---|---|
| Discovery Property | link → Inventory (optional) | Null for org-only mentions |
| Role | singleSelect | LEGAL_OWNER / MANAGEMENT_COMPANY / FRANCHISOR / … |
| Entity Name | singleLineText | |
| Relationship Status | singleSelect | CURRENTLY_VERIFIED / HISTORICALLY_VERIFIED / SUPERSEDED / CONFLICTED / CURRENT_STATUS_UNKNOWN |
| Effective Period Start/End | date | When disclosed |
| Source URL | url | Exhibit or primary doc |
| Parent Accession | singleLineText | |
| Explicit Evidence | checkbox | Must be true |
| Inferred From Portfolio Listing | checkbox | Must stay false for ownership claims |

## Migration plan (after founder approval)

1. Dry-run create table via ensure script (new, dedicated).
2. Backfill from `data/sec-edgar/discovery-inventory/hotels.json` **VERIFIED_PROPERTY + PROBABLE_PROPERTY** only.
3. Keep raw evidence JSON as immutable research archive.
4. Wire reconciliation: when HPC gains hotel → link Discovery Inventory → promote evidence.
5. Sync/recovery: Discovery Property ID is the idempotency key; never delete historical findings on re-import.

## Explicit non-goals (this proposal)

- No auto-create of HPC hotels from SEC candidates  
- No customer publication  
- No new HPC contract-expiration columns (still use HI Graph Evidence after match)

## Founder decision checklist

- [x] Approve table name `HI Hotel Discovery Inventory` on HPC base  
- [x] Approve field list above (or mark removals)  
- [x] Prefer general-purpose `HI Discovery Findings` (not JSON blob)  
- [x] Authorize ensure-script `--apply` (V2.3)  
- [x] Confirm no production customer surfaces read this table (`Publication State` = CUSTOMER_BLOCKED)

**Ongoing:** filesystem raw archive retained for ambiguous/rejected candidates; Airtable holds VERIFIED + PROBABLE cohorts only.
