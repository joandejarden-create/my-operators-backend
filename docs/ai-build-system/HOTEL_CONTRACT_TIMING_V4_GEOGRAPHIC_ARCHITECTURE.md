# Hotel Contract Timing V4 — Geographic Architecture & Schema (Founder-Approved)

**Status:** FOUNDER APPROVED WITH REVISIONS (2026-11-20) — **schema Meta apply NOT yet executed**  
**Gate:** PR #495 must merge + `main` synchronized before V4 implementation PR  
**Canonical base:** `appCCUsuGsE1ifoLk` (no second research base)  
**Do not extend PR #495**

## Founder decisions (binding)

| Layer | Scope |
|---|---|
| **Research / intelligence collection** | **GLOBAL** |
| **Systematic HPC enrichment** | **PRIORITY — Americas + Europe** (incl. Caribbean) |
| **Opportunistic HPC link** | Any global discovery already in HPC |
| **Controlled HPC hotel creation** | **`VERIFIED_DISCOVERY_TO_HPC_CREATION`** — allowed for verified discoveries (priority Americas/Europe); **no bulk auto-create** |

Do **not** discard research outside priority regions. Geography = **physical hotel location**, not registrant HQ or filing jurisdiction.

Region class is **independent** of identity confidence, verification, and publication state.

## Processing classes

| Class | Meaning |
|---|---|
| `PRIORITY_HPC_ENRICHMENT` | Americas + Caribbean + Europe — active match, alias resolve, relationship/contract linkage, controlled HPC create when missing |
| `GLOBAL_RESEARCH_STANDARD` | Rest of world — persist globally; intensive HPC enrichment deferred unless authorized |
| `EXISTING_HPC_OPPORTUNISTIC` | Already in HPC anywhere — safe verify/link |

## Live schema comparison (2026-11-20 Meta API read)

| Table | Table ID | Exists | Notes |
|---|---|---|---|
| Hotel Property Census | `tbl9aY5ijiuIzzWam` | Yes (154 fields) | Inverse link already → HI Hotel Discovery Inventory |
| HI Organizations | `tbliXEfUOWk32AN9y` | Yes | |
| HI Hotel–Org Relationships | `tblGiwN0foXXDESvZ` | Yes | Requires HPC Hotel link |
| HI Graph Evidence | `tblc9JhKT1edr4grE` | Yes | Requires HPC link — still post-match only for promotion |
| HI Hotel Discovery Inventory | `tblx77Nmh56h4xa9z` | Yes (32 fields) | Links: HPC Hotel, HI Discovery Findings |
| HI Discovery Findings | `tblGjTWNm8G4UOF1e` | Yes (21 fields) | Links: Discovery Property, Related Organization |
| **HI Hotel Contracts** | — | **No** | Approved — create pending Meta apply |
| **HI Contract Milestones** | — | **No** | Approved — create pending Meta apply |

Existing link graph (verified):

```
HPC ←→ HI Hotel Discovery Inventory → HI Discovery Findings → HI Organizations
HPC ← HI Hotel–Org Relationships → HI Organizations
HPC ← HI Graph Evidence
```

Required new links after apply:

```
HI Hotel Discovery Inventory ←→ HI Hotel Contracts ←→ HI Contract Milestones
HI Discovery Findings → HI Hotel Contracts (Linked Contract)
HI Discovery Findings → HI Contract Milestones (Linked Milestone)
HI Hotel Contracts → HPC Hotel (optional)
HI Hotel Contracts → HI Organizations (Counterparty Org)
HI Contract Milestones → HPC Hotel (optional)
HI Contract Milestones → HI Hotel Discovery Inventory
```

## Approved schema — HI Hotel Contracts

Stable `ctr_…` agreement identity; global persistence without requiring HPC.

| Field | Type | Notes |
|---|---|---|
| Contract ID | singleLineText | Primary |
| Discovery Property | link → HI Hotel Discovery Inventory | Research identity |
| HPC Hotel | link → Hotel Property Census | Optional until matched/created |
| Agreement Type | singleSelect | FRANCHISE / MANAGEMENT / LEASE / LICENSE / OTHER |
| Agreement Role | singleSelect | ORIGINAL / AMENDMENT / RENEWAL_EXTENSION / TERMINATION_INSTRUMENT |
| Counterparty Org | link → HI Organizations | |
| Counterparty Name Raw | singleLineText | |
| Brand Family | singleSelect | Align discovery inventory |
| Brand Label | singleLineText | |
| Governing Law / Jurisdiction | singleLineText | Optional |
| Hotel Country | singleLineText | Physical hotel |
| Hotel State / Region | singleLineText | |
| Hotel City | singleLineText | |
| Geographic Processing Class | singleSelect | PRIORITY_HPC_ENRICHMENT / GLOBAL_RESEARCH_STANDARD / EXISTING_HPC_OPPORTUNISTIC |
| Contract Status | singleSelect | V4 status enum |
| Commencement Date | date | |
| Scheduled Expiration Date | date | Not loan maturity |
| Term Years | number | |
| Renewal Options Summary | longText | Separate from expiration |
| Termination Rights Summary | longText | |
| Supersedes Contract | link → HI Hotel Contracts | |
| Superseded By Contract | link → HI Hotel Contracts | |
| Primary Source URL | url | |
| Primary SEC Accession | singleLineText | |
| Evidence Confidence | singleSelect | HIGH / MEDIUM / LOW / UNVERIFIED |
| Publication State | singleSelect | default **CUSTOMER_BLOCKED** |
| Schema Version | singleLineText | `4.0.0` |
| Notes / Quarantine | longText | |

## Approved schema — HI Contract Milestones

Stable `ms_…` milestone identity; Publication State required (default CUSTOMER_BLOCKED).

| Field | Type | Notes |
|---|---|---|
| Milestone ID | singleLineText | Primary |
| Contract | link → HI Hotel Contracts | Required |
| Discovery Property | link → HI Hotel Discovery Inventory | |
| HPC Hotel | link → Hotel Property Census | Optional |
| Milestone Type | singleSelect | FRANCHISE_EXPIRATION / MANAGEMENT_EXPIRATION / COMMENCEMENT / RENEWAL / EXTENSION / AMENDMENT_EFFECTIVE / TERMINATION / REFLAG / OPTION_EXERCISE / OTHER |
| Milestone Date | date | |
| Date Meaning Precision | singleSelect | EXACT_DAY / MONTH_YEAR / YEAR_ONLY / NARRATIVE |
| Reporting Date | date | |
| Current Status Class | singleSelect | See enum |
| Status Corroborated | checkbox | Future dates: is current agreement status corroborated? |
| Commercial Relevance Score | number | Research-only 0–100 — not “seeking brand/manager” |
| Source URL | url | |
| SEC Accession | singleLineText | |
| Evidence Snippet | longText | |
| Related Finding | link → HI Discovery Findings | Bridge V2/V3 |
| Superseded By Milestone | link → HI Contract Milestones | |
| **Publication State** | singleSelect | default **CUSTOMER_BLOCKED** |
| Schema Version | singleLineText | |
| Notes | longText | |

### Current-status enum

`CURRENTLY_CORROBORATED` · `HISTORICALLY_VERIFIED` · `FUTURE_DATE_REQUIRES_REVALIDATION` · `EXPIRED_STATUS_UNCONFIRMED` · `SUPERSEDED` · `TERMINATED_VERIFIED` · `CONFLICTED`

Silence ≠ continuity. Loan maturity ≠ franchise/management expiration. Duplicate disclosures ≠ new agreements.

## Approved additive fields (existing tables)

| Table | Field | Type |
|---|---|---|
| HI Hotel Discovery Inventory | Geographic Processing Class | singleSelect |
| HI Hotel Discovery Inventory | Continent / Macro Region | singleSelect or text |
| HI Discovery Findings | Linked Contract | link → HI Hotel Contracts |
| HI Discovery Findings | Linked Milestone | link → HI Contract Milestones |

## Exact schema diff (approved; apply deferred)

```
BASE appCCUsuGsE1ifoLk
+ CREATE TABLE "HI Hotel Contracts"
+ CREATE TABLE "HI Contract Milestones"
+ ADD FIELD Inventory."Geographic Processing Class"
+ ADD FIELD Inventory."Continent / Macro Region"
+ ADD FIELD Findings."Linked Contract" → HI Hotel Contracts
+ ADD FIELD Findings."Linked Milestone" → HI Contract Milestones
= REUSE HPC / HI Organizations / Relationships / Graph Evidence
= NO second Airtable base
= NO bulk HPC create without founder-approved pilot manifest
```

**Meta API create is blocked until:** #495 merged + sync + V4 PR opened + explicit apply authorization in that PR cycle.

## Controlled HPC creation — `VERIFIED_DISCOVERY_TO_HPC_CREATION`

Absolute “never create HPC” is **revoked**. Replaced by controlled canonical creation for verified discoveries (priority Americas/Europe).

### Preconditions (all required)

1. Confirm physical property identity  
2. Reliable location (preferably full address)  
3. Operating / renamed / converted / closed status researched  
4. Search existing HPC + historical aliases (`hpc-live-identity-collision-v1`)  
5. Reject ambiguous, duplicate, generic candidates  
6. Validate required HPC fields via approved onboarding contract  
7. Preserve research evidence + provenance  
8. Create **only** via approved pathway (`hpc-authorized-create-reuse-v1` / HE HPC population workflow)  
9. Link originating Discovery Inventory → new HPC hotel  
10. Mandatory Airtable read-back  
11. Keep **unpublished**; queue for later Hotel Explorer enrichment  

Historical-only hotels without reliable current canonical identity → remain in Discovery Inventory.

**Do not** auto-generate hundreds of HPC records. First validate a **25–50 US hotel dry-run pilot** with founder-approved creation manifest.

Workflow detail: `docs/ai-build-system/HOTEL_CONTRACT_TIMING_V4_HPC_CREATION_WORKFLOW.md`

## Reporting splits

1. **GLOBAL RESEARCH COVERAGE**  
2. **PRIORITY HPC ENRICHMENT — AMERICAS + EUROPE** (incl. controlled creates)  
3. **REST OF WORLD**

## Integration gate

1. IC merges [PR #495](https://github.com/joandejarden-create/my-operators-backend/pull/495)  
2. Post-merge sync product homes → `origin/main`  
3. Open **new sequential V4 PR** with ensure-script (dry-run first) + workflow modules  
4. Schema apply only with explicit authorization in that cycle  
5. HPC pilot: dry-run manifest → founder approve → apply + read-back every write  
6. Customer publication remains blocked  

## Approval checklist

- [x] Geographic processing classes as operational config  
- [x] Create `HI Hotel Contracts` (design)  
- [x] Create `HI Contract Milestones` (design; Publication State required)  
- [x] Additive Inventory / Findings fields  
- [x] `VERIFIED_DISCOVERY_TO_HPC_CREATION` replaces absolute no-create  
- [x] 25–50 US HPC pilot dry-run manifest required before apply  
- [ ] PR #495 merged + main synced  
- [ ] Meta schema apply authorized in V4 PR  
- [ ] HPC pilot manifest founder-approved for apply  
