# ADP Strategic Attribute Selection — Airtable Persistence Proposal V1

**Status:** `OPTION_B_DESIGN_APPROVED_IMPLEMENTATION_READY` (PR-2)  
**Contract:** `ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1`  
**Gate:** `DEALALITY_AIRTABLE_PERSISTENCE_GATE_V1` · classification `PERSISTENCE_DESIGNED`  
**Phase:** PR-2 finalizes field-level contract — **no production schema mutation or writes until PR-3 founder auth**

## Principle

Airtable must become the durable home for **approved Layer B strategic selections**.  
Hotel ADP Attributes remain **Layer A verified facts**. Do not collapse these layers.

Fixtures + attribute-set artifacts remain migration / compatibility inputs.  
`FULL_E2E_VALIDATED` cannot be claimed while approved selections exist only in fixtures or local JSON.

## Reuse evaluation

| Option | Summary | Verdict |
|---|---|---|
| **A** Extend Hotel ADP Attributes with `RG Priority?` / selection status | Reuses one table | **Not recommended** — mixes facts with strategic selection; weak configuration versioning |
| **B** Thin Configuration header + Attribute Lines | Smallest clean separation | **Recommended** |
| **C** Decisions / Decision Events only | Reuses action infra | **Not recommended** for steward editing UX |

## Recommended schema (Option B)

**Base:** Intelligence `appa2cE7FTRmIbB32` (same as Hotel ADP Attributes / Decisions)

### Table 1 — `ADP Priority Attribute Configurations` (header)

| Field | Type | Notes |
|---|---|---|
| Configuration ID | text | Stable id `adp_selcfg_{propertyId}_{version}` |
| HPC Hotel ID | text | Canonical `rec…` |
| ADP Property ID | text | `adp_*` |
| Hotel Name | text | Display |
| Configuration Version | number/text | Monotonic per hotel |
| Approval Status | select | `DRAFT` / `SUGGESTED` / `APPROVED` / `SUPERSEDED` |
| Approver / Steward | text | |
| Effective Date | date | |
| Selection Rationale Summary | long text | Optional |
| Minimum Eligible Required | number | Default 12 (configurable) |
| Eligible Count (cached) | number | Diagnostic cache |
| Meets Minimum? | checkbox | Diagnostic |
| Inventory Hash | text | Hash of approved keys |
| Attribute Definitions Version | text | Registry pin |
| Supersedes Configuration ID | text | History |
| Active? | checkbox | One active approved config per hotel |
| Schema Version | text | `adp_priority_attr_cfg_v1` |

### Table 2 — `ADP Priority Attribute Lines` (selected keys)

| Field | Type | Notes |
|---|---|---|
| Line ID | text | Stable |
| Configuration ID | text / link | Parent config |
| HPC Hotel ID | text | Isolation |
| ADP Property ID | text | |
| Governed Attribute Key | text | Must match registry when measurable |
| Label | text | From definitions |
| Line Status | select | `SUGGESTED` / `SELECTED` / `DISABLED` / `REJECTED` |
| Measurement Enabled? | checkbox | Selection for RG inventory |
| Strategic Priority | select | high/medium/low — **management only, not RG weights** |
| Verification Status | select | Mirrored classification; not a duplicate fact body |
| Measurement Eligibility | select | `ELIGIBLE` / `NEEDS_VERIFICATION` / `UNDEFINED` / `CONFLICTING` / `N_A` |
| Source Fact Refs | long text / JSON | Pointers to Hotel ADP Attributes dedupe keys / HI evidence ids |
| Evidence URLs | long text | References only |
| Selection Rationale | long text | Why this attribute for this hotel |
| Relevant Demand Segments | multiple select / text | |
| Approved At | date/time | |
| Active? | checkbox | |
| Dedupe Key | text | `{hpc}::{configVersion}::{attributeKey}` |
| Schema Version | text | `adp_priority_attr_line_v1` |

### Explicit non-duplication

- Do **not** copy Attribute Value / rooms / meeting sqft into selection lines.
- Join to Hotel ADP Attributes / HI Evidence by reference.
- Research-period snapshots remain immutable FS (or gated AT indexes) with `configurationVersion` + `inventoryHash`.

## Editing & approval UX (Airtable-first)

Authorized users can:

1. Review suggested lines (`SUGGESTED`)
2. Add / remove / enable / disable measurement
3. Adjust strategic priority (non-metric)
4. Review verification / eligibility
5. Approve configuration (header → `APPROVED`, prior → `SUPERSEDED`)
6. See minimum-12 compliance fields
7. Retain history via supersession chain

## Migration

1. Seed lines from existing fixture `attributes[]` as **`SELECTED` + `REVIEW_REQUIRED`** (or `DRAFT`) unless explicit approval evidence already exists — **do not auto-APPROVE** fixture baselines into Airtable.
2. Attach eligibility classification from Phase 1B diagnostics.
3. Suggestions appear as `SUGGESTED` lines — never auto-selected.
4. Fixtures remain readable fallback until cutover flag.
5. Configuration header starts as `DRAFT` / `SUGGESTED` until steward approval.

## Persistence gate classification path

| Stage | Classification |
|---|---|
| PR-1 diagnostics (this PR) | `PERSISTENCE_DESIGNED` |
| Schema approved, dry-run seed | `PERSISTENCE_DESIGNED` → toward `PERSISTENCE_VALIDATED` |
| Idempotent write + retrieve + history + isolation proven | `PERSISTENCE_VALIDATED` |
| Customer-facing measurement driven by approved Airtable config | Requires separate cutover + cannot claim `FULL_E2E_VALIDATED` before AT SoT |

## Founder approval required before

- Creating tables / fields in Airtable
- Any production writes
- Declaring selection SoT cutover from fixtures
