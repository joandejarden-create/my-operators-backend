# ADP Strategic Attribute Selection — Airtable Persistence Proposal V1

**Status:** `OPTION_B_DESIGN_APPROVED_IMPLEMENTATION_READY` (PR-2)  
**Contract:** `ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1` / schema `adp_priority_attr_cfg_v1_1`  
**Gate:** `DEALALITY_AIRTABLE_PERSISTENCE_GATE_V1` · classification `PERSISTENCE_DESIGNED`  
**Phase:** PR-2 finalizes field-level contract — **no production schema mutation or writes until PR-3 founder auth**

## Principle

Airtable must become the durable home for **approved Layer B strategic selections**.  
Hotel ADP Attributes remain **Layer A verified facts**. Do not collapse these layers.

Fixtures + attribute-set artifacts remain migration / compatibility inputs.  
`FULL_E2E_VALIDATED` cannot be claimed while approved selections exist only in fixtures or local JSON.

Coverage limits and semantic counting come from shared governed modules:

- `ADP_MEASUREMENT_COVERAGE_POLICY_V1` — min 12 / preferred 15–18 / max 20
- `ADP_SEMANTIC_EQUIVALENCE_RULES_V1_1` — unique eligible concept counting

Full candidate / research inventory is **not** capped at 20. Only the approved measurement inventory must satisfy the policy.

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
| Approval Status | select | `DRAFT` / `SUGGESTED` / `REVIEW_REQUIRED` / `APPROVED` / `SUPERSEDED` |
| Approver / Steward | text | |
| Effective Date | date | |
| Minimum Eligible Required | number | From shared policy (default 12) |
| Maximum Eligible Allowed | number | From shared policy (default 20) |
| Preferred Range Minimum | number | From shared policy (default 15) |
| Preferred Range Maximum | number | From shared policy (default 18) |
| Measurement Coverage Policy Version | text | Pin `ADP_MEASUREMENT_COVERAGE_POLICY_V1` |
| Semantic Equivalence Rules Version | text | Pin `ADP_SEMANTIC_EQUIVALENCE_RULES_V1_1` |
| Unique Eligible Concept Count | number | Diagnostic cache |
| Measurement Coverage Compliance Result | select | Below-minimum / Compliant / Above-maximum |
| Below-Minimum Flag | checkbox | Diagnostic |
| Above-Maximum Flag | checkbox | Diagnostic |
| Approved Measurement Inventory Hash | text | Hash of measurement-enabled unique concept keys |
| Full Candidate Inventory Count | number | Uncapped research/candidate inventory size |
| Attribute Definitions Version | text | Registry pin |
| Supersedes Configuration ID | text | History chain |
| Active? | checkbox | At most one Active APPROVED config per hotel |
| Idempotency Key | text | |
| Schema Version | text | `adp_priority_attr_cfg_v1_1` |
| Notes | long text | |

### Table 2 — `ADP Priority Attribute Lines` (selected + suggested keys)

| Field | Type | Notes |
|---|---|---|
| Line ID | text | Stable |
| Configuration ID | text / link | Parent config |
| HPC Hotel ID | text | Isolation |
| ADP Property ID | text | |
| Governed Attribute Key | text | Must match registry when measurable |
| Label | text | From definitions |
| Semantic Concept ID | text | |
| Line Status | select | `DRAFT` / `SUGGESTED` / `REVIEW_REQUIRED` / `SELECTED` / `DISABLED` / `REJECTED` |
| Measurement Enabled? | checkbox | Only enabled lines enter approved measurement inventory (≤ max unique concepts) |
| Strategic Priority | select | high/medium/low — **management only, not RG weights** |
| Verification Status | text | Mirrored classification; not a duplicate fact body |
| Measurement Eligibility | select | `ELIGIBLE` / `NEEDS_VERIFICATION` / `UNDEFINED` / `CONFLICTING` / `N_A` |
| Source Fact Refs | long text / JSON | Pointers to Hotel ADP Attributes / HI evidence — not duplicated fact bodies |
| Evidence URLs | long text | References only |
| Demand Segment Relevance | long text | |
| Selection Rationale | long text | |
| Recommended Action | text | |
| Approved At | date/time | |
| Active? | checkbox | |
| Dedupe Key | text | `{hpc}::{configVersion}::{attributeKey}` |
| Schema Version | text | `adp_priority_attr_line_v1_1` |

### Explicit non-duplication

- Do **not** copy Attribute Value / rooms / meeting sqft into selection lines.
- Join to Hotel ADP Attributes / HI Evidence by reference.
- Research-period snapshots remain immutable FS (or gated AT indexes) with `configurationVersion` + `Approved Measurement Inventory Hash`.

## Editing & approval lifecycle

Aligned states: **DRAFT → SUGGESTED → REVIEW_REQUIRED → APPROVED → SUPERSEDED**

Rules:

1. `DRAFT` / `SUGGESTED` / `REVIEW_REQUIRED` edits **never** mutate the Active `APPROVED` configuration.
2. Approve creates a **new** Configuration Version; prior Active APPROVED → `SUPERSEDED` (history retained and retrievable).
3. Suggestions appear as `SUGGESTED` lines until steward promotion.
4. Full candidate inventory may exceed Maximum Eligible Allowed; only `Measurement Enabled?` lines form the approved measurement inventory.
5. Below-Minimum / Above-Maximum compliance is diagnostic on the configuration header (no automatic production research blocking in PR-2).

## Migration

1. Seed lines from existing fixture `attributes[]` as **`REVIEW_REQUIRED`** — **never auto-APPROVE**.
2. Attach eligibility classification from Phase 1B / 1C diagnostics.
3. Suggestions appear as `SUGGESTED` lines — never auto-selected.
4. Fixtures remain readable fallback until cutover flag.
5. Configuration header starts as `REVIEW_REQUIRED` (or `DRAFT` / `SUGGESTED`) until steward approval.

## Persistence gate classification path

| Stage | Classification |
|---|---|
| PR-2 (this contract) | `PERSISTENCE_DESIGNED` |
| PR-3 (authorized schema + dry-run writes) | toward `PERSISTENCE_VALIDATED` |
| Approved AT selection + retrieval proof | required before `FULL_E2E_VALIDATED` |

PR-3 requires separate founder authorization of the final schema and write plan.
