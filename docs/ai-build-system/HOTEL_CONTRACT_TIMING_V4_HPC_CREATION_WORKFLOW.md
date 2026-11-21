# V4 — VERIFIED_DISCOVERY_TO_HPC_CREATION Workflow

**Status:** FOUNDER APPROVED (design) — **no production creates until pilot manifest approved**  
**Base:** `appCCUsuGsE1ifoLk` · HPC `tbl9aY5ijiuIzzWam`  
**Priority:** Americas + Europe systematic; opportunistic elsewhere when already in HPC  
**Publication:** New hotels remain unpublished / CUSTOMER_BLOCKED; queue for Hotel Explorer enrichment  

## Reuse (do not reinvent)

| Capability | Module |
|---|---|
| Minimum identity + onboarding | `lib/hotel-census/hpc-full-onboarding-contract-v1.js` |
| Shared upsert / null-fill | `lib/hotel-census/hpc-shared-upsert-v1.js` |
| Live identity collision | `lib/hotel-census/hpc-live-identity-collision-v1.js` |
| Authorized create/reuse state machine | `lib/hotel-census/hpc-authorized-create-reuse-v1.js` |
| HE population workflow | `docs/ai-build-system/HE_HPC_POPULATION_WORKFLOW_V1.md` |
| Controlled apply (HPC_CREATE) | `lib/hotel-intelligence/he-5-0-full-e2e/controlled-apply-executor-v1.js` |

Legacy Hotel Census is **not** a create target.

## State machine

```
DISCOVERY_PERSISTED
  → IDENTITY_CONFIRMED (name + location + operating status)
  → HPC_COLLISION_SEARCH (name/alias/city/country/brand)
  → MATCHED_EXISTING | AMBIGUOUS_REJECT | CREATE_CANDIDATE
  → DRY_RUN_MANIFEST (founder review)
  → CREATE_AUTHORIZED (exact payload)
  → PRE_CREATE_COLLISION_RECHECK
  → HPC_CREATE (approved pathway only)
  → READBACK_VALIDATE
  → LINK_DISCOVERY_INVENTORY + CONTRACTS/MILESTONES
  → QUEUE_HE_ENRICHMENT (unpublished)
```

## Gates before CREATE (all must pass)

1. Physical property identity confirmed (not brand portfolio / generic label)  
2. Reliable location; preferably full street address  
3. Operating / renamed / converted / closed status researched  
4. HPC + alias collision search completed  
5. Ambiguous / duplicate / generic rejected  
6. Required HPC fields validated (`HPC_MINIMUM_IDENTITY_FIELDS` + contract rules)  
7. Evidence + provenance retained on Discovery Inventory / Findings / Contracts  
8. Exact field payload in founder-approved creation manifest  
9. Discovery Property linked after create  
10. Read-back of every write  
11. Publication blocked; HE enrichment queued  

Historical-only hotels without reliable **current** canonical identity → stay in Discovery Inventory.

## Initial pilot (25–50 high-confidence US hotels)

**Source pool:** SEC/CMBS contract-timing discoveries (baseline + V2 + V3), US only for pilot.

| Step | Action | Apply? |
|---|---|---|
| 1 | Select 25–50 high-confidence US discoveries with franchise/management milestones | No |
| 2 | Collision search vs live HPC | No |
| 3 | Classify: ALREADY_IN_HPC / CREATE_CANDIDATE / AMBIGUOUS / HISTORICAL_ONLY | No |
| 4 | Dry-run creation manifest (names, geography, evidence, dupe screen, field map) | No |
| 5 | Founder approve eligible CREATE rows | Gate |
| 6 | Apply creates via authorized pathway | Yes (post-approval) |
| 7 | Link discovery + contracts/milestones; read-back every write | Yes |

**Hard stop:** Do not apply pilot creates until the dry-run manifest is explicitly approved.  
**Hard stop:** Do not open bulk CREATE for hundreds of hotels from this pilot.

## Creation manifest schema (founder-reviewable)

Each candidate row must include:

- `discoveryPropertyId` / Airtable Discovery Inventory record id  
- Hotel name (normalized + raw) + aliases  
- City / State / Country / Address (if known)  
- Brand family / label  
- Operating status assessment  
- Collision outcome + candidate HPC ids reviewed  
- Disposition: `LINK_EXISTING` | `CREATE` | `REJECT_AMBIGUOUS` | `KEEP_INVENTORY_ONLY`  
- Intended HPC field map (exact keys/values)  
- Source URLs / SEC accessions  
- Linked contract / milestone ids (or pending)  
- `publicationState`: `CUSTOMER_BLOCKED`  
- `heEnrichmentQueue`: true  

Template: `reports/sec-edgar-contract-timing-v4/HPC_CREATION_PILOT_MANIFEST_TEMPLATE.json`

## Success criteria (pilot)

- Global research retained regardless of country  
- Agreements/milestones in linked canonical tables (after schema apply)  
- Existing HPC matches linked without duplicate creates  
- Missing verified US hotels created **only** from approved manifest rows  
- Discovery ↔ HPC ↔ contracts linked with read-back  
- New hotels unpublished and queued for HE  
- Customer publication still blocked  
