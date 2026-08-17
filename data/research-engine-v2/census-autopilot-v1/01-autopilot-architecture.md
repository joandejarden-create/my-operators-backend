# DEALALITY CENSUS AUTOPILOT V1 — Architecture

**Version:** census-autopilot-v1.0.0  
**Run:** `cav1_2026-08-08T10-12-55_daca27`  
**Dry-run / no writes / no Webhound / no credits**

## Purpose

One orchestration layer that can ultimately **discover → identity → research → validate → reconcile → escalate → stage → maintain** the Dealality Hotel Census — optimizing for **accuracy + provenance + completeness + freshness + low human effort**, not maximum filled cells.

## Reuse map (no duplicates)

| Capability | Existing module |
|---|---|
| Contradiction-first / confidence / geography | Research Engine V2 + census-autopilot-confidence / geography helpers |
| Source hierarchy / corroboration | clean-census provenance + family adapters |
| IHG / Hilton / Choice / Marriott adapters | `census-autopilot-*-discovery-adapter.js`, `family-directory-adapters.js`, clean-census Mexico discovery |
| Property identity + temporal affiliation | `clean-census/property-identity.js`, `temporal-affiliation.js` |
| VIC staging + wave engine + firewall | `independent-record.js`, `verified-record.js`, `wave-engine.js`, `research-firewall.js` |
| Field research plans | `clean-census/field-research.js#FIELD_RESEARCH_PLANS` |
| Field contract (complete) | `production-census-field-contract-v111.js#buildFieldContractEntries` |
| Steward / queues / checkpoints | `census-autopilot-queue-*`, `census-autopilot-checkpoint.js` |
| Production eligibility | `clean-census/production-eligibility.js` |
| PVQL / Tab Factory / Company Validated | Brand Explorer governance (consume staging only) |

## Pipeline

```
DISCOVER (Lane A directories)
  → DURABLE PROPERTY IDENTITY (Exact/High required for material updates)
  → RESEARCH EVERY RESEARCHABLE FIELD (field routing registry)
  → TEMPORAL AFFILIATION / STATUS
  → RECONCILE (VIC / BE / OE staging)
  → IMAGE INTEGRITY (rights/metadata only)
  → PROVENANCE + COMPLETENESS SCORES
  → OUTPUT CLASS
  → ESCALATE hard cases (Lane C queue — no auto Webhound)
  → STEWARD / GOVERNANCE
  → MAINTENANCE MODE (future)
```

## Hard constraints

- `legacy_used_as_source = false`
- `cvent_used_as_source = false`
- No automatic brand activation
- No automatic image download/rehost
- Unknown is acceptable
- Airtable writes disabled in V1

## CLI

```bash
npm run census:autopilot-v1 -- --mode=unified_benchmark --group=IHG,Hilton,Choice --country=Mexico --dry-run
```
