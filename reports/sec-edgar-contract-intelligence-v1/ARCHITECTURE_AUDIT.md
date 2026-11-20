# Phase 1 — Architecture & schema audit

**Date:** 2026-11-20  
**Workspace:** `C:/Dev/dealality/ws-hotel-explorer`  
**Contract:** `SEC_EDGAR_CONTRACT_INTELLIGENCE_V1`

## Canonical destinations (reuse — no new tables)

| Destination | Role |
|---|---|
| HI Graph Evidence (`tblc9JhKT1edr4grE`) | Contract milestones as Field Name + Value Observed + SEC URL + snippet |
| HI Hotel–Org Relationships | Operator / Brand / Management Company with Effective From/To |
| HI Organizations | Registrant; CIK in Registry / Identifier |
| HI Ownership Research Runs | Run / cost / checkpoint |
| HPC (`tbl9aY5ijiuIzzWam`) | Allowlisted null-fill only (Management Model, operator fields, affiliation dates) |

## Gaps requiring founder schema approval

1. No HPC columns for expiration / renewal / termination.
2. No SEC Source Type enum on Partner Source Library.
3. No dedicated CIK registry table.

## Existing infrastructure reused

- Fair-access HTTP client pattern (new, SEC-specific)
- Budget ledger pattern (`budget-guard-v1`)
- HI graph field maps (`airtable-graph-contract-v1.js`)
- HPC shared upsert null-fill discipline (promotion path reserved; pilot defaults off)
- GDI weekly-change pattern adapted as finding-level change detection (not a parallel Weekly Change product)

## Concurrent work preserved

Local HE 5.0 dirty worktree (`feat/he50-expanded-standing-auth-completion-v1`, 100+ dirty paths) was **not** discarded. SEC EDGAR work is additive under `lib/hotel-intelligence/sec-edgar/`, `scripts/sec-edgar-*`, `docs/ai-build-system/SEC_EDGAR_CONTRACT_INTELLIGENCE.md`, and this report folder.
