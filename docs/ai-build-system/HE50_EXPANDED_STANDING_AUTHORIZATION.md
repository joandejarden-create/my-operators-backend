---
description: HE 5.0 expanded standing authorization for eight-hotel pilot Airtable persistence
alwaysApply: false
globs:
  - lib/hotel-intelligence/he-5-0-full-e2e/**
  - lib/demand-anchors/**
  - lib/travel-infrastructure/**
  - scripts/he50-*
  - scripts/import-demand-anchors-*
  - scripts/import-travel-infrastructure-*
---

# HE 5.0 expanded standing authorization (permanent)

Founder-approved 2026-11-20 (`he-5-0-standing-authorization-v2-expanded`).

## Scope

Eight-hotel pilot + reusable geographic/org intelligence directly relevant to those hotels.
Canonical base `appCCUsuGsE1ifoLk`. Never Hotel Census (`tblgj2qEwxjTcg6q0`).

## Allowlisted destinations

| Table | ID |
|---|---|
| Hotel Property Census | `tbl9aY5ijiuIzzWam` |
| Demand Anchors | `tblJRhi2rmeunzJzQ` |
| Travel Infrastructure Data | `tblux5Xs9o50MgpWF` |
| HI Organizations | `tbliXEfUOWk32AN9y` |
| HI Hotel–Org Relationships | `tblGiwN0foXXDESvZ` |
| HI Org Contacts | `tbldJAj0nK1l39JZL` |
| HI Graph Evidence | `tblc9JhKT1edr4grE` |

Machine source: `lib/hotel-intelligence/he-5-0-full-e2e/standing-authorization-v1.js`.

## Required mutation lifecycle

`RESEARCH_VERIFIED → MUTATION_ELIGIBLE → AUTO_APPROVED_IN_SCOPE → AIRTABLE_APPLIED → AIRTABLE_READBACK_VALIDATED → API_VALIDATED → LIVE_UI_VALIDATED`

## Hard bans

- No schema CREATE / destructive ops without separate auth
- No Surfe auto-enrichment or Surfe channel persistence
- No unverified ownership promotion (`PROPOSED_RELATIONSHIP` stays proposed)
- No unlicensed Guest Sentiment publication
- No out-of-cohort bulk writes; no budget reset ($50 cumulative paid ceiling)
- Demand Anchors / TI are reusable geographic entities — dedupe by identity/coords

## Quarantine, do not stop the run

Ownership conflicts, rights-restricted sentiment, Surfe-only channels, and out-of-scope tables must quarantine that mutation and continue eligible work.
