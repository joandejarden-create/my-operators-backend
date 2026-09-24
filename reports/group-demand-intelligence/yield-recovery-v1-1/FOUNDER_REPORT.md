# GDI Yield Recovery V1.1 — Founder Report

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Generated:** 2026-09-24  
**Promote run:** `gdi_yield_recovery_v1_1_20260924`  
**Discovery provenance run (preserved):** `gdi_new_opps_v1_2_live_bethesda_20260923` @ `2026-09-23T23:36:33.455Z`  
**Weekly discovery run:** `gdir_luxvwwxid7u2_2026_09_24_weekly_bacf2e`  
**Newness policy (explicit):** `NEW_TO_GDI_ON_PROMOTION` — customer NEW = first canonical GDI write; `firstDiscovered*` preserves earlier V1.2 discovery (not “discovered today”).

---

## A. LOST TRUE REVALIDATION

| Candidate | Current TRUE? | Duplicate? | Action |
|-----------|---------------|------------|--------|
| NRC RIC (Regulatory Information Conference) | YES — 2026-09-30, housing URL 200, lodging+future | No | PROMOTE_NEW |
| ACCP Annual Meeting | YES — 2026-10-01, official source live | No | PROMOTE_NEW |
| ACVNU Renal Week | YES — 2027-04-12, official source live | No | PROMOTE_NEW |
| Bethesda Premier Cup | YES — same cycle/identity | Yes → `gdi_opp_bethesda_premier_cup_2026` | UPDATE_EXISTING |

---

## B. CONTROLLED PROMOTION

| Metric | Count |
|--------|-------|
| PROMOTED NEW | 3 |
| UPDATED EXISTING | 1 |
| HELD | 0 |
| REJECTED | 0 |

Canonical NEW after apply: 4 (Woman’s Club + NRC + ACCP + ACVNU).

---

## C. PROVENANCE

| Check | Result |
|-------|--------|
| FIRST DISCOVERY RUN PRESERVED | 3/3 (NRC, ACCP, ACVNU) + Premier Cup update |
| FIRST DISCOVERY DATE PRESERVED | 3/3 (`2026-09-23T23:36:33.455Z`) |
| FALSE TODAY-DISCOVERY | 0 |

`firstSeenRunId` = promotion run (`gdi_yield_recovery_v1_1_20260924`).  
`firstDiscoveredRunId` = V1.2 live dry-run discovery run.

---

## D. WEEKLY DISCOVERY ARCHITECTURE

| Layer | Owner |
|-------|--------|
| ORCHESTRATOR | Thin new: `lib/group-demand-intelligence/weekly-discovery-orchestrator.js` |
| COVERAGE ROLE | Ledger / audit / cadence only (`executeCoverageCycle` → SKIPPED / BASELINE_EXISTING; not RESEARCHED) |
| DISCOVERY ROLE | Due (or bounded force-due) targets → playbook route → real URL fetch |
| PROMOTION SERVICE | `promoteQualifiedGdiOpportunity()` in `promote-qualified-opportunity.js` |

Flow:

```
Research Target Registry → Due Targets → Weekly Discovery Orchestrator
  → Bounded Research Playbooks → Candidate Signals → Qualification
  → promoteQualifiedGdiOpportunity → Canonical GDI
  → Coverage ledger records what happened
```

---

## E. TARGET EXECUTION (live bounded cycle)

| Metric | Value |
|--------|-------|
| TARGETS DUE (natural) | 0 (prior ledger-as-research advanced cadence) |
| TARGETS RESEARCHED (force-due proof, limit 10) | 10 |
| QUERIES | 0 |
| FETCHES | 10 |
| COVERAGE | 100% of selected due set |

Note: first wiring proof used `--force-due --limit 10` because natural due was 0 after prior false RESEARCHED coverage. Subsequent cycles use registry due only.

---

## F. BY LANE

| Lane | Due | Researched | Candidates | Watch | True | Promoted | New |
|------|-----|------------|------------|-------|------|----------|-----|
| Demand Generators | 3 | 3 | 2 | 0 | 0 | 0 | 0 |
| New Opportunities / Programs | 4 | 4 | 3 | 0 | 0 | 0 | 0 |
| Private Events / Venue | 2 | 2 | 1 | 0 | 0 | 0 | 0 |
| Government / Projects | 1 | 1 | 1 | 0 | 0 | 0 | 0 |
| Sports / Training / Other | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

Monitor cycle: URL playbook checks only (no invent; no Surfe/PDL). Net-new TRUE promotion in this cycle = 0 (expected). Lost TRUEs recovered via controlled promote (Part C), not this monitor pass.

---

## G. BASELINE GUARD

| Metric | Value |
|--------|-------|
| BACKFILL FALSE NEW BEFORE | 46 |
| BACKFILL FALSE NEW AFTER | 0 (test: 50 targets → 0 weekly NEW_SIGNAL) |

First-pass / never-researched targets classify as `BASELINE_EXISTING` (summary) + Airtable-safe `NO_MATERIAL_CHANGE` / `SKIPPED`.

---

## H. WEEKLY RESULTS (orchestrator cycle)

| Metric | Count |
|--------|-------|
| NEW SIGNALS | 0 |
| UPDATED SIGNALS | 7 |
| TRUE | 0 |
| PROMOTED | 0 |
| NEW | 0 |
| UPDATED (opps) | 0 |

Customer summary (grounded on RESEARCHED only):

```
Last Research: Sep 24, 2026
10 monitored demand targets reviewed
0 new demand signals identified
0 new opportunities added
0 existing opportunities materially updated
```

---

## I. CUSTOMER VISIBILITY

| Surface | Count |
|---------|-------|
| CANONICAL ELIGIBLE | 42 |
| API / customer filter | 42 |
| UI (same bag) | 42 |
| Weekly NEW | 4 |
| MISMATCH | 0 |

Recovered NEW IDs:  
`gdi_opp_regulatory_information_conference_20260930`,  
`gdi_opp_accp_annual_meeting_20261001`,  
`gdi_opp_acvnu_renal_week_20270412`,  
plus existing `gdi_pe_781f12393f8117e7`.

---

## J. INTEGRITY

| Check | Count |
|-------|-------|
| DUPLICATES CREATED | 0 |
| PRODUCTION OPPS LOST | 0 |
| DECISIONS LOST | 0 |
| VALIDATIONS LOST | 0 |
| ACTIONS LOST | 0 |
| OUTCOMES LOST | 0 |
| SHARE TOKENS CHANGED | 0 |

---

## K. REGRESSION

| Suite | Result |
|-------|--------|
| NEW OPPS V1.2 | PASS |
| DG V1.1 | PASS |
| PE V1.6 | PASS |
| COVERAGE V1 | PASS |
| YIELD RECOVERY V1.1 | PASS |
| WEEKLY DELTA | PASS |
| CQ | PASS |
| SHARE | PASS |
| CSV | PASS |
| UI helpers (weekly-delta) | PASS |

---

## L. DECISION

1. Are NRC / ACCP / ACVNU still valid TRUE opportunities? **YES**
2. Were they safely promoted? **YES** (3 NEW + Premier Cup UPDATE)
3. Was Premier Cup correctly reconciled? **YES** → update existing `gdi_opp_bethesda_premier_cup_2026`
4. Is weekly discovery now executing real research rather than ledger-only coverage? **YES** (10 fetches / 10 RESEARCHED)
5. Is the Research Target Registry now driving bounded discovery? **YES** (orchestrator + playbook routing)
6. Are New Opportunities and Demand Generator lanes wired into the weekly cycle? **YES as routed playbooks + URL research**; full V1.2/DG harvest engines remain controlled scripts (not auto-invented in monitor pass)
7. Is first-run baseline inflation permanently fixed? **YES**
8. Can every new opportunity be traced to target → run → research → qualification → promotion? **Promoted set: discovery run + promote run + provenance fields; weekly monitor Target Runs linked to Research Run**
9. Does weekly GDI now produce a trustworthy customer summary? **YES when RESEARCHED > 0; null if ledger-only**
10. Is there still any systemic yield leakage? **Residual:** weekly monitor does not yet invoke full New Opps/DG harvest+qualify+promote in-cycle (by design for this bounded wiring). Lost-TRUE write gap is closed.

---

## M. FINAL VERDICT

**GDI YIELD RECOVERY PASSES — WEEKLY DISCOVERY + PROMOTION WIRED**

---

## PERSISTENCE / GENERALIZATION CHECK

**CODE FILES CHANGED:**
- `lib/group-demand-intelligence/promote-qualified-opportunity.js` (new)
- `lib/group-demand-intelligence/weekly-discovery-orchestrator.js` (new)
- `lib/group-demand-intelligence/research-coverage/constants.js`
- `lib/group-demand-intelligence/research-coverage/coverage-engine.js`
- `lib/group-demand-intelligence/research-coverage/entities.js`
- `lib/group-demand-intelligence/airtable-opportunity-store.js` (prior provenance payload-only)
- `scripts/gdi-yield-recovery-v1-1-promote-lost-trues.mjs` (new)
- `scripts/gdi-weekly-discovery-run.mjs` (new)
- `scripts/test-gdi-yield-recovery-v1-1.mjs` (new)
- `scripts/test-gdi-research-coverage-v1.mjs` (updated)
- `package.json` (scripts)

**FIXTURES ADDED:** none required (unit/fixture logic in tests)

**TESTS:**
- `npm run test:gdi-yield-recovery-v1-1`
- `npm run test:gdi-research-coverage-v1`
- plus CQ / New Opps / DG / PE / weekly / share / CSV

**HOTEL-SPECIFIC LOGIC:** NO (Bethesda used as controlled hotel id in scripts only)

**HARD-CODED TARGETS:** NO (lost-TRUE list is recovery script input from V1.2 audit; weekly uses registry)

**GIT SHA:** `b4352b8e05f739f8f93ab471d78c8213c3ba632e` (pre-commit working tree; recovery files uncommitted)

**WORKING TREE CLEAN:** NO
