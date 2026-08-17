# Operator Explorer Phase 1 — Founder Review

**Mode:** apply (complete)  
**Branch:** `app-shell-left-nav`  
**Commit:** `3c88c0b4e22a35052e450d00c5e2f1b9e417c040`  
**Base:** live `AIRTABLE_BASE_ID`  
**Generated:** 2026-08-10  

## Decision summary

Phase 1 Airtable foundation + calibration seed **applied**. Operator Fit, scoring, owner pilot, and My Deals remain unchanged/disabled.

---

1. **Founder approvals applied** — `docs/architecture/decisions/operator-explorer-phase-1-founder-approvals.md`
2. **Webhound merge** — **Deferred supplemental enrichment** (session still running; not blocking)
3. **Backup** — verified under `backups/operator-explorer/phase-1/2026-08-10T15-21-07/` · `reports/operator-explorer-phase-1-backup-manifest.md`
4. **Record Purpose** — Production **24** · Research **13** · Test Fixture **9** (46 Masters total after creates)
5. **Test Fixture isolation** — 9 fixtures tagged; resolver in `lib/operator-explorer/phase-1-universe.js`
6. **Operating Model** — applied on calibration Masters + 10 new Masters
7. **Management Availability** — applied likewise (axis separate from OM)
8. **New Masters** — **10 created**, 0 held  
   Crosswalk: `data/operator-explorer/phase-1-provisional-crosswalk.json`
9. **Assignments table** — `Operator Intelligence - Assignments` `tblKh5p0K1tNAUnkj`
10. **Assignment schema** — calibration-minimal (35 fields); Mixed-Use/F&B not required
11. **Brand Relationships intel** — `Operator Intelligence - Brand Relationships` `tblt2pMLBEcTdgwdD`
12. **Claims changes** — `PI Source Library` link field added
13. **Market Presence changes** — `City / Metro`, `Verified Assignment Count`
14. **PI Sources** — 41 created · 4 reused (45 calibration URLs)
15. **Assignments seeded** — **75 created** · **9 held** (aggregates)
16. **Brand Relationships seeded** — **51** · BMC **24**
17. **Presence seeded** — **20 created** (table total ~62)
18. **Claims** — calibration Claim IDs already present (25 matched/skipped); PI link optional when `sourceIds` present
19. **Sources** — see #14
20. **Conflict holdouts** — 12 · `data/operator-explorer/phase-1-conflict-holdouts.json`
21. **Local vs Airtable parity** — `reports/operator-explorer-phase-1-local-vs-airtable-parity.md` (deltas = intentional aggregate holds)
22. **Derived-field shadow** — no Master summary overwrites · `reports/operator-explorer-phase-1-derived-field-shadow.md`
23. **Airtable payloads** — `data/operator-explorer/phase-1-airtable-profile-payloads/` (27)
24. **Strong Explorer Profiles** — 2 (Arbor, Hotel Equities)
25. **Explorer Publishable** — 5
26. **Thin / Not Publishable** — 14 / 8
27. **Fit Data Ready (diag)** — Ready 4 · Conditional 15 · Research Required 8 — **scoring untouched**
28. **Track 1 vs Track 2** — same core tables validated
29. **Entity resolution** — no MxM/HMS/NH/AccorHotels duplicate Masters
30. **Automation runbook** — `docs/process/operator-explorer-wave-runbook.md`
31. **Remaining data gaps** — named CALA managed hotels for enterprise brands; Webhound deferred; several Track 2 thin profiles
32. **Remaining schema issues** — Brand/Brand Parent text (optional Brand Basics links later); Claims Category still free text
33. **Founder decisions still needed** — Playa–Hyatt contracting clarity; when to graduate Research Masters to Production; approve Webhound supplemental merge when complete; Explorer internal preview vs wait for enrichment
34. **Recommended next phase** — Supplemental enrichment wave (Webhound merge + named assignments) → internal Explorer payload preview → **still stop before Fit/owner**

## Exact approvals already used

All 16 Phase 1 items in the approvals ADR were executed under write-plan controls.

## Confirmations

| Gate | Status |
| ---- | ------ |
| Operator Fit / v2.1 unchanged | Confirmed |
| Owner pilot disabled | Confirmed (do not enable) |
| My Deals unwired | Confirmed |
| Test Fixtures not deleted | Confirmed |
| Derived Master summaries not overwritten | Confirmed |
| Public Explorer UI not built | Confirmed |
