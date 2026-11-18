# Multi-hotel Airtable reconciliation V2 — founder summary

**STOP FOR FOUNDER REVIEW — NO PRODUCTION WRITES IN THIS PHASE**

**Package:** `reports/gdi-multi-hotel-airtable-reconciliation-v2/`  
**Generated:** 2026-11-18 (live Airtable read + dry-run planner)

## Verdict

Rock Creek's dual venue IDs are the **same physical venue**. Domain-canonical `pev_53b689545914754d` is authoritative; pilot structured `pev_42542bf58aac936b` is a geo-only rewrite from PR #367 while payload JSON stayed domain-based. Remediation is **planned, not applied**.

YOTEL / Radisson / Guadalajara gaps reflect **disjoint ID namespaces and lifecycle**, not missing bulk sync. Planner is deterministic, fail-closed: **0 CREATE**, **1 UPDATE**, **334 DEFERRED**, **2 CONFLICT**.

---

## Founder acceptance answers (§18)

1. **Why two Rock Creek venue IDs?** PR #367 updated structured Venue ID using `computeVenueId(name+city)` without domain; embedded Venue Payload JSON kept the prior domain-based ID.
2. **Which ID is canonical?** `pev_53b689545914754d` (name + `rockcreekmansion.com`).
3. **What references each ID?** Domain ID: 1 Hotel Venue Fit, 2 Private Event Signals. Pilot ID: 0 linked fits/signals; structured field on `recf35Q9B6jpX3Ivm` only.
4. **Safe to correct?** **Yes**, non-destructively: one structured-field UPDATE on the existing row — see `ROCK_CREEK_NON_DESTRUCTIVE_REMEDIATION_PLAN.md`.
5. **YOTEL 108 AT vs 42 FS?** **Zero ID overlap** — disjoint namespaces (`rx_d_` / `gdi_opp_yotel_fe2e_*` vs historical `gdi_opp_aifg_*` etc.). AT-only = valuable registry; FS-only = ongoing research — not one mirror.
6. **Radisson 66 FS vs 36 AT?** 30 shared (29 UNCHANGED), 36 FS-only DEFERRED (eligibility), 6 AT-only preserve, 1 org-name CONFLICT.
7. **Guadalajara 43 FS / 0 AT?** Never persisted to GDO; all DEFERRED below commercial eligibility; provisional need periods are not create authorization.
8. **How many qualify for sync now?** **0 CREATE**, **1 UPDATE** (`gdi_opp_accp_annual_meeting_20261001`). Conflicts excluded from auto-sync.
9. **How many research-only?** **334 DEFERRED** across eight hotels (includes all YOTEL/Guadalajara FS-only and AT-only historical preserve rows).
10. **Valuable AT-only history?** **Yes** — especially YOTEL 108, Nobu 65, Bethesda/Waterstone AT-only — preserve, no silent import.
11. **Global demand reuse safe?** **Yes** with isolation contract; distinct hotel GDO IDs for shared DG are valid; 2 org-name conflicts need founder policy (RC3).
12. **Weekly Change stable?** **Yes** — identical replans certified (`idempotencyRecheck.ok`); sync must not reset first-discovery on existing AT rows.
13. **Unresolved conflicts?** **2** — Radisson `rx_d_f5c3bd3d073a`, Bethesda `gdi_opp_series_ctn_annual_conference_2027`; Rock Creek venue ID mismatch (remediation pending RC1).
14. **Planner deterministic?** **Yes** — 20/20 tests pass; manifest `mutation: false`.
15. **Ready for another controlled pilot?** **Conditionally** — only minimal scope (Rock Creek venue align ± optional Bethesda UPDATE per `PRODUCTION_SYNC_ACTIVATION_PROPOSAL.md`).
16. **Exact records in proposed next apply?** PE Venue `recf35Q9B6jpX3Ivm` (structured ID → domain canonical); optional GDO `gdi_opp_accp_annual_meeting_20261001`.
17. **Approvals still needed?** `FOUNDER_DECISIONS_REQUIRED.md` RC1–RC8; explicit allowlist; `authorizeProductionWrites` + exact write manifest — not bulk multi-hotel sync.

---

## Rock Creek (quick reference)

| Item | Value |
|------|-------|
| Airtable record | `recf35Q9B6jpX3Ivm` |
| Structured Venue ID | `pev_42542bf58aac936b` |
| Payload venueId | `pev_53b689545914754d` |
| Same physical venue | yes |

## Sync readiness totals (8 hotels)

| CREATE | UPDATE | UNCHANGED | DEFERRED | CONFLICT | REJECTED |
|-------:|-------:|----------:|---------:|---------:|---------:|
| 0 | 1 | 217 | 334 | 2 | 0 |

## Safety

- No Airtable mutations in this phase
- No paid research or customer publication changes
- Weekly Change durability preserved in planner
- Hotel-private feedback not copied to global intelligence

**Next step:** Review `FOUNDER_DECISIONS_REQUIRED.md` and `PRODUCTION_SYNC_ACTIVATION_PROPOSAL.md`.