# GDI Full E2E Foundation Audit V2 — Founder Executive Summary

**Mode:** Read-only · **Checkout:** `C:\Dev\dealality\my-operators-backend` @ `a4224cc99` (`main`) · **Date:** 2026-11-17  
**Overall classification:** **NEEDS IMPROVEMENT**

GDI already has a usable customer publication and parity path (YOTEL 42 / Bethesda 67 / Radisson 66 CUSTOMER_PUBLISHED packages), fourteen-segment planning, Exa-led paid campaigns, Apify adapters (default mock/fail-closed), execution observability ledgers, and Weekly Change UI. It does **not** yet operate as one consistently instrumented, multi-provider Full E2E that always researches every applicable segment with comparable commercial depth.

## Most important confirmed findings

1. **Multiple orchestrators / campaign scripts coexist** with a shared fourteen-segment discovery executor registry classifying every segment as `PARTIAL_EXECUTOR` — not a single production-proven “one command → all segments → all providers → certified ledger” path.  
2. **Historical execution ledgers (PR #336)** for YOTEL/Bethesda/Radisson correctly keep segment status **UNKNOWN** where live per-segment calls were not ledgered; YOTEL historical cert is `FULL_E2E_PARTIAL` with `providersExecuted: ["exa_agent"]` only.  
3. **Apify is CONFIGURED but not orchestrated by canonical Full E2E** — the fulle2e bridge is not imported by the fourteen-segment/hotel-template path; hist ledgers show CONFIGURED→SKIPPED only (inventory “WIRED” overclaims).  
4. **YOTEL published inventory is commercially Account Development–heavy:** 42 opportunities, all `commercialStageKey=QUALIFICATION_NEEDED`, `commercialActionability` 34 ACCOUNT_DEVELOPMENT + 8 CMI, **zero Sales Ready** — valuable early-stage inventory, not “ready to book tonight.”  
5. **Weekly Change UI taxonomy ≠ weekly-refresh backend contract** (UI: NEW/UPDATED/REACTIVATED/HIGH_PRIORITY; backend: NEW_PROSPECT/MATERIAL_UPDATE/…). YOTEL package has empty `weeklyDeltaState`.  
6. **Customer-visible SoT for published hotels is bundled CURRENT packages**, not “whatever Airtable view shows.” Empty Airtable views can be architectural, not data loss — but write/read-back gaps remain a material investigation theme.

## Highest commercial-value opportunities

1. **Forward-looking discovery beyond event listings** (corporate expansion, projects, relocation, procurement) with segment-true Exa missions — not title-only differentiation.  
2. **Canonical drawer commercial usefulness** (hotel-specific why/fit, verified contact role, next action, unknowns) with measurable QA rubric.  
3. **Contact buyer vs entry-point discipline** without fake completeness.

## Main reliability concerns

- Silent incomplete segment/provider execution if orchestration skips Apify/Parallel/Firecrawl without ledgered SKIP.  
- Campaign-specific scripts that bypass shared gates.  
- Local vs CURRENT drift (mitigated for YOTEL by parity gate; still possible elsewhere).  
- Dedup / Weekly Change readiness for automatic weekly runs (not yet safe to enable).

## Critical unknowns

- Live Apify Actor completion → ingestion → qualified opportunity conversion rates (no paid run in this audit).  
- Full Airtable write path health across hotels (read-only; no live Airtable mutation).  
- Whether Exa segment H1 titles change underlying research missions enough to change outcomes (**LIKELY titles-first; mission depth UNVERIFIED without side-by-side run artifacts**).

## Top three recommendations

1. **Instrument and enforce one canonical Full E2E path** so every planned segment/provider call is ledgered EXECUTED/SKIPPED/FAILED (extend PR #336 wiring).  
2. **Discovery + drawer quality program** for forward-looking demand and sales-usable narratives.  
3. **Unify Weekly Change + stable identity** before any automatic weekly scheduler.

## Recommended implementation sequence

P0 integrity (if any confirmed empty-write / confidentiality) → P1 canonical Full E2E + Apify skip honesty → P2 discovery/drawer quality → P3 identity/Weekly Change → P4 multi-hotel reuse → P5 scheduled weekly.

See `PRIORITIZED_ENHANCEMENT_ROADMAP.md` and `DEFECT_AND_RISK_REGISTER.md`.
