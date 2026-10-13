# FOUNDER REPORT — Watch → Packet Completion Bottleneck

Generated: 2026-10-07T10:57:00Z  
Apply: YES (canonical FS + Airtable upsert via `saveOpportunitiesCanonical`)

## FINAL TOP ROOT CAUSE

**Stale cycles + wrong-destination contamination + missing lodging/buyer controllers** in the Watch bags.  
`surface_eligibility` is a **legitimate evidence blocker**, not a surface-logic or field-persistence bug.

## AC Hotel A Coruña

| Metric | Value |
|--------|------:|
| Starting Watch (frozen) | **28** |
| Target cohort (Watch + existing named ACTIVE) | 34 → cleaned |
| P0 completion candidates | 3 (IAPS 2027 primary; others mostly wrong-dest/stale) |
| Traveling entities proven | **0** (IAPS = STRONG_INFERENCE only) |
| Group motions resolved | 1 (IAPS association delegation) |
| Buyer roles resolved | 0 Ready-eligible (association public info only) |
| Relevant contact paths (Ready-eligible) | 0 on open Watch (IAPS = SOURCE_PAGE / GENERAL_ORG) |
| Direct lodging evidence | 0 |
| Strong hotel motion | 0 (IAPS = PLAUSIBLE only) |
| Future decision points | 1 material (IAPS abstract deadline 2026-12-31) |
| COMPLETE_STRONG (open/meaningful) | **1** (IAPS) |
| COMPLETE_PLAUSIBLE (open/meaningful) | 0–1 residual sports noise held non-Ready |
| Customer Ready before → after | **0 → 0** |
| Valid Future Watch after | **1** (IAPS Spaces in Transition 2027) |
| Stale / wrong-dest removed/downgraded | **22+** from original Watch |
| Top new Ready opportunity | *(none)* |
| Top remaining blocker | `buyer_contact_path_insufficient` + lodging page missing (IAPS); bag-wide still often `surface_eligibility` on closed rows |
| SURFACE_ELIGIBILITY BUG | **NO** |
| FIELD PERSISTENCE BUG | **NO** |
| JEV CALLS | **0** |

### AC forensic highlights
- **Eclipse:** past (2026-08-12); no named room-buyer entities retained.
- **ISC Softwood:** Dublin host hotels — wrong destination — CLOSED.
- **MoDELS:** Málaga/Torremolinos — CLOSED.
- **Misión China:** outbound Shanghai — CLOSED.
- **Super Copa Cadete:** Vigo + Hotel Coia — CLOSED.
- **IAPS 2027:** confirmed A Coruña 14–16 Jun 2027; abstracts due 31 Dec 2026; venue/housing TBA → Valid Future Watch, **not Ready**.

## Radisson Hotel Santo Domingo

| Metric | Value |
|--------|------:|
| Starting Watch (frozen) | **7** |
| Target cohort | 13 (Watch + existing named ACTIVE) |
| P0 completion candidates | 2–3 researched |
| Traveling entities proven | **2** (SDQ MICE hosted buyers; Family Business Summit attendees) — both non-Ready for Radisson |
| Group motions resolved | 2 |
| Buyer roles resolved | 2 functional paths |
| Relevant contact paths | 2 |
| Direct lodging evidence | 1 (SDQ MICE @ El Embajador — past / competitor) |
| Strong hotel motion | 1 (same — not Radisson Ready) |
| Future decision points | dated/closed |
| COMPLETE_STRONG (customer-open) | **0** |
| COMPLETE_PLAUSIBLE | 1 (FBS intelligence only) |
| Customer Ready before → after | **0 → 0** |
| Valid Future Watch after | **0** |
| Stale 2025 items found | **1+** (Sustainable Trends Summit 2025) |
| Stale Watch removed/downgraded | **7/7** original Watch closed or intelligence-only |
| Recurring series upgraded | SDQ MICE tagged `RECURRING_SERIES_UNCONFIRMED_NEXT` (no 2027 dates) — **not** promoted as Future Watch |
| Top new Ready | *(none)* |
| Top remaining blocker | No in-market future lodging controller open for Radisson |
| SURFACE_ELIGIBILITY BUG | **NO** |
| FIELD PERSISTENCE BUG | **NO** |
| JEV CALLS | **0** |

### Radisson forensic highlights
- **Sustainable Trends Summit 2025:** PAST_SIGNAL — CLOSED.
- **SDQ MICE 2026:** 21–23 Sep 2026 at El Embajador — PAST; hosted-buyer lodging proven; next cycle unconfirmed.
- **Family Business Summit:** 7–8 Oct 2026 at **Marriott Piantini** — host placed; MARKET_INTELLIGENCE_ONLY (no Radisson overflow evidence).
- **EUA Funding Forum:** Brno — wrong destination — CLOSED.

## Global checks

| Check | Result |
|-------|--------|
| NEW BROAD DISCOVERY RUN? | **NO** |
| APIFY USED? | **NO** |
| GDI THRESHOLDS CHANGED? | **NO** |
| READY STANDARD LOWERED? | **NO** |
| GENERIC HOMEPAGE ACCEPTED AS BUYER PATH? | **NO** |
| EVENT NAME PROMOTED AS ACCOUNT? | **NO** |
| VENUE SHELL PROMOTED? | **NO** |
| STALE PAST EVENT LEFT AS FUTURE OPPORTUNITY? | **NO** |
| SPECULATIVE HOTEL MOTION CREATED? | **NO** |
| AIRTABLE / FS / API / UI MATCH | **YES** (0 Ready / 0 facing; FS applied; Airtable upsert via canonical) |

## FINAL VERDICT

**Bottleneck diagnosed and cleaned.** Ready stays **0 / 0** under the unchanged gate. One honest AC Valid Future Watch remains (IAPS 2027). Radisson Watch universe had no open future lodging path after stale/wrong-dest cleanup.
