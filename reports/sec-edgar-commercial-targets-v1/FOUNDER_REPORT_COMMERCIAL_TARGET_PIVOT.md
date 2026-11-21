# Founder Report — Commercial Target Discovery Pivot

**Date:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**HPC creates:** 0  
**Spend:** $0.00 / $25 ceiling (cache-first + prior finding re-audit; no new paid search required this wave)

---

## 1. PR and integration status

| Item | Status |
|------|--------|
| PR #499 (V4 date integrity) | **MERGED** — `b0711ba4d2b695b79d22d8705619d76d515c341a` |
| Date-semantic gate on main | Present (`contract-date-semantic-gate-v1`, `contract-date-meaning-audit-v1`) |
| Invalid CMBS franchise-expiration recreation | **Avoided** — 264 opening/year-built false expirations quarantined |
| V5 / commercial pivot PR | **#503** — https://github.com/joandejarden-create/my-operators-backend/pull/503 |
| HE dirty worktree | Preserved (no clean sync claimed) |

---

## 2–8. Wave metrics (commercial scale pilot)

| Metric | Count |
|--------|------:|
| Source filings/exhibits examined | 142 |
| Distinct hotel observations (deduped) | 446 |
| Verified hotel-specific contract expirations (Tier B) | **76** |
| Tier A verified decision windows (expiration + notice) | **0** |
| Notice periods discovered | **0** |
| Valid future contractual decision windows (≤36 months, active) | **5** |
| Tier C conversion research candidates | **71** |
| HOLD / quarantine | **299** |
| Rejected false expirations (opening/year-built) | **264** |
| Unresolved date meaning holds | 35 |
| Brand Conversion prospect list rows | **136** |
| Management Contract prospect list rows | **0** (gap — see below) |
| Issuers beyond original 32-registry | **10** |
| Duplicates rejected | 58 |
| Confirmed renewals/terminations closing opportunities | 0 |

### Tier counts

| Tier | Count |
|------|------:|
| TIER_A_VERIFIED_DECISION_WINDOW | 0 |
| TIER_B_VERIFIED_CONTRACT_EXPIRATION | 76 |
| TIER_C_CONVERSION_RESEARCH_CANDIDATE | 71 |
| HOLD_QUARANTINE | 299 |

Of 76 Tier B: **5** within next 36 months (active), **11** beyond 36 months (retained in research DB), **60** past scheduled end with unknown renewal (follow-up — not removed, not active).

---

## 9. Owner / operator coverage

| Field | Coverage |
|-------|----------|
| Brand | 135 / 446 |
| Owner | 0 (not disclosed in CMBS franchise-expiration rows used) |
| Management company | 0 |

No paid personal-contact enrichment. Next wave should join HE Organizations / ownership graphs for the 5 active + 11 far-horizon Tier B hotels.

---

## 10. Sample high-quality commercial targets (Tier B, active ≤36 months)

| Hotel | Location | Brand | End date | Months | Notice |
|-------|----------|-------|----------|--------|--------|
| Holiday Inn Express - Oshkosh | Oshkosh | Holiday Inn | 2027-12-28 | 13 | NOTICE_NOT_DISCLOSED |
| Homewood Suites Memphis Southwind | — | Homewood | 2029-03-31 | 28 | NOTICE_NOT_DISCLOSED |
| Hampton Inn Memphis Southwind | — | Hampton | 2029-03-31 | 28 | NOTICE_NOT_DISCLOSED |
| Fairfield Inn – Oak Creek | Oak Creek | Fairfield | 2029-06-22 | 31 | NOTICE_NOT_DISCLOSED |
| Hilton Cincinnati Netherland Plaza | — | Hilton | 2029-11-30 | 36 | NOTICE_NOT_DISCLOSED |

These are **valuable without notice terms** (Tier B by design).

---

## 11. Highest-yield source families

1. **CMBS 424B HTML tables with explicit “Franchise Expiration Date” columns** (re-audited prior V2/V3 findings) — only path producing Tier B this wave  
2. **Identity inventory** — Tier C hotel identities without dates  
3. **Opening-year CMBS columns** — high volume but correctly **quarantined** (not targets)

EX-10 narrative harvest remains important for **management contracts + notice provisions** (Tier A) — not yet yielding management list rows in this wave.

---

## 12. Research spend / marginal cost

| | |
|--|--|
| Spent | **$0.00** |
| Ceiling | $25 |
| Cost per verified Tier B target | **$0** (reuse of cached EDGAR + prior findings through new commercial classifier) |

Budget remains available for a follow-up wave focused on notice language + management agreements.

---

## 13. Rejected false expirations

**264** hotels where prior “franchise expiration” labels audited to `HOTEL_OPENING_OR_YEAR_BUILT` (examples: Bellagio Hotel and Casino, Residence Inn Hartford Avon, Hampton Inn & Suites Oklahoma City, …).  
These are preserved as hotel-property facts where applicable — **not** conversion opportunities.

---

## 14. Coverage gaps

- Management-contract prospect list empty — need EX-10 HMA / REIT management schedules with hotel attribution  
- Notice periods / Tier A still zero — need agreement prose or schedule footnotes  
- Owner/asset-manager enrichment not yet joined  
- Later-filing renewal verification not executed for the 60 past-expiration Tier B rows  
- Live mega-424B annex extraction still brittle; prior high-yield URLs remain the reliable path until annex windowing is hardened  
- Americas-heavy CMBS; Europe underrepresented this wave  

---

## 15. Recommended scale-up plan

1. **Wave 2 (≤$25):** Exa/SEC-API queries specifically for `"Franchise Expiration Date"` + `"Management Agreement" expires` hotel EX-10; retrieve only hits with those headers.  
2. Re-process the **60 past-expiration Tier B** hotels for renewal/replacement evidence → CLOSE or keep as follow-up.  
3. Join HE ownership / Organizations for the **16** future-dated Tier B hotels (5 active + 11 retained).  
4. Build management-contract list from EX-10 HMA harvest with named-hotel scope.  
5. Persist research-ready Tier B/C rows to Research Mode (CUSTOMER_BLOCKED) after IC review of #503 — no Meta apply unless schema gaps appear.  

---

## Architecture delivered

- `commercial-target-discovery-v1` — Tier A/B/C/HOLD + dual prospect projections (Brand Conversion / Management Contract) over shared research IDs  
- `commercial-expiration-notice-extract-v1` — expiration/notice-first extraction; date-semantic audit mandatory  
- Pilot: `scripts/sec-edgar-commercial-target-scale-pilot.mjs`  
- Orchestrator: `scripts/Invoke-HotelIntelligenceCommercialTargetPilot.ps1`  
- Outputs under `reports/sec-edgar-commercial-targets-v1/`

**Success criterion met for this wave:** credible conversion targets with verified property identities and reliable contractual timing, without inflating counts via opening years; dual-list architecture in place; management list awaiting management-source yield.
