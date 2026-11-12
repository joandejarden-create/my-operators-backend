# GDI — Confirmed Competitor Bookings vs Future-Cycle Opportunities

**Contract:** Competitive Market Intelligence presentation clarity (no new demand segment)  
**Hotel validated:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Date:** 2026-11-12

## Verdict

Sales teams can now see **what is already booked at a competitor** separately from **what may become pursuable in a later cycle**. Booked editions no longer present as open host RFPs or “Pursue Now.”

## Market Scope (unchanged)

| Scope | Count |
|-------|------:|
| All | **67** |
| Hotel | **61** |
| Competitive | **10** |

- Confirmed competitor-booked editions stay **Competitive-only** unless independent target-hotel demand exists.
- `FUTURE_CYCLE_TO_MONITOR` (e.g. AFCEA) may appear in **Hotel + Competitive** with **Monitor Future Cycle** posture — not immediate bookability.
- All never double-counts dual-membership rows.
- Customer publication remains **19** without founder approval.

## Focus opportunities (Bethesda)

### NADO & DDAA WashCon 2028 — Crystal Gateway Marriott
- **Confirmed Competitor Business:** edition lodging confirmed at Crystal Gateway Marriott — not available to Bethesda Marriott.
- **Potential Future Opportunity:** qualify whether any overflow/secondary lodging is needed; do not pursue as open host RFP.
- **Action:** **Qualify Overflow**
- **Market Scope:** Competitive only

### NADO & DDAA WashCon 2027 — Crystal Gateway Marriott
- Same commercial pattern as 2028 (confirmed competitor host; overflow unconfirmed).
- **Action:** **Qualify Overflow**
- **Market Scope:** Competitive only

### AFCEA Bethesda Health IT Summit 2027
- **Confirmed:** prior (2026) lodging at Bethesda North Marriott — historical, not available 2027 host claim.
- **Future:** monitor subsequent editions; housing selection not confirmed; no invented next-edition dates.
- **Action:** **Monitor Future Cycle**
- **Market Scope:** Hotel + Competitive (monitor / account-development posture)

## Commercial action labels

| Label | When |
|-------|------|
| Monitor Future Cycle | Next edition may present an opportunity; current edition closed/historical |
| Develop Organizer Relationship | Needs verification / account development without open sourcing |
| Qualify Overflow | Competitor holds edition; residual lodging only if plausible — inquiry ≠ confirmed overflow |
| Pursue Open Business | Target-hotel open sourcing independently evidenced |

Blocked on contracted competitor editions without open evidence: **Request RFP** / **Pursue Now**.

## Evidence integrity preserved

Independent fields retained:
- Competitor booking certainty
- Target-hotel sourcing status
- Future-cycle opportunity status
- Commercial actionability

No manufactured future dates, open RFPs, overflow availability, or buyer contacts. PR #222 ledger standards and recovered Competitive inventory (10) retained. Authorization / publication protections unchanged.

## Surfaces updated

- List cards — competitive status pill + commercial action pill
- Detail drawer — Confirmed Competitor Business + Potential Future Opportunity + action banner
- CSV — columns: Competitive Commercial Action, Confirmed Competitor Business, Potential Future Opportunity

## Validation

```bash
npm run test:gdi-competitive-future-cycle-presentation-v1
npm run test:gdi-market-scope-toggle-v1
npm run test:gdi-commercial-intelligence-consistency-audit-v1
npm run test:gdi-bethesda-67-sales-baseline-cmi-v1
npm run test:gdi-customer-csv-export
```

All passed. Logic applies to all GDI hotels via `applyCompetitiveFutureCyclePresentation` in inventory annotation / commercial sanitizer. Detail API now re-attaches presentation fields so drawers match list cards and CSV.

## Browser verification (Bethesda, local :8080)

- Market Scope: All **67** / Hotel **61** / Competitive **10**
- Competitive cards: AFCEA shows **Monitor Future Cycle**; NADO 2028 shows **Qualify Overflow**
- NADO 2028 drawer: **Confirmed Competitor Business** (“not currently available to Bethesda Marriott”) + **Potential Future Opportunity** (qualify overflow; no open host RFP) + Recommended next step banner **Qualify Overflow**
- Screenshot: `reports/gdi-competitive-future-cycle-presentation-v1/screenshots/01-nado-2028-drawer.png`

## Founder requirement

GDI helps sales win **future** business from competitors **without** misleading them into pursuing business already contracted elsewhere.
