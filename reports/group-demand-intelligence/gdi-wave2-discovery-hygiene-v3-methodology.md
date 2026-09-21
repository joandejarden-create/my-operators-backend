# GDI Wave 2 Discovery Hygiene V3 — Methodology

## Scope

Discovery / qualification only. Does **not** modify V11 person gates, V12 WHO recall, Surfe/PDL, live promote, or Wave 2 discovery artifacts.

## Inputs

- Exact frozen Wave 2 qualified candidates (`wave2-qualified.json` × 3 hotels = 30)
- Evidence overlays = verified venue/date/housing facts (not encoded manual answers)
- Evaluation clock `asOfDate=2026-02-01` (Wave 2 near-term planning window)

## Pipeline (`discovery-hygiene-v3.js`)

1. **Event semantic type** — EVENT / EVENT_CYCLE may qualify; PLAN/PROGRAM/REPORT/INITIATIVE → INVALID  
2. **Real-event gate** — identity + date/cycle + location + organizer/source  
3. **Past-event gate** — past dates INVALID; recurrence → VALID_WATCH only  
4. **Date confidence** — TRUE requires DATE_CONFIRMED (or high-support inferred)  
5. **Geography** — hotel geo contracts (generic; no city hardcodes in rules)  
6. **Source authority** — aggregator-only rejected  
7. **Hotel demand thesis** — STRONG required for TRUE; weak sales phrases do not create demand; thesis copy cannot invent room-block language  
8. **Open sourcing** — UNKNOWN stays UNKNOWN (never → OPEN)  
9. **Host / venue lock** — competitor host without overflow → WATCH; fully placed → INVALID  
10. **Overflow thesis** — evidence-backed only; engine OVERFLOW_HOUSING / VERIFIED_HOUSING labels alone insufficient  
11. **Actionability contract** — TRUE_ACTIONABLE requires real event + current cycle + geo + source + STRONG demand + placement path + not duplicate/fully placed  

## Placement paths for TRUE

- Supported open sourcing (HOTEL_VENUE_TBD / RFP with evidence), **or**
- Meeting venue with rooms open, **or**
- Overflow evidence (overlay / housing evidence text — not sales thesis)

## Outputs

- Freeze: `data/group-demand-intelligence/evals/gdi-wave2-discovery-hygiene-v3.json`
- WHO (only after discovery pass): reuse staged Wave 2 WHO on V3 TRUE only — no provider calls
