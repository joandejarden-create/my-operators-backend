# CALA_25_BEFORE_AFTER_COMPLETENESS

> Packet 2.8A · Completeness snapshots for the 25-hotel factory pilot  
> **BEFORE_RESEARCH only** · AFTER / native results **DEFERRED** pending founder approval

## Policy

| Pass | Status |
|------|--------|
| Selection | **DONE** (25/25 CONFIRMED_ID) |
| BEFORE_RESEARCH completeness | **DECLARED STATE ONLY** — no live assembler batch executed in this packet write |
| Native research AFTER | **DEFERRED** |
| Webhound AFTER | **DEFERRED** (escalation queue empty) |

Domains (engine): IDENTITY · PROPERTY_FUNDAMENTALS · OWNERSHIP · OPERATOR · BRAND · ORGANIZATION · PORTFOLIO · PEOPLE · RELATIONSHIPS · DEVELOPMENT · TRANSACTIONS · MARKET · AREA_HOTELS · DEMAND · ACCESS · SOURCES · RESEARCH

Tiers: **A Foundational** · **B Intelligence** · **C Deep** (`completeness-evaluator.js`)

---

## BEFORE_RESEARCH (expected posture — no execution)

Until the generic assembly + reuse pass runs against live Census / Brand Explorer / relationship graphs:

| Expectation | Notes |
|-------------|--------|
| IDENTITY / BRAND / MARKET | Often PARTIAL→STRONG from Census for branded hotels |
| PROPERTY_FUNDAMENTALS | Rooms known for some TA phase2 rows; MISSING for phase3/missing-rooms samples |
| OWNERSHIP / PROPCO / PEOPLE | Typically MISSING or RESEARCH_REQUIRED for pilot hotels (not golden corpus) |
| OPERATOR | Sometimes PARTIAL when managementCompany / Aimbridge-class data present in radar fixtures |
| AREA_HOTELS / DEMAND / ACCESS | Reusable market modules mostly MISSING until Wave 0/1 assembly |
| RESEARCH | MISSING — no pilot dossiers |

**Aggregate BEFORE metrics:** N/A (batch not run). Do not invent per-hotel domain matrices.

---

## AFTER_RESEARCH / native

| Metric | Value |
|--------|-------|
| Hotels completed native pass | **0 / 25** |
| Foundational Complete % | **DEFERRED** |
| Intelligence Complete % | **DEFERRED** |
| Domain lifts | **DEFERRED** |

---

## Founder gate

Approve pilot selection → authorize durable batch (`hotel-intelligence-batches/batch_cala_25_v1`) → run assembly + completeness evaluator → rewrite this file with real BEFORE snapshots, then native AFTER.
