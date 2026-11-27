# Checkpoint A Wave 1 — Operational Defect

**At:** 2026-11-27  
**Auth:** `FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127`  
**Wave 1 spend:** USD 2.50 (25 × $0.10) · all jobs COMPLETED  

## Defect

Exa Agent returned commercially relevant named people in `evidence` (including LinkedIn profile URLs) while setting structured `directBuyer` / `entryPoints` to null / `NOT_FOUND`.

Structured recovery after Wave 1: **0 / 25**.  
Evidence with named LinkedIn URLs: **5+** opportunities (e.g. AICPA Mallory Joyner / Megan Gavin).

Quality gates correctly refused to invent structured contacts from evidence prose alone.

## Remediation (within Checkpoint A ceiling)

- Tightened live adapter prompt + output schema (contract `…-v1.1`)
- New job keys (no reuse of broken Wave 1 settled structures)
- Corrective Live wave under same founder GO
- Prior Wave 1 spend counts toward USD 5.00 Checkpoint A ceiling (remaining USD 2.50)
