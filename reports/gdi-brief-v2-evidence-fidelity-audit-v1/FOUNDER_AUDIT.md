# GDI Brief v2 — Evidence-fidelity audit (Phase A)

Fixed clock: 2026-11-19. Paid Exa calls: **0**. No invented source facts.

## Method

For each frozen fixture, compare composed Level-1 Brief fields against a curated **evidence catalog** built only from archived Exa / Webhound / opportunity JSON / contact findings. Facts marked `ABSENT_FROM_RESEARCH` are research gaps, not processing losses.

## Trace summary

| Sample | Fidelity | Judgment | Retained | Omitted | Research-absent | Unsupported |
|---|---:|---|---:|---:|---:|---:|
| pdac | 100 | STRONG | 4 | 2 | 1 | 0 |
| nice | 100 | STRONG | 3 | 2 | 1 | 0 |
| nrc-ric | 100 | STRONG | 3 | 0 | 1 | 0 |
| imc-construction | 100 | STRONG | 3 | 0 | 1 | 0 |
| owwa | 100 | STRONG | 3 | 3 | 1 | 0 |

## Composer honesty fixes applied

- Partner listing ≠ competitor overflow (PDAC → `LISTED_PARTNER_QUALIFY`)
- No “credible host” from venue TBA (NICE)
- Past cycle / selected venue does not get open-host template (NRC)
- Truncation repair never invents visiting-staff demand filler (IMC)
- Scaling accounts do not get conference RFP/site-visit bleed (IMC)

## Per-opportunity omitted material (still Level-2 / next-slice)

### pdac

**Posture:** `LISTED_PARTNER_QUALIFY`

**Omitted from Level-1 (may belong in Supporting Research):**
  - [SUPPORTING] PDAC does not use 3rd party housing bureaus; book directly with hotels
  - [SUPPORTING] convention@pdac.ca is a published organizational route on staff materials

### nice

**Posture:** `FUTURE_VENUE_OPEN`

**Omitted from Level-1 (may belong in Supporting Research):**
  - [SUPPORTING] FIU / New America operate conference logistics under NIST award
  - [SUPPORTING] Customer reported hotel added to sourced properties (2026-09-17)

### nrc-ric

**Posture:** `PAST_CYCLE_DO_NOT_PURSUE`

**Omitted from Level-1 (may belong in Supporting Research):**
  - (none in catalog vs Level-1 blob)

### imc-construction

**Posture:** `SCALING_ACCOUNT_QUALIFY`

**Omitted from Level-1 (may belong in Supporting Research):**
  - (none in catalog vs Level-1 blob)

### owwa

**Posture:** `COMPETITOR_PLACED_OVERFLOW_ONLY`

**Omitted from Level-1 (may belong in Supporting Research):**
  - [SUPPORTING] Venue is Enercare Centre, Exhibition Place
  - [SUPPORTING] Hotel reservations open October 20
  - [SUPPORTING] spetryna@owwa.ca verified on staff page (Exa)


## Two-level presentation

- **Level 1** — Commercial Intelligence Brief (existing drawer fields)
- **Level 2** — Supporting Research (lodging evidence, contacts, claims, sources, gaps)

Contract: `docs/data-intelligence/gdi-commercial-fact-selection-contract-v1.md`
