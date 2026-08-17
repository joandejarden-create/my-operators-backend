# Phase D Writer Root Cause

## What went wrong

`phase-d-section-writers.js` optimized for **section coverage** (fill empty Production sections) rather than **field semantic fidelity**.

Blank-fill used a short-value threshold (~24 chars) that treated real taglines as empty and **overwrote** them (RESTORE on `companyTagline`).

| Failure mode | Mechanism |
| ------------ | --------- |
| Section-level writers | One OE context packet fed many neighboring fields |
| Templates | Same sentence frames with operator/geo/brand/count swaps |
| Evidence stretch | Assignment counts → governance, cadence, tech sophistication |
| Diligence boilerplate | “Confirm in diligence / underwrite from MA” written as Setup truth |
| No exemplar gating | Did not compare to HE/Arbor Tier-1/2 real prose |
| No differentiation test | Did not reject “could apply to five other operators” |
| Completeness KPI | Empty→Partial/Complete rewarded filler |
| Create-row flood | Engagement/Infra/Leadership Platform rows cloned thin templates |

## Per-writer assessment

| Writer | Field-specific? | Uses exemplars? | Diff test? | Verdict |
| ------ | --------------- | --------------- | ---------- | ------- |
| phase-d-platform | No (3 caps + markets) | No | No | Unsafe for narrative |
| phase-d-commercial | No | No | No | Unsafe |
| phase-d-governance | No | No | No | Unsafe |
| phase-d-engagement | Section rows | No | No | Unsafe — CLEAR rows |
| phase-d-infrastructure | Section rows | No | No | Unsafe — CLEAR rows |
| phase-d-leadership-platform | Market list semi-ok; bodies template | No | No | CLEAR template bodies/rows |
| phase-d-oe-profile | Meta description | No | No | CLEAR OE-meta descriptions |
| profile-deepen | Pack field map | Pack content | N/A | KEEP structured; HOLD scaffold headlines |

## Required replacement

Field-Specific Writer v2: contract + evidence slice + exemplars → value **or BLANK**. No section packet reuse across unrelated fields.
