# Phase D Semantic Repair — Founder Review

**Status: Phase D NOT accepted as product-complete.**

Airtable repair: **NOT applied** (classification + pilot only). Fit: **BLOCKED**.

## 1. What went wrong

Phase D writers filled empty sections with OE-context templates. Completeness rose; semantic differentiation collapsed.

**Additional damage:** Phase D was supposed to blank-fill only, but a short-value threshold caused **overwrites** of real pre-D \`companyTagline\` values (e.g. Playa “Service from the Heart®”, Accor “Develop with Accor”, Royalton “All-In Luxury®”). Those verdicts are **RESTORE**.

## 2. Why technical completeness was misleading

Section Complete/Partial counted diligence boilerplate and assignment-count meta as “meaningful content.” That is not company intelligence.

## 3. Writer root cause

See `reports/operator-setup-phase-d-writer-root-cause.md`. Section-level templates; no field contracts; no exemplar gate; no differentiation test.

## 4. Field semantic-contract approach

`docs/data/operator-setup-field-semantic-contract-v2.md` + machine JSON. Blank if evidence does not answer the field.

## 5. Exemplar hierarchy

Tier1 pre-D Production → Tier2 Arbor/HE → Tier3 fixtures (format only). HE `infra_systems_technology` is the systems-map gold bar; Phase D hedges fail it.

## 6. Fixture leakage

Token-overlap hits vs fixture narratives: **0**. Primary failure mode is shared **agent templates**, not necessarily verbatim fixture clone — same product harm.

## 7. Repetition statistics

See `reports/operator-setup-phase-d-cross-company-duplication.md`. Highest generic-like fields include: phase_d_tech_stack, phase_d_reporting_systems, phase_d_engagement_cadence, Spanish, English, cap_profile_commercial, ownerEngagementNarrative, infra_systems_technology, infra_asset_management_reporting, ov_card_commercial, Mexico, _row.

## 8. Evidence fidelity

Assignment footprint ≠ governance quality, cadence, tech sophistication, or owner flexibility. Those Phase D fills are **UNSUPPORTED / GENERIC**.

## 9. KEEP / REWRITE / RESTORE / CLEAR / HOLD

| Verdict | Count |
| ------- | ----: |
| KEEP | 86 |
| REWRITE | 0 |
| RESTORE | 3 |
| CLEAR TO BLANK | 577 |
| HOLD | 58 |

## 10. Fields needing redesign

- NARROW: cap_profile_operational, cap_profile_commercial, cap_profile_transition, ownerEngagementNarrative, specializations, infra_systems_technology, infra_asset_management_reporting, differentiators
- STRUCTURE: specializations
- MOVE TO CLAIMS: cap_profile_transition, risk_programs_narrative
- DEPRECATE (as Setup truth): ov_card_commercial, ov_card_flexibility, risk_programs_narrative

## 11. Six-operator preview

`docs/reviews/operator-setup-semantic-repair-six-profile-preview.md` — pilot semantic verdict: **FAIL** for narratives.

## 12. Repair apply results

**Not applied.** Awaiting founder authorization to run selective CLEAR from `data/operator-setup/phase-d-repair/repair-write-plan.json`.

## 13–15. Post-repair / coverage / research

After planned CLEAR: invalid/generic → intentional blank. Research still required for owner engagement, systems, reporting, risk, deep ops, named leadership.

## 16. Can Setup be trusted by Fit?

**No — not yet.**

## 17. Exact founder decisions

1. Authorize CLEAR of Phase D template narratives and section create rows
2. KEEP structured profile-deepen facts + Management Structures Supported
3. HOLD explorer scaffold headlines pending product decision
4. Accept field semantic contract v2
5. Accept blank > generic policy
6. Do not start Fit until post-CLEAR semantic QA passes

## 18. Recommended next phase

**Phase D.1 — Apply selective CLEAR/KEEP/HOLD repair from repair-write-plan.json, then re-run duplication QA**

- No Fit/scoring changes
- Owner pilot remains disabled
