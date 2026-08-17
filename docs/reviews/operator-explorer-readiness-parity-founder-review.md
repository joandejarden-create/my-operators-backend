# Operator Explorer — Readiness Parity Founder Review

**Date:** 2026-08-10
**Fit/scoring:** unchanged · **Owner pilot:** disabled

## 1. Why this audit was required

Dry-run Explorer Publishable **19** vs Airtable-backed **5** despite near-parity persistence.

## 2. Discrepancy

| Metric | Dry-run | Airtable Phase1 |
| ------ | ------: | --------------: |
| Strong | 4 | 2 |
| Publishable | 19 | 5 |
| Thin | 6 | 14 |
| Not Publishable | 2 | 8 |

## 3. Operators downgraded (publishable)

- Aimbridge Hospitality (LATAM): Useful Profile → Thin Profile
- Grupo Hotelero Santa Fe: Useful Profile → Thin Profile
- Driftwood Hospitality Management: Useful Profile → Thin Profile
- Atlantica Hotels International (AHI): Useful Profile → Thin Profile
- Cenote Azul Operadores: Useful Profile → Thin Profile
- Hyatt (Managed): Useful Profile → Thin Profile
- Sonesta International: Useful Profile → Thin Profile
- Four Seasons Hotels and Resorts: Useful Profile → Thin Profile
- Rosewood Hotel Group: Useful Profile → Thin Profile
- Mandarin Oriental Hotel Group: Useful Profile → Thin Profile
- Meliá Hotels International: Useful Profile → Thin Profile
- Auberge Resorts Collection: Useful Profile → Thin Profile
- Shangri-La Group: Useful Profile → Thin Profile
- Barceló Hotel Group: Useful Profile → Thin Profile

Strong-only downgrades: GHL, Playa (Strong → Useful) — still publishable under Phase1.

## 4. Root causes

**Primary: Readiness-rule inconsistency** (12 of 14 publishable downgrades).

Phase 1 Airtable classifier required `asg≥5` (+ `mp≥2`) for Useful; dry-run required `asg≥2` + ≥1 country (+ Track2 BMC).

Secondary: **Aggregate holdouts** (2) — Atlantica (and partial Sonesta).

**Not causal:** Record Purpose, lifecycle, Claims PI links, Brand text debt, derived Master summaries (neither classifier reads them).

## 5. Record Purpose effect

**0** of the 19→5 drop. Neither classifier checks Record Purpose.

Canonical policy **should** gate Publishable on Production (would reduce publishable among Research Masters if adopted).

## 6. Lifecycle effect

**0** direct effect on Phase1 vs dry-run. Matrix (all Masters):

- Research | Research Stage: 13
- Production | Active: 23
- Test Fixture | In Review: 9
- Production | In Review: 1

Notable: Production + Active (expected); Research + Research Stage (new Track2 + Argentina); Research + Active should be reviewed for Purpose/lifecycle clarity but did not cause this drop.

## 7–11. Domain effects

- Assignments: named persistence strong; 9 aggregates held intentionally
- Brand Rel: 51/24 BMC OK; not root cause
- Presence: 20 creates / 0 updates; row-count gate stricter than country gate
- Claims/PI: existing Claim IDs present; not readiness gates
- Derived summaries: **no dependency** in either readiness function

## 12. Profile section parity

See `reports/operator-explorer-profile-section-parity.md`. Overview equivalent; numeric sections differ mainly by holds + gate math.

## 13. Canonical readiness policy

`docs/product/operator-explorer-readiness-canonical-policy.md`

## 14. Correct recalculated readiness (recommended)

### A) Dry-run rules on Airtable named data (parity recovery / defect fix)

| Class | Count |
| ----- | ----: |
| Strong (dry-style on Airtable) | 4 |
| Explorer Publishable (dry-style on Airtable) | 17 |

### B) Canonical policy (Production-gated Publishable)

| Class | Count |
| ----- | ----: |
| Strong | 4 |
| Explorer Publishable | 9 |
| Thin | 9 |
| Not Publishable | 9 |
| Content-complete but Research-gated | 8 |

## 15–16. Webhound

Still running — deferred. Supplemental value TBD; expect named Track2 assignments when complete.

## 17. CALA gap

See `reports/operator-explorer-cala-assignment-gap.md` — Confirmed Direct Management operators with 0 named CALA assignments are the enrichment priority.

## 18. Research Masters

**Graduate now:** none. **Remain Research:** all 13.

## 19–20. Conflicts

Playa–Hyatt: keep separate Masters; hold ownership narrative.  
Cenote: keep limited geo; no unsupported Active Countries.

## 21. Internal preview

**Ready With Minor Fixes** — adopt shared readiness module first.

## 22. Founder decisions required

1. Approve canonical readiness policy (incl. Research ≠ Publishable)
2. Approve replacing Phase1 thresholds with shared module (Path A)
3. Confirm no mass Research→Production graduation
4. Playa–Hyatt ownership narrative: hold vs publish Claim
5. When Webhound completes: allow supplemental dry-run write plan review

## 23. Recommended next phase — **Path D (Combination), priority order**

1. **Fix parity defect** — shared readiness module (restore consistent classification)
2. **Lifecycle clarity** — document Purpose vs submission_status; no mass graduation
3. **Targeted enrichment** — named CALA / Track2 assignments (Webhound merge when done)

**Not Path B alone** — would mask the rule bug. **Not Path C** — Research purpose is intentional for new Masters.
