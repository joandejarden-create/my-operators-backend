# GDI Semantic Commercial Remediation — Founder Report (V2)

**Generated:** 2026-11-13  
**Branch:** `fix/gdi-semantic-commercial-remediation-v1`  
**PR:** #234  
**Immutable baseline:** `reports/gdi-semantic-commercial-remediation-v1/00-immutable-baseline/` (unchanged)  
**Post-remediation:** `reports/gdi-semantic-commercial-remediation-v1/01-post-remediation/`  
**As-of clock:** 2026-11-13  
**Policy:** No merge / deploy without founder authorization. No paid research authorized.

---

## Founder deliverable summary

| # | Item | Result |
|---|---|---|
| 1 | Starting findings (V1 tip into V2) | **55** |
| 2 | Findings corrected in V2 | **55** (net product findings → **0**) |
| 3 | Remaining P0 / P1 / P2 / P3 | **0 / 0 / 0 / 0** |
| 4 | Scores (baseline → V1 → V2) | **85.1 → 91.6 → 92.1** overall |
| 5 | Unresolved classifications | **0** of 64 (all recovered/confirmed from existing research evidence) |
| 6 | Commit SHA | `2325e20751c512427446b8ceb21b83b568f1aa0f` |
| 7 | PR #234 CI | Pending after push — re-check Actions on PR #234 |
| 8 | Remaining risks | Commercial accuracy **88**, readability **86**, content **89** still below 90; Waterstone zero inventory separate; some classification recoveries are title-contract inferences that deserve founder skim |
| 9 | Merge recommendation | **Conditionally ready** for founder approval — zero product P0–P3 on re-audit; do **not** claim 90+ on commercial/readability/content |

---

## 1. Scores — baseline vs V1 vs V2

| Dimension | Baseline | V1 tip | V2 | 90+ claimed? |
|---|---:|---:|---:|---|
| Overall | 85.1 | 91.6 | **92.1** | Yes (overall only) |
| Semantic consistency | 81.1 | ~91.6 | **92** | Yes |
| Commercial accuracy | 78.6 | ~87.7 | **88** | **No** |
| Evidence reliability | 85.4 | — | **90** | Yes |
| Readability | 73.8 | 86 | **86** | **No** (formula caps clean rows at 86) |
| Content quality | 73.7 | ~88.8 | **89** | **No** |
| Research completeness | 99.9 | — | **100** | Yes |
| Technical consistency | 100 | 100 | **100** | Yes |

**Honesty rule:** V2 does **not** claim 90+ on commercial accuracy, readability, or content quality.

---

## 2. Findings corrected

| Metric | Baseline | V1 tip | V2 |
|---|---:|---:|---:|
| Product contradiction rows | 644 | 55 | **0** |
| P0 | 6 | 0 | **0** |
| P1 | 207 | 4 | **0** |
| P2 | 317 | 26 | **0** |
| P3 | 114 | 25 | **0** |

Harness-only codes (`SEM_HOTEL_VALIDATION_PLACEHOLDER`, `SEM_OTHER_DRAWER_BLOB_DUPLICATION`) remain excluded from product counts (1,398 harness hits across 699 packs).

---

## 3. V2 remediation themes (shared generators)

1. Demote `Booth Booth` / `OTHER_RELEVANT` exhibitor placeholders → contact research gap  
2. Soften unpublished-attendance hard crowd / room-demand claims  
3. Strip unsupported “increasing trend / opportune time / advance planning” urgency  
4. Filter decade-stale edition evidence URLs from customer-facing sources  
5. Past-cycle / monitor / closed-sourcing / title / enum / evidence-dedupe guards retained from V1  
6. Classification recovery via research-title contracts (no paid research; no arbitrary defaults)

Regression suite: `npm run test:gdi-semantic-commercial-remediation-v1` (+ title/sourcing + recovery baselines).

---

## 4. Classification outcomes (former 30 unresolved)

| Status | Count |
|---|---:|
| Recovered / confirmed from existing research evidence | **64** |
| Explicitly flagged unresolved | **0** |
| Paid research used | **0** |

Details: `01-post-remediation/04-classification-remediation.json`  
Founder skim recommended for stretch title-contract cases (e.g. sports/government inferences) before treating every recovery as permanently authoritative.

---

## 5. Browser / app validation

- Bethesda Marriott + Wyndham San Diego Bayside drawers: verified via `verify-gdi-regression-recovery-browser-v1`  
- Waterstone: still **0 of 0** customer-visible opportunities (separate investigation)  
- Card badges / filters: no verification badges reintroduced; toolbar heights stable  
- Surfe / guided tour / hotel feedback / authorization gates: unchanged by this remediation layer

---

## 6. Remaining commercial / research risks

1. **Score ceiling honesty** — commercial 88 / readability 86 / content 89 are intentional rubric outcomes, not defects to “fix” by weakening detectors.  
2. **Classification stretch cases** — title-contract recovery is evidence-backed but not a full research refresh.  
3. **Waterstone empty inventory** — not addressed in this PR.  
4. **Underlying research quality** — presentation remediations do not replace deeper lodging-path confirmation on future-cycle rows.

---

## 7. Release gates

| Gate | Status |
|---|---|
| Immutable baseline preserved | PASS |
| Re-render all 699 opportunities | PASS |
| Product P0/P1 = 0 | PASS |
| Regression tests | PASS |
| Merge / deploy | **NOT AUTHORIZED** (await founder) |
| Paid research | **NOT AUTHORIZED** |

---

## 8. Founder acceptance checklist

- [ ] Accept overall 92.1 with commercial/readability/content explicitly below 90  
- [ ] Accept 0 remaining product P0–P3 on the V2 re-audit  
- [ ] Skim classification recovery file for stretch inferences  
- [ ] Confirm Waterstone empty inventory stays a follow-up, not a blocker for this PR  
- [ ] Authorize merge of PR #234 (or request further work)  
- [ ] Do **not** authorize deploy until merge decision is explicit
