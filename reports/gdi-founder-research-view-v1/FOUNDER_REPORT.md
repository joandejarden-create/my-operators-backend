# GDI Founder Research View R1 — Founder Report

**Status:** READY FOR INTEGRATION CONTROLLER  
**Branch:** `feat/gdi-founder-research-view-v1`  
**Base:** `origin/main` @ `0956eea` (PR #588 tip)  
**Spend:** $0  
**Not done:** merge, STAGED/CURRENT pointer changes, Airtable writes, Exa, production deploy

## Verdict

Founders can see research inventory for hotels that lack a finalized STAGED package, without treating research as finalized and without weakening customer publication gates.

## What shipped

1. Hotel inventory summary — **Discovered / Qualified Research / Finalized STAGED** from independent sources  
2. Lifecycle nav — **Finalized Opportunities** (default) / **Research Opportunities**  
3. Missing-STAGED empty state — explicit copy + counts + **View Research Opportunities**  
4. Research banner — **RESEARCH VIEW — NOT FINALIZED** + return to Finalized  
5. Server-side founder gate for `?live=1` / research export (`gdi-founder-research-access-v1`)  
6. 409 STAGED responses enriched with `researchSummary` + `lifecycleState` (no silent research fallback)  
7. Lifecycle-specific export query params  
8. Permanent test `test:gdi-founder-research-view-v1` wired into GDI CI workflow

## Hilton New York Times Square acceptance

| Check | Expected | Observed | Pass |
|---|---|---|---|
| Canonical hotel ID | `rec35fExUxCClpOP6` | same | YES |
| Discovered | 45 | 45 | YES |
| Qualified Research | 44 | 44 | YES |
| STAGED API | 409 | 409 `STAGED_PUBLICATION_PREVIEW_UNAVAILABLE` | YES |
| lifecycleState | research available | `STAGED_UNAVAILABLE_RESEARCH_AVAILABLE` | YES |
| Research `?live=1` | 44 records | 44 | YES |
| NOT FINALIZED banner | visible | visible | YES |
| STAGED pointer created | no | no | YES |

Evidence: `HILTON_ACCEPTANCE.json`, screenshots under `screenshots/`.

## Seven-hotel Research visibility matrix

See `SEVEN_HOTEL_RESEARCH_MATRIX.json`. All seven return STAGED 409 with accessible research:

| Hotel | Discovered | Qualified / live | STAGED |
|---|---:|---:|---|
| Hilton New York Times Square | 45 | 44 | 409 |
| AC Hotel A Coruña | 41 | 5 | 409 |
| Cambridge Beaches Resort & Spa | 8 | 8 | 409 |
| Castillo Hotel Son Vida | 20 | 20 | 409 |
| NOW NOW NOHO | 8 | 8 | 409 |
| Sheraton Mallorca Arabella | 30 | 30 | 409 |
| The Westin Grand München | 26 | 21 | 409 |

## STAGED regression matrix (unchanged package counts)

| Hotel | Expected STAGED count | Result |
|---|---:|---|
| Condesa DF | 4 | PASS |
| LaLuna | 3 | PASS |
| Memmo Príncipe Real | 11 | PASS |
| Bethesda Marriott | 67 | PASS |

## Tests / CI precheck

```text
npm run test:gdi-founder-research-view-v1   → 11/11 PASS (GDI_AUDIT_BASE=http://localhost:8090)
npm run test:gdi-missing-inventory-empty-state-v1 → PASS
npm run dealality:ci-precheck → PASS (gitleaks)
```

Permanent CI: `.github/workflows/gdi-regression-recovery-baseline.yml` runs `test:gdi-founder-research-view-v1`.

## Auth / API notes

- Research browse requires founder/admin entitlement server-side (`assertFounderResearchAccess`).  
- Unauthorized `?live=1` rejected; external share path remains fail-closed.  
- STAGED `preview=staged` still 409 when package missing; research summary is additive metadata only.

## Screenshots

1. `screenshots/hilton-missing-staged-before.png` — missing STAGED + counts + View Research  
2. `screenshots/hilton-research-view-after.png` — Research view / NOT FINALIZED / 44 opportunities  

## Exceptions / remaining

1. Localhost selectable hotel catalog on tip WT acceptance server (8090) currently lists **22** hotels (not 25). Portfolio audit soft-asserted `>=20`; full 25-property list remains on production-like 8080 when that catalog is loaded.  
2. Cursor browser automation did not deliver DOM click events to page JS in this environment; Research navigation was verified via `?inventory=research&live=1` and API contracts. Lifecycle click handlers use root event delegation (`_gdiLifecycleDelegated`).  
3. Unrelated dirty `pursuits.json` under Design Hotels hotel path was **not** included in this PR.  
4. Integration Controller owns merge; product chat must not merge.

## Governance confirmation

- No paid research / Exa Agent execution  
- No Airtable writes  
- No STAGED/CURRENT pointer flips  
- No customer publication / production deploy  

**Final acceptance claim:** A founder opening a hotel with existing research can see that intelligence exists, open Research immediately, and distinguish it from finalized STAGED opportunities—without weakening customer publication gates.
