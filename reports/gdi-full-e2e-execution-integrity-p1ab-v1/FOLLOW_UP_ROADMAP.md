# Follow-up Roadmap (not in this PR)

| ID | Workstream | Notes |
|----|------------|-------|
| P1-C | Single canonical Full E2E entry point | Route campaign scripts through hotel template / research-to-page so all hotels inherit ledger |
| P1-D | Weekly Change contract unification | UI vs backend CHANGE_CLASS mismatch from audit |
| P1-E | Generalized publication parity | Extend YOTEL certification gate to cohort hotels |
| P2 | Segment-true discovery + drawer quality | After execution evidence is trustworthy |
| FND-012 | Cross-hotel confidentiality API assessment | Read-only security follow-up |

## Suggested next implementation PR

**P1-C** — thin wrapper ensuring `gdi:full-e2e` / hotel template always sets execution observability context and refuses to claim COMPLETE without 14 terminal segment rows — still no paid research required.
