# Operator Fit — Internal Pilot Founder Review

**Date:** 2026-08-04  
**Branch:** `app-shell-left-nav` @ `3c88c0b`  
**Owner pilot:** **Disabled** · **My Deals:** **Unwired** · **OAS / Brand Match v2 / intake:** **Unchanged**

---

## 1. Pilot objective

Validate whether Operator Fit gives a Dealality advisor enough credible, differentiated information to shortlist and move toward outreach — before any owner access.

## 2. Argentina Master onboarding

| Operator | Master ID | Lifecycle |
| -------- | --------- | --------- |
| Álvarez Argüelles Hoteles | `recjgHXqTJktijFUR` | Research Stage |
| Tremun Hoteles | `recHj56wpRLUnJ5Wx` | Research Stage |
| AADESA | `rec9JSyGQjvodsPSJ` | Research Stage |

Platform / Commercial / Profile children + Market Presence + Claims linked from Wave 3. **Not** promoted to Active. Details: `reports/operator-intelligence-argentina-master-onboarding.md`.

## 3. Deal B after onboarding

- Production Ranking Ready: **0**  
- Research-Stage Ranking Ready: **3** (Álvarez Argüelles, AADESA, Tremun)  
- Production visibility: **hidden**  
- Research Stage is **not** silently treated as production Active  

Report: `reports/operator-fit-deal-b-post-master-onboarding.md`

## 4. Shortlist implementation

- Dedicated object: `Operator Fit - Shortlist` (schema ensure dry-run: would_create)  
- File-backed pilot store: `data/operator-fit/shortlist-store.json`  
- Immutable decision snapshots; remove preserves history  
- **ODR not used** · outreach **not** auto-created  

## 5. Access-control implementation

Server-side: `OPERATOR_FIT_INTERNAL_PILOT` + admin runbook + `OPERATOR_FIT_PILOT_DEAL_ALLOWLIST` + production guard. Global owner kill switch `OPERATOR_FIT_ENGINE_V2` remains default **off**. Tests: `npm run test:operator-fit-internal-pilot`.

## 6. Internal workflow

Route: `/internal/operator-fit-pilot.html`  
Flow: Deal → Results → Evidence → Compare → Shortlist → Notes → (outreach later, not in pilot).

## 7. Comparison experience

Side-by-side ≤4; highlights genuine differences; Unknown styled separately from negatives.

## 8. Difference-driver experience

Deterministic `explainRankingDifference()` — ≤5 drivers, no AI invention. Tested for determinism.

## 9. Ranking-change validation experience

`listRankingChangeValidations()` — phased before shortlist / outreach / during outreach / proposal / final.

## 10. Pilot deals tested

| Deal | Archetype | Production RR | Research-Stage RR | Thin/Zero |
| ---- | --------- | ------------: | ----------------: | --------- |
| A | Urban Peru | 2 | 0 | thin |
| B | Argentina leisure | 0 | 3 | zero_production |
| C | Mexico complex | 5 | 0 | full |
| D | Conversion MX (synthetic) | 4 | 0 | thin |
| E | Resort DR (synthetic) | 3 | 0 | thin |

## 11. Advisor scorecards

Templates in `reports/operator-fit-internal-pilot-advisor-scorecards.md` — system fields pre-filled; human judgment marked Not Tested for founder sessions.

## 12. Main positive findings

- Ranking differs by geography / project type (A ≠ B ≠ C ≠ D/E)  
- Thin/zero states honest with constrained-universe language  
- Research Stage lane clear vs production  
- Shortlist snapshots preserve decision history  
- Comparison + difference drivers are deterministic  

## 13. Counterintuitive results

- Deal B can look “empty” in production lane while research lane is rich — correct, not a bug  
- Synthetic resort (DR) may rank operators without DR Market Presence strength depending on universe overlays — watch geo cliffs  

## 14. Ranking stability

Documented in `reports/operator-fit-internal-pilot-ranking-stability.md`. Market Presence eligibility cliffs are intentional; thin universes amplify relative rank volatility. **No auto weight tuning.**

## 15. Data gaps

Brand approval depth; operator interest; project fees; regional team confirmation; some comparable depth.

## 16. Brand-approval gaps

Systematically Unknown across Ranking Ready set — see `reports/operator-fit-brand-operator-relationship-gap-analysis.md`. Do not manufacture approval from portfolio flags.

## 17. Information hierarchy findings

Owners need three layers; claim/source graphs stay internal — `docs/reviews/operator-fit-information-hierarchy-review.md`.

## 18. Shortlist vs Target List

Target List remains brand-only. Operator Shortlist is separate. ODR = outreach later.

## 19. Validation lifecycle

`docs/process/operator-fit-validation-lifecycle.md`

## 20. Analytics/events captured

Local JSONL: `data/operator-fit/internal-pilot-events.jsonl` — results viewed, detail, evidence, shortlist, remove, compare, validations, research-stage viewed.

## 21. Security findings

- Client-only flags insufficient (enforced server-side)  
- Owner My Deals Fit path remains flag-off  
- Research Stage not in Active loader  
- Kill switches available without DB changes  

## 22. Controlled owner-pilot gates

Scored in `docs/reviews/operator-fit-controlled-owner-pilot-gates.md` — **Not ready** (brand approval Failed; Deal B production depth Partial; human scorecards Partial).

## 23. Remaining blockers (owner pilot)

1. Brand–operator approval depth  
2. Human advisor scorecard sessions pass  
3. Owner information hierarchy / terminology founder approval  
4. Sufficient production Ranking Ready depth for chosen owner deals  
5. Optional Airtable Shortlist table apply + operational refresh cadence  

## 24. Exact founder decisions required

1. Confirm continue **internal** pilot (recommended)  
2. Approve Airtable apply for Shortlist table when ready  
3. Approve owner information hierarchy + terminology  
4. Decide when Argentina operators may graduate Research Stage → Active (criteria only — not now)  
5. Explicit future decision for controlled owner pilot (not now)  

## 25. Recommendation

**Continue internal pilot. Not ready for controlled owner pilot.**

Do not enable owner pilot · do not wire My Deals · do not tune weights for optics · do not deprecate OAS.
