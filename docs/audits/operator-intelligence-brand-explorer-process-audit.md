# Operator Intelligence — Brand Explorer Process Audit

**Date:** 2026-08-03  
**Scope:** Reusable patterns for Operator Intelligence (not Operator Explorer presentation alone)  
**Mode:** Read-only audit

---

## Component reuse table

| Brand Explorer Component | Current Implementation | Reusable for Operators? | Reuse Directly | Adapt | Do Not Reuse | Reason |
| ------------------------ | ---------------------- | ----------------------- | -------------- | ----- | ------------ | ------ |
| Intelligence governance (validation levels, do-not-overwrite) | `docs/data-intelligence/INTELLIGENCE_GOVERNANCE.md`, `DATA_VALIDATION_PROTOCOL.md`, `SOURCE_RANKING_GUIDE.md` | Yes | ✓ | | | Entity-agnostic Brand/Operator/Parent |
| Partner Intelligence Source Library | `docs/partner-source-library-airtable-fields.md`, `api/lib/partner-intelligence-field-map.js` | Yes | ✓ | | | Already supports Operator links |
| Controlled publish / production queues | `intelligence-production-queue.js`, `controlled-publish-queue.js` | Yes | ✓ | | | Mixed brand+operator queues exist |
| Source ranking / CALA vs International Reference | `SOURCE_RANKING_GUIDE.md` | Yes | | ✓ | | Ranking rules generalize; brand wave packs do not |
| Tab Factory + release gates | Brand + Operator Explorer tab factories | Pattern | | ✓ | Brand contracts | Gate-aggregate release, not per-field founder approval |
| OS state machine / dry-run apply | `brand-explorer-os-*`, `operator-explorer-os.js` | Pattern | | ✓ | Merged enums | Separate domain modules |
| Factory Preview vs Active status | Brand Status Active/Live; OE submission_status | Pattern | | ✓ | Brand Status field | Operators use Master status |
| Protected baseline freeze | Brand 46/54 public-full; OE Arbor+HE | Pattern | | ✓ | Brand freeze artifacts | Freeze pattern yes; artifacts separate |
| Founder visual review packets | `brand-explorer-founder-visual-review.js` | Process | | ✓ | Brand packet fields | Methodology + exceptions, not every fact |
| Company Validated protection | Never auto-written | Yes | ✓ | | | Non-negotiable |
| Image uniqueness / Scene7 / Flexibility taxonomy | Brand gallery DAM pipeline | No | | | ✓ | Franchise DAM assumptions |
| PRIMARY_RELEASE / Lane restore lists | Brand operational overlays | No | | | ✓ | Not Active/Live SoT; do not import |
| FDD / fee-stack / Item 19 copy rules | Brand risk lexicon | Partial | | ✓ | Brand regex pack | Operators need different forbidden claims (performance guarantees) |
| Per-brand content writers | Hundreds of `*-writer.js` | Low | | ✓ | As-is writers | Reuse package **shape**, not brand content |
| Continuous auto-refresh of all Active brands | Not found as single loop | N/A | | ✓ | | Refresh is wave/audit-triggered — reuse that model |

---

## Answers

1. **Scalable:** Wave orchestration, automated gate bundles, dry-run-first apply, protected-universe freeze, shared PI capture→publish-readiness.
2. **Too founder-heavy today:** Founder visual review as hard gate for every Active release; Company Validated; source-library approvals; gold-bar editorial. **Operator Intelligence must shift routine facts to auto-publish rules** and keep founder on methodology + exceptions.
3. **Automatic objective validation:** Completeness, empty shells, provenance class, URL/date structure, forbidden-language scans, apply safety — **not** truth of commercial performance claims.
4. **Release gates vs field approval:** Mandatory gate bundles + OS canonical state + PVQL/baseline freeze decide release; build still audits fields mechanically.
5. **Generalizable:** Governance docs, Source Library, queues, Tab Factory process shape, apply-gate safety, freeze pattern, exception routers.
6. **Do not apply to operators:** Franchise Brand Status universe, Scene7 gallery uniqueness, Flexibility PIP taxonomy, FDD fee stacks, Value Creation Scenarios as brand economics, openings-as-franchise-proof.
7. **Reusable source/evidence:** Source Library schema, validation levels, source ranking, evidence-type + dated URL cards, controlled publish.
8. **Generalizable scripts:** Wave stage runners, OS/gate evaluators, quiet sequential audits, evidence/fact review package builders — not per-brand writers.
9. **Extendable validators:** Provenance-by-tab, tab contracts, completeness/empty shells, forbidden-language harness, evidence quality (dated+URL+entity), apply-gate enforcer, baseline freeze tests.
10. **Architecture choice:** **Separate Operator Intelligence domain layer** + **shared PI platform infrastructure** (already the repo direction). Do not merge Brand and Operator Explorer OS or Presentation schemas.

---

## Implication for this calibration

Build Operator Intelligence as claim→evidence→publication-policy (local first), consuming Fit v2 for scoring proof, without copying Brand Explorer presentation factories or requiring founder approval of every routine fact.
