# Deal C — Santa Fe / Highgate Tie Reproduction

**Audit only.** Source: live evaluate + `reports/operator-fit-differentiation-audit.json`.

## Side-by-side ranking components

| Component | Santa Fe | Highgate | Difference |
| --------- | -------: | -------: | ---------: |
| Eligibility status | Eligible With Conditions | Eligible With Conditions | 0 |
| Geography factor | 78 known | 78 known | 0 |
| Segment factor | 100 known | 100 known | 0 |
| Asset/development factor | 80 known | 80 known | 0 |
| Project complexity | 0 known | 0 known | 0 |
| Brand experience | N/A | N/A | 0 |
| Ownership/governance | N/A | N/A | 0 |
| Regional resources | 0 unknown | 0 unknown | 0 |
| Commercial differentiator | 0 unknown | 0 unknown | 0 |
| Operating Structure Alignment | unknown / null | unknown / null | 0 |
| Brand–Operator Compatibility | Not Applicable | Not Applicable | 0 |
| Raw Operator–Project | 59.0 | 59.0 | 0 |
| Layered primary (pre-risk) | 48.6 | 48.6 | 0 |
| Execution Risk penalty | 10 | 10 | 0 |
| Evidence Strength | Strong | Strong | 0 |
| Data Coverage % | 71.6 | 71.6 | 0 |
| Evidence ceiling | null | null | 0 |
| Displayed Alignment | 38.6 | 38.6 | **0** |
| Alignment band | Limited | Limited | 0 |
| Readiness | Ranking Ready | Ranking Ready | 0 |
| Final rank | 1 | 2 | ordinal only |
| Tie-breaker | `reckyv9…` | `recLjxt…` | localeCompare −1 |

**Hidden decimal difference:** none detected at 0.001 precision on displayed / pre-risk / factor scores.

## Why the Engine Cannot Distinguish These Two Operators

| Real Difference | Available Data? | Current Factor | Current Score Effect | Narrative Effect | Lost Differentiation? |
| --------------- | --------------- | -------------- | -------------------: | ---------------- | --------------------- |
| Mexico third-party portfolio comps vs DR luxury resort comparable | Yes (case-study / calibration comps) | Asset/development | Both land on **80** (one “relevant” comp → 70+10) | Different Why text | **Yes — score collapse** |
| Highgate also has Aloft Tulum (Mexico) + Tambo Peru in records | Yes | Asset/development relevance filter | Extra comps often **not** counted as “relevant” for New Build/Mixed-Use Deal C; no geography bonus inside asset once relevant | Not in Why (Ocean Club used) | **Yes** |
| Comparability Strength High/Moderate/Limited | **Not populated / not an engine enum** | — | None | None | Taxonomy gap |
| Geographic depth of Mexico presence (local operator vs international with Mexico) | Partial (presence / countries) | Geography factor | Both **78** country hit (no city on Deal C) | Same “Active country: Mexico” | **Yes — depth collapsed** |
| Market Presence type depth | Eligibility | Eligibility / geo eligibility | Both eligible with conditions | — | Depth not in factor score |
| Mixed-use evidence | Thin for both | Project complexity | Both **0** | Shared concern “No clear evidence for mixed-use” | Tied weakness |
| Preferred brands | Empty on pilot path | Brand experience + compat | Both N/A | Generic brand note | Path data gap |
| Owner structure preference | Empty on pilot path | Structure layer + risk | Both unknown → same −5 risk | Same Validate Next | Path data gap |
| Regional team confirmation | Missing both | Regional factor + risk | Both unknown 0 + −5 risk | Same unknown | Tied |
| Commercial differentiators | Missing both | Commercial factor | Both unknown 0 | Same | Tied |

### Exact reason for the tie

All ranking keys through #5 are equal. Order is decided only by stable `candidateId` tie-break. Owner narratives still show different comparable strings because `whyItMatches` copies **positiveEvidence text**, not a score delta.
