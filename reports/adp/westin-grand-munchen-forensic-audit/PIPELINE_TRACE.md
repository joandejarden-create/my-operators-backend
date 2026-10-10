# Pipeline Trace — Westin Grand München

| Stage | Input | Output | Dropped | Reason | Artifact |
|-------|------:|-------:|--------:|--------|----------|
| Property identity | 1 | 1 | 0 | | profile fixture |
| Scenario universe | — | 63 | 0 | | scenario registry |
| Provider requests | 63×4=252 | 251 success | 1 | timeout/error | runtime period |
| Hotel mentions (raw parse) | 251 | 1180 mentions / 673 unique names | — | | obs.competitorsMentioned |
| Entity resolution (ORIGINAL) | 673 names | **0** bound | ~all hotels | unbound_fail_closed / no Munich registry | customer-entity-resolution |
| Entity resolution (REPAIRED) | 673 names | 25 entities | unresolved prose/brands | fail-closed unbound remainder | Munich registry |
| Competitive universe (ORIGINAL published) | — | **0** competitors | all | bind empty | report competitiveSet.observed |
| Competitive universe (RECOMPUTED) | — | **10** | threshold/top10 | | buildOwnerPayload |
| Displacement (ORIGINAL) | — | **0** | | empty universe | lostDemand.displacement |
| Displacement (RECOMPUTED) | — | **5** | | | |
| Attributes tracked (ORIGINAL) | 10 profile | **8** | wellness, large_ballroom | missing ATTRIBUTE_DEFINITIONS | realityGap |
| Attributes tracked (RECOMPUTED) | 10 | **10** | | | |
| Certification (ORIGINAL) | | CERTIFIED | | did not validate non-empty competitive universe | |
