# Golden Quality Model

**Version:** golden-quality-model-v1

## Separation

| Measure | Meaning |
| --- | --- |
| **Golden Completeness** | Share of Priority fields populated |
| **Golden Quality / Validity** | Semantic correctness + evidence + identity + coherence |

A hotel at 95% completeness with wrong City or wrong Current Brand must **not** score as high quality.

## Dimensions (Quality)

1. field_completeness (15%)
2. semantic_validity (30%)
3. identity_confidence (15%)
4. source_eligibility (10%)
5. geography_coherence (15%)
6. affiliation_confidence (10%)
7. freshness (5%)

## Principle

Prefer **blank** over **wrong**. Completeness is optimized only after validity gates pass.
