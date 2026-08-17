# Retroactive Database Cleanup Design (NOT RUN)

## Queues

1. High-confidence correction (Exact/High + corroboration)
2. Review
3. Missing evidence
4. Activation candidate (census hotels, no Active BE)
5. Image remediation
6. Cross-table inconsistency

## Suggested batch sizes

| Queue | Safe batch | Cadence |
|-------|------------|---------|
| Shadow Indigo/Kimpton MX | ~20–40 hotels | Daily |
| Identity enrichment proposals | 50–100 | Weekly steward |
| Activation research | 3–5 brands | Weekly |
| Image integrity audit | 1 brand pack / run | Weekly |
| Full census freshness (IHG CALA) | 100 hotels | Weekly quiet sequential |

All batches remain **proposal → human review → existing validation gates → optional approved write**.
