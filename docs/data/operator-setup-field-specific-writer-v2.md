# Field-Specific Writer v2 Architecture

## Input

Operator + **one field contract** + relevant OE facts slice + Claims + sources + Tier1/2 exemplars

## Output

`{ value | null, confidence, evidence[], holdReason? }`

If evidence does not answer the field → **null (NO WRITE)**.

## Gates (all must pass)

1. Semantic contract match
2. Evidence supports THIS field
3. Company specificity (fails “applies to five operators” test unless standardized select)
4. Exemplar style shape
5. Cross-company duplication check vs Production peers
6. Only then completeness

## Anti-patterns banned

- Section-level context packets reused across neighboring fields
- Diligence boilerplate as Setup content
- Assignment-count meta as companyDescription
- Fixture prose as methodology

Implementation stub: `lib/operator-setup/field-specific-writer-v2.js`
