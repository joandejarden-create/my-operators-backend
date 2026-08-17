# Dealality QA Checklist

Use before finalizing any feature, content import, or significant change.

## Product Alignment

- [ ] Does this support Dealality's role as a hotel deal intelligence platform?
- [ ] Does this avoid turning Dealality into a listing marketplace?
- [ ] Does this support recurring use?
- [ ] Does this preserve owner control?
- [ ] Does this respect brand/operator/owner separation?

## AI Output Quality

- [ ] Does the output separate evidence, interpretation, and next action?
- [ ] Does it avoid overclaiming?
- [ ] Does it show confidence/source status where relevant?
- [ ] Does it identify missing data?
- [ ] Does it avoid legal/advisory language?

## Data Quality

- [ ] Are sources tracked?
- [ ] Is validation status clear?
- [ ] Is usage permission clear?
- [ ] Are stale/old sources labeled?
- [ ] Are regional/global distinctions clear?
- [ ] Is company-validated data protected?

## Technical QA

- [ ] Does it avoid inventing Airtable fields?
- [ ] Does it respect existing API patterns?
- [ ] Does it avoid unrelated file changes?
- [ ] Does it handle missing data gracefully?
- [ ] Were tests/checks run where practical?

## UI / Copy QA

- [ ] Proper Case for short UI labels.
- [ ] Sentence case for helper text.
- [ ] Premium, calm, structured tone.
- [ ] No generic AI-sounding copy.
- [ ] No unsupported "best," "guaranteed," or "validated" claims.

## Access / Security QA

- [ ] Does it preserve workspace access assumptions?
- [ ] Does it account for future region/deal access?
- [ ] Does it avoid exposing internal-only data externally?
- [ ] Does it avoid showing unreviewed intelligence to the wrong user?

## Definition of Done

A feature is done only when:

- It works technically.
- It fits Dealality's strategy.
- It uses correct language.
- It handles missing data.
- It respects data governance.
- It has tests or a test plan.
- It updates relevant docs if durable decisions were made.

## Related Documentation

- Product principles: [DEALALITY_PRODUCT_CONSTITUTION.md](./DEALALITY_PRODUCT_CONSTITUTION.md)
- Naming: [NAMING_AND_COPY_GUIDE.md](./NAMING_AND_COPY_GUIDE.md)
- Content QA: [../data-intelligence/CONTENT_QA_CHECKLIST.md](../data-intelligence/CONTENT_QA_CHECKLIST.md)
- PR matrix: [../dealality-pr-validation-matrix.md](../dealality-pr-validation-matrix.md)
