# Source Ranking Guide

How to rank source trust and regional relevance for Dealality intelligence.

## Highest Trust

- Direct company submission.
- Company-validated profile.
- Official brand/operator development brochure.
- Official company website.
- Official investor materials.
- Official press releases.
- Formal/regulatory disclosures where relevant.

## Medium Trust

- Recognized hospitality media.
- Hotel industry publications.
- Conference materials.
- Reputable market research.
- Public brand directory pages.
- Public portfolio pages.

## Lower Trust

- Old PDFs.
- Generic directories.
- Non-regional materials used for regional assumptions.
- AI summaries without sources.
- Scraped snippets with unclear origin.
- Unclear upload origin.

## Regional Relevance

For CALA-specific Dealality logic:

- Recent CALA-specific company material is strongest.
- Regional executive/company-provided information is strong.
- Global material may be used as reference but should be labeled accordingly.
- Old or non-regional material should not drive strong conclusions.

## Application

When extracting or scoring:

1. Assign trust level at source capture.
2. Downgrade confidence when regional mismatch exists.
3. Flag when lower-trust sources are the only evidence for a high-visibility field.
4. Prefer multiple corroborating sources over a single weak source.

## Related Documentation

- [DATA_VALIDATION_PROTOCOL.md](./DATA_VALIDATION_PROTOCOL.md)
- [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md)
- [CONTENT_EXTRACTION_TEMPLATE.md](./CONTENT_EXTRACTION_TEMPLATE.md)
- Existing patterns: [../partner-source-discovery-patterns.md](../partner-source-discovery-patterns.md)
