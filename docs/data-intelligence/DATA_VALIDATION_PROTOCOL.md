# Data Validation Protocol

How data and content enter Dealality and become platform-safe intelligence.

## Purpose

Dealality must treat platform content as structured intelligence, not generic AI-generated text.

## Standard Flow

```text
Source Capture
→ AI Extraction
→ Validation Status
→ Review
→ Structured Import
→ Platform Display With Trust Label
→ Refresh / Company Validation Loop
```

## Source Capture Requirements

For every source, track:

- Company / Brand / Operator / Market
- Source Title
- Source Type
- Source URL or File Path
- Source Region
- Source Year / Publication Date
- Downloaded Date
- Relevance Note
- Trust Level
- Used In
- Review Status

## Validation Levels

Use:

- Company Validated
- Company Published
- Source-Informed
- Owner-Provided
- AI-Assisted
- Needs Review
- Stale / Refresh Needed
- Do Not Use

See [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md) for definitions.

## Important Rules

- AI-generated content should not go directly into the platform as fact.
- AI should extract, structure, summarize, and flag.
- Human/company validation is required before calling something validated.
- Regional relevance matters. CALA-specific sources are stronger for CALA than global materials.
- Source date matters.
- Conflicting sources should be flagged, not blended silently.
- AI inference should never override sourced facts.
- Company-validated data should not be overwritten automatically.

## Minimum Useful Metadata

Every intelligence asset should ideally include:

- Validation Status
- Usage Permission
- Source Type
- Source Date
- Source Region
- Last Reviewed Date
- Confidence Level
- Evidence Notes
- Missing Data Flags

## Related Documentation

- Governance: [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md)
- Extraction template: [CONTENT_EXTRACTION_TEMPLATE.md](./CONTENT_EXTRACTION_TEMPLATE.md)
- Status workflow: [DATA_STATUS_WORKFLOW.md](./DATA_STATUS_WORKFLOW.md)
- Partner intake (existing): [../partner-reference-material-collection-guide.md](../partner-reference-material-collection-guide.md)
- Partner sources (existing): [../partner-source-library-airtable-fields.md](../partner-source-library-airtable-fields.md)
