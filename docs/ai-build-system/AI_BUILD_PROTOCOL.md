# Dealality AI Build Protocol

How AI (Cursor, ChatGPT, and similar tools) should participate in the Dealality build process.

## Standard Build Flow

1. Understand the objective.
2. Read relevant docs.
3. Inspect existing implementation.
4. Summarize current state before editing.
5. Identify assumptions and risks.
6. Create an implementation plan.
7. Implement in small phases.
8. Avoid unrelated changes.
9. Add or update tests where practical.
10. Run available checks.
11. Provide changed files summary.
12. Capture durable learnings.
13. Update relevant documentation.

## Required Context Before Major Builds

Cursor/AI should read:

- [DEALALITY_PRODUCT_CONSTITUTION.md](./DEALALITY_PRODUCT_CONSTITUTION.md)
- [BUILD_DECISIONS.md](./BUILD_DECISIONS.md)
- [NAMING_AND_COPY_GUIDE.md](./NAMING_AND_COPY_GUIDE.md)
- [FEATURE_BRIEF_TEMPLATE.md](./FEATURE_BRIEF_TEMPLATE.md), when defining a feature
- [DEALALITY_QA_CHECKLIST.md](./DEALALITY_QA_CHECKLIST.md), before finalizing
- [../data-intelligence/DATA_VALIDATION_PROTOCOL.md](../data-intelligence/DATA_VALIDATION_PROTOCOL.md), when data/content is involved
- [../data-intelligence/INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md), when platform intelligence is involved

Also read when applicable:

- [../../AGENTS.md](../../AGENTS.md) — repo layout, schema authority, common mistakes
- [../platform-reference/TESTING_PROTOCOL.md](../platform-reference/TESTING_PROTOCOL.md) — PR and test expectations
- Matching `docs/*-airtable-fields.md` before any Airtable read/write

## Rules

- Do not invent Airtable fields.
- Do not overwrite validated data.
- Do not change unrelated files.
- Do not bypass role/access assumptions.
- Do not hardcode demo logic unless explicitly requested.
- Do not create duplicate scoring systems without clear justification.
- Do not use "validated" unless the validation level supports it.
- Do not turn Dealality into a generic marketplace or lead list.
- Do not use Wized.
- Use Zapier, GitHub, and Railway as preferred workflow/deployment ecosystem unless specifically changed later.
- Preserve existing Deal Readiness Snapshot, Brand Alignment Snapshot, Operator Alignment Snapshot, Brand Explorer, and Operator Explorer behavior unless the task explicitly modifies them.
- Brand Explorer setup must follow the Tab Factory sequence (source → lens → tab generation → field validation → provenance → rendered audit → remediation → image distinctiveness → golden benchmark → founder review → active release). See `docs/data-intelligence/brand-explorer-tab-factory-build-operation.md`.

## Learning Capture

At the end of each meaningful task, identify:

- New product decisions.
- New naming/copy rules.
- New reusable implementation patterns.
- New data governance rules.
- New testing needs.
- New known risks.

Update the correct document only if the lesson is durable and useful for future builds.

Avoid documentation bloat.

## Related Documentation

- Cursor-specific steps: [CURSOR_IMPLEMENTATION_PROTOCOL.md](./CURSOR_IMPLEMENTATION_PROTOCOL.md)
- Reusable prompts: [CURSOR_PROMPTS.md](./CURSOR_PROMPTS.md)
- PR validation matrix: [../dealality-pr-validation-matrix.md](../dealality-pr-validation-matrix.md)
