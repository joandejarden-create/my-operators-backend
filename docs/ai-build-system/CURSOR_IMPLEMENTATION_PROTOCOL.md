# Cursor Implementation Protocol

Specific instructions for Cursor when implementing Dealality changes.

## Cursor Operating Mode

For every build:

1. Inspect first.
2. Plan second.
3. Implement third.
4. Test fourth.
5. Summarize fifth.
6. Capture learnings sixth.

## Required Pre-Implementation Response

Before editing, Cursor should summarize:

- Relevant files found.
- Existing implementation pattern.
- Data/tables/fields involved.
- Potential risks.
- Proposed implementation plan.
- Files likely to be changed.
- Assumptions needing confirmation.

If task is low-risk, proceed after summarizing. If task is high-risk, ask for confirmation.

**High-risk indicators:** Airtable writes, schema changes, scoring logic, auth/access changes, company-validated data paths, exports, approval/workflow states.

## Implementation Rules

- Prefer small, reversible changes.
- Do not rewrite large files unnecessarily.
- Keep existing naming conventions (`dc_`, `api_`, `map_`, `val_`, `ui_` prefixes where used).
- Maintain Proper Case for short UI labels.
- Use sentence case for helper copy.
- Avoid adding new dependencies unless justified.
- Keep demo/admin behavior separate from production logic.
- Add graceful missing-data handling.
- Add source/confidence labels where content is AI-assisted or source-informed.
- Do not write to protected auth/business fields unless explicitly approved.
- Use central field mapping objects; do not scatter raw Airtable field names.
- Every data-driven UI must include loading, empty, error, and success states.
- No silent catch blocks — log or rethrow.
- Dry-run first for scripts with `--apply` or live upsert.

## Final Summary Format

At the end, provide:

1. **What changed** — brief description of behavior or docs impact.
2. **Files changed** — list of paths.
3. **Tests/checks run** — commands and outcomes.
4. **Risks or assumptions** — what could break or what was assumed.
5. **Documentation updated** — if any durable decisions were captured.
6. **Suggested next step** — one concrete follow-up if useful.

## Brand Explorer Tab Factory (mandatory)

When building or remediating Brand Explorer profiles, follow:

`docs/data-intelligence/brand-explorer-tab-factory-build-operation.md`

Do not treat “tab rendered” as complete. Audit and remediate field-by-field. `auditPass` requires zero fail findings. Parent-company umbrella sources must not dominate brand-specific sections.

## Related Documentation

- [AI_BUILD_PROTOCOL.md](./AI_BUILD_PROTOCOL.md)
- [DEALALITY_QA_CHECKLIST.md](./DEALALITY_QA_CHECKLIST.md)
- [CURSOR_PROMPTS.md](./CURSOR_PROMPTS.md)
- Process rules: [../../.cursor/rules/deal-capture-implementation-partner.mdc](../../.cursor/rules/deal-capture-implementation-partner.mdc)
- Brand Explorer Tab Factory: [../data-intelligence/brand-explorer-tab-factory-build-operation.md](../data-intelligence/brand-explorer-tab-factory-build-operation.md)
