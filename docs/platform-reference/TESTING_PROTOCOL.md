# Testing Protocol

> **Placeholder** — expand with consolidated test commands and PR gates.

## Purpose

This file will document how to test Dealality changes: unit tests, route auth tests, dry-run scripts, manual QA paths, and PR validation requirements.

## Current References

| Resource | Path |
|----------|------|
| PR validation matrix | [../dealality-pr-validation-matrix.md](../dealality-pr-validation-matrix.md) |
| PR check suggest script | `npm run dealality:pr-check-suggest` |
| QA checklist (product) | [../ai-build-system/DEALALITY_QA_CHECKLIST.md](../ai-build-system/DEALALITY_QA_CHECKLIST.md) |
| Content QA | [../data-intelligence/CONTENT_QA_CHECKLIST.md](../data-intelligence/CONTENT_QA_CHECKLIST.md) |
| Parallel dry-run experiment | [../dealality-parallel-dry-run-experiment.md](../dealality-parallel-dry-run-experiment.md) |

## Common Patterns (Today)

- **Route auth:** `test:batch*-route-auth` when touching API routes
- **Airtable writes:** always `--dry-run` before `--apply`
- **Operator setup saves:** `npm run test:operator-setup-new-base-save-coverage` when available
- **Brand explorer fixtures:** `npm run audit-choice-explorer-presentation-gaps` before PR
- **Master todo:** `npm run test:dealality-master-todo`

## What to Add Later

- Full npm script inventory by risk tier
- Required checks per change type (UI, API, schema, fixtures, GTM)
- Staging vs production verification steps
- Regression suites for snapshots and explorers

## Related

- [ARCHITECTURE_MAP.md](./ARCHITECTURE_MAP.md)
- [DATA_DICTIONARY.md](./DATA_DICTIONARY.md)
- Build protocol: [../ai-build-system/AI_BUILD_PROTOCOL.md](../ai-build-system/AI_BUILD_PROTOCOL.md)
