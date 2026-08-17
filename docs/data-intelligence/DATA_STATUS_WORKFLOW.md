# Data Status Workflow

Simple workflow for moving intelligence assets from capture to platform use.

## Data Statuses

- Not Started
- Sources Collected
- AI Extracted
- Needs Review
- Reviewed
- Platform Ready
- Sent for Company Validation
- Company Validated
- Refresh Needed
- Archived

## Workflow

Primary path:

```text
Not Started
→ Sources Collected
→ AI Extracted
→ Needs Review
→ Reviewed
→ Platform Ready
→ Sent for Company Validation
→ Company Validated
```

Alternate paths:

```text
Platform Ready → Refresh Needed → Needs Review
Needs Review → Archived
AI Extracted → Do Not Use
```

## Status Rules

- **AI Extracted** is not Platform Ready.
- **Reviewed** is not the same as Company Validated.
- **Platform Ready** means it can be used according to its Usage Permission.
- **Company Validated** requires direct confirmation.
- **Refresh Needed** should limit or downgrade usage if confidence is affected.
- **Archived** should not power active platform outputs.

## Status vs Validation Level

| Workflow Status | Typical Validation Level |
|-----------------|--------------------------|
| AI Extracted | AI-Assisted or Needs Review |
| Needs Review | Needs Review |
| Reviewed | Source-Informed or Company Published |
| Platform Ready | Per assigned validation level |
| Company Validated | Company Validated |
| Refresh Needed | Stale / Refresh Needed |
| Archived | Do Not Use (if retired) |

See [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md) for validation and usage permission definitions.

## Related Documentation

- [DATA_VALIDATION_PROTOCOL.md](./DATA_VALIDATION_PROTOCOL.md)
- [CONTENT_QA_CHECKLIST.md](./CONTENT_QA_CHECKLIST.md)
