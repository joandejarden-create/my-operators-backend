# Choice Legacy Batch Pipeline — Choice Legacy Mini-Batch 2

Generated: 2026-07-07T09:22:57.969Z
Mode: **dry-run** · Batch: **mini-batch-2**

## Executive summary

| Metric | Value |
|--------|-------|
| Brands | 4 |
| Platform Ready | 4 |
| Blocked | 0 |
| Apply recommended | No |
| Next action | No action — batch Platform Ready |

### Current stage by brand

| Brand | Stage |
|-------|-------|
| Country Inn & Suites by Choice | Platform Ready |
| Radisson by Choice | Platform Ready |
| Radisson Individuals by Choice | Platform Ready |
| Radisson RED by Choice | Platform Ready |

### Apply command

```bash
npm run choice-legacy-batch-pipeline -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-pipeline
```

## Pipeline stages

| Stage | Status |
|-------|--------|
| sourcePackage | planned |
| urlCapture | planned |
| sourceStewardship | planned |
| extraction | planned |
| factStewardship | planned |
| governancePublish | planned |
| verification | undefined |

## Brands

### Country Inn & Suites by Choice

- Record: `recaayt9u7YYg8h7Y`
- Explorer: **Active** · Profile: **Platform Ready**
- Stage: **Platform Ready** · Platform Ready: **yes**
- Sources: undefined/2 approved
- Facts: 6 approved · 0 pending · 0 held
- Governance: **Company Published** · Chip: **AI-Assisted Profile** · Basis: **Company Materials**
- Rebuild needed: **No**
- Blockers: split_out:press_kit_uncertain_or_missing

### Radisson by Choice

- Record: `recywbx1YQSTCPqW1`
- Explorer: **Active** · Profile: **Platform Ready**
- Stage: **Platform Ready** · Platform Ready: **yes**
- Sources: undefined/3 approved
- Facts: 4 approved · 3 pending · 3 held
- Governance: **Company Published** · Chip: **AI-Assisted Profile** · Basis: **Company Materials**
- Rebuild needed: **No**
- Blockers: none

### Radisson Individuals by Choice

- Record: `recRyvM8OmLlDj9G7`
- Explorer: **Active** · Profile: **Platform Ready**
- Stage: **Platform Ready** · Platform Ready: **yes**
- Sources: undefined/3 approved
- Facts: 7 approved · 0 pending · 0 held
- Governance: **Company Published** · Chip: **AI-Assisted Profile** · Basis: **Company Materials**
- Rebuild needed: **No**
- Blockers: none

### Radisson RED by Choice

- Record: `recmKqo7M7mLZgRqQ`
- Explorer: **Active** · Profile: **Platform Ready**
- Stage: **Platform Ready** · Platform Ready: **yes**
- Sources: undefined/3 approved
- Facts: 5 approved · 3 pending · 3 held
- Governance: **Company Published** · Chip: **AI-Assisted Profile** · Basis: **Company Materials**
- Rebuild needed: **No**
- Blockers: none

## Does not do

- Rebuild Brand Explorer content or overwrite Brand Setup content fields
- Auto-approve unsafe or held facts
- Set Company Validated or Company Validation Date
- Weaken RHG/global safeguards for Choice/Americas brands
- Publish Source-Informed posture for official Choice company materials
- Duplicate existing sources or facts
- Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema