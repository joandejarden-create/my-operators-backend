# Brand Explorer Complete Build — Batch Queue

- Generated: 2026-07-10T14:14:58.818Z
- Mode: **dry-run**
- Brands: **radisson-individuals-by-choice, suburban-studios, woodspring-suites, everhome-suites**
- Max concurrency: **1**
- Airtable modified: **no**
- Company Validated untouched: **yes**
- Brand target resolver: **v28C**

## Target resolution
| Input | Resolved name | Record | Slug | Source |
| --- | --- | --- | --- | --- |
| `radisson-individuals-by-choice` | Radisson Individuals by Choice | `recRyvM8OmLlDj9G7` | `radisson-individuals-by-choice` | expansion_backlog |
| `suburban-studios` | Suburban Studios | `reclcjg5Foa9Vs5TC` | `suburban-studios` | expansion_backlog |
| `woodspring-suites` | WoodSpring Suites | `recsOd51NzRPYsMko` | `woodspring-suites` | expansion_backlog |
| `everhome-suites` | Everhome Suites | `recqkkrsevi4r9ibj` | `everhome-suites` | expansion_backlog |

## Aggregate
- Ready: 0
- Almost ready: 3
- Blocked: 0
- Missing source evidence: 4
- Needing fact approval: 1
- Needing UI/copy repair: 4
- Carryover risk: 3
- Safe for apply-approved: 0
- Not safe for apply: 4

### Ready
- none

### Almost ready
- Radisson Individuals by Choice (`radisson-individuals-by-choice`)
- Suburban Studios (`suburban-studios`)
- Everhome Suites (`everhome-suites`)

### Blocked
- none

### Missing source evidence
- Radisson Individuals by Choice (`radisson-individuals-by-choice`)
- Suburban Studios (`suburban-studios`)
- WoodSpring Suites (`woodspring-suites`)
- Everhome Suites (`everhome-suites`)

### Needing fact approval
- Everhome Suites (`everhome-suites`)

### UI/copy repair
- Radisson Individuals by Choice (`radisson-individuals-by-choice`)
- Suburban Studios (`suburban-studios`)
- WoodSpring Suites (`woodspring-suites`)
- Everhome Suites (`everhome-suites`)

### Carryover risk
- Suburban Studios (`suburban-studios`)
- WoodSpring Suites (`woodspring-suites`)
- Everhome Suites (`everhome-suites`)

### Safe for apply-approved
- none

### Not safe for apply
- Radisson Individuals by Choice (`radisson-individuals-by-choice`)
- Suburban Studios (`suburban-studios`)
  - Wrong-brand copy/carryover risk detected
  - Required sections are not ready
  - Required image slots are still missing
  - Source governance is insufficient for governed apply
- WoodSpring Suites (`woodspring-suites`)
  - Wrong-brand copy/carryover risk detected
  - Required sections are not ready
  - Required image slots are still missing
  - Source governance is insufficient for governed apply
- Everhome Suites (`everhome-suites`)
  - Wrong-brand copy/carryover risk detected
  - 3 pending explorer fact(s) would need approval before apply
  - Required sections are not ready
  - Source governance is insufficient for governed apply

## Per-brand summary
- **Radisson Individuals by Choice** (`radisson-individuals-by-choice`): contract 100, Final QA almost_ready, active-profile no, next `none_active_profile_ready`
- **Suburban Studios** (`suburban-studios`): contract 63, Final QA almost_ready, active-profile no, next `row creation writer`
- **WoodSpring Suites** (`woodspring-suites`): contract 63, Final QA not_ready, active-profile no, next `row creation writer`
- **Everhome Suites** (`everhome-suites`): contract 88, Final QA almost_ready, active-profile no, next `row creation writer`

## Queue command
```bash
npm run brand-explorer-complete-build -- --brands radisson-individuals-by-choice,suburban-studios,woodspring-suites,everhome-suites --dry-run --target-quality active-profile
```