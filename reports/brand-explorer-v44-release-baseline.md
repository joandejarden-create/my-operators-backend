# v44 Brand Explorer Release Baseline

Generated: 2026-07-21T15:34:27.590Z

Read-only freeze of golden `active_profile_ready` brands. No Airtable writes. No unlock. No Company Validated changes.

## Summary

- Released golden: **3**
- Incomplete routed: **4**
- Regression: **PASS** (0 failure(s))
- Released all active: **true**
- Incomplete all locked: **true**
- Preferred next batch: **A**

## Released golden baseline

| Brand | State ready | Full profile | Gallery | Property | Tabs | Company Validated | Release fields | Golden | Ext lock |
|---|---|---|---|---|---|---|---|---|---|
| everhome-suites | true | true | 6 | 4 | 10 | false (untouched=true) | true | true | true |
| kimpton | true | true | 6 | 5 | 10 | false (untouched=true) | true | true | true |
| radisson-individuals-by-choice | true | true | 6 | 3 | 10 | false (untouched=true) | true | true | true |

## Frozen minimums (must not regress)

- **everhome-suites**: state=`active_profile_ready` gallery≥6 property≥4 tabs≥10 companyValidated=false
- **kimpton**: state=`active_profile_ready` gallery≥6 property≥5 tabs≥10 companyValidated=false
- **radisson-individuals-by-choice**: state=`active_profile_ready` gallery≥6 property≥3 tabs≥10 companyValidated=false

## Regression checks

All regression checks passed.

## Guardrails

- No Airtable writes
- No active release / unlock
- No Company Validated changes
- No content changes to released brands
- Incomplete brands remain locked
- Process new brands only when OS-routed
