# Hard Enforcement Readiness — ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH=1

| Check | Result |
|-------|--------|
| New runs can certify via certifyAdpPeriod | YES |
| Hilton matched control CERTIFIED | YES (CERTIFIED) |
| Renaissance matched control CERTIFIED | YES (CERTIFIED) |
| Hilton/Ren EXACT_COMPARABLE | YES (EXACT_COMPARABLE) |
| Legacy reports remain accessible | YES (LEGACY grandfather in customer read) |
| Active customer report disappears risk | LOW — only explicit QA_* blocked |
| Hotel-specific certification bypasses | 0 |
| Post-fix engine FAIL count | 0 |

## SAFE_TO_ENABLE
**YES**

Enable in .env when ready; publish path already supports the gate.
