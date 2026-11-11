# Primary export closure

Reviewed against `main` at `be179b441`. No compatibility shim was added.

| Export | Result |
| --- | --- |
| `detectPropertyMention` | Present and exported from `response-parser.js`. |
| `hasCustomerFacingCompetitor` | Present as an internal function in `competitor-present-gaps.js` and used by the gap calculation. |
| `isProseFragmentCompetitor` | Present as an internal function in `competitor-name-resolution.js` and used by name resolution. |
| `ADP_ATOMIC_REPORT_VERSION_LOADING_V1` | Present and exported from `adp-atomic-bpp-pairing-v1.js`. Callers import it. |
| `WATERSTONE_SOUTH_FLORIDA_REGISTRY` | Present as the reason string in `adp-competitive-entity-resolve-v1.js`. |
| `scopeObservationsToScenarios`, `inventoryAdpClientShareRegistry`, `isProductionEligibleAdpShareRow`, `OPTIONAL_EXECUTIVE_METRICS_UNPROMPTED_VERSION`, `CHARLOTTE_MARRIOTT_CITY_CENTER_STABILIZED_CORE_IDS`, `getAdpPublishedTestOnlySeedDir`, `getAdminVerifyCustomerAccess`, `getAdpAdminRuntimeIdentity` | Not called by a current main workflow. Left excluded. |
| Events zero-paid orchestrator | Source is on disk and untracked. Its mock fixture file does not exist anywhere under the Dealality trees that were searched. It was not committed. |

The events orchestrator is the remaining source dependency. The excluded exports stay historical until a live workflow imports them.
