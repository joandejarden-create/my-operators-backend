# Capability gap matrix

| Capability | Classification | Implemented | Invoked | Working | Missing integration | Correction |
|---|---|---|---|---|---|---|
| Fourteen-segment discovery (Exa) | PRESERVED_AND_ACTIVE | true | true | true | — | — |
| Deep research (Exa + ORS) | PRESERVED_AND_ACTIVE | true | true | true | — | — |
| Contact research pass | PRESERVED_AND_ACTIVE | true | true | true | — | — |
| Named contact serialization to processed/scorecard | PRESENT_BUT_NOT_INVOKED | true | false | false | processed-row contact fields + scorecard namedContact | Ship processed-row hydration (this PR) |
| Paid Surfe enrichment | SUPERSEDED | true | false | true | — | Keep Surfe UI-only; do not reintroduce into chain |
| Scaling Companies specialist buyer intel | PARTIALLY_MIGRATED | true | false | true | Optional segment-specialized buyer intel hook for scaling | Defer — reuse restore pass first; evaluate scaling hook next |
| Commercial qualification / sales-ready | PRESERVED_AND_ACTIVE | true | true | true | — | Do not weaken gates; improve demand thesis quality separately |
| Drawer generation | PRESERVED_AND_ACTIVE | true | true | true | — | — |
| Airtable research persistence | PRESERVED_AND_ACTIVE | true | true | true | — | Keep founder-gated apply |
| Universal PS1 FullRun → canonical Full E2E | PRESERVED_AND_ACTIVE | true | true | true | — | — |

Full JSON: `CAPABILITY_GAP_MATRIX.json`.
