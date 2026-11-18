# Approved schema and field IDs

## Decision

Founder approved **GDI Hotel Research Opportunities** in base `appa2cE7FTRmIbB32`.

## Live result

- Mode: apply
- OK: true
- Table ID: `tblbkDj0vhQLfrssV`
- Primary field: `compositeKey` (= `hotelId::opportunityId`)
- Logical unique key: `(hotelId, opportunityId)` — enforced in adapter

## Fields

| Field | ID | Type |
|-------|----|------|
| compositeKey | `fldVM6kajedZYdrUP` | singleLineText |
| opportunityId | `fld40dEcrNbJugSUd` | singleLineText |
| hotelId | `fldJzxHB7WRam8VAO` | singleLineText |
| organizationName | `fldFsmbi8tjZ12M52` | singleLineText |
| researchLifecycleStatus | `fldrtbHBJqQnubb1i` | singleSelect |
| commercialEligibilityStatus | `fldPMbPzATvpOFol6` | singleSelect |
| demandThesis | `fldM70lM7HKeask79` | singleLineText |
| priority | `fldXHNMoxEBc0wqTe` | singleLineText |
| title | `fldEyJpjw5oSJ3tJa` | singleLineText |
| segment | `flderOrE13TK0TkTC` | singleLineText |
| sourceSummary | `fldrj3PIfmLBl2XjR` | multilineText |
| firstDiscoveredAt | `fldeC1v7mNXIARBq8` | singleLineText |
| firstDiscoveredRunId | `fldKSQKu9dapQh1zu` | singleLineText |
| lastResearchedAt | `fld3ETURnS7Aahc0Z` | singleLineText |
| lastMaterialChangeAt | `fldr52R55t5pwNqbn` | singleLineText |
| dgGeneratorId | `fld3WFMf1LpNv08ff` | singleLineText |
| researchPayloadJson | `fldyXr7Ga3G5P2M3g` | multilineText |
| schemaVersion | `fldmmFTbKVCXRJbSd` | singleLineText |

## Isolation

- Does **not** alter Group Demand Opportunities schema
- Does **not** create a new global organization table
- Does **not** change customer publication contracts
