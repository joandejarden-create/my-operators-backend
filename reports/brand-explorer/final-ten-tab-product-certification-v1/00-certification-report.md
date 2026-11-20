# Final 10-Tab Product Popup Certification V1

Generated: 2026-11-20T01:41:25.009Z
Base URL: http://127.0.0.1:8080
Acceptance URL: http://127.0.0.1:8080/app#/brand-explorer-combined
Git HEAD (workspace): a800e9cdcec2d9986e03d4eda7cd43603c14c793
Candidate version: phase2h-component-parity

## Candidate hashes

- Curio phase2h candidate SHA-256: `25748a0d574c37d8ed4dfb38a629cd4b23d9eeeff2c9f72a620ad8699ebef20d`
- Ascend phase2h candidate SHA-256: `f838e44f994a964c6eb83b1898e7e4d39912e02930e413cb5480ade817811fca`

## Classification

- Curio: **FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED**
- Ascend: **FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED**
- Combined: **FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED**

## Mode / mount

- curio: proposedModeOk=true popupVisible=true tabCount=10 brandMode=proposed openVia=already-open reopenOk=true
- ascend: proposedModeOk=true popupVisible=true tabCount=10 brandMode=proposed openVia=already-open reopenOk=true

## Founder lessons applied

- BE-QL-PRODUCT-POPUP-VISUAL-001
- BE-QL-LOCAL-PRODUCT-PARITY-001
- BE-QL-FOUNDER-PRODUCT-PREVIEW-001
- BE-QL-PROPERTY-IMAGE-INTEGRITY-001
- BE-QL-2H-001
- BE-QL-2H-A-001
- BE-QL-FINAL-TEN-TAB-PRODUCT-CERT-001

## Tab matrix (aggregate)

| Brand | Tab | Content | Styling | Images | Functionality | Parity | Quality | Status | Defects |
|---|---|---|---|---|---|---|---|---|---|
| curio | Overview | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| curio | Value to Owners | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| curio | Operating Model | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| curio | Owner Considerations | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| curio | Commercial Engine | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| curio | Economics & Obligations | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| curio | Loyalty Program | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| curio | Footprint & Growth | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| curio | Brand Materials | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| curio | Dealality Insight | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| ascend | Overview | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| ascend | Value to Owners | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| ascend | Operating Model | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| ascend | Owner Considerations | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| ascend | Commercial Engine | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| ascend | Economics & Obligations | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| ascend | Loyalty Program | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| ascend | Footprint & Growth | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| ascend | Brand Materials | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |
| ascend | Dealality Insight | PASS | PASS | PASS | PASS | PASS | PASS | PASS | — |

## Royal Palm (PR #476 survival)

- curio: hilton=true travelWeekly=false alt=`Royal Palm Galápagos | Hotel & Guest Villas` src=`https://stories-editor.hilton.com/wp-content/uploads/2022/07/Royal-Palm-Galapagos-Curio-Collection-by-Hilton_Aerial_Courtesy-of-Royal-Palm-Galapagos-Curio-Collection-by-Hilton.jpg`
- ascend: Royal Palm card not present in footprint (expected for Ascend)

## Outstanding blockers

- None recorded.

## Notes

- Auth uses mocked `/api/me` admin + synthetic shell token (same pattern as appshell preview auto cert).
- Image naturalWidth=0 in headless with a verified Hilton/Airtable hotel src is recorded as BLOCKED (hotlink/network), not FAIL, unless masthead identity is wrong.
- Airtable writes: none.
- Publication status remains NOT_APPROVED pending separate Gate 2 / writeback.
- Permanent publication process requires BOTH full product-quality certification AND local product parity.
