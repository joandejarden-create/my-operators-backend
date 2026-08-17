# Operator Setup Deal-Linked OAS Validation

Generated: 2026-06-02

## Validation summary

1. **Deal ID tested:** `recIeGRZP21udmTnt`
2. **Operator ID tested:** requested `recBVEgtm8cS96mu7`; validated with linked operator `recQ6Cf8O2z0tiqBz`
3. **How operator linkage was confirmed:** `buildOperatorAlignmentCompaniesSnapshot()` returned `companiesForConsideration` with 9 operators; requested operator was not present, selected operator was present by exact `operatorId` match.
4. **APIs/routes/modules tested:**
   - `lib/operator-alignment-company-utils.js` (`buildOperatorAlignmentCompaniesSnapshot`)
   - `lib/operator-alignment-profile-utils.js` (`buildOperatorAlignmentProfileSnapshot`)
   - `api/my-deals.js` (`fetchDealScoringContext`)
   - `api/lib/operator-setup-new-base-read.js` (`loadNewBaseOperatorBundle`)
   - `GET /api/operator-alignment-snapshot/:dealId/profile` (auth-gated check)
   - `GET /api/operator-alignment-snapshot/:dealId/companies` (auth-gated check)
   - `GET /api/my-deals/:dealId/operator-match-score-breakdown`
   - `GET /api/intake/third-party-operators/:recordId`
   - `GET /api/operator-explorer/operator`
5. **Score result run 1:** `85.8`
6. **Score result run 2:** `85.8`
7. **Score stable:** Yes (`85.8 == 85.8`) in both module and HTTP score checks.
8. **Operator Alignment Snapshot passed:** Yes (module-level profile + companies snapshot pass). HTTP OAS routes returned `401` (expected auth gate in this runtime).
9. **Operator Alignment Score Breakdown passed:** Yes (`GET /api/my-deals/:dealId/operator-match-score-breakdown` returned `200` twice; stable `operatorScore`).
10. **serviceModelsSupported source:** Canonical (`serviceModelsSupported = ["Select-service","Boutique"]`), fallback not used (`primaryServiceModel = "Mixed"` retained as compatibility field).
11. **Fallback diagnostics appeared:** No.
12. **Unresolved fields appeared:** No.
13. **Product issue vs data issue:** No product issue observed. Remaining issue is data/cohort selection: requested operator `recBVEgtm8cS96mu7` is not linked in this deal’s `companiesForConsideration`.
14. **Decision recommendation:** **needs targeted cleanup** (deal/operator test fixture alignment), not a source-of-truth or scoring regression.

## Additional runtime safety confirmations

- Operator context read path where applicable: `new_base_bundle` (bundle loaded; detail endpoint `200`).
- Mock data detection: none.
- Legacy/fail-open/shadow behavior during this pass: none observed.
- Scoring code changed during validation: no (validation-only run, no implementation/commit).

## Notes

- This pass closes the runtime parity gap for a confirmed linked operator in the target deal.
- If required, a follow-up is to align the target deal cohort so `recBVEgtm8cS96mu7` is explicitly testable in `companiesForConsideration`.

