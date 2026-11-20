/**
 * Airtable strategic selection persistence contract (design only — no writes).
 * Founder gate required before schema mutation or production writes.
 */

export const ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1 =
  "ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1";

/**
 * Reuse-first proposal. Full narrative:
 * docs/ai-demand-positioning/adp-strategic-attribute-selection-airtable-persistence-proposal-v1.md
 */
export const STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL = Object.freeze({
  contractVersion: ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1,
  status: "PROPOSAL_AWAITING_FOUNDER_APPROVAL",
  productionWritesAuthorized: false,
  layers: {
    verifiedFacts: {
      owner: "Hotel ADP Attributes",
      tableId: "tblMA6v0HAmsY9ImW",
      baseId: "appa2cE7FTRmIbB32",
      role: "Layer A facts only — do not treat Used in ADP? as strategic selection SoT",
    },
    strategicSelection: {
      recommendedApproach: "OPTION_B_CONFIG_PLUS_LINES",
      optionsEvaluated: [
        {
          id: "OPTION_A_EXTEND_HOTEL_ADP_ATTRIBUTES",
          summary:
            "Add RG Priority / Selection Status fields on Hotel ADP Attributes rows",
          risk: "Collapses Layer A facts with Layer B selection; harder versioning of full configuration",
          recommended: false,
        },
        {
          id: "OPTION_B_CONFIG_PLUS_LINES",
          summary:
            "Thin ADP Priority Attribute Configuration (header) + ADP Priority Attribute Lines (selected keys)",
          risk: "Two small tables — but preserves fact vs selection separation and version history",
          recommended: true,
        },
        {
          id: "OPTION_C_DECISIONS_ONLY",
          summary: "Store selection snapshots as Decision evidence JSON",
          risk: "Poor Airtable editability for hotel stewards; weak row-level enable/disable UX",
          recommended: false,
        },
      ],
      logicalFields: [
        "canonicalHotelId (HPC rec…)",
        "adpPropertyId",
        "governedAttributeKey",
        "sourceFactRefs / evidence refs",
        "suggestedOrSelectedStatus",
        "verificationStatus (linked/mirrored, not duplicated fact body)",
        "measurementEligibility",
        "strategicPriority (management only — not RG weights)",
        "approvalStatus",
        "approverSteward",
        "effectiveDate",
        "configurationVersion",
        "changeHistory / supersedesVersion",
        "researchPeriodReferences",
        "selectionRationale",
      ],
    },
    researchPeriodSnapshot: {
      owner: "Immutable period / report artifacts (FS) + optional AT index refs",
      role: "Pin approved selection version + inventory hash used for measurement",
    },
  },
  migrationPolicy: {
    fixtureBaselineImportStatus: "REVIEW_REQUIRED",
    autoApproveFixtureSelections: false,
    note: "Existing fixture attributes[] migrate as review-required, not newly APPROVED configurations",
  },
  persistenceGate: {
    fullE2eRequiresAirtableApprovedSelection: true,
    fixtureOnlyBlocksClassification: "FULL_E2E_VALIDATED",
    phase1bClassification: "PERSISTENCE_DESIGNED",
  },
});
