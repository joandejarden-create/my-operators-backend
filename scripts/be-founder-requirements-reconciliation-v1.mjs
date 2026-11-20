#!/usr/bin/env node
/**
 * Exhaustive founder requirements reconciliation — generates Part G deliverables.
 * Does NOT regenerate brand content. Gate 2 remains blocked.
 */
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";

const root = process.cwd();
const outDir = join(root, "reports/brand-explorer/founder-requirements-reconciliation");
mkdirSync(outDir, { recursive: true });

const gate1Approved = existsSync(
  join(root, "reports/brand-explorer/phase2h-a-renderer-parity/GATE1_FOUNDER_APPROVED.json"),
);
const gate1Evidence = existsSync(
  join(root, "reports/brand-explorer/phase2h-a-renderer-parity/00-payload-manifest.json"),
);

const STATUS = {
  NOT_CAPTURED: "NOT_CAPTURED",
  CAPTURED: "CAPTURED",
  CONTRACT_ENCODED: "CONTRACT_ENCODED",
  IMPLEMENTED: "IMPLEMENTED",
  AUTOMATED_TESTED: "AUTOMATED_TESTED",
  BROWSER_VERIFIED: "BROWSER_VERIFIED",
  FOUNDER_APPROVED: "FOUNDER_APPROVED",
  BLOCKED: "BLOCKED",
};

const contracts = {
  permanent: "configs/brand-explorer-permanent-content-standards-contract-v1.json",
  architecture: "configs/brand-explorer-founder-architecture-corrections-contract-v1.json",
  inventory: "configs/brand-explorer-canonical-subsection-inventory-v1.json",
  lessons: "configs/brand-explorer-founder-quality-lessons-registry-v1.json",
  parity: "configs/brand-explorer-subsection-parity-contract-v1.json",
};

function cov({ A = false, B = false, C = false, D = false, E = false, F = false, G = false } = {}) {
  return {
    documentationOnly: A && !B && !C,
    contract: B,
    productionCodeEnforces: C,
    pipelineSuppliesData: D,
    regressionTest: E,
    realAppDemonstrates: F,
    founderApproved: G,
  };
}

function req(partial) {
  return {
    originatingPhase: partial.originatingPhase || "multi",
    originatingDate: partial.originatingDate || "2026-11",
    sourceRef: partial.sourceRef || null,
    tab: partial.tab || null,
    section: partial.section || null,
    subsection: partial.subsection || null,
    requiredVisualBehavior: partial.requiredVisualBehavior || null,
    requiredContentResearchBehavior: partial.requiredContentResearchBehavior || null,
    canonicalDataSource: partial.canonicalDataSource || null,
    relevantCode: partial.relevantCode || [],
    existingImplementation: partial.existingImplementation || "NONE_OR_PARTIAL",
    permanentContractLocation: partial.permanentContractLocation || null,
    automatedTest: partial.automatedTest || null,
    browserValidationEvidence: partial.browserValidationEvidence || null,
    coverage: partial.coverage || cov({}),
    unresolvedIssues: partial.unresolvedIssues || [],
    disposition: partial.disposition || null,
    conflictId: partial.conflictId || null,
    ...partial,
  };
}

const checklist = [];

checklist.push(
  req({
    requirementId: "BE-REQ-001",
    checklistNo: 1,
    category: "typography_editorial",
    founderInstruction: "White section headings use proper Title Case.",
    originatingPhase: "permanent-standards / 2H-ARCH",
    originatingDate: "2026-11-19",
    sourceRef: "permanent standards + architecture corrections founder prompts",
    tab: "all",
    section: "all oe-section-title",
    requiredVisualBehavior: "Title Case white .oe-section-title",
    relevantCode: ["public/js/brand-explorer-atelier-from-api.js"],
    existingImplementation:
      "Hardcoded atelier section titles mostly Title Case; generated/candidate titles not universally Title-Cased by code.",
    permanentContractLocation: `${contracts.permanent}#typographyAndWriting.sectionHeadings`,
    automatedTest:
      "test:be-permanent-content-standards-v1 (contract integrity only — no rendered heading-case scanner yet)",
    browserValidationEvidence:
      "Gate1 Brand Snapshot title styles measured; full Title Case audit across tabs NOT complete",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: [
      "No production Title Case enforcer on generated titles",
      "No automated rendered heading-case gate",
      "Not founder-approved",
    ],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-002",
    checklistNo: 2,
    category: "typography_editorial",
    founderInstruction: "White card headings use proper Title Case.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    sourceRef: "founder architecture corrections §2",
    tab: "all",
    section: "cards",
    requiredVisualBehavior: "Title Case card headings",
    permanentContractLocation: `${contracts.architecture}#editorial.primaryHeadingsTitleCase`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Card heading Title Case not machine-enforced on candidates"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-003",
    checklistNo: 3,
    category: "typography_editorial",
    founderInstruction: "Purple and blue label hierarchy is preserved.",
    originatingPhase: "2F / 2H",
    originatingDate: "2026-11-18",
    sourceRef: "BE_PHASE2F_VISUAL_QUALITY_RCA / OE panels CSS",
    requiredVisualBehavior: "Preserve OE purple/blue label hierarchy colors",
    relevantCode: [
      "public/css/brand-explorer-atelier-oe-panels.css",
      "public/js/brand-explorer-atelier-from-api.js",
    ],
    existingImplementation:
      "Product CSS defines hierarchy; Gate1 measured white title color. Purple/blue label audit incomplete.",
    permanentContractLocation: `${contracts.permanent}#typographyAndWriting.sectionHeadings.useCanonicalHierarchyFontsWeightsSizesColors`,
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: false, F: false, G: false }),
    unresolvedIssues: [
      "No automated color hierarchy regression across all subsection labels",
      "Not founder-approved",
    ],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-004",
    checklistNo: 4,
    category: "typography_editorial",
    founderInstruction: "Bullets begin with capital letters.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.architecture}#editorial.bulletsBeginWithCapitalLetter`,
    automatedTest: "test:be-founder-architecture-corrections-v1 (differentiators fixture only)",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Capitalization not enforced across all bullet collections in pipeline"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-005",
    checklistNo: 5,
    category: "typography_editorial",
    founderInstruction: "No em dashes in generated customer-facing paragraphs.",
    originatingPhase: "permanent-standards",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#typographyAndWriting.editorialQuality.forbidEmDashInGeneratedCustomerFacingText`,
    relevantCode: ["lib/brand-explorer/be-permanent-content-standards-v1.js#textViolatesEditorialStandards"],
    automatedTest: "test:be-permanent-content-standards-v1",
    existingImplementation:
      "Detector helper exists; not wired as mandatory gate on all factory outputs universally.",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: true, F: false, G: false }),
    unresolvedIssues: [
      "Helper not applied as hard fail across generation pipeline outputs",
      "Live baseline may still contain em dashes from production content",
    ],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-006",
    checklistNo: 6,
    category: "typography_editorial",
    founderInstruction: "No nonbreaking-space artifacts or exposed HTML entities.",
    originatingPhase: "permanent-standards",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#typographyAndWriting.editorialQuality`,
    relevantCode: ["public/js/brand-explorer-atelier-from-api.js (uses &nbsp; as empty placeholder in DOM)"],
    existingImplementation:
      "Atelier intentionally inserts &nbsp; for empty cells; founder forbids exposed artifacts in customer-facing paragraphs — distinction needed.",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: [
      "Renderer empty-state &nbsp; vs forbidden visible entity artifacts not fully separated in gates",
    ],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-007",
    checklistNo: 7,
    category: "typography_editorial",
    founderInstruction: "No internal slot keys or generator instructions.",
    originatingPhase: "2G / permanent-standards",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#typographyAndWriting.forbiddenCustomerFacingPatterns`,
    relevantCode: [
      "lib/brand-explorer/be-phase2g-presentation-sanitize-v1.js",
      "lib/brand-explorer/be-phase2g-anti-padding-v1.js",
    ],
    automatedTest: "test:be-phase2g-parity-comparison-v1",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Sanitize clears content but Gate 2 refill blocked; customer profiles not founder-approved"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-008",
    checklistNo: 8,
    category: "typography_editorial",
    founderInstruction: "No generic numbered proof labels.",
    originatingPhase: "2G / 2H-ARCH",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.architecture}#subsections.proofPoints.forbidTitles`,
    automatedTest: "test:be-founder-architecture-corrections-v1 (ARCH_FAIL_PROOF_PLACEHOLDER_TITLES)",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Not applied as live Gate 2 population gate yet"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-009",
    checklistNo: 9,
    category: "typography_editorial",
    founderInstruction: "No artificial padding or repeated paragraphs.",
    originatingPhase: "2F rejection / 2G",
    originatingDate: "2026-11-18",
    sourceRef: "BE_PHASE2G_REPEATED_CONTENT_RCA.md; PR #453 founder-rejected",
    relevantCode: [
      "lib/brand-explorer/be-phase2g-anti-padding-v1.js",
      "lib/brand-explorer/be-phase2g-presentation-sanitize-v1.js",
    ],
    automatedTest: "test:be-phase2g-parity-comparison-v1",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Anti-padding empties without refill (depth regression); Gate 2 blocked"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-010",
    checklistNo: 10,
    category: "typography_editorial",
    founderInstruction: "Appropriate font weight, paragraph spacing, emphasis and visual density.",
    originatingPhase: "2F / 2H / 2H-A",
    originatingDate: "2026-11-19",
    relevantCode: ["public/css/brand-explorer-atelier-oe-panels.css"],
    browserValidationEvidence: "reports/brand-explorer/phase2h-a-renderer-parity/ (Brand Snapshot styles only)",
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Partial Gate1 style evidence; full density audit across tabs outstanding"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-011",
    checklistNo: 11,
    category: "owner_value",
    founderInstruction:
      "Where This Brand Creates the Most Value has three differentiated, research-backed scenarios with real hotel imagery.",
    originatingPhase: "2C–2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Overview",
    section: "Where This Brand Creates the Most Value",
    subsection: "overview.scenario.1-3",
    requiredVisualBehavior: "3 visual scenario cards with hotel imagery",
    requiredContentResearchBehavior: "Differentiated research-backed scenarios; verified hotel photos",
    canonicalDataSource: "presentation overview.scenario.* + verified property imagery",
    relevantCode: [
      "public/js/brand-explorer-atelier-from-api.js (scenario-card--visual)",
      "lib/partner-intelligence/brand-explorer-gallery-selection.js",
    ],
    existingImplementation:
      "Renderer supports 3 visual cards; Gate1 Curio payload has 3 scenario titles; reconstructed candidates historically thin / imagery gaps (Ascend blocker in 2F).",
    permanentContractLocation: `${contracts.architecture}#subsections.whereThisBrandCreatesTheMostValue`,
    automatedTest: "cardinality / visual quality tests partial",
    browserValidationEvidence: "Gate1 screenshots cover Overview partially; founder not approved",
    status: STATUS.BLOCKED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: [
      "Gate2 content reconstruction blocked",
      "Research-backed differentiation not validated on candidates",
      "Ascend imagery historically blocked",
    ],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-012",
    checklistNo: 12,
    category: "owner_value",
    founderInstruction:
      "Why Value Is Strongest uses bold Title Case introductory phrases followed by a colon and full explanatory sentences.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Overview",
    subsection: "overview.why_value",
    permanentContractLocation: `${contracts.architecture}#subsections.whyValueIsStrongest`,
    relevantCode: ["public/js/brand-explorer-atelier-from-api.js (why_value bullets)"],
    existingImplementation:
      "Renderer renders bullet list; does NOT enforce Bold Title Case Insight: sentence format in generation or DOM.",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: [
      "Format not implemented in composer/renderer",
      "min 5 insights not gated on product output",
    ],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-013",
    checklistNo: 13,
    category: "owner_value",
    founderInstruction:
      "Key Differentiators has exactly four meaningful bullets in each canonical category (Experience & Identity; Commercial & Distribution).",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Overview",
    subsection: "overview.differentiators.*",
    relevantCode: ["public/js/brand-explorer-atelier-from-api.js (pads to 4)"],
    existingImplementation:
      "Renderer pads/truncates to 4 per column; quality of bullets not enforced; Gate1 Curio differentiator bodies empty in payload inventory.",
    permanentContractLocation: `${contracts.architecture}#subsections.keyDifferentiators`,
    automatedTest: "test:be-founder-architecture-corrections-v1 failing fixture",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: [
      "Padding empty bullets fails founder substance rule",
      "Pipeline must supply 4+4 meaningful bullets",
    ],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-014",
    checklistNo: 14,
    category: "owner_value",
    founderInstruction: "Proof Points contain substantive evidence and properly formatted headings.",
    originatingPhase: "2G / 2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Overview",
    subsection: "overview.proof.*",
    permanentContractLocation: `${contracts.architecture}#subsections.proofPoints`,
    automatedTest: "ARCH_FAIL_PROOF_PLACEHOLDER_TITLES",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Gate2 research/population blocked", "Evidence sufficiency incomplete on candidates"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-015",
    checklistNo: 15,
    category: "owner_value",
    founderInstruction: "Value Creation Scenarios preserve full content and card styling.",
    originatingPhase: "2G / 2H",
    originatingDate: "2026-11-19",
    tab: "Value to Owners",
    subsection: "valueOwners.scenario.1-4",
    relevantCode: [
      "public/js/brand-explorer-atelier-from-api.js scenarioDetailCard",
      "npm run brand-explorer-value-creation-scenarios-audit",
    ],
    permanentContractLocation: `${contracts.architecture}#subsections.valueCreationScenarios`,
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Candidate depth historically thinner than baseline; Gate2 blocked"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-016",
    checklistNo: 16,
    category: "owner_value",
    founderInstruction:
      "What the Owner Is Really Buying distinctly explains what guests receive and what owners receive.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Value to Owners",
    section: "What the Owner Is Really Buying",
    relevantCode: [
      "public/js/brand-explorer-atelier-from-api.js ~L2627 — currently single Overview detail card",
    ],
    existingImplementation:
      "CRITICAL GAP: live renderer shows single Overview card; dual Guest/Owner propositions not implemented in product code.",
    permanentContractLocation: `${contracts.architecture}#subsections.whatTheOwnerIsReallyBuying`,
    automatedTest: "ARCH_FAIL_OWNER_BUYING_OUTCOMES_ONLY",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: true, F: false, G: false }),
    unresolvedIssues: [
      "Product renderer does not yet render dual propositions",
      "Cannot be FOUNDER_APPROVED until implemented + browser verified",
    ],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-017",
    checklistNo: 17,
    category: "owner_value",
    founderInstruction: "Guest value must explain the actual brand experience.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Value to Owners",
    subsection: "What the Guest Receives",
    permanentContractLocation: `${contracts.architecture}#subsections.whatTheOwnerIsReallyBuying.requiredPropositions[0]`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Depends on BE-REQ-016 implementation"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-018",
    checklistNo: 18,
    category: "owner_value",
    founderInstruction:
      "Owner value must explain commercial, operating, affiliation, investment and development implications.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Value to Owners",
    subsection: "What the Hotel Owner Receives",
    permanentContractLocation: `${contracts.architecture}#subsections.whatTheOwnerIsReallyBuying.requiredPropositions[1]`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Depends on BE-REQ-016 implementation"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-019",
    checklistNo: 19,
    category: "lifecycle_ops",
    founderInstruction:
      "Support Across the Lifecycle preserves the complete founder-required stage structure (founder requested 7; live baseline is 6).",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Value to Owners",
    section: "Support Across the Lifecycle",
    requiredVisualBehavior: "Ordered timeline cards",
    existingImplementation: "Production TIMELINE EXACT 6 stages implemented and rendered.",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.supportAcrossTheLifecycle`,
    automatedTest: "test:be-permanent-content-standards-v1 + ARCH_FAIL_LIFECYCLE_INVENTED_SEVENTH",
    browserValidationEvidence: "Gate1 mount shows 6 phases",
    status: STATUS.BLOCKED,
    disposition:
      "CONFLICT_OPEN — retain production EXACT 6 baseline; requested 7-stage enhancement OPEN awaiting founder decision; do not invent stage",
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: [
      "Founder decision on target design (6 vs 7) unresolved",
      "Stage body research depth for Gate2 outstanding",
    ],
    conflictId: "BE-CONFLICT-LIFECYCLE-6-VS-7",
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-020",
    checklistNo: 20,
    category: "lifecycle_ops",
    founderInstruction: "Opening & Conversion Path contains the five standard populated stages.",
    originatingPhase: "permanent-standards / 2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Economics & Obligations",
    section: "Opening & Conversion Path",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.openingAndConversionPath`,
    relevantCode: ["public/js/brand-explorer-atelier-from-api.js ECON_OPENING_STEPS"],
    existingImplementation: "Renderer EXACT 5; Gate1 Curio has populated step titles/bodies.",
    automatedTest: "test:be-permanent-content-standards-v1",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Candidate regeneration depth not Gate2-approved", "Not founder-approved"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-021",
    checklistNo: 21,
    category: "lifecycle_ops",
    founderInstruction: "Operating Model contains every canonical subsection.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Operating Model",
    subsection: "operations.model.* (12 fields)",
    permanentContractLocation: `${contracts.architecture}#subsections.operatingModel`,
    existingImplementation:
      "Renderer grids 12 fields; Gate1 payload has 12 slot keys (many empty titles/bodies in inventory dump).",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: false, F: false, G: false }),
    unresolvedIssues: [
      "Substantive population incomplete on reconstructed candidates",
      "No dedicated failing fixture for empty ops fields yet",
    ],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-022",
    checklistNo: 22,
    category: "lifecycle_ops",
    founderInstruction: "Third-Party Operator Compatibility has the full operator fit and capability analysis.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Operating Model",
    section: "Third-Party Operator Compatibility",
    permanentContractLocation: `${contracts.architecture}#subsections.thirdPartyOperatorCompatibility`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Depth vs live production not validated for reconstructed content"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-023",
    checklistNo: 23,
    category: "lifecycle_ops",
    founderInstruction: "Brand Standards has the proper, substantive multirow table.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Owner Considerations",
    section: "Standard Detail, Where Available",
    permanentContractLocation: `${contracts.architecture}#subsections.brandStandardsTable`,
    automatedTest: "ARCH_FAIL_STANDARDS_SINGLE_DILIGENCE_ROW",
    existingImplementation:
      "Renderer supports standards.requirement rows; Curio Gate1 has 7 rows; single diligence-row fails fixture.",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: [
      "Reconstructed candidates may collapse depth",
      "Not browser-verified for Gate2 candidates",
    ],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-024",
    checklistNo: 24,
    category: "lifecycle_ops",
    founderInstruction:
      "Training, QA, systems, staffing, reporting and brand involvement are meaningfully addressed.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Operating Model",
    permanentContractLocation: `${contracts.architecture}#subsections.operatingModel.canonicalFields`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Substance gates per field not implemented"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-025",
    checklistNo: 25,
    category: "lifecycle_ops",
    founderInstruction: "Term, Renewal & Exit Risk contains substantive contractual and owner-risk coverage.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Economics & Obligations",
    section: "Term, Renewal & Exit Risk",
    permanentContractLocation: `${contracts.architecture}#subsections.termRenewalExitRisk`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Must not invent agreement terms; research depth Gate2"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-026",
    checklistNo: 26,
    category: "commercial",
    founderInstruction:
      "Commercial KPI cards use concise meaningful labels, actual values when available and no internal language.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Commercial Engine",
    permanentContractLocation: `${contracts.architecture}#subsections.commercialEngine`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["KPI population quality not Gate2 validated"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-027",
    checklistNo: 27,
    category: "commercial",
    founderInstruction: "Commercial subsections preserve canonical styling and hierarchy.",
    originatingPhase: "2H / 2H-A",
    originatingDate: "2026-11-19",
    relevantCode: [
      "public/js/brand-explorer-atelier-from-api.js",
      "public/css/brand-explorer-atelier-oe-panels.css",
    ],
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Depends on Gate1 founder approval of renderer parity"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-028",
    checklistNo: 28,
    category: "commercial",
    founderInstruction: "Demand Scenario View has at least six differentiated, substantive scenarios.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    tab: "Commercial Engine",
    section: "Demand Scenario View",
    permanentContractLocation: `${contracts.architecture}#subsections.demandScenarioView`,
    existingImplementation: "Gate1 Curio has 6 commercial.demand rows (quality/differentiation not validated).",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Differentiation + substance gates incomplete"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-029",
    checklistNo: 29,
    category: "commercial",
    founderInstruction:
      "Recent Momentum contains researched hotel openings, signings, conversions or other relevant dated developments.",
    originatingPhase: "2H-ARCH + prior momentum contract",
    originatingDate: "2026-11-19",
    tab: "Footprint & Growth",
    section: "Recent Momentum",
    permanentContractLocation: `${contracts.architecture}#subsections.recentMomentum`,
    relevantCode: ["lib/partner-intelligence/brand-explorer-recent-momentum-contract.js"],
    automatedTest: "ARCH_FAIL_MOMENTUM_GENERIC_DILIGENCE; existing momentum quality tests",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Freshness + research pipeline for Gate2 outstanding"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-030",
    checklistNo: 30,
    category: "commercial",
    founderInstruction: "Geographic Footprint does not reuse brand-wide HPC counts as regional figures.",
    originatingPhase: "2H RCA",
    originatingDate: "2026-11-19",
    sourceRef: "BE_PHASE2H_CONTENT_DEPTH_RCA.md",
    tab: "Footprint & Growth",
    relevantCode: ["lib/brand-explorer/be-phase2h-depth-composition-v1.js"],
    existingImplementation:
      "Depth composer withholds brand-wide totals from regional cards when no regional rollup — pipeline code exists; Gate2 blocked from applying to approved profiles.",
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: true, E: true, F: false, G: false }),
    unresolvedIssues: ["Not browser-verified on founder-approved candidate", "Gate2 blocked"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-031",
    checklistNo: 31,
    category: "commercial",
    founderInstruction:
      "Brand-level and regional inventory claims have appropriate source attribution and coverage labels.",
    originatingPhase: "2H",
    originatingDate: "2026-11-19",
    relevantCode: ["lib/brand-explorer/be-phase2h-depth-composition-v1.js"],
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: true, E: false, F: false, G: false }),
    unresolvedIssues: ["Coverage label browser verification outstanding"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-032",
    checklistNo: 32,
    category: "loyalty",
    founderInstruction:
      "Loyalty benefits contain six distinct, researched benefits, each with a meaningful description, for participating brands.",
    originatingPhase: "permanent-standards",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.loyaltyKeyBenefits`,
    automatedTest: "test:be-permanent-content-standards-v1",
    existingImplementation: "Cardinality EXACT 6 contracted; Gate1 Curio has 6 loyalty.proof cards.",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Research freshness/depth for Gate2 candidates; not founder-approved"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-033",
    checklistNo: 33,
    category: "loyalty",
    founderInstruction: "Loyalty member totals use the latest verified, dated, attributable research available.",
    originatingPhase: "permanent-standards",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.loyaltyKpis`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["No paid research; verification pipeline Gate2"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-034",
    checklistNo: 34,
    category: "loyalty",
    founderInstruction: "Program statistics distinguish parent loyalty program figures from brand-specific participation.",
    originatingPhase: "permanent-standards",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.loyaltyKpis.forbidConfusingParentNetworkWithBrandSpecific`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Enforcement in generation incomplete"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-035",
    checklistNo: 35,
    category: "loyalty",
    founderInstruction: "Loyalty tiers reflect the applicable real program structure.",
    originatingPhase: "permanent-standards",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.loyaltyTiers`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Tier accuracy/browser verification outstanding"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-036",
    checklistNo: 36,
    category: "loyalty",
    founderInstruction: "Loyalty cards preserve the intended layout, hierarchy and explanatory depth.",
    originatingPhase: "2H / 2H-A",
    originatingDate: "2026-11-19",
    relevantCode: ["public/js/brand-explorer-atelier-from-api.js loyalty builders"],
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Gate1 partial; founder approval pending"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-037",
    checklistNo: 37,
    category: "imagery_competition",
    founderInstruction: "Gallery images prioritize actual operating hotels.",
    originatingPhase: "permanent-standards / AGENTS.md gallery rules",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.imageGallery.sourcingHierarchy`,
    relevantCode: ["lib/partner-intelligence/brand-explorer-gallery-selection.js"],
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: true, E: true, F: false, G: false }),
    unresolvedIssues: ["Ascend imagery historically failed; Gate2 blocked"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-038",
    checklistNo: 38,
    category: "imagery_competition",
    founderInstruction: "Official renderings are prioritized for unopened hotels.",
    originatingPhase: "permanent-standards",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.imageGallery.sourcingHierarchy`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Pipeline priority needs Gate2 validation"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-039",
    checklistNo: 39,
    category: "imagery_competition",
    founderInstruction: "Generic logos are used only when no suitable hotel or project imagery exists.",
    originatingPhase: "2F / permanent-standards",
    originatingDate: "2026-11-18",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.imageGallery.forbiddenReplacements`,
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Ascend 2F blocked on logos-as-fill"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-040",
    checklistNo: 40,
    category: "imagery_competition",
    founderInstruction: "Images have verified property identity, source and suitable usage rights.",
    originatingPhase: "permanent-standards",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.imageGallery.perImageRequired`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Rights metadata incomplete in many fixtures"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-041",
    checklistNo: 41,
    category: "imagery_competition",
    founderInstruction: "Image failures and inappropriate substitutions block approval.",
    originatingPhase: "2F / permanent-standards",
    originatingDate: "2026-11-18",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.imageGallery.missingMandatoryPropertyImagesBlockApproval`,
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Not founder-approved"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-042",
    checklistNo: 42,
    category: "imagery_competition",
    founderInstruction: "Similar Brands contains separate competitor cards.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.architecture}#subsections.similarBrands`,
    automatedTest: "ARCH_FAIL_SIMILAR_BRANDS_COMBINED",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Gate2 content quality outstanding"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-043",
    checklistNo: 43,
    category: "imagery_competition",
    founderInstruction:
      "Every competitor has a meaningful explanation of positioning, similarities and owner trade-offs.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.architecture}#subsections.similarBrands.perCard`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Depth not enforced per competitor card"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-044",
    checklistNo: 44,
    category: "imagery_competition",
    founderInstruction: "Brand Materials retains actual hotel examples, cases and supporting materials.",
    originatingPhase: "2C–2H",
    originatingDate: "2026-11-19",
    tab: "Brand Materials",
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Case study depth historically regressed in candidates"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-045",
    checklistNo: 45,
    category: "rendering_comparison",
    founderInstruction: "Comparison Studio uses the actual customer-facing subsection renderer.",
    originatingPhase: "2H / 2H-A",
    originatingDate: "2026-11-19",
    sourceRef: "BE_PHASE2H_A_GATE1_RENDERER_PARITY_FOUNDER_REVIEW.md",
    relevantCode: [
      "public/js/brand-explorer-compare-studio.js",
      "public/js/brand-explorer-atelier-from-api.js",
    ],
    existingImplementation: "Gate1 rendererParity=1 uses BrandExplorerAtelierFromApi.renderTabInnerHtml",
    browserValidationEvidence: "reports/brand-explorer/phase2h-a-renderer-parity/",
    status: gate1Approved ? STATUS.FOUNDER_APPROVED : STATUS.BROWSER_VERIFIED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: gate1Approved }),
    unresolvedIssues: gate1Approved
      ? []
      : ["FOUNDER_APPROVED=false — awaiting Gate1 founder decision"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-046",
    checklistNo: 46,
    category: "rendering_comparison",
    founderInstruction:
      "Identical data produces equivalent presentation in the product and comparison interface.",
    originatingPhase: "2H-A",
    originatingDate: "2026-11-19",
    browserValidationEvidence: "reports/brand-explorer/phase2h-a-renderer-parity/02-browser-capture.json",
    existingImplementation:
      "Evidence shows identical payload hash + Brand Snapshot HTML length match; full ten-tab geometric parity not founder-approved.",
    status: STATUS.BROWSER_VERIFIED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Founder approval pending", "Equal-width geometry caveat documented"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-047",
    checklistNo: 47,
    category: "rendering_comparison",
    founderInstruction: "Corresponding subsections start directly opposite each other.",
    originatingPhase: "2G / 2H",
    originatingDate: "2026-11-19",
    relevantCode: ["public/js/brand-explorer-compare-studio.js"],
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Founder approval pending"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-048",
    checklistNo: 48,
    category: "rendering_comparison",
    founderInstruction: "One continuous scroll maintains alignment.",
    originatingPhase: "2G",
    originatingDate: "2026-11-19",
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Founder approval pending"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-049",
    checklistNo: 49,
    category: "rendering_comparison",
    founderInstruction: "Collections, tables, bullets, colors and images remain intact.",
    originatingPhase: "2H / 2H-A",
    originatingDate: "2026-11-19",
    status: STATUS.BROWSER_VERIFIED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Partial tab coverage in Gate1 evidence pack; founder approval pending"],
  }),
);

checklist.push(
  req({
    requirementId: "BE-REQ-050",
    checklistNo: 50,
    category: "rendering_comparison",
    founderInstruction: "Browser visual evidence is required before claiming parity.",
    originatingPhase: "2H-A / lessons governance",
    originatingDate: "2026-11-19",
    permanentContractLocation: contracts.lessons,
    browserValidationEvidence: "reports/brand-explorer/phase2h-a-renderer-parity/screenshots/",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Policy encoded; Gate1 still not founder-approved"],
  }),
);

const additional = [
  req({
    requirementId: "BE-REQ-051",
    category: "governance",
    founderInstruction:
      "Do not optimize for automated completion scores; optimize for professionally useful advisory profiles.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.architecture}#qualityGates`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Scoring systems still exist in phase reports; must not drive approval"],
  }),
  req({
    requirementId: "BE-REQ-052",
    category: "governance",
    founderInstruction: "Gate 2 content reconstruction blocked until Gate 1 founder approval.",
    originatingPhase: "2H-A",
    originatingDate: "2026-11-19",
    relevantCode: ["lib/brand-explorer/be-permanent-content-standards-v1.js#assertGate2Allowed"],
    automatedTest: "test:be-permanent-content-standards-v1",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: [],
  }),
  req({
    requirementId: "BE-REQ-053",
    category: "governance",
    founderInstruction:
      "No production Airtable writes, paid research, mass population, or publication without founder auth.",
    originatingPhase: "2C–current",
    originatingDate: "2026-11",
    permanentContractLocation: `${contracts.permanent}#governance`,
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: [],
  }),
  req({
    requirementId: "BE-REQ-054",
    category: "governance",
    founderInstruction:
      "PR #453 (Phase 2F) founder-rejected — do not merge as acceptance; preserve as history.",
    originatingPhase: "2G",
    originatingDate: "2026-11-19",
    sourceRef: "BE_PHASE2G_FOUNDER_ACCEPTANCE_PACKAGE.md",
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: false, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Ensure IC does not merge rejected #453 as current quality bar"],
  }),
  req({
    requirementId: "BE-REQ-055",
    category: "pipeline",
    founderInstruction:
      "Anti-padding must not clear-without-refill; compose distinct evidence-backed claims.",
    originatingPhase: "2H RCA",
    originatingDate: "2026-11-19",
    sourceRef: "BE_PHASE2H_CONTENT_DEPTH_RCA.md",
    relevantCode: ["lib/brand-explorer/be-phase2h-depth-composition-v1.js"],
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: true, E: true, F: false, G: false }),
    unresolvedIssues: ["Gate2 application blocked; founder not approved"],
  }),
  req({
    requirementId: "BE-REQ-056",
    category: "pipeline",
    founderInstruction:
      "Factory reconstruction must not use production Presentation as content source (baseline compare-only).",
    originatingPhase: "2C",
    originatingDate: "2026-11-18",
    sourceRef: "BE_PHASE2C_CURIO_RECONSTRUCTION_V1.md",
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: true, E: true, F: false, G: false }),
    unresolvedIssues: ["Independent candidates still not founder-approved"],
  }),
  req({
    requirementId: "BE-REQ-057",
    category: "owner_value",
    founderInstruction:
      "Do not reduce What the Owner Is Really Buying to Owner outcomes; guest and owner are not interchangeable.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.architecture}#subsections.whatTheOwnerIsReallyBuying`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Renderer gap BE-REQ-016"],
  }),
  req({
    requirementId: "BE-REQ-058",
    category: "lifecycle_ops",
    founderInstruction: "Do not invent a seventh lifecycle stage merely to satisfy a count.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#fixedCollections.supportAcrossTheLifecycle.forbidInventingSeventhStage`,
    automatedTest: "ARCH_FAIL_LIFECYCLE_INVENTED_SEVENTH",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Paired with open conflict BE-CONFLICT-LIFECYCLE-6-VS-7"],
  }),
  req({
    requirementId: "BE-REQ-059",
    category: "rendering_comparison",
    founderInstruction: "Preserve Gate 1 frozen Curio payload evidence; no content mutation in Gate 1.",
    originatingPhase: "2H-A",
    originatingDate: "2026-11-19",
    browserValidationEvidence:
      "public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json",
    status: STATUS.BROWSER_VERIFIED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Awaiting Gate1 founder approval"],
  }),
  req({
    requirementId: "BE-REQ-060",
    category: "governance",
    founderInstruction: "Never mark a lesson VERIFIED without actual browser evidence.",
    originatingPhase: "lessons governance",
    originatingDate: "2026-11-19",
    permanentContractLocation: contracts.lessons,
    automatedTest: "test:be-founder-architecture-corrections-v1 (verifiedImproperly=0)",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: [],
  }),
  req({
    requirementId: "BE-REQ-061",
    category: "commercial",
    founderInstruction: "Critical omissions block approval regardless of aggregate score.",
    originatingPhase: "2H-ARCH",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.architecture}#qualityGates.criticalOmissionsBlockRegardlessOfAggregateScore`,
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Needs wiring into unified approval gate"],
  }),
  req({
    requirementId: "BE-REQ-062",
    category: "governance",
    founderInstruction: "Curio and Ascend remain NOT APPROVED until founder says otherwise.",
    originatingPhase: "2C–current",
    originatingDate: "2026-11",
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: [],
  }),
  // --- Recovered from historical audit (Phases 2C–2H-A) beyond the 50-item checklist ---
  req({
    requirementId: "BE-REQ-063",
    recoveryId: "ADD-2C-01",
    category: "pipeline",
    founderInstruction:
      "Reconstruct without reading production Presentation / Curio fixtures as generator input.",
    originatingPhase: "2C",
    originatingDate: "2026-11-18",
    sourceRef: "BE_PHASE2C_CURIO_RECONSTRUCTION_V1.md",
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: true, E: true, F: false, G: false }),
    unresolvedIssues: ["Candidates still not founder-approved"],
  }),
  req({
    requirementId: "BE-REQ-064",
    recoveryId: "ADD-2C-02",
    category: "pipeline",
    founderInstruction: "Preserve Brand Basics rec… identity; frozen baseline is never overwritten.",
    originatingPhase: "2C",
    originatingDate: "2026-11-18",
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: true, E: true, F: false, G: false }),
    unresolvedIssues: [],
  }),
  req({
    requirementId: "BE-REQ-065",
    recoveryId: "ADD-2C-04",
    category: "governance",
    founderInstruction: "Do not treat the number of generated fields as proof of meaningful population.",
    originatingPhase: "2C",
    originatingDate: "2026-11-18",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Needs unified approval gate rejecting field-count scores"],
  }),
  req({
    requirementId: "BE-REQ-066",
    recoveryId: "ADD-2C-07",
    category: "rendering_comparison",
    founderInstruction: "Real atelier render required; structural tab-ID tests alone do not count.",
    originatingPhase: "2C",
    originatingDate: "2026-11-18",
    status: STATUS.BROWSER_VERIFIED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Gate1 evidence exists; founder approval pending"],
  }),
  req({
    requirementId: "BE-REQ-067",
    recoveryId: "ADD-2D-01",
    category: "pipeline",
    founderInstruction:
      "Reconcile full Presentation denominator vs factory slots; do not shrink denominator to inflate scores.",
    originatingPhase: "2D",
    originatingDate: "2026-11-18",
    sourceRef: "BE_PHASE2D_CURIO_QUALITY_RECOVERY.md",
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: false, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Denominator discipline not wired as permanent CI gate"],
  }),
  req({
    requirementId: "BE-REQ-068",
    recoveryId: "ADD-2D-04",
    category: "lifecycle_ops",
    founderInstruction:
      "No fee % invention; when confidential figures unavailable use labeled diligence guidance.",
    originatingPhase: "2D",
    originatingDate: "2026-11-18",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Economics invention gates partial; Gate2 depth outstanding"],
  }),
  req({
    requirementId: "BE-REQ-069",
    recoveryId: "ADD-2D-05",
    category: "rendering_comparison",
    founderInstruction:
      "Validate via real atelier overlay (beInternalPreview + beCandidateOverlay), not standalone HTML substitute.",
    originatingPhase: "2D",
    originatingDate: "2026-11-18",
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Not founder-approved"],
  }),
  req({
    requirementId: "BE-REQ-070",
    recoveryId: "ADD-2E-02",
    category: "rendering_comparison",
    founderInstruction:
      "Comparison Studio must be visual ten-tab product render — do not build a generic JSON comparison tool.",
    originatingPhase: "2E",
    originatingDate: "2026-11-18",
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Gate1 founder approval pending"],
  }),
  req({
    requirementId: "BE-REQ-071",
    recoveryId: "ADD-2F-01",
    category: "governance",
    founderInstruction:
      "Automated ~99% / A/B/C scores are NOT accepted as production-quality evidence.",
    originatingPhase: "2F",
    originatingDate: "2026-11-18",
    sourceRef: "BE_PHASE2F_VISUAL_QUALITY_RCA.md",
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Scoreboards still exist in reports; must not drive approval"],
  }),
  req({
    requirementId: "BE-REQ-072",
    recoveryId: "ADD-2F-02",
    category: "governance",
    founderInstruction:
      "Capture founder visual failures as permanent regression baseline (titles-as-paragraphs, imagery, depth, economics, empty lists, hero, comparison misalignment).",
    originatingPhase: "2F",
    originatingDate: "2026-11-18",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Regression suite exists; not all classes browser-verified on current candidates"],
  }),
  req({
    requirementId: "BE-REQ-073",
    recoveryId: "ADD-2G-02",
    category: "rendering_comparison",
    founderInstruction:
      "Replace dual full-page scroll with ONE paired-subsection document; next pair below taller column.",
    originatingPhase: "2G",
    originatingDate: "2026-11-19",
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Founder approval pending"],
  }),
  req({
    requirementId: "BE-REQ-074",
    recoveryId: "ADD-2G-04",
    category: "rendering_comparison",
    founderInstruction:
      "Match collection cards by title/signature/source — not solely by array position; Missing/New empty counterparts.",
    originatingPhase: "2G",
    originatingDate: "2026-11-19",
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Browser verification incomplete across all collection types"],
  }),
  req({
    requirementId: "BE-REQ-075",
    recoveryId: "ADD-2G-07",
    category: "pipeline",
    founderInstruction:
      "If evidence insufficient: stop generation and record research gap — never min-words via repetition.",
    originatingPhase: "2G",
    originatingDate: "2026-11-19",
    sourceRef: "BE_PHASE2G_REPEATED_CONTENT_RCA.md",
    status: STATUS.AUTOMATED_TESTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: ["Clear-without-refill still leaves empty customer bodies until Gate2 compose"],
  }),
  req({
    requirementId: "BE-REQ-076",
    recoveryId: "ADD-2G-10",
    category: "pipeline",
    founderInstruction:
      "Separate pipeline stages: discovery → extract → verify → intelligence → owner analysis → generation → styling validation.",
    originatingPhase: "2G",
    originatingDate: "2026-11-19",
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: false, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Staged pipeline not fully separated in code"],
  }),
  req({
    requirementId: "BE-REQ-077",
    recoveryId: "ADD-2H-02",
    category: "rendering_comparison",
    founderInstruction: "Reuse the same product atelier renderers; do not create a second styling system.",
    originatingPhase: "2H",
    originatingDate: "2026-11-19",
    status: STATUS.BROWSER_VERIFIED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Gate1 founder approval pending"],
  }),
  req({
    requirementId: "BE-REQ-078",
    recoveryId: "ADD-2H-04",
    category: "rendering_comparison",
    founderInstruction:
      "Changes as non-destructive overlays; color alone must not indicate founder approval.",
    originatingPhase: "2H",
    originatingDate: "2026-11-19",
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: false, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Overlay approval semantics need explicit product rule"],
  }),
  req({
    requirementId: "BE-REQ-079",
    recoveryId: "ADD-2H-07",
    category: "owner_value",
    founderInstruction:
      "Priority subsections for recovery: Why Value, Operating Model, Loyalty, Geographic Footprint, Brand Materials.",
    originatingPhase: "2H",
    originatingDate: "2026-11-19",
    status: STATUS.CAPTURED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Gate2 blocked; priority recovery not executed under approved gate"],
  }),
  req({
    requirementId: "BE-REQ-080",
    recoveryId: "ADD-2H-A-02",
    category: "rendering_comparison",
    founderInstruction:
      "Identical saved Curio payload both sides; do not regenerate, rewrite, summarize or otherwise modify in Gate 1.",
    originatingPhase: "2H-A",
    originatingDate: "2026-11-19",
    status: STATUS.BROWSER_VERIFIED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: true, G: false }),
    unresolvedIssues: ["Awaiting Gate1 founder approval"],
  }),
  req({
    requirementId: "BE-REQ-081",
    recoveryId: "ADD-2H-A-05",
    category: "rendering_comparison",
    founderInstruction: "A successful HTML render or matching tab IDs is insufficient for parity claims.",
    originatingPhase: "2H-A",
    originatingDate: "2026-11-19",
    status: STATUS.CONTRACT_ENCODED,
    coverage: cov({ A: true, B: true, C: false, D: false, E: false, F: false, G: false }),
    unresolvedIssues: ["Need geometry/CSS/component evidence gates beyond tab IDs"],
  }),
  req({
    requirementId: "BE-REQ-082",
    recoveryId: "ADD-STD-03",
    category: "governance",
    founderInstruction:
      "Preserve Phase 2H WIP, PR #457, rejected PR #453; no ADP/GDI/HE edits from BE chat.",
    originatingPhase: "permanent-standards",
    originatingDate: "2026-11-19",
    permanentContractLocation: `${contracts.permanent}#governance`,
    status: STATUS.IMPLEMENTED,
    coverage: cov({ A: true, B: true, C: true, D: false, E: true, F: false, G: false }),
    unresolvedIssues: [],
  }),
];

const all = [...checklist, ...additional];

function summarize(rows) {
  const byStatus = {};
  for (const r of rows) byStatus[r.status] = (byStatus[r.status] || 0) + 1;
  return {
    byStatus,
    counts: {
      total: rows.length,
      checklist50: rows.filter((r) => r.checklistNo).length,
      additional: rows.filter((r) => !r.checklistNo).length,
      founderApproved: rows.filter((r) => r.coverage.founderApproved).length,
      contractsOnly: rows.filter((r) => r.coverage.contract && !r.coverage.productionCodeEnforces).length,
      codeWithoutBrowser: rows.filter(
        (r) => r.coverage.productionCodeEnforces && !r.coverage.realAppDemonstrates,
      ).length,
      browserWithoutFounder: rows.filter(
        (r) => r.coverage.realAppDemonstrates && !r.coverage.founderApproved,
      ).length,
    },
  };
}

const master = {
  version: "BE_MASTER_FOUNDER_REQUIREMENTS_REGISTER_V1",
  generatedAt: new Date().toISOString(),
  workspace: "C:/Dev/dealality/ws-brand-explorer",
  branch: "feat/be-phase2h-component-parity-v1",
  benchmark: {
    primary: "Curio Collection by Hilton",
    secondary: "Ascend Hotel Collection",
  },
  gateStatus: {
    gate1: {
      id: "RENDERER_PARITY",
      founderApproved: gate1Approved,
      evidencePreserved: gate1Evidence,
      status: gate1Approved ? "FOUNDER_APPROVED" : "AWAITING_FOUNDER_APPROVAL",
      urls: {
        compare:
          "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1",
        product:
          "http://127.0.0.1:8082/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=curio-gate1-renderer-parity-payload.json",
      },
      evidenceDir: "reports/brand-explorer/phase2h-a-renderer-parity/",
    },
    gate2: {
      id: "CONTENT_RECONSTRUCTION",
      status: "BLOCKED",
      reason: "Gate1 not founder-approved",
    },
    curioAscend: "NOT_APPROVED",
  },
  statusVocabulary: Object.values(STATUS),
  coverageDimensions: {
    A: "documentationOnly",
    B: "contract",
    C: "productionCodeEnforces",
    D: "pipelineSuppliesData",
    E: "regressionTest",
    F: "realAppDemonstrates",
    G: "founderApproved",
  },
  honestPolicy: {
    contractIsNotImplementation: true,
    unitTestIsNotBrowserVerification: true,
    automatedScoreIsNotFounderApproval: true,
    doNotDeclareArchitectureCompleteWithoutDisposition: true,
  },
  requirements: all,
  summary: summarize(all),
};

writeFileSync(join(outDir, "01-master-founder-requirements-register.json"), JSON.stringify(master, null, 2));

writeFileSync(
  join(outDir, "03-fifty-item-reconciliation-matrix.json"),
  JSON.stringify(
    {
      version: "BE_FOUNDER_50_ITEM_RECONCILIATION_MATRIX_V1",
      rows: checklist.map((r) => ({
        checklistNo: r.checklistNo,
        requirementId: r.requirementId,
        instruction: r.founderInstruction,
        status: r.status,
        B_contract: r.coverage.contract,
        C_code: r.coverage.productionCodeEnforces,
        D_pipeline: r.coverage.pipelineSuppliesData,
        E_test: r.coverage.regressionTest,
        F_browser: r.coverage.realAppDemonstrates,
        G_founder: r.coverage.founderApproved,
        contractLocation: r.permanentContractLocation,
        automatedTest: r.automatedTest,
        unresolvedIssues: r.unresolvedIssues,
        disposition: r.disposition || null,
      })),
    },
    null,
    2,
  ),
);

writeFileSync(
  join(outDir, "04-missing-requirements-report.json"),
  JSON.stringify(
    {
      version: "BE_MISSING_REQUIREMENTS_REPORT_V1",
      note: "Items lacking code enforcement, pipeline supply, browser evidence, and/or founder approval",
      items: all
        .filter((r) => !r.coverage.productionCodeEnforces || !r.coverage.founderApproved)
        .map((r) => ({
          requirementId: r.requirementId,
          status: r.status,
          gap: [
            !r.coverage.contract && "NO_CONTRACT",
            !r.coverage.productionCodeEnforces && "NO_CODE_ENFORCEMENT",
            !r.coverage.pipelineSuppliesData && "NO_PIPELINE_SUPPLY",
            !r.coverage.regressionTest && "NO_REGRESSION_TEST",
            !r.coverage.realAppDemonstrates && "NO_BROWSER_EVIDENCE",
            !r.coverage.founderApproved && "NOT_FOUNDER_APPROVED",
          ].filter(Boolean),
          unresolvedIssues: r.unresolvedIssues,
        })),
    },
    null,
    2,
  ),
);

writeFileSync(
  join(outDir, "05-contracts-only-not-product-code.json"),
  JSON.stringify(
    {
      version: "BE_CONTRACTS_ONLY_REPORT_V1",
      items: all
        .filter((r) => r.coverage.contract && !r.coverage.productionCodeEnforces)
        .map((r) => ({
          requirementId: r.requirementId,
          instruction: r.founderInstruction,
          status: r.status,
          permanentContractLocation: r.permanentContractLocation,
        })),
    },
    null,
    2,
  ),
);

writeFileSync(
  join(outDir, "06-implemented-not-browser-verified.json"),
  JSON.stringify(
    {
      version: "BE_IMPLEMENTED_NOT_BROWSER_VERIFIED_V1",
      items: all
        .filter((r) => r.coverage.productionCodeEnforces && !r.coverage.realAppDemonstrates)
        .map((r) => ({
          requirementId: r.requirementId,
          instruction: r.founderInstruction,
          status: r.status,
          relevantCode: r.relevantCode,
        })),
    },
    null,
    2,
  ),
);

writeFileSync(
  join(outDir, "07-conflicting-requirements-decision-register.json"),
  JSON.stringify(
    {
      version: "BE_CONFLICTING_REQUIREMENTS_DECISION_REGISTER_V1",
      conflicts: [
        {
          conflictId: "BE-CONFLICT-LIFECYCLE-6-VS-7",
          topic: "Support Across the Lifecycle stage count",
          requirementA: {
            id: "BE-REQ-019-BASELINE",
            statement:
              "Live production template / atelier TIMELINE / Curio Gate1 payload / slots.md = EXACT 6 stages",
            stages: [
              "Evaluation",
              "Conversion Design",
              "Pre-Opening",
              "Opening",
              "Ramp-Up",
              "Ongoing",
            ],
            evidence: [
              "public/js/brand-explorer-atelier-from-api.js TIMELINE length 6",
              "public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json lifecycle.1-6",
              "docs/brand-explorer-presentation-slots.md valueOwners.lifecycle.1…6",
              "browser Gate1 mount .timeline__phase",
            ],
          },
          requirementB: {
            id: "BE-REQ-019-FOUNDER-REQUEST",
            statement:
              "Founder requested exactly SEVEN canonical stages; prior permanent contract of six called incorrect",
            source: "Founder architecture corrections prompt 2026-11-19 §9",
            alsoSaid: "Do not invent a seventh stage; inspect live and recover exact structure",
          },
          historicalSeventhStageSearch: {
            valueOwnersLifecycle7InRepo: false,
            atelierPhase7InTimelines: false,
            note: "Stabilization exists as economics.opening.step.5 (Opening & Conversion Path), NOT as a 7th Support Across the Lifecycle stage. Search for valueOwners.lifecycle.7 finds only the intentional failing fixture.",
          },
          potentialReasonsForDifference: [
            "Founder may be counting a desired product enhancement beyond current live IA",
            "Possible conflation with Opening & Conversion Path stages or Owner Buying dual cards",
            "Earlier verbal/design intent never landed in production TIMELINE",
          ],
          documentedCurrentProductionBaseline: {
            count: 6,
            titles: [
              "Evaluation",
              "Conversion Design",
              "Pre-Opening",
              "Opening",
              "Ramp-Up",
              "Ongoing",
            ],
            retainedInContracts: true,
          },
          unresolvedFounderDecision: {
            targetDesign:
              "OPEN — choose retain 6 OR approve a specified 7th stage title/order/semantics",
            untilThen:
              "Retain six in unchanged production baseline; keep seven-stage enhancement OPEN; never invent unsupported stages",
          },
          status: "OPEN_CONFLICT",
        },
      ],
    },
    null,
    2,
  ),
);

writeFileSync(
  join(outDir, "08-contract-code-test-traceability-matrix.json"),
  JSON.stringify(
    {
      version: "BE_TRACEABILITY_MATRIX_V1",
      rows: all.map((r) => ({
        requirementId: r.requirementId,
        contract: r.permanentContractLocation,
        code: r.relevantCode,
        test: r.automatedTest,
        browserEvidence: r.browserValidationEvidence,
        status: r.status,
        B: r.coverage.contract,
        C: r.coverage.productionCodeEnforces,
        D: r.coverage.pipelineSuppliesData,
        E: r.coverage.regressionTest,
        F: r.coverage.realAppDemonstrates,
        G: r.coverage.founderApproved,
      })),
    },
    null,
    2,
  ),
);

writeFileSync(
  join(outDir, "09-visual-content-quality-gap-register.json"),
  JSON.stringify(
    {
      version: "BE_VISUAL_CONTENT_QUALITY_GAP_REGISTER_V1",
      gate2Blocked: true,
      gapsBySubsection: [
        {
          section: "What the Owner Is Really Buying",
          gap: "Renderer shows single Overview card; dual Guest/Owner propositions missing",
          req: ["BE-REQ-016", "BE-REQ-017", "BE-REQ-018"],
          severity: "CRITICAL",
        },
        {
          section: "Why Value Is Strongest",
          gap: "Bold Title Case Insight: sentence format not enforced",
          req: ["BE-REQ-012"],
          severity: "HIGH",
        },
        {
          section: "Key Differentiators",
          gap: "Geometry pads to 4; empty/generic bullets still possible",
          req: ["BE-REQ-013"],
          severity: "HIGH",
        },
        {
          section: "Support Across the Lifecycle",
          gap: "Stage count conflict OPEN; body depth Gate2",
          req: ["BE-REQ-019"],
          severity: "HIGH",
        },
        {
          section: "Operating Model",
          gap: "Canonical fields present; substantive depth incomplete on candidates",
          req: ["BE-REQ-021", "BE-REQ-024"],
          severity: "HIGH",
        },
        {
          section: "Brand Standards",
          gap: "Multirow table exists on baseline; candidate depth risk",
          req: ["BE-REQ-023"],
          severity: "MEDIUM",
        },
        {
          section: "Demand Scenario View",
          gap: "Count may meet 6; differentiation/substance unverified on candidates",
          req: ["BE-REQ-028"],
          severity: "MEDIUM",
        },
        {
          section: "Recent Momentum",
          gap: "Must stay openings timeline not generic diligence",
          req: ["BE-REQ-029"],
          severity: "MEDIUM",
        },
        {
          section: "Geographic Footprint",
          gap: "Regional HPC stamp historically failed; composer fix not founder-verified",
          req: ["BE-REQ-030", "BE-REQ-031"],
          severity: "HIGH",
        },
        {
          section: "Image Gallery / Ascend",
          gap: "Ascend property imagery historically blocked",
          req: ["BE-REQ-037", "BE-REQ-039", "BE-REQ-041"],
          severity: "HIGH",
        },
        {
          section: "Comparison Studio Gate1",
          gap: "Automated/browser evidence exists; FOUNDER_APPROVED=false",
          req: ["BE-REQ-045", "BE-REQ-046", "BE-REQ-050"],
          severity: "BLOCKER_FOR_GATE2",
        },
      ],
    },
    null,
    2,
  ),
);

writeFileSync(
  join(outDir, "10-prioritized-implementation-backlog.json"),
  JSON.stringify(
    {
      version: "BE_PRIORITIZED_IMPLEMENTATION_BACKLOG_V1",
      note: "Gate2 content work remains blocked until Gate1 approval",
      backlog: [
        {
          priority: 1,
          item: "Founder Gate1 approval decision (BE-REQ-045–050)",
          blockedBy: null,
        },
        {
          priority: 2,
          item: "Founder decision on lifecycle 6 vs 7 (BE-CONFLICT-LIFECYCLE-6-VS-7)",
          blockedBy: null,
        },
        {
          priority: 3,
          item: "Implement dual Guest/Owner propositions in atelier (BE-REQ-016–018)",
          blockedBy: "Prefer after Gate1; product IA change needs founder priority call",
        },
        {
          priority: 4,
          item: "Wire editorial Title Case / em-dash / capital bullet enforcers into generation path (BE-REQ-001–009)",
          blockedBy: "Gate2",
        },
        {
          priority: 5,
          item: "Why Value Insight format composer (BE-REQ-012)",
          blockedBy: "Gate2",
        },
        {
          priority: 6,
          item: "Ops/standards/demand/momentum/risk depth population (BE-REQ-021–029)",
          blockedBy: "Gate2",
        },
        {
          priority: 7,
          item: "Loyalty KPI dating + parent vs brand distinction (BE-REQ-033–034)",
          blockedBy: "Gate2 (+ research auth if needed)",
        },
        {
          priority: 8,
          item: "Ascend imagery resolution (BE-REQ-037–041)",
          blockedBy: "Gate2 + research/assets",
        },
        {
          priority: 9,
          item: "Ten-tab side-by-side browser verification after Gate2",
          blockedBy: "Gate2 complete candidates",
        },
        {
          priority: 10,
          item: "CI coverage check for requirements lacking disposition",
          blockedBy: null,
        },
      ],
    },
    null,
    2,
  ),
);

console.log(JSON.stringify(master.summary, null, 2));
console.log("wrote", outDir);
