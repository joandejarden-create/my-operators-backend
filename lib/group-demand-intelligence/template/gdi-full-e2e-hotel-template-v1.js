/**
 * GDI_FULL_E2E_HOTEL_TEMPLATE_V1 — permanent reusable Full E2E hotel operating template.
 *
 * Reference implementation: Bethesda Marriott local-master V2 and all approved
 * improvements through GitHub PR #275 (plus compatible UI cleanup PR #276).
 *
 * This module locks architecture, versions, gates, and invocation — it does not
 * execute paid research or mutate customer publications.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_FULL_E2E_HOTEL_TEMPLATE_V1 as TEMPLATE_ID,
  GDI_RESEARCH_PERSISTENCE_CONTRACT_V1,
  GDI_RESEARCH_STORAGE_ROLES,
  evaluateFullE2eHotelTemplateResearchStorageGate,
} from "../research/gdi-research-persistence-contract-v1.js";
import {
  GDI_CANONICAL_PRESENTATION_PERSISTENCE_V1,
  GDI_PRESENTATION_SCHEMA_VERSION,
  GDI_PRESENTATION_DERIVATION_RULE_VERSION,
  REQUIRED_CUSTOMER_PRESENTATION_KEYS,
  mergeCanonicalPresentationIntoOpportunity,
  validateRequiredPresentationFields,
  comparePresentationParity,
  reattachRequiredPresentationKeys,
} from "../presentation/gdi-canonical-presentation-persistence-v1.js";
import {
  GDI_FIELD_AUTHORITY_CONTRACT_V1,
  GDI_FIELD_AUTHORITY_MATRIX_V1,
} from "../presentation/gdi-field-authority-contract-v1.js";
import { FULL_E2E_CONTRACT_VERSION } from "../workflow/full-e2e-contract-v1.js";
import {
  SPECIALIST_HUNTER_META,
  HUNTER_CAPABILITY,
  GDI_NEXTGEN_DISCOVERY_ARCHITECTURE_V1,
} from "../nextgen-discovery/specialist-hunter-contract-v1.js";
import { compactOpportunityPayloadForAirtable } from "../airtable-opportunity-store.js";
import {
  GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  GDI_CMI_CONTRACT_VERSION,
  CMI_COMMERCIAL_DEFINITION,
  CMI_EVIDENCE_STATUS,
  CMI_QUALIFICATION_GATES,
  CMI_DISCOVERY_COVERAGE_CHECKLIST,
  CMI_DISCOVERY_STAGE,
  evaluateCmiQualificationGates,
  evaluateCmiDiscoveryCoverage,
  annotateCanonicalCmiContract,
} from "../commercial-consistency/cmi-canonical-eligibility-contract-v1.js";
import {
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  GDI_COMMERCIAL_ACTIONABILITY_CONTRACT_VERSION,
  COMMERCIAL_ACTIONABILITY_STATE,
  ACTIVE_HOTEL_MARKET_STATES,
  classifyCommercialActionability,
  applyCommercialActionabilityGate,
  assertCustomerInventoryPassesActionabilityGate,
} from "../commercial-actionability-gate-v1.js";

export const GDI_FULL_E2E_HOTEL_TEMPLATE_V1 = TEMPLATE_ID;
export const GDI_FULL_E2E_HOTEL_TEMPLATE_LOCK_V1 =
  "gdi-full-e2e-hotel-template-lock-v1";
/** Backward-compatible policy correction layer for mandatory CMI. */
export const GDI_FULL_E2E_HOTEL_TEMPLATE_CMI_POLICY_CORRECTION_V1 =
  "gdi-full-e2e-hotel-template-cmi-policy-correction-v1";
/** Permanent commercial-actionability gate lock (all hotels). */
export const GDI_FULL_E2E_HOTEL_TEMPLATE_ACTIONABILITY_GATE_V1 =
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const TEMPLATE_REFERENCE = Object.freeze({
  hotelId: "recLuxvwwxID7U2B8",
  hotelName: "Bethesda Marriott",
  publicationVersion: "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2",
  contentFingerprintSha256:
    "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66",
  opportunityCount: 67,
  baselinePrs: Object.freeze([
    264, 266, 267, 268, 269, 270, 272, 273, 274, 275,
  ]),
  compatibleFollowOnPrs: Object.freeze([276]),
});

export const TEMPLATE_VERSION_LOCK = Object.freeze({
  templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  lockId: GDI_FULL_E2E_HOTEL_TEMPLATE_LOCK_V1,
  cmiPolicyCorrectionId: GDI_FULL_E2E_HOTEL_TEMPLATE_CMI_POLICY_CORRECTION_V1,
  cmiCanonicalEligibilityContract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  cmiContractVersion: GDI_CMI_CONTRACT_VERSION,
  commercialActionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  commercialActionabilityContractVersion:
    GDI_COMMERCIAL_ACTIONABILITY_CONTRACT_VERSION,
  fullE2eContractVersion: FULL_E2E_CONTRACT_VERSION,
  researchPersistenceContract: GDI_RESEARCH_PERSISTENCE_CONTRACT_V1,
  presentationPersistence: GDI_CANONICAL_PRESENTATION_PERSISTENCE_V1,
  presentationSchemaVersion: GDI_PRESENTATION_SCHEMA_VERSION,
  presentationDerivationRuleVersion: GDI_PRESENTATION_DERIVATION_RULE_VERSION,
  fieldAuthorityContract: GDI_FIELD_AUTHORITY_CONTRACT_V1,
  nextgenDiscoveryArchitecture: GDI_NEXTGEN_DISCOVERY_ARCHITECTURE_V1,
  frontendAuthCacheMarker: "gdi-ui-no-mode-banner-v1",
  frontendUiCacheMarker: "gdi-contact-subtitle-freeze-v1",
  airtableBaseId: GDI_RESEARCH_STORAGE_ROLES.AIRTABLE_REGISTRY.baseId,
  airtableOpportunitiesTableId:
    GDI_RESEARCH_STORAGE_ROLES.AIRTABLE_REGISTRY.tableId,
});

/** Four separate data responsibilities — never collapse. */
export const TEMPLATE_DATA_RESPONSIBILITIES = GDI_RESEARCH_STORAGE_ROLES;

export const TEMPLATE_E2E_STAGES = Object.freeze([
  { id: 1, name: "Hotel intelligence", commandHint: "validate hotel config + HPC/ADP identity" },
  { id: 2, name: "Existing research reconciliation", commandHint: "gdi:research-storage-reconciliation-v1 --dry-run" },
  { id: 3, name: "Discovery planning", commandHint: "gdi:full-e2e --dry-run (plan only)" },
  { id: 4, name: "Demand discovery", commandHint: "gdi:full-e2e with authorized budget" },
  {
    id: "4a",
    name: "Competitor-booked business discovery (CMI)",
    mandatory: true,
    commandHint:
      "CMI discovery coverage checklist + classifyCompetitiveMarketIntelligence; zero CMI requires documented coverage proof",
    contract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
    stageSpec: CMI_DISCOVERY_STAGE,
  },
  {
    id: "4b",
    name: "Commercial-actionability gate (discovery qualification)",
    mandatory: true,
    commandHint:
      "classifyCommercialActionability against effective research date; expired one-time events must not enter active Hotel Market",
    contract: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  },
  {
    id: 5,
    name: "Commercial qualification / research review",
    commandHint:
      "quality gates + commercial-actionability reassessment inside research-to-page workflow",
    actionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  },
  { id: 6, name: "Airtable persistence", commandHint: "canonical upsert + presentation persistence contract" },
  {
    id: 7,
    name: "Quality review / customer-visible eligibility",
    commandHint:
      "test:gdi-full-customer-consistency-audit-v1 / field authority / assertCustomerInventoryPassesActionabilityGate",
    actionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  },
  { id: 8, name: "Founder preview", commandHint: "authenticated GDI publicationPackage=1 (Mode B ≡ share)" },
  {
    id: 9,
    name: "Package staging",
    commandHint:
      "immutable fingerprinted candidate; enforceCommercialActionabilityGate on staged inventory",
    actionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  },
  {
    id: 10,
    name: "Founder approval / publication certification",
    commandHint:
      "approve exact contentFingerprintSha256 after actionability certification",
    actionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  },
  { id: 11, name: "Production publication", commandHint: "Dealality production release (GDI)" },
  { id: 12, name: "Production parity", commandHint: "browser parity vs founder preview" },
  {
    id: 13,
    name: "Weekly refresh and feedback",
    commandHint:
      "weekly-refresh + feedback store; re-run actionability gate vs refresh date; stage removals/future-cycle reassessment",
    actionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  },
]);

export const HOTEL_CONFIGURATION_REQUIRED_KEYS = Object.freeze([
  "hotelId",
  "displayName",
]);

export const HOTEL_CONFIGURATION_OPTIONAL_KEYS = Object.freeze([
  "brand",
  "operator",
  "country",
  "city",
  "aliases",
  "commercialPriorities",
  "demandTerritory",
  "competitiveContext",
  "capabilityProfile",
  "locale",
  "geo",
  "adpFixturePath",
]);

/**
 * Map specialist hunter capability → template certification labels.
 */
export function buildResearchCapabilityMatrix() {
  const rows = [];
  for (const [hunterId, meta] of Object.entries(SPECIALIST_HUNTER_META)) {
    let status = "IMPLEMENTED_NOT_CERTIFIED";
    if (meta.capability === HUNTER_CAPABILITY.EXISTS) {
      status = "PRODUCTION_CERTIFIED";
    } else if (meta.capability === HUNTER_CAPABILITY.PARTIAL) {
      status = "IMPLEMENTED_NOT_CERTIFIED";
    } else if (meta.capability === HUNTER_CAPABILITY.NEW) {
      status = "EXPERIMENTAL";
    }
    rows.push({
      hunterId,
      label: meta.label,
      wave: meta.wave,
      hunterCapability: meta.capability,
      templateStatus: status,
      workflowIds: meta.workflowIds || [],
      researchSegments: meta.researchSegments || [],
      notes: meta.notes || null,
    });
  }
  // Additional product surfaces beyond specialist hunters
  rows.push(
    {
      hunterId: "competitive_market_intelligence",
      label: "Competitive Market Intelligence",
      wave: null,
      hunterCapability: "EXISTS",
      templateStatus: "PRODUCTION_CERTIFIED",
      workflowIds: ["competitor_booked_business_discovery"],
      researchSegments: [],
      notes:
        "Mandatory Full E2E stage 4a. Canonical contract gdi-cmi-canonical-eligibility-contract-v1 — verified competitor-committed booking + future account development. Not preferred-hotel lists. Zero CMI requires coverage proof.",
      contractId: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
      contractVersion: GDI_CMI_CONTRACT_VERSION,
    },
    {
      hunterId: "private_event_venues",
      label: "Private event venues and partnerships",
      wave: null,
      hunterCapability: "EXISTS",
      templateStatus: "IMPLEMENTED_NOT_CERTIFIED",
      workflowIds: [],
      researchSegments: [],
      notes: "Airtable venue tables exist; hotel-specific depth varies",
    },
    {
      hunterId: "need_date_match",
      label: "Need-Date Match",
      wave: null,
      hunterCapability: "EXISTS",
      templateStatus: "PRODUCTION_CERTIFIED",
      workflowIds: [],
      researchSegments: [],
      notes: "GDI Hotel Need Periods + opportunity matches; explicit absent allowed",
    },
  );
  return rows;
}

export const COMMERCIAL_ELIGIBILITY_POLICY_V1 = Object.freeze({
  schema: "gdi-commercial-eligibility-policy-v1",
  customerFacingRequires: Object.freeze([
    "credibleEvidenceGroundedDemandThesis",
    "realisticHotelSpecificCommercialOpportunity",
    "verifiedNamedCommerciallyRelevantContactOrDefensibleIntermediary",
    "appropriateRecommendedAction",
    "transparentUncertaintyAndSourcingStatus",
  ]),
  notUniversallyRequired: Object.freeze([
    "confirmedRoomNights",
    "openRfp",
  ]),
  demandClasses: Object.freeze({
    RELIABLE_INFERRED_DEMAND: "MAY_QUALIFY",
    SPECULATIVE_DEMAND: "RESEARCH_STAGE_OR_WATCH_ONLY_WHEN_INSUFFICIENT",
  }),
  scalingContactPolicy: Object.freeze({
    verifiedNamedPeopleConnectedToDemandGeneratingActivity:
      "MAY_BE_CREDIBLE_ENTRY_POINTS_EVEN_WITHOUT_ESTABLISHED_BOOKING_AUTHORITY",
  }),
  contactClasses: Object.freeze([
    "DIRECT_BUYER",
    "RELEVANT_COMMERCIAL_ENTRY_POINT",
    "INTERMEDIARY",
    "UNRELATED_CONTACT",
    "UNKNOWN_BOOKING_AUTHORITY",
  ]),
  reject: Object.freeze([
    "fabricatedResponsibilities",
    "genericUnsupportedContacts",
    "unsupportedDemandClaims",
    "expiredOneTimeEventInActiveHotelMarket",
    "researchOrArticleDateAsEventDate",
  ]),
  commercialActionability: Object.freeze({
    contractId: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
    contractVersion: GDI_COMMERCIAL_ACTIONABILITY_CONTRACT_VERSION,
    activeHotelMarketStates: ACTIVE_HOTEL_MARKET_STATES,
    states: COMMERCIAL_ACTIONABILITY_STATE,
    appliesAt: Object.freeze([
      "discovery_qualification",
      "commercial_research_review",
      "customer_visible_eligibility",
      "publication_staging",
      "weekly_refresh",
      "founder_publication_certification",
    ]),
    referenceDateRule: "effective_research_or_refresh_date",
    appliesToAllHotels: true,
    completedOneTimeWithoutFutureCycle: "NOT_ACTIVE_HOTEL_MARKET",
  }),
  /**
   * Competitive Market Intelligence — recovered Bethesda meaning (policy correction).
   * Evidence status remains separate from Market Scope / sales maturity.
   */
  competitiveMarketIntelligence: Object.freeze({
    contractId: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
    contractVersion: GDI_CMI_CONTRACT_VERSION,
    policyCorrectionId: GDI_FULL_E2E_HOTEL_TEMPLATE_CMI_POLICY_CORRECTION_V1,
    definition: CMI_COMMERCIAL_DEFINITION,
    evidenceStatuses: CMI_EVIDENCE_STATUS,
    qualificationGates: CMI_QUALIFICATION_GATES,
    discoveryCoverageChecklist: CMI_DISCOVERY_COVERAGE_CHECKLIST,
    discoveryStage: CMI_DISCOVERY_STAGE,
    hotelProgramListingIsNotConfirmedBooking: true,
    principalBookingMustNotAppearAsOpenHotelOpportunity: true,
    zeroCmiRequiresDocumentedCoverageProof: true,
  }),
});

export function loadHotelConfigOrNull(hotelId) {
  const id = String(hotelId || "").trim();
  if (!id) return null;
  const p = path.join(ROOT, "config/group-demand-intelligence/hotels", `${id}.json`);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function readBethesdaPublicationIdentity() {
  const currentPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    TEMPLATE_REFERENCE.hotelId,
    "CURRENT.json",
  );
  const pkgPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    TEMPLATE_REFERENCE.hotelId,
    TEMPLATE_REFERENCE.publicationVersion,
    "package.json",
  );
  const current = JSON.parse(fs.readFileSync(currentPath, "utf8"));
  const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  return { current, pkg, currentPath, pkgPath };
}

/**
 * Prospective Airtable presentation write/read cycle (in-memory; no live write).
 * Proves compaction + merge + reattach preserve required customer fields for a
 * synthetic new-hotel opportunity — not only Bethesda historical backfill.
 */
export function runProspectivePresentationPersistenceCycle(seed = {}) {
  const hotelId = seed.hotelId || "recTEST_TEMPLATE_PORTABILITY";
  const original = mergeCanonicalPresentationIntoOpportunity(
    {},
    {
      id: seed.id || "gdi_opp_template_prospective_1",
      hotelId,
      displayTitle: seed.displayTitle || "Template Prospective Summit 2027",
      title: seed.displayTitle || "Template Prospective Summit 2027",
      organizationName: seed.organizationName || "Template Org",
      salesActionKey: "BUILD_ACCOUNT",
      salesActionPillLabel: "Build Account",
      datePillDisplayLabel: "Mar 2027",
      whatIsHappening: "Annual industry summit with open hotel sourcing.",
      cardContactSubtitle: "Director of Meetings",
      revenueStreamClass: "GUESTROOMS_ONLY",
      primaryContact: {
        name: "Casey Lee",
        title: "Director of Meetings",
        email: "casey@example.com",
      },
      tileContactPreview: {
        displayRole: "Director of Meetings",
        displayRoleKind: "professional_title",
      },
      needDateMatchV1: null,
      canonicalNeedDateMatchStatus: "EXPLICIT_ABSENT_IN_APPROVED_PUBLICATION",
      priority: "HIGH_PRIORITY",
      recommendedAction: "Introduce inventory and request RFP timeline.",
      opportunityType: "ASSOCIATION_EVENT",
      sharedTenSectionDrawer: {
        sections: [{ id: "what_is_happening", title: "What is happening", body: "Annual industry summit." }],
      },
      ...seed.extra,
    },
    { recoveredFrom: "PROSPECTIVE_TEMPLATE_TEST" },
  );

  const afterCreate = compactOpportunityPayloadForAirtable({
    ...original,
    notes: "x".repeat(120000),
  });
  const createGate = validateRequiredPresentationFields(afterCreate);

  // Enrichment: contact change must not wipe sales action / drawers
  const enriched = mergeCanonicalPresentationIntoOpportunity(
    afterCreate,
    {
      ...afterCreate,
      primaryContact: {
        ...afterCreate.primaryContact,
        email: "casey.updated@example.com",
      },
      salesActionKey: "",
      salesActionPillLabel: "",
    },
    { recoveredFrom: "ENRICHMENT_PARTIAL" },
  );
  const afterEnrich = compactOpportunityPayloadForAirtable(enriched);
  const enrichKeepsAction =
    afterEnrich.salesActionKey === "BUILD_ACCOUNT" &&
    afterEnrich.salesActionPillLabel === "Build Account";

  // Stale overwrite protection: blank presentation cannot erase populated fields
  const reattached = reattachRequiredPresentationKeys(
    { id: original.id, hotelId },
    original,
  );
  const parity = comparePresentationParity(
    {
      displayTitle: original.displayTitle,
      salesActionKey: original.salesActionKey,
      salesActionPillLabel: original.salesActionPillLabel,
      datePillDisplayLabel: original.datePillDisplayLabel,
      whatIsHappening: original.whatIsHappening,
      cardContactSubtitle: original.cardContactSubtitle,
      revenueStreamClass: original.revenueStreamClass,
      primaryContact: original.primaryContact,
      tileContactPreview: original.tileContactPreview,
      needDateMatchV1: original.needDateMatchV1,
      canonicalNeedDateMatchStatus: original.canonicalNeedDateMatchStatus,
      priority: original.priority,
      recommendedAction: original.recommendedAction,
      organizationName: original.organizationName,
      opportunityType: original.opportunityType,
    },
    {
      displayTitle: afterCreate.displayTitle,
      salesActionKey: afterCreate.salesActionKey,
      salesActionPillLabel: afterCreate.salesActionPillLabel,
      datePillDisplayLabel: afterCreate.datePillDisplayLabel,
      whatIsHappening: afterCreate.whatIsHappening,
      cardContactSubtitle: afterCreate.cardContactSubtitle || afterCreate.tileContactPreview?.displayRole,
      revenueStreamClass: afterCreate.revenueStreamClass,
      primaryContact: afterCreate.primaryContact,
      tileContactPreview: afterCreate.tileContactPreview,
      needDateMatchV1: afterCreate.needDateMatchV1,
      canonicalNeedDateMatchStatus: afterCreate.canonicalNeedDateMatchStatus,
      priority: afterCreate.priority,
      recommendedAction: afterCreate.recommendedAction,
      organizationName: afterCreate.organizationName,
      opportunityType: afterCreate.opportunityType,
    },
  );

  // Negative mutation: wrong action must fail parity
  const mutated = { ...afterCreate, salesActionPillLabel: "Monitor" };
  const mutationCaught = comparePresentationParity(
    {
      salesActionPillLabel: original.salesActionPillLabel,
      displayTitle: original.displayTitle,
      datePillDisplayLabel: original.datePillDisplayLabel,
      whatIsHappening: original.whatIsHappening,
      cardContactSubtitle: original.cardContactSubtitle,
      revenueStreamClass: original.revenueStreamClass,
      primaryContact: original.primaryContact,
      tileContactPreview: original.tileContactPreview,
      needDateMatchV1: null,
      canonicalNeedDateMatchStatus: "EXPLICIT_ABSENT_IN_APPROVED_PUBLICATION",
      priority: original.priority,
      recommendedAction: original.recommendedAction,
      organizationName: original.organizationName,
      opportunityType: original.opportunityType,
      salesActionKey: original.salesActionKey,
    },
    {
      salesActionPillLabel: mutated.salesActionPillLabel,
      displayTitle: mutated.displayTitle,
      datePillDisplayLabel: mutated.datePillDisplayLabel,
      whatIsHappening: mutated.whatIsHappening,
      cardContactSubtitle: mutated.cardContactSubtitle || mutated.tileContactPreview?.displayRole,
      revenueStreamClass: mutated.revenueStreamClass,
      primaryContact: mutated.primaryContact,
      tileContactPreview: mutated.tileContactPreview,
      needDateMatchV1: null,
      canonicalNeedDateMatchStatus: "EXPLICIT_ABSENT_IN_APPROVED_PUBLICATION",
      priority: mutated.priority,
      recommendedAction: mutated.recommendedAction,
      organizationName: mutated.organizationName,
      opportunityType: mutated.opportunityType,
      salesActionKey: mutated.salesActionKey,
    },
  );

  return {
    ok:
      createGate.ok === true &&
      enrichKeepsAction === true &&
      parity.ok === true &&
      mutationCaught.ok === false &&
      reattached.salesActionPillLabel === "Build Account",
    createGate,
    enrichKeepsAction,
    parity,
    mutationDetected: mutationCaught.ok === false,
    hotelId,
    opportunityId: original.id,
  };
}

/**
 * Cross-hotel portability checks (read-only / fixture).
 */
export function runCrossHotelPortabilityChecks() {
  const hotels = [
    { hotelId: "recLuxvwwxID7U2B8", role: "REFERENCE_PUBLISHED", expectPkg: true },
    { hotelId: "recnPB3Q02NO8Ma1o", role: "EXISTING_UNPUBLISHED_US", expectPkg: false },
    { hotelId: "recKRJjcPnb4tVDDS", role: "INTERNATIONAL", expectPkg: false },
    { hotelId: "recG66DQJKP2c0UNh", role: "NO_PUBLISHED_PACKAGE", expectPkg: false },
  ];
  const results = [];
  const bethesdaName = "Bethesda Marriott";
  for (const h of hotels) {
    const cfg = loadHotelConfigOrNull(h.hotelId);
    const pkgDir = path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      h.hotelId,
    );
    const hasPkg = fs.existsSync(pkgDir);
    const leaksBethesda =
      cfg &&
      h.hotelId !== TEMPLATE_REFERENCE.hotelId &&
      JSON.stringify(cfg).includes(bethesdaName);
    results.push({
      ...h,
      configPresent: Boolean(cfg),
      displayName: cfg?.displayName || cfg?.redirectTo || null,
      publicationPackagePresent: hasPkg,
      expectPkgMatch: hasPkg === h.expectPkg,
      bethesdaDataLeak: leaksBethesda === true,
      sharedCurrentPointer: false,
    });
  }

  const authApp = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
    "utf8",
  );
  const shareApp = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
    "utf8",
  );
  const ui = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8",
  );

  const noDefaultModeBanner =
    /wantsPresentationModeDiagnostics/.test(authApp) &&
    !/gdiPresentationModeBanner/.test(shareApp);
  const noBethesdaHardcodeInUi =
    !/Bethesda Marriott/.test(ui) || /no_bethesda_hardcode/i.test(""); // UI may mention in comments; enforce via dedicated string scan below
  const uiHasBethesdaLiteral = /\bBethesda Marriott\b/.test(ui);

  const ok =
    results.every((r) => r.configPresent && r.expectPkgMatch && !r.bethesdaDataLeak) &&
    noDefaultModeBanner &&
    !uiHasBethesdaLiteral;

  return {
    ok,
    hotels: results,
    noDefaultModeBanner,
    uiHasBethesdaLiteral,
    note: "No Airtable writes; no publications activated.",
  };
}

/**
 * Evaluate full template certification from assembled evidence.
 */
export function evaluateFullE2eHotelTemplateCertificate(evidence = {}) {
  const fails = [];
  const push = (code, detail) => fails.push({ code, ...detail });

  if (!evidence.mainCommitSha || String(evidence.mainCommitSha).length < 7) {
    push("MISSING_MAIN_COMMIT_SHA", {});
  }
  if (evidence.publicationFingerprintOk !== true) {
    push("PUBLICATION_FINGERPRINT_NOT_VERIFIED", {
      required: TEMPLATE_REFERENCE.contentFingerprintSha256,
    });
  }
  if (evidence.airtableReconstruction67Ok !== true) {
    push("AIRTABLE_RECONSTRUCTION_NOT_67_OF_67", {});
  }
  if (evidence.prospectivePersistenceOk !== true) {
    push("PROSPECTIVE_PRESENTATION_PERSISTENCE_FAILED", {});
  }
  if (evidence.crossHotelOk !== true) {
    push("CROSS_HOTEL_PORTABILITY_FAILED", {});
  }
  if (evidence.fieldAuthorityContractPresent !== true) {
    push("FIELD_AUTHORITY_CONTRACT_MISSING", {});
  }
  if (evidence.modeBannerAbsentByDefault !== true) {
    push("MODE_BANNER_STILL_DEFAULT", {});
  }
  if (evidence.negativeMutationGatesOk !== true) {
    push("NEGATIVE_MUTATION_GATES_FAILED", {});
  }
  if (evidence.researchStorageGateOk !== true) {
    push("RESEARCH_STORAGE_GATE_FAILED", {});
  }
  if (evidence.noPaidResearchExecuted !== true) {
    push("PAID_RESEARCH_EXECUTED_DURING_CERT", {});
  }
  if (evidence.noPublicationMutated !== true) {
    push("PUBLICATION_MUTATED_DURING_CERT", {});
  }
  if (evidence.cmiCanonicalContractPresent !== true) {
    push("CMI_CANONICAL_CONTRACT_MISSING", {
      required: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
    });
  }
  if (evidence.cmiDiscoveryStageMandatory !== true) {
    push("CMI_DISCOVERY_STAGE_NOT_MANDATORY", {});
  }
  if (evidence.cmiPositiveNegativeTestsOk !== true) {
    push("CMI_POSITIVE_NEGATIVE_TESTS_FAILED", {});
  }
  if (evidence.cmiBethesdaFixturesReviewed !== true) {
    push("CMI_BETHESDA_10_FIXTURES_NOT_REVIEWED", {});
  }
  if (evidence.cmiCoverageGateOk !== true) {
    push("CMI_DISCOVERY_COVERAGE_GATE_FAILED", {});
  }
  if (evidence.commercialActionabilityGatePresent !== true) {
    push("COMMERCIAL_ACTIONABILITY_GATE_MISSING", {
      required: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
    });
  }
  if (evidence.commercialActionabilityPositiveNegativeTestsOk !== true) {
    push("COMMERCIAL_ACTIONABILITY_TESTS_FAILED", {});
  }
  if (evidence.commercialActionabilityStagesMandatory !== true) {
    push("COMMERCIAL_ACTIONABILITY_STAGES_NOT_MANDATORY", {});
  }

  return {
    schema: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
    lockId: GDI_FULL_E2E_HOTEL_TEMPLATE_LOCK_V1,
    cmiPolicyCorrectionId: GDI_FULL_E2E_HOTEL_TEMPLATE_CMI_POLICY_CORRECTION_V1,
    cmiContractVersion: GDI_CMI_CONTRACT_VERSION,
    commercialActionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
    commercialActionabilityContractVersion:
      GDI_COMMERCIAL_ACTIONABILITY_CONTRACT_VERSION,
    certified: fails.length === 0,
    status: fails.length === 0 ? "CERTIFIED" : "NOT_CERTIFIED",
    fails,
    versions: TEMPLATE_VERSION_LOCK,
    reference: TEMPLATE_REFERENCE,
    fieldAuthorityFieldCount: GDI_FIELD_AUTHORITY_MATRIX_V1.length,
    requiredPresentationKeyCount: REQUIRED_CUSTOMER_PRESENTATION_KEYS.length,
    evaluatedAt: new Date().toISOString(),
  };
}

export {
  evaluateFullE2eHotelTemplateResearchStorageGate,
  REQUIRED_CUSTOMER_PRESENTATION_KEYS,
  GDI_FIELD_AUTHORITY_MATRIX_V1,
  GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  GDI_CMI_CONTRACT_VERSION,
  CMI_COMMERCIAL_DEFINITION,
  CMI_EVIDENCE_STATUS,
  CMI_DISCOVERY_COVERAGE_CHECKLIST,
  CMI_DISCOVERY_STAGE,
  evaluateCmiQualificationGates,
  evaluateCmiDiscoveryCoverage,
  annotateCanonicalCmiContract,
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  GDI_COMMERCIAL_ACTIONABILITY_CONTRACT_VERSION,
  COMMERCIAL_ACTIONABILITY_STATE,
  ACTIVE_HOTEL_MARKET_STATES,
  classifyCommercialActionability,
  applyCommercialActionabilityGate,
  assertCustomerInventoryPassesActionabilityGate,
};
