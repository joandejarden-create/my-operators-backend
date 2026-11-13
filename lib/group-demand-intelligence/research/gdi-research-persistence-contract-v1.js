/**
 * GDI research persistence contract V1 — hotel-independent storage roles + integrity gates.
 *
 * Masters (do not collapse):
 *   1. Airtable Group Demand Opportunities — canonical mutable research registry
 *   2. Durable FS research artifacts — full evidence, runs, historical recovery
 *   3. Customer publication package — immutable FOUNDER_APPROVED content
 *   4. Feedback store — mutable outcomes, outside publication fingerprint
 *
 * Research sync must never mutate approved customer publication packages.
 */

export const GDI_RESEARCH_PERSISTENCE_CONTRACT_V1 =
  "gdi-research-persistence-contract-v1";

export const GDI_FULL_E2E_HOTEL_TEMPLATE_V1 = "GDI_FULL_E2E_HOTEL_TEMPLATE_V1";

export const GDI_RESEARCH_STORAGE_ROLES = Object.freeze({
  AIRTABLE_REGISTRY: Object.freeze({
    role: "CANONICAL_MUTABLE_RESEARCH_REGISTRY",
    baseId: "appa2cE7FTRmIbB32",
    tableName: "Group Demand Opportunities",
    tableId: "tblRuReslJMwsfRQj",
    owns: [
      "stableOpportunityId",
      "researchStatusAndCommercialClassification",
      "linksToEvidenceAndHistory",
      "hotelScopedInventory",
    ],
  }),
  DURABLE_RESEARCH_FS: Object.freeze({
    role: "DURABLE_RESEARCH_ARTIFACTS",
    root: "data/group-demand-intelligence/hotels/{hotelId}/",
    owns: [
      "completeSourceEvidence",
      "researchExecutionHistory",
      "detailedArtifacts",
      "historicalSnapshots",
      "recoveryInformation",
      "opportunitiesJsonMirror",
    ],
  }),
  CUSTOMER_PUBLICATION: Object.freeze({
    role: "IMMUTABLE_CUSTOMER_CONTENT",
    root: "lib/group-demand-intelligence/bundled-publication-packages/{hotelId}/",
    owns: [
      "approvedPresentationFields",
      "fullCardsAndDrawers",
      "contentFingerprintSha256",
      "publicationLifecycle",
    ],
    neverOverwrittenByResearchSync: true,
  }),
  FEEDBACK: Object.freeze({
    role: "MUTABLE_CUSTOMER_FEEDBACK",
    path: "data/group-demand-intelligence/hotels/{hotelId}/feedback.json",
    owns: ["validation", "action", "outcome", "stableOpportunityReferences"],
    outsidePublicationFingerprint: true,
  }),
});

/**
 * Assert published opportunity IDs are a subset of the research registry IDs.
 * Research-stage / archive rows may exceed the published set.
 *
 * @returns {{ ok: boolean, missingFromRegistry: string[], publishedCount: number, registryCount: number, extraInRegistry: number }}
 */
export function assertPublishedIdsCoveredByResearchRegistry({
  publishedOpportunityIds = [],
  researchRegistryIds = [],
} = {}) {
  const published = [
    ...new Set(
      (publishedOpportunityIds || [])
        .map((x) => String(x || "").trim())
        .filter(Boolean),
    ),
  ];
  const registry = new Set(
    (researchRegistryIds || [])
      .map((x) => String(x || "").trim())
      .filter(Boolean),
  );
  const missingFromRegistry = published.filter((id) => !registry.has(id));
  return {
    ok: missingFromRegistry.length === 0,
    missingFromRegistry,
    publishedCount: published.length,
    registryCount: registry.size,
    extraInRegistry: Math.max(0, registry.size - published.length),
    contract: GDI_RESEARCH_PERSISTENCE_CONTRACT_V1,
    gate: "PUBLISHED_IDS_SUBSET_OF_RESEARCH_REGISTRY",
  };
}

/**
 * Template certification gate: fail if research inventory is incomplete vs an
 * approved publication, or if publication fingerprint is required and mismatched.
 */
export function evaluateFullE2eHotelTemplateResearchStorageGate(input = {}) {
  const coverage = assertPublishedIdsCoveredByResearchRegistry(input);
  const requiredFp = String(input.requiredPublicationFingerprint || "").trim();
  const actualFp = String(input.actualPublicationFingerprint || "").trim();
  const fingerprintOk = !requiredFp || requiredFp.toLowerCase() === actualFp.toLowerCase();
  const fails = [];
  if (!coverage.ok) {
    fails.push({
      code: "RESEARCH_REGISTRY_MISSING_PUBLISHED_IDS",
      missingCount: coverage.missingFromRegistry.length,
      missingFromRegistry: coverage.missingFromRegistry,
    });
  }
  if (!fingerprintOk) {
    fails.push({
      code: "PUBLICATION_FINGERPRINT_MISMATCH",
      required: requiredFp,
      actual: actualFp || null,
    });
  }
  if (input.publicationMutatedByResearchSync === true) {
    fails.push({ code: "RESEARCH_SYNC_MUTATED_PUBLICATION" });
  }
  return {
    schema: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
    contract: GDI_RESEARCH_PERSISTENCE_CONTRACT_V1,
    ok: fails.length === 0,
    fails,
    coverage,
    note:
      "Research-stage / archive registry rows need not be customer-published; published IDs must not silently disappear from the research registry.",
  };
}

/**
 * Merge historical reconstruction + publication presentation into a CREATE-only
 * research registry candidate. Does not invent evidence or classifications.
 */
export function buildHistoricalRegistryUpsertCandidate({
  hotelId,
  packageOpportunity,
  reconstructedOpportunity,
  publicationVersion,
  publicationFingerprint,
  migratedAt = new Date().toISOString(),
} = {}) {
  const pkg = packageOpportunity && typeof packageOpportunity === "object"
    ? packageOpportunity
    : null;
  const recon =
    reconstructedOpportunity && typeof reconstructedOpportunity === "object"
      ? reconstructedOpportunity
      : null;
  if (!pkg?.id && !recon?.id) {
    return { ok: false, error: "MISSING_CANONICAL_ID" };
  }
  const id = String(pkg?.id || recon?.id).trim();
  if (!recon) {
    return {
      ok: false,
      error: "MISSING_RECONSTRUCTED_PROVENANCE",
      id,
      quarantine: true,
    };
  }

  const invented = Array.isArray(recon.inventedFields) ? recon.inventedFields : [];
  if (invented.length) {
    return {
      ok: false,
      error: "INVENTED_FIELDS_PRESENT",
      id,
      inventedFields: invented,
      quarantine: true,
    };
  }

  const sources = Array.isArray(recon.sources)
    ? recon.sources
    : Array.isArray(pkg?.sources)
      ? pkg.sources
      : [];

  const candidate = {
    ...recon,
    // Presentation overlays from approved package (contacts/titles/drawers) —
    // stronger customer-facing fields without inventing research body.
    ...(pkg
      ? {
          displayTitle: pkg.displayTitle || recon.displayTitle,
          title: pkg.title || pkg.displayTitle || recon.title || recon.displayTitle,
          organizationName:
            pkg.organizationName ||
            pkg.displayCompanyName ||
            recon.organizationName,
          primaryContact: pkg.primaryContact || recon.primaryContact,
          secondaryContact: pkg.secondaryContact || recon.secondaryContact,
          priority: pkg.priority || recon.priority,
          opportunityType: pkg.opportunityType || recon.opportunityType,
          summaryWhat: pkg.summaryWhat || recon.summaryWhat,
          whatIsHappening: pkg.whatIsHappening || recon.whatIsHappening,
          recommendedAction: pkg.recommendedAction || recon.recommendedAction,
          recommendedNextStep:
            pkg.recommendedNextStep || recon.recommendedNextStep,
          commercialDemandType:
            pkg.commercialDemandType || recon.commercialDemandType,
          revenueStreamClass:
            pkg.revenueStreamClass || recon.revenueStreamClass,
          demandFamily: pkg.demandFamily || recon.demandFamily,
          demandSignalType: pkg.demandSignalType || recon.demandSignalType,
          segment: pkg.segment || recon.segment,
          researchSegment: pkg.researchSegment || recon.researchSegment,
        }
      : {}),
    id,
    opportunityId: id,
    hotelId: hotelId || recon.hotelId || pkg?.hotelId,
    sources,
    sourceCount: sources.length,
    researchRegistryProvenance: {
      schema: "gdi-research-registry-historical-upsert-v1",
      migratedAt,
      sourceArtifacts: [
        "reconstructed-opportunities-v1.json",
        publicationVersion
          ? `publication:${publicationVersion}`
          : "publication-presentation",
      ],
      publicationFingerprint: publicationFingerprint || null,
      reconstructionStatus: recon.reconstructionStatus || null,
      researchStatus: recon.researchStatus || null,
      missingOriginalResearchBody: Boolean(recon.missingOriginalResearchBody),
      historicalAttestationRef: recon.historicalAttestationRef || null,
      provenance: recon.provenance || null,
      createOnly: true,
      doesNotAlterCustomerPublication: true,
    },
    updatedAt: migratedAt,
  };

  const blockers = [];
  if (!candidate.hotelId) blockers.push("hotelId_required");
  if (!String(candidate.title || candidate.displayTitle || "").trim()) {
    blockers.push("title_required");
  }
  if (!sources.length && !recon.provenance?.curatedAttestation) {
    blockers.push("no_sources_and_no_curated_attestation");
  }

  if (blockers.length) {
    return {
      ok: false,
      error: "VALIDATION_BLOCKERS",
      id,
      blockers,
      quarantine: true,
      candidate,
    };
  }

  return {
    ok: true,
    id,
    quarantine: false,
    candidate,
    notes: {
      missingOriginalResearchBody: Boolean(recon.missingOriginalResearchBody),
      sourceCount: sources.length,
      hasPackagePresentation: Boolean(pkg),
    },
  };
}
