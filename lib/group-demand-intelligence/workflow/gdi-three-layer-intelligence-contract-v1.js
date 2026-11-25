/**
 * Dealality Three-Layer Intelligence Contract V1
 *
 * Permanent architectural standard for GDI (reusable pattern for other
 * Dealality intelligence products). Enforced at the persistence boundary.
 *
 * Layer 1 — Research Source of Truth: what we know + evidence
 * Layer 2 — Hotel-Specific Qualification: actionable for this hotel?
 * Layer 3 — Commercial Presentation: what sales needs to know/do
 *
 * ONE coherent architecture. No competing SoT databases.
 * Enrich verified intelligence; never silently degrade it.
 */

import {
  GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
  SOT_AUTHORITATIVE_RESEARCH_KEYS,
  SOT_CERTIFICATION_OWNED_KEYS,
  SOT_DERIVED_PRESENTATION_KEYS,
  SOT_TELEMETRY_DROP_KEYS,
  classifySotPayloadKey,
  REVENUE_STREAM_STRENGTH,
} from "./gdi-research-sot-field-ownership-v1.js";
import {
  GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
  mergeResearchSotLossless,
  detectSotMergeConflicts,
} from "./gdi-research-sot-lossless-merge-v1.js";
/** Align with RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY (no bridge import — avoid cycles). */
export const THREE_LAYER_WRITE_SET_POLICY = Object.freeze({
  VALIDATED_FQO_ONLY: "VALIDATED_FQO_ONLY",
});

export const GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1 =
  "gdi-three-layer-intelligence-contract-v1";

export const INTELLIGENCE_LAYER = Object.freeze({
  RESEARCH_SOT: "LAYER_1_RESEARCH_SOT",
  HOTEL_QUALIFICATION: "LAYER_2_HOTEL_SPECIFIC_QUALIFICATION",
  COMMERCIAL_PRESENTATION: "LAYER_3_COMMERCIAL_PRESENTATION",
  TELEMETRY: "TELEMETRY_NON_SOT",
});

export const FOUNDER_PRINCIPLES = Object.freeze({
  discoverBroadlyQualifyRigorously:
    "Discover broadly. Research thoroughly. Qualify rigorously. Persist validated opportunities. Preserve rejected research as supporting intelligence, not as active hotel opportunities.",
  cumulativeIntelligence:
    "Research intelligence is a cumulative asset. New research must enrich existing verified intelligence, never silently degrade, erase or replace it with a thinner representation.",
  multiHotelIndependentFqo:
    "An opportunity can qualify for multiple hotels, but qualification must be independently established for every hotel.",
  presentationDerivedNotAuthoritative:
    "Customer-facing cards, drawers and summaries are derived views, not authoritative research records.",
  publicationIndependence:
    "Research persistence, founder STAGED activation and customer CURRENT publication are separate, independently authorized operations.",
});

/** Existing architecture mapping — reuse, do not replace. */
export const EXISTING_ARCHITECTURE_MAPPING = Object.freeze({
  layer1Surfaces: Object.freeze([
    "GDI Hotel Research Opportunities (tblbkDj0vhQLfrssV) — researchPayloadJson SoT",
    "Demand Generators",
    "Demand Programs",
    "Demand Generator Signals",
    "Hotel Demand Generator Fit",
    "Group Demand Opportunities (GDO) — distinct write-routing; not overwritten by research bridge",
  ]),
  layer2Surfaces: Object.freeze([
    "researchPayloadJson.hotelSpecificQualification",
    "FQO six-rule contract (gdi-finalized-qualified-opportunity-contract-v1)",
    "VALIDATED_FQO_ONLY persistence write policy",
    "commercialEligibilityStatus (org/GDO routing — NOT hotel FQO failure)",
  ]),
  layer3Surfaces: Object.freeze([
    "STAGED publication packages (founder review)",
    "CURRENT publication packages (customer)",
    "Slim / drawer / card presentation builders",
    "Sales-action pills and display titles",
  ]),
  forbidden: Object.freeze([
    "Competing research database",
    "Three independent opportunity engines",
    "Wholesale slim-over-fat researchPayloadJson replacement",
    "Cross-hotel FQO transfer",
    "Presentation overwrite of authoritative research",
  ]),
});

export const FIELD_OWNERSHIP_TABLE = Object.freeze([
  {
    fieldGroup: "Historical research and evidence",
    primaryAuthority: INTELLIGENCE_LAYER.RESEARCH_SOT,
    permittedModification: "Verified enrichment; corrections with provenance",
  },
  {
    fieldGroup: "Organization identities and relationships",
    primaryAuthority: INTELLIGENCE_LAYER.RESEARCH_SOT,
    permittedModification: "Canonical identity reconciliation",
  },
  {
    fieldGroup: "Contacts and verification audits",
    primaryAuthority: INTELLIGENCE_LAYER.RESEARCH_SOT,
    permittedModification: "Verified additions/corrections; preserve history",
  },
  {
    fieldGroup: "Demand and revenue classifications",
    primaryAuthority: INTELLIGENCE_LAYER.RESEARCH_SOT,
    permittedModification: "Evidence-grounded supported reclassification only",
  },
  {
    fieldGroup: "Hotel fit and FQO",
    primaryAuthority: INTELLIGENCE_LAYER.HOTEL_QUALIFICATION,
    permittedModification: "Certified qualification updates",
  },
  {
    fieldGroup: "Sales priority and recommended actions",
    primaryAuthority: INTELLIGENCE_LAYER.HOTEL_QUALIFICATION,
    permittedModification: "Commercial reevaluation under FQO",
  },
  {
    fieldGroup: "Titles, cards and drawer summaries",
    primaryAuthority: INTELLIGENCE_LAYER.COMMERCIAL_PRESENTATION,
    permittedModification:
      "Regenerate display values without rewriting research SoT",
  },
  {
    fieldGroup: "STAGED package",
    primaryAuthority: "FOUNDER_REVIEW_WORKFLOW",
    permittedModification: "Explicit activation only",
  },
  {
    fieldGroup: "CURRENT package",
    primaryAuthority: "CUSTOMER_PUBLICATION_WORKFLOW",
    permittedModification: "Explicit publication approval only",
  },
]);

/**
 * Map a payload key to an intelligence layer.
 */
export function classifyIntelligenceLayer(key) {
  const class_ = classifySotPayloadKey(key);
  if (class_ === "TELEMETRY") return INTELLIGENCE_LAYER.TELEMETRY;
  if (class_ === "CERTIFICATION") return INTELLIGENCE_LAYER.HOTEL_QUALIFICATION;
  if (class_ === "DERIVED_PRESENTATION") {
    return INTELLIGENCE_LAYER.COMMERCIAL_PRESENTATION;
  }
  // AUTHORITATIVE_RESEARCH (+ unknown) → Layer 1
  return INTELLIGENCE_LAYER.RESEARCH_SOT;
}

/**
 * Presentation / slim payloads must never be treated as authoritative SoT.
 */
export function assertPresentationCannotOverwriteResearch({
  livePayload = {},
  presentationPayload = {},
} = {}) {
  const violations = [];
  const live = livePayload && typeof livePayload === "object" ? livePayload : {};
  const pres =
    presentationPayload && typeof presentationPayload === "object"
      ? presentationPayload
      : {};

  for (const key of Object.keys(pres)) {
    const layer = classifyIntelligenceLayer(key);
    if (layer !== INTELLIGENCE_LAYER.RESEARCH_SOT) continue;
    if (live[key] === undefined || live[key] === null || live[key] === "") {
      continue;
    }
    // If presentation proposes a thinner/weaker value for an existing SoT key,
    // that is a contract violation when used as a write candidate.
    const liveStr = JSON.stringify(live[key]);
    const presStr = JSON.stringify(pres[key]);
    if (liveStr === presStr) continue;

    if (key === "revenueStreamClass") {
      const liveStrength = REVENUE_STREAM_STRENGTH[String(live[key])] || 0;
      const presStrength = REVENUE_STREAM_STRENGTH[String(pres[key])] || 0;
      if (presStrength < liveStrength) {
        violations.push({
          code: "PRESENTATION_REVENUE_DOWNGRADE",
          key,
          live: live[key],
          presentation: pres[key],
        });
      }
    } else if (Array.isArray(live[key]) && live[key].length > 0) {
      if (!Array.isArray(pres[key]) || pres[key].length < live[key].length) {
        violations.push({
          code: "PRESENTATION_ARRAY_SHRINK",
          key,
          liveCount: live[key].length,
          presentationCount: Array.isArray(pres[key]) ? pres[key].length : 0,
        });
      }
    } else if (
      typeof live[key] === "string" &&
      typeof pres[key] === "string" &&
      pres[key].length < live[key].length * 0.5
    ) {
      violations.push({
        code: "PRESENTATION_NARRATIVE_THINNING",
        key,
        liveChars: live[key].length,
        presentationChars: pres[key].length,
      });
    }
  }

  return {
    ok: violations.length === 0,
    violations,
    contract: GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
  };
}

/**
 * Hotel A's FQO must not transfer to Hotel B.
 */
export function assertMultiHotelFqoIndependence({
  hotelAId,
  hotelBId,
  hotelAQualification = null,
  hotelBQualification = null,
} = {}) {
  const a = String(hotelAId || "");
  const b = String(hotelBId || "");
  const blockers = [];
  if (!a || !b || a === b) {
    blockers.push({ code: "INVALID_HOTEL_PAIR", hotelAId: a, hotelBId: b });
  }
  const aPass = hotelAQualification?.finalizedQualified === true;
  const bPass = hotelBQualification?.finalizedQualified === true;
  // Independence: B is not automatically true because A is true.
  const illegallyTransferred =
    aPass &&
    hotelBQualification == null &&
    // Caller asking "does A approve B?" — answer must be no.
    true;
  return {
    ok: blockers.length === 0,
    hotelAQualified: aPass,
    hotelBQualified: bPass,
    aApprovesB: false,
    illegallyTransferredWouldBe: illegallyTransferred && !bPass,
    note: "Hotel A FQO approval does not approve Hotel B. Contacts may be reused but commercial relevance must be re-verified.",
    blockers,
    contract: GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
  };
}

/**
 * CommercialEligibilityStatus is org/GDO routing — not hotel FQO failure.
 */
export function interpretCommercialEligibilityStatus(status) {
  const s = String(status || "");
  return {
    status: s || null,
    isHotelFqoFailure: false,
    meaning:
      s === "GLOBAL_ORG_ONLY"
        ? "Organization/GDO write-routing — not a hotel-specific FQO failure"
        : "Commercial eligibility routing field",
    contract: GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
  };
}

/**
 * Future Weekly Change audit metadata — stable categories only (no UI).
 */
export function classifyWeeklyChangeMetadata({
  beforePayload = {},
  afterPayload = {},
  beforeQualification = null,
  afterQualification = null,
} = {}) {
  const before = beforePayload && typeof beforePayload === "object" ? beforePayload : {};
  const after = afterPayload && typeof afterPayload === "object" ? afterPayload : {};
  const researchChanges = [];
  const qualificationChanges = [];
  const commercialChanges = [];

  const allKeys = new Set([...Object.keys(before), ...Object.keys(after)]);
  for (const key of allKeys) {
    if (JSON.stringify(before[key]) === JSON.stringify(after[key])) continue;
    const layer = classifyIntelligenceLayer(key);
    const entry = {
      path: key,
      changeType:
        before[key] === undefined
          ? "ADDED"
          : after[key] === undefined
            ? "REMOVED"
            : "UPDATED",
    };
    if (layer === INTELLIGENCE_LAYER.RESEARCH_SOT) {
      if (key === "evidence" || key === "sources") {
        researchChanges.push({ ...entry, category: "NEW_OR_UPDATED_EVIDENCE" });
      } else if (
        key === "contacts" ||
        key === "primaryContact" ||
        key === "backupContacts"
      ) {
        researchChanges.push({ ...entry, category: "CONTACT_CHANGE" });
      } else if (
        key === "competitors" ||
        key === "meetingHistory" ||
        key === "likelyCompetitor"
      ) {
        researchChanges.push({ ...entry, category: "RELATIONSHIP_CHANGE" });
      } else {
        researchChanges.push({ ...entry, category: "FACT_OR_RESEARCH_UPDATE" });
      }
    } else if (layer === INTELLIGENCE_LAYER.HOTEL_QUALIFICATION) {
      qualificationChanges.push({ ...entry, category: "QUALIFICATION_FIELD" });
    } else if (layer === INTELLIGENCE_LAYER.COMMERCIAL_PRESENTATION) {
      commercialChanges.push({ ...entry, category: "PRESENTATION_DERIVED" });
    }
  }

  const bq = beforeQualification || before.hotelSpecificQualification || {};
  const aq = afterQualification || after.hotelSpecificQualification || {};
  const beforeFq = bq.finalizedQualified === true;
  const afterFq = aq.finalizedQualified === true;
  let qualificationLifecycle = "QUALIFICATION_MAINTAINED";
  if (!beforeFq && afterFq) qualificationLifecycle = "NEWLY_QUALIFIED";
  else if (beforeFq && !afterFq) qualificationLifecycle = "QUALIFICATION_LOST";
  else if (!beforeFq && !afterFq) qualificationLifecycle = "RESEARCH_REQUIRED_OR_NONQUALIFIED";

  if (qualificationLifecycle !== "QUALIFICATION_MAINTAINED" || beforeFq !== afterFq) {
    qualificationChanges.push({
      path: "hotelSpecificQualification.finalizedQualified",
      category: qualificationLifecycle,
      changeType: "UPDATED",
    });
  }

  // Sales priority / recommended action
  for (const k of [
    "priority",
    "recommendedAction",
    "recommendedNextStep",
    "salesActionKey",
    "salesActionPillLabel",
  ]) {
    if (JSON.stringify(before[k]) !== JSON.stringify(after[k])) {
      commercialChanges.push({
        path: k,
        category:
          k === "priority"
            ? "PRIORITY_CHANGE"
            : "SALES_RECOMMENDATION_CHANGE",
        changeType: before[k] === undefined ? "ADDED" : "UPDATED",
      });
    }
  }

  return {
    contract: GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
    researchChanges,
    qualificationChanges,
    commercialChanges,
    qualificationLifecycle,
    stableIdentities: {
      opportunityId: after.opportunityId || before.opportunityId || null,
      hotelId: after.hotelId || before.hotelId || null,
      organizationKey: after.organizationKey || before.organizationKey || null,
      eventSeriesId: after.eventSeriesId || before.eventSeriesId || null,
    },
  };
}

/**
 * Persistence-boundary guard: slim/presentation candidate vs live SoT.
 * Fail closed before any Airtable mutation.
 */
export function evaluatePersistenceBoundaryGuard({
  livePayloadJson = null,
  livePayload = null,
  proposedPayloadJson = null,
  proposedPayload = null,
  writeSetPolicy = THREE_LAYER_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
} = {}) {
  const merge = mergeResearchSotLossless({
    livePayloadJson,
    livePayload,
    proposedPayloadJson,
    proposedPayload,
  });

  let live = livePayload;
  if (!live && typeof livePayloadJson === "string") {
    try {
      live = JSON.parse(livePayloadJson || "{}");
    } catch {
      live = {};
    }
  }
  live = live && typeof live === "object" ? live : {};

  let prop = proposedPayload;
  if (!prop && typeof proposedPayloadJson === "string") {
    try {
      prop = JSON.parse(proposedPayloadJson || "{}");
    } catch {
      prop = {};
    }
  }
  prop = prop && typeof prop === "object" ? prop : {};

  const presentationGuard = assertPresentationCannotOverwriteResearch({
    livePayload: live,
    presentationPayload: prop,
  });

  const weekly = merge.ok
    ? classifyWeeklyChangeMetadata({
        beforePayload: live,
        afterPayload: merge.payload,
      })
    : null;

  const blockers = [];
  if (!merge.ok) {
    blockers.push({
      code: merge.code || "MERGE_BLOCKED",
      details: merge.blockers || merge.unresolvedConflicts,
    });
  }
  if (
    writeSetPolicy !== THREE_LAYER_WRITE_SET_POLICY.VALIDATED_FQO_ONLY &&
    writeSetPolicy !== "VALIDATED_FQO_ONLY"
  ) {
    blockers.push({
      code: "WRITE_SET_POLICY_NOT_VALIDATED_FQO_ONLY",
      writeSetPolicy,
    });
  }

  return {
    ok: merge.ok && blockers.length === 0,
    merge,
    presentationGuard,
    weeklyChangeMetadata: weekly,
    blockers,
    ownershipContract: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
    mergeVersion: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
    threeLayerContract: GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
    writeSetPolicy: THREE_LAYER_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
  };
}

export default {
  GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
  INTELLIGENCE_LAYER,
  FOUNDER_PRINCIPLES,
  EXISTING_ARCHITECTURE_MAPPING,
  FIELD_OWNERSHIP_TABLE,
  classifyIntelligenceLayer,
  assertPresentationCannotOverwriteResearch,
  assertMultiHotelFqoIndependence,
  interpretCommercialEligibilityStatus,
  classifyWeeklyChangeMetadata,
  evaluatePersistenceBoundaryGuard,
  // Re-exports for single import surface
  GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
  GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
  SOT_AUTHORITATIVE_RESEARCH_KEYS,
  SOT_CERTIFICATION_OWNED_KEYS,
  SOT_DERIVED_PRESENTATION_KEYS,
  SOT_TELEMETRY_DROP_KEYS,
  mergeResearchSotLossless,
  detectSotMergeConflicts,
};
