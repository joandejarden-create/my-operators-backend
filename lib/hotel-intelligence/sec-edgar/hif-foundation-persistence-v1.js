/**
 * HIF Foundation — Airtable destination mapping + write classification.
 * Reuses existing HPC/HI tables only. No schema invention.
 * CUSTOMER_BLOCKED.
 */

import {
  GRAPH_BASE_ID_CANONICAL,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  HPC_TABLE_ID,
  HI_ORGANIZATIONS_TABLE_ID,
  HI_GRAPH_EVIDENCE_TABLE_ID,
} from "./discovery-inventory-airtable-contract-v23.js";
import { TABLE_IDS as CONTRACT_SCHEMA_IDS } from "./hi-hotel-contracts-milestones-schema-v4.js";

export const HIF_FOUNDATION_PERSISTENCE = "hif-foundation-persistence-v1";

export const WRITE_CATEGORY = Object.freeze({
  AUTO_PERSIST_ELIGIBLE: "AUTO_PERSIST_ELIGIBLE",
  STAGED_FOR_REVIEW: "STAGED_FOR_REVIEW",
  CONFLICT_HOLD: "CONFLICT_HOLD",
  RESEARCH_ONLY: "RESEARCH_ONLY",
});

/** Canonical destinations on HPC base — verified live 2026-11-21. */
export const FOUNDATION_DESTINATIONS = Object.freeze({
  baseId: GRAPH_BASE_ID_CANONICAL,
  hotelPropertyCensus: {
    name: "Hotel Property Census",
    tableId: HPC_TABLE_ID,
    role: "Canonical hotel universe — verified attributes only; no auto-create from SEC/CB research",
  },
  discoveryInventory: {
    name: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
    tableId: CONTRACT_SCHEMA_IDS.inventory,
    role: "Matched + unmatched discovery hotel identities",
    idempotencyKey: MAP_DISCOVERY_INVENTORY.discoveryPropertyId,
  },
  discoveryFindings: {
    name: TABLE_HI_DISCOVERY_FINDINGS,
    tableId: CONTRACT_SCHEMA_IDS.findings,
    role: "Discrete contractual / brand / management / commercial findings",
    idempotencyKey: MAP_DISCOVERY_FINDINGS.findingId,
  },
  hotelContracts: {
    name: "HI Hotel Contracts",
    tableId: "tblMjUCBtXF2o3r9u",
    role: "Executed hotel agreement records (named-hotel only)",
    idempotencyKey: "Contract ID",
  },
  contractMilestones: {
    name: "HI Contract Milestones",
    tableId: "tbljzAYjYs3dyHWZk",
    role: "Dated contractual milestones linked to contracts",
    idempotencyKey: "Milestone ID",
  },
  organizations: {
    name: "HI Organizations",
    tableId: HI_ORGANIZATIONS_TABLE_ID,
    role: "Verified orgs — not registrant-by-default",
  },
  hotelOrgRelationships: {
    name: "HI Hotel–Org Relationships",
    tableId: CONTRACT_SCHEMA_IDS.relationships,
    role: "Requires HPC hotel link — never for unmatched-only properties",
  },
  graphEvidence: {
    name: "HI Graph Evidence",
    tableId: HI_GRAPH_EVIDENCE_TABLE_ID,
    role: "HPC-linked evidence only — never unmatched discovery",
  },
});

/**
 * Finding Type values that exist in Airtable (do not invent).
 * Commercial opportunities map onto BRAND_AFFILIATION / MANAGEMENT / OTHER.
 */
export const EXISTING_FINDING_TYPES = Object.freeze({
  CONTRACT_MILESTONE: "CONTRACT_MILESTONE",
  OWNERSHIP: "OWNERSHIP",
  MANAGEMENT: "MANAGEMENT",
  BRAND_AFFILIATION: "BRAND_AFFILIATION",
  TRANSACTION: "TRANSACTION",
  GEOGRAPHY: "GEOGRAPHY",
  IDENTITY_ALIAS: "IDENTITY_ALIAS",
  SUBSIDIARY: "SUBSIDIARY",
  OTHER: "OTHER",
});

/** Map commercial category → existing Finding Type (no new select options). */
export function mapCommercialCategoryToFindingType(category) {
  const c = String(category || "").toUpperCase();
  if (c.includes("BRAND") || c === "BCT" || c.includes("FRANCHISE") || c.includes("CONVERSION")) {
    return EXISTING_FINDING_TYPES.BRAND_AFFILIATION;
  }
  if (c.includes("MANAGEMENT") || c === "MCT" || c.includes("OPERATOR")) {
    return EXISTING_FINDING_TYPES.MANAGEMENT;
  }
  if (c.includes("TRANSACTION") || c.includes("ACQUISITION") || c.includes("SALE")) {
    return EXISTING_FINDING_TYPES.TRANSACTION;
  }
  if (c.includes("CONTRACT") || c.includes("MILESTONE") || c.includes("EXPIR")) {
    return EXISTING_FINDING_TYPES.CONTRACT_MILESTONE;
  }
  return EXISTING_FINDING_TYPES.OTHER;
}

export function commercialOpportunityFieldName(category) {
  const c = String(category || "").toUpperCase();
  if (c.includes("BRAND") || c === "BCT") return "COMMERCIAL_OPPORTUNITY_BCT";
  if (c.includes("MANAGEMENT") || c === "MCT") return "COMMERCIAL_OPPORTUNITY_MCT";
  if (c.includes("CONCESSION")) return "COMMERCIAL_SIGNAL_CONCESSION_ADJACENT";
  if (c.includes("DEVELOPMENT")) return "COMMERCIAL_SIGNAL_HOTEL_DEVELOPMENT_ADJACENT";
  if (c.includes("REDEVELOP")) return "COMMERCIAL_SIGNAL_REDEVELOPMENT_ADJACENT";
  return "COMMERCIAL_SIGNAL_OTHER";
}

/**
 * Classify a proposed write. Only AUTO_PERSIST_ELIGIBLE may auto-write.
 */
export function classifyWriteCategory(row = {}) {
  if (row.researchOnly === true || row.speculation === true) {
    return { category: WRITE_CATEGORY.RESEARCH_ONLY, reason: "UNVERIFIED_OR_SPECULATIVE" };
  }
  if (row.conflictHold === true || row.identityConflict === true) {
    return { category: WRITE_CATEGORY.CONFLICT_HOLD, reason: "IDENTITY_OR_FACT_CONFLICT" };
  }
  if (row.schemaGap === true || row.requiresNewField === true) {
    return { category: WRITE_CATEGORY.STAGED_FOR_REVIEW, reason: "SCHEMA_GAP_OR_NEEDS_REVIEW" };
  }
  if (row.stagedForReview === true) {
    return { category: WRITE_CATEGORY.STAGED_FOR_REVIEW, reason: "EXPLICIT_STAGING" };
  }

  // HPC auto-create never eligible from research alone
  if (row.destination === "Hotel Property Census" && row.createHpc === true) {
    return { category: WRITE_CATEGORY.RESEARCH_ONLY, reason: "HPC_AUTO_CREATE_FORBIDDEN" };
  }

  // Graph evidence / relationships require HPC match
  if (
    (row.destination === "HI Graph Evidence" || row.destination === "HI Hotel–Org Relationships") &&
    !row.hpcRecordId
  ) {
    return { category: WRITE_CATEGORY.RESEARCH_ONLY, reason: "REQUIRES_HPC_MATCH" };
  }

  if (!row.evidenceSupported || !row.sourceUrl) {
    return { category: WRITE_CATEGORY.STAGED_FOR_REVIEW, reason: "MISSING_EVIDENCE_OR_SOURCE" };
  }

  if (row.identityClass === "AMBIGUOUS_PROPERTY" || row.identityClass === "EXTRACTION_ARTIFACT") {
    return { category: WRITE_CATEGORY.RESEARCH_ONLY, reason: "IDENTITY_NOT_VERIFIED" };
  }

  if (
    row.destination === TABLE_HI_HOTEL_DISCOVERY_INVENTORY ||
    row.destination === TABLE_HI_DISCOVERY_FINDINGS ||
    row.destination === "HI Hotel Contracts" ||
    row.destination === "HI Contract Milestones"
  ) {
    if (row.verified === true || row.identityClass === "VERIFIED_PROPERTY" || row.identityClass === "PROBABLE_PROPERTY") {
      return { category: WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE, reason: "VERIFIED_EXISTING_DESTINATION" };
    }
  }

  return { category: WRITE_CATEGORY.STAGED_FOR_REVIEW, reason: "DEFAULT_STAGING" };
}

/**
 * Build Discovery Inventory fields for a verified hotel identity (non-destructive create plan).
 */
export function buildDiscoveryInventoryFields(hotel = {}) {
  const fields = {
    [MAP_DISCOVERY_INVENTORY.discoveryPropertyId]: hotel.discoveryPropertyId,
    [MAP_DISCOVERY_INVENTORY.hotelNameNormalized]: hotel.hotelNameNormalized || hotel.hotelName,
    [MAP_DISCOVERY_INVENTORY.hotelNameRaw]: hotel.hotelNameRaw || hotel.hotelName,
    [MAP_DISCOVERY_INVENTORY.city]: hotel.city || null,
    [MAP_DISCOVERY_INVENTORY.stateRegion]: hotel.stateProvince || hotel.stateRegion || null,
    [MAP_DISCOVERY_INVENTORY.country]: hotel.physicalCountry || hotel.country || null,
    [MAP_DISCOVERY_INVENTORY.address]: hotel.address || null,
    [MAP_DISCOVERY_INVENTORY.currentBrand]: hotel.currentBrand || null,
    [MAP_DISCOVERY_INVENTORY.historicalBrand]: hotel.historicalBrand || null,
    [MAP_DISCOVERY_INVENTORY.identityClass]: hotel.identityClass || "VERIFIED_PROPERTY",
    [MAP_DISCOVERY_INVENTORY.evidenceConfidence]: hotel.evidenceConfidence || "MEDIUM",
    [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: hotel.hpcMatchStatus || "DISCOVERED_NOT_IN_HPC",
    [MAP_DISCOVERY_INVENTORY.lifecycleStatus]: hotel.lifecycleStatus || "IDENTITY_VERIFIED",
    [MAP_DISCOVERY_INVENTORY.persistenceStatus]: "DISCOVERY_PERSISTED",
    [MAP_DISCOVERY_INVENTORY.publicationState]: "CUSTOMER_BLOCKED",
    [MAP_DISCOVERY_INVENTORY.sourceFilingUrl]: hotel.sourceUrl || null,
    [MAP_DISCOVERY_INVENTORY.discoveryMethod]: hotel.discoveryMethod || "CURRENT_BACKWARD_PUBLIC_RESEARCH",
    [MAP_DISCOVERY_INVENTORY.notesQuarantine]: hotel.notes || null,
  };
  for (const k of Object.keys(fields)) {
    if (fields[k] == null || fields[k] === "") delete fields[k];
  }
  return fields;
}

/**
 * Build Discovery Findings fields for a commercial opportunity (uses existing Finding Types).
 */
export function buildCommercialFindingFields(opp = {}, discoveryRecordId = null) {
  const findingType = mapCommercialCategoryToFindingType(opp.commercialCategory || opp.opportunityType);
  const fieldName = commercialOpportunityFieldName(opp.commercialCategory || opp.opportunityType);
  const valueObserved = [
    opp.commercialThesis || opp.commercialTrigger || "",
    opp.commercialRelevance ? `relevance=${opp.commercialRelevance}` : "",
    opp.commercialTiming ? `timing=${opp.commercialTiming}` : "",
    opp.commercialAccessibility ? `accessibility=${opp.commercialAccessibility}` : "",
    opp.evidencePathway ? `pathway=${opp.evidencePathway}` : "",
    opp.engagementRoute ? `engage=${opp.engagementRoute}` : "",
    opp.priority ? `priority=${opp.priority}` : "",
    opp.countsTowardFirst100 === true ? "first100=YES" : opp.countsTowardFirst100 === false ? "first100=NO" : "",
  ]
    .filter(Boolean)
    .join(" | ");

  const fields = {
    [MAP_DISCOVERY_FINDINGS.findingId]: opp.findingId || opp.id || null,
    [MAP_DISCOVERY_FINDINGS.findingType]: findingType,
    [MAP_DISCOVERY_FINDINGS.fieldName]: fieldName,
    [MAP_DISCOVERY_FINDINGS.valueObserved]: valueObserved.slice(0, 90000),
    [MAP_DISCOVERY_FINDINGS.effectiveDate]: opp.nextDecisionDate || opp.effectiveDate || null,
    [MAP_DISCOVERY_FINDINGS.reportingDate]: opp.sourcePublicationDate || opp.reportingDate || null,
    [MAP_DISCOVERY_FINDINGS.sourceUrl]: opp.sourceUrl || opp.sources?.[0]?.url || null,
    [MAP_DISCOVERY_FINDINGS.evidenceSnippet]: (opp.decisionEvidence || opp.evidenceSnippet || "").slice(0, 90000),
    [MAP_DISCOVERY_FINDINGS.confidence]: opp.evidenceConfidence || "MEDIUM",
    [MAP_DISCOVERY_FINDINGS.verificationStatus]: opp.verificationStatus || "EVIDENCE_SUPPORTED",
    [MAP_DISCOVERY_FINDINGS.relatedOrgName]: opp.decisionParty || opp.ownerOrContractingParty || null,
    [MAP_DISCOVERY_FINDINGS.hpcPromotionStatus]: "NOT_PROMOTED",
    [MAP_DISCOVERY_FINDINGS.notes]: opp.notes || `CUSTOMER_BLOCKED | foundation persist | publicRfpRequired=false`,
  };
  if (discoveryRecordId) {
    fields[MAP_DISCOVERY_FINDINGS.discoveryProperty] = [discoveryRecordId];
  }
  for (const k of Object.keys(fields)) {
    if (fields[k] == null || fields[k] === "") delete fields[k];
  }
  return fields;
}

/**
 * Plan a foundation persist batch (creates/updates/no-ops/conflicts).
 */
export function planFoundationPersistBatch(items = []) {
  const plan = {
    creates: [],
    updates: [],
    noops: [],
    conflicts: [],
    researchOnly: [],
    staged: [],
  };

  for (const item of items) {
    const cls = classifyWriteCategory(item);
    if (cls.category === WRITE_CATEGORY.RESEARCH_ONLY) {
      plan.researchOnly.push({ ...item, writeClass: cls });
      continue;
    }
    if (cls.category === WRITE_CATEGORY.CONFLICT_HOLD) {
      plan.conflicts.push({ ...item, writeClass: cls });
      continue;
    }
    if (cls.category === WRITE_CATEGORY.STAGED_FOR_REVIEW) {
      plan.staged.push({ ...item, writeClass: cls });
      continue;
    }
    if (item.existingAirtableId && item.fieldsUnchanged === true) {
      plan.noops.push({ ...item, writeClass: cls });
      continue;
    }
    if (item.existingAirtableId) {
      plan.updates.push({ ...item, writeClass: cls });
    } else {
      plan.creates.push({ ...item, writeClass: cls });
    }
  }

  return {
    ...plan,
    counts: {
      creates: plan.creates.length,
      updates: plan.updates.length,
      noops: plan.noops.length,
      conflicts: plan.conflicts.length,
      researchOnly: plan.researchOnly.length,
      staged: plan.staged.length,
      autoPersistEligible: plan.creates.length + plan.updates.length + plan.noops.length,
    },
  };
}
