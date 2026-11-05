/**
 * GDI_CANONICAL_OPPORTUNITY_CONTRACT_V1
 *
 * Hotel-agnostic customer-facing opportunity field contract.
 * Separates approved commercial presentation from research-only internals.
 * Does not publish, mutate Airtable, or issue share tokens.
 */
import {
  CUSTOMER_FACING_DRAWER_CONTENT_FIELDS,
  CUSTOMER_FACING_PARITY_FIELDS,
  GDI_CUSTOMER_FACING_OPPORTUNITY_CONTRACT_V1,
  prepareCustomerFacingOpportunity,
} from "../share/gdi-customer-facing-opportunity-contract-v1.js";
import {
  REQUIRED_PUBLISHED_COMMERCIAL_FIELDS,
  validateCustomerFacingPublishedSnapshot,
} from "../durable-publication/gdi-customer-facing-published-snapshot-v1.js";
import { SHARE_INTERNAL_ONLY_KEYS } from "../share/share-customer-presentation-parity-v1.js";

export const GDI_CANONICAL_OPPORTUNITY_CONTRACT_V1 =
  "gdi-canonical-opportunity-contract-v1";

/** Approved customer-facing commercial body (must survive publication). */
export const CANONICAL_CUSTOMER_FACING_FIELDS = Object.freeze([
  "id",
  "title",
  "displayTitle",
  "organizationName",
  "commercialInventoryClass",
  "inventoryLane",
  "inventoryLaneLabel",
  "priority",
  "whatIsHappening",
  "potentialHotelBusiness",
  "whyThisHotel",
  "whyActNow",
  "establishedFacts",
  "uncertainties",
  "primaryContact",
  "secondaryContact",
  "recommendedNextStep",
  "qualificationQuestions",
  "sources",
  "evidence",
  "salesActionKey",
  "salesActionPillLabel",
  "datePillDisplayLabel",
  "segmentDisplayLabel",
  "opportunityTypeFilterCategory",
  "opportunityTypeFilterLabel",
]);

/** Research / admin-only — must never reach external shares. */
export const RESEARCH_ONLY_FIELDS = Object.freeze([
  ...SHARE_INTERNAL_ONLY_KEYS,
  "scoreAudit",
  "researchRuns",
  "exaAgentDeepResearch",
  "adaptiveDeepResearch",
  "discoveryCandidateId",
  "dispositionReasonCode",
  "internalNotes",
]);

export {
  CUSTOMER_FACING_DRAWER_CONTENT_FIELDS,
  CUSTOMER_FACING_PARITY_FIELDS,
  REQUIRED_PUBLISHED_COMMERCIAL_FIELDS,
  GDI_CUSTOMER_FACING_OPPORTUNITY_CONTRACT_V1,
  prepareCustomerFacingOpportunity,
  validateCustomerFacingPublishedSnapshot,
};

/**
 * Assert an opportunity carries the canonical customer-facing commercial body.
 * Soft on optional secondary contact / qualification questions (warnings).
 */
export function assertCanonicalCustomerFacingOpportunity(opportunity = {}) {
  const prepared = prepareCustomerFacingOpportunity(opportunity);
  const gate = validateCustomerFacingPublishedSnapshot({
    ...prepared,
    id: prepared.id || opportunity.id,
    title: prepared.title || prepared.displayTitle || opportunity.title,
  });
  const researchLeaks = RESEARCH_ONLY_FIELDS.filter(
    (k) => opportunity?.[k] != null && prepared?.[k] != null,
  );
  return {
    ok: gate.ok,
    contractVersion: GDI_CANONICAL_OPPORTUNITY_CONTRACT_V1,
    fails: gate.fails,
    warnings: [
      ...gate.warnings,
      ...researchLeaks.map((k) => `research_field_present:${k}`),
    ],
    prepared,
  };
}

/**
 * One complete, versioned customer-facing presentation object.
 * Publication freezes this object; auth + share render the same fields.
 * Does not invent content — only shapes approved commercial fields.
 */
export function buildCanonicalPresentationObject(opportunity = {}, opts = {}) {
  const asserted = assertCanonicalCustomerFacingOpportunity(opportunity);
  const o = asserted.prepared;
  const title =
    o.title || o.displayTitle || o.canonicalTitle || o.organizationName || null;
  const classification =
    o.commercialInventoryClass ||
    o.commercialClassification ||
    opportunity.commercialInventoryClass ||
    opportunity.commercialClassification ||
    null;
  const primary = o.primaryContact || null;
  const secondary = o.secondaryContact || null;
  return {
    contractVersion: GDI_CANONICAL_OPPORTUNITY_CONTRACT_V1,
    presentationContractVersion: "gdi-presentation-contract-v1",
    publicationContractVersion: "gdi-publication-contract-v1",
    identity: {
      hotelId: o.hotelId || opts.hotelId || opportunity.hotelId || null,
      opportunityId: o.id || opportunity.id || null,
      canonicalTitle: title,
      organization: o.organizationName || null,
      opportunityType:
        o.opportunityTypeFilterLabel ||
        o.opportunityTypeLabel ||
        o.segmentDisplayLabel ||
        o.segment ||
        null,
    },
    commercialClassification: {
      inventoryClass: classification,
      priority: o.priority || null,
      salesRelevance: o.salesActionPillLabel || o.salesActionKey || null,
      recommendedAction:
        o.recommendedNextStep || o.recommendedAction || o.recommendedNextAction || null,
      hotelSourcingStatus: o.sourcingStatus || o.hotelSourcingStatus || null,
      demandTiming: o.datePillDisplayLabel || o.eventDateDisplay || null,
    },
    tile: {
      title,
      commercialCategory: classification,
      relevantDates: o.datePillDisplayLabel || o.eventDateDisplay || null,
      description: o.whatIsHappening || o.summaryWhat || null,
      primaryContact: primary,
      contactRole: primary?.role || primary?.title || null,
      badges: {
        priority: o.priorityPillLabel || o.priority || null,
        salesAction: o.salesActionPillLabel || null,
        inventoryClass: classification,
      },
    },
    drawer: {
      opportunityOverview: o.whatIsHappening || o.summaryWhat || null,
      whatIsHappening: o.whatIsHappening || o.summaryWhat || null,
      potentialBusinessForThisHotel:
        o.potentialHotelBusiness || o.potentialBusiness || null,
      whyThisHotel: o.whyThisHotel || o.summaryWhyHotel || null,
      advantages: o.whyThisHotelAdvantages || null,
      limitationsAndTradeOffs: o.whyThisHotelLimitations || null,
      whyActNow: o.whyActNow || o.whyNow || null,
      establishedFacts: o.establishedFacts || [],
      uncertainties: o.uncertainties || [],
      primaryContact: primary,
      secondaryContacts: secondary ? [secondary] : [],
      contactRelevance: o.contactRelevance || null,
      recommendedNextStep:
        o.recommendedNextStep || o.recommendedAction || o.recommendedNextAction || null,
      qualificationQuestions: o.qualificationQuestions || [],
      supportingEvidence: o.sources || o.evidence || [],
    },
    publicationMetadata: {
      presentationContractVersion: "gdi-presentation-contract-v1",
      contentHash: opts.contentHash || null,
      publicationVersion: opts.publicationVersion || o.publishedVersion || null,
      publicationTimestamp: opts.publicationTimestamp || o.publishedAt || null,
      publishedPresentationLocked:
        o.publishedPresentationLocked === true ||
        opportunity.publishedPresentationLocked === true ||
        opts.lock === true,
    },
    /** Flat approved opportunity retained for renderers / publication freeze. */
    opportunity: {
      ...o,
      title,
      displayTitle: title,
      canonicalTitle: title,
      commercialInventoryClass: classification,
      commercialClassification: classification,
      publishedPresentationLocked:
        o.publishedPresentationLocked === true ||
        opportunity.publishedPresentationLocked === true ||
        opts.lock === true,
    },
    gate: {
      ok: asserted.ok,
      fails: asserted.fails,
      warnings: asserted.warnings,
    },
  };
}
