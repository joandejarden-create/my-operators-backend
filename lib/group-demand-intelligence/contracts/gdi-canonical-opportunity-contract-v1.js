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
