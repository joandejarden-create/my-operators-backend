/**
 * Canonical customer-facing opportunity contract shared by authenticated GDI
 * and external client-share. Presentation only — does not publish or mutate
 * research stores.
 */
import { toOpportunityListDto } from "../opportunity-list-dto.js";
import {
  buildShareOpportunityDetailDto,
  buildShareOpportunityListDto,
  SHARE_INTERNAL_ONLY_KEYS,
} from "./share-customer-presentation-parity-v1.js";
import { stampCustomerFacingRecommendedNextStepAliases } from "./resolve-customer-facing-recommended-next-step-v1.js";
import { hydrateOpportunityFromPublishedCommercialFields } from "../durable-publication/gdi-customer-facing-published-snapshot-v1.js";

export const GDI_CUSTOMER_FACING_OPPORTUNITY_CONTRACT_V1 =
  "gdi-customer-facing-opportunity-contract-v1";

/** Fields compared for Spice Island / share ↔ auth parity certification. */
export const CUSTOMER_FACING_PARITY_FIELDS = Object.freeze([
  "id",
  "title",
  "displayTitle",
  "organizationName",
  "inventoryLane",
  "inventoryLaneLabel",
  "commercialInventoryClass",
  "accountDevelopmentPublished",
  "salesActionKey",
  "salesActionPillLabel",
  "tileContactName",
  "tileContactRoleLabel",
  "recommendedNextStep",
  "recommendedAction",
  "recommendedNextAction",
  "whyThisHotel",
  "summaryWhyHotel",
  "whatIsHappening",
  "summaryWhat",
  "potentialHotelBusiness",
  "potentialBusiness",
  "commercialClassification",
  "whyActNow",
  "whyNow",
  "datePillDisplayLabel",
  "segmentDisplayLabel",
  "opportunityTypeFilterCategory",
  "opportunityTypeFilterLabel",
]);

/** Drawer commercial body fields that must survive publication → share. */
export const CUSTOMER_FACING_DRAWER_CONTENT_FIELDS = Object.freeze([
  "whatIsHappening",
  "potentialHotelBusiness",
  "whyThisHotel",
  "whyActNow",
  "establishedFacts",
  "uncertainties",
  "recommendedNextStep",
  "qualificationQuestions",
  "primaryContact",
  "secondaryContact",
  "sources",
]);

export function prepareCustomerFacingOpportunity(opportunity = {}) {
  return stampCustomerFacingRecommendedNextStepAliases(
    hydrateOpportunityFromPublishedCommercialFields({ ...opportunity }),
  );
}

export function buildAuthCustomerFacingListDto(opportunity = {}, opts = {}) {
  return toOpportunityListDto(prepareCustomerFacingOpportunity(opportunity), opts);
}

export function buildShareCustomerFacingListDto(opportunity = {}, opts = {}) {
  return buildShareOpportunityListDto(
    prepareCustomerFacingOpportunity(opportunity),
    opts,
  );
}

export function buildShareCustomerFacingDetailDto(
  opportunity = {},
  sanitized = null,
  opts = {},
) {
  const prepared = prepareCustomerFacingOpportunity(opportunity);
  return buildShareOpportunityDetailDto(prepared, sanitized, opts);
}

export function assertNoInternalOnlyLeakage(dto = {}) {
  const leaked = SHARE_INTERNAL_ONLY_KEYS.filter((k) => dto[k] != null);
  return { ok: leaked.length === 0, leaked };
}

export function diffCustomerFacingParity(authDto = {}, shareDto = {}) {
  const diffs = [];
  for (const field of CUSTOMER_FACING_PARITY_FIELDS) {
    const a = authDto?.[field];
    const b = shareDto?.[field];
    const as =
      a == null
        ? ""
        : typeof a === "object"
          ? JSON.stringify(a)
          : String(a).trim();
    const bs =
      b == null
        ? ""
        : typeof b === "object"
          ? JSON.stringify(b)
          : String(b).trim();
    if (as === bs) continue;
    diffs.push({
      field,
      auth: as.slice(0, 280),
      share: bs.slice(0, 280),
    });
  }
  return diffs;
}
