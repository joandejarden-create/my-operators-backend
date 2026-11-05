/**
 * GDI_PUBLICATION_CONTRACT_V1
 *
 * Publication must preserve complete approved customer-facing content.
 * Research eligibility ≠ publication authorization.
 * Fail closed if required commercial fields disappear.
 */
import {
  GDI_CUSTOMER_FACING_PUBLISHED_SNAPSHOT_V1,
  REQUIRED_PUBLISHED_COMMERCIAL_FIELDS,
  buildCustomerFacingPublishedSnapshot,
  hydrateOpportunityFromPublishedCommercialFields,
  validateCustomerFacingPublishedSnapshot,
} from "../durable-publication/gdi-customer-facing-published-snapshot-v1.js";
import {
  COMMERCIAL_PUBLICATION_CLASS,
  PUBLICATION_STATE,
  createGdiPublishedInventoryRegistry,
} from "../durable-publication/gdi-published-inventory-registry-v1.js";
import { assertCanonicalCustomerFacingOpportunity } from "./gdi-canonical-opportunity-contract-v1.js";

export const GDI_PUBLICATION_CONTRACT_V1 = "gdi-publication-contract-v1";

/** Silent-loss failure modes that must block publication. */
export const PUBLICATION_SILENT_LOSS_CODES = Object.freeze([
  "whatIsHappening:equals_title",
  "potentialHotelBusiness:equals_title",
  "whatIsHappening:empty",
  "potentialHotelBusiness:empty",
  "establishedFacts:empty",
  "uncertainties:empty",
  "recommendedNextStep:empty",
  "primaryContact:missing_name",
  "sources:empty",
  "whyThisHotel:empty",
  "whyActNow:empty",
]);

export {
  GDI_CUSTOMER_FACING_PUBLISHED_SNAPSHOT_V1,
  REQUIRED_PUBLISHED_COMMERCIAL_FIELDS,
  COMMERCIAL_PUBLICATION_CLASS,
  PUBLICATION_STATE,
  buildCustomerFacingPublishedSnapshot,
  hydrateOpportunityFromPublishedCommercialFields,
  validateCustomerFacingPublishedSnapshot,
  createGdiPublishedInventoryRegistry,
};

/**
 * Build + validate a publishable snapshot. Returns gate result; does not write.
 */
export function preparePublicationSnapshot(opportunity = {}, opts = {}) {
  const canonical = assertCanonicalCustomerFacingOpportunity(opportunity);
  const snapshot = buildCustomerFacingPublishedSnapshot(canonical.prepared, {
    hotelId: opts.hotelId || opportunity.hotelId || null,
    canonicalTitle: opts.canonicalTitle || null,
    classification:
      opts.classification ||
      opportunity.commercialInventoryClass ||
      opportunity.commercialClassification ||
      null,
  });
  const gate = validateCustomerFacingPublishedSnapshot(snapshot);
  const silentLoss = gate.fails.filter((f) =>
    PUBLICATION_SILENT_LOSS_CODES.some((code) => String(f).startsWith(code.split(":")[0])),
  );
  return {
    ok: gate.ok,
    contractVersion: GDI_PUBLICATION_CONTRACT_V1,
    snapshot,
    fails: gate.fails,
    warnings: [...canonical.warnings, ...gate.warnings],
    silentLossBlocked: silentLoss.length > 0,
    silentLoss,
  };
}

/**
 * Compare lean (pre) vs enriched (post) snapshots for silent content loss.
 */
export function detectPublicationContentLoss(before = {}, after = {}) {
  const losses = [];
  const titleNorm = String(after?.title || after?.organizationName || "")
    .trim()
    .toLowerCase();
  const checkText = (field) => {
    const a = String(before?.[field] || "").trim();
    const b = String(after?.[field] || "").trim();
    if (a && !b) losses.push(`${field}:dropped`);
    if (
      field === "whatIsHappening" &&
      b &&
      titleNorm &&
      b.toLowerCase() === titleNorm
    ) {
      losses.push(`${field}:collapsed_to_title`);
    }
  };
  for (const f of [
    "whatIsHappening",
    "potentialHotelBusiness",
    "whyThisHotel",
    "whyActNow",
    "recommendedNextStep",
  ]) {
    checkText(f);
  }
  const arrCheck = (field) => {
    const a = Array.isArray(before?.[field]) ? before[field].length : 0;
    const b = Array.isArray(after?.[field]) ? after[field].length : 0;
    if (a > 0 && b === 0) losses.push(`${field}:emptied`);
    if (a > 0 && b > 0 && b < a) losses.push(`${field}:truncated`);
  };
  for (const f of ["establishedFacts", "uncertainties", "qualificationQuestions", "sources"]) {
    arrCheck(f);
  }
  if (before?.secondaryContact?.name && !after?.secondaryContact?.name) {
    losses.push("secondaryContact:dropped");
  }
  if (
    before?.commercialInventoryClass &&
    after?.commercialInventoryClass &&
    before.commercialInventoryClass !== after.commercialInventoryClass
  ) {
    losses.push("commercialInventoryClass:changed");
  }
  return {
    ok: losses.length === 0,
    losses,
    contractVersion: GDI_PUBLICATION_CONTRACT_V1,
  };
}
