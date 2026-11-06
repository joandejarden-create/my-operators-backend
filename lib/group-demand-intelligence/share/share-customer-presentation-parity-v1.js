/**
 * Share ↔ auth customer presentation parity V1.
 *
 * External client-share must reuse the same customer-facing list/detail
 * presentation as the authenticated GDI dashboard. Share sanitize remains for
 * capability gating / evidence tiering, but must not discard sales filters,
 * classification pills, tile contacts, or date presentation.
 */
import { toOpportunityListDto } from "../opportunity-list-dto.js";

export const SHARE_CUSTOMER_PRESENTATION_PARITY_V1 =
  "gdi-share-customer-presentation-parity-v1";

/** Fields that must never appear on external client-share payloads. */
export const SHARE_INTERNAL_ONLY_KEYS = Object.freeze([
  "founderReviewDecision",
  "founderReviewComment",
  "founderCustomerVisibilityDisable",
  "founderVisibilityDisableReason",
  "researchContinuationFocus",
  "exaAgentDeepResearch",
  "internalResearchEligible",
  "monitoringEligible",
  "doesNotAffectProduction",
  "localShadowOnly",
  "stageEShadowPreview",
  "liveCampaignV1Shadow",
  "certificationFixtureOnly",
  "gdiMaturityReason",
  "crossHotelContamination",
  "__waterstonePackDebug",
  "scoreAuditInternal",
  "generatorOnly",
  "isTestData",
]);

/**
 * Presentation fields that auth list DTOs expose and share clients need for
 * cards / Sales Opportunities filters. Copied from canonical DTO when sanitize
 * path would otherwise drop them.
 */
export const SHARE_PRESENTATION_PARITY_KEYS = Object.freeze([
  "title",
  "displayTitle",
  "drawerHeaderTitle",
  "organizationName",
  "displayCompanyName",
  "priority",
  "priorityFilterKey",
  "priorityPillLabel",
  "salesActivationCategory",
  "salesActivationLabel",
  "salesActivationKey",
  "salesActionKey",
  "salesActionPillLabel",
  "salesActionReason",
  "commercialCategory",
  "commercialStageKey",
  "commercialStagePillLabel",
  "salesOpportunityQuickFilterKeys",
  "salesOpportunityQuickFiltersVersion",
  "hotelSourcingStatusNormalized",
  "hotelSourcingStatusLabel",
  "commercialStageFilterKey",
  "datePillDisplayLabel",
  "tileEventStartDate",
  "tileEventEndDate",
  "eventDateDisplay",
  "canonicalTiming",
  "timingType",
  "cardWhyNowLine",
  "tileContactName",
  "tileContactRoleLabel",
  "tileContactPreview",
  "recommendedNextAction",
  "recommendedAction",
  "segmentDisplayLabel",
  "opportunityTypeFilterLabel",
  "opportunityTypeFilterCategory",
  "demandFamily",
  "demandFamilyLabel",
  "showSalesActionPill",
  "showPriorityPill",
  "cardPillStandard",
  "primaryContact",
  "whoPathClass",
  "buyerRole",
]);

function stripInternalOnly(dto = {}) {
  const out = { ...dto };
  for (const k of SHARE_INTERNAL_ONLY_KEYS) {
    if (k in out) delete out[k];
  }
  return out;
}

/**
 * Build the share list DTO from the canonical opportunity (auth parity),
 * then strip internal-only keys. Prefer this over sanitize→DTO.
 */
export function buildShareOpportunityListDto(opportunity = {}, opts = {}) {
  const canonical = toOpportunityListDto(opportunity, opts);
  return {
    ...stripInternalOnly(canonical),
    sharePresentationParityVersion: SHARE_CUSTOMER_PRESENTATION_PARITY_V1,
    mode: "read_only",
  };
}

/**
 * Merge sanitized detail (evidence tiering / contact package) onto the
 * presentation-complete canonical row without losing sales/commercial fields.
 *
 * Locked published opportunities: approvedCustomerFacingDetail is authoritative.
 * sanitizeOpportunityForShare uses a whitelist that drops that nested object —
 * restore it here and flatten its customer-facing fields onto the detail DTO
 * so EXTERNAL_SHARE drawers render the complete approved presentation.
 */
export function buildShareOpportunityDetailDto(
  opportunity = {},
  sanitized = null,
  opts = {},
) {
  const listParity = buildShareOpportunityListDto(opportunity, opts);
  const san = sanitized && typeof sanitized === "object" ? sanitized : {};
  const approved =
    opportunity?.approvedCustomerFacingDetail &&
    typeof opportunity.approvedCustomerFacingDetail === "object"
      ? opportunity.approvedCustomerFacingDetail
      : null;
  const locked =
    opportunity?.publishedPresentationLocked === true ||
    opportunity?.gdiPublishedInventoryRegistry === true ||
    Boolean(approved);

  // Locked / frozen path: approved customer-facing object is authoritative.
  // Flatten it for drawer renderers; keep the nested object for full-content
  // parity certification. Do not spread the raw research opportunity.
  if (locked && approved) {
    const classification =
      opportunity.commercialInventoryClass ||
      opportunity.commercialClassification ||
      approved.commercialInventoryClass ||
      approved.commercialClassification ||
      null;
    const potential =
      approved.potentialHotelBusiness ||
      approved.potentialBusiness ||
      opportunity.potentialHotelBusiness ||
      opportunity.potentialBusiness ||
      null;
    const sources =
      Array.isArray(approved.sources) && approved.sources.length
        ? approved.sources
        : Array.isArray(opportunity.sources) && opportunity.sources.length
          ? opportunity.sources
          : [];
    const mergedLocked = {
      ...approved,
      id: opportunity.id || approved.id || null,
      hotelId: opportunity.hotelId || null,
      publishedPresentationLocked: true,
      gdiPublishedInventoryRegistry:
        opportunity.gdiPublishedInventoryRegistry === true,
      approvedCustomerFacingDetail: approved,
      customerFacingPresentationHash:
        opportunity.customerFacingPresentationHash ||
        opportunity.fullContentParityHash ||
        null,
      fullContentParityHash: opportunity.fullContentParityHash || null,
      customerFacingSchemaPathCount:
        opportunity.customerFacingSchemaPathCount || null,
      sources,
      potentialHotelBusiness: potential,
      potentialBusiness: potential,
      commercialInventoryClass: classification,
      commercialClassification: classification,
      // Locked presentation fields from freeze — never let list DTO re-derive.
      salesActionKey:
        opportunity.salesActionKey ||
        approved.salesActionKey ||
        null,
      salesActionPillLabel:
        opportunity.salesActionPillLabel ||
        approved.salesActionPillLabel ||
        null,
      priorityFilterKey:
        opportunity.priorityFilterKey ||
        approved.priorityFilterKey ||
        opportunity.priority ||
        approved.priority ||
        null,
      priority:
        opportunity.priority ||
        approved.priority ||
        opportunity.priorityFilterKey ||
        approved.priorityFilterKey ||
        null,
      tileContactName:
        opportunity.tileContactName ||
        approved.tileContactName ||
        approved.primaryContact?.name ||
        null,
      tileContactRoleLabel:
        opportunity.tileContactRoleLabel ||
        approved.tileContactRoleLabel ||
        approved.primaryContact?.role ||
        approved.primaryContact?.title ||
        null,
    };
    // Fill only missing presentation keys from list DTO — never overwrite
    // approved / locked values with a re-derived list projection.
    for (const k of SHARE_PRESENTATION_PARITY_KEYS) {
      if (
        (mergedLocked[k] == null || mergedLocked[k] === "") &&
        listParity[k] != null &&
        listParity[k] !== ""
      ) {
        mergedLocked[k] = listParity[k];
      }
    }
    return stripInternalOnly({
      ...mergedLocked,
      approvedCustomerFacingDetail: approved,
      sharePresentationParityVersion: SHARE_CUSTOMER_PRESENTATION_PARITY_V1,
      mode: "read_only",
    });
  }

  const merged = {
    ...san,
    ...listParity,
  };
  // Prefer canonical presentation keys explicitly (san may null them out).
  for (const k of SHARE_PRESENTATION_PARITY_KEYS) {
    if (listParity[k] != null && listParity[k] !== "") {
      merged[k] = listParity[k];
    } else if (san[k] != null && merged[k] == null) {
      merged[k] = san[k];
    }
  }
  // Keep sanitize evidence/contact package when richer.
  if (Array.isArray(san.evidence) && san.evidence.length) {
    merged.evidence = san.evidence;
  }
  if (Array.isArray(opportunity.sources) && opportunity.sources.length) {
    merged.sources = opportunity.sources;
  } else if (Array.isArray(san.sources) && san.sources.length && !merged.sources?.length) {
    merged.sources = san.sources;
  }
  if (san.contactPackage && !merged.contactPackage) {
    merged.contactPackage = san.contactPackage;
  }
  if (san.commercialIntelligenceBrief && !merged.commercialIntelligenceBrief) {
    merged.commercialIntelligenceBrief = san.commercialIntelligenceBrief;
  }
  // Full-content aliases — must match authenticated presentation fields.
  const potential =
    merged.potentialHotelBusiness ||
    san.potentialHotelBusiness ||
    opportunity.potentialHotelBusiness ||
    opportunity.potentialBusiness ||
    null;
  merged.potentialHotelBusiness = potential;
  merged.potentialBusiness = potential;
  merged.commercialInventoryClass =
    merged.commercialInventoryClass ||
    san.commercialInventoryClass ||
    opportunity.commercialInventoryClass ||
    opportunity.commercialClassification ||
    null;
  merged.commercialClassification =
    merged.commercialClassification ||
    san.commercialClassification ||
    opportunity.commercialClassification ||
    opportunity.commercialInventoryClass ||
    null;
  return stripInternalOnly({
    ...merged,
    ...(approved ? { approvedCustomerFacingDetail: approved } : {}),
    sharePresentationParityVersion: SHARE_CUSTOMER_PRESENTATION_PARITY_V1,
    mode: "read_only",
  });
}

/**
 * Diff two list DTOs for certification (substantive customer-visible fields).
 */
export function diffSharePresentationFields(canonicalDto = {}, shareDto = {}) {
  const diffs = [];
  for (const k of SHARE_PRESENTATION_PARITY_KEYS) {
    const a = canonicalDto[k];
    const b = shareDto[k];
    const as =
      a == null
        ? ""
        : Array.isArray(a)
          ? JSON.stringify([...a].map(String).sort())
          : typeof a === "object"
            ? JSON.stringify(a)
            : String(a).trim();
    const bs =
      b == null
        ? ""
        : Array.isArray(b)
          ? JSON.stringify([...b].map(String).sort())
          : typeof b === "object"
            ? JSON.stringify(b)
            : String(b).trim();
    if (as === bs) continue;
    diffs.push({
      field: k,
      canonical: Array.isArray(a) ? a : as.slice(0, 240),
      share: Array.isArray(b) ? b : bs.slice(0, 240),
      severity:
        /salesOpportunityQuickFilterKeys|salesAction|salesActivation|title|tileContact|datePill|recommended/i.test(
          k,
        )
          ? "substantive"
          : "presentation",
    });
  }
  return diffs;
}
