/**
 * Sparse opportunity projection for list/browse opens.
 * Full opportunity (evidence, competitors, sources, theses, audits) stays on detail GET.
 * Live Commercial Quality V1: include weekly + commercial summary fields for browse.
 *
 * Card PRESENTATION is owned by the UI tile (pre-V5A Bethesda structure).
 * Customer ELIGIBILITY is owned by customer-visibility / active-eligibility gates.
 * Do not couple presentation enrichment into this DTO.
 */

import { formatCustomerDate, unknownDisplay } from "./live-commercial-quality-v1.js";
import { DEMAND_FAMILY_LABEL } from "./demand-signal-types.js";
import { OPPORTUNITY_TYPE_LABEL } from "./claim-types.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER_LABEL,
  normalizeOpportunityDemandClassification,
} from "./opportunity-type-filter-v1.js";
import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import { evaluateGdiSummaryQuality } from "./opportunity-summary-v1.js";
import { isLegacyVisibilityCompatible } from "./legacy-visibility-compatibility-v1.js";
import {
  buildContactDisplaySurface,
  applyContactDisplaySurfaceToOpportunity,
} from "./contact-display-surface-v1.js";
import {
  isAssessmentStatusProse,
  scrubProviderBrandNames,
  reconcileHotelFitScoreForDisplay,
} from "./customer-presentation-sanitize-v1.js";
import { deriveProspectCommercialSection } from "./priority-action-window/assignment-contract-v1.js";
import { qualifyCanonicalOpportunity } from "./canonical-commercial-contract-v1.js";
import { classifyInventoryPresentation } from "./inventory-visibility-v1.js";
import { normalizeScalingListPresentation } from "./scaling-tile-customer-summary-v1.js";
import { attachCustomerClassificationPresentation } from "./customer-classification-presentation-v1.js";
import { attachEventsOpportunityOverview } from "./events-opportunity-overview-v1.js";
import { applyUniversalTimingOverlay } from "./universal-timing-intelligence-v1.js";
import { LMC_SUBSEGMENT_LABEL } from "./workflow/local-meetings-catering-research-v1.js";
import { resolveCanonicalCustomerFacingTitle } from "./demand-segments/universal-opportunity-identity-v1.js";
import { customerFacingOpportunityTitle } from "./customer-facing-opportunity-title-contract-v1.js";
import { composeCustomerFacingHotelSourcingLabel } from "./commercial-consistency/commercial-status-dimensions-v1.js";

/** Demand-mechanism / research-axis suffixes historically appended after " — ". */
const DEMAND_MECHANISM_TITLE_SUFFIXES = new Set([
  ...Object.values(LMC_SUBSEGMENT_LABEL || {}),
  "Corporate meetings, training, board meetings & workshops",
  "Professional planners & intermediaries with recurring event business",
  "Scaling Corporate Account",
  "Corporate Account Relationship",
]);

/**
 * Tile titles show the account / event name only — never "Org — demand mechanism".
 * Mechanism belongs in the segment line (Local Catering, Scaling, etc.).
 */
export function stripDemandMechanismTitleSuffix(title = "", organizationName = "") {
  const full = String(title || "").replace(/\s+/g, " ").trim();
  const org = String(organizationName || "").replace(/\s+/g, " ").trim();
  if (!full) return full;
  const parts = full.split(/\s*[—–]\s+/);
  if (parts.length < 2) return full;
  const suffix = parts.slice(1).join(" — ").trim();
  const suffixIsMechanism =
    DEMAND_MECHANISM_TITLE_SUFFIXES.has(suffix) ||
    /^(corporate meetings|professional planners|scaling corporate account|meeting-room rental|association lunches|nonprofit fundraisers|social catering)\b/i.test(
      suffix,
    );
  if (!suffixIsMechanism) return full;
  const head = parts[0].trim();
  if (org && (head.toLowerCase() === org.toLowerCase() || org.toLowerCase().startsWith(head.toLowerCase()) || head.toLowerCase().startsWith(org.toLowerCase()))) {
    return org;
  }
  return head || org || full;
}

export {
  isAssessmentStatusProse,
  scrubProviderBrandNames,
  reconcileHotelFitScoreForDisplay,
} from "./customer-presentation-sanitize-v1.js";

function salesFacingSummaryField(primary, fallback) {
  const a = scrubProviderBrandNames(primary);
  if (a && !isAssessmentStatusProse(a)) return a;
  const b = scrubProviderBrandNames(fallback);
  if (b && !isAssessmentStatusProse(b)) return b;
  return null;
}

export const GDI_OPPORTUNITY_LIST_SCHEMA = "gdi_opportunity_list_v2";

/** Strip provider brand names from customer-facing evidence copy. */
export function scrubCustomerEvidenceCopy(text) {
  return scrubProviderBrandNames(text);
}

function slimPrimaryContact(contact) {
  if (!contact || typeof contact !== "object") return null;
  // Explicit allowlist — never pass Surfe/provider/cost/job metadata to list API
  // Do not surface organization inbox as person email (emailOwnership gate)
  const emailOwnership = contact.emailOwnership || null;
  const email = emailOwnership === "organization_or_department" ? null : contact.email || null;
  return {
    name: contact.name || null,
    role: contact.role || contact.title || null,
    title: contact.title || null,
    email,
    emailOwnership: email ? emailOwnership || "person_specific" : null,
    phone: contact.phone || null,
    phoneTypeLabel:
      contact.phoneTypeLabel ||
      (contact.phone && (!contact.phoneType || /unknown/i.test(String(contact.phoneType)))
        ? "Phone"
        : contact.phoneTypeLabel) ||
      null,
    relevanceEvidence: scrubCustomerEvidenceCopy(
      contact.relevanceEvidence || contact.whyThisContact || null
    ),
    suggestedContactLabel: contact.suggestedContactLabel || null,
    organizationContactRouteLabel: contact.organizationContactRouteLabel || null,
    askToBeDirectedTo: contact.askToBeDirectedTo || null,
  };
}

/** Customer-safe secondary / alternative named contact (optional; omit when empty). */
function slimSecondaryContact(contact) {
  if (!contact || typeof contact !== "object" || !contact.name) return null;
  const why =
    contact.whyThisContact ||
    contact.relationshipToOpportunity ||
    contact.whyRelevant ||
    contact.commercialRelevance ||
    null;
  return {
    name: contact.name || null,
    title: contact.title || contact.role || null,
    role: contact.role || contact.title || null,
    organization: contact.organization || null,
    relevantResponsibility: contact.relevantResponsibility || null,
    relationshipToOpportunity: scrubCustomerEvidenceCopy(
      contact.relationshipToOpportunity || contact.whyRelevant || null
    ),
    // Stage D/E: allow secondary card without email/phone when relevance is evidenced
    whyThisContact: scrubCustomerEvidenceCopy(why),
    commercialClass: contact.commercialClass || null,
    contactRelevanceLabel: contact.contactRelevanceLabel || null,
    contractingAuthorityStatus: contact.contractingAuthorityStatus || null,
    contactClassification: contact.contactClassification || null,
    contactClassificationLabel: contact.contactClassificationLabel || null,
    email: contact.email || null,
    phone: contact.phone || null,
    organizationRoutingAlternative: contact.organizationRoutingAlternative || null,
    organizationRoutingLabel: contact.organizationRoutingLabel || null,
    profileUrl: contact.profileUrl || contact.sourceUrl || null,
    sourceUrl: contact.sourceUrl || contact.profileUrl || null,
    evidenceUrls: Array.isArray(contact.evidenceUrls)
      ? contact.evidenceUrls.slice(0, 5)
      : null,
    verificationDate: contact.verificationDate || null,
  };
}

/** Acronyms preserved in tile/header Proper Case (presentation only). */
const TILE_SECONDARY_HEADLINE_ACRONYMS = Object.freeze({
  plus: "PLUS",
  arias: "ARIAS",
  nih: "NIH",
  nyc: "NYC",
  dmc: "DMC",
  rfp: "RFP",
  tbd: "TBD",
  mba: "MBA",
  csi: "CSI",
  dc: "DC",
  anade: "ANADE",
  canaco: "CANACO",
  amexcap: "AMEXCAP",
  icuh: "ICUH",
  lasa: "LASA",
  andellac: "ANDELLAC",
  amrob: "AMRob",
  comrob: "COMRob",
  smne: "SMNE",
  cdmx: "CDMX",
  mpi: "MPI",
  pco: "PCO",
  ctn: "CTN",
  amwa: "AMWA",
  ecs: "ECS",
  acvnu: "ACVNU",
  sbpo: "SBPO",
  hrhr: "HRHR",
  umd: "UMD",
  ors: "ORS",
});

/**
 * Strip internal research / sourcing commentary from customer-facing titles.
 * Presentation only — does not mutate stored source names.
 */
export function stripResearchCommentaryFromDisplayTitle(raw = "") {
  let s = String(raw || "").replace(/\s+/g, " ").trim();
  if (!s) return s;
  // Drop trailing research parentheses first.
  s = s
    .replace(
      /\s*\((?:next date TBD[^)]*|destination TBD[^)]*|TBD[^)]*|overflow[^)]*|not Bethesda[^)]*)\)\s*$/i,
      "",
    )
    .trim();
  const researchSuffix =
    /^(?:recurring\b.*|recommended[- ]hotel\b.*|hotel information\b.*|destination TBD\b.*|next date TBD\b.*|monitor(?:ing)?\b.*|parents weekend demand\b.*|unconfirmed\b.*|exact next dates\b.*|verified via\b.*|hotel not named\b.*|overflow\b.*|WEWCC\b.*|competitor room block\b.*|\d{4} dates not yet\b.*|dates not yet announced\b.*)$/i;
  const parts = s.split(/\s*[—–]\s+/);
  if (parts.length >= 2) {
    const head = parts[0].trim();
    const rest = parts.slice(1).join(" — ").trim();
    if (researchSuffix.test(rest) || rest.length > 55) {
      return head || s;
    }
  }
  return s;
}

/**
 * Tile/header Proper Case (presentation only).
 * Capitalizes all-lowercase words; preserves short ALL-CAPS, internal capitals,
 * accented proper nouns, and known hospitality acronyms.
 * Does not mutate canonical stored titles — apply only at display boundaries.
 */
const TITLE_CASE_MINOR_WORDS = new Set([
  "a",
  "an",
  "and",
  "as",
  "at",
  "but",
  "by",
  "for",
  "in",
  "nor",
  "of",
  "on",
  "or",
  "the",
  "to",
  "vs",
  "via",
]);

export function toProperCaseDisplayTitle(raw = "") {
  const s = String(raw || "").replace(/\s+/g, " ").trim();
  if (!s) return s;
  // Avoid \\b — breaks accented Spanish tokens (Médicos → Mé + Dicos).
  let wordIndex = 0;
  return s
    .split(/(\s+|[-–—|:/,()]+)/)
    .map((tok) => {
      if (!/[A-Za-zÁÉÍÓÚÑáéíóúñ]/.test(tok)) return tok;
      const lower = tok.toLowerCase();
      if (TILE_SECONDARY_HEADLINE_ACRONYMS[lower]) {
        wordIndex += 1;
        return TILE_SECONDARY_HEADLINE_ACRONYMS[lower];
      }
      if (/^[A-ZÁÉÍÓÚÑ]{2,8}$/.test(tok)) {
        wordIndex += 1;
        return tok;
      }
      if (/^[A-Z0-9]{2,8}$/.test(tok) && /[0-9]/.test(tok)) {
        wordIndex += 1;
        return tok;
      }
      // Preserve brand stylization with internal capitals (McMahon, iPhone-like).
      if (/[A-ZÁÉÍÓÚÑ]/.test(tok.slice(1))) {
        wordIndex += 1;
        return tok;
      }
      // Already title-cased / accented proper noun — leave alone.
      if (
        /^[A-ZÁÉÍÓÚÑ]/.test(tok) &&
        /[a-záéíóúñ]/.test(tok.slice(1))
      ) {
        wordIndex += 1;
        return tok;
      }
      const isFirst = wordIndex === 0;
      wordIndex += 1;
      if (!isFirst && TITLE_CASE_MINOR_WORDS.has(lower)) {
        return lower;
      }
      // Lift remaining all-lowercase words into Title Case (incl. accented).
      return tok.charAt(0).toUpperCase() + tok.slice(1);
    })
    .join("");
}

/**
 * Centralized customer display-title policy: strip research commentary, then
 * Title Case. Does not overwrite source/canonical stored names.
 */
export function normalizeGdiCustomerDisplayTitle(raw = "") {
  const stripped = stripResearchCommentaryFromDisplayTitle(raw);
  return toProperCaseDisplayTitle(stripped) || stripped;
}

/**
 * Browse-card display title (presentation only — does not mutate canonical title).
 *
 * Prefer a concise account/occasion title already stored on the record
 * (e.g. "AHIMA Advocacy Summit 2027"). Do NOT prepend the full legal
 * organization name when that would duplicate the card meta line or inflate
 * research-note leftovers into "Org — Org acronym Occasion".
 */
export function accountLevelDisplayTitle(opportunity = {}) {
  const o = opportunity || {};
  const finish = (title) =>
    customerFacingOpportunityTitle(
      stripResearchCommentaryFromDisplayTitle(title) || title,
      {
        fallback:
          String(o.organizationName || o.displayCompanyName || "").trim() || null,
        applyTitleCase: true,
        competitorHotel:
          o.cmiCompetitorHotel || o.competitorHotel || o.likelyCompetitor || null,
        competitorHotels: [
          o.cmiCompetitorHotel,
          o.competitorHotel,
          o.likelyCompetitor,
          o.venueName,
        ].filter(Boolean),
      }
    ) || null;
  // Single source of truth: rebuild from verified org + programme + segment so
  // stale generic identity.displayTitle values cannot win on tile/drawer/export.
  // Always finish through normalizeGdiCustomerDisplayTitle (acronym-safe Title
  // Case + research-commentary strip) — never leave raw research notes on tiles.
  const canonical = resolveCanonicalCustomerFacingTitle(o);
  if (canonical) {
    // Permanent title contract (v1) — supersedes normalizeGdiCustomerDisplayTitle.
    return finish(canonical);
  }
  // Universal identity pack (Segments 4/9/12+): single canonical displayTitle
  // for tile + drawer — never re-concatenate org — programme.
  const universalTitle = String(
    o.opportunityIdentity?.displayTitle ||
      o.displayTitle ||
      o.drawerHeaderTitle ||
      "",
  ).trim();
  if (
    universalTitle &&
    (o.opportunityIdentity?.version ||
      o.segments4912LiveWaveV1 === true ||
      o.semanticContentVersion)
  ) {
    return finish(toProperCaseDisplayTitle(universalTitle) || universalTitle);
  }
  const isScaling =
    o.researchSegment === "scaling_companies" ||
    /scaling/i.test(String(o.segment || "")) ||
    o.demandFamily === "SCALING_COMPANY" ||
    /Scaling Corporate Account/i.test(String(o.title || ""));
  if (isScaling) {
    const display =
      String(o.displayCompanyName || "").trim() ||
      String(o.organizationName || "")
        .split(/\s*[—–]\s*|\s+-\s+/)[0]
        .replace(
          /,?\s*(S\.?\s*C\.?|S\.?\s*A\.?\s*(?:de\s*)?C\.?\s*V\.?|Inc\.?|LLC)\s*$/i,
          "",
        )
        .trim();
    if (display) return finish(display);
  }
  // Local Catering / account rows: never show "Org — Corporate meetings…" on the tile.
  // Segment line already carries the demand family.
  const isLocalCateringAccount =
    o.researchSegment === "local_meetings_catering" ||
    /local (meetings|catering)/i.test(String(o.segment || o.segmentDisplayLabel || "")) ||
    /^gdi_lmc_/i.test(String(o.id || ""));
  if (isLocalCateringAccount) {
    const orgOnly =
      String(o.organizationName || "").trim() ||
      stripDemandMechanismTitleSuffix(
        o.displayTitle || o.title || "",
        o.organizationName || "",
      );
    if (orgOnly) return finish(toProperCaseDisplayTitle(orgOnly) || orgOnly);
  }
  // Association / Conference / Events — Event Name first (same as drawer header).
  const eventsHeader = String(
    o.drawerHeaderTitle ||
      o.eventsOpportunityOverview?.drawerHeaderTitle ||
      o.eventDisplayName ||
      "",
  ).trim();
  if (
    eventsHeader &&
    (o.researchSegment === "associations_government" ||
      /assoc|conference/i.test(String(o.segment || "")) ||
      o.eventsOpportunityOverview)
  ) {
    return finish(toProperCaseDisplayTitle(eventsHeader) || eventsHeader);
  }
  const org = String(o.organizationName || "").trim();
  // Prefer canonical / normalized event display over raw news headlines in title.
  const title = String(
    o.canonicalEventDisplayTitle ||
      o.displayTitle ||
      o.eventName ||
      o.title ||
      "",
  ).trim();
  if (!title && !org) return null;
  if (!title) return finish(toProperCaseDisplayTitle(org) || null);

  const stripRoleParen = (t) =>
    String(t || "")
      .replace(
        /\s*\((VENUE OPERATOR|ORGANIZER|SECRETARIAT|PARENT SOCIETY|UNIVERSITY HOST|EVENT ORGANIZER|ORGANIZING TEAM)\)\s*$/i,
        ""
      )
      .trim();

  const cleanedRaw = stripRoleParen(title) || title;
  const cleaned =
    stripResearchCommentaryFromDisplayTitle(
      stripDemandMechanismTitleSuffix(cleanedRaw, org) || cleanedRaw,
    ) || cleanedRaw;
  // Permanent title contract after research-commentary / demand-mechanism strip.
  const asDisplay = (t) => finish(t);

  // Research-note / verification titles — strip sourcing commentary for display.
  if (
    /verified via|verified:|hotel not named|hotel block tba|supported recurring|unconfirmed this pass|exact next dates|recommended-hotel|next date TBD|destination TBD|recurring Bethesda/i.test(
      cleaned
    )
  ) {
    const left = cleaned.split(/\s*[—–-]\s*/)[0].trim();
    if (left && left.length <= 90) return asDisplay(left);
  }

  // Concise occasion titles already good as-is (with or without year).
  if (
    cleaned.length <= 90 &&
    !/^https?:/i.test(cleaned) &&
    !/verified via|verified:/i.test(cleaned)
  ) {
    // Org + demand-mechanism suffix already stripped above — prefer org when equal.
    if (org && cleaned.toLowerCase() === org.toLowerCase()) {
      return asDisplay(org);
    }
    // Already org-led with full legal name — keep (stripRoleParen applied).
    if (org && cleaned.toLowerCase().startsWith(org.toLowerCase())) {
      return asDisplay(cleaned);
    }
    // Short occasion title — do NOT prefix full legal org (meta line carries org/segment).
    return asDisplay(cleaned);
  }

  if (org && cleaned.toLowerCase().includes(org.toLowerCase())) {
    return asDisplay(cleaned);
  }

  const role = String(o.participationRole || o.childEntityType || "")
    .replace(/_/g, " ")
    .trim();
  const motionLabel = (role || "Group demand")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .replace(/\s+/g, " ")
    .trim();
  return asDisplay(cleaned || `${org} — ${motionLabel}`);
}

function sanitizePeakRoomsValue(raw) {
  if (raw == null || raw === "") return null;
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  const s = String(raw).trim();
  if (!s) return null;
  // Housing-program status must never appear as a peak-rooms value
  if (/housing\s*program|verified\s*housing|stay[- ]to[- ]play/i.test(s) && !/\d/.test(s)) {
    return null;
  }
  if (/^\d+(\.\d+)?$/.test(s)) return Number(s);
  if (/\d/.test(s) && !/housing|program/i.test(s)) return s;
  return null;
}

function housingProgramFields(o = {}) {
  const status = String(o.housingProgramStatus || o.roomDemandStatus || "").toUpperCase();
  const label = o.housingProgramStatusLabel || o.roomDemandStatusLabel || null;
  if (
    status === "VERIFIED_HOUSING_PROGRAM" ||
    /verified housing program/i.test(String(label || ""))
  ) {
    return {
      housingProgramStatus: "VERIFIED_HOUSING_PROGRAM",
      housingProgramStatusLabel: label || "Verified Housing Program",
    };
  }
  return {
    housingProgramStatus: o.housingProgramStatus || null,
    housingProgramStatusLabel: o.housingProgramStatusLabel || null,
  };
}

export function toOpportunityListDto(opportunity, opts = {}) {
  const rawIn = opportunity || {};
  // Scaling: shared tile presentation (display name, CAR classification, clean summary)
  // so list tiles cannot drift from Stage F drawer identity.
  const raw = attachEventsOpportunityOverview(
    normalizeScalingListPresentation(rawIn),
  );
  // Normalize misfiled top-level contactEmail/contactPhone into display surface
  // so cards never hide a stored usable company phone/email behind "No primary contact".
  const surface = buildContactDisplaySurface(raw);
  const withContact = applyContactDisplaySurfaceToOpportunity(raw, surface);
  // Recover verified timing from structured fields + saved research narrative
  // before projecting sparse list fields (prevents Dates TBD when prose has dates).
  // Then recover demandSegmentId from documented research aliases (never invent).
  const o = normalizeOpportunityDemandClassification(
    applyUniversalTimingOverlay(withContact),
  );
  const peak = sanitizePeakRoomsValue(
    o.peakRooms ?? o.estimatedPeakRooms ?? o.publishedPeakRooms ?? null
  );
  const housing = housingProgramFields(o);
  const att = o.attendance ?? o.estimatedAttendance ?? o.publishedAttendance ?? null;
  const opportunityType = o.opportunityType || null;
  const demandFamily = o.demandFamily || null;
  const buyerRole = o.buyerRole || o.primaryContactRole || o.primaryContact?.role || null;
  const publicContactPath =
    o.publicContactPath ||
    o.organizationContactUrl ||
    o.officialContactPath ||
    o.contactUrl ||
    null;
  const canonical = qualifyCanonicalOpportunity(o);
  const commercialSection = deriveProspectCommercialSection(o, { eligibility: canonical.eligibility });
  const inventoryHotelId = o.hotelId || opts.hotelId || null;
  // Prefer precomputed classification from canonical row (share sanitize strips fields).
  // Always classify for tile commercial-stage pill (pill 2) — visibility allowlist does
  // not gate presentation labels on already-listed opportunities.
  const inventory =
    opts.inventoryPresentation ||
    classifyInventoryPresentation(o, { hotelId: inventoryHotelId });
  const isScalingRow =
    o.researchSegment === "scaling_companies" ||
    /scaling/i.test(String(o.segment || "")) ||
    demandFamily === "SCALING_COMPANY" ||
    /SCALING_COMPANY/i.test(String(o.demandSignalType || ""));
  const rawTypeLabel =
    o.opportunityTypeLabel ||
    (opportunityType ? OPPORTUNITY_TYPE_LABEL[opportunityType] || null : null);
  const scalingCustomerClassification = "Corporate Account Relationship";
  const customerTypeLabel =
    isScalingRow &&
    (!rawTypeLabel || /^scaling company$/i.test(String(rawTypeLabel).trim()))
      ? scalingCustomerClassification
      : isScalingRow && /^scaling company$/i.test(String(o.segmentDisplayLabel || "").trim())
        ? scalingCustomerClassification
        : rawTypeLabel;
  const customerSegmentLabel =
    isScalingRow
      ? scalingCustomerClassification
      : o.segmentDisplayLabel || null;
  const canonicalCustomerTitle = accountLevelDisplayTitle(o);
  const listDto = {
    id: o.id,
    hotelId: o.hotelId || null,
    title: o.title || null,
    displayTitle: canonicalCustomerTitle,
    displayCompanyName: o.displayCompanyName || null,
    legalEntityName: o.legalEntityName || null,
    segmentDisplayLabel: customerSegmentLabel,
    organizationName: o.organizationName || null,
    buyerEntity: o.buyerEntity || o.organizer || null,
    buyerRole,
    publicContactPath,
    whoPathClass: o.whoPathClass || o.contactPathClass || null,
    segment: o.segment || null,
    priority: o.priority || null,
    // Carry locked published presentation fields through list DTO construction
    // so attachCustomerClassificationPresentation can honor them (production
    // share must not re-derive Contact Buyer / Medium from lean hydration).
    priorityFilterKey: o.priorityFilterKey || null,
    priorityPillLabel: o.priorityPillLabel || null,
    publishedPresentationLocked: o.publishedPresentationLocked === true,
    gdiPublishedInventoryRegistry: o.gdiPublishedInventoryRegistry === true,
    salesActionKey: o.salesActionKey || null,
    salesActionPillLabel: o.salesActionPillLabel || null,
    salesOpportunityQuickFilterKeys: Array.isArray(
      o.salesOpportunityQuickFilterKeys,
    )
      ? o.salesOpportunityQuickFilterKeys
      : null,
    eventStartDate: o.eventStartDate || null,
    eventEndDate: o.eventEndDate || null,
    eventDateStatus: o.eventDateStatus || null,
    eventDateGranularity: o.eventDateGranularity || null,
    eventDateDisplay: o.eventDateDisplay || formatCustomerDate(o),
    eventTiming: o.eventTiming || null,
    eventYear: o.eventYear || null,
    eventMonth: o.eventMonth || null,
    tileEventStartDate: o.tileEventStartDate || null,
    tileEventEndDate: o.tileEventEndDate || null,
    datePillDisplayLabel: o.datePillDisplayLabel || null,
    canonicalTiming: o.canonicalTiming || null,
    timingType: o.canonicalTiming?.timingType || o.timingType || null,
    activeDateClass: o.activeDateClass || null,
    opportunityType,
    opportunityTypeLabel: customerTypeLabel,
    // Sales-intelligence packet V1 — customer card category / evidence / timing line
    commercialCategory: o.commercialCategory || null,
    salesActivationCategory: o.salesActivationCategory || null,
    salesActivationLabel: o.salesActivationLabel || null,
    accountFamilyId: o.accountFamilyId || null,
    needDateRelevanceClass: o.needDateRelevanceClass || null,
    evidenceStatusLabel: o.evidenceStatusLabel || null,
    demandEvidenceLabel: o.demandEvidenceLabel || null,
    cardWhyNowLine: o.cardWhyNowLine || null,
    researchSegment: o.researchSegment || null,
    demandSegmentId: o.demandSegmentId || null,
    segmentDisplayLabel: o.segmentDisplayLabel || null,
    opportunityTypeFilterCategory: classifyOpportunityTypeFilter(o),
    opportunityTypeFilterLabel:
      OPPORTUNITY_TYPE_FILTER_LABEL[classifyOpportunityTypeFilter(o)] || null,
    scalingDrawerModel: o.scalingDrawerModel || null,
    researchToDrawerV2: o.researchToDrawerV2
      ? {
          version: o.researchToDrawerV2.version || null,
          salesBrief: o.researchToDrawerV2.salesBrief || null,
          researchDetail: o.researchToDrawerV2.researchDetail || null,
          preservation: o.researchToDrawerV2.preservation || null,
        }
      : null,
    segmentOverviewModel: o.segmentOverviewModel || null,
    sharedTenSectionDrawer: o.sharedTenSectionDrawer
      ? {
          version: o.sharedTenSectionDrawer.version || null,
          segmentId: o.sharedTenSectionDrawer.segmentId || null,
          segmentLabel: o.sharedTenSectionDrawer.segmentLabel || null,
          overviewModel: o.sharedTenSectionDrawer.overviewModel || null,
          sectionIds: o.sharedTenSectionDrawer.sectionIds || null,
        }
      : null,
    demandFamily,
    demandFamilyLabel:
      o.demandFamilyLabel || (demandFamily ? DEMAND_FAMILY_LABEL[demandFamily] || null : null),
    demandSignalType: o.demandSignalType || null,
    demandSignalTypeLabel: o.demandSignalTypeLabel || null,
    peVenueId: o.peVenueId || null,
    peSignalId: o.peSignalId || null,
    hotelVenueFitId: o.hotelVenueFitId || null,
    newnessStatus: o.newnessStatus || null,
    recurrenceClass: o.recurrenceClass || null,
    potentialRoomNights: o.potentialRoomNights ?? null,
    potentialRoomNightsStatus: o.potentialRoomNightsStatus || null,
    hotelFitScore: reconcileHotelFitScoreForDisplay(o),
    hotelFitOverallStatus: o.hotelFitOverallStatus || null,
    hotelFitPosture: o.hotelFitPosture || null,
    hotelFitScoreNote: o.hotelFitScoreNote || null,
    hotelFitRationale: o.hotelFitRationale || null,
    hotelFitQualitativeComponents: o.hotelFitQualitativeComponents || null,
    hotelFitDimensionAssessments: o.hotelFitDimensionAssessments || null,
    hotelFitCalculationBasis: o.hotelFitCalculationBasis || null,
    hotelFitAssessmentVersion: o.hotelFitAssessmentVersion || null,
    hotelFitProvisional: o.hotelFitProvisional === true,
    hotelFitPostureNote: o.hotelFitPostureNote || null,
    hotelFitAssessedAt: o.hotelFitAssessedAt || null,
    physicalFitScore: o.physicalFitScore ?? null,
    geographyFitScore: o.geographyFitScore ?? null,
    timingScore: o.timingScore ?? null,
    commercialValueScore: o.commercialValueScore ?? null,
    historicalFitScore: o.historicalFitScore ?? null,
    competitiveAccessibilityScore: o.competitiveAccessibilityScore ?? null,
    contactabilityScore: o.contactabilityScore ?? null,
    evidenceConfidence: o.evidenceConfidence ?? null,
    evidenceConfidenceExplanation: o.evidenceConfidenceExplanation || null,
    summaryWhat: salesFacingSummaryField(
      o.tileCustomerSummary || o.summaryWhat,
      o.summaryWhyMatters,
    ),
    tileCustomerSummary: o.tileCustomerSummary || null,
    // Same executive summary as the tile — drawer "What is happening"
    whatIsHappening: o.whatIsHappening || o.tileCustomerSummary || null,
    // Hotel-specific thesis for drawer / Why This Hotel — do not fall back to
    // org summaryWhat (that caused repeated hotel-agnostic copy on cards).
    hotelOpportunityThesis: salesFacingSummaryField(o.hotelOpportunityThesis, null),
    summaryWhyHotel: salesFacingSummaryField(
      o.summaryWhyHotel,
      o.fitExplanation || o.hotelFitRationale || null
    ),
    fitExplanation: o.fitExplanation || null,
    // Coordinator hotel-relevance research (mandatory before qualify/cards)
    hotelRelevanceResearch: o.hotelRelevanceResearch || null,
    hotelRelevanceResearchVersion: o.hotelRelevanceResearchVersion || null,
    hotelRelevanceDisposition: o.hotelRelevanceDisposition || null,
    hotelRelevanceResearchedAt: o.hotelRelevanceResearchedAt || null,
    destinationStatus: o.destinationStatus || null,
    eventCity: o.eventCity || null,
    venueCity: o.venueCity || null,
    whyNow: o.whyNow || null,
    recommendedAction:
      o.recommendedAction ||
      o.recommendedNextStep ||
      o.recommendedNextAction ||
      null,
    recommendedNextStep:
      o.recommendedNextStep ||
      o.recommendedAction ||
      o.recommendedNextAction ||
      null,
    opportunityQualification: o.opportunityQualification || null,
    opportunityQualificationLabel: o.opportunityQualificationLabel || null,
    bookingWindowStatus: o.bookingWindowStatus || null,
    bookingWindowLabel: o.bookingWindowLabel || null,
    demandTerritoryFit: o.demandTerritoryFit || null,
    demandTerritoryFitLabel: o.demandTerritoryFitLabel || null,
    primaryContact: slimPrimaryContact(o.primaryContact || surface.primaryContact),
    secondaryContact: slimSecondaryContact(o.secondaryContact),
    // Universal tile footer: one name + approved role (or Buyer Not Yet Identified)
    tileContactPreview: o.tileContactPreview
      ? {
          version: o.tileContactPreview.version || null,
          kind: o.tileContactPreview.kind || null,
          name: o.tileContactPreview.name || null,
          roleLabel: o.tileContactPreview.roleLabel || null,
          displayName: o.tileContactPreview.displayName || null,
          displayRole: o.tileContactPreview.displayRole || null,
          displayRoleKind: o.tileContactPreview.displayRoleKind || null,
          professionalTitle: o.tileContactPreview.professionalTitle || null,
          opportunityRole: o.tileContactPreview.opportunityRole || null,
          drawerSlot: o.tileContactPreview.drawerSlot || null,
          buyerEligibleForGate: o.tileContactPreview.buyerEligibleForGate === true,
          buyerEligibility: o.tileContactPreview.buyerEligibility || null,
        }
      : null,
    tileContactName: o.tileContactName || null,
    tileContactRoleLabel: o.tileContactRoleLabel || null,
    functionalContactEmail: surface.functionalContactEmail || o.functionalContactEmail || null,
    organizationContactEmail:
      surface.organizationContactEmail ||
      o.organizationContactEmail ||
      o.functionalContactEmail ||
      null,
    companyPhone: surface.companyPhone || o.companyPhone || null,
    companyPhoneLabel: surface.companyPhoneLabel || o.companyPhoneLabel || null,
    contactEmail: surface.functionalContactEmail || null,
    contactPhone: surface.companyPhone || null,
    contactUrl: surface.contactUrl || publicContactPath || null,
    contactDisplayLines: surface.cardLines || null,
    contactDisplaySurfaceVersion: surface.version || null,
    namedBuyerEnrichmentComplete: surface.namedBuyerEnrichmentComplete === true,
    contactChannelSeparation: {
      suggestedPerson: surface.contactChannelSeparation?.suggestedPerson || null,
      personSpecificEmail: surface.contactChannelSeparation?.personSpecificEmail || null,
      personSpecificPhone: surface.contactChannelSeparation?.personSpecificPhone || null,
      organizationRoute: surface.contactChannelSeparation?.organizationRoute || null,
      companyPhone: surface.contactChannelSeparation?.companyPhone || null,
      askToBeDirectedTo: surface.contactChannelSeparation?.askToBeDirectedTo || null,
    },
    contactPathClass: o.contactPathClass || o.whoPathClass || null,
    // Reviewed prospect publication (customer-safe labels; not Ready)
    prospectPublicationBasis: o.prospectPublicationBasis || null,
    prospectReviewCardId: o.prospectReviewCardId || null,
    prospectReviewSection: commercialSection.section,
    prospectReviewSectionSource: commercialSection.source,
    prospectCommercialClassLabel: commercialSection.label,
    hotelFacing: canonical.publication.hotelFacing,
    historicalCustomerVisible: canonical.publication.historicalCustomerVisible,
    publicationEligibility: canonical.publication.eligibility,
    demandMechanism: canonical.demand.mechanism,
    accountDevelopmentPublished: false,
    prospectCustomerLabel: o.prospectCustomerLabel || o.prospectCommercialLabel || null,
    prospectCommercialLabel: o.prospectCommercialLabel || null,
    priorityReason: o.priorityReason || null,
    bookingWindowReason: o.bookingWindowReason || null,
    priorityActionAssignmentVersion: o.priorityActionAssignmentVersion || null,
    venueSourcingStatusLabel:
      o.venueSourcingStatusLabel ||
      o.hotelSourcingStatusLabel ||
      o.eventsOpportunityOverview?.hotelSourcing ||
      null,
    hotelSourcingStatusLabel:
      composeCustomerFacingHotelSourcingLabel(o) ||
      o.hotelSourcingStatusLabel ||
      o.venueSourcingStatusLabel ||
      o.eventsOpportunityOverview?.hotelSourcing ||
      null,
    hotelSourcingStatusNormalized:
      o.hotelSourcingStatusNormalized || null,
    hotelSourcingStatus:
      o.hotelSourcingStatus ||
      o.hotelSourcingStatusLabel ||
      o.venueSourcingStatusLabel ||
      null,
    eventTypeLabel:
      o.eventTypeLabel || o.eventsOpportunityOverview?.eventTypeLabel || null,
    eventSubtype: o.eventSubtype || null,
    tileClassificationLine: (() => {
      const composed = composeCustomerFacingHotelSourcingLabel(o);
      const ov = o.eventsOpportunityOverview;
      if (!ov) return null;
      if (!composed) return ov.tileClassificationLine || null;
      const org = ov.organizerName || "";
      const type = ov.eventTypeLabel || o.eventTypeLabel || "";
      return [org, type, composed]
        .filter((p) => p && p !== "Organizer TBD")
        .join(" · ");
    })(),
    // Hotel revenue-stream icons (list tiles) — attached by customer sanitize.
    revenueStreamClass: o.revenueStreamClass || null,
    revenueStreamLabel: o.revenueStreamLabel || null,
    revenueStreamIcons: o.revenueStreamIcons || null,
    revenueStreamIconsAria: o.revenueStreamIconsAria || null,
    revenueStreamIconsVersion: o.revenueStreamIconsVersion || null,
    // Commercial surface disposition — primary list vs research/archive.
    commercialSurfaceDisposition: o.commercialSurfaceDisposition || null,
    commercialSurfaceLabel: o.commercialSurfaceLabel || null,
    primaryCommercialEligible:
      o.primaryCommercialEligible == null ? null : o.primaryCommercialEligible === true,
    commercialSurfaceReason: o.commercialSurfaceReason || null,
    // Need-Date Intelligence tile calendar preview + universal V2 indicator (always present).
    tileNeedDatePreview: o.tileNeedDatePreview || null,
    needDateIndicator: o.needDateIndicator || null,
    needDateMatchV1: o.needDateMatchV1 || null,
    guestroomThesis: o.guestroomThesis || null,
    eventFnbThesis: o.eventFnbThesis || null,
    hotelDemandHypotheses: o.hotelDemandHypotheses || null,
    bookingResponsibilityLabel: o.bookingResponsibilityLabel || null,
    primaryContactPhoneTypeLabel:
      o.primaryContactPhoneTypeLabel || o.primaryContact?.phoneTypeLabel || null,
    contactCompletenessGroup: o.contactCompletenessGroup ?? null,
    prospectHotelFacing: o.prospectHotelFacing === true,
    inventoryLane: inventory?.lane || o.inventoryLane || null,
    inventoryLaneLabel: inventory?.label || o.inventoryLaneLabel || null,
    inventorySalesReady: inventory ? inventory.salesReady === true : null,
    inventoryWhyNotSalesReady: inventory
      ? (inventory.readyFailed || []).slice(0, 8)
      : o.inventoryWhyNotSalesReady || null,
    // Market Scope (Hotel vs Competitive) — required on share/auth list DTOs.
    inventoryPopulations: Array.isArray(o.inventoryPopulations)
      ? o.inventoryPopulations
      : null,
    inventoryPopulationPrimary: o.inventoryPopulationPrimary || null,
    hotelMarketScopeEligible:
      o.hotelMarketScopeEligible == null
        ? null
        : o.hotelMarketScopeEligible === true,
    competitiveMarketScopeEligible:
      o.competitiveMarketScopeEligible == null
        ? null
        : o.competitiveMarketScopeEligible === true,
    competitiveStatus: o.competitiveStatus || null,
    competitiveStatusLabel: o.competitiveStatusLabel || null,
    cmiVerified: o.cmiVerified === true,
    // Inventory exclusion flags for sales-filter counts (never customer-share fixtures)
    certificationFixtureOnly: o.certificationFixtureOnly === true,
    archived: o.archived === true || o.isArchived === true,
    isArchived: o.isArchived === true || o.archived === true,
    isTestData: o.isTestData === true,
    // Shadow commercial packaging (local sales workspace) — not Ready/SALES_READY
    commercialProspectTier: o.commercialProspectTier || o.shadowCommercialTier || null,
    dosGrade: o.dosGrade || null,
    // Commercial customer-readiness (distinct from inventorySalesReady / structural drawer)
    customerReadinessDisposition:
      o.customerReadinessDisposition ||
      o.contactCommercialDisposition ||
      o.customerReadinessAssessment?.disposition ||
      null,
    contactCommercialDisposition:
      o.contactCommercialDisposition || o.customerReadinessDisposition || null,
    customerReadinessAssessment: o.customerReadinessAssessment
      ? {
          version: o.customerReadinessAssessment.version || null,
          disposition: o.customerReadinessAssessment.disposition || null,
          demandClass: o.customerReadinessAssessment.demandClass || null,
          timingClass: o.customerReadinessAssessment.timingClass || null,
          recommendBuyerResearch:
            o.customerReadinessAssessment.recommendBuyerResearch === true,
          recommendRemoveFromActiveCustomerView:
            o.customerReadinessAssessment.recommendRemoveFromActiveCustomerView ===
            true,
          founderVisibilityReviewRequired:
            o.customerReadinessAssessment.founderVisibilityReviewRequired === true,
          autoHideApplied:
            o.customerReadinessAssessment.autoHideApplied === true,
          rationale: o.customerReadinessAssessment.rationale || null,
        }
      : null,
    commercialEligibilityOverlay: o.commercialEligibilityOverlay
      ? {
          version: o.commercialEligibilityOverlay.version || null,
          disposition: o.commercialEligibilityOverlay.disposition || null,
          appliedAt: o.commercialEligibilityOverlay.appliedAt || null,
          autoHideApplied:
            o.commercialEligibilityOverlay.autoHideApplied === true,
        }
      : null,
    localShadowPortfolio: o.localShadowPortfolio === true,
    founderReviewDecision: o.founderReviewDecision || null,
    founderReviewComment: o.founderReviewComment || null,
    contactPersonhood: o.contactPersonhood || null,
    hasVerifiedNamedRelevantContact: o.hasVerifiedNamedRelevantContact === true,
    isFunctionalOrganizationRoute: o.isFunctionalOrganizationRoute === true,
    hasCredibleOutreachRoute: o.hasCredibleOutreachRoute === true,
    peakRoomsClaimKind: o.peakRoomsClaimKind || o.estimatedPeakRoomsClaimKind || null,
    roomDemandStatus: o.roomDemandStatus || null,
    roomDemandStatusLabel:
      housing.housingProgramStatus === "VERIFIED_HOUSING_PROGRAM"
        ? null
        : o.roomDemandStatusLabel || null,
    housingStatus: o.housingStatus || null,
    housingStatusLabel: o.housingStatusLabel || null,
    housingProgramStatus: housing.housingProgramStatus,
    housingProgramStatusLabel: housing.housingProgramStatusLabel,
    estimatedPeakRooms: peak,
    peakRooms: peak,
    peakRoomsStatus: o.peakRoomsStatus || null,
    eventLocationSummary: o.eventLocationSummary || null,
    estimatedAttendance: att,
    attendance: att,
    attendanceStatus: o.attendanceStatus || null,
    // Same canonical title as tile/list/export — never an independent stale header.
    drawerHeaderTitle: canonicalCustomerTitle || o.drawerHeaderTitle || null,
    eventsOpportunityOverview: (() => {
      const ov = o.eventsOpportunityOverview;
      if (!ov || typeof ov !== "object") return ov || null;
      const composed = composeCustomerFacingHotelSourcingLabel(o);
      if (!composed) return ov;
      const org = ov.organizerName || "";
      const type = ov.eventTypeLabel || o.eventTypeLabel || "";
      return {
        ...ov,
        hotelSourcing: composed,
        tileClassificationLine: [org, type, composed]
          .filter((p) => p && p !== "Organizer TBD")
          .join(" · "),
        venueConfirmationNote:
          /Competitor venue confirmed/i.test(composed) &&
          (o.cmiCompetitorHotel || o.competitorHotel)
            ? `Competitor venue confirmed (${o.cmiCompetitorHotel || o.competitorHotel}). Target-hotel overflow or secondary lodging is not confirmed — do not treat as an open host RFP.`
            : ov.venueConfirmationNote,
      };
    })(),
    weeklyDeltaState: o.weeklyDeltaState || null,
    isNewThisWeek: o.isNewThisWeek === true,
    eventSeriesId: o.eventSeriesId || null,
    relatedOpportunityCount: Array.isArray(o.relatedOpportunityIds)
      ? o.relatedOpportunityIds.length
      : o.relatedOpportunityCount || 0,
    customerFacingState: o.customerFacingState || null,
    customerSurfaceDisposition: o.customerSurfaceDisposition || null,
    gdiMaturityState: o.gdiMaturityState || null,
    gdiMaturityReason: o.gdiMaturityReason || null,
    expansionPilotId: o.expansionPilotId || null,
    parentDemandSignalId: o.parentDemandSignalId || null,
    parentDemandSignalLabel: o.parentDemandSignalLabel || null,
    parentDemandSignalType: o.parentDemandSignalType || null,
    travelingCohortType: o.travelingCohortType || null,
    travelingCohortSummary: o.travelingCohortSummary || null,
    travelingCohortConfidence: o.travelingCohortConfidence || null,
    lodgingControlHypothesis: o.lodgingControlHypothesis || null,
    lodgingControlSummary: o.lodgingControlSummary || null,
    lodgingControlConfidence: o.lodgingControlConfidence || null,
    // Future Watch early-warning card (customer-safe; not Ready)
    watchCardWhyMatters: o.watchCardWhyMatters || null,
    watchCardCurrentStatus: o.watchCardCurrentStatus || null,
    watchCardLodgingController: o.watchCardLodgingController || null,
    watchCardHotelSelectionStatus: o.watchCardHotelSelectionStatus || null,
    watchCardWhenToAct: o.watchCardWhenToAct || null,
    watchCardNextTrigger: o.watchCardNextTrigger || o.nextTriggerCondition || null,
    watchCardMonitoringStatus: o.watchCardMonitoringStatus || null,
    watchCardMonitoringLastChecked: o.watchCardMonitoringLastChecked || null,
    watchCardMonitoringNextExpected: o.watchCardMonitoringNextExpected || null,
    watchCardRecommendedNextStep: o.watchCardRecommendedNextStep || o.recommendedAction || null,
    hotelSelectionProcess: o.hotelSelectionProcess || null,
    outreachReadiness: o.outreachReadiness || null,
    nextTriggerCondition: o.nextTriggerCondition || null,
    // Pursuit workflow mirror (sales status — independent of Ready/Watch)
    pursuitId: o.pursuitId || null,
    pursuitStatus: o.pursuitStatus || null,
    canStartPursuit:
      o.canStartPursuit === true ||
      /^(OUTREACH_NOW|OUTREACH_PREPARE)$/i.test(String(o.outreachReadiness || "")),
    modeledRoomsMin: o.modeledRoomsMin ?? null,
    modeledRoomsMax: o.modeledRoomsMax ?? null,
    modeledDemandDisclaimer: o.modeledDemandDisclaimer || null,
    lodgingVerified: o.lodgingVerified === true,
    headcountVerified: o.headcountVerified === true,
    salesWorkflowState: o.salesWorkflowState || null,
    buyerResearchStatus: o.buyerResearchStatus || null,
    recommendedNextAction:
      o.recommendedNextAction ||
      o.recommendedAction ||
      o.recommendedNextStep ||
      null,
    missingValidation: Array.isArray(o.missingValidation) ? o.missingValidation : null,
    // Internal readiness / convergence diagnostics (UI may ignore)
    readinessState: o.customerReadiness?.state || isGdiCustomerOpportunityReady(o).state || null,
    summaryQuality: o.summaryQuality || evaluateGdiSummaryQuality(o).quality || null,
    legacyCompatibility: isLegacyVisibilityCompatible(o),
    // Visibility stamps required for exact-ID merge/API/browser parity audits.
    // List responses are already customer-facing filtered; these fields must survive
    // DTO projection so consumers do not falsely re-exclude valid facing rows.
    customerVisible: o.customerVisible !== false,
    waterstonePrimarySalesInventory: o.waterstonePrimarySalesInventory === true,
    waterstoneInventoryLane: o.waterstoneInventoryLane || null,
    waterstoneCleanInventoryV1: o.waterstoneCleanInventoryV1 || null,
    localActiveInventory: o.localActiveInventory !== false,
    localInventorySuperseded: o.localInventorySuperseded === true,
    localShadowOnly: o.localShadowOnly === true,
    liveCampaignV1Shadow: o.liveCampaignV1Shadow === true,
    stageEShadowPreview: o.stageEShadowPreview === true,
    doesNotAffectProduction: o.doesNotAffectProduction === true,
    prospectSuppressed: o.prospectSuppressed === true,
    gdiListScope: "CUSTOMER_FACING",
    _listDto: true,
    schemaVersion: GDI_OPPORTUNITY_LIST_SCHEMA,
  };
  // Align Priority + Action Timing filter keys with tile pills (no invented values).
  return attachCustomerClassificationPresentation(listDto);
}

export function mapOpportunitiesToListDto(opportunities, opts = {}) {
  return (opportunities || []).map((o) =>
    toOpportunityListDto(o, {
      hotelId: opts.hotelId || o?.hotelId || null,
    }),
  );
}

export { unknownDisplay };
