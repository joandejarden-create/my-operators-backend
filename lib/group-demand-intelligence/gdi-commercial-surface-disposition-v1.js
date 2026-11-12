/**
 * GDI Commercial Surface Disposition V1
 *
 * Separates what may appear on the PRIMARY commercial opportunity list from
 * research pipeline, market-intelligence themes, and archive.
 *
 * Root cause of founder-visible failures: Bethesda internal union returned
 * family-collapsed 83 cards without applying salesperson/DQ filters, so
 * DISQUALIFIED, out-of-market, and research themes rendered as "opportunities."
 * Research integrity ≠ commercial surface eligibility.
 *
 * Hotel-agnostic rules; Bethesda wires them as the default auth list scope.
 */

import { PRIORITY } from "./claim-types.js";
import { FAMILY_ROLE } from "./research/bethesda-88-commercial-event-family-v1.js";

export const GDI_COMMERCIAL_SURFACE_DISPOSITION_V1 =
  "gdi-commercial-surface-disposition-v1";

/** Canonical surface lanes (reuse existing vocabulary where possible). */
export const COMMERCIAL_SURFACE = Object.freeze({
  COMMERCIAL_OPPORTUNITY: "COMMERCIAL_OPPORTUNITY",
  RESEARCH_PIPELINE: "RESEARCH_PIPELINE",
  MARKET_INTELLIGENCE: "MARKET_INTELLIGENCE",
  ARCHIVE_DISQUALIFIED: "ARCHIVE_DISQUALIFIED",
  LINKED_DUPLICATE: "LINKED_DUPLICATE",
  QUARANTINED: "QUARANTINED",
});

export const COMMERCIAL_SURFACE_LABEL = Object.freeze({
  [COMMERCIAL_SURFACE.COMMERCIAL_OPPORTUNITY]: "Commercial Opportunity",
  [COMMERCIAL_SURFACE.RESEARCH_PIPELINE]: "Research Pipeline",
  [COMMERCIAL_SURFACE.MARKET_INTELLIGENCE]: "Market Intelligence",
  [COMMERCIAL_SURFACE.ARCHIVE_DISQUALIFIED]: "Archive / Disqualified",
  [COMMERCIAL_SURFACE.LINKED_DUPLICATE]: "Linked Research Duplicate",
  [COMMERCIAL_SURFACE.QUARANTINED]: "Quarantined",
});

/** Default auth browse = commercial opportunities only. */
export const GDI_LIST_SURFACE_VIEW = Object.freeze({
  COMMERCIAL: "commercial",
  RESEARCH: "research",
  ARCHIVE: "archive",
  FULL: "full",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

/**
 * Known research-theme / category-watch IDs (no single buyer / no account).
 * Prefer structured hotelFitHonesty; IDs are a hard safety net.
 */
export const KNOWN_MARKET_INTELLIGENCE_IDS = Object.freeze([
  "gdi_opp_marriott_hq_adjacent_corporate_watch",
]);

/**
 * True when the row is a generic demand theme, not a bookable account/event.
 */
export function isMarketIntelligenceResearchTheme(opp = {}) {
  const id = text(opp.id || opp.opportunityId);
  if (KNOWN_MARKET_INTELLIGENCE_IDS.includes(id)) return true;
  if (
    upper(opp.commercialSurfaceDisposition) ===
      COMMERCIAL_SURFACE.MARKET_INTELLIGENCE ||
    upper(opp.surfaceDisposition) === COMMERCIAL_SURFACE.MARKET_INTELLIGENCE ||
    upper(opp.classification) === "MARKET_INTELLIGENCE" ||
    upper(opp.opportunityType) === "MARKET_INTELLIGENCE"
  ) {
    return true;
  }
  const fit = upper(opp.hotelFitHonesty || opp.hotelFitQuality || "");
  if (/CATEGORY_WATCH_NO_SINGLE_BUYER|NO_SINGLE_BUYER|MARKET_INTELLIGENCE_THEME/.test(fit)) {
    return true;
  }
  if (
    /NOT_APPLICABLE_CATEGORY_WATCH/i.test(text(opp.contactResearchStatus)) &&
    !text(opp.organizationName || opp.company || opp.accountName)
  ) {
    return true;
  }
  // Title pattern: "Corporate meetings near …" / corridor theme without account
  const title = text(opp.title || opp.opportunityName);
  if (
    /corporate (meetings|offsites?|training).*(near|corridor|hq)\b/i.test(title) &&
    /needs account|category watch|no single buyer|market (theme|intelligence)/i.test(
      [
        title,
        opp.hotelOpportunityThesis,
        opp.hotelFitNote,
        opp.recommendedSalesAction,
      ]
        .map(text)
        .join(" "),
    )
  ) {
    return true;
  }
  return false;
}

export function isArchiveDisqualified(opp = {}) {
  const priority = upper(opp.priority || opp.priorityFilterKey);
  if (priority === PRIORITY.DISQUALIFIED || priority === "DISQUALIFIED") return true;
  const fit = upper(opp.hotelFitHonesty || "");
  if (/^DESTINATION_OUTSIDE|^CLOSED_DISQUALIFIED|OUT_OF_MARKET_HOST_RESEARCH_GATED/.test(fit)) {
    // OUT_OF_MARKET_HOST_RESEARCH_GATED alone is not always archive — require DQ or OUTSIDE territory
    if (/DESTINATION_OUTSIDE|CLOSED_DISQUALIFIED/.test(fit)) return true;
  }
  if (text(opp.demandTerritoryFit).toUpperCase() === "OUTSIDE" && priority === "DISQUALIFIED") {
    return true;
  }
  if (
    upper(opp.commercialSurfaceDisposition) === COMMERCIAL_SURFACE.ARCHIVE_DISQUALIFIED ||
    upper(opp.category) === "CLOSED_DISQUALIFIED"
  ) {
    return true;
  }
  return false;
}

export function isLinkedOrQuarantined(opp = {}) {
  const role = upper(opp.commercialEventFamilyRole);
  if (role === FAMILY_ROLE.LINKED_RESEARCH || role === "LINKED_RESEARCH") {
    return COMMERCIAL_SURFACE.LINKED_DUPLICATE;
  }
  if (role === FAMILY_ROLE.QUARANTINED || role === "QUARANTINED") {
    return COMMERCIAL_SURFACE.QUARANTINED;
  }
  if (opp.listVisible === false) {
    if (/TITLE_SOURCE_INVALID|QUARANTINE/i.test(text(opp.historicalRecoveryDisposition))) {
      return COMMERCIAL_SURFACE.QUARANTINED;
    }
    return COMMERCIAL_SURFACE.LINKED_DUPLICATE;
  }
  return null;
}

/**
 * Specific prospective opportunity that still needs research (buyer, fit, etc.)
 * — not a theme, not DQ. May appear on Research Pipeline view.
 */
export function isResearchPipelineOpportunity(opp = {}) {
  if (isMarketIntelligenceResearchTheme(opp) || isArchiveDisqualified(opp)) {
    return false;
  }
  if (isLinkedOrQuarantined(opp)) return false;

  // Explicit stamp
  if (
    upper(opp.commercialSurfaceDisposition) === COMMERCIAL_SURFACE.RESEARCH_PIPELINE ||
    upper(opp.surfaceDisposition) === COMMERCIAL_SURFACE.RESEARCH_PIPELINE
  ) {
    return true;
  }

  // Named lodging owner missing + only org inbox → research pipeline (NCI RNA pattern)
  const named =
    text(opp.primaryContactName || opp.primaryContact?.name || opp.contactName) || "";
  const hasNamed = named.length >= 3 && !/buyer not|tbd|unknown|n\/a/i.test(named);
  const orgOnly =
    !hasNamed &&
    (Boolean(opp.organizationContactRoute || opp.functionalContactEmail) ||
      /CCR|inbox|conferences@/i.test(
        text(opp.organizationContactEmail || opp.functionalContactEmail || ""),
      ));
  const lodgingAdjacent =
    /NIH_CAMPUS_LODGING|LODGING_ADJACENT|ACCOMMODATIONS_LIST/i.test(
      text(opp.hotelFitHonesty || ""),
    ) ||
    /accommodations list|lodging.?adjacent/i.test(
      text(opp.hotelOpportunityThesis || opp.hotelFitNote || ""),
    );
  if (orgOnly && lodgingAdjacent && upper(opp.priority) === "WATCHLIST") {
    return true;
  }

  // Thin FUTURE_CYCLE / research continuation without commercial maturity
  if (
    /RESEARCH_CONTINUATION|STILL_MISSING/i.test(
      text(opp.commercialMaturity || opp.historicalRecoveryDisposition || ""),
    ) &&
    upper(opp.priority) === "WATCHLIST" &&
    !hasNamed
  ) {
    return true;
  }

  return false;
}

/**
 * Classify one opportunity into a commercial surface lane.
 */
export function classifyCommercialSurfaceDisposition(opp = {}) {
  const linked = isLinkedOrQuarantined(opp);
  if (linked) {
    return {
      version: GDI_COMMERCIAL_SURFACE_DISPOSITION_V1,
      surface: linked,
      label: COMMERCIAL_SURFACE_LABEL[linked],
      primaryCommercialEligible: false,
      reason: linked,
    };
  }
  if (isMarketIntelligenceResearchTheme(opp)) {
    return {
      version: GDI_COMMERCIAL_SURFACE_DISPOSITION_V1,
      surface: COMMERCIAL_SURFACE.MARKET_INTELLIGENCE,
      label: COMMERCIAL_SURFACE_LABEL[COMMERCIAL_SURFACE.MARKET_INTELLIGENCE],
      primaryCommercialEligible: false,
      reason: "RESEARCH_THEME_NO_ACCOUNT",
    };
  }
  if (isArchiveDisqualified(opp)) {
    return {
      version: GDI_COMMERCIAL_SURFACE_DISPOSITION_V1,
      surface: COMMERCIAL_SURFACE.ARCHIVE_DISQUALIFIED,
      label: COMMERCIAL_SURFACE_LABEL[COMMERCIAL_SURFACE.ARCHIVE_DISQUALIFIED],
      primaryCommercialEligible: false,
      reason: "DISQUALIFIED_OR_OUTSIDE_MARKET",
    };
  }
  if (isResearchPipelineOpportunity(opp)) {
    return {
      version: GDI_COMMERCIAL_SURFACE_DISPOSITION_V1,
      surface: COMMERCIAL_SURFACE.RESEARCH_PIPELINE,
      label: COMMERCIAL_SURFACE_LABEL[COMMERCIAL_SURFACE.RESEARCH_PIPELINE],
      primaryCommercialEligible: false,
      reason: "UNRESOLVED_BUYER_OR_FIT",
    };
  }
  return {
    version: GDI_COMMERCIAL_SURFACE_DISPOSITION_V1,
    surface: COMMERCIAL_SURFACE.COMMERCIAL_OPPORTUNITY,
    label: COMMERCIAL_SURFACE_LABEL[COMMERCIAL_SURFACE.COMMERCIAL_OPPORTUNITY],
    primaryCommercialEligible: true,
    reason: "SPECIFIC_COMMERCIAL_PATH",
  };
}

export function isPrimaryCommercialListEligible(opp = {}) {
  return classifyCommercialSurfaceDisposition(opp).primaryCommercialEligible === true;
}

/**
 * Filter opportunities for a list surface view.
 * @param {object[]} opportunities
 * @param {string} [view] commercial|research|archive|full
 */
export function filterOpportunitiesByCommercialSurface(
  opportunities = [],
  view = GDI_LIST_SURFACE_VIEW.COMMERCIAL,
) {
  const v = String(view || GDI_LIST_SURFACE_VIEW.COMMERCIAL).toLowerCase();
  if (v === GDI_LIST_SURFACE_VIEW.FULL || v === "complete" || v === "all") {
    return [...(opportunities || [])];
  }
  const classified = (opportunities || []).map((o) => ({
    opp: o,
    ...classifyCommercialSurfaceDisposition(o),
  }));
  if (v === GDI_LIST_SURFACE_VIEW.RESEARCH) {
    return classified
      .filter(
        (c) =>
          c.surface === COMMERCIAL_SURFACE.RESEARCH_PIPELINE ||
          c.surface === COMMERCIAL_SURFACE.MARKET_INTELLIGENCE ||
          c.surface === COMMERCIAL_SURFACE.LINKED_DUPLICATE,
      )
      .map((c) => c.opp);
  }
  if (v === GDI_LIST_SURFACE_VIEW.ARCHIVE) {
    return classified
      .filter(
        (c) =>
          c.surface === COMMERCIAL_SURFACE.ARCHIVE_DISQUALIFIED ||
          c.surface === COMMERCIAL_SURFACE.QUARANTINED,
      )
      .map((c) => c.opp);
  }
  // Default: primary commercial opportunities only
  return classified.filter((c) => c.primaryCommercialEligible).map((c) => c.opp);
}

/**
 * Attach surface disposition fields onto an opportunity (projection / persist).
 */
export function attachCommercialSurfaceDisposition(opp = {}) {
  const c = classifyCommercialSurfaceDisposition(opp);
  return {
    ...opp,
    commercialSurfaceDisposition: c.surface,
    commercialSurfaceLabel: c.label,
    primaryCommercialEligible: c.primaryCommercialEligible,
    commercialSurfaceReason: c.reason,
    // Suppress pursuit for non-commercial surfaces
    ...(c.primaryCommercialEligible
      ? {}
      : {
          salesActionSuppressed: true,
          salesActionSuppressionReason: c.reason,
        }),
  };
}

/**
 * Batch classify + count surfaces (for reports / acceptance).
 */
export function summarizeCommercialSurfaceBatch(opportunities = []) {
  const counts = {
    [COMMERCIAL_SURFACE.COMMERCIAL_OPPORTUNITY]: 0,
    [COMMERCIAL_SURFACE.RESEARCH_PIPELINE]: 0,
    [COMMERCIAL_SURFACE.MARKET_INTELLIGENCE]: 0,
    [COMMERCIAL_SURFACE.ARCHIVE_DISQUALIFIED]: 0,
    [COMMERCIAL_SURFACE.LINKED_DUPLICATE]: 0,
    [COMMERCIAL_SURFACE.QUARANTINED]: 0,
  };
  const rows = [];
  for (const o of opportunities || []) {
    const c = classifyCommercialSurfaceDisposition(o);
    counts[c.surface] = (counts[c.surface] || 0) + 1;
    rows.push({
      id: text(o.id),
      title: text(o.title).slice(0, 100),
      priority: o.priority || null,
      surface: c.surface,
      reason: c.reason,
      primaryCommercialEligible: c.primaryCommercialEligible,
      commercialDemandType: o.commercialDemandType || null,
      revenueStreamClass: o.revenueStreamClass || null,
    });
  }
  return {
    version: GDI_COMMERCIAL_SURFACE_DISPOSITION_V1,
    total: rows.length,
    counts,
    primaryCommercialCount: counts[COMMERCIAL_SURFACE.COMMERCIAL_OPPORTUNITY],
    rows,
  };
}

/**
 * Forbidden IDs / patterns that must NEVER appear on the primary commercial list.
 * Used by adversarial regression gates.
 */
export function primaryCommercialForbiddenFixtures() {
  return {
    marketIntelligenceIds: [...KNOWN_MARKET_INTELLIGENCE_IDS],
    disqualifiedIds: [
      "gdi_opp_aao_2027_annual_session_9",
      "gdi_opp_aao_2028_annual_meeting_10",
      "gdi_opp_48th_iscb_annual_conference_2027_12",
      "gdi_opp_aan_2027_disqualified",
      "gdi_opp_ada_2027_disqualified",
      "gdi_opp_agb_2027_disqualified",
      "gdi_opp_fba_qui_tam_2027_disqualified",
      "gdi_opp_ntca_2027_disqualified",
      "gdi_opp_ptab_2027_disqualified",
      "gdi_opp_asa_advance_2027_disqualified",
    ],
    researchPipelinePreferredIds: ["gdi_opp_2027_nci_rna_biology_symposium_7"],
    duplicateFamilyPrimaryOnly: {
      ife: ["gdi_s4912_512deebd0963", "gdi_beth_v16_evt_3c8228774517"],
      gbcc: [
        "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
        "gdi_e2d_770e3b44ad97c1ae",
      ],
    },
  };
}
