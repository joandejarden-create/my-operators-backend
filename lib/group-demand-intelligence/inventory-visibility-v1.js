/**
 * GDI Inventory Visibility V1 — status-labeled customer inventory without
 * inventing Ready/ACTIONABLE or weakening qualification fields.
 *
 * Waterstone-first (hotel allowlist). Surfaces structurally valid hotel
 * opportunities that fail strict readiness, with honest lane labels:
 *   SALES_OPPORTUNITY | ACCOUNT_DEVELOPMENT | FUTURE_OPPORTUNITY |
 *   RESEARCH_WATCHLIST | EXCLUDED
 *
 * Does NOT stamp Ready. Does NOT change prospectPublicationBasis.
 */

import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import { isReviewedProspectCustomerVisible } from "./reviewed-prospect-publication-v1.js";
import { isLegacyVisibilityCompatible } from "./legacy-visibility-compatibility-v1.js";
import { classifyCustomerSurfaceOpportunity } from "./customer-surface-revalidation-v1.js";
import { hasUsableContactRoute } from "./prospect-publication-eligibility-v1.js";
import { isGroupDemandIntelligenceLocalDev } from "./feature-flag.js";

const TEST_ID_RE =
  /_test_|fixture|sample|link_only|canary_promo|schema_repair|gdi_pe_test|v13_test|temp_validation/i;

function isBlockedInternalShell(opp = {}) {
  if (opp == null || typeof opp !== "object") return true;
  if (opp.isTestData === true) return true;
  if (opp.isDemandGenerator === true || opp.demandGeneratorOnly === true) return true;
  const id = String(opp.id || opp.opportunityId || "");
  if (TEST_ID_RE.test(id)) return true;
  const family = String(opp.demandFamily || "").toUpperCase();
  if (family === "DEMAND_GENERATOR" || family === "DEMAND_CAMPAIGN") return true;
  return false;
}

export const GDI_INVENTORY_VISIBILITY_ENV = "GDI_INVENTORY_VISIBILITY_V1";
export const GDI_INVENTORY_VISIBILITY_HOTEL_IDS_ENV =
  "GDI_INVENTORY_VISIBILITY_HOTEL_IDS";

/** Local sales workspace allowlist — Waterstone + Mexico Fiesta + Nobu London. */
export const INVENTORY_VISIBILITY_HOTEL_IDS = Object.freeze([
  "recgMYovrrZDJMqzX", // Waterstone Resort & Marina Boca Raton
  "recterKmNPdntCm0v", // Grand Fiesta Americana Chapultepec
  "recIEcSWuhSCoNLFN", // Fiesta Americana Condesa Cancún All Inclusive
  "recC2kbCaS48jFjFF", // Nobu Hotel London Portman Square (audit facing inventory)
]);

export const INVENTORY_LANE = Object.freeze({
  SALES_OPPORTUNITY: "SALES_OPPORTUNITY",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  FUTURE_OPPORTUNITY: "FUTURE_OPPORTUNITY",
  RESEARCH_WATCHLIST: "RESEARCH_WATCHLIST",
  EXCLUDED: "EXCLUDED",
});

export const INVENTORY_LANE_LABEL = Object.freeze({
  [INVENTORY_LANE.SALES_OPPORTUNITY]: "Sales opportunity",
  [INVENTORY_LANE.ACCOUNT_DEVELOPMENT]: "Account development",
  [INVENTORY_LANE.FUTURE_OPPORTUNITY]: "Future opportunity",
  [INVENTORY_LANE.RESEARCH_WATCHLIST]: "Research / watchlist",
  [INVENTORY_LANE.EXCLUDED]: "Excluded / history",
});

/** Active customer inventory (everything except EXCLUDED). */
export const ACTIVE_INVENTORY_LANES = Object.freeze([
  INVENTORY_LANE.SALES_OPPORTUNITY,
  INVENTORY_LANE.ACCOUNT_DEVELOPMENT,
  INVENTORY_LANE.FUTURE_OPPORTUNITY,
  INVENTORY_LANE.RESEARCH_WATCHLIST,
]);

function truthy(raw) {
  const v = String(raw || "0").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

function explicitlyFalse(raw) {
  if (raw == null || String(raw).trim() === "") return false;
  const v = String(raw).trim().toLowerCase();
  return v === "0" || v === "false" || v === "no" || v === "off";
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function isGdiInventoryVisibilityV1Enabled(env = process.env) {
  if (explicitlyFalse(env[GDI_INVENTORY_VISIBILITY_ENV])) return false;
  if (truthy(env[GDI_INVENTORY_VISIBILITY_ENV])) return true;
  return isGroupDemandIntelligenceLocalDev(env);
}

export function isInventoryVisibilityHotelEnabled(hotelId = "", env = process.env) {
  if (!isGdiInventoryVisibilityV1Enabled(env)) return false;
  const id = String(hotelId || "").trim();
  const allow = String(env[GDI_INVENTORY_VISIBILITY_HOTEL_IDS_ENV] || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  // Env allowlist extends (does not replace) the coded local sales workspace set
  // so Waterstone + Chapultepec remain reachable when .env still lists Waterstone-only.
  const set = [...new Set([...INVENTORY_VISIBILITY_HOTEL_IDS, ...allow])];
  return set.includes(id);
}

/** Positive future-cycle states only — never match NO_FUTURE_CYCLE_EVIDENCE. */
function hasPositiveFutureCycleEvidence(state = "") {
  const s = text(state).toUpperCase();
  if (!s || /^NO_/.test(s) || /NO_FUTURE/.test(s)) return false;
  return (
    /FUTURE_CYCLE_EXPECTED|FUTURE_CYCLE_UNCONFIRMED|CURRENT_FUTURE_CYCLE|RECURRING/.test(
      s,
    ) || /CURRENT_FUTURE/.test(s)
  );
}

function isPastWithoutFuture(opp = {}, opts = {}) {
  const start = text(opp.eventStartDate).slice(0, 10);
  const now = opts.nowDate || new Date().toISOString().slice(0, 10);
  if (!start || !/^\d{4}-\d{2}-\d{2}$/.test(start)) return false;
  if (start >= now) return false;
  return !hasPositiveFutureCycleEvidence(opp.futureCycleEvidenceState);
}

/**
 * Classify presentation lane. Never mutates Ready / publication basis.
 */
export function classifyInventoryPresentation(opp = {}, opts = {}) {
  const hotelId = text(opp.hotelId || opts.hotelId);
  const reasons = [];
  const ready = isGdiCustomerOpportunityReady(opp, opts);
  const surface = classifyCustomerSurfaceOpportunity
    ? classifyCustomerSurfaceOpportunity(opp, opts)
    : null;
  const disposition = text(surface?.disposition || surface?.customerSurfaceDisposition).toUpperCase();
  const priority = text(opp.priority).toUpperCase();
  const route = hasUsableContactRoute(opp);
  const reviewed = isReviewedProspectCustomerVisible(opp, { ...opts, hotelId });
  const legacy = isLegacyVisibilityCompatible(opp);
  const scaling =
    text(opp.researchSegment).toLowerCase() === "scaling_companies" ||
    /scaling compan/i.test(text(opp.segment));
  const opportunityType = text(opp.opportunityType).toUpperCase();
  const isCorporateAccount =
    opportunityType === "CORPORATE_ACCOUNT_RELATIONSHIP" || scaling;
  const hasVerifiedDemandDate = /^\d{4}-\d{2}-\d{2}/.test(
    text(opp.eventStartDate).slice(0, 10),
  );
  const bookingRaw = text(opp.bookingWindowStatus).toUpperCase();
  const pursueNowWindow =
    bookingRaw === "CONTACT_NOW" || bookingRaw === "PURSUE_NOW";
  // Open-ended CAR / scaling relationship: research packaging ≠ Sales Opportunity.
  // Keep Account Development unless a verified demand window or Pursue Now timing exists.
  const openEndedCorporateAccount =
    isCorporateAccount && !hasVerifiedDemandDate && !pursueNowWindow;

  // Founder-authorized published inventory registry — honor commercial class.
  const publishedClass = text(
    opp.commercialInventoryClass || opp.inventoryLane
  ).toUpperCase();
  if (
    opp.gdiPublishedInventoryRegistry === true &&
    (publishedClass === "SALES_READY" ||
      publishedClass === "SALES_OPPORTUNITY" ||
      publishedClass === "ACCOUNT_DEVELOPMENT")
  ) {
    const salesReady =
      publishedClass === "SALES_READY" || publishedClass === "SALES_OPPORTUNITY";
    const lane = salesReady
      ? INVENTORY_LANE.SALES_OPPORTUNITY
      : INVENTORY_LANE.ACCOUNT_DEVELOPMENT;
    return {
      lane,
      label: INVENTORY_LANE_LABEL[lane],
      reasons: ["gdi_published_inventory_registry_v1"],
      readyOk: salesReady,
      readyFailed: [],
      customerAccessible: true,
      salesReady,
    };
  }

  if (isBlockedInternalShell(opp)) {
    return {
      lane: INVENTORY_LANE.EXCLUDED,
      label: INVENTORY_LANE_LABEL[INVENTORY_LANE.EXCLUDED],
      reasons: ["test_fixture_or_generator_shell"],
      readyOk: false,
      readyFailed: ready.failed || [],
      customerAccessible: false,
      salesReady: false,
    };
  }

  if (opp.customerVisible === false || opp.prospectSuppressed === true || opp.archived === true) {
    return {
      lane: INVENTORY_LANE.EXCLUDED,
      label: INVENTORY_LANE_LABEL[INVENTORY_LANE.EXCLUDED],
      reasons: ["explicitly_suppressed"],
      readyOk: false,
      readyFailed: ready.failed || [],
      customerAccessible: false,
      salesReady: false,
    };
  }

  if (priority === "DISQUALIFIED" || disposition === "DQ_OTHER" || disposition === "INVALID_ENTITY") {
    reasons.push(
      priority === "DISQUALIFIED"
        ? "priority_disqualified"
        : `surface_${disposition.toLowerCase() || "invalid"}`,
    );
    return {
      lane: INVENTORY_LANE.EXCLUDED,
      label: INVENTORY_LANE_LABEL[INVENTORY_LANE.EXCLUDED],
      reasons,
      readyOk: false,
      readyFailed: ready.failed || [],
      customerAccessible: true, // history / excluded section
      salesReady: false,
      excludedSection: true,
    };
  }

  if (isPastWithoutFuture(opp, opts) || disposition === "PAST_CLOSED") {
    return {
      lane: INVENTORY_LANE.EXCLUDED,
      label: INVENTORY_LANE_LABEL[INVENTORY_LANE.EXCLUDED],
      reasons: ["past_closed_no_future_cycle"],
      readyOk: false,
      readyFailed: ready.failed || [],
      customerAccessible: true,
      salesReady: false,
      excludedSection: true,
    };
  }

  if (!text(opp.title || opp.opportunityName) || !text(opp.organizationName || opp.company)) {
    return {
      lane: INVENTORY_LANE.EXCLUDED,
      label: INVENTORY_LANE_LABEL[INVENTORY_LANE.EXCLUDED],
      reasons: ["missing_title_or_organization"],
      readyOk: false,
      readyFailed: ready.failed || [],
      customerAccessible: false,
      salesReady: false,
    };
  }

  const start = text(opp.eventStartDate).slice(0, 10);
  const now = opts.nowDate || new Date().toISOString().slice(0, 10);
  const futureDate = Boolean(start && start >= now);
  const watchPri = priority === "WATCHLIST";
  const futureCycle = hasPositiveFutureCycleEvidence(opp.futureCycleEvidenceState);

  function futureOrResearchLane(extraReasons = []) {
    const failed = ready.failed || [];
    // Future with thin evidence → research; future with substance → future opportunity
    const thin =
      failed.includes("account_quality_insufficient") &&
      failed.includes("buyer_contact_path_insufficient");
    const lane = thin
      ? INVENTORY_LANE.RESEARCH_WATCHLIST
      : INVENTORY_LANE.FUTURE_OPPORTUNITY;
    return {
      lane,
      label: INVENTORY_LANE_LABEL[lane],
      reasons: [
        ...extraReasons,
        ...failed.map((f) => `not_sales_ready:${f}`),
      ].filter(Boolean),
      readyOk: false,
      readyFailed: failed,
      customerAccessible: true,
      salesReady: false,
    };
  }

  // Sales: strict Ready / legacy — never stamp Sales Opportunity on open-ended
  // corporate accounts merely because Exa research / readiness text exists.
  if ((ready.ok === true || legacy) && !openEndedCorporateAccount) {
    return {
      lane: INVENTORY_LANE.SALES_OPPORTUNITY,
      label: INVENTORY_LANE_LABEL[INVENTORY_LANE.SALES_OPPORTUNITY],
      reasons: ready.ok ? ["readiness_gate_pass"] : ["legacy_visibility_cohort"],
      readyOk: ready.ok === true,
      readyFailed: ready.failed || [],
      customerAccessible: true,
      salesReady: true,
    };
  }

  // Verified future demand/event window → Future Opportunity (before reviewed CAR packaging).
  if (futureDate || futureCycle) {
    return futureOrResearchLane([
      futureDate ? "future_event_date" : null,
      futureCycle ? "future_cycle_evidence" : null,
    ]);
  }

  // Account development: reviewed package / scaling route / open-ended CAR
  if (
    reviewed ||
    (scaling && route.ok) ||
    text(opp.prospectPublicationBasis) === "REVIEWED_PACKAGE_V1" ||
    openEndedCorporateAccount
  ) {
    return {
      lane: INVENTORY_LANE.ACCOUNT_DEVELOPMENT,
      label: INVENTORY_LANE_LABEL[INVENTORY_LANE.ACCOUNT_DEVELOPMENT],
      reasons: [
        openEndedCorporateAccount
          ? "open_ended_corporate_account_development"
          : reviewed || text(opp.prospectPublicationBasis) === "REVIEWED_PACKAGE_V1"
            ? "reviewed_package_account_development"
            : "scaling_general_org_route",
        ...(ready.failed || []).map((f) => `not_sales_ready:${f}`),
        ...(ready.ok && openEndedCorporateAccount
          ? ["readiness_present_but_no_demand_window"]
          : []),
      ],
      readyOk: false,
      readyFailed: ready.failed || [],
      customerAccessible: true,
      salesReady: false,
    };
  }

  // Watchlist priority alone does not make an undated CAR a Future Opportunity.
  if (watchPri && !isCorporateAccount) {
    return futureOrResearchLane(["watchlist_priority"]);
  }

  // Default: research / watchlist (structurally present, not sales-ready)
  return {
    lane: INVENTORY_LANE.RESEARCH_WATCHLIST,
    label: INVENTORY_LANE_LABEL[INVENTORY_LANE.RESEARCH_WATCHLIST],
    reasons: [
      "structurally_present_not_sales_ready",
      ...(ready.failed || []).map((f) => `not_sales_ready:${f}`),
    ],
    readyOk: false,
    readyFailed: ready.failed || [],
    customerAccessible: true,
    salesReady: false,
  };
}

/**
 * Inventory-mode customer list visibility (Waterstone allowlist).
 * Includes EXCLUDED only when opts.includeExcluded === true.
 */
export function isInventoryCustomerVisible(opp = {}, opts = {}) {
  const env = opts.env || process.env;
  const hotelId = text(opp.hotelId || opts.hotelId);
  if (!isInventoryVisibilityHotelEnabled(hotelId, env)) return false;

  const classified = classifyInventoryPresentation(opp, opts);
  if (!classified.customerAccessible) return false;
  if (
    classified.lane === INVENTORY_LANE.EXCLUDED &&
    opts.includeExcluded !== true
  ) {
    // Still show excluded in inventory mode when they are labeled history
    // (founder asked for excluded/history accessibility). Default ON for
    // Waterstone inventory so the client sees why records are out.
    if (classified.excludedSection === true) return true;
    return false;
  }
  return ACTIVE_INVENTORY_LANES.includes(classified.lane) || classified.excludedSection === true;
}

export function attachInventoryPresentation(opp = {}, opts = {}) {
  const classified = classifyInventoryPresentation(opp, opts);
  return {
    ...opp,
    inventoryLane: classified.lane,
    inventoryLaneLabel: classified.label,
    inventoryWhyNotSalesReady: classified.salesReady
      ? []
      : (classified.readyFailed || []).slice(),
    inventoryPresentationReasons: classified.reasons,
    inventorySalesReady: classified.salesReady === true,
  };
}

export function filterInventoryCustomerOpportunities(opportunities = [], opts = {}) {
  return (opportunities || [])
    .filter((o) => isInventoryCustomerVisible(o, opts))
    .map((o) => attachInventoryPresentation(o, opts));
}

export function summarizeInventoryLanes(opportunities = [], opts = {}) {
  const rows = (opportunities || []).map((o) =>
    classifyInventoryPresentation(o, opts),
  );
  const counts = {
    total: rows.length,
    sales: 0,
    accountDevelopment: 0,
    future: 0,
    researchWatchlist: 0,
    excluded: 0,
    customerAccessible: 0,
  };
  for (const r of rows) {
    if (r.lane === INVENTORY_LANE.SALES_OPPORTUNITY) counts.sales += 1;
    else if (r.lane === INVENTORY_LANE.ACCOUNT_DEVELOPMENT) counts.accountDevelopment += 1;
    else if (r.lane === INVENTORY_LANE.FUTURE_OPPORTUNITY) counts.future += 1;
    else if (r.lane === INVENTORY_LANE.RESEARCH_WATCHLIST) counts.researchWatchlist += 1;
    else if (r.lane === INVENTORY_LANE.EXCLUDED) counts.excluded += 1;
    if (r.customerAccessible) counts.customerAccessible += 1;
  }
  return counts;
}
