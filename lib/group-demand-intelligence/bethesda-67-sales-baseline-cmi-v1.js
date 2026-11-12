/**
 * Bethesda Marriott — 67-record sales-opportunity baseline + CMI population V1.
 *
 * Corrects the operating model: the frozen 67 IDs are the sales-team baseline
 * inventory (19 published + 48 staged), not permanent founder-only research.
 *
 * Customer publication of the additional 48 requires an explicit founder
 * approval receipt. Until then, customer/share surfaces stay at the approved 19.
 *
 * COMPETITIVE_MARKET_INTELLIGENCE is a separate population (verified competitor
 * bookings) — not a fifteenth demand segment and not Sales Ready inflation.
 *
 * Zero paid research: classification uses existing cached opportunity fields only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_CMI_EVIDENCE_LEDGER,
  buildCmiClassificationFromLedger,
  getCmiEvidenceEntry,
  applyCmiLedgerRemediation,
} from "./commercial-consistency/cmi-evidence-ledger-v1.js";
import { sanitizeCommercialPresentation } from "./commercial-consistency/commercial-presentation-sanitizer-v1.js";

function hasCompetitiveVenueEvidence(o = {}) {
  if (o.competitiveVenueEvidence === true) return true;
  if (o.competitorVenueConfirmed === true) return true;
  const sourcing = upper(o.venueSourcingStatus || o.hotelSourcingStatus);
  if (
    (sourcing === "VENUE_CONFIRMED" || sourcing === "HISTORICAL_HOST") &&
    (o.competitiveCaptureEligible === true ||
      /COMPETITOR|CAPTURE|OVERFLOW/i.test(text(o.competitiveTrack)))
  ) {
    return true;
  }
  return false;
}

export const BETHESDA_67_SALES_BASELINE_CMI_V1 =
  "gdi-bethesda-67-sales-baseline-cmi-v1";

export const BETHESDA_HOTEL_ID = "recLuxvwwxID7U2B8";

/** Opportunity Type population values (orthogonal to demand-segment filters). */
export const INVENTORY_POPULATION = Object.freeze({
  SALES_OPPORTUNITIES: "SALES_OPPORTUNITIES",
  COMPETITIVE_MARKET_INTELLIGENCE: "COMPETITIVE_MARKET_INTELLIGENCE",
  ALL_COMMERCIAL_INTELLIGENCE: "ALL_COMMERCIAL_INTELLIGENCE",
});

export const INVENTORY_POPULATION_LABEL = Object.freeze({
  [INVENTORY_POPULATION.SALES_OPPORTUNITIES]: "Sales Opportunities",
  [INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE]:
    "Competitive Market Intelligence",
  [INVENTORY_POPULATION.ALL_COMMERCIAL_INTELLIGENCE]:
    "All Commercial Intelligence",
});

export const CMI_CLASS = Object.freeze({
  COMPETITIVE_MARKET_INTELLIGENCE: "COMPETITIVE_MARKET_INTELLIGENCE",
  NEEDS_FURTHER_VERIFICATION: "NEEDS_FURTHER_VERIFICATION",
  HISTORICAL_COMPETITIVE: "HISTORICAL_COMPETITIVE",
  NOT_CMI: "NOT_CMI",
});

/**
 * Competitive Market Scope secondary status (not a demand segment).
 * Competitive total = VERIFIED + NEEDS_VERIFICATION + HISTORICAL.
 * Never equate Competitive total with verified-booking total.
 */
export const COMPETITIVE_STATUS = Object.freeze({
  VERIFIED: "VERIFIED",
  NEEDS_VERIFICATION: "NEEDS_VERIFICATION",
  HISTORICAL: "HISTORICAL",
  NOT_COMPETITIVE: "NOT_COMPETITIVE",
});

export const COMPETITIVE_STATUS_LABEL = Object.freeze({
  [COMPETITIVE_STATUS.VERIFIED]: "Verified",
  [COMPETITIVE_STATUS.NEEDS_VERIFICATION]: "Needs Verification",
  [COMPETITIVE_STATUS.HISTORICAL]: "Historical",
  [COMPETITIVE_STATUS.NOT_COMPETITIVE]: "Not Competitive",
});

export const COMPETITIVE_STATUS_FILTER = Object.freeze({
  ALL: "ALL_COMPETITIVE",
  VERIFIED: COMPETITIVE_STATUS.VERIFIED,
  NEEDS_VERIFICATION: COMPETITIVE_STATUS.NEEDS_VERIFICATION,
  HISTORICAL: COMPETITIVE_STATUS.HISTORICAL,
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FORENSIC_RAW =
  "reports/gdi-bethesda-67-to-19-forensic-reconciliation-v1/01-frozen67-vs-facing19-raw.json";
const FORENSIC_SUMMARY =
  "reports/gdi-bethesda-67-to-19-forensic-reconciliation-v1/00-summary.json";
const PROTECTED_FREEZE =
  "reports/gdi-bethesda-dmv-territory-expansion-v1/00-protected-67-baseline-freeze.json";
const APPROVAL_RECEIPT =
  "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/bethesda-67-customer-publication-approval.json";

function readJson(rel) {
  const p = path.isAbsolute(rel) ? rel : path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function text(v) {
  return String(v == null ? "" : v).trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

let _manifestCache = null;

/**
 * Exact frozen 67-ID baseline (19 published + 48 staged).
 */
export function loadBethesda67SalesBaselineManifest(opts = {}) {
  if (_manifestCache && !opts.forceReload) return _manifestCache;
  const freeze = readJson(PROTECTED_FREEZE);
  const forensic = readJson(FORENSIC_RAW);
  const summary = readJson(FORENSIC_SUMMARY);
  const approved19 = freeze?.approvedCustomerFacingIds || forensic?.keptIds || [];
  const historical48 =
    freeze?.historicalResearchInventoryIds ||
    (forensic?.missingRows || []).map((r) => r.id);
  const all67 = [...new Set([...approved19, ...historical48])];
  if (all67.length !== 67) {
    throw new Error(
      `BETHESDA_67_BASELINE_INVALID: expected 67 unique ids, got ${all67.length}`
    );
  }
  if (approved19.length !== 19 || historical48.length !== 48) {
    throw new Error(
      `BETHESDA_67_SPLIT_INVALID: approved=${approved19.length} historical=${historical48.length}`
    );
  }
  _manifestCache = {
    version: BETHESDA_67_SALES_BASELINE_CMI_V1,
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: "Bethesda Marriott",
    provenance: {
      protectedFreeze: PROTECTED_FREEZE,
      forensicRaw: FORENSIC_RAW,
      forensicSummary: FORENSIC_SUMMARY,
      note: "Frozen PR #211 67-ID sales-team baseline (19 published + 48 staged).",
    },
    counts: {
      totalUniqueBaselineIds: 67,
      publishedCustomerFacing: 19,
      stagedForSalesTeamPublication: 48,
    },
    publishedIds: [...approved19],
    stagedIds: [...historical48],
    baselineIds: all67,
    forensicCategoryBreakdown: summary?.categoryBreakdownMissing48 || null,
    subcategoryBreakdown: summary?.subcategoryBreakdown || null,
  };
  return _manifestCache;
}

export function isBethesda67BaselineId(id, manifest = null) {
  const m = manifest || loadBethesda67SalesBaselineManifest();
  return m.baselineIds.includes(String(id || ""));
}

export function isBethesda67PublishedId(id, manifest = null) {
  const m = manifest || loadBethesda67SalesBaselineManifest();
  return m.publishedIds.includes(String(id || ""));
}

export function isBethesda67StagedId(id, manifest = null) {
  const m = manifest || loadBethesda67SalesBaselineManifest();
  return m.stagedIds.includes(String(id || ""));
}

/**
 * Explicit founder approval required before the 48 enter the hotel customer feed.
 * Never treat env absence as approval.
 */
export function isBethesda67CustomerPublicationApproved(opts = {}) {
  if (opts.forceApproved === true) return true;
  if (opts.forceApproved === false) return false;
  const env = String(
    process.env.GDI_BETHESDA_67_CUSTOMER_PUBLICATION_APPROVED || ""
  ).trim();
  if (env === "1" || /^true$/i.test(env)) return true;
  const receipt = readJson(opts.receiptPath || APPROVAL_RECEIPT);
  if (!receipt || typeof receipt !== "object") return false;
  if (receipt.approved !== true) return false;
  if (receipt.hotelId && receipt.hotelId !== BETHESDA_HOTEL_ID) return false;
  if (receipt.baselineVersion && receipt.baselineCount !== 67) return false;
  return true;
}

export function isInventoryPopulationValue(value) {
  const v = String(value || "");
  return Object.values(INVENTORY_POPULATION).includes(v);
}

/**
 * Commercial maturity / sales usability label for honest inventory staging.
 */
export function classifySalesBaselineMaturity(opp = {}, forensicRow = null, opts = {}) {
  const salesUsability = upper(
    forensicRow?.salesUsability ||
      opp.salesUsability ||
      opp.hotelSalesUsability ||
      ""
  );
  const maturity = upper(
    opp.commercialMaturity ||
      opp.gdiMaturityState ||
      forensicRow?.currentMaturity ||
      forensicRow?.persistedMaturity ||
      ""
  );
  const invClass = upper(
    opp.commercialInventoryClass || forensicRow?.commercialInventoryClass || ""
  );
  const priority = upper(opp.priority || forensicRow?.currentPriority || "");
  const engine = upper(
    forensicRow?.engineCommercialCertification ||
      opp.engineCommercialCertification ||
      ""
  );
  const qual = upper(opp.opportunityQualification || "");
  const publishedBaseline =
    opts.publishedBaseline === true ||
    opp.salesBaselineMembership === "PUBLISHED_19" ||
    (opp.customerVisible === true &&
      isBethesda67PublishedId(opp.id || opp.opportunityId));

  if (invClass === "SALES_READY" || maturity === "SALES_READY") {
    return {
      key: "SALES_READY",
      label: "Sales Ready",
      customerSafe: true,
    };
  }
  if (
    invClass === "ACCOUNT_DEVELOPMENT" ||
    maturity === "ACCOUNT_DEVELOPMENT" ||
    salesUsability === "ACCOUNT_DEVELOPMENT" ||
    engine === "ACCOUNT_DEVELOPMENT_ELIGIBLE"
  ) {
    return {
      key: "ACCOUNT_DEVELOPMENT",
      label: "Account Development",
      customerSafe: true,
    };
  }
  // Published customer-feed rows without explicit maturity stamps are active
  // sales inventory — prefer Sales Ready when qualification is verified/open.
  if (publishedBaseline) {
    if (
      engine === "SALES_READY_ELIGIBLE" ||
      qual === "VERIFIED_OPEN" ||
      qual === "ACTIONABLE_NOW" ||
      qual === "STRONG"
    ) {
      return {
        key: "SALES_READY",
        label: "Sales Ready",
        customerSafe: true,
      };
    }
    return {
      key: "ACCOUNT_DEVELOPMENT",
      label: "Account Development",
      customerSafe: true,
    };
  }
  if (engine === "SALES_READY_ELIGIBLE") {
    return {
      key: "SALES_READY",
      label: "Sales Ready",
      customerSafe: true,
    };
  }
  if (
    salesUsability === "QUALIFY_FIRST" ||
    /QUALIF/.test(maturity) ||
    engine.includes("QUALIF")
  ) {
    return {
      key: "QUALIFICATION_REQUIRED",
      label: "Qualification Required",
      customerSafe: true,
    };
  }
  if (
    /PAST|INACTIVE|HISTORIC/.test(salesUsability) ||
    /PAST|INACTIVE/.test(maturity) ||
    forensicRow?.subcategory === "PAST_EVENT_SURFACE_INACTIVE"
  ) {
    return {
      key: "PAST_INACTIVE",
      label: "Past / inactive activity",
      customerSafe: true,
    };
  }
  if (
    /FUTURE/.test(salesUsability) ||
    /FUTURE/.test(maturity) ||
    priority === "WATCHLIST" ||
    salesUsability === "MONITOR"
  ) {
    return {
      key: "RESEARCH_CONTINUATION_MONITOR",
      label: "Research Continuation / Monitor",
      customerSafe: true,
    };
  }
  if (/FUTURE/.test(text(opp.bookingWindowStatus))) {
    return {
      key: "FUTURE_BUSINESS",
      label: "Future business",
      customerSafe: true,
    };
  }
  return {
    key: "RESEARCH_CONTINUATION_MONITOR",
    label: "Research Continuation / Monitor",
    customerSafe: true,
  };
}

/**
 * Customer-readiness assessment for staged (non-published) baseline rows.
 * Does not invent content; classifies what is already known.
 */
export function assessStagedCustomerReadiness(opp = {}, forensicRow = null) {
  const maturity = classifySalesBaselineMaturity(opp, forensicRow);
  const exclusion = forensicRow?.exclusionReasons || [];
  const namedContact = text(
    forensicRow?.namedContact ||
      opp.primaryContact?.name ||
      opp.primaryContactName ||
      ""
  );
  const hasContact = Boolean(namedContact);
  const demand = upper(
    forensicRow?.demandConfidence ||
      opp.guestroomDemandConfidence ||
      opp.demandEvidenceLabel ||
      ""
  );
  const privacyConcern =
    /email|phone|personal/i.test(text(opp.privacyFlags)) ||
    opp.containsUnverifiedPersonalContact === true;
  const duplicateOf = text(
    opp.duplicateOfOpportunityId ||
      opp.canonicalDuplicateOf ||
      forensicRow?.duplicateOf ||
      ""
  );

  let readiness = "REQUIRES_ADDITIONAL_RESEARCH";
  let reasons = [];

  if (duplicateOf) {
    readiness = "DUPLICATE";
    reasons.push(`Duplicate of ${duplicateOf}`);
  } else if (maturity.key === "PAST_INACTIVE") {
    readiness = "HISTORICAL_OR_INACTIVE";
    reasons.push("Past or inactive commercial surface");
  } else if (
    forensicRow?.classification === "A" ||
    forensicRow?.subcategory === "PAST_EVENT_SURFACE_INACTIVE"
  ) {
    readiness = "HISTORICAL_OR_INACTIVE";
    reasons.push("Forensic category A / past-event inactive");
  } else if (
    maturity.key === "SALES_READY" ||
    maturity.key === "ACCOUNT_DEVELOPMENT"
  ) {
    if (hasContact && demand && !/SPECULATIVE|UNKNOWN/.test(demand)) {
      readiness = "CUSTOMER_READY_CONTENT";
      reasons.push("Named contact + non-speculative demand + AD/SR maturity");
    } else {
      readiness = "INCOMPLETE_OR_UNCERTAIN";
      reasons.push("AD/SR maturity but contact or demand still thin");
    }
  } else if (maturity.key === "QUALIFICATION_REQUIRED") {
    readiness = "REQUIRES_ADDITIONAL_RESEARCH";
    reasons.push("Qualification gaps remain (host/lodging/fit open)");
  } else {
    readiness = "REQUIRES_ADDITIONAL_RESEARCH";
    reasons.push("Monitor / research-continuation posture");
  }

  if (privacyConcern) {
    readiness = "PRIVACY_OR_DATA_QUALITY";
    reasons.push("Privacy or data-quality concern flagged");
  }

  if (exclusion.includes("customerVisible=false")) {
    reasons.push("Not yet customerVisible (publication gate)");
  }
  if (exclusion.includes("lacks_approved_commercial_class")) {
    reasons.push("Lacks approved commercial inventory class for feed");
  }

  return {
    opportunityId: opp.id || forensicRow?.id || "",
    readiness,
    maturity,
    namedContact: namedContact || null,
    demandConfidence: demand || null,
    exclusionReasons: exclusion,
    reasons,
    publicationReviewReady: readiness === "CUSTOMER_READY_CONTENT",
    autoPublishAllowed: false,
  };
}

/**
 * @deprecated Legacy seed map — retained for audit provenance only.
 * Classification authority is `BETHESDA_CMI_EVIDENCE_LEDGER` (evidence URLs required).
 * Boolean(seed) is no longer sufficient for verified CMI.
 */
export const BETHESDA_EXA_VERIFIED_CMI_SEEDS = Object.freeze(
  Object.fromEntries(
    Object.entries(BETHESDA_CMI_EVIDENCE_LEDGER).map(([id, row]) => [
      id,
      {
        competitorHotel: row.competitorHotel,
        evidence: row.demotionReason ||
          (row.claims || [])
            .filter((c) => c.status === "VERIFIED")
            .map((c) => c.statement)
            .join(" "),
        verifiedCmi: row.verifiedCmi === true,
        ledger: true,
      },
    ])
  )
);

/**
 * Collect URL-ish evidence strings from an opportunity (non-mutating).
 */
function collectOpportunitySourceUrls(opp = {}) {
  const out = [];
  const push = (v) => {
    const s = text(v);
    if (s && /^https?:\/\//i.test(s)) out.push(s);
  };
  for (const s of opp.sources || []) {
    if (typeof s === "string") push(s);
    else if (s && typeof s === "object") {
      push(s.url || s.href || s.sourceUrl);
    }
  }
  for (const e of opp.evidence || []) {
    if (e && typeof e === "object") push(e.url || e.sourceUrl);
  }
  push(opp.sourceUrl);
  push(opp.primarySourceUrl);
  push(opp.lodgingEvidenceUrl);
  return [...new Set(out)];
}

/**
 * Extract edition year from id/title/dates — required for non-ledger CMI claims.
 */
function extractOpportunityEditionYear(opp = {}) {
  const id = String(opp.id || opp.opportunityId || "");
  const mId = id.match(/(20\d{2})/);
  if (mId) return mId[1];
  const blob = `${opp.title || ""} ${opp.eventStartDate || ""} ${opp.eventDates || ""}`;
  const m = blob.match(/\b(20\d{2})\b/);
  return m ? m[1] : null;
}

/**
 * Legacy booking flags + generic source arrays are NOT sufficient.
 * Require edition-specific competitor venue/booking evidence for the cited year.
 */
function hasEditionSpecificCompetitorBookingEvidence(opp = {}, competitorHotel = "") {
  if (!competitorHotel) return false;

  const status = upper(
    opp.competitorVenueStatus || opp.competitorBookingStatus || ""
  );
  const confirmedStatus =
    status === "CONFIRMED_CONTRACTED_BLOCK" ||
    status === "CONFIRMED_EVENT_VENUE" ||
    status === "VERIFIED_BOOKING";

  const year = extractOpportunityEditionYear(opp);
  const urls = collectOpportunitySourceUrls(opp);
  if (!urls.length) return false;

  const claimBlob = [
    opp.competitorBookingClaim,
    opp.lodgingEvidence,
    opp.housingStatus,
    opp.summaryWhat,
    opp.whyNow,
    ...(opp.claims || []).map((c) => c?.statement || ""),
  ]
    .map(text)
    .join(" ");

  const yearInUrls = year
    ? urls.some((u) => u.includes(year) || new RegExp(`/${year}[a-z0-9_-]*`, "i").test(u))
    : false;
  const yearInClaim = year
    ? new RegExp(`\\b${year}\\b`).test(claimBlob)
    : false;
  const competitorInClaim = new RegExp(
    competitorHotel.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").slice(0, 24),
    "i"
  ).test(claimBlob);
  const bookingLanguage =
    /room block|host hotel|conference hotel|contracted|published (room )?rates|official (housing|accommodations)/i.test(
      claimBlob
    );

  // Explicit confirmed status still needs edition-tied sources (not a bare org homepage).
  if (confirmedStatus && (yearInUrls || (yearInClaim && bookingLanguage))) {
    return true;
  }

  // Booking flags alone are insufficient without edition-specific claim + URL.
  const legacyFlag =
    opp.competitorVenueConfirmed === true ||
    opp.competitiveVenueEvidence === true ||
    upper(opp.competitorBookingEvidenceClass) === "VERIFIED_BOOKING" ||
    upper(opp.competitiveBookingStatus) === "VERIFIED_BOOKING";

  if (legacyFlag && yearInUrls && competitorInClaim && bookingLanguage) {
    return true;
  }

  return false;
}

/**
 * Verified competitor-booked CMI standard (evidence-backed).
 * A seed / named competitor / overflow hypothesis is NOT proof.
 * Requires ledger claims with attached evidence URLs for the exact edition.
 */
function inferCompetitorHotelFromOpportunity(opp = {}) {
  const explicit = text(
    opp.competitorHotel ||
      opp.competitorVenue ||
      opp.likelyCompetitor ||
      opp.confirmedCompetitorVenue ||
      opp.competitiveDemand?.competitorHotel ||
      ""
  );
  if (explicit) return explicit;

  const blob = [
    opp.title,
    opp.whatIsHappening,
    opp.summaryWhat,
    opp.eventLocationSummary,
    opp.lodgingEvidence,
    opp.primaryContact?.whyRelevant,
    opp.contactRecovery?.note,
  ]
    .map(text)
    .join(" ");

  const patterns = [
    [/Bethesda North Marriott[^,]{0,40}/i, "Bethesda North Marriott Hotel & Conference Center"],
    [/The Bethesdan[^,]{0,40}/i, "The Bethesdan Hotel (Tapestry Collection by Hilton)"],
    [/Crystal Gateway Marriott/i, "Crystal Gateway Marriott"],
    [/Marriott Marquis[^,]{0,30}/i, "Marriott Marquis Washington, DC"],
    [/Hyatt Regency Bethesda/i, "Hyatt Regency Bethesda"],
  ];
  for (const [re, canonical] of patterns) {
    if (re.test(blob)) return canonical;
  }
  return "";
}

function isCompetitorHostedHistoricalSignal(opp = {}) {
  const qual = upper(opp.originalCommercialQualification || "");
  const disp = upper(
    opp.historicalRecoveryDisposition || opp.recoveryDisposition || ""
  );
  const venueStatus = upper(opp.competitorVenueStatus || "");
  return (
    qual === "COMPETITOR_HOSTED_MAIN_EVENT" ||
    disp === "OVERFLOW_WATCH" ||
    venueStatus === "VERIFIED_HISTORICAL_VENUE" ||
    /competitor[- ]host|booked at |host hotel is |hosted at /i.test(
      `${opp.title || ""} ${opp.whatIsHappening || ""} ${opp.contactRecovery?.note || ""}`
    )
  );
}

export function classifyCompetitiveMarketIntelligence(opp = {}) {
  const id = String(opp.id || opp.opportunityId || "");
  const ledgerClass = buildCmiClassificationFromLedger(opp);
  if (ledgerClass) {
    const status =
      ledgerClass.competitiveStatus ||
      (ledgerClass.verified
        ? COMPETITIVE_STATUS.VERIFIED
        : ledgerClass.class === "HISTORICAL_COMPETITIVE"
          ? COMPETITIVE_STATUS.HISTORICAL
          : COMPETITIVE_STATUS.NEEDS_VERIFICATION);
    return {
      class:
        status === COMPETITIVE_STATUS.VERIFIED
          ? CMI_CLASS.COMPETITIVE_MARKET_INTELLIGENCE
          : status === COMPETITIVE_STATUS.HISTORICAL
            ? CMI_CLASS.HISTORICAL_COMPETITIVE
            : CMI_CLASS.NEEDS_FURTHER_VERIFICATION,
      label:
        status === COMPETITIVE_STATUS.VERIFIED
          ? "Competitive Market Intelligence"
          : status === COMPETITIVE_STATUS.HISTORICAL
            ? "Historical Competitive"
            : "Needs further verification",
      verified: ledgerClass.verified === true,
      competitiveStatus: status,
      competitorHotel: ledgerClass.competitorHotel,
      reason: ledgerClass.reason,
      seedId: ledgerClass.seedId,
      competitorVenueStatus: ledgerClass.competitorVenueStatus,
      targetHotelSourcingStatus: ledgerClass.targetHotelSourcingStatus,
      commercialActionability: ledgerClass.commercialActionability,
      evidenceUrls: ledgerClass.evidenceUrls,
      verifiedAt: ledgerClass.verifiedAt,
      demotionReason: ledgerClass.demotionReason,
      ledgerVersion: ledgerClass.ledgerVersion,
    };
  }

  const competitorHotel = inferCompetitorHotelFromOpportunity(opp);
  const sourcing = upper(opp.venueSourcingStatus || opp.hotelSourcingStatus);
  const editionSpecific = hasEditionSpecificCompetitorBookingEvidence(
    opp,
    competitorHotel
  );

  const speculative =
    opp.competitiveCaptureEligible === true && !editionSpecific;
  const hasCompChipSignal = hasCompetitiveVenueEvidence(opp);
  const hostedSignal = isCompetitorHostedHistoricalSignal(opp);

  // Reject legacy path: overflow label + competitor string + PRIMARY_VENUE_SELECTED.
  if (
    (/overflow/i.test(id) || /\boverflow\b/i.test(text(opp.title))) &&
    competitorHotel &&
    sourcing === "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE" &&
    !editionSpecific
  ) {
    return {
      class: CMI_CLASS.NEEDS_FURTHER_VERIFICATION,
      label: "Needs further verification",
      verified: false,
      competitiveStatus: COMPETITIVE_STATUS.NEEDS_VERIFICATION,
      competitorHotel,
      reason:
        "Overflow hypothesis + named competitor without evidence-backed booking claim",
    };
  }

  // Legacy booking flag + generic sources array alone must NOT verify CMI.
  if (
    (opp.competitorVenueConfirmed === true ||
      opp.competitiveVenueEvidence === true ||
      upper(opp.competitorBookingEvidenceClass) === "VERIFIED_BOOKING" ||
      upper(opp.competitiveBookingStatus) === "VERIFIED_BOOKING") &&
    competitorHotel &&
    Array.isArray(opp.sources) &&
    opp.sources.length > 0 &&
    !editionSpecific
  ) {
    return {
      class: CMI_CLASS.NEEDS_FURTHER_VERIFICATION,
      label: "Needs further verification",
      verified: false,
      competitiveStatus: COMPETITIVE_STATUS.NEEDS_VERIFICATION,
      competitorHotel,
      reason:
        "Legacy booking flag and generic sources without edition-specific competitor venue/booking evidence",
    };
  }

  if (editionSpecific) {
    return {
      class: CMI_CLASS.COMPETITIVE_MARKET_INTELLIGENCE,
      label: "Competitive Market Intelligence",
      verified: true,
      competitiveStatus: COMPETITIVE_STATUS.VERIFIED,
      competitorHotel,
      reason:
        "Edition-specific competitor venue/booking evidence present with supporting sources",
      seedId: null,
    };
  }

  // Historical competitor host (prior edition / reconstructed research) —
  // eligible Competitive intelligence, not verified current-edition booking.
  if (
    competitorHotel &&
    hostedSignal &&
    upper(opp.competitorVenueStatus) === "VERIFIED_HISTORICAL_VENUE"
  ) {
    return {
      class: CMI_CLASS.HISTORICAL_COMPETITIVE,
      label: "Historical Competitive",
      verified: false,
      competitiveStatus: COMPETITIVE_STATUS.HISTORICAL,
      competitorHotel,
      reason: "Verified historical competitor venue — current edition not confirmed",
    };
  }

  if (hasCompChipSignal || speculative || competitorHotel || hostedSignal) {
    return {
      class: CMI_CLASS.NEEDS_FURTHER_VERIFICATION,
      label: "Needs further verification",
      verified: false,
      competitiveStatus: COMPETITIVE_STATUS.NEEDS_VERIFICATION,
      competitorHotel: competitorHotel || null,
      reason: hostedSignal
        ? "Competitor-hosted / overflow-watch signal without verified edition booking standard"
        : "Competitor association or capture eligibility without verified booking standard",
    };
  }
  return {
    class: CMI_CLASS.NOT_CMI,
    label: "Not CMI",
    verified: false,
    competitiveStatus: COMPETITIVE_STATUS.NOT_COMPETITIVE,
    competitorHotel: null,
    reason: "No competitor booking evidence",
  };
}

export { applyCmiLedgerRemediation, getCmiEvidenceEntry, BETHESDA_CMI_EVIDENCE_LEDGER };

export function isVerifiedCmiOpportunity(opp = {}) {
  return (
    classifyCompetitiveMarketIntelligence(opp).class ===
    CMI_CLASS.COMPETITIVE_MARKET_INTELLIGENCE
  );
}

/**
 * Annotate opportunities with baseline + CMI population metadata (non-mutating copy).
 */
export function annotateInventoryPopulations(opportunities = [], opts = {}) {
  const hotelId = opts.hotelId || null;
  const manifest =
    hotelId === BETHESDA_HOTEL_ID || opts.forceBethesdaManifest
      ? loadBethesda67SalesBaselineManifest()
      : null;
  const forensicById = new Map();
  if (manifest) {
    const forensic = readJson(FORENSIC_RAW);
    for (const row of forensic?.missingRows || []) {
      forensicById.set(row.id, row);
    }
  }

  return (opportunities || []).map((rawOpp) => {
    // Evidence-backed CMI remediation overlay (presentation + classification).
    // Preserves prior fields under commercialConsistencyPrior; does not publish.
    const remediable =
      hotelId === BETHESDA_HOTEL_ID || opts.forceBethesdaManifest
        ? applyCmiLedgerRemediation(rawOpp)
        : rawOpp;
    // Always sanitize unsafe open-RFP / open-lodging presentation once dimensions
    // are known — ledger rows and non-ledger commercial surface alike.
    const opp =
      hotelId === BETHESDA_HOTEL_ID || opts.forceBethesdaManifest
        ? sanitizeCommercialPresentation(remediable)
        : remediable;
    const id = String(opp.id || opp.opportunityId || "");
    const forensicRow = forensicById.get(id) || null;
    const onBaseline = manifest ? isBethesda67BaselineId(id, manifest) : false;
    const published = manifest
      ? isBethesda67PublishedId(id, manifest)
      : Boolean(opp.customerVisible !== false);
    const staged = manifest ? isBethesda67StagedId(id, manifest) : false;
    const maturity = classifySalesBaselineMaturity(opp, forensicRow, {
      publishedBaseline: published,
    });
    const readiness = staged
      ? assessStagedCustomerReadiness(opp, forensicRow)
      : null;
    const cmi = classifyCompetitiveMarketIntelligence(opp);
    const competitiveStatus =
      cmi.competitiveStatus ||
      (cmi.verified
        ? COMPETITIVE_STATUS.VERIFIED
        : cmi.class === CMI_CLASS.HISTORICAL_COMPETITIVE
          ? COMPETITIVE_STATUS.HISTORICAL
          : cmi.class === CMI_CLASS.NEEDS_FURTHER_VERIFICATION
            ? COMPETITIVE_STATUS.NEEDS_VERIFICATION
            : COMPETITIVE_STATUS.NOT_COMPETITIVE);
    const competitiveEligible =
      competitiveStatus === COMPETITIVE_STATUS.VERIFIED ||
      competitiveStatus === COMPETITIVE_STATUS.NEEDS_VERIFICATION ||
      competitiveStatus === COMPETITIVE_STATUS.HISTORICAL;

    const populations = [];
    if (onBaseline || !manifest) {
      // Bethesda 67 baseline + non-Bethesda commercial feed → Hotel / Sales.
      // CMI / Competitive is independent — same ID may qualify for both.
      populations.push(INVENTORY_POPULATION.SALES_OPPORTUNITIES);
    }
    // Competitive: verified + needs verification + historical (explicit status).
    // Never imply Competitive total === verified bookings.
    if (competitiveEligible) {
      populations.push(INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE);
    }

    return {
      ...opp,
      inventoryPopulations: populations,
      inventoryPopulationPrimary:
        populations[0] || INVENTORY_POPULATION.SALES_OPPORTUNITIES,
      salesBaselineMembership: onBaseline
        ? published
          ? "PUBLISHED_19"
          : "STAGED_48"
        : "OUTSIDE_67_BASELINE",
      salesBaselineMaturityKey: maturity.key,
      salesBaselineMaturityLabel: maturity.label,
      stagedCustomerReadiness: readiness?.readiness || null,
      cmiClass: cmi.class,
      cmiVerified: cmi.verified === true,
      competitiveStatus,
      competitiveStatusLabel:
        COMPETITIVE_STATUS_LABEL[competitiveStatus] || competitiveStatus,
      cmiCompetitorHotel: cmi.competitorHotel,
      cmiClassificationReason: cmi.reason,
      cmiEvidenceUrls: cmi.evidenceUrls || null,
      competitorVenueStatus:
        opp.competitorVenueStatus || cmi.competitorVenueStatus || null,
      targetHotelSourcingStatus:
        opp.targetHotelSourcingStatus || cmi.targetHotelSourcingStatus || null,
      commercialActionability:
        opp.commercialActionability || cmi.commercialActionability || null,
    };
  });
}

export function filterByInventoryPopulation(opportunities = [], population) {
  const pop = String(population || INVENTORY_POPULATION.SALES_OPPORTUNITIES);
  if (!pop || pop === INVENTORY_POPULATION.ALL_COMMERCIAL_INTELLIGENCE) {
    return opportunities || [];
  }
  return (opportunities || []).filter((o) => {
    const pops = o.inventoryPopulations || [o.inventoryPopulationPrimary];
    return pops.includes(pop);
  });
}

/**
 * Headline counts — never conflate Sales Opportunities with Sales Ready.
 * Competitive total ≠ verified-booking total.
 */
export function buildInventoryPopulationCounts(opportunities = []) {
  const rows = opportunities || [];
  const sales = rows.filter((o) =>
    (o.inventoryPopulations || []).includes(
      INVENTORY_POPULATION.SALES_OPPORTUNITIES
    )
  );
  const competitiveEligible = rows.filter((o) =>
    (o.inventoryPopulations || []).includes(
      INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE
    )
  );
  const verified = competitiveEligible.filter(
    (o) =>
      o.cmiVerified === true ||
      o.competitiveStatus === COMPETITIVE_STATUS.VERIFIED
  );
  const needsVerification = competitiveEligible.filter(
    (o) => o.competitiveStatus === COMPETITIVE_STATUS.NEEDS_VERIFICATION
  );
  const historical = competitiveEligible.filter(
    (o) => o.competitiveStatus === COMPETITIVE_STATUS.HISTORICAL
  );
  const maturity = {};
  for (const o of sales) {
    const k = o.salesBaselineMaturityKey || "UNKNOWN";
    maturity[k] = (maturity[k] || 0) + 1;
  }
  return {
    salesOpportunities: sales.length,
    competitiveMarketIntelligence: competitiveEligible.length,
    competitiveVerified: verified.length,
    competitiveNeedsVerification: needsVerification.length,
    competitiveHistorical: historical.length,
    allCommercialIntelligence: new Set([
      ...sales.map((o) => o.id),
      ...competitiveEligible.map((o) => o.id),
    ]).size,
    salesReadyAmongSales: maturity.SALES_READY || 0,
    accountDevelopmentAmongSales: maturity.ACCOUNT_DEVELOPMENT || 0,
    maturityBreakdown: maturity,
    headline: `${sales.length} Sales Opportunities + ${competitiveEligible.length} Competitive (${verified.length} verified / ${needsVerification.length} needs verification / ${historical.length} historical)`,
  };
}

export function filterByCompetitiveStatus(opportunities = [], statusFilter) {
  const raw = String(statusFilter || COMPETITIVE_STATUS_FILTER.ALL)
    .trim()
    .toUpperCase();
  const rows = (opportunities || []).filter((o) =>
    (o.inventoryPopulations || []).includes(
      INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE
    )
  );
  if (
    !raw ||
    raw === COMPETITIVE_STATUS_FILTER.ALL ||
    raw === "ALL" ||
    raw === "ALL_COMPETITIVE"
  ) {
    return rows;
  }
  const map = {
    VERIFIED: COMPETITIVE_STATUS.VERIFIED,
    NEEDS_VERIFICATION: COMPETITIVE_STATUS.NEEDS_VERIFICATION,
    HISTORICAL: COMPETITIVE_STATUS.HISTORICAL,
  };
  const target = map[raw] || raw;
  return rows.filter((o) => o.competitiveStatus === target);
}

export function buildCompetitiveStatusCounts(opportunities = []) {
  const eligible = (opportunities || []).filter((o) =>
    (o.inventoryPopulations || []).includes(
      INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE
    )
  );
  const counts = {
    ALL_COMPETITIVE: eligible.length,
    VERIFIED: 0,
    NEEDS_VERIFICATION: 0,
    HISTORICAL: 0,
  };
  for (const o of eligible) {
    const s = o.competitiveStatus;
    if (s === COMPETITIVE_STATUS.VERIFIED || o.cmiVerified === true) {
      counts.VERIFIED += 1;
    } else if (s === COMPETITIVE_STATUS.NEEDS_VERIFICATION) {
      counts.NEEDS_VERIFICATION += 1;
    } else if (s === COMPETITIVE_STATUS.HISTORICAL) {
      counts.HISTORICAL += 1;
    }
  }
  return counts;
}

/**
 * Inventory audience — separate sales-team working inventory from published feed.
 * Never treat localhost / query-string alone as publication approval.
 */
export const GDI_INVENTORY_AUDIENCE = Object.freeze({
  SALES_TEAM_INVENTORY: "SALES_TEAM_INVENTORY",
  PUBLISHED_CUSTOMER_FEED: "PUBLISHED_CUSTOMER_FEED",
});

export const BETHESDA_SALES_TEAM_LIST_SCOPE = "BETHESDA_SALES_TEAM_INVENTORY_67";
export const BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE =
  "BETHESDA_PUBLISHED_CUSTOMER_FEED_19";

/**
 * Hotels whose working sales inventory may exceed the published customer feed.
 * Env: GDI_SALES_TEAM_INVENTORY_HOTEL_IDS (comma-separated). Default: Bethesda only.
 * `gdiPilotUnauthRead` alone never expands inventory for hotels outside this set.
 */
export function resolveSalesTeamInventoryHotelAllowlist(env = process.env) {
  const raw = env?.GDI_SALES_TEAM_INVENTORY_HOTEL_IDS;
  if (raw == null || String(raw).trim() === "") {
    return new Set([BETHESDA_HOTEL_ID]);
  }
  return new Set(
    String(raw)
      .split(/[,;\s]+/)
      .map((s) => String(s || "").trim())
      .filter(Boolean)
  );
}

function collectUserHotelScopeIds(user = {}) {
  const bags = [
    user.allowedHotelIds,
    user.gdiHotelIds,
    user.hotelIds,
    user.authorizedHotelIds,
    user.gdiAllowedHotelIds,
  ];
  const out = [];
  for (const bag of bags) {
    if (!Array.isArray(bag)) continue;
    for (const id of bag) {
      const s = String(id || "").trim();
      if (s && !out.includes(s)) out.push(s);
    }
  }
  return out;
}

/** Explicit sales-inventory hotel grants — never inferred from missing scope. */
function collectExplicitSalesInventoryHotelIds(user = {}) {
  const bags = [
    user.gdiSalesInventoryHotelIds,
    user.salesInventoryHotelIds,
    user.gdiSalesTeamInventoryHotelIds,
  ];
  const out = [];
  for (const bag of bags) {
    if (!Array.isArray(bag)) continue;
    for (const id of bag) {
      const s = String(id || "").trim();
      if (s && !out.includes(s)) out.push(s);
    }
  }
  return out;
}

function isFounderOrAdminDealalityUser(user = {}) {
  if (!user || typeof user !== "object") return false;
  if (user.isAdmin === true) return true;
  const role = String(user.role || user.primaryRole || "").toLowerCase();
  if (role === "admin" || role === "founder") return true;
  if (user.flags?.platformAdmin === true || user.flags?.gdiFounder === true) {
    return true;
  }
  // Never trust client-supplied role strings beyond known server-resolved fields.
  return false;
}

/**
 * Local-only founder capability for sales-team inventory.
 * Production: always false unless GDI_SALES_INVENTORY_CAPABILITY_TOKEN matches.
 * Local/dev: requires explicit GDI_LOCAL_SALES_TEAM_INVENTORY_CAPABILITY=1
 * (defaults ON for non-Railway local so authorized founder browser keeps 67).
 */
export function hasServerIssuedSalesInventoryCapability(
  req = {},
  hotelId = "",
  env = process.env
) {
  const id = String(hotelId || "").trim();
  if (!id) return false;

  // Prefer middleware-attached capability (never client query alone).
  const cap = req?.gdiSalesInventoryCapability;
  if (cap && typeof cap === "object") {
    const hotels = Array.isArray(cap.hotelIds) ? cap.hotelIds : [];
    if (hotels.map(String).includes(id) && cap.scope === "SALES_TEAM_INVENTORY") {
      return true;
    }
  }

  // Shared secret capability token (server env) — header must match; hotel must be allowlisted.
  const expected = String(env.GDI_SALES_INVENTORY_CAPABILITY_TOKEN || "").trim();
  const provided = String(
    req?.headers?.["x-gdi-sales-inventory-capability"] ||
      req?.gdiSalesInventoryCapabilityToken ||
      ""
  ).trim();
  if (expected && provided && provided === expected) {
    return true;
  }

  // Local founder browser capability — explicit, fail-closed in production/Railway.
  const railwayEnv = String(env.RAILWAY_ENVIRONMENT || "").trim().toLowerCase();
  const isProd =
    String(env.NODE_ENV || "").toLowerCase() === "production" ||
    railwayEnv === "production" ||
    Boolean(env.RAILWAY_PROJECT_ID && railwayEnv);
  if (isProd) return false;

  const localCapRaw = env.GDI_LOCAL_SALES_TEAM_INVENTORY_CAPABILITY;
  const localCapEnabled =
    localCapRaw == null || String(localCapRaw).trim() === ""
      ? true // local default ON for founder Opportunities page
      : ["1", "true", "yes", "on"].includes(
          String(localCapRaw).trim().toLowerCase()
        );
  if (!localCapEnabled) return false;
  // Pilot-read alone is still insufficient without local capability flag above.
  if (req?.gdiPilotUnauthRead === true) return true;
  return false;
}

/**
 * Hotel-scoped authorization for expanded (unpublished-inclusive) sales inventory.
 *
 * FAIL CLOSED. A hotel allowlist is necessary but never sufficient.
 * Positive evidence required (one of):
 *  1) Server-resolved Dealality founder/admin
 *  2) Explicit sales-inventory hotel permission on the user record
 *  3) Server-issued hotel sales-inventory capability (token/local founder)
 *
 * Missing hotel-scope data ≠ permission.
 * Client-supplied roles / audience / hotel IDs are not trusted as grants.
 * Pilot-unauth alone never unlocks unpublished inventory.
 */
export function assertHotelScopedSalesTeamInventoryAccess(
  req = {},
  opts = {}
) {
  const env = opts.env || process.env;
  const hotelId = String(opts.hotelId || req?.params?.hotelId || "").trim();
  const allowlist = resolveSalesTeamInventoryHotelAllowlist(env);
  const hotelAllowlisted = Boolean(hotelId) && allowlist.has(hotelId);

  if (!hotelId) {
    return {
      ok: false,
      reason: "hotel_id_required",
      hotelAllowlisted: false,
    };
  }
  if (!hotelAllowlisted) {
    return {
      ok: false,
      reason: "hotel_not_on_sales_team_inventory_allowlist",
      hotelAllowlisted: false,
    };
  }

  // Share / external surfaces: never expand.
  if (
    opts.forcePublished === true ||
    opts.shareSurface === true ||
    req?.gdiShareCapability ||
    req?.gdiSignedShare ||
    req?.gdiExternalShare === true
  ) {
    return {
      ok: false,
      reason: "share_or_external_surface",
      hotelAllowlisted: true,
    };
  }

  // Client audience hints are ignored for grants (checked only as denials upstream).

  const user = req?.dealalityUser || null;
  if (user) {
    // Cross-hotel exclusion still applies when an explicit deny-scope is present.
    const scoped = collectUserHotelScopeIds(user);
    if (scoped.length > 0 && !scoped.includes(hotelId)) {
      return {
        ok: false,
        reason: "user_hotel_scope_excludes_requested_hotel",
        hotelAllowlisted: true,
        userHotelScope: scoped,
      };
    }

    if (isFounderOrAdminDealalityUser(user)) {
      return {
        ok: true,
        reason: "dealality_founder_or_admin",
        hotelAllowlisted: true,
        via: "dealalityUser.admin",
      };
    }

    const salesHotels = collectExplicitSalesInventoryHotelIds(user);
    if (salesHotels.includes(hotelId)) {
      return {
        ok: true,
        reason: "explicit_sales_inventory_hotel_permission",
        hotelAllowlisted: true,
        via: "dealalityUser.gdiSalesInventoryHotelIds",
      };
    }

    // Missing / empty sales-inventory grant → fail closed (do not treat as all-hotels).
    return {
      ok: false,
      reason: "dealality_user_lacks_sales_inventory_grant",
      hotelAllowlisted: true,
      via: "dealalityUser",
    };
  }

  if (hasServerIssuedSalesInventoryCapability(req, hotelId, env)) {
    return {
      ok: true,
      reason: "server_issued_sales_inventory_capability",
      hotelAllowlisted: true,
      via: "salesInventoryCapability",
    };
  }

  if (req?.gdiPilotUnauthRead === true) {
    return {
      ok: false,
      reason: "pilot_unauth_insufficient_without_capability",
      hotelAllowlisted: true,
      via: "gdiPilotUnauthRead",
    };
  }

  return {
    ok: false,
    reason: "no_sales_team_inventory_authorization",
    hotelAllowlisted: true,
  };
}

/**
 * Resolve which inventory the caller is authorized to see.
 *
 * Sales-team inventory (67): only when `assertHotelScopedSalesTeamInventoryAccess`
 * returns ok (founder/admin, explicit sales-inventory hotel grant, or
 * server-issued capability). Client `inventoryAudience=SALES_TEAM` never grants.
 * Fail closed → published customer feed (19).
 */
export function resolveGdiInventoryAudience(req = {}, opts = {}) {
  const hotelId = String(opts.hotelId || req?.params?.hotelId || "").trim();
  const q = req?.query || opts.query || {};
  const explicit = String(
    q.inventoryAudience || q.audience || opts.inventoryAudience || ""
  )
    .trim()
    .toUpperCase();

  if (
    explicit === "PUBLISHED" ||
    explicit === "PUBLISHED_CUSTOMER" ||
    explicit === "PUBLISHED_CUSTOMER_FEED" ||
    explicit === GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED
  ) {
    return GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED;
  }

  // Share / external capability routes must never expand to sales-team inventory.
  if (
    opts.forcePublished === true ||
    opts.shareSurface === true ||
    req?.gdiShareCapability ||
    req?.gdiSignedShare ||
    req?.gdiExternalShare === true
  ) {
    return GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED;
  }

  const access = assertHotelScopedSalesTeamInventoryAccess(req, {
    hotelId,
    forcePublished: opts.forcePublished,
    shareSurface: opts.shareSurface,
    env: opts.env,
  });

  // Client wantsSalesTeam is ignored as a grant — access.ok is the only expander.
  if (access.ok) {
    return GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY;
  }

  return GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED;
}

/**
 * Detail/list parity: unpublished rows require sales-team inventory audience.
 * Returns { ok, audience, reason } — callers should 404 on !ok for unpublished.
 */
export function assertOpportunityReadableForInventoryAudience(
  req = {},
  opportunity = {},
  opts = {}
) {
  const hotelId = String(opts.hotelId || req?.params?.hotelId || "").trim();
  const opportunityId = String(
    opportunity?.id || opportunity?.opportunityId || opts.opportunityId || ""
  ).trim();
  const audience =
    opts.inventoryAudience ||
    resolveGdiInventoryAudience(req, { hotelId, ...opts });

  if (audience === GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY) {
    return { ok: true, audience, reason: "sales_team_inventory" };
  }

  // Published audience: only published Bethesda baseline IDs (or customer-facing).
  if (hotelId === BETHESDA_HOTEL_ID && opportunityId) {
    const manifest = loadBethesda67SalesBaselineManifest();
    const published = new Set(manifest.publishedIds || []);
    if (published.has(opportunityId)) {
      return { ok: true, audience, reason: "published_baseline_id" };
    }
    if ((manifest.baselineIds || []).includes(opportunityId)) {
      return {
        ok: false,
        audience,
        reason: "unpublished_baseline_requires_sales_team_inventory",
      };
    }
  }

  if (opts.isCustomerFacing === true) {
    return { ok: true, audience, reason: "customer_facing" };
  }
  if (opts.isCustomerFacing === false) {
    return {
      ok: false,
      audience,
      reason: "not_customer_facing_under_published_audience",
    };
  }

  return { ok: true, audience, reason: "published_audience_default" };
}

/**
 * Select the authorized sales-opportunity set for a surface.
 * Customer/share: published 19 until founder approval; then full 67 baseline.
 * Sales-team / founder review: always full 67 when rows are available.
 */
export function selectBethesdaSalesOpportunityInventory(
  opportunities = [],
  opts = {}
) {
  const manifest = loadBethesda67SalesBaselineManifest();
  const byId = new Map(
    (opportunities || []).map((o) => [String(o.id || o.opportunityId), o])
  );
  const approved = isBethesda67CustomerPublicationApproved(opts);
  const founderReview = opts.founderReview === true;
  const salesTeamInventory =
    opts.salesTeamInventory === true ||
    opts.inventoryAudience === GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY;
  const ids =
    founderReview || salesTeamInventory || approved
      ? manifest.baselineIds
      : manifest.publishedIds;
  const selected = [];
  const missing = [];
  for (const id of ids) {
    const row = byId.get(id);
    if (row) selected.push(row);
    else missing.push(id);
  }
  return {
    hotelId: BETHESDA_HOTEL_ID,
    approved,
    founderReview,
    salesTeamInventory,
    inventoryAudience:
      founderReview || salesTeamInventory || approved
        ? GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY
        : GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED,
    expectedCount: ids.length,
    selected,
    missingIds: missing,
    publishedCount: manifest.publishedIds.filter((id) => byId.has(id)).length,
    stagedPresentCount: manifest.stagedIds.filter((id) => byId.has(id)).length,
  };
}
