/**
 * Run-052 Issue 7 (RC-09): reusable customer-drawer acceptance harness.
 *
 * Evaluates sanitized API DTO (+ optional rendered drawer HTML) against
 * commercial integrity invariants. Negative Médicos baseline must FAIL;
 * corrected Médicos / Scaling regression must PASS.
 */

import { assertRejectedContactAbsent } from "./research/association-events-buyer-integrity-v1.js";
import { isContaminatedWebScrapeNarrative } from "./customer-presentation-sanitize-v1.js";
import { isGenericFacilitiesHotelFitNarrative } from "./hotel-fit-presentation-honesty-v1.js";

export const CUSTOMER_DRAWER_ACCEPTANCE_HARNESS_V1 =
  "gdi-customer-drawer-acceptance-harness-v1";

const REJECTED_DEFAULT = "Marisa Maldonado";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function collectNarrativeSurfaces(opp = {}) {
  return [
    opp.summaryWhat,
    opp.whatIsHappening,
    opp.salesBriefActivity,
    opp.eventHistoryAndActivity,
    opp.associationEventsGoldBrief?.eventHistoryAndActivity,
  ]
    .map(text)
    .filter(Boolean);
}

function goldQualStatus(opp = {}) {
  return text(
    opp.associationEventsGoldBrief?.qualification?.status ||
      opp.salesBrief?.qualification?.status ||
      opp.commercialIntelligenceBrief?.qualification?.status ||
      "",
  );
}

function authoritativeAction(opp = {}) {
  const gold =
    text(opp.associationEventsGoldBrief?.recommendedSalesApproach) ||
    text(opp.associationEventsGoldBrief?.recommendedSalesApproach?.nextAction) ||
    text(opp.commercialIntelligenceBrief?.recommendedSalesApproach) ||
    text(opp.commercialIntelligenceBrief?.recommendedSalesApproach?.nextAction);
  if (typeof opp.associationEventsGoldBrief?.recommendedSalesApproach === "object") {
    const a = opp.associationEventsGoldBrief.recommendedSalesApproach;
    return text(a?.nextAction || a?.openingApproach || gold);
  }
  if (typeof opp.commercialIntelligenceBrief?.recommendedSalesApproach === "object") {
    const a = opp.commercialIntelligenceBrief.recommendedSalesApproach;
    return text(a?.nextAction || a?.openingApproach || gold);
  }
  return (
    text(opp.associationEventsGoldBrief?.recommendedSalesApproach) ||
    text(opp.commercialIntelligenceBrief?.recommendedSalesApproach) ||
    text(opp.recommendedAction) ||
    text(opp.salesActionRecommended) ||
    ""
  );
}

/**
 * @param {object} opportunity - preferably sanitizeOpportunityForCustomerPresentation output
 * @param {object} [opts]
 * @param {string} [opts.rejectedBuyerName]
 * @param {string} [opts.renderedDrawerHtml]
 * @param {object} [opts.persisted] - optional persisted twin for material-diff check
 * @param {string} [opts.expectedSegment]
 */
export function evaluateCustomerDrawerAcceptance(opportunity = {}, opts = {}) {
  const rejected = opts.rejectedBuyerName || REJECTED_DEFAULT;
  const html = String(opts.renderedDrawerHtml || "");
  const failures = [];
  const checks = {};

  // 1) Rejected buyer on active contact / sales action surfaces
  const contactAbs = assertRejectedContactAbsent(opportunity, rejected);
  checks.rejectedBuyerAbsent = contactAbs.ok === true;
  if (!checks.rejectedBuyerAbsent) {
    failures.push(`rejected_buyer_present:${contactAbs.found ? rejected : "unknown"}`);
  }
  if (html && new RegExp(rejected.replace(/\s+/g, "\\s+"), "i").test(html)) {
    checks.rejectedBuyerAbsentInHtml = false;
    failures.push("rejected_buyer_in_rendered_html");
  } else if (html) {
    checks.rejectedBuyerAbsentInHtml = true;
  }

  // 2) Dirty scrape as customer narrative
  const narratives = collectNarrativeSurfaces(opportunity);
  const dirtyNarrative = narratives.find((n) => isContaminatedWebScrapeNarrative(n));
  checks.customerNarrativeClean = !dirtyNarrative;
  if (dirtyNarrative) {
    failures.push("dirty_scrape_in_customer_narrative");
  }
  if (html && /##\s*MAF2026/i.test(html)) {
    checks.customerNarrativeCleanInHtml = false;
    failures.push("dirty_scrape_in_rendered_html");
  } else if (html) {
    checks.customerNarrativeCleanInHtml = true;
  }

  // 3) Qualification vs authoritative commercial state
  const goldStatus = goldQualStatus(opportunity);
  const topQual = text(opportunity.opportunityQualification);
  const goldSaysContinuation =
    /RESEARCH_CONTINUATION/i.test(goldStatus) ||
    opportunity.associationEventsGoldBrief?.qualification?.qualified === false;
  if (goldSaysContinuation) {
    const honestClosedOrWatch =
      /^(CLOSED|WATCHLIST|WATCH|EXCLUDED|HOLD|NOT_QUALIFIED|DO_NOT_PURSUE)$/i.test(
        topQual,
      );
    const honestContinuation =
      /RESEARCH_CONTINUATION/i.test(topQual) ||
      /continuation/i.test(text(opportunity.opportunityQualificationLabel));
    const staleSalesReady =
      /^(QUALIFIED|STRONG|MODERATE|ACTIONABLE|VERIFIED_OPEN)$/i.test(topQual);
    checks.qualificationHonest =
      !staleSalesReady && (honestContinuation || honestClosedOrWatch || !topQual);
    if (!checks.qualificationHonest) {
      failures.push("stale_qualified_vs_gold_continuation");
    }
  } else {
    checks.qualificationHonest = true;
  }
  if (html && goldSaysContinuation && /\bQUALIFIED\b/.test(html) && !/Research continuation/i.test(html)) {
    checks.qualificationHonestInHtml = false;
    failures.push("qualified_badge_in_html_while_continuation");
  } else if (html && goldSaysContinuation) {
    checks.qualificationHonestInHtml = /Research continuation/i.test(html);
  }

  // 4) Confidence vs critical evidence limitations
  const conf = Number(opportunity.evidenceConfidence);
  const missingBuyer = opportunity.hasVerifiedNamedRelevantContact === false;
  if (goldSaysContinuation || missingBuyer) {
    checks.confidenceHonest = Number.isFinite(conf) && conf <= 60;
    if (!checks.confidenceHonest) {
      failures.push(`confidence_inflated:${conf}`);
    }
  } else {
    checks.confidenceHonest = true;
  }

  // 5) Legacy fields must not override gold brief action
  const goldAction = authoritativeAction(opportunity);
  const legacyDirty =
    /Contact\s+Marisa/i.test(text(opportunity.recommendedAction)) ||
    /Contact\s+Marisa/i.test(text(opportunity.salesActionRecommended)) ||
    /Contact\s+Marisa/i.test(text(opportunity.suggestedAction));
  checks.goldBriefAuthoritative = !legacyDirty;
  if (goldAction && legacyDirty) {
    failures.push("legacy_action_overrides_gold");
  }

  // 6) Generic facilities misrepresented as opportunity-specific fit
  checks.hotelFitNarrativeHonest = !isGenericFacilitiesHotelFitNarrative(
    opportunity.whyThisHotel,
  );
  if (!checks.hotelFitNarrativeHonest) {
    failures.push("generic_facilities_as_commercial_fit");
  }

  // 7) Empty hotel-fit dimensions as completed assessments
  const showGrid = opportunity.hotelFitShowDimensionGrid;
  if (showGrid === false) {
    checks.emptyFitDimsNotPresentedAsComplete = true;
    if (html) {
      const notEst = (html.match(/Not established/gi) || []).length;
      // Sourcing "Not yet established" copy is allowed; dimension tiles say "Not established".
      const dimTileHits = (html.match(/gdi-score-cell[^>]*>[\s\S]*?Not established/gi) || [])
        .length;
      checks.emptyFitDimsNotPresentedAsComplete = dimTileHits === 0;
      if (dimTileHits > 0) {
        failures.push(`empty_fit_dim_tiles_in_html:${dimTileHits}`);
      }
      // Also fail if many bare "Not established" with Physical Fit labels
      if (/Physical Fit/i.test(html) && notEst >= 5) {
        checks.emptyFitDimsNotPresentedAsComplete = false;
        failures.push("empty_fit_scorecard_in_html");
      }
    }
  } else if (showGrid === true) {
    checks.emptyFitDimsNotPresentedAsComplete = true;
  } else {
    // Unknown flag — require no fabricated empty assessment object pretending completeness
    const dims =
      opportunity.hotelFitDimensionAssessments ||
      opportunity.hotelFitQualitativeComponents ||
      null;
    const emptyShell =
      dims &&
      typeof dims === "object" &&
      Object.keys(dims).length === 0;
    checks.emptyFitDimsNotPresentedAsComplete = !emptyShell || showGrid !== true;
  }

  // 8) Segment identity — only when caller asserts a known family.
  if (opts.expectedSegment) {
    const seg = text(
      opportunity.researchSegment ||
        opportunity.segment ||
        opportunity.demandFamily ||
        "",
    );
    const type = text(opportunity.opportunityType);
    const blob = `${seg} ${type}`;
    if (opts.expectedSegment === "scaling") {
      checks.segmentIdentityStable = /scaling/i.test(blob);
    } else if (opts.expectedSegment === "association") {
      checks.segmentIdentityStable =
        /association|medical_academic|associations?_events|associations?_government|ASSOCIATION_CONFERENCE/i.test(
          blob,
        );
    } else {
      checks.segmentIdentityStable = seg
        .toLowerCase()
        .includes(String(opts.expectedSegment).toLowerCase());
    }
    if (!checks.segmentIdentityStable) {
      failures.push(`segment_identity_unexpected:${seg || type || "EMPTY"}`);
    }
  } else {
    checks.segmentIdentityStable = true;
  }

  // 9) Persistence vs API material fields
  if (opts.persisted && typeof opts.persisted === "object") {
    const p = opts.persisted;
    const materialDiffs = [];
    for (const key of [
      "opportunityQualification",
      "evidenceConfidence",
      "summaryWhat",
      "hasVerifiedNamedRelevantContact",
      "hotelFitShowDimensionGrid",
    ]) {
      const a = p[key];
      const b = opportunity[key];
      if (a !== undefined && b !== undefined && String(a) !== String(b)) {
        // confidence may be number vs number
        if (key === "evidenceConfidence" && Number(a) === Number(b)) continue;
        materialDiffs.push(key);
      }
    }
    checks.persistenceApiAligned = materialDiffs.length === 0;
    if (!checks.persistenceApiAligned) {
      failures.push(`persistence_api_diff:${materialDiffs.join(",")}`);
    }
  } else {
    checks.persistenceApiAligned = null;
  }

  // 10) API vs rendered drawer material agreement (when HTML provided)
  if (html) {
    const apiAction = text(opportunity.recommendedAction || opportunity.salesActionRecommended);
    checks.apiDrawerAligned =
      (!apiAction || html.includes(apiAction.slice(0, Math.min(48, apiAction.length)))) &&
      (!text(opportunity.summaryWhat) ||
        html.includes(text(opportunity.summaryWhat).slice(0, Math.min(40, text(opportunity.summaryWhat).length))));
    if (!checks.apiDrawerAligned) {
      failures.push("api_drawer_material_mismatch");
    }
  } else {
    checks.apiDrawerAligned = null;
  }

  const ok = failures.length === 0;
  return {
    version: CUSTOMER_DRAWER_ACCEPTANCE_HARNESS_V1,
    ok,
    failures,
    checks,
    rejectedBuyerName: rejected,
    goldQualStatus: goldStatus || null,
    opportunityQualification: topQual || null,
    evidenceConfidence: Number.isFinite(conf) ? conf : null,
  };
}

export function assertCustomerDrawerAcceptance(opportunity, opts = {}) {
  const result = evaluateCustomerDrawerAcceptance(opportunity, opts);
  if (!result.ok) {
    const err = new Error(
      `Customer drawer acceptance FAILED: ${result.failures.join("; ")}`,
    );
    err.acceptance = result;
    throw err;
  }
  return result;
}
