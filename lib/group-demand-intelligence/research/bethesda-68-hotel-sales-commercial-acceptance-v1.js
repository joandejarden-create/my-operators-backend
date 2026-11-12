/**
 * Bethesda 68 — Hotel Sales Commercial Acceptance V1
 *
 * DOS/DOSM-style review of the ACTUAL default commercial list (not the raw 88).
 * Reuses quality contract, eligibility, contact snapshot, demand-type, surface
 * disposition, and sales-action resolver. Does not invent contacts or demand.
 *
 * Usability verdicts are commercial judgment overlays — they do NOT replace
 * product maturity fields and do NOT auto-publish.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  loadHotelOpportunityDoc,
  sha256File,
  hotelOpportunitiesPath,
  COMMERCIAL_ACCEPTANCE_REMEDIATED_59_ROW_SHA256,
  BETHESDA_PROTECTED_SCALING_IDS,
  BETHESDA_EXPECTED_UNION_COUNT,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./bethesda-internal-research-union-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "./bethesda-88-commercial-event-family-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
  primaryCommercialForbiddenIdList,
  summarizeCommercialSurfaceBatch,
  COMMERCIAL_SURFACE,
} from "../gdi-commercial-surface-disposition-v1.js";
import {
  evaluateOpportunityQualityContract,
  COMMERCIAL_CERT,
} from "../gdi-opportunity-quality-contract-v1.js";
import {
  extractContactSnapshot,
  COVERAGE_BUCKET,
} from "./bethesda-88-contact-quality-completion-v1.js";
import { resolveCustomerSalesAction } from "../customer-sales-action-resolver-v1.js";
import { HOTEL_REVENUE_STREAM_CLASS } from "../commercial-demand-type-v1.js";
import { writeJsonAtomic } from "./gdi-research-stage-persistence-v1.js";

export const BETHESDA_68_HOTEL_SALES_COMMERCIAL_ACCEPTANCE_V1 =
  "gdi-bethesda-68-hotel-sales-commercial-acceptance-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_DIR_REL = "reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1";

/** Hotel-sales usability verdicts (overlay — not product maturity). */
export const SALES_USABILITY = Object.freeze({
  ACTIONABLE_NOW: "ACTIONABLE_NOW",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  QUALIFY_FIRST: "QUALIFY_FIRST",
  MONITOR: "MONITOR",
  REMOVE_FROM_COMMERCIAL: "REMOVE_FROM_COMMERCIAL",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function len(v) {
  return text(v).length;
}

function upper(v) {
  return text(v).toUpperCase();
}

function firstDate(opp) {
  return text(
    opp.eventStartDate ||
      opp.startDate ||
      opp.futureEditionDate ||
      opp.eventDate ||
      opp.demandWindowStart ||
      "",
  );
}

function isPastEvent(opp, now = new Date()) {
  const d = firstDate(opp);
  if (!/^\d{4}-\d{2}-\d{2}/.test(d)) return false;
  const dt = new Date(d.slice(0, 10) + "T00:00:00Z");
  if (Number.isNaN(dt.getTime())) return false;
  return dt.getTime() < now.getTime() - 14 * 86400000;
}

function revenueLabel(cls) {
  const c = upper(cls);
  if (c === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY || c === "GUEST_ROOMS") {
    return "GUEST_ROOMS";
  }
  if (c === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY || c === "LOCAL_CATERING") {
    return "LOCAL_CATERING";
  }
  if (
    c === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB ||
    c === "BOTH"
  ) {
    return "BOTH";
  }
  return "UNCONFIRMED";
}

function hotelFitBand(opp, quality) {
  const honesty = upper(opp.hotelFitHonesty || opp.hotelFitQuality || "");
  if (/DISQUALIFIED|DESTINATION_OUTSIDE|OUT_OF_MARKET/.test(honesty)) {
    return "DISQUALIFIED";
  }
  if (/OVERFLOW|COMPETITOR_HOST|CATEGORY_WATCH/.test(honesty)) {
    return "OVERFLOW_OR_WATCH";
  }
  const fitAxis = (quality.axes || []).find((a) => a.axis === "HOTEL_FIT");
  if (fitAxis && fitAxis.ok === false) return "WEAK";
  const fitText = text(
    opp.hotelFitRationale ||
      opp.hotelFitNote ||
      opp.whyThisHotel ||
      opp.summaryWhyHotel ||
      opp.localConnection ||
      "",
  );
  if (/do not pursue|only if agency accepts|otherwise watch|overflow only/i.test(fitText)) {
    return "OVERFLOW_OR_WATCH";
  }
  if (len(fitText) >= 100) return "STRONG";
  if (len(fitText) >= 50) return "ADEQUATE";
  if (len(fitText) >= 20) return "THIN";
  return "MISSING";
}

function monitorActionSignal(opp) {
  const action = text(
    opp.recommendedAction ||
      opp.recommendedNextAction ||
      opp.suggestedAction ||
      opp.recommendedSalesAction ||
      "",
  );
  return /do not pursue|do not aggressively sell|do not force immediate sales|monitor venue\/housing|monitor venue|keep research continuation|overflow posture|retain overflow watch|continue partnership watch/i.test(
    action,
  );
}

function demandConfidence(opp, quality) {
  const thesis = quality?.detail?.demandThesis || quality?.demandThesis || null;
  const raw = upper(
    opp.demandConfidence ||
      opp.demandEvidenceClass ||
      opp.demandStatus ||
      thesis?.thesisStandard ||
      "",
  );
  if (/CONFIRMED/.test(raw)) return "CONFIRMED";
  if (/RELIABLE|INFERRED/.test(raw)) return "RELIABLE_INFERRED";
  if (/SPECULATIVE/.test(raw)) return "SPECULATIVE";
  const body = text(
    opp.whatIsHappening ||
      opp.hotelOpportunityThesis ||
      opp.summaryWhat ||
      opp.description ||
      "",
  );
  if (len(body) >= 120 && (opp.sources || []).length >= 1) return "RELIABLE_INFERRED";
  if (len(body) >= 40) return "SPECULATIVE";
  return "INSUFFICIENT";
}

function genericCopySignals(opp) {
  const blob = [
    opp.summaryWhat,
    opp.whatIsHappening,
    opp.description,
    opp.tileCustomerSummary,
    opp.hotelOpportunityThesis,
  ]
    .map(text)
    .join(" ");
  const hits = [];
  if (/organization is associated with/i.test(blob)) hits.push("GENERIC_ASSOCIATED_PHRASE");
  if (/commercial lodging details remain to be confirmed/i.test(blob)) {
    hits.push("PLACEHOLDER_LODGING_COPY");
  }
  if (/reach out to the organizer|explore opportunities|build relationship/i.test(blob)) {
    hits.push("GENERIC_OUTREACH_COPY");
  }
  if (len(blob) < 40) hits.push("THIN_DESCRIPTION");
  return hits;
}

function buildSpecificNextAction(opp, contact, sales, usability) {
  const org = text(opp.organizationName || opp.company || "the organization");
  const title = text(opp.title || opp.opportunityName || "this opportunity");
  const name = text(contact.name);
  const role = text(contact.role || contact.title);
  const who = name
    ? `${name}${role ? ` (${role})` : ""} at ${org}`
    : contact.orgEmail || contact.orgRoute
      ? `the verified organization route for ${org}`
      : `a verified commercial contact at ${org}`;
  const rev = revenueLabel(opp.revenueStreamClass || opp.commercialDemandType);
  const stream =
    rev === "LOCAL_CATERING"
      ? "local meeting / catering business"
      : rev === "BOTH"
        ? "guestroom and local catering business"
        : "guestroom / housing business";
  const date = firstDate(opp);
  const when = date ? ` around ${date}` : "";

  if (usability === SALES_USABILITY.MONITOR) {
    const trigger = date
      ? `when housing/venue status updates closer to ${date}`
      : "when the next public housing or destination confirmation appears";
    return `Monitor ${title} (${org}) and refresh Bethesda Marriott fit ${trigger}; do not pursue active outreach until a concrete lodging or catering opening is evidenced.`;
  }
  if (usability === SALES_USABILITY.QUALIFY_FIRST) {
    return `Qualify with ${who}: confirm whether ${title}${when} still needs ${stream} in the Bethesda / Montgomery County area and whether a room block or banquet RFP path is open before pitching Bethesda Marriott.`;
  }
  if (usability === SALES_USABILITY.ACCOUNT_DEVELOPMENT) {
    return `Open an account-development conversation with ${who} about ${org}'s recurring ${stream} needs near Bethesda Marriott; lead with the hotel's NIH / corporate corridor position and ask what upcoming programs need lodging or meeting space.`;
  }
  if (usability === SALES_USABILITY.ACTIONABLE_NOW) {
    const actionKey = upper(sales?.key || "");
    if (/PURSUE_CATERING|CATERING/.test(actionKey)) {
      return `Contact ${who} to propose Bethesda Marriott meeting/banquet space for ${title}${when}; ask about headcount, preferred dates, and whether an RFP or site visit is planned.`;
    }
    if (/BUILD_ACCOUNT|ACCOUNT/.test(actionKey)) {
      return `Contact ${who} to introduce Bethesda Marriott as a lodging partner for ${org}'s near-term programs tied to ${title}; ask which upcoming dates need rooms or day meetings.`;
    }
    return `Contact ${who} about ${title}${when} to determine whether Bethesda Marriott can bid on ${stream}; confirm current venue/housing status and decision timeline.`;
  }
  return `Do not pursue on the primary commercial list until research confirms a Bethesda-relevant demand path for ${title}.`;
}

/**
 * Map engine maturity + contact/fit/demand into a DOS usability verdict.
 */
export function evaluateSalesUsability(opp, quality, contact) {
  const blockers = [];
  const cert = quality.commercialCertification;
  const fit = hotelFitBand(opp, quality);
  const demand = demandConfidence(opp, quality);
  const generics = genericCopySignals(opp);
  const past = isPastEvent(opp);
  const honesty = upper(opp.hotelFitHonesty || "");

  if (
    cert === COMMERCIAL_CERT.DISQUALIFIED ||
    fit === "DISQUALIFIED" ||
    /DESTINATION_OUTSIDE|OUT_OF_MARKET/.test(honesty)
  ) {
    return {
      usability: SALES_USABILITY.REMOVE_FROM_COMMERCIAL,
      reason: "Disqualified or outside-market — should not be on commercial list",
      blockers: ["DISQUALIFIED_OR_OUTSIDE"],
    };
  }

  // Soft remove: past one-off with no recurring thesis
  if (past && !/recurring|annual|ongoing|account|scaling/i.test(text(opp.title) + text(opp.category))) {
    blockers.push("PAST_EVENT_STALE");
  }

  if (demand === "INSUFFICIENT" && fit === "MISSING") {
    return {
      usability: SALES_USABILITY.REMOVE_FROM_COMMERCIAL,
      reason: "Insufficient demand evidence and hotel fit for a salesperson",
      blockers: ["INSUFFICIENT_DEMAND", "MISSING_FIT"],
    };
  }

  const named = contact.named === true;
  const route = Boolean(contact.hasDirectEmail || contact.orgEmail || contact.orgRoute);
  const salesReadyEligible = cert === COMMERCIAL_CERT.SALES_READY_ELIGIBLE;
  const adEligible = cert === COMMERCIAL_CERT.ACCOUNT_DEVELOPMENT_ELIGIBLE;
  const partnership = cert === COMMERCIAL_CERT.PARTNERSHIP_WATCH;
  const overflow = cert === COMMERCIAL_CERT.OVERFLOW_WATCH;
  const monitorSignal = monitorActionSignal(opp);
  const priority = priorityBand(opp);

  // Explicit monitor / overflow posture beats engine Sales Ready stamps.
  if (
    overflow ||
    fit === "OVERFLOW_OR_WATCH" ||
    upper(opp.priority) === "WATCHLIST" ||
    (monitorSignal && (fit === "THIN" || fit === "MISSING" || fit === "OVERFLOW_OR_WATCH"))
  ) {
    if (!named && demand === "SPECULATIVE") {
      return {
        usability: SALES_USABILITY.QUALIFY_FIRST,
        reason: "Watch/overflow posture with unresolved buyer or speculative demand",
        blockers: [...blockers, "UNRESOLVED_BUYER_OR_SPECULATIVE"],
      };
    }
    return {
      usability: SALES_USABILITY.MONITOR,
      reason: "Future-cycle, overflow, or partnership watch — clear monitor posture",
      blockers: monitorSignal ? [...blockers, "RECOMMENDED_ACTION_MONITOR"] : blockers,
    };
  }

  // Destination still unconfirmed / competitor-host corridor → qualify before "act now"
  const destinationUnconfirmed =
    /DESTINATION_UNCONFIRMED|HOTEL_NOT_YET|HOTEL NOT YET|hotel not yet named|hotel TBA|hotel not named/i.test(
      text(
        [
          opp.hotelFitHonesty,
          opp.title,
          opp.eventLocationSummary,
          opp.hotelFitNote,
        ].join(" "),
      ),
    );

  if (
    salesReadyEligible &&
    named &&
    route &&
    fit !== "WEAK" &&
    fit !== "MISSING" &&
    !monitorSignal &&
    !destinationUnconfirmed
  ) {
    return {
      usability: SALES_USABILITY.ACTIONABLE_NOW,
      reason: "Engine Sales Ready eligible with named contact + route and defensible fit",
      blockers,
    };
  }

  if (
    (salesReadyEligible || adEligible) &&
    named &&
    route &&
    destinationUnconfirmed
  ) {
    return {
      usability: SALES_USABILITY.QUALIFY_FIRST,
      reason:
        "Named contact exists but host hotel / lodging destination is not yet confirmed for Bethesda pursuit",
      blockers: [...blockers, "DESTINATION_OR_HOST_UNCONFIRMED"],
    };
  }

  // High-priority local accounts with named buyer + route can start outreach now
  // even when product maturity is still Account Development (not auto-published).
  if (
    (adEligible || partnership || salesReadyEligible) &&
    named &&
    route &&
    priority === "HIGH" &&
    demand !== "INSUFFICIENT" &&
    fit !== "WEAK" &&
    fit !== "MISSING" &&
    !monitorSignal &&
    !generics.includes("PLACEHOLDER_LODGING_COPY")
  ) {
    return {
      usability: SALES_USABILITY.ACTIONABLE_NOW,
      reason:
        "High-priority commercial path with named contact + outreach route — salesperson can begin appropriate outreach",
      blockers,
    };
  }

  if (
    (adEligible || partnership || salesReadyEligible) &&
    named &&
    (route || contact.coverage === COVERAGE_BUCKET.C) &&
    demand !== "INSUFFICIENT"
  ) {
    if (
      generics.includes("PLACEHOLDER_LODGING_COPY") ||
      fit === "THIN" ||
      fit === "WEAK" ||
      fit === "MISSING" ||
      monitorSignal
    ) {
      return {
        usability: SALES_USABILITY.QUALIFY_FIRST,
        reason: "Credible account path but lodging/fit still needs salesperson qualification",
        blockers: [...blockers, ...generics, monitorSignal ? "RECOMMENDED_ACTION_MONITOR" : null].filter(Boolean),
      };
    }
    return {
      usability: SALES_USABILITY.ACCOUNT_DEVELOPMENT,
      reason: "Credible organization + named contact for proactive account development",
      blockers,
    };
  }

  if (named && route && demand !== "INSUFFICIENT") {
    return {
      usability: SALES_USABILITY.QUALIFY_FIRST,
      reason: "Promising but maturity/fit still require qualification before outreach",
      blockers,
    };
  }

  if (demand === "INSUFFICIENT" || (!named && !route)) {
    return {
      usability: SALES_USABILITY.MONITOR,
      reason: "Retain for research refresh; not yet salesperson-actionable",
      blockers: [...blockers, !named ? "NO_NAMED_CONTACT" : null, !route ? "NO_ROUTE" : null].filter(Boolean),
    };
  }

  return {
    usability: SALES_USABILITY.QUALIFY_FIRST,
    reason: "Commercially present but incomplete for immediate outreach",
    blockers,
  };
}

function priorityBand(opp) {
  const p = upper(opp.priorityFilterKey || opp.priority || "");
  if (/HIGH/.test(p)) return "HIGH";
  if (/MEDIUM/.test(p)) return "MEDIUM";
  if (/WATCH|LOW/.test(p)) return "WATCH";
  if (/DISQUALIFIED/.test(p)) return "DISQUALIFIED";
  return "WATCH";
}

/**
 * Load the same commercial list the default GDI API returns for Bethesda.
 */
export function loadBethesdaPrimaryCommercialOpportunities() {
  const doc = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID);
  const storePath = hotelOpportunitiesPath(BETHESDA_HOTEL_ID);
  const storeSha = sha256File(storePath);
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    doc.opportunities || [],
  );
  const family = applyBethesdaCommercialEventFamilyPresentation(union.opportunities, {
    hideLinkedFromList: true,
  });
  const beforeSurface = family.opportunities;
  const surfaceSummary = summarizeCommercialSurfaceBatch(beforeSurface);
  const commercial = filterOpportunitiesByCommercialSurface(
    beforeSurface,
    GDI_LIST_SURFACE_VIEW.COMMERCIAL,
  );
  const forbidden = primaryCommercialForbiddenIdList();
  const leaks = commercial.filter((o) => forbidden.includes(o.id));
  return {
    storePath,
    storeSha,
    expectedStoreSha: COMMERCIAL_ACCEPTANCE_REMEDIATED_59_ROW_SHA256,
    storeCount: (doc.opportunities || []).length,
    unionCount: union.opportunities.length,
    familyListCount: beforeSurface.length,
    commercial,
    commercialIds: commercial.map((o) => o.id),
    leaks,
    surfaceSummary,
    protectedScalingPresent: BETHESDA_PROTECTED_SCALING_IDS.every((id) =>
      union.opportunities.some((o) => o.id === id),
    ),
    rawIdsPreserved: union.opportunities.length === BETHESDA_EXPECTED_UNION_COUNT,
  };
}

/**
 * Full 68-row commercial acceptance scorecard.
 */
export function buildBethesda68HotelSalesCommercialAcceptance(opts = {}) {
  const loaded = loadBethesdaPrimaryCommercialOpportunities();
  const rows = [];
  const usabilityCounts = {
    [SALES_USABILITY.ACTIONABLE_NOW]: 0,
    [SALES_USABILITY.ACCOUNT_DEVELOPMENT]: 0,
    [SALES_USABILITY.QUALIFY_FIRST]: 0,
    [SALES_USABILITY.MONITOR]: 0,
    [SALES_USABILITY.REMOVE_FROM_COMMERCIAL]: 0,
  };
  const revenueCounts = {
    GUEST_ROOMS: 0,
    LOCAL_CATERING: 0,
    BOTH: 0,
    UNCONFIRMED: 0,
  };
  const contactCoverage = {};
  const maturityEngine = {};
  const remediationCandidates = [];
  const maturityPromotionCandidates = [];

  for (const opp of loaded.commercial) {
    const quality = evaluateOpportunityQualityContract(opp, {
      familyRole: opp.commercialEventFamilyRole,
      familyId: opp.commercialEventFamilyId,
    });
    const contact = extractContactSnapshot(opp);
    const sales = resolveCustomerSalesAction(opp);
    const usabilityEval = evaluateSalesUsability(opp, quality, contact);
    usabilityCounts[usabilityEval.usability] += 1;
    const rev = revenueLabel(opp.revenueStreamClass || opp.commercialDemandType);
    revenueCounts[rev] = (revenueCounts[rev] || 0) + 1;
    contactCoverage[contact.coverage] = (contactCoverage[contact.coverage] || 0) + 1;
    maturityEngine[quality.commercialCertification] =
      (maturityEngine[quality.commercialCertification] || 0) + 1;

    const nextAction = buildSpecificNextAction(
      opp,
      contact,
      sales,
      usabilityEval.usability,
    );
    const generics = genericCopySignals(opp);
    const fit = hotelFitBand(opp, quality);
    const demand = demandConfidence(opp, quality);

    const row = {
      id: opp.id,
      title: text(opp.title || opp.opportunityName).slice(0, 140),
      organization: text(opp.organizationName || opp.company).slice(0, 100),
      eventOrProject: text(opp.title || opp.opportunityName).slice(0, 140),
      revenueType: rev,
      revenueStreamClass: opp.revenueStreamClass || null,
      businessPotential: text(
        opp.hotelOpportunityThesis ||
          opp.whyThisHotel ||
          opp.summaryWhyHotel ||
          opp.whatIsHappening ||
          "",
      ).slice(0, 220),
      namedContact: contact.named ? contact.name : null,
      contactRole: contact.role || contact.title || null,
      contactCoverage: contact.coverage,
      hasDirectEmail: contact.hasDirectEmail === true,
      orgRoute: contact.orgEmail || contact.orgRoute || null,
      demandConfidence: demand,
      hotelFit: fit,
      engineCommercialCertification: quality.commercialCertification,
      persistedPriority: priorityBand(opp),
      persistedMaturity: text(opp.commercialMaturity || opp.priorityPillLabel || ""),
      salesActionKey: sales.key,
      salesUsability: usabilityEval.usability,
      salesUsabilityReason: usabilityEval.reason,
      nextAction,
      generics,
      blockers: usabilityEval.blockers,
      sourcesCount: Array.isArray(opp.sources) ? opp.sources.length : 0,
      eventDate: firstDate(opp) || null,
      pastEvent: isPastEvent(opp),
      isScalingCompany: BETHESDA_PROTECTED_SCALING_IDS.includes(opp.id),
      displayEligible:
        usabilityEval.usability !== SALES_USABILITY.REMOVE_FROM_COMMERCIAL,
      remediationApplied: [],
      outstandingBlocker: (usabilityEval.blockers || [])[0] || generics[0] || null,
    };

    if (usabilityEval.usability === SALES_USABILITY.REMOVE_FROM_COMMERCIAL) {
      remediationCandidates.push({
        id: opp.id,
        type: "DISPOSITION_TO_RESEARCH_OR_ARCHIVE",
        reason: usabilityEval.reason,
      });
    }
    if (
      quality.commercialCertification === COMMERCIAL_CERT.SALES_READY_ELIGIBLE &&
      upper(opp.commercialMaturity) !== "SALES_READY" &&
      opp.salesReady !== true
    ) {
      maturityPromotionCandidates.push({
        id: opp.id,
        from: opp.commercialMaturity || opp.priority || null,
        to: "SALES_READY",
        evidence: "Engine SALES_READY_ELIGIBLE; not persisted — founder approval required",
      });
    }
    if (
      quality.commercialCertification === COMMERCIAL_CERT.ACCOUNT_DEVELOPMENT_ELIGIBLE &&
      !/ACCOUNT_DEVELOPMENT/i.test(text(opp.commercialMaturity))
    ) {
      maturityPromotionCandidates.push({
        id: opp.id,
        from: opp.commercialMaturity || opp.priority || null,
        to: "ACCOUNT_DEVELOPMENT",
        evidence:
          "Engine ACCOUNT_DEVELOPMENT_ELIGIBLE; persistence/promotion requires founder approval",
      });
    }

    if (generics.length && row.displayEligible) {
      remediationCandidates.push({
        id: opp.id,
        type: "PRESENTATION_COPY",
        reason: generics.join(","),
      });
    }

    rows.push(row);
  }

  const topProspects = rows
    .filter((r) =>
      [
        SALES_USABILITY.ACTIONABLE_NOW,
        SALES_USABILITY.ACCOUNT_DEVELOPMENT,
      ].includes(r.salesUsability),
    )
    .sort((a, b) => {
      const rank = (u) =>
        u === SALES_USABILITY.ACTIONABLE_NOW
          ? 0
          : u === SALES_USABILITY.ACCOUNT_DEVELOPMENT
            ? 1
            : 2;
      const pr = (p) => (p === "HIGH" ? 0 : p === "MEDIUM" ? 1 : 2);
      return (
        rank(a.salesUsability) - rank(b.salesUsability) ||
        pr(a.persistedPriority) - pr(b.persistedPriority)
      );
    })
    .slice(0, 15);

  const focusIds = [
    "gdi_opp_loudoun_college_showcase_2027",
    "gdi_opp_acts_ts27",
    "gdi_opp_nado_ddaa_washcon_2028",
    "gdi_opp_nado_ddaa_washcon_2027_overflow",
    ...BETHESDA_PROTECTED_SCALING_IDS,
  ];
  const focusReconciliation = focusIds.map((id) => {
    const row = rows.find((r) => r.id === id);
    return row
      ? {
          id,
          title: row.title,
          engineEligible: row.engineCommercialCertification,
          persistedMaturity: row.persistedMaturity || null,
          persistedPriority: row.persistedPriority,
          proposedUsability: row.salesUsability,
          proposedNextAction: row.nextAction,
          namedContact: row.namedContact,
          revenueType: row.revenueType,
        }
      : { id, missingFromCommercialList: true };
  });

  return {
    version: BETHESDA_68_HOTEL_SALES_COMMERCIAL_ACCEPTANCE_V1,
    hotelId: BETHESDA_HOTEL_ID,
    generatedAt: new Date().toISOString(),
    lock: {
      storeSha: loaded.storeSha,
      expectedStoreSha: loaded.expectedStoreSha,
      storeShaMatch: loaded.storeSha === loaded.expectedStoreSha,
      storeCount: loaded.storeCount,
      unionCount: loaded.unionCount,
      commercialCount: loaded.commercial.length,
      familyListCount: loaded.familyListCount,
      forbiddenLeaks: loaded.leaks.map((o) => o.id),
      rawIdsPreserved: loaded.rawIdsPreserved,
      protectedScalingPresent: loaded.protectedScalingPresent,
    },
    counts: {
      commercial: rows.length,
      usability: usabilityCounts,
      revenue: revenueCounts,
      contactCoverage,
      engineMaturity: maturityEngine,
      displayEligible: rows.filter((r) => r.displayEligible).length,
      removeFromCommercial: usabilityCounts[SALES_USABILITY.REMOVE_FROM_COMMERCIAL],
    },
    rows,
    topProspects,
    focusReconciliation,
    remediationCandidates,
    maturityPromotionCandidates,
    commercialIds: loaded.commercialIds,
    paidAuthorized: false,
    publicationAuthorized: false,
  };
}

export function writeBethesda68HotelSalesCommercialAcceptanceReports(
  acceptance = buildBethesda68HotelSalesCommercialAcceptance(),
  { root = ROOT } = {},
) {
  const dir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(dir, { recursive: true });
  writeJsonAtomic(path.join(dir, "00-acceptance-summary.json"), {
    version: acceptance.version,
    hotelId: acceptance.hotelId,
    generatedAt: acceptance.generatedAt,
    lock: acceptance.lock,
    counts: acceptance.counts,
    topProspectIds: acceptance.topProspects.map((r) => r.id),
    maturityPromotionCount: acceptance.maturityPromotionCandidates.length,
    remediationCandidateCount: acceptance.remediationCandidates.length,
    paidAuthorized: false,
    publicationAuthorized: false,
  });
  writeJsonAtomic(path.join(dir, "01-commercial-sales-scorecard.json"), {
    version: acceptance.version,
    rows: acceptance.rows,
  });
  writeJsonAtomic(path.join(dir, "02-top-prospects-and-next-actions.json"), {
    topProspects: acceptance.topProspects,
    focusReconciliation: acceptance.focusReconciliation,
  });
  writeJsonAtomic(path.join(dir, "03-maturity-promotion-approval-package.json"), {
    note: "Governed maturity promotions — founder approval required; not auto-applied",
    candidates: acceptance.maturityPromotionCandidates,
  });
  writeJsonAtomic(path.join(dir, "04-remediation-candidates.json"), {
    candidates: acceptance.remediationCandidates,
  });
  writeJsonAtomic(path.join(dir, "05-commercial-id-list.json"), {
    count: acceptance.commercialIds.length,
    ids: acceptance.commercialIds,
  });
  return { reportDir: dir, acceptance };
}
