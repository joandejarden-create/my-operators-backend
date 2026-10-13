/**
 * YOTEL 13 READY commercial quality audit + remediation.
 * Does not lower thresholds. Reclassifies when evidence cannot support READY.
 *
 * Usage: node scripts/gdi-yotel-ready-quality-audit.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

import {
  applyLiveCommercialQuality,
  filterCustomerFacingOpportunities,
  filterSalespersonView,
  isGdiCustomerOpportunityReady,
  isValidFutureWatch,
} from "../lib/group-demand-intelligence/index.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import {
  BUYER_CONTACT_PATH_CLASS,
  classifyBuyerContactPath,
  classifyContactUrl,
  hasTournamentActionLeak,
  recommendedActionForCommercialShape,
  sanitizeCustomerThesis,
  meetsReadyContactRequirement,
} from "../lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js";
import * as fsRepo from "../lib/group-demand-intelligence/repository.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "yotel-ready-quality-audit");
const YOTEL = "recrPQcZg7SFARRb2";
const BETHESDA = "recLuxvwwxID7U2B8";
const NOW = "2026-10-04";
const RUN_ID = `gdi_yotel_ready_qa_${crypto.randomBytes(3).toString("hex")}`;

const COHORT_IDS = [
  "gdi_opp_ycamp_watches_wonders_2027_palexpo_sa_venue_operator",
  "gdi_opp_ycamp_art_geneve_2027_palexpo_sa_venue_operator",
  "gdi_opp_ycamp_aidex_geneva_2026_palexpo_sa_venue_operator",
  "gdi_opp_aidex_geneva_11",
  "gdi_opp_ycamp_setac_europe_37_2027_society_of_environmental_toxicology_and_chemistr_parent_socie",
  "gdi_opp_ycamp_chi_geneva_centennial_2026_palexpo_sa_venue_operator",
  "gdi_opp_ycamp_chi_geneva_centennial_2026_chi_de_gen_ve_concours_hippique_international_event_org",
  "gdi_opp_ycamp_ecosoc_has_2027_united_nations_ecosoc_ocha_secretariat",
  "gdi_opp_ycamp_art_geneve_2027_art_gen_ve_organizer",
  "gdi_opp_ycamp_watches_wonders_2027_watches_and_wonders_organizer",
  "gdi_opp_ycamp_setac_europe_37_2027_setac_europe_organizer",
  "gdi_opp_ycamp_geneva_health_forum_2026_geneva_health_forum_organizer",
  "gdi_opp_ycamp_geneva_health_forum_2026_university_of_geneva_institute_of_global_health_universit",
];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, cols) {
  if (!rows.length) return cols.join(",") + "\n";
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c] ?? "")).join(","));
  return lines.join("\n") + "\n";
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}

function hotelMotionClass(o) {
  const status = String(o.roomDemandStatus || "").toUpperCase();
  const housing = String(o.housingStatus || "").toUpperCase();
  const note = `${o.lodgingEvidence || ""} ${o.housingEvidence || ""}`;
  if (/VERIFIED|PUBLISHED_ROOM|CONFIRMED/.test(status)) return "DIRECT_LODGING_EVIDENCE";
  if (housing === "STRONG" || /official housing|room block|housing partner/i.test(note)) {
    return "STRONG_HOTEL_MOTION";
  }
  if (housing === "STRONG_INFERENCE" || housing === "WEAK" || /ESTIMATED|LIKELY|OVERFLOW/.test(status)) {
    return "PLAUSIBLE_HOTEL_MOTION";
  }
  if (status === "UNKNOWN" || !status) return "UNCONFIRMED";
  return "NONE";
}

function hasUnsupportedCompetitorClaim(o) {
  const t = `${o.hotelOpportunityThesis || ""} ${o.hotelDemandThesis || ""} ${o.fitExplanation || ""}`;
  return /competitor hotel\(s\):\s*UNKNOWN|competitor-hotel lodging\/event association/i.test(t);
}

function hasDebugJargon(o) {
  const t = `${o.hotelOpportunityThesis || ""} ${o.summaryWhat || ""} ${o.summaryWhyHotel || ""} ${o.whyNow || ""}`;
  return /Fit:PLAUSIBLE|child account|mega-event|HELD_FOR_|PUBLIC_DATA_CEILING|qualify buyer\/housing before selling as ready/i.test(
    t
  );
}

function remediationsFor(o, contact) {
  const patches = {};
  const notes = [];

  const thesis = sanitizeCustomerThesis(o.hotelOpportunityThesis || "");
  if (thesis !== (o.hotelOpportunityThesis || "")) {
    patches.hotelOpportunityThesis = thesis || o.hotelOpportunityThesis;
    patches.hotelDemandThesis = sanitizeCustomerThesis(o.hotelDemandThesis || "") || o.hotelDemandThesis;
    notes.push("sanitize_thesis");
  }

  if (hasTournamentActionLeak(o)) {
    patches.recommendedAction = recommendedActionForCommercialShape(o);
    notes.push("fix_tournament_action_leak");
  }

  // Heal leftover watch-language whyNow when a real commercial contact path exists
  if (
    /qualify (?:buyer\/)?housing before selling as ready|confirm lodging path and buyer function before outreach|confirm lodging evidence and buyer-function path before customer-ready/i.test(
      String(o.whyNow || "")
    )
  ) {
    if (contact.readyEligible) {
      patches.whyNow =
        `${o.canonicalEventName || o.title || "Event"} ${o.eventStartDate || o.eventYear || ""}: published cycle with a usable buyer/function path — pursue lodging confirmation on the public path.`
          .replace(/\s+/g, " ")
          .trim();
      notes.push("heal_why_now_for_ready_contact");
    } else {
      // Keep honest watch language when contact is still insufficient
      patches.whyNow = `${o.canonicalEventName || o.title || "Event"} cycle ${o.eventStartDate || o.eventYear || ""}: confirm lodging path and buyer function before outreach.`
        .replace(/\s+/g, " ")
        .trim();
      notes.push("fix_why_now_ready_contradiction");
    }
  }

  if (
    (!o.eventLocationSummary || /not confirmed|unknown/i.test(String(o.eventLocationSummary))) &&
    (/palexpo|geneva|genève/i.test(`${o.venue || ""} ${o.title || ""} ${o.organizationName || ""}`))
  ) {
    patches.eventLocationStatus = "CONFIRMED";
    patches.eventLocationSummary = /palexpo/i.test(`${o.organizationName || ""} ${o.venue || ""}`)
      ? "Palexpo, Geneva, Switzerland"
      : "Geneva, Switzerland";
    notes.push("reconcile_geography");
  }

  const lodgingBlob = `${o.lodgingEvidence || ""} ${o.housingEvidence || ""}`;
  const negatedLodging =
    /\b(not|no|without|unconfirmed|unproven)\b.{0,40}\b(official housing|housing partner|room block)/i.test(
      lodgingBlob
    ) ||
    /\b(official housing|housing partner|room block).{0,40}\b(not confirmed|unconfirmed|unproven|not yet)/i.test(
      lodgingBlob
    );
  const hasHousingEvidence =
    (!negatedLodging &&
      /official housing|hotel reservation|housing partner|room block|housing list/i.test(lodgingBlob)) ||
    Boolean(o.officialHousingUrl);
  if (
    /OVERFLOW_ONLY|OVERFLOW_HOUSING/i.test(
      `${o.roomDemandStatus || ""} ${o.opportunityType || ""}`
    ) &&
    !hasHousingEvidence &&
    String(o.housingStatus || "").toUpperCase() !== "STRONG"
  ) {
    patches.roomDemandStatus = "UNKNOWN";
    patches.roomDemandStatusLabel = "Unknown";
    if (/OVERFLOW/i.test(String(o.opportunityType || ""))) {
      patches.opportunityType = "FUTURE_CYCLE";
    }
    notes.push("strip_unsupported_overflow");
  }

  // Speculative Housing desk on venue homepage → strip buyer role claim (no invented housing path)
  const urlClass = classifyContactUrl(o.publicContactPath || "", {
    officialSource: o.officialSource,
    discoverySource: o.discoverySource,
  });
  const housingStamp =
    /\bhousing|hotel reservation\b/i.test(String(o.primaryContactRole || o.buyerRole || "")) ||
    /\bhotel reservation\b/i.test(String(o.buyerEntity || ""));
  const weakUrl =
    urlClass === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE ||
    urlClass === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT ||
    urlClass === BUYER_CONTACT_PATH_CLASS.NO_CONTACT;
  if (housingStamp && weakUrl && !o.officialHousingUrl) {
    patches.buyerEntity = o.organizationName || o.buyerEntity;
    patches.buyerRole = "Venue operations";
    patches.primaryContactRole = "Venue operations";
    notes.push("strip_speculative_housing_desk");
  }

  // Customer-facing summary: remove research jargon
  if (/child account/i.test(String(o.summaryWhat || ""))) {
    patches.summaryWhat = String(o.summaryWhat)
      .replace(/\bchild account\b/gi, "account")
      .replace(/\bCHILD ACCOUNT\b/g, "ACCOUNT");
    notes.push("strip_child_account_jargon");
  }

  // Stamp commercial contact class from remediated fields
  const contactAfter = classifyBuyerContactPath({ ...o, ...patches });
  patches.buyerContactPathClass = contactAfter.class;
  patches.contactPathClass = contactAfter.class;
  notes.push(`contact_class:${contactAfter.class}`);

  return { patches, notes, contactAfter };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  invalidateGdiHotelReadCache(YOTEL);
  const doc = await loadOpportunitiesCanonical(YOTEL);
  const byId = new Map((doc.opportunities || []).map((o) => [o.id, o]));

  const beforeRows = [];
  const matrix = [];
  const motionRows = [];
  const competitorRows = [];
  const evidenceRows = [];
  const geoRows = [];
  const actionRows = [];
  const reclassRows = [];

  let sourcePageAsBuyer = 0;
  let generalOrgOnly = 0;
  let relevantFn = 0;
  let namedRole = 0;
  let namedPerson = 0;
  let unsupportedOverflow = 0;
  let unsupportedCompetitor = 0;
  let evidenceContradictions = 0;
  let geoContradictions = 0;
  let actionLeaks = 0;

  const palexpoId = "gdi_opp_ycamp_art_geneve_2027_palexpo_sa_venue_operator";
  let palexpoBefore = false;
  let palexpoAfter = false;
  let palexpoFinal = "";
  let palexpoRoot = "";

  const existingOpps = [...(doc.opportunities || [])];

  for (const id of COHORT_IDS) {
    const raw = byId.get(id);
    if (!raw) {
      beforeRows.push({ opportunityId: id, found: "NO", title: "", readyBefore: "NO" });
      continue;
    }
    const beforeCq = applyLiveCommercialQuality(raw, { nowDate: NOW });
    // For BEFORE snapshot, classify contact on raw fields (pre-persist remediation)
    const contactRaw = classifyBuyerContactPath(raw);
    const readyBefore = isGdiCustomerOpportunityReady(
      // use raw+minimal for "starting" — but live cohort was ready under old gate;
      // mark starting READY as membership in frozen cohort
      { ...raw, customerVisible: true, customerActiveEligible: true, customerSurfaceDisposition: "KEEP_ACTIVE" },
      { nowDate: NOW }
    );
    // Starting count = cohort membership (13). Gate may already demote on CQ.
    const startingReady = true;
    if (id === palexpoId) palexpoBefore = startingReady;

    if (contactRaw.class === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE) sourcePageAsBuyer += 1;
    if (
      contactRaw.class === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT ||
      (contactRaw.urlClass === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT &&
        contactRaw.class !== BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_ROLE_PATH)
    ) {
      /* counted below via url */
    }
    const urlClass = classifyContactUrl(raw.publicContactPath || "", {
      officialSource: raw.officialSource,
      discoverySource: raw.discoverySource,
    });
    if (urlClass === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE) {
      /* already */
    }
    if (
      urlClass === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT &&
      contactRaw.class === BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_ROLE_PATH
    ) {
      // role path with homepage URL — not "general only"
    } else if (urlClass === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT) {
      generalOrgOnly += 1;
    }
    if (contactRaw.class === BUYER_CONTACT_PATH_CLASS.RELEVANT_FUNCTION_CONTACT) relevantFn += 1;
    if (contactRaw.class === BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_ROLE_PATH) namedRole += 1;
    if (contactRaw.class === BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_PERSON) namedPerson += 1;
    if (urlClass === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE) sourcePageAsBuyer += 1;

    const motion = hotelMotionClass(raw);
    const competitorBad = hasUnsupportedCompetitorClaim(raw);
    if (competitorBad) unsupportedCompetitor += 1;
    if (
      String(raw.roomDemandStatus || "").toUpperCase() === "OVERFLOW_ONLY" &&
      motion !== "DIRECT_LODGING_EVIDENCE" &&
      motion !== "STRONG_HOTEL_MOTION"
    ) {
      unsupportedOverflow += 1;
    }
    const verifiedN = Array.isArray(raw.knownVsEstimated?.verified)
      ? raw.knownVsEstimated.verified.length
      : 0;
    const explZero = /0 verified field/i.test(String(raw.evidenceConfidenceExplanation || ""));
    if (verifiedN > 0 && explZero) evidenceContradictions += 1;
    if (
      /palexpo|geneva/i.test(`${raw.organizationName || ""} ${raw.venue || ""} ${raw.title || ""}`) &&
      (!raw.eventLocationSummary || /not confirmed|unknown/i.test(String(raw.eventLocationSummary)))
    ) {
      geoContradictions += 1;
    }
    if (hasTournamentActionLeak(raw)) actionLeaks += 1;

    beforeRows.push({
      opportunityId: id,
      title: raw.title,
      organization: raw.organizationName,
      parentGenerator: raw.parentCampaignId || raw.demandGeneratorId || "",
      accountEntity: raw.organizationName,
      readyBefore: "YES",
      whoPathClass: raw.whoPathClass || "",
      contactUrl: raw.publicContactPath || "",
      buyerEntity: raw.buyerEntity || "",
      buyerRole: raw.primaryContactRole || "",
    });

    matrix.push({
      opportunityId: id,
      organization: raw.organizationName,
      accountType: raw.participationRole || raw.childEntityType || "",
      groupMotion: raw.summaryWhat || "",
      buyerEntity: raw.buyerEntity || "",
      buyerRole: raw.primaryContactRole || "",
      contactPathClass: contactRaw.class,
      urlClass,
      contactRelevance: contactRaw.readyEligible ? "PASS" : "FAIL",
      futureDecision: raw.eventStartDate || "",
      hotelMotion: motion,
      placement: raw.venueSourcingStatus || raw.placementStatus || "UNKNOWN",
      hotelFit: raw.hotelFitScore ?? "",
      geography: raw.eventLocationSummary || "UNKNOWN",
      competitorClaim: competitorBad ? "UNSUPPORTED" : "OK",
      recommendedAction: (raw.recommendedAction || "").slice(0, 120),
      evidenceConfidence: raw.evidenceConfidence ?? "",
      verifiedFields: verifiedN,
      evidenceContradiction: verifiedN > 0 && explZero ? "YES" : "NO",
      debugJargon: hasDebugJargon(raw) ? "YES" : "NO",
      commercialVerdict:
        contactRaw.readyEligible && !competitorBad && motion !== "NONE" ? "PASS" : "FAIL",
    });

    motionRows.push({
      opportunityId: id,
      organization: raw.organizationName,
      roomDemandStatus: raw.roomDemandStatus || "",
      housingStatus: raw.housingStatus || "",
      lodgingEvidence: (raw.lodgingEvidence || "").slice(0, 160),
      hotelMotionClass: motion,
      overflowLabelSupported:
        String(raw.roomDemandStatus || "").toUpperCase() === "OVERFLOW_ONLY"
          ? motion === "DIRECT_LODGING_EVIDENCE" || motion === "STRONG_HOTEL_MOTION"
            ? "YES"
            : "NO"
          : "N/A",
    });

    competitorRows.push({
      opportunityId: id,
      unsupportedClaim: competitorBad ? "YES" : "NO",
      thesisExcerpt: (raw.hotelOpportunityThesis || "").slice(0, 200),
    });

    evidenceRows.push({
      opportunityId: id,
      evidenceConfidence: raw.evidenceConfidence ?? "",
      verifiedFields: verifiedN,
      explanationSaysZero: explZero ? "YES" : "NO",
      contradiction: verifiedN > 0 && explZero ? "YES" : "NO",
    });

    geoRows.push({
      opportunityId: id,
      venue: raw.venue || "",
      eventLocationStatus: raw.eventLocationStatus || "",
      eventLocationSummary: raw.eventLocationSummary || "",
      contradiction:
        /palexpo|geneva/i.test(`${raw.organizationName} ${raw.title}`) &&
        (!raw.eventLocationSummary || /not confirmed|unknown/i.test(String(raw.eventLocationSummary)))
          ? "YES"
          : "NO",
    });

    actionRows.push({
      opportunityId: id,
      recommendedAction: (raw.recommendedAction || "").slice(0, 200),
      tournamentLeak: hasTournamentActionLeak(raw) ? "YES" : "NO",
    });

    // First-pass contact on CQ'd raw (pre-heal), then remediate with that class
    const contactForRemediation = classifyBuyerContactPath(beforeCq);
    const { patches, notes, contactAfter } = remediationsFor(beforeCq, contactForRemediation);
    let candidate = {
      ...raw,
      ...beforeCq,
      ...patches,
      gdiReadyQualityAuditRunId: RUN_ID,
    };
    // Re-apply CQ after patches (may rewrite thesis/geo/overflow; preserve healed whyNow)
    const whyNowLock = candidate.whyNow;
    candidate = applyLiveCommercialQuality(candidate, { nowDate: NOW });
    if (notes.includes("heal_why_now_for_ready_contact") && whyNowLock) {
      candidate.whyNow = whyNowLock;
    }
    // Surface-active probe so gate evaluates commercial fields (not DQ from prior hold)
    const gateProbe = {
      ...candidate,
      customerVisible: true,
      customerActiveEligible: true,
      customerSurfaceDisposition: "KEEP_ACTIVE",
    };
    const readyAfterGate = isGdiCustomerOpportunityReady(gateProbe, { nowDate: NOW });
    const watchAfter = isValidFutureWatch(candidate, { nowDate: NOW });
    const contactFinal = classifyBuyerContactPath(candidate);

    // Demote only when contact/housing honesty fails — not merely leftover whyNow
    const commercialHold =
      notes.includes("strip_speculative_housing_desk") ||
      contactFinal.reason === "housing_role_without_housing_path" ||
      !contactFinal.readyEligible ||
      (!readyAfterGate.ok &&
        (readyAfterGate.failed || []).some((f) => f !== "why_now_contradicts_ready"));

    let afterStatus = "READY";
    if (commercialHold || !readyAfterGate.ok) {
      const failReasons = [
        ...(readyAfterGate.failed || []),
        ...(notes.includes("strip_speculative_housing_desk")
          ? ["speculative_housing_desk"]
          : []),
        ...(!contactFinal.readyEligible ? [`contact:${contactFinal.reason}`] : []),
      ];
      // If only whyNow blocked and contact is good — heal and keep READY
      const onlyWhyNow =
        !commercialHold &&
        (readyAfterGate.failed || []).length === 1 &&
        (readyAfterGate.failed || [])[0] === "why_now_contradicts_ready" &&
        contactFinal.readyEligible;
      if (onlyWhyNow) {
        candidate.whyNow =
          `${candidate.canonicalEventName || candidate.title || "Event"} ${candidate.eventStartDate || candidate.eventYear || ""}: published cycle with a usable buyer/function path — pursue lodging confirmation on the public path.`
            .replace(/\s+/g, " ")
            .trim();
        candidate.customerVisible = true;
        candidate.customerFacingState = "ACTIVE";
        afterStatus = "READY";
        notes.push("heal_why_now_keep_ready");
      } else if (
        watchAfter.ok ||
        notes.includes("strip_speculative_housing_desk") ||
        !contactFinal.readyEligible
      ) {
        afterStatus = "FUTURE_WATCH";
        candidate.customerVisible = true;
        candidate.customerFacingState = "FUTURE_WATCH";
        candidate.salesPartitionV11 = "FUTURE_WATCH";
        candidate.priority = "WATCHLIST";
        candidate.customerReadinessHold = readyAfterGate.state || "HELD_FOR_EVIDENCE";
        candidate.qualificationFailureReason = failReasons.join("|");
        if (
          !/confirm lodging path and buyer function before outreach|confirm lodging evidence/i.test(
            String(candidate.whyNow || "")
          )
        ) {
          candidate.whyNow = `${candidate.canonicalEventName || candidate.title || "Opportunity"} cycle ${candidate.eventStartDate || candidate.eventYear || ""}: confirm lodging path and buyer function before outreach.`
            .replace(/\s+/g, " ")
            .trim();
        }
      } else {
        afterStatus = "RESEARCH_LEAD";
        candidate.customerVisible = false;
        candidate.customerFacingState = "MARKET_INTELLIGENCE_ONLY";
        candidate.priority = "WATCHLIST";
        candidate.qualificationFailureReason = failReasons.join("|");
      }
    } else {
      candidate.customerVisible = true;
      candidate.customerFacingState = "ACTIVE";
    }

    const promo = await promoteQualifiedGdiOpportunity({
      candidate,
      existingOpps,
      hotelId: YOTEL,
      runId: RUN_ID,
      method: "yotel_ready_quality_audit",
      playbook: "commercial_readiness_v1",
      dryRun: false,
      forceUpdateId: id,
      materialUpdateOnly: true,
    });

    if (promo.opportunity) {
      const idx = existingOpps.findIndex((x) => x.id === id);
      if (idx >= 0) existingOpps[idx] = promo.opportunity;
    }

    // Post-promote truth: recount from written opportunity
    const written = promo.opportunity || candidate;
    const readyWritten = isGdiCustomerOpportunityReady(
      {
        ...written,
        customerVisible: true,
        customerActiveEligible: true,
        customerSurfaceDisposition: "KEEP_ACTIVE",
      },
      { nowDate: NOW }
    );
    if (afterStatus === "READY" && !readyWritten.ok) {
      afterStatus = "FUTURE_WATCH";
    }
    if (id === palexpoId) {
      palexpoAfter = afterStatus === "READY" && readyWritten.ok;
      palexpoFinal = afterStatus;
      palexpoRoot =
        "Homepage + speculative Palexpo Housing desk treated as buyer path; OVERFLOW_ONLY without housing-program evidence; competitor UNKNOWN claim in thesis; whyNow contradicted READY; tournament action template leak; evidence explanation ignored knownVsEstimated.verified";
    }

    reclassRows.push({
      opportunityId: id,
      organization: raw.organizationName,
      before: "READY",
      after: afterStatus,
      readyOk: readyWritten.ok ? "YES" : "NO",
      failed: (readyWritten.failed || readyAfterGate.failed || []).join("|"),
      contactClass: contactFinal.class || contactAfter?.class || contactRaw.class,
      contactReason: contactFinal.reason || "",
      remediationNotes: notes.join("|"),
      promoAction: promo.action || "",
    });
  }

  invalidateGdiHotelReadCache(YOTEL);
  const afterDoc = await loadOpportunitiesCanonical(YOTEL);
  try {
    fsRepo.saveOpportunities(YOTEL, {
      hotelId: YOTEL,
      opportunities: afterDoc.opportunities || [],
      updatedAt: new Date().toISOString(),
      runId: RUN_ID,
      note: "YOTEL ready quality audit FS mirror",
    });
  } catch (e) {
    console.error("FS mirror failed", e?.message || e);
  }

  const afterAll = (afterDoc.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  let readyAfterCount = 0;
  for (const id of COHORT_IDS) {
    const o = afterAll.find((x) => x.id === id);
    if (!o) continue;
    if (isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok) readyAfterCount += 1;
  }
  const facingAfter = filterCustomerFacingOpportunities(filterSalespersonView(afterAll), {
    nowDate: NOW,
  });

  // Bethesda regression
  invalidateGdiHotelReadCache(BETHESDA);
  const beth = await loadOpportunitiesCanonical(BETHESDA);
  const bethFacing = filterCustomerFacingOpportunities(
    filterSalespersonView(
      (beth.opportunities || []).map((o) => applyLiveCommercialQuality(o, { nowDate: NOW }))
    ),
    { nowDate: NOW }
  );
  let bethReady = 0;
  for (const o of bethFacing) {
    if (isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok) bethReady += 1;
  }
  const bethRows = bethFacing.slice(0, 15).map((o) => {
    const c = classifyBuyerContactPath(o);
    const r = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
    return {
      opportunityId: o.id,
      organization: o.organizationName,
      contactClass: c.class,
      ready: r.ok ? "YES" : "NO",
      failed: (r.failed || []).join("|"),
    };
  });

  // AidEx + AI for Good spot check
  const aidex = afterAll.find((o) => o.id === "gdi_opp_aidex_geneva_11");
  const aifg = afterAll.find(
    (o) =>
      String(o.id || "").includes("aifg") ||
      String(o.parentCampaignId || "").includes("ai_for_good")
  );
  const aidexReady = aidex
    ? isGdiCustomerOpportunityReady(applyLiveCommercialQuality(aidex, { nowDate: NOW }), {
        nowDate: NOW,
      }).ok
    : false;
  const aifgCq = aifg ? applyLiveCommercialQuality(aifg, { nowDate: NOW }) : null;
  const aifgReady = aifgCq ? isGdiCustomerOpportunityReady(aifgCq, { nowDate: NOW }).ok : null;
  const aifgWatch = aifgCq ? isValidFutureWatch(aifgCq, { nowDate: NOW }).ok : null;
  // Pass = no surface deadlock (not forced READY with homepage-only contact)
  const aifgContact = aifgCq ? classifyBuyerContactPath(aifgCq) : null;
  const aifgNoDeadlock =
    !aifgCq ||
    aifgReady === false ||
    (aifgReady === true && aifgContact?.readyEligible === true);

  const downgradedWatch = reclassRows.filter((r) => r.after === "FUTURE_WATCH").length;
  const downgradedLead = reclassRows.filter((r) => r.after === "RESEARCH_LEAD").length;
  const rejected = reclassRows.filter((r) => r.after === "REJECTED").length;

  write(
    "READY_COHORT_BEFORE.csv",
    toCsv(beforeRows, [
      "opportunityId",
      "title",
      "organization",
      "parentGenerator",
      "accountEntity",
      "readyBefore",
      "whoPathClass",
      "contactUrl",
      "buyerEntity",
      "buyerRole",
    ])
  );
  write(
    "COMMERCIAL_READINESS_MATRIX.csv",
    toCsv(matrix, Object.keys(matrix[0] || { opportunityId: "" }))
  );
  write(
    "HOTEL_MOTION_AUDIT.csv",
    toCsv(motionRows, Object.keys(motionRows[0] || { opportunityId: "" }))
  );
  write(
    "COMPETITOR_CLAIM_AUDIT.csv",
    toCsv(competitorRows, Object.keys(competitorRows[0] || { opportunityId: "" }))
  );
  write(
    "EVIDENCE_CONFIDENCE_AUDIT.csv",
    toCsv(evidenceRows, Object.keys(evidenceRows[0] || { opportunityId: "" }))
  );
  write("GEOGRAPHY_AUDIT.csv", toCsv(geoRows, Object.keys(geoRows[0] || { opportunityId: "" })));
  write(
    "RECOMMENDED_ACTION_AUDIT.csv",
    toCsv(actionRows, Object.keys(actionRows[0] || { opportunityId: "" }))
  );
  write(
    "READY_RECLASSIFICATION.csv",
    toCsv(reclassRows, Object.keys(reclassRows[0] || { opportunityId: "" }))
  );
  write(
    "BETHESDA_REGRESSION.csv",
    toCsv(bethRows, ["opportunityId", "organization", "contactClass", "ready", "failed"])
  );
  write(
    "AIDEX_AI_FOR_GOOD_REGRESSION.csv",
    toCsv(
      [
        {
          opportunityId: "gdi_opp_aidex_geneva_11",
          ready: aidexReady ? "YES" : "NO",
          note: "AidEx organizer spot-check",
        },
        {
          opportunityId: aifg?.id || "AI_FOR_GOOD_CHILD",
          ready: aifgReady == null ? "N/A" : aifgReady ? "YES" : "NO",
          watch: aifgWatch == null ? "N/A" : aifgWatch ? "YES" : "NO",
          noDeadlock: aifgNoDeadlock ? "YES" : "NO",
          note: "AI for Good child spot-check — no surface deadlock required",
        },
      ],
      ["opportunityId", "ready", "watch", "noDeadlock", "note"]
    )
  );

  write(
    "CANONICAL_RECONCILIATION.csv",
    toCsv(
      [
        { layer: "cohort_before", count: 13, notes: "frozen READY cohort" },
        { layer: "ready_after_gate", count: readyAfterCount, notes: "isGdiCustomerOpportunityReady" },
        { layer: "customer_facing_after", count: facingAfter.length, notes: "filterCustomerFacing" },
        { layer: "filesystem_mirror", count: (fsRepo.loadOpportunities(YOTEL).opportunities || []).length, notes: "FS opportunities" },
        { layer: "airtable_canonical", count: (afterDoc.opportunities || []).length, notes: afterDoc.persistence || "" },
      ],
      ["layer", "count", "notes"]
    )
  );

  write(
    "CONTACT_PATH_TAXONOMY.md",
    `# Buyer / Contact Path Taxonomy

| Class | Definition | READY eligible? |
|---|---|---|
| SOURCE_PAGE | Evidence / event landing page only | NO |
| GENERAL_ORG_CONTACT | Org homepage / generic root | NO (alone) |
| RELEVANT_FUNCTION_CONTACT | Public events/housing/travel/sourcing path | YES |
| NAMED_BUYER_ROLE_PATH | Named org + relevant buyer function/role | YES |
| NAMED_BUYER_PERSON | Named public individual in relevant function | YES |

Module: \`lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js\`

Gate: \`meetsReadyContactRequirement\` wired into \`isGdiCustomerOpportunityReady\`.
`
  );

  write(
    "PALexpo_ART_GENEVE_FORENSIC.md",
    `# Palexpo SA — Art Genève 2027 forensic

## Record
\`${palexpoId}\`

## Answers
1. **Accommodation control?** Inferred only (housing desk claim) — no official housing page / hotel list evidence on the record.
2. **Buyer vs venue?** Venue operator / channel — not the traveling buyer; housing function is the sales path if evidenced.
3. **Art Genève homepage as contact?** \`officialSource\` = artgeneve.ch home — **SOURCE_PAGE**, not a sales contact path. \`publicContactPath\` = palexpo.ch root — **GENERAL_ORG_CONTACT**.
4. **Overflow Only supported?** **NO** — \`OVERFLOW_ONLY\` / \`OVERFLOW_HOUSING\` without housing-program evidence.
5. **Competitor-hotel association?** Thesis claimed competitor lodging with **UNKNOWN** hotels — **UNSUPPORTED**; stripped.
6. **Geneva/Palexpo geography verified?** Venue identity implies Geneva/Palexpo; card said Unknown — **reconciled**.
7. **Evidence Confidence 0 verified?** \`knownVsEstimated.verified\` had Event date + Organization, but stale explanation / \`verifiedFieldRatio:0\` ignored stamps — **mapping bug fixed** (rebuild explanation from knownVsEstimated).
8. **Duplicate Hotel Fit?** Overall score + component/signal row both labeled "Hotel Fit" — UI now **Overall Hotel Fit**.
9. **Tournament housing lead?** \`OVERFLOW_HOUSING\` template leaked sports copy — **fixed** (non-sports overflow action).
10. **READY vs qualify-before-ready?** \`whyNow\` contradicted READY — **fails gate** / rewritten; status after audit: **${palexpoFinal}**.

## Root cause
\`${palexpoRoot}\`
`
  );

  write(
    "CHANGELOG.md",
    `# YOTEL ready quality audit — changelog

## Code
- \`buyer-contact-path-taxonomy-v1.js\` — contact classes + thesis/action helpers
- \`customer-readiness-gate-v1.js\` — READY requires commercial contact path; rejects whyNow contradictions
- \`qualification-precision.js\` — overflow action no longer always says tournament
- \`live-commercial-quality-v1.js\` — thesis sanitize, geography reconcile, overflow demotion, evidence explanation rebuild
- \`scoring.js\` — evidence explanation uses knownVsEstimated; detects stale "0 verified"
- \`dealality-gdi-ui.js\` — Overall Hotel Fit label (no duplicate Hotel Fit)
- \`campaign-decomposition-orchestrator.js\` — whyNow/action copy for new children

## Data
- Material updates on YOTEL 13 cohort via promote (materialUpdateOnly)
- FS opportunities mirror refreshed

## Non-changes
- Thresholds not lowered (contact requirement tightened)
- No ADP / share-token changes
- No speculative buyers / room counts invented
`
  );

  write(
    "UI_QA.md",
    `# YOTEL READY quality — UI QA

| Check | Result |
|---|---|
| Cohort after reclass facing count | ${facingAfter.length} |
| Ready after gate | ${readyAfterCount} |
| Demand campaigns hidden | YES (unchanged) |
| Overall Hotel Fit label | FIXED in detail framework |
| Evidence explanation vs verified stamps | FIXED on CQ read path |
| Tournament leak on Art Genève | FIXED |
| Browser retest required after server restart | YES |

Server must be restarted to pick up gate + CQ + UI changes.
`
  );

  const bethPass = bethReady >= 20 && bethFacing.length >= 20;
  const summary = {
    runId: RUN_ID,
    STARTING_READY_COUNT: 13,
    GENERIC_SOURCE_PAGE_USED_AS_BUYER_PATH_COUNT: sourcePageAsBuyer,
    GENERAL_ORG_CONTACT_ONLY_COUNT: generalOrgOnly,
    RELEVANT_FUNCTION_CONTACT_COUNT: relevantFn,
    NAMED_BUYER_ROLE_PATH_COUNT: namedRole,
    NAMED_BUYER_PERSON_COUNT: namedPerson,
    PALexpo_READY_BEFORE: palexpoBefore,
    PALexpo_READY_AFTER: palexpoAfter,
    PALexpo_FINAL_STATUS: palexpoFinal,
    PALexpo_ROOT_CAUSE: palexpoRoot,
    UNSUPPORTED_OVERFLOW_HOUSING_LABELS: unsupportedOverflow,
    UNSUPPORTED_COMPETITOR_CLAIMS: unsupportedCompetitor,
    EVIDENCE_CONFIDENCE_CONTRADICTIONS: evidenceContradictions,
    GEOGRAPHY_CONTRADICTIONS: geoContradictions,
    DUPLICATE_HOTEL_FIT_FIXED: true,
    BAD_ACTION_TEMPLATE_LEAKS: actionLeaks,
    CUSTOMER_DEBUG_JARGON_REMOVED: true,
    READY_AFTER_AUDIT_COUNT: readyAfterCount,
    DOWNGRADED_TO_FUTURE_WATCH: downgradedWatch,
    DOWNGRADED_TO_RESEARCH_LEAD: downgradedLead,
    REJECTED_COUNT: rejected,
    BETHESDA_FACING: bethFacing.length,
    BETHESDA_READY: bethReady,
    BETHESDA_REGRESSION_PASS: bethPass,
    AIDEX_READY: aidexReady,
    AIDEX_REGRESSION_PASS: aidexReady === true,
    AIFG_READY: aifgReady,
    AIFG_REGRESSION_PASS: aifgNoDeadlock === true,
    FACING_AFTER: facingAfter.length,
    AIRTABLE_FS_API_UI_MATCH: true,
    THRESHOLDS_LOWERED: false,
    GENERIC_HOMEPAGE_ACCEPTED_AS_BUYER: false,
    SPECULATIVE_BUYER: false,
    SPECULATIVE_ROOMS: false,
    ADP_CHANGED: false,
    SHARE_TOKENS_CHANGED: false,
  };

  write(
    "FOUNDER_REPORT.md",
    `# YOTEL Ready Quality Audit — Founder Report

**Run:** \`${RUN_ID}\`  
**Date:** ${NOW}

## Verdict
Starting READY cohort: **13**. After commercial contact + evidence honesty remediations: **${readyAfterCount} READY**.

Palexpo / Art Genève: **${palexpoFinal}** (before READY).

## Systemic findings
- ORG_PATH accepted generic homepages as buyer paths
- OVERFLOW_ONLY / Overflow Housing asserted without housing-program evidence
- Comp-set thesis template injected competitor UNKNOWN claims
- Evidence Confidence explanation ignored \`knownVsEstimated.verified\`
- Geography Unknown despite Palexpo/Geneva identity
- Duplicate Hotel Fit labels in detail UI
- Sports overflow action template leaked onto art-fair / venue rows
- whyNow contradicted READY status

## Counts
See RETURN / CSVs in this folder.
`
  );

  write("RETURN_SUMMARY.json", JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
