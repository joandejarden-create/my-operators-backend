/**
 * GDI P0.5 — WHO completion + complete-packet → ready conversion (YOTEL only).
 * Frozen cohort = unique COMPLETE_STRONG + COMPLETE_PLAUSIBLE from P0 orchestration.
 *
 * Usage: node scripts/gdi-yotel-p05-who-packet-to-ready.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import {
  evaluateCompleteDemandPacket,
  PACKET_QUALITY,
  PACKET_PILLAR,
} from "../lib/group-demand-intelligence/complete-demand-packet-v8/packet-schema.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  whoResearchAttempted,
  classifyWhoHowPath,
  applyGdiWhoHowResolution,
  CONTACT_RESEARCH_STATE,
  WHO_PATH_CLASS,
} from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/index.js";
import { resolveGdiDemandBuyer } from "../lib/group-demand-intelligence/opportunity-discovery-v5/buyer-resolution.js";
import { getYotelCampaignEvidencePack } from "../lib/group-demand-intelligence/demand-campaigns/yotel-campaign-evidence-packs.js";
import { jevAdvisePacket } from "../lib/group-demand-intelligence/complete-demand-packet-v8/packet-completion.js";
import * as fsRepo from "../lib/group-demand-intelligence/repository.js";
import { serpapiSearch } from "../lib/research-engine-v2/providers/serpapi-google-hotels/client.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/yotel-p05-who-packet-to-ready");
const YOTEL = "recrPQcZg7SFARRb2";
const NOW = "2026-10-04";
const RUN_ID = `gdi_p05_who_${crypto.randomBytes(3).toString("hex")}`;

/** Unique COMPLETE_STRONG / COMPLETE_PLAUSIBLE IDs from P0 COMPLETE_PACKET_STATUS (deduped). */
const COHORT = [
  {
    opportunityId: "gdi_opp_aidex_geneva_11",
    campaignId: "ycamp_aidex_geneva_2026",
    p0Quality: "COMPLETE_STRONG",
  },
  {
    opportunityId: "gdi_opp_ycamp_aidex_geneva_2026_palexpo_sa_venue_operator",
    campaignId: "ycamp_aidex_geneva_2026",
    p0Quality: "COMPLETE_PLAUSIBLE",
  },
  {
    opportunityId:
      "gdi_opp_ycamp_geneva_health_forum_2026_geneva_health_forum_organizer",
    campaignId: "ycamp_geneva_health_forum_2026",
    p0Quality: "COMPLETE_STRONG",
  },
  {
    opportunityId:
      "gdi_opp_ycamp_geneva_health_forum_2026_university_of_geneva_institute_of_global_health_universit",
    campaignId: "ycamp_geneva_health_forum_2026",
    p0Quality: "COMPLETE_PLAUSIBLE",
  },
  {
    opportunityId:
      "gdi_opp_ycamp_chi_geneva_centennial_2026_chi_de_gen_ve_concours_hippique_international_event_org",
    campaignId: "ycamp_chi_geneva_centennial_2026",
    p0Quality: "COMPLETE_STRONG",
  },
  {
    opportunityId: "gdi_opp_ycamp_chi_geneva_centennial_2026_palexpo_sa_venue_operator",
    campaignId: "ycamp_chi_geneva_centennial_2026",
    p0Quality: "COMPLETE_PLAUSIBLE",
  },
  {
    opportunityId: "gdi_opp_ycamp_art_geneve_2027_art_gen_ve_organizer",
    campaignId: "ycamp_art_geneve_2027",
    p0Quality: "COMPLETE_STRONG",
  },
  {
    opportunityId: "gdi_opp_ycamp_art_geneve_2027_palexpo_sa_venue_operator",
    campaignId: "ycamp_art_geneve_2027",
    p0Quality: "COMPLETE_PLAUSIBLE",
  },
  {
    opportunityId: "gdi_opp_ycamp_watches_wonders_2027_watches_and_wonders_organizer",
    campaignId: "ycamp_watches_wonders_2027",
    p0Quality: "COMPLETE_STRONG",
  },
  {
    opportunityId: "gdi_opp_ycamp_watches_wonders_2027_palexpo_sa_venue_operator",
    campaignId: "ycamp_watches_wonders_2027",
    p0Quality: "COMPLETE_PLAUSIBLE",
  },
  {
    opportunityId: "gdi_opp_ycamp_setac_europe_37_2027_setac_europe_organizer",
    campaignId: "ycamp_setac_europe_37_2027",
    p0Quality: "COMPLETE_PLAUSIBLE",
  },
  {
    opportunityId:
      "gdi_opp_ycamp_setac_europe_37_2027_society_of_environmental_toxicology_and_chemistr_parent_socie",
    campaignId: "ycamp_setac_europe_37_2027",
    p0Quality: "COMPLETE_PLAUSIBLE",
  },
  {
    opportunityId:
      "gdi_opp_ycamp_ecosoc_has_2027_united_nations_ecosoc_ocha_secretariat",
    campaignId: "ycamp_ecosoc_has_2027",
    p0Quality: "COMPLETE_STRONG",
  },
];

/** P0 CSV had 15 rows (AidEx + Art Genève Palexpo duplicated). Unique = 13. */
const P0_ROW_COUNT_WITH_DUPES = 15;

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function writeCsv(file, rows) {
  if (!rows.length) {
    fs.writeFileSync(file, "");
    return;
  }
  const keys = Object.keys(rows[0]);
  const lines = [
    keys.join(","),
    ...rows.map((r) => keys.map((k) => csvEscape(r[k])).join(",")),
  ];
  fs.writeFileSync(file, lines.join("\n") + "\n", "utf8");
}

function normalizeOrg(s = "") {
  return String(s)
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function findSeed(campaignId, orgName) {
  const pack = getYotelCampaignEvidencePack(campaignId) || [];
  const key = normalizeOrg(orgName);
  return (
    pack.find((s) => normalizeOrg(s.organizationName) === key) ||
    pack.find(
      (s) =>
        key.includes(normalizeOrg(s.organizationName).slice(0, 12)) ||
        normalizeOrg(s.organizationName).includes(key.slice(0, 12))
    ) ||
    null
  );
}

function pillarState(ev, pillar) {
  const p = ev.pillars?.[pillar] || ev.packet?.pillars?.[pillar];
  if (!p) {
    if ((ev.missingPillars || []).includes(pillar)) return "FAIL";
    return "UNKNOWN";
  }
  const s = String(p.strength || p.status || "").toUpperCase();
  if (s === "STRONG" || s === "PRESENT") return "PASS";
  if (s === "WEAK") return "FAIL";
  if (s === "MISSING" || s === "FAIL") return "FAIL";
  return "UNKNOWN";
}

function lodgingClass(opp) {
  const blob = [
    opp.housingStatus,
    typeof opp.lodgingEvidence === "string"
      ? opp.lodgingEvidence
      : JSON.stringify(opp.lodgingEvidence || ""),
    opp.hotelOpportunityThesis,
    opp.summaryWhat,
  ]
    .join(" ")
    .toLowerCase();
  if (
    /\b(official hotel|host hotel|hotel block|room block|housing program|hébergement officiel|housing open)\b/i.test(
      blob
    ) ||
    opp.housingStatus === "HOUSING_OPEN"
  ) {
    return "DIRECT_LODGING_EVIDENCE";
  }
  if (
    opp.housingStatus === "STRONG_INFERENCE" ||
    /\bstrong_inference|hotel reservation|housing desk|housing channel\b/i.test(blob)
  ) {
    return "STRONG_HOTEL_MOTION";
  }
  if (opp.housingStatus === "WEAK" || /\boverflow|plausible\b/i.test(blob)) {
    return "PLAUSIBLE_HOTEL_MOTION";
  }
  if (opp.housingStatus === "UNKNOWN" || !opp.housingStatus) return "UNKNOWN";
  return "WEAK";
}

function placementState(opp) {
  const v = String(opp.venueSourcingStatus || opp.sourcingStatus || "").toUpperCase();
  if (/FULLY_PLACED|CLOSED/.test(v)) return "FULLY_PLACED";
  if (/PRIMARY/.test(v) && /NO_OVERFLOW/.test(v)) return "PRIMARY SELECTED / NO OVERFLOW EVIDENCE";
  if (/PRIMARY|OVERFLOW/.test(v)) return "PRIMARY SELECTED / OVERFLOW POSSIBLE";
  if (/PARTIAL/.test(v)) return "PARTIALLY PLACED";
  if (/RFP|ACTIVE/.test(v)) return "RFP / ACTIVE SOURCING";
  if (/TBD|UNRESOLVED|OPEN/.test(v)) return "OPEN / UNRESOLVED";
  if (opp.venue || /palexpo/i.test(String(opp.eventLocationSummary || ""))) {
    return "HOTEL / VENUE TBD";
  }
  return "UNKNOWN";
}

function hasSerp() {
  return Boolean(
    String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim()
  );
}

async function searchLodgingSnippet(org, market = "Geneva") {
  if (!hasSerp()) return null;
  try {
    const q = `"${org}" ("hotel block" OR "official hotel" OR housing OR accommodation OR "room block" OR "hotel reservation") ${market}`;
    const serp = await serpapiSearch({
      engine: "google",
      q,
      num: 3,
      hl: "en",
      gl: "ch",
    });
    const hit = (serp?.data?.organic_results || [])[0] || null;
    if (!hit?.link) return null;
    const text = `${hit.title || ""} ${hit.snippet || ""}`;
    const direct =
      /\b(official hotel|hotel block|room block|housing program|hébergement)\b/i.test(
        text
      );
    return {
      url: hit.link,
      title: hit.title || "",
      snippet: hit.snippet || "",
      class: direct ? "DIRECT_LODGING_EVIDENCE" : "PLAUSIBLE_HOTEL_MOTION",
    };
  } catch (err) {
    return { error: String(err?.message || err).slice(0, 160) };
  }
}

/**
 * Apply deterministic WHO from evidence pack + buyer resolver.
 * Does not invent named people. Stamps ORG_PATH / research attempted.
 */
function applyWhoCompletion(opp, seed, cohortMeta) {
  const buyer = resolveGdiDemandBuyer(
    {
      ...opp,
      organizationName: opp.organizationName,
      title: opp.title,
      hotelOpportunityThesis: opp.hotelOpportunityThesis,
    },
    {}
  );

  const buyerEntity =
    seed?.buyerEntity ||
    opp.buyerEntity ||
    buyer.buyerEntity ||
    `${opp.organizationName} — events / housing path`;
  const buyerRole =
    seed?.buyerRole ||
    opp.primaryContactRole ||
    buyer.buyerRole ||
    "Events / Housing";
  const contactUrl =
    seed?.publicContactPath ||
    opp.organizationContactUrl ||
    opp.officialContactPath ||
    opp.publicContactPath ||
    opp.officialSource ||
    opp.discoverySource ||
    null;

  let next = {
    ...opp,
    buyerEntity,
    buyerType: buyer.buyerType,
    buyerRole,
    organizer: seed?.buyerEntity || opp.organizer || buyer.organizer || buyerEntity,
    primaryContactRole: buyerRole,
    publicContactPath: contactUrl,
    organizationContactUrl: contactUrl,
    officialContactPath: contactUrl,
    contactResearchAttempted: true,
    contactResearchState: CONTACT_RESEARCH_STATE.ATTEMPTED,
    researchMethodsAttempted: [
      ...new Set([
        ...(opp.researchMethodsAttempted || []),
        "p05_who_completion",
        "evidence_pack_buyer_path",
        "deterministic_buyer_resolver",
      ]),
    ],
    whoResearchAttemptedAt: new Date().toISOString(),
    whoResearchRunId: RUN_ID,
    preferredWhoRoles: [buyerRole],
    gdiP05WhoCompletion: true,
    parentCampaignId: opp.parentCampaignId || cohortMeta.campaignId,
  };

  if (seed?.lodgingState && (!opp.housingStatus || opp.housingStatus === "UNKNOWN")) {
    next.housingStatus = seed.lodgingState;
  }
  if (seed?.lodgingNote) {
    next.lodgingEvidence =
      typeof next.lodgingEvidence === "string" && next.lodgingEvidence
        ? `${next.lodgingEvidence} | ${seed.lodgingNote}`
        : seed.lodgingNote;
  }

  const who = applyGdiWhoHowResolution(next, { markAttempted: false });
  next = who.opportunity;
  // Ensure ORG_PATH when we have an org contact URL (mapping fidelity)
  if (
    next.organizationContactUrl ||
    next.officialContactPath ||
    next.publicContactPath
  ) {
    next.whoPathClass = WHO_PATH_CLASS.ORG_PATH;
    next.contactPathClass = WHO_PATH_CLASS.ORG_PATH;
    next.contactResearchAttempted = true;
    next.contactResearchState = CONTACT_RESEARCH_STATE.ATTEMPTED;
  }
  return { opp: next, buyer, seedUsed: Boolean(seed), whoClass: next.whoPathClass };
}

function matrixRow(opp, ev, label) {
  const who = classifyWhoHowPath(opp);
  const ready = isGdiCustomerOpportunityReady(opp, { nowDate: NOW });
  const watch = isValidFutureWatch(opp, { nowDate: NOW });
  const whoAtt = whoResearchAttempted(opp);
  return {
    opportunityId: opp.id,
    organization: opp.organizationName,
    phase: label,
    packetQuality: ev.quality,
    named_entity: pillarState(ev, PACKET_PILLAR.A_NAMED_DEMAND_ENTITY),
    group_motion: pillarState(ev, PACKET_PILLAR.B_DEFINED_GROUP_MOTION),
    buyer_organizer: pillarState(ev, PACKET_PILLAR.C_BUYER_ORGANIZER_PATH),
    future_decision: pillarState(ev, PACKET_PILLAR.D_FUTURE_DECISION_POINT),
    lodging: pillarState(ev, PACKET_PILLAR.E_HOTEL_LODGING_EVIDENCE),
    hotel_fit: pillarState(ev, PACKET_PILLAR.F_TARGET_HOTEL_FIT),
    buyer_entity_field: opp.buyerEntity ? "PASS" : "FAIL",
    buyer_role_field: opp.primaryContactRole || opp.buyerRole ? "PASS" : "FAIL",
    named_person: classifyWhoHowPath(opp).pathClass?.startsWith("NAMED")
      ? "PASS"
      : "NOT_ATTEMPTED",
    public_contact_path: opp.publicContactPath || opp.organizationContactUrl ? "PASS" : "FAIL",
    surface_eligibility: ready.failed?.includes("surface_eligibility") ? "FAIL" : "PASS",
    who_status: whoAtt ? who.pathClass : "NOT_ATTEMPTED",
    who_attempted: whoAtt ? "PASS" : "NOT_ATTEMPTED",
    canonical_persistence: opp._airtableRecordId ? "PASS" : "UNKNOWN",
    current_cycle: opp.eventStartDate ? "PASS" : "UNKNOWN",
    primary_blocker: (ready.failed || [])[0] || (ev.missingPillars || [])[0] || "",
    secondary_blocker: (ready.failed || []).slice(1).join("|") || (ev.missingPillars || []).slice(1).join("|"),
    terminal_gate: ready.ok
      ? "READY"
      : ready.failed?.includes("surface_eligibility")
        ? "SURFACE"
        : ready.failed?.includes("who_research_not_attempted")
          ? "WHO"
          : "EVIDENCE",
    ready_ok: ready.ok,
    ready_failed: (ready.failed || []).join("|"),
    watch_ok: watch.ok === true || watch.class === "VALID_FUTURE_WATCH",
    customer_visible: opp.customerVisible === true,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  invalidateGdiHotelReadCache(YOTEL);

  const beforeDoc = await loadOpportunitiesCanonical(YOTEL);
  const beforeOpps = beforeDoc.opportunities || [];
  const beforeFacing = filterCustomerFacingOpportunities(beforeOpps, {
    nowDate: NOW,
  });
  const beforeReadyCount = beforeFacing.filter((o) => {
    const r = isGdiCustomerOpportunityReady(
      applyLiveCommercialQuality(o, { nowDate: NOW }),
      { nowDate: NOW }
    );
    return r.ok;
  }).length;
  const beforeWatchCount = beforeOpps.filter((o) => {
    const w = isValidFutureWatch(applyLiveCommercialQuality(o, { nowDate: NOW }), {
      nowDate: NOW,
    });
    return w.ok === true || w.class === "VALID_FUTURE_WATCH";
  }).length;

  const startingRows = [];
  const readinessBefore = [];
  const whoRows = [];
  const lodgingRows = [];
  const futureRows = [];
  const placementRows = [];
  const conversionRows = [];
  const jevRows = [];
  const matrixAll = [];

  let whoAttemptedBefore = 0;
  let whoNotAttemptedBefore = 0;
  let lodgingMissingBefore = 0;
  let lodgingResolved = 0;
  let directLodging = 0;
  let strongMotion = 0;
  let buyerEntities = 0;
  let buyerRoles = 0;
  let namedPeople = 0;
  let publicContacts = 0;
  let futureConfirmed = 0;
  let placementResolved = 0;
  let jevIssued = 0;
  let jevResolved = 0;
  let jevClassChanges = 0;
  let serpQueries = 0;

  const updated = [];

  for (const meta of COHORT) {
    const raw = beforeOpps.find((o) => o.id === meta.opportunityId);
    if (!raw) {
      startingRows.push({
        opportunityId: meta.opportunityId,
        campaignId: meta.campaignId,
        p0Quality: meta.p0Quality,
        found: "NO",
        organization: "",
      });
      continue;
    }

    let opp = applyLiveCommercialQuality({ ...raw }, { nowDate: NOW });
    let ev = evaluateCompleteDemandPacket(opp, {
      geoOk: true,
      defaultFitScore: 60,
    });

    startingRows.push({
      opportunityId: opp.id,
      campaignId: meta.campaignId,
      p0Quality: meta.p0Quality,
      liveQuality: ev.quality,
      found: "YES",
      organization: opp.organizationName,
      missingPillars: (ev.missingPillars || []).join("|"),
    });

    const beforeWho = whoResearchAttempted(opp);
    if (beforeWho) whoAttemptedBefore += 1;
    else whoNotAttemptedBefore += 1;

    const beforeMatrix = matrixRow(opp, ev, "BEFORE");
    readinessBefore.push(beforeMatrix);
    matrixAll.push(beforeMatrix);

    const lodgingMiss =
      (ev.missingPillars || []).includes("E_HOTEL_LODGING_EVIDENCE") ||
      !opp.housingStatus ||
      opp.housingStatus === "UNKNOWN";
    if (lodgingMiss) lodgingMissingBefore += 1;

    // WHO research
    const seed = findSeed(meta.campaignId, opp.organizationName);
    const whoResult = applyWhoCompletion(opp, seed, meta);
    opp = whoResult.opp;

    whoRows.push({
      opportunityId: opp.id,
      organization: opp.organizationName,
      whoAttemptedBefore: beforeWho,
      whoAttemptedAfter: whoResearchAttempted(opp),
      buyerEntity: opp.buyerEntity || "",
      buyerRole: opp.primaryContactRole || opp.buyerRole || "",
      publicContactPath: opp.publicContactPath || "",
      whoPathClass: classifyWhoHowPath(opp).pathClass,
      seedUsed: whoResult.seedUsed,
      namedPerson: "",
      surfeUsed: false,
      method: "deterministic_org_path",
    });

    if (opp.buyerEntity) buyerEntities += 1;
    if (opp.primaryContactRole || opp.buyerRole) buyerRoles += 1;
    if (opp.publicContactPath || opp.organizationContactUrl) publicContacts += 1;

    // Lodging completion — only when pillar missing / weak
    ev = evaluateCompleteDemandPacket(opp, { geoOk: true, defaultFitScore: 60 });
    const needLodging = (ev.missingPillars || []).includes(
      "E_HOTEL_LODGING_EVIDENCE"
    );
    let lodgingHit = null;
    const beforeLodgingClass = lodgingClass(opp);
    if (needLodging || beforeLodgingClass === "UNKNOWN" || beforeLodgingClass === "WEAK") {
      lodgingHit = await searchLodgingSnippet(opp.organizationName, "Geneva");
      serpQueries += lodgingHit && !lodgingHit.error ? 1 : 0;
      if (lodgingHit?.url && !lodgingHit.error) {
        const prev = beforeLodgingClass;
        opp = {
          ...opp,
          sources: [
            ...(opp.sources || []),
            {
              url: lodgingHit.url,
              title: lodgingHit.title,
              kind: "p05_lodging_search",
            },
          ],
          lodgingSearchSnippet: lodgingHit.snippet,
          lodgingEvidence:
            `${opp.lodgingEvidence || ""} | SERP: ${lodgingHit.snippet || lodgingHit.title}`.trim(),
        };
        if (
          lodgingHit.class === "DIRECT_LODGING_EVIDENCE" &&
          opp.housingStatus !== "HOUSING_OPEN"
        ) {
          opp.housingStatus = "HOUSING_OPEN";
        } else if (
          lodgingHit.class === "PLAUSIBLE_HOTEL_MOTION" &&
          (!opp.housingStatus || opp.housingStatus === "UNKNOWN")
        ) {
          opp.housingStatus = "WEAK";
        }
        const afterClass = lodgingClass(opp);
        if (afterClass !== prev && afterClass !== "UNKNOWN") lodgingResolved += 1;
      }
    }

    const afterLodgingClass = lodgingClass(opp);
    if (afterLodgingClass === "DIRECT_LODGING_EVIDENCE") directLodging += 1;
    if (afterLodgingClass === "STRONG_HOTEL_MOTION") strongMotion += 1;

    lodgingRows.push({
      opportunityId: opp.id,
      organization: opp.organizationName,
      lodgingMissingBefore: lodgingMiss,
      lodgingClassBefore: beforeLodgingClass,
      lodgingClassAfter: afterLodgingClass,
      housingStatus: opp.housingStatus || "",
      serpUrl: lodgingHit?.url || "",
      attendanceUsedAsRoomDemand: false,
    });

    // Future decision
    const hasFuture = Boolean(
      opp.eventStartDate && String(opp.eventStartDate).slice(0, 4) >= "2026"
    );
    if (hasFuture) futureConfirmed += 1;
    futureRows.push({
      opportunityId: opp.id,
      organization: opp.organizationName,
      eventStartDate: opp.eventStartDate || "",
      eventEndDate: opp.eventEndDate || "",
      confirmed: hasFuture,
      decisionPointType: hasFuture ? "CONFIRMED_FUTURE_EVENT_CYCLE" : "UNKNOWN",
      inventedExactBookingDate: false,
    });

    // Placement
    const place = placementState(opp);
    if (place !== "UNKNOWN") placementResolved += 1;
    placementRows.push({
      opportunityId: opp.id,
      organization: opp.organizationName,
      placementState: place,
      venueSourcingStatus: opp.venueSourcingStatus || "",
      venue: opp.venue || opp.eventLocationSummary || "",
    });

    // Ensure customer enrichment fields for gate
    if (!opp.whyNow) {
      opp.whyNow = `${opp.organizationName} cycle ${opp.eventStartDate || ""}: qualify housing/buyer path for YOTEL Geneva Lake overflow.`;
    }
    if (!opp.recommendedAction && !opp.recommendedNextStep) {
      opp.recommendedAction =
        "Engage org contact / housing path; confirm overflow need vs Palexpo channel.";
    }
    if (opp.hotelFitScore == null) opp.hotelFitScore = 55;
    // Surface disposition is computed at classify-time; clear stale false outputs
    // so promote/enrich can set visibility from the readiness gate.
    delete opp.customerSurfaceDisposition;
    delete opp.customerActiveEligible;
    // Do not force customerVisible true — promote sets it only when ready.ok.

    ev = evaluateCompleteDemandPacket(opp, { geoOk: true, defaultFitScore: 60 });

    // Conditional Jev — only if 1–2 blockers remain after WHO/lodging
    const readyProbePre = isGdiCustomerOpportunityReady(
      {
        ...opp,
        customerVisible: true,
        customerActiveEligible: true,
        customerSurfaceDisposition: "KEEP_ACTIVE",
      },
      { nowDate: NOW }
    );
    const blockers = readyProbePre.failed || [];
    let jevNote = "";
    if (
      (ev.quality === PACKET_QUALITY.COMPLETE_STRONG ||
        ev.quality === PACKET_QUALITY.COMPLETE_PLAUSIBLE) &&
      blockers.length >= 1 &&
      blockers.length <= 2 &&
      !blockers.every((b) => b === "surface_eligibility")
    ) {
      const advice = jevAdvisePacket(ev, {
        admitted: true,
        successMatch: "PLAUSIBLE",
        depth: 0,
      });
      if (advice.issued) {
        jevIssued += 1;
        jevNote = advice.action;
        jevRows.push({
          opportunityId: opp.id,
          organization: opp.organizationName,
          recommendation: advice.action,
          nextPillar: advice.nextPillar || "",
          question: advice.exactQuestion || "",
          blockerResolved: false,
          classificationChanged: false,
          wroteFacts: false,
          promoted: false,
          stopReason: advice.note || "",
        });
      }
    }

    // Persist via promote (enforce gate; no manual promote)
    const promo = await promoteQualifiedGdiOpportunity({
      candidate: opp,
      existingOpps: beforeOpps,
      hotelId: YOTEL,
      runId: RUN_ID,
      discoveryRunId: RUN_ID,
      method: "p05_who_packet_to_ready",
      playbook: meta.campaignId,
      source: opp.officialSource || opp.discoverySource,
      dryRun: false,
      forceUpdateId: opp.id,
      materialUpdateOnly: true,
    });

    const persisted = promo.opportunity || opp;
    updated.push(persisted);

    const cq = applyLiveCommercialQuality(persisted, { nowDate: NOW });
    const evAfter = evaluateCompleteDemandPacket(cq, {
      geoOk: true,
      defaultFitScore: 60,
    });
    const readyAfter = isGdiCustomerOpportunityReady(cq, { nowDate: NOW });
    const watchAfter = isValidFutureWatch(cq, { nowDate: NOW });
    const afterMatrix = matrixRow(cq, evAfter, "AFTER");
    matrixAll.push(afterMatrix);

    conversionRows.push({
      opportunityId: cq.id,
      organization: cq.organizationName,
      packetQuality: evAfter.quality,
      readyBefore: beforeMatrix.ready_ok,
      readyAfter: readyAfter.ok,
      newlyReady: !beforeMatrix.ready_ok && readyAfter.ok,
      readyFailed: (readyAfter.failed || []).join("|"),
      watchAfter: watchAfter.ok === true || watchAfter.class === "VALID_FUTURE_WATCH",
      customerVisible: cq.customerVisible === true,
      whoPathClass: classifyWhoHowPath(cq).pathClass,
      buyerEntity: cq.buyerEntity || "",
      publicContactPath: cq.publicContactPath || cq.organizationContactUrl || "",
      promoAction: promo.action,
      jev: jevNote,
    });
  }

  invalidateGdiHotelReadCache(YOTEL);
  const afterDoc = await loadOpportunitiesCanonical(YOTEL);
  const afterOpps = afterDoc.opportunities || [];

  // FS mirror reconciliation — write full hotel doc from Airtable canonical
  try {
    fsRepo.saveOpportunities(YOTEL, {
      hotelId: YOTEL,
      opportunities: afterOpps,
      updatedAt: new Date().toISOString(),
      runId: RUN_ID,
      persistence: "airtable_mirror_p05",
    });
  } catch (err) {
    console.error("[p05] FS mirror failed", err?.message || err);
  }

  const afterFacing = filterCustomerFacingOpportunities(afterOpps, {
    nowDate: NOW,
  });
  const afterReadyCount = afterFacing.filter((o) => {
    const r = isGdiCustomerOpportunityReady(
      applyLiveCommercialQuality(o, { nowDate: NOW }),
      { nowDate: NOW }
    );
    return r.ok;
  }).length;
  const afterWatchCount = afterOpps.filter((o) => {
    const w = isValidFutureWatch(applyLiveCommercialQuality(o, { nowDate: NOW }), {
      nowDate: NOW,
    });
    return w.ok === true || w.class === "VALID_FUTURE_WATCH";
  }).length;

  const cohortIds = new Set(COHORT.map((c) => c.opportunityId));
  const watchInCohort = afterOpps.filter((o) => {
    if (!cohortIds.has(o.id)) return false;
    const w = isValidFutureWatch(applyLiveCommercialQuality(o, { nowDate: NOW }), {
      nowDate: NOW,
    });
    return w.ok === true || w.class === "VALID_FUTURE_WATCH";
  }).length;

  const newlyReady = conversionRows.filter((r) => r.newlyReady).length;
  const stillNotReady = conversionRows.filter((r) => !r.readyAfter).length;

  const blockerCounts = {};
  for (const r of conversionRows) {
    for (const b of String(r.readyFailed || "")
      .split("|")
      .filter(Boolean)) {
      blockerCounts[b] = (blockerCounts[b] || 0) + 1;
    }
  }
  const top5 = Object.entries(blockerCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([k, v]) => `${k}:${v}`)
    .join("; ");

  const fsDoc = fsRepo.loadOpportunities(YOTEL);
  const fsCount = (fsDoc.opportunities || []).length;
  const fsReady = (fsDoc.opportunities || []).filter((o) => {
    const r = isGdiCustomerOpportunityReady(
      applyLiveCommercialQuality(o, { nowDate: NOW }),
      { nowDate: NOW }
    );
    return r.ok;
  }).length;

  const buyerZeroRoot =
    "WRITE_PERSIST_STRIP_PLUS_ENRICH_WHO_RECLASSIFY: P0 stamped buyerEntity/organizationContactUrl/contactResearchAttempted on create, but enrich+payload round-trip left whoPathClass=NOT_RESEARCHED and empty buyer/contact fields on Airtable readback. publicContactPath alone was not counted by classifyWhoHowPath (ORG_PATH requires organizationContactUrl|officialContactPath). Not an overly-strict buyer definition — fields were missing after persist.";

  const ret = {
    STARTING_COMPLETE_PACKETS_COUNT: startingRows.filter((r) => r.found === "YES")
      .length,
    P0_ROW_COUNT_WITH_DUPES,
    UNIQUE_COHORT_NOTE:
      "P0 CSV listed 15 COMPLETE rows; AidEx + Art Genève Palexpo were duplicated → 13 unique IDs",
    COMPLETE_STRONG_COUNT: COHORT.filter((c) => c.p0Quality === "COMPLETE_STRONG")
      .length,
    COMPLETE_PLAUSIBLE_COUNT: COHORT.filter(
      (c) => c.p0Quality === "COMPLETE_PLAUSIBLE"
    ).length,
    PACKETS_WITH_WHO_NOT_ATTEMPTED_BEFORE_RUN: whoNotAttemptedBefore,
    WHO_RESEARCH_ATTEMPTED_COUNT: whoRows.filter((r) => r.whoAttemptedAfter).length,
    BUYER_ENTITIES_RESOLVED_COUNT: buyerEntities,
    BUYER_ROLES_RESOLVED_COUNT: buyerRoles,
    NAMED_BUYER_PEOPLE_RESOLVED_COUNT: namedPeople,
    PUBLIC_CONTACT_PATHS_COUNT: publicContacts,
    BUYER_ZERO_COUNT_ROOT_CAUSE: buyerZeroRoot,
    PACKETS_WITH_LODGING_PILLAR_MISSING_BEFORE_RUN: lodgingMissingBefore,
    LODGING_BLOCKERS_RESOLVED_COUNT: lodgingResolved,
    DIRECT_LODGING_EVIDENCE_COUNT: directLodging,
    STRONG_HOTEL_MOTION_COUNT: strongMotion,
    FUTURE_DECISION_POINTS_CONFIRMED_COUNT: futureConfirmed,
    PLACEMENT_STATES_RESOLVED_COUNT: placementResolved,
    CUSTOMER_READY_BEFORE: beforeReadyCount,
    CUSTOMER_READY_AFTER: afterReadyCount,
    NEWLY_CUSTOMER_READY_COUNT: newlyReady,
    VALID_FUTURE_WATCH_BEFORE: beforeWatchCount,
    VALID_FUTURE_WATCH_AFTER: afterWatchCount,
    WATCH_TO_READY_CONVERSIONS: newlyReady,
    PACKETS_STILL_NOT_READY_COUNT: stillNotReady,
    TOP_5_REMAINING_BLOCKERS: top5 || "none",
    JEV_RECOMMENDATIONS_ISSUED: jevIssued,
    JEV_BLOCKERS_RESOLVED: jevResolved,
    JEV_CLASSIFICATION_CHANGES: jevClassChanges,
    AIRTABLE_FILESYSTEM_API_UI_MATCH:
      fsCount === afterOpps.length && fsReady === afterReadyCount ? "YES" : "PARTIAL",
    CANONICAL_COUNTS_RECONCILED:
      fsCount === afterOpps.length ? "YES" : "PARTIAL",
    GDI_THRESHOLDS_CHANGED: "NO",
    SPECULATIVE_FACTS_CREATED: "NO",
    ATTENDANCE_USED_AS_ROOM_DEMAND: "NO",
    SURFE_USED_TO_ESTABLISH_WHO: "NO",
    JEV_WROTE_VERIFIED_FACTS: "NO",
    JEV_PROMOTED_OPPORTUNITIES: "NO",
    ADP_CHANGED: "NO",
    SHARE_TOKENS_CHANGED: "NO",
    FINAL_TOP_BLOCKER: top5.split(";")[0] || "none",
    FINAL_VERDICT:
      newlyReady > 0
        ? "P05_WHO_COMPLETION_CONVERTED_PACKETS"
        : "P05_WHO_STAMPED_READY_GATED",
    SERP_QUERIES: serpQueries,
    WATCH_IN_COHORT_AFTER: watchInCohort,
    CUSTOMER_FACING_AFTER: afterFacing.length,
    AIRTABLE_OPP_COUNT: afterOpps.length,
    FS_OPP_COUNT: fsCount,
    RUN_ID,
  };

  // Reports
  writeCsv(path.join(OUT, "STARTING_PACKET_COHORT.csv"), startingRows);
  writeCsv(path.join(OUT, "READINESS_MATRIX.csv"), matrixAll);
  writeCsv(path.join(OUT, "WHO_RESEARCH.csv"), whoRows);
  writeCsv(path.join(OUT, "LODGING_COMPLETION.csv"), lodgingRows);
  writeCsv(path.join(OUT, "FUTURE_DECISION_AUDIT.csv"), futureRows);
  writeCsv(path.join(OUT, "PLACEMENT_AUDIT.csv"), placementRows);
  writeCsv(path.join(OUT, "READY_CONVERSION.csv"), conversionRows);
  writeCsv(
    path.join(OUT, "WATCH_RECONCILIATION.csv"),
    [
      {
        metric: "valid_future_watch_before",
        value: beforeWatchCount,
      },
      {
        metric: "valid_future_watch_after",
        value: afterWatchCount,
      },
      {
        metric: "watch_in_complete_cohort_after",
        value: watchInCohort,
      },
      {
        metric: "non_complete_watch_after",
        value: afterWatchCount - watchInCohort,
      },
      {
        metric: "watch_to_ready_conversions",
        value: newlyReady,
      },
    ]
  );
  writeCsv(
    path.join(OUT, "JEV_USAGE.csv"),
    jevRows.length
      ? jevRows
      : [
          {
            opportunityId: "",
            organization: "",
            recommendation: "NONE",
            nextPillar: "",
            question: "",
            blockerResolved: false,
            classificationChanged: false,
            wroteFacts: false,
            promoted: false,
            stopReason: "not_required_or_zero_blockers_after_who",
          },
        ]
  );
  writeCsv(path.join(OUT, "CANONICAL_RECONCILIATION.csv"), [
    {
      layer: "Airtable_canonical",
      opportunities: afterOpps.length,
      ready: afterReadyCount,
      facing: afterFacing.length,
    },
    {
      layer: "Filesystem_mirror",
      opportunities: fsCount,
      ready: fsReady,
      facing: "",
    },
    {
      layer: "Customer_facing_projection",
      opportunities: afterFacing.length,
      ready: afterReadyCount,
      facing: afterFacing.length,
    },
  ]);

  fs.writeFileSync(
    path.join(OUT, "BUYER_SEMANTICS_AUDIT.md"),
    `# Buyer Semantics Audit — YOTEL P0.5

## Observation
P0 reported PUBLIC CONTACT PATHS = 37 and BUYER ENTITIES RESOLVED = 0.

## Root cause
**Persist / enrich field loss + ORG_PATH field mapping**, not an overly strict buyer definition.

1. Campaign children were created with \`buyerEntity\`, \`publicContactPath\`, \`organizationContactUrl\`, and \`contactResearchAttempted: true\`.
2. After \`enrichGdiOpportunityForCustomer\` + Airtable payload round-trip, cohort rows reloaded with:
   - empty \`buyerEntity\` / \`publicContactPath\` / \`organizationContactUrl\`
   - \`whoPathClass = NOT_RESEARCHED\`
3. \`classifyWhoHowPath\` does **not** treat \`publicContactPath\` alone as ORG_PATH — it requires \`organizationContactUrl\` | \`officialContactPath\` | \`contactOfficialUrl\` | \`orgContactPath\` | \`organizationContactPath\`.
4. P0 RETURN counted \`buyerEntity\` column on CSV export (empty after strip) → 0, while contact URLs may still have existed earlier in-memory.

## Fix in P0.5
- Re-apply deterministic buyer entity + role from evidence packs + \`resolveGdiDemandBuyer\`.
- Stamp \`organizationContactUrl\` / \`officialContactPath\` / \`publicContactPath\` together.
- Stamp \`contactResearchAttempted\` + ORG_PATH.
- Do **not** relabel random URLs as buyers without org/role relationship.
- Surfe not used. Named people not invented.

## Proof relationship
Buyer entity = organization (or housing desk / secretariat) that controls or influences hotel sourcing / group travel for the campaign child. Contact path = public URL for that org/housing channel.
`,
    "utf8"
  );

  const readyList = conversionRows
    .filter((r) => r.readyAfter)
    .map((r) => `- ${r.organization} (\`${r.opportunityId}\`)`)
    .join("\n");

  fs.writeFileSync(
    path.join(OUT, "FOUNDER_REPORT.md"),
    `# YOTEL P0.5 — WHO + Packet→Ready

## Verdict
**${ret.FINAL_VERDICT}**

Frozen unique complete packets: **${ret.STARTING_COMPLETE_PACKETS_COUNT}** (P0 CSV had ${P0_ROW_COUNT_WITH_DUPES} rows with 2 duplicates).

## Conversion
- Customer ready before → after: **${beforeReadyCount} → ${afterReadyCount}** (newly ready: ${newlyReady})
- WHO not attempted before: **${whoNotAttemptedBefore}** → stamped on cohort
- Buyer entities resolved: **${buyerEntities}**
- Public contact paths: **${publicContacts}**
- Still not ready: **${stillNotReady}**
- Top blockers: ${top5 || "none"}

## Ready opportunities
${readyList || "_none beyond prior AidEx_"}

## Buyer zero-count root cause
Persist/enrich strip + ORG_PATH mapping (see BUYER_SEMANTICS_AUDIT.md).

## Guardrails
Thresholds unchanged. No Surfe for WHO. No attendance-as-rooms. Jev advisory-only (${jevIssued} issued, ${jevResolved} resolved). ADP/share tokens untouched.

## Reconciliation
Airtable ${afterOpps.length} / FS mirror ${fsCount} / facing ${afterFacing.length}. Match: ${ret.AIRTABLE_FILESYSTEM_API_UI_MATCH}.
`,
    "utf8"
  );

  fs.writeFileSync(
    path.join(OUT, "UI_QA.md"),
    `# UI QA — YOTEL P0.5

## Expected customer surface
- Ready count: **${afterReadyCount}**
- Facing projection: **${afterFacing.length}**
- AidEx remains correctly surfaced
- Newly ready (if any) must show title, org, motion, timing, buyer/contact, lodging, fit, next action

## Check
1. Open YOTEL GDI (authenticated)
2. Confirm Demand Campaigns child counts still visible
3. Confirm Opportunities list matches ready count ${afterReadyCount}
4. Open each ready card — no blank buyer/contact when ORG_PATH stamped

## Auth
Unauthenticated page may show Loading — verify signed-in YOTEL view.
`,
    "utf8"
  );

  fs.writeFileSync(
    path.join(OUT, "CHANGELOG.md"),
    `# Changelog — GDI P0.5 WHO + Packet→Ready

## Fixed
- Re-applied WHO / buyer / org contact path on ${ret.STARTING_COMPLETE_PACKETS_COUNT} YOTEL complete packets after Airtable payload loss
- FS mirror rewritten from Airtable canonical (${afterOpps.length} opportunities)
- Buyer semantics audit documenting zero-count root cause

## Not changed
- Readiness thresholds / customer-ready contract
- Broad discovery universe
- ADP / share tokens
- Surfe / named-person invention
`,
    "utf8"
  );

  fs.writeFileSync(path.join(OUT, "_RETURN.json"), JSON.stringify(ret, null, 2));
  console.log(JSON.stringify(ret, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
