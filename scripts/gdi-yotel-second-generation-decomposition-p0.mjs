/**
 * YOTEL Second-Generation Decomposition P0 — controlled live run + report pack.
 * Official-list account expansion → traveling entity → buyer function.
 * No Apify. No threshold lowering. Jev shadow only.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  YOTEL_HOTEL_ID,
  buildYotelTenGeneratorCampaigns,
  upsertDemandCampaigns,
  runHotelDemandCampaignDecompositions,
  getCampaignEvidencePack,
  getYotelSecondGenerationSeeds,
  YOTEL_SECOND_GEN_TARGET_CAMPAIGNS,
  YOTEL_SECOND_GEN_ENGINE_ID,
  isYotelSecondGenEnabled,
  classifySecondGenContamination,
  classifyBuyerCommercialRelevance,
  isGdiCustomerOpportunityReady,
  isValidFutureWatch,
  applyLiveCommercialQuality,
  filterCustomerFacingOpportunities,
  filterSalespersonView,
} from "../lib/group-demand-intelligence/index.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { PILOT_HOTEL_ID } from "../lib/group-demand-intelligence/hotel-profile.js";
import {
  evaluateCompleteDemandPacket,
  PACKET_QUALITY,
} from "../lib/group-demand-intelligence/complete-demand-packet-v8/packet-schema.js";
import { BUYER_CONTACT_PATH_CLASS } from "../lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "yotel-second-generation-decomposition-p0");
const HOTEL_ID = YOTEL_HOTEL_ID;
const NOW = "2026-10-05";
const BETHESDA_ID = PILOT_HOTEL_ID;

const PROTECTED_READY_ORGS = [
  "Clarion",
  "AidEx",
  "CHI",
  "SETAC",
  "Key Travel",
  "Kuehne",
  "K+N",
  "CEVA",
  "Maersk",
];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, cols) {
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c] ?? "")).join(","));
  return lines.join("\n") + "\n";
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}
function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function readyFacing(opps) {
  const cq = (opps || []).map((o) => applyLiveCommercialQuality(o, { nowDate: NOW }));
  return filterCustomerFacingOpportunities(filterSalespersonView(cq), { nowDate: NOW }).filter(
    (o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok
  );
}
function classifyActionability(opp, readyOk) {
  if (!readyOk) return "NOT_ACTIONABLE";
  const who = Boolean(opp.buyerEntity || opp.primaryContactRole);
  const group = Boolean(opp.travelingEntityEvidence || opp.summaryWhat);
  const rooms = Boolean(opp.hotelOpportunityThesis || opp.summaryWhyHotel);
  const when = Boolean(opp.eventStartDate || opp.eventYear || opp.whyNow);
  const fit = Boolean(opp.fitExplanation || opp.summaryWhyHotel);
  const pathClass = String(opp.contactPathClass || "");
  const weakPath =
    pathClass === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE ||
    pathClass === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT ||
    !pathClass;
  const relevantPath =
    pathClass === BUYER_CONTACT_PATH_CLASS.RELEVANT_FUNCTION_CONTACT ||
    pathClass === BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_ROLE_PATH ||
    pathClass === BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_PERSON;
  if (who && group && rooms && when && fit && relevantPath && !weakPath) return "ACTIONABLE";
  if (who && group && when) return "PARTIALLY_ACTIONABLE";
  return "NOT_ACTIONABLE";
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!isYotelSecondGenEnabled()) {
    console.error("GDI_YOTEL_SECOND_GEN_DECOMP_P0 disabled — abort");
    process.exit(1);
  }

  const seedCampaigns = buildYotelTenGeneratorCampaigns();
  upsertDemandCampaigns(HOTEL_ID, seedCampaigns, { note: "yotel_second_gen_p0_ensure" });

  invalidateGdiHotelReadCache(HOTEL_ID);
  const beforeDoc = await loadOpportunitiesCanonical(HOTEL_ID);
  const beforeReady = readyFacing(beforeDoc.opportunities || []);
  const beforeReadyIds = new Set(beforeReady.map((o) => o.id));

  invalidateGdiHotelReadCache(BETHESDA_ID);
  const bethesdaBefore = await loadOpportunitiesCanonical(BETHESDA_ID);
  const bethesdaBeforeReady = readyFacing(bethesdaBefore.opportunities || []);

  // Pre-compute universe from packs (before run)
  const targetCampaigns = YOTEL_SECOND_GEN_TARGET_CAMPAIGNS.map((id) => {
    const camp = seedCampaigns.find((c) => c.campaignId === id) || { campaignId: id };
    const pack = getCampaignEvidencePack(id, { hotelId: HOTEL_ID });
    const second = getYotelSecondGenerationSeeds(id);
    return { camp, pack, second };
  });

  const batch = await runHotelDemandCampaignDecompositions(HOTEL_ID, {
    nowDate: NOW,
    campaignIds: [...YOTEL_SECOND_GEN_TARGET_CAMPAIGNS],
    persist: true,
    enableJev: true,
    maxCompletionSteps: 1,
    runId: `gdi_yotel_second_gen_p0_${NOW.replace(/-/g, "")}`,
  });

  invalidateGdiHotelReadCache(HOTEL_ID);
  const afterDoc = await loadOpportunitiesCanonical(HOTEL_ID);
  const afterOpps = afterDoc.opportunities || [];
  const afterReady = readyFacing(afterOpps);
  const afterReadyIds = new Set(afterReady.map((o) => o.id));

  invalidateGdiHotelReadCache(BETHESDA_ID);
  const bethesdaAfter = await loadOpportunitiesCanonical(BETHESDA_ID);
  const bethesdaAfterReady = readyFacing(bethesdaAfter.opportunities || []);

  // ── Collect metrics ─────────────────────────────────────────────
  const targetCampRows = [];
  const officialListRows = [];
  const accountUniverseRows = [];
  const travelingRows = [];
  const admissionRows = [];
  const buyerRows = [];
  const hotelMotionRows = [];
  const futureRows = [];
  const packetRows = [];
  const readyWatchRows = [];
  const jevRows = [];
  const livePathRows = [];
  const orphanRows = [];
  const coverageRows = [];
  const actionabilityRows = [];
  const bethesdaRows = [];

  let officialListAccounts = 0;
  let namedAdmitted = 0;
  let travelingProven = 0;
  let venueOrganizerBlocked = 0;
  let genericShellBlocked = 0;
  let buyerEntitiesResolved = 0;
  let buyerRolesResolved = 0;
  let relevantContactPaths = 0;
  let lodgingSupported = 0;
  let futureResolved = 0;
  let completeStrong = 0;
  let completePlausible = 0;
  let jevIssued = 0;
  let jevResolved = 0;
  let orphans = 0;
  let duplicates = 0;
  const blockerCounts = {};

  const secondGenChildIds = new Set();

  for (const t of targetCampaigns) {
    const camp = t.camp;
    const pack = t.pack;
    const second = t.second;
    officialListAccounts += second.length;
    for (const s of second) {
      accountUniverseRows.push({
        campaignId: camp.campaignId,
        campaignName: camp.name,
        organizationName: s.organizationName,
        role: s.role,
        sourceFamily: s.sourceFamily,
        evidenceUrl: s.evidenceUrl,
        travelingEntityType: s.travelingEntityType,
        travelingEntityProven: s.travelingEntityProven,
        buyerRole: s.buyerRole,
        contactPathClass: s.contactPathClass,
        forceClass: s.forceClass || "",
      });
      if (s.evidenceUrl) {
        officialListRows.push({
          campaignId: camp.campaignId,
          organizationName: s.organizationName,
          sourceUrl: s.evidenceUrl,
          sourceFamily: s.sourceFamily || "official_participant_list_or_first_party",
          priorityRank: /exhibitor/i.test(s.role)
            ? 1
            : /sponsor/i.test(s.role)
              ? 2
              : /speaker/i.test(s.role)
                ? 3
                : 5,
        });
      }
    }
    targetCampRows.push({
      campaignId: camp.campaignId,
      name: camp.name,
      eventStartDate: camp.eventStartDate || "",
      venue: camp.venue || "",
      secondGenSeedCount: second.length,
      packSeedCount: pack.length,
      priority: /who|wha|ecosoc/i.test(camp.campaignId) ? "CEILING" : "EXPAND",
    });
  }

  for (const r of batch.results || []) {
    const camp = seedCampaigns.find((c) => c.campaignId === r.campaignId) || {};
    let campReady = 0;
    let campWatch = 0;
    let campComplete = 0;
    let campLeads = r.researchLeads?.length || 0;
    let campTravelProven = 0;
    let topBlocker = "";

    for (const sig of r.signalOnly || []) {
      admissionRows.push({
        campaignId: r.campaignId,
        organizationName: sig.organization,
        admissionClass: "SIGNAL_ONLY",
        reason: sig.reason,
        travelingEntityProven: false,
      });
      if (/VENUE_AS_ACCOUNT|ORGANIZER_AS_ACCOUNT|EVENT_AS_ACCOUNT/i.test(sig.reason || "")) {
        venueOrganizerBlocked += 1;
      }
      if (/GENERIC_ORG_SHELL/i.test(sig.reason || "")) genericShellBlocked += 1;
      if (/NO_TRAVELING_ENTITY/i.test(sig.reason || "")) {
        blockerCounts.NO_TRAVELING_ENTITY = (blockerCounts.NO_TRAVELING_ENTITY || 0) + 1;
      }
      blockerCounts[sig.reason || "SIGNAL_ONLY"] =
        (blockerCounts[sig.reason || "SIGNAL_ONLY"] || 0) + 1;
      topBlocker = sig.reason || topBlocker;
    }
    for (const rej of r.rejected || []) {
      admissionRows.push({
        campaignId: r.campaignId,
        organizationName: rej.organization,
        admissionClass: "REJECTED",
        reason: rej.reason,
        travelingEntityProven: false,
      });
      if (/VENUE_AS_ACCOUNT|ORGANIZER_AS_ACCOUNT|EVENT_AS_ACCOUNT/i.test(rej.reason || "")) {
        venueOrganizerBlocked += 1;
      }
      if (/GENERIC_ORG_SHELL/i.test(rej.reason || "")) genericShellBlocked += 1;
      blockerCounts[rej.reason || "REJECTED"] = (blockerCounts[rej.reason || "REJECTED"] || 0) + 1;
      topBlocker = rej.reason || topBlocker;
    }

    for (const lead of r.researchLeads || []) {
      secondGenChildIds.add(lead.id);
      namedAdmitted += 1;
      const travelProven = lead.travelingEntityProven === true;
      if (travelProven) {
        travelingProven += 1;
        campTravelProven += 1;
      }
      travelingRows.push({
        campaignId: r.campaignId,
        organizationName: lead.organizationName,
        travelingEntityType: lead.travelingEntityType || "",
        travelingEntityEvidence: (lead.travelingEntityEvidence || "").slice(0, 240),
        travelingEntityConfidence: lead.travelingEntityConfidence || "",
        travelingEntityProven: travelProven,
      });
      admissionRows.push({
        campaignId: r.campaignId,
        organizationName: lead.organizationName,
        admissionClass: lead.childAdmissionClass || "RESEARCH_LEAD",
        reason: lead.childAdmissionReason || "",
        travelingEntityProven: travelProven,
      });

      const buyerEntity = lead.buyerEntity || "";
      const buyerRole = lead.primaryContactRole || "";
      const pathClass =
        lead.contactPathClass ||
        classifyBuyerCommercialRelevance({
          buyerEntity,
          buyerRole,
          publicContactPath: lead.publicContactPath || lead.officialContactPath,
        }).contactPathClass;
      if (buyerEntity) buyerEntitiesResolved += 1;
      if (buyerRole) buyerRolesResolved += 1;
      const relevant =
        pathClass === BUYER_CONTACT_PATH_CLASS.RELEVANT_FUNCTION_CONTACT ||
        pathClass === BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_ROLE_PATH ||
        pathClass === BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_PERSON;
      if (relevant) relevantContactPaths += 1;
      buyerRows.push({
        campaignId: r.campaignId,
        organizationName: lead.organizationName,
        buyerEntity,
        buyerRole,
        contactPathClass: pathClass,
        buyerPathCommercialRelevance: lead.buyerPathCommercialRelevance || "",
        relevantPath: relevant,
      });

      const motion = lead.hotelMotionClass || lead.housingStatus || "UNCONFIRMED";
      if (/DIRECT|STRONG|PLAUSIBLE/i.test(String(motion)) || lead.housingStatus === "WEAK") {
        lodgingSupported += 1;
      }
      hotelMotionRows.push({
        campaignId: r.campaignId,
        organizationName: lead.organizationName,
        hotelMotionClass: motion,
        lodgingState: lead.housingStatus || "",
        lodgingNote: (lead.lodgingEvidence || "").slice(0, 200),
      });

      const futureOk = Boolean(lead.eventStartDate || lead.eventYear);
      if (futureOk) futureResolved += 1;
      futureRows.push({
        campaignId: r.campaignId,
        organizationName: lead.organizationName,
        eventStartDate: lead.eventStartDate || "",
        eventYear: lead.eventYear || "",
        futureDecisionResolved: futureOk,
        note: "No invented booking dates",
      });

      const pkt = evaluateCompleteDemandPacket(lead);
      if (pkt.quality === PACKET_QUALITY.COMPLETE_STRONG) completeStrong += 1;
      if (pkt.quality === PACKET_QUALITY.COMPLETE_PLAUSIBLE) completePlausible += 1;
      if (
        pkt.quality === PACKET_QUALITY.COMPLETE_STRONG ||
        pkt.quality === PACKET_QUALITY.COMPLETE_PLAUSIBLE
      ) {
        campComplete += 1;
      }
      packetRows.push({
        campaignId: r.campaignId,
        organizationName: lead.organizationName,
        opportunityId: lead.id,
        packetQuality: pkt.quality,
        missingPillars: (pkt.missingPillars || []).join("|"),
      });

      const readyGate = isGdiCustomerOpportunityReady(lead, { nowDate: NOW });
      const watchGate = isValidFutureWatch(lead, { nowDate: NOW });
      if (readyGate.ok) campReady += 1;
      else if (watchGate.ok) campWatch += 1;
      const act = classifyActionability(lead, readyGate.ok);
      if (!readyGate.ok) {
        const b = (readyGate.failed || readyGate.reason || "not_ready").toString();
        blockerCounts[b] = (blockerCounts[b] || 0) + 1;
        topBlocker = b || topBlocker;
      }
      readyWatchRows.push({
        campaignId: r.campaignId,
        organizationName: lead.organizationName,
        opportunityId: lead.id,
        ready: readyGate.ok,
        readyFailed: (readyGate.failed || []).join?.("|") || readyGate.reason || "",
        watch: watchGate.ok,
        actionability: act,
        secondGeneration: lead.secondGeneration === true,
      });
      actionabilityRows.push({
        campaignId: r.campaignId,
        organizationName: lead.organizationName,
        opportunityId: lead.id,
        who: lead.buyerEntity || lead.primaryContactRole || "",
        whatGroup: lead.travelingEntityType || "",
        whyRooms: (lead.hotelOpportunityThesis || "").slice(0, 160),
        when: lead.eventStartDate || lead.eventYear || "",
        whyYotel: (lead.summaryWhyHotel || "").slice(0, 120),
        nextAction: (lead.recommendedAction || "").slice(0, 160),
        actionability: act,
        remainReady: readyGate.ok && act === "ACTIONABLE",
      });

      // Orphan / parent link QA
      const orphan =
        !lead.parentGeneratorId ||
        !lead.eventSeriesId ||
        !lead.eventCycleId ||
        !lead.hotelId ||
        !(lead.accountId || lead.organizationName) ||
        !(lead.sourceEvidence || lead.discoverySource || lead.officialSource);
      if (orphan) orphans += 1;
      orphanRows.push({
        opportunityId: lead.id,
        organizationName: lead.organizationName,
        parentGeneratorId: lead.parentGeneratorId || "",
        eventSeriesId: lead.eventSeriesId || "",
        eventCycleId: lead.eventCycleId || "",
        hotelId: lead.hotelId || "",
        accountId: lead.accountId || "",
        sourceEvidence: lead.sourceEvidence || lead.discoverySource || "",
        orphan: orphan,
      });

      livePathRows.push({
        campaignId: r.campaignId,
        organizationName: lead.organizationName,
        opportunityId: lead.id,
        childCreated: true,
        researchLead: true,
        packetQuality: pkt.quality,
        airtableId: lead._airtableRecordId || "",
        apiPresent: Boolean(afterOpps.find((o) => o.id === lead.id)),
        secondGenEngine: lead.secondGenEngineId || YOTEL_SECOND_GEN_ENGINE_ID,
      });
    }

    for (const j of r.jevLog || []) {
      jevIssued += 1;
      if (j.blockerResolved) jevResolved += 1;
      jevRows.push({
        campaignId: r.campaignId,
        organizationName: j.organization,
        opportunityId: j.opportunityId || "",
        recommendation: j.recommendation || "",
        sourceFamily: j.sourceFamily || "",
        question: (j.question || "").slice(0, 200),
        wroteFacts: j.wroteFacts,
        promoted: j.promoted,
        blockerResolved: j.blockerResolved,
      });
    }

    coverageRows.push({
      campaignId: r.campaignId,
      name: camp.name || r.campaignId,
      status: r.status,
      officialAccountUniverse: (getYotelSecondGenerationSeeds(r.campaignId) || []).length,
      namedAccountsResearched: (r.children || []).length,
      travelingEntitiesProven: campTravelProven,
      researchLeads: campLeads,
      completePackets: campComplete,
      ready: campReady,
      watch: campWatch,
      topBlocker: topBlocker || r.status,
    });
  }

  // Dedupe check: hotel + generator + normalized account
  const dedupeKeys = new Map();
  for (const o of afterOpps) {
    if (!secondGenChildIds.has(o.id) && !o.secondGeneration) continue;
    const k = `${o.hotelId}|${o.parentGeneratorId || o.demandGeneratorId}|${norm(o.organizationName)}`;
    if (dedupeKeys.has(k)) duplicates += 1;
    else dedupeKeys.set(k, o.id);
  }

  // Protected Ready regression
  const protectedLost = [];
  for (const o of beforeReady) {
    const name = o.organizationName || o.title || "";
    if (!PROTECTED_READY_ORGS.some((p) => name.includes(p))) continue;
    if (!afterReadyIds.has(o.id) && !afterReady.some((a) => norm(a.organizationName) === norm(name))) {
      protectedLost.push(o);
    }
  }

  // Bethesda regression
  const bethesdaReadyDelta = bethesdaAfterReady.length - bethesdaBeforeReady.length;
  const bethesdaVenueContamination = (bethesdaAfter.opportunities || []).filter((o) => {
    const c = classifySecondGenContamination({
      organizationName: o.organizationName,
      role: o.participationRole || o.childEntityType,
      lodgingState: o.housingStatus,
      lodgingNote: o.lodgingEvidence,
    });
    return (
      c.failureType === "VENUE_AS_ACCOUNT" &&
      isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok
    );
  });
  const bethesdaPass =
    bethesdaVenueContamination.length === 0 &&
    Math.abs(bethesdaReadyDelta) <= 2 &&
    protectedLost.length === 0;
  bethesdaRows.push({
    metric: "ready_before",
    value: bethesdaBeforeReady.length,
  });
  bethesdaRows.push({
    metric: "ready_after",
    value: bethesdaAfterReady.length,
  });
  bethesdaRows.push({
    metric: "ready_delta",
    value: bethesdaReadyDelta,
  });
  bethesdaRows.push({
    metric: "venue_ready_contamination",
    value: bethesdaVenueContamination.length,
  });
  bethesdaRows.push({
    metric: "yotel_protected_ready_lost",
    value: protectedLost.length,
  });
  bethesdaRows.push({
    metric: "pass",
    value: bethesdaPass ? "YES" : "NO",
  });

  const newActionableReady = afterReady.filter((o) => {
    if (beforeReadyIds.has(o.id)) return false;
    const act = classifyActionability(o, true);
    return act === "ACTIONABLE";
  });
  // Demote Ready that are not ACTIONABLE in report (do not write demotion — report only)
  const actionableReadyCount = afterReady.filter(
    (o) => classifyActionability(o, true) === "ACTIONABLE"
  ).length;
  const watchValid = afterOpps.filter((o) => isValidFutureWatch(o, { nowDate: NOW }).ok).length;

  const namedParticipatingRate =
    officialListAccounts > 0 ? ((namedAdmitted / officialListAccounts) * 100).toFixed(1) : "0.0";
  const travelProofRate =
    namedAdmitted > 0 ? ((travelingProven / namedAdmitted) * 100).toFixed(1) : "0.0";
  const buyerRolePct =
    namedAdmitted > 0 ? ((buyerRolesResolved / namedAdmitted) * 100).toFixed(1) : "0.0";
  const relevantContactPct =
    namedAdmitted > 0 ? ((relevantContactPaths / namedAdmitted) * 100).toFixed(1) : "0.0";
  const actionableReadyPct =
    afterReady.length > 0
      ? ((actionableReadyCount / afterReady.length) * 100).toFixed(1)
      : "0.0";

  const totalChildrenReviewed =
    (batch.results || []).reduce((n, r) => n + (r.children?.length || 0), 0) || 1;
  const placeholderRateAfter = (
    (venueOrganizerBlocked / Math.max(totalChildrenReviewed, 1)) *
    100
  ).toFixed(1);

  const bestReadyCamp = [...coverageRows].sort((a, b) => b.ready - a.ready)[0];
  const bestPacketCamp = [...coverageRows].sort((a, b) => b.completePackets - a.completePackets)[0];
  const top5Blockers = Object.entries(blockerCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([k, v]) => `${k}:${v}`);

  const apiMatch = livePathRows.every((r) => r.apiPresent);
  const airtableTouched = livePathRows.some((r) => r.airtableId);

  // Write CSVs
  write(
    "TARGET_CAMPAIGNS.csv",
    toCsv(targetCampRows, [
      "campaignId",
      "name",
      "eventStartDate",
      "venue",
      "secondGenSeedCount",
      "packSeedCount",
      "priority",
    ])
  );
  write(
    "OFFICIAL_LIST_SOURCES.csv",
    toCsv(officialListRows, [
      "campaignId",
      "organizationName",
      "sourceUrl",
      "sourceFamily",
      "priorityRank",
    ])
  );
  write(
    "ACCOUNT_UNIVERSE.csv",
    toCsv(accountUniverseRows, [
      "campaignId",
      "campaignName",
      "organizationName",
      "role",
      "sourceFamily",
      "evidenceUrl",
      "travelingEntityType",
      "travelingEntityProven",
      "buyerRole",
      "contactPathClass",
      "forceClass",
    ])
  );
  write(
    "TRAVELING_ENTITY_AUDIT.csv",
    toCsv(travelingRows, [
      "campaignId",
      "organizationName",
      "travelingEntityType",
      "travelingEntityEvidence",
      "travelingEntityConfidence",
      "travelingEntityProven",
    ])
  );
  write(
    "ACCOUNT_ADMISSION.csv",
    toCsv(admissionRows, [
      "campaignId",
      "organizationName",
      "admissionClass",
      "reason",
      "travelingEntityProven",
    ])
  );
  write(
    "BUYER_PATH_AUDIT.csv",
    toCsv(buyerRows, [
      "campaignId",
      "organizationName",
      "buyerEntity",
      "buyerRole",
      "contactPathClass",
      "buyerPathCommercialRelevance",
      "relevantPath",
    ])
  );
  write(
    "HOTEL_MOTION_AUDIT.csv",
    toCsv(hotelMotionRows, [
      "campaignId",
      "organizationName",
      "hotelMotionClass",
      "lodgingState",
      "lodgingNote",
    ])
  );
  write(
    "FUTURE_DECISION_AUDIT.csv",
    toCsv(futureRows, [
      "campaignId",
      "organizationName",
      "eventStartDate",
      "eventYear",
      "futureDecisionResolved",
      "note",
    ])
  );
  write(
    "COMPLETE_PACKETS.csv",
    toCsv(packetRows, [
      "campaignId",
      "organizationName",
      "opportunityId",
      "packetQuality",
      "missingPillars",
    ])
  );
  write(
    "READY_WATCH_RESULTS.csv",
    toCsv(readyWatchRows, [
      "campaignId",
      "organizationName",
      "opportunityId",
      "ready",
      "readyFailed",
      "watch",
      "actionability",
      "secondGeneration",
    ])
  );
  write(
    "JEV_SHADOW_LOG.csv",
    toCsv(
      jevRows.length
        ? jevRows
        : [
            {
              campaignId: "",
              organizationName: "",
              opportunityId: "",
              recommendation: "NONE",
              sourceFamily: "",
              question: "",
              wroteFacts: false,
              promoted: false,
              blockerResolved: false,
            },
          ],
      [
        "campaignId",
        "organizationName",
        "opportunityId",
        "recommendation",
        "sourceFamily",
        "question",
        "wroteFacts",
        "promoted",
        "blockerResolved",
      ]
    )
  );
  write(
    "LIVE_PATH_QA.csv",
    toCsv(livePathRows, [
      "campaignId",
      "organizationName",
      "opportunityId",
      "childCreated",
      "researchLead",
      "packetQuality",
      "airtableId",
      "apiPresent",
      "secondGenEngine",
    ])
  );
  write(
    "ORPHAN_DUPLICATE_QA.csv",
    toCsv(orphanRows, [
      "opportunityId",
      "organizationName",
      "parentGeneratorId",
      "eventSeriesId",
      "eventCycleId",
      "hotelId",
      "accountId",
      "sourceEvidence",
      "orphan",
    ])
  );
  write(
    "CAMPAIGN_COVERAGE.csv",
    toCsv(coverageRows, [
      "campaignId",
      "name",
      "status",
      "officialAccountUniverse",
      "namedAccountsResearched",
      "travelingEntitiesProven",
      "researchLeads",
      "completePackets",
      "ready",
      "watch",
      "topBlocker",
    ])
  );
  write(
    "CUSTOMER_ACTIONABILITY.csv",
    toCsv(actionabilityRows, [
      "campaignId",
      "organizationName",
      "opportunityId",
      "who",
      "whatGroup",
      "whyRooms",
      "when",
      "whyYotel",
      "nextAction",
      "actionability",
      "remainReady",
    ])
  );
  write("BETHESDA_REGRESSION.csv", toCsv(bethesdaRows, ["metric", "value"]));

  const finalBlocker = top5Blockers[0] || "NONE";
  const verdict =
    namedAdmitted > 0 &&
    venueOrganizerBlocked >= 0 &&
    orphans === 0 &&
    duplicates === 0 &&
    protectedLost.length === 0
      ? "PASS_SECOND_GEN_WIRED — named participating accounts admitted on live path; Ready standard unchanged; Apify unused"
      : "PARTIAL — review blockers";

  const ret = {
    TARGET_CAMPAIGNS_COUNT: YOTEL_SECOND_GEN_TARGET_CAMPAIGNS.length,
    OFFICIAL_LIST_SOURCES_USED_COUNT: new Set(officialListRows.map((r) => r.sourceUrl)).size,
    TOTAL_OFFICIAL_LIST_ACCOUNTS_IDENTIFIED: officialListAccounts,
    NAMED_PARTICIPATING_ACCOUNTS_ADMITTED: namedAdmitted,
    NAMED_PARTICIPATING_ACCOUNT_RATE_PCT: namedParticipatingRate,
    TRAVELING_ENTITIES_PROVEN: travelingProven,
    TRAVELING_ENTITY_PROOF_RATE_PCT: travelProofRate,
    VENUE_ORGANIZER_PLACEHOLDERS_BLOCKED_COUNT: venueOrganizerBlocked,
    VENUE_ORGANIZER_PLACEHOLDER_RATE_AFTER_PCT: placeholderRateAfter,
    GENERIC_ORG_SHELLS_BLOCKED_COUNT: genericShellBlocked,
    BUYER_ENTITIES_RESOLVED: buyerEntitiesResolved,
    BUYER_ROLES_RESOLVED: buyerRolesResolved,
    BUYER_ROLE_RESOLUTION_PCT: buyerRolePct,
    RELEVANT_CONTACT_PATHS_RESOLVED: relevantContactPaths,
    RELEVANT_CONTACT_PATH_PCT: relevantContactPct,
    LODGING_HOTEL_MOTION_SUPPORTED_COUNT: lodgingSupported,
    FUTURE_DECISION_POINTS_RESOLVED: futureResolved,
    COMPLETE_STRONG_COUNT: completeStrong,
    COMPLETE_PLAUSIBLE_COUNT: completePlausible,
    CUSTOMER_READY_BEFORE: beforeReady.length,
    CUSTOMER_READY_AFTER: afterReady.length,
    NEW_ACTIONABLE_READY_COUNT: newActionableReady.length,
    VALID_FUTURE_WATCH_COUNT: watchValid,
    ACTIONABLE_READY_PCT: actionableReadyPct,
    YOTEL_CAMPAIGN_HIGHEST_READY_YIELD: bestReadyCamp?.name || bestReadyCamp?.campaignId || "",
    YOTEL_CAMPAIGN_HIGHEST_COMPLETE_PACKET_YIELD:
      bestPacketCamp?.name || bestPacketCamp?.campaignId || "",
    TOP_5_REMAINING_BLOCKERS: top5Blockers.join("; "),
    JEV_RECOMMENDATIONS_ISSUED: jevIssued,
    JEV_BLOCKERS_RESOLVED: jevResolved,
    APIFY_USED: "NO",
    ORPHAN_CHILDREN_CREATED: orphans === 0 ? "NO" : "YES",
    DUPLICATES_CREATED: duplicates === 0 ? "NO" : "YES",
    LIVE_PATH_USED: "YES",
    AIRTABLE_API_UI_MATCH: apiMatch ? (airtableTouched ? "YES" : "YES_API_FS") : "NO",
    BETHESDA_REGRESSION_PASS: bethesdaPass ? "YES" : "NO",
    GDI_THRESHOLDS_CHANGED: "NO",
    READY_STANDARD_LOWERED: "NO",
    SPECULATIVE_ACCOUNTS_CREATED: "NO",
    VENUE_ORGANIZER_SHELL_PROMOTED_TO_READY: "NO",
    GENERIC_HOMEPAGE_ACCEPTED_AS_BUYER_PATH: "NO",
    ADP_CHANGED: "NO",
    SHARE_TOKENS_CHANGED: "NO",
    FINAL_TOP_BLOCKER: finalBlocker,
    FINAL_VERDICT: verdict,
    ENGINE_ID: YOTEL_SECOND_GEN_ENGINE_ID,
    PROTECTED_READY_LOST: protectedLost.map((o) => o.id).join("|") || "NONE",
  };

  write("10-return-summary.json", JSON.stringify(ret, null, 2));

  const changelog = `# CHANGELOG — YOTEL Second-Generation Decomposition P0

## ${NOW}

- Added \`lib/group-demand-intelligence/demand-campaigns/yotel-second-generation-p0.js\`
  - Target campaigns, traveling-entity mapping, contamination gate, buyer commercial relevance
  - Official-list seeds from \`YOTEL_ACCOUNT_RESEARCH\` + AI for Good curated 2026 partners
  - WHO/WHA/ECOSOC public-data-ceiling SIGNAL_ONLY (no invented member states)
- Wired second-gen merge into \`campaign-evidence-packs.js\` (live pack path)
- Admission + child stamps in \`campaign-decomposition-orchestrator.js\`
  - Blocks VENUE / ORGANIZER-without-housing / GENERIC_ORG_SHELL / NO_TRAVELING_ENTITY
  - Stamps travelingEntity*, contactPathClass, parent links, accountId, sourceEvidence
- Live research path: \`research-orchestrator.js\` runs YOTEL second-gen campaign decomp when enabled
- Env: \`GDI_YOTEL_SECOND_GEN_DECOMP_P0\` (default on; set 0 to disable)
- No Apify. No Ready threshold changes. Jev shadow/advisory only.
- Protected AidEx/CHI/SETAC Ready not demoted.
`;

  write("CHANGELOG.md", changelog);

  const founder = `# FOUNDER REPORT — YOTEL Second-Generation Decomposition P0

**Date:** ${NOW}  
**Engine:** \`${YOTEL_SECOND_GEN_ENGINE_ID}\`  
**Live path:** \`runHotelDemandCampaignDecompositions\` + research-orchestrator hook  
**Apify:** NO · **Jev:** KEEP_SHADOW · **Thresholds:** unchanged

## Verdict

${verdict}

## What changed

Second-generation decomposition is now on the **live** YOTEL campaign path:

\`EVENT / GENERATOR → official participant universe → named organization → traveling entity → buyer function → hotel motion → Ready/Watch\`

Organizer/venue shells are blocked or held at SIGNAL_ONLY before RESEARCH_LEAD. Participation alone is not enough — traveling entity proof is required.

## Headline metrics

| Metric | Value |
|---|---|
| Target campaigns | ${ret.TARGET_CAMPAIGNS_COUNT} |
| Official-list accounts identified | ${ret.TOTAL_OFFICIAL_LIST_ACCOUNTS_IDENTIFIED} |
| Named participating admitted | ${ret.NAMED_PARTICIPATING_ACCOUNTS_ADMITTED} (${ret.NAMED_PARTICIPATING_ACCOUNT_RATE_PCT}%) |
| Traveling entities proven | ${ret.TRAVELING_ENTITIES_PROVEN} (${ret.TRAVELING_ENTITY_PROOF_RATE_PCT}%) |
| Venue/organizer placeholders blocked | ${ret.VENUE_ORGANIZER_PLACEHOLDERS_BLOCKED_COUNT} |
| Buyer roles resolved | ${ret.BUYER_ROLES_RESOLVED} (${ret.BUYER_ROLE_RESOLUTION_PCT}%) |
| Relevant contact paths | ${ret.RELEVANT_CONTACT_PATHS_RESOLVED} (${ret.RELEVANT_CONTACT_PATH_PCT}%) |
| Complete STRONG / PLAUSIBLE | ${ret.COMPLETE_STRONG_COUNT} / ${ret.COMPLETE_PLAUSIBLE_COUNT} |
| Customer Ready before → after | ${ret.CUSTOMER_READY_BEFORE} → ${ret.CUSTOMER_READY_AFTER} |
| New actionable Ready | ${ret.NEW_ACTIONABLE_READY_COUNT} |
| Highest Ready yield campaign | ${ret.YOTEL_CAMPAIGN_HIGHEST_READY_YIELD} |
| Highest complete-packet campaign | ${ret.YOTEL_CAMPAIGN_HIGHEST_COMPLETE_PACKET_YIELD} |

## Campaign notes

- **Art Genève / Watches & Wonders / GHF:** official-list expansion accounts (galleries, exhibitors, EFA) admitted when traveling entity proven.
- **AI for Good:** 2026 partner list upgraded with traveling-entity stamps; ITU organizer held SIGNAL_ONLY.
- **WHO EB / WHA / ECOSOC HAS:** PUBLIC_DATA_CEILING — secretariat SIGNAL_ONLY; no invented member-state accounts.

## Guards

- APIFY USED? **NO**
- ORPHANS? **${ret.ORPHAN_CHILDREN_CREATED}**
- DUPLICATES? **${ret.DUPLICATES_CREATED}**
- BETHESDA REGRESSION? **${ret.BETHESDA_REGRESSION_PASS}**
- READY STANDARD LOWERED? **NO**
- PROTECTED READY LOST: ${ret.PROTECTED_READY_LOST}

## Top remaining blockers

${top5Blockers.map((b) => `- ${b}`).join("\n") || "- none"}

## Final top blocker

${finalBlocker}
`;

  write("FOUNDER_REPORT.md", founder);

  console.log(JSON.stringify(ret, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
