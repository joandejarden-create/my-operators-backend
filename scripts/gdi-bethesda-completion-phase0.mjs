#!/usr/bin/env node
/**
 * Bethesda Marriott GDI Completion V1 — Phase 0 corpus snapshot (read-only).
 *   node scripts/gdi-bethesda-completion-v1.mjs --phase0
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";

const HOTEL = "recLuxvwwxID7U2B8";
const NOW = new Date().toISOString().slice(0, 10);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/group-demand-intelligence/bethesda-completion-v1"
);

function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2), "utf8");
}

function oppId(o) {
  return o?.id || o?.opportunityId || null;
}

function segmentOf(o) {
  const blob = `${o.title || ""} ${o.organizationName || ""} ${o.opportunityType || ""} ${o.demandSegment || ""} ${o.segment || ""} ${o.summaryWhat || ""}`.toLowerCase();
  if (/\bnih\b|national institutes of health|hhs\b|clinical center/.test(blob)) return "NIH";
  if (/\b(medical|healthcare|scientific|clinical|oncolog|radiolog|cardiolog|neurolog|symposium|congress)\b/.test(blob))
    return "MEDICAL_SCIENTIFIC";
  if (/\b(government|contractor|federal|dod|gsa|industry day|procurement)\b/.test(blob))
    return "GOVERNMENT_CONTRACTOR";
  if (/\b(university|alumni|college|commencement|reunion)\b/.test(blob)) return "UNIVERSITY";
  if (/\b(tournament|youth|cheer|soccer|lacrosse|sports|varsity)\b/.test(blob)) return "SPORTS";
  if (/\b(weekend|travel team)\b/.test(blob)) return "WEEKEND";
  if (/\b(corporate|summit|offsite|leadership|user conference)\b/.test(blob)) return "CORPORATE";
  if (/\b(association|society|annual meeting|conference)\b/.test(blob)) return "ASSOCIATION";
  return "OTHER";
}

function whoState(o) {
  const name = o.primaryContact?.name || o.contactName || o.whoName || null;
  const pathClass = o.contactPathClass || o.contactResearchState || null;
  if (name && String(name).trim() && !/unknown|n\/a|no_contact/i.test(name)) {
    return { state: "NAMED", name, title: o.primaryContact?.title || o.contactTitle || null, pathClass };
  }
  if (pathClass) return { state: "FUNCTIONAL", name: null, title: null, pathClass };
  return { state: "CEILING_OR_MISSING", name: null, title: null, pathClass: null };
}

function classifyRow(o, ready, visible) {
  const state = String(o.customerFacingState || o.state || "").toUpperCase();
  const priority = String(o.priority || "").toUpperCase();
  let classLabel = "STILL_VALID";
  if (/REJECT|DISQUAL/i.test(state) || /DISQUAL/i.test(priority)) classLabel = "CLOSED_OR_REJECTED";
  else if (/FUTURE_WATCH/i.test(state)) classLabel = "FUTURE_WATCH";
  else if (ready) classLabel = "STRICT_READY";
  else if (/WATCH/i.test(state) && visible) classLabel = "VISIBLE_WATCH";
  else if (/HELD|INTERNAL/i.test(state)) classLabel = "HELD";
  else if (!visible) classLabel = "LOW_VALUE_OR_HELD";
  return classLabel;
}

function primaryBlocker(o, readyGate) {
  if (readyGate?.ok) return null;
  const reasons = readyGate?.reasons || readyGate?.failedChecks || [];
  if (Array.isArray(reasons) && reasons.length) return reasons.slice(0, 3).join("; ");
  return o.holdReason || o.blocker || o.primaryBlocker || "not_ready";
}

async function phase0() {
  const cfg = loadHotelDemandConfig(HOTEL);
  const doc = await loadOpportunitiesCanonical(HOTEL);
  const opps = doc.opportunities || [];
  const enriched = opps.map((o) => applyLiveCommercialQuality(o, { nowDate: NOW }));
  const salesperson = filterSalespersonView(enriched);
  const visibleList = filterCustomerFacingOpportunities(salesperson, { nowDate: NOW });
  const visibleIds = new Set(visibleList.map(oppId));
  const readyList = visibleList.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok);
  const readyIds = new Set(readyList.map(oppId));

  const corpus = [];
  const segmentCounts = {};
  const classCounts = {};
  const priorityCounts = {};
  const summaryQ = {};
  const whoCounts = { NAMED: 0, FUNCTIONAL: 0, CEILING_OR_MISSING: 0 };
  const cycleKeys = new Map();

  for (const o of enriched) {
    const id = oppId(o);
    const readyGate = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
    const ready = readyIds.has(id);
    const visible = visibleIds.has(id);
    const seg = segmentOf(o);
    const who = whoState(o);
    const classLabel = classifyRow(o, ready, visible);
    segmentCounts[seg] = (segmentCounts[seg] || 0) + 1;
    classCounts[classLabel] = (classCounts[classLabel] || 0) + 1;
    priorityCounts[o.priority || "UNKNOWN"] = (priorityCounts[o.priority || "UNKNOWN"] || 0) + 1;
    const sq = o.summaryQuality || o.customerReadiness?.summaryQuality || "UNKNOWN";
    summaryQ[sq] = (summaryQ[sq] || 0) + 1;
    whoCounts[who.state] += 1;

    const cycleKey = [
      String(o.organizationName || o.org || "").toLowerCase().slice(0, 40),
      String(o.eventName || o.title || "").toLowerCase().slice(0, 40),
      String(o.eventYear || o.eventStartDate || "").slice(0, 10),
    ].join("|");
    if (!cycleKeys.has(cycleKey)) cycleKeys.set(cycleKey, []);
    cycleKeys.get(cycleKey).push(id);

    corpus.push({
      opportunityId: id,
      event: o.eventName || o.title || null,
      organization: o.organizationName || o.org || null,
      cycle: o.eventYear || o.eventCycleId || null,
      dates: o.eventStartDate || o.dateHint || o.dates || null,
      status: o.customerFacingState || o.state || null,
      commercialStatus: o.commercialStatus || o.placementStatus || null,
      destination: o.destinationStatus || o.destination || o.venueStatus || null,
      strictReady: ready,
      visible,
      priority: o.priority || null,
      who: who.name,
      whoTitle: who.title,
      whoState: who.state,
      summaryQuality: sq,
      hotelFitScore: o.hotelFitScore ?? o.fitScore ?? null,
      segment: seg,
      classLabel,
      primaryBlocker: primaryBlocker(o, readyGate),
      lodgingEvidence: o.lodgingEvidence?.status || o.lodgingEvidence || null,
      actionPath: o.recommendedAction || o.recommendedNextStep || null,
      peakRooms: o.estimatedPeakRooms || o.peakRooms || o.roomDemand?.estimatedPeakRooms || null,
      attendance: o.estimatedAttendance || o.attendance || null,
      marketOpportunityId: o.marketOpportunityId || null,
      eventSeriesId: o.eventSeriesId || null,
      officialSource: o.officialSource || o.discoverySource || o.sources?.[0]?.url || null,
    });
  }

  const duplicates = [...cycleKeys.entries()]
    .filter(([, ids]) => ids.length > 1)
    .map(([key, ids]) => ({ key, ids }));

  const snapshot = {
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL,
    hotelName: cfg?.displayName || "Bethesda Marriott",
    rooms: cfg?.capabilityProfile?.totalGuestrooms ?? null,
    meetingSqFt: cfg?.capabilityProfile?.totalMeetingSpaceSqFt ?? null,
    persistence: doc.persistence || doc.source || null,
    totals: {
      all: opps.length,
      strictReady: readyList.length,
      visible: visibleList.length,
      watch: enriched.filter((o) => /WATCH/i.test(String(o.customerFacingState || o.state || ""))).length,
      futureWatch: enriched.filter((o) =>
        /FUTURE_WATCH/i.test(String(o.customerFacingState || o.state || ""))
      ).length,
      held: enriched.filter((o) =>
        /HELD|INTERNAL/i.test(String(o.customerFacingState || o.state || ""))
      ).length,
      rejected: enriched.filter((o) =>
        /REJECT|DISQUAL/i.test(String(o.customerFacingState || o.priority || ""))
      ).length,
    },
    segmentCounts,
    classCounts,
    priorityCounts,
    summaryQuality: summaryQ,
    whoCounts,
    duplicateCycleGroups: duplicates.length,
    duplicates: duplicates.slice(0, 40),
  };

  writeJson("PHASE0_SNAPSHOT.json", snapshot);
  writeJson("CURRENT_BETHESDA_CORPUS.json", corpus);
  writeJson(
    "READY_CORPUS.json",
    corpus.filter((r) => r.strictReady)
  );

  console.log(JSON.stringify(snapshot.totals, null, 2));
  console.log("segments", segmentCounts);
  console.log("who", whoCounts);
  console.log("classes", classCounts);
  console.log("duplicates", duplicates.length);
  return { snapshot, corpus, readyList, visibleList, enriched, doc, cfg };
}

const args = process.argv.slice(2);
if (args.includes("--phase0") || args.length === 0) {
  phase0().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}

export { phase0, HOTEL, OUT, NOW, segmentOf, whoState, writeJson, oppId };
