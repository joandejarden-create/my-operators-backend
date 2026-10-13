/**
 * Reclassify existing GDI universe under Complete Demand Packet standard.
 * No Jev. Deduplicate first.
 */

import fs from "node:fs";
import path from "node:path";
import { loadActiveResearchUniverse } from "../jev-active-research-v2/load-universe.js";
import { loadV3CandidatesFromReports } from "../candidate-completion-v2/load-v3-candidates.js";
import {
  evaluateCompleteDemandPacket,
  PACKET_QUALITY,
} from "./packet-schema.js";

function parseCsv(text) {
  const lines = String(text || "")
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/)
    .filter(Boolean);
  if (!lines.length) return [];
  const headers = splitCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cols = splitCsvLine(line);
    const o = {};
    headers.forEach((h, i) => {
      o[h] = cols[i] ?? "";
    });
    return o;
  });
}

function splitCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQ) {
      if (ch === '"' && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else if (ch === '"') inQ = false;
      else cur += ch;
    } else if (ch === '"') inQ = true;
    else if (ch === ",") {
      out.push(cur);
      cur = "";
    } else cur += ch;
  }
  out.push(cur);
  return out;
}

function dedupeKey(hotel, url, title, id) {
  const u = String(url || "")
    .toLowerCase()
    .replace(/\/$/, "")
    .slice(0, 160);
  if (u) return `${hotel}|u:${u}`;
  if (id) return `${hotel}|id:${id}`;
  return `${hotel}|t:${String(title || "")
    .toLowerCase()
    .slice(0, 80)}`;
}

const HOTELS = [
  {
    hotelId: "recrPQcZg7SFARRb2",
    hotelKey: "YOTEL",
    label: "YOTEL Geneva Lake",
    marketId: "geneva_lake",
    placeNames: ["Geneva", "Nyon", "Palexpo"],
    geoTokens: ["geneva", "switzerland", "palexpo"],
    defaultFitScore: 52,
    serpGl: "ch",
  },
  {
    hotelId: "rec2PVBDavppGpenm",
    hotelKey: "AC",
    label: "AC Hotel A Coruña",
    marketId: "a_coruna",
    placeNames: ["A Coruña", "Galicia"],
    geoTokens: ["coruña", "galicia", "spain"],
    defaultFitScore: 50,
    serpGl: "es",
  },
  {
    hotelId: "recKRJjcPnb4tVDDS",
    hotelKey: "SPICE",
    label: "Spice Island Beach Resort",
    marketId: "grenada",
    placeNames: ["Grenada", "Grand Anse"],
    geoTokens: ["grenada"],
    defaultFitScore: 48,
    serpGl: "us",
  },
  {
    hotelId: "recIwaP1etgx2g9nA",
    hotelKey: "CAMBRIDGE",
    label: "Cambridge Beaches Resort & Spa",
    marketId: "bermuda",
    placeNames: ["Bermuda"],
    geoTokens: ["bermuda"],
    defaultFitScore: 48,
    serpGl: "us",
  },
  {
    hotelId: "recGkME49yYuxQl0u",
    hotelKey: "NOW_NOW",
    label: "NOW NOW NOHO",
    marketId: "nyc_noho",
    placeNames: ["New York", "NoHo"],
    geoTokens: ["new york", "nyc", "manhattan"],
    defaultFitScore: 50,
    serpGl: "us",
  },
];

/**
 * Load + dedupe prior artifacts, classify as Complete Demand Packets.
 */
export function reclassifyExistingUniverse(rootDir, opts = {}) {
  const root = rootDir || path.resolve("reports/gdi");
  const demandEngineDir = path.join(root, "demand-engine-jev-controller-v1");
  const v3Dir = path.join(root, "discovery-expansion-v3-2026-10-03");
  const completionDir = path.join(root, "candidate-completion-jev-v2-2026-10-03");
  const v4Dir = path.join(root, "discovery-quality-persistence-v4");
  const compDir = path.join(root, "comp-set-demand-mining-v1");
  const v6Dir = path.join(root, "demonstrated-demand-discovery-v6");

  const seen = new Set();
  const records = [];
  const push = (row) => {
    const hotelKey = row.hotelKey;
    if (!hotelKey) return;
    const key = dedupeKey(
      hotelKey,
      row.officialSource || row.source || row.url,
      row.title || row.eventProgram,
      row.id || row.researchId || row.traceId || row.opportunityId
    );
    if (seen.has(key)) {
      records.push({ ...row, _duplicate: true, _dedupeKey: key });
      return;
    }
    seen.add(key);
    records.push({ ...row, _duplicate: false, _dedupeKey: key });
  };

  // Active research universe (signals, rotation, pre-rfp, competitive)
  if (fs.existsSync(demandEngineDir)) {
    const uni = loadActiveResearchUniverse({
      demandEngineDir,
      completionDir: fs.existsSync(completionDir) ? completionDir : undefined,
      hotels: HOTELS,
    });
    for (const item of uni.items || []) {
      push({
        ...item,
        id: item.researchId,
        organizationName: item.organization,
        officialSource: item.source,
        artifact: "DEMAND_ENGINE_UNIVERSE",
      });
    }
  }

  // V3 candidates
  if (fs.existsSync(v3Dir)) {
    try {
      const v3 = loadV3CandidatesFromReports(v3Dir, HOTELS);
      const list = Array.isArray(v3) ? v3 : v3.candidates || [];
      for (const c of list) {
        if (!c) continue;
        push({
          ...c,
          hotelKey: c.hotelKey || c.discoveryMeta?.hotelKey,
          artifact: "V3_CANDIDATE",
        });
      }
    } catch {
      /* ignore reconstruct errors */
    }
  }

  // V4 association recovery + signal classification
  const v4Sig = path.join(v4Dir, "SIGNAL_CANDIDATE_CLASSIFICATION.csv");
  if (fs.existsSync(v4Sig)) {
    for (const r of parseCsv(fs.readFileSync(v4Sig, "utf8"))) {
      push({
        hotelKey: r.hotel || r.hotelKey,
        id: r.opportunityId || r.id,
        title: r.title,
        organizationName: r.organization || r.organizationName,
        officialSource: r.source || r.url,
        signalType: r.admissionClass || r.signalType || "V4_SIGNAL",
        artifact: "V4_CLASSIFICATION",
      });
    }
  }
  const v4Assoc = path.join(v4Dir, "ASSOCIATION_RECOVERY.csv");
  if (fs.existsSync(v4Assoc)) {
    for (const r of parseCsv(fs.readFileSync(v4Assoc, "utf8"))) {
      push({
        hotelKey: r.hotel || r.hotelKey,
        id: r.opportunityId || r.id,
        title: r.title,
        organizationName: r.organizationName || r.organization,
        officialSource: r.officialSource || r.source,
        signalType: "ASSOCIATION_RECOVERY",
        artifact: "V4_ASSOCIATION_RECOVERY",
      });
    }
  }

  // Comp traces
  const compTraces = path.join(compDir, "COMPETITOR_DEMAND_TRACES.csv");
  if (fs.existsSync(compTraces)) {
    for (const r of parseCsv(fs.readFileSync(compTraces, "utf8"))) {
      push({
        hotelKey: r.targetHotelKey,
        id: r.traceId,
        title: r.eventProgram,
        organizationName: r.organization,
        officialSource: r.source,
        competitorHotel: r.competitorHotel,
        evidenceClass: r.evidenceClass,
        eventYear: r.eventYear,
        demandEngine: r.demandEngine,
        groupType: r.groupType,
        feedsDeeperResearch: r.feedsDeeperResearch === "true",
        lodgingEvidence:
          r.evidenceClass === "DIRECT_CONFIRMED" || r.evidenceClass === "STRONG_ASSOCIATION"
            ? { housingPageFound: true, status: "WEAK" }
            : null,
        signalType: "COMP_TRACE",
        artifact: "COMP_SET_V1",
      });
    }
  }

  // V6 demonstrated
  const v6Final = path.join(v6Dir, "FINAL_GDI_OPPORTUNITIES.csv");
  if (fs.existsSync(v6Final)) {
    for (const r of parseCsv(fs.readFileSync(v6Final, "utf8"))) {
      push({
        hotelKey: r.hotelKey,
        id: r.id,
        title: r.title,
        organizationName: r.organization,
        officialSource: r.source,
        competitorHotel: r.competitorHotel,
        evidenceClass: r.evidenceClass,
        hotelFitScore: Number(r.fit || 48),
        signalType: "V6_DEMONSTRATED",
        artifact: "V6",
      });
    }
  }

  // Classify
  const classified = [];
  const rotationReclass = [];
  const tallies = {
    total: 0,
    duplicates: 0,
    SIGNAL_ONLY: 0,
    PARTIAL_PACKET: 0,
    COMPLETE_STRONG: 0,
    COMPLETE_PLAUSIBLE: 0,
    REJECTED: 0,
    DUPLICATE: 0,
    rotationTotal: 0,
    rotationDowngraded: 0,
    rotationComplete: 0,
  };

  for (const r of records) {
    tallies.total += 1;
    const hotel = HOTELS.find((h) => h.hotelKey === r.hotelKey);
    if (r._duplicate) {
      tallies.duplicates += 1;
      tallies.DUPLICATE += 1;
      classified.push({
        id: r.id || r.researchId,
        hotelKey: r.hotelKey,
        title: r.title || r.eventProgram,
        organization: r.organizationName || r.organization,
        artifact: r.artifact,
        signalType: r.signalType,
        quality: PACKET_QUALITY.DUPLICATE,
        source: r.officialSource || r.source,
      });
      continue;
    }

    const ev = evaluateCompleteDemandPacket(r, {
      defaultFitScore: hotel?.defaultFitScore ?? 48,
      geoOk: true,
    });
    tallies[ev.quality] = (tallies[ev.quality] || 0) + 1;

    const row = {
      id: r.id || r.researchId || r.traceId,
      hotelKey: r.hotelKey,
      title: r.title || r.eventProgram,
      organization: r.organizationName || r.organization,
      artifact: r.artifact,
      signalType: r.signalType,
      quality: ev.quality,
      strongCount: ev.strongCount,
      presentOrStrongCount: ev.presentOrStrongCount,
      missingPillars: (ev.missingPillars || []).join("|"),
      pillarA: ev.pillars?.A_NAMED_DEMAND_ENTITY?.strength,
      pillarB: ev.pillars?.B_DEFINED_GROUP_MOTION?.strength,
      pillarC: ev.pillars?.C_BUYER_ORGANIZER_PATH?.strength,
      pillarD: ev.pillars?.D_FUTURE_DECISION_POINT?.strength,
      pillarE: ev.pillars?.E_HOTEL_LODGING_EVIDENCE?.strength,
      pillarF: ev.pillars?.F_TARGET_HOTEL_FIT?.strength,
      source: r.officialSource || r.source,
      competitorHotel: r.competitorHotel || "",
      demandEngine: r.demandEngine || "",
    };
    classified.push(row);

    if (/ROTATION/i.test(String(r.signalType || ""))) {
      tallies.rotationTotal += 1;
      const downgraded =
        ev.quality !== PACKET_QUALITY.COMPLETE_STRONG &&
        ev.quality !== PACKET_QUALITY.COMPLETE_PLAUSIBLE;
      if (downgraded) tallies.rotationDowngraded += 1;
      else tallies.rotationComplete += 1;
      rotationReclass.push({
        ...row,
        rotationIntelligenceOnly: downgraded,
        note: downgraded
          ? "Recurrence without buyer+lodging+future decision → RotationIntelligence not CandidateOpportunity"
          : "Rotation meets complete packet pillars",
      });
    }
  }

  return {
    hotels: HOTELS,
    tallies,
    classified,
    rotationReclass,
    completePackets: classified.filter(
      (c) =>
        c.quality === PACKET_QUALITY.COMPLETE_STRONG ||
        c.quality === PACKET_QUALITY.COMPLETE_PLAUSIBLE
    ),
  };
}

export { HOTELS };
