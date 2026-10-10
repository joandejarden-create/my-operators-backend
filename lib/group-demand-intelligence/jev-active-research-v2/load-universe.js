/**
 * Build deduplicated active research universe from demand-engine V1 + V3/completion leftovers.
 */

import fs from "node:fs";
import path from "node:path";
import { classifyDemandEngine } from "../demand-engine-v1/classify-engine.js";

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
  if (u) return `${hotel}|url:${u}`;
  const t = String(title || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .slice(0, 80);
  if (id) return `${hotel}|id:${id}`;
  return `${hotel}|t:${t}`;
}

function engineCode(raw, title, org, scout) {
  if (raw && typeof raw === "string" && !raw.includes("[object")) return raw;
  const eng = classifyDemandEngine({
    title,
    organizationName: org,
    discoveryMeta: { scoutFamily: scout },
  });
  return eng?.demandEngine || "ASSOCIATION_NGO";
}

/**
 * @param {{ demandEngineDir: string, v3Dir?: string, completionDir?: string, hotels: object[] }} opts
 */
export function loadActiveResearchUniverse(opts = {}) {
  const { demandEngineDir, completionDir, hotels = [] } = opts;
  const byKey = Object.fromEntries(hotels.map((h) => [h.hotelKey, h]));
  const seen = new Set();
  const items = [];
  const tallies = {
    signals: 0,
    rotation: 0,
    preRfp: 0,
    competitive: 0,
    v3Unresolved: 0,
    duplicatesRemoved: 0,
  };

  const push = (row) => {
    const hotelKey = row.hotelKey;
    const h = byKey[hotelKey];
    if (!h) return;
    const key = dedupeKey(hotelKey, row.source, row.eventProgram || row.title, row.externalId);
    if (seen.has(key)) {
      tallies.duplicatesRemoved += 1;
      return;
    }
    seen.add(key);
    items.push({
      researchId: `jar_${hotelKey}_${items.length + 1}_${(row.externalId || key).toString().replace(/\W+/g, "_").slice(0, 40)}`,
      hotelId: h.hotelId,
      hotelKey,
      marketId: h.marketId,
      demandEngine: row.demandEngine,
      subsegment: row.subsegment || "unknown",
      signalType: row.signalType,
      organization: row.organization || null,
      eventProgram: row.eventProgram || row.title || null,
      title: row.title || row.eventProgram || row.organization,
      source: row.source || null,
      language: row.language || "en",
      feederMarket: row.feederMarket || null,
      timingState: row.timingState || "FUTURE_UNCONFIRMED",
      lodgingState: row.lodgingState || "UNKNOWN",
      whoState: row.whoState || "UNKNOWN",
      fitState: row.fitState || "UNKNOWN",
      placementState: row.placementState || "UNKNOWN",
      existingEvidence: row.existingEvidence || {},
      currentBlockers: row.currentBlockers || ["TIMING", "LODGING", "WHO"],
      estimatedCommercialPotential: row.estimatedCommercialPotential ?? 0,
      priorResearchSteps: row.priorResearchSteps || 0,
      priorCost: row.priorCost || 0,
      opportunityId: row.opportunityId || null,
      patternId: row.patternId || null,
      rotates: row.rotates === true || row.rotates === "true",
      preRfp: row.preRfp === true,
      nextResearchTrigger: row.nextResearchTrigger || null,
      geoTokens: h.geoTokens || [],
      placeNames: h.placeNames || [],
      hotelLabel: h.label,
      defaultFitScore: h.defaultFitScore ?? 48,
      serpGl: h.serpGl || "us",
    });
  };

  // Signals
  const sigPath = path.join(demandEngineDir, "SIGNAL_DISCOVERY_RESULTS.csv");
  if (fs.existsSync(sigPath)) {
    for (const r of parseCsv(fs.readFileSync(sigPath, "utf8"))) {
      tallies.signals += 1;
      push({
        hotelKey: r.hotel,
        externalId: r.signalId,
        signalType: "DEMAND_SIGNAL",
        demandEngine: engineCode(r.demandEngine, r.title, r.organizationName, r.sourceFamily),
        title: r.title,
        organization: r.organizationName,
        eventProgram: r.title,
        source: r.url,
        language: r.queryLanguage || "en",
        feederMarket: r.originMarket || null,
        lodgingState: /housing|accommodation|hotel|hébergement|alojamiento/i.test(
          `${r.title} ${r.url}`
        )
          ? "HINT"
          : "UNKNOWN",
        currentBlockers: ["TIMING", "LODGING", "WHO", "SURFACE"],
        estimatedCommercialPotential: 3,
      });
    }
  }

  // Rotation series
  const rotPath = path.join(demandEngineDir, "ROTATION_INTELLIGENCE.csv");
  if (fs.existsSync(rotPath)) {
    for (const r of parseCsv(fs.readFileSync(rotPath, "utf8"))) {
      tallies.rotation += 1;
      push({
        hotelKey: r.hotel,
        externalId: r.opportunityId || r.eventSeriesId,
        opportunityId: r.opportunityId,
        signalType: "ROTATION_SERIES",
        demandEngine: "ASSOCIATION_NGO",
        title: r.organization || r.eventSeriesId,
        organization: r.organization,
        eventProgram: r.eventSeriesId,
        source: null,
        language: "en",
        timingState: r.timingState || "FUTURE_UNCONFIRMED",
        rotates: r.rotates,
        nextResearchTrigger: r.nextResearchTrigger,
        currentBlockers: ["TIMING", "LODGING", "WHO"],
        estimatedCommercialPotential: 5,
        existingEvidence: { nextKnownCycle: r.nextKnownCycle || null, seriesId: r.eventSeriesId },
      });
    }
  }

  // Pre-RFP
  const prePath = path.join(demandEngineDir, "PRE_RFP_WATCH.csv");
  if (fs.existsSync(prePath)) {
    for (const r of parseCsv(fs.readFileSync(prePath, "utf8"))) {
      tallies.preRfp += 1;
      push({
        hotelKey: r.hotel,
        externalId: `prerfp_${r.opportunityId}`,
        opportunityId: r.opportunityId,
        signalType: "PRE_RFP",
        demandEngine: /PROCUREMENT|RFP|tender/i.test(r.trigger || "")
          ? "PROCUREMENT_RFP"
          : "ASSOCIATION_NGO",
        title: r.organization,
        organization: r.organization,
        eventProgram: r.organization,
        source: null,
        language: "en",
        timingState: r.timingState || "FUTURE_UNCONFIRMED",
        preRfp: true,
        nextResearchTrigger: r.trigger,
        currentBlockers: ["TIMING", "LODGING", "WHO"],
        estimatedCommercialPotential: 6,
        existingEvidence: { rationale: r.rationale },
      });
    }
  }

  // Competitive
  const cdpPath = path.join(demandEngineDir, "COMPETITIVE_DEMAND_PATTERNS.csv");
  if (fs.existsSync(cdpPath)) {
    for (const r of parseCsv(fs.readFileSync(cdpPath, "utf8"))) {
      tallies.competitive += 1;
      push({
        hotelKey: r.hotel,
        externalId: r.patternId,
        patternId: r.patternId,
        signalType: "COMPETITIVE_PATTERN",
        demandEngine: engineCode(r.demandEngine, r.organization, r.organization, null),
        title: r.organization,
        organization: r.organization,
        eventProgram: r.organization,
        source: null,
        language: "en",
        lodgingState: r.lodgingPattern || "UNKNOWN",
        fitState: r.targetHotelFit || "UNKNOWN",
        currentBlockers: ["LODGING", "TIMING", "PLACEMENT"],
        estimatedCommercialPotential: r.couldCompeteNextCycle === "true" ? 5 : 2,
        existingEvidence: {
          historicHotels: r.historicHotels,
          couldCompeteNextCycle: r.couldCompeteNextCycle,
        },
      });
    }
  }

  // V3 / completion unresolved — only P0/P1 not already depth-exhausted
  if (completionDir) {
    const fin = path.join(completionDir, "FINAL_CLASSIFICATION.csv");
    const pagePath = path.join(completionDir, "PAGE_LEVEL_RESEARCH.csv");
    const urlByOpp = {};
    if (fs.existsSync(pagePath)) {
      for (const r of parseCsv(fs.readFileSync(pagePath, "utf8"))) {
        if (r.opportunityId && r.url && !urlByOpp[r.opportunityId]) {
          urlByOpp[r.opportunityId] = r.url;
        }
      }
    }
    if (fs.existsSync(fin)) {
      const prioPath = path.join(completionDir, "COMPLETION_PRIORITY_QUEUE.csv");
      const prio = fs.existsSync(prioPath)
        ? Object.fromEntries(
            parseCsv(fs.readFileSync(prioPath, "utf8")).map((r) => [r.opportunityId, r])
          )
        : {};
      for (const r of parseCsv(fs.readFileSync(fin, "utf8"))) {
        if (r.useful === "true") continue;
        if (r.researched === "true" && Number(r.steps || 0) >= 2) continue;
        const p = prio[r.opportunityId] || {};
        const band = String(p.priority || r.priority || "");
        if (!/P0_|P1_/i.test(band)) continue;
        const url = urlByOpp[r.opportunityId] || null;
        if (!url && !r.title) continue;
        tallies.v3Unresolved += 1;
        push({
          hotelKey: r.hotel,
          externalId: r.opportunityId,
          signalType: "V3_UNRESOLVED",
          demandEngine: engineCode(
            r.demandEngine || p.demandEngine,
            r.title,
            r.title,
            p.scoutFamily
          ),
          title: r.title,
          organization: r.title,
          eventProgram: r.title,
          source: url,
          language: p.language || "en",
          priorResearchSteps: Number(r.steps || 0),
          priorCost: Number(r.cost || 0),
          currentBlockers: ["TIMING", "LODGING", "WHO"],
          estimatedCommercialPotential: Number(p.score || 4),
        });
      }
    }
  }

  // Enrich rotation/pre-RFP without source: leave source null — research loop may SERP
  return { items, tallies, universeSize: items.length };
}
