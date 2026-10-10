/**
 * Reconstruct V3 discovery candidates from scout RESULT CSVs + series rows.
 * AssociationScout detail rows were not persisted in V3 report CSVs — series + SCOUT_YIELD tallies noted.
 */

import fs from "node:fs";
import path from "node:path";
import { classifyDemandEngine } from "../demand-engine-v1/classify-engine.js";

const CANDIDATE_CSV_FILES = [
  "ASSOCIATION_RESULTS.csv",
  "PROCUREMENT_RESULTS.csv",
  "CORPORATE_TRIGGER_RESULTS.csv",
  "PROJECT_WORKFORCE_RESULTS.csv",
  "MEDICAL_RESEARCH_RESULTS.csv",
  "SPORTS_HOUSING_RESULTS.csv",
  "UNIVERSITY_RESULTS.csv",
  "TOUR_DMC_RESULTS.csv",
  "HIDDEN_DEMAND_RESULTS.csv",
];

function parseCsv(text) {
  const lines = String(text || "").replace(/^\uFEFF/, "").split(/\r?\n/).filter(Boolean);
  if (!lines.length) return [];
  const headers = splitCsvLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = splitCsvLine(lines[i]);
    if (!cols.length) continue;
    const obj = {};
    headers.forEach((h, idx) => {
      obj[h] = cols[idx] ?? "";
    });
    rows.push(obj);
  }
  return rows;
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

function rowToCandidate(row, hotelCtx) {
  const title = String(row.title || "").trim();
  const url = String(row.officialSource || row.sourceUrl || row.source || "").trim();
  if (!title && !url) return null;
  const id = String(row.opportunityId || "").trim() || `gdi_v3_recon_${hotelCtx.hotelKey}_${iHash(title + url)}`;
  const org = String(row.organizationName || row.organization || "").trim() || title.split(/[|\-—:]/)[0].trim();
  const scoutFamily = String(row.scoutFamily || "ReconstructedScout").trim();
  const queryLanguage = String(row.queryLanguage || "en").trim() || "en";
  const lodgingHint =
    /\b(room block|host hotel|housing bureau|official housing|accommodation|hébergement|alojamiento|hotel block|preferred hotel|overflow hotel|prestations hôtelières)\b/i.test(
      `${title} ${url}`
    ) || /\/(accommodation|housing|hotels?|hébergement|alojamiento)\b/i.test(url);

  const cand = {
    id,
    title: title || org || id,
    organizationName: org,
    opportunityName: title || org,
    opportunityType: /Procurement|RFP|tender|appel/i.test(`${scoutFamily} ${title}`)
      ? "PRIMARY_PURSUIT"
      : lodgingHint
        ? "OVERFLOW_HOUSING"
        : "FUTURE_CYCLE",
    officialSource: url || null,
    discoverySource: url || null,
    sources: url ? [{ url, kind: "discovery_expansion_v3_reconstructed", scout: scoutFamily }] : [],
    summaryWhat: title.slice(0, 280),
    hotelOpportunityThesis: lodgingHint
      ? `${hotelCtx.label || hotelCtx.hotelKey} can pursue overflow / preferred lodging related to ${title}.`
      : `${title} may generate group lodging demand for ${hotelCtx.label || hotelCtx.hotelKey} — requires lodging confirmation.`,
    whyNow: "V3 discovery candidate — page-level completion required before promotion.",
    recommendedAction: "Validate official source, lodging, timing, and contact path.",
    summaryWhyMatters: "Public discovery signal pending page-level validation.",
    summaryWhyHotel: hotelCtx.fitLine || `${hotelCtx.label}: market-fit pending validation.`,
    hotelFitScore: hotelCtx.defaultFitScore ?? 48,
    venueStatus: "Unknown",
    eventLocationSummary: hotelCtx.placeNames?.[0] || null,
    destinationStatus: hotelCtx.placeNames?.[0] || null,
    discoveryMeta: {
      scoutFamily,
      queryLanguage,
      marketPlace: hotelCtx.placeNames?.[0] || null,
      feederMarket: row.originMarket || null,
      v3: true,
      reconstructed: true,
    },
    queryLanguage,
    sourceLanguage: queryLanguage,
    eventYear: (title.match(/\b(202[6-9]|203[0-2])\b/) || [])[1] || null,
    eventStartDate: null,
    lodgingEvidence: lodgingHint
      ? {
          housingPageFound: /accommodation|housing|hébergement|alojamiento/i.test(url),
          roomBlockMentioned: /room.?block|host.?hotel/i.test(title),
          status: "WEAK",
        }
      : null,
    hotelId: hotelCtx.hotelId,
    hotelKey: hotelCtx.hotelKey,
    gdiDiscoveryVersion: "discovery_expansion_v3",
    gdiCompletionSource: "v3_csv_reconstructed",
  };
  const eng = classifyDemandEngine(cand);
  cand.demandEngine = eng?.demandEngine || eng || null;
  cand.demandEngineMeta = eng;
  return cand;
}

function iHash(s) {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return h.toString(16).slice(0, 8);
}

/**
 * @param {string} v3ReportDir
 * @param {object[]} hotels
 */
export function loadV3CandidatesFromReports(v3ReportDir, hotels = []) {
  const byHotel = Object.fromEntries(hotels.map((h) => [h.hotelKey, h]));
  const seen = new Set();
  const candidates = [];
  const meta = {
    v3OfficialTotal: 156,
    associationScoutMissingFromCsv: true,
    associationScoutYieldFromScoutCsv: 0,
    reconstructedFromDetailCsv: 0,
    seriesAugmented: 0,
  };

  // SCOUT_YIELD Association tallies
  const scoutYieldPath = path.join(v3ReportDir, "SCOUT_YIELD.csv");
  if (fs.existsSync(scoutYieldPath)) {
    for (const r of parseCsv(fs.readFileSync(scoutYieldPath, "utf8"))) {
      if (/AssociationScout/i.test(r.scoutFamily || "")) {
        meta.associationScoutYieldFromScoutCsv += Number(r.candidates || 0) || 0;
      }
    }
  }

  for (const file of CANDIDATE_CSV_FILES) {
    const fp = path.join(v3ReportDir, file);
    if (!fs.existsSync(fp)) continue;
    for (const row of parseCsv(fs.readFileSync(fp, "utf8"))) {
      const hotelKey = String(row.hotel || "").trim();
      const h = byHotel[hotelKey];
      if (!h) continue;
      const id = String(row.opportunityId || "").trim();
      if (id && seen.has(id)) continue;
      const cand = rowToCandidate(row, h);
      if (!cand) continue;
      if (cand.id) seen.add(cand.id);
      candidates.push(cand);
      meta.reconstructedFromDetailCsv += 1;
    }
  }

  // Association series rows (partial Association recovery)
  const seriesPath = path.join(v3ReportDir, "ASSOCIATION_SERIES_RESULTS.csv");
  if (fs.existsSync(seriesPath)) {
    for (const row of parseCsv(fs.readFileSync(seriesPath, "utf8"))) {
      const hotelKey = String(row.hotel || "").trim();
      const h = byHotel[hotelKey];
      if (!h) continue;
      const id = String(row.opportunityId || "").trim();
      if (id && seen.has(id)) continue;
      const cand = rowToCandidate(
        {
          ...row,
          title: row.organization || row.eventSeriesId || row.opportunityId,
          organizationName: row.organization,
          officialSource: row.sourceUrl,
          scoutFamily: "AssociationScout",
          queryLanguage: "en",
        },
        h
      );
      if (!cand) continue;
      if (row.timingState) cand.futureCycleEvidenceState = row.timingState;
      if (row.nextKnownCycle && /^\d{4}-\d{2}-\d{2}/.test(row.nextKnownCycle)) {
        cand.eventStartDate = row.nextKnownCycle.slice(0, 10);
      }
      cand.eventSeriesId = row.eventSeriesId || null;
      cand.gdiCompletionSource = "v3_association_series";
      if (cand.id) seen.add(cand.id);
      candidates.push(cand);
      meta.seriesAugmented += 1;
    }
  }

  meta.poolSize = candidates.length;
  meta.note =
    "V3 AssociationScout detail candidates were not written to a RESULTS.csv; pool = detail CSVs + association series. Official V3 total remains 156.";

  return { candidates, meta };
}
