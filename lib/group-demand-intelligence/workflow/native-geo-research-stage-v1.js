/**
 * Native blind + geographic expansion research stage for the GDI coordinator.
 * Default: plan / dry-run only (no SerpAPI / OpenAI spend).
 * Paid native execution requires explicit authorizeNative=true.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runNativeBlindDiscovery } from "../native-blind-discovery.js";
import { buildGdiDiscoveryQueries } from "../discovery/gdi-discovery-queries-v1.js";

export const NATIVE_GEO_RESEARCH_STAGE_VERSION = "gdi-native-geo-research-stage-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadHotelConfig(hotelId) {
  const p = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${String(hotelId || "").trim()}.json`
  );
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Build geographic expansion query plan from hotel territory config (no paid calls).
 */
export function planGeographicExpansion(hotelId, opts = {}) {
  const id = text(hotelId);
  const cfg = opts.hotelConfig || loadHotelConfig(id);
  const territory = cfg?.demandTerritory || {};
  const includes = Array.isArray(territory.includes) ? territory.includes : [];
  const corridors = Array.isArray(territory.corridors) ? territory.corridors : [];
  const places = [
    cfg?.city,
    territory.label,
    ...includes.slice(0, 6).map((line) =>
      String(line)
        .replace(/^(PRIMARY|SECONDARY):\s*/i, "")
        .split(/[,/]/)[0]
        .trim()
    ),
    ...corridors.slice(0, 4),
  ]
    .map((p) => text(p))
    .filter(Boolean);

  const uniquePlaces = [...new Set(places)].slice(0, 8);
  const queries = [];
  for (const place of uniquePlaces) {
    queries.push({
      family: "geographic_expansion",
      place,
      query: `${place} association conference hotel room block 2026 OR 2027`,
    });
    queries.push({
      family: "geographic_expansion",
      place,
      query: `${place} corporate offsite lodging RFP OR housing`,
    });
  }

  return {
    version: NATIVE_GEO_RESEARCH_STAGE_VERSION,
    hotelId: id,
    mode: "plan_only",
    territoryLabel: territory.label || null,
    placeCount: uniquePlaces.length,
    places: uniquePlaces,
    queryCount: queries.length,
    queries: queries.slice(0, opts.maxQueries || 16),
    paidCalls: 0,
    executed: false,
    note: "Geographic expansion planned from territory config — not auto-paid; do not claim no demand from unused expansion",
  };
}

/**
 * Run native + geographic research stage.
 * @param {object} opts
 * @param {boolean} [opts.authorizeNative=false] — when true and keys present, may call Serp/OpenAI
 * @param {boolean} [opts.dryRun=true] — default dry-run (plan tasks, zero paid)
 * @param {string[]} [opts.feedbackQueryHints] — from feedback→next-research loop
 */
export async function runNativeGeoResearchStage(hotelId, opts = {}) {
  const id = text(hotelId);
  const cfg = opts.hotelConfig || loadHotelConfig(id);
  const authorizeNative = opts.authorizeNative === true;
  const dryRun = opts.dryRun !== false && !authorizeNative;
  const startedAt = new Date().toISOString();

  const geo = planGeographicExpansion(id, {
    hotelConfig: cfg,
    maxQueries: opts.maxGeoQueries || 16,
  });

  // Fold feedback-driven hints into geo plan (selection, not inventing prospects)
  const feedbackHints = Array.isArray(opts.feedbackQueryHints)
    ? opts.feedbackQueryHints.filter(Boolean).slice(0, 8)
    : [];
  for (const hint of feedbackHints) {
    geo.queries.push({
      family: "feedback_driven",
      place: null,
      query: hint,
    });
  }
  geo.queryCount = geo.queries.length;
  geo.feedbackHintCount = feedbackHints.length;

  let native = null;
  let nativeStatus = "planned_dry_run";
  let nativePaid = 0;
  let nativeError = null;

  try {
    if (authorizeNative && !dryRun) {
      native = await runNativeBlindDiscovery({
        hotelId: id,
        config: cfg,
        dryRun: false,
        maxQueries: opts.maxNativeQueries || 6,
        maxPagesPerQuery: 1,
        maxExtractBatches: 2,
      });
      nativeStatus = "executed_live";
      nativePaid =
        (native?.ledger?.serpCharged || 0) + (native?.ledger?.openaiCalls || 0);
    } else {
      native = await runNativeBlindDiscovery({
        hotelId: id,
        config: cfg,
        dryRun: true,
        maxQueries: opts.maxNativeQueries || 10,
      });
      // Enrich dry-run with explicit query families for path evidence
      const qdoc = buildGdiDiscoveryQueries({
        hotelId: id,
        hotelProfile: cfg || {},
        maxPerBase: 4,
      });
      native = {
        ...native,
        discoveryQueryFamilies: qdoc?.queries?.slice?.(0, 12) || qdoc || [],
      };
      nativeStatus = "planned_dry_run";
      nativePaid = 0;
    }
  } catch (err) {
    nativeError = err?.message || String(err);
    nativeStatus =
      /SERPAPI|OPENAI/i.test(nativeError) && authorizeNative
        ? "blocked_missing_keys"
        : "failed";
    native = {
      hotelId: id,
      mode: dryRun ? "DRY_RUN" : "LIVE",
      candidates: [],
      tasks: [],
      error: nativeError,
    };
  }

  const finishedAt = new Date().toISOString();
  return {
    version: NATIVE_GEO_RESEARCH_STAGE_VERSION,
    hotelId: id,
    startedAt,
    finishedAt,
    authorizeNative,
    dryRun,
    paidCalls: nativePaid,
    geographic: geo,
    native: {
      status: nativeStatus,
      mode: native?.mode || null,
      taskCount: (native?.tasks || []).length,
      candidateCount: (native?.candidates || []).length,
      ledger: native?.ledger || null,
      error: nativeError,
      discoveryQueryFamilies: native?.discoveryQueryFamilies || null,
    },
    pathEvidence: {
      geographicExecuted: geo.queryCount > 0,
      geographicMode: "plan_only",
      nativeExecuted: nativeStatus === "executed_live" || nativeStatus === "planned_dry_run",
      nativeMode: nativeStatus,
      contactNotInThisStage: true,
    },
    notes: [
      dryRun || !authorizeNative
        ? "Native/geo stage ran as plan/dry-run — zero paid Serp/OpenAI calls"
        : "Native live execution authorized",
      "Geographic expansion from territory is planned even when live search is not authorized",
      "Feedback query hints are selection inputs, not fabricated prospects",
    ],
  };
}

export function writeNativeGeoArtifacts(stageDoc, outDir) {
  if (!outDir) throw new Error("outDir_required");
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "native-geo-research-stage.json"),
    JSON.stringify(stageDoc, null, 2) + "\n"
  );
  const geo = stageDoc.geographic || {};
  const lines = ["family,place,query"];
  for (const q of geo.queries || []) {
    lines.push(
      [
        q.family || "",
        `"${String(q.place || "").replace(/"/g, '""')}"`,
        `"${String(q.query || "").replace(/"/g, '""')}"`,
      ].join(",")
    );
  }
  fs.writeFileSync(
    path.join(outDir, "geographic-expansion-plan.csv"),
    lines.join("\n") + "\n"
  );
  return {
    stagePath: path.join(outDir, "native-geo-research-stage.json"),
    geoCsvPath: path.join(outDir, "geographic-expansion-plan.csv"),
  };
}
