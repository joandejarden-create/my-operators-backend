/**
 * Full CALA Core Identity Census Orchestrator v1
 *
 * Hybrid:
 *  HBX_SOURCE_LANE (budgeted TEST quota) → pause on Quota exceeded
 *  CORE_IDENTITY_CENSUS_LANE continues (shell apply + SerpAPI HOLD upgrades + 52-geo matrix)
 *
 * Never writes Brand Explorer / Brand Setup / VIC / Rooms / Brand fields.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";

import { resolveHbxConfig, hbxFetchJson, contentUrl } from "./hbx-content-api-client.js";
import {
  extractHbxHotel,
  pullCountryHotels,
} from "./hbx-content-api-cala-wave1-dry-run-v1.js";
import { createHbxRequestRateLimiter } from "./hbx-request-rate-limiter-v1.js";
import {
  listDealalityCalaGeographies,
  normalizeGeographyLabel,
  DEALALITY_CALA_GEOGRAPHY_REGISTRY_VERSION,
} from "./dealality-cala-geography-registry-v1.js";
import {
  HBX_DISCOVERY_STATUS,
  HBX_ALTERNATE_QUERY_CODES,
  initOrLoadLedger,
} from "./full-cala-hbx-geography-discovery-wave-v1.js";
import { rankHbxPriorityGeographies } from "./full-cala-core-identity-hbx-priority-v1.js";
import {
  CENSUS_TABLE_ID,
  MATCH,
  loadMasterUniverseCandidates,
  loadHbxCandidates,
  mergeCandidateUniverses,
  listCensusIndex,
  classifyAgainstCensus,
  classifyShellPreflightQuality,
  SHELL_PREFLIGHT_CLASS,
} from "./full-cala-15k-census-shell-insert-v1.js";
import { runFullCala15kShellOrchestratorV1 } from "./full-cala-15k-shell-orchestrator-v1.js";
import { runFullCalaGeographyCoverageRegistryAuditV1 } from "./full-cala-geography-coverage-registry-audit-v1.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { searchGoogleHotels } from "./providers/serpapi-google-hotels/search.js";
import { SerpApiCreditTracker } from "./providers/serpapi-google-hotels/credit-tracker.js";
import { normName } from "./census-autopilot-v2/identity-dedupe.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const CORE_IDENTITY_OBJECTIVE =
  "full-cala-core-identity-census-orchestrator-v1";
export const CORE_IDENTITY_VERSION =
  "full-cala-core-identity-census-orchestrator-v1";

export const CORE_STATUS = Object.freeze({
  DRY_RUN: "production_census_core_identity_dry_run_ready",
  RUNNING: "production_census_core_identity_running",
  COMPLETE: "production_census_core_identity_complete",
  FOUNDER_STOP: "production_census_core_identity_stop_for_founder_review",
});

const STATE_DIR = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-core-identity-census"
);
const STATE_FILE = path.join(STATE_DIR, "core-identity-state.json");
const CANDIDATES_DIR = path.join(STATE_DIR, "hbx-candidates");
const MERGED_PACK = path.join(
  ROOT,
  "reports/research-engine-v2/hbx-cala-full-geography-candidate-pack.json"
);
const WAVE1_PACK = path.join(
  ROOT,
  "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json"
);
const HOLDS_FILE = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json"
);

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}
function readJson(fp, fallback = null) {
  if (!fs.existsSync(fp)) return fallback;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function queryCodesForGeography(g) {
  const alts = HBX_ALTERNATE_QUERY_CODES[g.geography_id] || [];
  const primary = g.iso_code ? [g.iso_code] : [];
  const out = [];
  for (const c of [...primary, ...alts]) {
    const up = String(c || "").toUpperCase();
    if (up && !out.includes(up)) out.push(up);
  }
  return out;
}

function hotelToCandidate(hotel, geographyName, waveId) {
  return {
    hbx_hotel_code: hotel.hbx_hotel_code,
    name: hotel.name,
    country: geographyName,
    city: hotel.city,
    address: hotel.address,
    postal_code: hotel.postal_code || null,
    website: hotel.website,
    phonehotel: hotel.phonehotel,
    chain_code: hotel.chain_code,
    category: hotel.category,
    latitude: null,
    longitude: null,
    rooms_total_supported: false,
    room_types_count: 0,
    match_class: "new_candidate_medium",
    match_score: 0.5,
    match_signals: ["hbx_core_identity"],
    census_record_id: null,
    discovery_wave: waveId,
    country_code: hotel.country_code,
    discovered_at: new Date().toISOString(),
  };
}

function rebuildMergedPack() {
  const byCode = new Map();
  if (fs.existsSync(WAVE1_PACK)) {
    const j = readJson(WAVE1_PACK, { candidates: [] });
    for (const c of j.candidates || []) {
      if (c.hbx_hotel_code != null) byCode.set(Number(c.hbx_hotel_code), c);
    }
  }
  // Prior geography discovery candidates
  const priorDir = path.join(
    ROOT,
    "data/research-engine-v2/full-cala-hbx-geography-discovery/candidates"
  );
  for (const dir of [priorDir, CANDIDATES_DIR]) {
    if (!fs.existsSync(dir)) continue;
    for (const f of fs.readdirSync(dir).filter((x) => x.endsWith(".json"))) {
      const j = readJson(path.join(dir, f), { candidates: [] });
      for (const c of j.candidates || []) {
        if (c.hbx_hotel_code == null) continue;
        const code = Number(c.hbx_hotel_code);
        if (!byCode.has(code)) byCode.set(code, c);
      }
    }
  }
  const candidates = [...byCode.values()];
  const by_country_counts = {};
  for (const c of candidates) {
    by_country_counts[c.country] = (by_country_counts[c.country] || 0) + 1;
  }
  const pack = {
    objective: CORE_IDENTITY_OBJECTIVE,
    generated_at: new Date().toISOString(),
    count: candidates.length,
    by_country_counts,
    candidates,
  };
  writeJson(MERGED_PACK, pack);
  return pack;
}

async function oneContentAccessGate(cfg) {
  const res = await hbxFetchJson(
    contentUrl(
      cfg,
      "hotels?fields=code,name&language=ENG&from=1&to=1&useSecondaryLanguage=false"
    ),
    cfg
  );
  return {
    ok: res.ok,
    status: res.status,
    error_message: res.error_message || null,
    rate_limit: res.response_headers || {},
  };
}

/**
 * Spend HBX budget on ranked plan. Quota → PAUSED_QUOTA (not COMPLETE).
 */
async function runHbxSourceLane(opts) {
  const log = opts.log || (() => {});
  const cfg = opts.cfg;
  const plan = opts.plan;
  const pageSize = opts.pageSize || 1000;
  const waveId = opts.waveId;
  const ledger = opts.ledger;
  const limiter = opts.rateLimiter;

  let pausedQuota = false;
  let hotelsReturned = 0;
  let uniqueCodes = new Set();
  let geosAttempted = [];
  const geos = listDealalityCalaGeographies({ includeScopeReview: false });

  for (const item of plan) {
    if (pausedQuota) break;
    if (limiter.requestCount >= limiter.maxRequestsPerRun) {
      pausedQuota = true;
      break;
    }
    const g = geos.find((x) => x.geography_id === item.geography_id);
    if (!g) continue;
    const codes = queryCodesForGeography(g);
    if (!codes.length) continue;

    const entry = ledger.geographies[g.geography_id];
    entry.hbx_status = HBX_DISCOVERY_STATUS.IN_PROGRESS;
    entry.search_started_at = entry.search_started_at || new Date().toISOString();
    writeJson(
      path.join(STATE_DIR, "hbx-geography-discovery-ledger-mirror.json"),
      ledger
    );

    log(
      `[core-id] HBX pull ${g.name} pages≤${item.allocated_pages} code=${codes[0]}`
    );
    const pulled = await pullCountryHotels(cfg, g.name, codes[0], {
      batchSize: pageSize,
      maxHotelsPerCountry: item.allocated_hotels,
      delayMs: 1200,
      rateLimiter: limiter,
      fields: "all",
      onBatch: (b) =>
        log(
          `[core-id] ${g.name} from=${b.from}-${b.to} page=${b.page} pulled=${b.pulled}/${b.total ?? "?"}`
        ),
    });

    geosAttempted.push(g.name);
    const hotels = [];
    for (const raw of pulled.hotels || []) {
      const hotel = extractHbxHotel(raw, g.name);
      if (hotel.hbx_hotel_code == null) continue;
      hotel.country = g.name;
      hotel.country_code = codes[0];
      hotels.push(hotel);
      uniqueCodes.add(Number(hotel.hbx_hotel_code));
    }
    hotelsReturned += hotels.length;

    const candidates = hotels.map((h) => hotelToCandidate(h, g.name, waveId));
    writeJson(path.join(CANDIDATES_DIR, `${g.geography_id}.json`), {
      geography_id: g.geography_id,
      geography: g.name,
      generated_at: new Date().toISOString(),
      count: candidates.length,
      pagination: {
        from: pulled.from || null,
        total: pulled.total,
        partial: Boolean(pulled.partial || pulled.error?.quota_exceeded),
      },
      candidates,
    });

    entry.hbx_query_codes_tried = [codes[0]];
    entry.source_rows_returned = (entry.source_rows_returned || 0) + hotels.length;
    entry.unique_hotel_codes = hotels.length;
    entry.candidate_rows_persisted = hotels.length;
    entry.errors = pulled.error ? [pulled.error] : [];

    if (pulled.error?.quota_exceeded) {
      entry.hbx_status = "PAUSED_QUOTA";
      entry.pagination_completed = false;
      entry.retry_status = "awaiting_quota_reset";
      pausedQuota = true;
      log(`[core-id] HBX_PAUSED_QUOTA at ${g.name} (partial=${hotels.length})`);
    } else if (!pulled.ok && hotels.length === 0) {
      entry.hbx_status = HBX_DISCOVERY_STATUS.FAILED_RETRYABLE;
      entry.pagination_completed = false;
    } else if (pulled.ok) {
      const total = pulled.total;
      const complete =
        total == null || hotels.length >= total || hotels.length >= item.allocated_hotels;
      entry.hbx_status = hotels.length
        ? complete
          ? HBX_DISCOVERY_STATUS.COMPLETE
          : "IN_PROGRESS"
        : HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS;
      entry.pagination_completed = Boolean(complete);
      entry.search_completed_at = complete ? new Date().toISOString() : null;
    } else {
      // partial success then error
      entry.hbx_status = "IN_PROGRESS";
      entry.pagination_completed = false;
    }

    // Persist into discovery ledger file too
    writeJson(
      path.join(
        ROOT,
        "data/research-engine-v2/full-cala-hbx-geography-discovery/hbx-geography-discovery-ledger.json"
      ),
      {
        ...ledger,
        updated_at: new Date().toISOString(),
        wave_status: pausedQuota
          ? "production_census_hbx_lane_paused_quota"
          : "production_census_hbx_lane_running",
      }
    );
  }

  rebuildMergedPack();
  return {
    pausedQuota,
    hotelsReturned,
    uniqueIdentities: uniqueCodes.size,
    geosAttempted,
    requestsUsed: limiter.requestCount,
  };
}

/**
 * SerpAPI: upgrade HOLDs missing city (bounded). Identity only.
 */
async function runSerpApiHoldUpgrades(opts) {
  const log = opts.log || (() => {});
  const maxSearches = Math.max(0, Number(opts.maxSearches || 40));
  if (maxSearches === 0) {
    return { resolved: 0, searched: 0, skipped: true };
  }
  if (!String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim()) {
    log(`[core-id] SerpAPI key missing — skip HOLD upgrades`);
    return { resolved: 0, searched: 0, skipped: true };
  }

  const holds = readJson(HOLDS_FILE, { by_candidate_id: {} });
  const universe = loadMasterUniverseCandidates();
  const byId = new Map(universe.map((c) => [c.candidate_id, c]));

  const weak = Object.entries(holds.by_candidate_id || {})
    .filter(([, h]) =>
      /weak|review|missing_city|insufficient/i.test(
        `${h.class || ""} ${h.reason || ""}`
      )
    )
    .map(([id, h]) => ({ id, ...h, cand: byId.get(id) }))
    .filter((x) => x.cand && !String(x.cand.city || "").trim())
    .sort((a, b) => {
      const prio = (c) =>
        ({ Brazil: 0, Argentina: 1, Mexico: 2, Chile: 3, Peru: 4, Jamaica: 5 }[
          c
        ] ?? 9);
      return prio(a.country) - prio(b.country);
    })
    .slice(0, maxSearches);

  const tracker = new SerpApiCreditTracker({
    ceiling: maxSearches,
  });

  let resolved = 0;
  let searched = 0;
  const upgrades = [];

  for (const row of weak) {
    if (!tracker.canSpend(1)) break;
    const q = `${row.cand.property_name} hotel ${row.country}`;
    log(`[core-id] SerpAPI HOLD upgrade q="${row.cand.property_name}" (${row.country})`);
    const res = await searchGoogleHotels({ q, gl: "us", hl: "en" }, { tracker });
    searched += 1;
    await sleep(400);
    if (!res.ok || !res.candidates?.length) continue;
    const hit = res.candidates[0];
    const city =
      hit.city ||
      hit.address_components?.city ||
      (typeof hit.address === "string"
        ? hit.address.split(",").slice(-2, -1)[0]?.trim()
        : null) ||
      null;
    if (!city) continue;
    row.cand.city = city;
    if (hit.website && !row.cand.website) row.cand.website = hit.website;
    if (hit.phone && !row.cand.phone) row.cand.phone = hit.phone;
    row.cand.merged_sources = [
      ...new Set([...(row.cand.merged_sources || [row.cand.source_type]), "serpapi_google_hotels"]),
    ];
    // Reclassify hold
    const pf = classifyShellPreflightQuality(row.cand, { cventOnlyQualityGate: true });
    if (pf.class === SHELL_PREFLIGHT_CLASS.SAFE) {
      delete holds.by_candidate_id[row.id];
      resolved += 1;
      upgrades.push({
        candidate_id: row.id,
        country: row.country,
        city,
        preflight: pf.reason,
      });
    } else {
      holds.by_candidate_id[row.id] = {
        ...holds.by_candidate_id[row.id],
        class: pf.class,
        reason: `serpapi_upgraded:${pf.reason}`,
        evidence_fingerprint: `serp_${normName(city)}`,
        held_at: new Date().toISOString(),
      };
    }
  }

  writeJson(HOLDS_FILE, holds);
  writeJson(path.join(STATE_DIR, "serpapi-hold-upgrades.json"), {
    generated_at: new Date().toISOString(),
    searched,
    resolved,
    upgrades,
  });
  // Persist upgraded cities back into a sidecar pack for shell merge
  writeJson(path.join(STATE_DIR, "serpapi-upgraded-candidates.json"), {
    candidates: upgrades.map((u) => {
      const c = byId.get(u.candidate_id);
      return c;
    }).filter(Boolean),
  });

  return { resolved, searched, skipped: false, upgrades };
}

function coverageStatusForRow(row) {
  const census = Number(row.census_count || 0);
  const hbx = row.HBX_SEARCHED === "YES" || row.hbx_status === "COMPLETE";
  const sources =
    (row.hbx_candidates || 0) + (row.cvent_candidates || 0) + (row.other_candidates || 0);
  if (census === 0 && sources === 0) return "ZERO_CONFIRMED_PROPERTIES";
  if (census === 0) return "NEEDS_TARGETED_DISCOVERY";
  if (census < 40 && (row.tourism_priority === "S" || row.tourism_priority === "A"))
    return "SOURCE_GAP";
  if (census >= 200) return "CORE_COVERAGE_STRONG";
  if (census >= 80) return "CORE_COVERAGE_MODERATE";
  if (hbx || sources > 20) return "CORE_COVERAGE_WEAK";
  return "DISCOVERY_NOT_COMPLETE";
}

function buildMacro(matrix) {
  const buckets = {
    Mexico: (r) => r.name === "Mexico",
    "Central America ex-Mexico": (r) =>
      r.dealality_region === "Central America" && r.name !== "Mexico",
    Caribbean: (r) => r.dealality_region === "Caribbean",
    Brazil: (r) => r.name === "Brazil",
    "South America ex-Brazil": (r) =>
      r.dealality_region === "South America" && r.name !== "Brazil",
  };
  const out = {};
  for (const [label, fn] of Object.entries(buckets)) {
    const rows = matrix.filter(fn);
    const census = rows.reduce((s, r) => s + Number(r.census_after || r.census_count || 0), 0);
    out[label] = {
      geographies: rows.length,
      census_count: census,
      remaining_holds: rows.reduce((s, r) => s + Number(r.remaining_holds || 0), 0),
      coverage_mix: rows.reduce((acc, r) => {
        acc[r.coverage_status] = (acc[r.coverage_status] || 0) + 1;
        return acc;
      }, {}),
    };
  }
  return out;
}

/**
 * Main entry
 */
export async function runFullCalaCoreIdentityCensusV1(opts = {}) {
  const log = opts.log || console.log;
  const mode = String(opts.mode || "dry-run").toLowerCase();
  const enableWrites = Boolean(
    opts.enableProductionWrites && (mode === "run" || mode === "resume")
  );
  const generated_at = new Date().toISOString();
  const requestBudget = Math.max(
    1,
    Number(opts.hbxRequestBudget || process.env.HBX_CORE_REQUEST_BUDGET || 40)
  );
  const pageSize = Math.min(
    1000,
    Math.max(100, Number(opts.hbxPageSize || process.env.HBX_BATCH_SIZE || 1000))
  );
  const serpMax = Number(
    opts.serpApiMaxSearches ?? process.env.SERPAPI_CORE_HOLD_UPGRADES ?? 40
  );
  const skipHbx = Boolean(opts.skipHbx);
  const skipSerp = Boolean(opts.skipSerpApi);
  const skipShell = Boolean(opts.skipShell);

  try {
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
    if (CENSUS_TABLE_ID !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
      throw new Error("wrong_production_table");
    }
  } catch (err) {
    return {
      ok: false,
      CORE_CENSUS_STATUS: CORE_STATUS.FOUNDER_STOP,
      FOUNDER_DECISION_REQUIRED: "YES",
      FOUNDER_DECISION: String(err?.message || err),
      generated_at,
    };
  }

  const priority = rankHbxPriorityGeographies({
    requestBudget,
    pageSize,
  });
  writeJson(path.join(STATE_DIR, "hbx-priority-plan.json"), priority);

  if (mode === "dry-run") {
    const report = {
      ok: true,
      CORE_CENSUS_STATUS: CORE_STATUS.DRY_RUN,
      mode: "dry-run",
      production_writes: false,
      HBX_REQUEST_LIMIT: requestBudget,
      HBX_PAGE_SIZE: pageSize,
      HBX_PLAN_TOP: priority.plan.slice(0, 15),
      SERPAPI_HOLD_UPGRADE_BUDGET: serpMax,
      FOUNDER_DECISION_REQUIRED: "NO",
      NEXT_STEP:
        "Launch with gate env + --mode run --enable-production-writes",
      generated_at,
    };
    writeJson(
      path.join(ROOT, "reports/research-engine-v2/full-cala-core-identity-census-dry-run.json"),
      report
    );
    log(`[core-id] DRY-RUN plan geos=${priority.plan.length} budget=${requestBudget}`);
    return report;
  }

  const token = resolvePat();
  const base = resolveTargetBase();
  const baseId = base?.target_base_id || base?.baseId;
  const censusBeforeIndex = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
  const censusBefore = censusBeforeIndex.count;
  const holdsBefore = Object.keys(readJson(HOLDS_FILE, { by_candidate_id: {} }).by_candidate_id || {})
    .length;

  const cfg = resolveHbxConfig(process.env);
  if (!cfg.ok) {
    return {
      ok: false,
      CORE_CENSUS_STATUS: CORE_STATUS.FOUNDER_STOP,
      FOUNDER_DECISION_REQUIRED: "YES",
      FOUNDER_DECISION: `missing_hbx_credentials:${cfg.missing?.join(",")}`,
      CENSUS_BEFORE: censusBefore,
      generated_at,
    };
  }

  // Single access gate
  let hbxPausedQuota = false;
  let hbxLane = {
    hotelsReturned: 0,
    uniqueIdentities: 0,
    geosAttempted: [],
    requestsUsed: 0,
  };
  let gate = { ok: false, status: 0 };
  if (!skipHbx) {
    gate = await oneContentAccessGate(cfg);
    log(`[core-id] HBX access gate status=${gate.status}`);
    if (!gate.ok) {
      if (/quota/i.test(String(gate.error_message || ""))) {
        hbxPausedQuota = true;
        log(`[core-id] HBX already PAUSED_QUOTA at gate — continuing core lane`);
      } else {
        log(`[core-id] HBX gate failed status=${gate.status} — continuing core lane without HBX`);
        hbxPausedQuota = true;
      }
    } else {
      const limiter = createHbxRequestRateLimiter({
        minIntervalMs: Number(process.env.HBX_MIN_REQUEST_INTERVAL_MS || 1200),
        maxRequestsPerRun: requestBudget,
        maxRetriesOn429: 2,
      });
      const ledger = initOrLoadLedger();
      const waveId = `coreid_${generated_at.replace(/[:.]/g, "-")}`;
      hbxLane = await runHbxSourceLane({
        log,
        cfg,
        plan: priority.plan,
        pageSize,
        waveId,
        ledger,
        rateLimiter: limiter,
      });
      hbxPausedQuota = hbxLane.pausedQuota;
    }
  }

  // Cross-link stats (read-only classify)
  rebuildMergedPack();
  const hbxPack = loadHbxCandidates();
  const universe = loadMasterUniverseCandidates();
  const { merged } = mergeCandidateUniverses(universe, hbxPack);
  let hbxHoldsUpgraded = 0;
  let existingMatches = 0;
  let newSafe = 0;
  const index = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
  const cventKeys = new Set();
  for (const m of merged) {
    const isCvent =
      m.source_type === "cvent_candidate" ||
      (m.merged_sources || []).includes("cvent_candidate");
    if (!isCvent) continue;
    cventKeys.add(
      `${normName(m.property_name || m.normalized_property_name)}|${normalizeGeographyLabel(m.country)}`
    );
  }
  for (const c of hbxPack) {
    const candidate = {
      candidate_id: c.candidate_id || `hbx_${c.external_ids?.hbx_code}`,
      property_name: c.property_name || c.name,
      normalized_property_name:
        c.normalized_property_name || normName(c.property_name || c.name),
      country: c.country,
      city: c.city,
      address: c.address,
      website: c.website,
      phone: c.phone || c.phonehotel,
      source_type: "hbx_content_api",
      merged_sources: ["hbx_content_api"],
      external_ids: c.external_ids || { hbx_code: c.hbx_hotel_code },
    };
    const key = `${candidate.normalized_property_name}|${normalizeGeographyLabel(candidate.country)}`;
    if (cventKeys.has(key)) {
      candidate.merged_sources = ["cvent_candidate", "hbx_content_api"];
      hbxHoldsUpgraded += 1;
    }
    const cls = classifyAgainstCensus(candidate, index);
    if (
      cls.match_class === MATCH.EXISTING_HIGH ||
      cls.match_class === MATCH.EXISTING_MEDIUM
    ) {
      existingMatches += 1;
    } else {
      const pf = classifyShellPreflightQuality(candidate, {
        cventOnlyQualityGate: true,
      });
      if (pf.class === SHELL_PREFLIGHT_CLASS.SAFE) newSafe += 1;
    }
  }

  // SerpAPI HOLD upgrades
  let serp = { resolved: 0, searched: 0, skipped: true };
  if (!skipSerp) {
    serp = await runSerpApiHoldUpgrades({ log, maxSearches: serpMax });
  }

  // Shell auto-apply (HBX-first then core identity mode)
  let shellsInserted = 0;
  let duplicatesSkipped = 0;
  if (enableWrites && !skipShell) {
    log(`[core-id] shell orchestrator (HBX-backed SAFE)…`);
    const orch1 = await runFullCala15kShellOrchestratorV1({
      mode: "run",
      enableProductionWrites: true,
      maxBatches: 30,
      coreIdentityMode: false,
      log,
    });
    shellsInserted += orch1.SHELLS_ADDED_THIS_RUN || 0;
    if (orch1.FOUNDER_DECISION_REQUIRED === "YES") {
      return {
        ok: false,
        CORE_CENSUS_STATUS: CORE_STATUS.FOUNDER_STOP,
        FOUNDER_DECISION_REQUIRED: "YES",
        FOUNDER_DECISION: orch1.STOP_REASON || orch1.FOUNDER_DECISION,
        CENSUS_BEFORE: censusBefore,
        NEW_CORE_SHELLS_INSERTED: shellsInserted,
        HBX_PAUSED_QUOTA: hbxPausedQuota ? "YES" : "NO",
        generated_at,
      };
    }

    log(`[core-id] shell orchestrator (Core Identity mode)…`);
    const orch2 = await runFullCala15kShellOrchestratorV1({
      mode: "run",
      enableProductionWrites: true,
      maxBatches: 40,
      coreIdentityMode: true,
      log,
    });
    shellsInserted += orch2.SHELLS_ADDED_THIS_RUN || 0;
    if (orch2.FOUNDER_DECISION_REQUIRED === "YES") {
      return {
        ok: false,
        CORE_CENSUS_STATUS: CORE_STATUS.FOUNDER_STOP,
        FOUNDER_DECISION_REQUIRED: "YES",
        FOUNDER_DECISION: orch2.STOP_REASON || orch2.FOUNDER_DECISION,
        CENSUS_BEFORE: censusBefore,
        NEW_CORE_SHELLS_INSERTED: shellsInserted,
        HBX_PAUSED_QUOTA: hbxPausedQuota ? "YES" : "NO",
        generated_at,
      };
    }
  }

  const censusAfterIndex = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
  const censusAfter = censusAfterIndex.count;
  const holdsAfter = Object.keys(
    readJson(HOLDS_FILE, { by_candidate_id: {} }).by_candidate_id || {}
  ).length;

  // 52-geography assessment
  log(`[core-id] geography coverage audit…`);
  const geoAudit = await runFullCalaGeographyCoverageRegistryAuditV1({ log });
  const matrixSrc =
    geoAudit.FULL_GEOGRAPHY_COVERAGE_MATRIX ||
    geoAudit.GEOGRAPHY_MATRIX ||
    geoAudit.geography_matrix ||
    [];
  const holdsMap = {};
  for (const [id, h] of Object.entries(
    readJson(HOLDS_FILE, { by_candidate_id: {} }).by_candidate_id || {}
  )) {
    const c = h.country || "unknown";
    holdsMap[c] = (holdsMap[c] || 0) + 1;
  }
  const ledger = initOrLoadLedger();
  const matrix = matrixSrc.map((r) => {
    const name = r.name || r.geography || r.canonical_geography;
    const gid = listDealalityCalaGeographies({ includeScopeReview: false }).find(
      (g) => normalizeGeographyLabel(g.name) === normalizeGeographyLabel(name)
    )?.geography_id;
    const hbxEntry = gid ? ledger.geographies[gid] : null;
    const row = {
      geography: name,
      name,
      dealality_region: r.dealality_region || r.region,
      tourism_priority: r.tourism_priority,
      census_before: r.census_count,
      census_after: r.census_count, // audit is post-run listing
      census_count: r.census_count,
      holds_before: holdsMap[name] || 0,
      remaining_holds: holdsMap[name] || 0,
      HBX_STATUS: hbxEntry?.hbx_status || (r.HBX_SEARCHED === "YES" ? "COMPLETE" : "NOT_STARTED"),
      CVENT_STATUS: r.CVENT_SEARCHED,
      HBX_SEARCHED: r.HBX_SEARCHED,
      hbx_candidates: r.hbx_candidates,
      cvent_candidates: r.cvent_candidates,
      other_candidates: r.other_candidates,
      name_pct: null,
      city_pct: null,
      address_pct: null,
      website_pct: null,
      phone_pct: null,
      state_region_pct: null,
      primary_gap: r.recommended_next_action,
      next_best_source:
        r.HBX_SEARCHED !== "YES" ? "hbx_content_api" : "serpapi_or_official_web",
    };
    row.coverage_status = coverageStatusForRow(row);
    return row;
  });

  // Enrich completeness from census index sample by country (approximate)
  const byCountryFields = {};
  for (const rec of censusAfterIndex.records || []) {
    const c = String(rec.fields?.Country || "").trim();
    if (!c) continue;
    byCountryFields[c] = byCountryFields[c] || {
      n: 0,
      name: 0,
      city: 0,
      address: 0,
      web: 0,
      phone: 0,
      state: 0,
    };
    const b = byCountryFields[c];
    b.n += 1;
    if (rec.fields?.["Property Name"] || rec.fields?.["Canonical Property Name"])
      b.name += 1;
    if (rec.fields?.City) b.city += 1;
    if (rec.fields?.Address || rec.fields?.["Street Address"]) b.address += 1;
    if (rec.fields?.Website) b.web += 1;
    if (rec.fields?.Phone || rec.fields?.PHONEHOTEL) b.phone += 1;
    if (
      rec.fields?.State ||
      rec.fields?.["State / Region"] ||
      rec.fields?.Region ||
      rec.fields?.Province
    )
      b.state += 1;
  }
  for (const row of matrix) {
    const b = byCountryFields[row.name];
    if (!b || !b.n) continue;
    row.name_pct = Math.round((100 * b.name) / b.n);
    row.city_pct = Math.round((100 * b.city) / b.n);
    row.address_pct = Math.round((100 * b.address) / b.n);
    row.website_pct = Math.round((100 * b.web) / b.n);
    row.phone_pct = Math.round((100 * b.phone) / b.n);
    row.state_region_pct = Math.round((100 * b.state) / b.n);
  }

  matrix.sort((a, b) => {
    const rank = (s) =>
      ({
        ZERO_CONFIRMED_PROPERTIES: 0,
        NEEDS_TARGETED_DISCOVERY: 1,
        SOURCE_GAP: 2,
        DISCOVERY_NOT_COMPLETE: 3,
        CORE_COVERAGE_WEAK: 4,
        CORE_COVERAGE_MODERATE: 5,
        CORE_COVERAGE_STRONG: 6,
      }[s] ?? 5);
    return rank(a.coverage_status) - rank(b.coverage_status) || a.census_count - b.census_count;
  });

  const matrixPath = path.join(
    ROOT,
    "reports/research-engine-v2/full-cala-core-identity-52-geography-matrix.json"
  );
  writeJson(matrixPath, {
    generated_at,
    registry_version: DEALALITY_CALA_GEOGRAPHY_REGISTRY_VERSION,
    census_before: censusBefore,
    census_after: censusAfter,
    matrix,
  });
  writeMd(
    path.join(
      ROOT,
      "reports/research-engine-v2/full-cala-core-identity-52-geography-matrix.md"
    ),
    [
      `# Core Identity 52-Geography Matrix`,
      ``,
      `Census ${censusBefore} → ${censusAfter}`,
      ``,
      `| Geography | Census | Holds | HBX | Coverage | City% | Next |`,
      `| --- | ---: | ---: | --- | --- | ---: | --- |`,
      ...matrix.map(
        (r) =>
          `| ${r.geography} | ${r.census_count} | ${r.remaining_holds} | ${r.HBX_STATUS} | ${r.coverage_status} | ${r.city_pct ?? "—"} | ${r.next_best_source} |`
      ),
    ].join("\n")
  );

  const totalN = Object.values(byCountryFields).reduce((s, b) => s + b.n, 0) || 1;
  const sumF = (k) =>
    Object.values(byCountryFields).reduce((s, b) => s + b[k], 0);
  const pct = (k) => Math.round((100 * sumF(k)) / totalN);

  const targetGap = Math.max(0, 15000 - censusAfter);
  const counts = matrix.reduce((acc, r) => {
    acc[r.coverage_status] = (acc[r.coverage_status] || 0) + 1;
    return acc;
  }, {});

  const final = {
    ok: true,
    CORE_CENSUS_STATUS: CORE_STATUS.COMPLETE,
    CENSUS_BEFORE: censusBefore,
    CENSUS_AFTER: censusAfter,
    TARGET_15K_REACHED: censusAfter >= 15000 ? "YES" : "NO",
    TARGET_GAP_IF_ANY: targetGap,
    NEW_CORE_SHELLS_INSERTED: shellsInserted,
    EXISTING_HOLDS_RESOLVED: Math.max(0, holdsBefore - holdsAfter) + serp.resolved,
    NEW_PROPERTIES_DISCOVERED: hbxLane.uniqueIdentities,
    DUPLICATES_SKIPPED: existingMatches,
    INVALIDS_EXCLUDED: duplicatesSkipped,
    TOTAL_REMAINING_HOLDS: holdsAfter,
    HBX_REQUESTS_USED: hbxLane.requestsUsed,
    HBX_REQUESTS_REMAINING: Math.max(0, requestBudget - hbxLane.requestsUsed),
    HBX_REQUEST_LIMIT: requestBudget,
    HBX_NEW_IDENTITIES: hbxLane.uniqueIdentities,
    HBX_HOLDS_UPGRADED: hbxHoldsUpgraded,
    HBX_PAUSED_QUOTA: hbxPausedQuota ? "YES" : "NO",
    HBX_HOTELS_RETURNED: hbxLane.hotelsReturned,
    HBX_GEOGRAPHIES_ATTEMPTED: hbxLane.geosAttempted,
    SERPAPI_IDENTITIES_RESOLVED: serp.resolved,
    SERPAPI_SEARCHES: serp.searched,
    STAYINGAPI_IDENTITIES_RESOLVED: 0,
    OTHER_SOURCE_IDENTITIES_RESOLVED: 0,
    NAME_COMPLETENESS: pct("name"),
    COUNTRY_COMPLETENESS: 100,
    STATE_REGION_COMPLETENESS: pct("state"),
    CITY_COMPLETENESS: pct("city"),
    ADDRESS_COMPLETENESS: pct("address"),
    WEBSITE_COMPLETENESS: pct("web"),
    PHONE_COMPLETENESS: pct("phone"),
    GEOGRAPHIES_ASSESSED: `${matrix.length} / 52`,
    GEOGRAPHIES_CORE_COVERAGE_STRONG: counts.CORE_COVERAGE_STRONG || 0,
    GEOGRAPHIES_CORE_COVERAGE_MODERATE: counts.CORE_COVERAGE_MODERATE || 0,
    GEOGRAPHIES_CORE_COVERAGE_WEAK: counts.CORE_COVERAGE_WEAK || 0,
    GEOGRAPHIES_SOURCE_GAP: counts.SOURCE_GAP || 0,
    ZERO_CENSUS_GEOGRAPHIES_AFTER: counts.ZERO_CONFIRMED_PROPERTIES || 0,
    TOP_10_REMAINING_GEOGRAPHIC_GAPS: matrix.slice(0, 10).map((r) => ({
      geography: r.geography,
      coverage_status: r.coverage_status,
      census: r.census_count,
      hbx_status: r.HBX_STATUS,
      next: r.next_best_source,
    })),
    FULL_52_GEOGRAPHY_MATRIX_PATH:
      "reports/research-engine-v2/full-cala-core-identity-52-geography-matrix.json",
    SOURCE_PERFORMANCE_SUMMARY: {
      hbx_lane: hbxLane,
      serpapi: serp,
      new_safe_estimate: newSafe,
      shells_inserted: shellsInserted,
    },
    MACRO_ASSESSMENT: buildMacro(matrix),
    FOUNDER_DECISION_REQUIRED: "NO",
    FOUNDER_DECISION: null,
    production_table_id: CENSUS_TABLE_ID,
    generated_at,
  };

  writeJson(
    path.join(ROOT, "reports/research-engine-v2/full-cala-core-identity-census-final.json"),
    final
  );
  writeMd(
    path.join(ROOT, "reports/research-engine-v2/full-cala-core-identity-census-final.md"),
    `# Core Identity Census\n\n**Status:** \`${final.CORE_CENSUS_STATUS}\`\n\nCensus ${final.CENSUS_BEFORE} → ${final.CENSUS_AFTER} (15k gap ${final.TARGET_GAP_IF_ANY})\n\nHBX paused quota: **${final.HBX_PAUSED_QUOTA}** · requests used ${final.HBX_REQUESTS_USED}/${final.HBX_REQUEST_LIMIT}\n\nShells: ${final.NEW_CORE_SHELLS_INSERTED} · Holds remaining: ${final.TOTAL_REMAINING_HOLDS}\n`
  );
  writeMd(
    path.join(ROOT, "docs/data-intelligence/full-cala-core-identity-census-orchestrator-v1.md"),
    `# Core Identity Census Orchestrator v1\n\nSee \`reports/research-engine-v2/full-cala-core-identity-census-final.json\`.\n`
  );

  writeJson(STATE_FILE, {
    ...final,
    updated_at: new Date().toISOString(),
  });

  log(
    `[core-id] COMPLETE census ${censusBefore}→${censusAfter} shells=${shellsInserted} hbx_paused=${hbxPausedQuota}`
  );
  return final;
}
