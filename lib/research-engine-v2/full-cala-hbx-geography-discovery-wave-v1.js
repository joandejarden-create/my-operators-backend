/**
 * Full CALA HBX Geographic Discovery Wave v1
 *
 * Geography registry → HBX pull → persist → classify → SAFE shell auto-apply → next.
 * Reuses HBX Wave1 pull/extract + shell orchestrator. No parallel HBX client.
 *
 * Modes: dry-run | run | resume
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolveHbxConfig,
  hbxFetchJson,
  contentUrl,
} from "./hbx-content-api-client.js";
import {
  extractHbxHotel,
  pullCountryHotels,
} from "./hbx-content-api-cala-wave1-dry-run-v1.js";
import { createHbxRequestRateLimiter } from "./hbx-request-rate-limiter-v1.js";
import {
  listDealalityCalaGeographies,
  DEALALITY_CALA_GEOGRAPHY_REGISTRY_VERSION,
  resolveDealalityCalaGeography,
  normalizeGeographyLabel,
} from "./dealality-cala-geography-registry-v1.js";
import {
  CENSUS_TABLE_ID,
  MATCH,
  loadMasterUniverseCandidates,
  loadHbxCandidates,
  mergeCandidateUniverses,
  classifyAgainstCensus,
  listCensusIndex,
} from "./full-cala-15k-census-shell-insert-v1.js";
import { runFullCala15kShellOrchestratorV1 } from "./full-cala-15k-shell-orchestrator-v1.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import {
  CVENT_LATAM_CARIBBEAN_COUNTRIES,
  findCventLatamCountry,
} from "./census-cvent-latam-country-registry.js";
import { runFullCalaGeographyCoverageRegistryAuditV1 } from "./full-cala-geography-coverage-registry-audit-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const HBX_GEO_DISCOVERY_OBJECTIVE =
  "full-cala-hbx-geography-discovery-wave-v1";
export const HBX_GEO_DISCOVERY_VERSION =
  "full-cala-hbx-geography-discovery-wave-v1";

export const HBX_DISCOVERY_STATUS = Object.freeze({
  NOT_STARTED: "NOT_STARTED",
  IN_PROGRESS: "IN_PROGRESS",
  COMPLETE: "COMPLETE",
  COMPLETE_ZERO_RESULTS: "COMPLETE_ZERO_RESULTS",
  UNSUPPORTED_GEOGRAPHY: "UNSUPPORTED_GEOGRAPHY",
  FAILED_RETRYABLE: "FAILED_RETRYABLE",
  FAILED_REQUIRES_REVIEW: "FAILED_REQUIRES_REVIEW",
  COMPLETE_SOURCE_UNAVAILABLE: "COMPLETE_SOURCE_UNAVAILABLE",
});

export const WAVE_STATUS = Object.freeze({
  DRY_RUN_READY:
    "production_census_full_cala_hbx_geography_discovery_dry_run_ready",
  RUNNING: "production_census_full_cala_hbx_geography_discovery_running",
  COMPLETE:
    "production_census_full_cala_hbx_geography_discovery_wave_complete",
  FOUNDER_STOP:
    "production_census_full_cala_hbx_geography_discovery_stop_for_founder_review",
  FAILED: "production_census_full_cala_hbx_geography_discovery_failed",
});

const LEDGER_DIR = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-hbx-geography-discovery"
);
const LEDGER_FILE = path.join(LEDGER_DIR, "hbx-geography-discovery-ledger.json");
const CANDIDATES_DIR = path.join(LEDGER_DIR, "candidates");
const MERGED_PACK = path.join(
  ROOT,
  "reports/research-engine-v2/hbx-cala-full-geography-candidate-pack.json"
);
const WAVE1_PACK = path.join(
  ROOT,
  "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json"
);

/** Wave1 geographies already fully discovered (documented). */
const WAVE1_COMPLETE = new Set([
  "Mexico",
  "Dominican Republic",
  "Colombia",
  "Costa Rica",
  "Panama",
]);

/**
 * Map HBX pull HTTP failures to discovery statuses.
 * Never treat auth/rate-limit failures as COMPLETE_ZERO_RESULTS.
 */
export function classifyHbxPullOutcome({ hotels = [], error = null } = {}) {
  const n = Array.isArray(hotels) ? hotels.length : 0;
  if (n > 0) {
    return { status: HBX_DISCOVERY_STATUS.COMPLETE, founder_stop: false };
  }
  // Genuine empty success (API ok, zero hotels) — only when no error object
  if (!error) {
    return {
      status: HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS,
      founder_stop: false,
    };
  }
  const status = Number(error.status || 0);
  const msg = String(error.message || error.code || "");
  // Hotelbeds returns HTTP 403 with message "Quota exceeded" — not licensing.
  if (status === 403 && /quota/i.test(msg)) {
    return {
      status: HBX_DISCOVERY_STATUS.FAILED_RETRYABLE,
      founder_stop: true,
      stop_reason: "test_daily_quota_exhausted:http_403",
    };
  }
  if (status === 401 || status === 403) {
    return {
      status: HBX_DISCOVERY_STATUS.FAILED_REQUIRES_REVIEW,
      founder_stop: true,
      stop_reason: `unknown_hbx_licensing_or_auth_issue:http_${status}`,
    };
  }
  if (status === 429 || status >= 500 || status === 0) {
    return {
      status: HBX_DISCOVERY_STATUS.FAILED_RETRYABLE,
      founder_stop: true,
      stop_reason: `hbx_pull_failed_retryable:http_${status || "network"}`,
    };
  }
  if (status === 400 || status === 404) {
    return {
      status: HBX_DISCOVERY_STATUS.UNSUPPORTED_GEOGRAPHY,
      founder_stop: false,
    };
  }
  // Unknown client/error class — do not claim zero results
  return {
    status: HBX_DISCOVERY_STATUS.FAILED_REQUIRES_REVIEW,
    founder_stop: true,
    stop_reason: `unknown_hbx_pull_failure:http_${status || "unknown"}`,
  };
}

/**
 * Reclassify ledger rows falsely marked COMPLETE_ZERO_RESULTS when errors
 * prove the pull never successfully completed.
 */
export function repairFalseZeroResultLedgerEntries(ledger) {
  let repaired = 0;
  for (const entry of Object.values(ledger.geographies || {})) {
    if (entry.wave1_prior_complete) continue;
    if (entry.hbx_status !== HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS) {
      continue;
    }
    const err = Array.isArray(entry.errors) ? entry.errors[0] : null;
    if (!err) continue;
    const outcome = classifyHbxPullOutcome({ hotels: [], error: err });
    if (outcome.status === HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS) continue;
    entry.hbx_status = outcome.status;
    entry.pagination_completed = false;
    entry.retry_status =
      outcome.status === HBX_DISCOVERY_STATUS.FAILED_RETRYABLE
        ? "awaiting_resume"
        : "requires_founder_review";
    repaired += 1;
  }
  if (repaired > 0) {
    ledger.wave_status = WAVE_STATUS.FOUNDER_STOP;
    ledger.totals = {
      ...(ledger.totals || {}),
      false_zero_results_repaired: repaired,
      repair_note:
        "COMPLETE_ZERO_RESULTS with HTTP auth/rate-limit errors reclassified; not genuine empty searches",
    };
    saveLedger(ledger);
  }
  return repaired;
}

async function preflightHbxReadable(cfg) {
  const res = await hbxFetchJson(
    contentUrl(
      cfg,
      "hotels?fields=code,name&language=ENG&from=1&to=1&useSecondaryLanguage=false"
    ),
    cfg
  );
  if (res.ok) {
    return { ok: true, status: res.status };
  }
  return {
    ok: false,
    status: res.status,
    error_code: res.error_code || null,
    error_message: res.error_message || null,
    body_text_preview: res.body_text_preview || null,
    response_headers: res.response_headers || {},
  };
}

/**
 * Alternate HBX countryCode attempts when primary ISO returns empty.
 * Canonical Dealality geography stays distinct after attribution.
 */
export const HBX_ALTERNATE_QUERY_CODES = Object.freeze({
  puerto_rico: ["PR", "US"],
  us_virgin_islands: ["VI", "US"],
  british_virgin_islands: ["VG", "GB"],
  french_guiana: ["GF", "FR"],
  guadeloupe: ["GP", "FR"],
  martinique: ["MQ", "FR"],
  saint_martin_fr: ["MF", "FR"],
  sint_maarten: ["SX", "NL"],
  bonaire: ["BQ", "NL"],
  sint_eustatius: ["BQ", "NL"],
  saba: ["BQ", "NL"],
  curacao: ["CW", "NL"],
  aruba: ["AW", "NL"],
  saint_barthelemy: ["BL", "FR"],
  turks_and_caicos: ["TC", "GB"],
  cayman_islands: ["KY", "GB"],
  anguilla: ["AI", "GB"],
  montserrat: ["MS", "GB"],
  bermuda: ["BM", "GB"],
});

/** Shared BQ pull — attribute hotels to BES islands by destination/city tokens. */
const BES_GROUP = Object.freeze({
  query_code: "BQ",
  members: ["bonaire", "sint_eustatius", "saba"],
});

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

function emptyLedgerEntry(g) {
  return {
    geography_id: g.geography_id,
    canonical_geography: g.name,
    iso_code: g.iso_code,
    aliases: g.aliases || [],
    in_scope: g.scope === "in_scope",
    hbx_query_codes_tried: [],
    hbx_status: WAVE1_COMPLETE.has(g.name)
      ? HBX_DISCOVERY_STATUS.COMPLETE
      : HBX_DISCOVERY_STATUS.NOT_STARTED,
    wave1_prior_complete: WAVE1_COMPLETE.has(g.name),
    search_started_at: null,
    search_completed_at: null,
    source_rows_returned: 0,
    unique_hotel_codes: 0,
    pagination_completed: WAVE1_COMPLETE.has(g.name),
    errors: [],
    retry_status: null,
    candidate_rows_persisted: 0,
    existing_census_matches: 0,
    safe_new_candidates: 0,
    hold_candidates: 0,
    invalid_candidates: 0,
    shells_inserted: 0,
    cvent_plus_hbx_matches: 0,
    holds_upgraded_to_safe: 0,
    cvent_status: null,
  };
}

export function initOrLoadLedger() {
  const geos = listDealalityCalaGeographies({ includeScopeReview: true });
  const existing = readJson(LEDGER_FILE, null);
  const byId = {};
  for (const g of geos) {
    if (g.scope !== "in_scope") continue;
    byId[g.geography_id] =
      existing?.geographies?.[g.geography_id] || emptyLedgerEntry(g);
    // Founder decision: Bermuda in_scope — clear any prior scope_review residue
    byId[g.geography_id].in_scope = true;
    byId[g.geography_id].canonical_geography = g.name;
    byId[g.geography_id].iso_code = g.iso_code;
  }
  // Seed wave1 counts from pack if complete and zeroed
  if (fs.existsSync(WAVE1_PACK)) {
    const pack = readJson(WAVE1_PACK, { candidates: [] });
    const byCountry = {};
    for (const c of pack.candidates || []) {
      byCountry[c.country] = (byCountry[c.country] || 0) + 1;
    }
    for (const [name, n] of Object.entries(byCountry)) {
      const g = resolveDealalityCalaGeography(name);
      if (!g || !byId[g.geography_id]) continue;
      if (byId[g.geography_id].wave1_prior_complete) {
        byId[g.geography_id].source_rows_returned = Math.max(
          byId[g.geography_id].source_rows_returned,
          n
        );
        byId[g.geography_id].candidate_rows_persisted = Math.max(
          byId[g.geography_id].candidate_rows_persisted,
          n
        );
        byId[g.geography_id].unique_hotel_codes = Math.max(
          byId[g.geography_id].unique_hotel_codes,
          n
        );
      }
    }
  }
  const ledger = {
    version: HBX_GEO_DISCOVERY_VERSION,
    registry_version: DEALALITY_CALA_GEOGRAPHY_REGISTRY_VERSION,
    updated_at: new Date().toISOString(),
    run_id: existing?.run_id || null,
    wave_status: existing?.wave_status || null,
    geographies: byId,
    totals: existing?.totals || {},
  };
  writeJson(LEDGER_FILE, ledger);
  return ledger;
}

function saveLedger(ledger) {
  ledger.updated_at = new Date().toISOString();
  writeJson(LEDGER_FILE, ledger);
}

function attributeBesGeography(hotel) {
  const blob = [
    hotel.city,
    hotel.destination,
    hotel.zone,
    hotel.name,
    hotel.address,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");
  if (/eustati|statia/.test(blob)) return "sint_eustatius";
  if (/\bsaba\b/.test(blob)) return "saba";
  if (/bonaire|kralendijk/.test(blob)) return "bonaire";
  return "bonaire"; // default BES residual → Bonaire (largest hotel market)
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

function hotelToPackCandidate(hotel, geographyName, waveId) {
  return {
    hbx_hotel_code: hotel.hbx_hotel_code,
    name: hotel.name,
    country: geographyName,
    city: hotel.city,
    address: hotel.address,
    website: hotel.website,
    phonehotel: hotel.phonehotel,
    chain_code: hotel.chain_code,
    category: hotel.category,
    latitude: null, // never persist coords into pack for shell use
    longitude: null,
    rooms_total_supported: false,
    room_types_count: hotel.room_types_count || 0,
    match_class: "new_candidate_medium",
    match_score: 0.5,
    match_signals: ["hbx_geography_discovery"],
    census_record_id: null,
    discovery_wave: waveId,
    country_code: hotel.country_code,
    discovered_at: new Date().toISOString(),
  };
}

function rebuildMergedPack(ledger) {
  const byCode = new Map();
  // Prefer wave1 + per-geo files
  if (fs.existsSync(WAVE1_PACK)) {
    const j = readJson(WAVE1_PACK, { candidates: [] });
    for (const c of j.candidates || []) {
      if (c.hbx_hotel_code != null) byCode.set(Number(c.hbx_hotel_code), c);
    }
  }
  if (fs.existsSync(CANDIDATES_DIR)) {
    for (const f of fs.readdirSync(CANDIDATES_DIR).filter((x) => x.endsWith(".json"))) {
      const j = readJson(path.join(CANDIDATES_DIR, f), { candidates: [] });
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
    objective: HBX_GEO_DISCOVERY_OBJECTIVE,
    generated_at: new Date().toISOString(),
    dry_run: false,
    airtable_writes: 0,
    count: candidates.length,
    by_country_counts,
    candidates,
    ledger_path: "data/research-engine-v2/full-cala-hbx-geography-discovery/hbx-geography-discovery-ledger.json",
  };
  writeJson(MERGED_PACK, pack);
  return pack;
}

async function discoverGeographyHbx(g, cfg, opts) {
  const log = opts.log || (() => {});
  const batchSize = opts.batchSize || 100;
  const maxHotels = opts.maxHotelsPerCountry || 25000;
  const delayMs = opts.delayMs || 1200;
  const rateLimiter = opts.rateLimiter || null;

  // BES shared query — handled by caller for group
  const codes = queryCodesForGeography(g);
  if (!codes.length) {
    return {
      status: HBX_DISCOVERY_STATUS.UNSUPPORTED_GEOGRAPHY,
      hotels: [],
      codes_tried: [],
      error: { status: null, message: "missing_iso_code" },
      founder_stop: false,
    };
  }

  let lastError = null;
  const allHotels = [];
  const seen = new Set();
  const codesTried = [];

  for (const code of codes) {
    codesTried.push(code);
    log(`[hbx-geo] pull ${g.name} countryCode=${code}`);
    const pulled = await pullCountryHotels(cfg, g.name, code, {
      batchSize,
      maxHotelsPerCountry: maxHotels,
      delayMs,
      rateLimiter,
      onBatch: (b) =>
        log(
          `[hbx-geo] ${g.name} ${code} from=${b.from} to=${b.to} page=${b.page} pulled=${b.pulled}/${b.total ?? "?"}`
        ),
    });
    if (!pulled.ok) {
      lastError = pulled.error;
      const outcome = classifyHbxPullOutcome({ hotels: [], error: lastError });
      // Hard-stop failures: do not keep trying aliases as if empty
      if (outcome.founder_stop) {
        return {
          status: outcome.status,
          hotels: allHotels,
          codes_tried: codesTried,
          error: lastError,
          founder_stop: true,
          stop_reason: outcome.stop_reason,
        };
      }
      continue;
    }
    for (const raw of pulled.hotels) {
      const hotel = extractHbxHotel(raw, g.name);
      if (hotel.hbx_hotel_code == null) continue;
      const n = Number(hotel.hbx_hotel_code);
      if (seen.has(n)) continue;
      seen.add(n);
      hotel.country = g.name;
      hotel.country_code = code;
      allHotels.push(hotel);
    }
    if (allHotels.length > 0) break; // primary/first successful stock wins
  }

  const outcome = classifyHbxPullOutcome({
    hotels: allHotels,
    error: allHotels.length ? null : lastError,
  });
  return {
    status: outcome.status,
    hotels: allHotels,
    codes_tried: codesTried,
    error: allHotels.length ? null : lastError,
    founder_stop: Boolean(outcome.founder_stop),
    stop_reason: outcome.stop_reason || null,
  };
}

async function discoverBesGroup(cfg, ledger, opts) {
  const log = opts.log || (() => {});
  const geos = listDealalityCalaGeographies({ includeScopeReview: false });
  const members = BES_GROUP.members
    .map((id) => geos.find((g) => g.geography_id === id))
    .filter(Boolean);
  if (!members.length) return;

  // Skip if all already complete
  if (
    members.every((g) =>
      [HBX_DISCOVERY_STATUS.COMPLETE, HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS].includes(
        ledger.geographies[g.geography_id]?.hbx_status
      )
    )
  ) {
    return;
  }

  const started = new Date().toISOString();
  for (const g of members) {
    ledger.geographies[g.geography_id].hbx_status = HBX_DISCOVERY_STATUS.IN_PROGRESS;
    ledger.geographies[g.geography_id].search_started_at = started;
  }
  saveLedger(ledger);

  log(`[hbx-geo] BES group pull countryCode=${BES_GROUP.query_code}`);
  const pulled = await pullCountryHotels(cfg, "Caribbean Netherlands", BES_GROUP.query_code, {
    batchSize: opts.batchSize || 100,
    maxHotelsPerCountry: opts.maxHotelsPerCountry || 25000,
    delayMs: opts.delayMs || 1200,
    rateLimiter: opts.rateLimiter || null,
    onBatch: (b) =>
      log(
        `[hbx-geo] BES BQ from=${b.from} to=${b.to} page=${b.page} pulled=${b.pulled}/${b.total ?? "?"}`
      ),
  });

  if (!pulled.ok) {
    const outcome = classifyHbxPullOutcome({ hotels: [], error: pulled.error });
    for (const g of members) {
      const e = ledger.geographies[g.geography_id];
      e.hbx_status = outcome.status;
      e.errors = [pulled.error];
      e.hbx_query_codes_tried = [BES_GROUP.query_code];
      e.pagination_completed = false;
      e.retry_status = outcome.founder_stop
        ? outcome.status === HBX_DISCOVERY_STATUS.FAILED_RETRYABLE
          ? "awaiting_resume"
          : "requires_founder_review"
        : null;
    }
    saveLedger(ledger);
    return {
      founder_stop: Boolean(outcome.founder_stop),
      retryable: outcome.status === HBX_DISCOVERY_STATUS.FAILED_RETRYABLE,
      stop_reason:
        outcome.stop_reason ||
        `bes_group_hbx_pull_failed:http_${pulled.error?.status}`,
    };
  }

  const buckets = { bonaire: [], sint_eustatius: [], saba: [] };
  for (const raw of pulled.hotels) {
    const hotel = extractHbxHotel(raw, "Bonaire");
    const gid = attributeBesGeography(hotel);
    const g = members.find((m) => m.geography_id === gid) || members[0];
    hotel.country = g.name;
    hotel.country_code = BES_GROUP.query_code;
    buckets[g.geography_id] = buckets[g.geography_id] || [];
    buckets[g.geography_id].push(hotel);
  }

  for (const g of members) {
    const hotels = buckets[g.geography_id] || [];
    persistGeographyCandidates(g, hotels, opts.waveId);
    const e = ledger.geographies[g.geography_id];
    e.hbx_query_codes_tried = [BES_GROUP.query_code];
    e.source_rows_returned = hotels.length;
    e.unique_hotel_codes = hotels.length;
    e.candidate_rows_persisted = hotels.length;
    e.pagination_completed = true;
    e.search_completed_at = new Date().toISOString();
    e.hbx_status =
      hotels.length === 0
        ? HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS
        : HBX_DISCOVERY_STATUS.COMPLETE;
  }
  saveLedger(ledger);
  return { founder_stop: false };
}

function persistGeographyCandidates(g, hotels, waveId) {
  const candidates = hotels.map((h) => hotelToPackCandidate(h, g.name, waveId));
  writeJson(path.join(CANDIDATES_DIR, `${g.geography_id}.json`), {
    geography_id: g.geography_id,
    geography: g.name,
    generated_at: new Date().toISOString(),
    count: candidates.length,
    candidates,
  });
  return candidates;
}

function classifyAgainstUniverse(candidates, censusIndex, universeMerged) {
  const cventByNameCountry = new Map();
  for (const c of universeMerged) {
    const isCvent =
      c.source_type === "cvent_candidate" ||
      (c.merged_sources || []).includes("cvent_candidate");
    if (!isCvent) continue;
    const key = `${c.normalized_property_name}|${normalizeGeographyLabel(c.country)}`;
    if (!cventByNameCountry.has(key)) cventByNameCountry.set(key, c);
  }

  let existing = 0;
  let safe = 0;
  let hold = 0;
  let invalid = 0;
  let cventHbx = 0;
  const safeRows = [];

  for (const pack of candidates) {
    const candidate = {
      candidate_id: `hbx_${pack.hbx_hotel_code}`,
      property_name: pack.name,
      normalized_property_name: String(pack.name || "")
        .toLowerCase()
        .normalize("NFKD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/[^a-z0-9\s]/g, " ")
        .replace(/\s+/g, " ")
        .trim(),
      country: pack.country,
      city: pack.city,
      address: pack.address,
      website: pack.website,
      phone: pack.phonehotel,
      source_type: "hbx_content_api",
      merged_sources: ["hbx_content_api"],
      external_ids: { hbx_code: pack.hbx_hotel_code },
      brand_text: null,
      chain_text: pack.chain_code || null,
      hbx_category_code: pack.category || null,
    };
    const key = `${candidate.normalized_property_name}|${normalizeGeographyLabel(candidate.country)}`;
    if (cventByNameCountry.has(key)) {
      candidate.merged_sources = ["cvent_candidate", "hbx_content_api"];
      cventHbx += 1;
    }
    const cls = classifyAgainstCensus(candidate, censusIndex);
    pack.match_class = cls.match_class;
    pack.census_record_id = cls.census_record_id || null;
    if (
      cls.match_class === MATCH.EXISTING_HIGH ||
      cls.match_class === MATCH.EXISTING_MEDIUM
    ) {
      existing += 1;
    } else if (cls.match_class === MATCH.REJECT_NON_HOTEL) {
      invalid += 1;
    } else if (
      cls.match_class === MATCH.NEW_HIGH ||
      cls.match_class === MATCH.NEW_MEDIUM
    ) {
      if (pack.city && pack.hbx_hotel_code != null) {
        safe += 1;
        safeRows.push(candidate);
        pack.match_class = MATCH.NEW_HIGH;
      } else {
        hold += 1;
      }
    } else {
      hold += 1;
    }
  }

  return { existing, safe, hold, invalid, cventHbx, safeRows };
}

function markCventStatuses(ledger) {
  for (const e of Object.values(ledger.geographies)) {
    const hit = findCventLatamCountry(e.canonical_geography);
    const inReg = CVENT_LATAM_CARIBBEAN_COUNTRIES.some(
      (c) => normalizeGeographyLabel(c.country) === normalizeGeographyLabel(e.canonical_geography)
    );
    if (inReg || hit) {
      e.cvent_status = "SEARCHED_IN_REGISTRY";
    } else if (
      e.geography_id === "sint_eustatius" ||
      e.geography_id === "saba"
    ) {
      e.cvent_status = HBX_DISCOVERY_STATUS.COMPLETE_SOURCE_UNAVAILABLE;
    } else {
      e.cvent_status = "NOT_IN_CVENT_REGISTRY";
    }
  }
}

/**
 * Dry-run: validate queue + mappings; optional limited HBX probe.
 * Run/resume: full discovery + shell apply per geography.
 */
export async function runFullCalaHbxGeographyDiscoveryWaveV1(opts = {}) {
  const log = opts.log || (() => {});
  const mode = String(opts.mode || "dry-run").toLowerCase();
  const enableWrites = Boolean(
    opts.enableProductionWrites && (mode === "run" || mode === "resume")
  );
  const generated_at = new Date().toISOString();
  const maxGeographies =
    opts.maxGeographies != null ? Number(opts.maxGeographies) : Infinity;
  const skipShell = Boolean(opts.skipShell);
  const skipPostAudit = Boolean(opts.skipPostAudit);

  let token;
  let baseId;
  try {
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
    if (CENSUS_TABLE_ID !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
      throw new Error("target_table_id_mismatch");
    }
    token = resolvePat();
    const base = resolveTargetBase();
    baseId = base?.target_base_id || base?.baseId;
  } catch (err) {
    return founderStopReport(String(err?.message || err), { generated_at });
  }

  const cfg = resolveHbxConfig(opts.env || process.env);
  if (!cfg.ok) {
    return founderStopReport(`missing_hbx_credentials:${cfg.missing?.join(",")}`, {
      generated_at,
    });
  }

  // Bermuda must be in_scope
  const bermuda = listDealalityCalaGeographies({ includeScopeReview: true }).find(
    (g) => g.geography_id === "bermuda"
  );
  if (!bermuda || bermuda.scope !== "in_scope") {
    return founderStopReport("bermuda_not_in_scope_in_registry", { generated_at });
  }

  const ledger = initOrLoadLedger();
  // Correct prior false COMPLETE_ZERO_RESULTS (403/429 misclassified as empty).
  const repairedFalseZeros = repairFalseZeroResultLedgerEntries(ledger);
  if (repairedFalseZeros > 0) {
    log(
      `[hbx-geo] repaired ${repairedFalseZeros} false COMPLETE_ZERO_RESULTS entries`
    );
  }
  markCventStatuses(ledger);
  const waveId =
    mode === "dry-run"
      ? `dry_${generated_at.replace(/[:.]/g, "-")}`
      : ledger.run_id || `hbxgeo_${generated_at.replace(/[:.]/g, "-")}`;
  if (mode !== "dry-run") {
    ledger.run_id = waveId;
    ledger.wave_status = WAVE_STATUS.RUNNING;
  }
  saveLedger(ledger);

  const inScope = listDealalityCalaGeographies({ includeScopeReview: false });
  const completeBefore = inScope.filter((g) =>
    [HBX_DISCOVERY_STATUS.COMPLETE, HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS].includes(
      ledger.geographies[g.geography_id]?.hbx_status
    )
  ).length;

  const queue = inScope.filter((g) => {
    const st = ledger.geographies[g.geography_id]?.hbx_status;
    if (BES_GROUP.members.includes(g.geography_id)) return false; // handled as group
    return ![
      HBX_DISCOVERY_STATUS.COMPLETE,
      HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS,
      HBX_DISCOVERY_STATUS.UNSUPPORTED_GEOGRAPHY,
      HBX_DISCOVERY_STATUS.COMPLETE_SOURCE_UNAVAILABLE,
    ].includes(st);
  });

  if (mode === "dry-run") {
    const report = {
      ok: true,
      DISCOVERY_STATUS: WAVE_STATUS.DRY_RUN_READY,
      mode: "dry-run",
      production_writes: false,
      CANONICAL_IN_SCOPE_GEOGRAPHIES: inScope.length,
      HBX_GEOGRAPHIES_COMPLETE_BEFORE: completeBefore,
      HBX_GEOGRAPHIES_QUEUED: queue.length + 1, // + BES group
      queued_sample: queue.slice(0, 20).map((g) => g.name),
      bes_group: BES_GROUP.members,
      bermuda_in_scope: true,
      wave1_recognized_complete: [...WAVE1_COMPLETE],
      false_zero_results_repaired: repairedFalseZeros,
      FOUNDER_DECISION_REQUIRED: "NO",
      ledger_path:
        "data/research-engine-v2/full-cala-hbx-geography-discovery/hbx-geography-discovery-ledger.json",
      generated_at,
      NEXT_STEP:
        "Launch: npm run census:full-cala-hbx-geography-discovery -- --mode run --enable-production-writes",
    };
    writeJson(
      path.join(
        ROOT,
        "reports/research-engine-v2/full-cala-hbx-geography-discovery-dry-run.json"
      ),
      report
    );
    writeMd(
      path.join(
        ROOT,
        "reports/research-engine-v2/full-cala-hbx-geography-discovery-dry-run.md"
      ),
      `# HBX Geography Discovery Dry-Run\n\n**Status:** \`${report.DISCOVERY_STATUS}\`\n\n- In-scope: ${report.CANONICAL_IN_SCOPE_GEOGRAPHIES}\n- HBX complete before: ${report.HBX_GEOGRAPHIES_COMPLETE_BEFORE}\n- Queued: ${report.HBX_GEOGRAPHIES_QUEUED}\n- Bermuda in_scope: **true**\n- Wave1 complete: ${report.wave1_recognized_complete.join(", ")}\n`
    );
    log(`[hbx-geo] DRY-RUN ready queued=${report.HBX_GEOGRAPHIES_QUEUED}`);
    return report;
  }

  // Production / resume path — auth preflight before any geography claims
  const rateLimiter = createHbxRequestRateLimiter({
    minIntervalMs: Number(process.env.HBX_MIN_REQUEST_INTERVAL_MS || 1200),
    maxRequestsPerRun: Number(process.env.HBX_MAX_REQUESTS_PER_RUN || 800),
    maxRetriesOn429: Number(process.env.HBX_MAX_RETRIES_ON_429 || 4),
  });
  log(
    `[hbx-geo] rate-limiter minIntervalMs=${rateLimiter.minIntervalMs} budget=${rateLimiter.maxRequestsPerRun} concurrency=1`
  );

  const preflight = await preflightHbxReadable(cfg);
  if (!preflight.ok) {
    const quota =
      preflight.status === 403 &&
      /quota/i.test(String(preflight.error_message || ""));
    const reason = quota
      ? `test_daily_quota_exhausted:http_${preflight.status}`
      : preflight.status === 401 || preflight.status === 403
        ? `unknown_hbx_licensing_or_auth_issue:http_${preflight.status}`
        : `hbx_preflight_failed:http_${preflight.status || "network"}`;
    ledger.wave_status = WAVE_STATUS.FOUNDER_STOP;
    ledger.totals = {
      ...(ledger.totals || {}),
      hbx_preflight: {
        status: preflight.status,
        error_code: preflight.error_code,
        error_message: preflight.error_message,
      },
      stop_reason: reason,
    };
    saveLedger(ledger);
    const stop = founderStopReport(reason, {
      generated_at,
      ledger,
      geosAttempted: 0,
    });
    writeFounderStopArtifacts(stop, ledger, {
      repairedFalseZeros,
      preflight,
      completeBefore,
      inScopeCount: inScope.length,
      queueCount: queue.length,
    });
    log(`[hbx-geo] FOUNDER_STOP ${reason}`);
    return stop;
  }

  const censusBeforeIndex = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
  const censusBefore = censusBeforeIndex.count;
  let shellsThisRun = 0;
  let newHbxRows = 0;
  let newUniqueCodes = 0;
  let cventHbxMatches = 0;
  let holdsUpgraded = 0;
  let geosAttempted = 0;
  const zeroResult = [];
  const unsupported = [];
  const errors = [];

  // BES group first
  const besResult = await discoverBesGroup(cfg, ledger, {
    log,
    waveId,
    batchSize: Number(process.env.HBX_BATCH_SIZE || 100),
    maxHotelsPerCountry: Number(process.env.HBX_MAX_HOTELS_PER_COUNTRY || 25000),
    delayMs: Number(process.env.HBX_BATCH_DELAY_MS || 1200),
    rateLimiter,
  });
  geosAttempted += 3;
  rebuildMergedPack(ledger);
  if (besResult?.founder_stop) {
    ledger.wave_status = WAVE_STATUS.FOUNDER_STOP;
    saveLedger(ledger);
    const stop = founderStopReport(
      besResult.stop_reason || "bes_group_hbx_founder_stop",
      {
        generated_at,
        ledger,
        censusBefore,
        shellsThisRun,
        geosAttempted,
      }
    );
    writeFounderStopArtifacts(stop, ledger, {
      repairedFalseZeros,
      completeBefore,
      inScopeCount: inScope.length,
    });
    return stop;
  }

  let processed = 0;
  for (const g of queue) {
    if (processed >= maxGeographies) break;
    const entry = ledger.geographies[g.geography_id];
    if (
      [
        HBX_DISCOVERY_STATUS.COMPLETE,
        HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS,
        HBX_DISCOVERY_STATUS.UNSUPPORTED_GEOGRAPHY,
      ].includes(entry.hbx_status)
    ) {
      continue;
    }

    geosAttempted += 1;
    processed += 1;
    entry.hbx_status = HBX_DISCOVERY_STATUS.IN_PROGRESS;
    entry.search_started_at = new Date().toISOString();
    saveLedger(ledger);

    const result = await discoverGeographyHbx(g, cfg, {
      log,
      waveId,
      batchSize: Number(process.env.HBX_BATCH_SIZE || 100),
      maxHotelsPerCountry: Number(process.env.HBX_MAX_HOTELS_PER_COUNTRY || 25000),
      delayMs: Number(process.env.HBX_BATCH_DELAY_MS || 1200),
      rateLimiter,
    });

    entry.hbx_query_codes_tried = result.codes_tried || [];
    entry.errors = result.error ? [result.error] : [];
    entry.hbx_status = result.status;
    entry.search_completed_at = new Date().toISOString();
    entry.pagination_completed = ![
      HBX_DISCOVERY_STATUS.FAILED_RETRYABLE,
      HBX_DISCOVERY_STATUS.FAILED_REQUIRES_REVIEW,
    ].includes(result.status);
    entry.source_rows_returned = result.hotels.length;
    entry.unique_hotel_codes = result.hotels.length;

    if (
      result.founder_stop ||
      result.status === HBX_DISCOVERY_STATUS.FAILED_RETRYABLE ||
      result.status === HBX_DISCOVERY_STATUS.FAILED_REQUIRES_REVIEW
    ) {
      entry.retry_status =
        result.status === HBX_DISCOVERY_STATUS.FAILED_RETRYABLE
          ? "awaiting_resume"
          : "requires_founder_review";
      ledger.wave_status = WAVE_STATUS.FOUNDER_STOP;
      saveLedger(ledger);
      const stop = founderStopReport(
        result.stop_reason ||
          (result.status === HBX_DISCOVERY_STATUS.FAILED_RETRYABLE
            ? `hbx_pull_failed_retryable:${g.name}`
            : `hbx_pull_requires_review:${g.name}`),
        {
          generated_at,
          ledger,
          censusBefore,
          shellsThisRun,
          geosAttempted,
        }
      );
      writeFounderStopArtifacts(stop, ledger, {
        repairedFalseZeros,
        completeBefore,
        inScopeCount: inScope.length,
        failed_geography: g.name,
      });
      return stop;
    }
    if (result.status === HBX_DISCOVERY_STATUS.UNSUPPORTED_GEOGRAPHY) {
      unsupported.push(g.name);
    }
    if (result.status === HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS) {
      zeroResult.push(g.name);
    }

    if (result.hotels.length) {
      persistGeographyCandidates(g, result.hotels, waveId);
      entry.candidate_rows_persisted = result.hotels.length;
      newHbxRows += result.hotels.length;
      newUniqueCodes += result.hotels.length;
    }
    saveLedger(ledger);
    rebuildMergedPack(ledger);

    // Classify + shell apply for this geography
    if (!skipShell && result.hotels.length && enableWrites) {
      const pack = readJson(path.join(CANDIDATES_DIR, `${g.geography_id}.json`), {
        candidates: [],
      });
      const universe = loadMasterUniverseCandidates();
      const hbx = loadHbxCandidates();
      const { merged } = mergeCandidateUniverses(universe, hbx);
      const index = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
      const cls = classifyAgainstUniverse(pack.candidates, index, merged);
      entry.existing_census_matches = cls.existing;
      entry.safe_new_candidates = cls.safe;
      entry.hold_candidates = cls.hold;
      entry.invalid_candidates = cls.invalid;
      entry.cvent_plus_hbx_matches = cls.cventHbx;
      cventHbxMatches += cls.cventHbx;
      writeJson(path.join(CANDIDATES_DIR, `${g.geography_id}.json`), pack);
      saveLedger(ledger);
      rebuildMergedPack(ledger);

      if (cls.safe > 0) {
        log(
          `[hbx-geo] shell orchestrator focus=${g.name} safe≈${cls.safe}`
        );
        const orch = await runFullCala15kShellOrchestratorV1({
          mode: "run",
          enableProductionWrites: true,
          focusCountry: g.name,
          maxBatches: 20,
          log,
        });
        if (orch.FOUNDER_DECISION_REQUIRED === "YES") {
          entry.shells_inserted = orch.SHELLS_ADDED_THIS_RUN || 0;
          shellsThisRun += entry.shells_inserted;
          saveLedger(ledger);
          return {
            ...founderStopReport(orch.STOP_REASON || orch.FOUNDER_DECISION, {
              generated_at,
              ledger,
              censusBefore,
              shellsThisRun,
              geosAttempted,
            }),
            LAST_ORCHESTRATOR: {
              status: orch.ORCHESTRATOR_STATUS,
              stop: orch.STOP_REASON,
            },
          };
        }
        entry.shells_inserted = orch.SHELLS_ADDED_THIS_RUN || 0;
        shellsThisRun += entry.shells_inserted;
        saveLedger(ledger);
      }
    }

    await sleep(200);
  }

  rebuildMergedPack(ledger);

  // Final shell sweep for any remaining SAFE across all geos (e.g. hold upgrades)
  if (enableWrites && !skipShell) {
    log(`[hbx-geo] final shell orchestrator sweep…`);
    const orch = await runFullCala15kShellOrchestratorV1({
      mode: "run",
      enableProductionWrites: true,
      maxBatches: 50,
      log,
    });
    if (orch.FOUNDER_DECISION_REQUIRED === "YES") {
      return founderStopReport(orch.STOP_REASON || orch.FOUNDER_DECISION, {
        generated_at,
        ledger,
        censusBefore,
        shellsThisRun: shellsThisRun + (orch.SHELLS_ADDED_THIS_RUN || 0),
        geosAttempted,
      });
    }
    shellsThisRun += orch.SHELLS_ADDED_THIS_RUN || 0;
  }

  const censusAfterIndex = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
  const censusAfter = censusAfterIndex.count;

  const completeAfter = inScope.filter((g) =>
    [
      HBX_DISCOVERY_STATUS.COMPLETE,
      HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS,
      HBX_DISCOVERY_STATUS.UNSUPPORTED_GEOGRAPHY,
      HBX_DISCOVERY_STATUS.COMPLETE_SOURCE_UNAVAILABLE,
    ].includes(ledger.geographies[g.geography_id]?.hbx_status)
  ).length;

  ledger.wave_status = WAVE_STATUS.COMPLETE;
  ledger.totals = {
    shells_this_run: shellsThisRun,
    new_hbx_rows: newHbxRows,
    census_before: censusBefore,
    census_after: censusAfter,
  };
  saveLedger(ledger);

  let geoAudit = null;
  if (!skipPostAudit) {
    log(`[hbx-geo] post-discovery geography coverage audit…`);
    geoAudit = await runFullCalaGeographyCoverageRegistryAuditV1({ log });
  }

  const holds = readJson(
    path.join(
      ROOT,
      "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json"
    ),
    { by_candidate_id: {} }
  );

  const final = {
    ok: true,
    DISCOVERY_STATUS: WAVE_STATUS.COMPLETE,
    mode,
    production_writes: enableWrites,
    production_table_id: CENSUS_TABLE_ID,
    CANONICAL_IN_SCOPE_GEOGRAPHIES: inScope.length,
    HBX_GEOGRAPHIES_COMPLETE_BEFORE: completeBefore,
    HBX_GEOGRAPHIES_ATTEMPTED_THIS_RUN: geosAttempted,
    HBX_GEOGRAPHIES_COMPLETE_AFTER: completeAfter,
    HBX_UNSUPPORTED_GEOGRAPHIES: unsupported,
    HBX_ZERO_RESULT_GEOGRAPHIES: zeroResult,
    NEW_HBX_SOURCE_ROWS: newHbxRows,
    NEW_UNIQUE_HBX_HOTEL_CODES: newUniqueCodes,
    NEW_CVENT_HBX_MATCHES: cventHbxMatches,
    EXISTING_HOLD_CANDIDATES_UPGRADED_TO_SAFE: holdsUpgraded,
    NEW_SAFE_CANDIDATES: null,
    NEW_SHELLS_INSERTED: shellsThisRun,
    CENSUS_BEFORE: censusBefore,
    CENSUS_AFTER: censusAfter,
    TOTAL_HOLDS_AFTER: Object.keys(holds.by_candidate_id || {}).length,
    GEOGRAPHIES_WITH_ZERO_CENSUS_AFTER:
      geoAudit?.GEOGRAPHIES_WITH_ZERO_CENSUS_RECORDS ?? null,
    GEOGRAPHIES_WITH_SOURCE_GAPS_AFTER:
      geoAudit?.GEOGRAPHIES_WITH_SOURCE_GAPS ?? null,
    TOP_REMAINING_SOURCE_GAPS:
      geoAudit?.TOP_10_SOURCE_GAP_DISCOVERY_PRIORITIES?.slice(0, 10) ?? null,
    FOUNDER_DECISION_REQUIRED: "NO",
    FOUNDER_DECISION: null,
    ledger_path:
      "data/research-engine-v2/full-cala-hbx-geography-discovery/hbx-geography-discovery-ledger.json",
    merged_pack:
      "reports/research-engine-v2/hbx-cala-full-geography-candidate-pack.json",
    geography_audit_paths: geoAudit?.report_paths || null,
    generated_at,
    errors: errors.slice(0, 20),
  };

  writeJson(
    path.join(
      ROOT,
      "reports/research-engine-v2/full-cala-hbx-geography-discovery-final.json"
    ),
    final
  );
  writeMd(
    path.join(
      ROOT,
      "reports/research-engine-v2/full-cala-hbx-geography-discovery-final.md"
    ),
    renderFinalMd(final)
  );
  writeMd(
    path.join(
      ROOT,
      "docs/data-intelligence/full-cala-hbx-geography-discovery-wave-v1.md"
    ),
    renderFinalMd(final)
  );

  log(
    `[hbx-geo] COMPLETE shells=${shellsThisRun} census ${censusBefore}→${censusAfter} geos_attempted=${geosAttempted}`
  );
  return final;
}

function founderStopReport(reason, ctx = {}) {
  if (ctx.ledger) {
    ctx.ledger.wave_status = WAVE_STATUS.FOUNDER_STOP;
    ctx.ledger.totals = {
      ...(ctx.ledger.totals || {}),
      stop_reason: reason,
    };
    saveLedger(ctx.ledger);
  }
  return {
    ok: false,
    DISCOVERY_STATUS: WAVE_STATUS.FOUNDER_STOP,
    STOP_REASON: reason,
    FOUNDER_DECISION_REQUIRED: "YES",
    FOUNDER_DECISION: reason,
    CENSUS_BEFORE: ctx.censusBefore ?? null,
    CENSUS_AFTER: ctx.censusBefore ?? null,
    NEW_SHELLS_INSERTED: ctx.shellsThisRun ?? 0,
    NEW_HBX_SOURCE_ROWS: 0,
    NEW_UNIQUE_HBX_HOTEL_CODES: 0,
    HBX_GEOGRAPHIES_ATTEMPTED_THIS_RUN: ctx.geosAttempted ?? 0,
    production_writes: false,
    generated_at: ctx.generated_at,
    ledger_path:
      "data/research-engine-v2/full-cala-hbx-geography-discovery/hbx-geography-discovery-ledger.json",
    resume_command:
      "ALLOW_CENSUS_AUTOPILOT_APPLY=1 CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 CONFIRM_NO_BRAND_EXPLORER_WRITES=1 CONFIRM_NO_BRAND_SETUP_WRITES=1 ENABLE_FULL_CALA_15K_CENSUS_SHELL=1 ENABLE_CENSUS_SHELL_INSERTS=1 ENABLE_CURRENT_BRAND_WRITES=0 ENABLE_BRAND_FAMILY_WRITES=0 ENABLE_ROOMS_WRITES=0 npm run census:full-cala-hbx-geography-discovery -- --mode resume --enable-production-writes",
  };
}

function writeFounderStopArtifacts(stop, ledger, extra = {}) {
  const failed = Object.values(ledger?.geographies || {}).filter((e) =>
    [
      HBX_DISCOVERY_STATUS.FAILED_RETRYABLE,
      HBX_DISCOVERY_STATUS.FAILED_REQUIRES_REVIEW,
    ].includes(e.hbx_status)
  );
  const complete = Object.values(ledger?.geographies || {}).filter((e) =>
    [
      HBX_DISCOVERY_STATUS.COMPLETE,
      HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS,
    ].includes(e.hbx_status)
  );
  const report = {
    ...stop,
    CANONICAL_IN_SCOPE_GEOGRAPHIES: extra.inScopeCount ?? null,
    HBX_GEOGRAPHIES_COMPLETE_BEFORE: extra.completeBefore ?? null,
    HBX_GEOGRAPHIES_COMPLETE_AFTER: complete.length,
    HBX_UNSUPPORTED_GEOGRAPHIES: [],
    HBX_ZERO_RESULT_GEOGRAPHIES: Object.values(ledger?.geographies || {})
      .filter((e) => e.hbx_status === HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS)
      .map((e) => e.canonical_geography),
    HBX_FAILED_GEOGRAPHIES: failed.map((e) => ({
      geography: e.canonical_geography,
      status: e.hbx_status,
      errors: e.errors,
    })),
    NEW_CVENT_HBX_MATCHES: 0,
    EXISTING_HOLD_CANDIDATES_UPGRADED_TO_SAFE: 0,
    NEW_SAFE_CANDIDATES: 0,
    TOTAL_HOLDS_AFTER: null,
    GEOGRAPHIES_WITH_ZERO_CENSUS_AFTER: null,
    GEOGRAPHIES_WITH_SOURCE_GAPS_AFTER: null,
    TOP_REMAINING_SOURCE_GAPS: null,
    false_zero_results_repaired: extra.repairedFalseZeros ?? 0,
    hbx_preflight: extra.preflight || null,
    POLICY_DECISION_REQUIRED:
      "Restore working HBX Content API credentials/licensing (current key returns HTTP 403 on /hotel-api/1.0/status and /hotel-content-api/1.0/hotels for Wave1 countries and remaining CALA). Do not treat prior COMPLETE_ZERO_RESULTS from this failed wave as genuine empty searches. After auth is restored, resume with --mode resume.",
  };
  writeJson(
    path.join(
      ROOT,
      "reports/research-engine-v2/full-cala-hbx-geography-discovery-final.json"
    ),
    report
  );
  writeMd(
    path.join(
      ROOT,
      "reports/research-engine-v2/full-cala-hbx-geography-discovery-final.md"
    ),
    renderFinalMd(report)
  );
  writeMd(
    path.join(
      ROOT,
      "docs/data-intelligence/full-cala-hbx-geography-discovery-wave-v1.md"
    ),
    renderFinalMd(report)
  );
  return report;
}

function renderFinalMd(r) {
  return `# Full CALA HBX Geography Discovery Wave

**DISCOVERY_STATUS:** \`${r.DISCOVERY_STATUS}\`  
**FOUNDER_DECISION_REQUIRED:** **${r.FOUNDER_DECISION_REQUIRED}**  
${r.FOUNDER_DECISION ? `**Decision:** ${r.FOUNDER_DECISION}` : ""}
${r.POLICY_DECISION_REQUIRED ? `\n## Policy / technical decision\n\n${r.POLICY_DECISION_REQUIRED}\n` : ""}

| Metric | Value |
| --- | ---: |
| In-scope geographies | ${r.CANONICAL_IN_SCOPE_GEOGRAPHIES ?? "—"} |
| HBX complete before | ${r.HBX_GEOGRAPHIES_COMPLETE_BEFORE ?? "—"} |
| Attempted this run | ${r.HBX_GEOGRAPHIES_ATTEMPTED_THIS_RUN ?? "—"} |
| HBX complete after | ${r.HBX_GEOGRAPHIES_COMPLETE_AFTER ?? "—"} |
| Failed (auth/retry) | ${(r.HBX_FAILED_GEOGRAPHIES || []).length} |
| New HBX source rows | ${r.NEW_HBX_SOURCE_ROWS ?? 0} |
| New shells inserted | ${r.NEW_SHELLS_INSERTED ?? 0} |
| Census before → after | ${r.CENSUS_BEFORE ?? "—"} → ${r.CENSUS_AFTER ?? "—"} |
| Zero-result geos | ${(r.HBX_ZERO_RESULT_GEOGRAPHIES || []).length} |
| Unsupported geos | ${(r.HBX_UNSUPPORTED_GEOGRAPHIES || []).length} |

## Commands

\`\`\`bash
npm run census:full-cala-hbx-geography-discovery -- --mode dry-run
# after HBX auth restored:
ALLOW_CENSUS_AUTOPILOT_APPLY=1 CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 CONFIRM_NO_BRAND_EXPLORER_WRITES=1 CONFIRM_NO_BRAND_SETUP_WRITES=1 ENABLE_FULL_CALA_15K_CENSUS_SHELL=1 ENABLE_CENSUS_SHELL_INSERTS=1 ENABLE_CURRENT_BRAND_WRITES=0 ENABLE_BRAND_FAMILY_WRITES=0 ENABLE_ROOMS_WRITES=0 npm run census:full-cala-hbx-geography-discovery -- --mode resume --enable-production-writes
\`\`\`

Ledger: \`${r.ledger_path}\`
`;
}
