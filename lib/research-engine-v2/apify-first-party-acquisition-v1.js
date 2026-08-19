/**
 * Run / validate / harvest Apify first-party hotel Actors.
 * Production writes stay NULL_FILL via applyNullFillToRecords.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { runApifyActor, getApifyToken } from "../hotel-intelligence/apify/local-client.js";
import {
  APIFY_HOTEL_ACTOR_CATALOG,
  APIFY_USAGE_STATUS,
  SOURCE_CLASS,
  actorRefForApi,
  normalizeApifyHotelRow,
  fieldAvailabilityFromRows,
  evaluateActorApproval,
  emptyActorMatrixRow,
  buildApifyHarvestPatch,
  similarText,
  haversineMeters,
  APPROVAL_THRESHOLDS,
  isCalaCountry,
} from "./apify-first-party-extractor-v1.js";
import {
  loadApifyHotelSourceMatrix,
  saveApifyHotelSourceMatrix,
  upsertActorMatrixRow,
} from "./apify-hotel-source-matrix-v1.js";
import {
  scorePortfolioToCensus,
  MAP_BRAND,
} from "./master-brand-portfolio-validation-v1.js";
import { MAP_MASTER } from "./master-census-enrichment-v1.js";
import { buildCanonicalBrandDictionary } from "./census-brand-canonical-dictionary.js";
import { loadRoomsCorroborationQueue } from "./rooms-candidate-corroboration-v1.js";
import {
  loadSourceRegistry,
  saveSourceRegistry,
  upsertSource,
  bumpSourceStats,
  markSourceState,
} from "./source-acquisition-registry-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");
const SAMPLE_DIR = path.join(ROOT, "data/research-engine-v2/apify-first-party/samples");

export const APIFY_ACQUISITION_VERSION = "apify-first-party-acquisition-v1";

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
}

export function loadPacksFromSampleCache() {
  const packs = [];
  if (!fs.existsSync(SAMPLE_DIR)) return packs;
  const catalogById = new Map(APIFY_HOTEL_ACTOR_CATALOG.map((a) => [a.ACTOR_ID, a]));
  for (const name of fs.readdirSync(SAMPLE_DIR)) {
    if (!name.endsWith(".json")) continue;
    try {
      const json = JSON.parse(fs.readFileSync(path.join(SAMPLE_DIR, name), "utf8"));
      const actor = catalogById.get(json.actor_id);
      if (!actor || !Array.isArray(json.items)) continue;
      packs.push({
        actor,
        rows: json.items.map((raw) => normalizeApifyHotelRow(actor, raw)),
      });
    } catch {
      // ignore corrupt sample files
    }
  }
  return packs;
}

function rowToPortfolio(row) {
  return {
    company: row.company,
    brand: row.brand,
    name: row.name,
    url: row.url,
    city: row.city,
    country: row.country || row.country_code,
    state: row.state,
    address: row.address,
    postal: row.postal,
    phone: row.phone,
    property_code: row.property_code,
    host: row.host,
    name_key: row.name_key,
    city_key: row.city_key,
    country_norm: row.country_norm,
  };
}

export function matchApifyRowToCensus(row, censusRecords = []) {
  let best = null;
  const portfolio = rowToPortfolio(row);
  for (const rec of censusRecords) {
    const fields = rec.fields || {};
    const scored = scorePortfolioToCensus(portfolio, fields);
    if (!best || scored.score > best.scored.score) {
      best = { rec, scored };
    }
  }
  if (!best) return { confidence: "none", score: 0, rec: null, reasons: ["no_census"] };
  return {
    rec: best.rec,
    scored: best.scored,
    confidence: best.scored.confidence,
    score: best.scored.score,
    reasons: best.scored.reasons,
  };
}

export function uniqueHighMatches(rows, censusRecords) {
  const used = new Set();
  const matches = [];
  for (const row of rows) {
    const hit = matchApifyRowToCensus(row, censusRecords);
    if (hit.confidence !== "high" || !hit.rec) continue;
    if (used.has(hit.rec.id)) continue;
    used.add(hit.rec.id);
    matches.push({ row, rec: hit.rec, scored: hit.scored });
  }
  return matches;
}

function compareField(censusVal, actorVal, kind) {
  if (isBlank(censusVal) || isBlank(actorVal) && actorVal !== 0) {
    return { compared: false, match: null };
  }
  if (kind === "rooms") {
    return { compared: true, match: Number(censusVal) === Number(actorVal) };
  }
  if (kind === "postal") {
    const a = String(censusVal).replace(/\s+/g, "").toUpperCase();
    const b = String(actorVal).replace(/\s+/g, "").toUpperCase();
    return { compared: true, match: a === b || a.includes(b) || b.includes(a) };
  }
  return { compared: true, match: similarText(censusVal, actorVal) >= 0.72 };
}

export function validateMatches(actor, matches) {
  let brandOk = 0;
  let brandN = 0;
  let roomsOk = 0;
  let roomsN = 0;
  let addrOk = 0;
  let addrN = 0;
  let cityOk = 0;
  let cityN = 0;
  let stateOk = 0;
  let stateN = 0;
  let postalOk = 0;
  let postalN = 0;
  let phoneOk = 0;
  let phoneN = 0;
  let webOk = 0;
  let webN = 0;
  let coordOk = 0;
  let coordN = 0;
  let roomsGuestroom = actor.ROOMS_SEMANTICS
    ? /guestroom|total_rooms|property guestroom/i.test(String(actor.ROOMS_SEMANTICS))
    : false;
  if (actor.FORBID_ROOMS_FIELD) roomsGuestroom = false;

  for (const m of matches) {
    const f = m.rec.fields || {};
    const r = m.row;
    const b = compareField(f[MAP_MASTER.currentBrand], r.brand, "text");
    if (b.compared) {
      brandN += 1;
      if (b.match) brandOk += 1;
    }
    if (!r.rooms_forbidden && r.rooms != null) {
      const rm = compareField(f[MAP_MASTER.roomsKeys], r.rooms, "rooms");
      if (rm.compared) {
        roomsN += 1;
        if (rm.match) roomsOk += 1;
      }
    }
    const a = compareField(f[MAP_MASTER.address], r.address, "text");
    if (a.compared) {
      addrN += 1;
      if (a.match) addrOk += 1;
    }
    const c = compareField(f[MAP_MASTER.city], r.city, "text");
    if (c.compared) {
      cityN += 1;
      if (c.match) cityOk += 1;
    }
    const s = compareField(f[MAP_MASTER.stateRegion], r.state, "text");
    if (s.compared) {
      stateN += 1;
      if (s.match) stateOk += 1;
    }
    const p = compareField(f[MAP_MASTER.postalCode], r.postal, "postal");
    if (p.compared) {
      postalN += 1;
      if (p.match) postalOk += 1;
    }
    const ph = compareField(f[MAP_MASTER.phone], r.phone, "text");
    if (ph.compared) {
      phoneN += 1;
      if (ph.match) phoneOk += 1;
    }
    const w = compareField(f[MAP_MASTER.officialUrl], r.url, "text");
    if (w.compared) {
      webN += 1;
      if (w.match) webOk += 1;
    }
    if (r.lat != null && r.lng != null && !isBlank(f[MAP_MASTER.latitude])) {
      coordN += 1;
      const meters = haversineMeters(
        { lat: Number(f[MAP_MASTER.latitude]), lng: Number(f[MAP_MASTER.longitude]) },
        { lat: r.lat, lng: r.lng }
      );
      if (meters != null && meters <= APPROVAL_THRESHOLDS.coordinate_meters) coordOk += 1;
    }
  }

  const ratio = (ok, n) => (n > 0 ? ok / n : null);
  return {
    BRAND_COMPARED: brandN,
    BRAND_ACCURACY: ratio(brandOk, brandN),
    ROOMS_COMPARED: roomsN,
    ROOM_ACCURACY: ratio(roomsOk, roomsN),
    ADDRESS_ACCURACY: ratio(addrOk, addrN),
    CITY_ACCURACY: ratio(cityOk, cityN),
    STATE_ACCURACY: ratio(stateOk, stateN),
    POSTAL_ACCURACY: ratio(postalOk, postalN),
    PHONE_ACCURACY: ratio(phoneOk, phoneN),
    WEBSITE_ACCURACY: ratio(webOk, webN),
    COORDS_COMPARED: coordN,
    COORDINATE_ACCURACY: ratio(coordOk, coordN),
    ROOMS_SEMANTICS_GUESTROOM: roomsGuestroom,
    FIELD_SEMANTICS_UNDERSTOOD: true,
    ACCESS_POLICY_OK: true,
  };
}

async function runActorSample(actor, opts = {}) {
  if (actor.SKIP_LIVE_SAMPLE) {
    return {
      skipped: true,
      reason: actor.SKIP_REASON,
      items: [],
      status: "SKIPPED",
      usage_total_usd: 0,
    };
  }
  if (!getApifyToken(opts.env || process.env)) {
    return {
      skipped: true,
      reason: "APIFY_TOKEN_missing",
      items: [],
      status: "SKIPPED",
      usage_total_usd: 0,
    };
  }
  const input = { ...(actor.SAMPLE_INPUT || {}), ...(opts.input || {}) };
  const result = await runApifyActor({
    actorId: actorRefForApi(actor.ACTOR_ID),
    input,
    waitSecs: opts.waitSecs ?? 180,
    memoryMbytes: actor.MEMORY_MB || 1024,
    maxTotalChargeUsd: actor.MAX_CHARGE_USD || 0.4,
    env: opts.env,
  });
  return {
    skipped: false,
    items: result.items || [],
    status: result.status,
    run_id: result.run_id,
    usage_total_usd: result.usage_total_usd,
    dataset_id: result.dataset_id,
  };
}

export function summarizeActorSample(actor, run, censusRecords = []) {
  const rows = (run.items || []).map((raw) => normalizeApifyHotelRow(actor, raw));
  const errorPages = rows.filter((r) =>
    /something went wrong|access denied|just a moment/i.test(String(r.name || ""))
  );
  const usable = rows.filter((r) => !errorPages.includes(r) && r.name);
  const calaRows = usable.filter((r) => r.cala);
  const matchPool = calaRows.length >= 8 ? calaRows : usable;
  const matches = uniqueHighMatches(matchPool, censusRecords);
  const identityDenom = matchPool.filter((r) => r.name && (r.country || r.country_code)).length;
  const identityAccuracy = identityDenom ? matches.length / identityDenom : 0;
  const fieldMetrics = validateMatches(actor, matches);
  const availability = fieldAvailabilityFromRows(usable);
  let stability = run.status || "UNKNOWN";
  if (rows.length && errorPages.length / rows.length >= 0.5) {
    stability = "ERROR_PAGES";
  }
  const metrics = {
    SAMPLE_SIZE: usable.length,
    HIGH_MATCHES: matches.length,
    IDENTITY_ACCURACY: identityAccuracy,
    TECHNICAL_STABILITY: stability,
    CALA_ROWS: calaRows.length,
    ERROR_PAGES: errorPages.length,
    ...fieldMetrics,
  };
  const gate = evaluateActorApproval(actor, metrics);
  if (stability === "ERROR_PAGES") {
    gate.ok = false;
    gate.status = APIFY_USAGE_STATUS.USAGE_REVIEW;
    gate.reasons = [...(gate.reasons || []), "actor_returned_error_page_titles"];
  }
  return {
    rows,
    calaRows,
    matches,
    availability,
    metrics,
    gate,
  };
}

export async function inventoryAndSampleApifyActors(opts = {}) {
  const log = opts.log || (() => {});
  const censusRecords = opts.censusRecords || [];
  const matrix = loadApifyHotelSourceMatrix();
  const tested = [];
  let totalCost = 0;
  const catalog = opts.catalog || APIFY_HOTEL_ACTOR_CATALOG;

  for (const actor of catalog) {
    if (actor.DEFAULT_STATUS && actor.SKIP_LIVE_SAMPLE) {
      upsertActorMatrixRow(
        matrix,
        emptyActorMatrixRow(actor, {
          OVERALL_STATUS: actor.DEFAULT_STATUS,
          USAGE_STATUS: actor.DEFAULT_STATUS,
          NOTES: actor.SKIP_REASON,
        })
      );
      continue;
    }
    if (opts.onlyCompanies && !opts.onlyCompanies.includes(actor.HOTEL_COMPANY)) {
      continue;
    }
    log(`[apify] sampling ${actor.ACTOR_ID}`);
    let run;
    try {
      run = await (opts.runActorFn || runActorSample)(actor, opts);
    } catch (err) {
      log(`[apify] ${actor.ACTOR_ID} failed: ${String(err?.message || err).slice(0, 180)}`);
      upsertActorMatrixRow(
        matrix,
        emptyActorMatrixRow(actor, {
          OVERALL_STATUS: APIFY_USAGE_STATUS.USAGE_REVIEW,
          USAGE_STATUS: APIFY_USAGE_STATUS.USAGE_REVIEW,
          NOTES: String(err?.message || err).slice(0, 240),
        })
      );
      continue;
    }
    totalCost += Number(run.usage_total_usd || 0);
    if (run.run_id) {
      writeJson(path.join(SAMPLE_DIR, `${actor.HOTEL_COMPANY || "x"}-${run.run_id}.json`), {
        actor_id: actor.ACTOR_ID,
        run_id: run.run_id,
        status: run.status,
        usage_total_usd: run.usage_total_usd,
        item_count: (run.items || []).length,
        items: (run.items || []).slice(0, 80),
      });
    }
    const summary = summarizeActorSample(actor, run, censusRecords);
    tested.push({ actor, run, summary });
    upsertActorMatrixRow(
      matrix,
      emptyActorMatrixRow(actor, {
        availability: summary.availability,
        SAMPLE_SIZE: summary.metrics.SAMPLE_SIZE,
        IDENTITY_ACCURACY: summary.metrics.IDENTITY_ACCURACY,
        BRAND_ACCURACY: summary.metrics.BRAND_ACCURACY,
        ROOM_ACCURACY: summary.metrics.ROOM_ACCURACY,
        OVERALL_STATUS: summary.gate.status,
        USAGE_STATUS: summary.gate.status,
        TOTAL_APIFY_COST: run.usage_total_usd || 0,
        ROOMS_APPROVED: summary.gate.rooms_approved === true,
        COORDS_APPROVED: summary.gate.coords_approved === true,
        NOTES: (summary.gate.reasons || []).join("; "),
      })
    );
    log(
      `[apify] ${actor.HOTEL_COMPANY} status=${summary.gate.status} n=${summary.metrics.SAMPLE_SIZE} high=${summary.metrics.HIGH_MATCHES} cost=${run.usage_total_usd}`
    );
  }

  const fp = saveApifyHotelSourceMatrix(matrix);
  return {
    ok: true,
    matrix_path: fp,
    matrix,
    tested,
    TOTAL_APIFY_COST: totalCost,
    SOURCE_CLASS,
  };
}

export function refreshMatrixFromCachedSamples(censusRecords = []) {
  const matrix = loadApifyHotelSourceMatrix();
  if (!fs.existsSync(SAMPLE_DIR)) {
    return { matrix, packs: [], TOTAL_APIFY_COST: 0 };
  }
  const catalogById = new Map(APIFY_HOTEL_ACTOR_CATALOG.map((a) => [a.ACTOR_ID, a]));
  const byActor = new Map();
  for (const name of fs.readdirSync(SAMPLE_DIR)) {
    if (!name.endsWith(".json")) continue;
    try {
      const json = JSON.parse(fs.readFileSync(path.join(SAMPLE_DIR, name), "utf8"));
      const actor = catalogById.get(json.actor_id);
      if (!actor) continue;
      const cur = byActor.get(actor.ACTOR_ID) || {
        actor,
        items: [],
        status: json.status || "SUCCEEDED",
        cost: 0,
      };
      cur.items.push(...(json.items || []));
      cur.cost += Number(json.usage_total_usd || 0);
      if (json.status) cur.status = json.status;
      byActor.set(actor.ACTOR_ID, cur);
    } catch {
      // ignore
    }
  }
  const packs = [];
  let totalCost = 0;
  for (const cur of byActor.values()) {
    totalCost += cur.cost;
    const summary = summarizeActorSample(
      cur.actor,
      { items: cur.items, status: cur.status },
      censusRecords
    );
    packs.push({ actor: cur.actor, rows: summary.rows });
    upsertActorMatrixRow(
      matrix,
      emptyActorMatrixRow(cur.actor, {
        availability: summary.availability,
        SAMPLE_SIZE: summary.metrics.SAMPLE_SIZE,
        IDENTITY_ACCURACY: summary.metrics.IDENTITY_ACCURACY,
        BRAND_ACCURACY: summary.metrics.BRAND_ACCURACY,
        ROOM_ACCURACY: summary.metrics.ROOM_ACCURACY,
        OVERALL_STATUS: summary.gate.status,
        USAGE_STATUS: summary.gate.status,
        TOTAL_APIFY_COST: cur.cost,
        ROOMS_APPROVED: summary.gate.rooms_approved === true,
        COORDS_APPROVED: summary.gate.coords_approved === true,
        NOTES: (summary.gate.reasons || []).join("; "),
      })
    );
  }
  saveApifyHotelSourceMatrix(matrix);
  return { matrix, packs, TOTAL_APIFY_COST: totalCost };
}

export async function expandApprovedActors(opts = {}) {
  const log = opts.log || (() => {});
  const censusRecords = opts.censusRecords || [];
  const matrix = loadApifyHotelSourceMatrix();
  const approvedIds = new Set(
    (matrix.actors || [])
      .filter((a) => a.USAGE_STATUS === APIFY_USAGE_STATUS.APPROVED)
      .map((a) => a.ACTOR_ID)
  );
  let extraCost = 0;
  const extraPacks = [];
  for (const actor of APIFY_HOTEL_ACTOR_CATALOG) {
    if (!approvedIds.has(actor.ACTOR_ID)) continue;
    const locations = actor.HARVEST_LOCATIONS || [];
    for (const location of locations) {
      if (location === (actor.SAMPLE_INPUT?.location || "Cancun") && !opts.repeatSampleLocation) {
        continue;
      }
      log(`[apify] expand ${actor.HOTEL_COMPANY} location=${location}`);
      try {
        const run = await runActorSample(actor, {
          ...opts,
          input: { ...(actor.SAMPLE_INPUT || {}), location, maxItems: actor.SAMPLE_INPUT?.maxItems || 40 },
        });
        extraCost += Number(run.usage_total_usd || 0);
        if (run.run_id) {
          writeJson(path.join(SAMPLE_DIR, `${actor.HOTEL_COMPANY}-${location.replace(/\s+/g, "_")}-${run.run_id}.json`), {
            actor_id: actor.ACTOR_ID,
            run_id: run.run_id,
            status: run.status,
            usage_total_usd: run.usage_total_usd,
            item_count: (run.items || []).length,
            items: run.items || [],
          });
        }
        extraPacks.push({
          actor,
          rows: (run.items || []).map((raw) => normalizeApifyHotelRow(actor, raw)),
        });
        log(`[apify] expand ${location} n=${(run.items || []).length} status=${run.status} cost=${run.usage_total_usd}`);
      } catch (err) {
        log(`[apify] expand failed ${location}: ${String(err?.message || err).slice(0, 180)}`);
      }
    }
  }
  return { extraPacks, extraCost, matrix: refreshMatrixFromCachedSamples(censusRecords).matrix };
}

function tally(acc, counts) {
  for (const [k, v] of Object.entries(counts || {})) {
    acc[k] = Number(acc[k] || 0) + Number(v || 0);
  }
  return acc;
}

export function harvestApprovedApifyRows(opts = {}) {
  const records = opts.censusRecords || [];
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary({});
  const matrix = opts.matrix || loadApifyHotelSourceMatrix();
  const approved = new Map(
    (matrix.actors || [])
      .filter((a) => a.USAGE_STATUS === APIFY_USAGE_STATUS.APPROVED)
      .map((a) => [a.ACTOR_ID, a])
  );
  const proposals = [];
  const totals = {
    CURRENT_BRAND_WRITES: 0,
    BRAND_FAMILY_DERIVATIONS: 0,
    ROOMS_WRITES: 0,
    ROOM_CANDIDATES_CORROBORATED: 0,
    ADDRESS_PATCHES: 0,
    POSTAL_PATCHES: 0,
    STATE_PATCHES: 0,
    CITY_PATCHES: 0,
    COORDINATE_PATCHES: 0,
    PHONE_PATCHES: 0,
    WEBSITE_PATCHES: 0,
  };
  let highMatches = 0;
  const roomsQueue = loadRoomsCorroborationQueue();
  const roomsById = new Map(roomsQueue.map((c) => [c.id, c]));

  const packsByActor = new Map();
  for (const pack of opts.packs || []) {
    const id = pack.actor?.ACTOR_ID;
    if (!id) continue;
    const prev = packsByActor.get(id);
    if (!prev) packsByActor.set(id, { actor: pack.actor, rows: [...(pack.rows || [])] });
    else prev.rows.push(...(pack.rows || []));
  }
  for (const pack of packsByActor.values()) {
    const actor = pack.actor;
    const approval = approved.get(actor.ACTOR_ID);
    if (!approval) continue;
    const rows = (pack.rows || []).filter((r) => r.cala || opts.allowNonCala === true);
    const matches = uniqueHighMatches(rows, records);
    highMatches += matches.length;
    for (const m of matches) {
      const built = buildApifyHarvestPatch(m.rec.fields || {}, m.row, {
        dictionary,
        roomsApproved: approval.ROOMS_APPROVED === true,
        coordsApproved: approval.COORDS_APPROVED === true,
      });
      const cand = roomsById.get(m.rec.id);
      if (
        cand &&
        m.row.rooms != null &&
        Number(cand.rooms || cand.count) === Number(m.row.rooms) &&
        approval.ROOMS_APPROVED === true
      ) {
        built.counts.ROOM_CANDIDATES_CORROBORATED = 1;
        if (isBlank(m.rec.fields?.[MAP_MASTER.roomsKeys]) && !built.patch[MAP_MASTER.roomsKeys]) {
          built.patch[MAP_MASTER.roomsKeys] = Number(m.row.rooms);
          built.counts.ROOMS_WRITES = 1;
        }
      }
      if (!built.ok && !Object.keys(built.patch || {}).length) continue;
      proposals.push({ id: m.rec.id, fields: built.patch });
      tally(totals, built.counts);
    }
  }

  return {
    ok: true,
    proposals,
    CALA_PROPERTIES_MATCHED_HIGH: highMatches,
    ...totals,
    TOTAL_PRODUCTION_FIELDS_WRITTEN: proposals.reduce(
      (n, p) => n + Object.keys(p.fields || {}).length,
      0
    ),
  };
}

/**
 * Adaptive overnight phase: harvest approved Actors; sample untested first-party
 * Actors only when a token is present and cost budget remains.
 */
export async function runApifyFirstPartyAcquisition(opts = {}) {
  const log = opts.log || (() => {});
  const records = opts.censusRecords || [];
  let matrix = loadApifyHotelSourceMatrix();
  const hasApproved = (matrix.actors || []).some(
    (a) => a.USAGE_STATUS === APIFY_USAGE_STATUS.APPROVED
  );
  const alreadySampled = (matrix.actors || []).some((a) => Number(a.SAMPLE_SIZE) > 0);
  const packs = [...(opts.packs || [])];
  /** Production overnight: harvest approved Actors only — no Store discovery unless explicit. */
  const allowSample = opts.allowSample === true;

  if (!packs.length) {
    packs.push(...loadPacksFromSampleCache());
  }

  if (!hasApproved && allowSample) {
    const sampled = await inventoryAndSampleApifyActors({
      ...opts,
      censusRecords: records,
      onlyCompanies: opts.onlyCompanies || ["Hilton", "Marriott", "Choice", "IHG"],
    });
    matrix = sampled.matrix;
    for (const t of sampled.tested || []) {
      packs.push({ actor: t.actor, rows: t.summary.rows });
    }
  }

  const harvest = harvestApprovedApifyRows({
    censusRecords: records,
    matrix,
    packs,
    dictionary: opts.dictionary,
  });

  const registry = loadSourceRegistry();
  for (const row of matrix.actors || []) {
    if (row.USAGE_STATUS !== APIFY_USAGE_STATUS.APPROVED) continue;
    const id = `apify:${row.ACTOR_ID}`;
    const entry = upsertSource(registry, {
      SOURCE_ID: id,
      DOMAIN: String(row.UNDERLYING_SOURCE || "").replace(/^https?:\/\//, ""),
      COMPANY: row.HOTEL_COMPANY,
      SOURCE_TYPE: SOURCE_CLASS,
      FIELDS_AVAILABLE: row.FIELDS_AVAILABLE,
    });
    bumpSourceStats(entry, {
      REQUESTS: 1,
      HIGH_MATCHES: harvest.CALA_PROPERTIES_MATCHED_HIGH,
      FIELDS_WRITTEN: harvest.TOTAL_PRODUCTION_FIELDS_WRITTEN,
    });
    markSourceState(entry, "ACTIVE_HIGH_YIELD");
  }
  saveSourceRegistry(registry);

  log(
    `[apify] harvest high=${harvest.CALA_PROPERTIES_MATCHED_HIGH} fields=${harvest.TOTAL_PRODUCTION_FIELDS_WRITTEN} brand=${harvest.CURRENT_BRAND_WRITES} rooms=${harvest.ROOMS_WRITES}`
  );

  return {
    ok: true,
    ...harvest,
    exhausted: harvest.proposals.length === 0,
    APIFY_COST: Number(
      (matrix.actors || []).reduce((n, a) => n + Number(a.TOTAL_APIFY_COST || 0), 0)
    ),
    matrix,
    LIVE_SOURCE_PRIORITY_APPLIED: true,
  };
}

export function expectedCalaFilter(rows) {
  return (rows || []).filter((r) => isCalaCountry(r.country, r.country_code));
}

export { MAP_BRAND };
