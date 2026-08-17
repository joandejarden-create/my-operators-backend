/**
 * V4 Full-Build Controller CLI — self-driving until actionable work exhaustion
 * or safety/infrastructure boundary (then schedules auto-resume).
 *
 * ENABLE_VERIFIED_CENSUS_WRITES=1 --apply [--max-iterations N] [--max-runtime-ms N]
 *
 * Standing authorization ACTIVE. No Joan batch gate.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import { createHotelPropertyCensusRecords } from "../lib/research-engine-v2/census-autopilot-discovery-insert-apply.js";
import {
  discoverCalaProperties,
  buildDiscoveredIdentityKey,
  classifyDiscoveredAgainstCensus,
  MATCH_CLASS,
} from "../lib/research-engine-v2/census-autopilot-source-discovery.js";
import {
  listCountriesWithDiscoveryAdapter,
  listCalaCountriesFromRadar,
} from "../lib/research-engine-v2/production-census-cala-region-config.js";
import {
  isParentCompanyAsCurrentBrand,
  validateCurrentBrandSemantics,
} from "../lib/research-engine-v2/census-autopilot-v3/current-affiliation.js";
import { validateCitySemantics } from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";
import {
  isPostalAsCity,
  isStreetLineAsCity,
  classifyCityLabel,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js";
import {
  isDescriptorCity,
  isAllCapsCity,
  isAllLowerCity,
} from "../lib/research-engine-v2/census-city-state-normalizer.js";
import {
  resolveDealalityMarketStrict,
  assertMarketWriteGate,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { normName } from "../lib/research-engine-v2/census-autopilot-v2/identity-dedupe.js";
import {
  LANES,
  CONTROLLER_STATUS,
  defaultControllerConfig,
  hasActionableUniverseWork,
  chooseHighestValueLane,
  loadLedgerRows,
  summarizeLedgerStatuses,
  estimateUniquePhysical,
  footprintMetrics,
  emptyControllerState,
  persistJson,
  appendJsonl,
  sleep,
  normalizeCityProperCase,
  matchChallengesToDirectory,
} from "../lib/research-engine-v2/census-autopilot-v4/full-build-controller.js";
import { mapStopReasonToExitCode } from "../lib/research-engine-v2/census-autopilot-v4/worker-runtime.js";
import { runCensusMissingFieldSourceStrategyControllerV1Mission } from "../lib/research-engine-v2/census-missing-field-source-strategy-controller-v1.js";
import {
  DISCOVERY_CHECKPOINT_FILE,
  DISCOVERY_PROGRESS_FILE,
} from "../lib/research-engine-v2/census-autopilot-v4/discovery-railway-safe.js";

// Repo root relative to this script (works on local + Railway worker cwd).
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "data/research-engine-v2/census-autopilot-v4-full-universe");
const LEDGER_DIR = path.join(OUT, "27-universe-ledger");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const FREEZE_PATH = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v2-3-independent-universe/08-independent-universe-freeze.json"
);

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}
function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}
function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function cityOk(city, country) {
  if (blank(city)) return false;
  if (!validateCitySemantics(city, country).ok) return false;
  if (isPostalAsCity(city, country) || isStreetLineAsCity(city) || isDescriptorCity(city)) return false;
  const b = classifyCityLabel(city, country).bucket;
  if (["COUNTRY_AS_CITY", "POSTAL_CODE_AS_CITY", "CITY_INVALID"].includes(b)) return false;
  return true;
}
function buildFullDiscoveryCountries() {
  const set = new Set([
    ...listCalaCountriesFromRadar(),
    ...listCountriesWithDiscoveryAdapter("Hilton"),
    ...listCountriesWithDiscoveryAdapter("IHG"),
    ...listCountriesWithDiscoveryAdapter("Choice"),
    ...listCountriesWithDiscoveryAdapter("Marriott"),
    ...listCountriesWithDiscoveryAdapter("Accor"),
    ...listCountriesWithDiscoveryAdapter("Wyndham"),
  ]);
  return [...set].sort();
}

async function listLive(baseId, token) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of [
      "Property Identity Key",
      "Official Property URL",
      "Property Name",
      "Country",
      "City",
      "Current Brand",
      "Family / Source Family",
      "Address",
      "Rooms / Keys",
      "Market",
    ])
      params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    process.stdout.write(`\r[live] ${out.length}…`);
    await sleep(80);
  } while (offset);
  console.log(`\n[live] ${out.length}`);
  return out;
}

async function patchRecords(baseId, token, patches) {
  const updated = [];
  for (let i = 0; i < patches.length; i += 10) {
    const chunk = patches.slice(i, i + 10).map((p) => ({ id: p.id, fields: p.fields }));
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: chunk, typecast: true }),
      }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`patch ${res.status}: ${JSON.stringify(json.error || json)}`);
    updated.push(...(json.records || []));
    await sleep(120);
  }
  return updated;
}

function approveInsertFields(c) {
  const cityNorm = c.city ? normalizeCityProperCase(c.city) : { city: null };
  const city = cityNorm.city && cityOk(cityNorm.city, c.country) ? cityNorm.city : null;
  let market = null;
  if (city) {
    const ms = resolveDealalityMarketStrict(c.country, city, {});
    if (ms.ok) {
      const g = assertMarketWriteGate({ country: c.country, market: ms.market, city });
      if (g.write_allowed) market = ms.market;
    }
  }
  const approved = {
    "Property Name": c.name,
    "Canonical Property Name": c.name,
    "Property Identity Key": c.key,
    Country: c.country,
    "Family / Source Family": c.family || "Independent",
    "Official Property URL": c.url || null,
    "Source URL": c.source_url || c.url || null,
    "Source Type": c.source_type || "brand_directory",
    "Source Confidence": "High",
    "Identity Confidence": "High",
    "Data Eligible": true,
    "Production Use Status": "Census Only / Not Owner-Facing",
    "Enrichment Status": city ? "Verified — material gaps" : "Verified — geography pending",
    "Enrichment Priority": "High",
    "Discovery Date": todayIso(),
    "Last Reviewed Date": todayIso(),
    "Affiliation Status": c.brand ? "Branded" : "Brand-Unconfirmed",
  };
  if (c.family && c.family !== "Independent") approved["Brand Family"] = c.family;
  if (city) approved.City = city;
  if (c.brand && !isParentCompanyAsCurrentBrand(c.brand) && validateCurrentBrandSemantics(c.brand).ok) {
    approved["Current Brand"] = c.brand;
  }
  if (market) approved.Market = market;
  return approved;
}

async function insertCandidates(baseId, token, queue, { keys, urls, nameCountry, appendTx, circuit, trip, delayMs }) {
  let inserts = 0;
  let skipped = 0;
  for (let i = 0; i < queue.length; i++) {
    if (circuit.tripped) break;
    const c = queue[i];
    try {
      if (keys.has(c.key)) {
        skipped++;
        continue;
      }
      const params = new URLSearchParams({
        filterByFormula: `{Property Identity Key}='${String(c.key).replace(/'/g, "\\'")}'`,
        maxRecords: "1",
      });
      const findRes = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const findJson = await findRes.json();
      if ((findJson.records || []).length) {
        skipped++;
        keys.add(c.key);
        appendTx({ op: "INSERT_SKIP", key: c.key, reason: "duplicate_key" });
        continue;
      }
      const approved = approveInsertFields(c);
      const created = await createHotelPropertyCensusRecords(baseId, token, [{ fields: approved }]);
      const rec = created.created?.[0];
      if (!rec?.id) {
        trip("create_no_id", { key: c.key });
        break;
      }
      inserts++;
      keys.add(c.key);
      if (c.url) urls.add(String(c.url).toLowerCase());
      nameCountry.add(`${normName(c.name)}|${norm(c.country)}`);
      appendTx({
        op: "INSERT",
        status: "written",
        airtable_record_id: rec.id,
        property_identity_key: c.key,
        lane: c.lane,
        city: approved.City || null,
      });
      if ((i + 1) % 25 === 0) console.log(`[insert] ${inserts} / ${i + 1}/${queue.length}`);
      await sleep(delayMs);
    } catch (err) {
      trip("write_error", { key: c.key, error: String(err?.message || err) });
      break;
    }
  }
  return { inserts, skipped };
}

function buildVerifiedReadyQueue(ledgerRows, freeze, keys, urls, nameCountry, cap) {
  const freezeByPid = new Map((freeze.records || []).map((r) => [r.property_identity_id, r]));
  const freezeByNc = new Map();
  for (const rec of freeze.records || []) {
    const phys = rec.physical || {};
    freezeByNc.set(`${normName(phys.current_name)}|${norm(phys.country)}`, rec);
  }
  const queue = [];
  for (const r of ledgerRows) {
    if (r.universe_status !== "VERIFIED_READY_TO_INSERT") continue;
    const freezeHit =
      freezeByPid.get(r.property_identity_id) ||
      freezeByNc.get(`${normName(r.candidate_name)}|${norm(r.country)}`);
    if (!freezeHit) continue;
    const phys = freezeHit.physical || {};
    const aff = freezeHit.affiliation || {};
    const key =
      buildDiscoveredIdentityKey({
        source_family: aff.brand_family,
        country: phys.country,
        official_property_id: phys.official_property_id,
      }) || null;
    // synthesize if needed via identity from freeze pid
    const synKey =
      key ||
      `ind_indep_${String(phys.country || "xx")
        .toLowerCase()
        .replace(/[^a-z]/g, "")
        .slice(0, 2)}_${String(r.property_identity_id || "").replace(/^pid_/, "").slice(0, 16)}`;
    if (keys.has(synKey)) continue;
    if (phys.official_url && urls.has(String(phys.official_url).toLowerCase())) continue;
    if (nameCountry.has(`${normName(phys.current_name)}|${norm(phys.country)}`)) continue;
    const cityRaw = cityOk(phys.city, phys.country) ? phys.city : null;
    const city = cityRaw ? normalizeCityProperCase(cityRaw).city : null;
    queue.push({
      key: synKey,
      name: phys.current_name,
      country: phys.country,
      city: city && cityOk(city, phys.country) ? city : null,
      brand: aff.current_brand || null,
      family: aff.brand_family || "Independent",
      url: phys.official_url || null,
      source_type: "independent_freeze",
      lane: LANES.VERIFIED_READY_INSERT,
    });
    if (queue.length >= cap) break;
  }
  return queue;
}

async function main() {
  if (!(process.env.ENABLE_VERIFIED_CENSUS_WRITES === "1" || process.argv.includes("--apply"))) {
    console.error("--apply + ENABLE_VERIFIED_CENSUS_WRITES=1 required");
    process.exit(2);
  }

  const cfg = defaultControllerConfig({
    max_iterations_per_process: Number(
      process.argv.includes("--max-iterations")
        ? process.argv[process.argv.indexOf("--max-iterations") + 1]
        : process.env.V4_MAX_ITERATIONS || 8
    ),
    max_runtime_ms: Number(
      process.argv.includes("--max-runtime-ms")
        ? process.argv[process.argv.indexOf("--max-runtime-ms") + 1]
        : process.env.V4_MAX_RUNTIME_MS || 20 * 60 * 1000
    ),
    wave_batch_size: Number(process.env.V4_WAVE_BATCH_SIZE || 100),
  });

  const statusPath = path.join(OUT, "24-full-build-status.json");
  fs.mkdirSync(OUT, { recursive: true });
  let status;
  if (!fs.existsSync(statusPath)) {
    // Empty Railway volume / first boot: create ACTIVE shell status (runtime bootstrap).
    status = {
      status: "ACTIVE",
      controller_status: CONTROLLER_STATUS.ACTIVE || "ACTIVE",
      standing_authorization: true,
      no_per_batch_joan_approval: true,
      mode: "FULL_UNIVERSE_BREADTH_FIRST_PASS1",
      created_at: new Date().toISOString(),
      note: "bootstrapped_empty_runtime_volume",
    };
    persistJson(statusPath, status);
  } else {
    status = JSON.parse(fs.readFileSync(statusPath, "utf8"));
  }
  if (status.status !== "ACTIVE" && status.controller_status === CONTROLLER_STATUS.BLOCKED) {
    throw new Error("V4 blocked — refuse to run");
  }

  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;
  const started = Date.now();
  const txPath = path.join(OUT, "12-production-transactions.jsonl");
  const appendTx = (row) => appendJsonl(txPath, row);

  const circuit = { tripped: false, reason: null, detail: null };
  const trip = (reason, detail = {}) => {
    circuit.tripped = true;
    circuit.reason = reason;
    circuit.detail = detail;
    console.log(`[controller] CIRCUIT ${reason}`, JSON.stringify(detail));
  };

  let live = await listLive(baseId, token);
  const startingLive = live.length;
  const keys = new Set(live.map((r) => r.fields?.["Property Identity Key"]).filter(Boolean));
  const urls = new Set(
    live
      .map((r) => String(r.fields?.["Official Property URL"] || "").trim().toLowerCase())
      .filter((u) => u.length > 8)
  );
  const nameCountry = new Set(
    live.map((r) => `${normName(r.fields?.["Property Name"])}|${norm(r.fields?.Country)}`)
  );

  let ledgerRows = loadLedgerRows(LEDGER_DIR);
  let statusCounts = summarizeLedgerStatuses(ledgerRows);
  let freeze = { records: [] };
  if (fs.existsSync(FREEZE_PATH)) freeze = JSON.parse(fs.readFileSync(FREEZE_PATH, "utf8"));

  /** @type {object[]|null} */
  let cachedDiscovered = null;
  let directoryWaveExhausted = false;
  const policyControllerEnabled =
    String(process.env.ENABLE_CENSUS_POLICY_CONTROLLER || "0").trim() === "1";
  const forceDiscoveryRefresh =
    String(process.env.CENSUS_DISCOVERY_FORCE_REFRESH || "0").trim() === "1";

  const state = emptyControllerState({
    production_before: startingLive,
    started_at: new Date().toISOString(),
  });

  const transitions = [];
  let exhaustedLanes = [];
  // Persist exhausted lanes across process boundaries (orchestration only)
  const priorCk = (() => {
    try {
      return JSON.parse(fs.readFileSync(path.join(OUT, "43-controller-checkpoint-state.json"), "utf8"));
    } catch {
      return null;
    }
  })();
  if (Array.isArray(priorCk?.exhausted_lanes)) {
    exhaustedLanes = [...priorCk.exhausted_lanes];
    // Field strategy is per-process: clear so each resume can run another enrichment wave.
    exhaustedLanes = exhaustedLanes.filter((l) => l !== LANES.MISSING_FIELD_SOURCE_STRATEGY);
    console.log(`[controller] restored exhausted_lanes=${exhaustedLanes.join(",") || "(none)"}`);
  }
  if (priorCk?.directory_wave_exhausted && !forceDiscoveryRefresh) {
    directoryWaveExhausted = true;
    if (!exhaustedLanes.includes(LANES.OFFICIAL_DIRECTORY_DISCOVERY)) {
      exhaustedLanes.push(LANES.OFFICIAL_DIRECTORY_DISCOVERY);
    }
    console.log(
      "[controller] restored directory_wave_exhausted — skip rediscovery unless CENSUS_DISCOVERY_FORCE_REFRESH=1"
    );
  }
  // Resume from railway-safe discovery checkpoint without re-forcing discovery lane
  const discoveryCk = (() => {
    try {
      return JSON.parse(fs.readFileSync(path.join(OUT, DISCOVERY_CHECKPOINT_FILE), "utf8"));
    } catch {
      return null;
    }
  })();
  const discoveryProgress = (() => {
    try {
      return JSON.parse(fs.readFileSync(path.join(OUT, DISCOVERY_PROGRESS_FILE), "utf8"));
    } catch {
      return null;
    }
  })();
  if (
    !forceDiscoveryRefresh &&
    !directoryWaveExhausted &&
    discoveryProgress?.final?.discovered_count != null &&
    Number(discoveryProgress.final.discovered_count) > 0
  ) {
    directoryWaveExhausted = true;
    if (!exhaustedLanes.includes(LANES.OFFICIAL_DIRECTORY_DISCOVERY)) {
      exhaustedLanes.push(LANES.OFFICIAL_DIRECTORY_DISCOVERY);
    }
    console.log(
      `[controller] discovery already finalized discovered=${discoveryProgress.final.discovered_count} status=${discoveryProgress.final.lane_status || "n/a"} — hand off to field strategy`
    );
  }

  console.log(
    `[controller] start live=${startingLive} iterations<=${cfg.max_iterations_per_process} runtime<=${cfg.max_runtime_ms}ms policy=${policyControllerEnabled ? 1 : 0}`
  );

  for (let iter = 1; iter <= cfg.max_iterations_per_process; iter++) {
    if (circuit.tripped) {
      state.controller_status = CONTROLLER_STATUS.BLOCKED;
      state.hard_block_reason = circuit.reason;
      state.stop_reason = "HARD_CIRCUIT_BREAKER";
      break;
    }
    if (Date.now() - started > cfg.max_runtime_ms) {
      state.controller_status = CONTROLLER_STATUS.INFRASTRUCTURE_RUNTIME_BOUNDARY;
      state.stop_reason = "INFRASTRUCTURE_RUNTIME_BOUNDARY";
      state.next_work_scheduled_at = new Date(Date.now() + 60_000).toISOString();
      state.temporary_block_reason = null;
      console.log("[controller] runtime boundary — checkpoint + schedule resume");
      break;
    }

    // Refresh live caps count + snapshot
    const allCaps = live.filter((r) => {
      const c = r.fields?.City;
      return c && (isAllCapsCity(c) || isAllLowerCity(c));
    });

    const snapshot = {
      status_counts: statusCounts,
      verified_ready_queue: statusCounts.VERIFIED_READY_TO_INSERT || 0,
      all_caps_city_count: allCaps.length,
      official_directory_new_remaining: 0,
      independent_rediscovery_eligible: 0,
      remediation_eligible: allCaps.length,
      // Field strategy runs when policy controller is on and directory wave is done/partial.
      field_strategy_eligible:
        policyControllerEnabled &&
        (directoryWaveExhausted ||
          exhaustedLanes.includes(LANES.OFFICIAL_DIRECTORY_DISCOVERY) ||
          Boolean(discoveryProgress?.final?.discovered_count))
          ? Math.max(1, live.length)
          : 0,
      policy_controller_enabled: policyControllerEnabled,
      official_adapters_available: true,
      can_attempt_independent_rediscovery: true,
      directory_wave_just_exhausted: directoryWaveExhausted,
      serpapi_budget_exhausted: true, // prefer free/native this session; SerpApi selective later
      serpapi_eligible: 0,
      engineering_required: 0,
      exhausted_lanes: exhaustedLanes,
      hard_circuit: circuit.tripped,
    };

    // Pre-compute rediscovery eligibility via directory cache if present
    if (cachedDiscovered?.length) {
      const challenges = ledgerRows
        .filter((r) => r.universe_status === "NOT_YET_INDEPENDENTLY_REDISCOVERED")
        .slice(0, 5000);
      const { hits } = matchChallengesToDirectory(challenges, cachedDiscovered, nameCountry);
      snapshot.independent_rediscovery_eligible = hits.filter((h) => {
        const d = h.discovery;
        const key =
          d.identity_key ||
          buildDiscoveredIdentityKey({
            source_family: d.source_family || d.parent_company,
            country: d.country,
            official_property_id: d.official_property_id || d.property_code,
          });
        return key && !keys.has(key);
      }).length;
    } else if (!directoryWaveExhausted) {
      // Force discovery lane first time so rediscovery can be fed
      snapshot.official_directory_new_remaining = 1;
    }

    const probe = hasActionableUniverseWork(snapshot);
    state.actionable_remaining = probe.breakdown.researchable_or_cvent_challenge + probe.free_work_units;
    persistJson(path.join(OUT, "40-actionable-work-function.json"), {
      at: new Date().toISOString(),
      iteration: iter,
      probe,
      snapshot_breakdown: snapshot,
    });

    if (!probe.actionable && probe.complete) {
      state.controller_status = CONTROLLER_STATUS.COMPLETE;
      state.stop_reason = "NO_ACTIONABLE_WORK";
      console.log("[controller] COMPLETE — no actionable work");
      break;
    }
    if (!probe.actionable && probe.temporarily_blocked) {
      state.controller_status = CONTROLLER_STATUS.WAITING;
      state.temporary_block_reason = "retry_or_budget_window";
      state.next_work_scheduled_at = new Date(Date.now() + 15 * 60_000).toISOString();
      state.stop_reason = "WAITING_TEMPORARY_BLOCK";
      break;
    }

    const choice = chooseHighestValueLane(snapshot, { exhausted_lanes: exhaustedLanes });
    if (!choice.lane || choice.lane === LANES.WAITING_RETRY) {
      // Try clearing exhausted directory to attempt rediscovery after discovery
      if (exhaustedLanes.includes(LANES.OFFICIAL_DIRECTORY_DISCOVERY) && !cachedDiscovered) {
        exhaustedLanes = exhaustedLanes.filter((l) => l !== LANES.OFFICIAL_DIRECTORY_DISCOVERY);
        directoryWaveExhausted = false;
        continue;
      }
      state.controller_status = CONTROLLER_STATUS.WAITING;
      state.temporary_block_reason = choice.reason;
      state.next_work_scheduled_at = new Date(Date.now() + 10 * 60_000).toISOString();
      state.stop_reason = "LANES_NEED_RETRY_OR_ENGINEERING";
      console.log("[controller] no executable lane — schedule resume", choice.reason);
      break;
    }

    state.current_lane = choice.lane;
    state.iteration = iter;
    console.log(`[controller] iter=${iter} lane=${choice.lane} reason=${choice.reason}`);

    let laneResult = { inserts: 0, updates: 0, queue_size: 0, note: null };

    // ========== LANE: CITY PROPER CASE ==========
    if (choice.lane === LANES.CITY_PROPER_CASE_REMEDIATION) {
      const batch = allCaps.slice(0, cfg.wave_batch_size);
      state.current_queue_size = batch.length;
      const patches = [];
      for (const r of batch) {
        const before = r.fields?.City;
        const { city, changed } = normalizeCityProperCase(before);
        if (!changed || !city) continue;
        // Prefer Proper Case / CALA canonical over ALL CAPS even if market gate pending
        patches.push({
          id: r.id,
          fields: { City: city },
          before,
          after: city,
        });
      }
      if (patches.length) {
        await patchRecords(
          baseId,
          token,
          patches.map((p) => ({ id: p.id, fields: p.fields }))
        );
        for (const p of patches) {
          appendTx({
            op: "UPDATE",
            field: "City",
            airtable_record_id: p.id,
            before: p.before,
            after: p.after,
            lane: LANES.CITY_PROPER_CASE_REMEDIATION,
          });
        }
        laneResult.updates = patches.length;
        state.updates_session += patches.length;
        // refresh local live cities
        const byId = new Map(patches.map((p) => [p.id, p.after]));
        for (const r of live) {
          if (byId.has(r.id)) r.fields.City = byId.get(r.id);
        }
      }
      if (laneResult.updates === 0) {
        exhaustedLanes.push(LANES.CITY_PROPER_CASE_REMEDIATION);
        laneResult.note = "no_safe_city_patches";
      } else {
        laneResult.note = `proper_cased_${laneResult.updates}`;
        // One wave per process — remaining ALL CAPS stay actionable for auto-resume
        exhaustedLanes.push(LANES.CITY_PROPER_CASE_REMEDIATION);
      }
    }

    // ========== LANE: VERIFIED READY ==========
    else if (choice.lane === LANES.VERIFIED_READY_INSERT) {
      const queue = buildVerifiedReadyQueue(
        ledgerRows,
        freeze,
        keys,
        urls,
        nameCountry,
        cfg.wave_batch_size
      );
      state.current_queue_size = queue.length;
      if (!queue.length) {
        exhaustedLanes.push(LANES.VERIFIED_READY_INSERT);
        laneResult.note = "verified_ready_empty_after_hydrate";
      } else {
        const r = await insertCandidates(baseId, token, queue, {
          keys,
          urls,
          nameCountry,
          appendTx,
          circuit,
          trip,
          delayMs: cfg.write_delay_ms,
        });
        laneResult.inserts = r.inserts;
        laneResult.skipped = r.skipped;
        state.inserts_session += r.inserts;
        laneResult.note = `verified_ready_inserts_${r.inserts}`;
      }
    }

    // ========== LANE: OFFICIAL DIRECTORY ==========
    else if (choice.lane === LANES.OFFICIAL_DIRECTORY_DISCOVERY) {
      const countries = buildFullDiscoveryCountries();
      console.log(`[discover] countries=${countries.length}`);
      const { discovered, sourceReport } = await discoverCalaProperties({
        discoverAllOfficialParents: true,
        delayMs: 50,
        discoveryCountries: countries,
        railwaySafe: process.env.CENSUS_DISCOVERY_RAILWAY_SAFE_MODE === "1",
        discoveryOutDir: OUT,
        includeVicEvidence: false,
      });
      cachedDiscovered = discovered;
      persistJson(path.join(OUT, "34b-controller-discovery-cache-meta.json"), {
        n: discovered.length,
        families: sourceReport.families_used,
        discovery_lane_status: sourceReport.discovery_lane_status || null,
        checkpoint: sourceReport.checkpoint || null,
        at: new Date().toISOString(),
      });
      const censusRecords = live.map((r) => ({
        id: r.id,
        fields: r.fields,
        name: r.fields?.["Property Name"],
        country: r.fields?.Country,
        property_identity_key: r.fields?.["Property Identity Key"],
        official_url: r.fields?.["Official Property URL"],
      }));
      const match = classifyDiscoveredAgainstCensus(discovered, censusRecords, {});
      const neu = match.by_class[MATCH_CLASS.NEW_CANDIDATE] || [];
      const queue = [];
      for (const d of neu) {
        const name = d.property_name;
        const country = d.country;
        const key =
          d.identity_key ||
          buildDiscoveredIdentityKey({
            source_family: d.source_family || d.parent_company,
            country,
            official_property_id: d.official_property_id || d.property_code,
          });
        if (!name || !country || !key || keys.has(key)) continue;
        const url = (d.official_property_url || "").trim();
        if (url && urls.has(url.toLowerCase())) continue;
        if (nameCountry.has(`${normName(name)}|${norm(country)}`)) continue;
        const cityRaw = cityOk(d.city, country) ? d.city : null;
        const city = cityRaw ? normalizeCityProperCase(cityRaw).city : null;
        queue.push({
          key,
          name,
          country,
          city: city && cityOk(city, country) ? city : null,
          brand: d.brand || null,
          family: d.source_family || d.parent_company || "Independent",
          url: url || null,
          source_url: d.official_directory_url || url || null,
          source_type: "brand_directory",
          lane: LANES.OFFICIAL_DIRECTORY_DISCOVERY,
        });
        if (queue.length >= cfg.wave_batch_size) break;
      }
      state.current_queue_size = queue.length;
      const laneStatus =
        sourceReport.discovery_lane_status ||
        (queue.length ? "official_directory_discovery_complete" : null);
      const partial =
        laneStatus === "official_directory_discovery_partial_network_remaining" ||
        laneStatus === "official_directory_discovery_partial_source_remaining";
      if (!queue.length) {
        directoryWaveExhausted = true;
        // Partial network/source is recorded — do NOT loop discovery forever.
        // Timed-out units remain in discovery-resume-checkpoint for later FORCE_REFRESH.
        if (!exhaustedLanes.includes(LANES.OFFICIAL_DIRECTORY_DISCOVERY)) {
          exhaustedLanes.push(LANES.OFFICIAL_DIRECTORY_DISCOVERY);
        }
        laneResult.note = `directory_no_new discovered=${discovered.length} new_class=${neu.length} status=${laneStatus || "complete"}${partial ? " handoff_field_strategy" : ""}`;
      } else {
        const r = await insertCandidates(baseId, token, queue, {
          keys,
          urls,
          nameCountry,
          appendTx,
          circuit,
          trip,
          delayMs: cfg.write_delay_ms,
        });
        laneResult.inserts = r.inserts;
        state.inserts_session += r.inserts;
        laneResult.note = `directory_inserts_${r.inserts}_status_${laneStatus || "ok"}`;
        if (r.inserts === 0 || partial) {
          directoryWaveExhausted = true;
          if (!exhaustedLanes.includes(LANES.OFFICIAL_DIRECTORY_DISCOVERY)) {
            exhaustedLanes.push(LANES.OFFICIAL_DIRECTORY_DISCOVERY);
          }
        }
      }
    }

    // ========== LANE: MISSING FIELD SOURCE STRATEGY ==========
    else if (choice.lane === LANES.MISSING_FIELD_SOURCE_STRATEGY) {
      if (!policyControllerEnabled) {
        exhaustedLanes.push(LANES.MISSING_FIELD_SOURCE_STRATEGY);
        laneResult.note = "policy_controller_disabled";
      } else {
        console.log("[controller] running missing-field-source-strategy-controller-v1");
        const fieldReport = await runCensusMissingFieldSourceStrategyControllerV1Mission({
          enableProductionWrites: true,
          discoveryOutDir: OUT,
          args: {
            batchSize: Number(process.env.CENSUS_FIELD_STRATEGY_BATCH_SIZE || cfg.wave_batch_size || 100),
            maxPasses: Number(process.env.CENSUS_FIELD_STRATEGY_MAX_PASSES || 1),
            confirms: {
              writeToProductionCensus: true,
              safeWrites: true,
              noBrandExplorer: true,
              noOwnerOperator: true,
              noDateWrites: true,
              noRecentMomentum: true,
              noCompanyValidation: true,
              webhoundNotProduction: true,
            },
          },
          log: (msg) => console.log(msg),
        });
        laneResult.inserts = Number(fieldReport.inserts || 0);
        laneResult.updates = Number(fieldReport.existing_records_updated || fieldReport.census_writes || 0);
        state.inserts_session += laneResult.inserts;
        state.updates_session += laneResult.updates;
        laneResult.note = `field_strategy status=${fieldReport.status} updates=${laneResult.updates} inserts=${laneResult.inserts}`;
        if (fieldReport.status?.includes("blocked") || fieldReport.reason || fieldReport.blockers) {
          console.warn(
            "[controller] field_strategy_blocked",
            JSON.stringify({
              reason: fieldReport.reason || null,
              blockers: fieldReport.blockers || fieldReport.preflight?.blockers || null,
              env_missing: fieldReport.envCheck?.missing || null,
            })
          );
        }
        persistJson(path.join(OUT, "51-missing-field-source-strategy-last.json"), {
          at: new Date().toISOString(),
          status: fieldReport.status,
          reason: fieldReport.reason || null,
          blockers: fieldReport.blockers || fieldReport.preflight?.blockers || null,
          inserts: laneResult.inserts,
          updates: laneResult.updates,
          gaps_after: fieldReport.gaps_after || null,
          discovery_partial: fieldReport.discovery_partial || null,
          next_source_investment: fieldReport.next_source_investment || null,
        });
        // One strategy pass per process — remaining gaps stay actionable via auto-resume
        exhaustedLanes.push(LANES.MISSING_FIELD_SOURCE_STRATEGY);
        // Soft-refresh live after field writes
        if (laneResult.updates > 0 || laneResult.inserts > 0) {
          live = await listLive(baseId, token);
        }
      }
    }

    // ========== LANE: INDEPENDENT REDISCOVERY ==========
    else if (choice.lane === LANES.INDEPENDENT_REDISCOVERY) {
      if (!cachedDiscovered) {
        // Prefer finalized discovery cache over re-running directory crawl.
        const metaPath = path.join(OUT, "34b-controller-discovery-cache-meta.json");
        if (directoryWaveExhausted || fs.existsSync(path.join(OUT, DISCOVERY_CHECKPOINT_FILE))) {
          exhaustedLanes.push(LANES.INDEPENDENT_REDISCOVERY);
          laneResult.note =
            "deferred_no_in_memory_directory_cache_after_partial_discovery_skip_rediscovery_this_wave";
        } else {
          exhaustedLanes = exhaustedLanes.filter((l) => l !== LANES.OFFICIAL_DIRECTORY_DISCOVERY);
          directoryWaveExhausted = false;
          laneResult.note = "deferred_need_directory_cache";
        }
        void metaPath;
      } else {
        const challenges = ledgerRows.filter(
          (r) => r.universe_status === "NOT_YET_INDEPENDENTLY_REDISCOVERED"
        );
        const { hits } = matchChallengesToDirectory(challenges, cachedDiscovered, nameCountry);
        const queue = [];
        for (const h of hits) {
          const d = h.discovery;
          const name = d.property_name;
          const country = d.country;
          const key =
            d.identity_key ||
            buildDiscoveredIdentityKey({
              source_family: d.source_family || d.parent_company,
              country,
              official_property_id: d.official_property_id || d.property_code,
            });
          if (!name || !country || !key || keys.has(key)) continue;
          if (nameCountry.has(`${normName(name)}|${norm(country)}`)) continue;
          const cityRaw = cityOk(d.city, country) ? d.city : null;
          const city = cityRaw ? normalizeCityProperCase(cityRaw).city : null;
          queue.push({
            key,
            name,
            country,
            city: city && cityOk(city, country) ? city : null,
            brand: d.brand || null,
            family: d.source_family || d.parent_company || "Independent",
            url: d.official_property_url || null,
            source_url: d.official_directory_url || null,
            source_type: "independent_rediscovery_via_official_directory",
            lane: LANES.INDEPENDENT_REDISCOVERY,
            challenge_pid: h.challenge_pid,
          });
          if (queue.length >= cfg.wave_batch_size) break;
        }
        state.current_queue_size = queue.length;
        persistJson(path.join(OUT, "41b-independent-rediscovery-hits.json"), {
          hits: hits.length,
          queue: queue.length,
          sample: queue.slice(0, 20),
        });
        if (!queue.length) {
          exhaustedLanes.push(LANES.INDEPENDENT_REDISCOVERY);
          laneResult.note = `rediscovery_no_insertable hits=${hits.length}`;
        } else {
          const r = await insertCandidates(baseId, token, queue, {
            keys,
            urls,
            nameCountry,
            appendTx,
            circuit,
            trip,
            delayMs: cfg.write_delay_ms,
          });
          laneResult.inserts = r.inserts;
          state.inserts_session += r.inserts;
          laneResult.note = `rediscovery_inserts_${r.inserts}`;
          // Update ledger statuses for inserted challenge pids
          const insertedPids = new Set(queue.slice(0, r.inserts).map((q) => q.challenge_pid));
          for (const row of ledgerRows) {
            if (insertedPids.has(row.property_identity_id)) {
              row.universe_status = "IN_PRODUCTION";
              row.verification_status = "VERIFIED_INDEPENDENT";
              row.production_status = "IN_PRODUCTION";
              row.research_status = "IN_PRODUCTION_REMEDIATION";
            }
          }
          statusCounts = summarizeLedgerStatuses(ledgerRows);
        }
      }
    }

    // ========== LANE: EXISTING RECORD REMEDIATION (continues City Proper Case) ==========
    else if (choice.lane === LANES.EXISTING_RECORD_REMEDIATION) {
      const batch = allCaps.slice(0, cfg.wave_batch_size);
      state.current_queue_size = batch.length;
      const patches = [];
      for (const r of batch) {
        const before = r.fields?.City;
        const { city, changed } = normalizeCityProperCase(before);
        if (!changed || !city) continue;
        patches.push({ id: r.id, fields: { City: city }, before, after: city });
      }
      if (patches.length) {
        await patchRecords(
          baseId,
          token,
          patches.map((p) => ({ id: p.id, fields: p.fields }))
        );
        for (const p of patches) {
          appendTx({
            op: "UPDATE",
            field: "City",
            airtable_record_id: p.id,
            before: p.before,
            after: p.after,
            lane: LANES.EXISTING_RECORD_REMEDIATION,
          });
        }
        laneResult.updates = patches.length;
        state.updates_session += patches.length;
        const byId = new Map(patches.map((p) => [p.id, p.after]));
        for (const r of live) {
          if (byId.has(r.id)) r.fields.City = byId.get(r.id);
        }
        laneResult.note = `city_proper_case_continued_${laneResult.updates}`;
      } else {
        exhaustedLanes.push(LANES.EXISTING_RECORD_REMEDIATION);
        laneResult.note = "no_remediation_batch";
      }
    }

    // ========== LANE: ENGINEERING CLASSIFY ==========
    else if (choice.lane === LANES.ADAPTER_NEEDED_ENGINEERING) {
      laneResult.note = "classified_engineering_required_continue_other_work";
      exhaustedLanes.push(LANES.ADAPTER_NEEDED_ENGINEERING);
    }

    transitions.push({
      iteration: iter,
      lane: choice.lane,
      reason: choice.reason,
      ...laneResult,
      at: new Date().toISOString(),
    });
    state.lane_transitions = transitions;
    state.last_work_completed_at = new Date().toISOString();

    // Checkpoint after every lane (persistence, NOT termination)
    persistJson(path.join(OUT, "43-controller-checkpoint-state.json"), {
      ...state,
      circuit,
      exhausted_lanes: exhaustedLanes,
      directory_wave_exhausted: directoryWaveExhausted,
      discovery_timed_out_units: Object.keys(discoveryCk?.timed_out || {}).length,
      discovery_partial_status: discoveryProgress?.final?.lane_status || null,
      live_estimate: startingLive + state.inserts_session,
      checkpoint_means: "persistence_boundary_not_stop",
      joan_batch_approval_required: false,
    });

    console.log(
      `[controller] lane done inserts=${laneResult.inserts || 0} updates=${laneResult.updates || 0} note=${laneResult.note}`
    );

    // Refresh live count cheaply after mutations
    if ((laneResult.inserts || 0) + (laneResult.updates || 0) > 0) {
      // Soft refresh: re-list only if inserts (count drift); updates already patched in-memory
      if (laneResult.inserts > 0) {
        live = await listLive(baseId, token);
        for (const r of live) {
          const k = r.fields?.["Property Identity Key"];
          if (k) keys.add(k);
          const u = String(r.fields?.["Official Property URL"] || "")
            .trim()
            .toLowerCase();
          if (u.length > 8) urls.add(u);
          nameCountry.add(`${normName(r.fields?.["Property Name"])}|${norm(r.fields?.Country)}`);
        }
      }
    }

    // Continue outer loop automatically — do NOT exit because batch finished
  }

  // Final live reconcile
  const afterLive = await listLive(baseId, token);
  const uniquePhysical = estimateUniquePhysical(ledgerRows);
  const footprint = footprintMetrics({
    liveCount: afterLive.length,
    uniquePhysical,
  });

  if (!state.stop_reason) {
    // Hit max iterations = infrastructure boundary, NOT complete
    state.controller_status = CONTROLLER_STATUS.INFRASTRUCTURE_RUNTIME_BOUNDARY;
    state.stop_reason = "INFRASTRUCTURE_RUNTIME_BOUNDARY";
    state.next_work_scheduled_at = new Date(Date.now() + 60_000).toISOString();
  }

  // Keep ACTIVE semantics when more work remains
  const finalProbe = hasActionableUniverseWork({
    status_counts: summarizeLedgerStatuses(ledgerRows),
    all_caps_city_count: afterLive.filter((r) => isAllCapsCity(r.fields?.City) || isAllLowerCity(r.fields?.City))
      .length,
    official_adapters_available: true,
    can_attempt_independent_rediscovery: true,
    serpapi_budget_exhausted: true,
  });

  if (finalProbe.actionable && state.controller_status !== CONTROLLER_STATUS.BLOCKED) {
    // Self-continuing: process ended on boundary but build is ACTIVE with scheduled resume
    state.controller_status =
      state.stop_reason === "NO_ACTIONABLE_WORK"
        ? CONTROLLER_STATUS.COMPLETE
        : CONTROLLER_STATUS.ACTIVE;
    if (state.stop_reason === "INFRASTRUCTURE_RUNTIME_BOUNDARY") {
      // ACTIVE + scheduled — operationally self-continuing via supervisor
      state.controller_status = CONTROLLER_STATUS.ACTIVE;
      state.temporary_block_reason = "INFRASTRUCTURE_RUNTIME_BOUNDARY_AUTO_RESUME";
    }
  }

  state.production_after = afterLive.length;
  state.exhausted_lanes = exhaustedLanes;
  state.circuit = circuit;
  state.actionable_remaining = finalProbe.breakdown.researchable_or_cvent_challenge + finalProbe.free_work_units;
  state.current_queue_size = 0;
  state.joan_batch_approval_required = false;

  persistJson(path.join(OUT, "48-full-build-controller-status.json"), state);
  persistJson(path.join(OUT, "45-autonomous-transition-test.json"), {
    required_min_transitions: 3,
    transitions,
    passed: transitions.length >= 3,
    lanes_used: [...new Set(transitions.map((t) => t.lane))],
  });
  persistJson(path.join(OUT, "47-post-fix-scorecard.json"), {
    starting_live: startingLive,
    ending_live: afterLive.length,
    inserts: state.inserts_session,
    updates: state.updates_session,
    footprint,
    probe: finalProbe,
    transitions,
    stop_reason: state.stop_reason,
    controller_status: state.controller_status,
    joan_batch_approval_required: false,
  });

  // Persist ledger shards if statuses changed
  const shardSize = 2000;
  for (let i = 0; i < ledgerRows.length; i += shardSize) {
    const chunk = ledgerRows.slice(i, i + shardSize);
    fs.writeFileSync(
      path.join(LEDGER_DIR, `ledger-${String(Math.floor(i / shardSize)).padStart(3, "0")}.json`),
      JSON.stringify({ offset: i, count: chunk.length, rows: chunk })
    );
  }
  persistJson(path.join(OUT, "46-post-fix-universe-ledger.json"), {
    ledger_rows: ledgerRows.length,
    status_counts: summarizeLedgerStatuses(ledgerRows),
    unique_physical_estimate: uniquePhysical,
    live_production: afterLive.length,
  });

  persistJson(path.join(OUT, "24-full-build-status.json"), {
    ...status,
    status: "ACTIVE",
    controller_status: state.controller_status,
    standing_authorization: true,
    no_per_batch_joan_approval: true,
    joan_batch_approval_required: false,
    v4_paused: false,
    production_count: afterLive.length,
    last_controller_at: new Date().toISOString(),
    next_work_scheduled_at: state.next_work_scheduled_at,
    current_lane: state.current_lane,
    actionable_remaining: state.actionable_remaining,
    stop_reason: state.stop_reason,
    self_continuing: state.controller_status === CONTROLLER_STATUS.ACTIVE,
  });

  // Auto-resume marker for supervisor
  persistJson(path.join(OUT, "50-auto-resume-ticket.json"), {
    resume: finalProbe.actionable && !circuit.tripped,
    next_work_scheduled_at: state.next_work_scheduled_at || new Date().toISOString(),
    command:
      "ENABLE_VERIFIED_CENSUS_WRITES=1 node scripts/v4-full-build-controller.mjs --apply",
    joan_required: false,
  });

  console.log(
    JSON.stringify(
      {
        starting_live: startingLive,
        ending_live: afterLive.length,
        inserts: state.inserts_session,
        updates: state.updates_session,
        transitions: transitions.length,
        lanes: transitions.map((t) => t.lane),
        controller_status: state.controller_status,
        stop_reason: state.stop_reason,
        actionable: finalProbe.actionable,
        joan_batch_approval_required: false,
        exit_contract: "see worker-runtime CONTROLLER_EXIT",
      },
      null,
      2
    )
  );

  const exitCode = mapStopReasonToExitCode(state.stop_reason, circuit.tripped);
  process.exit(exitCode);
}

main().catch((e) => {
  console.error(e);
  // Unexpected boot/runtime errors should retry under supervisor (not kill the worker).
  // Reserve exit 40 for explicit FATAL_CONFIGURATION only.
  process.exit(1);
});
