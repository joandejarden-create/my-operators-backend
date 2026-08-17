/**
 * Census Autopilot Policy Controller v1.
 * One approved policy → multi-pass loop without per-field founder gates.
 * Hotel Property Census only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
} from "./production-census-source-of-truth.js";
import {
  checkAutopilotApplyEnv,
  applyPreflight,
} from "./census-autopilot-apply-guard.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import {
  resolveCensusMode,
  assertNoInsertInFieldCompletionMode,
  CENSUS_MODE,
} from "./census-autopilot-full-latam-v3.js";
import {
  buildCensusGapLedger,
  buildCompletionScorecard,
  writeCensusGapLedger,
} from "./census-gap-ledger.js";
import {
  resolveMarketFromCity,
  resolveSubmarketHighOnly,
} from "./census-region-market-map.js";
import { runCoordinateCompletionQueue } from "./census-coordinate-completion.js";
import { evaluateMapboxPermanentReadiness } from "./census-coordinate-provider.js";
import { runDataForSeoLocalAddressScaleV1Mission } from "./dataforseo-local-address-scale-v1.js";
import { runDataForSeoLocalBusinessEnrichmentV1Mission } from "./dataforseo-local-business-enrichment-v1.js";
import { runRoomsSecondarySourceWave2V1Mission } from "./census-autopilot-rooms-secondary-source-wave-2-v1.js";
import {
  resolveCensusAutopilotPolicyGates,
  assertHighConfidenceInsertPolicy,
  classifyPhoneUnderAutopilotPolicy,
  classifyDirectLocalCoordinatesUnderPolicy,
  POLICY_CONTROLLER_PASSES,
  CENSUS_AUTOPILOT_APPROVED_POLICY_VERSION,
  NEVER_WRITE_FIELDS,
} from "./census-autopilot-approved-policy.js";
import { writeDataForSeoNewHotelInsertReviewPack } from "./dataforseo-new-hotel-insert-review-pack.js";
import { applyDataForSeoHighConfidenceInternalInserts } from "./dataforseo-high-confidence-internal-insert.js";
import {
  PHONE_PROVENANCE_SCHEMA_TODO,
  CONFIDENCE_TIERED_INTERNAL_POLICY_VERSION,
} from "./census-confidence-tiered-internal-completion.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const POLICY_CONTROLLER_OBJECTIVE =
  "census-autopilot-policy-controller-v1";
export const POLICY_CONTROLLER_VERSION =
  "census-autopilot-policy-controller-v1";

export const POLICY_CONTROLLER_STATUS = Object.freeze({
  COMPLETE: "production_census_autopilot_policy_controller_v1_complete",
  PARTIAL_SOURCE:
    "production_census_autopilot_policy_controller_v1_partial_source_remaining",
  PARTIAL_INSERT:
    "production_census_autopilot_policy_controller_v1_partial_insert_policy_needed",
  BLOCKED: "production_census_autopilot_policy_controller_v1_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

const READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Country",
  "City",
  "State / Region",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Official Property URL",
  "Source URL",
  "Phone",
  "Rooms / Keys",
  "Latitude",
  "Longitude",
  "Market",
  "Submarket",
  "Continent",
  "Sub-Continent",
  "Enrichment Status",
  "Last Reviewed Date",
];

function isBlank(v) {
  return v == null || !String(v).trim();
}

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeMd(filePath, md) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

async function listCensus(baseId, token, tableId) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of READ_FIELDS) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(
        `census_list_failed:${res.status}:${json?.error?.message || ""}`
      );
    }
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function applyFieldPatches(proposals, { baseId, token, tableId, log }) {
  let updatesApplied = 0;
  const writeErrors = [];

  for (let i = 0; i < proposals.length; i += 10) {
    const chunk = proposals.slice(i, i + 10);
    const records = chunk
      .map((p) => {
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (isForbiddenAutopilotField(k)) continue;
          if (NEVER_WRITE_FIELDS.includes(k)) continue;
          if (v === undefined || v === null || v === "") continue;
          fields[k] = v;
        }
        return { id: p.record_id, fields };
      })
      .filter((u) => Object.keys(u.fields).length > 0);
    if (!records.length) continue;

    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records, typecast: true }),
      }
    );
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      writeErrors.push({ status: res.status, error: json.error || json });
      log?.(`[policy-controller] market patch batch failed ${res.status}`);
    } else {
      updatesApplied += records.length;
    }
  }
  return { updatesApplied, writeErrors };
}

function buildMarketSubmarketProposals(census) {
  const proposals = [];
  let marketWrites = 0;
  let submarketWrites = 0;
  let submarketHeld = 0;

  for (const rec of census) {
    const f = rec.fields || {};
    /** @type {Record<string, unknown>} */
    const patch = {};

    if (isBlank(f.Market)) {
      const m = resolveMarketFromCity({
        city: f.City,
        country: f.Country,
        state: f["State / Region"],
      });
      if (m?.ok && m.market) {
        patch.Market = m.market;
        marketWrites += 1;
      }
    }

    if (isBlank(f.Submarket)) {
      const s = resolveSubmarketHighOnly({
        city: f.City,
        country: f.Country,
        address: f.Address,
        market: patch.Market || f.Market,
        propertyName: f["Canonical Property Name"] || f["Property Name"],
      });
      if (s?.ok && s.submarket) {
        patch.Submarket = s.submarket;
        submarketWrites += 1;
      } else {
        submarketHeld += 1;
      }
    }

    if (Object.keys(patch).length) {
      patch["Last Reviewed Date"] = todayIsoDate();
      proposals.push({
        record_id: rec.id,
        reason: "policy_controller_market_submarket",
        patch,
      });
    }
  }

  return { proposals, marketWrites, submarketWrites, submarketHeld };
}

function countGaps(census) {
  let missingAddress = 0;
  let missingUrl = 0;
  let missingCoords = 0;
  let missingRooms = 0;
  let missingPhone = 0;
  let missingMarket = 0;
  let missingSubmarket = 0;
  for (const r of census) {
    const f = r.fields || {};
    if (isBlank(f.Address)) missingAddress += 1;
    if (isBlank(f["Official Property URL"])) missingUrl += 1;
    if (isBlank(f.Latitude) || isBlank(f.Longitude)) missingCoords += 1;
    if (isBlank(f["Rooms / Keys"])) missingRooms += 1;
    if (isBlank(f.Phone)) missingPhone += 1;
    if (isBlank(f.Market)) missingMarket += 1;
    if (isBlank(f.Submarket)) missingSubmarket += 1;
  }
  return {
    missing_address: missingAddress,
    missing_official_url: missingUrl,
    missing_coordinates: missingCoords,
    missing_rooms: missingRooms,
    missing_phone: missingPhone,
    missing_market: missingMarket,
    missing_submarket: missingSubmarket,
  };
}

function renderReportMd(report) {
  return [
    `# Census Autopilot Policy Controller v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${report.objective}\``,
    `**Generated:** ${report.generated_at}`,
    `**Policy version:** \`${report.policy_version}\``,
    `**Census mode:** \`${report.census_mode}\``,
    `**Founder gates between passes:** **false**`,
    ``,
    `## Summary`,
    ``,
    `- Records scanned: **${report.records_scanned}**`,
    `- Existing records updated: **${report.existing_records_updated}**`,
    `- Address writes: **${report.address_writes}**`,
    `- Website writes: **${report.website_writes}**`,
    `- Phone Medium writes: **${report.phone_writes ?? 0}**`,
    `- Mapbox coordinate writes: **${report.mapbox_coordinate_writes}**`,
    `- Medium addresses geocoded (Mapbox): **${report.medium_addresses_geocoded ?? 0}**`,
    `- Mapbox rejects: **${report.mapbox_rejects ?? 0}**`,
    `- Medium-tier field writes (addr+phone+med coords): **${report.medium_field_writes ?? 0}**`,
    `- Rooms writes: **${report.rooms_writes}**`,
    `- Market writes: **${report.market_writes}**`,
    `- Submarket writes: **${report.submarket_writes}**`,
    `- Phone held by policy: **${report.phone_held_by_policy}**`,
    `- Direct DataForSEO coords held: **${report.coordinate_direct_held}**`,
    `- New hotel candidates found: **${report.new_hotel_candidates_found}**`,
    `- Insert review pack count: **${report.insert_review_pack_count ?? 0}**`,
    `- Inserts (Census Only / Hold): **${report.inserts}**`,
    `- Duplicate-risk candidates: **${report.duplicate_risk_candidates}**`,
    `- Estimated cost: **$${Number(report.estimated_cost_usd || 0).toFixed(4)}**`,
    `- Internal medium completion: **${report.gates?.internal_medium_completion ? "yes" : "no"}**`,
    `- Phone Confidence schema: **missing** (provenance in Notes for Steward)`,
    ``,
    `## Remaining gaps (after)`,
    ``,
    ...Object.entries(report.gaps_after || {}).map(
      ([k, n]) => `- \`${k}\`: ${n}`
    ),
    ``,
    `## Pass log`,
    ``,
    ...(report.pass_log || []).map(
      (p) =>
        `- **Pass ${p.pass} · ${p.name}:** ${p.summary || p.status || "ok"}`
    ),
    ``,
    `## Blocker reasons`,
    ``,
    ...((report.blocker_reasons || []).length
      ? report.blocker_reasons.map((b) => `- ${b}`)
      : ["- (none)"]),
    ``,
    `## Next backlog`,
    ``,
    report.next_backlog || "",
    ``,
    `## Another founder approval needed?`,
    ``,
    report.founder_approval_needed ||
      "No — continue Autopilot under encoded policy.",
    ``,
    `## Safety`,
    ``,
    `- Census table: Hotel Property Census (\`${CENSUS_TABLE_ID}\`)`,
    `- Brand Setup / Brand Explorer: **0**`,
    `- Owner / operator / dates / Company Validated / Brand Verified: **0**`,
    `- Phone writes: **0**`,
    `- Direct DataForSEO coordinate writes: **0**`,
    ``,
  ].join("\n");
}

/**
 * @param {{
 *   argv?: string[],
 *   args?: object,
 *   env?: NodeJS.ProcessEnv,
 *   enableProductionWrites?: boolean,
 *   censusMode?: string,
 *   log?: Function,
 *   skipPasses?: string[],
 * }} [opts]
 */
export async function runCensusAutopilotPolicyControllerV1Mission(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || console.log;
  const args = opts.args || {};
  const enableWrites = Boolean(opts.enableProductionWrites);
  const skipPasses = new Set(opts.skipPasses || []);

  const censusMode = resolveCensusMode(opts.argv || [], {
    censusMode: opts.censusMode || args.censusMode || CENSUS_MODE.GROWTH,
  });
  // Sticky DATAFORSEO_WRITE_CANDIDATES_ONLY must not disable field-completion
  // writes or accidentally run discovery during field-completion-only.
  const candidatesOnlyFlag =
    String(env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "0") === "1" &&
    censusMode !== CENSUS_MODE.FIELD_COMPLETION_ONLY;
  const candidateOnly =
    censusMode === CENSUS_MODE.CANDIDATE_ONLY ||
    candidatesOnlyFlag ||
    !enableWrites;

  const gates = resolveCensusAutopilotPolicyGates(env);
  if (!gates.ok) {
    return {
      ok: false,
      status: POLICY_CONTROLLER_STATUS.BLOCKED,
      objective: POLICY_CONTROLLER_OBJECTIVE,
      reason: "policy_gates_failed",
      blockers: gates.blockers,
      inserts: 0,
      census_writes: 0,
    };
  }

  const insertGuard = assertNoInsertInFieldCompletionMode(censusMode, 0);
  if (!insertGuard.ok) {
    return {
      ok: false,
      status: POLICY_CONTROLLER_STATUS.BLOCKED,
      objective: POLICY_CONTROLLER_OBJECTIVE,
      reason: insertGuard.reason,
      inserts: 0,
      census_writes: 0,
    };
  }

  const insertPolicy = assertHighConfidenceInsertPolicy({
    censusMode,
    gates,
  });

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${stamp}_CALA-census-autopilot-policy-controller-v1`
  );
  fs.mkdirSync(runDir, { recursive: true });

  const envCheck = checkAutopilotApplyEnv(env);
  const preflightArgs = {
    ...args,
    mode: enableWrites && !candidateOnly ? "mission" : args.mode || "controlled",
    region: args.region || "CALA",
    scope: args.scope || "official-parent-inventory",
    confirms: {
      safeWrites: true,
      writeToProductionCensus: true,
      noBrandExplorer: true,
      noOwnerOperator: true,
      noDateWrites: true,
      noRecentMomentum: true,
      noCompanyValidation: true,
      webhoundNotProduction: true,
      ...(args.confirms || {}),
    },
    allApplyConfirms: true,
  };
  const preflight = applyPreflight(preflightArgs, envCheck);
  if (enableWrites && !candidateOnly && (!envCheck.allOk || !preflight.ok)) {
    return {
      ok: false,
      status: POLICY_CONTROLLER_STATUS.BLOCKED,
      objective: POLICY_CONTROLLER_OBJECTIVE,
      reason: "missing_confirmations",
      envCheck,
      preflight,
      inserts: 0,
      census_writes: 0,
      run_dir: runDir,
    };
  }

  const targetAssert = assertProductionCensusWriteTarget({
    tableId: CENSUS_TABLE_ID,
    baseId: resolveTargetBase().target_base_id || null,
  });
  if (!targetAssert.ok) {
    return {
      ok: false,
      status: POLICY_CONTROLLER_STATUS.BLOCKED,
      objective: POLICY_CONTROLLER_OBJECTIVE,
      reason: targetAssert.code || "blocked_wrong_census_target",
      inserts: 0,
      census_writes: 0,
      run_dir: runDir,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases.target_base_id) {
    return {
      ok: false,
      status: POLICY_CONTROLLER_STATUS.BLOCKED,
      objective: POLICY_CONTROLLER_OBJECTIVE,
      reason: "missing_airtable_credentials",
      inserts: 0,
      census_writes: 0,
      run_dir: runDir,
    };
  }

  const maxPasses = Math.min(
    10,
    Math.max(1, Number(args.maxPasses || opts.maxPasses || 10))
  );
  const batchSize = Math.max(1, Number(args.batchSize || 100));

  log(
    `[policy-controller] mode=${censusMode} writes=${enableWrites && !candidateOnly} max_passes=${maxPasses} batch=${batchSize} founder_gates=false`
  );

  /** @type {object[]} */
  const passLog = [];
  const totals = {
    address_writes: 0,
    website_writes: 0,
    phone_writes: 0,
    mapbox_coordinate_writes: 0,
    medium_addresses_geocoded: 0,
    mapbox_rejects: 0,
    medium_field_writes: 0,
    high_field_writes: 0,
    rooms_writes: 0,
    market_writes: 0,
    submarket_writes: 0,
    existing_records_updated: 0,
    new_hotel_candidates_found: 0,
    inserts: 0,
    insert_review_pack_count: 0,
    duplicate_risk_candidates: 0,
    phone_held_by_policy: 0,
    coordinate_direct_held: 0,
    estimated_cost_usd: 0,
  };
  const blockerReasons = [];

  classifyPhoneUnderAutopilotPolicy();
  classifyDirectLocalCoordinatesUnderPolicy();

  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const recordsScanned = census.length;
  const gapsBefore = countGaps(census);
  log(`[policy-controller] scanned=${recordsScanned}`);

  if (!skipPasses.has("gap_audit")) {
    const ledger = buildCensusGapLedger(census, {});
    const scorecard = buildCompletionScorecard(census, {});
    writeCensusGapLedger(ledger, scorecard, { runDir });
    writeJson(path.join(runDir, "executable-backlog.json"), {
      note: "policy_controller_v1",
      gaps_before: gapsBefore,
    });
    passLog.push({
      pass: 1,
      name: "gap_audit",
      status: "ok",
      summary: `gaps address=${gapsBefore.missing_address} rooms=${gapsBefore.missing_rooms} coords=${gapsBefore.missing_coordinates}`,
    });
    log(`[policy-controller] pass 1 gap_audit done`);
  }

  // One full Autopilot cycle (outer=1). maxPasses reserved for future multi-cycle.
  const outer = 1;
  let wroteThisPass = 0;

  if (
    !skipPasses.has("existing_record_enrichment") &&
    !candidateOnly &&
    enableWrites &&
    gates.dataforseo_enabled &&
    (gates.address_writes || gates.website_writes || gates.phone_writes)
  ) {
    log(`[policy-controller] pass enrichment`);
    const enrichEnv = {
      ...env,
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION: gates.internal_medium_completion
        ? "1"
        : env.ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION || "0",
      ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: gates.address_writes ? "1" : "0",
      ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: gates.website_writes ? "1" : "0",
      ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: gates.phone_writes ? "1" : "0",
      ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_INSERTS: "0",
      DATAFORSEO_MAX_RECORDS: String(env.DATAFORSEO_MAX_RECORDS || batchSize),
    };
    const enrich = await runDataForSeoLocalAddressScaleV1Mission({
      argv: opts.argv || [],
      args: { ...args, batchSize, maxPasses: Math.min(5, maxPasses) },
      env: enrichEnv,
      enableProductionWrites: true,
      censusMode: CENSUS_MODE.FIELD_COMPLETION_ONLY,
      log,
    });
    totals.address_writes += Number(enrich.address_writes || 0);
    totals.website_writes += Number(enrich.website_writes || 0);
    totals.phone_writes += Number(enrich.phone_writes || 0);
    totals.medium_field_writes +=
      Number(enrich.address_writes || 0) + Number(enrich.phone_writes || 0);
    totals.existing_records_updated += Number(enrich.records_updated || 0);
    totals.phone_held_by_policy += Number(enrich.phone_candidates_held || 0);
    totals.coordinate_direct_held += Number(
      enrich.coordinate_candidates_held || 0
    );
    totals.estimated_cost_usd += Number(enrich.estimated_cost_usd || 0);
    wroteThisPass += Number(enrich.records_updated || 0);
    passLog.push({
      pass: 2,
      name: "existing_record_enrichment",
      outer,
      status: enrich.status || "ok",
      summary: `address=${enrich.address_writes || 0} website=${enrich.website_writes || 0} phone=${enrich.phone_writes || 0} updated=${enrich.records_updated || 0}`,
    });
  } else if (!skipPasses.has("existing_record_enrichment")) {
    passLog.push({
      pass: 2,
      name: "existing_record_enrichment",
      status: candidateOnly ? "skipped_candidate_only" : "skipped",
      summary: "enrichment skipped",
    });
  }

  if (
    !skipPasses.has("mapbox_coordinates") &&
    !candidateOnly &&
    enableWrites &&
    gates.mapbox_after_validated_address
  ) {
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
    const mapboxReady = evaluateMapboxPermanentReadiness(env);
    if (!mapboxReady?.approved_for_geocode_apply && !mapboxReady?.ready) {
      blockerReasons.push(
        "mapbox_permanent_not_ready — set MAPBOX_ACCESS_TOKEN + MAPBOX_PERMANENT_GEOCODING=1"
      );
      passLog.push({
        pass: 3,
        name: "mapbox_coordinates",
        outer,
        status: "provider_not_ready",
        summary: mapboxReady?.block_reason || "mapbox_not_ready",
      });
    } else {
      log(`[policy-controller] pass mapbox`);
      const mediumMapbox =
        gates.mapbox_after_medium_match_high_address === true;
      const coord = await runCoordinateCompletionQueue({
        censusRecords: census,
        env: {
          ...env,
          CENSUS_COORDINATE_COMPLETION_ENABLED: "1",
          ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS: mediumMapbox
            ? "1"
            : env.ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS || "0",
        },
        log,
        allowMediumAddressWithProvenance: mediumMapbox,
        mediumMatchHighPathway: mediumMapbox,
        maxRequests: Number(env.MAPBOX_MAX_REQUESTS_PER_RUN || batchSize),
        dryRun: false,
        runDir,
      });
      const proposals = (coord?.proposals || []).filter(
        (p) => p.write_allowed_now && p.patch
      );
      let applied = 0;
      if (proposals.length) {
        const write = await applyFieldPatches(
          proposals.map((p) => ({
            record_id: p.record_id,
            patch: {
              ...p.patch,
              "Last Reviewed Date": todayIsoDate(),
            },
          })),
          {
            baseId: bases.target_base_id,
            token,
            tableId: CENSUS_TABLE_ID,
            log,
          }
        );
        applied = write.updatesApplied;
      }
      const mediumApplied = proposals.filter(
        (p) => p.from_medium_address && p.write_allowed_now
      ).length;
      totals.mapbox_coordinate_writes += applied;
      totals.medium_addresses_geocoded += Math.min(applied, mediumApplied);
      totals.medium_field_writes += Math.min(applied, mediumApplied);
      totals.mapbox_rejects += Number(coord?.counters?.mapbox_rejects || 0);
      totals.existing_records_updated += applied;
      wroteThisPass += applied;
      passLog.push({
        pass: 3,
        name: "mapbox_coordinates",
        outer,
        status: coord?.status || "ok",
        summary: `mapbox_writes=${applied} medium=${mediumApplied} proposals=${proposals.length} eligible=${coord?.counters?.records_eligible_for_mapbox ?? "n/a"} rejects=${coord?.counters?.mapbox_rejects ?? 0}`,
      });
    }
  }

  if (
    !skipPasses.has("rooms_completion") &&
    !candidateOnly &&
    enableWrites &&
    (gates.secondary_rooms || gates.secondary_hotel_data)
  ) {
    log(`[policy-controller] pass rooms`);
    const roomsEnv = {
      ...env,
      ENABLE_SECONDARY_ROOMS_SOURCES: "1",
      ENABLE_SECONDARY_HOTEL_DATA_SOURCES: "1",
      ENABLE_SECONDARY_PHONE_SOURCES: "0",
    };
    const rooms = await runRoomsSecondarySourceWave2V1Mission({
      argv: opts.argv || [],
      args: { ...args, censusMode: CENSUS_MODE.FIELD_COMPLETION_ONLY },
      env: roomsEnv,
      enableProductionWrites: true,
      censusMode: CENSUS_MODE.FIELD_COMPLETION_ONLY,
      log,
    });
    const rw = Number(rooms.rooms_writes || rooms.records_updated || 0);
    totals.rooms_writes += rw;
    totals.existing_records_updated += Number(rooms.records_updated || rw);
    totals.phone_held_by_policy += Number(rooms.phone_held || 0);
    wroteThisPass += Number(rooms.records_updated || rw);
    passLog.push({
      pass: 4,
      name: "rooms_completion",
      outer,
      status: rooms.status || "ok",
      summary: `rooms=${rw}`,
    });
  }

  if (
    !skipPasses.has("market_submarket") &&
    !candidateOnly &&
    enableWrites
  ) {
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
    const mkt = buildMarketSubmarketProposals(census);
    if (mkt.proposals.length) {
      log(
        `[policy-controller] pass market/submarket proposals=${mkt.proposals.length}`
      );
      const applied = await applyFieldPatches(mkt.proposals, {
        baseId: bases.target_base_id,
        token,
        tableId: CENSUS_TABLE_ID,
        log,
      });
      totals.market_writes += mkt.marketWrites;
      totals.submarket_writes += mkt.submarketWrites;
      totals.existing_records_updated += applied.updatesApplied;
      wroteThisPass += applied.updatesApplied;
    }
    passLog.push({
      pass: 5,
      name: "market_submarket",
      outer,
      status: "ok",
      summary: `market=${mkt.marketWrites} submarket=${mkt.submarketWrites} held_submarket=${mkt.submarketHeld}`,
    });
  }

  if (
    !skipPasses.has("new_hotel_discovery") &&
    (censusMode === CENSUS_MODE.GROWTH ||
      censusMode === CENSUS_MODE.CANDIDATE_ONLY ||
      candidateOnly)
  ) {
    log(`[policy-controller] pass discovery`);
    const discEnv = {
      ...env,
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_INSERTS: "0",
      DATAFORSEO_ENABLED: "1",
    };
    try {
      const disc = await runDataForSeoLocalBusinessEnrichmentV1Mission({
        argv: opts.argv || [],
        args,
        env: discEnv,
        log,
      });
      totals.new_hotel_candidates_found += Number(
        disc.new_hotel_candidates || 0
      );
      totals.duplicate_risk_candidates += Number(
        disc.duplicate_risk_matches || disc.possible_duplicates || 0
      );
      totals.estimated_cost_usd += Number(disc.estimated_cost_usd || 0);
      totals.phone_held_by_policy += Number(disc.phone_candidates || 0);
      totals.coordinate_direct_held += Number(disc.coordinate_candidates || 0);
      try {
        const pack = writeDataForSeoNewHotelInsertReviewPack({
          runDir: disc.run_dir || runDir,
          root: ROOT,
        });
        totals.insert_review_pack_count = Number(pack.candidate_count || 0);
        passLog.push({
          pass: 6.5,
          name: "insert_review_pack",
          outer,
          status: pack.ok ? "ok" : "missing_queue",
          summary: `candidates=${pack.candidate_count || 0}`,
        });
      } catch (packErr) {
        passLog.push({
          pass: 6.5,
          name: "insert_review_pack",
          outer,
          status: "error",
          summary: String(packErr?.message || packErr),
        });
      }

      let insertCount = 0;
      if (insertPolicy.allowed && enableWrites && !candidateOnly) {
        census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
        const ins = await applyDataForSeoHighConfidenceInternalInserts({
          env,
          log,
          runDir: disc.run_dir || runDir,
          censusRecords: census,
          enableWrites: true,
          maxInserts: Number(env.CENSUS_MAX_HIGH_CONFIDENCE_INSERTS || 25),
        });
        insertCount = Number(ins.inserts || 0);
        totals.inserts += insertCount;
        totals.existing_records_updated += insertCount;
        wroteThisPass += insertCount;
        passLog.push({
          pass: 6.6,
          name: "high_confidence_internal_inserts",
          outer,
          status: ins.ok ? "ok" : "error",
          summary: `inserts=${insertCount} selected=${ins.selected || 0} skipped=${(ins.skipped || []).length}`,
        });
      }

      passLog.push({
        pass: 6,
        name: "new_hotel_discovery",
        outer,
        status: disc.status || "ok",
        summary: `new_hotels=${disc.new_hotel_candidates || 0} inserts=${insertCount}`,
      });
    } catch (err) {
      passLog.push({
        pass: 6,
        name: "new_hotel_discovery",
        outer,
        status: "error",
        summary: String(err?.message || err),
      });
      blockerReasons.push(`discovery_error:${err?.message || err}`);
    }
  } else if (
    !skipPasses.has("new_hotel_discovery") &&
    censusMode === CENSUS_MODE.FIELD_COMPLETION_ONLY
  ) {
    passLog.push({
      pass: 6,
      name: "new_hotel_discovery",
      outer,
      status: "skipped_field_completion_only",
      summary: "discovery skipped in field-completion-only",
    });
    // Optional: apply duplicate-safe inserts from existing review pack
    if (insertPolicy.allowed && enableWrites && !candidateOnly) {
      const ins = await applyDataForSeoHighConfidenceInternalInserts({
        env,
        log,
        runDir,
        censusRecords: census,
        enableWrites: true,
        maxInserts: Number(env.CENSUS_MAX_HIGH_CONFIDENCE_INSERTS || 25),
      });
      totals.inserts += Number(ins.inserts || 0);
      totals.existing_records_updated += Number(ins.inserts || 0);
      wroteThisPass += Number(ins.inserts || 0);
      passLog.push({
        pass: 6.6,
        name: "high_confidence_internal_inserts",
        outer,
        status: ins.ok ? "ok" : "error",
        summary: `inserts=${ins.inserts || 0} selected=${ins.selected || 0} from_existing_queue`,
      });
    }
  }

  if (!skipPasses.has("reaudit")) {
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
    const ledger = buildCensusGapLedger(census, {});
    const scorecard = buildCompletionScorecard(census, {});
    writeCensusGapLedger(ledger, scorecard, { runDir });
    passLog.push({
      pass: 7,
      name: "reaudit",
      outer,
      status: "ok",
      summary: `wrote_this_cycle=${wroteThisPass}`,
    });
  }

  const gapsAfter = countGaps(census);
  const nextBacklog = `Continue Autopilot: remaining address=${gapsAfter.missing_address} rooms=${gapsAfter.missing_rooms} coords=${gapsAfter.missing_coordinates} market=${gapsAfter.missing_market}; inserts_held=${!insertPolicy.allowed}`;

  let status = POLICY_CONTROLLER_STATUS.PARTIAL_SOURCE;
  if (
    !insertPolicy.allowed &&
    censusMode === CENSUS_MODE.GROWTH &&
    totals.new_hotel_candidates_found > 0
  ) {
    status = POLICY_CONTROLLER_STATUS.PARTIAL_INSERT;
  }
  const remainingSafe =
    gapsAfter.missing_address +
    gapsAfter.missing_rooms +
    gapsAfter.missing_coordinates +
    gapsAfter.missing_market;
  if (
    totals.existing_records_updated > 0 &&
    remainingSafe === 0 &&
    totals.new_hotel_candidates_found === 0
  ) {
    status = POLICY_CONTROLLER_STATUS.COMPLETE;
  }

  const founderApprovalNeeded =
    totals.inserts === 0 &&
    !insertPolicy.allowed &&
    totals.new_hotel_candidates_found > 0
      ? "Optional: enable high-confidence Census Only inserts (ENABLE_DATAFORSEO_LOCAL_INSERTS + ENABLE_HIGH_CONFIDENCE_INSERTS). Direct DataForSEO coordinates remain held."
      : "No — confidence-tiered internal Autopilot can continue under encoded policy. Medium fields stay internal-only until steward review.";

  const report = {
    ok: status !== POLICY_CONTROLLER_STATUS.BLOCKED,
    status,
    objective: POLICY_CONTROLLER_OBJECTIVE,
    version: POLICY_CONTROLLER_VERSION,
    policy_version: CENSUS_AUTOPILOT_APPROVED_POLICY_VERSION,
    confidence_tiered_policy_version: CONFIDENCE_TIERED_INTERNAL_POLICY_VERSION,
    generated_at: new Date().toISOString(),
    census_mode: censusMode,
    candidate_only: candidateOnly,
    founder_gate_between_passes: false,
    gates,
    insert_policy: insertPolicy,
    phone_provenance_schema_todo: PHONE_PROVENANCE_SCHEMA_TODO,
    passes: POLICY_CONTROLLER_PASSES,
    pass_log: passLog,
    records_scanned: recordsScanned,
    existing_records_updated: totals.existing_records_updated,
    address_writes: totals.address_writes,
    website_writes: totals.website_writes,
    phone_writes: totals.phone_writes,
    mapbox_coordinate_writes: totals.mapbox_coordinate_writes,
    medium_addresses_geocoded: totals.medium_addresses_geocoded,
    mapbox_rejects: totals.mapbox_rejects,
    medium_field_writes: totals.medium_field_writes,
    rooms_writes: totals.rooms_writes,
    market_writes: totals.market_writes,
    submarket_writes: totals.submarket_writes,
    phone_held_by_policy:
      totals.phone_held_by_policy || gapsAfter.missing_phone,
    coordinate_direct_held: totals.coordinate_direct_held,
    new_hotel_candidates_found: totals.new_hotel_candidates_found,
    insert_review_pack_count: totals.insert_review_pack_count,
    inserts: totals.inserts,
    duplicate_risk_candidates: totals.duplicate_risk_candidates,
    fields_written_by_confidence_tier: {
      Medium:
        totals.address_writes +
        totals.phone_writes +
        totals.medium_addresses_geocoded,
      High: totals.rooms_writes + totals.market_writes + totals.website_writes,
    },
    public_safe_vs_internal_only: {
      public_safe_writes: 0,
      internal_only_writes:
        totals.address_writes +
        totals.phone_writes +
        totals.medium_addresses_geocoded +
        totals.inserts,
      note: "Medium confidence fields and Census Only inserts are internal-only; not public-facing",
    },
    gaps_before: gapsBefore,
    gaps_after: gapsAfter,
    remaining_missing_fields: gapsAfter,
    blocker_reasons: [...new Set(blockerReasons)],
    estimated_cost_usd: totals.estimated_cost_usd,
    next_backlog: nextBacklog,
    founder_approval_needed: founderApprovalNeeded,
    brand_setup_writes: 0,
    brand_explorer_writes: 0,
    owner_operator_date_writes: 0,
    census_writes: totals.existing_records_updated,
    production_target: {
      base: "Deal Capture Platform",
      table: "Hotel Property Census",
      table_id: CENSUS_TABLE_ID,
    },
    run_dir: runDir,
    enable_production_writes: enableWrites && !candidateOnly,
  };

  writeJson(path.join(runDir, "mission-report.json"), report);
  const reportJson = path.join(
    ROOT,
    "reports/research-engine-v2/census-autopilot-policy-controller-v1.json"
  );
  const reportMd = path.join(
    ROOT,
    "reports/research-engine-v2/census-autopilot-policy-controller-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/census-autopilot-policy-controller-v1.md"
  );
  writeJson(reportJson, report);
  const md = renderReportMd(report);
  writeMd(reportMd, md);
  writeMd(docsPath, md);

  log(
    `[policy-controller] done status=${status} updated=${totals.existing_records_updated} address=${totals.address_writes} mapbox=${totals.mapbox_coordinate_writes} rooms=${totals.rooms_writes}`
  );
  return report;
}
