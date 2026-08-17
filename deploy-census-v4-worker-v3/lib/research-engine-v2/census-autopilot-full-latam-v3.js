/**
 * Full LATAM Census Autopilot Controller v3 — gap-ledger driven.
 *
 * Self-directed loop: audit → prioritize → execute High writes → re-audit.
 * No founder gate between passes.
 *
 * Write target: Hotel Property Census only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
} from "./production-census-source-of-truth.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  checkAutopilotApplyEnv,
  applyPreflight,
  parseAutopilotArgs,
} from "./census-autopilot-apply-guard.js";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import {
  buildActiveBrandIndex,
  classifyCensusReviewReasons,
} from "./census-brand-governance.js";
import { buildCanonicalBrandDictionary } from "./census-brand-canonical-dictionary.js";
import {
  buildStateRegionMapPatches,
  resolveStateRegionFromCity,
} from "./census-city-to-state-map.js";
import {
  resolveMarketFromCity,
  resolveContinentSubContinentFromCountry,
  resolveSubmarketHighOnly,
  CENSUS_GEO_FIELDS,
} from "./census-region-market-map.js";
import {
  buildCensusGapLedger,
  buildCompletionScorecard,
  writeCensusGapLedger,
  isOtaOrBlockedUrl,
  isHttpUrl,
} from "./census-gap-ledger.js";
import {
  prioritizeGapActions,
  selectSourceStrategiesForPass,
  buildExecutableBacklog,
  SOURCE_STRATEGY,
} from "./census-gap-prioritization.js";
import { runLevel2SourceExtractionV1Mission } from "./census-autopilot-level-2-source-extraction-v1.js";
import { runCalaCensusCompletionV1Mission } from "./census-autopilot-cala-census-completion-v1.js";
import { runCoverageReconciliation } from "./census-autopilot-coverage-reconciliation.js";
import { runSourceConfirmedCensusV2Mission } from "./census-autopilot-source-confirmed-census-v2.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const FULL_LATAM_AUTOPILOT_V3_OBJECTIVE = "full-latam-census-autopilot-v3";
export const FULL_LATAM_AUTOPILOT_V3_VERSION = "full-latam-census-autopilot-v3";

export const FULL_LATAM_AUTOPILOT_V3_STATUS = Object.freeze({
  COMPLETE: "production_census_full_latam_autopilot_v3_complete",
  PARTIAL_SOURCE:
    "production_census_full_latam_autopilot_v3_partial_source_remaining",
  PARTIAL_ADAPTER:
    "production_census_full_latam_autopilot_v3_partial_adapter_backlog_remaining",
  BLOCKED: "production_census_full_latam_autopilot_v3_blocked_safety_stop",
});

export const CENSUS_MODE = Object.freeze({
  GROWTH: "growth",
  FIELD_COMPLETION_ONLY: "field-completion-only",
  GOVERNANCE_ONLY: "governance-only",
  CANDIDATE_ONLY: "candidate-only",
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
  "Source URL",
  "Official Property URL",
  "Family / Source Family",
  "Human Review Required",
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Latitude",
  "Longitude",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Data Confidence Tier",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Production Use Status",
  "Public Display Review Status",
  "Radar Display Status",
  "Radar Display Reason",
];

const GEO_ALLOWED = new Set([
  "State / Region",
  "Market",
  "Submarket",
  "Continent",
  "Sub-Continent",
  "Official Property URL",
  "Source URL",
  "Family / Source Family",
  "Source Type",
  "Source Confidence",
  "Last Reviewed Date",
  "Enrichment Status",
  "Enrichment Priority",
  "Data Confidence Tier",
]);

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

/**
 * Parse --census-mode from argv / args.
 * @param {string[]} argv
 * @param {object} [args]
 */
export function resolveCensusMode(argv = [], args = {}) {
  if (args.censusMode) {
    const m = String(args.censusMode).toLowerCase();
    if (Object.values(CENSUS_MODE).includes(m)) return m;
  }
  const i = argv.indexOf("--census-mode");
  if (i >= 0 && argv[i + 1] && !argv[i + 1].startsWith("--")) {
    const m = String(argv[i + 1]).toLowerCase();
    if (m === "census_growth" || m === "census-growth") return CENSUS_MODE.GROWTH;
    if (Object.values(CENSUS_MODE).includes(m)) return m;
  }
  return CENSUS_MODE.GROWTH;
}

export function assertNoInsertInFieldCompletionMode(censusMode, insertCount) {
  if (
    censusMode === CENSUS_MODE.FIELD_COMPLETION_ONLY &&
    Number(insertCount || 0) > 0
  ) {
    return {
      ok: false,
      reason: "field_completion_only_insert_forbidden",
      insert_count: insertCount,
    };
  }
  return { ok: true };
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
      throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    }
    out.push(...(json.records || []));
    offset = json.offset;
    await new Promise((r) => setTimeout(r, 120));
  } while (offset);
  return out;
}

async function applyPatches(proposals, { baseId, token, tableId, batchSize, log, allowed }) {
  let updatesApplied = 0;
  const writeErrors = [];
  const fieldsWritten = new Set();
  const allow = allowed || GEO_ALLOWED;
  const size = Math.min(100, Math.max(1, batchSize || 100));

  for (let i = 0; i < proposals.length; i += size) {
    const chunk = proposals.slice(i, i + size);
    const updates = chunk
      .map((p) => {
        /** @type {Record<string, unknown>} */
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (isForbiddenAutopilotField(k)) continue;
          if (!allow.has(k)) continue;
          if (v === undefined || v === null || v === "") continue;
          fields[k] = v;
          fieldsWritten.add(k);
        }
        return { id: p.record_id, fields };
      })
      .filter((u) => Object.keys(u.fields).length > 0);

    for (let j = 0; j < updates.length; j += 10) {
      const records = updates.slice(j, j + 10);
      if (!records.length) continue;
      try {
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
        } else {
          updatesApplied += (json.records || []).length;
        }
      } catch (err) {
        writeErrors.push({ error: err?.message || String(err) });
      }
      await new Promise((r) => setTimeout(r, 180));
    }
    log?.(
      `[full-latam-v3] patch batch ${Math.floor(i / size) + 1}: written=${updatesApplied} errors=${writeErrors.length}`
    );
  }
  return { updatesApplied, writeErrors, fieldsWritten: [...fieldsWritten] };
}

/**
 * Promote Official Property URL from Source URL when property-level official.
 * Never OTA / Google. Never overwrite stronger official URL with weaker.
 */
export function buildHotelUrlCompletionPatches(records = []) {
  const proposals = [];
  const skippedOta = [];
  for (const rec of records) {
    const f = rec.fields || {};
    const official = String(f["Official Property URL"] || "").trim();
    const source = String(f["Source URL"] || "").trim();
    const family = String(f["Family / Source Family"] || f["Brand Family"] || "").trim();

    if (isHttpUrl(official) && !isOtaOrBlockedUrl(official)) continue;

    const candidate = isHttpUrl(source) ? source : "";
    if (!candidate) continue;
    if (isOtaOrBlockedUrl(candidate)) {
      skippedOta.push({ record_id: rec.id, url: candidate });
      continue;
    }

    // Require property-ish path (not bare domain homepage)
    try {
      const u = new URL(candidate);
      const pathOk = (u.pathname || "/").length > 1;
      const hostOk = /\.(hilton|marriott|ihg|choicehotels|accor|wyndham|preferred|hyatt)\./i.test(
        u.hostname
      ) || /hilton\.com|marriott\.com|ihg\.com|choicehotels\.com|all\.accor|wyndhamhotels|preferredhotels/i.test(
        u.hostname
      );
      if (!pathOk || !hostOk) continue;
    } catch {
      continue;
    }

    /** @type {Record<string, unknown>} */
    const patch = {
      "Official Property URL": candidate,
      "Last Reviewed Date": todayIsoDate(),
      "Source Confidence": "High",
    };
    if (!family) {
      // leave family blank — do not invent
    } else if (!f["Family / Source Family"]) {
      patch["Family / Source Family"] = family;
    }
    proposals.push({
      record_id: rec.id,
      reason: "hotel_url_from_official_source_url",
      confidence: "High",
      patch,
    });
  }
  return { proposals, skippedOta };
}

/**
 * Market + continent patches from approved maps (High only).
 */
export function buildMarketGeographyPatches(records = []) {
  const proposals = [];
  const stewardMarket = [];
  for (const rec of records) {
    const f = rec.fields || {};
    const city = String(f.City || "").trim();
    const country = String(f.Country || "").trim();
    /** @type {Record<string, unknown>} */
    const patch = {};

    const cont = resolveContinentSubContinentFromCountry(country);
    if (cont) {
      if (!String(f.Continent || "").trim()) patch[CENSUS_GEO_FIELDS.continent] = cont.continent;
      if (!String(f["Sub-Continent"] || "").trim()) {
        patch[CENSUS_GEO_FIELDS.subContinent] = cont.subContinent;
      }
    }

    if (!String(f.Market || "").trim()) {
      const m = resolveMarketFromCity({ city, country });
      if (m.ok && m.market) {
        patch[CENSUS_GEO_FIELDS.market] = m.market;
      } else if (city && country) {
        stewardMarket.push({
          record_id: rec.id,
          city,
          country,
          reason: m.reason || "market_mapping_missing",
        });
      }
    }

    if (
      String(f.Market || patch[CENSUS_GEO_FIELDS.market] || "").trim() &&
      !String(f.Submarket || "").trim()
    ) {
      const sub = resolveSubmarketHighOnly({
        market: String(f.Market || patch[CENSUS_GEO_FIELDS.market] || ""),
        city,
        address: f.Address,
        propertyName: f["Property Name"],
      });
      if (sub.ok && sub.submarket) {
        patch[CENSUS_GEO_FIELDS.submarket] = sub.submarket;
      }
    }

    if (Object.keys(patch).length) {
      patch["Last Reviewed Date"] = todayIsoDate();
      proposals.push({
        record_id: rec.id,
        reason: "approved_market_geography_map",
        confidence: "High",
        patch,
      });
    }
  }
  return { proposals, stewardMarket };
}

function targetsMet(scorecard) {
  const p = scorecard?.percents || {};
  const t = scorecard?.targets || {};
  const checks = [
    ["clean_core", p.clean_core, t.clean_core],
    ["state_region", p.state_region, t.state_region],
    ["market", p.market, t.market],
    ["hotel_url", p.hotel_url, t.hotel_url],
    ["address_high", p.address_high, t.address_high],
    ["lat_long", p.lat_long, t.lat_long],
    ["phone", p.phone, t.phone],
    ["rooms", p.rooms, t.rooms],
    ["complete_census_v1", p.complete_census_v1, t.complete_census_v1],
  ];
  const missed = checks.filter(([, got, need]) => (got || 0) < (need || 0));
  return { met: missed.length === 0, missed };
}

function renderMissionMd(report) {
  const b = report.before_scorecard?.percents || {};
  const a = report.after_scorecard?.percents || {};
  const bc = report.before_scorecard?.counts || {};
  const ac = report.after_scorecard?.counts || {};
  return [
    `# Full LATAM Census Autopilot Controller v3`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${FULL_LATAM_AUTOPILOT_V3_OBJECTIVE}\``,
    `**Census mode:** \`${report.census_mode}\``,
    `**Region:** ${report.region}`,
    `**Write target:** Hotel Property Census (\`${CENSUS_TABLE_ID}\`)`,
    `**Airtable writes:** ${report.airtable_writes ? "yes" : "no"}`,
    `**Brand Setup / Brand Explorer writes:** false`,
    `**Founder gate between passes:** false`,
    ``,
    `## Scorecard before → after`,
    ``,
    `| Metric | Before % | After % | Before n | After n |`,
    `| --- | ---: | ---: | ---: | ---: |`,
    `| Total records | — | — | ${bc.total ?? "—"} | ${ac.total ?? "—"} |`,
    `| Clean Core | ${b.clean_core ?? "—"} | ${a.clean_core ?? "—"} | ${bc.clean_core ?? "—"} | ${ac.clean_core ?? "—"} |`,
    `| State / Region | ${b.state_region ?? "—"} | ${a.state_region ?? "—"} | ${bc.state_region ?? "—"} | ${ac.state_region ?? "—"} |`,
    `| Market | ${b.market ?? "—"} | ${a.market ?? "—"} | ${bc.market ?? "—"} | ${ac.market ?? "—"} |`,
    `| Hotel URL | ${b.hotel_url ?? "—"} | ${a.hotel_url ?? "—"} | ${bc.hotel_url ?? "—"} | ${ac.hotel_url ?? "—"} |`,
    `| Address | ${b.address ?? "—"} | ${a.address ?? "—"} | ${bc.address ?? "—"} | ${ac.address ?? "—"} |`,
    `| Address High | ${b.address_high ?? "—"} | ${a.address_high ?? "—"} | ${bc.address_high ?? "—"} | ${ac.address_high ?? "—"} |`,
    `| Lat/Long | ${b.lat_long ?? "—"} | ${a.lat_long ?? "—"} | ${bc.lat_long ?? "—"} | ${ac.lat_long ?? "—"} |`,
    `| Phone | ${b.phone ?? "—"} | ${a.phone ?? "—"} | ${bc.phone ?? "—"} | ${ac.phone ?? "—"} |`,
    `| Rooms | ${b.rooms ?? "—"} | ${a.rooms ?? "—"} | ${bc.rooms ?? "—"} | ${ac.rooms ?? "—"} |`,
    `| Complete Census v1 | ${b.complete_census_v1 ?? "—"} | ${a.complete_census_v1 ?? "—"} | ${bc.complete_census_v1 ?? "—"} | ${ac.complete_census_v1 ?? "—"} |`,
    `| Governance Hold | ${b.governance_hold ?? "—"} | ${a.governance_hold ?? "—"} | ${bc.governance_hold ?? "—"} | ${ac.governance_hold ?? "—"} |`,
    `| Data Quality Hold | ${b.data_quality_hold ?? "—"} | ${a.data_quality_hold ?? "—"} | ${bc.data_quality_hold ?? "—"} | ${ac.data_quality_hold ?? "—"} |`,
    ``,
    `## Writes`,
    ``,
    `- Records updated: ${report.records_updated ?? 0}`,
    `- Records inserted: ${report.records_inserted ?? 0}`,
    `- Inserts by parent: ${JSON.stringify(report.inserts_by_parent || {})}`,
    `- Fields written: ${(report.fields_written || []).join(", ") || "—"}`,
    `- Passes run: ${report.passes_run ?? 0}`,
    `- Safety stops: ${(report.safety_stops || []).join("; ") || "none"}`,
    ``,
    `## Top recommended actions (final)`,
    ``,
    ...((report.final_ranked?.top_actions || []).slice(0, 10).map(
      (a, i) =>
        `${i + 1}. **${a.field}** via \`${a.strategy}\` — ${a.records_affected} records, expected High ~${a.expected_high_yield}`
    ) || ["—"]),
    ``,
    `## Command to continue`,
    ``,
    "```bash",
    report.backlog?.command_to_continue || "",
    "```",
    ``,
  ].join("\n");
}

export function writeFullLatamV3Reports(report) {
  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-full-latam-autopilot-v3.json"
  );
  const mdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-full-latam-autopilot-v3.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-full-latam-autopilot-v3.md"
  );
  const md = renderMissionMd(report);
  writeJson(jsonPath, report);
  writeText(mdPath, md);
  writeText(docsPath, md);
  return { jsonPath, mdPath, docsPath };
}

/**
 * Mission entrypoint — self-directed Autopilot Controller v3.
 */
export async function runFullLatamCensusAutopilotV3Mission(opts = {}) {
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || parseAutopilotArgs(argv);
  const env = opts.env || process.env;
  const log = opts.log || ((m) => console.log(m));
  const started = Date.now();
  const censusMode = resolveCensusMode(argv, { ...args, censusMode: opts.censusMode });
  const maxPasses = Math.min(10, Math.max(1, args.maxPasses || opts.maxPasses || 10));
  const region = args.region || "CALA";

  const envCheck = checkAutopilotApplyEnv(env);
  const preflight = applyPreflight(args, envCheck);
  const enableWrites = Boolean(
    opts.enableProductionWrites &&
      argv.includes("--enable-production-writes") &&
      args.allApplyConfirms &&
      envCheck.allOk &&
      preflight.ok &&
      args.mode === "mission"
  );

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    const blocked = {
      ok: false,
      status: FULL_LATAM_AUTOPILOT_V3_STATUS.BLOCKED,
      objective: FULL_LATAM_AUTOPILOT_V3_OBJECTIVE,
      blocked_reason: writeTarget.reason || "wrong_census_target",
      airtable_writes: false,
      safety_stops: ["wrong_census_target"],
    };
    writeFullLatamV3Reports(blocked);
    return blocked;
  }

  if (args.mode === "mission" && !preflight.ok) {
    const blocked = {
      ok: false,
      status: FULL_LATAM_AUTOPILOT_V3_STATUS.BLOCKED,
      objective: FULL_LATAM_AUTOPILOT_V3_OBJECTIVE,
      blocked_reason: "confirmation_or_env",
      blockers: preflight.blockers,
      airtable_writes: false,
      safety_stops: preflight.blockers || ["confirmation_or_env"],
    };
    writeFullLatamV3Reports(blocked);
    return blocked;
  }

  const token = opts.token ?? resolvePat();
  const bases = opts.bases ?? resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    const blocked = {
      ok: false,
      status: FULL_LATAM_AUTOPILOT_V3_STATUS.BLOCKED,
      objective: FULL_LATAM_AUTOPILOT_V3_OBJECTIVE,
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
      safety_stops: ["missing_airtable_credentials"],
    };
    writeFullLatamV3Reports(blocked);
    return blocked;
  }

  const runDir =
    opts.runDir ||
    path.join(
      ROOT,
      "reports/research-engine-v2/autopilot",
      `${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${region}-full-latam-autopilot-v3`
    );
  fs.mkdirSync(runDir, { recursive: true });

  log(`[full-latam-v3] Phase SoT OK — Hotel Property Census ${CENSUS_TABLE_ID}`);
  log(`[full-latam-v3] census_mode=${censusMode} max_passes=${maxPasses} writes=${enableWrites}`);

  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const activeIndex = buildActiveBrandIndex({ region });
  const dictionary = buildCanonicalBrandDictionary({ region });

  const beforeLedger = buildCensusGapLedger(census, { activeIndex, dictionary });
  const beforeScore = buildCompletionScorecard(census, { activeIndex, dictionary });
  writeCensusGapLedger(beforeLedger, beforeScore, { runDir });
  writeJson(path.join(runDir, "before-scorecard.json"), beforeScore);

  const safetyStops = [];
  const passSummaries = [];
  const completedActions = [];
  const attemptedInsufficient = [];
  let recordsUpdated = 0;
  let recordsInserted = 0;
  /** @type {Record<string, number>} */
  const insertsByParent = {};
  /** @type {Set<string>} */
  const fieldsWritten = new Set();
  let lastRanked = prioritizeGapActions(beforeLedger);

  for (let pass = 1; pass <= maxPasses; pass += 1) {
    log(`[full-latam-v3] ===== PASS ${pass}/${maxPasses} (no founder gate) =====`);
    const ledger = buildCensusGapLedger(census, { activeIndex, dictionary });
    const ranked = prioritizeGapActions(ledger);
    lastRanked = ranked;
    const selected = selectSourceStrategiesForPass(ranked, { pass, censusMode });
    writeJson(path.join(runDir, `pass-${pass}-strategy.json`), selected);

    let passUpdates = 0;
    let passInserts = 0;

    // --- Deterministic geo / URL phases (always except pure governance-only for L2) ---
    if (censusMode !== CENSUS_MODE.GOVERNANCE_ONLY || pass === 1) {
      const statePack = buildStateRegionMapPatches(census);
      const marketPack = buildMarketGeographyPatches(census);
      const urlPack = buildHotelUrlCompletionPatches(census);
      const geoProposals = [
        ...statePack.proposals,
        ...marketPack.proposals,
        ...urlPack.proposals,
      ];
      writeJson(path.join(runDir, `pass-${pass}-geo-url-proposals.json`), {
        state: statePack.proposals.length,
        market: marketPack.proposals.length,
        hotel_url: urlPack.proposals.length,
        steward_state: statePack.stewardNeeded?.length || 0,
        steward_market: marketPack.stewardMarket?.length || 0,
        skipped_ota_urls: urlPack.skippedOta?.length || 0,
      });

      if (enableWrites && geoProposals.length) {
        const applied = await applyPatches(geoProposals, {
          baseId: bases.target_base_id,
          token,
          tableId: CENSUS_TABLE_ID,
          batchSize: args.batchSize || 100,
          log,
        });
        passUpdates += applied.updatesApplied;
        applied.fieldsWritten.forEach((f) => fieldsWritten.add(f));
        if (applied.writeErrors.length) {
          safetyStops.push(`pass_${pass}_geo_errors:${applied.writeErrors.length}`);
        }
        completedActions.push({
          pass,
          action: "geo_url_completion",
          updates: applied.updatesApplied,
        });
        census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
      } else {
        log(
          `[full-latam-v3] pass ${pass} geo/url proposals=${geoProposals.length} (write=${enableWrites})`
        );
        if (!geoProposals.length) {
          attemptedInsufficient.push({
            pass,
            action: "geo_url_completion",
            reason: "no_high_deterministic_proposals",
          });
        }
      }
    }

    // --- Growth inserts (official inventory only) ---
    if (censusMode === CENSUS_MODE.GROWTH && pass <= 2) {
      try {
        log(`[full-latam-v3] pass ${pass} coverage reconciliation (growth inserts)…`);
        const cov = await runCoverageReconciliation({
          region,
          parentCompany: args.parentCompany || null,
          brand: args.brand || null,
          country: args.country || null,
          mode: args.mode || "mission",
          batchSize: args.batchSize || 100,
          enableProductionWrites: enableWrites,
          allApplyConfirms: Boolean(args.allApplyConfirms),
          confirms: args.confirms,
          env,
          log,
          censusRecords: census,
        });
        const inserts = Number(
          cov?.inserts_applied || cov?.records_inserted || cov?.insert_count || 0
        );
        const insertGuard = assertNoInsertInFieldCompletionMode(censusMode, inserts);
        if (!insertGuard.ok) {
          safetyStops.push(insertGuard.reason);
          break;
        }
        passInserts += inserts;
        recordsInserted += inserts;
        const byParent = cov?.inserts_by_parent || cov?.by_parent || {};
        for (const [p, n] of Object.entries(byParent)) {
          insertsByParent[p] = (insertsByParent[p] || 0) + Number(n || 0);
        }
        completedActions.push({
          pass,
          action: "coverage_reconciliation_growth",
          inserts,
          updates: cov?.updates_applied || 0,
          status: cov?.status,
        });
        if (inserts > 0 || (cov?.updates_applied || 0) > 0) {
          census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
        }
      } catch (err) {
        attemptedInsufficient.push({
          pass,
          action: "coverage_reconciliation",
          reason: err?.message || String(err),
        });
        log(`[full-latam-v3] coverage error: ${err?.message || err}`);
      }
    }

    // --- Level 2 address/phone/rooms (not governance-only) ---
    if (censusMode !== CENSUS_MODE.GOVERNANCE_ONLY && pass >= 1 && pass <= 4) {
      try {
        log(`[full-latam-v3] pass ${pass} Level 2 source extraction…`);
        const l2 = await runLevel2SourceExtractionV1Mission({
          argv,
          args: { ...args, objective: "level-2-source-extraction-v1" },
          env,
          enableProductionWrites: enableWrites,
          token,
          bases,
          log,
          skipCalaChain: true,
        });
        const u = Number(l2?.updates_applied || 0);
        passUpdates += u;
        completedActions.push({
          pass,
          action: "level_2_source_extraction",
          updates: u,
          status: l2?.status,
        });
        if (u === 0) {
          attemptedInsufficient.push({
            pass,
            action: "level_2_source_extraction",
            reason: "no_high_proposals_or_source_insufficient",
          });
        }
        census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
      } catch (err) {
        attemptedInsufficient.push({
          pass,
          action: "level_2",
          reason: err?.message || String(err),
        });
      }
    }

    // --- Cala completion for coords / market / phone follow-on ---
    if (censusMode !== CENSUS_MODE.GOVERNANCE_ONLY && (pass === 2 || pass === 3 || pass === 5)) {
      try {
        log(`[full-latam-v3] pass ${pass} cala-census-completion-v1…`);
        const cala = await runCalaCensusCompletionV1Mission({
          argv: censusMode === CENSUS_MODE.FIELD_COMPLETION_ONLY
            ? [...argv, "--cleanup-existing-only"]
            : argv,
          args: {
            ...args,
            objective: "cala-census-completion-v1",
            cleanupExistingOnly:
              censusMode === CENSUS_MODE.FIELD_COMPLETION_ONLY || args.cleanupExistingOnly,
          },
          env,
          enableProductionWrites: enableWrites,
          log,
        });
        const u = Number(cala?.updates_applied || 0);
        const ins = Number(cala?.inserts_applied || cala?.records_inserted || 0);
        const insertGuard = assertNoInsertInFieldCompletionMode(censusMode, ins);
        if (!insertGuard.ok) {
          safetyStops.push(insertGuard.reason);
          break;
        }
        passUpdates += u;
        passInserts += ins;
        recordsInserted += ins;
        completedActions.push({
          pass,
          action: "cala_census_completion",
          updates: u,
          inserts: ins,
          status: cala?.status,
        });
        census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
      } catch (err) {
        attemptedInsufficient.push({
          pass,
          action: "cala_completion",
          reason: err?.message || String(err),
        });
      }
    }

    // --- Source-confirmed remap mid-loop ---
    if (pass === 4 || pass === 6) {
      try {
        const sc = await runSourceConfirmedCensusV2Mission({
          argv,
          args: { ...args, objective: "source-confirmed-census-v2" },
          env,
          enableProductionWrites: enableWrites,
          log,
        });
        const u = Number(sc?.updates_applied || 0);
        passUpdates += u;
        completedActions.push({
          pass,
          action: "source_confirmed_census_v2",
          updates: u,
          status: sc?.status,
        });
        census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
      } catch (err) {
        attemptedInsufficient.push({
          pass,
          action: "source_confirmed",
          reason: err?.message || String(err),
        });
      }
    }

    recordsUpdated += passUpdates;
    const afterPassScore = buildCompletionScorecard(census, { activeIndex, dictionary });
    passSummaries.push({
      pass,
      updates: passUpdates,
      inserts: passInserts,
      scorecard: afterPassScore.percents,
      selected_strategies: (selected.selected_actions || []).map((a) => a.strategy),
    });
    writeJson(path.join(runDir, `pass-${pass}-summary.json`), passSummaries[passSummaries.length - 1]);

    log(
      `[full-latam-v3] pass ${pass} done updates=${passUpdates} inserts=${passInserts} address%=${afterPassScore.percents.address} phone%=${afterPassScore.percents.phone}`
    );

    // Exhaustion: no High writes this pass
    if (passUpdates === 0 && passInserts === 0 && pass >= 3) {
      log(`[full-latam-v3] no High work remaining after pass ${pass} — stopping loop`);
      break;
    }

    const met = targetsMet(afterPassScore);
    if (met.met) {
      log(`[full-latam-v3] scorecard targets met — stopping`);
      break;
    }
  }

  const afterLedger = buildCensusGapLedger(census, { activeIndex, dictionary });
  const afterScore = buildCompletionScorecard(census, { activeIndex, dictionary });
  writeCensusGapLedger(afterLedger, afterScore, { runDir });
  lastRanked = prioritizeGapActions(afterLedger);

  const backlog = buildExecutableBacklog({
    completed: completedActions,
    attempted_insufficient: attemptedInsufficient,
    ranked: lastRanked,
    region,
    censusMode,
  });
  writeJson(path.join(runDir, "executable-backlog.json"), backlog);

  const targetCheck = targetsMet(afterScore);
  let status = FULL_LATAM_AUTOPILOT_V3_STATUS.PARTIAL_SOURCE;
  if (safetyStops.some((s) => /wrong_census|missing_airtable|confirmation|insert_forbidden/i.test(s))) {
    status = FULL_LATAM_AUTOPILOT_V3_STATUS.BLOCKED;
  } else if (targetCheck.met) {
    status = FULL_LATAM_AUTOPILOT_V3_STATUS.COMPLETE;
  } else if ((lastRanked.top_actions || []).some((a) => !a.adapter_exists && a.records_affected > 20)) {
    status = FULL_LATAM_AUTOPILOT_V3_STATUS.PARTIAL_ADAPTER;
  } else {
    status = FULL_LATAM_AUTOPILOT_V3_STATUS.PARTIAL_SOURCE;
  }

  const report = {
    ok: status !== FULL_LATAM_AUTOPILOT_V3_STATUS.BLOCKED,
    status,
    version: FULL_LATAM_AUTOPILOT_V3_VERSION,
    objective: FULL_LATAM_AUTOPILOT_V3_OBJECTIVE,
    census_mode: censusMode,
    region,
    scope: args.scope || "official-parent-inventory",
    strategy: args.strategy || "highest-yield-safe",
    mode: args.mode || "mission",
    generated_at: new Date().toISOString(),
    elapsed_ms: Date.now() - started,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    airtable_writes: enableWrites && (recordsUpdated > 0 || recordsInserted > 0),
    brand_setup_writes: false,
    brand_explorer_writes: false,
    founder_gate_between_passes: false,
    passes_run: passSummaries.length,
    max_passes: maxPasses,
    records_updated: recordsUpdated,
    records_inserted: recordsInserted,
    inserts_by_parent: insertsByParent,
    fields_written: [...fieldsWritten],
    before_scorecard: beforeScore,
    after_scorecard: afterScore,
    target_check: targetCheck,
    pass_summaries: passSummaries,
    final_ranked: {
      top_actions: lastRanked.top_actions,
      steward_backlog: lastRanked.steward_backlog,
    },
    backlog,
    gap_ledger_paths: {
      json: "reports/research-engine-v2/census-gap-ledger.json",
      md: "reports/research-engine-v2/census-gap-ledger.md",
      docs: "docs/data-intelligence/census-gap-ledger.md",
    },
    safety_stops: safetyStops,
    run_dir: runDir,
    next_recommended_action: backlog.command_to_continue,
  };

  writeJson(path.join(runDir, "final-summary.json"), report);
  writeFullLatamV3Reports(report);
  log(
    `[full-latam-v3] done status=${status} updated=${recordsUpdated} inserted=${recordsInserted} clean_core ${beforeScore.percents.clean_core}%→${afterScore.percents.clean_core}%`
  );
  return report;
}

// Re-export helpers used in tests
export {
  resolveStateRegionFromCity,
  classifyCensusReviewReasons,
};
