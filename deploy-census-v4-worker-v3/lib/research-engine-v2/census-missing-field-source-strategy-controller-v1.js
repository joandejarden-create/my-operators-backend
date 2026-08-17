/**
 * Census Missing Field Source Strategy Controller v1.
 *
 * Objective: census-missing-field-source-strategy-controller-v1
 * Thin specialization of Autopilot Policy Controller for post-discovery
 * field completion (Hotel Property Census only).
 *
 * Does not write Brand Explorer / Brand Setup / owner-operator-dates /
 * Recent Momentum / Company Validated / Brand Verified.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  runCensusAutopilotPolicyControllerV1Mission,
  POLICY_CONTROLLER_STATUS,
} from "./census-autopilot-policy-controller-v1.js";
import { CENSUS_MODE } from "./census-autopilot-full-latam-v3.js";
import { productionHotelPropertyCensus } from "./production-census-source-of-truth.js";
import {
  DISCOVERY_PROGRESS_FILE,
  DISCOVERY_STALL_DIAGNOSTICS_FILE,
  DISCOVERY_CHECKPOINT_FILE,
} from "./census-autopilot-v4/discovery-railway-safe.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const MISSING_FIELD_SOURCE_STRATEGY_OBJECTIVE =
  "census-missing-field-source-strategy-controller-v1";
export const MISSING_FIELD_SOURCE_STRATEGY_VERSION =
  "census-missing-field-source-strategy-controller-v1";

export const MISSING_FIELD_SOURCE_STRATEGY_STATUS = Object.freeze({
  COMPLETE: "production_census_missing_field_source_strategy_controller_v1_complete",
  PARTIAL_SOURCE:
    "production_census_missing_field_source_strategy_controller_v1_partial_source_remaining",
  PARTIAL_NETWORK:
    "production_census_missing_field_source_strategy_controller_v1_partial_network_remaining",
  BLOCKED: "production_census_missing_field_source_strategy_controller_v1_blocked",
});

/** Priority fields for this controller (candidate-only for owner/operator/dates). */
export const MISSING_FIELD_PRIORITY = Object.freeze([
  "identity_dedupe",
  "Address",
  "State / Region",
  "Market",
  "Submarket",
  "Latitude_Longitude_Mapbox_after_validated_address",
  "Official Property URL",
  "Phone_Medium_internal_with_provenance",
  "Rooms / Keys",
]);

export const HELD_FIELDS_CANDIDATE_ONLY = Object.freeze([
  "owner",
  "operator",
  "developer",
  "opening date",
  "renovation date",
  "affiliation start date",
  "Recent Momentum",
  "Company Validated",
  "Brand Verified",
  "Brand Status",
]);

function readJsonSafe(fp) {
  try {
    if (!fs.existsSync(fp)) return null;
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return null;
  }
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}

/**
 * Load V4 discovery resume/partial artifacts (Railway volume or local OUT).
 */
export function loadDiscoveryPartialSnapshot(outDir) {
  const progress = readJsonSafe(path.join(outDir, DISCOVERY_PROGRESS_FILE));
  const stalls = readJsonSafe(path.join(outDir, DISCOVERY_STALL_DIAGNOSTICS_FILE));
  const checkpoint = readJsonSafe(path.join(outDir, DISCOVERY_CHECKPOINT_FILE));
  const meta = readJsonSafe(path.join(outDir, "34b-controller-discovery-cache-meta.json"));
  const completed = Object.keys(checkpoint?.completed || {}).length;
  const timedOut = Object.keys(checkpoint?.timed_out || {}).length;
  const failed = Object.keys(checkpoint?.failed || {}).length;
  return {
    discovery_units_completed: completed,
    discovery_units_timed_out: timedOut,
    discovery_units_failed: failed,
    discovery_lane_status: meta?.discovery_lane_status || progress?.final?.lane_status || null,
    discovered_count: meta?.n ?? progress?.final?.discovered_count ?? null,
    stalls: stalls?.stalls || [],
    last_stall: stalls?.last_stall || null,
    progress_totals: progress?.totals || null,
  };
}

function mapPolicyStatus(policyStatus, discoveryPartial) {
  if (policyStatus === POLICY_CONTROLLER_STATUS.BLOCKED) {
    return MISSING_FIELD_SOURCE_STRATEGY_STATUS.BLOCKED;
  }
  const timedOut = Number(discoveryPartial?.discovery_units_timed_out || 0);
  if (timedOut > 0) return MISSING_FIELD_SOURCE_STRATEGY_STATUS.PARTIAL_NETWORK;
  if (
    policyStatus === POLICY_CONTROLLER_STATUS.PARTIAL_SOURCE ||
    policyStatus === POLICY_CONTROLLER_STATUS.PARTIAL_INSERT
  ) {
    return MISSING_FIELD_SOURCE_STRATEGY_STATUS.PARTIAL_SOURCE;
  }
  if (policyStatus === POLICY_CONTROLLER_STATUS.COMPLETE) {
    return MISSING_FIELD_SOURCE_STRATEGY_STATUS.COMPLETE;
  }
  return MISSING_FIELD_SOURCE_STRATEGY_STATUS.PARTIAL_SOURCE;
}

function buildMarkdown(report) {
  const g = report.gaps_after || {};
  const d = report.discovery_partial || {};
  return `# Census Missing Field Source Strategy Controller v1

**Status:** \`${report.status}\`
**Objective:** \`${MISSING_FIELD_SOURCE_STRATEGY_OBJECTIVE}\`
**Generated:** ${report.generated_at}
**Census mode:** \`${report.census_mode}\`
**Write target:** Hotel Property Census (\`${productionHotelPropertyCensus.tableId}\`)

## Discovery resume (V4)

- Units completed: **${d.discovery_units_completed ?? "n/a"}**
- Units timed out: **${d.discovery_units_timed_out ?? "n/a"}**
- Units failed: **${d.discovery_units_failed ?? "n/a"}**
- Discovered count (cache): **${d.discovered_count ?? "n/a"}**
- Discovery lane status: \`${d.discovery_lane_status || "n/a"}\`
- Last stall: ${
    d.last_stall
      ? `${d.last_stall.family}/${d.last_stall.country} (${d.last_stall.error || d.last_stall.skip_reason || "timeout"})`
      : "(none)"
  }

## Field completion

- Records scanned: **${report.records_scanned ?? 0}**
- Records updated: **${report.existing_records_updated ?? 0}**
- Records inserted (Census Only / Hold): **${report.inserts ?? 0}**
- Address writes: **${report.address_writes ?? 0}**
- Website writes: **${report.website_writes ?? 0}**
- Phone Medium writes: **${report.phone_writes ?? 0}**
- Mapbox coordinate writes: **${report.mapbox_coordinate_writes ?? 0}**
- Rooms writes: **${report.rooms_writes ?? 0}**
- Market writes: **${report.market_writes ?? 0}**
- Submarket writes: **${report.submarket_writes ?? 0}**

### By confidence tier
- Medium (internal-only): **${report.fields_written_by_confidence_tier?.Medium ?? 0}**
- High: **${report.fields_written_by_confidence_tier?.High ?? 0}**

## Gaps after

- missing_address: ${g.missing_address ?? "n/a"}
- missing_official_url: ${g.missing_official_url ?? "n/a"}
- missing_coordinates: ${g.missing_coordinates ?? "n/a"}
- missing_rooms: ${g.missing_rooms ?? "n/a"}
- missing_phone: ${g.missing_phone ?? "n/a"}
- missing_market: ${g.missing_market ?? "n/a"}
- missing_submarket: ${g.missing_submarket ?? "n/a"}

## Policy

- Brand Explorer / Brand Setup writes: **0**
- Owner / operator / dates / Recent Momentum / Company Validated / Brand Verified: **0** (held; candidate reports only)
- Direct DataForSEO coordinates: **held**
- Medium fields: **internal-only**

## Next source investment

${report.next_source_investment || report.next_backlog || "(see JSON)"}

## Another founder approval needed?

${report.founder_approval_needed || "No — continue under encoded confidence-tiered internal policy."}
`;
}

/**
 * @param {object} opts
 * @param {boolean} [opts.enableProductionWrites]
 * @param {object} [opts.args]
 * @param {string[]} [opts.argv]
 * @param {object} [opts.env]
 * @param {string} [opts.discoveryOutDir]
 * @param {(msg: string) => void} [opts.log]
 */
export async function runCensusMissingFieldSourceStrategyControllerV1Mission(opts = {}) {
  const log = opts.log || console.log;
  const env = {
    ...(opts.env || process.env),
    ENABLE_CENSUS_POLICY_CONTROLLER: "1",
    ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION:
      opts.env?.ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION ||
      process.env.ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION ||
      "1",
    // Field-completion must not be stuck in discovery-candidates-only.
    DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
  };
  const discoveryOutDir =
    opts.discoveryOutDir ||
    path.join(ROOT, "data/research-engine-v2/census-autopilot-v4-full-universe");

  const discoveryPartial = loadDiscoveryPartialSnapshot(discoveryOutDir);
  log(
    `[missing-field-strategy] discovery_partial completed=${discoveryPartial.discovery_units_completed} timed_out=${discoveryPartial.discovery_units_timed_out} discovered=${discoveryPartial.discovered_count}`
  );

  const policyReport = await runCensusAutopilotPolicyControllerV1Mission({
    ...opts,
    env,
    censusMode: CENSUS_MODE.FIELD_COMPLETION_ONLY || "field-completion-only",
    args: {
      ...(opts.args || {}),
      censusMode: "field-completion-only",
      batchSize: Number(opts.args?.batchSize || process.env.CENSUS_FIELD_STRATEGY_BATCH_SIZE || 100),
      maxPasses: Number(opts.args?.maxPasses || process.env.CENSUS_FIELD_STRATEGY_MAX_PASSES || 1),
      confirms: {
        writeToProductionCensus: true,
        safeWrites: true,
        ...(opts.args?.confirms || {}),
      },
    },
    log,
  });

  const status = mapPolicyStatus(policyReport.status, discoveryPartial);
  const nextSourceInvestment = [
    discoveryPartial.discovery_units_timed_out > 0
      ? "Invest in Railway-reachable Hilton Mexico brand-page crawl (or offline Mexico cache seed); currently skip-on-timeout."
      : null,
    (discoveryPartial.stalls || []).some((s) => /choice/i.test(String(s.family || "")))
      ? "Choice regional pages often timeout on Railway egress — seed regional extracts or raise Choice-only timeout carefully."
      : null,
    Number(policyReport.gaps_after?.missing_rooms || 0) > 500
      ? "Rooms/Keys: expand official factsheet/PDF + tourism registry adapters (never SERP-only)."
      : null,
    Number(policyReport.gaps_after?.missing_address || 0) > 400
      ? "Address: continue DataForSEO local match_high + official page extraction under Medium internal policy."
      : null,
    Number(policyReport.gaps_after?.missing_coordinates || 0) > 400
      ? "Coordinates: Mapbox Permanent after validated address only (no direct DFS coords)."
      : null,
  ]
    .filter(Boolean)
    .join(" ");

  const report = {
    ...policyReport,
    ok: status !== MISSING_FIELD_SOURCE_STRATEGY_STATUS.BLOCKED,
    status,
    objective: MISSING_FIELD_SOURCE_STRATEGY_OBJECTIVE,
    version: MISSING_FIELD_SOURCE_STRATEGY_VERSION,
    parent_controller: policyReport.objective,
    parent_controller_status: policyReport.status,
    field_priority: MISSING_FIELD_PRIORITY,
    held_fields_candidate_only: HELD_FIELDS_CANDIDATE_ONLY,
    discovery_partial: discoveryPartial,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    brand_explorer_writes: 0,
    brand_setup_writes: 0,
    owner_operator_date_writes: 0,
    recent_momentum_writes: 0,
    company_validated_writes: 0,
    brand_verified_writes: 0,
    next_source_investment:
      nextSourceInvestment ||
      policyReport.next_backlog ||
      "Continue field-completion passes under encoded policy.",
    generated_at: new Date().toISOString(),
  };

  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  writeJson(path.join(reportsDir, "census-missing-field-source-strategy-controller-v1.json"), report);
  writeMd(path.join(reportsDir, "census-missing-field-source-strategy-controller-v1.md"), buildMarkdown(report));
  writeMd(path.join(docsDir, "census-missing-field-source-strategy-controller-v1.md"), buildMarkdown(report));

  // Refresh gap ledger pointers at canonical report paths (policy controller also writes runDir).
  if (policyReport.run_dir) {
    const ledgerJson = path.join(policyReport.run_dir, "census-gap-ledger-full.json");
    const scorecard = path.join(policyReport.run_dir, "census-completion-scorecard.json");
    if (fs.existsSync(ledgerJson)) {
      fs.copyFileSync(ledgerJson, path.join(reportsDir, "census-gap-ledger.json"));
    }
    if (fs.existsSync(scorecard)) {
      const sc = readJsonSafe(scorecard);
      writeMd(
        path.join(reportsDir, "census-gap-ledger.md"),
        `# Census Gap Ledger\n\n**Updated:** ${report.generated_at}\n\nVia missing-field strategy · scanned=${report.records_scanned}\n\n\`\`\`json\n${JSON.stringify(report.gaps_after || {}, null, 2)}\n\`\`\`\n`
      );
      writeMd(
        path.join(docsDir, "census-gap-ledger.md"),
        `# Census Gap Ledger\n\n**Updated:** ${report.generated_at}\n\nSource: missing-field-source-strategy-controller-v1\n\n\`\`\`json\n${JSON.stringify({ gaps_after: report.gaps_after, scorecard: sc }, null, 2)}\n\`\`\`\n`
      );
    }
  }

  return report;
}
