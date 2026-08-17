/**
 * Census Autopilot Production Cycle — apply safe High-confidence writes until exhausted.
 *
 * Founder approval = the production-cycle CLI invocation (confirms + env + --enable-production-writes).
 * No per-bundle ChatGPT approval. Stop only on true safety exceptions.
 *
 * Writes only Hotel Property Census (tbl9aY5ijiuIzzWam). Brand Setup / Brand Explorer read-only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  parseAutopilotArgs,
  applyPreflight,
  checkAutopilotApplyEnv,
  isProductionWriteMode,
} from "./census-autopilot-apply-guard.js";
import {
  runUntilComplete,
  createLiveHotelPropertyCensusAdapter,
  createMemoryAirtableAdapter,
  COMPLETION_STATUS,
} from "./census-autopilot-batch-engine.js";
import { saveCheckpoint } from "./census-autopilot-checkpoint.js";
import { orchestrateAutopilotQueues } from "./census-autopilot-queue-orchestrator.js";
import {
  AUTOPILOT_TARGET_BASE_LABEL,
  AUTOPILOT_TARGET_TABLE,
  AUTOPILOT_TARGET_TABLE_ID,
  AUTOPILOT_FORBIDDEN_FIELDS,
} from "./census-autopilot-field-allowlist.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";
import { evaluateProviderReadiness } from "./production-census-description-extraction.js";
import {
  buildDiscoveryInsertApprovalBundle,
  SOURCE_DISCOVERY_QUEUE_ID,
} from "./census-autopilot-source-discovery.js";
import {
  KEY_FIELD_COMPLETION_QUEUE_ID,
  runKeyFieldCompletionQueue,
} from "./census-autopilot-key-field-completion.js";
import {
  rededupeInsertsAgainstCensus,
  runDiscoveryInsertApply,
} from "./census-autopilot-discovery-insert-apply.js";
import {
  applyChoiceRadissonStewardResolutionToInserts,
  writeChoiceRadissonStewardResolutionReports,
  RESOLUTION_CLASS,
  FINAL_STATUS as RADISSON_FINAL_STATUS,
} from "./census-autopilot-choice-radisson-steward-resolution.js";
import {
  evaluateInsertIdentityGate,
  QUALITY_GATE_STATUS,
} from "./census-core-identity-quality.js";
import {
  CLEAN_CORE_IDENTITY_REPAIR_QUEUE_ORDER,
  filterCleanCoreIdentityProposals,
  resolveCleanCoreIdentityQueues,
  auditAllCoreIdentityIssues,
  buildCleanCoreIdentityRepairReport,
} from "./census-clean-core-identity-repair.js";
import {
  buildMarriottUrlCityBackfillReport,
  writeMarriottUrlCityBackfillReports,
  filterProposalsToMarriottParent,
  isMarriottCensusRecord,
} from "./census-marriott-property-url-city-backfill.js";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import { routeWebhoundCandidates } from "./census-autopilot-queue-router.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const PRODUCTION_CYCLE_VERSION = "census-autopilot-production-cycle-v1";

export const PRODUCTION_CYCLE_STATUS = Object.freeze({
  COMPLETE: "production_census_autopilot_production_cycle_complete",
  PARTIAL: "production_census_autopilot_production_cycle_partial_steward_remaining",
  BLOCKED: "production_census_autopilot_production_cycle_blocked_safety_stop",
});

/** Founder-specified production-cycle queue order. */
export const PRODUCTION_CYCLE_QUEUE_ORDER = Object.freeze([
  "source_discovery",
  "brand_normalization",
  "parent_company_normalization",
  "core_identity_quality",
  "core_identity_source_lookup",
  "clean_core_classification",
  "key_field_completion",
  "address_confirmation",
  "coordinate_completion",
  "phone_number_enrichment",
  "rooms_keys",
  "property_type_asset_context",
  "description_extraction",
  "amenities_extraction",
  "radar_public_readiness",
]);

const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"] || AUTOPILOT_TARGET_TABLE_ID;
const MAX_PASSES_DEFAULT = 3;

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}
function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function buildProductionCycleRunFolderName(region = "CALA", now = new Date()) {
  const iso = now.toISOString().replace(/[:.]/g, "-").slice(0, 19);
  return `${iso}_${region}-production-cycle`;
}

/**
 * Steward inserts that fail core identity quality gate (Unknown/descriptor city, etc.).
 * Resolved High candidates (steward_resolution.classification) are eligible for auto-insert.
 */
export function classifyProductionCycleInsertCandidate(row = {}) {
  if (
    row.steward_resolution?.classification ===
    "resolved_high_confidence_insert_candidate"
  ) {
    const city = String(row.fields?.City || row.discovery?.city || "").trim();
    const name = String(row.property_name || row.fields?.["Property Name"] || "");
    if (city && !/^unknown$/i.test(city) && name && !/a\s+member\s+of\s+radisson\s+individuals/i.test(name)) {
      const gate = evaluateInsertIdentityGate({
        property_name: name,
        brand: row.brand || row.fields?.["Current Brand"],
        city,
        country: row.fields?.Country || row.discovery?.country,
        state_region: row.fields?.["State / Region"] || row.discovery?.state_region,
        official_property_url:
          row.fields?.["Official Property URL"] || row.discovery?.official_property_url,
        official_directory_url: row.fields?.["Source URL"] || row.discovery?.official_directory_url,
        source_family: row.fields?.["Family / Source Family"] || row.source_family,
        canonical_property_name: row.fields?.["Canonical Property Name"],
        fields: row.fields,
      });
      if (gate.allow_insert) return { steward: false, reason: null };
      return {
        steward: true,
        reason: gate.reason || "core_identity_quality_gate",
        gate_status: gate.gate_status,
      };
    }
  }

  const name = String(row.property_name || row.fields?.["Property Name"] || "");
  const city = String(row.fields?.City || row.discovery?.city || "").trim();
  const brand = String(row.brand || row.fields?.["Current Brand"] || "");

  if (/a\s+member\s+of\s+radisson\s+individuals/i.test(name)) {
    return {
      steward: true,
      reason: "choice_radisson_individuals_member_of_name",
    };
  }
  if (/radisson\s+individuals/i.test(brand) && /^unknown$/i.test(city)) {
    return {
      steward: true,
      reason: "choice_radisson_individuals_unknown_city",
    };
  }
  if (!name.trim()) {
    return { steward: true, reason: "missing_property_name" };
  }
  if (!city || /^unknown$/i.test(city)) {
    return { steward: true, reason: "missing_or_unknown_city" };
  }

  const gate = evaluateInsertIdentityGate({
    property_name: name,
    brand,
    city,
    country: row.fields?.Country || row.discovery?.country,
    state_region: row.fields?.["State / Region"] || row.discovery?.state_region,
    official_property_url:
      row.fields?.["Official Property URL"] || row.discovery?.official_property_url,
    official_directory_url: row.fields?.["Source URL"] || row.discovery?.official_directory_url,
    source_family: row.fields?.["Family / Source Family"] || row.source_family,
    canonical_property_name: row.fields?.["Canonical Property Name"],
    fields: row.fields,
  });
  if (!gate.allow_insert) {
    return {
      steward: true,
      reason: gate.reason || "core_identity_quality_gate",
      gate_status: gate.gate_status || QUALITY_GATE_STATUS.BLOCKED_DIRTY_CORE_IDENTITY,
    };
  }
  return { steward: false, reason: null };
}

export function splitInsertCandidates(inserts = []) {
  const auto = [];
  const steward = [];
  for (const row of inserts) {
    const c = classifyProductionCycleInsertCandidate(row);
    if (c.steward) steward.push({ ...row, steward_reason: c.reason });
    else auto.push(row);
  }
  return { auto_inserts: auto, steward_inserts: steward };
}

export function filterHighUpdateProposals(proposals = []) {
  return (proposals || []).filter((p) => {
    if (p.action === "insert" || p.type === "insert") return false;
    if (p.queue === SOURCE_DISCOVERY_QUEUE_ID) return false;
    if (String(p.confidence || "") !== "High") return false;
    if (!p.record_id && !p.id) return false;
    const patch = p.patch || p.fields || {};
    if (!Object.keys(patch).length) return false;
    for (const k of Object.keys(patch)) {
      if (AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) return false;
    }
    return true;
  });
}

async function listCensusCount(baseId, token, tableId) {
  let count = 0;
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    params.append("fields[]", "Property Identity Key");
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census count ${res.status}: ${JSON.stringify(json.error || json)}`);
    count += (json.records || []).length;
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return count;
}

async function listCensusRecordsForRededupe(baseId, token, tableId) {
  const out = [];
  let offset;
  const fields = [
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
    "Latitude",
    "Longitude",
    "Phone",
    "Source URL",
    "Official Property URL",
    "Human Review Required",
    "Production Use Status",
    "Family / Source Family",
    "Identity Confidence",
    "Data Confidence Tier",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
    // Market geography — required so proposals do not treat filled Continent/Market as blank
    "Continent",
    "Sub-Continent",
    "Market",
    "Submarket",
  ];
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

function extractDiscoveryInserts(orchestration) {
  const report = orchestration?.discoveryReport || null;
  if (!report) return [];
  if (Array.isArray(report.approval_bundle?.proposed_inserts)) {
    return report.approval_bundle.proposed_inserts;
  }
  if (Array.isArray(report.proposed_inserts)) {
    return report.proposed_inserts;
  }
  const bundle = buildDiscoveryInsertApprovalBundle({
    new_property_candidates: report.new_property_candidates || [],
    duplicate_risks: report.duplicate_risks || [],
    steward_review_cases: report.steward_review_cases || [],
    existing_exact_count: report.summary?.existing_hotel_property_census_matches,
    run_id: null,
    scope: "active-brand-setup",
    region: "CALA",
  });
  return bundle.proposed_inserts || [];
}

/**
 * Run production cycle (plan → preflight → apply → checkpoint → repeat).
 */
export async function runProductionCycle(opts = {}) {
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || parseAutopilotArgs(argv);
  const env = opts.env || process.env;
  const log = opts.log || ((msg) => console.log(msg));
  const started = Date.now();
  const maxPasses =
    opts.maxPasses ??
    args.maxPasses ??
    MAX_PASSES_DEFAULT;

  args.mode = args.mode === "mission" ? "production-cycle" : args.mode || "production-cycle";
  if (args.mode !== "production-cycle") args.mode = "production-cycle";

  const cleanupExistingOnly = Boolean(
    args.cleanupExistingOnly || argv.includes("--cleanup-existing-only")
  );
  const skipInserts = Boolean(opts.skipInserts || cleanupExistingOnly);
  const missionPhase = opts.missionPhase || null;
  const missionFilterProposals =
    typeof opts.missionFilterProposals === "function"
      ? opts.missionFilterProposals
      : null;
  const repairQueues = resolveCleanCoreIdentityQueues(
    args.queues?.length ? args.queues : args.queue,
    { cleanupExistingOnly }
  );
  const cycleQueueOrder = cleanupExistingOnly
    ? repairQueues.length
      ? repairQueues
      : [...CLEAN_CORE_IDENTITY_REPAIR_QUEUE_ORDER]
    : args.queues?.length
      ? resolveCleanCoreIdentityQueues(args.queues, { cleanupExistingOnly: false })
      : [...PRODUCTION_CYCLE_QUEUE_ORDER];

  const envCheck = checkAutopilotApplyEnv(env);
  const preflight = applyPreflight(args, envCheck);
  if (!preflight.ok) {
    return {
      version: PRODUCTION_CYCLE_VERSION,
      status: PRODUCTION_CYCLE_STATUS.BLOCKED,
      ok: false,
      blocked_reason: "confirmation_or_env_or_target",
      blockers: preflight.blockers,
      airtable_writes: false,
      brand_explorer_writes: false,
      brand_setup_writes: false,
    };
  }

  const enableWrites = Boolean(
    opts.enableProductionWrites &&
      argv.includes("--enable-production-writes") &&
      args.allApplyConfirms &&
      envCheck.allOk
  );

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok || CENSUS_TABLE_ID !== productionHotelPropertyCensus.tableId) {
    return {
      version: PRODUCTION_CYCLE_VERSION,
      status: PRODUCTION_CYCLE_STATUS.BLOCKED,
      ok: false,
      blocked_reason: BLOCKED_WRONG_CENSUS_TARGET,
      write_target: writeTarget,
      airtable_writes: false,
    };
  }

  const token = opts.token ?? resolvePat();
  const bases = opts.bases ?? resolveTargetBase();
  if (enableWrites && (!token || !bases?.target_base_id) && !opts.airtable) {
    return {
      version: PRODUCTION_CYCLE_VERSION,
      status: PRODUCTION_CYCLE_STATUS.BLOCKED,
      ok: false,
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
    };
  }

  const autopilotRoot =
    opts.root || path.join(ROOT, "reports/research-engine-v2/autopilot");
  const runName =
    opts.runId || buildProductionCycleRunFolderName(args.region || "CALA");
  const runDir = path.isAbsolute(runName)
    ? runName
    : path.join(autopilotRoot, runName);
  fs.mkdirSync(path.join(runDir, "batches"), { recursive: true });

  const providerReady = evaluateProviderReadiness(env);
  const censusBefore = opts.censusCountBefore ?? (await listCensusCount(bases.target_base_id, token, CENSUS_TABLE_ID));

  const plan = {
    version: PRODUCTION_CYCLE_VERSION,
    mode: "production-cycle",
    region: args.region,
    scope: args.scope,
    strategy: args.strategy || "fastest-safe",
    batch_size: args.batchSize,
    run_until_complete: Boolean(args.runUntilComplete),
    max_passes: maxPasses,
    queue_order: [...cycleQueueOrder],
    cleanup_existing_only: cleanupExistingOnly,
    paused_queues: cleanupExistingOnly
      ? [
          "source_discovery_inserts",
          "address_confirmation",
          "coordinate_completion",
          "phone_number_enrichment",
          "rooms_keys",
          "description_extraction",
          "amenities_extraction",
        ]
      : [],
    founder_approval: "production-cycle_cli_command",
    per_bundle_chatgpt_approval: false,
    enable_production_writes: enableWrites,
    geocode_provider_ready: Boolean(providerReady.approved_for_geocode_apply),
    target: {
      base: AUTOPILOT_TARGET_BASE_LABEL,
      table: AUTOPILOT_TARGET_TABLE,
      tableId: CENSUS_TABLE_ID,
    },
    census_records_before: censusBefore,
    notes: [
      "Applies High-confidence allowlisted updates automatically.",
      "Inserts clean High discovery candidates; stewards Choice Radisson Individuals member-of / Unknown city.",
      "Geocode soft-deferred without approved provider — does not stop cycle.",
      "Stops only on true safety exceptions.",
    ],
  };
  writeJson(path.join(runDir, "production-cycle-plan.json"), plan);
  writeText(
    path.join(runDir, "production-cycle-plan.md"),
    [
      `# Production Cycle Plan`,
      ``,
      `- Mode: production-cycle`,
      `- Region: ${plan.region}`,
      `- Scope: ${plan.scope}`,
      `- Batch size: ${plan.batch_size}`,
      `- Max passes: ${plan.max_passes}`,
      `- Writes enabled: ${plan.enable_production_writes}`,
      `- Census before: ${plan.census_records_before}`,
      `- Queue order: ${plan.queue_order.join(" → ")}`,
      `- Per-bundle ChatGPT approval: **false** (founder CLI is approval)`,
      ``,
    ].join("\n")
  );

  const airtable =
    opts.airtable ||
    (enableWrites
      ? createLiveHotelPropertyCensusAdapter({
          baseId: bases.target_base_id,
          token,
          tableId: CENSUS_TABLE_ID,
        })
      : createMemoryAirtableAdapter({}));

  const passReports = [];
  const allUpdatesApplied = [];
  const allInsertsApplied = [];
  const allSkipped = [];
  const allSteward = [];
  const allBlocked = [];
  const allProvider = [];
  const queuesExecuted = new Set();
  const queuesExhausted = new Set();
  const queuesBlocked = new Set();
  let safetyStop = null;
  let fieldsWritten = new Set();
  let censusRecords = opts.censusRecords || null;
  let lastRadissonResolution = null;
  let censusRecordsBeforeAudit = null;
  let censusRecordsBeforeSnapshot = null;
  const identityRepairExamples = [];

  for (let pass = 1; pass <= maxPasses; pass += 1) {
    log(`[production-cycle] pass ${pass}/${maxPasses} — orchestrating…`);

    if ((!censusRecords || pass > 1) && !opts.skipLiveCensusReload) {
      censusRecords = await listCensusRecordsForRededupe(
        bases.target_base_id,
        token,
        CENSUS_TABLE_ID
      );
    } else if (opts.skipLiveCensusReload && pass > 1) {
      censusRecords = opts.censusRecords || [];
    }

    const limit = args.maxRecords
      ? args.maxRecords
      : args.runUntilComplete
        ? 10000
        : args.batchSize || 100;

    if (cleanupExistingOnly && pass === 1 && censusRecords && !censusRecordsBeforeAudit) {
      censusRecordsBeforeSnapshot = censusRecords;
      censusRecordsBeforeAudit = auditAllCoreIdentityIssues(censusRecords, {
        canonicalFieldExists: opts.canonicalFieldExists !== false,
      });
      writeJson(path.join(runDir, "clean-core-identity-before.json"), {
        counters: censusRecordsBeforeAudit.counters,
      });
    }

    let orchestration;
    try {
      orchestration =
        opts.orchestrationFactory
          ? await opts.orchestrationFactory({ pass, censusRecords, limit })
          : await orchestrateAutopilotQueues({
              orderedQueueIds: [...cycleQueueOrder],
              targetedQueue: null,
              targetedQueues: cleanupExistingOnly || args.queues?.length ? [...cycleQueueOrder] : null,
              cleanupExistingOnly,
              limit,
              parentCompany: args.parentCompany,
              region: args.region || "CALA",
              censusRecords,
              geocodeProviderReady: cleanupExistingOnly
                ? false
                : Boolean(providerReady.approved_for_geocode_apply),
              schemaV114Ready: opts.schemaV114Ready !== false,
              canonicalFieldExists: opts.canonicalFieldExists !== false,
              continentFieldExists: opts.continentFieldExists === true,
              subContinentFieldExists: opts.subContinentFieldExists === true,
              marketFieldExists: opts.marketFieldExists === true,
              submarketFieldExists: opts.submarketFieldExists === true,
              forAutopilot: true,
              log,
            });
    } catch (err) {
      safetyStop = {
        reason: "orchestrator_failure",
        message: err?.message || String(err),
        pass,
      };
      break;
    }

    for (const q of orchestration.queues_executed || []) queuesExecuted.add(q);
    for (const q of orchestration.queues_soft_deferred || []) {
      if (q === "coordinate_resolution" || q === "coordinate_completion") {
        queuesExhausted.add(q);
      }
    }
    for (const r of orchestration.queue_results || []) {
      if (r.status === "executed_exhausted") queuesExhausted.add(r.queue_id);
      if (r.status === "blocked") queuesBlocked.add(r.queue_id);
    }

    let updateProposals = filterHighUpdateProposals(orchestration.proposals || []);
    if (missionFilterProposals) {
      updateProposals = missionFilterProposals(updateProposals, censusRecords || []);
      log(
        `[production-cycle] mission phase filter (${missionPhase?.id || "custom"}): ${updateProposals.length} proposals`
      );
    } else if (cleanupExistingOnly) {
      updateProposals = filterCleanCoreIdentityProposals(updateProposals);
      log(
        `[production-cycle] cleanup-existing-only: ${updateProposals.length} core-identity High proposals (address/coords/phone/rooms filtered out)`
      );
    }
    if (args.parentCompany && /marriott/i.test(String(args.parentCompany))) {
      const beforeFilter = updateProposals.length;
      updateProposals = filterProposalsToMarriottParent(
        updateProposals,
        censusRecords,
        args.parentCompany
      );
      log(
        `[production-cycle] Marriott parent filter: ${beforeFilter} → ${updateProposals.length} proposals`
      );
    }
    let rawInserts = skipInserts ? [] : extractDiscoveryInserts(orchestration);

    // Resolve stewarded Choice Radisson Individuals before insert split
    const resolutionApplied = applyChoiceRadissonStewardResolutionToInserts(rawInserts, {
      censusRecords,
    });
    rawInserts = resolutionApplied.inserts;
    let radissonResolutionReport = null;
    if (resolutionApplied.resolution_rows.length) {
      radissonResolutionReport = {
        version: "census-autopilot-choice-radisson-steward-resolution-v1",
        generated_at: new Date().toISOString(),
        input_count: resolutionApplied.resolution_rows.length,
        results: resolutionApplied.resolution_rows,
        resolved_inserts: resolutionApplied.inserts.filter(
          (i) => i.steward_resolution?.classification === RESOLUTION_CLASS.RESOLVED
        ),
        counts: {
          [RESOLUTION_CLASS.RESOLVED]: resolutionApplied.resolution_rows.filter(
            (r) => r.classification === RESOLUTION_CLASS.RESOLVED
          ).length,
          [RESOLUTION_CLASS.STILL_STEWARD]: resolutionApplied.resolution_rows.filter(
            (r) => r.classification === RESOLUTION_CLASS.STILL_STEWARD
          ).length,
          [RESOLUTION_CLASS.DUPLICATE]: resolutionApplied.resolution_rows.filter(
            (r) => r.classification === RESOLUTION_CLASS.DUPLICATE
          ).length,
          [RESOLUTION_CLASS.SOURCE_INSUFFICIENT]: resolutionApplied.resolution_rows.filter(
            (r) => r.classification === RESOLUTION_CLASS.SOURCE_INSUFFICIENT
          ).length,
          [RESOLUTION_CLASS.IDENTITY_CONFLICT]: resolutionApplied.resolution_rows.filter(
            (r) => r.classification === RESOLUTION_CLASS.IDENTITY_CONFLICT
          ).length,
        },
      };
      writeChoiceRadissonStewardResolutionReports(radissonResolutionReport, { runDir });
      lastRadissonResolution = radissonResolutionReport;
      log(
        `[production-cycle] Choice Radisson steward resolution: resolved=${radissonResolutionReport.counts[RESOLUTION_CLASS.RESOLVED]} / ${radissonResolutionReport.input_count}`
      );
    }

    const { auto_inserts, steward_inserts } = splitInsertCandidates(rawInserts);
    allSteward.push(
      ...steward_inserts.map((s) => ({
        ...s,
        lane: "source_discovery_insert",
        pass,
      }))
    );

    writeJson(path.join(runDir, `pass-${pass}-orchestration.json`), {
      pass,
      queues_executed: orchestration.queues_executed,
      queues_soft_deferred: orchestration.queues_soft_deferred,
      high_update_proposals: updateProposals.length,
      insert_candidates_raw: rawInserts.length,
      insert_auto: auto_inserts.length,
      insert_steward: steward_inserts.length,
    });

    let insertsCreated = 0;
    if (auto_inserts.length && enableWrites) {
      const insertBundlePath = path.join(runDir, `pass-${pass}-insert-bundle.json`);
      const insertBundle = {
        version: PRODUCTION_CYCLE_VERSION,
        type: "hotel_property_census_insert_approval_bundle",
        queue: SOURCE_DISCOVERY_QUEUE_ID,
        mode: "production-cycle",
        status: "production_cycle_auto_approved",
        stop_before_writes: false,
        proposed_inserts: auto_inserts,
        records_proposed_for_insert: auto_inserts.length,
      };
      writeJson(insertBundlePath, insertBundle);

      const insertReport = await runDiscoveryInsertApply({
        doWrite: true,
        useLiveAirtable: !opts.createRecords,
        createRecords: opts.createRecords || null,
        censusRecords,
        bundlePath: insertBundlePath,
        checkpointDir: runDir,
        env,
        args: {
          mode: "apply",
          apply: true,
          approvalBundlePath: insertBundlePath,
          batchSize: args.batchSize || 100,
          confirms: args.confirms,
          allConfirmsOk: true,
        },
      });

      insertsCreated = insertReport.created_count || insertReport.created_record_ids?.length || 0;
      if (insertReport.created?.length) {
        allInsertsApplied.push(
          ...insertReport.created.map((c) => ({ ...c, pass }))
        );
      } else if (insertReport.created_record_ids?.length) {
        allInsertsApplied.push(
          ...insertReport.created_record_ids.map((id) => ({
            id,
            pass,
            identity_key: null,
          }))
        );
      }
      if (insertReport.checkpoint?.identity_keys_created?.length) {
        for (let i = 0; i < insertReport.checkpoint.identity_keys_created.length; i += 1) {
          const existing = allInsertsApplied[allInsertsApplied.length - insertsCreated + i];
          if (existing && !existing.identity_key) {
            existing.identity_key = insertReport.checkpoint.identity_keys_created[i];
          }
        }
      }
      if (insertReport.steward?.length) {
        allSteward.push(...insertReport.steward.map((s) => ({ ...s, pass })));
      }
      if (insertReport.blocked?.length) {
        allBlocked.push(...insertReport.blocked.map((b) => ({ ...b, pass })));
      }
      if (insertReport.status?.includes("blocked") && insertsCreated === 0 && auto_inserts.length) {
        // duplicate/empty after rededupe is not a full cycle stop
        log(
          `[production-cycle] insert pass ${pass}: ${insertReport.blocked_reason || insertReport.status}`
        );
      }
    } else if (auto_inserts.length && !enableWrites) {
      log(`[production-cycle] pass ${pass}: ${auto_inserts.length} inserts proposed (writes disabled)`);
    }

    // After inserts: immediately re-run key_field_completion on refreshed Census
    let postInsertKeyField = null;
    if (insertsCreated > 0 && enableWrites && !opts.skipPostInsertKeyFieldCompletion) {
      log(
        `[production-cycle] pass ${pass}: post-insert key_field_completion on refreshed Census…`
      );
      try {
        if (!opts.skipLiveCensusReload) {
          censusRecords = await listCensusRecordsForRededupe(
            bases.target_base_id,
            token,
            CENSUS_TABLE_ID
          );
        }
        postInsertKeyField = runKeyFieldCompletionQueue({
          censusRecords: censusRecords || [],
          runDir,
          writeReports: true,
          env,
          providerReady: Boolean(providerReady.approved_for_geocode_apply),
        });
        const kfcProps = (postInsertKeyField.proposals || []).map((p) => ({
          ...p,
          queue: KEY_FIELD_COMPLETION_QUEUE_ID,
          pass,
          post_insert: true,
        }));
        if (kfcProps.length) {
          updateProposals.push(...kfcProps);
          log(
            `[production-cycle] post-insert key_field_completion: +${kfcProps.length} High proposals`
          );
        }
        writeJson(path.join(runDir, `pass-${pass}-post-insert-key-field-completion.json`), {
          inserts_created: insertsCreated,
          high_proposals: kfcProps.length,
          matrix_status: postInsertKeyField.status,
          provider_decision_needed: (postInsertKeyField.provider_decision_needed || []).length,
        });
      } catch (err) {
        log(
          `[production-cycle] post-insert key_field_completion failed (non-fatal): ${err?.message || err}`
        );
      }
    }

    let updatesApplied = 0;
    let updateBatch = null;
    let updateQueuesOrdered = [];
    if (updateProposals.length) {
      // Exclude geocode-only if provider not ready — runUntilComplete soft-skips coordinate_resolution
      const queuesForUpdates = [
        ...new Set(
          updateProposals
            .map((p) => p.queue)
            .filter((q) => q && q !== SOURCE_DISCOVERY_QUEUE_ID)
        ),
      ];
      updateQueuesOrdered =
        cycleQueueOrder.filter((q) => queuesForUpdates.includes(q)).length
          ? cycleQueueOrder.filter((q) => queuesForUpdates.includes(q))
          : queuesForUpdates;

      updateBatch = await runUntilComplete({
        runDir,
        runId: `${runName}-pass${pass}`,
        args: { ...args, mode: "production-cycle" },
        proposals: updateProposals,
        queues: updateQueuesOrdered,
        airtable,
        schemaV114Ready: opts.schemaV114Ready !== false,
        completedRecordIds: [],
        env,
        enableProductionWrites: enableWrites,
      });

      if (updateBatch.completion_status === COMPLETION_STATUS.BLOCKED_SAFETY) {
        // Credit any successful writes before the stop (e.g. identity gate applied, later queue tripped).
        updatesApplied = updateBatch.total_updated || 0;
        for (const f of updateBatch.fields_populated || []) fieldsWritten.add(f);
        allUpdatesApplied.push({
          pass,
          count: updatesApplied,
          fields: updateBatch.fields_populated || [],
          written_ids: updateBatch.checkpoint?.airtable_record_ids_written || [],
          partial_before_safety_stop: true,
        });
        allSkipped.push(...(updateBatch.skipped_records || []));
        allSteward.push(...(updateBatch.steward_review_queue || []).map((s) => ({ ...s, pass })));
        allBlocked.push(...(updateBatch.blocked_records || []).map((b) => ({ ...b, pass })));
        safetyStop = {
          reason: updateBatch.stop_reason || "blocked_safety_failure",
          blockers: updateBatch.blockers,
          pass,
          updates_applied_before_stop: updatesApplied,
        };
        passReports.push({
          pass,
          updates_applied: updatesApplied,
          inserts_created: insertsCreated,
          update_batch: updateBatch,
          safety_stop: safetyStop,
        });
        break;
      }

      updatesApplied = updateBatch.total_updated || 0;
      for (const f of updateBatch.fields_populated || []) fieldsWritten.add(f);
      allUpdatesApplied.push({
        pass,
        count: updatesApplied,
        fields: updateBatch.fields_populated || [],
        written_ids: updateBatch.checkpoint?.airtable_record_ids_written || [],
      });
      allSkipped.push(...(updateBatch.skipped_records || []));
      allSteward.push(...(updateBatch.steward_review_queue || []).map((s) => ({ ...s, pass })));
      allBlocked.push(...(updateBatch.blocked_records || []).map((b) => ({ ...b, pass })));
      allProvider.push(
        ...(updateBatch.provider_decision_needed || []).map((p) => ({ ...p, pass }))
      );
    }

    const webhound = routeWebhoundCandidates(
      [
        ...(orchestration.blocked || []),
        ...allSteward.filter((s) => s.pass === pass),
      ],
      { max: 25 }
    );
    writeJson(path.join(runDir, `pass-${pass}-webhound-candidates.json`), webhound);

    saveCheckpoint(runDir, {
      run_id: runName,
      mode: "production-cycle",
      scope: args.scope,
      region: args.region,
      strategy: args.strategy,
      batch_size: args.batchSize,
      current_queue: updateQueuesOrdered[0] || null,
      current_batch_number: pass,
      records_updated: allUpdatesApplied.reduce((n, u) => n + (u.count || 0), 0),
      airtable_record_ids_written: allUpdatesApplied.flatMap((u) => u.written_ids || []),
      fields_written: [...fieldsWritten],
      completion_status:
        safetyStop ? "blocked_safety" : pass < maxPasses ? "in_progress" : "complete",
      queues_remaining: [],
      provider_decision_needed: allProvider,
    });

    passReports.push({
      pass,
      high_update_proposals: updateProposals.length,
      updates_applied: updatesApplied,
      inserts_raw: rawInserts.length,
      inserts_auto: auto_inserts.length,
      inserts_steward: steward_inserts.length,
      inserts_created: insertsCreated,
      queues_executed: orchestration.queues_executed,
      queues_soft_deferred: orchestration.queues_soft_deferred,
      marriott_counters: orchestration.marriottUrlCityBackfill?.counters || null,
      marriott_examples: orchestration.marriottUrlCityBackfill?.examples || null,
    });

    log(
      `[production-cycle] pass ${pass} done updates=${updatesApplied} inserts=${insertsCreated} steward_inserts=${steward_inserts.length}`
    );

    if (updatesApplied === 0 && insertsCreated === 0) {
      log(`[production-cycle] no safe High writes remaining after pass ${pass}`);
      break;
    }

    // Reload census after writes for next pass
    censusRecords = null;
  }

  const censusAfter =
    opts.censusCountAfter ??
    (enableWrites
      ? await listCensusCount(bases.target_base_id, token, CENSUS_TABLE_ID)
      : censusBefore + allInsertsApplied.length);

  const updatesTotal = allUpdatesApplied.reduce((n, u) => n + (u.count || 0), 0);
  const insertsTotal =
    allInsertsApplied.length ||
    passReports.reduce((n, p) => n + (p.inserts_created || 0), 0);
  const stewardCount = allSteward.length;

  let status = PRODUCTION_CYCLE_STATUS.COMPLETE;
  if (safetyStop) status = PRODUCTION_CYCLE_STATUS.BLOCKED;
  else if (stewardCount > 0) status = PRODUCTION_CYCLE_STATUS.PARTIAL;

  // When Choice Radisson steward resolution participated, prefer task-specific statuses
  if (lastRadissonResolution && !safetyStop) {
    const resolvedN = lastRadissonResolution.counts?.[RESOLUTION_CLASS.RESOLVED] || 0;
    const unresolvedN = Math.max(0, lastRadissonResolution.input_count - resolvedN);
    if (unresolvedN === 0 && insertsTotal >= resolvedN && stewardCount === 0) {
      status = RADISSON_FINAL_STATUS.COMPLETE;
    } else if (resolvedN > 0 || insertsTotal > 0 || unresolvedN > 0) {
      status = RADISSON_FINAL_STATUS.PARTIAL;
    }
  } else if (lastRadissonResolution && safetyStop) {
    status = RADISSON_FINAL_STATUS.BLOCKED;
  }

  writeJson(path.join(runDir, "inserts-applied.json"), {
    count: insertsTotal,
    records: allInsertsApplied,
  });
  writeJson(path.join(runDir, "updates-applied.json"), {
    count: updatesTotal,
    by_pass: allUpdatesApplied,
  });
  writeJson(path.join(runDir, "skipped-idempotent.json"), {
    count: allSkipped.length,
    records: allSkipped.slice(0, 500),
  });
  writeJson(path.join(runDir, "steward-review-queue.json"), {
    count: stewardCount,
    items: allSteward,
    note: "Includes Choice Radisson Individuals member-of / Unknown city held out of auto-insert.",
  });
  writeJson(path.join(runDir, "provider-decision-needed.json"), {
    count: allProvider.length,
    items: allProvider,
  });
  writeJson(path.join(runDir, "webhound-candidates.json"), routeWebhoundCandidates(allSteward, { max: 25 }));

  const blockedCsv = [
    "record_id,identity_key,property_name,queue,reason",
    ...allBlocked.map((b) => {
      const id = b.record_id || b.id || "";
      const key = b.identity_key || "";
      const name = String(b.property_name || "").replace(/"/g, '""');
      const q = b.queue || b.lane || "";
      const reason = String(b.block_reason || b.steward_reason || (b.errors || []).join("|") || "").replace(
        /"/g,
        '""'
      );
      return `${id},${key},"${name}",${q},"${reason}"`;
    }),
    ...allSteward.map((s) => {
      const id = s.record_id || s.id || "";
      const key = s.identity_key || "";
      const name = String(s.property_name || "").replace(/"/g, '""');
      const reason = String(s.steward_reason || "steward").replace(/"/g, '""');
      return `${id},${key},"${name}",source_discovery_insert,"${reason}"`;
    }),
  ].join("\n");
  writeText(path.join(runDir, "blocked-records.csv"), blockedCsv);

  const finalSummary = {
    version: PRODUCTION_CYCLE_VERSION,
    generated_at: new Date().toISOString(),
    status,
    ok: status !== PRODUCTION_CYCLE_STATUS.BLOCKED,
    run_id: runName,
    run_dir: runDir,
    mode: "production-cycle",
    region: args.region,
    scope: args.scope,
    strategy: args.strategy || "fastest-safe",
    cleanup_existing_only: cleanupExistingOnly,
    airtable_writes: enableWrites && (updatesTotal > 0 || insertsTotal > 0),
    brand_explorer_writes: false,
    brand_setup_writes: false,
    vic_writes: false,
    per_bundle_chatgpt_approval: false,
    records_before: censusBefore,
    records_after: censusAfter,
    inserts_applied: skipInserts || cleanupExistingOnly ? 0 : insertsTotal,
    updates_applied: updatesTotal,
    fields_written: [...fieldsWritten],
    queues_executed: [...queuesExecuted],
    queues_exhausted: [...queuesExhausted],
    queues_blocked: [...queuesBlocked],
    steward_cases: stewardCount,
    provider_decision_cases: allProvider.length,
    webhound_candidates: Math.min(25, stewardCount),
    runtime_ms: Date.now() - started,
    passes: passReports,
    safety_stops: safetyStop ? [safetyStop] : [],
    target: {
      base: AUTOPILOT_TARGET_BASE_LABEL,
      table: AUTOPILOT_TARGET_TABLE,
      tableId: CENSUS_TABLE_ID,
    },
    next_recommended_action: safetyStop
      ? "Resolve safety stop, then re-run production-cycle."
      : cleanupExistingOnly
        ? "Continue official source lookup for Unknown/descriptor cities and blank Canonical; keep address/Mapbox paused."
        : stewardCount
          ? "Clean Choice Radisson Individuals name/city steward queue, then re-run production-cycle."
          : "Continue Autopilot production-cycle on next Active Brand Setup expansion or Accor/Wyndham adapters.",
  };

  if (cleanupExistingOnly) {
    try {
      let afterRecords = null;
      let afterAudit = null;
      if (enableWrites && !opts.skipLiveCensusReload) {
        afterRecords = await listCensusRecordsForRededupe(
          bases.target_base_id,
          token,
          CENSUS_TABLE_ID
        );
        afterAudit = auditAllCoreIdentityIssues(afterRecords, {
          canonicalFieldExists: opts.canonicalFieldExists !== false,
        });
      }
      const examples = [];
      for (const u of allUpdatesApplied) {
        for (const id of (u.written_ids || []).slice(0, 5)) {
          if (examples.length >= 15) break;
          examples.push({
            record_id: id,
            before: "(see pass proposals)",
            after: `fields=${(u.fields || []).join(",")}`,
          });
        }
      }
      const repairReport = buildCleanCoreIdentityRepairReport({
        before: censusRecordsBeforeAudit || {
          counters: {},
          rows: [],
        },
        after: afterAudit,
        applied: {
          records_fixed: updatesTotal,
          fields_written: [...fieldsWritten],
          examples,
        },
        airtable_writes: enableWrites && updatesTotal > 0,
        queues_executed: [...queuesExecuted],
        blocked: Boolean(safetyStop),
        writeReports: true,
      });
      writeJson(path.join(runDir, "clean-core-identity-repair.json"), repairReport);
      finalSummary.clean_core_identity_repair_status = repairReport.status;
      finalSummary.clean_core_before = censusRecordsBeforeAudit?.counters || null;
      finalSummary.clean_core_after = afterAudit?.counters || null;
      log(`[production-cycle] clean-core identity repair status=${repairReport.status}`);

      if (args.parentCompany && /marriott/i.test(String(args.parentCompany))) {
        const bestPass = [...passReports]
          .reverse()
          .find((p) => (p.marriott_counters?.property_urls_found || 0) > 0);
        const firstPass = passReports.find((p) => (p.marriott_counters?.targets || 0) > 0);
        const marriottCounters = firstPass?.marriott_counters || bestPass?.marriott_counters || null;
        const marriottExamples =
          firstPass?.marriott_examples || bestPass?.marriott_examples || [];
        const marriottReport = buildMarriottUrlCityBackfillReport({
          censusRecordsBefore: censusRecordsBeforeSnapshot || [],
          censusRecordsAfter: afterRecords || censusRecordsBeforeSnapshot || [],
          applied: {
            records_fixed: updatesTotal,
            fields_written: [...fieldsWritten],
            examples: marriottExamples.length ? marriottExamples : examples,
          },
          counters: marriottCounters,
          examples: marriottExamples,
          airtable_writes: enableWrites && updatesTotal > 0,
          blocked: Boolean(safetyStop),
          steward_remaining: allSteward.slice(0, 40),
        });
        writeMarriottUrlCityBackfillReports(marriottReport, { runDir });
        finalSummary.marriott_property_url_city_backfill_status = marriottReport.status;
        finalSummary.status_detail = marriottReport.status;
        log(
          `[production-cycle] Marriott URL/city backfill status=${marriottReport.status}`
        );
      }
    } catch (err) {
      log(
        `[production-cycle] clean-core identity repair report failed (non-fatal): ${err?.message || err}`
      );
    }
  }

  writeJson(path.join(runDir, "final-summary.json"), finalSummary);
  writeText(
    path.join(runDir, "final-summary.md"),
    [
      `# Production Cycle — Final Summary`,
      ``,
      `- Status: **${status}**`,
      `- Records before → after: ${censusBefore} → ${censusAfter}`,
      `- Inserts applied: ${insertsTotal}`,
      `- Updates applied: ${updatesTotal}`,
      `- Fields written: ${[...fieldsWritten].join(", ") || "(none)"}`,
      `- Queues executed: ${[...queuesExecuted].join(", ") || "(none)"}`,
      `- Queues exhausted: ${[...queuesExhausted].join(", ") || "(none)"}`,
      `- Steward cases: ${stewardCount}`,
      `- Provider-decision cases: ${allProvider.length}`,
      `- Runtime: ${Math.round((Date.now() - started) / 1000)}s`,
      `- Safety stops: ${safetyStop ? safetyStop.reason : "none"}`,
      `- Brand Explorer / Brand Setup writes: false`,
      `- Next: ${finalSummary.next_recommended_action}`,
      ``,
      `## Passes`,
      ``,
      ...passReports.map(
        (p) =>
          `- Pass ${p.pass}: updates=${p.updates_applied}, inserts=${p.inserts_created}, steward_inserts=${p.inserts_steward}`
      ),
      ``,
    ].join("\n")
  );

  writeJson(path.join(runDir, "learning-update.json"), {
    census_autopilot_production_cycle: true,
    status,
    updates_applied: updatesTotal,
    inserts_applied: insertsTotal,
    steward_cases: stewardCount,
    per_bundle_chatgpt_approval: false,
    brand_explorer_untouched: true,
    brand_setup_untouched: true,
  });

  writeJson(path.join(runDir, "checkpoint.json"), {
    status,
    mode: "production-cycle",
    completion_status: safetyStop ? "blocked_safety" : "complete",
    records_updated: updatesTotal,
    inserts_applied: insertsTotal,
    airtable_writes: finalSummary.airtable_writes,
    updated_at: finalSummary.generated_at,
  });

  return finalSummary;
}

export { rededupeInsertsAgainstCensus };
