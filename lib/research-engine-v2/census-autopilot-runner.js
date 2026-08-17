/**
 * Census Autopilot Runner v2 — batch-by-batch Hotel Property Census engine.
 *
 * Modes: plan | dry-run | apply | controlled | schema-apply
 * Apply writes High-confidence allowlisted fields to production Hotel Property Census
 * when confirms + env are set and enableProductionWrites is true.
 *
 * Webhound = hard-case learning only (never production writes).
 * Brand Setup / Brand Explorer = read-only; never patched.
 * VIC source claims = evidence lineage only; never written.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  parseAutopilotArgs,
  applyPreflight,
  guardApplyBatch,
  checkAutopilotApplyEnv,
  AUTOPILOT_FORBIDDEN_FIELDS,
} from "./census-autopilot-apply-guard.js";
import { tallyConfidence, isWritableConfidence } from "./census-autopilot-confidence.js";
import { routeWebhoundCandidates } from "./census-autopilot-queue-router.js";
import {
  buildAutopilotPlan,
  renderAutopilotPlanMarkdown,
  recommendBatchSize,
} from "./census-autopilot-planner.js";
import { createCensusCacheManager } from "./census-cache-manager.js";
import { evaluateCensusProcessingGates } from "./census-processing-gates.js";
import { evaluateProviderReadiness } from "./production-census-description-extraction.js";
import { inspectRoomsKeysSchemaStatus } from "./production-census-rooms-keys-queue.js";
import {
  runUntilComplete,
  createMemoryAirtableAdapter,
  COMPLETION_STATUS,
  BATCH_ENGINE_VERSION,
} from "./census-autopilot-batch-engine.js";
import {
  loadCheckpoint,
  resolveRunDir,
  saveCheckpoint,
  buildCheckpoint,
} from "./census-autopilot-checkpoint.js";
import {
  AUTOPILOT_TARGET_TABLE,
  AUTOPILOT_TARGET_BASE_LABEL,
  AUTOPILOT_TARGET_TABLE_ID,
  AUTOPILOT_ALLOWED_WRITE_FIELDS,
} from "./census-autopilot-field-allowlist.js";
import {
  PRECISE_MATCH_SUMMARY_LINE,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";
import {
  buildActiveBrandSetupControlList,
  writeActiveBrandSetupControlList,
} from "./census-autopilot-active-brand-scope.js";
import {
  matchActiveBrandsToCensus,
  writeBrandToCensusMatchReport,
} from "./census-autopilot-brand-census-matcher.js";
import {
  buildFastestSafePriorityPlan,
  writeQueuePriorityPlan,
  STRATEGY_FASTEST_SAFE,
} from "./census-autopilot-fastest-safe.js";
import { routeAutopilotQueues } from "./census-autopilot-queue-router.js";
import {
  buildMultiQueueApprovalBundle,
  renderQueueExecutionMarkdown,
} from "./census-autopilot-queue-orchestrator.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const AUTOPILOT_RUNNER_VERSION = "census-autopilot-runner-v2.1-active-brand-setup";

export const STATUS = Object.freeze({
  READY: "production_census_autopilot_active_brand_setup_fastest_safe_ready",
  READY_NEEDS_V114:
    "production_census_autopilot_active_brand_setup_fastest_safe_ready_needs_v114_schema",
  NEEDS_REFACTOR: "production_census_autopilot_active_brand_setup_fastest_safe_needs_refactor",
  BLOCKED: "production_census_autopilot_active_brand_setup_fastest_safe_blocked",
  // prior aliases
  V2_READY: "production_census_autopilot_v2_batch_apply_ready",
  V2_READY_NEEDS_V114: "production_census_autopilot_v2_batch_apply_ready_needs_v114_schema",
});

export { COMPLETION_STATUS, BATCH_ENGINE_VERSION };

/**
 * @param {string} region
 * @param {string} parentCompany
 */
export function buildRunFolderName(region, parentCompany, now = new Date()) {
  const ts = now.toISOString().replace(/[:.]/g, "-").replace("T", "_").slice(0, 19);
  const r = String(region || "CALA").replace(/[^\w-]+/g, "_");
  const p = String(parentCompany || "unknown").replace(/[^\w-]+/g, "_");
  return `${ts}-${r}-${p}`;
}

export function ensureRunFolder(runDir) {
  fs.mkdirSync(runDir, { recursive: true });
  return runDir;
}

function writeJson(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
}

function writeText(filePath, text) {
  fs.writeFileSync(filePath, text, "utf8");
}

function csvEscape(v) {
  const s = String(v ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export function renderBlockedCsv(rows = []) {
  const header = ["record_id", "identity_key", "property_name", "queue", "block_reason", "confidence"];
  const lines = [header.join(",")];
  for (const r of rows) {
    lines.push(
      [
        csvEscape(r.record_id),
        csvEscape(r.identity_key),
        csvEscape(r.property_name),
        csvEscape(r.queue),
        csvEscape(r.block_reason),
        csvEscape(r.confidence),
      ].join(",")
    );
  }
  return lines.join("\n") + "\n";
}

export function buildRunSummary(input) {
  const conf = input.confidence_tally || { High: 0, Medium: 0, Low: 0, Hold: 0 };
  return {
    parent_company: input.parent_company,
    region: input.region,
    country: input.country || null,
    mode: input.mode,
    batch_size: input.batch_size ?? null,
    max_records: input.max_records ?? null,
    run_until_complete: Boolean(input.run_until_complete),
    total_records_in_scope: input.total_records_in_scope ?? input.records_scanned ?? 0,
    total_processed: input.total_processed ?? 0,
    total_updated: input.total_updated ?? input.records_updated_or_proposed ?? 0,
    total_skipped: input.total_skipped ?? 0,
    total_blocked: input.total_blocked ?? 0,
    fields_populated: input.fields_populated || input.fields_updated_or_proposed || [],
    confidence_counts: conf,
    blocked_reasons: input.blocked_reasons || {},
    webhound_candidates: input.webhound_candidates_count ?? 0,
    steward_review_cases: input.steward_review_count ?? 0,
    runtime_ms: input.runtime_ms ?? 0,
    remaining_queues: input.remaining_queues || [],
    completion_status: input.completion_status || null,
    resume_command: input.resume_command || null,
    recommended_next_run_command: input.recommended_next_run_command || null,
    status: input.status,
    airtable_writes: Boolean(input.airtable_writes),
    brand_explorer_writes: false,
    brand_setup_writes: false,
    vic_writes: false,
    match_summary_line: input.match_summary_line || PRECISE_MATCH_SUMMARY_LINE,
    target: input.target || {
      base: AUTOPILOT_TARGET_BASE_LABEL,
      table: AUTOPILOT_TARGET_TABLE,
      tableId: AUTOPILOT_TARGET_TABLE_ID,
      role: productionHotelPropertyCensus.role,
    },
  };
}

export function renderSummaryMarkdown(summary, extras = {}) {
  return [
    `# Hotel Property Census Autopilot Summary (v2)`,
    ``,
    summary.match_summary_line || PRECISE_MATCH_SUMMARY_LINE,
    ``,
    `1. **Parent company:** ${summary.parent_company}`,
    `2. **Region / country:** ${summary.region}${summary.country ? ` / ${summary.country}` : ""}`,
    `3. **Mode:** ${summary.mode}`,
    `4. **Total records in scope:** ${summary.total_records_in_scope}`,
    `5. **Total processed:** ${summary.total_processed}`,
    `6. **Total updated:** ${summary.total_updated}`,
    `7. **Total skipped:** ${summary.total_skipped}`,
    `8. **Total blocked:** ${summary.total_blocked}`,
    `9. **Fields populated:** ${(summary.fields_populated || []).join(", ") || "(none)"}`,
    `10. **Confidence High/Medium/Low/Hold:** ${summary.confidence_counts.High} / ${summary.confidence_counts.Medium} / ${summary.confidence_counts.Low} / ${summary.confidence_counts.Hold}`,
    `11. **Runtime:** ${summary.runtime_ms} ms`,
    `12. **Remaining queues:** ${(summary.remaining_queues || []).join(", ") || "(none)"}`,
    `13. **Completion status:** \`${summary.completion_status || "n/a"}\``,
    `14. **Resume command:** ${summary.resume_command || "(n/a)"}`,
    `15. **Recommended next:**`,
    ``,
    "```bash",
    summary.recommended_next_run_command || summary.resume_command || "(n/a)",
    "```",
    ``,
    `- **Batch size:** ${summary.batch_size} (chunk only)`,
    `- **Max records:** ${summary.max_records ?? "(none — full scope)"}`,
    `- **Run until complete:** ${summary.run_until_complete}`,
    `- **Status:** \`${summary.status}\``,
    `- **Airtable writes:** ${summary.airtable_writes} → ${summary.target?.base || AUTOPILOT_TARGET_BASE_LABEL} / ${summary.target?.table || AUTOPILOT_TARGET_TABLE} (\`${summary.target?.tableId || AUTOPILOT_TARGET_TABLE_ID}\`)`,
    `- **Brand Setup / Brand Explorer / VIC writes:** false (read-only or blocked)`,
    `- **Webhound candidates:** ${summary.webhound_candidates}`,
    `- **Steward cases:** ${summary.steward_review_cases}`,
    extras.notes ? `\n## Notes\n\n${extras.notes}\n` : "",
  ].join("\n");
}

export function buildSchemaV114PlanPayload() {
  return {
    version: "production-census-schema-v1.1.4-rooms-keys-provenance",
    mode: "plan_only_unless_schema_apply_confirmed",
    airtable_schema_writes: false,
    existing_live_fields: ["Rooms / Keys", "Rooms Confidence", "Rooms Source URL"],
    recommend_add_hold_to_rooms_confidence: true,
    proposed_fields: [
      {
        name: "Rooms Source Type",
        type: "singleSelect",
        options: [
          "official_property_page",
          "official_brand_directory",
          "official_hotel_website",
          "official_press_release",
          "official_development_page",
          "trusted_secondary_source",
          "steward_review",
        ],
      },
      { name: "Rooms Reviewed Date", type: "date" },
      { name: "Rooms Notes", type: "multilineText" },
    ],
  };
}

export async function assembleDryRunBundle(args, plan, opts = {}) {
  const proposals = opts.proposals || [];
  const blocked = opts.blocked || [];
  const steward = opts.steward || [];
  const hardCases = opts.hardCases || [];
  const conf = tallyConfidence([...proposals, ...blocked]);
  const webhound = routeWebhoundCandidates(hardCases, { max: 25 });
  const fieldSet = new Set();
  for (const p of proposals) {
    for (const f of Object.keys(p.patch || p.fields || {})) fieldSet.add(f);
  }
  const blockedReasons = {};
  for (const b of blocked) {
    const reason = b.block_reason || "unknown";
    blockedReasons[reason] = (blockedReasons[reason] || 0) + 1;
  }
  return {
    mode: args.mode,
    parent_company: args.parentCompany,
    region: args.region,
    country: args.country,
    batch_size: args.batchSize,
    max_records: args.maxRecords,
    run_until_complete: args.runUntilComplete,
    queues_executed: (plan.steps || []).map((s) => s.queue_id),
    proposals,
    blocked,
    steward_review_queue: steward,
    webhound_candidates: webhound,
    confidence_tally: conf,
    fields_proposed: [...fieldSet],
    records_scanned: opts.records_scanned ?? proposals.length,
    records_eligible: opts.records_eligible ?? proposals.length,
    provider_readiness: plan.provider_readiness,
    gates: evaluateCensusProcessingGates({ patches: proposals }),
    airtable_writes: false,
    note: opts.note || "v2 dry-run bundle",
  };
}

/**
 * Main runner entry (v2).
 */
export async function runCensusAutopilot(argv = process.argv.slice(2), opts = {}) {
  const started = Date.now();
  const args = parseAutopilotArgs(argv);
  const root = opts.root || ROOT;
  const autopilotRoot = path.join(root, "reports/research-engine-v2/autopilot");

  // Resume path
  if (args.resume) {
    const runDir = resolveRunDir(autopilotRoot, args.resume);
    const checkpoint = loadCheckpoint(runDir);
    if (!checkpoint) {
      return {
        ok: false,
        status: STATUS.BLOCKED,
        error: `checkpoint_not_found:${args.resume}`,
        airtable_writes: false,
      };
    }
    args.parentCompany = args.parentCompany || checkpoint.parent_company;
    args.region = args.region || checkpoint.region;
    args.country = args.country || checkpoint.country;
    args.mode = args.mode === "plan" ? checkpoint.mode || "apply" : args.mode;
    args.batchSize = args.batchSize || checkpoint.batch_size || 100;
    args.runUntilComplete = true;

    const priorProposals = opts.proposals || [];
    const result = await runUntilComplete({
      runDir,
      runId: checkpoint.run_id || args.resume,
      args,
      proposals: priorProposals.length
        ? priorProposals
        : opts.resumeProposals || [],
      queues: checkpoint.queues_remaining?.length
        ? checkpoint.queues_remaining
        : [checkpoint.current_queue || "rooms_keys"],
      airtable: opts.airtable || null,
      schemaV114Ready: Boolean(opts.schemaV114Ready),
      completedRecordIds: checkpoint.completed_record_ids || [],
      env: opts.env || process.env,
      enableProductionWrites: Boolean(opts.enableProductionWrites),
    });

    const status = opts.schemaV114Ready ? STATUS.READY : STATUS.READY_NEEDS_V114;
    const summary = buildRunSummary({
      parent_company: args.parentCompany,
      region: args.region,
      country: args.country,
      mode: args.mode,
      batch_size: args.batchSize,
      max_records: args.maxRecords,
      run_until_complete: true,
      total_records_in_scope: result.total_records_in_scope,
      total_processed: result.total_processed,
      total_updated: result.total_updated,
      total_skipped: result.total_skipped,
      total_blocked: result.total_blocked,
      fields_populated: result.fields_populated,
      confidence_tally: result.confidence_distribution,
      webhound_candidates_count: result.webhound_candidates?.candidates?.length || 0,
      steward_review_count: result.steward_review_queue?.length || 0,
      runtime_ms: Date.now() - started,
      remaining_queues: result.checkpoint?.queues_remaining || [],
      completion_status: result.completion_status,
      resume_command:
        result.completion_status === COMPLETION_STATUS.COMPLETE
          ? null
          : result.resume_command,
      recommended_next_run_command:
        result.completion_status === COMPLETION_STATUS.COMPLETE
          ? `npm run census:autopilot -- --region ${args.region} --parent-company ${args.parentCompany} --mode plan`
          : result.resume_command,
      status,
      airtable_writes: result.airtable_writes,
      target: result.target,
    });
    writeJson(path.join(runDir, "summary.json"), summary);
    writeText(path.join(runDir, "summary.md"), renderSummaryMarkdown(summary, { notes: "Resumed run" }));
    if (args.mode === "apply") {
      writeJson(path.join(runDir, "apply-summary.json"), result);
      writeText(
        path.join(runDir, "apply-summary.md"),
        `# Apply Summary\n\n- Completion: ${result.completion_status}\n- Updated: ${result.total_updated}\n- Writes: ${result.airtable_writes}\n`
      );
    }
    writeJson(path.join(runDir, "learning-update.json"), {
      census_autopilot_v2: true,
      resumed: true,
      batch_size_is_chunk_only: true,
      run_until_complete: true,
    });
    return {
      ok: result.ok,
      status,
      run_id: checkpoint.run_id || args.resume,
      run_dir: runDir,
      batch_result: result,
      summary,
      airtable_writes: result.airtable_writes,
      brand_explorer_writes: false,
    };
  }

  if (!args.parentCompany && args.scope !== "active-brand-setup" && !args.resume) {
    return {
      ok: false,
      status: STATUS.BLOCKED,
      error: "missing --parent-company (or use --scope active-brand-setup)",
      airtable_writes: false,
    };
  }

  const runName = buildRunFolderName(
    args.region,
    args.parentCompany || (args.scope === "active-brand-setup" ? "active-brands" : "unknown")
  );
  const runDir = ensureRunFolder(path.join(autopilotRoot, runName));
  const cache = createCensusCacheManager(path.join(runDir, "cache"));

  let schemaV114Ready = Boolean(opts.schemaV114Ready);
  if (opts.liveFieldNames) {
    schemaV114Ready = !inspectRoomsKeysSchemaStatus(opts.liveFieldNames).needs_v114_schema;
  }

  const providerReady = evaluateProviderReadiness(opts.env || process.env);
  let controlList = opts.controlList || null;
  let matchReport = opts.matchReport || null;
  let priorityPlan = opts.priorityPlan || null;

  if (args.scope === "active-brand-setup") {
    controlList =
      controlList ||
      buildActiveBrandSetupControlList({
        region: args.region,
        parentCompany: args.parentCompany,
        brands: opts.activeBrands,
        includeHeldProbe: Boolean(opts.includeHeldProbe),
        skipUniverseLoad: Boolean(opts.activeBrands),
      });
    writeActiveBrandSetupControlList(runDir, controlList);

    const censusRows = opts.censusRecords || [];
    matchReport =
      matchReport ||
      matchActiveBrandsToCensus(controlList, censusRows, {
        region: args.region,
        country: args.country,
      });
    writeBrandToCensusMatchReport(runDir, matchReport);
  }

  if ((args.strategy || STRATEGY_FASTEST_SAFE) === STRATEGY_FASTEST_SAFE) {
    const routed = routeAutopilotQueues({
      parentCompany: args.parentCompany,
      region: args.region,
      country: args.country,
      mode: args.mode,
      geocodeProviderReady: providerReady.approved_for_geocode_apply,
      schemaV114Ready,
    });
    priorityPlan =
      priorityPlan ||
      buildFastestSafePriorityPlan(routed.queues, {
        geocodeProviderReady: providerReady.approved_for_geocode_apply,
        schemaV114Ready,
      });
    if (args.queue) {
      priorityPlan = {
        ...priorityPlan,
        targeted_queue: args.queue,
        ordered_queue_ids: [args.queue],
        why: `Targeted queue run (--queue ${args.queue}); fastest-safe order overridden.`,
      };
    }
    writeQueuePriorityPlan(runDir, priorityPlan);
  } else if (args.queue) {
    priorityPlan = {
      version: "census-autopilot-targeted-queue-v1",
      strategy: args.strategy,
      ordered_queue_ids: [args.queue],
      targeted_queue: args.queue,
      why: `Targeted queue run (--queue ${args.queue})`,
    };
    writeQueuePriorityPlan(runDir, priorityPlan);
  }

  const plan = buildAutopilotPlan(args, {
    schemaV114Ready,
    recordsScanned: opts.records_scanned ?? matchReport?.census_records_matched ?? null,
    recordsEligible: opts.records_eligible ?? matchReport?.census_records_matched ?? null,
  });
  plan.scope = args.scope;
  plan.strategy = args.strategy || STRATEGY_FASTEST_SAFE;
  plan.active_brands_in_scope = controlList?.active_brands_in_scope ?? null;
  plan.parent_companies_in_scope = controlList?.parent_companies_in_scope ?? null;
  plan.processing_order = priorityPlan?.ordered_queue_ids || plan.queue_order;
  plan.processing_order_why = priorityPlan?.why || null;
  writeJson(path.join(runDir, "plan.json"), plan);
  writeText(path.join(runDir, "plan.md"), renderAutopilotPlanMarkdown(plan));

  if (args.warnings?.length) {
    writeJson(path.join(runDir, "warnings.json"), args.warnings);
  }

  if (args.mode === "schema-apply") {
    const schemaPlan = buildSchemaV114PlanPayload();
    writeJson(path.join(runDir, "schema-v114-plan.json"), schemaPlan);
    const summary = buildRunSummary({
      parent_company: args.parentCompany,
      region: args.region,
      country: args.country,
      mode: args.mode,
      batch_size: args.batchSize,
      status: STATUS.READY_NEEDS_V114,
      completion_status: COMPLETION_STATUS.BLOCKED_SCHEMA,
      runtime_ms: Date.now() - started,
      airtable_writes: false,
      recommended_next_run_command: plan.recommended_commands?.plan,
    });
    writeJson(path.join(runDir, "summary.json"), summary);
    writeText(path.join(runDir, "summary.md"), renderSummaryMarkdown(summary));
    writeText(path.join(runDir, "blocked-records.csv"), renderBlockedCsv([]));
    writeJson(path.join(runDir, "steward-review-queue.json"), []);
    writeJson(path.join(runDir, "webhound-candidates.json"), { candidates: [], capped_at: 25 });
    writeJson(path.join(runDir, "learning-update.json"), {
      census_autopilot_v2: true,
      schema_v114_applied: false,
    });
    return {
      ok: true,
      status: STATUS.READY_NEEDS_V114,
      run_id: runName,
      run_dir: runDir,
      plan,
      summary,
      airtable_writes: false,
    };
  }

  if (args.mode === "plan") {
    saveCheckpoint(runDir, {
      run_id: runName,
      parent_company: args.parentCompany,
      scope: args.scope,
      strategy: args.strategy,
      region: args.region,
      country: args.country,
      mode: "plan",
      batch_size: args.batchSize,
      completion_status: COMPLETION_STATUS.COMPLETE,
      total_records_in_scope: matchReport?.census_records_matched || opts.records_scanned || 0,
    });
    const summary = buildRunSummary({
      parent_company: args.parentCompany || "(active-brand-setup)",
      region: args.region,
      country: args.country,
      mode: "plan",
      batch_size: args.batchSize,
      max_records: args.maxRecords,
      run_until_complete: args.runUntilComplete,
      total_records_in_scope: matchReport?.census_records_matched || opts.records_scanned || 0,
      completion_status: COMPLETION_STATUS.COMPLETE,
      runtime_ms: Date.now() - started,
      status: schemaV114Ready ? STATUS.READY : STATUS.READY_NEEDS_V114,
      airtable_writes: false,
      recommended_next_run_command:
        `npm run census:autopilot -- --region ${args.region} --scope active-brand-setup --mode controlled --strategy fastest-safe --run-until-complete --batch-size ${args.batchSize}`,
      remaining_queues: priorityPlan?.ordered_queue_ids || [],
    });
    summary.active_brands_in_scope = controlList?.active_brands_in_scope;
    summary.parent_companies_in_scope = controlList?.parent_companies_in_scope;
    summary.processing_order = priorityPlan?.ordered_queue_ids;
    summary.processing_order_why = priorityPlan?.why;
    writeJson(path.join(runDir, "summary.json"), summary);
    writeText(
      path.join(runDir, "summary.md"),
      renderSummaryMarkdown(summary, {
        notes: [
          `Scope: ${args.scope}`,
          `Strategy: ${args.strategy || STRATEGY_FASTEST_SAFE}`,
          `Active brands: ${controlList?.active_brands_in_scope ?? "n/a"}`,
          `Parents: ${(controlList?.parent_companies_in_scope || []).join(", ") || "n/a"}`,
          `Order: ${(priorityPlan?.ordered_queue_ids || []).join(" → ")}`,
          `Why: ${priorityPlan?.why || ""}`,
        ].join("\n"),
      })
    );
    writeText(path.join(runDir, "blocked-records.csv"), renderBlockedCsv([]));
    writeJson(path.join(runDir, "steward-review-queue.json"), []);
    writeJson(path.join(runDir, "webhound-candidates.json"), { candidates: [], capped_at: 25 });
    writeJson(path.join(runDir, "learning-update.json"), {
      census_autopilot_active_brand_setup: true,
      fastest_safe: true,
      batch_size_is_chunk_only: true,
      run_until_complete_required_for_full_scope: true,
      brand_setup_read_only: true,
      brand_explorer_untouched: true,
    });
    return {
      ok: true,
      status: schemaV114Ready ? STATUS.READY : STATUS.READY_NEEDS_V114,
      run_id: runName,
      run_dir: runDir,
      plan,
      control_list: controlList,
      match_report: matchReport,
      priority_plan: priorityPlan,
      summary,
      airtable_writes: false,
      brand_explorer_writes: false,
      cache: cache.stats(),
    };
  }

  // dry-run / controlled / apply — batch engine
  let proposals = opts.proposals || [];
  if (!proposals.length && matchReport?.matched?.length && opts.synthesizeProposalsFromMatch) {
    proposals = matchReport.matched.map((m) => ({
      record_id: m.record_id,
      identity_key: m.identity_key,
      property_name: m.property_name,
      queue: (priorityPlan?.ordered_queue_ids || []).find((q) => q !== "coordinate_resolution") || "rooms_keys",
      confidence: "High",
      family: m.brand_family,
      patch: opts.proposalPatchForMatch?.(m) || {},
      current_fields: {},
    }));
  }
  // Live proposal loading is owned by the CLI multi-queue orchestrator.
  // Do not fall back to rooms_keys-only — that broke default fastest-safe runs.
  if (opts.liveDryRun && args.live && !proposals.length && !opts.orchestration) {
    writeJson(path.join(runDir, "warnings-live-proposals.json"), {
      warning: "live_flag_set_but_no_proposals_and_no_orchestration",
      hint: "Use census:autopilot CLI controlled/dry-run path (orchestrator) or pass opts.proposals",
    });
  }

  const orch = opts.orchestration || null;
  const orderedQueues =
    opts.queues ||
    (args.queue ? [args.queue] : null) ||
    (orch?.queues_executed?.length || orch?.queues_soft_deferred?.length || orch?.queues_skipped?.length
      ? [
          ...(orch.queues_executed || []),
          ...(orch.queues_skipped || []).filter((q) => !(orch.queues_executed || []).includes(q)),
          ...(orch.queues_soft_deferred || []),
        ]
      : null) ||
    priorityPlan?.ordered_queue_ids?.filter(
      (id) => id !== "steward_webhound_hard_cases" && id !== "source_discovery"
    ) ||
    ["rooms_keys"];

  const airtable =
    opts.airtable ||
    (opts.enableProductionWrites ? opts.liveAirtable : null) ||
    createMemoryAirtableAdapter(
      Object.fromEntries(
        proposals.map((p) => [
          p.record_id || p.id,
          { id: p.record_id || p.id, fields: p.current_fields || {} },
        ])
      )
    );

  const batchResult = await runUntilComplete({
    runDir,
    runId: runName,
    args,
    proposals,
    queues: orderedQueues,
    airtable,
    schemaV114Ready,
    completedRecordIds: [],
    env: opts.env || process.env,
    enableProductionWrites: Boolean(opts.enableProductionWrites),
  });

  writeText(
    path.join(runDir, "blocked-records.csv"),
    renderBlockedCsv(
      (batchResult.blocked_records || []).map((b) => ({
        record_id: b.record_id || b.id,
        identity_key: b.identity_key,
        property_name: b.property_name,
        queue: b.queue || "rooms_keys",
        block_reason: b.block_reason || b.guard?.errors?.[0] || b.idempotent?.reason,
        confidence: b.confidence,
      }))
    )
  );
  writeJson(path.join(runDir, "steward-review-queue.json"), batchResult.steward_review_queue || []);
  writeJson(path.join(runDir, "webhound-candidates.json"), batchResult.webhound_candidates || {});
  writeJson(path.join(runDir, "provider-decision-needed.json"), batchResult.provider_decision_needed || []);

  if (args.mode === "dry-run" || args.mode === "controlled") {
    const dry = await assembleDryRunBundle(args, plan, {
      proposals,
      blocked: batchResult.blocked_records,
      steward: batchResult.steward_review_queue,
      hardCases: batchResult.webhound_candidates?.candidates || [],
      records_scanned: batchResult.total_records_in_scope,
      records_eligible: proposals.length,
    });
    writeJson(path.join(runDir, "dry-run.json"), { ...dry, batch_result: batchResult });
    writeText(
      path.join(runDir, "dry-run.md"),
      `# Dry-Run / Controlled\n\n- Batches: ${batchResult.batches_run}\n- Scope: ${batchResult.total_records_in_scope}\n- Updated(proposed): ${batchResult.total_updated}\n- Airtable writes: false\n`
    );
  }

  if (args.mode === "controlled") {
    const multiBundle = buildMultiQueueApprovalBundle({
      run_id: runName,
      mode: args.mode,
      scope: args.scope,
      region: args.region,
      strategy: args.strategy || STRATEGY_FASTEST_SAFE,
      batch_size: args.batchSize,
      proposals,
      blocked: batchResult.blocked_records || orch?.blocked || [],
      queues_executed: orch?.queues_executed || orderedQueues.filter((q) =>
        proposals.some((p) => p.queue === q)
      ),
      queues_skipped: orch?.queues_skipped || [],
      queues_soft_deferred: orch?.queues_soft_deferred || [],
      steward: batchResult.steward_review_queue || [],
      webhound: batchResult.webhound_candidates || { candidates: [], capped_at: 25 },
    });
    writeJson(path.join(runDir, "approval-bundle.json"), {
      ...multiBundle,
      run_until_complete: args.runUntilComplete,
      forbidden_fields: AUTOPILOT_FORBIDDEN_FIELDS,
    });

    if (orch?.execution_report) {
      const execReport = {
        ...orch.execution_report,
        run_id: runName,
        mode: args.mode,
        scope: args.scope,
        region: args.region,
        strategy: args.strategy || STRATEGY_FASTEST_SAFE,
      };
      writeJson(path.join(runDir, "queue-execution-report.json"), execReport);
      writeText(
        path.join(runDir, "queue-execution-report.md"),
        renderQueueExecutionMarkdown(execReport)
      );
    }
  }

  if (args.mode === "apply") {
    writeJson(path.join(runDir, "apply-summary.json"), batchResult);
    writeText(
      path.join(runDir, "apply-summary.md"),
      [
        `# Apply Summary`,
        ``,
        `- Target: ${AUTOPILOT_TARGET_BASE_LABEL} / ${AUTOPILOT_TARGET_TABLE}`,
        `- Completion: ${batchResult.completion_status}`,
        `- Batches: ${batchResult.batches_run}`,
        `- Updated: ${batchResult.total_updated}`,
        `- Airtable writes: ${batchResult.airtable_writes}`,
        `- Provider decision needed: ${(batchResult.provider_decision_needed || []).length}`,
        ``,
      ].join("\n")
    );
    writeJson(path.join(runDir, "apply.json"), batchResult);
  }

  const status = schemaV114Ready ? STATUS.READY : STATUS.READY_NEEDS_V114;
  const summary = buildRunSummary({
    parent_company: args.parentCompany || "(active-brand-setup)",
    region: args.region,
    country: args.country,
    mode: args.mode,
    batch_size: args.batchSize,
    max_records: args.maxRecords,
    run_until_complete: args.runUntilComplete,
    total_records_in_scope: batchResult.total_records_in_scope,
    total_processed: batchResult.total_processed,
    total_updated: batchResult.total_updated,
    total_skipped: batchResult.total_skipped,
    total_blocked: batchResult.total_blocked,
    fields_populated: batchResult.fields_populated,
    confidence_tally: batchResult.confidence_distribution,
    blocked_reasons: {
      ...(batchResult.provider_decision_needed?.length
        ? { provider_decision_needed: batchResult.provider_decision_needed.length }
        : {}),
      ...(batchResult.blocked_source_families?.length
        ? { blocked_source_families: batchResult.blocked_source_families.length }
        : {}),
    },
    webhound_candidates_count: batchResult.webhound_candidates?.candidates?.length || 0,
    steward_review_count: batchResult.steward_review_queue?.length || 0,
    runtime_ms: Date.now() - started,
    remaining_queues: batchResult.checkpoint?.queues_remaining || [],
    completion_status: batchResult.completion_status,
    resume_command:
      batchResult.completion_status === COMPLETION_STATUS.COMPLETE
        ? null
        : batchResult.resume_command,
    recommended_next_run_command:
      batchResult.completion_status === COMPLETION_STATUS.COMPLETE
        ? args.mode === "controlled"
          ? `npm run census:autopilot -- --region ${args.region} --scope active-brand-setup --mode apply --strategy fastest-safe --run-until-complete --batch-size ${args.batchSize} --approval-bundle reports/research-engine-v2/autopilot/${runName}/approval-bundle.json --confirm-approval-bundle-bound --enable-production-writes (+ all confirm flags)`
          : plan.recommended_commands?.plan
        : batchResult.resume_command || plan.recommended_commands?.apply,
    status,
    airtable_writes: batchResult.airtable_writes,
    target: batchResult.target,
  });
  summary.scope = args.scope;
  summary.strategy = args.strategy;
  summary.active_brands_in_scope = controlList?.active_brands_in_scope;
  summary.parent_companies_in_scope = controlList?.parent_companies_in_scope;
  summary.processing_order = priorityPlan?.ordered_queue_ids;
  summary.processing_order_why = priorityPlan?.why;
  summary.runtime_metrics = batchResult.runtime_metrics;

  writeJson(path.join(runDir, "summary.json"), summary);
  writeText(path.join(runDir, "summary.md"), renderSummaryMarkdown(summary));
    writeJson(path.join(runDir, "learning-update.json"), {
      census_autopilot_active_brand_setup: true,
      fastest_safe: true,
      batch_by_batch_production_writes: true,
      batch_size_is_chunk_only: true,
      run_until_complete: Boolean(args.runUntilComplete),
      apply_writes_high_confidence_allowlist: true,
      hard_cases_route_instead_of_full_stop: true,
      webhound_hard_case_only: true,
      runtime_metrics_required: true,
      performance_guardrails: true,
      brand_setup_read_only: true,
      brand_explorer_untouched: true,
      completion_status: batchResult.completion_status,
    });

  return {
    ok: batchResult.ok !== false,
    status,
    run_id: runName,
    run_dir: runDir,
    plan,
    control_list: controlList,
    match_report: matchReport,
    priority_plan: priorityPlan,
    batch_result: batchResult,
    summary,
    airtable_writes: batchResult.airtable_writes,
    brand_explorer_writes: false,
    cache: cache.stats(),
    warnings: args.warnings,
  };
}

export {
  parseAutopilotArgs,
  isWritableConfidence,
  guardApplyBatch,
  createMemoryAirtableAdapter,
  applyPreflight,
  checkAutopilotApplyEnv,
  buildCheckpoint,
};
