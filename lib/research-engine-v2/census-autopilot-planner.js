/**
 * Census Autopilot planner v2 — batch-size / run-until-complete oriented.
 */

import { routeAutopilotQueues, selectQueuesForMode, BLOCKED_PRODUCTION_WRITE_LANES } from "./census-autopilot-queue-router.js";
import { resolveExtractorFamily } from "./census-family-extractor-registry.js";
import { CENSUS_PROCESSING_GATES } from "./census-processing-gates.js";
import { evaluateProviderReadiness } from "./production-census-description-extraction.js";
import { listCensusQueues } from "./production-census-queue-engine.js";
import { AUTOPILOT_ALLOWED_WRITE_FIELDS } from "./census-autopilot-field-allowlist.js";

export const AUTOPILOT_PLANNER_VERSION = "census-autopilot-planner-v2";

export const DEFAULT_BATCH_SIZES = Object.freeze({
  "dry-run": { min: 25, max: 250, default: 250 },
  apply: { min: 25, max: 100, default: 100 },
  webhound_candidates: { min: 10, max: 25, default: 25 },
});

export function normalizeParentCompanyLabel(parentCompany) {
  const { family } = resolveExtractorFamily(parentCompany);
  const raw = String(parentCompany || "").trim();
  if (!raw) return { label: null, family: "generic" };
  return { label: raw, family };
}

/**
 * Recommend batch-size (chunk size only — does not cap total scope).
 * @param {string} mode
 * @param {number|null|undefined} requested
 */
export function recommendBatchSize(mode, requested) {
  const key = mode === "apply" ? "apply" : "dry-run";
  const cfg = DEFAULT_BATCH_SIZES[key];
  if (requested == null || !Number.isFinite(Number(requested))) return cfg.default;
  const n = Number(requested);
  return Math.min(cfg.max, Math.max(1, n));
}

/** @deprecated use recommendBatchSize */
export function recommendLimit(mode, requested) {
  return recommendBatchSize(mode, requested);
}

/**
 * @param {object} args
 * @param {object} [ctx]
 */
export function buildAutopilotPlan(args, ctx = {}) {
  const provider = evaluateProviderReadiness(process.env);
  const parent = normalizeParentCompanyLabel(args.parentCompany);
  const batchSize = recommendBatchSize(args.mode, args.batchSize ?? args.limit);
  const routed = routeAutopilotQueues({
    parentCompany: args.parentCompany,
    region: args.region,
    country: args.country,
    mode: args.mode,
    geocodeProviderReady: provider.approved_for_geocode_apply,
    schemaV114Ready: Boolean(ctx.schemaV114Ready),
  });
  const selected = selectQueuesForMode(routed, args.mode || "plan");
  const existingQueues = listCensusQueues();

  const steps = selected.map((q, i) => ({
    step: i + 1,
    queue_id: q.id,
    letter: q.letter,
    label: q.label,
    status: q.status,
    blockers: q.blockers,
    target_fields: q.target_fields,
    writes: q.writes,
    existing_module: q.existing_module,
    queue_engine_id: q.queue_engine_id || null,
    note: q.note,
  }));

  const pc = args.parentCompany || "IHG";
  const region = args.region || "CALA";
  const recommendedCommands = {
    plan: `npm run census:autopilot -- --region ${region} --parent-company ${pc} --mode plan`,
    dry_run: `npm run census:autopilot -- --region ${region} --parent-company ${pc} --mode dry-run --run-until-complete --batch-size ${batchSize}`,
    controlled: `npm run census:autopilot -- --region ${region} --parent-company ${pc} --mode controlled --run-until-complete --batch-size ${batchSize}`,
    apply: `npm run census:autopilot -- --region ${region} --parent-company ${pc} --mode apply --run-until-complete --batch-size ${Math.min(batchSize, DEFAULT_BATCH_SIZES.apply.max)} --confirm-safe-writes --confirm-write-to-production-census --confirm-no-brand-explorer-writes --confirm-no-owner-operator --confirm-no-date-writes --confirm-no-recent-momentum --confirm-no-company-validation --confirm-webhound-not-production-source`,
    sample: `npm run census:autopilot -- --region ${region} --parent-company ${pc} --mode dry-run --max-records 50 --batch-size 25`,
  };

  return {
    version: AUTOPILOT_PLANNER_VERSION,
    generated_at: new Date().toISOString(),
    parent_company: args.parentCompany || null,
    parent_family: parent.family,
    region,
    country: args.country || null,
    mode: args.mode || "plan",
    batch_size: batchSize,
    max_records: args.maxRecords ?? null,
    run_until_complete: Boolean(args.runUntilComplete),
    legacy_limit: args.legacyLimit ?? args.limit ?? null,
    confidence_threshold: args.confidenceThreshold || "High",
    provider: args.provider || provider.provider_info?.provider || null,
    provider_readiness: {
      approved_for_geocode_apply: provider.approved_for_geocode_apply,
      block_reason: provider.block_reason,
      preferred: provider.preferred,
      note: "Missing provider decision skips geocode only; other queues continue",
    },
    schema_v114: {
      ready: Boolean(ctx.schemaV114Ready),
      note: ctx.schemaV114Ready
        ? "Rooms provenance fields present"
        : "Rooms Source Type / Reviewed Date / Notes + Hold still planned (v1.1.4)",
    },
    records_scanned: ctx.recordsScanned ?? null,
    records_eligible: ctx.recordsEligible ?? null,
    queue_order: routed.order,
    steps,
    existing_queue_engine: existingQueues,
    blocked_production_lanes: BLOCKED_PRODUCTION_WRITE_LANES,
    allowed_write_fields: AUTOPILOT_ALLOWED_WRITE_FIELDS,
    processing_gates: CENSUS_PROCESSING_GATES,
    batch_sizes: DEFAULT_BATCH_SIZES,
    extractor_family: parent,
    speed_rules: [
      "batch_size_is_chunk_only",
      "run_until_complete_for_full_parent_region",
      "max_records_for_sample_test_only",
      "checkpoint_after_each_batch",
      "re_read_before_write",
      "idempotent_blank_or_match_only",
      "provider_soft_route_does_not_stop_run",
      "no_full_be_gates_on_census_dry_run",
    ],
    recommended_commands: recommendedCommands,
    recommended_next: recommendedCommands.dry_run,
    airtable_writes: false,
    brand_explorer_writes: false,
  };
}

export function renderAutopilotPlanMarkdown(plan) {
  const lines = [
    `# Census Autopilot Plan (v2)`,
    ``,
    `- **Parent company:** ${plan.parent_company || "(required)"}`,
    `- **Region / country:** ${plan.region}${plan.country ? ` / ${plan.country}` : ""}`,
    `- **Mode:** ${plan.mode}`,
    `- **Batch size (chunk):** ${plan.batch_size}`,
    `- **Max records (sample cap):** ${plan.max_records ?? "(none — full scope)"}`,
    `- **Run until complete:** ${plan.run_until_complete}`,
    `- **Confidence threshold:** ${plan.confidence_threshold}`,
    `- **Geocode apply ready:** ${plan.provider_readiness.approved_for_geocode_apply}`,
    `- **v1.1.4 schema ready:** ${plan.schema_v114.ready}`,
    ``,
    `## Queue order`,
    ``,
  ];
  for (const s of plan.steps) {
    lines.push(
      `${s.step}. **${s.letter}. ${s.label}** (\`${s.queue_id}\`) — status: \`${s.status}\``
    );
    if (s.blockers?.length) lines.push(`   - blockers: ${s.blockers.join(", ")}`);
  }
  lines.push(
    ``,
    `## Recommended next`,
    ``,
    "```bash",
    plan.recommended_next,
    "```",
    ``
  );
  return lines.join("\n");
}
