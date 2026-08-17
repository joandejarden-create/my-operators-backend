/**
 * DEALALITY CENSUS AUTOPILOT V1 — orchestrator.
 * Dry-run by default. No Webhound. No Airtable writes. No credit spend.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";

import {
  AUTOPILOT_V1_VERSION,
  AUTOPILOT_V1_ARTIFACT_DIR,
  AUTOPILOT_V1_CONSTRAINTS,
} from "./constants.js";
import { resolveMode, MODE_REGISTRY } from "./modes.js";
import { buildFieldRoutingRegistry } from "./field-routing.js";
import { SOURCE_LANE_REGISTRY } from "./source-lanes.js";
import { prioritizeQueue } from "./priority-engine.js";
import { resolveAllResearchableFields } from "./field-resolution.js";
import { assessCompleteness } from "./completeness.js";
import { classifyOutput, imageRightsReviewRequired } from "./output-classes.js";
import {
  createHotelEffortTracker,
  shouldStopHotelResearch,
} from "./research-budget.js";
import {
  buildCventDiscoveryChallenges,
  findMexicoCventHarvest,
  loadCventHarvestUrls,
  buildLegacyChallengeSummaryFromOverlap,
} from "./challenge-adapters.js";
import {
  aggregateBrands,
  buildActivationCandidates,
  stageOperatorRelationship,
  assessImageIntegrity,
} from "./brand-and-media.js";
import {
  saveResumeState,
  loadResumeState,
  resolveAutopilotV1RunDir,
  buildResumeState,
} from "./resume-state.js";
import { writeAllArtifacts } from "./artifact-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../../..");

const DEFAULT_VIC_INDEX = path.join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family/01_combined_4family_index.json"
);
const DEFAULT_BE_READY = path.join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family/12_brand_explorer_completion_readiness.json"
);
const DEFAULT_LEGACY_OVERLAP = path.join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined/06-legacy-overlap-summary.json"
);

/**
 * @param {object} opts
 */
export async function runCensusAutopilotV1(opts = {}) {
  const started = Date.now();
  const log = opts.log || console.log;
  const dryRun = opts.dryRun !== false;
  const mode = resolveMode(opts.mode || "unified_benchmark");
  const artifactRoot = path.join(ROOT, opts.artifactDir || AUTOPILOT_V1_ARTIFACT_DIR);
  fs.mkdirSync(artifactRoot, { recursive: true });

  let runId = opts.runId || null;
  let resume = null;
  if (opts.resume) {
    const runDir = resolveAutopilotV1RunDir(artifactRoot, opts.resume);
    resume = loadResumeState(runDir);
    if (!resume) throw new Error(`Resume state not found for ${opts.resume}`);
    runId = resume.run_id;
    log(`[autopilot-v1] resuming ${runId}`);
  } else {
    runId =
      runId ||
      `cav1_${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${randomUUID().slice(0, 6)}`;
  }

  const runDir = path.join(artifactRoot, "runs", runId);
  fs.mkdirSync(runDir, { recursive: true });

  // Load field registry (complete researchable set)
  const fieldRouting = buildFieldRoutingRegistry();
  const researchable = fieldRouting.researchable;

  // Load VIC freeze — benchmark families
  const vicPath = opts.vicIndexPath || DEFAULT_VIC_INDEX;
  const vic = JSON.parse(fs.readFileSync(vicPath, "utf8"));
  let records = vic.records || [];

  const families = normalizeList(opts.group || opts.families || "IHG,Hilton,Choice");
  const country = opts.country || "Mexico";
  const brandFilter = opts.brand || null;
  const maxRecords = opts.maxRecords != null ? Number(opts.maxRecords) : null;

  records = records.filter((r) => {
    if (country && r.country && r.country !== country) return false;
    if (families.length && !families.includes(r.family)) return false;
    if (brandFilter && r.brand !== brandFilter) return false;
    return true;
  });

  // Priority queue
  const priorityBands = opts.priority
    ? String(opts.priority)
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : null;
  let queue = prioritizeQueue(records, { priorityBands });
  if (maxRecords != null && Number.isFinite(maxRecords)) {
    queue = queue.slice(0, maxRecords);
  }

  const completedSet = new Set(resume?.completed_entity_ids || []);
  if (completedSet.size) {
    queue = queue.filter((r) => !completedSet.has(r.independent_record_id));
  }

  log(
    `[autopilot-v1] mode=${mode.mode} country=${country} families=${families.join("|")} queue=${queue.length} researchable_fields=${researchable.length} dry_run=${dryRun}`
  );

  /** @type {object[]} */
  const processed = [];
  const failed = [];
  const completedIds = [...completedSet];
  const sourceFailures = [...(resume?.source_failures || [])];
  const researchCheckpoints = [...(resume?.research_checkpoints || [])];

  let hotelsDiscovered = records.length;
  let fieldsResearched = 0;
  let fieldsResolved = 0;
  let fieldsUnresolved = 0;
  let escalations = 0;
  let externalCost = 0;

  for (let i = 0; i < queue.length; i++) {
    const rec = queue[i];
    try {
      const effort = createHotelEffortTracker(rec.independent_record_id);
      const fieldResult = resolveAllResearchableFields(rec, researchable);
      effort.field_attempts = fieldResult.fields_researched;
      effort.escalations = fieldResult.escalations.length;
      effort.sufficient_authoritative_core = (rec.core_pct || 0) >= 90;

      const stop = shouldStopHotelResearch(effort);
      const completeness = assessCompleteness(fieldResult, rec);
      const image_integrity = assessImageIntegrity(rec);
      const image_rights_review_required = imageRightsReviewRequired(image_integrity);
      const output_class = classifyOutput(rec, fieldResult, completeness, {
        // Image rights stay parallel — do not override census data class
        factual_source_rights_blocked: false,
      });

      const hotel = {
        independent_record_id: rec.independent_record_id,
        name: rec.name,
        brand: rec.brand,
        family: rec.family,
        country: rec.country,
        city: rec.city,
        status: rec.status,
        website: rec.website,
        priority: rec.priority,
        field_result: fieldResult,
        completeness,
        image_integrity,
        image_rights_review_required,
        output_class,
        research_stop: stop,
        effort,
        legacy_used_as_source: false,
        cvent_used_as_source: false,
        candidate_origin_reference: "verified_independent_census_freeze",
      };
      hotel.operator_staging = stageOperatorRelationship(hotel);
      processed.push(hotel);

      fieldsResearched += fieldResult.fields_researched;
      fieldsResolved += fieldResult.fields_resolved;
      fieldsUnresolved += fieldResult.fields_unresolved;
      escalations += fieldResult.escalations.length;
      completedIds.push(rec.independent_record_id);

      if ((i + 1) % 50 === 0 || i === queue.length - 1) {
        researchCheckpoints.push({
          at: new Date().toISOString(),
          completed: completedIds.length,
          last_id: rec.independent_record_id,
        });
        saveResumeState(runDir, {
          run_id: runId,
          mode: mode.mode,
          group: families.join(","),
          brand: brandFilter,
          country,
          dry_run: dryRun,
          total_in_scope: completedSet.size + queue.length,
          completed: completedIds.length,
          failed: failed.length,
          remaining: queue.length - (i + 1),
          completed_entity_ids: completedIds,
          failed_entities: failed,
          source_failures: sourceFailures,
          research_checkpoints: researchCheckpoints,
        });
        log(`[autopilot-v1] progress ${i + 1}/${queue.length}`);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      failed.push({ independent_record_id: rec.independent_record_id, error: msg });
      sourceFailures.push({ entity: rec.independent_record_id, error: msg });
      log(`[autopilot-v1] fail ${rec.independent_record_id}: ${msg}`);
    }
  }

  // Brand aggregation + activation (no activate)
  let beReady = null;
  if (fs.existsSync(DEFAULT_BE_READY)) {
    beReady = JSON.parse(fs.readFileSync(DEFAULT_BE_READY, "utf8"));
  }
  const brandAgg = aggregateBrands(processed);
  const activation = buildActivationCandidates(brandAgg, beReady);
  const operatorStaging = {
    version: "census-autopilot-v1-operator-relationship-staging",
    note: "Future Operator Explorer input — not Operator Research Mode",
    relationships: processed.map((h) => h.operator_staging),
  };

  // Cvent challenge (Mexico harvest URLs only — no field values as evidence)
  const cventHarvest = findMexicoCventHarvest(ROOT);
  let cventUrls = cventHarvest ? loadCventHarvestUrls(cventHarvest) : [];
  // Cap challenge emit for artifact size; full URL count retained in summary
  const cventUrlTotal = cventUrls.length;
  cventUrls = cventUrls.slice(0, opts.cventChallengeLimit ?? 400);
  const cventChallenges = buildCventDiscoveryChallenges(cventUrls, records, {
    maxChallenges: opts.cventChallengeLimit ?? 400,
  });
  cventChallenges.mexico_harvest_path = cventHarvest;
  cventChallenges.mexico_hotel_urls_total = cventUrlTotal;

  // Legacy challenge summary from overlap (quarantine)
  let legacyOverlap = {};
  if (fs.existsSync(DEFAULT_LEGACY_OVERLAP)) {
    legacyOverlap = JSON.parse(fs.readFileSync(DEFAULT_LEGACY_OVERLAP, "utf8"));
  }
  const legacyChallenges = buildLegacyChallengeSummaryFromOverlap(legacyOverlap);

  const imageResults = {
    version: "census-autopilot-v1-image-integrity",
    hotels: processed.map((h) => ({
      independent_record_id: h.independent_record_id,
      name: h.name,
      ...h.image_integrity,
    })),
  };

  const escalationResults = {
    version: "census-autopilot-v1-escalation",
    auto_call_webhound: false,
    hotels: processed
      .filter((h) => (h.field_result.escalations || []).length > 0)
      .map((h) => ({
        independent_record_id: h.independent_record_id,
        name: h.name,
        escalations: h.field_result.escalations,
        destinations: ["Webhound Candidate (queued only)", "Human Research", "First-Party Validation"],
      })),
  };

  const observability = {
    version: "census-autopilot-v1-observability",
    run_id: runId,
    mode: mode.mode,
    hotels_discovered: hotelsDiscovered,
    hotels_researched: processed.length,
    hotels_failed: failed.length,
    fields_researched: fieldsResearched,
    fields_resolved: fieldsResolved,
    fields_unresolved: fieldsUnresolved,
    corrections_proposed: 0,
    new_properties_found: 0,
    reflags: 0,
    openings: 0,
    operator_relationships_staged: operatorStaging.relationships.filter((r) => r.operator).length,
    owner_relationships: processed.filter((h) =>
      (h.field_result.fields || []).some(
        (f) => f.field === "Owner Name" && f.independently_researched_value
      )
    ).length,
    brand_activation_candidates: activation.candidate_count,
    image_issues: imageResults.hotels.filter((h) => h.issues?.length).length,
    image_rights_review_required: processed.filter((h) => h.image_rights_review_required).length,
    source_failures: sourceFailures.length,
    escalations,
    runtime_ms: Date.now() - started,
    external_cost_usd: externalCost,
    provenance_completeness_avg: avg(
      processed.map((h) => h.completeness.provenance_completeness)
    ),
    material_completeness_avg: avg(processed.map((h) => h.completeness.material_completeness)),
    output_class_counts: countBy(processed, (h) => h.output_class),
    priority_band_counts: countBy(processed, (h) => h.priority?.band),
    constraints: AUTOPILOT_V1_CONSTRAINTS,
  };

  const result = {
    version: AUTOPILOT_V1_VERSION,
    run_id: runId,
    dry_run: dryRun,
    mode,
    field_routing: fieldRouting,
    source_lanes: SOURCE_LANE_REGISTRY,
    mode_registry: MODE_REGISTRY,
    processed,
    brand_aggregation: brandAgg,
    activation_candidates: activation,
    operator_staging: operatorStaging,
    image_integrity: imageResults,
    cvent_challenges: cventChallenges,
    legacy_challenges: legacyChallenges,
    escalations: escalationResults,
    observability,
    run_dir: runDir,
    artifact_root: artifactRoot,
  };

  // Persist run snapshot
  fs.writeFileSync(path.join(runDir, "result-summary.json"), JSON.stringify({
    run_id: runId,
    observability,
    constraints: AUTOPILOT_V1_CONSTRAINTS,
  }, null, 2), "utf8");

  saveResumeState(runDir, {
    run_id: runId,
    mode: mode.mode,
    group: families.join(","),
    brand: brandFilter,
    country,
    dry_run: dryRun,
    total_in_scope: hotelsDiscovered,
    completed: completedIds.length,
    failed: failed.length,
    remaining: 0,
    completed_entity_ids: completedIds,
    failed_entities: failed,
    source_failures: sourceFailures,
    research_checkpoints: researchCheckpoints,
    observability_snapshot: observability,
  });

  if (opts.writeArtifacts !== false) {
    await writeAllArtifacts(result, { root: ROOT, artifactRoot, log });
  }

  log(`[autopilot-v1] done run_id=${runId} researched=${processed.length} ms=${observability.runtime_ms}`);
  return result;
}

function normalizeList(v) {
  if (Array.isArray(v)) return v;
  return String(v || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function avg(nums) {
  if (!nums.length) return 0;
  return Math.round(nums.reduce((a, b) => a + b, 0) / nums.length);
}

function countBy(arr, fn) {
  const o = {};
  for (const x of arr) {
    const k = fn(x) || "unknown";
    o[k] = (o[k] || 0) + 1;
  }
  return o;
}

export { buildResumeState, MODE_REGISTRY };
