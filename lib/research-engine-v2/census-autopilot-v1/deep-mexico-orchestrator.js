/**
 * Census Autopilot V1.1 — Live deep-research Mexico benchmark orchestrator.
 * No Webhound, no paid credits, no Airtable writes.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";

import { AUTOPILOT_V1_CONSTRAINTS, OUTPUT_CLASS, FIELD_RESOLUTION_STATUS } from "./constants.js";
import { buildFieldRoutingRegistry } from "./field-routing.js";
import { prioritizeQueue } from "./priority-engine.js";
import { assessCompleteness } from "./completeness.js";
import { classifyOutput, imageRightsReviewRequired } from "./output-classes.js";
import {
  liveDeepResearchHotel,
  warmFamilyDirectoryCaches,
  MATERIAL_HARD_FIELDS,
  LIVE_DEEP_VERSION,
} from "./live-deep-research.js";
import {
  aggregateBrands,
  buildActivationCandidates,
  stageOperatorRelationship,
  assessImageIntegrity,
} from "./brand-and-media.js";
import {
  buildCventDiscoveryChallenges,
  findMexicoCventHarvest,
  loadCventHarvestUrls,
  buildLegacyChallengeSummaryFromOverlap,
} from "./challenge-adapters.js";
import { saveResumeState, loadResumeState, resolveAutopilotV1RunDir } from "./resume-state.js";
import { sleep } from "../adapters/adapter-utils.js";
import { writeDeepMexicoArtifacts } from "./deep-mexico-artifact-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../../..");

const DEFAULT_VIC = path.join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family/01_combined_4family_index.json"
);
const DEFAULT_BE = path.join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family/12_brand_explorer_completion_readiness.json"
);
const DEFAULT_LEGACY = path.join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined/06-legacy-overlap-summary.json"
);
const ARTIFACT_DIR = "data/research-engine-v2/census-autopilot-v1-1-deep-mexico";

const V1_BASELINE = Object.freeze({
  hotels: 365,
  material_completeness_avg: 60,
  compact_index_native_pct: 47,
  production_candidates: 99,
  material_remediation: 248,
  partial: 18,
  field_escalations: 730,
  brand_completion_candidates: 37,
});

/**
 * @param {object} opts
 */
export async function runDeepMexicoBenchmark(opts = {}) {
  const started = Date.now();
  const log = opts.log || console.log;
  const artifactRoot = path.join(ROOT, opts.artifactDir || ARTIFACT_DIR);
  fs.mkdirSync(artifactRoot, { recursive: true });

  let runId = opts.runId || null;
  let resume = null;
  if (opts.resume) {
    const runDir = resolveAutopilotV1RunDir(artifactRoot, opts.resume);
    resume = loadResumeState(runDir);
    if (!resume) throw new Error(`Resume state not found: ${opts.resume}`);
    runId = resume.run_id;
    log(`[v1.1] resuming ${runId}`);
  } else {
    runId =
      runId ||
      `cav11_${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${randomUUID().slice(0, 6)}`;
  }
  const runDir = path.join(artifactRoot, "runs", runId);
  fs.mkdirSync(runDir, { recursive: true });

  // Gap map first (always rewrite)
  writeGapMap(artifactRoot);

  const fieldRouting = buildFieldRoutingRegistry();
  const researchable = fieldRouting.researchable;

  const vic = JSON.parse(fs.readFileSync(opts.vicIndexPath || DEFAULT_VIC, "utf8"));
  let records = (vic.records || []).filter(
    (r) =>
      r.country === "Mexico" &&
      ["IHG", "Hilton", "Choice"].includes(r.family)
  );

  const maxRecords = opts.maxRecords != null ? Number(opts.maxRecords) : null;
  let queue = prioritizeQueue(records, {
    ctx: { brandExplorerActivationValue: true },
  });
  if (maxRecords != null && Number.isFinite(maxRecords)) queue = queue.slice(0, maxRecords);

  const completedSet = new Set(resume?.completed_entity_ids || []);
  if (completedSet.size) {
    queue = queue.filter((r) => !completedSet.has(r.independent_record_id));
  }

  log(
    `[v1.1] LIVE deep research hotels=${queue.length} researchable=${researchable.length} delayMs=${opts.delayMs ?? 350}`
  );

  // Warm Lane A caches once
  try {
    log("[v1.1] warming Hilton/Choice directory caches…");
    await warmFamilyDirectoryCaches({ delayMs: 150 });
    log("[v1.1] directory caches warm");
  } catch (err) {
    log(`[v1.1] cache warm warning: ${err?.message || err}`);
  }

  /** @type {object[]} */
  const processed = [];
  const failed = [];
  const completedIds = [...completedSet];
  const sourceFailures = [...(resume?.source_failures || [])];
  const researchCheckpoints = [...(resume?.research_checkpoints || [])];

  const delayMs = opts.delayMs ?? 350;
  const concurrency = Math.max(1, Math.min(Number(opts.concurrency || 3), 5));

  for (let i = 0; i < queue.length; i += concurrency) {
    const batch = queue.slice(i, i + concurrency);
    const results = await Promise.all(
      batch.map(async (rec) => {
        try {
          const live = await liveDeepResearchHotel(rec, {
            researchable,
            delayMs,
            timeoutMs: opts.timeoutMs ?? 25000,
          });
          return { ok: true, rec, live };
        } catch (err) {
          return {
            ok: false,
            rec,
            error: err?.message || String(err),
          };
        }
      })
    );

    for (const r of results) {
      if (!r.ok) {
        failed.push({ independent_record_id: r.rec.independent_record_id, error: r.error });
        sourceFailures.push({ entity: r.rec.independent_record_id, error: r.error });
        log(`[v1.1] FAIL ${r.rec.independent_record_id}: ${r.error}`);
        // Continue — do not abort cohort
        continue;
      }

      const live = r.live;
      const field_result = {
        independent_record_id: live.independent_record_id,
        name: live.name,
        family: live.family,
        brand: live.brand,
        fields_researched: live.fields_researched,
        fields_resolved: live.fields_resolved,
        fields_unresolved: live.fields_unresolved,
        fields: live.fields,
        escalations: live.fields
          .filter((f) =>
            [
              FIELD_RESOLUTION_STATUS.DEEP_RESEARCH_REQUIRED,
              FIELD_RESOLUTION_STATUS.SOURCE_BLOCKED,
              FIELD_RESOLUTION_STATUS.CONFLICTING_EVIDENCE,
            ].includes(f.resolution_status)
          )
          .map((f) => ({
            field: f.field,
            status: f.resolution_status,
            escalation_status: f.escalation_status,
          })),
      };

      const enrichedRecord = {
        ...r.rec,
        phone: valueOf(live, "Phone"),
        address: valueOf(live, "Address"),
        rooms: valueOf(live, "Rooms / Keys"),
        latitude: valueOf(live, "Latitude"),
        longitude: valueOf(live, "Longitude"),
        amenities: valueOf(live, "Amenities - Source Text"),
        description: valueOf(live, "Hotel Description - Source Text"),
        operator: valueOf(live, "Operator / Management Company"),
        owner: valueOf(live, "Owner Name"),
        open_date: valueOf(live, "Opening Date"),
        material_pct: estimateMaterialPct(live, r.rec),
      };

      const completeness = assessCompleteness(field_result, enrichedRecord);
      // Prefer live material estimate
      completeness.material_completeness = Math.max(
        completeness.material_completeness,
        enrichedRecord.material_pct || 0
      );
      completeness.overall_research_readiness = Math.round(
        0.25 * completeness.core_completeness +
          0.3 * completeness.material_completeness +
          0.15 * completeness.evidence_completeness +
          0.15 * completeness.provenance_completeness +
          0.1 * completeness.freshness_completeness +
          0.05 * completeness.image_completeness
      );

      const image_integrity = assessImageIntegrity(enrichedRecord);
      // Image research from official page presence
      if (live.page_ok) {
        image_integrity.official_imagery_source_identified = "Possible on official property page";
        image_integrity.current_image_exists = "Likely on official page — not downloaded";
        image_integrity.source = live.website;
      }
      const output_class = classifyOutput(enrichedRecord, field_result, completeness, {});

      const hotel = {
        ...live,
        priority: r.rec.priority,
        completeness,
        image_integrity,
        image_rights_review_required: imageRightsReviewRequired(image_integrity),
        output_class,
        field_result,
        material_pct_after: completeness.material_completeness,
        v1_class_before: classifyFromV1Baseline(r.rec),
      };
      hotel.operator_staging = stageOperatorRelationship(hotel);
      processed.push(hotel);
      completedIds.push(live.independent_record_id);
    }

    const done = Math.min(i + concurrency, queue.length);
    if (done % 15 < concurrency || done === queue.length) {
      researchCheckpoints.push({
        at: new Date().toISOString(),
        completed: completedIds.length,
        failed: failed.length,
      });
      saveResumeState(runDir, {
        run_id: runId,
        mode: "live_deep_mexico",
        group: "IHG,Hilton,Choice",
        country: "Mexico",
        dry_run: true,
        total_in_scope: completedSet.size + queue.length,
        completed: completedIds.length,
        failed: failed.length,
        remaining: queue.length - done,
        completed_entity_ids: completedIds,
        failed_entities: failed,
        source_failures: sourceFailures,
        research_checkpoints: researchCheckpoints,
      });
      // Persist incremental hotel results
      fs.writeFileSync(
        path.join(runDir, "processed-partial.json"),
        JSON.stringify(
          {
            count: processed.length,
            hotels: processed.map(compactHotel),
          },
          null,
          2
        ),
        "utf8"
      );
      log(
        `[v1.1] progress ${done}/${queue.length} ok=${processed.length} fail=${failed.length} mat_avg=${avg(processed.map((h) => h.material_pct_after))}%`
      );
    }
  }

  // Bounded Cvent challenge sample (100) — independent rediscovery only
  log("[v1.1] Cvent challenge sample (100)…");
  const cventSample = await runCventChallengeSample({
    independentRecords: records,
    limit: opts.cventSampleLimit ?? 100,
    delayMs: Math.min(delayMs, 300),
    log,
  });

  // Legacy challenge sample (summary + synthetic rediscovery notes)
  let legacyOverlap = {};
  if (fs.existsSync(DEFAULT_LEGACY)) {
    legacyOverlap = JSON.parse(fs.readFileSync(DEFAULT_LEGACY, "utf8"));
  }
  const legacySample = buildLegacyChallengeSample(legacyOverlap, processed);

  let beReady = null;
  if (fs.existsSync(DEFAULT_BE)) beReady = JSON.parse(fs.readFileSync(DEFAULT_BE, "utf8"));
  const brandAgg = aggregateBrands(processed);
  const activation = buildActivationCandidates(brandAgg, beReady);

  const result = {
    version: LIVE_DEEP_VERSION,
    run_id: runId,
    dry_run: true,
    constraints: AUTOPILOT_V1_CONSTRAINTS,
    v1_baseline: V1_BASELINE,
    field_routing: fieldRouting,
    processed,
    failed,
    brand_aggregation: brandAgg,
    activation_candidates: activation,
    cvent_sample: cventSample,
    legacy_sample: legacySample,
    run_dir: runDir,
    artifact_root: artifactRoot,
    observability: buildObservability(processed, failed, started, runId),
  };

  await writeDeepMexicoArtifacts(result, { root: ROOT, log });

  saveResumeState(runDir, {
    run_id: runId,
    mode: "live_deep_mexico",
    group: "IHG,Hilton,Choice",
    country: "Mexico",
    dry_run: true,
    total_in_scope: records.length,
    completed: completedIds.length,
    failed: failed.length,
    remaining: 0,
    completed_entity_ids: completedIds,
    failed_entities: failed,
    source_failures: sourceFailures,
    research_checkpoints: researchCheckpoints,
    observability_snapshot: result.observability,
  });

  log(
    `[v1.1] DONE researched=${processed.length} mat=${result.observability.material_completeness_avg_after}% prod=${result.observability.production_candidates} ms=${result.observability.runtime_ms}`
  );
  return result;
}

function valueOf(live, field) {
  const f = (live.fields || []).find((x) => x.field === field);
  return f?.researched_value ?? null;
}

function estimateMaterialPct(live, prior) {
  const material = [
    "Property Name",
    "Current Brand",
    "Brand Family",
    "Affiliation Status",
    "Country",
    "City",
    "Official Property URL",
    "Property Identity Key",
    "Rooms / Keys",
    "Address",
    "Phone",
    "Latitude",
    "Longitude",
    "Opening Date",
    "Operator / Management Company",
    "Owner Name",
    "Amenities - Source Text",
  ];
  const by = new Map((live.fields || []).map((f) => [f.field, f]));
  let ok = 0;
  let n = 0;
  for (const name of material) {
    n += 1;
    const f = by.get(name);
    if (
      f &&
      [
        FIELD_RESOLUTION_STATUS.VERIFIED,
        FIELD_RESOLUTION_STATUS.CONFIRMED_EXISTING,
        FIELD_RESOLUTION_STATUS.MISSING_FOUND,
        FIELD_RESOLUTION_STATUS.DERIVED,
        FIELD_RESOLUTION_STATUS.SUPERSEDED,
      ].includes(f.resolution_status)
    ) {
      ok += 1;
    }
  }
  const livePct = Math.round((100 * ok) / n);
  return Math.max(livePct, Number(prior.material_pct) || 0);
}

function classifyFromV1Baseline(rec) {
  const m = Number(rec.material_pct) || 0;
  const c = Number(rec.core_pct) || 0;
  if (c >= 90 && m >= 70) return OUTPUT_CLASS.VERIFIED_PRODUCTION_CANDIDATE;
  if (c >= 90 && m >= 50) return OUTPUT_CLASS.VERIFIED_MATERIAL_REMEDIATION;
  if (c >= 80) return OUTPUT_CLASS.PARTIAL_NONCRITICAL;
  return OUTPUT_CLASS.DEEP_RESEARCH_REQUIRED;
}

function compactHotel(h) {
  return {
    id: h.independent_record_id,
    family: h.family,
    brand: h.brand,
    name: h.name,
    output_class: h.output_class,
    material_before: h.material_pct_before,
    material_after: h.material_pct_after,
    resolved: h.fields_resolved,
    page_ok: h.page_ok,
    lane_b_ok: h.lane_b_ok,
    effort: h.research_effort_score,
  };
}

function avg(nums) {
  if (!nums.length) return 0;
  return Math.round(nums.reduce((a, b) => a + b, 0) / nums.length);
}

function buildObservability(processed, failed, started, runId) {
  const classes = {};
  for (const h of processed) {
    classes[h.output_class] = (classes[h.output_class] || 0) + 1;
  }
  return {
    run_id: runId,
    hotels_discovered: 365,
    hotels_deeply_researched: processed.length,
    hotels_failed: failed.length,
    material_completeness_avg_before: V1_BASELINE.material_completeness_avg,
    material_completeness_avg_after: avg(processed.map((h) => h.material_pct_after)),
    production_candidates: classes[OUTPUT_CLASS.VERIFIED_PRODUCTION_CANDIDATE] || 0,
    material_remediation: classes[OUTPUT_CLASS.VERIFIED_MATERIAL_REMEDIATION] || 0,
    partial: classes[OUTPUT_CLASS.PARTIAL_NONCRITICAL] || 0,
    deep_research: classes[OUTPUT_CLASS.DEEP_RESEARCH_REQUIRED] || 0,
    output_class_counts: classes,
    page_ok_count: processed.filter((h) => h.page_ok).length,
    lane_b_ok_count: processed.filter((h) => h.lane_b_ok).length,
    source_blocked_count: processed.filter((h) => h.source_blocked).length,
    operator_resolved: processed.filter((h) => valueOf(h, "Operator / Management Company")).length,
    owner_resolved: processed.filter((h) => valueOf(h, "Owner Name")).length,
    rooms_resolved: processed.filter((h) => valueOf(h, "Rooms / Keys")).length,
    external_cost_usd: 0,
    runtime_ms: Date.now() - started,
    constraints: AUTOPILOT_V1_CONSTRAINTS,
  };
}

async function runCventChallengeSample({ independentRecords, limit, delayMs, log }) {
  const harvest = findMexicoCventHarvest(ROOT);
  const urls = harvest ? loadCventHarvestUrls(harvest) : [];
  // Prefer unmatched via challenge builder first
  const built = buildCventDiscoveryChallenges(urls.slice(0, 800), independentRecords, {
    maxChallenges: 800,
  });
  const unmatched = (built.challenges || [])
    .filter((c) => c.challenge_type === "INDEPENDENT DISCOVERY CHALLENGE")
    .slice(0, limit);

  const outcomes = [];
  for (const chal of unmatched) {
    // Independent rediscovery: try to find official brand page via name hint ONLY for steward matching —
    // We do NOT fetch Cvent venue pages or use Cvent field values.
    // Instead: search within independent Census by fuzzy name; if no match, mark insufficient evidence
    // (cannot invent a hotel from a Cvent slug alone without Lane A discovery).
    const hint = chal.candidate_name_hint_for_steward_only;
    const cand = {
      cvent_candidate_id: chal.cvent_candidate_id,
      candidate_origin_reference: "cvent_latam_harvest",
      name_hint_steward_only: hint,
      cvent_used_as_source: false,
      legacy_used_as_source: false,
      include_cvent_values_in_research: false,
      independent_confirmation_status: "insufficient_evidence",
      outcome: "insufficient_evidence",
      note: "Autonomous rediscovery requires Lane A directory coverage or explicit official URL — Cvent slug alone is not production evidence",
    };

    // Check if name appears in independent universe (duplicate of verified census)
    const n = String(hint || "").toLowerCase();
    const hit = (independentRecords || []).find((r) => {
      const rn = String(r.name || "").toLowerCase();
      return n && rn && (rn.includes(n.slice(0, 12)) || n.includes(rn.slice(0, 12)));
    });
    if (hit) {
      cand.outcome = "duplicate_of_verified_census";
      cand.independent_confirmation_status = "duplicate";
      cand.independent_record_id = hit.independent_record_id;
      cand.note = "Name-hint overlap with VIC — bookkeeping only; no Cvent fields adopted";
    }

    // Optional: if hint looks like a known brand family hotel, mark as discovery challenge retained
    if (!hit && /hilton|holiday inn|crowne|indigo|comfort|quality|sleep inn|radisson/i.test(n)) {
      cand.outcome = "independent_discovery_challenge_retained";
      cand.independent_confirmation_status = "not_confirmed_pending_directory_sweep";
      cand.note =
        "Likely branded hotel not in IHG/Hilton/Choice VIC freeze — escalate to family directory discovery (non-Cvent)";
    }

    outcomes.push(cand);
    if (delayMs) await sleep(5); // no network in this path
  }

  const counts = {};
  for (const o of outcomes) counts[o.outcome] = (counts[o.outcome] || 0) + 1;

  log(
    `[v1.1] Cvent sample done n=${outcomes.length} outcomes=${JSON.stringify(counts)}`
  );

  return {
    version: "census-autopilot-v1.1-cvent-challenge-sample",
    mexico_harvest_path: harvest,
    mexico_urls_total: urls.length,
    sample_size: outcomes.length,
    cvent_used_as_source: false,
    outcome_counts: counts,
    outcomes,
  };
}

function buildLegacyChallengeSample(overlap, processed) {
  const families = [];
  let totalLegacyOnly = 0;
  for (const [family, stats] of Object.entries(overlap || {})) {
    if (!["IHG", "Hilton", "Choice"].includes(family)) continue;
    const legacyOnly = stats.legacy_only || 0;
    totalLegacyOnly += legacyOnly;
    families.push({
      family,
      legacy_only: legacyOnly,
      independent_only: stats.independent_only || 0,
      exact_matches: stats.exact_matches || 0,
      rediscovery_policy: "strict_independent — never copy legacy values",
      coverage_improvement_signal:
        (stats.independent_only || 0) > 0
          ? "Independent discovery already exceeds or complements legacy for this family"
          : "Need more Lane A discovery",
      legacy_used_as_source: false,
    });
  }
  return {
    version: "census-autopilot-v1.1-legacy-challenge-sample",
    legacy_used_as_source: false,
    total_legacy_only_ihg_hilton_choice: totalLegacyOnly,
    families,
    note: "Bounded sample uses prior VIC overlap freeze — no legacy field values ingested into live claims",
    hotels_in_live_run: processed.length,
  };
}

function writeGapMap(artifactRoot) {
  const md = `# Autopilot V1.1 — Existing Capability Gap Map

## Already exists (reuse — do not rebuild)

| Capability | Location |
|---|---|
| Mode registry / field routing / priority / resume | \`census-autopilot-v1/*\` |
| Field contract (63 researchable) | \`production-census-field-contract-v111.js\` |
| Field research plans | \`clean-census/field-research.js\` |
| Family directory adapters (Hilton/Choice/IHG signals) | \`census-autopilot-family-directory-adapters.js\` |
| Official page deep extract | \`extractDeepOfficialPageSignals\` |
| IHG amenity extract | \`ihg-hotel-amenities-extract.js\` |
| IHG / Hilton adapters | \`adapters/ihg.js\`, \`adapters/hilton.js\` |
| Property identity / temporal / firewall | \`clean-census/*\` |
| Cvent/legacy quarantine adapters | \`census-autopilot-v1/challenge-adapters.js\` |
| Completeness / output classes | \`completeness.js\`, \`output-classes.js\` |

## Gap that blocked “research depth” in V1

| Gap | Impact | V1.1 fix |
|---|---|---|
| Orchestrator only read VIC compact index — **no live HTTP** | Completeness frozen at freeze-time | \`live-deep-research.js\` fetches official pages |
| Lane B not auto-triggered when directory lacks rooms/operator | Premature escalation | Ladder Level 3 standalone website |
| Hard fields not attacked systematically | 248 stuck in remediation | Per-field ladder + hard-field stats |
| Effort / stop levels not tracked per field | Could not prove autonomous stop | \`resolution_level\`, \`research_effort_score\` |
| Image rights collapsed data class (fixed in V1) | Mis-classification | Keep separate |

## Explicitly NOT built

- No new identity engine, steward queue, source registry, field registry, activation engine, or image engine
- No DataForSEO / paid geocoding / Webhound
- No Airtable writes
`;
  fs.mkdirSync(artifactRoot, { recursive: true });
  fs.writeFileSync(path.join(artifactRoot, "01-existing-capability-gap-map.md"), md, "utf8");
}

export { V1_BASELINE, ARTIFACT_DIR };
