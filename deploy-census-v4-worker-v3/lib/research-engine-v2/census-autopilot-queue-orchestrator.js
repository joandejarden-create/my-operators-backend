/**
 * Census Autopilot multi-queue orchestrator.
 *
 * Default (no --queue): execute all eligible queues in fastest-safe order.
 * Targeted (--queue X): execute only that queue.
 *
 * Exhausted / soft-deferred queues do not stop the run.
 * Controlled mode: proposals only — never writes Airtable.
 */

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import { QUEUE_ORDER } from "./census-autopilot-queue-router.js";
import {
  parseDescArgs,
  runDescriptionExtractionDryRun,
} from "./production-census-description-extraction.js";
import { runLane2DryRun } from "./production-census-population-lane-2.js";
import { runRoomsKeysQueueDryRun } from "./production-census-rooms-keys-queue.js";
import { runPropertyNameCleanupQueueDryRun } from "./production-census-property-name-cleanup-queue.js";
import {
  parseAddressGeocodeArgs,
  runAddressGeocodeDryRun,
} from "./production-census-address-geocode-resolver.js";
import { AUTOPILOT_ALLOWED_WRITE_FIELDS } from "./census-autopilot-field-allowlist.js";
import { recommendApplyFromYield } from "./census-autopilot-source-yield-diagnostic.js";
import { warmFamilyDirectoryCaches } from "./census-autopilot-family-directory-adapters.js";
import {
  runSourceDiscoveryControlled,
  SOURCE_DISCOVERY_QUEUE_ID,
} from "./census-autopilot-source-discovery.js";
import {
  KEY_FIELD_COMPLETION_QUEUE_ID,
  runKeyFieldCompletionQueue,
  KEY_FIELD_MATRIX,
} from "./census-autopilot-key-field-completion.js";
import {
  COORDINATE_COMPLETION_QUEUE_ID,
  runCoordinateCompletionQueue,
} from "./census-coordinate-completion.js";
import {
  CORE_IDENTITY_QUALITY_QUEUE_ID,
  CITY_STATE_NORMALIZATION_QUEUE_ID,
  runCoreIdentityQualityGate,
} from "./census-data-quality-gate.js";
import {
  CLEAN_CORE_CLASSIFICATION_QUEUE_ID,
  CORE_IDENTITY_SOURCE_LOOKUP_QUEUE_ID,
  runMapContactSizeReadinessAudit,
  runCoreIdentitySourceLookup,
  evaluateCleanCorePass,
} from "./census-map-contact-size-readiness.js";
import {
  buildMarriottPropertyUrlCityProposals,
  filterProposalsToMarriottParent,
  isMarriottCensusRecord,
} from "./census-marriott-property-url-city-backfill.js";
import {
  PHONE_NUMBER_QUEUE_ID,
  PHONE_FIELD,
  buildPhoneEnrichmentProposals,
  runPhoneEnrichmentQueueDryRun,
} from "./census-phone-number-enrichment.js";
import {
  auditAllCoreIdentityIssues,
  buildUrlSlugCityProposals,
  CLEAN_CORE_QUEUE_ALIASES,
} from "./census-clean-core-identity-repair.js";
import {
  MARKET_GEOGRAPHY_QUEUE_ID,
  runMarketGeographyCompletionQueue,
} from "./census-market-submarket-classifier.js";
import {
  BRAND_NORMALIZATION_QUEUE_ID,
  runBrandNormalizationQueue,
  writeBrandNormalizationReports,
} from "./census-brand-normalization.js";
import {
  PARENT_COMPANY_NORMALIZATION_QUEUE_ID,
  runParentCompanyNormalizationQueue,
  writeParentCompanyNormalizationReports,
} from "./census-parent-company-normalization.js";

export const QUEUE_ORCHESTRATOR_VERSION = "census-autopilot-queue-orchestrator-v2";

/** Queues that never produce Autopilot production proposals in this orchestrator. */
export const NON_EXECUTABLE_QUEUES = Object.freeze([
  "steward_webhound_hard_cases",
]);

/** Field sets used to split / attribute High proposals by queue. */
export const QUEUE_FIELD_SETS = Object.freeze({
  brand_normalization: [
    MAP_FIRST_PASS.currentBrand,
    MAP_FIRST_PASS.brandFamily,
    "Data Confidence Tier",
    MAP_FIRST_PASS.humanReview,
    MAP_FIRST_PASS.enrichmentStatus,
    MAP_FIRST_PASS.enrichmentPriority,
    MAP_FIRST_PASS.lastReviewed,
    MAP_FIRST_PASS.publicDisplayReviewStatus,
    MAP_FIRST_PASS.radarDisplayStatus,
    MAP_FIRST_PASS.radarDisplayReason,
  ],
  parent_company_normalization: [
    MAP_FIRST_PASS.brandFamily,
    "Data Confidence Tier",
    MAP_FIRST_PASS.humanReview,
    MAP_FIRST_PASS.enrichmentStatus,
    MAP_FIRST_PASS.enrichmentPriority,
    MAP_FIRST_PASS.lastReviewed,
    MAP_FIRST_PASS.publicDisplayReviewStatus,
    MAP_FIRST_PASS.radarDisplayStatus,
    MAP_FIRST_PASS.radarDisplayReason,
  ],
  description_extraction: [
    MAP_FIRST_PASS.descriptionSource,
    MAP_FIRST_PASS.descriptionAi,
  ],
  amenities_extraction: [
    MAP_FIRST_PASS.amenitiesSource,
    MAP_FIRST_PASS.amenitiesTags,
    MAP_FIRST_PASS.flagFb,
    MAP_FIRST_PASS.flagMeeting,
    MAP_FIRST_PASS.flagResort,
    MAP_FIRST_PASS.flagExtendedStay,
    MAP_FIRST_PASS.flagMixedUse,
    MAP_FIRST_PASS.flagResidences,
  ],
  radar_public_readiness: [
    MAP_FIRST_PASS.radarDisplayStatus,
    MAP_FIRST_PASS.radarDisplayReason,
    MAP_FIRST_PASS.radarGeographyStatus,
    MAP_FIRST_PASS.publicCensusEligibility,
    MAP_FIRST_PASS.publicDisplayConfidence,
    MAP_FIRST_PASS.publicDisplayReviewStatus,
  ],
  address_confirmation: [
    MAP_FIRST_PASS.address,
    MAP_FIRST_PASS.addressConfidence,
    MAP_FIRST_PASS.addressSourceUrl,
    "City",
    "State / Region",
    "Country",
  ],
  property_name_cleanup: [MAP_FIRST_PASS.propertyName],
  property_type_asset_context: [
    MAP_FIRST_PASS.propertyType,
    MAP_FIRST_PASS.assetContext,
    MAP_FIRST_PASS.marketSubmarket,
    "Market",
    "Submarket",
  ],
  rooms_keys: [
    "Rooms / Keys",
    "Rooms Confidence",
    "Rooms Source URL",
    "Rooms Source Type",
    "Rooms Reviewed Date",
    "Rooms Notes",
  ],
  coordinate_resolution: [
    MAP_FIRST_PASS.latitude,
    MAP_FIRST_PASS.longitude,
    MAP_FIRST_PASS.coordinateSourceType,
    MAP_FIRST_PASS.coordinateConfidence,
    MAP_FIRST_PASS.geocodeProvider,
    MAP_FIRST_PASS.geocodeMethod,
    MAP_FIRST_PASS.geocodeReviewedDate,
  ],
  coordinate_completion: [
    MAP_FIRST_PASS.latitude,
    MAP_FIRST_PASS.longitude,
    MAP_FIRST_PASS.coordinateSourceType,
    MAP_FIRST_PASS.coordinateConfidence,
    MAP_FIRST_PASS.geocodeProvider,
    MAP_FIRST_PASS.geocodeMethod,
    MAP_FIRST_PASS.geocodeReviewedDate,
    MAP_FIRST_PASS.radarGeographyStatus,
    MAP_FIRST_PASS.radarDisplayStatus,
    MAP_FIRST_PASS.radarDisplayReason,
    MAP_FIRST_PASS.publicCensusEligibility,
    MAP_FIRST_PASS.publicDisplayConfidence,
    MAP_FIRST_PASS.publicDisplayReviewStatus,
    MAP_FIRST_PASS.lastReviewed,
    MAP_FIRST_PASS.enrichmentStatus,
  ],
  key_field_completion: [
    ...new Set([
      ...KEY_FIELD_MATRIX.map((f) => f.airtable),
      MAP_FIRST_PASS.officialUrl,
      MAP_FIRST_PASS.canonicalPropertyName,
    ]),
  ],
  core_identity_quality: [
    MAP_FIRST_PASS.propertyName,
    MAP_FIRST_PASS.canonicalPropertyName,
    MAP_FIRST_PASS.currentBrand,
    MAP_FIRST_PASS.city,
    MAP_FIRST_PASS.stateRegion,
    MAP_FIRST_PASS.country,
  ],
  city_state_normalization: [MAP_FIRST_PASS.city, MAP_FIRST_PASS.stateRegion],
  phone_number_enrichment: [PHONE_FIELD],
  clean_core_classification: [],
  core_identity_source_lookup: [],
  market_geography_completion: [
    "Continent",
    "Sub-Continent",
    "Market",
    "Submarket",
    "Data Confidence Tier",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
  ],
});

/**
 * Resolve which queue IDs to attempt.
 * @param {{
 *   orderedQueueIds?: string[],
 *   targetedQueue?: string|null,
 *   targetedQueues?: string[]|null,
 *   cleanupExistingOnly?: boolean,
 * }} opts
 */
export function resolveQueuesToExecute(opts = {}) {
  if (opts.targetedQueues?.length) {
    return [...new Set(opts.targetedQueues)];
  }
  if (opts.targetedQueue) {
    // Support comma-separated in a single --queue string
    if (String(opts.targetedQueue).includes(",")) {
      return String(opts.targetedQueue)
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    }
    return [opts.targetedQueue];
  }
  const ordered =
    opts.orderedQueueIds?.length > 0
      ? opts.orderedQueueIds
      : QUEUE_ORDER.map((q) => q.id);
  return ordered.filter((id) => !NON_EXECUTABLE_QUEUES.includes(id));
}

/**
 * Pick allowlisted patch fields for a queue.
 * @param {Record<string, unknown>} patch
 * @param {string[]} fieldSet
 */
export function pickQueuePatch(patch = {}, fieldSet = []) {
  /** @type {Record<string, unknown>} */
  const out = {};
  for (const f of fieldSet) {
    if (patch[f] == null || patch[f] === "") continue;
    if (!AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(f)) continue;
    out[f] = patch[f];
  }
  return out;
}

/**
 * Normalize a High-confidence Autopilot proposal.
 */
export function toHighAutopilotProposal(row, queue, patch, extras = {}) {
  const keys = Object.keys(patch || {});
  if (!keys.length) return null;
  return {
    record_id: row.record_id || row.id,
    identity_key: row.identity_key || null,
    property_name: row.property_name || null,
    brand: row.brand || row.current_brand || null,
    family: row.family || row.brand_family || null,
    queue,
    action: "propose_high_write",
    confidence: "High",
    write_allowed_now: true,
    patch,
    current_fields: row.current_fields || row.before || {},
    source_url: row.source_url || extras.source_url || null,
    method: row.method || extras.method || null,
    notes: row.notes || extras.notes || null,
    ...extras,
  };
}

function isHighConfidence(value) {
  return String(value || "") === "High";
}

/**
 * Build approval-bundle payload from orchestration + run context.
 */
export function buildMultiQueueApprovalBundle(ctx = {}) {
  const proposals = ctx.proposals || [];
  const byQueue = {};
  const fields = new Set();
  for (const p of proposals) {
    const q = p.queue || "unknown";
    if (!byQueue[q]) byQueue[q] = [];
    byQueue[q].push({
      record_id: p.record_id,
      identity_key: p.identity_key,
      property_name: p.property_name,
      confidence: p.confidence,
      patch_fields: Object.keys(p.patch || {}),
      patch: p.patch,
    });
    for (const f of Object.keys(p.patch || {})) fields.add(f);
  }

  const blocked = ctx.blocked || [];
  const blockedByReason = {};
  for (const b of blocked) {
    const reason = b.block_reason || b.blocked_reason || "unknown";
    if (!blockedByReason[reason]) blockedByReason[reason] = [];
    blockedByReason[reason].push({
      record_id: b.record_id,
      identity_key: b.identity_key,
      queue: b.queue || null,
      reason,
    });
  }

  const runId = ctx.run_id || null;
  const batchSize = ctx.batch_size || 250;
  const region = ctx.region || "CALA";

  return {
    version: QUEUE_ORCHESTRATOR_VERSION,
    status: "awaiting_founder_approval",
    stop_before_writes: true,
    airtable_writes: false,
    brand_explorer_writes: false,
    brand_setup_writes: false,
    run_id: runId,
    mode: ctx.mode || "controlled",
    scope: ctx.scope || "active-brand-setup",
    region,
    strategy: ctx.strategy || "fastest-safe",
    queues_executed: ctx.queues_executed || [],
    queues_skipped: ctx.queues_skipped || [],
    queues_soft_deferred: ctx.queues_soft_deferred || [],
    records_proposed: proposals.length,
    fields_proposed: [...fields],
    proposed_writes_by_queue: byQueue,
    proposed_writes: proposals.map((p) => ({
      record_id: p.record_id,
      identity_key: p.identity_key,
      property_name: p.property_name,
      queue: p.queue,
      confidence: p.confidence,
      patch: p.patch,
      patch_fields: Object.keys(p.patch || {}),
    })),
    blocked_records_by_reason: blockedByReason,
    steward_review_cases: ctx.steward || [],
    webhound_candidates: ctx.webhound || { candidates: [], capped_at: 25 },
    safety_status: {
      controlled_mode: true,
      production_writes: false,
      owner_operator_date_blocked: true,
      company_validated_blocked: true,
      brand_verified_blocked: true,
      recent_momentum_blocked: true,
      brand_explorer_untouched: true,
      brand_setup_read_only: true,
    },
    apply_recommendation: recommendApplyFromYield({
      high_proposals: proposals.length,
      safety_ok: true,
    }),
    recommended_apply_command: [
      `npm run census:autopilot -- --region ${region} --scope active-brand-setup --mode apply`,
      `--strategy fastest-safe --run-until-complete --batch-size ${batchSize}`,
      `--approval-bundle reports/research-engine-v2/autopilot/${runId || "<run-id>"}/approval-bundle.json`,
      `--confirm-safe-writes --confirm-write-to-production-census`,
      `--confirm-no-brand-explorer-writes --confirm-no-owner-operator`,
      `--confirm-no-date-writes --confirm-no-recent-momentum`,
      `--confirm-no-company-validation --confirm-webhound-not-production-source`,
      `--confirm-approval-bundle-bound --enable-production-writes`,
    ].join(" "),
    apply_would_require: [
      "--mode apply",
      "--run-until-complete",
      "--confirm-safe-writes",
      "--confirm-write-to-production-census",
      "--confirm-no-brand-explorer-writes",
      "--confirm-no-owner-operator",
      "--confirm-no-date-writes",
      "--confirm-no-recent-momentum",
      "--confirm-no-company-validation",
      "--confirm-webhound-not-production-source",
      "--confirm-approval-bundle-bound",
      "ALLOW_CENSUS_AUTOPILOT_APPLY=1",
      "CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1",
      "CONFIRM_NO_BRAND_EXPLORER_WRITES=1",
      "CONFIRM_NO_OWNER_OPERATOR_WRITES=1",
    ],
    forbidden_fields_note:
      "Owner/operator/developer/date, Recent Momentum, Company Validated, Brand Verified blocked",
    allowed_fields: AUTOPILOT_ALLOWED_WRITE_FIELDS,
  };
}

/**
 * Render queue-execution report markdown.
 */
export function renderQueueExecutionMarkdown(report) {
  const rows = (report.queue_results || [])
    .map(
      (r) =>
        `| ${r.queue_id} | ${r.status} | ${r.high_proposals ?? 0} | ${r.eligible_scanned ?? "—"} | ${(r.notes || []).join("; ") || "—"} |`
    )
    .join("\n");
  return [
    `# Queue Execution Report`,
    ``,
    `- **Run:** ${report.run_id || "(pending)"}`,
    `- **Mode:** ${report.mode}`,
    `- **Strategy:** ${report.strategy}`,
    `- **Targeted queue:** ${report.targeted_queue || "(none — full fastest-safe plan)"}`,
    `- **Airtable writes:** false`,
    ``,
    `## Results`,
    ``,
    `| Queue | Status | High proposals | Eligible scanned | Notes |`,
    `| --- | --- | ---: | --- | --- |`,
    rows || `| — | — | 0 | — | no queues |`,
    ``,
    `## Summary`,
    ``,
    `- Executed: ${(report.queues_executed || []).join(", ") || "(none)"}`,
    `- Skipped: ${(report.queues_skipped || []).join(", ") || "(none)"}`,
    `- Soft-deferred: ${(report.queues_soft_deferred || []).join(", ") || "(none)"}`,
    `- Total High proposals: ${report.total_high_proposals ?? 0}`,
    `- Runtime ms: ${report.runtime_ms ?? "n/a"}`,
    ``,
  ].join("\n");
}

/**
 * Execute Autopilot queues in order.
 *
 * @param {{
 *   orderedQueueIds?: string[],
 *   targetedQueue?: string|null,
 *   limit?: number,
 *   fetchLimit?: number,
 *   parentCompany?: string|null,
 *   geocodeProviderReady?: boolean,
 *   schemaV114Ready?: boolean,
 *   forAutopilot?: boolean,
 *   censusRecords?: object[],
 *   region?: string,
 *   log?: (msg: string) => void,
 * }} opts
 */
export async function orchestrateAutopilotQueues(opts = {}) {
  const started = Date.now();
  const log = opts.log || ((msg) => console.log(msg));
  const targeted = opts.targetedQueue || null;
  const targetedQueuesRaw = opts.targetedQueues || null;
  let queueIds = resolveQueuesToExecute({
    orderedQueueIds: opts.orderedQueueIds,
    targetedQueue: targeted,
    targetedQueues: targetedQueuesRaw,
  });
  if (opts.cleanupExistingOnly || targetedQueuesRaw?.length || String(targeted || "").includes(",")) {
    const resolved = [];
    const seen = new Set();
    for (const id of queueIds) {
      const canon = CLEAN_CORE_QUEUE_ALIASES[id] || id;
      if (seen.has(canon)) continue;
      seen.add(canon);
      resolved.push(canon);
    }
    queueIds = resolved;
  }
  const limit = opts.limit ?? 10000;
  const fetchLimit =
    opts.fetchLimit ??
    (Number(process.env.AUTOPILOT_DESC_FETCH_LIMIT || 0) || Math.min(limit, 80));
  const geocodeReady = Boolean(opts.geocodeProviderReady);
  const forAutopilot = opts.forAutopilot !== false;

  // Prefetch Hilton locations + Choice regional directories before queue work
  try {
    log(`[orchestrator] warming family directory adapters (Hilton + Choice Mexico)…`);
    const warm = await warmFamilyDirectoryCaches({ delayMs: 100 });
    log(
      `[orchestrator] family directories ready hilton=${warm.hilton_count} choice=${warm.choice_count} errors=${warm.errors?.length || 0}`
    );
  } catch (err) {
    log(`[orchestrator] family directory warm failed (continuing): ${err?.message || err}`);
  }

  /** @type {any} */
  const cache = {
    descriptionReport: null,
    lane2Report: null,
    roomsReport: null,
    nameCleanupReport: null,
    addressReport: null,
    discoveryReport: null,
  };

  /** @type {object[]} */
  const allProposals = [];
  /** @type {object[]} */
  const allBlocked = [];
  /** @type {object[]} */
  const queueResults = [];
  const executed = [];
  const skipped = [];
  const softDeferred = [];

  async function ensureDescription() {
    if (cache.descriptionReport) return cache.descriptionReport;
    log(`[orchestrator] description_extraction dry-run (fetchLimit=${fetchLimit})…`);
    const args = { ...parseDescArgs([]), fetchLimit, dryRun: true, apply: false };
    cache.descriptionReport = await runDescriptionExtractionDryRun(args);
    return cache.descriptionReport;
  }

  async function ensureLane2() {
    if (cache.lane2Report) return cache.lane2Report;
    log(`[orchestrator] lane-2 dry-run (amenities/radar/property-type)…`);
    cache.lane2Report = await runLane2DryRun();
    return cache.lane2Report;
  }

  async function ensureRooms() {
    if (cache.roomsReport) return cache.roomsReport;
    const roomsCap = forAutopilot
      ? Math.min(
          Number(process.env.AUTOPILOT_ROOMS_FETCH_LIMIT || 200) || 200,
          Math.max(50, Math.min(limit, 200))
        )
      : limit;
    log(`[orchestrator] rooms_keys dry-run (limit=${roomsCap})…`);
    cache.roomsReport = await runRoomsKeysQueueDryRun({
      limit: roomsCap,
      parentCompany: opts.parentCompany,
      forAutopilot,
    });
    return cache.roomsReport;
  }

  async function ensureNameCleanup() {
    if (cache.nameCleanupReport) return cache.nameCleanupReport;
    log(`[orchestrator] property_name_cleanup dry-run (limit=${limit})…`);
    cache.nameCleanupReport = await runPropertyNameCleanupQueueDryRun({
      limit,
      forAutopilot,
    });
    return cache.nameCleanupReport;
  }

  async function ensureAddress() {
    if (cache.addressReport) return cache.addressReport;
    // Mission completion needs a higher fetch budget; still hard-capped to avoid hang.
    // Prefer AUTOPILOT_ADDRESS_FETCH_LIMIT when set — do not clamp it down by DESC fetchLimit.
    const envAddressCap = Number(process.env.AUTOPILOT_ADDRESS_FETCH_LIMIT || 0);
    const fetchCap = forAutopilot
      ? Math.min(500, envAddressCap > 0 ? envAddressCap : Math.max(300, fetchLimit || 300))
      : Math.min(80, Math.max(30, fetchLimit));
    log(
      `[orchestrator] address_confirmation dry-run (fetchLimit=${fetchCap}, geocodeLimit=0, address-only High OK)…`
    );
    const args = {
      ...parseAddressGeocodeArgs([]),
      dryRun: true,
      apply: false,
      fetchLimit: fetchCap,
      geocodeLimit: 0,
      forAutopilot: true,
      delayMs: forAutopilot ? 100 : undefined,
      perRecordTimeoutMs: forAutopilot ? 12000 : undefined,
    };
    cache.addressReport = await runAddressGeocodeDryRun(args);
    return cache.addressReport;
  }

  function proposalsFromDescription(queueId) {
    const report = cache.descriptionReport;
    const fieldSet = QUEUE_FIELD_SETS[queueId] || [];
    const out = [];
    for (const p of report?.proposals || []) {
      if (p.action !== "propose_update") {
        if (p.blocked_reason || p.action) {
          allBlocked.push({
            ...p,
            queue: queueId,
            block_reason: p.blocked_reason || p.action,
          });
        }
        continue;
      }
      // Description extractor marks High/Medium; Autopilot would-writes require High
      if (queueId === "description_extraction") {
        if (!isHighConfidence(p.extraction_meta?.description_confidence)) continue;
      } else if (queueId === "amenities_extraction") {
        const amenSrc = (p.sources || []).find(
          (s) => s.lane === "amenities" || s.fields?.includes?.(MAP_FIRST_PASS.amenitiesSource)
        );
        const amenConf =
          amenSrc?.confidence ||
          p.extraction_meta?.amenities_confidence ||
          (p.extraction_meta?.amenities_count > 0 ? null : null);
        if (!isHighConfidence(amenConf)) continue;
      }
      const patch = pickQueuePatch(p.patch || p.after || {}, fieldSet);
      const prop = toHighAutopilotProposal(p, queueId, patch, {
        method: p.extraction_meta?.description_method || "official_page_extraction",
        source_url: p.sources?.[0]?.source_url || p.source_url,
      });
      if (prop) out.push(prop);
    }
    return out;
  }

  function proposalsFromLane2(queueId) {
    const report = cache.lane2Report;
    const fieldSet = QUEUE_FIELD_SETS[queueId] || [];
    const out = [];
    const rows = report?.proposals || report?.proposal_index || [];
    for (const p of rows) {
      if (!p.eligible) {
        allBlocked.push({
          record_id: p.record_id,
          identity_key: p.identity_key,
          queue: queueId,
          block_reason: p.block_reason || "not_eligible",
        });
        continue;
      }
      const patch = pickQueuePatch(p.patch || {}, fieldSet);
      if (!Object.keys(patch).length) continue;
      // Prefer High from sources; allow inferred Dealality mapping when no Low signal
      const srcConfs = (p.sources || [])
        .filter((s) => (s.fields || []).some((f) => fieldSet.includes(f)))
        .map((s) => s.confidence)
        .filter(Boolean);
      const conf = srcConfs.includes("High")
        ? "High"
        : srcConfs.length === 0
          ? "High"
          : srcConfs[0];
      if (!isHighConfidence(conf)) continue;
      const prop = toHighAutopilotProposal(p, queueId, patch, {
        method: `lane2:${(p.lanes || []).join(",")}`,
      });
      if (prop) out.push(prop);
    }
    return out;
  }

  function proposalsFromRooms() {
    const report = cache.roomsReport;
    const out = [];
    for (const p of report?.proposals || []) {
      if (!(p.write_allowed_now || p.action === "propose_high_write")) {
        if (p.block_reason || p.action) {
          allBlocked.push({
            ...p,
            queue: "rooms_keys",
            block_reason: p.block_reason || p.action,
          });
        }
        continue;
      }
      if (p.confidence && p.confidence !== "High") continue;
      const patch = pickQueuePatch(p.patch || {}, QUEUE_FIELD_SETS.rooms_keys);
      const prop = toHighAutopilotProposal(p, "rooms_keys", patch);
      if (!prop) continue;
      const rec = (opts.censusRecords || []).find(
        (r) => r.id === (prop.record_id || p.record_id || p.id)
      );
      if (
        rec &&
        !evaluateCleanCorePass(rec, {
          canonicalFieldExists: opts.canonicalFieldExists !== false,
        }).pass
      ) {
        allBlocked.push({
          ...prop,
          block_reason: "clean_core_not_pass",
        });
        continue;
      }
      out.push(prop);
    }
    return out;
  }

  function proposalsFromNameCleanup() {
    const report = cache.nameCleanupReport;
    const out = [];
    for (const p of report?.proposals || []) {
      if (!(p.write_allowed_now || p.action === "propose_high_write")) {
        if (p.block_reason || p.action) {
          allBlocked.push({
            ...p,
            queue: "property_name_cleanup",
            block_reason: p.block_reason || p.action,
          });
        }
        continue;
      }
      const patch = pickQueuePatch(p.patch || {}, QUEUE_FIELD_SETS.property_name_cleanup);
      const prop = toHighAutopilotProposal(p, "property_name_cleanup", patch);
      if (prop) out.push(prop);
    }
    return out;
  }

  function proposalsFromAddress() {
    const report = cache.addressReport;
    const fieldSet = QUEUE_FIELD_SETS.address_confirmation;
    const out = [];
    const rows = report?.proposed_updates || report?.proposals || report?.results || [];
    for (const p of rows) {
      if (p.action !== "propose") continue;
      if (!isHighConfidence(p.confidence)) continue;
      const rawPatch = p.proposal || p.patch || {};
      const patch = pickQueuePatch(rawPatch, fieldSet);
      // Address queue requires Address (or confidence provenance) — skip pure coord patches
      if (!patch[MAP_FIRST_PASS.address] && !patch[MAP_FIRST_PASS.addressConfidence]) {
        continue;
      }
      const prop = toHighAutopilotProposal(
        {
          record_id: p.record_id,
          identity_key: p.identity_key,
          property_name: p.property_name,
          brand: p.brand,
          family: p.family,
          current_fields: p.current_fields || {},
        },
        "address_confirmation",
        patch,
        { method: p.report_meta?.extraction_method || "official_address" }
      );
      if (prop) out.push(prop);
    }
    return out;
  }

  for (const queueId of queueIds) {
    const notes = [];
    let status = "executed";
    let high = [];
    let eligibleScanned = null;

    try {
      if (queueId === "coordinate_completion" || queueId === COORDINATE_COMPLETION_QUEUE_ID) {
        log(`[orchestrator] coordinate_completion Mapbox Permanent…`);
        const coord = await runCoordinateCompletionQueue({
          censusRecords: opts.censusRecords || [],
          env: opts.env || process.env,
          fetchImpl: opts.fetchImpl,
          writeReports: opts.writeCoordinateReports !== false,
          runDir: opts.runDir || null,
          log,
          activeBrandScope: opts.activeBrandScope !== false,
        });
        cache.coordinateCompletionReport = coord;
        if (!geocodeReady || !coord.provider_readiness?.ready) {
          status = "soft_deferred";
          notes.push("mapbox_permanent_or_completion_flag_missing");
          notes.push(
            `provider_decision=${(coord.provider_decision_needed || []).length}`
          );
          softDeferred.push(queueId);
          queueResults.push({
            queue_id: queueId,
            status,
            high_proposals: 0,
            eligible_scanned: coord.counters?.records_eligible_for_mapbox ?? 0,
            notes,
            coordinate_status: coord.status,
          });
          log(`[orchestrator] ${queueId}: soft_deferred (${notes.join(", ")})`);
          continue;
        }
        high = (coord.proposals || []).map((p) => ({
          ...p,
          queue: COORDINATE_COMPLETION_QUEUE_ID,
        }));
        eligibleScanned = coord.counters?.records_eligible_for_mapbox ?? 0;
        notes.push(
          `status=${coord.status}`,
          `mapbox_requests=${coord.counters?.mapbox_requests ?? 0}`,
          `steward=${(coord.steward_review || []).length}`,
          `provider_decision=${(coord.provider_decision_needed || []).length}`
        );
      } else if (queueId === "coordinate_resolution") {
        // Legacy queue: soft-defer in favor of coordinate_completion
        status = "soft_deferred";
        notes.push("use_coordinate_completion_for_mapbox_permanent");
        softDeferred.push(queueId);
        queueResults.push({
          queue_id: queueId,
          status,
          high_proposals: 0,
          eligible_scanned: 0,
          notes,
        });
        log(`[orchestrator] ${queueId}: soft_deferred (${notes.join(", ")})`);
        continue;
      } else if (queueId === "description_extraction") {
        const report = await ensureDescription();
        eligibleScanned =
          report?.summary?.eligible ??
          report?.records_eligible ??
          (report?.proposals || []).length;
        high = proposalsFromDescription("description_extraction");
      } else if (queueId === "amenities_extraction") {
        // Prefer official-page amenities from description pass; supplement with lane-2 VIC
        await ensureDescription();
        const fromDesc = proposalsFromDescription("amenities_extraction");
        await ensureLane2();
        const fromLane = proposalsFromLane2("amenities_extraction");
        const seen = new Set(fromDesc.map((p) => p.record_id));
        high = [...fromDesc, ...fromLane.filter((p) => !seen.has(p.record_id))];
        eligibleScanned =
          (cache.descriptionReport?.proposals || []).length +
          (cache.lane2Report?.proposal_index || cache.lane2Report?.proposals || []).length;
      } else if (queueId === "radar_public_readiness") {
        await ensureLane2();
        high = proposalsFromLane2("radar_public_readiness");
        eligibleScanned = (cache.lane2Report?.proposal_index || [])
          .filter((p) => p.eligible)
          .length;
      } else if (queueId === "property_type_asset_context") {
        await ensureLane2();
        high = proposalsFromLane2("property_type_asset_context");
        eligibleScanned = (cache.lane2Report?.proposal_index || [])
          .filter((p) => p.eligible)
          .length;
      } else if (queueId === "address_confirmation") {
        await ensureAddress();
        high = proposalsFromAddress();
        eligibleScanned = (cache.addressReport?.proposed_updates || []).length;
        notes.push("geocode_calls_disabled_in_address_queue_geocodeLimit=0");
      } else if (queueId === "property_name_cleanup") {
        await ensureNameCleanup();
        high = proposalsFromNameCleanup();
        eligibleScanned =
          cache.nameCleanupReport?.summary?.eligible ??
          cache.nameCleanupReport?.summary?.processed ??
          (cache.nameCleanupReport?.proposals || []).length;
      } else if (queueId === "rooms_keys") {
        await ensureRooms();
        high = proposalsFromRooms();
        eligibleScanned =
          cache.roomsReport?.summary?.eligible ??
          cache.roomsReport?.summary?.processed ??
          (cache.roomsReport?.proposals || []).length;
      } else if (queueId === SOURCE_DISCOVERY_QUEUE_ID || queueId === "source_discovery") {
        log(`[orchestrator] source_discovery controlled (insert proposals only; no Airtable writes)…`);
        const discovery = await runSourceDiscoveryControlled({
          region: opts.region || "CALA",
          parentCompany: opts.parentCompany || null,
          censusRecords: opts.censusRecords || [],
          writeArtifacts: false,
          includeVicEvidence: true,
        });
        cache.discoveryReport = discovery;
        const insertCount = discovery.summary?.estimated_insert_count ?? 0;
        eligibleScanned = discovery.discovered_properties ?? discovery.classified?.length ?? 0;
        status = insertCount > 0 ? "executed" : "executed_exhausted";
        notes.push(
          `insert_candidates=${insertCount}`,
          `existing_exact=${discovery.summary?.existing_hotel_property_census_matches ?? 0}`,
          `status=${discovery.status}`
        );
        if (insertCount === 0) notes.push("no_high_confidence_insert_candidates");
        executed.push(queueId);
        queueResults.push({
          queue_id: queueId,
          status,
          high_proposals: 0,
          insert_candidates: insertCount,
          eligible_scanned: eligibleScanned,
          notes,
          discovery_status: discovery.status,
        });
        log(
          `[orchestrator] ${queueId}: ${status} inserts=${insertCount} discovered=${eligibleScanned ?? "n/a"}`
        );
        continue;
      } else if (
        queueId === CORE_IDENTITY_SOURCE_LOOKUP_QUEUE_ID ||
        queueId === "core_identity_source_lookup"
      ) {
        log(`[orchestrator] core_identity_source_lookup (Marriott property URL + city)…`);
        const lookup = runCoreIdentitySourceLookup({
          censusRecords: opts.censusRecords || [],
          canonicalFieldExists: opts.canonicalFieldExists !== false,
        });
        cache.coreIdentitySourceLookup = lookup;
        eligibleScanned = lookup.counters?.scanned ?? 0;
        notes.push(
          `needing_lookup=${lookup.counters?.needing_lookup ?? 0}`,
          `with_official_url=${lookup.counters?.with_official_url ?? 0}`
        );

        // Marriott Unknown city → official sitemap property URL + High slug/IATA city
        const marriottParent =
          !opts.parentCompany || /marriott/i.test(String(opts.parentCompany));
        if (marriottParent) {
          const marriott = await buildMarriottPropertyUrlCityProposals({
            censusRecords: opts.censusRecords || [],
            delayMs: opts.marriottDelayMs ?? 120,
            marriottCache: opts.marriottCache || null,
          });
          cache.marriottUrlCityBackfill = marriott;
          high = (marriott.proposals || []).map((p) => ({
            ...p,
            queue: CORE_IDENTITY_SOURCE_LOOKUP_QUEUE_ID,
          }));
          notes.push(
            `marriott_targets=${marriott.counters?.targets ?? 0}`,
            `marriott_high=${high.length}`,
            `marriott_urls=${marriott.counters?.property_urls_found ?? 0}`,
            `marriott_cities=${marriott.counters?.cities_written ?? 0}`,
            `marriott_steward=${marriott.counters?.stewarded ?? 0}`
          );
        } else {
          notes.push("marriott_backfill_skipped_parent_filter");
        }

        if (high.length === 0) {
          status = "executed_exhausted";
          notes.push("no_high_confidence_proposals");
        } else {
          status = "executed";
        }
        executed.push(queueId);
        allProposals.push(...high);
        queueResults.push({
          queue_id: queueId,
          status,
          high_proposals: high.length,
          eligible_scanned: eligibleScanned,
          notes,
        });
        log(
          `[orchestrator] ${queueId}: ${status} high=${high.length} needing=${lookup.counters?.needing_lookup ?? 0}`
        );
        continue;
      } else if (
        queueId === CLEAN_CORE_CLASSIFICATION_QUEUE_ID ||
        queueId === "clean_core_classification"
      ) {
        log(`[orchestrator] clean_core_classification + map/contact/size readiness…`);
        const liveNames = opts.liveFieldNames || null;
        const phoneFieldExists =
          opts.phoneFieldExists != null
            ? opts.phoneFieldExists
            : !liveNames || liveNames.includes(PHONE_FIELD);
        const readiness = runMapContactSizeReadinessAudit({
          censusRecords: opts.censusRecords || [],
          phoneFieldExists,
          canonicalFieldExists: opts.canonicalFieldExists !== false,
          env: opts.env || process.env,
          runDir: opts.runDir || null,
          writeReports: opts.writeMapContactSizeReports !== false,
        });
        cache.mapContactSizeReadiness = readiness;
        eligibleScanned = readiness.counters?.total_records ?? 0;
        status = "executed_exhausted";
        notes.push(
          `status=${readiness.status}`,
          `clean_core=${readiness.counters?.clean_core ?? 0}`,
          `level2=${readiness.counters?.map_contact_size_ready ?? 0}`,
          `lat_long_eligible=${readiness.counters?.lat_long_eligible ?? 0}`,
          `phone_complete=${readiness.counters?.phone_complete ?? 0}`,
          `rooms_complete=${readiness.counters?.rooms_complete ?? 0}`
        );
        executed.push(queueId);
        queueResults.push({
          queue_id: queueId,
          status,
          high_proposals: 0,
          eligible_scanned: eligibleScanned,
          notes,
          readiness_status: readiness.status,
        });
        log(
          `[orchestrator] ${queueId}: ${status} clean_core=${readiness.counters?.clean_core ?? 0}`
        );
        continue;
      } else if (
        queueId === PHONE_NUMBER_QUEUE_ID ||
        queueId === "phone_number_enrichment"
      ) {
        log(`[orchestrator] phone_number_enrichment (official sources only)…`);
        const liveNames = opts.liveFieldNames || null;
        const phoneFieldExists =
          opts.phoneFieldExists != null
            ? opts.phoneFieldExists
            : !liveNames || liveNames.includes(PHONE_FIELD);
        const phoneFetchLimit =
          Number(process.env.AUTOPILOT_PHONE_FETCH_LIMIT || 0) > 0
            ? Number(process.env.AUTOPILOT_PHONE_FETCH_LIMIT)
            : Math.min(150, Math.max(40, fetchLimit || 150));
        const phoneReport =
          Object.keys(opts.officialPhoneByRecordId || {}).length ||
          Object.keys(opts.pageHtmlByRecordId || {}).length
            ? buildPhoneEnrichmentProposals({
                censusRecords: opts.censusRecords || [],
                phoneFieldExists,
                officialPhoneByRecordId: opts.officialPhoneByRecordId || {},
                pageHtmlByRecordId: opts.pageHtmlByRecordId || {},
              })
            : await runPhoneEnrichmentQueueDryRun({
                censusRecords: opts.censusRecords || [],
                phoneFieldExists,
                fetchLimit: phoneFetchLimit,
                delayMs: forAutopilot ? 150 : 200,
                log,
              });
        cache.phoneEnrichmentReport = phoneReport;
        high = (phoneReport.proposals || []).map((p) => ({
          ...p,
          queue: PHONE_NUMBER_QUEUE_ID,
        }));
        eligibleScanned = phoneReport.counters?.records_scanned ?? 0;
        notes.push(
          `phone_field_exists=${phoneFieldExists}`,
          `complete=${phoneReport.counters?.phone_complete ?? 0}`,
          `source_available=${phoneReport.counters?.phone_source_available ?? 0}`,
          `fetch_ok=${phoneReport.counters?.fetch_ok ?? 0}/${phoneReport.counters?.fetch_attempted ?? 0}`,
          `steward=${phoneReport.counters?.steward_conflicts ?? 0}`
        );
        if (!phoneFieldExists) {
          notes.push("phone_field_missing_no_schema_create");
        }
      } else if (
        queueId === BRAND_NORMALIZATION_QUEUE_ID ||
        queueId === "brand_normalization"
      ) {
        log(`[orchestrator] brand_normalization (canonical Brand Source-of-Truth)…`);
        const brandReport = runBrandNormalizationQueue({
          censusRecords: opts.censusRecords || [],
          region: opts.region || "CALA",
          parentCompany: opts.parentCompany || null,
        });
        cache.brandNormalizationReport = brandReport;
        if (opts.writeBrandNormReports !== false) {
          try {
            writeBrandNormalizationReports(brandReport, {
              airtable_writes: false,
              mode: "controlled_orchestrator",
            });
          } catch (err) {
            notes.push(`brand_norm_report_write_failed:${err?.message || err}`);
          }
        }
        high = (brandReport.proposals || []).map((p) => ({
          ...p,
          queue: BRAND_NORMALIZATION_QUEUE_ID,
        }));
        eligibleScanned = brandReport.counters?.records_scanned ?? opts.censusRecords?.length ?? 0;
        notes.push(
          `status=${brandReport.status}`,
          `high=${high.length}`,
          `valid=${brandReport.counters?.brand_valid ?? 0}`,
          `alias=${brandReport.counters?.brand_alias_normalizable ?? 0}`,
          `misspelled=${brandReport.counters?.brand_misspelled ?? 0}`,
          `steward=${(brandReport.steward_cases || []).length}`
        );
      } else if (
        queueId === PARENT_COMPANY_NORMALIZATION_QUEUE_ID ||
        queueId === "parent_company_normalization"
      ) {
        log(`[orchestrator] parent_company_normalization (canonical Brand Family)…`);
        const parentReport = runParentCompanyNormalizationQueue({
          censusRecords: opts.censusRecords || [],
          region: opts.region || "CALA",
          parentCompany: opts.parentCompany || null,
        });
        cache.parentCompanyNormalizationReport = parentReport;
        if (opts.writeParentNormReports !== false) {
          try {
            writeParentCompanyNormalizationReports(parentReport, {
              airtable_writes: false,
              mode: "controlled_orchestrator",
            });
          } catch (err) {
            notes.push(`parent_norm_report_write_failed:${err?.message || err}`);
          }
        }
        high = (parentReport.proposals || [])
          .filter((p) => p.action === "update" || p.confidence === "High")
          .map((p) => ({
            ...p,
            queue: PARENT_COMPANY_NORMALIZATION_QUEUE_ID,
          }));
        // Prefer parent alias/blank fills over steward HR flags in apply batches
        // unless explicitly requested — steward cases remain in report.
        if (opts.includeParentStewardFlags !== true) {
          high = high.filter((p) => p.action !== "steward_flag");
        }
        eligibleScanned =
          parentReport.counters?.records_scanned ?? opts.censusRecords?.length ?? 0;
        notes.push(
          `status=${parentReport.status}`,
          `high=${high.length}`,
          `valid=${parentReport.counters?.parent_valid ?? 0}`,
          `alias=${parentReport.counters?.parent_alias_normalizable ?? 0}`,
          `blank=${parentReport.counters?.parent_blank ?? 0}`,
          `steward=${(parentReport.steward_cases || []).length}`
        );
      } else if (
        queueId === CORE_IDENTITY_QUALITY_QUEUE_ID ||
        queueId === "core_identity_quality" ||
        queueId === CITY_STATE_NORMALIZATION_QUEUE_ID ||
        queueId === "city_state_normalization"
      ) {
        log(`[orchestrator] core_identity_quality gate (city/state/canonical)…`);
        const gate = runCoreIdentityQualityGate({
          censusRecords: opts.censusRecords || [],
          runDir: opts.runDir || null,
          writeReports: opts.writeQualityGateReports !== false,
          canonicalFieldExists: opts.canonicalFieldExists !== false,
        });
        cache.coreIdentityQualityReport = gate;
        high = (gate.proposals || []).map((p) => ({
          ...p,
          queue: CORE_IDENTITY_QUALITY_QUEUE_ID,
        }));
        // High URL-slug city for Unknown/blank (official Source URL only)
        const identityAudit = auditAllCoreIdentityIssues(opts.censusRecords || [], {
          canonicalFieldExists: opts.canonicalFieldExists !== false,
        });
        const slugProps = buildUrlSlugCityProposals(identityAudit.rows);
        const existingIds = new Set(high.map((p) => p.record_id));
        for (const sp of slugProps) {
          if (existingIds.has(sp.record_id)) continue;
          high.push(sp);
        }
        if (slugProps.length) {
          notes.push(`url_slug_city_proposals=${slugProps.length}`);
        }
        eligibleScanned = gate.counters?.records_scanned ?? opts.censusRecords?.length ?? 0;
        notes.push(
          `status=${gate.status}`,
          `autofix=${gate.counters?.safe_autofix_proposals ?? 0}`,
          `unknown_city=${gate.counters?.unknown_city ?? 0}`,
          `descriptor_city=${gate.counters?.descriptor_city ?? 0}`,
          `coord_blocked=${gate.counters?.coordinate_blocked_dirty_identity ?? 0}`,
          `steward=${(gate.steward_review || []).length}`
        );
      } else if (
        queueId === MARKET_GEOGRAPHY_QUEUE_ID ||
        queueId === "market_geography_completion"
      ) {
        log(`[orchestrator] market_geography_completion (continent/subcontinent/market)…`);
        const geo = runMarketGeographyCompletionQueue({
          censusRecords: opts.censusRecords || [],
          fieldExists: {
            continent: opts.continentFieldExists === true,
            subContinent: opts.subContinentFieldExists === true,
            market: opts.marketFieldExists === true,
            submarket: opts.submarketFieldExists === true,
          },
          fillSubmarket: opts.fillSubmarket !== false,
        });
        cache.marketGeographyReport = geo;
        high = (geo.proposals || []).map((p) => ({
          ...p,
          queue: MARKET_GEOGRAPHY_QUEUE_ID,
        }));
        eligibleScanned = geo.counters?.records_scanned ?? opts.censusRecords?.length ?? 0;
        notes.push(
          `status=${geo.status}`,
          `high=${high.length}`,
          `continent=${geo.counters?.high_continent_proposals ?? 0}`,
          `subcontinent=${geo.counters?.high_subcontinent_proposals ?? 0}`,
          `market=${geo.counters?.high_market_proposals ?? 0}`,
          `submarket=${geo.counters?.high_submarket_proposals ?? 0}`,
          `steward=${geo.counters?.steward_cases ?? 0}`
        );
      } else if (queueId === KEY_FIELD_COMPLETION_QUEUE_ID || queueId === "key_field_completion") {
        log(`[orchestrator] key_field_completion matrix + High autofill proposals…`);
        const kfc = runKeyFieldCompletionQueue({
          censusRecords: opts.censusRecords || [],
          runDir: opts.runDir || null,
          writeReports: opts.writeKeyFieldReports !== false,
          env: opts.env || process.env,
          providerReady: geocodeReady,
          canonicalFieldExists: opts.canonicalFieldExists !== false,
        });
        cache.keyFieldCompletionReport = kfc;
        high = (kfc.proposals || []).map((p) => ({
          ...p,
          queue: KEY_FIELD_COMPLETION_QUEUE_ID,
        }));
        // Attach provider_decision / steward routing onto orchestration via notes + cache
        eligibleScanned = opts.censusRecords?.length ?? 0;
        notes.push(
          `matrix_status=${kfc.status}`,
          `autofill_proposals=${high.length}`,
          `provider_decision=${(kfc.provider_decision_needed || []).length}`,
          `source_adapter=${(kfc.source_adapter_needed || []).length}`
        );
        if ((kfc.provider_decision_needed || []).length) {
          notes.push("geocode_provider_blocked_coords_routed");
        }
      } else {
        status = "skipped_no_executor";
        notes.push("no_orchestrator_adapter");
        skipped.push(queueId);
        queueResults.push({
          queue_id: queueId,
          status,
          high_proposals: 0,
          eligible_scanned: 0,
          notes,
        });
        log(`[orchestrator] ${queueId}: skipped_no_executor`);
        continue;
      }

      if (high.length === 0) {
        status = "executed_exhausted";
        notes.push("no_high_confidence_proposals");
      } else {
        status = "executed";
      }
      executed.push(queueId);
      allProposals.push(...high);

      queueResults.push({
        queue_id: queueId,
        status,
        high_proposals: high.length,
        eligible_scanned: eligibleScanned,
        notes,
      });
      log(
        `[orchestrator] ${queueId}: ${status} high=${high.length} eligible=${eligibleScanned ?? "n/a"}`
      );
    } catch (err) {
      const message = err?.message || String(err);
      console.error(`[orchestrator] ${queueId} failed:`, message);
      skipped.push(queueId);
      queueResults.push({
        queue_id: queueId,
        status: "skipped_executor_error",
        high_proposals: 0,
        eligible_scanned: eligibleScanned,
        notes: [message],
      });
      // Continue to next queue — do not stop the run for a single queue failure
    }
  }

  const exhausted = queueResults
    .filter((r) => r.status === "executed_exhausted")
    .map((r) => r.queue_id);

  const runtimeMs = Date.now() - started;
  const executionReport = {
    version: QUEUE_ORCHESTRATOR_VERSION,
    mode: "controlled_or_dry_run",
    strategy: "fastest-safe",
    targeted_queue: targeted,
    queues_planned: queueIds,
    queues_executed: executed,
    queues_skipped: skipped,
    queues_soft_deferred: softDeferred,
    queues_exhausted: exhausted,
    queue_results: queueResults,
    total_high_proposals: allProposals.length,
    runtime_ms: runtimeMs,
    airtable_writes: false,
    fetch_limit_description: fetchLimit,
    reports_attached: {
      description: Boolean(cache.descriptionReport),
      lane2: Boolean(cache.lane2Report),
      rooms: Boolean(cache.roomsReport),
      property_name_cleanup: Boolean(cache.nameCleanupReport),
      address: Boolean(cache.addressReport),
      source_discovery: Boolean(cache.discoveryReport),
    },
  };

  return {
    ok: true,
    version: QUEUE_ORCHESTRATOR_VERSION,
    proposals: allProposals,
    blocked: allBlocked,
    queues_executed: executed,
    queues_skipped: skipped,
    queues_soft_deferred: softDeferred,
    queues_exhausted: exhausted,
    queue_results: queueResults,
    execution_report: executionReport,
    roomsReport: cache.roomsReport,
    nameCleanupReport: cache.nameCleanupReport,
    descriptionReport: cache.descriptionReport,
    lane2Report: cache.lane2Report,
    addressReport: cache.addressReport,
    discoveryReport: cache.discoveryReport,
    marriottUrlCityBackfill: cache.marriottUrlCityBackfill || null,
    runtime_ms: runtimeMs,
  };
}
