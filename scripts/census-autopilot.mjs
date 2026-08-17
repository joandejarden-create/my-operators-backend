#!/usr/bin/env node
/**
 * Census Autopilot CLI v2
 *
 * Default controlled (all eligible queues, fastest-safe order, no writes):
 *   npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
 *     --strategy fastest-safe --run-until-complete --batch-size 250
 *
 * Targeted queue (controlled, no writes):
 *   npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
 *     --strategy fastest-safe --queue description_extraction --run-until-complete --batch-size 250
 *
 * Safety: production Airtable writes require ALLOW_CENSUS_AUTOPILOT_APPLY=1 and
 * CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 plus all CLI confirms. Pass --enable-production-writes
 * explicitly when founder intends live apply.
 */

import "dotenv/config";
import { writeFileSync, mkdirSync } from "node:fs";
import { join, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  runCensusAutopilot,
  parseAutopilotArgs,
} from "../lib/research-engine-v2/census-autopilot-runner.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
  BLOCKED_WRONG_CENSUS_TARGET,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import {
  inspectRoomsKeysSchemaStatus,
  CENSUS_TABLE_ID,
} from "../lib/research-engine-v2/production-census-rooms-keys-queue.js";
import {
  renderPropertyNameCleanupMarkdown,
  STATUS as NAME_CLEANUP_STATUS,
} from "../lib/research-engine-v2/production-census-property-name-cleanup-queue.js";
import {
  orchestrateAutopilotQueues,
  renderQueueExecutionMarkdown,
  buildMultiQueueApprovalBundle,
} from "../lib/research-engine-v2/census-autopilot-queue-orchestrator.js";
import {
  buildSourceYieldDiagnostic,
  renderSourceYieldDiagnosticMarkdown,
  buildWebhoundLearningCandidates,
} from "../lib/research-engine-v2/census-autopilot-source-yield-diagnostic.js";
import {
  buildFastestSafePriorityPlan,
} from "../lib/research-engine-v2/census-autopilot-fastest-safe.js";
import { routeAutopilotQueues } from "../lib/research-engine-v2/census-autopilot-queue-router.js";
import { evaluateProviderReadiness } from "../lib/research-engine-v2/production-census-description-extraction.js";
import { MAP_FIRST_PASS } from "../lib/research-engine-v2/production-census-first-pass-enrichment.js";
import { KEY_FIELD_MATRIX } from "../lib/research-engine-v2/census-autopilot-key-field-completion.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import {
  runAddressAssetApprovalBundleApply,
  renderPreflightMarkdown,
  renderApplyMarkdown,
  writeJson as writeAaJson,
  writeMd as writeAaMd,
  STATUS as ADDRESS_ASSET_STATUS,
} from "../lib/research-engine-v2/census-autopilot-address-asset-preflight-apply.js";
import {
  runPtacApprovalBundleApply,
  renderPtacPreflightMarkdown,
  renderPtacApplyMarkdown,
  writeJson as writePtacJson,
  writeMd as writePtacMd,
  isPtacOnlyApprovalBundle,
  STATUS as PTAC_STATUS,
} from "../lib/research-engine-v2/census-autopilot-property-type-asset-context-apply.js";
import {
  runProductionCycle,
  PRODUCTION_CYCLE_STATUS,
} from "../lib/research-engine-v2/census-autopilot-production-cycle.js";
import {
  runCoverageReconciliationMission,
  COVERAGE_STATUS,
  COVERAGE_RECONCILIATION_OBJECTIVE,
} from "../lib/research-engine-v2/census-autopilot-coverage-reconciliation.js";
import {
  runCoverageStewardResolutionMission,
  COVERAGE_STEWARD_STATUS,
  COVERAGE_STEWARD_RESOLUTION_OBJECTIVE,
} from "../lib/research-engine-v2/census-autopilot-coverage-steward-resolution.js";
import {
  runCleanCensusV1Mission,
  resolveMissionObjective,
  MISSION_STATUS,
  MISSION_OBJECTIVE_CLEAN_CENSUS_V1,
  MISSION_OBJECTIVE_COMPLETE_CENSUS_V1,
  MISSION_OBJECTIVE_COVERAGE_RECONCILIATION_V1,
  MISSION_OBJECTIVE_COVERAGE_STEWARD_RESOLUTION_V1,
  MISSION_OBJECTIVE_SOURCE_CONFIRMED_CENSUS_V2,
  MISSION_OBJECTIVE_BRAND_REGISTRY_RESOLUTION_V1,
  MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1,
  MISSION_OBJECTIVE_LEVEL_2_SOURCE_EXTRACTION_V1,
  MISSION_OBJECTIVE_OFFICIAL_PARENT_INVENTORY_CENSUS_V1,
  MISSION_OBJECTIVE_OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1,
  MISSION_OBJECTIVE_FULL_LATAM_CENSUS_AUTOPILOT_V3,
  MISSION_OBJECTIVE_MARRIOTT_WEBHOUND_SOURCE_PATTERN_LEARNING_V1,
  MISSION_OBJECTIVE_UNIVERSAL_RECORD_RESOLVER_V1,
  MISSION_OBJECTIVE_COMMERCIAL_FIELDS_AND_DESCRIPTION_V1,
  MISSION_OBJECTIVE_ROOMS_COUNT_COMPLETION_V1,
  MISSION_OBJECTIVE_ROOMS_SECONDARY_SOURCE_WAVE_2_V1,
  MISSION_OBJECTIVE_DATAFORSEO_DISCOVERY_PILOT_V2,
  MISSION_OBJECTIVE_DATAFORSEO_VALIDATED_WRITE_POLICY_V1,
  MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_ENRICHMENT_V1,
  MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_VALIDATED_WRITE_V1,
  MISSION_OBJECTIVE_DATAFORSEO_LOCAL_ADDRESS_SCALE_V1,
  MISSION_OBJECTIVE_CENSUS_AUTOPILOT_POLICY_CONTROLLER_V1,
  MISSION_OBJECTIVE_CENSUS_MISSING_FIELD_SOURCE_STRATEGY_CONTROLLER_V1,
  MISSION_OBJECTIVE_HBX_PHASE1_EXISTING_MATCH_HIGH_APPLY_V1,
  MISSION_OBJECTIVE_HBX_CENSUS_SCHEMA_AND_IDENTITY_LINKAGE_V1,
  MISSION_OBJECTIVE_FULL_CALA_15K_CENSUS_SHELL_INSERT_V1,
  MISSION_OBJECTIVE_FULL_CALA_15K_SHELL_FORMAT_SOURCE_BRAND_BACKFILL_V1,
  COMPLETE_CENSUS_MISSION_STATUS,
} from "../lib/research-engine-v2/census-autopilot-mission.js";
import {
  runSourceConfirmedCensusV2Mission,
  SOURCE_CONFIRMED_STATUS,
} from "../lib/research-engine-v2/census-autopilot-source-confirmed-census-v2.js";
import {
  runBrandRegistryResolutionV1Mission,
  BRAND_REGISTRY_RESOLUTION_STATUS,
} from "../lib/research-engine-v2/census-autopilot-brand-registry-resolution-v1.js";
import {
  runOfficialParentInventoryCensusV1Mission,
  OFFICIAL_PARENT_INVENTORY_STATUS,
  OFFICIAL_PARENT_INVENTORY_CENSUS_V1_OBJECTIVE,
} from "../lib/research-engine-v2/census-autopilot-official-parent-inventory-census-v1.js";
import {
  runOfficialParentLevel2CompletionV1Mission,
  OFFICIAL_PARENT_LEVEL_2_STATUS,
  OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_OBJECTIVE,
} from "../lib/research-engine-v2/census-autopilot-official-parent-level-2-completion-v1.js";
import {
  runFullLatamCensusAutopilotV3Mission,
  FULL_LATAM_AUTOPILOT_V3_STATUS,
  FULL_LATAM_AUTOPILOT_V3_OBJECTIVE,
} from "../lib/research-engine-v2/census-autopilot-full-latam-v3.js";
import {
  runMarriottWebhoundSourcePatternLearningV1Mission,
  MARRIOTT_WEBHOUND_LEARNING_STATUS,
  MARRIOTT_WEBHOUND_LEARNING_V1_OBJECTIVE,
} from "../lib/research-engine-v2/census-autopilot-marriott-webhound-source-pattern-learning-v1.js";
import {
  runUniversalRecordResolverV1Mission,
  UNIVERSAL_RECORD_RESOLVER_STATUS,
  UNIVERSAL_RECORD_RESOLVER_V1_OBJECTIVE,
} from "../lib/research-engine-v2/census-autopilot-universal-record-resolver-v1.js";
import {
  runCommercialFieldsAndDescriptionV1Mission,
  COMMERCIAL_FIELDS_DESCRIPTION_STATUS,
  COMMERCIAL_FIELDS_DESCRIPTION_V1_OBJECTIVE,
} from "../lib/research-engine-v2/census-autopilot-commercial-fields-and-description-v1.js";
import {
  runRoomsCountCompletionV1Mission,
  ROOMS_COUNT_COMPLETION_STATUS,
  ROOMS_COUNT_COMPLETION_V1_OBJECTIVE,
} from "../lib/research-engine-v2/census-autopilot-rooms-count-completion-v1.js";
import {
  runRoomsSecondarySourceWave2V1Mission,
  ROOMS_SECONDARY_WAVE_2_STATUS,
  ROOMS_SECONDARY_WAVE_2_OBJECTIVE,
} from "../lib/research-engine-v2/census-autopilot-rooms-secondary-source-wave-2-v1.js";
import {
  runDataForSeoDiscoveryPilotV2Mission,
  DATAFORSEO_DISCOVERY_PILOT_OBJECTIVE,
  DATAFORSEO_DISCOVERY_STATUS,
} from "../lib/research-engine-v2/dataforseo-discovery-pilot.js";
import {
  runDataForSeoValidatedWritePolicyV1Mission,
  DATAFORSEO_VALIDATED_WRITE_OBJECTIVE,
  DATAFORSEO_VALIDATED_WRITE_STATUS,
} from "../lib/research-engine-v2/census-autopilot-dataforseo-validated-write-policy-v1.js";
import {
  runDataForSeoLocalBusinessEnrichmentV1Mission,
  DATAFORSEO_LOCAL_ENRICHMENT_OBJECTIVE,
  DATAFORSEO_LOCAL_ENRICHMENT_STATUS,
} from "../lib/research-engine-v2/dataforseo-local-business-enrichment-v1.js";
import {
  runDataForSeoLocalBusinessValidatedWriteV1Mission,
  DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
  DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS,
} from "../lib/research-engine-v2/dataforseo-local-business-validated-write-v1.js";
import {
  runDataForSeoLocalAddressScaleV1Mission,
  DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
  DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS,
} from "../lib/research-engine-v2/dataforseo-local-address-scale-v1.js";
import {
  runCensusAutopilotPolicyControllerV1Mission,
  POLICY_CONTROLLER_OBJECTIVE,
  POLICY_CONTROLLER_STATUS,
} from "../lib/research-engine-v2/census-autopilot-policy-controller-v1.js";
import {
  runCensusMissingFieldSourceStrategyControllerV1Mission,
  MISSING_FIELD_SOURCE_STRATEGY_OBJECTIVE,
  MISSING_FIELD_SOURCE_STRATEGY_STATUS,
} from "../lib/research-engine-v2/census-missing-field-source-strategy-controller-v1.js";
import {
  runHbxPhase1ExistingMatchHighApplyV1,
  HBX_PHASE1_OBJECTIVE,
  HBX_PHASE1_STATUS,
} from "../lib/research-engine-v2/hbx-phase1-existing-match-high-apply-v1.js";
import {
  runHbxCensusSchemaAndIdentityLinkageV1,
  HBX_SCHEMA_LINKAGE_OBJECTIVE,
  HBX_SCHEMA_LINKAGE_STATUS,
} from "../lib/research-engine-v2/hbx-census-schema-and-identity-linkage-v1.js";
import {
  runFullCala15kCensusShellInsertV1,
  FULL_CALA_15K_OBJECTIVE,
  FULL_CALA_15K_STATUS,
} from "../lib/research-engine-v2/full-cala-15k-census-shell-insert-v1.js";
import {
  runFullCala15kShellFormatSourceBrandBackfillV1,
  SHELL_FORMAT_BACKFILL_OBJECTIVE,
  SHELL_FORMAT_BACKFILL_STATUS,
} from "../lib/research-engine-v2/full-cala-15k-shell-format-source-brand-backfill-v1.js";
import {
  runCalaCensusCompletionV1Mission,
  CALA_CENSUS_COMPLETION_STATUS as CALA_COMPLETION_STATUS,
} from "../lib/research-engine-v2/census-autopilot-cala-census-completion-v1.js";
import {
  runLevel2SourceExtractionV1Mission,
  LEVEL_2_SOURCE_EXTRACTION_STATUS,
  LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE,
} from "../lib/research-engine-v2/census-autopilot-level-2-source-extraction-v1.js";
import {
  runChoiceAddressResourcingControlled,
  persistChoiceAddressResourcingOutputs,
  RECORD_SET_STEWARDED_CHOICE_29,
  STATUS as CHOICE_ADDR_STATUS,
  runChoiceAddressApprovalBundleApply,
  renderChoiceAddressApplyMarkdown,
  APPLY_STATUS,
  writeJson as writeChoiceJson,
  writeMd as writeChoiceMd,
} from "../lib/research-engine-v2/census-autopilot-choice-address-resourcing.js";
import {
  PATHS as LEARNING_PATHS,
  buildSeedLearningEntries,
  buildLedgerDocument,
  renderLedgerMarkdown,
  runBatchLearningAudit,
} from "../lib/data-intelligence/dealality-batch-learning-system.js";
import {
  runSourceDiscoveryControlled,
  DISCOVERY_STATUS,
  SOURCE_DISCOVERY_QUEUE_ID,
} from "../lib/research-engine-v2/census-autopilot-source-discovery.js";
import {
  runDiscoveryInsertApply,
  loadDiscoveryInsertApprovalBundle,
  parseDiscoveryInsertApplyArgs,
  checkDiscoveryInsertApplyEnv,
  INSERT_APPLY_STATUS,
} from "../lib/research-engine-v2/census-autopilot-discovery-insert-apply.js";
import { existsSync, readFileSync } from "node:fs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

const argv = process.argv.slice(2);
const args = parseAutopilotArgs(argv);
const enableProductionWrites =
  argv.includes("--enable-production-writes") &&
  (args.mode === "apply" || args.mode === "production-cycle" || args.mode === "mission");

if (args.warnings?.length) {
  for (const w of args.warnings) console.warn(`[census:autopilot] ${w}`);
}

/**
 * Controlled Choice address re-sourcing for stewarded_choice_address_29 (no writes).
 */
async function runStewardedChoiceAddressResourcing() {
  console.log(
    `[census:autopilot] controlled Choice address resourcing record-set=${args.recordSet} (no Airtable writes)`
  );
  const report = await runChoiceAddressResourcingControlled({});
  persistChoiceAddressResourcingOutputs(report, { reportsRoot: REPORTS, docsRoot: DOCS });
  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        passing: report.records_passing_preflight,
        steward: report.records_still_stewarded,
        blocked: report.records_blocked,
        airtable_writes: false,
        run_dir: report.run_dir,
        approval_bundle: join(report.run_dir, "approval-bundle.json"),
      },
      null,
      2
    )
  );
  if (report.status === CHOICE_ADDR_STATUS.BLOCKED) process.exitCode = 1;
}

// Short-circuit: controlled + stewarded Choice address record-set
if (
  args.mode === "controlled" &&
  args.recordSet === RECORD_SET_STEWARDED_CHOICE_29 &&
  (!args.queue || args.queue === "address_confirmation")
) {
  await runStewardedChoiceAddressResourcing();
  process.exit(process.exitCode || 0);
}

/**
 * Controlled CALA source discovery (insert approval bundle; no Airtable writes).
 */
async function runSourceDiscoveryControlledCli() {
  console.log(
    `[census:autopilot] source_discovery controlled (Hotel Property Census match only; no writes)`
  );
  const token = resolvePat();
  const bases = resolveTargetBase();
  let censusRecords = [];
  if (token && bases?.target_base_id) {
    const live = await loadLiveAutopilotContext({
      ...args,
      mode: "plan",
      queue: null,
    });
    if (live.ok) censusRecords = live.censusRecords || [];
    else console.warn(`[census:autopilot] live census load skipped: ${live.error}`);
  } else {
    console.warn("[census:autopilot] no Airtable credentials — discovery will match against empty Census index");
  }

  const report = await runSourceDiscoveryControlled({
    region: args.region || "CALA",
    parentCompany: args.parentCompany || null,
    country: args.country || null,
    scope: args.scope || "active-brand-setup",
    strategy: args.strategy || "fastest-safe",
    censusRecords,
    writeArtifacts: true,
  });

  writeJson(join(REPORTS, "production-census-cala-discovery-mode.json"), {
    status: report.status,
    summary: report.summary,
    run_dir: report.run_dir,
    airtable_writes: false,
  });
  writeMd(
    join(REPORTS, "production-census-cala-discovery-mode.md"),
    report.summary_md || `# CALA Discovery\n\nStatus: ${report.status}\n`
  );

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        discovered: report.summary?.discovered_properties,
        existing_matches: report.summary?.existing_hotel_property_census_matches,
        new_candidates: report.summary?.new_property_candidates,
        estimated_inserts: report.summary?.estimated_insert_count,
        airtable_writes: false,
        run_dir: report.run_dir,
        approval_bundle: report.run_dir
          ? join(report.run_dir, "approval-bundle.json")
          : null,
      },
      null,
      2
    )
  );
  if (report.status === DISCOVERY_STATUS.BLOCKED) process.exitCode = 1;
}

if (args.mode === "controlled" && args.queue === SOURCE_DISCOVERY_QUEUE_ID) {
  await runSourceDiscoveryControlledCli();
  process.exit(process.exitCode || 0);
}

/**
 * Insert apply path — approval-bundle-bound; only when founder enables apply.
 */
async function runSourceDiscoveryInsertApplyCli() {
  console.log(
    `[census:autopilot] source_discovery insert apply (approval-bundle-bound): ${args.approvalBundle}`
  );
  const insertArgs = parseDiscoveryInsertApplyArgs(argv);
  const envCheck = checkDiscoveryInsertApplyEnv(process.env);
  const wantsLiveWrite =
    args.mode === "apply" &&
    enableProductionWrites &&
    insertArgs.allConfirmsOk &&
    envCheck.allOk;

  const loaded = loadDiscoveryInsertApprovalBundle(args.approvalBundle);
  if (!loaded.ok) {
    console.error(JSON.stringify({ ok: false, error: loaded.error, fieldViolations: loaded.fieldViolations }, null, 2));
    process.exit(1);
  }

  const runDir = dirname(resolve(args.approvalBundle));
  let censusRecords = [];
  if (wantsLiveWrite || argv.includes("--rededupe-live")) {
    const live = await loadLiveAutopilotContext({
      ...args,
      mode: "plan",
      queue: null,
    });
    if (!live.ok) {
      console.error(JSON.stringify({ ok: false, error: live.error, write_target: live.write_target }, null, 2));
      process.exit(1);
    }
    // Reload with address for address-match rededupe
    const token = resolvePat();
    const bases = resolveTargetBase();
    censusRecords = await listCensusRecords(bases.target_base_id, token, [
      MAP_FIRST_PASS.identityKey,
      MAP_FIRST_PASS.propertyName,
      MAP_FIRST_PASS.currentBrand,
      MAP_FIRST_PASS.country,
      MAP_FIRST_PASS.city,
      MAP_FIRST_PASS.address,
      MAP_FIRST_PASS.sourceUrl,
      MAP_FIRST_PASS.officialUrl,
    ]);
    console.log(`[census:autopilot] live Census rows for rededupe: ${censusRecords.length}`);
  }

  const report = await runDiscoveryInsertApply({
    argv,
    env: process.env,
    doWrite: wantsLiveWrite,
    useLiveAirtable: wantsLiveWrite,
    bundlePath: args.approvalBundle,
    censusRecords,
    checkpointDir: runDir,
    args: {
      ...insertArgs,
      apply: wantsLiveWrite,
      approvalBundlePath: args.approvalBundle,
    },
  });

  writeJson(join(runDir, "apply-summary.json"), report);
  writeMd(
    join(runDir, "apply-summary.md"),
    [
      `# Discovery Insert Apply Summary`,
      ``,
      `Status: **${report.status}**`,
      ``,
      `- Bundle inserts: ${report.inserts_in_bundle}`,
      `- Writable after rededupe: ${report.writable_after_rededupe}`,
      `- Created: ${report.created_count || 0}`,
      `- Blocked duplicates: ${report.blocked_duplicates || 0}`,
      `- Steward routed: ${report.steward_routed || 0}`,
      `- Airtable writes: ${report.airtable_writes}`,
      `- Table: Hotel Property Census (${report.table_id})`,
      ``,
    ].join("\n")
  );
  writeJson(join(runDir, "learning-update.json"), {
    census_discovery_insert_apply: true,
    status: report.status,
    created_count: report.created_count || 0,
    approval_bundle_bound: true,
    brand_explorer_untouched: true,
    brand_setup_untouched: true,
    vic_writes: false,
  });
  if (report.blocked?.length) {
    writeJson(join(runDir, "duplicate-risk.json"), report.blocked);
  }
  if (report.steward?.length) {
    writeJson(join(runDir, "steward-review-queue.json"), report.steward);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.status !== INSERT_APPLY_STATUS.BLOCKED,
        status: report.status,
        note: report.note,
        airtable_writes: report.airtable_writes,
        created_count: report.created_count,
        writable_after_rededupe: report.writable_after_rededupe,
        blocked_duplicates: report.blocked_duplicates,
        steward_routed: report.steward_routed,
        run_dir: runDir,
      },
      null,
      2
    )
  );
  if (report.status === INSERT_APPLY_STATUS.BLOCKED) process.exitCode = 1;
}

if (
  args.mode === "apply" &&
  args.approvalBundle &&
  args.queue === SOURCE_DISCOVERY_QUEUE_ID
) {
  await runSourceDiscoveryInsertApplyCli();
  process.exit(process.exitCode || 0);
}

/**
 * Approval-bundle-bound Choice Address apply (29 records only).
 */
async function runChoiceAddressBundleApply() {
  console.log(
    `[census:autopilot] Choice Address approval-bundle apply (29 only, no re-plan): ${args.approvalBundle}`
  );
  const report = await runChoiceAddressApprovalBundleApply(argv, process.env);
  const runDir = report.run_dir || dirname(args.approvalBundle);

  writeChoiceJson(
    join(REPORTS, "production-census-choice-address-apply.json"),
    report
  );
  writeChoiceMd(
    join(REPORTS, "production-census-choice-address-apply.md"),
    renderChoiceAddressApplyMarkdown(report)
  );
  writeChoiceMd(
    join(DOCS, "production-census-choice-address-apply.md"),
    [
      `# Production Census — Choice Address Apply`,
      ``,
      `Status: **${report.status}**`,
      ``,
      `- Approval-bundle-bound; 29 Choice Address records only.`,
      `- Fields: Address, Address Confidence, Address Source URL, Last Reviewed Date.`,
      `- Records updated: ${report.records_updated ?? 0}`,
      `- Airtable writes: ${report.airtable_writes}`,
      `- Brand Explorer / Brand Setup: untouched.`,
      ``,
      `See \`reports/research-engine-v2/production-census-choice-address-apply.json\`.`,
      ``,
    ].join("\n")
  );

  if (runDir && existsSync(runDir)) {
    writeChoiceJson(join(runDir, "apply-summary.json"), report);
    writeChoiceMd(join(runDir, "apply-summary.md"), renderChoiceAddressApplyMarkdown(report));
    writeChoiceJson(join(runDir, "learning-update.json"), {
      census_choice_address_apply: true,
      status: report.status,
      records_updated: report.records_updated || 0,
      no_replan: true,
      approval_bundle_bound: true,
      property_level_urls_only: true,
      brand_explorer_untouched: true,
      brand_setup_untouched: true,
    });
    writeChoiceJson(join(runDir, "checkpoint.json"), {
      status: report.status,
      mode: "apply",
      record_set: RECORD_SET_STEWARDED_CHOICE_29,
      approval_bundle: args.approvalBundle,
      completion_status: "complete",
      records_updated: report.records_updated || 0,
      airtable_writes: Boolean(report.apply_executed && report.records_updated > 0),
      updated_at: report.generated_at,
    });
    writeChoiceMd(
      join(runDir, "summary.md"),
      [
        `# Run Summary — Choice Address Apply`,
        ``,
        `- Status: ${report.status}`,
        `- Updated: ${report.records_updated || 0}`,
        `- Skipped matching: ${report.records_skipped_matching || 0}`,
        `- Failed: ${report.records_failed || 0}`,
        `- Airtable writes: ${report.airtable_writes}`,
        ``,
      ].join("\n")
    );
  }

  // Learning ledger
  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-choice-address-property-level-apply",
      date: today,
      process: "census",
      batch_name: "production_census_choice_address_apply",
      source_report: "reports/research-engine-v2/production-census-choice-address-apply.json",
      issue_type: "learned_validation_rule",
      example_records: (report.write_results || [])
        .filter((w) => w.ok && !w.skipped)
        .slice(0, 8)
        .map((w) => w.identity_key),
      reusable_pattern:
        "Stewarded Choice regional placeId URLs must be re-sourced to VIC/property-level official URLs before apply; never write shared regional-hotels?placeId= as Address Source URL.",
      proposed_code_change:
        "census-autopilot-choice-address-resourcing.js — controlled resourcing + approval-bundle apply",
      module_to_update: "lib/research-engine-v2/census-autopilot-choice-address-resourcing.js",
      fixture_added: false,
      test_added: true,
      status: report.status === APPLY_STATUS.CLEAN ? "implemented" : "proposed",
      next_action:
        report.status === APPLY_STATUS.CLEAN
          ? "Continue Autopilot queues; keep geocode deferred."
          : "Review Choice address apply partial/blocked before further writes.",
      lane: "address_confirmation",
      metrics: {
        records_updated: report.records_updated,
        rooms_filled: report.rooms_filled_after,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeChoiceJson(join(ROOT, LEARNING_PATHS.ledgerJson), ledger);
    writeChoiceMd(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger));
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.status !== APPLY_STATUS.BLOCKED,
        status: report.status,
        records_updated: report.records_updated || 0,
        skipped: report.records_skipped_matching || 0,
        failed: report.records_failed || 0,
        apply_executed: report.apply_executed,
        post_verify_ok: report.post_verify_ok,
      },
      null,
      2
    )
  );
  if (report.status === APPLY_STATUS.BLOCKED) process.exitCode = 1;
}

/**
 * Approval-bundle-bound Property Type / Asset Context apply (no inserts / no re-plan).
 */
async function runPtacBundleApply() {
  console.log(
    `[census:autopilot] property_type_asset_context approval-bundle apply (24 updates only): ${args.approvalBundle}`
  );
  const report = await runPtacApprovalBundleApply(argv, process.env);

  const preflightPathMd = join(
    REPORTS,
    "production-census-property-type-asset-context-preflight.md"
  );
  const preflightPathJson = join(
    REPORTS,
    "production-census-property-type-asset-context-preflight.json"
  );
  const applyPathMd = join(REPORTS, "production-census-property-type-asset-context-apply.md");
  const applyPathJson = join(REPORTS, "production-census-property-type-asset-context-apply.json");
  const docsPath = join(DOCS, "production-census-property-type-asset-context-apply.md");

  const preflight = report.preflight || report;
  writePtacJson(preflightPathJson, preflight);
  writePtacMd(preflightPathMd, renderPtacPreflightMarkdown(preflight));
  writePtacJson(applyPathJson, report);
  writePtacMd(applyPathMd, renderPtacApplyMarkdown(report));
  writePtacMd(
    docsPath,
    [
      `# Production Census — Property Type / Asset Context Apply`,
      ``,
      `Status: **${report.status}**`,
      ``,
      `- Approval-bundle-bound only (no re-plan, no inserts).`,
      `- Allowed fields: Property Type, Asset Context (+ enrichment metadata).`,
      `- Source discovery / Choice Radisson Individuals inserts: untouched.`,
      `- Geocode / descriptions / rooms / names / Brand Explorer / Brand Setup: untouched.`,
      `- Records updated: ${report.records_updated ?? 0}`,
      `- Steward review: ${report.steward_count ?? preflight.records_routed_to_steward_review ?? 0}`,
      `- Census count: ${report.census_record_count_after ?? "(n/a)"}`,
      ``,
      `See \`reports/research-engine-v2/production-census-property-type-asset-context-apply.json\`.`,
      ``,
    ].join("\n")
  );

  const runDir = report.run_dir || dirname(resolve(args.approvalBundle));
  if (runDir && existsSync(runDir)) {
    writePtacJson(join(runDir, "apply-summary.json"), report);
    writePtacMd(join(runDir, "apply-summary.md"), renderPtacApplyMarkdown(report));
    writePtacJson(join(runDir, "steward-review-queue.json"), {
      generated_at: report.generated_at,
      count: (report.steward_queue || preflight.steward_queue || []).length,
      items: report.steward_queue || preflight.steward_queue || [],
      note: "PT/AC weak-support steward only; source_discovery Choice Radisson Individuals remain separate.",
    });
    const blocked = report.blocked_queue || preflight.blocked_queue || [];
    const steward = report.steward_queue || preflight.steward_queue || [];
    const csvLines = [
      "record_id,identity_key,property_name,lane,reason",
      ...blocked.map(
        (b) =>
          `${b.record_id},${b.identity_key},"${(b.property_name || "").replace(/"/g, '""')}",${b.lane || "property_type_asset_context"},"${(b.errors || []).join("|")}"`
      ),
      ...steward.map(
        (s) =>
          `${s.record_id},${s.identity_key},"${(s.property_name || "").replace(/"/g, '""')}",${s.lane || "property_type_asset_context"},"${(s.reasons || []).join("|")}"`
      ),
    ];
    writeFileSync(join(runDir, "blocked-records.csv"), csvLines.join("\n"), "utf8");
    writePtacJson(join(runDir, "learning-update.json"), {
      census_autopilot_property_type_asset_context_apply: true,
      status: report.status,
      records_updated: report.records_updated || 0,
      steward_count: report.steward_count || 0,
      no_replan: true,
      no_inserts: true,
      approval_bundle_bound: true,
      brand_explorer_untouched: true,
      brand_setup_untouched: true,
      source_discovery_untouched: true,
      geocode_untouched: true,
    });
    writePtacJson(join(runDir, "checkpoint.json"), {
      status: report.status,
      mode: "apply",
      queue: "property_type_asset_context",
      approval_bundle: args.approvalBundle,
      completion_status: "complete",
      records_updated: report.records_updated || 0,
      inserts_applied: 0,
      airtable_writes: Boolean(report.apply_executed && report.records_updated > 0),
      updated_at: report.generated_at,
    });
    writePtacMd(
      join(runDir, "summary.md"),
      [
        `# Run Summary — Property Type / Asset Context Apply`,
        ``,
        `- Status: ${report.status}`,
        `- Updated: ${report.records_updated || 0}`,
        `- Steward: ${report.steward_count || 0}`,
        `- Inserts applied: 0`,
        `- Airtable writes: ${report.airtable_writes}`,
        `- Census count: ${report.census_record_count_after}`,
        ``,
      ].join("\n")
    );
  }

  // Learning ledger
  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-property-type-asset-context-apply",
      date: today,
      process: "census",
      batch_name: "production_census_property_type_asset_context_apply",
      source_report: "reports/research-engine-v2/production-census-property-type-asset-context-apply.json",
      issue_type: "learned_validation_rule",
      example_records: (report.write_results || [])
        .filter((w) => w.ok && !w.skipped)
        .slice(0, 8)
        .map((w) => w.identity_key),
      reusable_pattern:
        "Apply property_type_asset_context High updates via dedicated approval-bundle path; never co-apply source_discovery inserts; require name/brand/amenity support tokens.",
      proposed_code_change:
        "census-autopilot-property-type-asset-context-apply.js — PT/AC-only approval-bundle apply",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-property-type-asset-context-apply.js",
      fixture_added: false,
      test_added: false,
      status: report.status === PTAC_STATUS.CLEAN ? "implemented" : "proposed",
      next_action:
        report.status === PTAC_STATUS.CLEAN
          ? "Continue Autopilot; keep source_discovery steward inserts held."
          : "Review PT/AC steward/partial before further Census writes.",
      lane: "property_type_asset_context",
      metrics: {
        records_updated: report.records_updated,
        census_count: report.census_record_count_after,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writePtacJson(join(ROOT, LEARNING_PATHS.ledgerJson), ledger);
    writePtacMd(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger));
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.status !== PTAC_STATUS.BLOCKED,
        status: report.status,
        records_updated: report.records_updated || 0,
        steward: report.steward_count || 0,
        apply_executed: report.apply_executed,
        census_count: report.census_record_count_after,
        inserts_applied: 0,
      },
      null,
      2
    )
  );

  if (report.status === PTAC_STATUS.BLOCKED) process.exitCode = 1;
}

/**
 * Approval-bundle-bound Address + Asset Context apply (no re-plan / no orchestrator).
 */
async function runApprovalBundleBoundApply() {
  // Route Choice Address-only bundle to dedicated apply (correct status + field contract)
  let bundleMeta = null;
  try {
    if (args.approvalBundle && existsSync(args.approvalBundle)) {
      bundleMeta = JSON.parse(readFileSync(args.approvalBundle, "utf8"));
    }
  } catch {
    bundleMeta = null;
  }
  if (
    bundleMeta?.record_set === RECORD_SET_STEWARDED_CHOICE_29 ||
    String(bundleMeta?.version || "").includes("choice-address-resourcing")
  ) {
    await runChoiceAddressBundleApply();
    return;
  }

  // Route PT/AC-only bundles (or explicit queue) away from address-asset apply
  if (
    args.queue === "property_type_asset_context" ||
    isPtacOnlyApprovalBundle(args.approvalBundle)
  ) {
    await runPtacBundleApply();
    return;
  }

  // Brand normalization High proposals — apply via production-cycle (Census updates only)
  if (
    args.queue === "brand_normalization" ||
    (Array.isArray(bundleMeta?.queues_executed) &&
      bundleMeta.queues_executed.length === 1 &&
      bundleMeta.queues_executed[0] === "brand_normalization")
  ) {
    console.log(
      `[census:autopilot] brand_normalization apply via production-cycle (cleanup-existing-only, no inserts)`
    );
    const report = await runProductionCycle({
      argv,
      args: {
        ...args,
        mode: "production-cycle",
        queue: "brand_normalization",
        cleanupExistingOnly: true,
      },
      env: process.env,
      enableProductionWrites: Boolean(enableProductionWrites),
      skipInserts: true,
      log: (msg) => console.log(msg),
    });
    try {
      const { writeBrandNormalizationReports, BRAND_NORMALIZATION_STATUS } = await import(
        "../lib/research-engine-v2/census-brand-normalization.js"
      );
      const brandStatus =
        report.updates_applied > 0 && (report.steward_cases || 0) === 0
          ? BRAND_NORMALIZATION_STATUS.APPLIED_CLEAN
          : report.updates_applied > 0
            ? BRAND_NORMALIZATION_STATUS.PARTIAL
            : report.ok === false
              ? BRAND_NORMALIZATION_STATUS.BLOCKED
              : BRAND_NORMALIZATION_STATUS.PARTIAL;
      writeBrandNormalizationReports(
        {
          version: "census-brand-normalization-v1",
          queue: "brand_normalization",
          status: brandStatus,
          write_target: {
            base: productionHotelPropertyCensus.baseName,
            table: productionHotelPropertyCensus.tableName,
            table_id: productionHotelPropertyCensus.tableId,
          },
          brand_setup_read_only: true,
          brand_explorer_writes: false,
          counters: {
            high_proposals: report.updates_applied,
            records_scanned: report.records_before,
          },
          proposals: [],
          steward_cases: [],
          examples_before_after: [],
          production_cycle: {
            updates_applied: report.updates_applied,
            steward_cases: report.steward_cases,
            status: report.status,
            run_dir: report.run_dir,
          },
        },
        {
          airtable_writes: Boolean(report.airtable_writes),
          updates_applied: report.updates_applied,
          mode: "mission_apply",
          clean_core_before: report.records_before,
          clean_core_after: report.records_after,
        }
      );
    } catch (err) {
      console.warn(`[census:autopilot] brand norm report update failed: ${err?.message || err}`);
    }
    console.log(
      JSON.stringify(
        {
          ok: report.ok !== false,
          status: report.status,
          records_updated: report.updates_applied,
          steward: report.steward_cases,
          apply_executed: Boolean(report.airtable_writes),
          run_dir: report.run_dir,
          brand_explorer_writes: false,
          brand_setup_writes: false,
        },
        null,
        2
      )
    );
    if (report.status === PRODUCTION_CYCLE_STATUS.BLOCKED || report.ok === false) {
      process.exitCode = 1;
    }
    return;
  }

  console.log(
    `[census:autopilot] approval-bundle-bound apply (no re-plan): ${args.approvalBundle}`
  );
  const report = await runAddressAssetApprovalBundleApply(argv, process.env);

  const preflightPathMd = join(REPORTS, "production-census-autopilot-address-asset-preflight.md");
  const preflightPathJson = join(
    REPORTS,
    "production-census-autopilot-address-asset-preflight.json"
  );
  const applyPathMd = join(REPORTS, "production-census-autopilot-address-asset-apply.md");
  const applyPathJson = join(REPORTS, "production-census-autopilot-address-asset-apply.json");
  const docsPath = join(DOCS, "production-census-autopilot-address-asset-apply.md");

  const preflight = report.preflight || report;
  writeAaJson(preflightPathJson, preflight);
  writeAaMd(preflightPathMd, renderPreflightMarkdown(preflight));
  writeAaJson(applyPathJson, report);
  writeAaMd(applyPathMd, renderApplyMarkdown(report));
  writeAaMd(
    docsPath,
    [
      `# Production Census Autopilot — Address + Asset Context Apply`,
      ``,
      `Status: **${report.status}**`,
      ``,
      `- Approval-bundle-bound only (no re-plan).`,
      `- Allowed lanes: Address + Asset Context.`,
      `- Geocode / descriptions / rooms / names / Brand Explorer / Brand Setup: untouched.`,
      `- Records updated: ${report.records_updated ?? 0}`,
      `- Steward review: ${report.steward_count ?? preflight.records_routed_to_steward_review ?? 0}`,
      ``,
      `See \`reports/research-engine-v2/production-census-autopilot-address-asset-apply.json\`.`,
      ``,
    ].join("\n")
  );

  const runDir = report.run_dir || dirname(args.approvalBundle);
  if (runDir && existsSync(runDir)) {
    writeAaJson(join(runDir, "apply-summary.json"), report);
    writeAaMd(join(runDir, "apply-summary.md"), renderApplyMarkdown(report));
    writeAaJson(join(runDir, "steward-review-queue.json"), {
      generated_at: report.generated_at,
      count: (report.steward_queue || preflight.steward_queue || []).length,
      items: report.steward_queue || preflight.steward_queue || [],
    });
    const blocked = report.blocked_queue || preflight.blocked_queue || [];
    const steward = report.steward_queue || preflight.steward_queue || [];
    const csvLines = [
      "record_id,identity_key,property_name,lane,reason",
      ...blocked.map(
        (b) =>
          `${b.record_id},${b.identity_key},"${(b.property_name || "").replace(/"/g, '""')}",${b.lane},"${(b.errors || []).join("|")}"`
      ),
      ...steward.map(
        (s) =>
          `${s.record_id},${s.identity_key},"${(s.property_name || "").replace(/"/g, '""')}",${s.lane},"${(s.reasons || []).join("|")}"`
      ),
    ];
    writeFileSync(join(runDir, "blocked-records.csv"), csvLines.join("\n"), "utf8");
    writeAaJson(join(runDir, "learning-update.json"), {
      census_autopilot_address_asset_apply: true,
      status: report.status,
      records_updated: report.records_updated || 0,
      steward_count: report.steward_count || 0,
      no_replan: true,
      approval_bundle_bound: true,
      brand_explorer_untouched: true,
      brand_setup_untouched: true,
      geocode_untouched: true,
    });
    writeAaJson(join(runDir, "checkpoint.json"), {
      status: report.status,
      mode: "apply",
      approval_bundle: args.approvalBundle,
      completion_status: "complete",
      records_updated: report.records_updated || 0,
      airtable_writes: Boolean(report.apply_executed && report.records_updated > 0),
      updated_at: report.generated_at,
    });
    writeAaMd(
      join(runDir, "summary.md"),
      [
        `# Run Summary — Address + Asset Apply`,
        ``,
        `- Status: ${report.status}`,
        `- Updated: ${report.records_updated || 0}`,
        `- Steward: ${report.steward_count || 0}`,
        `- Airtable writes: ${report.airtable_writes}`,
        ``,
      ].join("\n")
    );
  }

  console.log(
    JSON.stringify(
      {
        ok: report.status !== ADDRESS_ASSET_STATUS.BLOCKED,
        status: report.status,
        records_updated: report.records_updated || 0,
        steward: report.steward_count || 0,
        apply_executed: report.apply_executed,
      },
      null,
      2
    )
  );

  if (report.status === ADDRESS_ASSET_STATUS.BLOCKED) process.exitCode = 1;
}

// Short-circuit: level-2-source-extraction-v1 (High Address/Phone/Rooms + chain cala)
const resolvedObjective = resolveMissionObjective(args.objective);

// Short-circuit: full-cala-15k-shell-format-source-brand-backfill-v1
if (
  resolvedObjective ===
    MISSION_OBJECTIVE_FULL_CALA_15K_SHELL_FORMAT_SOURCE_BRAND_BACKFILL_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] full-cala-15k-shell-format-source-brand-backfill-v1 mode=${args.mode} census_mode=${args.censusMode || "shell-backfill"} countries=${(args.countries || []).join("|") || "—"} backfill=${process.env.ENABLE_CENSUS_SHELL_FORMAT_BACKFILL || "0"}`
  );
  const report = await runFullCala15kShellFormatSourceBrandBackfillV1({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        args.confirms?.noBrandExplorer &&
        args.confirms?.noOwnerOperator &&
        (args.mode === "mission" || args.mode === "controlled") &&
        String(process.env.ENABLE_FULL_CALA_15K_CENSUS_SHELL || "0") === "1" &&
        String(process.env.ENABLE_CENSUS_SHELL_FORMAT_BACKFILL || "0") === "1" &&
        String(process.env.ENABLE_CURRENT_BRAND_WRITES || "0") === "0"
    ),
    censusMode: args.censusMode || "shell-backfill",
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-full-cala-15k-shell-format-source-brand-backfill-v1",
      date: today,
      process: "census",
      batch_name:
        "production_census_full_cala_15k_shell_format_source_brand_backfill_v1",
      source_report:
        "reports/research-engine-v2/full-cala-15k-shell-format-source-brand-backfill-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Shell backfill: smart Canonical casing; Discovery/Candidate provenance fields; Candidate Brand* only — never Current Brand/Family / Source Family from unvalidated Cvent; Family / Source Family is brand-family select.",
      proposed_code_change:
        "full-cala-15k-shell-format-source-brand-backfill-v1.js",
      module_to_update:
        "lib/research-engine-v2/full-cala-15k-shell-format-source-brand-backfill-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === SHELL_FORMAT_BACKFILL_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action:
        "Validate candidate brands separately before any Current Brand writes",
      lane: "full_cala_15k_shell_format_source_brand_backfill_v1",
      metrics: {
        records_updated: report.records_updated,
        canonical_names_fixed: report.canonical_names_fixed,
        candidate_brand_text_writes: report.candidate_brand_text_writes,
        current_brand_writes: report.current_brand_writes,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        secondary_status: report.secondary_status || null,
        objective: SHELL_FORMAT_BACKFILL_OBJECTIVE,
        records_reviewed: report.records_reviewed,
        records_updated: report.records_updated,
        canonical_names_fixed: report.canonical_names_fixed,
        provenance_writes: report.provenance_writes,
        candidate_brand_text_writes: report.candidate_brand_text_writes,
        current_brand_writes: report.current_brand_writes,
        schema_created: report.schema?.created?.length || 0,
        schema_missing: report.schema?.missing?.length || 0,
        airtable_writes: report.airtable_writes,
        dry_run: report.dry_run,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: full-cala-15k-census-shell-insert-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_FULL_CALA_15K_CENSUS_SHELL_INSERT_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  // Country-batch objective aliases force country when not already set
  const objectiveRaw = String(args.objective || "").toLowerCase();
  if (objectiveRaw.includes("colombia-batch") && !args.country) {
    args.country = "Colombia";
  }
  if (objectiveRaw.includes("mexico-batch") && !args.country) {
    args.country = "Mexico";
  }
  const mexicoBatchMatch = objectiveRaw.match(/mexico-batch-(\d+)/);
  if (mexicoBatchMatch) {
    args.shellCountryBatch =
      args.shellCountryBatch || `Mexico Batch ${mexicoBatchMatch[1]}`;
  }
  console.log(
    `[census:autopilot] full-cala-15k-census-shell-insert-v1 mode=${args.mode} census_mode=${args.censusMode || "universe-shell"} inserts=${process.env.ENABLE_CENSUS_SHELL_INSERTS || "0"} country=${args.country || "—"}`
  );
  const report = await runFullCala15kCensusShellInsertV1({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        args.confirms?.noBrandExplorer &&
        args.confirms?.noOwnerOperator &&
        (args.mode === "mission" || args.mode === "controlled") &&
        String(process.env.ENABLE_FULL_CALA_15K_CENSUS_SHELL || "0") === "1" &&
        String(process.env.ENABLE_CENSUS_SHELL_INSERTS || "0") === "1" &&
        String(process.env.ENABLE_CURRENT_BRAND_WRITES || "0") === "0"
    ),
    force: true,
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-full-cala-15k-census-shell-insert-v1",
      date: today,
      process: "census",
      batch_name: "production_census_full_cala_15k_shell_insert_v1",
      source_report:
        "reports/research-engine-v2/full-cala-15k-census-shell-insert-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Shell-first 15K CALA: inventury+normalize+dedupe; insert Property Name/Country/City shells with Hold+HR; Cvent identity-only; no rooms/coords/media/owner; batch by country.",
      proposed_code_change: "full-cala-15k-census-shell-insert-v1.js",
      module_to_update:
        "lib/research-engine-v2/full-cala-15k-census-shell-insert-v1.js",
      fixture_added: false,
      test_added: false,
      status:
        report.status === FULL_CALA_15K_STATUS.BLOCKED ? "proposed" : "implemented",
      next_action: "Apply remaining country batches after DO/CR/PA/CO/MX review",
      lane: "full_cala_15k_census_shell_insert_v1",
      metrics: {
        eligible: report.eligible_shell_inserts,
        inserts_applied: report.inserts_applied,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: FULL_CALA_15K_OBJECTIVE,
        census_before_count: report.census_before_count,
        total_candidates: report.total_candidates,
        eligible_shell_inserts: report.eligible_shell_inserts,
        inserts_applied: report.inserts_applied,
        first_batch: report.first_batch,
        census_after_estimate: report.census_after_estimate,
        existing_match_high_skipped: report.existing_match_high_skipped,
        hbx_index_stats: report.hbx_index_stats,
        plan_skipped_hbx_dedupe: report.plan_skipped_hbx_dedupe,
        plan_skipped_name_dedupe: report.plan_skipped_name_dedupe,
        dry_run: report.dry_run,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: hbx-census-schema-and-identity-linkage-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_HBX_CENSUS_SCHEMA_AND_IDENTITY_LINKAGE_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] hbx-census-schema-and-identity-linkage-v1 mode=${args.mode} census_mode=${args.censusMode || "identity-linkage-only"} schema_repair=${process.env.ENABLE_HBX_SCHEMA_REPAIR || "0"} linkage=${process.env.ENABLE_HBX_IDENTITY_LINKAGE_WRITES || "0"}`
  );
  const report = await runHbxCensusSchemaAndIdentityLinkageV1({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        args.confirms?.noBrandExplorer &&
        args.confirms?.noOwnerOperator &&
        (args.mode === "mission" || args.mode === "controlled") &&
        String(process.env.ENABLE_HBX_CENSUS_WRITES || "0") === "1" &&
        String(process.env.ENABLE_HBX_INSERTS || "0") === "0"
    ),
    censusMode: args.censusMode || "identity-linkage-only",
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-hbx-census-schema-and-identity-linkage-v1",
      date: today,
      process: "census",
      batch_name: "production_census_hbx_census_schema_and_identity_linkage_v1",
      source_report:
        "reports/research-engine-v2/hbx-census-schema-and-identity-linkage-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "HBX schema repair + identity linkage only for existing_match_high; never rewrite Address/Phone/URL; never rooms/coords/media/inserts; Notes for Steward left in place.",
      proposed_code_change: "hbx-census-schema-and-identity-linkage-v1.js",
      module_to_update:
        "lib/research-engine-v2/hbx-census-schema-and-identity-linkage-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === HBX_SCHEMA_LINKAGE_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action:
        report.schema?.missing?.length
          ? "Complete manual Census field creation then re-run identity linkage"
          : "Keep HBX inserts and enrichment lanes off until founder approval",
      lane: "hbx_census_schema_and_identity_linkage_v1",
      metrics: {
        records_updated: report.records_updated,
        hbx_hotel_codes_written: report.hbx_hotel_codes_written,
        schema_created: report.schema?.created?.length || 0,
        schema_missing: report.schema?.missing?.length || 0,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        reason: report.reason || null,
        objective: HBX_SCHEMA_LINKAGE_OBJECTIVE,
        records_reviewed: report.records_reviewed,
        records_updated: report.records_updated,
        hbx_hotel_codes_written: report.hbx_hotel_codes_written,
        hbx_chain_codes_written: report.hbx_chain_codes_written,
        phone_provenance_writes: report.phone_provenance_writes,
        schema_created: report.schema?.created?.length || 0,
        schema_missing: report.schema?.missing?.length || 0,
        airtable_writes: report.airtable_writes,
        inserts: report.inserts,
        dry_run: report.dry_run,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: hbx-phase1-existing-match-high-apply-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_HBX_PHASE1_EXISTING_MATCH_HIGH_APPLY_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] hbx-phase1-existing-match-high-apply-v1 mode=${args.mode} census_mode=${args.censusMode || "field-completion-only"} writes=${process.env.ENABLE_HBX_CENSUS_WRITES || "0"} high=${process.env.ENABLE_HBX_EXISTING_MATCH_HIGH_WRITES || "0"}`
  );
  const report = await runHbxPhase1ExistingMatchHighApplyV1({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        args.confirms?.noBrandExplorer &&
        args.confirms?.noOwnerOperator &&
        (args.mode === "mission" || args.mode === "controlled") &&
        String(process.env.ENABLE_HBX_CENSUS_WRITES || "0") === "1" &&
        String(process.env.ENABLE_HBX_EXISTING_MATCH_HIGH_WRITES || "0") === "1" &&
        String(process.env.ENABLE_HBX_INSERTS || "0") === "0"
    ),
    censusMode: args.censusMode || "field-completion-only",
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-hbx-phase1-existing-match-high-apply-v1",
      date: today,
      process: "census",
      batch_name: "production_census_hbx_phase1_existing_match_high_apply_v1",
      source_report:
        "reports/research-engine-v2/hbx-phase1-existing-match-high-apply-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "HBX Phase 1: existing_match_high only; blank Address/City/Country/Phone(PHONEHOTEL)/Official Property URL; never rooms/coords/images/descriptions/facilities/inserts; HBX identity fields schema_missing until created.",
      proposed_code_change: "hbx-phase1-existing-match-high-apply-v1.js",
      module_to_update:
        "lib/research-engine-v2/hbx-phase1-existing-match-high-apply-v1.js",
      fixture_added: false,
      test_added: false,
      status:
        report.status === HBX_PHASE1_STATUS.BLOCKED ? "proposed" : "implemented",
      next_action:
        "Create HBX Hotel Code Census field; license review for coords/media; keep inserts off",
      lane: "hbx_phase1_existing_match_high_apply_v1",
      metrics: {
        records_updated: report.records_updated,
        address_writes: report.address_writes,
        website_writes: report.website_writes,
        phonehotel_writes: report.phonehotel_writes,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        reason: report.reason || null,
        objective: HBX_PHASE1_OBJECTIVE,
        existing_match_high_reviewed: report.existing_match_high_reviewed,
        records_updated: report.records_updated,
        address_writes: report.address_writes,
        website_writes: report.website_writes,
        phonehotel_writes: report.phonehotel_writes,
        schema_missing_count: report.schema_missing_fields?.length || 0,
        airtable_writes: report.airtable_writes,
        inserts: report.inserts,
        dry_run: report.dry_run,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: universal-record-resolver-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_UNIVERSAL_RECORD_RESOLVER_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] universal-record-resolver-v1 mode=${args.mode} census_mode=${args.censusMode || "field-completion-only"} record=${args.recordId || "—"} code=${args.propertyCode || "—"}`
  );
  const report = await runUniversalRecordResolverV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        (args.mode === "mission" || args.mode === "controlled")
    ),
    censusMode: args.censusMode || "field-completion-only",
    recordId: args.recordId || null,
    propertyCode: args.propertyCode || null,
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-universal-record-resolver-v1",
      date: today,
      process: "census",
      batch_name: "production_census_universal_record_resolver_v1",
      source_report:
        "reports/research-engine-v2/production-census-universal-record-resolver-v1.json",
      issue_type: "learned_validation_rule",
      example_records: report.mx043?.record_id ? [report.mx043.record_id] : [],
      reusable_pattern:
        "Record-level resolver: inspect missing/incorrect fields → route by parent/code → official High writes only; secondary sources gated; Webhound never SoT.",
      proposed_code_change:
        "universal-hotel-record-resolver.js + choice-property-record-resolver.js + census-autopilot-universal-record-resolver-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-universal-record-resolver-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === UNIVERSAL_RECORD_RESOLVER_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action: "Continue universal-record-resolver-v1 until High official writes exhaust",
      lane: "universal_record_resolver_v1",
      metrics: {
        records_updated: report.records_updated,
        records_resolved: report.records_resolved,
        secondary_opportunities: report.secondary_opportunities_count,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(JSON.stringify({
    ok: report.ok,
    status: report.status,
    reason: report.reason || null,
    objective: UNIVERSAL_RECORD_RESOLVER_V1_OBJECTIVE,
    records_updated: report.records_updated,
    records_inserted: report.records_inserted,
    mx043: report.mx043,
    airtable_writes: report.airtable_writes,
    run_dir: report.run_dir,
    preflight: report.preflight || null,
    envCheck: report.envCheck || null,
  }, null, 2));
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: census-missing-field-source-strategy-controller-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_CENSUS_MISSING_FIELD_SOURCE_STRATEGY_CONTROLLER_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] ${MISSING_FIELD_SOURCE_STRATEGY_OBJECTIVE} mode=${args.mode} policy=${process.env.ENABLE_CENSUS_POLICY_CONTROLLER || "0"}`
  );
  const report = await runCensusMissingFieldSourceStrategyControllerV1Mission({
    argv,
    args,
    env: {
      ...process.env,
      ENABLE_CENSUS_POLICY_CONTROLLER: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
    },
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        (args.mode === "mission" || args.mode === "controlled")
    ),
    censusMode: "field-completion-only",
    log: (msg) => console.log(msg),
  });
  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective,
        inserts: report.inserts,
        existing_records_updated: report.existing_records_updated,
        gaps_after: report.gaps_after,
        discovery_partial: report.discovery_partial,
        next_source_investment: report.next_source_investment,
      },
      null,
      2
    )
  );
  process.exit(
    report.status === MISSING_FIELD_SOURCE_STRATEGY_STATUS.BLOCKED ? 1 : 0
  );
}

// Short-circuit: census-autopilot-policy-controller-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_CENSUS_AUTOPILOT_POLICY_CONTROLLER_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] census-autopilot-policy-controller-v1 mode=${args.mode} census_mode=${args.censusMode || "growth"} controller=${process.env.ENABLE_CENSUS_POLICY_CONTROLLER || "0"}`
  );
  const fieldCompletionMode = String(args.censusMode || "").includes(
    "field-completion"
  );
  const report = await runCensusAutopilotPolicyControllerV1Mission({
    argv,
    args,
    env: {
      ...process.env,
      // Sticky discovery-only flag must not block field-completion applies.
      ...(fieldCompletionMode ? { DATAFORSEO_WRITE_CANDIDATES_ONLY: "0" } : {}),
    },
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        (args.mode === "mission" || args.mode === "controlled") &&
        String(process.env.ENABLE_CENSUS_POLICY_CONTROLLER || "0") === "1" &&
        (String(process.env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "0") !== "1" ||
          fieldCompletionMode)
    ),
    censusMode: args.censusMode || "growth",
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-policy-controller-v1",
      date: today,
      process: "census",
      batch_name: "production_census_autopilot_policy_controller_v1",
      source_report:
        "reports/research-engine-v2/census-autopilot-policy-controller-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Single approved Autopilot policy encodes Address/Website/Mapbox-after-address/Rooms/Market rules; no per-field founder gates; phone + DFS coords + inserts held unless explicit flags.",
      proposed_code_change:
        "census-autopilot-approved-policy.js + census-autopilot-policy-controller-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-policy-controller-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === POLICY_CONTROLLER_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action:
        "Continue Autopilot cycles; enable high-confidence inserts only when founder sets both insert flags",
      lane: "census_autopilot_policy_controller_v1",
      metrics: {
        records_updated: report.existing_records_updated,
        address_writes: report.address_writes,
        mapbox_writes: report.mapbox_coordinate_writes,
        rooms_writes: report.rooms_writes,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        reason: report.reason || null,
        objective: POLICY_CONTROLLER_OBJECTIVE,
        census_mode: report.census_mode,
        records_scanned: report.records_scanned,
        existing_records_updated: report.existing_records_updated,
        address_writes: report.address_writes,
        website_writes: report.website_writes,
        mapbox_coordinate_writes: report.mapbox_coordinate_writes,
        rooms_writes: report.rooms_writes,
        market_writes: report.market_writes,
        submarket_writes: report.submarket_writes,
        new_hotel_candidates_found: report.new_hotel_candidates_found,
        inserts: report.inserts,
        estimated_cost_usd: report.estimated_cost_usd,
        founder_approval_needed: report.founder_approval_needed,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: dataforseo-local-address-scale-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_DATAFORSEO_LOCAL_ADDRESS_SCALE_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] dataforseo-local-address-scale-v1 mode=${args.mode} census_mode=${args.censusMode || "field-completion-only"} address=${process.env.ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES || "0"}`
  );
  const report = await runDataForSeoLocalAddressScaleV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        (args.mode === "mission" || args.mode === "controlled") &&
        String(process.env.ENABLE_DATAFORSEO_VALIDATED_WRITES || "0") === "1" &&
        String(process.env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "1") === "0" &&
        String(process.env.ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES || "0") === "1"
    ),
    censusMode: args.censusMode || "field-completion-only",
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-dataforseo-local-address-scale-v1",
      date: today,
      process: "census",
      batch_name: "production_census_dataforseo_local_address_scale_v1",
      source_report:
        "reports/research-engine-v2/dataforseo-local-address-scale-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Scale DataForSEO local match_high Address (Medium) on Clean Core missing-address only; Mapbox eligibility pending until High or geocode-approved Medium; never phone/DFS coords/inserts.",
      proposed_code_change: "dataforseo-local-address-scale-v1.js",
      module_to_update:
        "lib/research-engine-v2/dataforseo-local-address-scale-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action:
        "Spot-check Medium addresses; decide Mapbox Medium geocode-approved policy",
      lane: "dataforseo_local_address_scale_v1",
      metrics: {
        records_updated: report.records_updated,
        address_writes: report.address_writes,
        mapbox_pending: report.mapbox_pending_address_confidence,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        reason: report.reason || null,
        objective: DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
        records_scanned: report.records_scanned,
        match_high_reviewed: report.match_high_reviewed,
        address_writes: report.address_writes,
        records_updated: report.records_updated,
        address_complete_before: report.address_complete_before,
        address_complete_after: report.address_complete_after,
        mapbox_eligible_after_address_writes:
          report.mapbox_eligible_after_address_writes,
        mapbox_pending_address_confidence:
          report.mapbox_pending_address_confidence,
        phone_candidates_held: report.phone_candidates_held,
        coordinate_candidates_held: report.coordinate_candidates_held,
        new_hotel_candidates_queued: report.new_hotel_candidates_queued,
        estimated_cost_usd: report.estimated_cost_usd,
        census_writes: report.census_writes,
        inserts: report.inserts,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: dataforseo-local-business-validated-write-v1
if (
  resolvedObjective ===
    MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_VALIDATED_WRITE_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] dataforseo-local-business-validated-write-v1 mode=${args.mode} census_mode=${args.censusMode || "field-completion-only"} website=${process.env.ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES || "0"} address=${process.env.ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES || "0"}`
  );
  const report = await runDataForSeoLocalBusinessValidatedWriteV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        (args.mode === "mission" || args.mode === "controlled") &&
        String(process.env.ENABLE_DATAFORSEO_VALIDATED_WRITES || "0") === "1" &&
        String(process.env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "1") === "0"
    ),
    censusMode: args.censusMode || "field-completion-only",
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-dataforseo-local-business-validated-write-v1",
      date: today,
      process: "census",
      batch_name: "production_census_dataforseo_local_business_validated_write_v1",
      source_report:
        "reports/research-engine-v2/dataforseo-local-business-validated-write-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "DataForSEO local match_high only: write Address (Medium) + Official Property URL when blank and non-OTA; never overwrite brand-official URL; never phone/coords/rooms/inserts.",
      proposed_code_change:
        "dataforseo-local-business-validated-write-v1.js",
      module_to_update:
        "lib/research-engine-v2/dataforseo-local-business-validated-write-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action:
        "Spot-check address writes; decide phone/coord policy next",
      lane: "dataforseo_local_business_validated_write_v1",
      metrics: {
        records_updated: report.records_updated,
        website_writes: report.website_writes,
        address_writes: report.address_writes,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        reason: report.reason || null,
        objective: DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
        match_high_reviewed: report.match_high_reviewed,
        website_writes: report.website_writes,
        address_writes: report.address_writes,
        records_updated: report.records_updated,
        address_conflicts: report.address_conflicts,
        website_conflicts: report.website_conflicts,
        phone_candidates_held: report.phone_candidates_held,
        coordinate_candidates_held: report.coordinate_candidates_held,
        new_hotel_candidates_queued: report.new_hotel_candidates_queued,
        census_writes: report.census_writes,
        inserts: report.inserts,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: dataforseo-local-business-enrichment-v1 (candidate-only)
if (
  resolvedObjective === MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_ENRICHMENT_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] dataforseo-local-business-enrichment-v1 mode=${args.mode} census_mode=${args.censusMode || "field-completion-only"} candidates_only=${process.env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "0"}`
  );
  const report = await runDataForSeoLocalBusinessEnrichmentV1Mission({
    argv,
    args,
    env: process.env,
    censusMode: args.censusMode || "field-completion-only",
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-dataforseo-local-business-enrichment-v1",
      date: today,
      process: "census",
      batch_name: "production_census_dataforseo_local_business_enrichment_v1",
      source_report:
        "reports/research-engine-v2/dataforseo-local-business-enrichment-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "DataForSEO Maps/local = candidate enrichment + discovery only; match_high required for future writes; no rooms from Maps; no auto-insert in field-completion-only; field-level write flags required separately for address/phone/website/coords.",
      proposed_code_change:
        "dataforseo-local-match.js + dataforseo-local-business-enrichment-v1.js",
      module_to_update:
        "lib/research-engine-v2/dataforseo-local-business-enrichment-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === DATAFORSEO_LOCAL_ENRICHMENT_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action:
        "Review write-policy recommendation; approve field-level flags before any Census writes",
      lane: "dataforseo_local_business_enrichment_v1",
      metrics: {
        queries_run: report.queries_run,
        estimated_cost_usd: report.estimated_cost_usd,
        high_confidence_matches: report.high_confidence_matches,
        new_hotel_candidates: report.new_hotel_candidates,
        census_writes: report.census_writes,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        reason: report.reason || null,
        objective: DATAFORSEO_LOCAL_ENRICHMENT_OBJECTIVE,
        existing_records_tested: report.existing_records_tested,
        markets_tested: report.markets_tested,
        queries_run: report.queries_run,
        estimated_cost_usd: report.estimated_cost_usd,
        high_confidence_matches: report.high_confidence_matches,
        medium_confidence_matches: report.medium_confidence_matches,
        new_hotel_candidates: report.new_hotel_candidates,
        address_candidates: report.address_candidates,
        phone_candidates: report.phone_candidates,
        website_candidates: report.website_candidates,
        coordinate_candidates: report.coordinate_candidates,
        census_writes: report.census_writes,
        inserts: report.inserts,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: dataforseo-validated-write-policy-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_DATAFORSEO_VALIDATED_WRITE_POLICY_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] dataforseo-validated-write-policy-v1 mode=${args.mode} census_mode=${args.censusMode || "field-completion-only"} validated_writes=${process.env.ENABLE_DATAFORSEO_VALIDATED_WRITES || "0"}`
  );
  const report = await runDataForSeoValidatedWritePolicyV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        (args.mode === "mission" || args.mode === "controlled") &&
        String(process.env.ENABLE_DATAFORSEO_VALIDATED_WRITES || "0") === "1" &&
        String(process.env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "1") === "0"
    ),
    censusMode: args.censusMode || "field-completion-only",
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-dataforseo-validated-write-policy-v1",
      date: today,
      process: "census",
      batch_name: "production_census_dataforseo_validated_write_policy_v1",
      source_report:
        "reports/research-engine-v2/dataforseo-validated-write-policy-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "DataForSEO discovers only; write Official Property URL / Rooms only after fetch+property match; brand_official property-specific URLs; hotel_official strict page validation; never Maps/address/phone/coords/Travel Weekly; never SERP snippet alone.",
      proposed_code_change:
        "dataforseo-validated-write-policy.js + census-autopilot-dataforseo-validated-write-policy-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-dataforseo-validated-write-policy-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === DATAFORSEO_VALIDATED_WRITE_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action:
        report.status === DATAFORSEO_VALIDATED_WRITE_STATUS.COMPLETE
          ? "Scale validated promotion beyond v2 200-set"
          : "Review hotel_official rejects + decide Maps/secondary policy",
      lane: "dataforseo_validated_write_policy_v1",
      metrics: {
        records_updated: report.records_updated,
        official_url_writes: report.official_url_writes,
        rooms_writes: report.rooms_writes,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        reason: report.reason || null,
        objective: DATAFORSEO_VALIDATED_WRITE_OBJECTIVE,
        candidates_reviewed: report.candidates_reviewed,
        candidates_validated: report.candidates_validated,
        official_url_writes: report.official_url_writes,
        hotel_official_accepted: report.hotel_official_accepted,
        hotel_official_rejected: report.hotel_official_rejected,
        rooms_writes: report.rooms_writes,
        records_updated: report.records_updated,
        address_phone_maps_held: report.address_phone_maps_held,
        airtable_writes: report.airtable_writes,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: dataforseo-discovery-pilot-v2 (candidate-only; never Census writes)
if (
  resolvedObjective === MISSION_OBJECTIVE_DATAFORSEO_DISCOVERY_PILOT_V2 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] dataforseo-discovery-pilot-v2 mode=${args.mode} census_mode=${args.censusMode || "field-completion-only"} candidates_only=1 validated_writes=0`
  );
  const report = await runDataForSeoDiscoveryPilotV2Mission({
    argv,
    args,
    env: process.env,
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-dataforseo-discovery-pilot-v2",
      date: today,
      process: "census",
      batch_name: "production_census_dataforseo_discovery_pilot_v2",
      source_report:
        "reports/research-engine-v2/dataforseo-discovery-pilot-v2.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "DataForSEO discovers candidates only: prefer brand_official / tourism / factsheet; reject affiliate mirrors (hoteles.com, *-hotels.com, rooms.aero); Travel Weekly = trusted secondary not official; Maps candidate-only; never Census SoT.",
      proposed_code_change:
        "dataforseo-candidate-classifier-v2 + dataforseo-discovery-pilot-v2 multilingual queries + parent prioritization",
      module_to_update:
        "lib/research-engine-v2/dataforseo-discovery-pilot.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === DATAFORSEO_DISCOVERY_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action:
        report.status === DATAFORSEO_DISCOVERY_STATUS.COMPLETE
          ? "Policy decision on steward validation write path"
          : "Review precision + top URLs; keep candidate-only until write policy approved",
      lane: "dataforseo_discovery_pilot_v2",
      metrics: {
        records_piloted: report.records_piloted,
        queries_run: report.queries_run,
        estimated_cost_usd: report.estimated_cost_usd,
        useful_candidates: report.useful_candidates_found,
        official_urls: report.official_hotel_urls_found,
        census_writes: report.census_writes,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        reason: report.reason || null,
        objective: DATAFORSEO_DISCOVERY_PILOT_OBJECTIVE,
        recommendation: report.recommendation,
        records_piloted: report.records_piloted,
        queries_run: report.queries_run,
        estimated_cost_usd: report.estimated_cost_usd,
        useful_candidates_found: report.useful_candidates_found,
        cost_per_useful_candidate: report.cost_per_useful_candidate,
        official_hotel_urls_found: report.official_hotel_urls_found,
        rooms_evidence_pages_found: report.rooms_evidence_pages_found,
        address_candidates_found: report.address_candidates_found,
        phone_candidates_found: report.phone_candidates_found,
        google_maps_candidates_found: report.google_maps_candidates_found,
        source_classifier_precision_estimate:
          report.source_classifier_precision_estimate,
        census_writes: report.census_writes,
        scale_estimate: report.scale_estimate,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: rooms-secondary-source-wave-2-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_ROOMS_SECONDARY_SOURCE_WAVE_2_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] rooms-secondary-source-wave-2-v1 mode=${args.mode} census_mode=${args.censusMode || "field-completion-only"} country=${args.country || "all"} secondary_rooms=${process.env.ENABLE_SECONDARY_ROOMS_SOURCES || "0"}`
  );
  const report = await runRoomsSecondarySourceWave2V1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        (args.mode === "mission" || args.mode === "controlled")
    ),
    censusMode: args.censusMode || "field-completion-only",
    country: args.country || null,
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-rooms-secondary-source-wave-2-v1",
      date: today,
      process: "census",
      batch_name: "production_census_rooms_secondary_source_wave_2_v1",
      source_report:
        "reports/research-engine-v2/production-census-rooms-secondary-source-wave-2-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Wave 2 rooms: ensure Rooms Evidence Tier schema; country discovery before inventing adapters; Colombia RNT fuzzy requires brand/distinctive tokens + no location conflict; never Official High for secondary; phone stays blocked.",
      proposed_code_change:
        "production-census-rooms-evidence-tier-schema.js + census-rooms-country-source-discovery.js + census-rooms-secondary-match.js v2 + census-autopilot-rooms-secondary-source-wave-2-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-rooms-secondary-source-wave-2-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === ROOMS_SECONDARY_WAVE_2_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action:
        report.status === ROOMS_SECONDARY_WAVE_2_STATUS.COMPLETE
          ? "Rooms coverage complete"
          : "Build Mexico SECTUR property-level rooms adapter; steward Colombia remainders",
      lane: "rooms_secondary_source_wave_2_v1",
      metrics: {
        updated: report.records_updated,
        rooms_written: report.rooms_written,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        reason: report.reason || null,
        objective: ROOMS_SECONDARY_WAVE_2_OBJECTIVE,
        records_updated: report.records_updated,
        records_inserted: report.records_inserted,
        rooms_written: report.rooms_written,
        phone_written: report.phone_written,
        airtable_writes: report.airtable_writes,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: rooms-count-completion-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_ROOMS_COUNT_COMPLETION_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] rooms-count-completion-v1 mode=${args.mode} census_mode=${args.censusMode || "field-completion-only"} secondary_rooms=${process.env.ENABLE_SECONDARY_ROOMS_SOURCES || "0"} secondary_phone=${process.env.ENABLE_SECONDARY_PHONE_SOURCES || "0"}`
  );
  const report = await runRoomsCountCompletionV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        (args.mode === "mission" || args.mode === "controlled")
    ),
    censusMode: args.censusMode || "field-completion-only",
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-rooms-count-completion-v1",
      date: today,
      process: "census",
      batch_name: "production_census_rooms_secondary_source_completion_v1",
      source_report:
        "reports/research-engine-v2/production-census-rooms-secondary-source-completion-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Rooms secondary sources require ENABLE_SECONDARY_ROOMS_SOURCES=1; never label secondary as Official High; Phone stays phone_secondary_source_policy_not_approved; every rooms write needs URL/Type/Confidence/Reviewed Date + evidence_tier in Notes until schema field exists.",
      proposed_code_change:
        "census-secondary-hotel-data-policy.js + census-rooms-secondary-match.js + census-autopilot-rooms-count-completion-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-rooms-count-completion-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === ROOMS_COUNT_COMPLETION_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action:
        report.status === ROOMS_COUNT_COMPLETION_STATUS.COMPLETE
          ? "Rooms coverage complete — consider Phone secondary decision separately"
          : "Continue rooms official adapters + country open-data; Phone secondary still not approved",
      lane: "rooms_count_completion_v1",
      metrics: {
        updated: report.records_updated,
        rooms_written: report.rooms_written,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        reason: report.reason || null,
        objective: ROOMS_COUNT_COMPLETION_V1_OBJECTIVE,
        records_updated: report.records_updated,
        records_inserted: report.records_inserted,
        rooms_written: report.rooms_written,
        phone_written: report.phone_written,
        airtable_writes: report.airtable_writes,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: commercial-fields-and-description-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_COMMERCIAL_FIELDS_AND_DESCRIPTION_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] commercial-fields-and-description-v1 mode=${args.mode} census_mode=${args.censusMode || "field-completion-only"} record=${args.recordId || "—"} code=${args.propertyCode || "—"}`
  );
  const report = await runCommercialFieldsAndDescriptionV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(
      argv.includes("--enable-production-writes") &&
        args.confirms?.writeToProductionCensus &&
        args.confirms?.safeWrites &&
        (args.mode === "mission" || args.mode === "controlled")
    ),
    censusMode: args.censusMode || "field-completion-only",
    recordId: args.recordId || null,
    propertyCode: args.propertyCode || null,
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-commercial-fields-and-description-v1",
      date: today,
      process: "census",
      batch_name: "production_census_commercial_fields_and_description_v1",
      source_report:
        "reports/research-engine-v2/production-census-commercial-fields-and-description-v1.json",
      issue_type: "learned_validation_rule",
      example_records: report.mx043?.record_id ? [report.mx043.record_id] : [],
      reusable_pattern:
        "Market/Submarket from approved maps only; descriptions from Census fields only; never invent phone/rooms; reject Choice central phones and rooms=25 defaults; secondary sources need founder approval.",
      proposed_code_change:
        "census-commercial-market-map.js + census-hotel-description-generator.js + census-autopilot-commercial-fields-and-description-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-commercial-fields-and-description-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === COMMERCIAL_FIELDS_DESCRIPTION_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action: "Continue commercial-fields-and-description-v1; founder decides secondary phone/rooms policy",
      lane: "commercial_fields_and_description_v1",
      metrics: {
        updated: report.records_updated,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger skip: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        reason: report.reason || null,
        objective: COMMERCIAL_FIELDS_DESCRIPTION_V1_OBJECTIVE,
        records_updated: report.records_updated,
        records_inserted: report.records_inserted,
        mx043: report.mx043,
        airtable_writes: report.airtable_writes,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

// Short-circuit: marriott-webhound-source-pattern-learning-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_MARRIOTT_WEBHOUND_SOURCE_PATTERN_LEARNING_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] marriott-webhound-source-pattern-learning-v1 mode=${args.mode} census_mode=${args.censusMode || "field-completion-only"} region=${args.region || "CALA"}`
  );
  const report = await runMarriottWebhoundSourcePatternLearningV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(enableProductionWrites && args.mode === "mission"),
    censusMode: args.censusMode || "field-completion-only",
    webhoundSessionId: process.env.WEBHOUND_MARRIOTT_SESSION_ID || null,
    extractLimit: Number(process.env.MARRIOTT_LEARN_EXTRACT_LIMIT || 80),
    skipChainV3: process.env.MARRIOTT_LEARN_SKIP_CHAIN_V3 === "1",
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-marriott-webhound-source-pattern-learning-v1",
      date: today,
      process: "census",
      batch_name: "production_census_marriott_webhound_source_pattern_learning_v1",
      source_report:
        "reports/research-engine-v2/production-census-marriott-webhound-source-pattern-learning-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Webhound discovers Marriott official source patterns only; Census writes require revalidated official source URL + High confidence. Never Webhound as SoT.",
      proposed_code_change: "marriott-*-adapter.js + census-autopilot-marriott-webhound-source-pattern-learning-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-marriott-webhound-source-pattern-learning-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === MARRIOTT_WEBHOUND_LEARNING_STATUS.BLOCKED ? "proposed" : "implemented",
      next_action: report.command_to_continue,
      lane: "marriott_webhound_source_pattern_learning_v1",
      metrics: {
        updated: report.records_updated,
        tested: report.adapter_learning?.records_tested,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective || MARRIOTT_WEBHOUND_LEARNING_V1_OBJECTIVE,
        records_updated: report.records_updated,
        records_inserted: report.records_inserted,
        extraction_success: report.adapter_learning?.extraction_success,
        bot_blocked: report.discovery?.bot_blocked_attempts,
        webhound_as_census_sot: false,
        airtable_writes: report.airtable_writes,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  if (report.status === MARRIOTT_WEBHOUND_LEARNING_STATUS.BLOCKED) process.exitCode = 1;
  process.exit(process.exitCode || 0);
}

// Short-circuit: full-latam-census-autopilot-v3 (gap-ledger self-directed controller)
if (
  resolvedObjective === MISSION_OBJECTIVE_FULL_LATAM_CENSUS_AUTOPILOT_V3 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] full-latam-census-autopilot-v3 mode=${args.mode} census_mode=${args.censusMode || "growth"} region=${args.region || "CALA"}`
  );
  const report = await runFullLatamCensusAutopilotV3Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(enableProductionWrites && args.mode === "mission"),
    censusMode: args.censusMode,
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-full-latam-v3",
      date: today,
      process: "census",
      batch_name: "production_census_full_latam_autopilot_v3",
      source_report:
        "reports/research-engine-v2/production-census-full-latam-autopilot-v3.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Gap-ledger Autopilot Controller v3: audit → prioritize → High writes → re-audit; no founder gate between passes; census-mode growth|field-completion-only|governance-only.",
      proposed_code_change: "census-autopilot-full-latam-v3.js",
      module_to_update: "lib/research-engine-v2/census-autopilot-full-latam-v3.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === FULL_LATAM_AUTOPILOT_V3_STATUS.BLOCKED ? "proposed" : "implemented",
      next_action: report.next_recommended_action,
      lane: "full_latam_census_autopilot_v3",
      metrics: {
        updated: report.records_updated,
        inserted: report.records_inserted,
        passes: report.passes_run,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective || FULL_LATAM_AUTOPILOT_V3_OBJECTIVE,
        census_mode: report.census_mode,
        records_updated: report.records_updated,
        records_inserted: report.records_inserted,
        passes_run: report.passes_run,
        clean_core_after_pct: report.after_scorecard?.percents?.clean_core,
        address_after_pct: report.after_scorecard?.percents?.address,
        airtable_writes: report.airtable_writes,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  if (report.status === FULL_LATAM_AUTOPILOT_V3_STATUS.BLOCKED) process.exitCode = 1;
  process.exit(process.exitCode || 0);
}

// Short-circuit: official-parent-level-2-completion-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] official-parent-level-2-completion-v1 mode=${args.mode} region=${args.region || "CALA"}`
  );
  const report = await runOfficialParentLevel2CompletionV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(enableProductionWrites && args.mode === "mission"),
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-official-parent-level-2-completion-v1",
      date: today,
      process: "census",
      batch_name: "production_census_official_parent_level_2_completion_v1",
      source_report:
        "reports/research-engine-v2/production-census-official-parent-level-2-completion-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Separate governance_review_required from data_quality_review_required; Level 2 runs for Clean Core even when Census Only / Public Hold / evidence-backed non-active.",
      proposed_code_change: "census-autopilot-official-parent-level-2-completion-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-official-parent-level-2-completion-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === OFFICIAL_PARENT_LEVEL_2_STATUS.BLOCKED ? "proposed" : "implemented",
      next_action: report.next_recommended_action,
      lane: "official_parent_level_2_completion_v1",
      metrics: {
        reclassify: report.reclassify_updates,
        level2: report.level2_updates,
        clean_core_after: report.after?.clean_core,
        hr_after: report.after?.human_review_required,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective || OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_OBJECTIVE,
        reclassify_updates: report.reclassify_updates,
        level2_updates: report.level2_updates,
        clean_core_before: report.before?.clean_core,
        clean_core_after: report.after?.clean_core,
        hr_before: report.before?.human_review_required,
        hr_after: report.after?.human_review_required,
        address_after: report.after?.address_complete,
        phone_after: report.after?.phone_complete,
        rooms_after: report.after?.rooms_complete,
        airtable_writes: report.airtable_writes,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  if (report.status === OFFICIAL_PARENT_LEVEL_2_STATUS.BLOCKED) process.exitCode = 1;
  process.exit(process.exitCode || 0);
}

// Short-circuit: official-parent-inventory-census-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_OFFICIAL_PARENT_INVENTORY_CENSUS_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] official-parent-inventory-census-v1 mode=${args.mode} region=${args.region || "CALA"} scope=${args.scope || "official-parent-inventory"}`
  );
  const report = await runOfficialParentInventoryCensusV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(enableProductionWrites && args.mode === "mission"),
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-official-parent-inventory-census-v1",
      date: today,
      process: "census",
      batch_name: "production_census_official_parent_inventory_census_v1",
      source_report:
        "reports/research-engine-v2/production-census-official-parent-inventory-census-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Official parent inventory discovery/coverage not Active/Live-only; Brand Governance Status; Census Only/Hold for non-active; Brand Setup promotion pack read-only.",
      proposed_code_change: "census-autopilot-official-parent-inventory-census-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-official-parent-inventory-census-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === OFFICIAL_PARENT_INVENTORY_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action: report.next_recommended_action,
      lane: "official_parent_inventory_census_v1",
      metrics: {
        inserts: report.inserts_applied,
        patches: report.updates_applied,
        clean_core_after: report.after?.clean_core,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective || OFFICIAL_PARENT_INVENTORY_CENSUS_V1_OBJECTIVE,
        inserts_applied: report.inserts_applied,
        updates_applied: report.updates_applied,
        clean_core_before: report.before?.clean_core,
        clean_core_after: report.after?.clean_core,
        promotion_pack_candidates: report.promotion_pack_candidates,
        airtable_writes: report.airtable_writes,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  if (report.status === OFFICIAL_PARENT_INVENTORY_STATUS.BLOCKED) process.exitCode = 1;
  process.exit(process.exitCode || 0);
}

if (
  resolvedObjective === MISSION_OBJECTIVE_LEVEL_2_SOURCE_EXTRACTION_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] level-2-source-extraction-v1 mode=${args.mode} region=${args.region || "CALA"} parent=${args.parentCompany || "all"}`
  );
  const report = await runLevel2SourceExtractionV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(enableProductionWrites && args.mode === "mission"),
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-level-2-source-extraction-v1",
      date: today,
      process: "census",
      batch_name: "production_census_level_2_source_extraction_v1",
      source_report:
        "reports/research-engine-v2/production-census-level-2-source-extraction-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Level 2 High Address/Phone/Rooms from Hilton/Choice CALA directories + official property JSON-LD; never invent; chain cala-census-completion after apply.",
      proposed_code_change: "census-autopilot-level-2-source-extraction-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-level-2-source-extraction-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === LEVEL_2_SOURCE_EXTRACTION_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action: report.next_recommended_action,
      lane: "level_2_source_extraction_v1",
      metrics: {
        updates: report.updates_applied,
        address_after: report.after?.address_complete,
        phone_after: report.after?.phone_complete,
        rooms_after: report.after?.rooms_complete,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update skipped: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective || LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE,
        updates_applied: report.updates_applied,
        address_complete: report.after?.address_complete,
        phone_complete: report.after?.phone_complete,
        rooms_complete: report.after?.rooms_complete,
        high_proposals: report.extraction?.counters?.high_proposals,
        chained: report.chained_cala_completion?.status || null,
      },
      null,
      2
    )
  );
  if (report.status === LEVEL_2_SOURCE_EXTRACTION_STATUS.BLOCKED) process.exitCode = 1;
  process.exit();
}

// Short-circuit: cala-census-completion-v1 (park dirty brands + full Level 2)
if (
  resolvedObjective === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] cala-census-completion-v1 mode=${args.mode} region=${args.region || "CALA"}`
  );
  const report = await runCalaCensusCompletionV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(enableProductionWrites && args.mode === "mission"),
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-cala-census-completion-v1",
      date: today,
      process: "census",
      batch_name: "production_census_cala_completion_v1",
      source_report:
        "reports/research-engine-v2/production-census-cala-completion-v1.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "CALA census completion: park dirty partner brand labels; complete Clean Core Level 2 from official sources; Brand Setup promotion pack remains read-only.",
      proposed_code_change: "census-autopilot-cala-census-completion-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-cala-census-completion-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === CALA_COMPLETION_STATUS.BLOCKED ? "proposed" : "implemented",
      next_action: report.next_recommended_action,
      lane: "cala_census_completion_v1",
      metrics: {
        updates: report.updates_applied,
        clean_core_after: report.after?.clean_core,
        address_after: report.after?.address_complete,
        coords_after: report.after?.lat_long_complete,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective,
        updates_applied: report.updates_applied,
        inserts_applied: report.inserts_applied,
        before: report.before,
        after: report.after,
        dirty_partner_park: report.dirty_partner_park,
        promotion_candidates: (report.brand_setup_promotion_pack?.candidates || []).length,
        run_dir: report.run_dir,
        airtable_writes: report.airtable_writes,
        brand_setup_writes: false,
        brand_explorer_writes: false,
      },
      null,
      2
    )
  );
  if (report.status === CALA_COMPLETION_STATUS.BLOCKED) process.exitCode = 1;
  process.exit(process.exitCode || 0);
}

// Short-circuit: brand-registry-resolution-v1 → then chains source-confirmed-census-v2
if (
  resolvedObjective === MISSION_OBJECTIVE_BRAND_REGISTRY_RESOLUTION_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] brand-registry-resolution-v1 mode=${args.mode} region=${args.region || "CALA"}`
  );
  const report = await runBrandRegistryResolutionV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(enableProductionWrites && args.mode === "mission"),
    chainSourceConfirmed: args.mode === "mission",
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-brand-registry-resolution-v1",
      date: today,
      process: "census",
      batch_name: "production_census_brand_registry_resolution_v1",
      source_report:
        "reports/research-engine-v2/production-census-brand-registry-resolution-v1.json",
      issue_type: "learned_validation_rule",
      example_records: (report.examples_before_after || [])
        .slice(0, 5)
        .map((e) => `${e.before}→${e.after}`),
      reusable_pattern:
        "Brand registry resolution: Accor code map + URL/catalog decode High remaps; SAM/partner dirty labels stewarded; Brand Setup promotion pack read-only; chain source-confirmed-census-v2.",
      proposed_code_change: "census-autopilot-brand-registry-resolution-v1.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-brand-registry-resolution-v1.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === BRAND_REGISTRY_RESOLUTION_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action: report.next_recommended_action,
      lane: "brand_registry_resolution_v1",
      metrics: {
        unknown_before: report.before?.unknown,
        unknown_after: report.after?.unknown,
        updates: report.high_brand_remaps_applied,
        status: report.status,
        chained: report.chained_source_confirmed?.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective,
        high_brand_remaps_applied: report.high_brand_remaps_applied,
        before: report.before,
        after: report.after,
        promotion_candidates: (report.promotion_candidates || []).length,
        chained_source_confirmed: report.chained_source_confirmed,
        run_dir: report.run_dir,
        airtable_writes: report.airtable_writes,
        brand_setup_writes: false,
        brand_explorer_writes: false,
      },
      null,
      2
    )
  );
  if (report.status === BRAND_REGISTRY_RESOLUTION_STATUS.BLOCKED) process.exitCode = 1;
  process.exit(process.exitCode || 0);
}

// Short-circuit: source-confirmed-census-v2 (brand steward resolution)
if (
  resolvedObjective === MISSION_OBJECTIVE_SOURCE_CONFIRMED_CENSUS_V2 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] source-confirmed-census-v2 mode=${args.mode} region=${args.region || "CALA"}`
  );
  const report = await runSourceConfirmedCensusV2Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites: Boolean(enableProductionWrites && args.mode === "mission"),
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-source-confirmed-census-v2",
      date: today,
      process: "census",
      batch_name: "production_census_source_confirmed_census_v2",
      source_report:
        "reports/research-engine-v2/production-census-source-confirmed-census-v2.json",
      issue_type: "learned_validation_rule",
      example_records: (report.examples_before_after || [])
        .slice(0, 5)
        .map((e) => `${e.before}→${e.after}`),
      reusable_pattern:
        "Source-confirmed census v2: official URL/slug/code maps High remaps; opaque codes stewarded; Brand Setup promotion candidates listed read-only.",
      proposed_code_change: "census-autopilot-source-confirmed-census-v2.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-source-confirmed-census-v2.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === SOURCE_CONFIRMED_STATUS.BLOCKED ? "proposed" : "implemented",
      next_action: report.next_recommended_action,
      lane: "source_confirmed_census_v2",
      metrics: {
        unknown_before: report.before?.unknown,
        unknown_after: report.after?.unknown,
        updates: report.updates_applied,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective,
        updates_applied: report.updates_applied,
        before: report.before,
        after: report.after,
        run_dir: report.run_dir,
        airtable_writes: report.airtable_writes,
        brand_setup_writes: false,
        brand_explorer_writes: false,
      },
      null,
      2
    )
  );
  if (report.status === SOURCE_CONFIRMED_STATUS.BLOCKED) process.exitCode = 1;
  process.exit(process.exitCode || 0);
}

// Short-circuit: coverage-steward-resolution-v1
if (
  resolvedObjective === MISSION_OBJECTIVE_COVERAGE_STEWARD_RESOLUTION_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] coverage-steward-resolution-v1 mode=${args.mode} region=${args.region || "CALA"}`
  );
  const report = await runCoverageStewardResolutionMission({
    args,
    env: process.env,
    enableProductionWrites: Boolean(enableProductionWrites && args.mode === "mission"),
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-coverage-steward-resolution-v1",
      date: today,
      process: "census",
      batch_name: "production_census_coverage_steward_resolution_v1",
      source_report:
        "reports/research-engine-v2/production-census-coverage-steward-resolution-v1.json",
      issue_type: "learned_validation_rule",
      example_records: (report.inserted_hotels || [])
        .slice(0, 5)
        .map((m) => m.official_property_id || m.property_name),
      reusable_pattern:
        "Coverage steward resolution: official URL/name/IATA city + brand slug normalize; High inserts only; leave ambiguous stewarded.",
      proposed_code_change: "census-autopilot-coverage-steward-resolution.js",
      module_to_update:
        "lib/research-engine-v2/census-autopilot-coverage-steward-resolution.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === COVERAGE_STEWARD_STATUS.BLOCKED ? "proposed" : "implemented",
      next_action: report.next_recommended_action,
      lane: "coverage_steward_resolution",
      metrics: {
        steward_before: report.steward_cases_before,
        inserted: report.inserted_count,
        remaining: report.unresolved_count,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective || COVERAGE_STEWARD_RESOLUTION_OBJECTIVE,
        steward_cases_before: report.steward_cases_before,
        inserted_count: report.inserted_count,
        unresolved_count: report.unresolved_count,
        census_before: report.census_before,
        census_after: report.census_after,
        airtable_writes: report.airtable_writes,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  if (report.status === COVERAGE_STEWARD_STATUS.BLOCKED) process.exitCode = 1;
  process.exit(process.exitCode || 0);
}

// Short-circuit: coverage-reconciliation-v1 (controlled or mission)
if (
  resolvedObjective === MISSION_OBJECTIVE_COVERAGE_RECONCILIATION_V1 &&
  (args.mode === "controlled" || args.mode === "mission" || args.mode === "dry-run")
) {
  console.log(
    `[census:autopilot] coverage-reconciliation-v1 mode=${args.mode} parent=${args.parentCompany || "(active)"} brand=${args.brand || "(all)"} region=${args.region || "CALA"}`
  );
  const report = await runCoverageReconciliationMission({
    args,
    env: process.env,
    enableProductionWrites: Boolean(enableProductionWrites && args.mode === "mission"),
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-coverage-reconciliation-v1",
      date: today,
      process: "census",
      batch_name: "production_census_coverage_reconciliation_v1",
      source_report:
        "reports/research-engine-v2/production-census-coverage-reconciliation-v1.json",
      issue_type: "learned_validation_rule",
      example_records: (report.missing_hotels || []).slice(0, 5).map((m) => m.identity_key || m.property_name),
      reusable_pattern:
        "Official brand inventory vs Hotel Property Census coverage reconciliation; High-confidence inserts only; no fuzzy/name-only; no lat/long/phone/rooms on insert.",
      proposed_code_change: "census-autopilot-coverage-reconciliation.js",
      module_to_update: "lib/research-engine-v2/census-autopilot-coverage-reconciliation.js",
      fixture_added: false,
      test_added: true,
      status: report.status === COVERAGE_STATUS.BLOCKED ? "proposed" : "implemented",
      next_action: report.next_recommended_action,
      lane: "coverage_reconciliation",
      metrics: {
        official_inventory_count: report.official_inventory_count,
        census_inventory_count: report.census_inventory_count,
        inserted_count: report.inserted_count,
        status: report.status,
        brand: report.brand,
        parent_company: report.parent_company,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective || COVERAGE_RECONCILIATION_OBJECTIVE,
        official_inventory_count: report.official_inventory_count,
        census_inventory_count: report.census_inventory_count,
        missing_high: report.coverage_counts?.missing_high_confidence || 0,
        missing_steward: report.coverage_counts?.missing_needs_steward || 0,
        inserted_count: report.inserted_count,
        airtable_writes: report.airtable_writes,
        run_dir: report.run_dir,
      },
      null,
      2
    )
  );
  if (report.status === COVERAGE_STATUS.BLOCKED) process.exitCode = 1;
  process.exit(process.exitCode || 0);
}

// Short-circuit: mission mode (founder-approved multi-phase Clean Census; no per-step ChatGPT)
if (args.mode === "mission") {
  console.log(
    `[census:autopilot] mission mode objective=${args.objective || MISSION_OBJECTIVE_CLEAN_CENSUS_V1} (founder CLI = approval; no per-phase ChatGPT gate)`
  );
  const report = await runCleanCensusV1Mission({
    argv,
    args,
    env: process.env,
    enableProductionWrites,
    log: (msg) => console.log(msg),
  });

  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const isComplete = report.objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1;
    const entry = {
      id: isComplete
        ? "census-autopilot-complete-census-v1-mission"
        : "census-autopilot-clean-census-v1-mission",
      date: today,
      process: "census",
      batch_name: isComplete
        ? "production_census_complete_census_v1_mission"
        : "production_census_clean_census_v1_mission",
      source_report: isComplete
        ? "reports/research-engine-v2/production-census-complete-census-v1-mission.json"
        : "reports/research-engine-v2/production-census-clean-census-v1-mission.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern: isComplete
        ? "Complete Census v1 mission fills Level 2 (State/Address/Coords/Phone/Rooms) for Clean Core existing records only; no inserts; soft-continues source gaps."
        : "Mission mode runs phased Clean Census v1 until High writes exhaust without per-phase ChatGPT approval; soft-continues steward/source gaps; hard-stops only on true safety.",
      proposed_code_change: "census-autopilot-mission.js",
      module_to_update: "lib/research-engine-v2/census-autopilot-mission.js",
      fixture_added: false,
      test_added: true,
      status:
        report.status === MISSION_STATUS.BLOCKED ||
        report.status === COMPLETE_CENSUS_MISSION_STATUS.BLOCKED
          ? "proposed"
          : "implemented",
      next_action: report.next_recommended_action,
      lane: "mission",
      metrics: {
        updates_applied: report.updates_applied,
        inserts_applied: report.inserts_applied,
        status: report.status,
        objective: report.objective,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        objective: report.objective,
        updates_applied: report.updates_applied,
        inserts_applied: report.inserts_applied,
        run_dir: report.run_dir,
        airtable_writes: report.airtable_writes,
        before: report.before,
        after: report.after,
      },
      null,
      2
    )
  );
  if (
    report.status === MISSION_STATUS.BLOCKED ||
    report.status === COMPLETE_CENSUS_MISSION_STATUS.BLOCKED
  ) {
    process.exitCode = 1;
  }
  process.exit(process.exitCode || 0);
}

// Short-circuit: production-cycle mode (founder-approved continuous High writes)
if (args.mode === "production-cycle") {
  console.log(
    `[census:autopilot] production-cycle mode (founder CLI = approval; no per-bundle ChatGPT gate)`
  );
  const report = await runProductionCycle({
    argv,
    args,
    env: process.env,
    enableProductionWrites,
    log: (msg) => console.log(msg),
  });

  // Mirror key artifacts to reports/ + docs/
  try {
    const docsPath = join(DOCS, "production-census-autopilot-production-cycle.md");
    writeFileSync(
      docsPath,
      [
        `# Production Census Autopilot — Production Cycle`,
        ``,
        `Status: **${report.status}**`,
        ``,
        `- Records: ${report.records_before} → ${report.records_after}`,
        `- Inserts applied: ${report.inserts_applied}`,
        `- Updates applied: ${report.updates_applied}`,
        `- Steward cases: ${report.steward_cases}`,
        `- Run dir: \`${report.run_dir}\``,
        ``,
        `See \`${report.run_dir}/final-summary.json\`.`,
        ``,
      ].join("\n"),
      "utf8"
    );
    if (report.run_dir && existsSync(join(report.run_dir, "final-summary.json"))) {
      writeFileSync(
        join(REPORTS, "production-census-autopilot-production-cycle.json"),
        readFileSync(join(report.run_dir, "final-summary.json"), "utf8"),
        "utf8"
      );
      writeFileSync(
        join(REPORTS, "production-census-autopilot-production-cycle.md"),
        readFileSync(join(report.run_dir, "final-summary.md"), "utf8"),
        "utf8"
      );
    }
  } catch (err) {
    console.warn(`[census:autopilot] production-cycle report mirror failed: ${err?.message || err}`);
  }

  // Learning ledger
  try {
    const today = new Date().toISOString().slice(0, 10);
    const seed = buildSeedLearningEntries();
    const entry = {
      id: "census-autopilot-production-cycle",
      date: today,
      process: "census",
      batch_name: "production_census_autopilot_production_cycle",
      source_report: "reports/research-engine-v2/production-census-autopilot-production-cycle.json",
      issue_type: "learned_validation_rule",
      example_records: [],
      reusable_pattern:
        "Production-cycle mode applies High allowlisted Hotel Property Census writes until exhausted without per-bundle ChatGPT approval; steward Choice Radisson Individuals member-of/Unknown city; soft-defer geocode.",
      proposed_code_change: "census-autopilot-production-cycle.js",
      module_to_update: "lib/research-engine-v2/census-autopilot-production-cycle.js",
      fixture_added: false,
      test_added: true,
      status: report.status === PRODUCTION_CYCLE_STATUS.BLOCKED ? "proposed" : "implemented",
      next_action: report.next_recommended_action,
      lane: "production-cycle",
      metrics: {
        inserts_applied: report.inserts_applied,
        updates_applied: report.updates_applied,
        steward_cases: report.steward_cases,
        status: report.status,
      },
    };
    const idx = seed.findIndex((e) => e.id === entry.id);
    if (idx >= 0) seed[idx] = { ...seed[idx], ...entry };
    else seed.push(entry);
    const ledger = buildLedgerDocument(seed);
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerJson), JSON.stringify(ledger, null, 2), "utf8");
    writeFileSync(join(ROOT, LEARNING_PATHS.ledgerMd), renderLedgerMarkdown(ledger), "utf8");
    runBatchLearningAudit(ledger);
  } catch (err) {
    console.warn(`[census:autopilot] learning ledger update failed: ${err?.message || err}`);
  }

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        records_before: report.records_before,
        records_after: report.records_after,
        inserts_applied: report.inserts_applied,
        updates_applied: report.updates_applied,
        steward_cases: report.steward_cases,
        run_dir: report.run_dir,
        airtable_writes: report.airtable_writes,
      },
      null,
      2
    )
  );
  if (report.status === PRODUCTION_CYCLE_STATUS.BLOCKED) process.exitCode = 1;
  process.exit(process.exitCode || 0);
}

// Short-circuit: apply + --approval-bundle → frozen writes only (no orchestrator re-plan)
if (args.mode === "apply" && args.approvalBundle) {
  await runApprovalBundleBoundApply();
  process.exit(process.exitCode || 0);
}

async function listCensusRecords(baseId, token, fields) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await new Promise((r) => setTimeout(r, 120));
  } while (offset);
  return out;
}

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

async function loadLiveAutopilotContext(modeArgs) {
  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    return {
      ok: false,
      error: "missing_airtable_credentials_for_live_context",
    };
  }

  const metaRes = await fetch(
    `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(bases.target_base_id)}/tables`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const metaJson = await metaRes.json();
  if (!metaRes.ok) {
    return { ok: false, error: `meta_tables_${metaRes.status}` };
  }
  const table = (metaJson.tables || []).find(
    (t) => t.id === CENSUS_TABLE_ID || t.name === "Hotel Property Census"
  );
  const writeTargetCheck = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    baseId: bases.target_base_id,
    tableName: table?.name || "Hotel Property Census",
    tableId: table?.id || CENSUS_TABLE_ID,
  });
  if (!writeTargetCheck.ok || !table || table.id !== productionHotelPropertyCensus.tableId) {
    return {
      ok: false,
      error: BLOCKED_WRONG_CENSUS_TARGET,
      write_target: writeTargetCheck,
      detail:
        !table
          ? "Hotel Property Census table not found on Deal Capture Platform"
          : `expected tableId ${productionHotelPropertyCensus.tableId}, got ${table.id}`,
    };
  }
  const liveFieldNames = (table?.fields || []).map((f) => f.name);
  const schema = inspectRoomsKeysSchemaStatus(liveFieldNames);
  const schemaV114Ready = !schema.needs_v114_schema;
  const providerReady = evaluateProviderReadiness();
  const canonicalFieldExists = liveFieldNames.includes("Canonical Property Name");
  const continentFieldExists = liveFieldNames.includes("Continent");
  const subContinentFieldExists = liveFieldNames.includes("Sub-Continent");
  const marketFieldExists = liveFieldNames.includes("Market");
  const submarketFieldExists = liveFieldNames.includes("Submarket");

  // Include key-field matrix (+ identity helpers) so completion queues see live values
  const censusReadFields = [
    ...new Set([
      MAP_FIRST_PASS.identityKey,
      MAP_FIRST_PASS.propertyName,
      MAP_FIRST_PASS.canonicalPropertyName,
      MAP_FIRST_PASS.currentBrand,
      MAP_FIRST_PASS.brandSlug,
      MAP_FIRST_PASS.country,
      MAP_FIRST_PASS.city,
      MAP_FIRST_PASS.stateRegion,
      MAP_FIRST_PASS.address,
      MAP_FIRST_PASS.affiliationStatus,
      MAP_FIRST_PASS.sourceUrl,
      MAP_FIRST_PASS.officialUrl,
      MAP_FIRST_PASS.marketSubmarket,
      MAP_FIRST_PASS.humanReview,
      MAP_FIRST_PASS.family,
      "Production Use Status",
      "Source Confidence",
      "Brand Family",
      "Data Confidence Tier",
      "Identity Confidence",
      "Phone",
      "Rooms / Keys",
      "Continent",
      "Sub-Continent",
      "Market",
      "Submarket",
      ...KEY_FIELD_MATRIX.map((f) => f.airtable),
    ]),
  ].filter((f) => liveFieldNames.includes(f));

  if (!canonicalFieldExists) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          status: "canonical_property_name_field_missing",
          error: "Canonical Property Name field missing on Hotel Property Census — do not invent schema",
        },
        null,
        2
      )
    );
  }

  const censusRecords = await listCensusRecords(
    bases.target_base_id,
    token,
    censusReadFields
  );

  let proposals = [];
  let roomsReport = null;
  let nameCleanupReport = null;
  let orchestration = null;
  const targeted = modeArgs.queue || null;
  const needsProposals =
    modeArgs.mode === "controlled" || modeArgs.mode === "dry-run" || modeArgs.live;

  const limit = modeArgs.maxRecords
    ? modeArgs.maxRecords
    : modeArgs.runUntilComplete
      ? 10000
      : modeArgs.batchSize || 250;

  if (needsProposals) {
    const routed = routeAutopilotQueues({
      parentCompany: modeArgs.parentCompany,
      region: modeArgs.region,
      country: modeArgs.country,
      mode: modeArgs.mode,
      geocodeProviderReady: providerReady.approved_for_geocode_apply,
      schemaV114Ready,
    });
    const priorityPlan = buildFastestSafePriorityPlan(routed.queues, {
      geocodeProviderReady: providerReady.approved_for_geocode_apply,
      schemaV114Ready,
    });

    console.log(
      `[census:autopilot] multi-queue orchestrator (${
        targeted ? `targeted=${targeted}` : "default=fastest-safe all eligible"
      }, limit=${limit})…`
    );

    orchestration = await orchestrateAutopilotQueues({
      orderedQueueIds: priorityPlan.ordered_queue_ids,
      targetedQueue: targeted,
      limit,
      parentCompany: modeArgs.parentCompany,
      region: modeArgs.region || "CALA",
      censusRecords,
      geocodeProviderReady: providerReady.approved_for_geocode_apply,
      schemaV114Ready,
      canonicalFieldExists,
      continentFieldExists,
      subContinentFieldExists,
      marketFieldExists,
      submarketFieldExists,
      forAutopilot: true,
      log: (msg) => console.log(msg),
    });

    proposals = orchestration.proposals || [];
    roomsReport = orchestration.roomsReport || null;
    nameCleanupReport = orchestration.nameCleanupReport || null;

    console.log(
      `[census:autopilot] orchestrator done high=${proposals.length} executed=${(
        orchestration.queues_executed || []
      ).join(",") || "(none)"} soft_deferred=${(
        orchestration.queues_soft_deferred || []
      ).join(",") || "(none)"} skipped=${(orchestration.queues_skipped || []).join(",") || "(none)"}`
    );
  }

  return {
    ok: true,
    liveFieldNames,
    censusRecords,
    schemaV114Ready,
    schema,
    proposals,
    roomsReport,
    nameCleanupReport,
    orchestration,
    providerReady,
    canonicalFieldExists,
    continentFieldExists,
    subContinentFieldExists,
    marketFieldExists,
    submarketFieldExists,
  };
}

const needsLive =
  !args.resume &&
  (args.scope === "active-brand-setup" ||
    args.mode === "controlled" ||
    args.mode === "dry-run" ||
    args.mode === "plan" ||
    args.live);

let liveOpts = {};
if (needsLive) {
  console.log(
    `[census:autopilot] loading live Census context (mode=${args.mode}, queue=${args.queue || "default:fastest-safe"})…`
  );
  const live = await loadLiveAutopilotContext(args);
  if (!live.ok) {
    console.error(JSON.stringify({ ok: false, error: live.error }, null, 2));
    process.exit(1);
  }
  liveOpts = {
    liveFieldNames: live.liveFieldNames,
    censusRecords: live.censusRecords,
    schemaV114Ready: live.schemaV114Ready,
    proposals: live.proposals,
    roomsReport: live.roomsReport,
    nameCleanupReport: live.nameCleanupReport,
    orchestration: live.orchestration,
  };
  console.log(
    `[census:autopilot] census=${live.censusRecords.length} fields=${live.liveFieldNames.length} schema_v114=${live.schemaV114Ready}`
  );
}

const result = await runCensusAutopilot(argv, {
  liveDryRun: Boolean(args.live),
  enableProductionWrites,
  ...liveOpts,
});

if (!result.ok && result.error) {
  console.error(JSON.stringify({ ok: false, error: result.error, status: result.status }, null, 2));
  process.exit(1);
}

// Persist queue-execution + multi-queue approval artifacts
if (liveOpts.orchestration && result.run_dir) {
  const orch = liveOpts.orchestration;
  const execReport = {
    ...orch.execution_report,
    run_id: result.run_id,
    mode: args.mode,
    scope: args.scope,
    region: args.region,
    strategy: args.strategy || "fastest-safe",
  };
  writeJson(join(result.run_dir, "queue-execution-report.json"), execReport);
  writeMd(
    join(result.run_dir, "queue-execution-report.md"),
    renderQueueExecutionMarkdown(execReport)
  );

  if (args.mode === "controlled") {
    const bundle = buildMultiQueueApprovalBundle({
      run_id: result.run_id,
      mode: args.mode,
      scope: args.scope,
      region: args.region,
      strategy: args.strategy || "fastest-safe",
      batch_size: args.batchSize,
      proposals: liveOpts.proposals || [],
      blocked: orch.blocked || [],
      queues_executed: orch.queues_executed,
      queues_skipped: orch.queues_skipped,
      queues_soft_deferred: orch.queues_soft_deferred,
      steward: result.batch_result?.steward_review_queue || [],
      webhound: result.batch_result?.webhound_candidates || { candidates: [], capped_at: 25 },
    });
    writeJson(join(result.run_dir, "approval-bundle.json"), bundle);

    const yieldDiag = buildSourceYieldDiagnostic({
      run_id: result.run_id,
      mode: args.mode,
      orchestration: orch,
      proposals: liveOpts.proposals || [],
      family_gaps: {
        note: "See production-census-autopilot-source-yield-diagnostic.md for full family gap table",
      },
    });
    writeJson(join(result.run_dir, "source-yield-diagnostic.json"), yieldDiag);
    writeMd(
      join(result.run_dir, "source-yield-diagnostic.md"),
      renderSourceYieldDiagnosticMarkdown(yieldDiag)
    );

    const {
      getUnresolvedSourcePatterns,
    } = await import("../lib/research-engine-v2/census-autopilot-family-directory-adapters.js");
    const wh = buildWebhoundLearningCandidates({
      unresolved_patterns: getUnresolvedSourcePatterns({ minCount: 2 }),
    });
    writeJson(join(result.run_dir, "webhound-candidates.json"), wh);
  }
}

// Persist property name cleanup reports when that queue produced a report
if (liveOpts.nameCleanupReport && (args.queue === "property_name_cleanup" || !args.queue)) {
  const report = {
    ...liveOpts.nameCleanupReport,
    autopilot_run_id: result.run_id,
    autopilot_run_dir: result.run_dir,
    airtable_writes: Boolean(result.airtable_writes),
    brand_explorer_writes: false,
    brand_setup_writes: false,
    controlled_mode: args.mode === "controlled",
    validation: {
      airtable_writes: false,
      brand_explorer_writes: false,
      brand_setup_writes: false,
      protected_fields: false,
      owner_operator_date_writes: false,
      coordinates_changed: false,
      rooms_changed: false,
      descriptions_changed: false,
      held_records_included: false,
      brand_unconfirmed_included: false,
    },
  };
  if (!report.status) report.status = NAME_CLEANUP_STATUS.BLOCKED;

  if (args.queue === "property_name_cleanup") {
    writeJson(join(REPORTS, "production-census-property-name-cleanup-queue.json"), report);
    writeMd(
      join(REPORTS, "production-census-property-name-cleanup-queue.md"),
      renderPropertyNameCleanupMarkdown(report)
    );
    writeMd(
      join(DOCS, "production-census-property-name-cleanup-queue.md"),
      renderPropertyNameCleanupMarkdown(report)
    );
  }
  if (result.run_dir) {
    writeJson(join(result.run_dir, "property-name-cleanup-queue.json"), report);
    writeMd(
      join(result.run_dir, "property-name-cleanup-queue.md"),
      renderPropertyNameCleanupMarkdown(report)
    );
  }
}

console.log(
  JSON.stringify(
    {
      ok: result.ok !== false,
      status: result.status,
      queue_status: liveOpts.nameCleanupReport?.status || null,
      run_id: result.run_id,
      run_dir: result.run_dir,
      mode: args.mode,
      scope: args.scope,
      strategy: args.strategy,
      queue: args.queue || null,
      queue_mode: args.queue ? "targeted" : "fastest-safe-all-eligible",
      queues_executed: liveOpts.orchestration?.queues_executed || null,
      queues_skipped: liveOpts.orchestration?.queues_skipped || null,
      queues_soft_deferred: liveOpts.orchestration?.queues_soft_deferred || null,
      parent_company: args.parentCompany || result.summary?.parent_company,
      region: args.region || result.summary?.region,
      batch_size: args.batchSize,
      max_records: args.maxRecords,
      run_until_complete: args.runUntilComplete,
      completion_status: result.summary?.completion_status || result.batch_result?.completion_status,
      airtable_writes: result.airtable_writes,
      brand_explorer_writes: false,
      brand_setup_writes: false,
      schema_v114_ready: liveOpts.schemaV114Ready ?? null,
      high_write_proposals: liveOpts.proposals?.length ?? null,
      name_cleanup_summary: liveOpts.nameCleanupReport?.summary || null,
      summary: result.summary,
      warnings: result.warnings || args.warnings,
    },
    null,
    2
  )
);
