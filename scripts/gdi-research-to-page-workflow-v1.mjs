#!/usr/bin/env node
/**
 * GDI research-to-page workflow coordinator (v1 + v2 reuse/discovery stages).
 *
 * Single entry point over existing modules. Defaults to dry-run / preview.
 * Publication apply is explicit (--authorize-publish).
 * Webhound is RETIRED — never invoked.
 *
 * Usage:
 *   node scripts/gdi-research-to-page-workflow-v1.mjs --hotel rec0eTEnCqlsGJJt9 --dry-run
 *   node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recnPB3Q02NO8Ma1o --run-discovery --stage discover_gaps
 *   node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage preview
 *
 * Stages (resume via --resume-run <id> + stage ledger):
 *   validate_config → reuse_evidence → discover_gaps → assess_fit →
 *   identify_people → enrich_contacts → evaluate_eligibility →
 *   generate_cards → prepare_manifest → preview_verify → apply_publish
 *
 * v2 wiring inside those stages:
 *   reuse_evidence → reuse_prior_intelligence ledger
 *   discover_gaps → Parallel adapter (replay default; live plan only)
 *   assess_fit → independent hotel fit
 *   enrich_contacts → targeted contact plan (Surfe off by default)
 *   generate_cards → drawer completeness + optional Jev advisory
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createHash, randomBytes } from "node:crypto";
import {
  buildReusePriorIntelligenceLedger,
  reuseLedgerToCsv,
  researchPathSelectionToCsv,
} from "../lib/group-demand-intelligence/workflow/reuse-prior-intelligence-v1.js";
import {
  peIntelligenceToCsv,
  peSupportedCandidatesToDraftOpportunities,
} from "../lib/group-demand-intelligence/workflow/private-event-intelligence-v1.js";
import { buildPrivateEventIntelligence } from "../lib/group-demand-intelligence/workflow/private-event-intelligence-v1.js";
import { assessIndependentHotelFitBatch } from "../lib/group-demand-intelligence/workflow/independent-hotel-fit-v1.js";
import {
  runParallelDiscoveryStage,
  writeDiscoveryStageArtifacts,
  DISCOVERY_MODE,
} from "../lib/group-demand-intelligence/workflow/parallel-discovery-adapter-v1.js";
import {
  runNativeGeoResearchStage,
  writeNativeGeoArtifacts,
} from "../lib/group-demand-intelligence/workflow/native-geo-research-stage-v1.js";
import { selectResearchPaths } from "../lib/group-demand-intelligence/workflow/research-path-selection-v1.js";
import { buildOccasionDraftsFromPrograms } from "../lib/group-demand-intelligence/workflow/programs-occasion-draft-v1.js";
import { proposeHotelVenueFits } from "../lib/group-demand-intelligence/workflow/proposed-hotel-venue-fit-v1.js";
import { buildTargetedContactPlan } from "../lib/group-demand-intelligence/workflow/targeted-contact-plan-v1.js";
import { executeTargetedContactResolution } from "../lib/group-demand-intelligence/workflow/execute-targeted-contact-resolution-v1.js";
import { buildDraftReviewPackage } from "../lib/group-demand-intelligence/workflow/draft-review-package-v1.js";
import { auditAirtableReuseForHotel } from "../lib/group-demand-intelligence/workflow/airtable-reuse-audit-v1.js";
import {
  reportDrawerCompleteness,
  mapLabCardToDrawerShape,
} from "../lib/group-demand-intelligence/workflow/drawer-content-contract-v1.js";
import {
  runJevAdvisoryShadow,
  writeJevAdvisoryArtifacts,
} from "../lib/group-demand-intelligence/workflow/jev-advisory-shadow-v1.js";
import { WEBHOUND_PROVIDER_STATUS } from "../lib/group-demand-intelligence/workflow/webhound-retired-v1.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}
function has(flag) {
  return process.argv.includes(flag);
}

const STAGES = [
  "validate_config",
  "reuse_evidence",
  "discover_gaps",
  "assess_fit",
  "identify_people",
  "enrich_contacts",
  "evaluate_eligibility",
  "generate_cards",
  "prepare_manifest",
  "preview_verify",
  "apply_publish",
];

const hotelId = String(arg("--hotel", "") || "").trim();
const horizon = String(arg("--horizon", "18m") || "18m").trim();
const budgetUsd = Number(arg("--budget-usd", "5") || 5);
const budgetSearches = Number(arg("--budget-searches", "60") || 60);
const budgetPages = Number(arg("--budget-pages", "100") || 100);
const targetEnv = String(arg("--target-env", "local") || "local").trim();
const adapter = String(arg("--adapter", "filesystem") || "filesystem").trim();
const packageDirArg = arg("--package-dir");
const outDir =
  arg("--out-dir") ||
  path.join(ROOT, "reports/gdi-repeatable-workflow-v1");
const resumeRun = arg("--resume-run");
const stageOnly = arg("--stage");
const authorizePublish = has("--authorize-publish");
const dryRun = !authorizePublish || has("--dry-run");
const runDiscovery = has("--run-discovery");
const authorizePaidDiscovery = has("--authorize-paid-discovery");
const authorizeSurfe = has("--authorize-surfe");
const authorizeNative = has("--authorize-native");
const skipAirtableReuse = has("--skip-airtable-reuse");
const runJevShadow = has("--jev-shadow");
const discoveryModeArg = String(arg("--discovery-mode", "") || "")
  .trim()
  .toLowerCase();
/** Explicit discovery execution kind when --run-discovery is set. */
function resolveDiscoveryMode() {
  if (!runDiscovery) return DISCOVERY_MODE.DRY_RUN_PLAN;
  if (discoveryModeArg === "plan" || discoveryModeArg === "dry_run_plan") {
    return DISCOVERY_MODE.DRY_RUN_PLAN;
  }
  if (discoveryModeArg === "live-mock" || discoveryModeArg === "live_mock") {
    return DISCOVERY_MODE.LIVE_MOCK;
  }
  if (discoveryModeArg === "live") return DISCOVERY_MODE.LIVE;
  if (authorizePaidDiscovery) return DISCOVERY_MODE.LIVE;
  return DISCOVERY_MODE.REPLAY;
}
const discoveryMode = resolveDiscoveryMode();

// Clear skip-airtable env unless explicitly opted out for this run
if (!skipAirtableReuse && process.env.GDI_V2_SKIP_AIRTABLE === "1") {
  delete process.env.GDI_V2_SKIP_AIRTABLE;
}

if (!hotelId) {
  console.error("Required: --hotel <canonicalHotelId>");
  process.exit(2);
}

const runId =
  resumeRun ||
  `gdi_workflow_${hotelId.slice(0, 8)}_${new Date()
    .toISOString()
    .replace(/[:.]/g, "-")}_${randomBytes(3).toString("hex")}`;

const runDir = path.join(outDir, "runs", runId);
fs.mkdirSync(runDir, { recursive: true });
const ledgerPath = path.join(runDir, "stage-completion-ledger.json");

function loadLedger() {
  if (fs.existsSync(ledgerPath)) {
    return JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  }
  return {
    runId,
    hotelId,
    horizon,
    targetEnv,
    adapter,
    budget: {
      maxUsd: budgetUsd,
      maxSearches: budgetSearches,
      maxPages: budgetPages,
      usedUsd: 0,
      usedSearches: 0,
      usedPages: 0,
      exhausted: false,
      incompleteCoverageReason: null,
    },
    authorizePublish,
    dryRun,
    startedAt: new Date().toISOString(),
    stages: {},
    remainingManualSteps: [],
    failures: [],
  };
}

function saveLedger(ledger) {
  ledger.updatedAt = new Date().toISOString();
  fs.writeFileSync(ledgerPath, JSON.stringify(ledger, null, 2) + "\n");
}

function mark(ledger, stage, status, detail = {}) {
  ledger.stages[stage] = {
    status,
    at: new Date().toISOString(),
    ...detail,
  };
  saveLedger(ledger);
}

function shaFile(p) {
  if (!fs.existsSync(p)) return null;
  return createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

function resolveConfigPath(id) {
  return path.join(ROOT, "config/group-demand-intelligence/hotels", `${id}.json`);
}

function resolveStoreDir(id) {
  const root = process.env.GDI_DATA_ROOT
    ? path.resolve(process.env.GDI_DATA_ROOT)
    : path.join(ROOT, "data/group-demand-intelligence");
  return path.join(root, "hotels", id);
}

function defaultPackageDir(id) {
  if (packageDirArg) return path.resolve(packageDirArg);
  if (id === "recLuxvwwxID7U2B8" || id === "rec0eTEnCqlsGJJt9") {
    return path.join(
      ROOT,
      "reports/gdi-bethesda-process-replication/hotel-review"
    );
  }
  if (id === "recG66DQJKP2c0UNh") {
    return path.join(outDir, "renaissance-package");
  }
  if (id === "recnPB3Q02NO8Ma1o") {
    return path.join(outDir, "packages", "san-diego-bayside-lab-replay");
  }
  return path.join(outDir, "packages", id);
}

const ledger = loadLedger();
ledger.horizonResolved = {
  requested: horizon,
  canonicalPrimary: {
    start: "2026-10-14",
    end: "2028-04-14",
    months: 18,
  },
  note:
    horizon === "24m" || horizon === "24"
      ? "Requested 24m differs from canonical 18m primary used by Bethesda/Mayflower packages — do not silently alter those packages; treat 24m as explicit override for this run only."
      : "Using canonical 18-month primary horizon (2026-10-14 → 2028-04-14).",
};

const pkgDir = defaultPackageDir(hotelId);
const stagesToRun = stageOnly
  ? [stageOnly]
  : STAGES.filter((s) => {
      const prev = ledger.stages[s];
      if (resumeRun && prev?.status === "completed") return false;
      if (s === "apply_publish" && dryRun) return false;
      return true;
    });

console.log(
  JSON.stringify(
    {
      runId,
      hotelId,
      dryRun,
      authorizePublish,
      targetEnv,
      adapter,
      packageDir: pkgDir,
      stagesToRun,
      ledgerPath,
      runDiscovery,
      discoveryMode,
      discoveryExecutionKind:
        !runDiscovery
          ? "plan_only"
          : discoveryMode === DISCOVERY_MODE.REPLAY
            ? "replay_frozen_fixtures"
            : discoveryMode === DISCOVERY_MODE.LIVE_MOCK
              ? "live_mock_transport"
              : discoveryMode === DISCOVERY_MODE.LIVE
                ? "live_or_plan_gated"
                : "plan_only",
      authorizePaidDiscovery,
      skipAirtableReuse,
      webhound: WEBHOUND_PROVIDER_STATUS.status,
    },
    null,
    2
  )
);

async function runStages() {
for (const stage of stagesToRun) {
  try {
    if (stage === "validate_config") {
      const cfgPath = resolveConfigPath(hotelId);
      if (!fs.existsSync(cfgPath)) {
        mark(ledger, stage, "failed", { error: "hotel_config_missing", cfgPath });
        throw new Error(`Hotel config missing: ${cfgPath}`);
      }
      const cfg = JSON.parse(fs.readFileSync(cfgPath, "utf8"));
      mark(ledger, stage, "completed", {
        configPath: cfgPath,
        displayName: cfg.displayName || null,
        onboarding: cfg.gdiOnboarding?.status || null,
        rooms: cfg.capabilityProfile?.totalGuestrooms || null,
      });
      continue;
    }

    if (stage === "reuse_evidence") {
      const store = resolveStoreDir(hotelId);
      const reuseDir = path.join(runDir, "reuse_prior_intelligence");
      fs.mkdirSync(reuseDir, { recursive: true });
      const airtableAudit = await auditAirtableReuseForHotel(hotelId, {
        skipAirtable: skipAirtableReuse,
      });
      fs.writeFileSync(
        path.join(reuseDir, "airtable-reuse-audit.json"),
        JSON.stringify(airtableAudit, null, 2) + "\n"
      );
      const ledgerDoc = await buildReusePriorIntelligenceLedger(hotelId, {
        skipAirtable: skipAirtableReuse,
        runDiscovery,
      });
      fs.writeFileSync(
        path.join(reuseDir, "reuse-ledger.json"),
        JSON.stringify(ledgerDoc, null, 2) + "\n"
      );
      fs.writeFileSync(
        path.join(reuseDir, "reuse-ledger.csv"),
        reuseLedgerToCsv(ledgerDoc)
      );
      if (ledgerDoc.researchPathSelection) {
        fs.writeFileSync(
          path.join(reuseDir, "research-path-selection.json"),
          JSON.stringify(ledgerDoc.researchPathSelection, null, 2) + "\n"
        );
        fs.writeFileSync(
          path.join(reuseDir, "research-path-selection.csv"),
          researchPathSelectionToCsv(ledgerDoc.researchPathSelection)
        );
      }
      const peDoc = await buildPrivateEventIntelligence(hotelId, {
        skipAirtable: skipAirtableReuse,
      });
      fs.writeFileSync(
        path.join(reuseDir, "private-event-intelligence.json"),
        JSON.stringify(peDoc, null, 2) + "\n"
      );
      fs.writeFileSync(
        path.join(reuseDir, "private-event-intelligence.csv"),
        peIntelligenceToCsv(peDoc)
      );
      // Programs→occasion drafts when signals missing for fitted generators
      const programRows = (ledgerDoc.rows || []).filter(
        (r) => r.sourceType === "demand_program"
      );
      const signalRows = (ledgerDoc.rows || []).filter(
        (r) => r.sourceType === "demand_generator_signal"
      );
      const fitRows = (ledgerDoc.rows || []).filter(
        (r) => r.sourceType === "demand_generator_fit"
      );
      const cfgPathReuse = resolveConfigPath(hotelId);
      const hotelCfg = fs.existsSync(cfgPathReuse)
        ? JSON.parse(fs.readFileSync(cfgPathReuse, "utf8"))
        : null;
      const occasionDrafts = buildOccasionDraftsFromPrograms({
        hotelId,
        hotelLabel: hotelCfg?.displayName || null,
        programs: programRows.map((r) => ({
          programId: r.programId || r.sourceRecordId,
          programName: r.title || r.organizationName,
          demandGeneratorId: r.demandGeneratorId,
          programType: r.programType,
          market: null,
          typicalTiming: null,
          organizationName: r.organizationName,
        })),
        signals: signalRows.map((r) => ({
          programId: r.programId,
          signalId: r.sourceRecordId,
        })),
        fits: fitRows.map((r) => ({
          demandGeneratorId: r.demandGeneratorId,
          organizationName: r.organizationName,
          fitRationale: r.reason,
        })),
        hotelConfig: hotelCfg,
      });
      fs.writeFileSync(
        path.join(reuseDir, "programs-occasion-drafts.json"),
        JSON.stringify(occasionDrafts, null, 2) + "\n"
      );
      const proposedFits = proposeHotelVenueFits({
        hotelId,
        hotelConfig: hotelCfg,
        venues: peDoc.venues || [],
        existingFits: peDoc.fits || [],
      });
      if ((peDoc.counts?.fits || 0) === 0 && (peDoc.counts?.venues || 0) > 0) {
        fs.writeFileSync(
          path.join(reuseDir, "proposed-hotel-venue-fits.json"),
          JSON.stringify(proposedFits, null, 2) + "\n"
        );
      }
      ledger.programsOccasionDrafts = {
        path: path.join(reuseDir, "programs-occasion-drafts.json"),
        draftCount: occasionDrafts.draftCount,
      };
      ledger.proposedHotelVenueFits =
        (peDoc.counts?.fits || 0) === 0
          ? {
              path: path.join(reuseDir, "proposed-hotel-venue-fits.json"),
              proposalCount: proposedFits.proposalCount,
            }
          : null;
      ledger.reusePriorIntelligence = {
        path: path.join(reuseDir, "reuse-ledger.json"),
        counts: ledgerDoc.counts,
        gapsBeforePaidDiscovery: ledgerDoc.gapsBeforePaidDiscovery,
        demandGeneratorFit: ledgerDoc.demandGeneratorFit,
        privateEventIntelligence: ledgerDoc.privateEventIntelligence,
        researchPathSelectionSummary: ledgerDoc.researchPathSelection?.summary || null,
        airtableReuseAudit: {
          path: path.join(reuseDir, "airtable-reuse-audit.json"),
          summary: airtableAudit.summary,
          configured: airtableAudit.configured,
          reason: airtableAudit.reason,
        },
      };
      mark(ledger, stage, "completed", {
        module: "reuse_prior_intelligence_v1",
        storeDir: store,
        opportunityCount: ledgerDoc.opportunityCount,
        counts: ledgerDoc.counts,
        demandPrograms: ledgerDoc.demandPrograms || null,
        demandGeneratorSignals: ledgerDoc.demandGeneratorSignals || null,
        feedbackDrivenResearch: ledgerDoc.feedbackDrivenResearch || null,
        gapsBeforePaidDiscovery: ledgerDoc.gapsBeforePaidDiscovery,
        airtableReuse: {
          skipped: airtableAudit.skipped === true,
          reason: airtableAudit.reason,
          tableSummary: airtableAudit.summary,
          tables: (airtableAudit.tables || []).map((t) => ({
            table: t.table,
            status: t.status,
            read: t.read,
            matched: t.matched,
            excluded: t.excluded,
            unresolved: t.unresolved,
            reason: t.reason,
          })),
        },
        privateEvent: {
          source: peDoc.source,
          airtableOk: peDoc.airtableOk ?? null,
          liveAirtable: peDoc.liveAirtable || null,
          fixtureFallback: peDoc.fixtureFallback || null,
          venues: peDoc.counts?.venues,
          fits: peDoc.counts?.fits,
          signals: peDoc.counts?.signals,
          dispositions: peDoc.counts?.byDisposition,
        },
        researchPathSelection: ledgerDoc.researchPathSelection?.summary || null,
        webhound: WEBHOUND_PROVIDER_STATUS,
        note: "Prior intelligence + live Airtable audit + PE graph + path selection before paid discovery; DG/PE ≠ auto-qualify",
      });
      continue;
    }

    if (stage === "discover_gaps") {
      const discDir = path.join(runDir, "parallel_discovery");
      fs.mkdirSync(discDir, { recursive: true });

      // Native + geographic research through coordinator (plan/dry-run default; no paid)
      const nativeGeoDir = path.join(runDir, "native_geo_research");
      const reuseLedgerPath = path.join(
        runDir,
        "reuse_prior_intelligence",
        "reuse-ledger.json"
      );
      const reuseLedger = fs.existsSync(reuseLedgerPath)
        ? JSON.parse(fs.readFileSync(reuseLedgerPath, "utf8"))
        : null;
      const feedbackHints =
        reuseLedger?.feedbackDrivenResearch?.discoveryQueryHints || [];
      const nativeGeo = await runNativeGeoResearchStage(hotelId, {
        authorizeNative,
        dryRun: !authorizeNative,
        feedbackQueryHints: feedbackHints,
        maxNativeQueries: Math.min(10, budgetSearches),
        maxGeoQueries: 16,
      });
      writeNativeGeoArtifacts(nativeGeo, nativeGeoDir);
      ledger.nativeGeoResearch = {
        path: path.join(nativeGeoDir, "native-geo-research-stage.json"),
        paidCalls: nativeGeo.paidCalls || 0,
        nativeStatus: nativeGeo.native?.status || null,
        geoQueryCount: nativeGeo.geographic?.queryCount || 0,
        feedbackHintCount: nativeGeo.geographic?.feedbackHintCount || 0,
      };
      // Refresh path selection with native/geo execution evidence
      const refreshedPaths = selectResearchPaths(hotelId, {
        runDiscovery,
        discoveryMode: discoveryMode || null,
        evidenceHints: {
          ...(reuseLedger?.researchPathSelection?.evidence || {}),
          opportunityCount: reuseLedger?.opportunityCount ?? 0,
          dgFitCount: reuseLedger?.demandGeneratorFit?.count ?? 0,
          dgCount: reuseLedger?.demandGeneratorFit?.count ?? 0,
          programCount: reuseLedger?.demandPrograms?.count ?? 0,
          dgSignalCount: reuseLedger?.demandGeneratorSignals?.count ?? 0,
          peVenueCount: reuseLedger?.privateEventIntelligence?.counts?.venues ?? 0,
          peFitCount: reuseLedger?.privateEventIntelligence?.counts?.fits ?? 0,
          peSignalCount: reuseLedger?.privateEventIntelligence?.counts?.signals ?? 0,
          peFixtureUsed:
            reuseLedger?.privateEventIntelligence?.fixtureFallback?.used === true,
          feedbackCount:
            (reuseLedger?.feedbackItemCount || 0) +
            (reuseLedger?.shareValidationItemCount || 0),
          feedbackDrivenHintCount: feedbackHints.length,
          contactTargets:
            reuseLedger?.gapsBeforePaidDiscovery?.enrichmentNeeded || 0,
          geographicPlanned: true,
          nativePlannedOrExecuted: true,
          nativeMode: nativeGeo.native?.status || "planned_dry_run",
        },
      });
      fs.writeFileSync(
        path.join(
          runDir,
          "reuse_prior_intelligence",
          "research-path-selection.json"
        ),
        JSON.stringify(refreshedPaths, null, 2) + "\n"
      );
      fs.writeFileSync(
        path.join(
          runDir,
          "reuse_prior_intelligence",
          "research-path-selection.csv"
        ),
        researchPathSelectionToCsv(refreshedPaths)
      );

      if (!runDiscovery) {
        ledger.budget.incompleteCoverageReason =
          "new_external_discovery_not_run_reuse_only";
        const planOnly = await runParallelDiscoveryStage(hotelId, {
          runDiscovery: false,
          maxUsd: budgetUsd,
          maxTasks: Math.min(8, budgetSearches),
          maxPages: budgetPages,
        });
        writeDiscoveryStageArtifacts(planOnly, discDir);
        mark(ledger, stage, "incomplete_coverage", {
          reason: "reuse_existing_evidence_only",
          module: "parallel_discovery_adapter_v1+native_geo_research_stage_v1",
          planPath: path.join(discDir, "bounded-live-discovery-plan.json"),
          nativeGeo: ledger.nativeGeoResearch,
          pathSelectionSummary: refreshedPaths.summary,
          note: "Native/geo plan/dry-run executed via coordinator (0 paid). Parallel discovery not requested. Did not claim no demand.",
          budgetRemainingUsd: Math.max(0, budgetUsd - ledger.budget.usedUsd),
          webhound: WEBHOUND_PROVIDER_STATUS,
        });
        ledger.remainingManualSteps.push({
          step: "discover_gaps",
          action:
            "Optionally: --run-discovery for frozen lab replay; --authorize-native for paid Serp/OpenAI (not used this pass)",
        });
        continue;
      }
      const peGaps = [];
      if ((reuseLedger?.privateEventIntelligence?.counts?.fits || 0) === 0) {
        peGaps.push(
          "Hotel Venue Fit rows missing — research local private-event venues and lodging catchment for this hotel only (no Bethesda copy)"
        );
      }
      if ((reuseLedger?.demandGeneratorSignals?.count || 0) === 0) {
        peGaps.push(
          "Demand Generator Signals empty for fitted generators — discover dated occasions under known Demand Programs"
        );
      }
      const programDraftPath = path.join(
        runDir,
        "reuse_prior_intelligence",
        "programs-occasion-drafts.json"
      );
      const programDraftDoc = fs.existsSync(programDraftPath)
        ? JSON.parse(fs.readFileSync(programDraftPath, "utf8"))
        : null;
      const researchGaps = [
        ...(reuseLedger?.feedbackDrivenResearch?.discoveryQueryHints || []),
        ...((programDraftDoc?.drafts || [])
          .slice(0, 6)
          .map(
            (d) =>
              `Confirm dated cycle + lodging procurement for program: ${d.title || d.organizationName}`
          ) || []),
      ];
      const discovery = await runParallelDiscoveryStage(hotelId, {
        runDiscovery: true,
        mode: discoveryMode,
        authorizePaidDiscovery,
        maxUsd: Math.min(budgetUsd, 2),
        maxTasks: Math.min(2, budgetSearches),
        maxPages: budgetPages,
        researchGaps,
        privateEventGaps: peGaps,
        feedbackQueryHints: feedbackHints,
      });
      writeDiscoveryStageArtifacts(discovery, discDir);
      ledger.parallelDiscovery = {
        status: discovery.status,
        mode: discovery.mode || discoveryMode,
        executionKind: discovery.executionKind || null,
        paidCalls: discovery.paidCalls || 0,
        freshDiscoveryClaimed: discovery.freshDiscoveryClaimed === true,
        candidateCount: (discovery.candidates || []).length,
        path: path.join(discDir, "parallel-discovery-stage.json"),
      };
      if (
        discovery.status === "completed_replay" ||
        discovery.status === "completed_live_mock"
      ) {
        mark(ledger, stage, "completed", {
          module: "parallel_discovery_adapter_v1+native_geo_research_stage_v1",
          mode: discovery.mode || discoveryMode,
          executionKind: discovery.executionKind,
          paidCalls: (discovery.paidCalls || 0) + (nativeGeo.paidCalls || 0),
          freshDiscoveryClaimed: false,
          candidateCount: (discovery.candidates || []).length,
          nativeGeo: ledger.nativeGeoResearch,
          reviewPackage: discovery.reviewPackage || null,
          note:
            discovery.status === "completed_live_mock"
              ? "Live mock transport + native/geo plan — not a paid Parallel call; not fresh discovery success"
              : "Frozen Parallel fixture replay + native/geo plan — not a fresh discovery success claim",
        });
      } else {
        ledger.budget.incompleteCoverageReason =
          discovery.reason || "discovery_incomplete";
        mark(ledger, stage, "incomplete_coverage", {
          module: "parallel_discovery_adapter_v1+native_geo_research_stage_v1",
          mode: discovery.mode || discoveryMode,
          executionKind: discovery.executionKind || "plan_only",
          reason: discovery.reason,
          paidCalls: (discovery.paidCalls || 0) + (nativeGeo.paidCalls || 0),
          nativeGeo: ledger.nativeGeoResearch,
          note: discovery.note,
        });
        ledger.remainingManualSteps.push({
          step: "discover_gaps",
          action: discovery.note || "Review bounded live plan; do not treat stop as no demand",
        });
      }
      continue;
    }

    if (stage === "assess_fit") {
      const fitDir = path.join(runDir, "independent_hotel_fit");
      fs.mkdirSync(fitDir, { recursive: true });
      const doc = await loadOpportunitiesCanonical(hotelId);
      const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
      // Cross-hotel reuse sample: Bethesda sports candidates when assessing Mayflower
      let crossHotel = [];
      if (hotelId === "rec0eTEnCqlsGJJt9") {
        try {
          const beth = await loadOpportunitiesCanonical("recLuxvwwxID7U2B8");
          crossHotel = (beth?.opportunities || [])
            .filter((o) =>
              /soccer|lacrosse|sports?\s*tournament|soccerplex|stay[- ]to[- ]play/i.test(
                `${o.title || ""} ${o.organizationName || ""} ${o.summaryWhat || ""}`
              )
            )
            .slice(0, 12)
            .map((o) => ({ ...o, hotelId: "recLuxvwwxID7U2B8" }));
        } catch {
          crossHotel = [];
        }
      }
      const batch = assessIndependentHotelFitBatch(
        [...ops.slice(0, 40), ...crossHotel],
        hotelId
      );
      // Also score any replayed discovery candidates
      const discPath = path.join(
        runDir,
        "parallel_discovery",
        "parallel-discovery-stage.json"
      );
      let discoveryFit = null;
      if (fs.existsSync(discPath)) {
        const disc = JSON.parse(fs.readFileSync(discPath, "utf8"));
        discoveryFit = assessIndependentHotelFitBatch(
          disc.candidates || [],
          hotelId
        );
      }
      fs.writeFileSync(
        path.join(fitDir, "independent-hotel-fit.json"),
        JSON.stringify({ store: batch, discoveryReplay: discoveryFit }, null, 2) +
          "\n"
      );
      ledger.independentHotelFit = {
        path: path.join(fitDir, "independent-hotel-fit.json"),
        counts: batch.counts,
        discoveryReplayCounts: discoveryFit?.counts || null,
      };
      mark(ledger, stage, "completed", {
        module: "independent_hotel_fit_v1",
        counts: batch.counts,
        discoveryReplayCounts: discoveryFit?.counts || null,
        note: "Independent per-hotel fit; sports not copied to Mayflower; unsupported orgs not promoted",
      });
      ledger.remainingManualSteps.push({
        step: "assess_fit",
        action:
          "Human shortlist of NEEDS_HUMAN_REVIEW / POSSIBLE_FIT; do not auto-publish fit outcomes",
      });
      continue;
    }

    if (stage === "identify_people") {
      mark(ledger, stage, "completed", {
        mode: "deferred_to_enrich_contacts_targets",
        note: "People identification runs only for targeted contact plan on qualified/enrichment candidates",
      });
      continue;
    }

    if (stage === "enrich_contacts") {
      const contactDir = path.join(runDir, "targeted_contact");
      const candidateDir = path.join(contactDir, "candidates");
      fs.mkdirSync(candidateDir, { recursive: true });
      const doc = await loadOpportunitiesCanonical(hotelId);
      const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
      const reusePath = path.join(
        runDir,
        "reuse_prior_intelligence",
        "reuse-ledger.json"
      );
      const reuseRows = fs.existsSync(reusePath)
        ? JSON.parse(fs.readFileSync(reusePath, "utf8")).rows || []
        : [];
      const plan = buildTargetedContactPlan({
        hotelId,
        opportunities: ops,
        reuseRows,
        authorizeSurfe,
        maxTargets: 25,
      });
      fs.writeFileSync(
        path.join(contactDir, "targeted-contact-plan.json"),
        JSON.stringify(plan, null, 2) + "\n"
      );
      const executed = await executeTargetedContactResolution({
        hotelId,
        opportunities: ops,
        plan,
        outDir: candidateDir,
        authorizeSurfe,
        maxExecute: 12,
      });
      ledger.targetedContact = {
        path: path.join(contactDir, "targeted-contact-plan.json"),
        candidatesPath: candidateDir,
        targetCount: plan.targetCount,
        attempted: executed.attempted,
        resolved: executed.resolved,
        unresolved: executed.unresolved,
        blocked: executed.blocked,
        surfeInvoked: false,
        authorizeSurfe,
      };
      // Mark contact path executed in path selection
      const pathJson = path.join(
        runDir,
        "reuse_prior_intelligence",
        "research-path-selection.json"
      );
      if (fs.existsSync(pathJson)) {
        try {
          const prior = JSON.parse(fs.readFileSync(pathJson, "utf8"));
          const withContact = selectResearchPaths(hotelId, {
            runDiscovery,
            evidenceHints: {
              ...(prior.evidence || {}),
              contactExecuted: true,
              contactTargets: plan.targetCount,
              geographicPlanned: true,
              nativePlannedOrExecuted: true,
              nativeMode:
                ledger.nativeGeoResearch?.nativeStatus || "planned_dry_run",
            },
          });
          fs.writeFileSync(pathJson, JSON.stringify(withContact, null, 2) + "\n");
          fs.writeFileSync(
            path.join(
              runDir,
              "reuse_prior_intelligence",
              "research-path-selection.csv"
            ),
            researchPathSelectionToCsv(withContact)
          );
        } catch (err) {
          console.warn(
            "[gdi-workflow] path selection refresh after contact failed:",
            err?.message || err
          );
        }
      }
      mark(ledger, stage, "completed", {
        module: "execute_targeted_contact_resolution_v1",
        targetCount: plan.targetCount,
        attempted: executed.attempted,
        resolved: executed.resolved,
        unresolved: executed.unresolved,
        blocked: executed.blocked,
        surfe: authorizeSurfe
          ? "authorized_but_not_invoked_this_pass"
          : "not_authorized",
        order: plan.targets[0]?.resolutionOrder || [
          "reuse_verified_existing",
          "official_sources",
          "native_who",
          "surfe_if_authorized",
        ],
        note: "Executed contact resolution into isolated candidate storage — no production writes; Surfe not called",
      });
      if (executed.unresolved > 0 || executed.blocked > 0) {
        ledger.remainingManualSteps.push({
          step: "enrich_contacts",
          action: `Human follow-up on ${executed.unresolved} unresolved + ${executed.blocked} blocked contact targets (candidates under ${candidateDir})`,
        });
      }
      continue;
    }

    if (stage === "evaluate_eligibility") {
      const test = spawnSync(
        process.execPath,
        [path.join(ROOT, "scripts/test-gdi-prospect-publication-eligibility-v1.mjs")],
        { cwd: ROOT, encoding: "utf8" }
      );
      mark(ledger, stage, test.status === 0 ? "completed" : "failed", {
        exitCode: test.status,
        note: "Eligibility SoT gate (prospect-publication-eligibility-v1)",
      });
      if (test.status !== 0) throw new Error("eligibility_gate_failed");
      continue;
    }

    if (stage === "generate_cards") {
      const drawerDir = path.join(runDir, "drawer_completeness");
      fs.mkdirSync(drawerDir, { recursive: true });

      // Renaissance (and any hotel without a hand package) uses draft package
      // generation below — do not require a hotel-specific builder script.

      // San Diego: evaluate frozen lab cards for drawer contract (no package CSV required)
      if (hotelId === "recnPB3Q02NO8Ma1o") {
        const discPath = path.join(
          runDir,
          "parallel_discovery",
          "parallel-discovery-stage.json"
        );
        let cards = [];
        if (fs.existsSync(discPath)) {
          const disc = JSON.parse(fs.readFileSync(discPath, "utf8"));
          cards = (disc.candidates || []).map((c) =>
            mapLabCardToDrawerShape(
              {
                cardId: c.candidateId,
                account: c.organizationName,
                timing: {
                  start: c.eventStartDate,
                  end: c.eventEndDate,
                  raw: c.title,
                },
                demandSignal: c.demandSignal,
                whyWyndhamFits: c.hotelOpportunityThesis,
                sourcingStatus: c.sourcingStatus,
                recommendedFirstAction: c.recommendedAction,
                evidenceLinks: c.evidenceLinks,
                contact: c.primaryContact,
                materialLimitations: c.materialLimitations,
                noveltyVsBaseline: c.provenance?.noveltyVsBaseline,
              },
              hotelId
            )
          );
        }
        const drawerReport = reportDrawerCompleteness(cards, { hotelId });
        fs.writeFileSync(
          path.join(drawerDir, "drawer-completeness.json"),
          JSON.stringify(drawerReport, null, 2) + "\n"
        );
        const jev = runJevAdvisoryShadow({
          hotelId,
          cards: cards.map((c, i) => ({
            cardId: c.id,
            evidenceLinks: c.evidenceLinks,
            whyWyndhamFits: c.hotelOpportunityThesis,
            contact: c.primaryContact,
            noveltyVsBaseline: c.noveltyVsBaseline,
          })),
          invokeJev: runJevShadow,
        });
        writeJevAdvisoryArtifacts(jev, path.join(runDir, "jev_advisory"));
        ledger.drawerCompleteness = {
          path: path.join(drawerDir, "drawer-completeness.json"),
          pass: drawerReport.pass,
          fail: drawerReport.fail,
          total: drawerReport.total,
        };
        mark(ledger, stage, "completed", {
          packageDir: null,
          mode: "san_diego_lab_replay_cards",
          drawerCompleteness: ledger.drawerCompleteness,
          jevAdvisory: "deterministic_only",
          note: "Drawer scored on replayed lab cards; lab ≠ production classifier PASS",
        });
        continue;
      }

      if (!fs.existsSync(path.join(pkgDir, "final-prospect-register.csv"))) {
        // Generate draft package from store + contact candidates + supported PE (no hand CSV prerequisite)
        const doc = await loadOpportunitiesCanonical(hotelId);
        const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
        const contactSummaryPath = path.join(
          runDir,
          "targeted_contact",
          "candidates",
          "execution-summary.json"
        );
        const contactResults = fs.existsSync(contactSummaryPath)
          ? JSON.parse(fs.readFileSync(contactSummaryPath, "utf8")).results || []
          : [];
        const fitPath = path.join(
          runDir,
          "independent_hotel_fit",
          "independent-hotel-fit.json"
        );
        const fitDoc = fs.existsSync(fitPath)
          ? JSON.parse(fs.readFileSync(fitPath, "utf8"))
          : null;
        const pePath = path.join(
          runDir,
          "reuse_prior_intelligence",
          "private-event-intelligence.json"
        );
        const peDoc = fs.existsSync(pePath)
          ? JSON.parse(fs.readFileSync(pePath, "utf8"))
          : await buildPrivateEventIntelligence(hotelId, {
              skipAirtable: skipAirtableReuse,
            });
        const peDrafts = peSupportedCandidatesToDraftOpportunities(peDoc, hotelId);
        const draftDir = path.join(runDir, "draft_review_package");
        const hotelKey =
          hotelId === "rec0eTEnCqlsGJJt9"
            ? "may"
            : hotelId === "recG66DQJKP2c0UNh"
              ? "ren"
              : hotelId === "recLuxvwwxID7U2B8"
                ? "beth"
                : hotelId.slice(0, 6);
        const draft = buildDraftReviewPackage({
          hotelId,
          hotelKey,
          hotelLabel:
            hotelId === "rec0eTEnCqlsGJJt9"
              ? "Mayflower"
              : hotelId === "recG66DQJKP2c0UNh"
                ? "Renaissance"
                : hotelId === "recLuxvwwxID7U2B8"
                  ? "Bethesda"
                  : hotelId,
          opportunities: ops,
          peDraftOpportunities: peDrafts,
          contactResults,
          fitAssessments: (fitDoc?.store?.results || []).map((r) => ({
            opportunityId: r.id || r.candidateId || r.opportunityId,
            outcome: r.fit?.outcome || r.outcome || null,
            ...r,
          })),
          outDir: draftDir,
        });
        const drawerPool = [...ops.slice(0, 40), ...peDrafts];
        const drawerReport = reportDrawerCompleteness(drawerPool, {
          hotelId,
        });
        fs.writeFileSync(
          path.join(drawerDir, "drawer-completeness.json"),
          JSON.stringify(drawerReport, null, 2) + "\n"
        );
        fs.writeFileSync(
          path.join(draftDir, "pe-draft-candidates.json"),
          JSON.stringify(
            {
              hotelId,
              count: peDrafts.length,
              candidates: peDrafts,
              note: "Supported PE dispositions only — human review checkpoint required; not auto-published",
            },
            null,
            2
          ) + "\n"
        );
        ledger.drawerCompleteness = {
          path: path.join(drawerDir, "drawer-completeness.json"),
          pass: drawerReport.pass,
          fail: drawerReport.fail,
          total: drawerReport.total,
        };
        ledger.draftReviewPackage = draft;
        mark(ledger, stage, "completed", {
          mode: "draft_package_from_candidates_and_pe",
          packageDir: draftDir,
          draft,
          peDraftCount: peDrafts.length,
          drawerCompleteness: ledger.drawerCompleteness,
          note: "Draft package from store/candidates + supported PE — human review checkpoint pending before publication",
        });
        ledger.remainingManualSteps.push({
          step: "generate_cards",
          action: `Record human review decisions in ${path.join(draftDir, "human-review-checkpoint.json")} before treating package as reviewed`,
        });
        continue;
      }

      const doc = await loadOpportunitiesCanonical(hotelId);
      const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
      // Supported PE candidates → draft sidecar (does not overwrite reviewed package)
      const pePathReviewed = path.join(
        runDir,
        "reuse_prior_intelligence",
        "private-event-intelligence.json"
      );
      const peDocReviewed = fs.existsSync(pePathReviewed)
        ? JSON.parse(fs.readFileSync(pePathReviewed, "utf8"))
        : null;
      const peDraftsReviewed = peDocReviewed
        ? peSupportedCandidatesToDraftOpportunities(peDocReviewed, hotelId)
        : [];
      const peDraftDir = path.join(runDir, "draft_review_package");
      if (peDraftsReviewed.length) {
        const peDraft = buildDraftReviewPackage({
          hotelId,
          hotelKey:
            hotelId === "rec0eTEnCqlsGJJt9"
              ? "may"
              : hotelId === "recLuxvwwxID7U2B8"
                ? "beth"
                : hotelId.slice(0, 6),
          hotelLabel: hotelId,
          opportunities: [],
          peDraftOpportunities: peDraftsReviewed,
          outDir: peDraftDir,
        });
        ledger.peDraftReviewPackage = peDraft;
      }
      const drawerReport = reportDrawerCompleteness(
        [...ops, ...peDraftsReviewed],
        { hotelId }
      );
      fs.writeFileSync(
        path.join(drawerDir, "drawer-completeness.json"),
        JSON.stringify(drawerReport, null, 2) + "\n"
      );
      const threeLayer = {
        version: "gdi-publication-readiness-layers-v1",
        hotelId,
        note: "Commercial eligibility, contact completeness, and drawer content are separately inspectable.",
        commercialEligibility: {
          hotelFacing: ops.filter((o) => o.prospectHotelFacing === true).length,
          enrichmentQueue: ops.filter((o) => o.prospectEnrichmentQueue === true)
            .length,
        },
        contactCompleteness: {
          byGroup: [1, 2, 3, 4].reduce((acc, g) => {
            acc[g] = ops.filter((o) => o.contactCompletenessGroup === g).length;
            return acc;
          }, {}),
        },
        drawerContent: {
          pass: drawerReport.pass,
          fail: drawerReport.fail,
          total: drawerReport.total,
          path: path.join(drawerDir, "drawer-completeness.json"),
        },
        peDraftCandidates: peDraftsReviewed.length,
      };
      fs.writeFileSync(
        path.join(drawerDir, "publication-readiness-layers.json"),
        JSON.stringify(threeLayer, null, 2) + "\n"
      );
      const jev = runJevAdvisoryShadow({
        hotelId,
        cards: ops.slice(0, 30),
        invokeJev: runJevShadow,
      });
      writeJevAdvisoryArtifacts(jev, path.join(runDir, "jev_advisory"));
      ledger.drawerCompleteness = {
        path: path.join(drawerDir, "drawer-completeness.json"),
        pass: drawerReport.pass,
        fail: drawerReport.fail,
        total: drawerReport.total,
      };
      ledger.publicationReadinessLayers = threeLayer;
      mark(ledger, stage, "completed", {
        packageDir: pkgDir,
        registerHash: shaFile(path.join(pkgDir, "final-prospect-register.csv")),
        packagesHash: shaFile(path.join(pkgDir, "contact-packages.json")),
        drawerCompleteness: ledger.drawerCompleteness,
        publicationReadinessLayers: threeLayer,
        jevAdvisory: "deterministic_shadow_only",
      });
      continue;
    }

    if (stage === "prepare_manifest") {
      if (
        hotelId === "recnPB3Q02NO8Ma1o" &&
        !fs.existsSync(path.join(pkgDir, "final-prospect-register.csv"))
      ) {
        mark(ledger, stage, "incomplete_coverage", {
          reason: "san_diego_lab_replay_not_publication_package",
          note: "Frozen Parallel replay is not a reviewed publication package — no import manifest",
        });
        ledger.remainingManualSteps.push({
          step: "prepare_manifest",
          action:
            "Human-review San Diego lab cards → build reviewed prospect package before publication prep",
        });
        continue;
      }
      const draftPkgDir = path.join(runDir, "draft_review_package");
      const effectivePkgDir = fs.existsSync(
        path.join(pkgDir, "final-prospect-register.csv")
      )
        ? pkgDir
        : fs.existsSync(path.join(draftPkgDir, "final-prospect-register.csv"))
          ? draftPkgDir
          : pkgDir;
      if (
        !fs.existsSync(path.join(effectivePkgDir, "final-prospect-register.csv"))
      ) {
        mark(ledger, stage, "incomplete_coverage", {
          reason: "no_reviewed_or_draft_package",
          pkgDir,
          draftPkgDir,
          note: "No hand package or draft package available for import dry-run",
        });
        ledger.remainingManualSteps.push({
          step: "prepare_manifest",
          action: "Complete generate_cards draft package or supply reviewed package CSV",
        });
        continue;
      }
      const dataRoot = path.join(outDir, "_local-preview");
      fs.mkdirSync(dataRoot, { recursive: true });
      // Seed local preview store from canonical FS hotel pack when present
      const src = resolveStoreDir(hotelId);
      const dst = path.join(dataRoot, "hotels", hotelId);
      fs.mkdirSync(dst, { recursive: true });
      for (const f of ["opportunities.json", "profile.json"]) {
        const from = path.join(src, f);
        if (fs.existsSync(from)) {
          fs.copyFileSync(from, path.join(dst, f));
        }
      }
      const overlayPath = path.join(
        ROOT,
        "reports/gdi-drawer-completeness/sales-review-overlay.json"
      );
      const dryArgs = [
        path.join(ROOT, "scripts/gdi-import-reviewed-prospects.mjs"),
        "--hotel",
        hotelId,
        "--dry-run",
        "--adapter",
        adapter === "airtable" && authorizePublish ? "airtable" : "filesystem",
        "--data-root",
        dataRoot,
        "--package-dir",
        effectivePkgDir,
        "--out-dir",
        path.join(runDir, "manifest-dryrun"),
      ];
      if (fs.existsSync(overlayPath)) {
        dryArgs.push("--content-overlay", overlayPath);
      }
      const r = spawnSync(process.execPath, dryArgs, {
        cwd: ROOT,
        encoding: "utf8",
        env: { ...process.env, GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1" },
      });
      fs.writeFileSync(
        path.join(runDir, "manifest-dryrun-stdout.txt"),
        r.stdout || ""
      );
      let drawerFromImport = null;
      try {
        const importResultGlob = fs
          .readdirSync(path.join(runDir, "manifest-dryrun"))
          .filter((f) => f.startsWith("import-result-"))
          .sort()
          .pop();
        if (importResultGlob) {
          const ir = JSON.parse(
            fs.readFileSync(
              path.join(runDir, "manifest-dryrun", importResultGlob),
              "utf8"
            )
          );
          drawerFromImport = ir.results?.[0]?.drawerCompleteness || null;
        }
      } catch {
        drawerFromImport = null;
      }
      mark(ledger, stage, r.status === 0 ? "completed" : "failed", {
        exitCode: r.status,
        dataRoot,
        packageDirUsed: effectivePkgDir,
        draftPackage:
          effectivePkgDir === draftPkgDir
            ? "used_draft_pending_human_review"
            : "used_reviewed_package",
        contentOverlay: fs.existsSync(overlayPath) ? overlayPath : null,
        publicationReadinessLayers: {
          commercialEligibility: "see import manifest hotelFacing / enrichmentQueue",
          contactCompleteness: "see import manifest contactGroup",
          drawerContent: drawerFromImport,
        },
        note: "Dry-run import manifest prepared; eligibility, contact group, and drawer completeness reported separately",
      });
      if (r.status !== 0) throw new Error("manifest_dryrun_failed");
      continue;
    }

    if (stage === "preview_verify") {
      if (
        hotelId === "recnPB3Q02NO8Ma1o" &&
        !fs.existsSync(path.join(pkgDir, "final-prospect-register.csv"))
      ) {
        mark(ledger, stage, "incomplete_coverage", {
          reason: "san_diego_preview_requires_reviewed_package",
          note: "Skipped local preview apply — lab replay cards are not an import package",
        });
        continue;
      }
      const draftPkgDir = path.join(runDir, "draft_review_package");
      const effectivePkgDir = fs.existsSync(
        path.join(pkgDir, "final-prospect-register.csv")
      )
        ? pkgDir
        : fs.existsSync(path.join(draftPkgDir, "final-prospect-register.csv"))
          ? draftPkgDir
          : pkgDir;
      if (
        !fs.existsSync(path.join(effectivePkgDir, "final-prospect-register.csv"))
      ) {
        mark(ledger, stage, "incomplete_coverage", {
          reason: "no_package_for_preview",
          note: "No reviewed or draft package for local preview apply",
        });
        continue;
      }
      const dataRoot = path.join(outDir, "_local-preview");
      const applyArgs = [
        path.join(ROOT, "scripts/gdi-import-reviewed-prospects.mjs"),
        "--hotel",
        hotelId,
        "--apply",
        "--adapter",
        "filesystem",
        "--data-root",
        dataRoot,
        "--package-dir",
        effectivePkgDir,
        "--out-dir",
        path.join(runDir, "preview-apply"),
        "--backup-dir",
        path.join(runDir, "preview-apply/backups"),
      ];
      const r1 = spawnSync(process.execPath, applyArgs, {
        cwd: ROOT,
        encoding: "utf8",
        env: { ...process.env, GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1" },
      });
      const r2 = spawnSync(process.execPath, applyArgs, {
        cwd: ROOT,
        encoding: "utf8",
        env: { ...process.env, GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1" },
      });
      // Local feedback preservation probe (filesystem share-validation)
      const sharePath = path.join(
        dataRoot,
        "hotels",
        hotelId,
        "share-validation.json"
      );
      const probeOpp = "gdi_opp_2026_education_engagement_forum_1";
      const shareDoc = fs.existsSync(sharePath)
        ? JSON.parse(fs.readFileSync(sharePath, "utf8"))
        : { hotelId, items: {} };
      shareDoc.items = shareDoc.items || {};
      shareDoc.items[probeOpp] = {
        opportunityId: probeOpp,
        familiarityStatus: "ALREADY_KNOWN",
        commercialValue: null,
        note: "workflow_v1_local_preview_probe — not a customer sales judgment",
        validatedAt: new Date().toISOString(),
        validator: "workflow_v1_local_preview",
      };
      fs.writeFileSync(sharePath, JSON.stringify(shareDoc, null, 2) + "\n");
      const r3 = spawnSync(process.execPath, applyArgs, {
        cwd: ROOT,
        encoding: "utf8",
        env: { ...process.env, GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1" },
      });
      const shareAfter = JSON.parse(fs.readFileSync(sharePath, "utf8"));
      const feedbackPreserved = !!shareAfter.items?.[probeOpp];
      mark(ledger, stage, "completed", {
        apply1: r1.status,
        apply2: r2.status,
        apply3_after_feedback: r3.status,
        feedbackPreserved,
        previewDataRoot: dataRoot,
        previewUrlHint: `/group-demand-intelligence.html?hotelId=${hotelId}&embed=1`,
        note: "Local FS preview only — not production publication",
      });
      if (r1.status !== 0 || r2.status !== 0) {
        throw new Error("preview_import_failed");
      }
      continue;
    }

    if (stage === "apply_publish") {
      if (!authorizePublish) {
        mark(ledger, stage, "skipped", { reason: "authorize_publish_required" });
        continue;
      }
      if (targetEnv !== "production") {
        mark(ledger, stage, "failed", {
          error: "target_env_must_be_production_for_publish",
        });
        throw new Error("Set --target-env production with --authorize-publish");
      }
      if (hotelId === "recG66DQJKP2c0UNh") {
        mark(ledger, stage, "blocked", {
          reason: "renaissance_production_publication_not_authorized_in_this_step",
        });
        throw new Error("Renaissance production publish not authorized");
      }
      const r = spawnSync(
        process.execPath,
        [
          path.join(ROOT, "scripts/gdi-import-reviewed-prospects.mjs"),
          "--hotel",
          hotelId,
          "--apply",
          "--adapter",
          "airtable",
          "--package-dir",
          pkgDir,
          "--out-dir",
          path.join(runDir, "production-apply"),
          "--backup-dir",
          path.join(runDir, "production-apply/backups"),
        ],
        {
          cwd: ROOT,
          encoding: "utf8",
          env: {
            ...process.env,
            GDI_OPPORTUNITIES_PERSISTENCE: "airtable",
            GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "0",
          },
        }
      );
      mark(ledger, stage, r.status === 0 ? "completed" : "failed", {
        exitCode: r.status,
        note: "Airtable apply with flag-off import; enable GDI_REVIEWED_PROSPECT_PUBLICATION_V1 separately",
      });
      if (r.status !== 0) throw new Error("production_apply_failed");
      continue;
    }

    mark(ledger, stage, "skipped", { reason: "unknown_stage" });
  } catch (err) {
    ledger.failures.push({
      stage,
      error: err?.message || String(err),
      at: new Date().toISOString(),
    });
    saveLedger(ledger);
    console.error(JSON.stringify({ ok: false, stage, error: err?.message }, null, 2));
    process.exit(1);
  }
}

ledger.finishedAt = new Date().toISOString();
ledger.status = "ok";
saveLedger(ledger);
// Convenience copy for deliverable path
fs.copyFileSync(
  ledgerPath,
  path.join(outDir, "stage-completion-ledger.json")
);
console.log(
  JSON.stringify({ ok: true, runId, ledgerPath, copiedLedger: path.join(outDir, "stage-completion-ledger.json") }, null, 2)
);
}

runStages().catch((err) => {
  console.error(JSON.stringify({ ok: false, error: err?.message || String(err) }, null, 2));
  process.exit(1);
});
