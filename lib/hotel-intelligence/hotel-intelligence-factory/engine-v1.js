/**
 * Hotel Intelligence Factory engine — stage runner.
 * Foundations only: permissions, durable checkpoint/cost, auth binding.
 * Live Research persistence / Promotion apply ship after #495 in V4 PR.
 *
 * IMPORTANT: Successful dry-run or scaffold completion is NOT evidence that
 * live research persistence or canonical promotion has been validated.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

import { assertModePermissions, HIF_MODE } from "./modes-v1.js";
import {
  DEFAULT_PROMOTION_STAGES,
  DEFAULT_RESEARCH_STAGES,
  HIF_STAGE,
  parseStages,
} from "./stages-v1.js";
import { buildRunId, loadFactoryConfig, configFingerprint } from "./config-v1.js";
import {
  loadCheckpoint,
  saveCheckpoint,
  markStageComplete,
  resolveFactoryPaths,
  runDir,
} from "./checkpoint-v1.js";
import { createCostLedger } from "./cost-ledger-v1.js";
import { detectChanges, CHANGE_KIND } from "./change-detection-v1.js";
import { repoReportMirrorDir } from "./durable-paths-v1.js";
import {
  assertPromotionAuthorization,
  loadPromotionAuthorization,
} from "./promotion-authorization-v1.js";
import { runEnrichContractTermsStage } from "../sec-edgar/contract-terms-enrich-stage-v1.js";

export const HIF_ENGINE_V1 = "hotel-intelligence-factory-engine-v1";

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function writeJson(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
}

function appendLog(durableRoot, runId, event) {
  const dir = path.join(runDir(durableRoot, runId), "logs");
  ensureDir(dir);
  fs.appendFileSync(
    path.join(dir, "events.jsonl"),
    `${JSON.stringify({ at: new Date().toISOString(), ...event })}\n`,
    "utf8",
  );
}

async function runStageStub(stage, ctx) {
  appendLog(ctx.durableRoot, ctx.runId, { type: "stage_start", stage });
  const result = {
    stage,
    status: "DECLARED_FOUNDATION",
    note: "Full stage body ships in sequential V4 PR after #495 merge",
    validationClaim: "NOT_LIVE_VALIDATED",
  };

  if (stage === HIF_STAGE.CONFIG) {
    result.geographicPriority = ctx.config.geographicPriority;
    result.globalResearch = true;
    result.hpcEnrichmentPriorityViaConfig = true;
    result.status = "CONFIG_OK";
  }

  if (stage === HIF_STAGE.MANIFEST) {
    const template = path.join(
      ctx.cwd,
      "reports/sec-edgar-contract-timing-v4/HPC_CREATION_PILOT_MANIFEST_TEMPLATE.json",
    );
    const selection = path.join(
      ctx.cwd,
      "reports/sec-edgar-contract-timing-v4/US_PILOT_SELECTION_DRY_RUN.json",
    );
    result.manifestInputs = {
      templateExists: fs.existsSync(template),
      selectionExists: fs.existsSync(selection),
    };
    result.status = "MANIFEST_INPUTS_VERIFIED";
  }

  if (stage === HIF_STAGE.RECONCILE) {
    const detected = detectChanges({}, []);
    result.changeDetection = detected.counts;
    result.changeKinds = Object.values(CHANGE_KIND);
    result.status = "RECONCILE_FOUNDATION";
  }

  if (stage === HIF_STAGE.PROMOTE) {
    const auth =
      ctx.authorization ||
      (ctx.authorizationPath ? loadPromotionAuthorization(ctx.authorizationPath) : null);
    const gate = assertPromotionAuthorization({
      authorization: auth,
      manifestPath: ctx.manifestPath,
      runId: ctx.runId,
      requestedActions: ctx.requestedActions || ["LINK_EXISTING"],
      candidateIdentities: ctx.candidateIdentities || auth?.recordIdentities || [],
      batchSize: ctx.batchSize || 0,
    });
    if (!gate.ok) {
      throw Object.assign(new Error(gate.error), { code: gate.code || 2, detail: gate });
    }
    throw Object.assign(new Error("PROMOTE_BLOCKED_UNTIL_V4_LIVE_IMPL"), {
      code: 2,
      note: "Authorization passed foundation checks; live HPC writes not enabled in scaffold",
    });
  }

  if (stage === HIF_STAGE.PERSIST_RESEARCH && ctx.applyResearch) {
    result.status = "PERSIST_RESEARCH_NOT_LIVE_IN_SCAFFOLD";
    result.validationClaim = "NOT_LIVE_VALIDATED";
  }

  if (stage === HIF_STAGE.DISCOVER_EXPAND) {
    result.status = "DISCOVER_EXPAND_DECLARED";
    result.workstream = "SEC_DISCOVERY_COVERAGE_EXPANSION";
    result.note =
      "Brand-neutral commercial target discovery — expiration/notice first; dual prospect lists; metered pilots";
    result.exhaustiveCoverageClaim = false;
    result.validationClaim = "NOT_LIVE_VALIDATED";
    result.commercialTargetMode = true;
    result.commercialObjective =
      "Maximize credible brand-conversion + management-contract targets with verified timing (not legal-term depth)";
    result.opportunityTiers = [
      "TIER_A_VERIFIED_DECISION_WINDOW",
      "TIER_B_VERIFIED_CONTRACT_EXPIRATION",
      "TIER_C_CONVERSION_RESEARCH_CANDIDATE",
      "HOLD_QUARANTINE",
    ];
    result.prospectLists = ["BRAND_CONVERSION", "MANAGEMENT_CONTRACT"];
    result.pilotScript = "scripts/sec-edgar-commercial-target-scale-pilot.mjs";
    const roadmapPath = path.join(
      ctx.cwd,
      "reports/sec-edgar-discovery-expansion/SEC_DISCOVERY_EXPANSION_ROADMAP.md",
    );
    result.roadmapPresent = fs.existsSync(roadmapPath);
  }

  if (stage === HIF_STAGE.EXHIBIT_HARVEST) {
    result.status = "EXHIBIT_HARVEST_READY";
    result.workstream = "SEC_EX10_CONTRACT_HARVEST_V5";
    result.note =
      "Document-level EX-10 harvest with form/executed + hotel/portfolio classification; run pilot under $25 ceiling";
    result.module = "ex10-contract-harvest-v1";
    result.verificationGates = [
      "AGREEMENT_EXISTENCE_VERIFIED",
      "CONTRACT_TERMS_VERIFIED",
      "CURRENT_STATUS_VERIFIED",
    ];
    result.validationClaim = "NOT_LIVE_VALIDATED";
  }

  if (stage === HIF_STAGE.ENRICH_CONTRACT_TERMS) {
    const enrich = await runEnrichContractTermsStage({
      cohort: ctx.termsCohort || [],
      budget: ctx.ledger,
      fetchDocuments: ctx.fetchDocuments !== false,
      env: process.env,
    });
    result.status = enrich.status;
    result.enrich = {
      agreementsSampled: enrich.agreementsSampled,
      enriched: enrich.enriched,
      documentsRetrieved: enrich.documentsRetrieved,
      cacheHits: enrich.cacheHits,
      paidUsd: enrich.paidUsd,
      errorCount: (enrich.errors || []).length,
      schemaProposalPending: enrich.schemaProposal?.status || null,
    };
    result.validationClaim = enrich.validationClaim;
    if (ctx.durableRoot && ctx.runId) {
      writeJson(path.join(runDir(ctx.durableRoot, ctx.runId), "ENRICH_CONTRACT_TERMS.json"), enrich);
    }
  }

  appendLog(ctx.durableRoot, ctx.runId, { type: "stage_end", stage, status: result.status });
  return result;
}

export async function runHotelIntelligenceFactory(opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const paths = resolveFactoryPaths({ cwd, durableRoot: opts.durableRoot });
  const durableRoot = paths.root;

  const stagesPreview = parseStages(opts.stages);
  const perm = assertModePermissions({
    mode: opts.mode,
    stages: stagesPreview,
    applyResearch: opts.applyResearch,
    applyPromotion: opts.applyPromotion,
    manifestPath: opts.manifestPath,
    authorization: opts.authorization,
    authorizationPath: opts.authorizationPath,
  });
  if (!perm.ok) {
    return { ok: false, ...perm };
  }

  const config = loadFactoryConfig(opts.configPath);
  const runId = opts.runId || buildRunId(perm.mode, config);
  const stages = parseStages(
    opts.stages,
    perm.mode === HIF_MODE.CANONICAL_PROMOTION
      ? DEFAULT_PROMOTION_STAGES
      : DEFAULT_RESEARCH_STAGES,
  );

  const perm2 = assertModePermissions({
    mode: perm.mode,
    stages,
    applyResearch: opts.applyResearch,
    applyPromotion: opts.applyPromotion,
    manifestPath: opts.manifestPath,
    authorization: opts.authorization,
    authorizationPath: opts.authorizationPath,
  });
  if (!perm2.ok) return { ok: false, ...perm2 };

  const ledger = createCostLedger({ ceilingUsd: config.budgetUsd });
  let checkpoint = loadCheckpoint(durableRoot, runId);
  ensureDir(runDir(durableRoot, runId));
  writeJson(path.join(runDir(durableRoot, runId), "config.fingerprint.json"), {
    runId,
    mode: perm.mode,
    fingerprint: configFingerprint(config),
    durableRoot,
    durableSource: paths.source,
    worktreeIndependent: paths.worktreeIndependent,
    config,
  });

  const stageResults = [];
  for (const stage of stages) {
    if (opts.resume && checkpoint.completedStages.includes(stage) && stage !== HIF_STAGE.REPORT) {
      appendLog(durableRoot, runId, { type: "stage_skip_resume", stage });
      stageResults.push({ stage, status: "SKIPPED_RESUME" });
      continue;
    }
    try {
      const result = await runStageStub(stage, {
        cwd,
        durableRoot,
        runId,
        mode: perm.mode,
        config,
        ledger,
        dryRun: opts.dryRun !== false,
        applyResearch: opts.applyResearch,
        manifestPath: opts.manifestPath,
        authorization: opts.authorization,
        authorizationPath: opts.authorizationPath,
        requestedActions: opts.requestedActions,
        candidateIdentities: opts.candidateIdentities,
        batchSize: opts.batchSize,
        termsCohort: opts.termsCohort || [],
        fetchDocuments: opts.fetchDocuments,
      });
      stageResults.push(result);
      markStageComplete(checkpoint, stage);
      saveCheckpoint(durableRoot, runId, checkpoint);
      ledger.persist(durableRoot, runId);
    } catch (err) {
      ledger.persist(durableRoot, runId);
      saveCheckpoint(durableRoot, runId, checkpoint);
      return {
        ok: false,
        runId,
        durableRoot,
        error: err.message,
        code: err.code || 4,
        detail: err.detail || null,
        stageResults,
        validationClaim: "NOT_LIVE_VALIDATED",
      };
    }
  }

  const durableReportDir = path.join(paths.layout.reports, runId);
  const mirrorDir = repoReportMirrorDir(cwd, runId);
  ensureDir(durableReportDir);
  ensureDir(mirrorDir);

  const report = {
    module: HIF_ENGINE_V1,
    ok: true,
    runId,
    mode: perm.mode,
    stages,
    stageResults,
    dryRun: opts.dryRun !== false,
    applyResearch: Boolean(opts.applyResearch),
    applyPromotion: Boolean(opts.applyPromotion),
    cost: ledger.snapshot(),
    durableRoot,
    worktreeIndependent: paths.worktreeIndependent,
    publicationState: config.publication?.defaultState || "CUSTOMER_BLOCKED",
    validationClaim: "NOT_LIVE_VALIDATED",
    note: "Scaffold/dry-run success does not validate live research persistence or canonical promotion",
    gate: {
      pr495MustMergeBeforeFullV4: true,
      unattendedHpcCreation: false,
      customerPublication: false,
      recurringUsesSameEngine: true,
    },
  };

  for (const dir of [durableReportDir, mirrorDir]) {
    writeJson(path.join(dir, "RUN_REPORT.json"), report);
    writeJson(path.join(dir, "CHANGES.json"), {
      note: "Populated when RECONCILE compares prior fingerprints in durable ledger",
      kinds: Object.values(CHANGE_KIND),
    });
    fs.writeFileSync(
      path.join(dir, "RUN_REPORT.md"),
      `# HIF Run ${runId}\n\nMode: **${perm.mode}** · DryRun: ${report.dryRun}\nDurable: \`${durableRoot}\`\n\n**validationClaim: NOT_LIVE_VALIDATED**\n\nStages: ${stages.join(" → ")}\n`,
      "utf8",
    );
  }

  return {
    ok: true,
    runId,
    durableRoot,
    reportPath: durableReportDir,
    mirrorPath: mirrorDir,
    report,
    validationClaim: "NOT_LIVE_VALIDATED",
  };
}

export function sha256File(filePath) {
  const buf = fs.readFileSync(filePath);
  return crypto.createHash("sha256").update(buf).digest("hex");
}

export default { runHotelIntelligenceFactory, sha256File };
