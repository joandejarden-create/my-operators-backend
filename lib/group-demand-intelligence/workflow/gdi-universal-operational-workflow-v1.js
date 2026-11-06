/**
 * GDI_UNIVERSAL_OPERATIONAL_WORKFLOW_V1
 *
 * Node adapter behind scripts/gdi-workflow.ps1.
 * Orchestrates existing research / Airtable / publish / parity modules.
 * Does not implement a competing research engine or publication DTO path.
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";
import { listGdiSelectableHotels } from "../repository.js";
import {
  syncHotelOpportunitiesToAirtable,
  reconcilePublicationStatusToAirtable,
  GDI_AIRTABLE_SYNC_V1,
} from "./gdi-airtable-sync-v1.js";
import {
  GDI_AIRTABLE_FIELD_OWNERSHIP_V1,
  GDI_WORKFLOW_LIFECYCLE,
} from "./gdi-airtable-field-ownership-v1.js";
import { publishHotelInventoryOneClick } from "../durable-publication/gdi-one-click-hotel-publish-v1.js";
import { resolveApprovedPublicationOpportunities } from "../durable-publication/gdi-approved-publication-opportunity-source-v1.js";
import { verifyGdiHotelFullContentParity } from "../presentation/gdi-hotel-full-content-parity-verify-v1.js";
import { runGdiPortfolioFullContentParityAudit } from "../presentation/gdi-portfolio-full-content-parity-v1.js";
import { recordHotelParityStatus } from "../presentation/gdi-full-content-parity-status-store-v1.js";

export const GDI_UNIVERSAL_OPERATIONAL_WORKFLOW_V1 =
  "gdi-universal-operational-workflow-v1";

export const WORKFLOW_ACTIONS = Object.freeze([
  "Research",
  "Validate",
  "SyncAirtable",
  "Review",
  "Publish",
  "Verify",
  "FullRun",
  "Inventory",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function newRunId(hotelId) {
  return `gdi_wf_${text(hotelId).slice(0, 12)}_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
}

function resolveHotelName(hotelId) {
  try {
    const hotels = listGdiSelectableHotels() || [];
    const hit = hotels.find((h) => (h.hotelId || h.id) === hotelId);
    return hit?.displayName || hit?.hotelName || hit?.name || null;
  } catch {
    return null;
  }
}

function ensureReportDir(reportPath) {
  const dir = reportPath
    ? path.resolve(reportPath)
    : path.join(
        REPO_ROOT,
        "reports/gdi-universal-operational-workflow-v1",
      );
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(value, null, 2) + "\n", "utf8");
}

function emptyReport(ctx) {
  return {
    contractVersion: GDI_UNIVERSAL_OPERATIONAL_WORKFLOW_V1,
    ownershipContract: GDI_AIRTABLE_FIELD_OWNERSHIP_V1,
    syncContract: GDI_AIRTABLE_SYNC_V1,
    status: "IN_PROGRESS",
    hotelId: ctx.hotelId,
    hotelName: ctx.hotelName,
    workflowRunId: ctx.runId,
    action: ctx.action,
    environment: ctx.environment,
    dryRun: ctx.dryRun,
    startedAt: nowIso(),
    finishedAt: null,
    researchStatus: null,
    opportunitiesDiscovered: null,
    opportunitiesQualified: null,
    opportunitiesExcluded: null,
    airtableCreated: null,
    airtableUpdated: null,
    airtableUnchanged: null,
    airtableFailed: null,
    approvalStatus: null,
    publicationVersion: null,
    publishedOpportunityCount: null,
    fullContentParityResult: null,
    liveShareVerification: null,
    errors: [],
    nextActions: [],
    stages: {},
  };
}

function runNodeScript(scriptRel, args = [], opts = {}) {
  const script = path.join(REPO_ROOT, scriptRel);
  const nodeArgs = [script, ...args];
  const result = spawnSync(process.execPath, nodeArgs, {
    cwd: REPO_ROOT,
    encoding: "utf8",
    env: { ...process.env, ...(opts.env || {}) },
    timeout: opts.timeoutMs || 0,
  });
  return {
    ok: result.status === 0,
    status: result.status,
    stdout: result.stdout || "",
    stderr: result.stderr || "",
    error: result.error ? String(result.error.message || result.error) : null,
  };
}

async function stageResearch(ctx, report) {
  const args = [
    "--hotel",
    ctx.hotelId,
    "--mode",
    "full-e2e",
  ];
  if (ctx.dryRun || ctx.environment === "Local") {
    args.push("--dry-run");
  }
  if (ctx.resumeRun) {
    args.push("--resume-run", ctx.resumeRun);
  }
  // Never pass paid/publish authorizations from this orchestrator by default.
  report.stages.research = {
    dryRun: ctx.dryRun !== false,
    script: "scripts/gdi-research-to-page-workflow-v1.mjs",
    args,
    note: "Research delegated to existing Full E2E coordinator. Paid spend requires explicit authorize flags on that script (not auto-authorized here).",
  };

  // Safe default: dry-run FullRun / Research records the planned invocation
  // without spawning a long supervisor unless executeResearch=true.
  if (ctx.dryRun !== false && ctx.executeResearch !== true) {
    report.stages.research.invoked = false;
    report.stages.research.plannedOnly = true;
    report.researchStatus = "PLANNED_DRY_RUN";
    report.nextActions.push(
      "To execute research preflight: re-run Research with executeResearch (Node --execute-research) or call gdi-research-to-page-workflow-v1.mjs directly.",
    );
    return true;
  }

  report.stages.research.invoked = true;
  const result = runNodeScript(
    "scripts/gdi-research-to-page-workflow-v1.mjs",
    args,
    { timeoutMs: 600_000 },
  );
  report.stages.research.result = {
    ok: result.ok,
    status: result.status,
    stderrTail: String(result.stderr || "").slice(-800),
    stdoutTail: String(result.stdout || "").slice(-800),
  };
  report.researchStatus = result.ok ? "COMPLETED_OR_PREFLIGHT" : "FAILED";
  if (!result.ok) {
    report.errors.push({
      stage: "Research",
      error: result.error || `exit_${result.status}`,
      detail: String(result.stderr || result.stdout).slice(0, 500),
    });
  }
  return result.ok;
}

async function stageValidate(ctx, report) {
  const doc = await loadOpportunitiesCanonical(ctx.hotelId, {
    bypassCache: true,
  });
  const opps = doc?.opportunities || [];
  const excluded = opps.filter(
    (o) =>
      o.customerVisible === false ||
      o.prospectSuppressed === true ||
      o.archived === true,
  );
  const qualified = opps.filter((o) => !excluded.includes(o));
  report.opportunitiesDiscovered = opps.length;
  report.opportunitiesQualified = qualified.length;
  report.opportunitiesExcluded = excluded.length;
  report.stages.validate = {
    ok: true,
    opportunityCount: opps.length,
    qualified: qualified.length,
    excluded: excluded.length,
    persistence: doc?.persistence || null,
  };

  // Contract status script (read-only when possible)
  const status = runNodeScript("scripts/gdi-full-e2e-status-v1.mjs", [
    "--hotel",
    ctx.hotelId,
  ]);
  report.stages.validate.fullE2eStatus = {
    ok: status.ok,
    stdoutTail: String(status.stdout || "").slice(-600),
  };
  return true;
}

async function stageSyncAirtable(ctx, report) {
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: ctx.hotelId,
    dryRun: ctx.dryRun,
    runId: ctx.runId,
    lifecycleHint: GDI_WORKFLOW_LIFECYCLE.READY_FOR_REVIEW,
  });
  report.stages.syncAirtable = sync;
  report.airtableCreated = sync.summary?.created ?? null;
  report.airtableUpdated = sync.summary?.updated ?? null;
  report.airtableUnchanged = sync.summary?.unchanged ?? null;
  report.airtableFailed = sync.summary?.failed ?? null;
  if (!sync.ok && sync.code !== "AIRTABLE_NOT_CONFIGURED") {
    report.errors.push({
      stage: "SyncAirtable",
      error: sync.code || "sync_failed",
      detail: sync.summary?.errors || sync.message,
    });
  } else if (sync.code === "AIRTABLE_NOT_CONFIGURED") {
    report.nextActions.push(
      "Configure AIRTABLE_API_KEY and AIRTABLE_GDI_BASE_ID before live SyncAirtable.",
    );
  }
  return sync.ok || sync.code === "AIRTABLE_NOT_CONFIGURED";
}

async function stageReview(ctx, report) {
  const doc = await loadOpportunitiesCanonical(ctx.hotelId, {
    bypassCache: true,
  });
  const opps = (doc?.opportunities || []).filter(
    (o) =>
      o.customerVisible !== false &&
      o.prospectSuppressed !== true &&
      o.archived !== true,
  );
  const reviewPackage = {
    hotelId: ctx.hotelId,
    hotelName: ctx.hotelName,
    workflowRunId: ctx.runId,
    generatedAt: nowIso(),
    opportunityCount: opps.length,
    opportunities: opps.map((o) => ({
      id: o.id || o.opportunityId,
      title: o.title || o.opportunityName,
      organizationName: o.organizationName,
      priority: o.priority || o.priorityFilterKey,
      salesActionPillLabel: o.salesActionPillLabel,
      commercialInventoryClass:
        o.commercialInventoryClass || o.commercialClassification,
      primaryContact: o.primaryContact?.name || null,
      secondaryContact: o.secondaryContact?.name || null,
      qualificationQuestionsCount: Array.isArray(o.qualificationQuestions)
        ? o.qualificationQuestions.length
        : 0,
      sourcesCount: Array.isArray(o.sources) ? o.sources.length : 0,
    })),
    approvalBoundary:
      "Founder must approve customer-facing inventory before Publish. Pass -ApprovalReference and Production environment.",
  };
  const reviewFile = path.join(
    ctx.reportDir,
    `${ctx.runId}-review-package.json`,
  );
  writeJson(reviewFile, reviewPackage);
  report.stages.review = {
    ok: true,
    reviewFile,
    opportunityCount: opps.length,
    pausedForApproval: true,
  };
  report.approvalStatus = "AWAITING_FOUNDER_APPROVAL";
  report.nextActions.push(
    `Review ${reviewFile}, then re-run Publish with -ApprovalReference and -Environment Production (not DryRun) when authorized.`,
  );
  return true;
}

async function stagePublish(ctx, report) {
  // Dry-run publish plans are allowed without approval. Live apply is gated.
  if (!ctx.dryRun && !ctx.approvalReference) {
    report.approvalStatus = "BLOCKED_MISSING_APPROVAL_REFERENCE";
    report.errors.push({
      stage: "Publish",
      error: "APPROVAL_REFERENCE_REQUIRED",
      detail:
        "Pass -ApprovalReference with founder authorization phrase. Research ≠ publication permission.",
    });
    report.stages.publish = { ok: false, code: "APPROVAL_REFERENCE_REQUIRED" };
    return false;
  }
  if (ctx.environment !== "Production" && ctx.dryRun === false) {
    report.errors.push({
      stage: "Publish",
      error: "PRODUCTION_ENVIRONMENT_REQUIRED_FOR_APPLY",
      detail: "Live publish requires -Environment Production.",
    });
    report.stages.publish = {
      ok: false,
      code: "PRODUCTION_ENVIRONMENT_REQUIRED_FOR_APPLY",
    };
    return false;
  }

  // Prefer local approved customer-facing inventory; fall back to certified pack /
  // registry resolution (Spice path). Never publish from Airtable rows.
  const localDoc = await loadOpportunitiesCanonical(ctx.hotelId, {
    bypassCache: true,
  });
  const localFacing = (localDoc?.opportunities || []).filter(
    (o) =>
      o.customerVisible !== false &&
      o.prospectSuppressed !== true &&
      o.archived !== true &&
      o.isArchived !== true,
  );
  const opportunityIds = localFacing
    .map((o) => text(o.id || o.opportunityId))
    .filter(Boolean);
  const resolved = resolveApprovedPublicationOpportunities({
    hotelId: ctx.hotelId,
    opportunities: localFacing,
    opportunityIds: opportunityIds.length ? opportunityIds : undefined,
  });
  if (!resolved.ok && !localFacing.length) {
    report.stages.publish = { ok: false, resolved };
    report.errors.push({
      stage: "Publish",
      error: resolved.code || "APPROVED_SOURCE_INCOMPLETE",
      detail: resolved.missing || resolved,
    });
    return false;
  }
  const opportunitiesForPublish =
    resolved.ok && resolved.opportunities?.length
      ? resolved.opportunities
      : localFacing;
  if (!opportunitiesForPublish.length) {
    report.stages.publish = { ok: false, code: "NO_APPROVED_OPPORTUNITIES" };
    report.errors.push({
      stage: "Publish",
      error: "NO_APPROVED_OPPORTUNITIES",
    });
    return false;
  }

  const result = publishHotelInventoryOneClick({
    hotelId: ctx.hotelId,
    opportunities: opportunitiesForPublish,
    opportunityIds: opportunitiesForPublish
      .map((o) => text(o.id || o.opportunityId))
      .filter(Boolean),
    dryRun: ctx.dryRun,
    approval: ctx.dryRun
      ? null
      : {
          authorized: true,
          authorizedBy: "founder",
          authorizedAt: nowIso(),
          authorizationPhrase: ctx.approvalReference,
        },
  });

  report.stages.publish = {
    ok: result.ok === true,
    dryRun: result.dryRun,
    publicationStatus: result.publicationStatus || null,
    plan: result.plan || null,
    code: result.code || null,
    postVerify: result.postVerify || null,
  };
  report.approvalStatus = ctx.dryRun
    ? "DRY_RUN_ONLY"
    : result.ok
      ? "AUTHORIZED_APPLIED"
      : result.code || "FAILED";
  report.publicationVersion =
    result.pointer?.oneClickPublishVersion ||
    result.plan?.presentationSchemaVersion ||
    null;
  report.publishedOpportunityCount =
    result.plan?.opportunityCount ||
    result.pointer?.opportunityCount ||
    null;

  if (!result.ok) {
    report.errors.push({
      stage: "Publish",
      error: result.code || "PUBLISH_FAILED",
      detail: result.message || result.verify || null,
    });
    return false;
  }

  if (!ctx.dryRun && result.ok && result.pointerAdvanced) {
    const reconcile = await reconcilePublicationStatusToAirtable({
      hotelId: ctx.hotelId,
      dryRun: false,
      publishedOpportunityIds: result.plan?.opportunityIds || [],
      publicationId: result.pointer?.publicationId,
      publicationVersion: result.pointer?.oneClickPublishVersion,
      publishedAt: result.pointer?.publishedAt,
      fullContentParityStatus: result.publicationStatus || "VERIFIED",
      parityVerifiedAt: nowIso(),
      runId: ctx.runId,
      provenance: {
        workflowRunId: ctx.runId,
        approvalReference: ctx.approvalReference,
        publicationId: result.pointer?.publicationId,
      },
    });
    report.stages.postPublishAirtable = reconcile;
    if (!reconcile.ok) {
      report.nextActions.push(
        "Publication succeeded but Airtable stamp failed — run SyncAirtable reconciliation; do not roll back publication.",
      );
      report.errors.push({
        stage: "PostPublishAirtable",
        error: "RECONCILE_FAILED",
        detail: reconcile.errors,
        publicationRollbackRequired: false,
      });
    }
  }

  return result.ok === true;
}

async function stageVerify(ctx, report) {
  const verify = await verifyGdiHotelFullContentParity(ctx.hotelId, {
    hotelName: ctx.hotelName,
    liveShare: Boolean(ctx.shareToken),
    shareToken: ctx.shareToken || undefined,
    prodBase: ctx.prodBase,
  });
  recordHotelParityStatus(ctx.hotelId, verify);
  report.stages.verify = {
    ok: verify.ok === true,
    status: verify.status,
    publishedOpportunityCount: verify.publishedOpportunityCount,
    fieldsCompared: verify.fieldsCompared,
    criticalDifferenceCount: (verify.criticalDifferences || []).length,
    criticalDifferences: (verify.criticalDifferences || []).slice(0, 25),
  };
  report.fullContentParityResult = verify.status;
  report.publishedOpportunityCount =
    verify.publishedOpportunityCount ?? report.publishedOpportunityCount;
  report.liveShareVerification = ctx.shareToken
    ? verify.opportunities?.some((o) => o.liveVerify && !o.liveVerify.skipped)
      ? "ATTEMPTED"
      : "TOKEN_SUPPLIED_OFFLINE_DTO_ONLY"
    : "NOT_REQUESTED";
  if (!verify.ok && verify.status !== "NOT_PUBLISHED") {
    report.errors.push({
      stage: "Verify",
      error: verify.status || "PARITY_FAILED",
      detail: verify.criticalDifferences?.slice(0, 10),
    });
  }
  return verify.ok === true || verify.status === "NOT_PUBLISHED";
}

async function stageInventory(_ctx, report) {
  const audit = await runGdiPortfolioFullContentParityAudit({});
  report.stages.inventory = {
    ok: true,
    status: audit.status,
    summary: audit.summary,
  };
  return true;
}

/**
 * Run one workflow action.
 */
export async function runGdiUniversalOperationalWorkflow(input = {}) {
  const hotelId = text(input.hotelId);
  const action = text(input.action) || "Validate";
  if (!hotelId && action !== "Inventory") {
    return { ok: false, code: "HOTEL_ID_REQUIRED" };
  }
  if (!WORKFLOW_ACTIONS.includes(action)) {
    return {
      ok: false,
      code: "UNKNOWN_ACTION",
      allowed: WORKFLOW_ACTIONS,
    };
  }

  const ctx = {
    hotelId,
    hotelName: input.hotelName || resolveHotelName(hotelId),
    action,
    dryRun: input.dryRun !== false,
    environment: text(input.environment) || "Local",
    resumeRun: text(input.resumeRun) || null,
    approvalReference: text(input.approvalReference) || null,
    shareToken: text(input.shareToken) || null,
    prodBase:
      text(input.prodBase) ||
      process.env.GDI_PROD_BASE ||
      "https://my-operators-backend-production.up.railway.app",
    executeResearch: input.executeResearch === true,
    runId: text(input.runId) || newRunId(hotelId || "portfolio"),
    reportDir: ensureReportDir(input.reportPath),
  };

  const report = emptyReport(ctx);
  let ok = true;

  try {
    if (action === "Research" || action === "FullRun") {
      ok = (await stageResearch(ctx, report)) && ok;
    }
    if (action === "Validate" || action === "FullRun") {
      ok = (await stageValidate(ctx, report)) && ok;
    }
    if (action === "SyncAirtable" || action === "FullRun") {
      ok = (await stageSyncAirtable(ctx, report)) && ok;
    }
    if (action === "Review" || action === "FullRun") {
      ok = (await stageReview(ctx, report)) && ok;
      if (action === "FullRun") {
        report.status = "PAUSED_AWAITING_APPROVAL";
        report.finishedAt = nowIso();
        report.nextActions.unshift(
          "FullRun stops at Review. Re-run Publish then Verify after founder approval.",
        );
        const outFile = path.join(
          ctx.reportDir,
          `${ctx.runId}-workflow-report.json`,
        );
        writeJson(outFile, report);
        return {
          ok: true,
          paused: true,
          status: "GDI_UNIVERSAL_WORKFLOW_READY",
          report,
          reportFile: outFile,
        };
      }
    }
    if (action === "Publish") {
      ok = (await stagePublish(ctx, report)) && ok;
    }
    if (action === "Verify") {
      ok = (await stageVerify(ctx, report)) && ok;
    }
    if (action === "Inventory") {
      ok = (await stageInventory(ctx, report)) && ok;
    }
  } catch (err) {
    ok = false;
    report.errors.push({
      stage: action,
      error: String(err?.message || err).slice(0, 500),
    });
  }

  report.finishedAt = nowIso();
  report.status = ok
    ? "GDI_UNIVERSAL_WORKFLOW_READY"
    : "GDI_UNIVERSAL_WORKFLOW_INCOMPLETE";
  if (!report.nextActions.length) {
    if (action === "Review") {
      report.nextActions.push("Await founder approval, then Publish + Verify.");
    } else if (action === "Publish" && ctx.dryRun) {
      report.nextActions.push(
        "Dry-run publish only. Authorize Production apply with -ApprovalReference when ready.",
      );
    } else if (action === "Verify" && report.fullContentParityResult === "VERIFIED") {
      report.nextActions.push("Parity VERIFIED. No further action required.");
    }
  }

  const outFile = path.join(ctx.reportDir, `${ctx.runId}-workflow-report.json`);
  writeJson(outFile, report);
  return {
    ok,
    status: report.status,
    paused: false,
    report,
    reportFile: outFile,
  };
}
