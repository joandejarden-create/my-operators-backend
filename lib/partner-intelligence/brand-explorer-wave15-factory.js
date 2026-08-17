/**
 * Brand Explorer Wave 15 factory orchestrator (Hilton Brand Family cohort).
 *
 * Stages in this task: preflight → manifest → factory-preview-cohort → source-packs.
 * Guardrails: no Presentation builds, no image materialization, no Brand Status
 * promotion, no release fields, no protected-54 baseline changes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  EXPECTED_ACTIVE_COUNT_54,
  BASELINE_VERSION_54,
  BASELINE_54_ACCEPTED_MINOR_CLEANUP_SLUGS,
} from "./brand-explorer-54-active-public-full-baseline.js";
import {
  WAVE15_VERSION,
  WAVE15_STAGES,
  WAVE15_SLUGS,
  WAVE15_PROTECTED_BASELINE_COUNT,
} from "./brand-explorer-wave15-factory-plan.js";
import { runWave15Manifest } from "./brand-explorer-wave15-manifest.js";
import { runWave15FactoryPreviewCohort } from "./brand-explorer-wave15-factory-preview-cohort.js";

export { WAVE15_VERSION, WAVE15_STAGES, WAVE15_SLUGS };

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

const READY_STATEMENT = "protected_54_live_clean_wave15_may_resume";
const DEFAULT_REUSE_MAX_AGE_MS = 6 * 60 * 60 * 1000;

function runNpm(scriptArgs, { timeoutMs = 90 * 60 * 1000 } = {}) {
  const result = spawnSync("npm", ["run", ...scriptArgs], {
    cwd: ROOT,
    encoding: "utf8",
    timeout: timeoutMs,
    maxBuffer: 20 * 1024 * 1024,
    shell: true,
  });
  return {
    ok: result.status === 0,
    status: result.status,
    error: result.error ? String(result.error.message || result.error) : null,
    stdout: result.stdout || "",
    stderr: result.stderr || "",
  };
}

function writeJsonMd(base, report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, `${base}.json`);
  const mdPath = path.join(REPORTS_DIR, `${base}.md`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`);
  return { jsonPath, mdPath };
}

function readJson(name) {
  const p = path.join(REPORTS_DIR, name);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    console.error(`[wave15-preflight] failed reading ${name}:`, err?.message || err);
    return null;
  }
}

function assertNoForbiddenOwnerFacingInPvql(pvql) {
  const issues = [];
  for (const b of pvql?.brands || []) {
    if (b.publicFullProfile !== true) continue;
    const hits = b.gateResults?.forbidden_owner_facing_language?.hits || [];
    for (const h of hits) {
      issues.push(`${b.slug}:${h.slotKey || "?"}:${h.id || h.label || "forbidden"}`);
    }
    if (b.lockPass !== true) {
      issues.push(`${b.slug}:lockPass_false:${(b.failures || []).join(",")}`);
    }
  }
  return issues;
}

function footnoteAuditClean({ standardization, enrichedAudit, rawAudit } = {}) {
  if (
    standardization?.readyState === "ai_assisted_profile_footnote_standardized_globally" ||
    standardization?.acceptance?.ai_assisted_profile_footnote_standardized_globally === true ||
    standardization?.acceptance?.everyActiveProfileFootnoteVisible === true
  ) {
    return true;
  }
  const enriched =
    standardization?.enrichedAuditSummary ||
    enrichedAudit?.summary ||
    null;
  if (
    enriched &&
    Number(enriched.activeCount) === EXPECTED_ACTIVE_COUNT_54 &&
    Number(enriched.fail || 0) === 0 &&
    Number(enriched.footnoteMissingCount || 0) === 0
  ) {
    return true;
  }
  // Raw audit alone is not sufficient (pre-enricher gaps are expected).
  void rawAudit;
  return false;
}

/**
 * STAGE 0 — Protected 54 live-clean preflight (read-only).
 */
export async function runWave15Preflight({
  skipLongGates = false,
  reuseFreshReports = false,
  maxReportAgeMs = DEFAULT_REUSE_MAX_AGE_MS,
} = {}) {
  const gates = [];
  const existingPvql = readJson("brand-explorer-public-visibility-quality-lock.json");
  const existingQuality = readJson("brand-explorer-24-tab-section-quality-audit.json");
  const existing54 = readJson("brand-explorer-54-active-public-full-baseline.json");
  const pvqlAgeMs = existingPvql?.generatedAt
    ? Date.now() - new Date(existingPvql.generatedAt).getTime()
    : Number.POSITIVE_INFINITY;
  const qualityAgeMs = existingQuality?.generatedAt
    ? Date.now() - new Date(existingQuality.generatedAt).getTime()
    : Number.POSITIVE_INFINITY;

  const canReuse =
    reuseFreshReports &&
    pvqlAgeMs >= 0 &&
    pvqlAgeMs <= maxReportAgeMs &&
    qualityAgeMs >= 0 &&
    qualityAgeMs <= maxReportAgeMs;

  const commands = [];
  commands.push([
    "test:brand-explorer-54-active-public-full-baseline",
    "--",
    "--allow-cached-pvql-if-pass",
  ]);

  if (!canReuse) {
    commands.push(
      ["test:brand-explorer-public-visibility-quality-lock", "--", "--public-full-only"],
      ["brand-explorer-24-tab-section-quality-audit", "--", "--dry-run"]
    );
  } else {
    gates.push({
      command: "reuse-fresh-reports:pvql+quality",
      ok: true,
      status: 0,
      error: null,
      tail: [
        `pvqlAgeMin=${Math.round(pvqlAgeMs / 60000)}`,
        `qualityAgeMin=${Math.round(qualityAgeMs / 60000)}`,
        `pvqlAt=${existingPvql.generatedAt}`,
        `qualityAt=${existingQuality.generatedAt}`,
        `baseline54At=${existing54?.generatedAt || "n/a"}`,
      ],
    });
  }

  commands.push(
    ["brand-explorer-ai-assisted-footnote-standardization", "--", "--audit"],
    ["test:brand-explorer-recent-momentum-evidence-quality"],
    ["test:brand-explorer-mandatory-release-gates"]
  );

  if (!skipLongGates) {
    commands.push([
      "brand-explorer-os",
      "--",
      "--stage",
      "release-readiness",
      "--dry-run",
      "--skip-regression",
    ]);
  }

  for (const args of commands) {
    console.log(`[wave15-preflight] running npm run ${args.join(" ")}`);
    const r = runNpm(args);
    gates.push({
      command: `npm run ${args.join(" ")}`,
      ok: r.ok,
      status: r.status,
      error: r.error,
      tail: `${r.stdout}\n${r.stderr}`.split(/\r?\n/).filter(Boolean).slice(-12),
    });
    if (!r.ok) break;
  }

  const universe = await loadActiveUniverse({ includeDetails: false });
  const pvql = readJson("brand-explorer-public-visibility-quality-lock.json");
  const quality = readJson("brand-explorer-24-tab-section-quality-audit.json");
  const baseline54 = readJson("brand-explorer-54-active-public-full-baseline.json");
  const footnoteStandardization = readJson(
    "brand-explorer-ai-assisted-footnote-standardization.json"
  );
  const footnoteEnriched = readJson(
    "brand-explorer-ai-assisted-footnote-audit-enriched.json"
  );
  const footnoteRaw = readJson("brand-explorer-ai-assisted-footnote-audit.json");
  const footnoteOk = footnoteAuditClean({
    standardization: footnoteStandardization,
    enrichedAudit: footnoteEnriched,
    rawAudit: footnoteRaw,
  });

  const forbiddenIssues = assertNoForbiddenOwnerFacingInPvql(pvql);
  const publicFullRows = (pvql?.brands || []).filter((b) => b.publicFullProfile === true);
  const publicFullCount = publicFullRows.length;
  const publicFullPass =
    publicFullCount === EXPECTED_ACTIVE_COUNT_54 &&
    publicFullRows.every((b) => b.lockPass === true);

  const qualityApproveCount = (quality?.brandResults || []).filter(
    (b) => b.overallRecommendation === "approve_for_baseline_freeze"
  ).length;
  const qualityMinorAcceptedCount = (quality?.brandResults || []).filter(
    (b) =>
      b.overallRecommendation === "approve_after_minor_cleanup" &&
      BASELINE_54_ACCEPTED_MINOR_CLEANUP_SLUGS.includes(b.slug)
  ).length;
  const qualityRemediationCount = quality?.recommendationCounts?.remediation_required || 0;
  const qualityOk =
    quality?.baselineFreezeDecision === "ready_to_freeze_54_active_public_full_baseline" ||
    quality?.baselineFreezeDecision === "frozen_54_active_public_full_baseline" ||
    quality?.baselineFreezeDecision === "freeze_after_minor_cleanup_pass" ||
    (qualityApproveCount + qualityMinorAcceptedCount === EXPECTED_ACTIVE_COUNT_54 &&
      qualityRemediationCount === 0 &&
      qualityApproveCount >=
        EXPECTED_ACTIVE_COUNT_54 - BASELINE_54_ACCEPTED_MINOR_CLEANUP_SLUGS.length);

  // Frozen 54 already accepted the documented mgallery minor — treat as live-clean.
  const freezeConfirmsMinorOk =
    baseline54?.freezeDecision === "frozen_54_active_public_full_baseline_semantic_clean_flex_held";
  const qualityOkWithFrozenMinor =
    qualityOk ||
    (freezeConfirmsMinorOk &&
      qualityApproveCount + qualityMinorAcceptedCount === EXPECTED_ACTIVE_COUNT_54 &&
      qualityRemediationCount === 0);

  const freezeOk =
    baseline54?.freezeDecision === "frozen_54_active_public_full_baseline_semantic_clean_flex_held" ||
    baseline54?.frozen === true ||
    baseline54?.pass === true;

  const wave15Drift = (universe.brands || [])
    .filter((b) => WAVE15_SLUGS.includes(b.slug))
    .map((b) => b.slug);

  const issues = [];
  if (universe.totalCount !== EXPECTED_ACTIVE_COUNT_54) {
    issues.push(`active_count_not_54:got=${universe.totalCount}`);
  }
  if (!publicFullPass) issues.push("fresh_pvql_public_full_not_clean");
  if (forbiddenIssues.length) {
    issues.push(`forbidden_owner_facing:${forbiddenIssues.slice(0, 20).join("|")}`);
  }
  if (!qualityOkWithFrozenMinor) {
    issues.push(
      `quality_not_freeze_ready:decision=${quality?.baselineFreezeDecision || "missing"}:approve=${qualityApproveCount}:minorAccepted=${qualityMinorAcceptedCount}:remediation=${qualityRemediationCount}`
    );
  }
  if (!footnoteOk) {
    issues.push("ai_assisted_footnote_not_complete_for_54");
  }
  if (!freezeOk && !canReuse) {
    // Soft: 54 regression gate above already enforces freeze when run.
    issues.push(`baseline54_freeze_not_confirmed:decision=${baseline54?.freezeDecision || "missing"}`);
  }
  if (wave15Drift.length) {
    issues.push(`wave15_active_live_drift:${wave15Drift.join(",")}`);
  }
  for (const g of gates) {
    if (!g.ok) issues.push(`gate_failed:${g.command}`);
  }

  const pass = gates.every((g) => g.ok) && issues.length === 0;
  const readyStatement = pass
    ? READY_STATEMENT
    : "wave15_preflight_blocked_protected_54_not_live_clean";

  const report = {
    version: WAVE15_VERSION,
    stage: "preflight",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    airtableWrites: false,
    presentationWrites: false,
    brandStatusWrites: false,
    releaseFieldWrites: false,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    protectedBaselineTouched: false,
    wave15ContentGenerationStarted: false,
    reusedFreshReports: !!canReuse,
    protectedBaselineVersion: BASELINE_VERSION_54,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_54,
    protectedBaselineCount: WAVE15_PROTECTED_BASELINE_COUNT,
    liveActiveCount: universe.totalCount,
    publicFullCount,
    publicFullPass,
    qualityApproveCount,
    qualityDecision: quality?.baselineFreezeDecision || null,
    freezeDecision: baseline54?.freezeDecision || null,
    footnoteAuditOk: footnoteOk,
    forbiddenOwnerFacingIssues: forbiddenIssues,
    wave15ActiveLiveDrift: wave15Drift,
    pass,
    stopRecommended: !pass,
    issues,
    gates,
    readyStatement,
    summary: {
      protected54LiveClean: pass,
      activeUniverseIs54: universe.totalCount === 54,
      publicFullClean: publicFullPass,
      pvqlClean: publicFullPass,
      approveForBaselineFreeze: qualityApproveCount,
      aiAssistedFootnoteComplete: footnoteOk,
      noActiveLiveDrift: wave15Drift.length === 0,
    },
  };

  const md = [
    `# Brand Explorer Wave 15 — Preflight`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Pass: **${report.pass}**`,
    `Ready: **${report.readyStatement}**`,
    ``,
    `| Metric | Value |`,
    `| --- | --- |`,
    `| Protected baseline | ${report.protectedBaselineVersion} |`,
    `| Live Active/Live count | ${report.liveActiveCount} (expected ${report.expectedActiveCount}) |`,
    `| Public-full clean | ${report.publicFullPass} (${report.publicFullCount}/54) |`,
    `| Quality approve_for_baseline_freeze | ${report.qualityApproveCount}/54 |`,
    `| Quality decision | ${report.qualityDecision || "—"} |`,
    `| Freeze decision | ${report.freezeDecision || "—"} |`,
    `| AI-Assisted footnote complete | ${report.footnoteAuditOk} |`,
    `| Wave 15 Active/Live drift | ${wave15Drift.length ? wave15Drift.join(", ") : "none"} |`,
    `| Reused fresh PVQL/quality | ${report.reusedFreshReports} |`,
    `| Airtable writes | false |`,
    ``,
    `## Gates`,
    ``,
    ...gates.map(
      (g) =>
        `- ${g.ok ? "PASS" : "FAIL"} \`${g.command}\`${
          g.tail?.length ? `\n  - ${g.tail.slice(-3).join(" / ")}` : ""
        }`
    ),
    ``,
    ...(issues.length
      ? [`## Issues`, ``, ...issues.map((i) => `- ${i}`), ``]
      : [`## Issues`, ``, `- none`, ``]),
    report.pass
      ? "Preflight clean — Wave 15 may proceed to Stage 1 manifest (dry-run)."
      : "**STOP** — do not start Wave 15 manifest / factory preview / source packs until protected 54 is live-clean.",
    ``,
  ].join("\n");

  const paths = writeJsonMd("brand-explorer-wave15-preflight", report, md);
  return { ...report, paths };
}

export async function runWave15Factory({ stage, dryRun = true, argv = [] } = {}) {
  const s = String(stage || "").trim();
  if (!WAVE15_STAGES.includes(s)) {
    throw new Error(`Unknown wave15 stage ${s}. Allowed: ${WAVE15_STAGES.join(", ")}`);
  }
  if (s === "preflight") {
    return runWave15Preflight({
      skipLongGates: argv.includes("--skip-long-gates"),
      reuseFreshReports: argv.includes("--reuse-fresh-reports"),
    });
  }
  if (s === "manifest") {
    return runWave15Manifest({
      dryRun: dryRun !== false,
      reuseFreshReports: argv.includes("--reuse-fresh-reports"),
    });
  }
  if (s === "factory-preview-cohort") {
    return runWave15FactoryPreviewCohort({
      apply: argv.includes("--apply"),
      argv,
    });
  }
  if (s === "source-packs") {
    const { runWave15SourcePacks } = await import("./brand-explorer-wave15-source-packs.js");
    return runWave15SourcePacks({
      dryRun: dryRun !== false,
      reuseFreshReports: argv.includes("--reuse-fresh-reports"),
    });
  }
  if (s === "tab-factory-build") {
    const { runWave15TabFactoryBuild } = await import(
      "./brand-explorer-wave15-tab-factory-build.js"
    );
    return runWave15TabFactoryBuild({
      dryRun: dryRun !== false && !argv.includes("--apply"),
      argv,
    });
  }
  if (s === "image-materialization") {
    const { runWave15ImageMaterialization } = await import(
      "./brand-explorer-wave15-image-materialization.js"
    );
    return runWave15ImageMaterialization({
      dryRun: dryRun !== false && !argv.includes("--apply"),
      argv,
    });
  }
  if (s === "post-image-content-cleanup") {
    const { runWave15PostImageCleanup } = await import(
      "./brand-explorer-wave15-post-image-content-cleanup.js"
    );
    return runWave15PostImageCleanup({
      dryRun: dryRun !== false && !argv.includes("--apply"),
      argv,
    });
  }
  if (s === "founder-review") {
    const { runWave15FounderReview } = await import("./brand-explorer-wave15-founder-review.js");
    return runWave15FounderReview({ argv });
  }
  if (s === "status-promotion") {
    const { runWave15StatusPromotion } = await import(
      "./brand-explorer-wave15-status-promotion.js"
    );
    return runWave15StatusPromotion({
      apply: argv.includes("--apply"),
      argv,
      skipBaselineNpm: argv.includes("--skip-baseline-npm"),
    });
  }
  if (s === "public-release") {
    const { runWave15PublicRelease } = await import(
      "./brand-explorer-wave15-public-release.js"
    );
    return runWave15PublicRelease({
      apply: argv.includes("--apply"),
      argv,
    });
  }

  const deferred = {
    version: WAVE15_VERSION,
    stage: s,
    dryRun: true,
    deferred: true,
    pass: false,
    stopRecommended: true,
    airtableWrites: false,
    message: `Wave 15 stage "${s}" is out of scope for initial setup — complete Stages 0–3 first.`,
    nextRequired:
      "npm run brand-explorer-wave15-factory -- --stage source-packs --dry-run",
    summary: { deferred: true, stage: s },
  };
  const paths = writeJsonMd(
    `brand-explorer-wave15-${s}-deferred`,
    deferred,
    `# Wave 15 ${s} — Deferred\n\n${deferred.message}\n`
  );
  return { ...deferred, paths };
}
