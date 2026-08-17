/**
 * Brand Explorer Wave 13 factory orchestrator.
 *
 * Stages: preflight → manifest → factory-preview-cohort → source-packs → …
 *
 * Guardrails:
 * - Never writes protected 39 baseline brands.
 * - Manifest is read-only.
 * - Source packs are read-only report generation after clean preflight + manifest.
 * - Tab-factory-build remains deferred until source packs + open items are cleared.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  EXPECTED_ACTIVE_COUNT_39,
  BASELINE_VERSION_39,
} from "./brand-explorer-39-active-public-full-baseline.js";
import {
  WAVE13_VERSION,
  WAVE13_STAGES,
  WAVE13_SLUGS,
} from "./brand-explorer-wave13-factory-plan.js";
import { runWave13Manifest } from "./brand-explorer-wave13-manifest.js";
import { runWave13FactoryPreviewCohort } from "./brand-explorer-wave13-factory-preview-cohort.js";

export { WAVE13_VERSION, WAVE13_STAGES, WAVE13_SLUGS };

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

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
  fs.writeFileSync(mdPath, md);
  return { jsonPath, mdPath };
}

function readJson(name) {
  const p = path.join(REPORTS_DIR, name);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
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

/**
 * STAGE 0 — Protected 39 live-clean preflight (read-only).
 */
export async function runWave13Preflight({
  skipLongGates = false,
  reuseFreshReports = false,
  maxReportAgeMs = 2 * 60 * 60 * 1000,
} = {}) {
  const gates = [];
  const existingPvql = readJson("brand-explorer-public-visibility-quality-lock.json");
  const existingQuality = readJson("brand-explorer-24-tab-section-quality-audit.json");
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
      ],
    });
  }
  commands.push(
    [
      "test:brand-explorer-39-active-public-full-baseline",
      "--",
      "--allow-cached-pvql-if-pass",
    ],
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
    console.log(`[wave13-preflight] running npm run ${args.join(" ")}`);
    const r = runNpm(args);
    gates.push({
      command: `npm run ${args.join(" ")}`,
      ok: r.ok,
      status: r.status,
      error: r.error,
      tail: `${r.stdout}\n${r.stderr}`.split(/\r?\n/).filter(Boolean).slice(-10),
    });
    if (!r.ok) break;
  }

  const universe = await loadActiveUniverse({ includeDetails: false });
  const pvql = readJson("brand-explorer-public-visibility-quality-lock.json");
  const quality = readJson("brand-explorer-24-tab-section-quality-audit.json");

  const forbiddenIssues = assertNoForbiddenOwnerFacingInPvql(pvql);
  const publicFullCount = (pvql?.brands || []).filter((b) => b.publicFullProfile === true).length;
  const publicFullPass =
    publicFullCount === EXPECTED_ACTIVE_COUNT_39 &&
    (pvql?.brands || [])
      .filter((b) => b.publicFullProfile === true)
      .every((b) => b.lockPass === true);

  const qualityApproveCount = (quality?.brandResults || []).filter(
    (b) => b.overallRecommendation === "approve_for_baseline_freeze"
  ).length;
  const qualityOk =
    quality?.baselineFreezeDecision === "ready_to_freeze_45_active_public_full_baseline" ||
    quality?.baselineFreezeDecision === "ready_to_freeze_39_active_public_full_baseline" ||
    (qualityApproveCount >= EXPECTED_ACTIVE_COUNT_39 &&
      (quality?.recommendationCounts?.remediation_required || 0) === 0);

  const issues = [];
  if (universe.totalCount !== EXPECTED_ACTIVE_COUNT_39) {
    issues.push(`active_count_not_39:got=${universe.totalCount}`);
  }
  if (!publicFullPass) issues.push("fresh_pvql_public_full_not_clean");
  if (forbiddenIssues.length) {
    issues.push(`forbidden_owner_facing:${forbiddenIssues.slice(0, 20).join("|")}`);
  }
  if (!qualityOk) {
    issues.push(
      `quality_not_freeze_ready:decision=${quality?.baselineFreezeDecision || "missing"}:approve=${qualityApproveCount}:remediation=${quality?.recommendationCounts?.remediation_required ?? "?"}`
    );
  }
  for (const g of gates) {
    if (!g.ok) issues.push(`gate_failed:${g.command}`);
  }

  const pass = gates.every((g) => g.ok) && issues.length === 0;
  const readyStatement = pass
    ? "protected_39_live_clean_wave13_may_resume"
    : "wave13_preflight_blocked_protected_39_not_live_clean";

  const report = {
    version: WAVE13_VERSION,
    stage: "preflight",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    airtableWrites: false,
    presentationWrites: false,
    wave13SourcePacksStarted: false,
    wave13ContentGenerationStarted: false,
    cachedPvqlAccepted: false,
    requireFreshLivePvql: true,
    reusedFreshReports: !!canReuse,
    protectedBaselineVersion: BASELINE_VERSION_39,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_39,
    liveActiveCount: universe.totalCount,
    publicFullCount,
    publicFullPass,
    qualityApproveCount,
    qualityDecision: quality?.baselineFreezeDecision || null,
    forbiddenOwnerFacingIssues: forbiddenIssues,
    pass,
    stopRecommended: !pass,
    issues,
    gates,
    readyStatement,
    summary: {
      protected39LiveClean: pass,
      activeUniverseIs39: universe.totalCount === 39,
      freshPvqlRequired: true,
      cachedPvqlRejected: true,
    },
  };

  const md = [
    `# Brand Explorer Wave 13 — Preflight`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Pass: **${report.pass}**`,
    `Ready: **${report.readyStatement}**`,
    ``,
    `| Metric | Value |`,
    `| --- | --- |`,
    `| Protected baseline | ${report.protectedBaselineVersion} |`,
    `| Live Active/Live count | ${report.liveActiveCount} (expected ${report.expectedActiveCount}) |`,
    `| Fresh live PVQL required | true |`,
    `| Cached PVQL accepted | false |`,
    `| Public-full clean | ${report.publicFullPass} (${report.publicFullCount}/39) |`,
    `| Quality approve_for_baseline_freeze | ${report.qualityApproveCount}/39 |`,
    `| Quality decision | ${report.qualityDecision || "—"} |`,
    `| Airtable writes | false |`,
    `| Wave 13 source packs | not started |`,
    ``,
    `## Gates`,
    ``,
    ...gates.map(
      (g) =>
        `- ${g.ok ? "PASS" : "FAIL"} \`${g.command}\`${g.tail?.length ? `\n  - ${g.tail.slice(-3).join(" / ")}` : ""}`
    ),
    ``,
    report.pass
      ? "Preflight clean — Wave 13 may resume (next: manifest dry-run)."
      : "**STOP** — do not start Wave 13 source packs or content generation until protected 39 is live-clean.",
    ``,
  ].join("\n");

  const paths = writeJsonMd("brand-explorer-wave13-preflight", report, md);
  return { ...report, paths };
}

export async function runWave13Factory({ stage, dryRun = true, argv = [] } = {}) {
  const s = String(stage || "").trim();
  if (!WAVE13_STAGES.includes(s)) {
    throw new Error(`Unknown wave13 stage ${s}. Allowed: ${WAVE13_STAGES.join(", ")}`);
  }
  if (s === "preflight") {
    return runWave13Preflight({
      skipLongGates: argv.includes("--skip-long-gates"),
      reuseFreshReports: argv.includes("--reuse-fresh-reports"),
    });
  }
  if (s === "manifest") {
    return runWave13Manifest({
      dryRun: dryRun !== false,
      reuseFreshReports: argv.includes("--reuse-fresh-reports"),
    });
  }
  if (s === "factory-preview-cohort") {
    return runWave13FactoryPreviewCohort({
      apply: argv.includes("--apply"),
      argv,
    });
  }
  if (s === "source-packs") {
    const { runWave13SourcePacks } = await import("./brand-explorer-wave13-source-packs.js");
    return runWave13SourcePacks({
      dryRun: dryRun !== false,
      reuseFreshReports: argv.includes("--reuse-fresh-reports"),
    });
  }
  if (s === "open-items-resolution") {
    const { runWave13OpenItemsResolution } = await import(
      "./brand-explorer-wave13-open-items-resolution.js"
    );
    return runWave13OpenItemsResolution({
      dryRun: dryRun !== false && !argv.includes("--apply"),
      apply: argv.includes("--apply"),
      argv,
      runPostValidation: !argv.includes("--skip-post-validation"),
    });
  }
  if (s === "tab-factory-build") {
    const { runWave13TabFactoryBuild } = await import("./brand-explorer-wave13-tab-factory-build.js");
    return runWave13TabFactoryBuild({
      dryRun: dryRun !== false && !argv.includes("--apply"),
      argv,
    });
  }
  if (s === "stage4-content-cleanup") {
    const { runWave13Stage4ContentCleanup } = await import("./brand-explorer-wave13-tab-factory-build.js");
    return runWave13Stage4ContentCleanup({
      dryRun: dryRun !== false && !argv.includes("--apply"),
      argv,
    });
  }
  if (s === "image-materialization") {
    const { runWave13ImageMaterialization } = await import(
      "./brand-explorer-wave13-image-materialization.js"
    );
    return runWave13ImageMaterialization({
      dryRun: dryRun !== false && !argv.includes("--apply"),
      argv,
    });
  }
  if (s === "post-image-content-cleanup") {
    const { runWave13PostImageContentCleanup } = await import(
      "./brand-explorer-wave13-post-image-content-cleanup.js"
    );
    return runWave13PostImageContentCleanup({
      dryRun: dryRun !== false && !argv.includes("--apply"),
      argv,
    });
  }
  if (s === "founder-review") {
    const { runWave13FounderReview } = await import("./brand-explorer-wave13-founder-review.js");
    return runWave13FounderReview({ argv });
  }
  if (s === "status-promotion") {
    const { runWave13StatusPromotion } = await import(
      "./brand-explorer-wave13-status-promotion.js"
    );
    return runWave13StatusPromotion({
      apply: argv.includes("--apply"),
      argv,
      skipBaselineNpm: argv.includes("--skip-baseline-npm"),
    });
  }
  if (s === "public-release") {
    const { runWave13PublicRelease } = await import("./brand-explorer-wave13-public-release.js");
    return runWave13PublicRelease({
      apply: argv.includes("--apply"),
      argv,
    });
  }
  if (s === "value-scenario-pattern-cleanup") {
    const { runWave13ValueScenarioPatternCleanup } = await import(
      "./brand-explorer-wave13-value-scenario-pattern-cleanup.js"
    );
    return runWave13ValueScenarioPatternCleanup({
      dryRun: dryRun !== false && !argv.includes("--apply"),
      argv,
    });
  }
  if (s === "public-six-geo-momentum-cleanup") {
    const { runWave13PublicSixGeoMomentumCleanup } = await import(
      "./brand-explorer-wave13-public-six-geo-momentum-cleanup.js"
    );
    return runWave13PublicSixGeoMomentumCleanup({
      dryRun: dryRun !== false && !argv.includes("--apply"),
      argv,
    });
  }
  if (s === "so-hold-remediation") {
    const { runWave13SoHoldRemediation } = await import(
      "./brand-explorer-wave13-so-hold-remediation.js"
    );
    return runWave13SoHoldRemediation({
      dryRun: dryRun !== false && !argv.includes("--apply"),
      argv,
    });
  }
  if (s === "so-status-promotion") {
    const { runWave13SoStatusPromotion } = await import(
      "./brand-explorer-wave13-so-status-promotion.js"
    );
    return runWave13SoStatusPromotion({
      apply: argv.includes("--apply"),
      argv,
    });
  }
  if (s === "so-public-release") {
    const { runWave13SoPublicRelease } = await import(
      "./brand-explorer-wave13-so-public-release.js"
    );
    return runWave13SoPublicRelease({
      apply: argv.includes("--apply"),
      argv,
    });
  }
  if (s === "so-section-pattern-cleanup") {
    const { runWave13SoSectionPatternCleanup } = await import(
      "./brand-explorer-wave13-so-section-pattern-cleanup.js"
    );
    return runWave13SoSectionPatternCleanup({
      apply: argv.includes("--apply"),
      argv,
    });
  }
  const deferred = {
    version: WAVE13_VERSION,
    stage: s,
    dryRun: true,
    deferred: true,
    pass: false,
    stopRecommended: true,
    airtableWrites: false,
    message: `Wave 13 stage "${s}" is deferred — complete open-items-resolution first.`,
    nextRequired:
      "npm run brand-explorer-wave13-factory -- --stage open-items-resolution --dry-run",
    summary: { deferred: true, stage: s },
  };
  const paths = writeJsonMd(
    `brand-explorer-wave13-${s}-deferred`,
    deferred,
    `# Wave 13 ${s} — Deferred\n\n${deferred.message}\n`
  );
  return { ...deferred, paths };
}
