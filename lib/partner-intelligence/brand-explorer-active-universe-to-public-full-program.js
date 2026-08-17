/**
 * Active Universe → Public-Full Program (orchestrator).
 *
 * Canonical universe: Brand Basics Brand Status Active/Live (24 brands).
 * Operational cohorts are overlays — never the universe.
 *
 * Lanes:
 *  1. pvql-public-full-scrub
 *  2. restored-pending-validation
 *  3. everhome-remediation
 *  4. unconfigured-full-build
 *  5. final-public-full-validation
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ACTIVE_UNIVERSE_SOURCE, loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  ACTIVE_UNIVERSE_PVQL_SCRUB_TARGETS,
  REQUIRED_APPLY_FLAGS as LANE1_FLAGS,
  planActiveUniversePvqlScrub,
  applyActiveUniversePvqlScrub,
  writeActiveUniversePvqlScrubReports,
} from "./brand-explorer-active-universe-pvql-scrub.js";
import {
  RESTORED_PENDING_TARGETS,
  RESTORED_PENDING_REQUIRED_APPLY_FLAGS,
  planRestoredPendingValidationRepair,
  applyRestoredPendingValidationRepair,
  writeRestoredPendingValidationReports,
} from "./brand-explorer-restored-pending-validation-repair.js";
import {
  EVERHOME_REQUIRED_APPLY_FLAGS,
  planEverhomeActiveRemediation,
  applyEverhomeActiveRemediation,
  writeEverhomeActiveRemediationReports,
} from "./brand-explorer-everhome-active-remediation.js";
import {
  UNCONFIGURED_FULL_BUILD_TARGETS,
  UNCONFIGURED_FULL_BUILD_APPLY_FLAGS,
  UNCONFIGURED_PUBLIC_RESTORE_FLAGS,
  planUnconfiguredFullBuild,
  applyUnconfiguredFullBuild,
  applyUnconfiguredPublicRestore,
  writeUnconfiguredFullBuildReports,
} from "./brand-explorer-unconfigured-full-build.js";

export const PROGRAM_VERSION = "active-universe-to-public-full-v1";
export const PROGRAM_LANES = Object.freeze([
  "pvql-public-full-scrub",
  "restored-pending-validation",
  "everhome-remediation",
  "unconfigured-full-build",
  "final-public-full-validation",
]);

export const REPORT_JSON = "brand-explorer-active-universe-to-public-full-program.json";
export const REPORT_MD = "brand-explorer-active-universe-to-public-full-program.md";
export const DOC_MD = "brand-explorer-active-universe-to-public-full-program.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function parseProgramArgs(argv = []) {
  const laneIdx = argv.indexOf("--lane");
  const laneRaw = laneIdx >= 0 ? nz(argv[laneIdx + 1]) : null;
  const lane = laneRaw && PROGRAM_LANES.includes(laneRaw) ? laneRaw : null;
  return {
    dryRun: !argv.includes("--apply") && !argv.includes("--apply-public-restore"),
    apply: argv.includes("--apply"),
    applyPublicRestore: argv.includes("--apply-public-restore"),
    lane,
    allLanes: !lane,
    argv,
  };
}

async function runLane1(opts) {
  const plan = await planActiveUniversePvqlScrub({
    brands: [...ACTIVE_UNIVERSE_PVQL_SCRUB_TARGETS],
  });
  let applyResult = { applied: false, reason: "dry_run_only" };
  if (opts.apply) {
    const missing = LANE1_FLAGS.filter((f) => !opts.argv.includes(f));
    if (missing.length) {
      applyResult = { applied: false, reason: "missing_apply_flags", missing };
    } else {
      applyResult = await applyActiveUniversePvqlScrub({
        report: plan,
        apply: true,
        argv: opts.argv,
      });
    }
  }
  const paths = writeActiveUniversePvqlScrubReports(plan, applyResult);
  return {
    lane: "pvql-public-full-scrub",
    targets: [...ACTIVE_UNIVERSE_PVQL_SCRUB_TARGETS],
    summary: plan.summary,
    validation: plan.validation,
    applyResult,
    paths,
    note:
      "Lane 1 may already be complete from prior active-universe-pvql-scrub apply; re-run is idempotent when offenders=0.",
  };
}

async function runLane2(opts) {
  const plan = await planRestoredPendingValidationRepair({
    brands: [...RESTORED_PENDING_TARGETS],
  });
  let applyResult = { applied: false, reason: "dry_run_only" };
  if (opts.apply) {
    const missing = RESTORED_PENDING_REQUIRED_APPLY_FLAGS.filter((f) => !opts.argv.includes(f));
    if (missing.length) {
      applyResult = { applied: false, reason: "missing_apply_flags", missing };
    } else {
      applyResult = await applyRestoredPendingValidationRepair({
        report: plan,
        apply: true,
        argv: opts.argv,
      });
    }
  }
  const paths = writeRestoredPendingValidationReports(plan, applyResult);
  return {
    lane: "restored-pending-validation",
    targets: [...RESTORED_PENDING_TARGETS],
    summary: plan.summary,
    validation: plan.validation,
    applyResult,
    paths,
  };
}

async function runLane3(opts) {
  const plan = await planEverhomeActiveRemediation();
  let applyResult = { applied: false, reason: "dry_run_only" };
  if (opts.apply) {
    const missing = EVERHOME_REQUIRED_APPLY_FLAGS.filter((f) => !opts.argv.includes(f));
    if (missing.length) {
      applyResult = { applied: false, reason: "missing_apply_flags", missing };
    } else {
      applyResult = await applyEverhomeActiveRemediation({
        report: plan,
        apply: true,
        argv: opts.argv,
      });
    }
  }
  const paths = writeEverhomeActiveRemediationReports(plan, applyResult);
  return {
    lane: "everhome-remediation",
    targets: ["everhome-suites"],
    summary: plan.summary,
    validation: plan.validation,
    applyResult,
    paths,
  };
}

async function runLane4(opts) {
  const plan = await planUnconfiguredFullBuild({
    brands: [...UNCONFIGURED_FULL_BUILD_TARGETS],
  });
  let applyResult = { applied: false, reason: "dry_run_only" };
  if (opts.applyPublicRestore) {
    const missing = UNCONFIGURED_PUBLIC_RESTORE_FLAGS.filter((f) => !opts.argv.includes(f));
    if (missing.length) {
      applyResult = { applied: false, reason: "missing_apply_flags", missing };
    } else {
      applyResult = await applyUnconfiguredPublicRestore({
        report: plan,
        apply: true,
        argv: opts.argv,
      });
    }
  } else if (opts.apply) {
    const missing = UNCONFIGURED_FULL_BUILD_APPLY_FLAGS.filter((f) => !opts.argv.includes(f));
    if (missing.length) {
      applyResult = { applied: false, reason: "missing_apply_flags", missing };
    } else {
      applyResult = await applyUnconfiguredFullBuild({
        report: plan,
        apply: true,
        argv: opts.argv,
      });
    }
  }
  const paths = writeUnconfiguredFullBuildReports(plan, applyResult);
  return {
    lane: "unconfigured-full-build",
    targets: [...UNCONFIGURED_FULL_BUILD_TARGETS],
    summary: plan.summary,
    validation: plan.validation,
    applyResult,
    paths,
    blockedUntilContentPacks: plan.blockedUntilContentPacks === true,
  };
}

async function runFinalValidation() {
  const universe = await loadActiveUniverse({ includeDetails: true });
  const inventory = universe.brands.map((b) => ({
    slug: b.slug,
    name: b.name,
    recordId: b.recordId,
    brandStatus: b.status,
    presentationRows: b.presentationRowCount || 0,
    publicFull: b.publicFull === true,
    displayState: b.displayState || null,
    ready: b.readyForActiveProfile === true,
    approved: b.activeProfileApproved === true,
    founder: b.founderVisualReviewPass === true,
  }));
  const publicFull = inventory.filter((b) => b.publicFull);
  const missingPublic = inventory.filter((b) => !b.publicFull);
  const zeroRows = inventory.filter((b) => (b.presentationRows || 0) === 0);

  return {
    lane: "final-public-full-validation",
    activeUniverse: {
      ...ACTIVE_UNIVERSE_SOURCE,
      totalCount: universe.totalCount,
      reconcilesTo24: universe.totalCount === 24,
    },
    inventory,
    summary: {
      activeCount: universe.totalCount,
      publicFullCount: publicFull.length,
      notPublicFullCount: missingPublic.length,
      unconfiguredCount: zeroRows.length,
      publicFullSlugs: publicFull.map((b) => b.slug),
      notPublicFullSlugs: missingPublic.map((b) => b.slug),
      unconfiguredSlugs: zeroRows.map((b) => b.slug),
    },
    acceptance: {
      activeCountIs24: universe.totalCount === 24,
      publicFullIs24: publicFull.length === 24,
      noUnconfigured: zeroRows.length === 0,
      excludedStatusConflicts: [
        "radisson-collection",
        "tapestry-collection-by-hilton",
      ],
      nextCommands: [
        "npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only",
        "npm run brand-explorer-os -- --stage release-readiness --dry-run --skip-regression",
        "npm run test:brand-explorer-mandatory-release-gates",
        "npm run brand-explorer-active-universe-source-of-truth -- --dry-run",
      ],
    },
    note:
      "This lane inventories Active/Live display state. Full PVQL/OS/mandatory gates are separate npm commands listed in acceptance.nextCommands.",
  };
}

/**
 * @param {{ dryRun?: boolean, apply?: boolean, applyPublicRestore?: boolean, lane?: string|null, argv?: string[] }} opts
 */
export async function runActiveUniverseToPublicFullProgram(opts = {}) {
  const parsed = {
    dryRun: opts.dryRun !== false && !opts.apply && !opts.applyPublicRestore,
    apply: opts.apply === true,
    applyPublicRestore: opts.applyPublicRestore === true,
    lane: opts.lane || null,
    argv: opts.argv || [],
  };

  const lanesToRun = parsed.lane ? [parsed.lane] : [...PROGRAM_LANES];
  const laneResults = [];

  for (const lane of lanesToRun) {
    console.log(`[${PROGRAM_VERSION}] lane=${lane} mode=${parsed.apply || parsed.applyPublicRestore ? "APPLY" : "dry-run"}`);
    if (lane === "pvql-public-full-scrub") {
      laneResults.push(await runLane1(parsed));
    } else if (lane === "restored-pending-validation") {
      laneResults.push(await runLane2(parsed));
    } else if (lane === "everhome-remediation") {
      laneResults.push(await runLane3(parsed));
    } else if (lane === "unconfigured-full-build") {
      laneResults.push(await runLane4(parsed));
    } else if (lane === "final-public-full-validation") {
      laneResults.push(await runFinalValidation());
    }
  }

  const report = {
    version: PROGRAM_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: parsed.dryRun,
    apply: parsed.apply,
    applyPublicRestore: parsed.applyPublicRestore,
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE,
    lanesRequested: lanesToRun,
    laneResults,
    excludedFromUniverse: [
      { slug: "radisson-collection", reason: "Brand Status Draft — not Active/Live" },
      { slug: "tapestry-collection-by-hilton", reason: "Brand Status Under Review — not Active/Live" },
    ],
    guardrails: {
      companyValidatedUntouched: true,
      sourceLibraryUntouched: true,
      registryUntouched: true,
      brandStatusUntouched: true,
      stale23NotUsedAsUniverse: true,
    },
  };

  return report;
}

export function writeProgramReports(report) {
  const reportsDir = path.join(ROOT, "reports");
  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  const docPath = path.join(docsDir, DOC_MD);

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    "# Active Universe → Public-Full Program",
    "",
    `Version: \`${report.version}\` · Generated: ${report.generatedAt}`,
    `Mode: **${report.apply || report.applyPublicRestore ? "APPLY" : "dry-run"}**`,
    "",
    "## Active universe source of truth",
    "",
    `- ${report.activeUniverseSource.name}`,
    `- \`${report.activeUniverseSource.formula}\``,
    "",
    "## Lanes",
    "",
  ];
  for (const lr of report.laneResults || []) {
    lines.push(`### ${lr.lane}`);
    lines.push("");
    lines.push(`Targets: ${(lr.targets || []).map((s) => `\`${s}\``).join(", ") || "—"}`);
    if (lr.summary) {
      lines.push("");
      lines.push("```json");
      lines.push(JSON.stringify(lr.summary, null, 2));
      lines.push("```");
    }
    if (lr.applyResult) {
      lines.push("");
      lines.push(`Applied: **${lr.applyResult.applied === true}** (${lr.applyResult.reason || "ok"})`);
    }
    if (lr.acceptance) {
      lines.push("");
      lines.push("```json");
      lines.push(JSON.stringify(lr.acceptance, null, 2));
      lines.push("```");
    }
    lines.push("");
  }
  lines.push("## Excluded from active universe", "");
  for (const e of report.excludedFromUniverse || []) {
    lines.push(`- \`${e.slug}\` — ${e.reason}`);
  }
  lines.push("");
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");

  const doc = `# Active Universe → Public-Full Program

Orchestrates getting all **24 Active/Live** Brand Explorer brands to public-full + PVQL-clean.

## Source of truth

- Brand Basics \`Brand Status\` Active/Live
- \`lib/partner-intelligence/brand-explorer-active-universe.js\`
- \`lib/brand-status-active.js\`

Operational cohorts (PRIMARY_RELEASE, Lane 1/2 restore lists, prior 23) are **not** the universe.

## Lanes

1. \`pvql-public-full-scrub\` — 16 current public-full PVQL failures
2. \`restored-pending-validation\` — Quality Inn / Radisson / Blu / RED
3. \`everhome-remediation\` — Everhome targeted fixes
4. \`unconfigured-full-build\` — BW Premier / BW Signature / Preferred (full Tab Factory)
5. \`final-public-full-validation\` — inventory + next validation commands

## Run

\`\`\`bash
npm run brand-explorer-active-universe-to-public-full-program -- --dry-run
npm run brand-explorer-active-universe-to-public-full-program -- --lane pvql-public-full-scrub --dry-run
npm run brand-explorer-active-universe-to-public-full-program -- --lane restored-pending-validation --dry-run
npm run brand-explorer-active-universe-to-public-full-program -- --lane everhome-remediation --dry-run
npm run brand-explorer-active-universe-to-public-full-program -- --lane unconfigured-full-build --dry-run
npm run brand-explorer-active-universe-to-public-full-program -- --lane final-public-full-validation --dry-run
\`\`\`

## Forbidden

Company Validated, Source Library, Registry, Brand Status, Radisson Collection / Tapestry status, stale 23-brand universe.

Latest: \`reports/${REPORT_JSON}\`
`;
  fs.writeFileSync(docPath, `${doc}\n`, "utf8");

  return { jsonPath, mdPath, docPath };
}
