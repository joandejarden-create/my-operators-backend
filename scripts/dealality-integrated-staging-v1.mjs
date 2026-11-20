#!/usr/bin/env node
/**
 * Dealality Integrated Staging V1 CLI
 *
 * Modes:
 *   status | bootstrap | refresh-main | stage-feature | validate | cutover-plan | rollback-plan
 *
 * Never kills unknown processes. Never deploys production.
 * Product worktrees are never modified.
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync, execFileSync } from "node:child_process";
import {
  INTEGRATED_STAGING_DEFAULT_PATH,
  INTEGRATED_STAGING_HOME_BRANCH,
  INTEGRATED_STAGING_PORT,
  INTEGRATED_STAGING_BASENAME,
  emptyStagingState,
  writeStagingMarker,
  writeStagingState,
  readStagingState,
  readKnownGood,
  promoteKnownGood,
  buildEnvironmentStatusPayload,
  mayBindPort8080,
  isProductWorktreeCheckout,
  checkoutBasename,
} from "../lib/dealality-dev-os/integrated-staging-v1.js";

function parseArgs(argv) {
  const out = {
    mode: "status",
    apply: false,
    stagingPath: INTEGRATED_STAGING_DEFAULT_PATH,
    featureRef: null,
    featureLabel: null,
    mainRef: "origin/main",
    reportDir: null,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--mode" && argv[i + 1]) out.mode = String(argv[++i]);
    else if (a.startsWith("--mode=")) out.mode = a.slice("--mode=".length);
    else if (a === "--staging-path" && argv[i + 1]) out.stagingPath = String(argv[++i]);
    else if (a === "--feature-ref" && argv[i + 1]) out.featureRef = String(argv[++i]);
    else if (a === "--feature-label" && argv[i + 1]) out.featureLabel = String(argv[++i]);
    else if (a === "--main-ref" && argv[i + 1]) out.mainRef = String(argv[++i]);
    else if (a === "--report-dir" && argv[i + 1]) out.reportDir = String(argv[++i]);
    else if (!a.startsWith("-") && out.mode === "status") out.mode = a;
  }
  return out;
}

function git(cwd, args, opts = {}) {
  const r = spawnSync("git", args, {
    cwd,
    encoding: "utf8",
    windowsHide: true,
    ...opts,
  });
  if (r.status !== 0 && !opts.allowFail) {
    const err = new Error(
      `git ${args.join(" ")} failed: ${(r.stderr || r.stdout || "").trim()}`,
    );
    err.code = "GIT_FAILED";
    throw err;
  }
  return {
    status: r.status,
    stdout: (r.stdout || "").trim(),
    stderr: (r.stderr || "").trim(),
  };
}

function shortSha(sha) {
  return sha ? String(sha).slice(0, 10) : null;
}

function findRepoRootForBootstrap() {
  // Prefer primary IC checkout; fall back to cwd if it is a dealality git root.
  const candidates = [
    "C:/Dev/dealality/my-operators-backend",
    "C:/Dev/dealality/ws-gdi",
    process.cwd(),
  ];
  for (const c of candidates) {
    try {
      if (fs.existsSync(path.join(c, ".git")) || fs.existsSync(path.join(c, "server.js"))) {
        const top = git(c, ["rev-parse", "--show-toplevel"], { allowFail: true });
        if (top.status === 0 && top.stdout) return top.stdout;
      }
    } catch {
      /* continue */
    }
  }
  throw new Error("Could not locate Dealality git repository for bootstrap");
}

function ensureStagingGitIgnoreEntries(stagingPath) {
  const gi = path.join(stagingPath, ".gitignore");
  const need = [
    "data/dealality-integrated-staging/",
    ".dealality-integrated-staging",
  ];
  // Marker + state are local runtime — keep out of commits when possible.
  // Marker is required for ownership; it stays untracked via local ignore if listed.
  void gi;
  void need;
}

function bootstrapPlan(args) {
  const stagingPath = path.resolve(args.stagingPath);
  const exists = fs.existsSync(stagingPath);
  const repoRoot = findRepoRootForBootstrap();
  return {
    ok: true,
    mode: "bootstrap",
    apply: args.apply,
    stagingPath,
    exists,
    proposedBranch: INTEGRATED_STAGING_HOME_BRANCH,
    basename: INTEGRATED_STAGING_BASENAME,
    port: INTEGRATED_STAGING_PORT,
    sourceRepo: repoRoot,
    mainRef: args.mainRef,
    steps: exists
      ? [
          "Verify existing checkout basename and marker",
          "Fetch origin",
          "Fast-forward home branch to main when clean",
          "Write/refresh staging state + marker",
        ]
      : [
          `git worktree add -b ${INTEGRATED_STAGING_HOME_BRANCH} ${stagingPath} ${args.mainRef}`,
          "Write .dealality-integrated-staging marker",
          "Initialize data/dealality-integrated-staging/CURRENT.json",
          "Copy .env from a trusted checkout only if missing (manual)",
          "Do NOT start port 8080 until founder cutover approval",
        ],
    productionImpact: "none",
    worktreePreservation: "source worktrees untouched",
  };
}

function applyBootstrap(args) {
  const plan = bootstrapPlan(args);
  const stagingPath = plan.stagingPath;
  const repoRoot = plan.sourceRepo;

  if (!fs.existsSync(stagingPath)) {
    git(repoRoot, ["fetch", "origin", "main"]);
    // Prefer worktree add so we share object store; fall back to clone if needed.
    const add = git(
      repoRoot,
      [
        "worktree",
        "add",
        "-b",
        INTEGRATED_STAGING_HOME_BRANCH,
        stagingPath,
        args.mainRef,
      ],
      { allowFail: true },
    );
    if (add.status !== 0) {
      // Branch may already exist remotely/locally — try without -b
      const add2 = git(
        repoRoot,
        ["worktree", "add", stagingPath, INTEGRATED_STAGING_HOME_BRANCH],
        { allowFail: true },
      );
      if (add2.status !== 0) {
        throw new Error(
          `Failed to create staging worktree: ${add.stderr || add.stdout}\n${add2.stderr || add2.stdout}`,
        );
      }
    }
  }

  writeStagingMarker(stagingPath, {
    bootstrappedAt: new Date().toISOString(),
    port: INTEGRATED_STAGING_PORT,
  });

  git(stagingPath, ["fetch", "origin", "main"], { allowFail: true });
  const head = git(stagingPath, ["rev-parse", "HEAD"]).stdout;
  const mainSha = git(stagingPath, ["rev-parse", args.mainRef], { allowFail: true });
  const baseMain = mainSha.status === 0 ? mainSha.stdout : head;

  const state = writeStagingState(
    stagingPath,
    emptyStagingState({
      stagingPath,
      baseMainSha: baseMain,
      baseMainShort: shortSha(baseMain),
      headSha: head,
      headShort: shortSha(head),
      lastRefreshAt: new Date().toISOString(),
      lastRefreshMode: "bootstrap",
      validationStatus: "BOOTSTRAPPED",
      serverStatus: "NOT_STARTED",
      includedFeatures: {
        mergedIntoMain: [
          {
            id: "surfe-usage-admin",
            label: "Admin Resources → Surfe Usage",
            evidence: "merged on origin/main (PR #493)",
          },
        ],
        stagingOnly: [],
        notIntegrated: [],
      },
      notes: [
        "Bootstrap complete. Port 8080 cutover requires explicit founder approval.",
        "Production deployment controls unchanged.",
      ],
    }),
  );

  return { ok: true, mode: "bootstrap", applied: true, state, plan };
}

function refreshMainPlan(args) {
  const stagingPath = path.resolve(args.stagingPath);
  const state = readStagingState(stagingPath);
  return {
    ok: true,
    mode: "refresh-main",
    apply: args.apply,
    stagingPath,
    mainRef: args.mainRef,
    currentHead: state.headSha,
    steps: [
      "Refuse if staging working tree is dirty with unapproved WIP",
      "git fetch origin",
      `git merge --ff-only ${args.mainRef}`,
      "Update CURRENT.json + promote KNOWN_GOOD on validate success",
      "Do not kill foreign :8080 processes; cutover is separate",
    ],
    productionImpact: "none",
  };
}

function applyRefreshMain(args) {
  const stagingPath = path.resolve(args.stagingPath);
  if (!fs.existsSync(stagingPath)) {
    throw new Error(`Staging path missing: ${stagingPath}. Run bootstrap first.`);
  }
  const status = git(stagingPath, ["status", "--porcelain"]).stdout;
  if (status) {
    return {
      ok: false,
      mode: "refresh-main",
      blocked: true,
      reason: "STAGING_DIRTY",
      dirty: status.split("\n").slice(0, 20),
      message:
        "Staging checkout has local changes. Preserve them; do not force refresh.",
    };
  }

  const prior = readStagingState(stagingPath);
  promoteKnownGood(stagingPath, {
    ...prior,
    reason: "pre_refresh_snapshot",
  });

  git(stagingPath, ["fetch", "origin"]);
  const merge = git(stagingPath, ["merge", "--ff-only", args.mainRef], {
    allowFail: true,
  });
  if (merge.status !== 0) {
    return {
      ok: false,
      mode: "refresh-main",
      blocked: true,
      reason: "FF_FAILED",
      message: merge.stderr || merge.stdout,
      knownGoodRetained: true,
      knownGood: readKnownGood(stagingPath),
    };
  }

  const head = git(stagingPath, ["rev-parse", "HEAD"]).stdout;
  const state = writeStagingState(stagingPath, {
    ...prior,
    baseMainSha: head,
    baseMainShort: shortSha(head),
    headSha: head,
    headShort: shortSha(head),
    stagedFeatureCommits: [],
    lastRefreshAt: new Date().toISOString(),
    lastRefreshMode: "refresh-main",
    validationStatus: "REFRESHED_MAIN",
    includedFeatures: {
      ...prior.includedFeatures,
      stagingOnly: [],
    },
    notes: [
      ...(prior.notes || []).slice(-5),
      `Refreshed to ${args.mainRef} @ ${shortSha(head)}`,
    ],
  });

  return { ok: true, mode: "refresh-main", applied: true, state };
}

function stageFeaturePlan(args) {
  if (!args.featureRef) {
    return {
      ok: false,
      mode: "stage-feature",
      error: "MISSING_FEATURE_REF",
      message: "Pass --feature-ref <branch|sha> and optional --feature-label",
    };
  }
  return {
    ok: true,
    mode: "stage-feature",
    apply: args.apply,
    featureRef: args.featureRef,
    featureLabel: args.featureLabel || args.featureRef,
    steps: [
      "Verify feature ref exists",
      "Check merge-base / conflicts against staging HEAD (dry merge-tree when available)",
      "Cherry-pick or merge into staging branch only (never source worktree)",
      "Record staging-only feature in CURRENT.json",
      "On conflict: abort and retain KNOWN_GOOD",
    ],
    productionImpact: "none",
    mergePermission: "staging update does NOT authorize main merge",
  };
}

function applyStageFeature(args) {
  const plan = stageFeaturePlan(args);
  if (!plan.ok) return plan;
  const stagingPath = path.resolve(args.stagingPath);
  if (!fs.existsSync(stagingPath)) {
    throw new Error(`Staging path missing: ${stagingPath}`);
  }

  const prior = readStagingState(stagingPath);
  promoteKnownGood(stagingPath, { ...prior, reason: "pre_stage_feature" });

  git(stagingPath, ["fetch", "origin"], { allowFail: true });
  const rev = git(stagingPath, ["rev-parse", args.featureRef], { allowFail: true });
  if (rev.status !== 0) {
    return {
      ok: false,
      mode: "stage-feature",
      reason: "UNKNOWN_REF",
      message: rev.stderr || `Cannot resolve ${args.featureRef}`,
      knownGoodRetained: true,
    };
  }

  const featureSha = rev.stdout;
  const merge = git(
    stagingPath,
    ["merge", "--no-ff", "--no-edit", featureSha, "-m", `staging: integrate ${args.featureLabel || args.featureRef}`],
    { allowFail: true },
  );
  if (merge.status !== 0) {
    git(stagingPath, ["merge", "--abort"], { allowFail: true });
    return {
      ok: false,
      mode: "stage-feature",
      reason: "CONFLICT",
      message: merge.stderr || merge.stdout,
      knownGoodRetained: true,
      knownGood: readKnownGood(stagingPath),
      note: "Incompatible branches were not silently combined.",
    };
  }

  const head = git(stagingPath, ["rev-parse", "HEAD"]).stdout;
  const label = args.featureLabel || args.featureRef;
  const stagingOnly = [
    ...(prior.includedFeatures?.stagingOnly || []),
    {
      id: label,
      label,
      ref: args.featureRef,
      sha: featureSha,
      integratedAt: new Date().toISOString(),
      productionReady: false,
    },
  ];
  const state = writeStagingState(stagingPath, {
    ...prior,
    headSha: head,
    headShort: shortSha(head),
    stagedFeatureCommits: [
      ...(prior.stagedFeatureCommits || []),
      { ref: args.featureRef, sha: featureSha, label },
    ],
    lastRefreshAt: new Date().toISOString(),
    lastRefreshMode: "stage-feature",
    validationStatus: "FEATURE_STAGED",
    includedFeatures: {
      mergedIntoMain: prior.includedFeatures?.mergedIntoMain || [],
      stagingOnly,
      notIntegrated: prior.includedFeatures?.notIntegrated || [],
    },
  });

  return { ok: true, mode: "stage-feature", applied: true, state };
}

function validateStaging(args) {
  const stagingPath = path.resolve(args.stagingPath);
  const checks = [];
  const push = (id, pass, detail) => checks.push({ id, pass, detail });

  push("path_exists", fs.existsSync(stagingPath), stagingPath);
  push(
    "basename",
    checkoutBasename(stagingPath) === INTEGRATED_STAGING_BASENAME ||
      fs.existsSync(path.join(stagingPath, ".dealality-integrated-staging")),
    checkoutBasename(stagingPath),
  );
  push("may_bind_8080", mayBindPort8080(stagingPath), "ownership");
  push(
    "product_worktrees_denied",
    !mayBindPort8080("C:/Dev/dealality/ws-gdi") &&
      !mayBindPort8080("C:/Dev/dealality/ws-adp"),
    "ws-gdi/ws-adp cannot claim 8080",
  );

  let surfeNav = false;
  try {
    const appJs = path.join(stagingPath, "public/app.js");
    const surfeHtml = path.join(stagingPath, "public/app/admin/surfe-usage.html");
    if (fs.existsSync(appJs) && fs.existsSync(surfeHtml)) {
      const txt = fs.readFileSync(appJs, "utf8");
      surfeNav =
        txt.includes("/admin/surfe-usage") &&
        txt.includes("Surfe Usage") &&
        (txt.includes("internalRunbookOnly: false") ||
          txt.includes("navId: 'surfe-usage'") ||
          txt.includes('navId: "surfe-usage"'));
    }
  } catch {
    surfeNav = false;
  }
  push("surfe_admin_nav", surfeNav, "Admin Resources → Surfe Usage");

  const apps = [
    ["gdi", "public/js/group-demand-intelligence"],
    ["adp", "public/js"],
    ["hotel_explorer", "public"],
    ["brand_explorer", "public/js/brand-explorer-atelier-from-api.js"],
  ];
  for (const [id, rel] of apps) {
    push(`surface_${id}`, fs.existsSync(path.join(stagingPath, rel)), rel);
  }

  const state = readStagingState(stagingPath);
  const allPass = checks.every((c) => c.pass);
  if (allPass && args.apply) {
    const next = writeStagingState(stagingPath, {
      ...state,
      validationStatus: "PASS",
      lastRefreshAt: state.lastRefreshAt || new Date().toISOString(),
    });
    promoteKnownGood(stagingPath, { ...next, reason: "validate_pass" });
  } else if (!allPass && args.apply) {
    writeStagingState(stagingPath, {
      ...state,
      validationStatus: "FAIL",
    });
  }

  return {
    ok: allPass,
    mode: "validate",
    checks,
    environmentStatus: buildEnvironmentStatusPayload(readStagingState(stagingPath)),
    knownGood: readKnownGood(stagingPath),
  };
}

function cutoverPlan(args) {
  return {
    ok: true,
    mode: "cutover-plan",
    apply: false,
    founderApprovalRequired: true,
    stagingPath: path.resolve(args.stagingPath),
    port: INTEGRATED_STAGING_PORT,
    steps: [
      "Confirm founder GO for :8080 cutover",
      "Identify current :8080 owner PID (do not kill unknowns)",
      "Stop only the process whose cwd is the previous staging/temp hold",
      "Start node server.js from ws-integrated-staging with PORT=8080",
      "Verify /api/health, /app#/, Admin Resources → Surfe Usage",
      "Update CURRENT.json serverStatus=RUNNING",
    ],
    refuse: [
      "Do not terminate processes whose cwd is a dirty product worktree",
      "Do not use DEALALITY_CANONICAL_CHECKOUT=1 on ws-* to steal 8080",
      "Do not deploy to Railway/production",
    ],
    status: "AWAITING_FOUNDER_CUTOVER_APPROVAL",
  };
}

function rollbackPlan(args) {
  const stagingPath = path.resolve(args.stagingPath);
  const known = readKnownGood(stagingPath);
  return {
    ok: true,
    mode: "rollback-plan",
    apply: args.apply,
    stagingPath,
    knownGood: known,
    steps: known
      ? [
          `git reset --hard ${known.headSha || known.baseMainSha} (STAGING CHECKOUT ONLY — founder confirm)`,
          "Restore CURRENT.json from KNOWN_GOOD.json",
          "Restart staging server if it was the :8080 owner",
        ]
      : ["No KNOWN_GOOD snapshot yet — cannot auto-rollback"],
    note: "Rollback never touches product worktrees or production.",
    applied: false,
    requiresFounderConfirm: true,
  };
}

function statusReport(args) {
  const stagingPath = path.resolve(args.stagingPath);
  const exists = fs.existsSync(stagingPath);
  const state = exists ? readStagingState(stagingPath) : emptyStagingState();
  const productDenied = {
    "ws-gdi": !mayBindPort8080("C:/Dev/dealality/ws-gdi"),
    "ws-adp": !mayBindPort8080("C:/Dev/dealality/ws-adp"),
    "ws-hotel-explorer": !mayBindPort8080("C:/Dev/dealality/ws-hotel-explorer"),
    "ws-brand-explorer": !mayBindPort8080("C:/Dev/dealality/ws-brand-explorer"),
  };
  return {
    ok: true,
    mode: "status",
    stagingExists: exists,
    stagingPath,
    mayBind8080Here: mayBindPort8080(process.cwd()),
    cwdBasename: checkoutBasename(process.cwd()),
    isProductCwd: isProductWorktreeCheckout(process.cwd()),
    productWorktreesDenied8080: productDenied,
    environmentStatus: buildEnvironmentStatusPayload(state),
    knownGood: exists ? readKnownGood(stagingPath) : null,
    cutover: "AWAITING_FOUNDER_APPROVAL",
    productionUntouched: true,
  };
}

function writeReport(args, result) {
  const dir =
    args.reportDir ||
    path.join(
      process.cwd(),
      "reports",
      "dealality-integrated-staging-v1",
    );
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${result.mode || "status"}-${Date.now()}.json`);
  fs.writeFileSync(file, `${JSON.stringify(result, null, 2)}\n`);
  return file;
}

function main() {
  const args = parseArgs(process.argv);
  let result;
  switch (args.mode) {
    case "bootstrap":
      result = args.apply ? applyBootstrap(args) : bootstrapPlan(args);
      break;
    case "refresh-main":
      result = args.apply ? applyRefreshMain(args) : refreshMainPlan(args);
      break;
    case "stage-feature":
      result = args.apply ? applyStageFeature(args) : stageFeaturePlan(args);
      break;
    case "validate":
      result = validateStaging(args);
      break;
    case "cutover-plan":
      result = cutoverPlan(args);
      break;
    case "rollback-plan":
      result = rollbackPlan(args);
      break;
    case "status":
    default:
      result = statusReport(args);
      break;
  }
  const reportFile = writeReport(args, result);
  result.reportFile = reportFile;
  console.log(JSON.stringify(result, null, 2));
  if (result.ok === false) process.exitCode = 2;
}

main();
