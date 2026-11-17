#!/usr/bin/env node
/**
 * Dealality repository-wide CI precheck (local, before open/update PR).
 *
 * Goals:
 *   - Catch avoidable GitHub Actions failures before they email the founder
 *   - Run gitleaks against the intended PR commit range (not only working tree)
 *   - Surface path-based test suggestions from the PR validation matrix
 *
 * Usage:
 *   npm run dealality:ci-precheck
 *   node scripts/dealality-ci-precheck-v1.mjs --base origin/main
 *   node scripts/dealality-ci-precheck-v1.mjs --json
 *   node scripts/dealality-ci-precheck-v1.mjs --allow-skip-gitleaks   # document-only when binary unavailable
 *
 * Does not weaken CI: GitHub gitleaks and path-gated suites remain required merge checks.
 */
import { execFileSync, execSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { suggestPrChecks } from "../lib/dealality-pr-check-matrix.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const GITLEAKS_VERSION = "8.24.3";

function hasArg(name) {
  return process.argv.includes(name);
}

function getArg(name, fallback = "") {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : fallback;
}

function run(cmd, opts = {}) {
  try {
    return execSync(cmd, {
      cwd: ROOT,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      ...opts,
    }).trim();
  } catch (err) {
    if (opts.throwOnFail) throw err;
    return "";
  }
}

function changedFiles(base) {
  const mergeBase = run(`git merge-base HEAD ${base}`) || base;
  const committed = run(`git diff --name-only ${mergeBase}...HEAD`);
  const unstaged = run("git diff --name-only");
  const staged = run("git diff --name-only --cached");
  const untracked = run("git ls-files --others --exclude-standard");
  return [
    ...new Set(
      [...committed.split("\n"), ...unstaged.split("\n"), ...staged.split("\n"), ...untracked.split("\n")].filter(
        Boolean
      )
    ),
  ];
}

function which(bin) {
  try {
    const out = execFileSync(process.platform === "win32" ? "where.exe" : "which", [bin], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    return String(out).split(/\r?\n/).map((s) => s.trim()).filter(Boolean)[0] || "";
  } catch {
    return "";
  }
}

function resolveGitleaksBinary() {
  const onPath = which("gitleaks");
  if (onPath) return { kind: "path", bin: onPath };

  const cacheDir = path.join(ROOT, "tmp", "gitleaks-bin");
  const binName = process.platform === "win32" ? "gitleaks.exe" : "gitleaks";
  const cached = path.join(cacheDir, `gitleaks-${GITLEAKS_VERSION}`, binName);
  if (fs.existsSync(cached)) return { kind: "cache", bin: cached };

  if (which("docker")) return { kind: "docker", bin: "docker" };
  return { kind: "missing", bin: "" };
}

function ensureCachedGitleaks() {
  const cacheDir = path.join(ROOT, "tmp", "gitleaks-bin", `gitleaks-${GITLEAKS_VERSION}`);
  const binName = process.platform === "win32" ? "gitleaks.exe" : "gitleaks";
  const dest = path.join(cacheDir, binName);
  if (fs.existsSync(dest)) return dest;

  fs.mkdirSync(cacheDir, { recursive: true });
  const platform =
    process.platform === "win32"
      ? "windows_x64"
      : process.platform === "darwin"
        ? process.arch === "arm64"
          ? "darwin_arm64"
          : "darwin_x64"
        : process.arch === "arm64"
          ? "linux_arm64"
          : "linux_x64";
  const ext = process.platform === "win32" ? "zip" : "tar.gz";
  const asset = `gitleaks_${GITLEAKS_VERSION}_${platform}.${ext}`;
  const url = `https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}/${asset}`;
  const archive = path.join(cacheDir, asset);

  try {
    execFileSync("curl", ["-fsSL", "-o", archive, url], { cwd: ROOT, stdio: "inherit" });
  } catch {
    throw new Error(`Failed to download gitleaks from ${url}`);
  }

  if (ext === "zip") {
    execFileSync("tar", ["-xf", archive, "-C", cacheDir], { stdio: "inherit" });
  } else {
    execFileSync("tar", ["-xzf", archive, "-C", cacheDir], { stdio: "inherit" });
  }
  if (!fs.existsSync(dest)) {
    // Archive may extract gitleaks next to archive without version folder nesting.
    const found = fs
      .readdirSync(cacheDir)
      .map((n) => path.join(cacheDir, n))
      .find((p) => path.basename(p).toLowerCase().startsWith("gitleaks") && fs.statSync(p).isFile());
    if (found && found !== dest) fs.copyFileSync(found, dest);
  }
  if (!fs.existsSync(dest)) throw new Error(`gitleaks binary missing after extract: ${dest}`);
  try {
    fs.chmodSync(dest, 0o755);
  } catch {
    /* win */
  }
  return dest;
}

function runGitleaks(base) {
  const allowSkip = hasArg("--allow-skip-gitleaks");
  const configPath = path.join(ROOT, ".gitleaks.toml");
  if (!fs.existsSync(configPath)) {
    return { ok: false, status: "MISSING_CONFIG", detail: ".gitleaks.toml missing" };
  }

  const mergeBase = run(`git merge-base HEAD ${base}`) || base;
  const logOpts = `--no-merges --first-parent ${mergeBase}..HEAD`;
  const resolved = resolveGitleaksBinary();

  let bin = resolved.bin;
  let mode = resolved.kind;
  if (mode === "missing") {
    try {
      bin = ensureCachedGitleaks();
      mode = "cache";
    } catch (err) {
      if (allowSkip) {
        return {
          ok: true,
          status: "SKIPPED_UNAVAILABLE",
          detail: String(err.message || err),
          note: "Documented skip; GitHub CI gitleaks remains mandatory.",
        };
      }
      return {
        ok: false,
        status: "GITLEAKS_UNAVAILABLE",
        detail: String(err.message || err),
        hint: "Install gitleaks, enable Docker, or pass --allow-skip-gitleaks and document why.",
      };
    }
  }

  if (mode === "docker") {
    const r = spawnSync(
      "docker",
      [
        "run",
        "--rm",
        "-v",
        `${ROOT}:/repo`,
        "-w",
        "/repo",
        `zricethezav/gitleaks:v${GITLEAKS_VERSION}`,
        "detect",
        "--source=/repo",
        "--config=/repo/.gitleaks.toml",
        "--redact",
        "--exit-code=1",
        `--log-opts=${logOpts}`,
      ],
      { cwd: ROOT, encoding: "utf8" }
    );
    if (r.status === 0) return { ok: true, status: "PASS", mode, logOpts };
    return {
      ok: false,
      status: "FINDINGS",
      mode,
      logOpts,
      detail: (r.stdout || "") + (r.stderr || ""),
    };
  }

  const r = spawnSync(
    bin,
    [
      "detect",
      `--source=${ROOT}`,
      `--config=${configPath}`,
      "--redact",
      "--exit-code=1",
      `--log-opts=${logOpts}`,
    ],
    { cwd: ROOT, encoding: "utf8" }
  );
  if (r.status === 0) return { ok: true, status: "PASS", mode, logOpts };
  if (r.error) {
    return { ok: false, status: "EXEC_ERROR", mode, detail: String(r.error.message || r.error) };
  }
  return {
    ok: false,
    status: "FINDINGS",
    mode,
    logOpts,
    detail: (r.stdout || "") + (r.stderr || ""),
  };
}

function validateWorkflowConcurrency() {
  const dir = path.join(ROOT, ".github", "workflows");
  const files = fs.readdirSync(dir).filter((f) => f.endsWith(".yml") || f.endsWith(".yaml"));
  const prWorkflows = [];
  const missingConcurrency = [];
  for (const f of files) {
    const text = fs.readFileSync(path.join(dir, f), "utf8");
    const hasPr = /\bon:\s*[\s\S]*?pull_request\b/.test(text) || /pull_request:\s*$/m.test(text);
    if (!hasPr) continue;
    prWorkflows.push(f);
    if (!/\bconcurrency\s*:/.test(text)) missingConcurrency.push(f);
  }
  return { prWorkflows, missingConcurrency, ok: missingConcurrency.length === 0 };
}

function main() {
  const base = getArg("--base", "origin/main");
  const asJson = hasArg("--json");
  const files = changedFiles(base);
  const suggestions = suggestPrChecks(files);
  const gitleaks = runGitleaks(base);
  const workflows = validateWorkflowConcurrency();

  const report = {
    contract: "dealality-ci-precheck-v1",
    base,
    changedFileCount: files.length,
    maxRisk: suggestions.maxRisk,
    suggestedCommands: suggestions.allCommands || [],
    gitleaks,
    workflows,
    notificationReminder:
      "GitHub Actions emails are controlled in GitHub Settings → Notifications → System → Actions → Don't notify. Repo code cannot suppress failure emails.",
    ok: gitleaks.ok && workflows.ok,
  };

  if (asJson) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log(`[dealality:ci-precheck] base=${base} files=${files.length} risk=${suggestions.maxRisk || "n/a"}`);
    console.log(`[gitleaks] ${gitleaks.status}${gitleaks.mode ? ` mode=${gitleaks.mode}` : ""}`);
    if (gitleaks.detail && gitleaks.status !== "PASS") {
      console.log(String(gitleaks.detail).slice(0, 4000));
    }
    if (suggestions.allCommands?.length) {
      console.log("[suggested tests]");
      for (const c of suggestions.allCommands) console.log(`  - ${c}`);
    } else {
      console.log("[suggested tests] none matched (docs-only or unmapped paths)");
    }
    if (!workflows.ok) {
      console.log(`[workflows] missing concurrency on: ${workflows.missingConcurrency.join(", ")}`);
    } else {
      console.log(`[workflows] PR concurrency present on ${workflows.prWorkflows.length} workflow(s)`);
    }
    console.log(`[notify] ${report.notificationReminder}`);
    console.log(report.ok ? "[dealality:ci-precheck] PASS" : "[dealality:ci-precheck] FAIL");
  }

  process.exit(report.ok ? 0 : 1);
}

main();
