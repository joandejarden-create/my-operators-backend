/**
 * AVA V4 production release preflight against the current checkout.
 * Does not deploy or publish.
 */
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);

function run(cmd) {
  console.log(`$ ${cmd}`);
  execSync(cmd, { cwd: ROOT, stdio: "inherit" });
}

function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch (e) {
    return String(e.stdout || e.message || e);
  }
}

const gates = [];
function gate(name, ok, detail) {
  gates.push({ name, ok: Boolean(ok), detail: detail || "" });
  console.log(`${ok ? "PASS" : "FAIL"} ${name}${detail ? ` — ${detail}` : ""}`);
}

const commit = sh("git rev-parse HEAD");
const branch = sh("git rev-parse --abbrev-ref HEAD");
const dirty = sh("git status --porcelain");
const releaseBranchOk =
  branch.startsWith("release/ava-v4") ||
  branch.startsWith("release/dealality-") ||
  branch === "HEAD"; // detached clean-checkout of exact SHA
gate(
  "CLEAN_OR_RELEASE_BRANCH",
  (releaseBranchOk || !dirty) && !dirty,
  `branch=${branch}`
);
gate(
  "SERVER_WIRES_AVA",
  fs.readFileSync(path.join(ROOT, "server.js"), "utf8").includes("/api/admin/ai-visibility-audit/"),
  "server.js route markers"
);
gate(
  "RAILWAYIGNORE_AVA",
  fs.readFileSync(path.join(ROOT, ".railwayignore"), "utf8").includes("RAILWAY_LEAN_AVA_V1")
);
gate(
  "ADMIN_HTML_AVA_TAB",
  fs
    .readFileSync(path.join(ROOT, "public/app/admin/ai-demand-admin.html"), "utf8")
    .includes("adaPanelVisibilityAudit")
);
gate(
  "NO_GDI_TOKEN_SCRIPTS_IN_PKG_DIFF_HINT",
  !fs
    .readFileSync(path.join(ROOT, "package.json"), "utf8")
    .includes("dealality:adp-38-hotel-final-standardization"),
  "package.json should not carry unrelated dirty ADP scripts from source branch"
);

try {
  run("node scripts/test-ava-customer-report-quality-standard-v1.mjs");
  gate("TEST_CUSTOMER_QUALITY", true);
} catch {
  gate("TEST_CUSTOMER_QUALITY", false);
}
try {
  run("node scripts/test-ava-pdf-channel-parity-v1.mjs");
  gate("TEST_PDF_CHANNEL_PARITY", true);
} catch (e) {
  gate("TEST_PDF_CHANNEL_PARITY", false, String(e.message || e).slice(0, 200));
}
try {
  run("node scripts/test-ava-durable-storage-gates-v1.mjs");
  gate("TEST_DURABLE_STORAGE", true);
} catch (e) {
  gate("TEST_DURABLE_STORAGE", false, String(e.message || e).slice(0, 200));
}
try {
  run("node scripts/ava-v4-verify-38-manifest-v1.mjs");
  gate("MANIFEST_38_HASHES", true);
} catch {
  gate("MANIFEST_38_HASHES", false);
}

const failed = gates.filter((g) => !g.ok);
const out = {
  contract: "AVA_V4_PRODUCTION_RELEASE_PREFLIGHT_V1",
  generatedAt: new Date().toISOString(),
  branch,
  commit,
  gates,
  failed: failed.map((g) => g.name),
  verdict: failed.length
    ? "AVA_V4_RELEASE_BLOCKED_WITH_EXACT_REASONS"
    : "AVA_V4_RELEASE_READY_FOR_FOUNDER_DEPLOY_AUTH",
};
fs.writeFileSync(
  path.join(REPORT, "AVA_V4_PRODUCTION_RELEASE_PREFLIGHT_RESULTS.json"),
  `${JSON.stringify(out, null, 2)}\n`
);
console.log(JSON.stringify({ verdict: out.verdict, failed: out.failed, commit }, null, 2));
if (failed.length) process.exit(2);
