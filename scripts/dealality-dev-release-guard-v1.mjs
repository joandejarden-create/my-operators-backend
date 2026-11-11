/**
 * Lightweight source and entry-point guard. Does not run paid research or deploy.
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "../lib/group-demand-intelligence/research/research-constants-v1.js";
import { FOURTEEN_WORKFLOWS } from "../lib/group-demand-intelligence/research/discovery-to-decision/pipeline-trace-v1.js";
import { initializeCanonical31CertificationFramework } from "../lib/ai-demand-positioning/governance/adp-canonical-31-certification-posture-v1.js";

const KNOWN_UNTRACKED_SOURCE = new Set([
  "lib/group-demand-intelligence/research/events-full-integration-zero-paid-orchestrator-v1.js",
  "scripts/gdi-research-to-page-workflow-v1.mjs.lf",
]);

function assert(cond, message) {
  if (!cond) throw new Error(message);
}

const required = [
  "server.js",
  "scripts/gdi-research-to-page-workflow-v1.mjs",
  "lib/group-demand-intelligence/research/research-supervisor-v2.js",
  "middleware/requireDealalityUser.js",
  "middleware/requireAdminAccess.js",
  "middleware/gdiPilotReadAuth.js",
  ".github/workflows/secret-scan-pr.yml",
  "public/group-demand-intelligence.html",
  "public/owner-ai-demand.html",
  "public/app/admin/ai-demand-admin.html",
];
for (const file of required) assert(fs.existsSync(file), `missing ${file}`);

assert(SUPERVISOR_EXPECTED_SEGMENTS.length === 11, "supervisor segments");
assert(FOURTEEN_WORKFLOWS.length === 14, "workflow catalog");
assert(new Set(FOURTEEN_WORKFLOWS.map((row) => row.id)).size === 14, "workflow ids");

const posture = initializeCanonical31CertificationFramework({ readMatrix: () => null });
assert(posture.identityCount === 31 && posture.certifiedCount === 0, "certification posture");

const workflow = fs.readFileSync("scripts/gdi-research-to-page-workflow-v1.mjs", "utf8");
assert(workflow.includes("explicitDryRun") || workflow.includes("--dry-run"), "workflow dry-run gate");
const server = fs.readFileSync("server.js", "utf8");
assert(server.includes("requireAdminAccess"), "admin gate mounted");
assert(server.includes("admin-publication-control"), "publication control route");
assert(server.includes("published-pdf-history"), "pdf history route");

const untracked = execFileSync("git", ["ls-files", "--others", "--exclude-standard"], { encoding: "utf8" })
  .split(/\r?\n/)
  .filter(Boolean)
  .map((file) => file.replace(/\\/g, "/"))
  .filter((file) => /^(api|lib|public|scripts|middleware)\//.test(file))
  .filter((file) => /\.(js|mjs|cjs|md)$/.test(file))
  .filter((file) => !KNOWN_UNTRACKED_SOURCE.has(file));
assert(untracked.length === 0, `untracked source:${untracked.slice(0, 12).join(",")}`);

console.log(JSON.stringify({
  ok: true,
  guard: "dealality-dev-release-guard-v1",
  workflows: FOURTEEN_WORKFLOWS.length,
  segments: SUPERVISOR_EXPECTED_SEGMENTS.length,
  knownUntrackedBlockers: [...KNOWN_UNTRACKED_SOURCE],
}));
