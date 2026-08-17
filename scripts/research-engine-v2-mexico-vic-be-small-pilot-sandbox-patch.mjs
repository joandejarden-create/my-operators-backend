/**
 * Mexico VIC → BE small pilot sandbox patch runner.
 *
 *   npm run research-engine-v2:mexico-vic-be-small-pilot-sandbox-patch
 *   npm run research-engine-v2:mexico-vic-be-small-pilot-sandbox-patch -- --execute
 *
 * Always validates sandbox first. Writes only when --execute AND validation READY.
 * Never writes production Airtable. Never mutates frozen VIC/62 artifacts.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import "dotenv/config";
import { validateAirtableSandbox, STATUS as SANDBOX_STATUS } from "../lib/research-engine-v2/airtable-sandbox-validation.js";
import {
  runMexicoVicBeSmallPilotSandboxPatch,
  EXEC_STATUS,
  EXPECTED_FREEZE,
} from "../lib/research-engine-v2/mexico-vic-be-small-pilot-sandbox-patch.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const BASELINE = join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family"
);
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");
const BE62_JSON = join(ROOT, "reports/brand-explorer-62-active-public-full-baseline.json");
const BE62_LIB = join(ROOT, "lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js");
const VIC_LOCK = join(BASELINE, "baseline-lock.json");

const PROPOSAL = join(BASELINE, "be-small-pilot-staging-patch-proposal.json");
const PREVIEW = join(BASELINE, "be-small-pilot-rendered-preview.json");

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}
function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}
function fingerprint(path) {
  if (!existsSync(path)) return null;
  const st = statSync(path);
  return { path: path.replace(/\\/g, "/"), size: st.size, mtimeMs: st.mtimeMs };
}

function runCmd(command, args, timeoutMs = 900000) {
  return new Promise((resolve) => {
    const child = spawn(command, args, { cwd: ROOT, shell: true, env: process.env });
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => {
      child.kill("SIGTERM");
      resolve({ ok: false, timedOut: true, stdout: stdout.slice(-6000), stderr: stderr.slice(-6000) });
    }, timeoutMs);
    child.stdout.on("data", (d) => {
      stdout += d.toString();
    });
    child.stderr.on("data", (d) => {
      stderr += d.toString();
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      resolve({ ok: code === 0, code, timedOut: false, stdout: stdout.slice(-10000), stderr: stderr.slice(-10000) });
    });
  });
}

function renderExecutionMd(report, prodChecks) {
  return `# Mexico VIC → BE Small Pilot — Sandbox Patch Execution

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}

## Sandbox validation

| Check | Value |
|-------|-------|
| Validation status | \`${report.sandbox_validation?.status || "n/a"}\` |
| Production base | \`${report.sandbox_validation?.production_base_id_masked || report.production_safety?.production_base_masked || "(unset)"}\` |
| Sandbox base | \`${report.sandbox_validation?.sandbox_base_id_masked || report.production_safety?.sandbox_base_masked || "(unset)"}\` |
| IDs differ | **${report.sandbox_validation?.ids_differ ?? report.production_safety?.ids_differ ?? false}** |
| Execute requested | ${report.execute_requested === true} |
| Executed | ${report.executed === true} |
| Ops executed | ${report.ops_executed ?? 0} |

## Production safety

- Production writes: **${report.production_writes ?? 0}**
- Sandbox writes: **${report.sandbox_writes ?? 0}**
- Production write client initialized: **${report.production_write_client_initialized === true}**
- Brand Status unchanged: **${report.brand_status_unchanged ?? "n/a"}**
- Recent Momentum unchanged: **${report.recent_momentum_unchanged ?? true}**
- Forbidden fields untouched: **${report.forbidden_fields_untouched ?? true}**
- Frozen 62 modified: **${report.frozen_62_modified === true}**
- Frozen VIC modified: **${report.frozen_vic_modified === true}**
- Freeze hash: \`${report.freeze_hash_sha256 || EXPECTED_FREEZE}\`

## Records / fields touched

${
  (report.records_touched || []).length
    ? (report.records_touched || [])
        .map((r) => `- \`${r.brand_slug}\` ${r.table} \`${r.record_id}\` slot \`${r.slot_key}\``)
        .join("\n")
    : "- (none — patch not executed)"
}

Fields: ${(report.fields_touched || []).join(", ") || "(none)"}

## Production protected checks

| Check | Result |
|-------|--------|
| Active universe SoT | ${prodChecks?.active_universe_sot_ok ?? "skipped"} |
| Semantic audit | ${prodChecks?.semantic_audit_ok ?? "skipped"} |
| Quiet PVQL | ${prodChecks?.quiet_pvql?.skipped ? "cited freeze / skipped" : prodChecks?.quiet_pvql?.ok ?? "skipped"} |
| Momentum evidence | ${prodChecks?.momentum_evidence_ok ?? "skipped"} |
| Mandatory release gates | ${prodChecks?.mandatory_release_gates_ok ?? "skipped"} |

## Ready for manual review

**${report.ready_for_manual_review === true}**

${report.recommended_next_step ? `## Next step\n\n${report.recommended_next_step}\n` : ""}
${report.reason ? `## Reason\n\n\`${report.reason}\`\n` : ""}
`;
}

async function main() {
  const execute = process.argv.includes("--execute");
  console.log(`[sandbox-patch] starting execute=${execute}`);

  const be62Before = fingerprint(BE62_JSON);
  const be62LibBefore = fingerprint(BE62_LIB);
  const vicBefore = existsSync(VIC_LOCK) ? fingerprint(VIC_LOCK) : null;

  // Step 1 — always validate (also refreshes validation reports)
  console.log("[sandbox-patch] step 1: validate sandbox");
  const validation = await validateAirtableSandbox();
  writeJson(join(REPORTS, "airtable-sandbox-validation.json"), validation);
  const { renderSandboxValidationMarkdown } = await import(
    "../lib/research-engine-v2/airtable-sandbox-validation.js"
  );
  writeMd(join(REPORTS, "airtable-sandbox-validation.md"), renderSandboxValidationMarkdown(validation));
  writeMd(
    join(DOCS, "airtable-sandbox-validation-for-vic-be-pilot.md"),
    `${renderSandboxValidationMarkdown(validation)}\n\n## Gate\n\nPatch execution requires \`${SANDBOX_STATUS.READY}\`.\n`
  );

  if (validation.status !== SANDBOX_STATUS.READY) {
    const report = {
      version: "mexico-vic-be-small-pilot-sandbox-patch-v1",
      status: EXEC_STATUS.VALIDATION_FAILED,
      generated_at: new Date().toISOString(),
      execute_requested: execute,
      executed: false,
      ops_executed: 0,
      production_writes: 0,
      sandbox_writes: 0,
      sandbox_validation: {
        status: validation.status,
        production_base_id_masked: validation.production_base_id_masked,
        sandbox_base_id_masked: validation.sandbox_base_id_masked,
        ids_differ: validation.ids_differ,
        blockers: validation.blockers,
        failed_checks: (validation.checks || []).filter((c) => !c.pass).map((c) => ({
          id: c.id,
          detail: c.detail,
        })),
      },
      production_write_client_initialized: false,
      forbidden_fields_untouched: true,
      recent_momentum_unchanged: true,
      frozen_62_modified: false,
      frozen_vic_modified: false,
      freeze_hash_sha256: EXPECTED_FREEZE,
      ready_for_manual_review: false,
      recommended_next_step:
        "Add AIRTABLE_ENV=sandbox, AIRTABLE_BASE_ID_SANDBOX=<sandbox app id>, BE_PILOT_SANDBOX_CONFIRMED=1 to .env; ensure the sandbox base is shared with the API key and named with Sandbox/Staging/Test; re-run validate then patch --execute.",
    };

    writeJson(join(REPORTS, "mexico-vic-be-small-pilot-sandbox-patch-execution.json"), report);
    writeMd(join(REPORTS, "mexico-vic-be-small-pilot-sandbox-patch-execution.md"), renderExecutionMd(report, null));
    writeMd(join(DOCS, "mexico-vic-be-small-pilot-sandbox-patch-execution.md"), renderExecutionMd(report, null));
    writeJson(join(BASELINE, "be-small-pilot-sandbox-patch-result.json"), {
      ...report,
      note: "No sandbox writes — validation failed",
    });
    writeJson(join(BASELINE, "be-small-pilot-sandbox-after-preview.json"), {
      generated_at: report.generated_at,
      applied: false,
      note: "Patch not applied — sandbox_validation_failed_do_not_execute",
      source_lineage_freeze_hash: EXPECTED_FREEZE,
    });

    console.error(`[sandbox-patch] ${EXEC_STATUS.VALIDATION_FAILED}`);
    console.error(
      "[sandbox-patch] failed checks:",
      report.sandbox_validation.failed_checks.map((c) => c.id).join(", ")
    );
    process.exitCode = 2;
    return;
  }

  // Step 2 — execute only if validated
  if (!existsSync(PROPOSAL) || !existsSync(PREVIEW)) {
    throw new Error("Missing proposal or preview artifacts");
  }
  const proposal = readJson(PROPOSAL);
  const preview = readJson(PREVIEW);

  console.log(`[sandbox-patch] step 2: ${execute ? "EXECUTE" : "dry-run"} sandbox patch`);
  const result = await runMexicoVicBeSmallPilotSandboxPatch({
    proposal,
    preview,
    execute,
  });

  const be62After = fingerprint(BE62_JSON);
  const be62LibAfter = fingerprint(BE62_LIB);
  const vicAfter = existsSync(VIC_LOCK) ? fingerprint(VIC_LOCK) : null;
  result.frozen_62_modified =
    JSON.stringify(be62Before) !== JSON.stringify(be62After) ||
    JSON.stringify(be62LibBefore) !== JSON.stringify(be62LibAfter);
  result.frozen_vic_modified = JSON.stringify(vicBefore) !== JSON.stringify(vicAfter);

  // Production checks only when we actually executed (prove production unchanged)
  let prodChecks = { skipped: true };
  if (result.executed && process.env.BE_PILOT_SKIP_PROD_CHECKS !== "1") {
    console.log("[sandbox-patch] step 3: production protected checks (read-only)");
    const sot = await runCmd("npm", ["run", "brand-explorer-active-universe-source-of-truth", "--", "--dry-run"], 300000);
    const sem = await runCmd(
      "npm",
      ["run", "brand-explorer-global-active-semantic-audit", "--", "--dry-run", "--fresh"],
      600000
    );
    const mom = await runCmd("npm", ["run", "test:brand-explorer-recent-momentum-evidence-quality"], 300000);
    const gates = await runCmd("npm", ["run", "test:brand-explorer-mandatory-release-gates"], 600000);
    let pvql = {
      ok: true,
      skipped: true,
      reason: "No production writes; citing frozen 62 PVQL. Set BE_PILOT_RUN_QUIET_PVQL=1 to force.",
    };
    if (process.env.BE_PILOT_RUN_QUIET_PVQL === "1") {
      pvql = await runCmd("node", ["scripts/brand-explorer-quiet-sequential-pvql.mjs"], 1200000);
    }
    prodChecks = {
      skipped: false,
      active_universe_sot_ok: sot.ok,
      semantic_audit_ok: sem.ok,
      momentum_evidence_ok: mom.ok,
      mandatory_release_gates_ok: gates.ok,
      quiet_pvql: pvql,
    };
    result.production_protected_checks = prodChecks;
  }

  writeJson(join(REPORTS, "mexico-vic-be-small-pilot-sandbox-patch-execution.json"), result);
  writeMd(join(REPORTS, "mexico-vic-be-small-pilot-sandbox-patch-execution.md"), renderExecutionMd(result, prodChecks));
  writeMd(join(DOCS, "mexico-vic-be-small-pilot-sandbox-patch-execution.md"), renderExecutionMd(result, prodChecks));
  writeJson(join(BASELINE, "be-small-pilot-sandbox-patch-result.json"), result);
  writeJson(
    join(BASELINE, "be-small-pilot-sandbox-after-preview.json"),
    result.after_preview || {
      generated_at: result.generated_at,
      applied: false,
      source_lineage_freeze_hash: EXPECTED_FREEZE,
      note: result.status,
    }
  );

  console.log(`[sandbox-patch] status=${result.status} ops=${result.ops_executed ?? 0}`);
  if (result.status !== EXEC_STATUS.EXECUTED && execute) process.exitCode = 2;
  if (result.status === EXEC_STATUS.DRY_RUN) {
    console.log("[sandbox-patch] dry-run complete — re-run with --execute to apply sandbox writes");
  }
}

main().catch((err) => {
  console.error("[sandbox-patch] FATAL", err);
  process.exit(1);
});
