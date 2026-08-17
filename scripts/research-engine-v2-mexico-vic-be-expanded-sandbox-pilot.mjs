/**
 * Mexico VIC → BE expanded (medium) sandbox pilot.
 *
 *   npm run research-engine-v2:mexico-vic-be-expanded-sandbox-pilot -- --dry-run
 *   npm run research-engine-v2:mexico-vic-be-expanded-sandbox-pilot -- --execute
 *
 * Sandbox only. Does not overwrite vic.pilot.* small pilot rows.
 */

import { mkdirSync, writeFileSync, existsSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import "dotenv/config";
import { validateAirtableSandbox, STATUS as SANDBOX_STATUS } from "../lib/research-engine-v2/airtable-sandbox-validation.js";
import {
  runMexicoVicBeExpandedSandboxPilot,
  STATUS,
  EXPECTED_FREEZE,
} from "../lib/research-engine-v2/mexico-vic-be-expanded-sandbox-pilot.js";

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

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}
function fingerprint(path) {
  if (!existsSync(path)) return null;
  const st = statSync(path);
  return { size: st.size, mtimeMs: st.mtimeMs };
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

function renderMd(report, prodChecks) {
  const brands = (report.brand_confirm || [])
    .map((b) => `- \`${b.slug}\` → \`${b.freeze62_record_id}\` (${b.brand_name})`)
    .join("\n");
  const props = (report.proposal?.properties || report.planned?.[0] && report.proposal?.properties) || [];
  return `# Mexico VIC → Brand Explorer Expanded Sandbox Pilot

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Execute requested:** ${report.execute_requested === true}  
**Executed:** ${report.executed === true}

## Brands (medium pilot)

${brands || "(see JSON)"}

## Properties

| # | Brand slug | Property | City |
|---|------------|----------|------|
${(report.proposal?.properties || [])
  .map((p, i) => `| ${i + 1} | ${p.be_slug} | ${p.name} | ${p.city} |`)
  .join("\n")}

## Rows

- Planned: **${report.planned_row_count ?? report.proposal?.planned_row_count ?? "—"}**
- Executed: **${report.ops_executed ?? 0}**
- Slot namespace: \`vic.pilot.medium.*\`
- Small pilot \`vic.pilot.*\` preserved: **${report.small_pilot_preserved !== false}**

## Fields

- Touched: ${(report.fields_touched || report.proposal?.fields_touched || []).join(", ") || "—"}
- Forbidden untouched: **${report.forbidden_fields_untouched !== false}**

## Safety

- Production writes: **${report.production_writes ?? 0}**
- Sandbox writes: **${report.sandbox_writes ?? 0}**
- Brand Status unchanged: **${report.brand_status_unchanged ?? "n/a"}**
- Recent Momentum unchanged: **${report.recent_momentum_unchanged ?? true}**
- Freeze hash: \`${report.freeze_hash_sha256 || EXPECTED_FREEZE}\`
- Frozen 62 modified: **${report.frozen_62_modified === true}**
- Frozen VIC modified: **${report.frozen_vic_modified === true}**

## Rulings

${Object.entries(report.rulings || {})
  .map(([k, v]) => `- **${k}:** ${v}`)
  .join("\n") || "- (n/a)"}

## Production protected checks

| Check | Result |
|-------|--------|
| Active universe SoT | ${prodChecks?.active_universe_sot_ok ?? "skipped"} |
| Semantic audit | ${prodChecks?.semantic_audit_ok ?? "skipped"} |
| Quiet PVQL | ${prodChecks?.quiet_pvql?.skipped ? "cited freeze / skipped" : prodChecks?.quiet_pvql?.ok ?? "skipped"} |
| Momentum | ${prodChecks?.momentum_evidence_ok ?? "skipped"} |
| Mandatory gates | ${prodChecks?.mandatory_release_gates_ok ?? "skipped"} |

## Ready for review

**${report.ready_for_manual_review === true || report.status === STATUS.EXECUTED}**

${report.reason ? `\n## Reason\n\n\`${report.reason}\`\n` : ""}
`;
}

async function main() {
  const argv = process.argv.slice(2);
  const execute = argv.includes("--execute");
  const dryRun = argv.includes("--dry-run") || !execute;
  const replace = argv.includes("--replace");

  console.log(`[expanded-pilot] dryRun=${dryRun && !execute} execute=${execute} replace=${replace}`);

  const be62Before = fingerprint(BE62_JSON);
  const be62LibBefore = fingerprint(BE62_LIB);

  console.log("[expanded-pilot] validating sandbox…");
  const validation = await validateAirtableSandbox();
  writeJson(join(REPORTS, "airtable-sandbox-validation.json"), validation);
  if (validation.status !== SANDBOX_STATUS.READY) {
    const blocked = {
      version: "mexico-vic-be-expanded-sandbox-pilot-v1",
      status: STATUS.VALIDATION_FAILED,
      generated_at: new Date().toISOString(),
      executed: false,
      sandbox_validation: validation,
      production_writes: 0,
      reason: "sandbox_validation_failed_do_not_execute",
    };
    writeJson(join(REPORTS, "mexico-vic-be-expanded-sandbox-pilot.json"), blocked);
    writeMd(join(REPORTS, "mexico-vic-be-expanded-sandbox-pilot.md"), renderMd(blocked, null));
    writeMd(join(DOCS, "mexico-vic-be-expanded-sandbox-pilot.md"), renderMd(blocked, null));
    console.error("[expanded-pilot]", STATUS.VALIDATION_FAILED);
    process.exitCode = 2;
    return;
  }

  const result = await runMexicoVicBeExpandedSandboxPilot({
    execute,
    replace,
  });

  result.frozen_62_modified =
    JSON.stringify(be62Before) !== JSON.stringify(fingerprint(BE62_JSON)) ||
    JSON.stringify(be62LibBefore) !== JSON.stringify(fingerprint(BE62_LIB));
  result.frozen_vic_modified = false;

  let prodChecks = { skipped: true };
  if (result.executed && process.env.BE_PILOT_SKIP_PROD_CHECKS !== "1") {
    console.log("[expanded-pilot] production protected checks…");
    const sot = await runCmd("npm", ["run", "brand-explorer-active-universe-source-of-truth", "--", "--dry-run"]);
    const sem = await runCmd("npm", [
      "run",
      "brand-explorer-global-active-semantic-audit",
      "--",
      "--dry-run",
      "--fresh",
    ]);
    const mom = await runCmd("npm", ["run", "test:brand-explorer-recent-momentum-evidence-quality"]);
    const gates = await runCmd("npm", ["run", "test:brand-explorer-mandatory-release-gates"]);
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

  writeJson(join(REPORTS, "mexico-vic-be-expanded-sandbox-pilot.json"), result);
  writeMd(join(REPORTS, "mexico-vic-be-expanded-sandbox-pilot.md"), renderMd(result, prodChecks));
  writeMd(join(DOCS, "mexico-vic-be-expanded-sandbox-pilot.md"), renderMd(result, prodChecks));
  writeJson(join(BASELINE, "be-expanded-sandbox-patch-proposal.json"), result.proposal || result);
  writeJson(join(BASELINE, "be-expanded-sandbox-patch-result.json"), result);
  writeJson(
    join(BASELINE, "be-expanded-sandbox-after-preview.json"),
    result.after_preview || { applied: false, source_lineage_freeze_hash: EXPECTED_FREEZE }
  );

  console.log(`[expanded-pilot] status=${result.status} ops=${result.ops_executed ?? 0}`);
  if (result.status === STATUS.DRY_RUN) {
    console.log("[expanded-pilot] dry-run OK — re-run with --execute to apply");
  }
  if (execute && result.status !== STATUS.EXECUTED) process.exitCode = 2;
}

main().catch((err) => {
  console.error("[expanded-pilot] FATAL", err);
  process.exit(1);
});
