#!/usr/bin/env node
/**
 * CLI for AVA permanent audit runner.
 * Invoked by scripts/Run-AVA-Audit.ps1 — do not duplicate methodology here.
 *
 * Usage:
 *   node scripts/run-ava-permanent-audit-v1.mjs --hotel adp_spice_island_beach_resort
 *   node scripts/run-ava-permanent-audit-v1.mjs --hotels adp_a,adp_b --dry-run
 *   node scripts/run-ava-permanent-audit-v1.mjs --all --dry-run
 *   node scripts/run-ava-permanent-audit-v1.mjs --reproduce --audit-run-id AVA-RUN-...
 *   node scripts/run-ava-permanent-audit-v1.mjs --hotel adp_x --certify-only
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runPermanentAvaAudit,
  runPermanentAvaAuditBatch,
  runPermanentAvaAuditAllEligible,
  compareAvaAuditRuns,
  listAvaEligibleHotels,
} from "../lib/ai-visibility-audit-action-plan/ava-permanent-audit-runner-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function parseArgs(argv) {
  const out = {
    hotels: [],
    all: false,
    dryRun: false,
    certifyOnly: false,
    reproduce: false,
    auditRunId: null,
    compare: null,
    list: false,
    help: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--help" || a === "-h") out.help = true;
    else if (a === "--list") out.list = true;
    else if (a === "--all") out.all = true;
    else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--certify-only") out.certifyOnly = true;
    else if (a === "--reproduce") out.reproduce = true;
    else if (a === "--hotel" || a === "--hotel-id") out.hotels.push(argv[++i]);
    else if (a === "--hotels" || a === "--hotel-ids") {
      const raw = argv[++i] || "";
      out.hotels.push(...raw.split(/[;,]/).map((s) => s.trim()).filter(Boolean));
    } else if (a === "--audit-run-id") out.auditRunId = argv[++i];
    else if (a === "--compare") {
      out.compare = { earlier: argv[++i], later: argv[++i] };
    } else if (a.startsWith("--")) {
      console.error(JSON.stringify({ error: "UNKNOWN_ARG", arg: a }));
      process.exit(2);
    } else {
      out.hotels.push(a);
    }
  }
  return out;
}

function modeFromArgs(args) {
  if (args.reproduce) return "reproduce";
  if (args.dryRun) return "dry-run";
  if (args.certifyOnly) return "certify-only";
  return "full";
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log(`AVA Permanent Audit Runner V1

  --hotel <id>           Airtable rec…, adp_* propertyId, or payload slug
  --hotels a,b,c         Multiple hotels
  --all                  Entire certified ADP/AVA universe
  --dry-run              Preview certification; no writes
  --certify-only         Certify + archive without re-running evidence research
  --reproduce --audit-run-id <id>
  --compare <earlier> <later>
  --list                 List eligible hotels
`);
    process.exit(0);
  }

  if (args.list) {
    const hotels = listAvaEligibleHotels();
    console.log(JSON.stringify({ ok: true, count: hotels.length, hotels }, null, 2));
    process.exit(0);
  }

  if (args.compare) {
    const cmp = compareAvaAuditRuns(args.compare.earlier, args.compare.later);
    console.log(JSON.stringify(cmp, null, 2));
    process.exit(cmp.ok ? 0 : 2);
  }

  const mode = modeFromArgs(args);
  let result;

  if (mode === "reproduce") {
    if (!args.auditRunId) {
      console.error(JSON.stringify({ error: "AUDIT_RUN_ID_REQUIRED" }));
      process.exit(2);
    }
    result = await runPermanentAvaAudit(null, {
      mode: "reproduce",
      auditRunId: args.auditRunId,
    });
  } else if (args.all) {
    result = await runPermanentAvaAuditAllEligible({ mode });
  } else if (args.hotels.length > 1) {
    result = await runPermanentAvaAuditBatch(args.hotels, { mode });
  } else if (args.hotels.length === 1) {
    result = await runPermanentAvaAudit(args.hotels[0], { mode });
  } else {
    console.error(JSON.stringify({ error: "HOTEL_OR_ALL_REQUIRED" }));
    process.exit(2);
  }

  const outDir = path.join(ROOT, "reports/ava-permanent-audit-runner-v1");
  fs.mkdirSync(outDir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outFile = path.join(outDir, `execution-${stamp}.json`);
  fs.writeFileSync(outFile, `${JSON.stringify(result, null, 2)}\n`);
  console.log(JSON.stringify({ ...result, executionReportPath: outFile }, null, 2));
  process.exit(typeof result.exitCode === "number" ? result.exitCode : result.ok ? 0 : 1);
}

main().catch((err) => {
  console.error(JSON.stringify({ ok: false, error: err.message || String(err) }));
  process.exit(1);
});
