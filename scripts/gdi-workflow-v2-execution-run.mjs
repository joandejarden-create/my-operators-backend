#!/usr/bin/env node
/**
 * Close GDI workflow v2 execution gaps for Mayflower + Renaissance.
 * Clears GDI_V2_SKIP_AIRTABLE, runs coordinator stages, writes reports/gdi-workflow-v2-execution/.
 *
 * No production writes. No paid Parallel/Surfe.
 *
 *   node scripts/gdi-workflow-v2-execution-run.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-workflow-v2-execution");

const HOTELS = [
  {
    id: "rec0eTEnCqlsGJJt9",
    key: "mayflower",
    label: "Mayflower",
  },
  {
    id: "recG66DQJKP2c0UNh",
    key: "renaissance",
    label: "Renaissance",
  },
];

// Clear skip settings for this verification
delete process.env.GDI_V2_SKIP_AIRTABLE;
if (process.env.GDI_OPPORTUNITIES_PERSISTENCE === "filesystem") {
  // Keep filesystem for candidate writes, but Airtable DG/PE reads still use token.
  // Opportunity canonical load will follow env — prefer airtable for reuse audit.
}

fs.mkdirSync(OUT, { recursive: true });
const runsDir = path.join(OUT, "workflow-runs");
fs.mkdirSync(runsDir, { recursive: true });

function runHotel(hotel, extraArgs = []) {
  const args = [
    path.join(ROOT, "scripts/gdi-research-to-page-workflow-v1.mjs"),
    "--hotel",
    hotel.id,
    "--dry-run",
    "--out-dir",
    runsDir,
    ...extraArgs,
  ];
  const env = {
    ...process.env,
  };
  delete env.GDI_V2_SKIP_AIRTABLE;
  const r = spawnSync(process.execPath, args, {
    cwd: ROOT,
    encoding: "utf8",
    env,
    maxBuffer: 20 * 1024 * 1024,
  });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  fs.writeFileSync(
    path.join(OUT, `${hotel.key}-workflow-stdout.txt`),
    (r.stdout || "") + (r.stderr || "")
  );
  return {
    hotelId: hotel.id,
    key: hotel.key,
    status: r.status,
    stdoutTail: (r.stdout || "").slice(-2000),
    stderrTail: (r.stderr || "").slice(-1000),
  };
}

const results = [];

// 1) Full reuse → fit → contact → cards for each hotel (no discovery paid)
for (const h of HOTELS) {
  console.log(`\n=== Running ${h.label} (${h.id}) ===`);
  results.push(
    runHotel(h, [
      // run through generate_cards; stop before publish
      // (default runs all stages except apply_publish when dry-run)
    ])
  );
}

// 2) Discovery mode clarity proofs (San Diego fixtures / mock) — no paid
const discoveryProofs = [];
for (const mode of ["replay", "live-mock", "plan"]) {
  const args = [
    path.join(ROOT, "scripts/gdi-research-to-page-workflow-v1.mjs"),
    "--hotel",
    "recnPB3Q02NO8Ma1o",
    "--dry-run",
    "--stage",
    "discover_gaps",
    "--out-dir",
    path.join(OUT, "discovery-mode-proofs"),
    ...(mode === "plan" ? [] : ["--run-discovery", "--discovery-mode", mode]),
  ];
  const env = { ...process.env };
  delete env.GDI_V2_SKIP_AIRTABLE;
  const r = spawnSync(process.execPath, args, {
    cwd: ROOT,
    encoding: "utf8",
    env,
    maxBuffer: 10 * 1024 * 1024,
  });
  discoveryProofs.push({
    mode,
    status: r.status,
    stdout: (r.stdout || "").slice(0, 1500),
  });
  fs.writeFileSync(
    path.join(OUT, `discovery-mode-${mode}.txt`),
    (r.stdout || "") + (r.stderr || "")
  );
}

// Collect latest run ledgers
function latestRun(prefix) {
  const dir = path.join(runsDir, "runs");
  if (!fs.existsSync(dir)) return null;
  const runs = fs
    .readdirSync(dir)
    .filter((d) => d.startsWith(prefix))
    .sort();
  if (!runs.length) return null;
  const runId = runs[runs.length - 1];
  const ledgerPath = path.join(dir, runId, "stage-completion-ledger.json");
  return {
    runId,
    ledger: fs.existsSync(ledgerPath)
      ? JSON.parse(fs.readFileSync(ledgerPath, "utf8"))
      : null,
    runDir: path.join(dir, runId),
  };
}

const may = latestRun("gdi_workflow_rec0eTEn");
const ren = latestRun("gdi_workflow_recG66DQ");

function stageSummary(ledger) {
  if (!ledger?.stages) return {};
  const out = {};
  for (const [k, v] of Object.entries(ledger.stages)) {
    out[k] = {
      status: v.status,
      enteringLeaving: {
        counts: v.counts || v.targetCount != null
          ? {
              targetCount: v.targetCount,
              attempted: v.attempted,
              resolved: v.resolved,
              unresolved: v.unresolved,
              blocked: v.blocked,
              opportunityCount: v.opportunityCount,
              ...v.counts,
            }
          : null,
        privateEvent: v.privateEvent || null,
        airtableReuse: v.airtableReuse || null,
        draft: v.draft || null,
        drawerCompleteness: v.drawerCompleteness || null,
        mode: v.mode || v.executionKind || null,
        note: v.note || null,
      },
    };
  }
  return out;
}

const report = {
  asOf: new Date().toISOString(),
  envCleared: ["GDI_V2_SKIP_AIRTABLE"],
  hotelRuns: results,
  discoveryModeProofs: discoveryProofs.map((d) => ({
    mode: d.mode,
    status: d.status,
  })),
  mayflower: may
    ? {
        runId: may.runId,
        stages: stageSummary(may.ledger),
        remainingManualSteps: may.ledger?.remainingManualSteps || [],
        airtableTables:
          may.ledger?.stages?.reuse_evidence?.airtableReuse?.tables || [],
        privateEvent: may.ledger?.stages?.reuse_evidence?.privateEvent || null,
        contact: may.ledger?.stages?.enrich_contacts || null,
        draft: may.ledger?.stages?.generate_cards?.draft || null,
      }
    : null,
  renaissance: ren
    ? {
        runId: ren.runId,
        stages: stageSummary(ren.ledger),
        remainingManualSteps: ren.ledger?.remainingManualSteps || [],
        airtableTables:
          ren.ledger?.stages?.reuse_evidence?.airtableReuse?.tables || [],
        privateEvent: ren.ledger?.stages?.reuse_evidence?.privateEvent || null,
        contact: ren.ledger?.stages?.enrich_contacts || null,
        draft: ren.ledger?.stages?.generate_cards?.draft || null,
      }
    : null,
  nextBoundedFreshDiscoveryCommand:
    "node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recnPB3Q02NO8Ma1o --run-discovery --discovery-mode live --authorize-paid-discovery --budget-usd 2 --dry-run --stage discover_gaps --out-dir reports/gdi-workflow-v2-execution/bounded-live",
};

fs.writeFileSync(
  path.join(OUT, "execution-run-summary.json"),
  JSON.stringify(report, null, 2) + "\n"
);

console.log(JSON.stringify({ ok: true, out: OUT, results }, null, 2));
