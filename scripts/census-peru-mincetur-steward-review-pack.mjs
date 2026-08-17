/**
 * Build Peru MINCETUR steward review pack from HPC match-plan JSON (read-only).
 *
 * Usage:
 *   npm run census:peru-mincetur-steward-review-pack -- --input reports/research-engine-v2/peru-mincetur-hpc-match-plan-….json
 *   npm run census:peru-mincetur-steward-review-pack -- --pilot-limit 25
 */
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  PERU_MINCETUR_STEWARD_TIERS,
  buildPeruMinceturStewardReviewPack,
  renderPeruMinceturStewardReviewMarkdown,
} from "../lib/research-engine-v2/peru-mincetur-steward-review-pack.js";
import {
  PERU_MINCETUR_ADAPTER_VERSION,
  runPeruMinceturDryRun,
} from "../lib/research-engine-v2/peru-mincetur-open-data-adapter.js";
import {
  PERU_MINCETUR_HPC_PLAN_VERSION,
  buildPeruMinceturHpcPlan,
  toPeruMinceturHpcMatchInput,
} from "../lib/research-engine-v2/peru-mincetur-hpc-match-plan.js";
import {
  loadHotelPropertyCensusReadOnly,
  matchAllCandidatesToHotelPropertyCensus,
} from "../lib/independent-census/match-hotel-property-census.js";
import "../load-env.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

function parseArgs(argv = process.argv.slice(2)) {
  const out = {
    input: "",
    pilotLimit: 25,
    pilotTier: PERU_MINCETUR_STEWARD_TIERS.A,
    maxRows: 500,
    hotelsOnly: true,
    help: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--help" || a === "-h") out.help = true;
    else if (a === "--input" || a === "--plan") out.input = argv[++i];
    else if (a === "--pilot-limit") out.pilotLimit = Number(argv[++i]) || out.pilotLimit;
    else if (a === "--pilot-tier") {
      const t = String(argv[++i] || "").toUpperCase();
      if (t === "A") out.pilotTier = PERU_MINCETUR_STEWARD_TIERS.A;
      else if (t === "B") out.pilotTier = PERU_MINCETUR_STEWARD_TIERS.B;
      else if (t === "C") out.pilotTier = PERU_MINCETUR_STEWARD_TIERS.C;
      else out.pilotTier = argv[i];
    } else if (a === "--max-rows") out.maxRows = Number(argv[++i]) || out.maxRows;
  }
  return out;
}

function stamp() {
  return new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
}

function resolvePath(p) {
  if (!p) return "";
  return p.startsWith("/") || /^[A-Za-z]:/.test(p) ? p : join(process.cwd(), p);
}

const args = parseArgs();
if (args.help) {
  console.log(`Peru MINCETUR steward review pack (read-only)

Options:
  --input PATH       Prior peru-mincetur-hpc-match-plan JSON
  --pilot-limit N    Recommended pilot size (default 25)
  --pilot-tier A|B|C Default A (URL+rooms+RUC)
  --max-rows N       If no --input, fetch MINCETUR dry-run cap (default 500)
`);
  process.exit(0);
}

console.log("=== Peru MINCETUR steward review pack (read-only) ===\n");

let planRows = [];
let sourceMeta = {};

if (args.input) {
  const path = resolvePath(args.input);
  if (!existsSync(path)) throw new Error(`Not found: ${path}`);
  const planDoc = JSON.parse(readFileSync(path, "utf8"));
  planRows = planDoc.plan?.rows || planDoc.rows || [];
  sourceMeta = { input: path, plan_version: planDoc.version || null };
  console.log(`Loaded ${planRows.length} plan rows from ${path}`);
} else {
  console.log(`Fetching MINCETUR + HPC match (maxRows=${args.maxRows})…`);
  const dry = await runPeruMinceturDryRun({
    maxRows: args.maxRows,
    hotelsOnly: args.hotelsOnly,
  });
  if (!dry.ok) {
    console.error(`MINCETUR fetch failed: ${dry.error_kind} ${dry.message}`);
    process.exit(1);
  }
  const hpc = await loadHotelPropertyCensusReadOnly({ countryFilter: "Peru" });
  const matchInputs = dry.candidates.map(toPeruMinceturHpcMatchInput);
  const { rows: hpcMatches } = matchAllCandidatesToHotelPropertyCensus(
    matchInputs,
    hpc,
    { identityKeyFn: (c) => c.proposedIdentityKey || "" }
  );
  const plan = buildPeruMinceturHpcPlan(dry.candidates, hpcMatches);
  planRows = plan.rows;
  sourceMeta = {
    adapter_version: PERU_MINCETUR_ADAPTER_VERSION,
    plan_version: PERU_MINCETUR_HPC_PLAN_VERSION,
    dry_summary: dry.summary,
    hpc_pool: hpc.rows.length,
  };
  console.log(`Plan rows: ${planRows.length}`);
}

const pack = buildPeruMinceturStewardReviewPack(planRows, {
  pilotLimit: args.pilotLimit,
  pilotTier: args.pilotTier,
});
pack.source = sourceMeta;

mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });
const ts = stamp();
const jsonPath = join(REPORTS, `peru-mincetur-steward-review-pack-${ts}.json`);
const mdPath = join(REPORTS, `peru-mincetur-steward-review-pack-${ts}.md`);
const docPath = join(DOCS, "production-census-peru-mincetur-steward-review-pack.md");
const bundlePath = join(REPORTS, `peru-mincetur-steward-insert-approval-bundle-${ts}.json`);
const md = renderPeruMinceturStewardReviewMarkdown(pack);

writeFileSync(jsonPath, JSON.stringify(pack, null, 2));
writeFileSync(mdPath, md);
writeFileSync(docPath, md);
writeFileSync(bundlePath, JSON.stringify(pack.approval_bundle, null, 2));

console.log("\n--- Steward pack ---");
console.log(`  Tier A: ${pack.summary.tier_a}`);
console.log(`  Tier B: ${pack.summary.tier_b}`);
console.log(`  Tier C: ${pack.summary.tier_c}`);
console.log(`  Pilot:  ${pack.summary.pilot_proposed_inserts} (${pack.recommended_pilot.tier})`);
console.log(`\n  wrote: ${jsonPath}`);
console.log(`  wrote: ${bundlePath}`);
console.log(`  wrote: ${docPath}`);
console.log("✓ Read-only. Review pilot list before any apply.");
