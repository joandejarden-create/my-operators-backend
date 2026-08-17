/**
 * Smoke: Railway-safe discovery timeouts + progress artifacts (no Airtable writes).
 * Usage:
 *   node scripts/smoke-census-v4-railway-discovery-safe.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import { discoverCalaProperties } from "../lib/research-engine-v2/census-autopilot-source-discovery.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "data/research-engine-v2/census-autopilot-v4-full-universe");

process.env.CENSUS_DISCOVERY_RAILWAY_SAFE_MODE = "1";
process.env.CENSUS_DISCOVERY_FETCH_TIMEOUT_MS =
  process.env.CENSUS_DISCOVERY_FETCH_TIMEOUT_MS || "15000";
process.env.CENSUS_DISCOVERY_SOURCE_TIMEOUT_MS =
  process.env.CENSUS_DISCOVERY_SOURCE_TIMEOUT_MS || "45000";
process.env.CENSUS_DISCOVERY_COUNTRY_TIMEOUT_MS =
  process.env.CENSUS_DISCOVERY_COUNTRY_TIMEOUT_MS || "60000";
process.env.CENSUS_DISCOVERY_SKIP_ON_TIMEOUT = "1";
process.env.CENSUS_DISCOVERY_RESUME = process.env.CENSUS_DISCOVERY_RESUME || "0";
process.env.CENSUS_DISCOVERY_FORCE_REFRESH = "1";
process.env.CENSUS_DISCOVERY_CONCURRENCY = "2";
process.env.CENSUS_DISCOVERY_MAX_RETRIES = "1";

const countries = (process.env.SMOKE_DISCOVERY_COUNTRIES || "Puerto Rico,Jamaica")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const started = Date.now();
console.log(JSON.stringify({ smoke: "railway_discovery_safe", countries }, null, 2));

const { discovered, sourceReport } = await discoverCalaProperties({
  discoverAllOfficialParents: true,
  discoveryCountries: countries,
  railwaySafe: true,
  discoveryOutDir: OUT,
  includeVicEvidence: false,
  delayMs: 50,
});

const elapsed_ms = Date.now() - started;
const progressPath = path.join(OUT, "discovery-progress.json");
const stallPath = path.join(OUT, "discovery-stall-diagnostics.json");
const checkpointPath = path.join(OUT, "discovery-resume-checkpoint.json");

const report = {
  status:
    sourceReport.discovery_lane_status ||
    "official_directory_discovery_complete",
  elapsed_ms,
  discovered_count: discovered.length,
  families_used: sourceReport.families_used,
  checkpoint: sourceReport.checkpoint,
  adapter_errors: (sourceReport.adapter_errors || []).slice(0, 20),
  artifacts: {
    progress: fs.existsSync(progressPath),
    stall: fs.existsSync(stallPath),
    checkpoint: fs.existsSync(checkpointPath),
  },
  hang_free: elapsed_ms < 10 * 60 * 1000,
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "63-railway-discovery-stall-diagnostics.json"),
  JSON.stringify({ generated_at: new Date().toISOString(), smoke: report, sourceReport }, null, 2)
);

const md = `# Railway Discovery Stall Diagnostics

**Status:** \`${report.status}\`
**Generated:** ${new Date().toISOString()}

## Smoke result
- elapsed_ms: ${report.elapsed_ms}
- discovered_count: ${report.discovered_count}
- families_used: ${(report.families_used || []).join(", ") || "(none)"}
- hang_free: ${report.hang_free}
- artifacts: progress=${report.artifacts.progress} stall=${report.artifacts.stall} checkpoint=${report.artifacts.checkpoint}

## Fix summary
- Per-request AbortController timeouts (\`timedFetch\`)
- Per-country/source unit timeouts with skip-on-stall
- Resume checkpoint + unit cache under \`discovery-unit-cache/\`
- Railway safe mode: concurrency=2, max_retries=1, no unbounded Promise.all

## Expected production statuses
- \`production_census_v4_railway_discovery_timeout_fix_complete\`
- \`production_census_v4_railway_discovery_partial_network_remaining\`
- \`production_census_v4_railway_discovery_partial_source_remaining\`
`;

fs.writeFileSync(path.join(OUT, "63-railway-discovery-stall-diagnostics.md"), md);
console.log(JSON.stringify(report, null, 2));
if (!report.hang_free) process.exit(2);
if (!report.artifacts.progress || !report.artifacts.stall) process.exit(3);
