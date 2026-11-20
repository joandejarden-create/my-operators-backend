#!/usr/bin/env node
/**
 * Factory 2.0 — process next N brands (stage/cert/dry-run). No production writeback.
 * Usage: node scripts/be-factory-2-process-batch-v1.mjs [--limit 5]
 */
import { config as loadEnv } from "dotenv";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { processBatch } from "../lib/brand-explorer/factory-2/be-factory-2-orchestrator-v1.js";
import { writeJson, reportsRoot } from "../lib/brand-explorer/factory-2/be-factory-2-staging-store-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

function parseArgs(argv) {
  let limit = 5;
  let force = false;
  const brandIds = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--limit" && argv[i + 1]) limit = Math.max(1, Number(argv[++i]) || 5);
    else if (argv[i] === "--force") force = true;
    else if (argv[i] === "--brand" && argv[i + 1]) brandIds.push(String(argv[++i]));
  }
  return { limit, force, brandIds };
}

async function main() {
  const { limit, force, brandIds } = parseArgs(process.argv.slice(2));
  console.log(
    JSON.stringify(
      { phase: "process_batch_start", limit, force, brandIds, writeback: false },
      null,
      2
    )
  );

  const { results, dashboard } = await processBatch(ROOT, {
    limit,
    concurrency: 1,
    force,
    brandIds: brandIds.length ? brandIds : null,
  });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  writeJson(join(reportsRoot(ROOT), `03-batch-run-${stamp}.json`), {
    limit,
    results,
    dashboardSummary: dashboard.summary,
  });
  writeJson(join(reportsRoot(ROOT), "03-latest-batch-run.json"), {
    limit,
    results,
    dashboardSummary: dashboard.summary,
    generatedAt: new Date().toISOString(),
  });
  writeJson(join(reportsRoot(ROOT), "04-portfolio-dashboard.json"), dashboard);

  console.log(
    JSON.stringify(
      {
        ok: results.every((r) => r.ok),
        processed: results.length,
        results,
        dashboardSummary: dashboard.summary,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
