#!/usr/bin/env node
/**
 * Factory 2.0 — build complete Active/Live brand queue from Airtable.
 * Usage: node scripts/be-factory-2-queue-build-v1.mjs
 */
import { config as loadEnv } from "dotenv";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { buildCompleteBrandQueue } from "../lib/brand-explorer/factory-2/be-factory-2-queue-builder-v1.js";
import {
  saveQueue,
  buildPortfolioDashboard,
  loadBrandStage,
  writeJson,
  reportsRoot,
} from "../lib/brand-explorer/factory-2/be-factory-2-staging-store-v1.js";
import { AUTOGRAPH_BRAND_ID } from "../lib/brand-explorer/factory-2/be-factory-2-contracts-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) {
    console.error("Missing AIRTABLE_API_KEY/AIRTABLE_TOKEN or AIRTABLE_BASE_ID");
    process.exit(2);
  }

  const existingAutograph = loadBrandStage(ROOT, AUTOGRAPH_BRAND_ID);
  const queue = await buildCompleteBrandQueue({
    root: ROOT,
    token,
    baseId,
    existingAutographStage: existingAutograph,
  });
  saveQueue(ROOT, queue);
  const dashboard = buildPortfolioDashboard(ROOT);
  writeJson(join(reportsRoot(ROOT), "02-complete-brand-queue.json"), queue);
  writeJson(join(reportsRoot(ROOT), "02-portfolio-dashboard-after-queue.json"), dashboard);

  console.log(
    JSON.stringify(
      {
        ok: true,
        contract: queue.contract,
        summary: queue.summary,
        dashboardSummary: dashboard.summary,
        firstFive: queue.brands
          .filter((b) => b.status !== "SKIPPED_PROTECTED")
          .slice(0, 5)
          .map((b) => ({ priority: b.priority, brandName: b.brandName, brandId: b.brandId })),
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
