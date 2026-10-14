#!/usr/bin/env node
/**
 * Backfill hotel-relevance research through the coordinator research stage
 * (not a hand overlay). Writes research artifacts, generates importer overlay
 * FROM research, then applies via canonical reviewed-prospect importer.
 *
 * Usage:
 *   node scripts/gdi-backfill-hotel-relevance-research-v1.mjs --hotel rec0eTEnCqlsGJJt9 --dry-run
 *   node scripts/gdi-backfill-hotel-relevance-research-v1.mjs --hotel rec0eTEnCqlsGJJt9 --apply --adapter airtable
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { isReviewedProspectCustomerVisible } from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";
import {
  synthesizeHotelRelevanceResearchBatch,
  researchBatchToSalesReviewOverlay,
  HOTEL_RELEVANCE_RESEARCH_VERSION,
} from "../lib/group-demand-intelligence/workflow/hotel-relevance-research-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}
function has(flag) {
  return process.argv.includes(flag);
}

const hotelId = String(arg("--hotel", "rec0eTEnCqlsGJJt9") || "").trim();
const apply = has("--apply");
const adapter = String(arg("--adapter", "airtable") || "airtable").trim();
const packageDir =
  arg("--package-dir") ||
  path.join(ROOT, "reports/gdi-bethesda-process-replication/hotel-review");
const outDir =
  arg("--out-dir") ||
  path.join(
    ROOT,
    "reports/gdi-operational-baseline/hotel-relevance-backfill",
    hotelId
  );

fs.mkdirSync(outDir, { recursive: true });

process.env.GDI_OPPORTUNITIES_PERSISTENCE =
  adapter === "filesystem" || adapter === "fs" ? "filesystem" : "airtable";
if (process.env.GDI_OPPORTUNITIES_PERSISTENCE === "airtable") {
  delete process.env.GDI_DATA_ROOT;
}
process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 =
  process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 || "1";

const hotelKey =
  hotelId === "rec0eTEnCqlsGJJt9"
    ? "mayflower"
    : hotelId === "recLuxvwwxID7U2B8"
      ? "bethesda"
      : hotelId === "recnPB3Q02NO8Ma1o"
        ? "san-diego"
        : hotelId.slice(0, 8);

const doc = await loadOpportunitiesCanonical(hotelId);
const all = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
const facing = all.filter((o) =>
  isReviewedProspectCustomerVisible(o, { hotelId })
);
const pool = facing.length ? facing : all.slice(0, 40);

console.log(
  JSON.stringify(
    {
      hotelId,
      mode: apply ? "apply" : "dry-run",
      pool: pool.length,
      facing: facing.length,
      researchVersion: HOTEL_RELEVANCE_RESEARCH_VERSION,
    },
    null,
    2
  )
);

const batch = synthesizeHotelRelevanceResearchBatch(pool, hotelId, {});
const overlay = researchBatchToSalesReviewOverlay(batch, { hotelKey });

fs.writeFileSync(
  path.join(outDir, "hotel-relevance-research.json"),
  JSON.stringify(batch, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(outDir, "sales-review-overlay-from-research.json"),
  JSON.stringify(overlay, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(outDir, "opportunities-with-relevance.json"),
  JSON.stringify(
    {
      hotelId,
      version: HOTEL_RELEVANCE_RESEARCH_VERSION,
      opportunities: batch.results.map((r) => r.opportunity),
    },
    null,
    2
  ) + "\n"
);

console.log(
  JSON.stringify(
    {
      counts: batch.counts,
      overlayCards: overlay.cards.length,
      sample: batch.results.slice(0, 3).map((r) => ({
        id: r.opportunityId,
        cardId: r.cardId,
        disposition: r.disposition,
        gaps: r.research?.completeness?.gaps || [],
        fitPreview: String(r.research?.propertySpecificFit?.text || "").slice(0, 120),
      })),
    },
    null,
    2
  )
);

const importArgs = [
  path.join(ROOT, "scripts/gdi-import-reviewed-prospects.mjs"),
  "--hotel",
  hotelId,
  "--adapter",
  adapter,
  "--package-dir",
  packageDir,
  "--content-overlay",
  path.join(outDir, "sales-review-overlay-from-research.json"),
  "--out-dir",
  path.join(outDir, "import"),
  "--backup-dir",
  path.join(outDir, "import", "backups"),
];
if (apply) importArgs.push("--apply");
else importArgs.push("--dry-run");

const result = spawnSync(process.execPath, importArgs, {
  cwd: ROOT,
  encoding: "utf8",
  env: process.env,
});
process.stdout.write(result.stdout || "");
process.stderr.write(result.stderr || "");

fs.writeFileSync(
  path.join(outDir, "backfill-summary.json"),
  JSON.stringify(
    {
      hotelId,
      apply,
      researchCounts: batch.counts,
      overlayCards: overlay.cards.length,
      importExitCode: result.status,
      researchPath: path.join(outDir, "hotel-relevance-research.json"),
      overlayPath: path.join(outDir, "sales-review-overlay-from-research.json"),
    },
    null,
    2
  ) + "\n"
);

process.exit(result.status === 0 ? 0 : result.status || 1);
