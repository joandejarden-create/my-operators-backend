/**
 * Sync FS-mirror contact / revenue-stream / title enrichments into Airtable
 * opportunityPayloadJson so Airtable-primary reads stay durable.
 *
 * Default: dry-run. Apply only with --apply.
 *
 * Usage:
 *   node scripts/gdi-sync-fs-mirror-enrichment-to-airtable-v1.mjs --hotel recLuxvwwxID7U2B8
 *   node scripts/gdi-sync-fs-mirror-enrichment-to-airtable-v1.mjs --hotel recLuxvwwxID7U2B8 --apply
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { mergeFsMirrorFieldsIntoOpportunity } from "../lib/group-demand-intelligence/fs-mirror-opportunity-enrichment-v1.js";
import {
  listOpportunitiesForHotel,
  upsertOpportunity,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import * as fsRepo from "../lib/group-demand-intelligence/repository.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2);
const apply = args.includes("--apply");
const hotelIdx = args.indexOf("--hotel");
const hotelId =
  hotelIdx >= 0
    ? String(args[hotelIdx + 1] || "").trim()
    : "recLuxvwwxID7U2B8";

if (!hotelId) {
  console.error("Missing --hotel <hotelId>");
  process.exit(1);
}

const atRows = await listOpportunitiesForHotel(hotelId);
const fsDoc = fsRepo.loadOpportunities(hotelId);
const fsById = new Map(
  (fsDoc.opportunities || []).map((o) => [String(o.id || o.opportunityId), o]),
);

const plan = [];
for (const at of atRows) {
  const id = String(at.id || at.opportunityId || "");
  const fsRow = fsById.get(id);
  if (!fsRow) continue;
  const { opportunity, patches } = mergeFsMirrorFieldsIntoOpportunity(at, fsRow);
  if (!patches.length) continue;
  plan.push({
    id,
    patches,
    before: {
      contact: at.primaryContact?.name || null,
      rev: at.revenueStreamClass || null,
      title: (at.title || "").slice(0, 80),
    },
    after: {
      contact: opportunity.primaryContact?.name || null,
      rev: opportunity.revenueStreamClass || null,
      title: (opportunity.title || "").slice(0, 80),
    },
    opportunity,
  });
}

const outDir = path.join(
  ROOT,
  "reports/gdi-opportunity-card-quality-v1",
);
fs.mkdirSync(outDir, { recursive: true });
const manifestPath = path.join(
  outDir,
  `fs-mirror-airtable-sync-${hotelId}${apply ? "-applied" : "-dry-run"}.json`,
);
fs.writeFileSync(
  manifestPath,
  JSON.stringify(
    {
      hotelId,
      apply,
      planned: plan.length,
      rows: plan.map(({ id, patches, before, after }) => ({
        id,
        patches,
        before,
        after,
      })),
    },
    null,
    2,
  ),
);

console.log(
  JSON.stringify(
    {
      hotelId,
      apply,
      airtableCount: atRows.length,
      planned: plan.length,
      manifestPath,
    },
    null,
    2,
  ),
);

if (!apply) {
  console.log("Dry-run only. Re-run with --apply to upsert Airtable payloads.");
  process.exit(0);
}

let ok = 0;
let fail = 0;
for (const row of plan) {
  try {
    await upsertOpportunity(row.opportunity, { hotelId });
    ok += 1;
  } catch (err) {
    fail += 1;
    console.error("upsert failed", row.id, err?.message || err);
  }
}
console.log(JSON.stringify({ applied: ok, failed: fail }, null, 2));
if (fail) process.exit(1);
