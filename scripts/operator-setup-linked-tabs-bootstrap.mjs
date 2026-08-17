#!/usr/bin/env node
/**
 * Bootstrap Operator Setup 1:1 linked tabs for Masters.
 *
 *   npm run operator-setup-linked-tabs-bootstrap -- --dry-run --slugs tafer-hotels-resorts
 *   npm run operator-setup-linked-tabs-bootstrap -- --apply --approve-operator-setup-linked-tabs-bootstrap --slugs tafer-hotels-resorts,grupo-presidente
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  runOperatorSetupLinkedTabsBootstrap,
  OPERATOR_SETUP_LINKED_BOOTSTRAP_VERSION,
} from "../lib/partner-intelligence/operator-setup-linked-tabs-bootstrap.js";
import { getOperatorFactoryQueueEntry } from "../lib/partner-intelligence/operator-explorer-factory-queue.js";
import { getWebsiteContentPack } from "../lib/partner-intelligence/operator-setup-website-content-packs.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function parseArgs(argv) {
  const out = { apply: false, approve: false, slugs: null };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-operator-setup-linked-tabs-bootstrap") out.approve = true;
    else if (a === "--slugs" && argv[i + 1]) {
      out.slugs = argv[++i].split(",").map((s) => s.trim()).filter(Boolean);
    }
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const defaultSlugs = [
    "tafer-hotels-resorts",
    "grupo-presidente",
    "highgate",
    "grupo-hotelero-santa-fe",
    "arriva-hospitality-group",
    "brittain-resorts-hotels",
    "atlantica-hotels-international",
    "aimbridge-latam",
    "ghl-hoteles",
  ];
  const slugs = args.slugs || defaultSlugs;
  const masters = slugs.map((slug) => {
    const q = getOperatorFactoryQueueEntry(slug);
    if (!q?.recordId) throw new Error(`No queue Master for ${slug}`);
    const pack = getWebsiteContentPack(slug);
    return {
      recordId: q.recordId,
      companyName: q.companyName,
      website: pack?.profile?.website || (q.domain ? `https://www.${q.domain}/` : undefined),
    };
  });

  console.log(`[${OPERATOR_SETUP_LINKED_BOOTSTRAP_VERSION}] dryRun=${!args.apply} masters=${masters.length}`);
  const report = await runOperatorSetupLinkedTabsBootstrap({
    masters,
    apply: args.apply,
    approveBootstrap: args.approve,
  });
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "operator-setup-linked-tabs-bootstrap.json");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  console.log(`Wrote ${jsonPath}`);
  console.log(JSON.stringify(report.summary, null, 2));
  for (const r of report.results) {
    console.log(
      `  ${r.companyName || r.recordId}: create/would=${r.createdCount} exists=${r.existingCount}`
    );
  }
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
