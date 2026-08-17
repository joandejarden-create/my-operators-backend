#!/usr/bin/env node
/**
 * CALA Tribute Property Visual Candidate Discovery v1.
 *
 *   npm run cala-tribute-property-visual-discovery -- --dry-run
 *
 * Apply gated: --apply --approve-cala-tribute-visual-candidates
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildCalaTributeDiscoveryMarkdown,
  buildCalaTributeDiscoveryReport,
} from "../lib/partner-intelligence/cala-tribute-property-visual-discovery.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

async function main() {
  const apply = process.argv.includes("--apply");
  const applyApproved = process.argv.includes("--approve-cala-tribute-visual-candidates");
  const noProbe = process.argv.includes("--no-probe");
  const noCrawl = process.argv.includes("--no-crawl");
  const usePuppeteer =
    process.argv.includes("--use-puppeteer") ||
    process.env.CALA_TRIBUTE_DISCOVERY_USE_PUPPETEER === "1";

  if (apply && !applyApproved) {
    console.error(
      "[cala-tribute-property-visual-discovery] --apply requires --approve-cala-tribute-visual-candidates"
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const mode = apply && applyApproved ? "candidates-apply" : "dry-run";
  console.log(`[cala-tribute-property-visual-discovery] mode=${mode}`);

  const report = await buildCalaTributeDiscoveryReport({
    probePages: !noProbe,
    crawlSitemaps: !noCrawl,
    usePuppeteer,
    apply: apply && applyApproved,
    applyApproved,
  });

  if (report.error) {
    console.error(report.error);
    process.exit(1);
  }

  console.log(
    `  properties=${report.propertiesDiscovered} cala=${report.calaRelevantCount} images=${report.imageCandidatesFound} proposed=${report.registry.proposedCount} airtable_modified=${report.airtableModified}`
  );

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildCalaTributeDiscoveryMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
