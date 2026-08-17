/**
 * Generate Mexico corporate-web-first draft enrichment files from registry queue.
 * SIGER/RNT are optional fallbacks only — no gov portal signup required for Wave 1.
 *
 * Usage:
 *   node scripts/draft-gtm-mx-registry-enrichments.mjs
 *
 * Reads: reports/gtm-owner-registry-enrichment-queue.json
 * Writes: data/internal/gtm-registry-enrichments/drafts/*.json
 *         reports/gtm-mx-registry-enrichment-drafts.json
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  buildMxCorporateWebPlan,
  buildDraftEnrichmentFromCorporateWebPlan,
} from "../lib/gtm-owner-target/adapters/mx-corporate-web-first.js";
import { isMxRntLookupEnabled } from "../lib/gtm-owner-target/adapters/mx-rnt-portal-config.js";
import { buildMxRntSearchPlan } from "../lib/gtm-owner-target/adapters/mx-rnt-hospedaje.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const QUEUE_JSON = join(ROOT, "reports", "gtm-owner-registry-enrichment-queue.json");
const REPORT_JSON = join(ROOT, "reports", "gtm-mx-registry-enrichment-drafts.json");
const DRAFT_DIR = join(ROOT, "data", "internal", "gtm-registry-enrichments", "drafts");

function slugify(value) {
  return String(value || "owner")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 64);
}

function parseSampleProperty(sample) {
  const parts = String(sample || "")
    .split("—")
    .map((p) => p.trim());
  return {
    buildingName: parts[0] || "",
    city: parts[1] || "",
    country: parts[2] || "Mexico",
  };
}

async function main() {
  if (!existsSync(QUEUE_JSON)) {
    throw new Error(`Missing ${QUEUE_JSON}. Run report-gtm-owner-registry-enrichment-queue.mjs first.`);
  }

  const queue = JSON.parse(readFileSync(QUEUE_JSON, "utf8"));
  const mexicoItems = (queue.items || []).filter(
    (item) => item.primaryCountry === "Mexico" && item.verificationStatus !== "verified"
  );

  mkdirSync(DRAFT_DIR, { recursive: true });

  /** @type {object[]} */
  const reportItems = [];

  for (const item of mexicoItems) {
    const properties =
      item.sampleProperties?.length > 0
        ? item.sampleProperties.map(parseSampleProperty)
        : [{ buildingName: item.entitySearchName, city: "", country: "Mexico" }];

    const primaryProperty = properties[0];
    const corporatePlan = buildMxCorporateWebPlan(item, primaryProperty);
    const draft = buildDraftEnrichmentFromCorporateWebPlan(item, primaryProperty);

    if (isMxRntLookupEnabled()) {
      draft.optionalRntSearchPlan = buildMxRntSearchPlan(item, primaryProperty);
    }

    draft.propertySearchVariants = properties.slice(0, 5).map((property) => ({
      property,
      corporateWebPlan: buildMxCorporateWebPlan(item, property),
    }));

    const fileName = `${slugify(item.ownerName)}.json`;
    const filePath = join(DRAFT_DIR, fileName);
    writeFileSync(filePath, JSON.stringify(draft, null, 2));

    reportItems.push({
      ownerName: item.ownerName,
      ownerTargetId: item.id,
      draftPath: filePath,
      registryPrimaryPath: corporatePlan.registryPath,
      corporateWebsite: corporatePlan.website,
      entityType: corporatePlan.entityType,
      recommendedContact: corporatePlan.recommendedContact,
      corporateWebPlan: corporatePlan,
      rntIncluded: isMxRntLookupEnabled(),
    });
  }

  const readyCount = reportItems.filter((i) => i.recommendedContact?.email || i.recommendedContact?.linkedIn).length;

  const report = {
    generatedAt: new Date().toISOString(),
    mexicoQueueCount: mexicoItems.length,
    draftCount: reportItems.length,
    primaryPath: "corporate_web_first",
    readyForImportHint: readyCount,
    rntLookupEnabled: isMxRntLookupEnabled(),
    note:
      "Wave 1: corporate website / IR / LinkedIn first. No SIGER CURP signup or RNT required. " +
      "Import ready contacts: node scripts/import-gtm-wave1-mx-enrichments.mjs --dry-run",
    items: reportItems,
  };

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  console.log(`Mexico queue items: ${mexicoItems.length}`);
  console.log(`Corporate-web-first drafts: ${reportItems.length} → ${DRAFT_DIR}`);
  console.log(`With recommended contact (email or LinkedIn): ${readyCount}`);
  console.log("Wrote", REPORT_JSON);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
