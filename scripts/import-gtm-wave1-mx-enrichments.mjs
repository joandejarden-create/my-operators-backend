/**
 * Import Wave 1 Mexico corporate-web enrichments (pre-researched contacts).
 *
 * Usage:
 *   node scripts/import-gtm-wave1-mx-enrichments.mjs --dry-run
 *   node scripts/import-gtm-wave1-mx-enrichments.mjs --apply
 *   node scripts/import-gtm-wave1-mx-enrichments.mjs --dry-run --owner="Fibra Inn"
 *
 * Writes fixture JSON to data/internal/gtm-registry-enrichments/ then delegates to import script.
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import { readFileSync } from "fs";
import {
  MX_CORPORATE_WEB_SEEDS,
  resolveMxCorporateSeed,
} from "../lib/gtm-owner-target/adapters/mx-corporate-web-seeds.js";
import { buildEnrichmentFromSeedContact } from "../lib/gtm-owner-target/adapters/mx-corporate-web-first.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT_DIR = join(ROOT, "data", "internal", "gtm-registry-enrichments");
const QUEUE_JSON = join(ROOT, "reports", "gtm-owner-registry-enrichment-queue.json");
const APPLY = process.argv.includes("--apply");
const DRY_RUN = !APPLY;

/** Wave 1 import set — named-person V1R emails only (no info@, ir@, or role channels). */
const WAVE1_IMPORTS = [];

function loadOwnerTargetId(ownerName) {
  try {
    const queue = JSON.parse(readFileSync(QUEUE_JSON, "utf8"));
    const item = (queue.items || []).find(
      (i) => String(i.ownerName).toLowerCase() === String(ownerName).toLowerCase()
    );
    return item?.id || null;
  } catch {
    return null;
  }
}

function parseOwnerFilter() {
  const arg = process.argv.find((a) => a.startsWith("--owner="));
  if (!arg) return null;
  return arg.slice("--owner=".length).replace(/^"|"$/g, "");
}

function main() {
  const ownerFilter = parseOwnerFilter();
  mkdirSync(OUT_DIR, { recursive: true });

  /** @type {object[]} */
  const generated = [];
  /** @type {string[]} */
  const writtenFiles = [];

  for (const spec of WAVE1_IMPORTS) {
    const seed = MX_CORPORATE_WEB_SEEDS.find((s) => s.slug === spec.seedSlug);
    if (!seed) continue;
    const ownerName = seed.ownerNameMatch[0];
    if (ownerFilter && ownerName.toLowerCase() !== ownerFilter.toLowerCase()) continue;

    const ownerTargetId = loadOwnerTargetId(ownerName);
    const enrichment = buildEnrichmentFromSeedContact(seed, {
      ownerTargetId,
      contactKey: spec.contactKey,
    });

    const filePath = join(OUT_DIR, `${seed.slug}.json`);
    writeFileSync(filePath, JSON.stringify(enrichment, null, 2));
    writtenFiles.push(filePath);
    generated.push({
      ownerName,
      ownerTargetId,
      contact: enrichment.contact?.name,
      email: enrichment.contact?.email || null,
      tier: enrichment.contact?.verificationTier,
      filePath,
    });
  }

  console.log(`Wave 1 Mexico enrichments: ${generated.length}`);
  for (const g of generated) {
    console.log(
      `  - ${g.ownerName}: ${g.contact}${g.email ? ` <${g.email}>` : " (LinkedIn)"} [${g.tier}]`
    );
  }

  if (generated.length === 0) {
    console.log("No enrichments matched filter.");
    process.exit(0);
  }

  for (const file of writtenFiles) {
    const args = [
      "scripts/import-gtm-registry-contact-enrichments.mjs",
      DRY_RUN ? "--dry-run" : "--apply",
      `--file=${file}`,
    ];
    const result = spawnSync(process.execPath, args, { cwd: ROOT, stdio: "inherit" });
    if (result.status !== 0) process.exit(result.status || 1);
  }
}

main();
