/**
 * Generate P1 CALA enrichment JSON files from corporate web research seeds.
 *
 *   node scripts/generate-gtm-p1-cala-enrichment-files.mjs
 *   node scripts/generate-gtm-p1-cala-enrichment-files.mjs --batch=1 --import --apply
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import {
  CALA_P1_BATCH1_SPECS,
  CALA_P1_BATCH2_SPECS,
  buildCalaP1Enrichment,
} from "../lib/gtm-owner-target/adapters/cala-p1-enrichment-research.js";
import { validateRegistryEnrichmentRecord } from "../lib/gtm-owner-target/registry-contact-verification.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT_DIR = join(ROOT, "data", "internal", "gtm-registry-enrichments");
const IMPORT = process.argv.includes("--import");
const APPLY = process.argv.includes("--apply");

const BATCH_ARG = process.argv.find((a) => a.startsWith("--batch="));
const BATCH = BATCH_ARG ? BATCH_ARG.split("=")[1] : "1";

function resolveSpecs() {
  if (BATCH === "2") return CALA_P1_BATCH2_SPECS;
  if (BATCH === "1") return CALA_P1_BATCH1_SPECS;
  throw new Error(`Unknown --batch=${BATCH} (use 1 or 2)`);
}

function main() {
  mkdirSync(OUT_DIR, { recursive: true });
  const specs = resolveSpecs();

  /** @type {object[]} */
  const results = [];

  for (const spec of specs) {
    const enrichment = buildCalaP1Enrichment(spec);
    const validation = validateRegistryEnrichmentRecord(enrichment);
    const slug = spec.slug || "custom";
    const key = spec.fileKey || spec.contactKey || "primary";
    const fileName = `p1-cala-${slug}-${key}.json`;
    const filePath = join(OUT_DIR, fileName);

    writeFileSync(filePath, JSON.stringify(enrichment, null, 2));
    results.push({
      fileName,
      ownerName: enrichment.ownerName,
      contact: enrichment.contact?.name,
      tier: enrichment.contact?.verificationTier,
      email: enrichment.contact?.email || null,
      linkedIn: enrichment.contact?.linkedIn || null,
      validationOk: validation.ok,
      failures: validation.failures,
    });
  }

  console.log(`P1 CALA enrichment files: ${results.length}`);
  for (const r of results) {
    const status = r.validationOk ? "OK" : `FAIL ${r.failures.join("; ")}`;
    console.log(
      `  ${r.fileName}: ${r.contact} [${r.tier}]${r.email ? ` <${r.email}>` : r.linkedIn ? " (LinkedIn)" : ""} — ${status}`
    );
  }

  if (IMPORT) {
    for (const r of results) {
      if (!r.validationOk) continue;
      if (r.tier === "V3" && !r.email) {
        console.log(`  skip import (V3 research-only): ${r.fileName}`);
        continue;
      }
      const filePath = join(OUT_DIR, r.fileName);
      const args = [
        "scripts/import-gtm-registry-contact-enrichments.mjs",
        APPLY ? "--apply" : "--dry-run",
        `--file=${filePath}`,
      ];
      const result = spawnSync(process.execPath, args, { cwd: ROOT, stdio: "inherit" });
      if (result.status !== 0) process.exit(result.status || 1);
    }
  }
}

main();
