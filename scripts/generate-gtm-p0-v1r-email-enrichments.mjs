/**
 * Generate P0 V1R email upgrade JSON files (LinkedIn V2 → named email V1R).
 *
 *   node scripts/generate-gtm-p0-v1r-email-enrichments.mjs
 *   node scripts/generate-gtm-p0-v1r-email-enrichments.mjs --import --dry-run
 *   node scripts/generate-gtm-p0-v1r-email-enrichments.mjs --import --apply
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import {
  P0_V1R_EMAIL_SPECS,
  buildP0V1rEmailEnrichment,
} from "../lib/gtm-owner-target/adapters/p0-v1r-email-research.js";
import { validateRegistryEnrichmentRecord } from "../lib/gtm-owner-target/registry-contact-verification.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT_DIR = join(ROOT, "data", "internal", "gtm-registry-enrichments");
const IMPORT = process.argv.includes("--import");
const APPLY = process.argv.includes("--apply");

function main() {
  mkdirSync(OUT_DIR, { recursive: true });

  /** @type {object[]} */
  const results = [];

  for (const spec of P0_V1R_EMAIL_SPECS) {
    const enrichment = buildP0V1rEmailEnrichment(spec);
    const validation = validateRegistryEnrichmentRecord(enrichment);
    const key = spec.contactKey || "primary";
    const fileName = `p0-v1r-${spec.slug}-${key}.json`;
    const filePath = join(OUT_DIR, fileName);

    writeFileSync(filePath, JSON.stringify(enrichment, null, 2));
    results.push({
      fileName,
      ownerName: enrichment.ownerName,
      contact: enrichment.contact?.name,
      tier: enrichment.contact?.verificationTier,
      email: enrichment.contact?.email || null,
      validationOk: validation.ok,
      failures: validation.failures,
    });
  }

  console.log(`P0 V1R email enrichments: ${results.length}`);
  for (const r of results) {
    const status = r.validationOk ? "OK" : `FAIL ${r.failures.join("; ")}`;
    console.log(`  ${r.fileName}: ${r.contact} [${r.tier}] <${r.email}> — ${status}`);
  }

  if (IMPORT) {
    for (const r of results) {
      if (!r.validationOk) continue;
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
