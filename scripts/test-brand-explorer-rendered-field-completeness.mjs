#!/usr/bin/env node
/**
 * test:brand-explorer-rendered-field-completeness
 * Uses live rendered payload / atelier HTML field map — not row existence alone.
 */
import "dotenv/config";
import {
  auditBrandRenderedFieldCompleteness,
  evaluateRenderedFieldCompletenessForTest,
} from "../lib/partner-intelligence/brand-explorer-rendered-field-completeness-audit.js";
import { TARGET_BRANDS } from "../lib/partner-intelligence/brand-explorer-rendered-field-completeness-inventory.js";

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1]
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [...TARGET_BRANDS];
}

async function main() {
  const brands = parseBrands(process.argv.slice(2));
  let failed = 0;
  for (const slug of brands) {
    const result = await auditBrandRenderedFieldCompleteness(slug);
    const evalResult = evaluateRenderedFieldCompletenessForTest(result);
    if (evalResult.pass) {
      console.log(`[PASS] ${slug} rendered field completeness (${result.summary.totalPass} fields)`);
    } else {
      failed += 1;
      console.log(`[FAIL] ${slug} decision=${result.releaseQualityDecision} fails=${evalResult.failures.length}`);
      for (const f of evalResult.failures.slice(0, 40)) console.log(`  - ${f}`);
      if (evalResult.failures.length > 40) {
        console.log(`  … +${evalResult.failures.length - 40} more`);
      }
    }
  }
  if (failed) {
    console.error(`\n${failed} brand(s) failed rendered field completeness.`);
    process.exit(1);
  }
  console.log(`\nAll ${brands.length} brand(s) passed rendered field completeness.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
