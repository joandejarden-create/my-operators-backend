#!/usr/bin/env node
import "dotenv/config";
import { auditBrandTabFactory, evaluateTabFactoryForTest } from "../lib/partner-intelligence/brand-explorer-tab-factory-audit.js";
import { TAB_FACTORY_TARGET_BRANDS } from "../lib/partner-intelligence/brand-explorer-tab-contracts.js";

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1].split(",").map((s) => s.trim()).filter(Boolean);
  }
  return [...TAB_FACTORY_TARGET_BRANDS];
}

async function main() {
  const brands = parseBrands(process.argv.slice(2));
  let failed = 0;
  for (const slug of brands) {
    const result = await auditBrandTabFactory(slug);
    const evalResult = evaluateTabFactoryForTest(result);
    if (evalResult.pass) {
      console.log(`[PASS] ${slug} tab factory`);
    } else {
      failed += 1;
      console.log(`[FAIL] ${slug} auditPass=${result.auditPass} fails=${evalResult.failures.length}`);
      for (const f of evalResult.failures.slice(0, 40)) console.log(`  - ${f}`);
    }
  }
  if (failed) process.exit(1);
  console.log(`\nAll ${brands.length} brand(s) passed tab factory audit.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
