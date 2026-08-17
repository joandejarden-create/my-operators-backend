#!/usr/bin/env node
import "dotenv/config";
import {
  runImageUniquenessAudit,
  writeImageUniquenessReports,
  IMAGE_UNIQUENESS_VERSION,
} from "../lib/partner-intelligence/brand-explorer-image-uniqueness-audit.js";

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1].split(",").map((s) => s.trim()).filter(Boolean);
  }
  return ["hotel-indigo", "mgallery-collection", "small-luxury-hotels-of-the-world"];
}

async function main() {
  const brands = parseBrands(process.argv.slice(2));
  console.log(`[${IMAGE_UNIQUENESS_VERSION}] image uniqueness audit (dry-run)`);
  console.log(`  brands: ${brands.join(", ")}`);
  const report = await runImageUniquenessAudit({ brands });
  const paths = writeImageUniquenessReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: auditPass=${report.auditPass} pass=${report.summary.passCount} fail=${report.summary.failCount}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: pass=${b.pass} galleryDistinct=${b.galleryDistinctCount}/${b.gallerySlotCount} scenario=${b.scenarioDistinctCount} property=${b.propertyExampleDistinctCount} action=${b.requiredAction}`
    );
  }
  if (!report.auditPass) process.exit(3);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
