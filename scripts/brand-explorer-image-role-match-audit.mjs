#!/usr/bin/env node
/**
 * Image role-match audit — captions must match visual/metadata evidence.
 */
import "dotenv/config";
import {
  runImageRoleMatchAudit,
  writeImageRoleMatchAuditReports,
} from "../lib/partner-intelligence/brand-explorer-image-role-match-audit.js";

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1]
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return ["mgallery-collection", "hotel-indigo", "small-luxury-hotels-of-the-world"];
}

async function main() {
  const brands = parseBrands(process.argv.slice(2));
  console.log(`[image-role-match-audit] brands=${brands.join(",")}`);
  const report = await runImageRoleMatchAudit({ brands });
  const paths = writeImageRoleMatchAuditReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: auditPass=${report.auditPass} pass=${report.summary.passCount} fail=${report.summary.failCount}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: pass=${b.pass} roleMatch=${b.imageRoleMatchPass} unresolved=${b.unresolvedRoleMismatchCount} action=${b.requiredAction}`
    );
  }
  if (!report.auditPass) process.exit(3);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
