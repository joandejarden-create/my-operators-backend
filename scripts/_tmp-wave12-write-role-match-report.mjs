#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { evaluateImageUniqueness } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";
import {
  evaluateBrandImageRoleMatch,
  IMAGE_ROLE_MATCH_VERSION,
} from "../lib/partner-intelligence/brand-explorer-image-role-match.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";
import { WAVE12_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave12-factory-plan.js";

const brandResults = [];
for (const slug of WAVE12_SLUGS) {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  const { rows } = await listPresentationRowsLight(id.recordId, id.name);
  const roleMatch = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: rows });
  const uniqueness = evaluateImageUniqueness({ brandSlug: slug, presentationRows: rows });
  brandResults.push({
    brandSlug: slug,
    brandName: id.name,
    pass: roleMatch.pass === true && uniqueness.pass === true,
    imageRoleMatchPass: roleMatch.pass === true,
    uniquenessPass: uniqueness.pass === true,
    unresolvedRoleMismatchCount: roleMatch.unresolvedRoleMismatchCount,
    galleryDistinctCount: uniqueness.galleryDistinctCount,
    scenarioDistinctCount: uniqueness.scenarioDistinctCount,
    propertyExampleDistinctCount: uniqueness.propertyExampleDistinctCount,
  });
}
const passCount = brandResults.filter((b) => b.pass).length;
const report = {
  version: IMAGE_ROLE_MATCH_VERSION,
  generatedAt: new Date().toISOString(),
  dryRun: true,
  source: "wave12-presentation-rows-light",
  brands: [...WAVE12_SLUGS],
  brandResults,
  summary: {
    brandCount: brandResults.length,
    passCount,
    failCount: brandResults.length - passCount,
    auditPass: passCount === brandResults.length,
  },
  auditPass: passCount === brandResults.length,
};
fs.writeFileSync(
  "reports/brand-explorer-image-role-match-audit.json",
  `${JSON.stringify(report, null, 2)}\n`
);
const md = [
  `# Image role-match audit`,
  ``,
  `- Pass: **${report.auditPass}** (${passCount}/${brandResults.length})`,
  `- Source: presentation-rows-light (Wave 12 factory preview)`,
  ``,
  ...brandResults.map(
    (b) =>
      `- ${b.brandSlug}: role=${b.imageRoleMatchPass} uniq=${b.uniquenessPass} g${b.galleryDistinctCount}/6`
  ),
  ``,
].join("\n");
fs.writeFileSync("reports/brand-explorer-image-role-match-audit.md", md);
console.log("Wrote role-match audit", report.summary);
