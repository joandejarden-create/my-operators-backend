/**
 * Brand Explorer image role-match audit (read-only).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import {
  IMAGE_ROLE_MATCH_VERSION,
  evaluateBrandImageRoleMatch,
} from "./brand-explorer-image-role-match.js";

export const REPORT_JSON = "brand-explorer-image-role-match-audit.json";
export const REPORT_MD = "brand-explorer-image-role-match-audit.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

export async function auditBrandImageRoleMatch(brandSlug) {
  const { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } = await import(
    "./brand-explorer-factory-preview-candidates.js"
  );
  const factory = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[String(brandSlug || "").trim().toLowerCase()];
  const ctx = await loadBrandFactoryContext(factory?.recordId || brandSlug);
  const rows = ctx.presentationRows || [];
  const roleMatch = evaluateBrandImageRoleMatch({
    presentationRows: rows,
    brandSlug,
  });
  const uniqueness = evaluateImageUniqueness({
    brandSlug,
    presentationRows: rows,
  });

  return {
    brandSlug,
    brandName: ctx.brandConfig?.name || factory?.name || brandSlug,
    pass: roleMatch.pass === true && uniqueness.pass === true,
    imageRoleMatchPass: roleMatch.pass === true,
    uniquenessPass: uniqueness.pass === true,
    unresolvedRoleMismatchCount: roleMatch.unresolvedRoleMismatchCount,
    galleryDistinctCount: uniqueness.galleryDistinctCount,
    scenarioDistinctCount: uniqueness.scenarioDistinctCount,
    propertyExampleDistinctCount: uniqueness.propertyExampleDistinctCount,
    roleMatch,
    uniqueness,
    requiredAction:
      roleMatch.pass && uniqueness.pass
        ? "no_action"
        : roleMatch.pass
          ? "image_remediation_uniqueness"
          : "image_role_match_remediation",
  };
}

export async function runImageRoleMatchAudit({ brands = [] } = {}) {
  const brandResults = [];
  for (const slug of brands) {
    brandResults.push(await auditBrandImageRoleMatch(slug));
  }
  const passCount = brandResults.filter((b) => b.pass).length;
  return {
    version: IMAGE_ROLE_MATCH_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands,
    brandResults,
    summary: {
      brandCount: brandResults.length,
      passCount,
      failCount: brandResults.length - passCount,
      auditPass: passCount === brandResults.length && brandResults.length > 0,
    },
    auditPass: passCount === brandResults.length && brandResults.length > 0,
  };
}

export function writeImageRoleMatchAuditReports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const lines = [];
  lines.push("# Brand Explorer Image Role-Match Audit");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`auditPass: **${report.auditPass}**`);
  lines.push("");
  lines.push(`- Brands: **${report.summary.brandCount}**`);
  lines.push(`- Pass: **${report.summary.passCount}**`);
  lines.push(`- Fail: **${report.summary.failCount}**`);
  lines.push("");

  for (const b of report.brandResults) {
    lines.push(`### ${b.brandSlug}`);
    lines.push(
      `- pass: **${b.pass}** · roleMatch=${b.imageRoleMatchPass} · uniqueness=${b.uniquenessPass} · unresolved=${b.unresolvedRoleMismatchCount}`
    );
    lines.push(
      `- galleryDistinct=${b.galleryDistinctCount} scenario=${b.scenarioDistinctCount} property=${b.propertyExampleDistinctCount}`
    );
    lines.push(`- action: \`${b.requiredAction}\``);
    lines.push("");
    lines.push(
      "| Brand | Section | Slot | Current Caption | Current Role | Detected | Match Status | Issue | Recommended |"
    );
    lines.push("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for (const row of b.roleMatch.fieldRows || []) {
      lines.push(
        `| ${row.brand} | ${row.section} | ${row.slot} | ${(row.currentCaption || "").slice(0, 48)} | ${row.currentRole} | ${row.detectedVisualCategory} | ${row.matchStatus} | ${row.issue} | ${(row.recommendedCaptionOrReplacement || "—").slice(0, 40)} |`
      );
    }
    lines.push("");
  }

  fs.writeFileSync(mdPath, lines.join("\n"));
  return { jsonPath, mdPath };
}
