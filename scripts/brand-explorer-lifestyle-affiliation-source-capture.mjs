#!/usr/bin/env node
/**
 * Brand Explorer Lifestyle / Affiliation Source Capture v35C.
 *
 *   npm run brand-explorer-lifestyle-affiliation-source-capture -- --brands design-hotels,small-luxury-hotels-of-the-world --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_DESIGN_SLH_ONLY,
  APPLY_FLAG_NO_ACTIVE_APPROVAL,
  APPLY_FLAG_NO_IMAGE_FIELDS,
  APPLY_FLAG_NO_PRESENTATION,
  APPLY_FLAG_NO_REGISTRY,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_SOURCE_ONLY,
  V35C_VERSION,
  buildLifestyleAffiliationSourceCaptureV35CReport,
} from "../lib/partner-intelligence/brand-explorer-lifestyle-affiliation-source-capture-v35C.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function hasFlag(name) {
  return process.argv.includes(name);
}

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  const apply = hasFlag("--apply");
  const brands = argValue("--brands", "design-hotels,small-luxury-hotels-of-the-world");

  const report = await buildLifestyleAffiliationSourceCaptureV35CReport({
    brands,
    apply,
    approveBatch: hasFlag(APPLY_FLAG_APPROVE),
    sourceOnly: hasFlag(APPLY_FLAG_SOURCE_ONLY),
    noValidationClaim: hasFlag(APPLY_FLAG_NO_VALIDATION),
    noPresentation: hasFlag(APPLY_FLAG_NO_PRESENTATION),
    noRegistry: hasFlag(APPLY_FLAG_NO_REGISTRY),
    noImageFields: hasFlag(APPLY_FLAG_NO_IMAGE_FIELDS),
    noActiveApproval: hasFlag(APPLY_FLAG_NO_ACTIVE_APPROVAL),
    designSlhOnly: hasFlag(APPLY_FLAG_DESIGN_SLH_ONLY),
  });

  const combinedJson = join(ROOT, "reports", "brand-explorer-lifestyle-affiliation-source-capture-v35C.json");
  const combinedMd = join(ROOT, "reports", "brand-explorer-lifestyle-affiliation-source-capture-v35C.md");
  mkdirSync(dirname(combinedJson), { recursive: true });
  writeFileSync(combinedJson, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(combinedMd, `${report.markdown}\n`);

  for (const brand of report.brands) {
    const slugKey = brand.slug === "small-luxury-hotels-of-the-world" ? "slh" : brand.slug;
    const jsonPath = join(ROOT, "reports", `brand-explorer-source-capture-${slugKey}-v35C.json`);
    const mdPath = join(ROOT, "reports", `brand-explorer-source-capture-${slugKey}-v35C.md`);
    const brandReport = {
      version: V35C_VERSION,
      generatedAt: report.generatedAt,
      mode: report.mode,
      brand,
      applyBlockers: report.applyBlockers,
      exactApplyCommand: report.exactApplyCommand,
    };
    writeFileSync(jsonPath, `${JSON.stringify(brandReport, null, 2)}\n`);
    writeFileSync(mdPath, `${buildBrandMarkdown(brand)}\n`);
    console.log(`Wrote ${jsonPath}`);
    console.log(`Wrote ${mdPath}`);
  }

  console.log(`Wrote ${combinedJson}`);
  console.log(`Wrote ${combinedMd}`);
  console.log(`v35C mode: ${report.mode}`);
  console.log(`Company Validated untouched: ${report.companyValidatedUntouched ? "yes" : "no"}`);
  for (const brand of report.brands) {
    console.log(
      `  ${brand.name}: ${brand.proposedCreates.length} creates, ${brand.proposedUpdates.length} updates, projection=${brand.assetPackProjection.classification}`
    );
  }
  if (report.applyBlockers.length) console.log(`Apply blockers: ${report.applyBlockers.join("; ")}`);
}

function buildBrandMarkdown(brand) {
  const lines = [];
  lines.push(`# Source Capture ${V35C_VERSION} — ${brand.name}`);
  lines.push("");
  lines.push(`- Slug: \`${brand.slug}\``);
  lines.push(`- Record: \`${brand.recordId}\``);
  lines.push(`- Asset-pack projection: **${brand.assetPackProjection.classification}**`);
  lines.push("");
  lines.push("## Source Library plan");
  for (const create of brand.proposedCreates) {
    lines.push(`### CREATE — ${create.role}`);
    lines.push(`- Title: ${create.fields["Source Title"]}`);
    lines.push(`- URL: ${create.sourceUrl}`);
    lines.push(`- Type: ${create.fields["Source Type"]}`);
    lines.push(`- Status: ${create.fields.Status}`);
    lines.push(`- Approved for Explorer Use: ${create.fields["Approved for Explorer Use?"]}`);
    lines.push(`- Notes: ${create.fields.Notes}`);
    lines.push("");
  }
  lines.push("## Property examples");
  for (const p of brand.propertyExampleCandidates) {
    lines.push(`- **${p.propertyName}** (${p.geographyLabel})`);
    lines.push(`  - Page: ${p.sourcePageUrl}`);
    lines.push(`  - Image: ${p.primaryImage?.imageUrl || "pending probe"}`);
    lines.push(`  - Note: ${p.ownerRelevance}`);
  }
  lines.push("");
  lines.push("## Gallery candidates");
  for (const g of brand.galleryImageCandidates) {
    lines.push(`- ${g.intendedGalleryLabel}: ${g.imageUrl}`);
  }
  return lines.join("\n");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
