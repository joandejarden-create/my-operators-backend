#!/usr/bin/env node
/**
 * Read-only: which public-full Brand Explorer brands show the hero trust chip.
 * Uses Brand Basics P1 governance → normalizeProfileGovernance (same as API).
 *
 * Usage:
 *   node scripts/audit-brand-explorer-trust-chip-governance-gap.mjs
 *
 * Requires: AIRTABLE_API_KEY, AIRTABLE_BASE_ID
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { MAP_PROFILE_GOVERNANCE_AIRTABLE } from "../lib/profile-governance/profile-governance-fields.js";
import { normalizeProfileGovernance } from "../lib/profile-governance/normalize-profile-governance.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "brand-explorer-trust-chip-governance-gap-audit.json");
const REPORT_MD = join(ROOT, "reports", "brand-explorer-trust-chip-governance-gap-audit.md");

/** Public-full baseline (docs/data-intelligence + PVQL baseline). */
const COHORT = [
  { slug: "ascend", recordId: "reclkgOzvAcBheUSo", name: "Ascend Hotel Collection" },
  { slug: "comfort-inn-suites", recordId: "recOzH5iAE1xEjyD0", name: "Comfort Inn & Suites" },
  { slug: "curio-collection", recordId: "receQkxgjlezsc1xg", name: "Curio Collection by Hilton" },
  { slug: "design-hotels", recordId: "rec02zPClpWUTCyXM", name: "Design Hotels" },
  { slug: "everhome-suites", recordId: "recqkkrsevi4r9ibj", name: "Everhome Suites" },
  { slug: "hotel-indigo", recordId: "recegXrqaPiSLGCIe", name: "Hotel Indigo" },
  { slug: "kimpton", recordId: "recCKuXCmGvxHPfb3", name: "Kimpton Hotels" },
  { slug: "mgallery-collection", recordId: "recrWCD1LMqu864oU", name: "MGallery Collection" },
  {
    slug: "radisson-individuals-by-choice",
    recordId: "recjjSnY2opb8P4DG",
    name: "Radisson Individuals by Choice",
  },
  {
    slug: "small-luxury-hotels-of-the-world",
    recordId: "recRyvM8OmLlDj9G7",
    name: "Small Luxury Hotels of the World",
  },
  { slug: "tribute-portfolio", recordId: "recCvV0PuZOi8c3hC", name: "Tribute Portfolio" },
];

const BRAND_TABLE = "Brand Setup - Brand Basics";

function cellSnap(v) {
  if (v == null || v === "") return null;
  if (typeof v === "object" && v.name) return String(v.name).trim() || null;
  return v;
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const rows = [];

  for (const brand of COHORT) {
    const rec = await base(BRAND_TABLE).find(brand.recordId);
    const fields = rec.fields || {};
    const gov = normalizeProfileGovernance(fields, {
      entityType: "brand",
      sourceTable: BRAND_TABLE,
    });
    const rawGovernance = {};
    for (const [k, col] of Object.entries(MAP_PROFILE_GOVERNANCE_AIRTABLE)) {
      rawGovernance[k] = cellSnap(fields[col]);
    }
    rows.push({
      slug: brand.slug,
      name: brand.name,
      recordId: brand.recordId,
      chipVisible: Boolean(gov.displayLabel),
      displayLabel: gov.displayLabel,
      displaySubtitle: gov.displaySubtitle,
      validationStatus: gov.validationStatus,
      usagePermission: gov.usagePermission,
      externalDisplayStatus: gov.externalDisplayStatus,
      sourceRegion: gov.sourceRegion,
      lastReviewedDate: gov.lastReviewedDate,
      sourceBasis: gov.sourceBasis,
      internalWarnings: gov.internalWarnings,
      rawGovernance,
    });
  }

  const withChip = rows.filter((r) => r.chipVisible);
  const withoutChip = rows.filter((r) => !r.chipVisible);
  const report = {
    generatedAt: new Date().toISOString(),
    mode: "read-only",
    cohort: "public_full_profiles_baseline_11",
    baseId,
    summary: {
      total: rows.length,
      withTrustChip: withChip.length,
      missingTrustChip: withoutChip.length,
    },
    withTrustChip: withChip.map((r) => ({
      slug: r.slug,
      displayLabel: r.displayLabel,
      displaySubtitle: r.displaySubtitle,
    })),
    missingTrustChip: withoutChip.map((r) => ({
      slug: r.slug,
      name: r.name,
      recordId: r.recordId,
      validationStatus: r.validationStatus,
      usagePermission: r.usagePermission,
      externalDisplayStatus: r.externalDisplayStatus,
      lastReviewedDate: r.lastReviewedDate,
      sourceRegion: r.sourceRegion,
      internalWarnings: r.internalWarnings,
    })),
    brands: rows,
  };

  const md = [
    "# Brand Explorer Trust Chip Governance Gap Audit",
    "",
    `Generated: ${report.generatedAt}`,
    "Mode: **read-only**",
    "Cohort: 11 public-full baseline brands",
    "",
    "## Summary",
    "",
    `| With trust chip | ${withChip.length} |`,
    `| Missing trust chip | ${withoutChip.length} |`,
    "",
    "## Brands WITH chip",
    "",
    ...withChip.map(
      (r) =>
        `- **${r.slug}**: ${r.displayLabel} — ${r.displaySubtitle || "(no subtitle)"}`
    ),
    "",
    "## Brands MISSING chip",
    "",
    "| Brand | Validation Status | Usage Permission | External Display | Last Reviewed | Warnings |",
    "|-------|-------------------|------------------|------------------|---------------|----------|",
    ...withoutChip.map((r) => {
      const warnings = (r.internalWarnings || []).join("; ") || "—";
      return `| ${r.slug} | ${r.validationStatus || "*(blank)*"} | ${r.usagePermission || "*(blank)*"} | ${r.externalDisplayStatus || "*(blank)*"} | ${r.lastReviewedDate || "—"} | ${warnings} |`;
    }),
    "",
    "## Next steps",
    "",
    "1. For each missing brand: `npm run audit-partner-intelligence-publish-readiness`",
    "2. Steward PI package if blocked.",
    "3. Dry-run: `npm run publish-partner-intelligence-profile-governance -- --entity-type brand --target-rec-id <rec> --dry-run --recompute`",
    "4. Apply only after founder/steward approval.",
    "",
  ].join("\n");

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, md, "utf8");

  console.log(JSON.stringify(report.summary, null, 2));
  console.log("WITH CHIP:");
  for (const r of withChip) console.log(" -", r.slug, "=>", r.displayLabel);
  console.log("MISSING CHIP:");
  for (const r of withoutChip) {
    console.log(
      " -",
      r.slug,
      "|",
      r.validationStatus || "blank",
      "|",
      r.externalDisplayStatus || "blank EDS",
      "|",
      (r.internalWarnings || []).join("; ")
    );
  }
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
