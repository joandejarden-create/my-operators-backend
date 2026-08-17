#!/usr/bin/env node
/**
 * Brand Basics P1 governance backfill for public-full brands missing the trust chip.
 * Default dry-run. Apply requires --apply --approve-trust-chip-basics-backfill.
 * Never sets Company Validated / Company Validation Date.
 *
 * Usage:
 *   node scripts/dry-run-brand-explorer-trust-chip-basics-backfill.mjs
 *   node scripts/dry-run-brand-explorer-trust-chip-basics-backfill.mjs --brands hotel-indigo
 *   node scripts/dry-run-brand-explorer-trust-chip-basics-backfill.mjs --apply --approve-trust-chip-basics-backfill
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { MAP_PROFILE_GOVERNANCE_AIRTABLE } from "../lib/profile-governance/profile-governance-fields.js";
import { normalizeProfileGovernance } from "../lib/profile-governance/normalize-profile-governance.js";
import { PARTNER_INTELLIGENCE_TABLES } from "../api/lib/partner-intelligence-field-map.js";
import { normalizePartnerSourceRecord } from "../lib/partner-intelligence/airtable-source.js";
import { normalizePartnerFactRecord } from "../lib/partner-intelligence/airtable-facts.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "brand-explorer-trust-chip-basics-backfill-dry-run.json");
const REPORT_MD = join(ROOT, "reports", "brand-explorer-trust-chip-basics-backfill-dry-run.md");
const APPLY_REPORT_JSON = join(ROOT, "reports", "brand-explorer-trust-chip-basics-backfill-apply.json");
const APPLY_REPORT_MD = join(ROOT, "reports", "brand-explorer-trust-chip-basics-backfill-apply.md");

const ALL_TARGETS = [
  // Wave 1 (applied 2026-07-23)
  { slug: "design-hotels", recordId: "rec02zPClpWUTCyXM", name: "Design Hotels" },
  { slug: "hotel-indigo", recordId: "recegXrqaPiSLGCIe", name: "Hotel Indigo" },
  { slug: "mgallery-collection", recordId: "recrWCD1LMqu864oU", name: "MGallery Collection" },
  {
    slug: "radisson-individuals-by-choice",
    recordId: "recjjSnY2opb8P4DG",
    name: "Radisson Individuals by Choice",
  },
  // Wave 2 — remaining Active/Live missing hero trust row
  { slug: "autograph-collection", recordId: "recEJCTDj1zrsjPM6", name: "Autograph Collection" },
  { slug: "handwritten-collection", recordId: "rec7hTXwMRC81EPqz", name: "Handwritten Collection" },
  { slug: "vignette-collection", recordId: "recDwzv86TWnz2gGB", name: "Vignette Collection" },
  { slug: "suburban-studios", recordId: "reclcjg5Foa9Vs5TC", name: "Suburban Studios" },
  { slug: "woodspring-suites", recordId: "recsOd51NzRPYsMko", name: "WoodSpring Suites" },
  { slug: "bw-premier-collection", recordId: "recwXZ5gVZ8ZH8ekA", name: "BW Premier Collection" },
  {
    slug: "bw-signature-collection",
    recordId: "recdeh1NsP4gjrv80",
    name: "BW Signature Collection",
  },
  {
    slug: "preferred-hotels-and-resorts",
    recordId: "recwl5JOYxlChuCAr",
    name: "Preferred Hotels & Resorts",
  },
];

/** Matches Kimpton-style external chip; never sets Company Validated. */
const PROPOSED = {
  validationStatus: "Company Published",
  usagePermission: "Platform Display Allowed",
  // Must match live Brand Basics select options (see Tribute / Ascend). "Brand Page" is PI-only — not a Basics option.
  sourceType: "Company Website",
  sourceRegion: "Global Reference",
  confidenceLevel: "Medium",
  externalDisplayStatus: "Show Trust Label",
  lastReviewedDate: "2026-07-23",
  companyValidated: false,
  evidenceNotes:
    "Explorer trust-chip backfill — Brand Basics P1 governance. Not company validated.",
  missingDataFlags: "Company validation not yet completed.",
  internalNotes:
    "Trust-chip gap backfill (Active/Live wave). Never Company Validated via this path.",
};

const NEVER_WRITE_API_KEYS = new Set(["companyValidationDate"]);
const BRAND_TABLE = "Brand Setup - Brand Basics";
const APPLY = process.argv.includes("--apply");
const APPROVE = process.argv.includes("--approve-trust-chip-basics-backfill");
const DRY_RUN = !APPLY;

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

function readVal(fields, col) {
  const raw = fields?.[col];
  if (raw == null || raw === "") return null;
  if (typeof raw === "object" && raw.name) return String(raw.name).trim() || null;
  return raw;
}

function brandIdsOnRecord(rec) {
  return []
    .concat(rec.brandId || [], rec.brandRecordId || [], rec.linkedBrandIds || [])
    .filter(Boolean)
    .map(String);
}

async function fetchAllRecords(base, tableName) {
  const records = [];
  await new Promise((resolve, reject) => {
    base(tableName)
      .select({ pageSize: 100 })
      .eachPage(
        (page, next) => {
          records.push(...page);
          next();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });
  return records;
}

function buildMarkdown(report) {
  const lines = [
    "# Brand Explorer Trust Chip — Brand Basics Backfill",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Impact: **High** — Brand Setup - Brand Basics P1 governance`,
    "",
    "## Proposed external chip (cohort)",
    "",
    "- **displayLabel:** AI-Assisted Profile",
    "- **displaySubtitle:** Last Reviewed: Jul 23, 2026 · Source Basis: Company Materials · Region: Global Reference",
    "- Internal Validation Status: `Company Published` (not company validated)",
    "- Never writes: Company Validated = true, Company Validation Date",
    "",
    "## Summary",
    "",
    `| Brands | ${report.summary.total} |`,
    `| Applied | ${report.summary.applied} |`,
    `| Skipped | ${report.summary.skipped} |`,
    `| Failed | ${report.summary.failed} |`,
    "",
    "## Per-brand",
    "",
  ];

  for (const b of report.brands) {
    lines.push(`### ${b.name} (\`${b.slug}\`)`, "");
    lines.push(`- Record: \`${b.recordId}\``);
    lines.push(`- Write status: **${b.write?.status || "planned"}**`);
    if (b.write?.reason) lines.push(`- Reason: ${b.write.reason}`);
    if (b.write?.error) lines.push(`- Error: ${b.write.error}`);
    lines.push(`- Protected: ${b.protected ? "yes" : "no"}`);
    lines.push(
      `- PI sources: ${b.pi.sourceCount} · approved facts: ${b.pi.approvedFacts}/${b.pi.factCount}`
    );
    lines.push(
      `- Expected chip: **${b.expectedChip.displayLabel || "(none)"}** — ${b.expectedChip.displaySubtitle || ""}`
    );
    if (b.verifiedChip) {
      lines.push(
        `- Verified chip after write: **${b.verifiedChip.displayLabel || "(none)"}** — ${b.verifiedChip.displaySubtitle || ""}`
      );
    }
    lines.push(`- Fields in patch: **${b.fieldDiff.length}**`, "");
    if (b.fieldDiff.length) {
      lines.push("| Airtable field | From | To |");
      lines.push("|----------------|------|-----|");
      for (const d of b.fieldDiff) {
        const from = d.from == null ? "*(blank)*" : JSON.stringify(d.from);
        lines.push(`| \`${d.airtableField}\` | ${from} | ${JSON.stringify(d.to)} |`);
      }
      lines.push("");
    }
  }

  lines.push(
    "## Rollback",
    "",
    "Clear or revert the P1 governance columns on Brand Basics for the listed rec IDs, or set `External Display Status` = `Hide Trust Label`.",
    "",
    "## Regression",
    "",
    "- Retest Brand Explorer hero for: design-hotels, hotel-indigo, mgallery-collection, radisson-individuals-by-choice",
    "- Confirm Kimpton / Tribute chips unchanged",
    "- Confirm no Company Validated checkbox flipped on",
    ""
  );

  return lines.join("\n");
}

async function main() {
  if (APPLY && !APPROVE) {
    throw new Error(
      "Refusing apply without --approve-trust-chip-basics-backfill. Dry-run first, then re-run with both flags."
    );
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");

  const brandsArg = argValue("--brands");
  const filterSlugs = brandsArg
    ? brandsArg.split(",").map((s) => s.trim()).filter(Boolean)
    : null;
  const targets = filterSlugs
    ? ALL_TARGETS.filter((t) => filterSlugs.includes(t.slug))
    : ALL_TARGETS;
  if (!targets.length) throw new Error(`No targets matched --brands ${brandsArg}`);

  const base = new Airtable({ apiKey }).base(baseId);
  const [sourceRaw, factRaw] = await Promise.all([
    fetchAllRecords(
      base,
      process.env.PARTNER_INTELLIGENCE_SOURCE_TABLE_ID || PARTNER_INTELLIGENCE_TABLES.sourceLibrary
    ),
    fetchAllRecords(
      base,
      process.env.PARTNER_INTELLIGENCE_FACTS_TABLE_ID || PARTNER_INTELLIGENCE_TABLES.extractedFacts
    ),
  ]);
  const allSources = sourceRaw.map(normalizePartnerSourceRecord);
  const allFacts = factRaw.map(normalizePartnerFactRecord);

  const brands = [];
  let applied = 0;
  let skipped = 0;
  let failed = 0;

  for (const t of targets) {
    const rec = await base(BRAND_TABLE).find(t.recordId);
    const fields = rec.fields || {};
    const current = normalizeProfileGovernance(fields, {
      entityType: "brand",
      sourceTable: BRAND_TABLE,
    });

    const patch = {};
    const mapping = {};
    const diffs = [];
    for (const [apiKeyName, value] of Object.entries(PROPOSED)) {
      if (NEVER_WRITE_API_KEYS.has(apiKeyName)) continue;
      if (apiKeyName === "companyValidated" && value !== false) {
        throw new Error("Safety: companyValidated may only be written as false");
      }
      const col = MAP_PROFILE_GOVERNANCE_AIRTABLE[apiKeyName];
      if (!col) continue;
      mapping[apiKeyName] = col;
      const from = readVal(fields, col);
      if (JSON.stringify(from) !== JSON.stringify(value)) {
        patch[col] = value;
        diffs.push({ apiKey: apiKeyName, airtableField: col, from, to: value });
      }
    }

    // Never allow Company Validated true or Company Validation Date in patch
    const companyValidatedCol = MAP_PROFILE_GOVERNANCE_AIRTABLE.companyValidated;
    const companyValidationDateCol = MAP_PROFILE_GOVERNANCE_AIRTABLE.companyValidationDate;
    if (patch[companyValidatedCol] === true) {
      throw new Error("Safety: refused patch with Company Validated = true");
    }
    if (Object.prototype.hasOwnProperty.call(patch, companyValidationDateCol)) {
      delete patch[companyValidationDateCol];
    }

    const expected = normalizeProfileGovernance(
      { ...fields, ...patch },
      { entityType: "brand", sourceTable: BRAND_TABLE }
    );

    const sourcesForBrand = allSources.filter((s) =>
      brandIdsOnRecord(s).includes(t.recordId)
    );
    const factsForBrand = allFacts.filter((f) => brandIdsOnRecord(f).includes(t.recordId));

    const protectedRecord =
      Boolean(current.companyValidated) ||
      current.validationStatus === "Company Validated" ||
      current.validationStatus === "Do Not Use";

    const entry = {
      slug: t.slug,
      name: t.name,
      recordId: t.recordId,
      protected: protectedRecord,
      currentChip: {
        displayLabel: current.displayLabel,
        displaySubtitle: current.displaySubtitle,
        warnings: current.internalWarnings,
      },
      pi: {
        sourceCount: sourcesForBrand.length,
        sources: sourcesForBrand.map((s) => ({
          id: s.id,
          title: s.sourceTitle,
          status: s.status,
          approvedForExplorerUse: s.approvedForExplorerUse,
          origin: s.sourceOrigin,
        })),
        factCount: factsForBrand.length,
        approvedFacts: factsForBrand.filter((f) =>
          /^(Approved|Edited)$/i.test(String(f.humanReviewStatus || ""))
        ).length,
      },
      validation: {
        pass: diffs.length > 0 && !protectedRecord,
        failedChecks: protectedRecord
          ? ["company_validated_or_do_not_use_protected"]
          : diffs.length
            ? []
            : ["no_changes"],
      },
      fieldMapping: mapping,
      sanitizedPayloadPreview: patch,
      fieldDiff: diffs,
      expectedChip: {
        displayLabel: expected.displayLabel,
        displaySubtitle: expected.displaySubtitle,
      },
      errorHandling: {
        validationError: "Do not PATCH if protected (Company Validated / Do Not Use).",
        apiError: "Surface Airtable error; do not retry blindly.",
        networkError: "Retry once; leave Basics unchanged on failure.",
        userFacing: "Trust label unavailable until governance is set.",
      },
      write: null,
      verifiedChip: null,
    };

    if (protectedRecord) {
      entry.write = { status: "skipped", reason: "protected_fields" };
      skipped += 1;
    } else if (!diffs.length) {
      entry.write = { status: "skipped", reason: "no_changes" };
      skipped += 1;
    } else if (!expected.displayLabel) {
      entry.write = {
        status: "skipped",
        reason: "expected_chip_missing_after_patch",
      };
      skipped += 1;
    } else if (DRY_RUN) {
      entry.write = { status: "dry_run", patch };
    } else {
      try {
        await base(BRAND_TABLE).update(t.recordId, patch);
        const after = await base(BRAND_TABLE).find(t.recordId);
        const verified = normalizeProfileGovernance(after.fields || {}, {
          entityType: "brand",
          sourceTable: BRAND_TABLE,
        });
        entry.verifiedChip = {
          displayLabel: verified.displayLabel,
          displaySubtitle: verified.displaySubtitle,
          validationStatus: verified.validationStatus,
          externalDisplayStatus: verified.externalDisplayStatus,
          companyValidated: verified.companyValidated,
        };
        if (verified.companyValidated === true) {
          throw new Error("Post-write safety fail: Company Validated became true");
        }
        if (!verified.displayLabel) {
          throw new Error("Post-write safety fail: displayLabel still blank");
        }
        entry.write = { status: "applied", patch };
        applied += 1;
        console.log(`[trust-chip-basics] applied ${t.slug} → ${verified.displayLabel}`);
      } catch (err) {
        entry.write = {
          status: "failed",
          error: err?.message || String(err),
          patch,
        };
        failed += 1;
        console.error(`[trust-chip-basics] failed ${t.slug}:`, err?.message || err);
      }
    }

    brands.push(entry);
  }

  const mode = DRY_RUN ? "dry-run" : "apply";
  const report = {
    generatedAt: new Date().toISOString(),
    mode,
    impact: "High — Brand Basics governance writes",
    baseId,
    cohort: brands.map((b) => b.slug),
    proposedValues: PROPOSED,
    summary: {
      total: brands.length,
      applied: DRY_RUN ? 0 : applied,
      skipped,
      failed,
      dryRunPlanned: DRY_RUN ? brands.filter((b) => b.write?.status === "dry_run").length : 0,
    },
    brands,
    applyGate:
      "Requires --apply --approve-trust-chip-basics-backfill. Never sets Company Validated.",
  };

  const outJson = DRY_RUN ? REPORT_JSON : APPLY_REPORT_JSON;
  const outMd = DRY_RUN ? REPORT_MD : APPLY_REPORT_MD;
  mkdirSync(dirname(outJson), { recursive: true });
  writeFileSync(outJson, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(outMd, buildMarkdown(report), "utf8");

  console.log(
    `mode=${mode} total=${report.summary.total} applied=${report.summary.applied} skipped=${skipped} failed=${failed}`
  );
  for (const b of brands) {
    console.log(
      `${b.slug} | ${b.write?.status} | patchFields ${b.fieldDiff.length} | chip ${b.verifiedChip?.displayLabel || b.expectedChip.displayLabel}`
    );
  }
  console.log(`Wrote ${outMd}`);
  console.log(`Wrote ${outJson}`);

  if (failed > 0) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
