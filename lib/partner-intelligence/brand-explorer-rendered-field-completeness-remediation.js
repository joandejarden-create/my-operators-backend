/**
 * Apply rendered field-completeness remediation patches (Presentation only).
 * Keep brands active_profile_ready; no release / Company Validated / Source / Registry writes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import {
  auditBrandRenderedFieldCompleteness,
  writeRenderedFieldCompletenessReports,
  runRenderedFieldCompletenessAudit,
} from "./brand-explorer-rendered-field-completeness-audit.js";
import { RENDERED_FIELD_REMEDIATION_CONTENT } from "./brand-explorer-rendered-field-completeness-remediation-content.js";
import {
  PROTECTED_BRANDS,
  TARGET_BRANDS,
} from "./brand-explorer-rendered-field-completeness-inventory.js";

export { TARGET_BRANDS, PROTECTED_BRANDS };
export const REMEDIATION_VERSION = "rendered-field-completeness-remediation";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-rendered-field-completeness-remediation",
  "--confirm-keep-active-profile-ready",
  "--confirm-no-company-validation-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-protected-brands-unchanged",
  "--confirm-no-visible-empty-fields",
  "--confirm-no-unsupported-metrics",
  "--confirm-no-empty-bullets",
  "--confirm-no-blank-cards",
  "--confirm-no-duplicate-scenario-images",
  "--confirm-brand-specific-copy",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function findSlot(rows, slotKey) {
  return (rows || []).find(
    (r) =>
      nz(r.slotKey) === slotKey &&
      r.active !== false &&
      !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );
}

export function parseRemediationApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

export async function planRenderedFieldRemediation(brandSlug) {
  if (!TARGET_BRANDS.includes(brandSlug)) {
    throw new Error(`Targets only: ${TARGET_BRANDS.join(", ")}`);
  }
  if (PROTECTED_BRANDS.includes(brandSlug)) {
    throw new Error(`Refuse protected brand ${brandSlug}`);
  }
  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig) throw new Error(`No brand config for ${brandSlug}`);
  const pack = RENDERED_FIELD_REMEDIATION_CONTENT[brandSlug];
  if (!pack) throw new Error(`No remediation content for ${brandSlug}`);

  const audit = await auditBrandRenderedFieldCompleteness(brandSlug);
  const ctx = await loadBrandFactoryContext(brandSlug);
  const rows = ctx.presentationRows || [];
  const patches = [];
  const blockers = [];

  if (audit.liveState.displayState !== "active_profile_ready") {
    blockers.push("must_remain_active_profile_ready");
  }

  for (const row of pack.presentation || []) {
    const existing = findSlot(rows, row.slotKey);
    const body = nz(row.body);
    const forbidden = scanForbiddenLanguage(body);
    if (forbidden.length) {
      blockers.push(`forbidden:${row.slotKey}:${forbidden.map((h) => h.id).join(",")}`);
      continue;
    }
    if (existing?.recordId && nz(existing.body) === body) continue;
    if (existing?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: existing.recordId,
        slotKey: row.slotKey,
        fields: { Body: body, ...(row.title != null ? { Title: row.title } : {}) },
        before: { body: existing.body },
        after: { body },
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey: row.slotKey,
        fields: {
          "Slot Key": row.slotKey,
          "Brand Name": brandConfig.name,
          Brand: [brandConfig.recordId],
          Active: true,
          "Sort Order": row.sortOrder ?? 0,
          Title: row.title || "",
          Body: body,
        },
        before: null,
        after: { body },
      });
    }
  }

  return {
    brandSlug,
    brandName: brandConfig.name,
    recordId: brandConfig.recordId,
    auditSummary: audit.summary,
    releaseQualityDecision: audit.releaseQualityDecision,
    patches,
    blockers,
    blocked: blockers.length > 0,
  };
}

async function airtableWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `${method} ${table} failed: ${res.status}`);
  return json;
}

export async function applyRenderedFieldRemediation({ brandResults, apply = false, argv = [] } = {}) {
  const flagCheck = parseRemediationApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  }
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of brandResults) {
    if (brand.blocked) {
      resultsByBrand[brand.brandSlug] = { applied: false, reason: "blocked", blockers: brand.blockers };
      continue;
    }
    if (PROTECTED_BRANDS.includes(brand.brandSlug)) {
      throw new Error(`Refuse protected brand write ${brand.brandSlug}`);
    }
    const created = [];
    const updated = [];
    const errors = [];
    for (const patch of brand.patches) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_WRITE_FIELDS.has(key)) throw new Error(`Forbidden field write: ${key}`);
      }
      try {
        if (patch.action === "POST") {
          const json = await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            fields: patch.fields,
            method: "POST",
          });
          created.push({ recordId: json.id, slotKey: patch.slotKey });
        } else {
          await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            recordId: patch.recordId,
            fields: patch.fields,
            method: "PATCH",
          });
          updated.push({ recordId: patch.recordId, slotKey: patch.slotKey });
        }
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        errors.push({ slotKey: patch.slotKey, message: err.message });
      }
    }
    resultsByBrand[brand.brandSlug] = {
      applied: errors.length === 0,
      created,
      updated,
      errors,
    };
  }
  return { applied: true, resultsByBrand, flagCheck };
}

export async function runRenderedFieldCompletenessRemediation({
  brands = TARGET_BRANDS,
  dryRun = true,
  argv = [],
} = {}) {
  for (const b of brands) {
    if (!TARGET_BRANDS.includes(b)) throw new Error(`Targets only: ${TARGET_BRANDS.join(", ")}`);
    if (PROTECTED_BRANDS.includes(b)) throw new Error(`Refuse protected ${b}`);
  }
  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await planRenderedFieldRemediation(brandSlug));
  }
  const applyResult = dryRun
    ? { applied: false, reason: "dry_run_only" }
    : await applyRenderedFieldRemediation({ brandResults, apply: true, argv });

  return {
    version: REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    brands,
    brandResults,
    applyResult,
    summary: {
      totalPatches: brandResults.reduce((n, b) => n + b.patches.length, 0),
      blockedBrands: brandResults.filter((b) => b.blocked).length,
      writes: dryRun ? false : applyResult.applied === true,
    },
  };
}

export function writeRemediationPlanReports(report) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "brand-explorer-rendered-field-completeness-remediation.json");
  const mdPath = path.join(reportsDir, "brand-explorer-rendered-field-completeness-remediation.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const md = [
    `# Rendered Field Completeness Remediation`,
    "",
    `Generated: ${report.generatedAt}`,
    `dryRun=${report.dryRun}`,
    "",
    `- Patches: **${report.summary.totalPatches}**`,
    `- Blocked: **${report.summary.blockedBrands}**`,
    `- Writes: **${report.summary.writes}**`,
    "",
  ];
  for (const b of report.brandResults) {
    md.push(`### ${b.brandSlug}`);
    md.push(`- Decision: ${b.releaseQualityDecision}`);
    md.push(`- Patches: ${b.patches.length}`);
    md.push(`- Blocked: ${b.blocked}`);
    md.push("");
  }
  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath };
}

export { runRenderedFieldCompletenessAudit, writeRenderedFieldCompletenessReports };
