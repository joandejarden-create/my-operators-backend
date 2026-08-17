/**
 * Tab Factory remediation — Presentation writes only.
 * Applies full tab packs (ops + lifecycle + flexibility + opening path + …).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import { auditBrandTabFactory, writeTabFactoryAuditReports, runTabFactoryAudit } from "./brand-explorer-tab-factory-audit.js";
import {
  getTabFactoryRemediationPack,
  TAB_FACTORY_REMEDIATION_CONTENT,
  TAB_FACTORY_TARGET_BRANDS,
} from "./brand-explorer-tab-factory-remediation-content.js";
import { TAB_FACTORY_PROTECTED_BRANDS } from "./brand-explorer-tab-contracts.js";

export { TAB_FACTORY_TARGET_BRANDS, TAB_FACTORY_PROTECTED_BRANDS };
export const REMEDIATION_VERSION = "tab-factory-remediation-v1";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-tab-factory-remediation",
  "--confirm-no-company-validation-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-protected-brands-unchanged",
  "--confirm-no-empty-rendered-fields",
  "--confirm-source-provenance-by-tab",
  "--confirm-brand-specific-copy",
  "--confirm-benchmark-quality-met",
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

function normalizeSlotKey(slotKey) {
  // insight.similar.1 → insight.similar (atelier uses multi-row same slot)
  if (/^insight\.similar\.\d+$/i.test(slotKey)) return "insight.similar";
  return slotKey;
}

function findSlot(rows, slotKey, { title = null } = {}) {
  const key = normalizeSlotKey(slotKey);
  const list = (rows || []).filter(
    (r) =>
      nz(r.slotKey) === key &&
      r.active !== false &&
      !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );
  if (title != null && nz(title)) {
    const byTitle = list.find((r) => nz(r.title) === nz(title));
    if (byTitle) return byTitle;
  }
  return list[0] || null;
}

export function parseTabFactoryApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

export async function planTabFactoryRemediation(brandSlug) {
  if (!TAB_FACTORY_TARGET_BRANDS.includes(brandSlug)) {
    throw new Error(`Targets only: ${TAB_FACTORY_TARGET_BRANDS.join(", ")}`);
  }
  if (TAB_FACTORY_PROTECTED_BRANDS.includes(brandSlug)) {
    throw new Error(`Refuse protected brand ${brandSlug}`);
  }
  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig) throw new Error(`No brand config for ${brandSlug}`);
  const pack = getTabFactoryRemediationPack(brandSlug) || TAB_FACTORY_REMEDIATION_CONTENT[brandSlug];
  if (!pack) throw new Error(`No tab-factory remediation content for ${brandSlug}`);

  const audit = await auditBrandTabFactory(brandSlug);
  const ctx = await loadBrandFactoryContext(brandSlug);
  const rows = ctx.presentationRows || [];
  const patches = [];
  const blockers = [];

  for (const row of pack.presentation || []) {
    const slotKey = normalizeSlotKey(row.slotKey);
    const body = nz(row.body);
    const title = nz(row.title);
    const existing = findSlot(rows, slotKey, { title: title || null });
    const forbidden = scanForbiddenLanguage(`${title}\n${body}`);
    if (forbidden.length) {
      blockers.push(`forbidden:${slotKey}:${forbidden.map((h) => h.id).join(",")}`);
      continue;
    }
    if (existing?.recordId && nz(existing.body) === body && nz(existing.title) === title) {
      const caseOv = nz(row.caseSummaryOverview);
      if (!caseOv || nz(existing.caseSummaryOverview) === caseOv) continue;
    }
    const fields = {
      Body: body,
      ...(title ? { Title: title } : {}),
      ...(nz(row.caseSummaryOverview) ? { "Case Summary Overview": nz(row.caseSummaryOverview) } : {}),
    };
    if (existing?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: existing.recordId,
        slotKey,
        fields,
        before: { body: existing.body, title: existing.title, caseSummaryOverview: existing.caseSummaryOverview },
        after: { body, title, caseSummaryOverview: row.caseSummaryOverview || null },
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey,
        fields: {
          "Slot Key": slotKey,
          "Brand Name": brandConfig.name,
          Brand: [brandConfig.recordId],
          Active: true,
          "Sort Order": row.sortOrder ?? 0,
          Title: title || "",
          Body: body,
          ...(nz(row.caseSummaryOverview) ? { "Case Summary Overview": nz(row.caseSummaryOverview) } : {}),
        },
        before: null,
        after: { body, title, caseSummaryOverview: row.caseSummaryOverview || null },
      });
    }
  }

  return {
    brandSlug,
    brandName: brandConfig.name,
    recordId: brandConfig.recordId,
    auditPass: audit.auditPass,
    failFindings: audit.failFindings,
    releaseQualityDecision: audit.releaseQualityDecision,
    gates: audit.gates,
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

export async function applyTabFactoryRemediation({ brandResults, apply = false, argv = [] } = {}) {
  const flagCheck = parseTabFactoryApplyFlags(argv);
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
    if (TAB_FACTORY_PROTECTED_BRANDS.includes(brand.brandSlug)) {
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
            table: PRESENTATION_TABLE,
            fields: patch.fields,
            method: "POST",
          });
          created.push(json.id);
        } else {
          await airtableWrite({
            baseId,
            apiKey,
            table: PRESENTATION_TABLE,
            recordId: patch.recordId,
            fields: patch.fields,
            method: "PATCH",
          });
          updated.push(patch.recordId);
        }
      } catch (err) {
        errors.push({ slotKey: patch.slotKey, error: err.message });
      }
    }
    resultsByBrand[brand.brandSlug] = {
      applied: errors.length === 0,
      created: created.length,
      updated: updated.length,
      errors,
    };
  }
  return { applied: true, resultsByBrand, flagCheck };
}

export async function runTabFactoryRemediation({
  brands = TAB_FACTORY_TARGET_BRANDS,
  apply = false,
  argv = [],
} = {}) {
  const brandResults = [];
  for (const slug of brands) {
    brandResults.push(await planTabFactoryRemediation(slug));
  }
  const applyResult = await applyTabFactoryRemediation({ brandResults, apply, argv });

  const report = {
    version: REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    brands,
    brandResults,
    applyResult,
    summary: {
      patches: brandResults.reduce((n, b) => n + b.patches.length, 0),
      blocked: brandResults.filter((b) => b.blocked).length,
      writes: apply === true && applyResult.applied === true,
    },
  };

  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "brand-explorer-tab-factory-remediation.json");
  const mdPath = path.join(reportsDir, "brand-explorer-tab-factory-remediation.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const md = [
    `# Tab Factory Remediation`,
    "",
    `Generated: ${report.generatedAt}`,
    `dryRun: **${report.dryRun}**`,
    `patches: **${report.summary.patches}** · blocked: **${report.summary.blocked}** · writes: **${report.summary.writes}**`,
    "",
  ];
  for (const b of brandResults) {
    md.push(`### ${b.brandSlug}`);
    md.push(`- patches: ${b.patches.length} · blocked: ${b.blocked}`);
    md.push(`- prior auditPass: ${b.auditPass} · failFindings: ${b.failFindings}`);
    md.push("");
  }
  fs.writeFileSync(mdPath, md.join("\n"));

  // Also refresh audit reports after planning
  try {
    const audit = await runTabFactoryAudit({ brands, includeBenchmarks: false });
    writeTabFactoryAuditReports(audit);
  } catch (err) {
    console.error("[tab-factory-remediation] post-plan audit refresh failed:", err.message);
  }

  return { report, jsonPath, mdPath };
}
