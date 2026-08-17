/**
 * Lane 3 — Everhome targeted active remediation.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import {
  planPvqlFailureScrubForBrand,
  extractPvqlFieldOffenders,
} from "./brand-explorer-pvql-failure-scrub.js";
import {
  MAP_PRESENTATION_FIELDS,
  PRESENTATION_TABLE,
} from "./brand-explorer-residual-owner-copy-remediation.js";

export const EVERHOME_REMEDIATION_VERSION = "everhome-active-remediation-v1";
export const EVERHOME_RECORD_ID = "recqkkrsevi4r9ibj";
export const EVERHOME_SLUG = "everhome-suites";

export const EVERHOME_REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-everhome-active-remediation",
  "--confirm-targeted-field-fixes-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-broad-rewrites",
]);

const ALLOWED = new Set([
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Brand Relevance",
  "Case Summary Owner Objective",
  "Case Summary Interpretation",
  "Case Summary Tags",
]);

const AIRTABLE_TO_API = Object.fromEntries(
  Object.entries(MAP_PRESENTATION_FIELDS).map(([api, at]) => [at, api])
);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function mockRes() {
  return {
    statusCode: 200,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
}

export async function planEverhomeActiveRemediation() {
  const res = mockRes();
  await getBrandLibraryBrandById(
    { query: { brandId: EVERHOME_RECORD_ID }, headers: {} },
    res
  );
  const brand = res.payload?.brand;
  if (!brand) throw new Error("Everhome brand fetch failed");

  const scrub = planPvqlFailureScrubForBrand(brand, EVERHOME_SLUG, { force: true });
  const patches = (scrub.patches || []).filter((p) =>
    Object.keys(p.fields || {}).every((k) => ALLOWED.has(k))
  );

  const projectedBlocks = (brand.brandExplorer?.blocks || []).map((b) => {
    const next = { ...b };
    for (const p of patches) {
      if (p.recordId !== b.recordId) continue;
      for (const [airtableKey, value] of Object.entries(p.fields || {})) {
        const apiKey = AIRTABLE_TO_API[airtableKey];
        if (apiKey) next[apiKey] = value;
      }
    }
    return next;
  });
  const remaining = extractPvqlFieldOffenders(
    { ...brand, brandExplorer: { blocks: projectedBlocks } },
    EVERHOME_SLUG
  );

  return {
    version: EVERHOME_REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    brandSlug: EVERHOME_SLUG,
    brandName: brand.name,
    recordId: brand.id,
    publicFullBefore: brand.shouldRenderFullProfile === true,
    displayStateBefore: brand.brandExplorerDisplayState || null,
    blockersBefore: brand.brandExplorerDisplayBlockers || [],
    fieldRows: scrub.fieldRows || [],
    patches,
    summary: {
      offenders: scrub.offenderCount,
      patches: patches.length,
      remainingAfterProjection: remaining.length,
    },
    validation: {
      pass: remaining.length === 0,
      failedChecks:
        remaining.length === 0
          ? []
          : remaining.slice(0, 10).map((r) => `${r.section}:${r.field}:${r.failureType}`),
    },
    remainingSample: remaining.slice(0, 10),
    guardrails: {
      companyValidatedUntouched: true,
      sourceLibraryUntouched: true,
      registryUntouched: true,
      brandStatusUntouched: true,
      broadRewriteForbidden: true,
    },
  };
}

export async function applyEverhomeActiveRemediation({
  report,
  apply = false,
  argv = [],
} = {}) {
  if (!apply) return { applied: false, reason: "dry_run_only", results: [] };
  const missing = EVERHOME_REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  if (missing.length) {
    return { applied: false, reason: "missing_apply_flags", missing, results: [] };
  }
  if (!report?.validation?.pass) {
    return {
      applied: false,
      reason: "validation_failed",
      failedChecks: report?.validation?.failedChecks || [],
      results: [],
    };
  }
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE credentials required");

  const results = [];
  for (const patch of report.patches || []) {
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${patch.recordId}`;
    const res = await fetch(url, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: patch.fields }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `PATCH failed ${patch.recordId}`);
    results.push({
      recordId: patch.recordId,
      slotKey: patch.slotKey,
      fields: Object.keys(patch.fields),
      id: json.id,
    });
  }
  return { applied: true, results, companyValidatedUntouched: true };
}

export function writeEverhomeActiveRemediationReports(report, applyResult = null) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const out = { ...report, applyResult: applyResult || { applied: false } };
  const jsonPath = path.join(reportsDir, "brand-explorer-everhome-active-remediation.json");
  const mdPath = path.join(reportsDir, "brand-explorer-everhome-active-remediation.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(out, null, 2)}\n`, "utf8");
  const lines = [
    "# Everhome Active Remediation",
    "",
    `Version: \`${report.version}\` · ${report.generatedAt}`,
    `Applied: **${applyResult?.applied === true}**`,
    "",
    `| Field | Value |`,
    `| --- | --- |`,
    `| Record ID | \`${report.recordId}\` |`,
    `| Public full before | ${report.publicFullBefore} |`,
    `| Display before | ${report.displayStateBefore} |`,
    `| Offenders | ${report.summary.offenders} |`,
    `| Patches | ${report.summary.patches} |`,
    `| Remaining | ${report.summary.remainingAfterProjection} |`,
    "",
    "## Field rows",
    "",
    "```json",
    JSON.stringify(report.fieldRows || [], null, 2),
    "```",
    "",
  ];
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");
  return { jsonPath, mdPath };
}
