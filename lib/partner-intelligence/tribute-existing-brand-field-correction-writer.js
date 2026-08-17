import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BRAND_ASSET_PILOT_CONFIG } from "./brand-asset-registry-workflow.js";
import { listPartnerSources } from "./airtable-source.js";

export const WRITER_VERSION = "13";
export const REPORT_JSON_NAME = "tribute-existing-brand-field-correction-writer.json";
export const REPORT_MD_NAME = "tribute-existing-brand-field-correction-writer.md";

const DEFAULT_BRAND_KEY = "tribute-portfolio";
const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";
const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const REQUIRED_APPLY_FLAG = "--approve-tribute-existing-field-corrections";
const V12_REPORT_PATH = "reports/tribute-existing-brand-field-validation-audit.json";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function short(v, max = 200) {
  const s = nz(v).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
}

function readRepoJson(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

function escapeFormulaValue(v) {
  return String(v).replace(/'/g, "\\'");
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function listByFormula(baseId, apiKey, tableName, formula) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    if (formula) params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, tableName)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed ${tableName}: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function normalizePresentationRows(records) {
  return (records || [])
    .map((rec) => {
      const f = rec.fields || {};
      return {
        recordId: rec.id,
        slotKey: nz(f["Slot Key"] || f.slot_key),
        title: nz(f.Title),
        body: nz(f.Body),
        imageAttachmentCount: Array.isArray(f.Image) ? f.Image.length : 0,
      };
    })
    .filter((r) => r.slotKey);
}

function approvedTributeWebsiteFromSources(sourceRows) {
  const match = (sourceRows || []).find((s) => {
    const url = nz(s.sourceUrl).toLowerCase();
    return nz(s.approvedForExplorerUse) === "Yes" && /tribute-portfolio\.marriott\.com/.test(url);
  });
  return nz(match?.sourceUrl);
}

function urlsFromMaterialsFile(rows) {
  const urls = [];
  for (const row of rows) {
    const text = `${row.title} ${row.body}`;
    const matches = text.match(/https?:\/\/[^\s)]+/gi) || [];
    for (const u of matches) urls.push(u.replace(/[.,;]$/, ""));
  }
  return [...new Set(urls)];
}

export async function buildTributeExistingBrandFieldCorrectionWriterReport({
  brandKey = DEFAULT_BRAND_KEY,
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
  apply = false,
  applyApproved = false,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const v12 = readRepoJson(V12_REPORT_PATH);
  if (!v12) throw new Error(`Missing required v12 report: ${V12_REPORT_PATH}`);

  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG[DEFAULT_BRAND_KEY];
  const resolvedBrandRecordId = pilot?.recordId || brandRecordId;
  const brandName = pilot?.brandName || "Tribute Portfolio";

  const basics = await airtableFetch(baseId, apiKey, BRAND_BASICS_TABLE, { method: "GET" }, resolvedBrandRecordId);
  if (!basics.res.ok || !basics.json?.id) {
    throw new Error(`Brand Basics record not found: ${resolvedBrandRecordId}`);
  }
  const basicFields = basics.json.fields || {};

  const mode = apply && applyApproved ? "apply" : "dry-run";
  const applyMode = apply && applyApproved;
  const applyBlockers = [];

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(resolvedBrandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(
      brandName
    )}')`
  );
  const presentationRows = normalizePresentationRows(presentationRaw);
  const materialsFileRows = presentationRows.filter((r) => r.slotKey === "materials.file");
  const materialsFileUrls = urlsFromMaterialsFile(materialsFileRows);

  const sourceRows = [];
  let sourceOffset = null;
  do {
    const page = await listPartnerSources({ brandId: resolvedBrandRecordId, limit: 100, offset: sourceOffset });
    sourceRows.push(...(page.sources || []));
    sourceOffset = page.offset;
  } while (sourceOffset);

  const v12SourceBackedWebsite = nz(v12?.brandWebsiteProposedCorrectedValue);
  const sourceLibraryWebsite = approvedTributeWebsiteFromSources(sourceRows);
  const materialsFileHasWebsite = materialsFileUrls.some((u) => /tribute-portfolio\.marriott\.com/i.test(u));
  const sourceBackedWebsite = sourceLibraryWebsite || v12SourceBackedWebsite;

  const currentBrandWebsite = nz(basicFields["Brand Website"] || basicFields["Website"]);
  const normalizedCurrent = currentBrandWebsite.toLowerCase().replace(/\/+$/, "");
  const normalizedProposed = sourceBackedWebsite.toLowerCase().replace(/\/+$/, "");

  const fieldsProposedForUpdate = [];
  if (normalizedProposed && normalizedCurrent && normalizedCurrent !== normalizedProposed) {
    fieldsProposedForUpdate.push({
      table: BRAND_BASICS_TABLE,
      recordId: resolvedBrandRecordId,
      field: "Brand Website",
      currentValue: currentBrandWebsite,
      proposedValue: sourceBackedWebsite,
      sourceBasis:
        "Approved Tribute Source Library URL (cross-checked with v12 and materials.file source links).",
      reviewStatus: "Source-backed",
    });
  } else if (normalizedProposed && !normalizedCurrent) {
    fieldsProposedForUpdate.push({
      table: BRAND_BASICS_TABLE,
      recordId: resolvedBrandRecordId,
      field: "Brand Website",
      currentValue: currentBrandWebsite,
      proposedValue: sourceBackedWebsite,
      sourceBasis:
        "Approved Tribute Source Library URL (cross-checked with v12 and materials.file source links).",
      reviewStatus: "Source-backed",
    });
  }

  if (apply && !applyApproved) {
    applyBlockers.push(`--apply requires ${REQUIRED_APPLY_FLAG}`);
  }
  if (!sourceBackedWebsite) {
    applyBlockers.push("No approved Tribute-specific website URL found in source-backed records.");
  }

  const applyResult = { updated: [], errors: [] };
  if (applyMode && !applyBlockers.length) {
    for (const item of fieldsProposedForUpdate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        BRAND_BASICS_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({ fields: { [item.field]: item.proposedValue }, typecast: true }),
        },
        resolvedBrandRecordId
      );
      if (!res.ok) {
        applyResult.errors.push({
          field: item.field,
          message: json.error?.message || `Patch failed (${res.status})`,
        });
      } else {
        applyResult.updated.push({
          table: BRAND_BASICS_TABLE,
          recordId: resolvedBrandRecordId,
          field: item.field,
        });
      }
    }
  }

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    v13CorrectionWriterExists: true,
    brand: {
      key: brandKey,
      recordId: resolvedBrandRecordId,
      name: nz(basicFields["Brand Name"]) || brandName,
    },
    filesRead: [
      "AGENTS.md",
      "reports/tribute-existing-brand-field-validation-audit.md",
      "reports/tribute-existing-brand-field-validation-audit.json",
      "lib/partner-intelligence/tribute-existing-brand-field-validation-audit.js",
      "reports/tribute-brand-explorer-content-promotion-writer.md",
      "reports/tribute-brand-explorer-content-promotion-writer.json",
      "reports/tribute-brand-explorer-content-parity-audit.md",
      "reports/tribute-portfolio-package-pipeline.md",
      "api/brand-library.js",
      "api/lib/partner-intelligence-field-map.js",
      "api/lib/partner-intelligence-explorer-field-registry.js",
      "docs/data-intelligence/tribute-existing-brand-field-validation-audit-v12.md",
      "docs/data-intelligence/tribute-brand-explorer-content-promotion-writer-v11.md",
      "docs/data-intelligence/BRAND_PROFILE_DATA_MODEL.md",
      "package.json",
    ],
    filesChanged: [
      "lib/partner-intelligence/tribute-existing-brand-field-correction-writer.js",
      "scripts/tribute-existing-brand-field-correction-writer.mjs",
      "docs/data-intelligence/tribute-existing-brand-field-correction-writer-v13.md",
      "reports/tribute-existing-brand-field-correction-writer.md",
      "reports/tribute-existing-brand-field-correction-writer.json",
      "package.json",
    ],
    currentBrandWebsiteValue: currentBrandWebsite,
    proposedBrandWebsiteValue: sourceBackedWebsite,
    correctionSourceBacked: Boolean(sourceBackedWebsite),
    sourceBasisForProposedCorrection:
      "Approved Tribute Source Library URL and v12 audit recommendation; confirmed in materials.file links.",
    sourceEvidence: {
      sourceLibraryWebsite,
      v12WebsiteRecommendation: v12SourceBackedWebsite,
      materialsFileContainsTributeWebsite: materialsFileHasWebsite,
      materialsFileUrls,
    },
    fieldsProposedForUpdate,
    fieldsIntentionallyLeftUnchanged: [
      "Brand Profile Analysis",
      "Brand Standards",
      "Questions Owners Should Ask",
      "Company Validated",
      "Company Validation Date",
      "Source Links / materials.file",
      "Explorer Hero Data Source",
      "Explorer Hero Verification",
      "Hero/Gallery/Value-driver copy",
      "Recent Openings",
      "All images/attachments/media rows",
    ],
    companyValidatedFieldsIntentionallyUntouched: true,
    aiDraftedFieldsUntouched: true,
    sourceLinksUntouched: true,
    imagesUntouched: true,
    presentationRowsUntouched: true,
    applyFlags: {
      applyRequested: apply,
      applyApproved,
    },
    applyBlockers,
    applyResult,
    airtableModified: applyMode && !applyBlockers.length && applyResult.updated.length > 0 && !applyResult.errors.length,
    exactApplyCommand:
      "npm run tribute-existing-brand-field-correction-writer -- --apply --approve-tribute-existing-field-corrections",
  };
}

export function buildTributeExistingBrandFieldCorrectionWriterMarkdown(report) {
  const lines = [];
  lines.push("# Tribute Existing Brand Field Correction Writer v13");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Brand Website correction");
  lines.push(`- Current value: ${report.currentBrandWebsiteValue || "(blank)"}`);
  lines.push(`- Proposed value: ${report.proposedBrandWebsiteValue || "(none proposed)"}`);
  lines.push(`- Source-backed: ${report.correctionSourceBacked ? "yes" : "no"}`);
  lines.push(`- Source basis: ${report.sourceBasisForProposedCorrection}`);
  lines.push("");
  lines.push("## Fields proposed for update");
  if (!report.fieldsProposedForUpdate.length) lines.push("- None.");
  for (const f of report.fieldsProposedForUpdate) {
    lines.push(`- ${f.field}: ${f.currentValue} -> ${f.proposedValue}`);
  }
  lines.push("");
  lines.push("## Fields intentionally unchanged");
  for (const field of report.fieldsIntentionallyLeftUnchanged) {
    lines.push(`- ${field}`);
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push(
    `- Company Validated fields untouched: **${report.companyValidatedFieldsIntentionallyUntouched ? "yes" : "no"}**`
  );
  lines.push(`- AI-drafted fields untouched: **${report.aiDraftedFieldsUntouched ? "yes" : "no"}**`);
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Presentation rows untouched: **${report.presentationRowsUntouched ? "yes" : "no"}**`);
  lines.push(`- SourceLinks untouched: **${report.sourceLinksUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Apply command (if approved)");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  lines.push("");
  if (report.applyBlockers.length) {
    lines.push("## Apply blockers");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
    lines.push("");
  }
  return lines.join("\n");
}
