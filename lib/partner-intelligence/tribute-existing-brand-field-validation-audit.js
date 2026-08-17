import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BRAND_ASSET_PILOT_CONFIG } from "./brand-asset-registry-workflow.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { listPartnerSources } from "./airtable-source.js";

export const WRITER_VERSION = "12";
export const REPORT_JSON_NAME = "tribute-existing-brand-field-validation-audit.json";
export const REPORT_MD_NAME = "tribute-existing-brand-field-validation-audit.md";

const DEFAULT_BRAND_KEY = "tribute-portfolio";
const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";

const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const LOYALTY_TABLE = "Brand Setup - Loyalty & Commercial";

const REFERENCE_BRANDS = [
  "Radisson Blu by Choice",
  "Radisson by Choice",
  "Kimpton Hotels",
  "Curio Collection by Hilton",
  "Ascend Hotel Collection",
];

const REFERENCE_FIXTURES = [
  "fixtures/brand-explorer-presentation-radisson-blu.example.json",
  "fixtures/brand-explorer-presentation-radisson-choice-overview.json",
  "fixtures/brand-explorer-presentation-kimpton-full.json",
  "fixtures/brand-explorer-presentation-curio-full.json",
  "fixtures/brand-explorer-presentation-ascend-hotel-collection-full.json",
];

const AUDIT_FIELDS = [
  { key: "brandWebsite", label: "Brand Website", basicsField: "Brand Website", required: true },
  { key: "brandName", label: "Brand Name", basicsField: "Brand Name", required: true, factKey: "be.identity.brandName" },
  {
    key: "parentCompany",
    label: "Parent Company",
    basicsField: "Parent Company",
    required: true,
    factKey: "be.identity.parentCompany",
  },
  { key: "brandStatus", label: "Brand Status", basicsField: "Brand Status", required: true },
  { key: "brandDescription", label: "Brand Description", basicsField: "Brand Description" },
  { key: "brandProfileAnalysis", label: "Brand Profile Analysis", presentationSlot: "overview.typical_use_case" },
  { key: "brandStandards", label: "Brand Standards", presentationSlot: "standards.intro" },
  {
    key: "questionsOwnersShouldAsk",
    label: "Questions Owners Should Ask",
    presentationSlot: "standards.questions",
  },
  { key: "brandFamilyCollection", label: "Brand Family / Collection", basicsField: "Brand Architecture" },
  { key: "segmentChainScale", label: "Segment / Chain Scale", basicsField: "Hotel Chain Scale" },
  {
    key: "loyaltyProgramRelationship",
    label: "Loyalty Program / Marriott Bonvoy relationship",
    basicsField: "Brand Tagline",
    factKey: "be.loyalty.programName",
  },
  {
    key: "developmentModel",
    label: "Development Model",
    basicsField: "Brand Model",
    factKey: "be.overview.developmentModel",
  },
  {
    key: "positioningSummary",
    label: "Positioning Summary",
    basicsField: "Brand Positioning",
    factKey: "be.positioning.summary",
  },
  {
    key: "guestPromise",
    label: "Guest Promise",
    basicsField: "Brand Customer Promise",
    factKey: "be.positioning.guestPromise",
  },
  { key: "ownerValueProposition", label: "Owner Value Proposition", basicsField: "Brand Value Proposition" },
  {
    key: "brandStandardsOwnerConsiderations",
    label: "Brand Standards & Owner Considerations",
    basicsField: "Key Brand Differentiators",
  },
  { key: "explorerHeroDataSource", label: "Explorer Hero Data Source", basicsField: "Explorer Hero Data Source" },
  { key: "explorerHeroVerification", label: "Explorer Hero Verification", basicsField: "Explorer Hero Verification" },
  { key: "externalDisplayStatus", label: "External Display Status", basicsField: "External Display Status" },
  { key: "validationStatus", label: "Validation Status", basicsField: "Validation Status" },
  { key: "sourceBasis", label: "Source Basis", basicsField: "Source Type" },
  { key: "companyValidated", label: "Company Validated", basicsField: "Company Validated" },
  { key: "companyValidationDate", label: "Company Validation Date", basicsField: "Company Validation Date" },
  {
    key: "sourceLinksMaterialsFile",
    label: "Source Links / materials.file",
    presentationSlot: "materials.file",
  },
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function short(v, max = 240) {
  const s = nz(v).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
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

function collectSourceLinksFromMaterialsFile(materialRows) {
  const links = [];
  for (const row of materialRows) {
    const text = `${row.title} ${row.body}`;
    const matches = text.match(/https?:\/\/[^\s)]+/gi) || [];
    for (const url of matches) links.push(url.replace(/[.,;]$/, ""));
  }
  return [...new Set(links)];
}

function mergedPresentationSlotValue(rows, slotKey) {
  const scopedRows = (rows || []).filter((r) => r.slotKey === slotKey);
  if (!scopedRows.length) return "";
  return scopedRows
    .map((r) => `${nz(r.title)} ${nz(r.body)}`.trim())
    .filter(Boolean)
    .join(" || ");
}

function classifyField({
  value,
  sourceBacked,
  label,
  required,
  approvedWebsiteUrl,
  isBrandWebsite,
  hasWrongBrandToken,
}) {
  const v = nz(value);
  const lower = v.toLowerCase();
  if (!v) return required ? "missing" : "missing";
  if (/(mock|demo|placeholder|tbd|lorem ipsum)/i.test(v)) return "placeholder/demo";
  if (hasWrongBrandToken) return "wrong-brand";
  if (isBrandWebsite) {
    if (lower === "https://www.marriott.com" || lower === "https://marriott.com" || lower === "marriott.com") {
      if (approvedWebsiteUrl) return "generic";
      return "too broad";
    }
  }
  if (/^https?:\/\/(www\.)?marriott\.com\/?$/.test(lower)) return "too broad";
  if (/^(n\/a|unknown|not confirmed in available sources\.?)$/i.test(v)) return "generic";
  if (sourceBacked && v.length < 25 && /description|analysis|standards|positioning|proposition|considerations/i.test(label)) {
    return "source-backed but weakly written";
  }
  if (/ai-drafted|human-review/i.test(v)) return "AI-drafted and needing review";
  if (sourceBacked) return "correct/source-backed";
  if (v.length < 18 && /description|analysis|standards|positioning|proposition|considerations/i.test(label)) {
    return "generic";
  }
  return "correct/source-backed";
}

function parseIsoDate(dateText) {
  const d = new Date(dateText);
  return Number.isNaN(d.getTime()) ? null : d;
}

function staleByDate(text) {
  const d = parseIsoDate(text);
  if (!d) return false;
  const ageMs = Date.now() - d.getTime();
  return ageMs > 1000 * 60 * 60 * 24 * 540;
}

function approvedFactsMap(facts) {
  const byKey = new Map();
  for (const f of facts || []) {
    const status = nz(f.humanReviewStatus);
    if (!["Approved", "Edited"].includes(status)) continue;
    const key = nz(f.fieldName);
    if (!key) continue;
    const value = nz(f.approvedValue || f.normalizedValue || f.extractedValue);
    if (!value) continue;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(value);
  }
  return byKey;
}

function detectApprovedTributeWebsite(sourceRows) {
  const candidate = (sourceRows || []).find((s) => {
    const url = nz(s.sourceUrl).toLowerCase();
    return (
      nz(s.approvedForExplorerUse) === "Yes" &&
      /tribute-portfolio\.marriott\.com/.test(url) &&
      /marriott/.test(url)
    );
  });
  return nz(candidate?.sourceUrl);
}

function listReferenceProfileNamesFromFixtures() {
  const out = [];
  for (const rel of REFERENCE_FIXTURES) {
    const abs = path.join(ROOT, rel);
    if (!fs.existsSync(abs)) continue;
    try {
      const parsed = JSON.parse(fs.readFileSync(abs, "utf8"));
      const name = nz(parsed?.brandName || parsed?.profile || "");
      if (name) out.push(name);
    } catch {
      // Ignore malformed fixture safely.
    }
  }
  return [...new Set(out)];
}

export async function buildTributeExistingBrandFieldValidationAuditReport({
  brandKey = DEFAULT_BRAND_KEY,
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG[DEFAULT_BRAND_KEY];
  const resolvedBrandRecordId = pilot?.recordId || brandRecordId;
  const brandName = pilot?.brandName || "Tribute Portfolio";

  const basics = await airtableFetch(baseId, apiKey, BRAND_BASICS_TABLE, { method: "GET" }, resolvedBrandRecordId);
  if (!basics.res.ok || !basics.json?.id) {
    throw new Error(`Brand Basics record not found: ${resolvedBrandRecordId}`);
  }

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
  const materialUrls = collectSourceLinksFromMaterialsFile(materialsFileRows);

  const loyaltyRows = await listByFormula(
    baseId,
    apiKey,
    LOYALTY_TABLE,
    `{Brand Name}='${escapeFormulaValue(brandName)}'`
  );
  const loyaltyFields = (loyaltyRows[0] || {}).fields || {};
  const loyaltyProgramName = nz(loyaltyFields["Typical Loyalty Program Name"]);

  const sourceRows = [];
  let sourceOffset = null;
  do {
    const page = await listPartnerSources({ brandId: resolvedBrandRecordId, limit: 100, offset: sourceOffset });
    sourceRows.push(...(page.sources || []));
    sourceOffset = page.offset;
  } while (sourceOffset);

  const factRows = [];
  let factOffset = null;
  do {
    const page = await listPartnerFacts({ brandId: resolvedBrandRecordId, limit: 100, offset: factOffset });
    factRows.push(...(page.facts || []));
    factOffset = page.offset;
  } while (factOffset);
  const approvedFactMap = approvedFactsMap(factRows);

  const approvedWebsiteUrl = detectApprovedTributeWebsite(sourceRows);
  const referenceProfilesInspected = [...new Set([...REFERENCE_BRANDS, ...listReferenceProfileNamesFromFixtures()])];
  const basicsFields = basics.json.fields || {};

  const fieldByFieldCorrectionTable = [];
  const fieldsConfirmedCorrect = [];
  const fieldsFlagged = [];
  const sourceBackedCorrections = [];
  const requiresHumanReview = [];
  const fieldsRemainUnchanged = [];
  const wrongBrandTokens = ["radisson", "kimpton", "curio", "ascend", "choice hotels", "hilton"];

  for (const fieldDef of AUDIT_FIELDS) {
    let currentValue = "";
    let sourceBacked = false;
    let proposedValue = "";
    let sourceBasis = "";
    let reviewStatus = "";

    if (fieldDef.presentationSlot) {
      if (fieldDef.presentationSlot === "materials.file") {
        currentValue = materialsFileRows
          .map((r) => `${r.title} | ${r.body}`)
          .filter(Boolean)
          .join(" || ");
        sourceBacked = materialUrls.length > 0;
      } else {
        currentValue = mergedPresentationSlotValue(presentationRows, fieldDef.presentationSlot);
        sourceBacked = Boolean(currentValue);
      }
    } else if (fieldDef.key === "loyaltyProgramRelationship") {
      currentValue = nz(basicsFields[fieldDef.basicsField] || loyaltyProgramName);
      sourceBacked = Boolean(
        approvedFactMap.get("be.loyalty.programName")?.length || /bonvoy/i.test(currentValue)
      );
      if (!currentValue && loyaltyProgramName) currentValue = loyaltyProgramName;
    } else {
      currentValue = nz(basicsFields[fieldDef.basicsField]);
      sourceBacked = fieldDef.factKey ? Boolean(approvedFactMap.get(fieldDef.factKey)?.length) : Boolean(currentValue);
    }

    const hasWrongBrandToken = wrongBrandTokens.some((token) => nz(currentValue).toLowerCase().includes(token));
    let classification = classifyField({
      value: currentValue,
      sourceBacked,
      label: fieldDef.label,
      required: Boolean(fieldDef.required),
      approvedWebsiteUrl,
      isBrandWebsite: fieldDef.key === "brandWebsite",
      hasWrongBrandToken,
    });

    if (fieldDef.key === "companyValidationDate" && staleByDate(currentValue)) {
      classification = "stale";
    }
    if (fieldDef.key === "companyValidated" && nz(currentValue).toLowerCase() === "true") {
      classification = "AI-drafted and needing review";
    }

    if (fieldDef.key === "brandWebsite") {
      const currentNormalized = nz(currentValue).toLowerCase().replace(/\/+$/, "");
      const approvedNormalized = nz(approvedWebsiteUrl).toLowerCase().replace(/\/+$/, "");
      if (approvedNormalized && currentNormalized && currentNormalized !== approvedNormalized) {
        proposedValue = approvedWebsiteUrl;
        sourceBasis = "Approved Source Library record (Marriott-controlled Tribute URL)";
        reviewStatus = "Source-backed";
      } else if (approvedNormalized && !currentNormalized) {
        proposedValue = approvedWebsiteUrl;
        sourceBasis = "Approved Source Library record (Marriott-controlled Tribute URL)";
        reviewStatus = "Source-backed";
      }
    }

    if (classification === "correct/source-backed") {
      fieldsConfirmedCorrect.push(fieldDef.label);
    } else {
      fieldsFlagged.push({
        field: fieldDef.label,
        classification,
      });
    }

    if (proposedValue && reviewStatus === "Source-backed") {
      sourceBackedCorrections.push({
        field: fieldDef.label,
        currentValue: short(currentValue, 140),
        proposedValue,
        sourceBasis,
      });
    }
    if (/AI-drafted|review/i.test(classification) || /human review/i.test(reviewStatus)) {
      requiresHumanReview.push({
        field: fieldDef.label,
        currentValue: short(currentValue, 140),
        reason: classification,
      });
    }
    if (!proposedValue) {
      fieldsRemainUnchanged.push({
        field: fieldDef.label,
        reason: "No safe source-backed correction proposed in read-only audit.",
      });
    }

    fieldByFieldCorrectionTable.push({
      field: fieldDef.label,
      currentValue: short(currentValue, 360),
      classification,
      proposedValue: proposedValue || "",
      sourceBasis: sourceBasis || (sourceBacked ? "Existing source-backed content/pattern" : "Not source-backed"),
      reviewStatus: reviewStatus || (classification === "correct/source-backed" ? "No change needed" : "Review required"),
      safeToCorrectInFutureWriter: Boolean(proposedValue) && reviewStatus === "Source-backed",
    });
  }

  const report = {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    companyValidatedUntouched: true,
    brand: {
      key: brandKey,
      recordId: resolvedBrandRecordId,
      name: nz(basicsFields["Brand Name"]) || brandName,
    },
    filesRead: [
      "AGENTS.md",
      "reports/tribute-brand-explorer-content-parity-audit.md",
      "reports/tribute-brand-explorer-content-promotion-writer.md",
      "reports/tribute-portfolio-package-pipeline.md",
      "reports/brand-explorer-visual-qa-verification.md",
      "reports/brand-explorer-presentation-copy-parity-audit.md",
      "api/brand-library.js",
      "api/lib/partner-intelligence-field-map.js",
      "api/lib/partner-intelligence-explorer-field-registry.js",
      "docs/data-intelligence/BRAND_PROFILE_DATA_MODEL.md",
      "docs/data-intelligence/tribute-brand-explorer-content-parity-audit-v10.md",
      "docs/data-intelligence/tribute-brand-explorer-content-promotion-writer-v11.md",
      ...REFERENCE_FIXTURES,
    ],
    filesChanged: [
      "lib/partner-intelligence/tribute-existing-brand-field-validation-audit.js",
      "scripts/tribute-existing-brand-field-validation-audit.mjs",
      "docs/data-intelligence/tribute-existing-brand-field-validation-audit-v12.md",
      "reports/tribute-existing-brand-field-validation-audit.md",
      "reports/tribute-existing-brand-field-validation-audit.json",
      "package.json",
    ],
    v12ExistingFieldValidationAuditExists: true,
    referenceBrandsInspected: referenceProfilesInspected,
    currentLiveTributeFieldValuesAudited: fieldByFieldCorrectionTable.map((r) => ({
      field: r.field,
      currentValue: r.currentValue,
      classification: r.classification,
    })),
    fieldsConfirmedCorrect,
    fieldsFlaggedGenericWrongWeak: fieldsFlagged,
    brandWebsiteCurrentValue: nz(basicsFields["Brand Website"] || basicsFields["Website"]),
    brandWebsiteProposedCorrectedValue: approvedWebsiteUrl || "",
    brandWebsiteProposedSourceBasis: approvedWebsiteUrl
      ? "Approved Source Library record: Tribute Portfolio official Marriott-controlled URL."
      : "No approved Tribute-specific Source Library URL detected.",
    fieldByFieldCorrectionTable,
    sourceBackedCorrections,
    correctionsRequireHumanReview: requiresHumanReview,
    fieldsRemainUnchanged,
    sourceLinksMaterialsFileSummary: {
      rowCount: materialsFileRows.length,
      urls: materialUrls,
    },
    explorerPresentationRowsRead: presentationRows.length,
    approvedSourceLibraryCount: sourceRows.filter((s) => nz(s.approvedForExplorerUse) === "Yes").length,
    approvedExtractedFactCount: factRows.filter((f) => ["Approved", "Edited"].includes(nz(f.humanReviewStatus))).length,
    gatedV13CorrectionWriterNeeded: true,
    exactNextCommand: "npm run tribute-existing-brand-field-validation-audit -- --dry-run",
  };

  return report;
}

export function buildTributeExistingBrandFieldValidationAuditMarkdown(report) {
  const lines = [];
  lines.push("# Tribute Existing Brand Field Validation Audit v12");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Brand Website check");
  lines.push(`- Current value: ${report.brandWebsiteCurrentValue || "(blank)"}`);
  lines.push(`- Proposed corrected value: ${report.brandWebsiteProposedCorrectedValue || "(none proposed)"}`);
  lines.push(`- Source basis: ${report.brandWebsiteProposedSourceBasis}`);
  lines.push("");
  lines.push("## Fields confirmed correct/source-backed");
  if (!report.fieldsConfirmedCorrect.length) lines.push("- None.");
  for (const f of report.fieldsConfirmedCorrect) lines.push(`- ${f}`);
  lines.push("");
  lines.push("## Fields flagged");
  if (!report.fieldsFlaggedGenericWrongWeak.length) lines.push("- None.");
  for (const f of report.fieldsFlaggedGenericWrongWeak) {
    lines.push(`- ${f.field}: ${f.classification}`);
  }
  lines.push("");
  lines.push("## Field-by-field correction table");
  lines.push("");
  lines.push("| Field | Classification | Current | Proposed | Safe source-backed correction |");
  lines.push("|---|---|---|---|---|");
  for (const row of report.fieldByFieldCorrectionTable) {
    lines.push(
      `| ${row.field} | ${row.classification} | ${short(row.currentValue, 90)} | ${short(row.proposedValue, 90)} | ${
        row.safeToCorrectInFutureWriter ? "yes" : "no"
      } |`
    );
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Next command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}
