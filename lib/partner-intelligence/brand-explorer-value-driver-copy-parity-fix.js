/**
 * Brand Explorer Value-Driver Copy Parity Fix v9.1.
 * Targeted copy writer for Tribute `overview.scenario.1` and `.2`.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BRAND_ASSET_PILOT_CONFIG } from "./brand-asset-registry-workflow.js";

export const WRITER_VERSION = "9.1";
export const REPORT_JSON_NAME = "brand-explorer-value-driver-copy-parity-fix.json";
export const REPORT_MD_NAME = "brand-explorer-value-driver-copy-parity-fix.md";

const DEFAULT_BRAND_KEY = "tribute-portfolio";
const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const TARGET_SLOTS = ["overview.scenario.1", "overview.scenario.2"];
const REQUIRED_APPLY_FLAG = "--approve-brand-explorer-value-driver-copy-fix";
const PARITY_REPORT_PATH = "reports/brand-explorer-presentation-copy-parity-audit.json";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}
function escapeFormulaValue(v) {
  return String(v).replace(/'/g, "\\'");
}
function readJsonFromRepo(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
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
    params.set("filterByFormula", formula);
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
function hasPropertySpecificValueDriverCopy(title, body) {
  const text = `${nz(title)} ${nz(body)}`.toLowerCase();
  if (!text) return false;
  if (/\ba tribute portfolio hotel\b/.test(text)) return true;
  if (/\ba tribute portfolio resort\b/.test(text)) return true;
  return /\b(crystal cove|humano|ermita|casa nizuc|hotel rumbao|loma medellin|barbados|lima|cartagena|medellin)\b/.test(
    text
  );
}

export async function buildBrandExplorerValueDriverCopyParityFixReport({
  brandKey = DEFAULT_BRAND_KEY,
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
  apply = false,
  applyApproved = false,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG[DEFAULT_BRAND_KEY];
  const resolvedBrandRecordId = pilot?.recordId || brandRecordId;
  const brandName = pilot?.brandName || "Tribute Portfolio";
  const mode = apply && applyApproved ? "apply" : "dry-run";

  const parityReport = readJsonFromRepo(PARITY_REPORT_PATH);
  if (!parityReport || !Array.isArray(parityReport.proposedTributeSectionCopy)) {
    throw new Error(`Missing or invalid parity recommendations: ${PARITY_REPORT_PATH}`);
  }
  const recommendationBySlot = new Map(
    parityReport.proposedTributeSectionCopy.map((r) => [nz(r.slotKey), r]).filter(([slotKey]) => Boolean(slotKey))
  );

  const presentationRecords = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(resolvedBrandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(
      brandName
    )}')`
  );
  const rows = normalizePresentationRows(presentationRecords);
  const rowBySlot = new Map(rows.map((r) => [r.slotKey, r]));

  const slotAudit = [];
  const fieldsProposedForUpdate = [];
  const fieldsLeftUnchanged = [];
  const applyBlockers = [];
  const slotsLeftUnchanged = [
    "overview.scenario.3",
    "overview.scenario.boutique_lifestyle",
    "overview.scenario.mixed_use",
    "materials.gallery.3",
    "footprint.openings",
    "PR / Opening Link",
  ];

  for (const slotKey of TARGET_SLOTS) {
    const row = rowBySlot.get(slotKey);
    const recmd = recommendationBySlot.get(slotKey);
    const currentTitle = row?.title || "";
    const currentBody = row?.body || "";
    const proposedTitle = nz(recmd?.proposedTributeTitle);
    const proposedBody = nz(recmd?.proposedTributeCaptionBody);
    const containsHotelName = hasPropertySpecificValueDriverCopy(currentTitle, currentBody);

    if (!row) {
      slotAudit.push({
        slotKey,
        recordId: null,
        containsHotelName,
        currentTitle,
        currentBody,
        proposedTitle,
        proposedBody,
        decision: "missing_slot_row",
      });
      fieldsLeftUnchanged.push({
        slotKey,
        reason: "Missing presentation slot row; module does not create new slots.",
      });
      continue;
    }

    if (!recmd || !proposedTitle || !proposedBody) {
      slotAudit.push({
        slotKey,
        recordId: row.recordId,
        containsHotelName,
        currentTitle,
        currentBody,
        proposedTitle,
        proposedBody,
        decision: "missing_recommendation",
      });
      fieldsLeftUnchanged.push({
        slotKey,
        recordId: row.recordId,
        reason: "Missing parity recommendation for slot.",
      });
      continue;
    }

    if (currentTitle === proposedTitle && currentBody === proposedBody) {
      slotAudit.push({
        slotKey,
        recordId: row.recordId,
        containsHotelName,
        currentTitle,
        currentBody,
        proposedTitle,
        proposedBody,
        decision: "unchanged_already_parity",
      });
      fieldsLeftUnchanged.push({
        slotKey,
        recordId: row.recordId,
        reason: "Already matches strategic value-driver copy standard.",
      });
      continue;
    }

    fieldsProposedForUpdate.push({
      slotKey,
      recordId: row.recordId,
      fields: { Title: proposedTitle, Body: proposedBody },
      containsHotelName,
      currentTitle,
      currentBody,
      proposedTitle,
      proposedBody,
      sourceBasis: nz(recmd.sourceBasis),
      reviewStatus: nz(recmd.reviewStatus),
    });
    slotAudit.push({
      slotKey,
      recordId: row.recordId,
      containsHotelName,
      currentTitle,
      currentBody,
      proposedTitle,
      proposedBody,
      decision: "proposed_update",
    });
  }

  const applyMode = apply && applyApproved;
  const applyResult = { updated: [], errors: [] };
  if (applyMode) {
    for (const patch of fieldsProposedForUpdate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({ fields: patch.fields, typecast: true }),
        },
        patch.recordId
      );
      if (!res.ok) {
        applyResult.errors.push({
          slotKey: patch.slotKey,
          recordId: patch.recordId,
          message: json.error?.message || `Patch failed with ${res.status}`,
        });
      } else {
        applyResult.updated.push({ slotKey: patch.slotKey, recordId: patch.recordId });
      }
    }
  } else if (apply && !applyApproved) {
    applyBlockers.push(`--apply requires ${REQUIRED_APPLY_FLAG}`);
  }

  const referenceBrandsInspected = [
    "Kimpton Hotels",
    "Radisson Blu by Choice",
    "Radisson by Choice",
    "Curio Collection by Hilton",
    "Ascend Hotel Collection",
  ];

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    filesRead: [
      "AGENTS.md",
      "reports/brand-explorer-presentation-copy-parity-audit.md",
      "reports/brand-explorer-presentation-copy-promotion-writer.md",
      "reports/brand-explorer-visual-qa-verification.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "docs/brand-explorer-presentation-slots.md",
      "fixtures/brand-explorer-presentation-radisson-blu.example.json",
      "fixtures/brand-explorer-presentation-radisson-choice-overview.json",
      "fixtures/brand-explorer-presentation-kimpton-full.json",
      "fixtures/brand-explorer-presentation-curio-full.json",
      "fixtures/brand-explorer-presentation-ascend-hotel-collection-full.json",
      PARITY_REPORT_PATH,
    ],
    brand: {
      key: brandKey,
      recordId: resolvedBrandRecordId,
      name: brandName,
    },
    referenceBrandsInspected,
    referenceValueDriverPattern:
      "Value-driver titles and bodies are strategic and owner-facing (market/use-case framing), with no property/hotel names in user-facing copy.",
    tributeCurrentValueDriverCopy: slotAudit.map((s) => ({
      slotKey: s.slotKey,
      recordId: s.recordId,
      currentTitle: s.currentTitle,
      currentBody: s.currentBody,
      containsHotelName: s.containsHotelName,
    })),
    proposedCorrectedValueDriverCopy: fieldsProposedForUpdate.map((s) => ({
      slotKey: s.slotKey,
      recordId: s.recordId,
      proposedTitle: s.proposedTitle,
      proposedBody: s.proposedBody,
      sourceBasis: s.sourceBasis,
      reviewStatus: s.reviewStatus,
    })),
    slotAudit,
    fieldsProposedForUpdate,
    fieldsLeftUnchanged,
    slotsLeftUnchanged,
    applyBlockers,
    applyFlags: {
      applyRequested: apply,
      applyApproved,
    },
    applyResult,
    hotelNamesRemovedFromUserFacingCopy: fieldsProposedForUpdate.every(
      (s) => !hasPropertySpecificValueDriverCopy(s.proposedTitle, s.proposedBody)
    ),
    imagesUntouched: true,
    brandSetupFieldsUntouched: true,
    companyValidatedFieldsUntouched: true,
    airtableModified: applyMode && applyResult.updated.length > 0 && applyResult.errors.length === 0,
    exactApplyCommand:
      "npm run brand-explorer-value-driver-copy-parity-fix -- --brand tribute-portfolio --apply --approve-brand-explorer-value-driver-copy-fix",
    reachesValueDriverCopyParityAfterApply:
      fieldsProposedForUpdate.length > 0 && applyBlockers.length === 0 && applyResult.errors.length === 0,
  };
}

export function buildBrandExplorerValueDriverCopyParityFixMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Value-Driver Copy Parity Fix v9.1");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Reference brands inspected");
  for (const name of report.referenceBrandsInspected) lines.push(`- ${name}`);
  lines.push("");
  lines.push("## Reference value-driver pattern");
  lines.push(`- ${report.referenceValueDriverPattern}`);
  lines.push("");
  lines.push("## Tribute current value-driver copy");
  for (const row of report.tributeCurrentValueDriverCopy) {
    lines.push(
      `- ${row.slotKey}: ${row.currentTitle || "(blank)"} | ${row.currentBody || "(blank)"} · contains hotel names: ${row.containsHotelName ? "yes" : "no"}`
    );
  }
  lines.push("");
  lines.push("## Proposed corrected value-driver copy");
  if (!report.proposedCorrectedValueDriverCopy.length) lines.push("- None.");
  for (const row of report.proposedCorrectedValueDriverCopy) {
    lines.push(`- ${row.slotKey}`);
    lines.push(`  - Proposed title: ${row.proposedTitle}`);
    lines.push(`  - Proposed body: ${row.proposedBody}`);
    lines.push(`  - Source basis: ${row.sourceBasis}`);
    lines.push(`  - Review status: ${row.reviewStatus}`);
  }
  lines.push("");
  lines.push("## Slots left unchanged");
  for (const slot of report.slotsLeftUnchanged) lines.push(`- ${slot}`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Brand Setup fields untouched: **${report.brandSetupFieldsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated fields untouched: **${report.companyValidatedFieldsUntouched ? "yes" : "no"}**`);
  lines.push(`- Hotel names removed from user-facing value-driver copy: **${report.hotelNamesRemovedFromUserFacingCopy ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Exact apply command (if approved)");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  lines.push("");
  lines.push(
    `Tribute reaches value-driver copy parity after apply: **${
      report.reachesValueDriverCopyParityAfterApply ? "yes" : "pending"
    }**`
  );
  lines.push("");
  return lines.join("\n");
}
