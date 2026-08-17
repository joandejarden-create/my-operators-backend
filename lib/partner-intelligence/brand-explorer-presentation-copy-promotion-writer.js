/**
 * Brand Explorer Presentation Copy Promotion Writer v9.
 * Copy-only updater for existing Tribute presentation slots.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BRAND_ASSET_PILOT_CONFIG } from "./brand-asset-registry-workflow.js";

export const WRITER_VERSION = "9";
export const REPORT_JSON_NAME = "brand-explorer-presentation-copy-promotion-writer.json";
export const REPORT_MD_NAME = "brand-explorer-presentation-copy-promotion-writer.md";

const DEFAULT_BRAND_KEY = "tribute-portfolio";
const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const PROMOTED_SLOTS = [
  "overview.hero",
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
  "overview.scenario.1",
  "overview.scenario.2",
];
const REQUIRED_APPLY_FLAG = "--approve-brand-explorer-copy-promotion";
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
function isGenericOrDemoCopy(title, body) {
  const hay = `${nz(title)} ${nz(body)}`.toLowerCase();
  return /(image gallery \d|\(gallery\)|where this brand creates the most value|mock|demo|placeholder|candidate|asset|approved)/i.test(
    hay
  );
}
function impliesMarriottValidation(text) {
  return /(marriott validated|validated by marriott|marriott-approved|marriott approved)/i.test(nz(text));
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

export async function buildBrandExplorerPresentationCopyPromotionWriterReport({
  brandKey = DEFAULT_BRAND_KEY,
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
  apply = false,
  applyApproved = false,
  allowNonblankCopyOverwrite = false,
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
    throw new Error(`Missing or invalid v8.5 parity recommendations: ${PARITY_REPORT_PATH}`);
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

  const slotsInspected = [];
  const fieldsProposedForUpdate = [];
  const fieldsLeftUnchanged = [];
  const overwriteRisks = [];
  const applyBlockers = [];
  const copySourceBasis = [];
  const humanReviewStatus = [];

  for (const slotKey of PROMOTED_SLOTS) {
    const rec = rowBySlot.get(slotKey);
    const recmd = recommendationBySlot.get(slotKey);
    const currentTitle = rec?.title || "";
    const currentBody = rec?.body || "";
    const proposedTitle = nz(recmd?.proposedTributeTitle);
    const proposedBody = nz(recmd?.proposedTributeCaptionBody);
    const sourceBasis = nz(recmd?.sourceBasis);
    const reviewStatus = nz(recmd?.reviewStatus);

    copySourceBasis.push({ slotKey, sourceBasis });
    humanReviewStatus.push({ slotKey, reviewStatus });

    if (!rec) {
      fieldsLeftUnchanged.push({
        slotKey,
        reason: "Missing presentation slot row in Airtable; v9 does not create new rows.",
      });
      slotsInspected.push({
        slotKey,
        recordId: null,
        currentTitle,
        currentBody,
        proposedTitle,
        proposedBody,
        decision: "unchanged_missing_slot_row",
      });
      continue;
    }
    if (!recmd) {
      fieldsLeftUnchanged.push({
        slotKey,
        recordId: rec.recordId,
        reason: "No v8.5 recommendation available for slot.",
      });
      slotsInspected.push({
        slotKey,
        recordId: rec.recordId,
        currentTitle,
        currentBody,
        proposedTitle,
        proposedBody,
        decision: "unchanged_no_recommendation",
      });
      continue;
    }
    if (!proposedTitle && !proposedBody) {
      fieldsLeftUnchanged.push({
        slotKey,
        recordId: rec.recordId,
        reason: "Proposed copy is blank; v9 does not write blank replacement copy.",
      });
      slotsInspected.push({
        slotKey,
        recordId: rec.recordId,
        currentTitle,
        currentBody,
        proposedTitle,
        proposedBody,
        decision: "unchanged_proposed_blank",
      });
      continue;
    }
    if (impliesMarriottValidation(`${proposedTitle} ${proposedBody}`)) {
      applyBlockers.push(`Slot ${slotKey} proposed copy implies Marriott validation and is blocked.`);
      fieldsLeftUnchanged.push({
        slotKey,
        recordId: rec.recordId,
        reason: "Blocked proposed copy due to Marriott validation implication.",
      });
      slotsInspected.push({
        slotKey,
        recordId: rec.recordId,
        currentTitle,
        currentBody,
        proposedTitle,
        proposedBody,
        decision: "blocked_validation_language",
      });
      continue;
    }

    const isDifferent = currentTitle !== proposedTitle || currentBody !== proposedBody;
    if (!isDifferent) {
      fieldsLeftUnchanged.push({
        slotKey,
        recordId: rec.recordId,
        reason: "Current copy already matches recommendation.",
      });
      slotsInspected.push({
        slotKey,
        recordId: rec.recordId,
        currentTitle,
        currentBody,
        proposedTitle,
        proposedBody,
        decision: "unchanged_already_good",
      });
      continue;
    }

    const currentHasNonblank = Boolean(currentTitle || currentBody);
    const currentGeneric = isGenericOrDemoCopy(currentTitle, currentBody);
    const mayUpdateByDefault = !currentHasNonblank || currentGeneric;
    if (!mayUpdateByDefault && !allowNonblankCopyOverwrite) {
      overwriteRisks.push({
        slotKey,
        recordId: rec.recordId,
        reason: "Nonblank polished copy differs; requires --allow-nonblank-copy-overwrite",
      });
      fieldsLeftUnchanged.push({
        slotKey,
        recordId: rec.recordId,
        reason: "Protected polished nonblank copy (overwrite flag not provided).",
      });
      slotsInspected.push({
        slotKey,
        recordId: rec.recordId,
        currentTitle,
        currentBody,
        proposedTitle,
        proposedBody,
        decision: "unchanged_protected_nonblank",
      });
      continue;
    }

    fieldsProposedForUpdate.push({
      slotKey,
      recordId: rec.recordId,
      fields: {
        Title: proposedTitle,
        Body: proposedBody,
      },
      reason: currentHasNonblank ? "Replace generic/demo-style copy" : "Fill blank copy",
      sourceBasis,
      reviewStatus,
      userFacingCopyNote:
        "Copy is owner-facing and source-grounded; internal governance labels remain report-only.",
    });
    slotsInspected.push({
      slotKey,
      recordId: rec.recordId,
      currentTitle,
      currentBody,
      proposedTitle,
      proposedBody,
      decision: "proposed_update",
    });
  }

  let applyResult = { updated: [], errors: [] };
  const applyMode = apply && applyApproved;
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

  const missingSlotsLeftBlank = [
    "materials.gallery.3",
    "overview.scenario.3",
    "footprint.openings",
    "overview.scenario.boutique_lifestyle",
    "overview.scenario.mixed_use",
    "PR / Opening Link",
  ];

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    filesRead: [
      "AGENTS.md",
      "reports/brand-explorer-presentation-copy-parity-audit.md",
      "reports/brand-explorer-presentation-copy-parity-audit.json",
      "lib/partner-intelligence/brand-explorer-presentation-copy-parity-audit.js",
      "reports/brand-explorer-visual-qa-verification.md",
      "reports/explorer-media-promotion-writer.md",
      "reports/tribute-portfolio-package-pipeline.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "docs/brand-explorer-presentation-slots.md",
      "docs/data-intelligence/brand-explorer-presentation-copy-parity-audit-v8-5.md",
      "docs/data-intelligence/brand-explorer-visual-qa-verification-v8.md",
      "docs/data-intelligence/explorer-media-promotion-writer-v7.md",
      "fixtures/brand-explorer-presentation-radisson-blu.example.json",
      "fixtures/brand-explorer-presentation-radisson-choice-overview.json",
      "fixtures/brand-explorer-presentation-kimpton-full.json",
      "fixtures/brand-explorer-presentation-curio-full.json",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-presentation-copy-promotion-writer.js",
      "scripts/brand-explorer-presentation-copy-promotion-writer.mjs",
      "docs/data-intelligence/brand-explorer-presentation-copy-promotion-writer-v9.md",
      "reports/brand-explorer-presentation-copy-promotion-writer.md",
      "reports/brand-explorer-presentation-copy-promotion-writer.json",
      "package.json",
      "docs/data-intelligence/brand-explorer-presentation-copy-parity-audit-v8-5.md",
      "docs/data-intelligence/brand-explorer-visual-qa-verification-v8.md",
      "docs/data-intelligence/partner-intelligence-priority-profile-production-tracker.md",
      "docs/README.md",
    ],
    v9WriterExists: true,
    brand: {
      key: brandKey,
      recordId: resolvedBrandRecordId,
      name: brandName,
      tributeTextGovernancePlatformReady: true,
    },
    slotsInspected,
    currentCopyBySlot: slotsInspected.map((s) => ({
      slotKey: s.slotKey,
      recordId: s.recordId,
      currentTitle: s.currentTitle,
      currentBody: s.currentBody,
    })),
    proposedCopyBySlot: slotsInspected.map((s) => ({
      slotKey: s.slotKey,
      recordId: s.recordId,
      proposedTitle: s.proposedTitle,
      proposedBody: s.proposedBody,
      decision: s.decision,
    })),
    copySourceBasis,
    humanReviewStatus,
    fieldsProposedForUpdate,
    fieldsLeftUnchanged,
    missingSlotsLeftBlank,
    overwriteRisks,
    applyBlockers,
    applyFlags: {
      applyRequested: apply,
      applyApproved,
      allowNonblankCopyOverwrite,
    },
    applyResult,
    airtableModified: applyMode && applyResult.updated.length > 0 && applyResult.errors.length === 0,
    imagesUntouched: true,
    brandSetupFieldsUntouched: true,
    companyValidatedFieldsUntouched: true,
    registryRecordsUntouched: true,
    exactApplyCommand:
      "npm run brand-explorer-presentation-copy-promotion-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-copy-promotion",
    exactApplyCommandWithOverwrite:
      "npm run brand-explorer-presentation-copy-promotion-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-copy-promotion --allow-nonblank-copy-overwrite",
    reachesPresentationCopyParityAfterApply:
      fieldsProposedForUpdate.length > 0 && overwriteRisks.length === 0 && applyBlockers.length === 0,
  };
}

export function buildBrandExplorerPresentationCopyPromotionWriterMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Presentation Copy Promotion Writer v9");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Slots inspected");
  for (const s of report.slotsInspected) {
    lines.push(`- ${s.slotKey}: ${s.decision}`);
  }
  lines.push("");
  lines.push("## Current copy problems found");
  const problemRows = report.slotsInspected.filter((s) =>
    /proposed_update|unchanged_protected_nonblank|blocked_/.test(s.decision)
  );
  if (!problemRows.length) lines.push("- None.");
  for (const s of problemRows) {
    lines.push(`- ${s.slotKey}: ${s.currentBody || "(blank)"} -> ${s.decision}`);
  }
  lines.push("");
  lines.push("## Proposed copy updates");
  if (!report.fieldsProposedForUpdate.length) lines.push("- None.");
  for (const p of report.fieldsProposedForUpdate) {
    lines.push(`- ${p.slotKey} (\`${p.recordId}\`)`);
    lines.push(`  - Title: ${p.fields.Title || "(blank)"}`);
    lines.push(`  - Body: ${p.fields.Body || "(blank)"}`);
    lines.push(`  - Source basis: ${p.sourceBasis}`);
    lines.push(`  - Human review: ${p.reviewStatus}`);
  }
  lines.push("");
  lines.push("## Missing slots left blank");
  for (const m of report.missingSlotsLeftBlank) lines.push(`- ${m}`);
  lines.push("");
  lines.push("## Overwrite risks");
  if (!report.overwriteRisks.length) lines.push("- None.");
  for (const r of report.overwriteRisks) {
    lines.push(`- ${r.slotKey}: ${r.reason}`);
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Brand Setup fields untouched: **${report.brandSetupFieldsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated fields untouched: **${report.companyValidatedFieldsUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Exact apply command (if approved)");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  lines.push("");
  lines.push("Optional overwrite flag for polished nonblank copy:");
  lines.push("- `--allow-nonblank-copy-overwrite`");
  lines.push("");
  lines.push(`Tribute reaches presentation copy parity after apply: **${report.reachesPresentationCopyParityAfterApply ? "yes" : "partially / pending overwrite decisions"}**`);
  lines.push("");
  return lines.join("\n");
}
