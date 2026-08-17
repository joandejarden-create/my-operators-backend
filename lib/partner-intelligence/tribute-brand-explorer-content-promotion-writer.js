/**
 * Tribute Brand Explorer Content Promotion Writer v11.
 *
 * Scope-limited writer (human-reviewed copy only):
 * - idealAssetProfile → presentation slot overview.typical_use_case (Body)
 * - standards → presentation slot standards.intro (Body)
 * - questionsOwnersShouldAsk → presentation slot standards.questions (Body)
 *
 * Guardrails:
 * - Dry-run by default
 * - Schema preflight before apply (no writes to missing fields)
 * - No image/media writes
 * - No sourceLinks / materials.file writes
 * - No Brand Website writes
 * - No hero/gallery/value-driver/recent-openings rewrites
 * - No Company Validated field writes
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BRAND_ASSET_PILOT_CONFIG } from "./brand-asset-registry-workflow.js";
import { listPartnerFacts } from "./airtable-facts.js";

export const WRITER_VERSION = "11";
export const REPORT_JSON_NAME = "tribute-brand-explorer-content-promotion-writer.json";
export const REPORT_MD_NAME = "tribute-brand-explorer-content-promotion-writer.md";

const DEFAULT_BRAND_KEY = "tribute-portfolio";
const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";
const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const REQUIRED_APPLY_FLAG = "--approve-tribute-brand-explorer-content-promotion";
const V10_REPORT_PATH = "reports/tribute-brand-explorer-content-parity-audit.json";

const PRESENTATION_WRITE_FIELDS = {
  slotKey: "Slot Key",
  title: "Title",
  body: "Body",
  brand: "Brand",
  brandName: "Brand Name",
  active: "Active",
  sortOrder: "Sort Order",
};

/** Completed-brand pattern: Explorer presentation slots, not Brand Basics columns. */
const CONTENT_TARGETS = [
  {
    normalizedKey: "idealAssetProfile",
    displayLabel: "Brand Profile Analysis",
    section: "idealAssetProfile",
    slotKey: "overview.typical_use_case",
    sortOrder: 0,
    factKey: "be.overview.typicalUseCase",
    buildDraft: () => ({
      value:
        "Best suited for independent boutique, lifestyle, and leisure hotels with a clear local point of view, where owners want Marriott Bonvoy, distribution, and commercial support without losing the property’s individual identity. Strongest fit when the asset already has design character, destination relevance, or a story that can be sharpened rather than standardized.",
      sourceBasis:
        "AI-drafted / human-reviewed by founder (not company-validated; not Marriott-validated)",
      reviewStatus:
        "AI-drafted / human-reviewed by founder (safe only with --allow-human-review-copy; not company-validated; not Marriott-validated)",
    }),
  },
  {
    normalizedKey: "standards",
    displayLabel: "Brand Standards",
    section: "standards",
    slotKey: "standards.intro",
    sortOrder: 1,
    buildDraft: () => ({
      value:
        "Owners should expect a soft-brand path with more flexibility than a prototype-led flag, but still with Marriott brand, quality, systems, loyalty, and operating requirements. The key planning question is what can remain unique, what must be upgraded through the PIP, and which standards are mandatory for Tribute Portfolio affiliation.",
      sourceBasis:
        "AI-drafted / human-reviewed by founder (not company-validated; not Marriott-validated)",
      reviewStatus:
        "AI-drafted / human-reviewed by founder (safe only with --allow-human-review-copy; not company-validated; not Marriott-validated)",
    }),
  },
  {
    normalizedKey: "questionsOwnersShouldAsk",
    displayLabel: "Questions Owners Should Ask",
    section: "standards",
    slotKey: "standards.questions",
    sortOrder: 30,
    buildDraft: () => ({
      value:
        "Which elements of the hotel’s identity, design, F&B, and local programming can remain unique under Tribute Portfolio? What brand, systems, quality, and Bonvoy participation requirements are mandatory, and what PIP scope, timing, and cost should be planned before affiliation?",
      sourceBasis:
        "AI-drafted / human-reviewed by founder (not company-validated; not Marriott-validated)",
      reviewStatus:
        "AI-drafted / human-reviewed by founder (safe only with --allow-human-review-copy; not company-validated; not Marriott-validated)",
    }),
  },
];

const TARGET_SECTIONS = ["idealAssetProfile", "standards"];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function short(v, max = 280) {
  const s = nz(v).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
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

async function fetchAirtableTableSchemas(baseId, apiKey) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Schema fetch failed: ${res.status}`);
  const byName = new Map((json.tables || []).map((t) => [t.name, t]));
  return { tables: json.tables || [], byName };
}

function schemaField(table, fieldName) {
  if (!table) return null;
  return (table.fields || []).find((f) => f.name === fieldName) || null;
}

function runSchemaPreflight({ schemaByName, presentationRowsBySlot }) {
  const presentationTable = schemaByName.get(PRESENTATION_TABLE);
  const bodyField = schemaField(presentationTable, PRESENTATION_WRITE_FIELDS.body);
  const slotKeyField = schemaField(presentationTable, PRESENTATION_WRITE_FIELDS.slotKey);
  const tableWritable = Boolean(presentationTable && bodyField && slotKeyField);

  const requiredPresentationFields = Object.values(PRESENTATION_WRITE_FIELDS);
  const missingPresentationFields = requiredPresentationFields.filter(
    (name) => !schemaField(presentationTable, name)
  );

  const rows = [];
  for (const target of CONTENT_TARGETS) {
    const row = presentationRowsBySlot.get(target.slotKey);
    const currentValue = row?.body || "";
    const proposedValue = target.buildDraft().value;
    const fieldExists = Boolean(bodyField);
    const writable =
      tableWritable &&
      fieldExists &&
      !missingPresentationFields.length &&
      !currentValue;

    rows.push({
      normalizedKey: target.normalizedKey,
      displayLabel: target.displayLabel,
      airtableTable: PRESENTATION_TABLE,
      airtableTableId: presentationTable?.id || "",
      slotKey: target.slotKey,
      airtableFieldName: PRESENTATION_WRITE_FIELDS.body,
      airtableFieldId: bodyField?.id || "",
      presentationRecordId: row?.recordId || null,
      currentValue,
      proposedValue,
      writable,
      schemaGap:
        missingPresentationFields.length > 0
          ? `Missing presentation columns: ${missingPresentationFields.join(", ")}`
          : !presentationTable
            ? `Table not found: ${PRESENTATION_TABLE}`
            : !fieldExists
              ? `Body column not found on ${PRESENTATION_TABLE}`
              : currentValue
                ? "Nonblank existing Body preserved (no overwrite)."
                : "",
    });
  }

  return {
    tableWritable,
    missingPresentationFields,
    presentationTableId: presentationTable?.id || "",
    targets: rows,
    preflightPassed: rows.every((r) => r.writable) && tableWritable && !missingPresentationFields.length,
  };
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

function approvedFactsByKey(facts) {
  const byKey = new Map();
  for (const fact of facts || []) {
    const status = nz(fact.humanReviewStatus);
    if (!["Approved", "Edited"].includes(status)) continue;
    const key = nz(fact.fieldName);
    if (!key) continue;
    const value = nz(fact.approvedValue || fact.normalizedValue || fact.extractedValue);
    if (!value) continue;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push({ value, status, id: fact.id });
  }
  return byKey;
}

function v10GapSummary(v10) {
  const rowByKey = new Map((v10?.fieldByFieldTributeGapTable || []).map((r) => [r.fieldOrSectionKey, r]));
  return {
    idealAssetProfile: rowByKey.get("idealAssetProfile")?.gapAssessment || "unknown",
    standards: rowByKey.get("standards")?.gapAssessment || "unknown",
    sourceLinks: rowByKey.get("sourceLinks")?.gapAssessment || "unknown",
  };
}

export async function buildTributeBrandExplorerContentPromotionWriterReport({
  brandKey = DEFAULT_BRAND_KEY,
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
  apply = false,
  applyApproved = false,
  allowHumanReviewCopy = false,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const v10 = readJsonFromRepo(V10_REPORT_PATH);
  if (!v10) throw new Error(`Missing required v10 report: ${V10_REPORT_PATH}`);

  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG[DEFAULT_BRAND_KEY];
  const resolvedBrandRecordId = pilot?.recordId || brandRecordId;
  const brandName = pilot?.brandName || "Tribute Portfolio";

  const mode = apply && applyApproved ? "apply" : "dry-run";
  const applyMode = apply && applyApproved;

  const { byName: schemaByName } = await fetchAirtableTableSchemas(baseId, apiKey);

  const basics = await airtableFetch(baseId, apiKey, BRAND_BASICS_TABLE, { method: "GET" }, resolvedBrandRecordId);
  if (!basics.res.ok || !basics.json?.id) {
    throw new Error(`Brand Basics record not found: ${resolvedBrandRecordId}`);
  }
  const basicFields = basics.json.fields || {};
  const brandWebsite = nz(basicFields["Brand Website"]);

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(resolvedBrandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(
      brandName
    )}')`
  );
  const presentationRows = normalizePresentationRows(presentationRaw);
  const presentationRowsBySlot = new Map(presentationRows.map((r) => [r.slotKey, r]));
  const materialsFileRows = presentationRows.filter((r) => r.slotKey === "materials.file");

  const factRows = [];
  let factOffset = null;
  do {
    const page = await listPartnerFacts({ brandId: resolvedBrandRecordId, limit: 100, offset: factOffset });
    factRows.push(...(page.facts || []));
    factOffset = page.offset;
  } while (factOffset);
  const factByKey = approvedFactsByKey(factRows);

  const preflight = runSchemaPreflight({ schemaByName, presentationRowsBySlot });

  const updatesProposed = [];
  const fieldsLeftUnchanged = [];
  const fieldsRemainBlank = [];
  const applyBlockers = [];
  const sourceBackedUpdates = [];
  const aiDraftedUpdates = [];

  const pushProposal = (proposal) => {
    updatesProposed.push(proposal);
    if (/source-backed|Approved PI fact/i.test(proposal.sourceBasis)) {
      sourceBackedUpdates.push(proposal);
    } else {
      aiDraftedUpdates.push(proposal);
    }
  };

  for (const target of CONTENT_TARGETS) {
    const row = presentationRowsBySlot.get(target.slotKey);
    const currentValue = row?.body || "";
    const draft = target.buildDraft();
    const factValue = target.factKey ? factByKey.get(target.factKey)?.[0]?.value || "" : "";
    const proposal = factValue
      ? {
          value: factValue,
          sourceBasis: "Approved PI fact",
          reviewStatus: "Source-backed",
          requiresHumanReviewFlag: false,
        }
      : {
          value: draft.value,
          sourceBasis: draft.sourceBasis,
          reviewStatus: draft.reviewStatus,
          requiresHumanReviewFlag: true,
        };

    if (currentValue) {
      fieldsLeftUnchanged.push({
        section: target.section,
        normalizedKey: target.normalizedKey,
        slotKey: target.slotKey,
        field: PRESENTATION_WRITE_FIELDS.body,
        reason: "Nonblank existing content preserved (section not rewritten).",
      });
      continue;
    }

    pushProposal({
      section: target.section,
      normalizedKey: target.normalizedKey,
      displayLabel: target.displayLabel,
      table: PRESENTATION_TABLE,
      recordId: row?.recordId || null,
      slotKey: target.slotKey,
      field: PRESENTATION_WRITE_FIELDS.body,
      airtableFieldId: preflight.targets.find((p) => p.normalizedKey === target.normalizedKey)?.airtableFieldId || "",
      currentValue,
      proposedValue: proposal.value,
      sourceBasis: proposal.sourceBasis,
      reviewStatus: proposal.reviewStatus,
      requiresHumanReviewFlag: proposal.requiresHumanReviewFlag,
      createRow: !row?.recordId,
      sortOrder: target.sortOrder,
      fields: {
        [PRESENTATION_WRITE_FIELDS.slotKey]: target.slotKey,
        [PRESENTATION_WRITE_FIELDS.title]: "",
        [PRESENTATION_WRITE_FIELDS.body]: proposal.value,
        [PRESENTATION_WRITE_FIELDS.brand]: [resolvedBrandRecordId],
        [PRESENTATION_WRITE_FIELDS.brandName]: brandName,
        [PRESENTATION_WRITE_FIELDS.active]: true,
        [PRESENTATION_WRITE_FIELDS.sortOrder]: target.sortOrder,
      },
    });
  }

  const filteredForApply = updatesProposed.filter((u) => {
    if (!u.proposedValue) return false;
    if (u.requiresHumanReviewFlag && !allowHumanReviewCopy) return false;
    return true;
  });
  const skippedForHumanReviewGate = updatesProposed.filter(
    (u) => u.requiresHumanReviewFlag && !allowHumanReviewCopy
  );

  if (!preflight.preflightPassed) {
    applyBlockers.push(
      "Schema preflight failed: one or more target presentation fields are missing or not writable."
    );
    for (const row of preflight.targets.filter((r) => !r.writable)) {
      applyBlockers.push(
        `${row.normalizedKey} (${row.slotKey}): ${row.schemaGap || "not writable"}`
      );
    }
  }

  if (apply && !applyApproved) {
    applyBlockers.push(`--apply requires ${REQUIRED_APPLY_FLAG}`);
  }
  if (apply && skippedForHumanReviewGate.length && !allowHumanReviewCopy) {
    applyBlockers.push(
      "AI-drafted/human-review copy proposals are blocked without --allow-human-review-copy."
    );
  }

  const applyResult = { updated: [], created: [], errors: [], blocked: false };
  if (applyMode && applyBlockers.length === 0) {
    for (const update of filteredForApply) {
      if (update.table !== PRESENTATION_TABLE) continue;
      if (update.createRow) {
        const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
          method: "POST",
          body: JSON.stringify({ fields: update.fields, typecast: true }),
        });
        if (!res.ok) {
          applyResult.errors.push({
            section: update.section,
            normalizedKey: update.normalizedKey,
            slotKey: update.slotKey,
            field: update.field,
            message: json.error?.message || `Create failed ${res.status}`,
          });
        } else {
          applyResult.created.push({
            table: PRESENTATION_TABLE,
            recordId: json.id || "",
            slotKey: update.slotKey,
            field: update.field,
          });
        }
      } else if (update.recordId) {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          {
            method: "PATCH",
            body: JSON.stringify({
              fields: { [PRESENTATION_WRITE_FIELDS.body]: update.proposedValue },
              typecast: true,
            }),
          },
          update.recordId
        );
        if (!res.ok) {
          applyResult.errors.push({
            section: update.section,
            normalizedKey: update.normalizedKey,
            slotKey: update.slotKey,
            field: update.field,
            message: json.error?.message || `Patch failed ${res.status}`,
          });
        } else {
          applyResult.updated.push({
            table: PRESENTATION_TABLE,
            recordId: update.recordId,
            slotKey: update.slotKey,
            field: update.field,
          });
        }
      }
    }
  } else if (applyMode && applyBlockers.length > 0) {
    applyResult.blocked = true;
  }

  const gapSummary = v10GapSummary(v10);
  const completionAfterApply = (() => {
    let score = Number(v10?.tributeCurrentCompletionScore || 83);
    const coreKeys = new Set(["idealAssetProfile", "standards", "questionsOwnersShouldAsk"]);
    const writableCore = filteredForApply.filter((u) => coreKeys.has(u.normalizedKey)).length;
    if (writableCore >= 3) score += 12;
    else if (writableCore >= 2) score += 8;
    else if (writableCore >= 1) score += 4;
    if (score > 98) score = 98;
    return score;
  })();

  const remainsNotAtParity = CONTENT_TARGETS.map((t) => t.normalizedKey).filter(
    (key) => !filteredForApply.some((u) => u.normalizedKey === key)
  );

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    filesRead: [
      "AGENTS.md",
      "reports/tribute-brand-explorer-content-parity-audit.md",
      "reports/tribute-brand-explorer-content-parity-audit.json",
      "api/brand-library.js",
      "api/lib/partner-intelligence-field-map.js",
      "api/lib/partner-intelligence-explorer-field-registry.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "docs/data-intelligence/tribute-brand-explorer-content-parity-audit-v10.md",
      "docs/data-intelligence/BRAND_PROFILE_DATA_MODEL.md",
      "docs/brand-explorer-presentation-slots.md",
      "docs/standards-owner-considerations-v1-audit.md",
      "fixtures/brand-explorer-presentation-curio-full.json",
      "fixtures/brand-explorer-presentation-kimpton-full.json",
    ],
    filesChanged: [
      "lib/partner-intelligence/tribute-brand-explorer-content-promotion-writer.js",
      "docs/data-intelligence/tribute-brand-explorer-content-promotion-writer-v11.md",
      "reports/tribute-brand-explorer-content-promotion-writer.md",
      "reports/tribute-brand-explorer-content-promotion-writer.json",
    ],
    v11WriterExists: true,
    rootCause:
      "v11 previously PATCHed Brand Setup - Brand Basics using display labels (Brand Profile Analysis, Brand Standards, Questions Owners Should Ask) that do not exist in live Airtable. Completed brands store this copy in Brand Explorer Presentation slots (overview.typical_use_case, standards.intro, standards.questions).",
    brand: {
      key: brandKey,
      recordId: resolvedBrandRecordId,
      name: brandName,
      parentCompany: nz(basicFields["Parent Company"]) || "Marriott International, Inc.",
      brandWebsite,
    },
    sectionsTargeted: TARGET_SECTIONS,
    contentTargets: CONTENT_TARGETS.map((t) => ({
      normalizedKey: t.normalizedKey,
      displayLabel: t.displayLabel,
      slotKey: t.slotKey,
      airtableTable: PRESENTATION_TABLE,
      airtableFieldName: PRESENTATION_WRITE_FIELDS.body,
    })),
    schemaPreflight: preflight,
    sectionsInspected: {
      idealAssetProfile: {
        slotKey: "overview.typical_use_case",
        field: PRESENTATION_WRITE_FIELDS.body,
        current: presentationRowsBySlot.get("overview.typical_use_case")?.body || "",
      },
      standards: {
        standardsIntro: {
          slotKey: "standards.intro",
          field: PRESENTATION_WRITE_FIELDS.body,
          current: presentationRowsBySlot.get("standards.intro")?.body || "",
        },
        questionsOwnersShouldAsk: {
          slotKey: "standards.questions",
          field: PRESENTATION_WRITE_FIELDS.body,
          current: presentationRowsBySlot.get("standards.questions")?.body || "",
        },
      },
      sourceLinks: {
        slotKey: "materials.file",
        rowCount: materialsFileRows.length,
        writeScope: "excluded — sourceLinks already promoted; v11 does not write materials.file",
        rows: materialsFileRows.map((r) => ({
          recordId: r.recordId,
          title: r.title,
          body: short(r.body),
          imageAttachmentCount: r.imageAttachmentCount,
        })),
      },
    },
    currentGapsFromV10: gapSummary,
    proposedUpdates: updatesProposed,
    sourceBackedUpdates,
    aiDraftedHumanReviewUpdates: aiDraftedUpdates,
    fieldsLeftUnchanged,
    fieldsRemainBlank,
    sourceLinksExcluded: true,
    brandWebsiteUntouched: true,
    brandWebsiteCurrent: brandWebsite,
    recentOpeningsRemainsBlank: true,
    imagesUntouched: true,
    unrelatedPresentationRowsUntouched: true,
    brandSetupMediaFieldsUntouched: true,
    companyValidatedFieldsUntouched: true,
    heroValueDriverGalleryUntouched: true,
    applyFlags: {
      applyRequested: apply,
      applyApproved,
      allowHumanReviewCopy,
    },
    applyBlockers,
    skippedForHumanReviewGate: skippedForHumanReviewGate.map((u) => ({
      section: u.section,
      normalizedKey: u.normalizedKey,
      slotKey: u.slotKey,
      reason: "Requires --allow-human-review-copy",
    })),
    applyResult,
    airtableModified:
      applyMode &&
      !applyResult.blocked &&
      applyResult.errors.length === 0 &&
      (applyResult.updated.length > 0 || applyResult.created.length > 0),
    exactApplyCommand:
      "npm run tribute-brand-explorer-content-promotion-writer -- --apply --approve-tribute-brand-explorer-content-promotion --allow-human-review-copy",
    expectedCompletionScoreAfterApply: completionAfterApply,
    reachesCompletedBrandContentParityAfterApply: remainsNotAtParity.length === 0,
    remainingParityGapsAfterApply: remainsNotAtParity,
  };
}

export function buildTributeBrandExplorerContentPromotionWriterMarkdown(report) {
  const lines = [];
  lines.push("# Tribute Brand Explorer Content Promotion Writer v11");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  if (report.rootCause) {
    lines.push("");
    lines.push("## Root cause (prior apply failure)");
    lines.push(report.rootCause);
  }
  lines.push("");
  lines.push("## Schema preflight");
  lines.push(`Preflight passed: **${report.schemaPreflight?.preflightPassed ? "yes" : "no"}**`);
  if (report.schemaPreflight?.targets?.length) {
    for (const row of report.schemaPreflight.targets) {
      lines.push(
        `- \`${row.normalizedKey}\` · ${row.displayLabel} → ${row.airtableTable} · slot \`${row.slotKey}\` · field **${row.airtableFieldName}** (\`${row.airtableFieldId}\`) · writable: **${row.writable ? "yes" : "no"}**`
      );
      if (row.schemaGap) lines.push(`  - Note: ${row.schemaGap}`);
      lines.push(`  - Current: ${short(row.currentValue || "(blank)", 160)}`);
      lines.push(`  - Proposed: ${short(row.proposedValue || "", 160)}`);
    }
  }
  lines.push("");
  lines.push("## Sections targeted");
  for (const s of report.sectionsTargeted) lines.push(`- ${s}`);
  lines.push("");
  lines.push("## Current gaps (from v10)");
  lines.push(`- idealAssetProfile: ${report.currentGapsFromV10.idealAssetProfile}`);
  lines.push(`- standards: ${report.currentGapsFromV10.standards}`);
  lines.push(`- sourceLinks: ${report.currentGapsFromV10.sourceLinks} (excluded from v11 writes)`);
  lines.push("");
  lines.push("## Proposed updates");
  if (!report.proposedUpdates.length) lines.push("- None.");
  for (const u of report.proposedUpdates) {
    lines.push(`- ${u.normalizedKey} · ${u.displayLabel} · slot \`${u.slotKey}\` · ${u.field}`);
    lines.push(`  - Source basis: ${u.sourceBasis}`);
    lines.push(`  - Review status: ${u.reviewStatus}`);
    lines.push(`  - Action: ${u.createRow ? "create presentation row" : "patch Body"}`);
    lines.push(`  - Proposed: ${short(u.proposedValue || "", 240)}`);
  }
  lines.push("");
  lines.push("## Apply blockers");
  if (!report.applyBlockers.length) lines.push("- None.");
  for (const b of report.applyBlockers) lines.push(`- ${b}`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Brand Website untouched: **${report.brandWebsiteUntouched ? "yes" : "no"}** (current: ${report.brandWebsiteCurrent || "(blank)"})`);
  lines.push(`- sourceLinks excluded: **${report.sourceLinksExcluded ? "yes" : "no"}**`);
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Unrelated presentation rows untouched: **${report.unrelatedPresentationRowsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated fields untouched: **${report.companyValidatedFieldsUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Exact apply command (if approved)");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  lines.push("");
  lines.push(`Expected completion score after apply: **${report.expectedCompletionScoreAfterApply}/100**`);
  lines.push(
    `Reaches completed-brand content parity after apply: **${
      report.reachesCompletedBrandContentParityAfterApply ? "yes" : "not yet"
    }**`
  );
  lines.push("");
  return lines.join("\n");
}
