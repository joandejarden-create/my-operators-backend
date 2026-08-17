/**
 * Partner Intelligence Repository — Airtable schema ensure (Phase 2).
 *
 * Creates / verifies four tables on the primary base (AIRTABLE_BASE_ID):
 *   - Partner Intelligence - Source Library
 *   - Partner Intelligence - Extracted Facts
 *   - Partner Intelligence - Published Explorer Fields
 *   - Partner Intelligence - Helena Outreach Intake
 *
 * Default: dry-run. Use --apply to create missing tables/fields via Metadata API.
 *
 *   node scripts/ensure-partner-intelligence-tables.mjs
 *   node scripts/ensure-partner-intelligence-tables.mjs --apply
 *
 * Report: reports/ensure-partner-intelligence-tables.json
 *
 * Pilot operator (Arbor Lodging): recF5Z87OAqFgndoq — see api/lib/partner-intelligence-explorer-field-registry.js
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  PARTNER_INTELLIGENCE_TABLES,
  PARTNER_INTELLIGENCE_LINKS,
  MAP_PARTNER_SOURCE,
  MAP_PARTNER_FACT,
  MAP_PARTNER_PUBLISHED,
  MAP_PARTNER_HELENA,
  VAL_PARTNER_SOURCE_SELECTS,
  VAL_PARTNER_FACT_SELECTS,
  VAL_PARTNER_PUBLISHED_SELECTS,
  VAL_PARTNER_HELENA_SELECTS,
} from "../api/lib/partner-intelligence-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

const SOURCE_TYPE_CHOICES = [
  "FDD",
  "Development Brochure",
  "Development Page",
  "Brand Page",
  "Operator Capability Deck",
  "Owner Presentation",
  "Portfolio Page",
  "Case Study",
  "Press Release",
  "Investor Presentation",
  "RFP Response",
  "Internal Note",
  "Website Capture",
  "Other",
];

const FILE_TYPE_CHOICES = ["PDF", "PPTX", "DOCX", "XLSX", "HTML", "PNG/JPG", "TXT", "Other"];

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

function singleSelect(name, optionNames, description) {
  const field = { name, type: "singleSelect", options: choices(optionNames) };
  if (description) field.description = description;
  return field;
}

function dateField(name, description) {
  const field = {
    name,
    type: "date",
    options: { dateFormat: { name: "iso" } },
  };
  if (description) field.description = description;
  return field;
}

function linkField(name, linkedTableId, description) {
  const field = {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId },
  };
  if (description) field.description = description;
  return field;
}

async function metaFetch(baseId, token, metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function buildSourceLibraryFields(brandBasicsId, operatorMasterId, usersTableId, selfTableId) {
  const fields = [
    singleSelect(MAP_PARTNER_SOURCE.profileType, VAL_PARTNER_SOURCE_SELECTS.profileType),
    linkField(MAP_PARTNER_SOURCE.parentCompany, brandBasicsId, "Parent company (Brand Basics row when applicable)."),
    linkField(MAP_PARTNER_SOURCE.brand, brandBasicsId),
    linkField(MAP_PARTNER_SOURCE.operator, operatorMasterId),
    { name: MAP_PARTNER_SOURCE.region, type: "singleLineText" },
    { name: MAP_PARTNER_SOURCE.countryMarket, type: "singleLineText" },
    { name: MAP_PARTNER_SOURCE.sourceTitle, type: "singleLineText" },
    singleSelect(MAP_PARTNER_SOURCE.sourceType, SOURCE_TYPE_CHOICES),
    { name: MAP_PARTNER_SOURCE.sourceUrl, type: "url" },
    { name: MAP_PARTNER_SOURCE.sourceFile, type: "multipleAttachments" },
    singleSelect(MAP_PARTNER_SOURCE.fileType, FILE_TYPE_CHOICES),
    dateField(MAP_PARTNER_SOURCE.sourceDate),
    dateField(MAP_PARTNER_SOURCE.captureDate),
    singleSelect(MAP_PARTNER_SOURCE.sourceOrigin, VAL_PARTNER_SOURCE_SELECTS.sourceOrigin),
    singleSelect(MAP_PARTNER_SOURCE.visibility, VAL_PARTNER_SOURCE_SELECTS.visibility),
    singleSelect(MAP_PARTNER_SOURCE.verifiedSource, VAL_PARTNER_SOURCE_SELECTS.verifiedSource),
    singleSelect(MAP_PARTNER_SOURCE.sourceQuality, VAL_PARTNER_SOURCE_SELECTS.sourceQuality),
    singleSelect(MAP_PARTNER_SOURCE.status, VAL_PARTNER_SOURCE_SELECTS.status),
    { name: MAP_PARTNER_SOURCE.notes, type: "multilineText" },
    dateField(MAP_PARTNER_SOURCE.lastReviewed),
    { name: MAP_PARTNER_SOURCE.reviewedBy, type: "singleCollaborator" },
    singleSelect(MAP_PARTNER_SOURCE.approvedForExtraction, VAL_PARTNER_SOURCE_SELECTS.approvedForExtraction),
    singleSelect(MAP_PARTNER_SOURCE.approvedForExplorerUse, VAL_PARTNER_SOURCE_SELECTS.approvedForExplorerUse),
    { name: MAP_PARTNER_SOURCE.confidentialityNotes, type: "multilineText" },
    { name: MAP_PARTNER_SOURCE.permissionVisibilityNotes, type: "multilineText" },
    { name: MAP_PARTNER_SOURCE.extractionRunId, type: "singleLineText" },
    { name: MAP_PARTNER_SOURCE.localFilePath, type: "singleLineText", description: "Relative path under PARTNER_REFERENCE_ROOT." },
  ];
  if (usersTableId) {
    fields.push(linkField(MAP_PARTNER_SOURCE.relatedContact, usersTableId));
  }
  if (selfTableId) {
    fields.push(linkField(MAP_PARTNER_SOURCE.duplicateOf, selfTableId, "Duplicate source record."));
  }
  return fields;
}

function buildExtractedFactsFields(brandBasicsId, operatorMasterId, sourceLibraryId) {
  return [
    singleSelect(MAP_PARTNER_FACT.profileType, VAL_PARTNER_SOURCE_SELECTS.profileType),
    linkField(MAP_PARTNER_FACT.parentCompany, brandBasicsId),
    linkField(MAP_PARTNER_FACT.brand, brandBasicsId),
    linkField(MAP_PARTNER_FACT.operator, operatorMasterId),
    linkField(MAP_PARTNER_FACT.sourceRecord, sourceLibraryId),
    singleSelect(MAP_PARTNER_FACT.explorerType, VAL_PARTNER_FACT_SELECTS.explorerType),
    { name: MAP_PARTNER_FACT.explorerSection, type: "singleLineText" },
    { name: MAP_PARTNER_FACT.fieldName, type: "singleLineText" },
    { name: MAP_PARTNER_FACT.extractedValue, type: "multilineText" },
    { name: MAP_PARTNER_FACT.normalizedValue, type: "multilineText" },
    { name: MAP_PARTNER_FACT.evidenceText, type: "multilineText" },
    { name: MAP_PARTNER_FACT.pageSectionAnchor, type: "singleLineText" },
    singleSelect(MAP_PARTNER_FACT.sourceType, SOURCE_TYPE_CHOICES),
    singleSelect(MAP_PARTNER_FACT.sourceQuality, VAL_PARTNER_SOURCE_SELECTS.sourceQuality),
    { name: MAP_PARTNER_FACT.confidenceScore, type: "number", options: { precision: 0 } },
    singleSelect(MAP_PARTNER_FACT.confidenceLevel, VAL_PARTNER_FACT_SELECTS.confidenceLevel),
    singleSelect(MAP_PARTNER_FACT.extractionType, VAL_PARTNER_FACT_SELECTS.extractionType),
    singleSelect(MAP_PARTNER_FACT.publicVisibility, VAL_PARTNER_FACT_SELECTS.publicVisibility),
    singleSelect(MAP_PARTNER_FACT.humanReviewStatus, VAL_PARTNER_FACT_SELECTS.humanReviewStatus),
    { name: MAP_PARTNER_FACT.approvedValue, type: "multilineText" },
    { name: MAP_PARTNER_FACT.reviewerNotes, type: "multilineText" },
    singleSelect(MAP_PARTNER_FACT.dataGap, VAL_PARTNER_FACT_SELECTS.dataGap),
    { name: MAP_PARTNER_FACT.followUpQuestion, type: "multilineText" },
    dateField(MAP_PARTNER_FACT.lastUpdated),
    { name: MAP_PARTNER_FACT.extractionRunId, type: "singleLineText" },
    { name: MAP_PARTNER_FACT.reviewedBy, type: "singleCollaborator" },
    dateField(MAP_PARTNER_FACT.reviewedAt),
  ];
}

function buildPublishedFields(brandBasicsId, operatorMasterId, sourceLibraryId, factsTableId) {
  return [
    singleSelect(MAP_PARTNER_PUBLISHED.profileType, VAL_PARTNER_SOURCE_SELECTS.profileType),
    linkField(MAP_PARTNER_PUBLISHED.brand, brandBasicsId),
    linkField(MAP_PARTNER_PUBLISHED.operator, operatorMasterId),
    linkField(MAP_PARTNER_PUBLISHED.supportingFacts, factsTableId),
    linkField(MAP_PARTNER_PUBLISHED.primarySource, sourceLibraryId),
    singleSelect(MAP_PARTNER_PUBLISHED.explorerType, VAL_PARTNER_FACT_SELECTS.explorerType),
    { name: MAP_PARTNER_PUBLISHED.explorerSection, type: "singleLineText" },
    { name: MAP_PARTNER_PUBLISHED.fieldName, type: "singleLineText" },
    { name: MAP_PARTNER_PUBLISHED.approvedValue, type: "multilineText" },
    { name: MAP_PARTNER_PUBLISHED.displayLabel, type: "singleLineText" },
    singleSelect(MAP_PARTNER_PUBLISHED.publicVisibility, VAL_PARTNER_FACT_SELECTS.publicVisibility),
    singleSelect(
      MAP_PARTNER_PUBLISHED.overallSourceConfidence,
      VAL_PARTNER_PUBLISHED_SELECTS.overallSourceConfidence
    ),
    dateField(MAP_PARTNER_PUBLISHED.lastReviewedDate),
    { name: MAP_PARTNER_PUBLISHED.reviewedBy, type: "singleCollaborator" },
    singleSelect(MAP_PARTNER_PUBLISHED.publishStatus, VAL_PARTNER_PUBLISHED_SELECTS.publishStatus),
    dateField(MAP_PARTNER_PUBLISHED.publishedAt),
    { name: MAP_PARTNER_PUBLISHED.stale, type: "checkbox", options: { icon: "check", color: "redBright" } },
    { name: MAP_PARTNER_PUBLISHED.dataGap, type: "checkbox", options: { icon: "check", color: "yellowBright" } },
    { name: MAP_PARTNER_PUBLISHED.reviewerNotes, type: "multilineText" },
    { name: MAP_PARTNER_PUBLISHED.registryVersion, type: "number", options: { precision: 0 } },
  ];
}

function buildHelenaIntakeFields(brandBasicsId, operatorMasterId, sourceLibraryId) {
  return [
    singleSelect(MAP_PARTNER_HELENA.profileType, VAL_PARTNER_HELENA_SELECTS.profileType),
    linkField(MAP_PARTNER_HELENA.parentCompany, brandBasicsId),
    linkField(MAP_PARTNER_HELENA.brand, brandBasicsId),
    linkField(MAP_PARTNER_HELENA.operator, operatorMasterId),
    { name: MAP_PARTNER_HELENA.contactName, type: "singleLineText" },
    { name: MAP_PARTNER_HELENA.contactTitle, type: "singleLineText" },
    { name: MAP_PARTNER_HELENA.contactEmail, type: "email" },
    { name: MAP_PARTNER_HELENA.company, type: "singleLineText" },
    { name: MAP_PARTNER_HELENA.region, type: "singleLineText" },
    { name: MAP_PARTNER_HELENA.requestedMaterials, type: "multilineText" },
    { name: MAP_PARTNER_HELENA.receivedMaterials, type: "multilineText" },
    dateField(MAP_PARTNER_HELENA.dateRequested),
    dateField(MAP_PARTNER_HELENA.dateReceived),
    singleSelect(MAP_PARTNER_HELENA.sourceOrigin, VAL_PARTNER_HELENA_SELECTS.sourceOrigin),
    { name: MAP_PARTNER_HELENA.permissionVisibilityNotes, type: "multilineText" },
    { name: MAP_PARTNER_HELENA.confidentialityNotes, type: "multilineText" },
    singleSelect(MAP_PARTNER_HELENA.followUpNeeded, VAL_PARTNER_HELENA_SELECTS.followUpNeeded),
    dateField(MAP_PARTNER_HELENA.suggestedFollowUpDate),
    singleSelect(
      MAP_PARTNER_HELENA.uploadedToSourceLibrary,
      VAL_PARTNER_HELENA_SELECTS.uploadedToSourceLibrary
    ),
    singleSelect(MAP_PARTNER_HELENA.extractionStatus, VAL_PARTNER_HELENA_SELECTS.extractionStatus),
    linkField(MAP_PARTNER_HELENA.linkedSourceRecord, sourceLibraryId),
    { name: MAP_PARTNER_HELENA.notes, type: "multilineText" },
  ];
}

async function ensureTable(baseId, apiKey, tables, report, spec) {
  const { tableName, description, buildFields, dependsOn } = spec;
  const entry = {
    tableName,
    tableId: null,
    createdTable: false,
    fieldsWouldCreate: [],
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
  };

  let table = tables.find((t) => t.name === tableName);
  const deps = typeof dependsOn === "function" ? dependsOn(report) : dependsOn || {};
  const fieldSpecs = buildFields(deps);

  if (!table) {
    if (!APPLY) {
      entry.wouldCreateTable = true;
      entry.fieldsWouldCreate = fieldSpecs.map((f) => f.name);
      console.log("WOULD CREATE TABLE", tableName, `(${fieldSpecs.length} fields)`);
    } else {
      const primaryName = MAP_PARTNER_SOURCE.sourceTitle;
      const primary = fieldSpecs.find((f) => f.name === primaryName) || {
        name: primaryName,
        type: "singleLineText",
      };
      const rest = fieldSpecs.filter((f) => f.name !== primary.name);
      const { res, json } = await metaFetch(baseId, apiKey, "/tables", {
        method: "POST",
        body: JSON.stringify({
          name: tableName,
          description,
          fields: [primary, ...rest],
        }),
      });
      if (!res.ok) throw new Error(`Create table ${tableName} failed: ${JSON.stringify(json)}`);
      table = json;
      entry.createdTable = true;
      entry.tableId = json.id;
      entry.fieldsCreated = (json.fields || []).map((f) => f.name);
      console.log("CREATED TABLE", json.name, json.id);
      tables.push(json);
    }
  } else {
    entry.tableId = table.id;
    console.log("TABLE EXISTS", table.name, table.id);
    const existing = new Set((table.fields || []).map((f) => f.name));
    for (const fieldSpec of fieldSpecs) {
      if (existing.has(fieldSpec.name)) {
        entry.fieldsSkipped.push(fieldSpec.name);
        continue;
      }
      if (!APPLY) {
        entry.fieldsWouldCreate.push(fieldSpec.name);
        continue;
      }
      const { res, json } = await metaFetch(baseId, apiKey, `/tables/${table.id}/fields`, {
        method: "POST",
        body: JSON.stringify(fieldSpec),
      });
      if (!res.ok) {
        entry.fieldsFailed.push({ name: fieldSpec.name, error: json });
        console.error("FIELD FAILED", tableName, fieldSpec.name, JSON.stringify(json));
      } else {
        entry.fieldsCreated.push(fieldSpec.name);
        console.log("CREATED FIELD", tableName, fieldSpec.name);
      }
    }
  }

  report.tables.push(entry);
  return table;
}

async function ensureDuplicateOfSelfLink(baseId, apiKey, report, sourceTable) {
  if (!sourceTable || !sourceTable.id) return;
  const fieldName = MAP_PARTNER_SOURCE.duplicateOf;
  const existing = new Set((sourceTable.fields || []).map((f) => f.name));
  if (existing.has(fieldName)) return;

  const entry = report.tables.find((t) => t.tableName === PARTNER_INTELLIGENCE_TABLES.sourceLibrary);
  if (!APPLY) {
    if (entry) entry.fieldsWouldCreate = [...(entry.fieldsWouldCreate || []), fieldName];
    console.log("WOULD CREATE SELF-LINK", fieldName, "on", sourceTable.name);
    return;
  }

  const { res, json } = await metaFetch(baseId, apiKey, `/tables/${sourceTable.id}/fields`, {
    method: "POST",
    body: JSON.stringify(linkField(fieldName, sourceTable.id, "Duplicate source record.")),
  });
  if (!res.ok) {
    if (entry) entry.fieldsFailed.push({ name: fieldName, error: json });
    console.error("SELF-LINK FAILED", JSON.stringify(json));
  } else {
    if (entry) entry.fieldsCreated.push(fieldName);
    console.log("CREATED SELF-LINK", fieldName);
  }
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    throw new Error("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY");
  }

  const { res: listRes, json: listJson } = await metaFetch(baseId, apiKey, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${JSON.stringify(listJson)}`);

  const tables = listJson.tables || [];
  const brandBasics = tables.find((t) => t.name === PARTNER_INTELLIGENCE_LINKS.brandBasics);
  const operatorMaster = tables.find((t) => t.name === PARTNER_INTELLIGENCE_LINKS.operatorMaster);
  const usersTable = tables.find((t) => t.name === PARTNER_INTELLIGENCE_LINKS.users);

  if (!brandBasics) throw new Error(`Missing table: ${PARTNER_INTELLIGENCE_LINKS.brandBasics}`);
  if (!operatorMaster) throw new Error(`Missing table: ${PARTNER_INTELLIGENCE_LINKS.operatorMaster}`);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    pilotOperator: {
      recordId: "recF5Z87OAqFgndoq",
      companyName: "Arbor Lodging (CALA)",
      referenceFolder: "Arbor Lodging",
    },
    linkedTables: {
      brandBasics: brandBasics.id,
      operatorMaster: operatorMaster.id,
      users: usersTable ? usersTable.id : null,
    },
    tables: [],
  };

  const sourceTable = await ensureTable(baseId, apiKey, tables, report, {
    tableName: PARTNER_INTELLIGENCE_TABLES.sourceLibrary,
    description:
      "Partner Intelligence source documents and URLs. Human review before extraction or Explorer use.",
    buildFields: ({ brandBasicsId, operatorMasterId, usersTableId, selfTableId }) =>
      buildSourceLibraryFields(brandBasicsId, operatorMasterId, usersTableId, selfTableId),
    dependsOn: () => ({
      brandBasicsId: brandBasics.id,
      operatorMasterId: operatorMaster.id,
      usersTableId: usersTable ? usersTable.id : null,
      selfTableId: null,
    }),
  });

  await ensureDuplicateOfSelfLink(baseId, apiKey, report, sourceTable);

  const sourceLibraryId =
    sourceTable && sourceTable.id
      ? sourceTable.id
      : tables.find((t) => t.name === PARTNER_INTELLIGENCE_TABLES.sourceLibrary)?.id;

  if (!sourceLibraryId && !APPLY) {
    console.log("(Dry-run: Extracted Facts / Published / Helena link fields deferred until Source Library exists.)");
  }

  const factsTable = await ensureTable(baseId, apiKey, tables, report, {
    tableName: PARTNER_INTELLIGENCE_TABLES.extractedFacts,
    description: "Extracted facts pending human review. Never auto-published to Explorer.",
    buildFields: ({ brandBasicsId, operatorMasterId, sourceLibraryId: srcId }) =>
      buildExtractedFactsFields(brandBasicsId, operatorMasterId, srcId || "tblPLACEHOLDER"),
    dependsOn: () => ({
      brandBasicsId: brandBasics.id,
      operatorMasterId: operatorMaster.id,
      sourceLibraryId: sourceLibraryId || "tblPLACEHOLDER",
    }),
  });

  const factsTableId =
    factsTable && factsTable.id
      ? factsTable.id
      : tables.find((t) => t.name === PARTNER_INTELLIGENCE_TABLES.extractedFacts)?.id;

  await ensureTable(baseId, apiKey, tables, report, {
    tableName: PARTNER_INTELLIGENCE_TABLES.publishedFields,
    description: "Human-approved Explorer field values. Read-merge into Brand/Operator Explorer APIs.",
    buildFields: ({ brandBasicsId, operatorMasterId, sourceLibraryId: srcId, factsTableId: fId }) =>
      buildPublishedFields(
        brandBasicsId,
        operatorMasterId,
        srcId || "tblPLACEHOLDER",
        fId || "tblPLACEHOLDER"
      ),
    dependsOn: () => ({
      brandBasicsId: brandBasics.id,
      operatorMasterId: operatorMaster.id,
      sourceLibraryId: sourceLibraryId || "tblPLACEHOLDER",
      factsTableId: factsTableId || "tblPLACEHOLDER",
    }),
  });

  await ensureTable(baseId, apiKey, tables, report, {
    tableName: PARTNER_INTELLIGENCE_TABLES.helenaIntake,
    description: "Helena AI outreach material requests and receipts.",
    buildFields: ({ brandBasicsId, operatorMasterId, sourceLibraryId: srcId }) =>
      buildHelenaIntakeFields(brandBasicsId, operatorMasterId, srcId || "tblPLACEHOLDER"),
    dependsOn: () => ({
      brandBasicsId: brandBasics.id,
      operatorMasterId: operatorMaster.id,
      sourceLibraryId: sourceLibraryId || "tblPLACEHOLDER",
    }),
  });

  const outPath = path.join(ROOT, "reports", "ensure-partner-intelligence-tables.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("Wrote", outPath);
  console.log(
    APPLY
      ? "Apply complete. Verify tables in Airtable, then proceed to Phase 3 Source Library API."
      : "Dry-run complete. Re-run with --apply to create missing tables/fields."
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
