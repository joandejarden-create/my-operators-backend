/**
 * Acquisition Intelligence — Airtable apply layer (GTM base).
 *
 * Dry-run safe by default when opts.dryRun !== false for CLI;
 * API passes dryRun:false after admin confirmation.
 */

import {
  GTM_ACQUISITION_RELATIONSHIPS_TABLE,
  GTM_ACQUISITION_IMPORT_BATCHES_TABLE,
  MAP_ACQUISITION_RELATIONSHIP as R,
  MAP_ACQUISITION_IMPORT_BATCH as B,
} from "./field-map.js";
import { GTM_CONTACT_TABLE, MAP_GTM_CONTACT } from "../gtm-owner-target/contact-field-map.js";
import {
  assertGtmBaseConfigured,
  assertNotProductBase,
  getGtmAirtableBase,
} from "../gtm-owner-target/platform-base.js";
import { parseLinkedInConnectionsCsv } from "./linkedin-connections-parse.js";
import { buildLinkedInConnectionsPreview, assertPreviewImportable } from "./linkedin-connections-preview.js";
import {
  buildAcquisitionImportPlan,
  buildImportBatchFieldsFromPlan,
  getAcquisitionImportFieldMappingSnapshot,
} from "./import-plan.js";
import {
  buildRelationshipDedupeKey,
  buildAcquisitionContactDedupeKey,
  normalizeLinkedInProfileUrl,
} from "./linkedin-identity.js";

async function listAllRecords(base, tableName, fields) {
  const out = [];
  await base(tableName)
    .select({
      pageSize: 100,
      fields: fields || undefined,
    })
    .eachPage((records, next) => {
      for (const rec of records) {
        out.push({ id: rec.id, fields: rec.fields || {} });
      }
      next();
    });
  return out;
}

/**
 * Load existing Contacts + user-scoped relationships for idempotent import.
 * @param {string} sourceUserId
 */
export async function loadAcquisitionImportIndexes(sourceUserId) {
  assertGtmBaseConfigured();
  const base = getGtmAirtableBase();
  assertNotProductBase(assertGtmBaseConfigured().baseId);

  const contactFields = [
    MAP_GTM_CONTACT.name,
    MAP_GTM_CONTACT.email,
    MAP_GTM_CONTACT.linkedIn,
    MAP_GTM_CONTACT.company,
    MAP_GTM_CONTACT.title,
    MAP_GTM_CONTACT.contactDedupeKey,
    MAP_GTM_CONTACT.sourceFile,
    MAP_GTM_CONTACT.internalNotes,
    MAP_GTM_CONTACT.verificationTier,
    MAP_GTM_CONTACT.outreachStatus,
  ];

  const relFields = Object.values(R);

  const [contacts, relationships] = await Promise.all([
    listAllRecords(base, GTM_CONTACT_TABLE, contactFields),
    listAllRecords(base, GTM_ACQUISITION_RELATIONSHIPS_TABLE, relFields),
  ]);

  const existingContactsByDedupeKey = new Map();
  const existingContactsByLinkedIn = new Map();
  for (const c of contacts) {
    const dedupe = String(c.fields[MAP_GTM_CONTACT.contactDedupeKey] || "").trim();
    const li = normalizeLinkedInProfileUrl(c.fields[MAP_GTM_CONTACT.linkedIn] || "");
    if (dedupe) existingContactsByDedupeKey.set(dedupe, c);
    if (li) existingContactsByLinkedIn.set(li, c);
  }

  const existingRelationshipsByDedupeKey = new Map();
  const uid = String(sourceUserId || "").trim();
  for (const rel of relationships) {
    if (String(rel.fields[R.sourceUserId] || "").trim() !== uid) continue;
    const key =
      String(rel.fields[R.relationshipDedupeKey] || "").trim() ||
      buildRelationshipDedupeKey(uid, {
        linkedInUrl: rel.fields[R.linkedInUrl],
        firstName: rel.fields[R.firstName],
        lastName: rel.fields[R.lastName],
        company: rel.fields[R.company],
      });
    if (key) existingRelationshipsByDedupeKey.set(key, rel);
  }

  return {
    existingContactsByDedupeKey,
    existingContactsByLinkedIn,
    existingRelationshipsByDedupeKey,
    contactCount: contacts.length,
    relationshipCountForUser: existingRelationshipsByDedupeKey.size,
  };
}

async function createRecords(base, tableName, fieldSets) {
  const created = [];
  for (let i = 0; i < fieldSets.length; i += 10) {
    const chunk = fieldSets.slice(i, i + 10).map((fields) => ({ fields }));
    const rows = await base(tableName).create(chunk);
    for (const row of rows) created.push({ id: row.id, fields: row.fields });
  }
  return created;
}

async function updateRecords(base, tableName, patches) {
  const updated = [];
  for (let i = 0; i < patches.length; i += 10) {
    const chunk = patches.slice(i, i + 10).map((p) => ({ id: p.id, fields: p.patch }));
    const rows = await base(tableName).update(chunk);
    for (const row of rows) updated.push({ id: row.id, fields: row.fields });
  }
  return updated;
}

/**
 * Preview-only path (no Airtable writes).
 * @param {string} csvText
 * @param {{ fileName?: string, sourceUserId?: string }} opts
 */
export function previewLinkedInConnectionsImport(csvText, opts = {}) {
  const parsed = parseLinkedInConnectionsCsv(csvText, { fileName: opts.fileName });
  const preview = buildLinkedInConnectionsPreview(parsed);
  return {
    ...preview,
    sourceUserId: opts.sourceUserId || null,
    fieldMapping: getAcquisitionImportFieldMappingSnapshot(),
    parsedRows: parsed.ok ? parsed.rows.filter((r) => !r.duplicateOfRow) : [],
    invalidRows: parsed.invalidRows || [],
  };
}

/**
 * Apply import for a user. Requires GTM base.
 * @param {string} csvText
 * @param {{
 *   sourceUserId: string,
 *   fileName?: string,
 *   dryRun?: boolean,
 *   previewReportPath?: string,
 * }} opts
 */
export async function applyLinkedInConnectionsImport(csvText, opts) {
  const sourceUserId = String(opts?.sourceUserId || "").trim();
  if (!sourceUserId) {
    return {
      ok: false,
      validation: { pass: false, failedChecks: ["missing_source_user_id"] },
      error: "missing_source_user_id",
      message: "sourceUserId is required for Acquisition Intelligence imports.",
    };
  }

  const preview = previewLinkedInConnectionsImport(csvText, {
    fileName: opts.fileName,
    sourceUserId,
  });
  const gate = assertPreviewImportable(preview);
  if (!gate.ok) {
    return {
      ok: false,
      validation: { pass: false, failedChecks: gate.failedChecks },
      error: gate.error,
      message: gate.message,
      preview,
    };
  }

  const indexes = await loadAcquisitionImportIndexes(sourceUserId);
  const plan = buildAcquisitionImportPlan(preview.parsedRows, sourceUserId, {
    ...indexes,
    sourceFileName: opts.fileName || preview.fileName,
    importedAt: new Date().toISOString(),
  });

  const batchFields = buildImportBatchFieldsFromPlan(
    plan,
    preview.stats,
    sourceUserId,
    opts.fileName || preview.fileName
  );
  if (opts.previewReportPath) {
    batchFields[B.previewReportPath] = opts.previewReportPath;
  }

  const sanitizedPreview = {
    stats: preview.stats,
    planSummary: plan.summary,
    sampleCreates: plan.toCreateRelationships.slice(0, 5).map((r) => ({
      name: r.row.displayName,
      company: r.row.company,
      linkedInUrl: r.row.linkedInUrl,
      dedupeKey: r.relationshipDedupeKey,
    })),
  };

  // dryRun defaults to true unless explicitly false (CLI/API safety)
  const isDryRun = opts.dryRun !== false;

  if (isDryRun) {
    return {
      ok: true,
      dryRun: true,
      validation: { pass: true, failedChecks: [] },
      sanitizedPayloadPreview: sanitizedPreview,
      fieldMapping: getAcquisitionImportFieldMappingSnapshot(),
      planSummary: plan.summary,
      batchFieldsPreview: batchFields,
      preview,
    };
  }

  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  // 1) Import batch audit row first
  const [batchRow] = await createRecords(base, GTM_ACQUISITION_IMPORT_BATCHES_TABLE, [batchFields]);
  const importBatchId = batchRow.id;

  // 2) Create contacts
  const createdContactIdByPlanKey = new Map();
  if (plan.toCreateContacts.length) {
    const created = await createRecords(
      base,
      GTM_CONTACT_TABLE,
      plan.toCreateContacts.map((c) => c.fields)
    );
    created.forEach((rec, i) => {
      const item = plan.toCreateContacts[i];
      createdContactIdByPlanKey.set(item.planKey, rec.id);
      const li = normalizeLinkedInProfileUrl(rec.fields[MAP_GTM_CONTACT.linkedIn] || "");
      const dedupe = String(rec.fields[MAP_GTM_CONTACT.contactDedupeKey] || "");
      if (li) indexes.existingContactsByLinkedIn.set(li, rec);
      if (dedupe) indexes.existingContactsByDedupeKey.set(dedupe, rec);
    });
  }

  // 3) Update contacts
  if (plan.toUpdateContacts.length) {
    await updateRecords(base, GTM_CONTACT_TABLE, plan.toUpdateContacts);
  }

  // Resolve contact ids for relationships
  function resolveContactId(contactRef, row) {
    if (contactRef?.id) return contactRef.id;
    if (contactRef?.planKey && createdContactIdByPlanKey.has(contactRef.planKey)) {
      return createdContactIdByPlanKey.get(contactRef.planKey);
    }
    const li = normalizeLinkedInProfileUrl(row.linkedInUrl || "");
    if (li && indexes.existingContactsByLinkedIn.get(li)) {
      return indexes.existingContactsByLinkedIn.get(li).id;
    }
    const dedupe = buildAcquisitionContactDedupeKey(row);
    if (dedupe && indexes.existingContactsByDedupeKey.get(dedupe)) {
      return indexes.existingContactsByDedupeKey.get(dedupe).id;
    }
    return null;
  }

  // 4) Create relationships
  const createRelPayloads = plan.toCreateRelationships.map((item) => {
    const contactId = resolveContactId(item.contactRef, item.row);
    const fields = {
      ...item.fields,
      [R.importBatch]: [importBatchId],
    };
    if (contactId) fields[R.contact] = [contactId];
    return fields;
  });
  const createdRels = createRelPayloads.length
    ? await createRecords(base, GTM_ACQUISITION_RELATIONSHIPS_TABLE, createRelPayloads)
    : [];

  // 5) Update relationships
  const updateRelPayloads = plan.toUpdateRelationships.map((item) => {
    const contactId = resolveContactId(item.contactRef, item.row);
    const patch = {
      ...item.patch,
      [R.importBatch]: [importBatchId],
    };
    if (contactId && !item.patch[R.contact]) {
      // keep existing contact link; only set if missing on record — apply layer does not have full fields check here
    }
    return { id: item.id, patch };
  });
  const updatedRels = updateRelPayloads.length
    ? await updateRecords(base, GTM_ACQUISITION_RELATIONSHIPS_TABLE, updateRelPayloads)
    : [];

  return {
    ok: true,
    dryRun: false,
    validation: { pass: true, failedChecks: [] },
    importBatchId,
    sanitizedPayloadPreview: sanitizedPreview,
    fieldMapping: getAcquisitionImportFieldMappingSnapshot(),
    planSummary: plan.summary,
    created: {
      contacts: plan.toCreateContacts.length,
      relationships: createdRels.length,
    },
    updated: {
      contacts: plan.toUpdateContacts.length,
      relationships: updatedRels.length,
    },
    skipped: plan.skipped.length,
    preview,
    errorHandling: {
      validationError: "Client receives failedChecks; no Airtable write.",
      apiError: "Thrown/returned from Airtable SDK; batch may be Partially Applied — check batch status.",
      network: "Retry import; idempotent dedupe keys prevent duplicates.",
    },
  };
}

/**
 * List import batches for a user (newest first).
 * @param {string} sourceUserId
 * @param {{ limit?: number }} [opts]
 */
export async function listAcquisitionImportBatchesForUser(sourceUserId, opts = {}) {
  const uid = String(sourceUserId || "").trim();
  if (!uid) return { ok: false, error: "missing_source_user_id", batches: [] };

  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();
  const limit = Math.min(Math.max(Number(opts.limit) || 25, 1), 100);

  const formula = `{${B.sourceUserId}} = "${uid.replace(/"/g, '\\"')}"`;
  const batches = [];
  await base(GTM_ACQUISITION_IMPORT_BATCHES_TABLE)
    .select({
      filterByFormula: formula,
      sort: [{ field: B.importedAt, direction: "desc" }],
      maxRecords: limit,
    })
    .eachPage((records, next) => {
      for (const rec of records) {
        batches.push({
          id: rec.id,
          batchLabel: rec.fields[B.batchLabel] || "",
          sourceFileName: rec.fields[B.sourceFileName] || "",
          importedAt: rec.fields[B.importedAt] || null,
          status: rec.fields[B.status] || "",
          rowsDetected: rec.fields[B.rowsDetected] ?? null,
          createdCount: rec.fields[B.createdCount] ?? null,
          updatedCount: rec.fields[B.updatedCount] ?? null,
          skippedCount: rec.fields[B.skippedCount] ?? null,
          invalidCount: rec.fields[B.invalidCount] ?? null,
          duplicateCount: rec.fields[B.duplicateCount] ?? null,
        });
      }
      next();
    });

  return { ok: true, batches };
}

/**
 * Count relationships for user (Stage 1 metrics).
 * @param {string} sourceUserId
 */
export async function getAcquisitionNetworkSummaryForUser(sourceUserId) {
  const uid = String(sourceUserId || "").trim();
  if (!uid) return { ok: false, error: "missing_source_user_id" };

  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  let total = 0;
  const formula = `AND({${R.sourceUserId}} = "${uid.replace(/"/g, '\\"')}", {${R.status}} = "Active")`;
  await base(GTM_ACQUISITION_RELATIONSHIPS_TABLE)
    .select({ filterByFormula: formula, fields: [R.relationshipName], pageSize: 100 })
    .eachPage((records, next) => {
      total += records.length;
      next();
    });

  return {
    ok: true,
    metrics: {
      totalConnections: total,
      relevantConnections: null,
      directProspects: null,
      highValueConnectors: null,
      researchQueue: null,
      founderPriority: null,
      note: "Stage 1 — only totalConnections populated; classification metrics arrive in Stage 2+.",
    },
  };
}
