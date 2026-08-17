/**
 * Build idempotent import plan for Acquisition Network Relationships + Contacts.
 *
 * Existing indexes are injected by the apply layer (Airtable lookups).
 * Pure function — safe for unit tests without network.
 */

import {
  MAP_ACQUISITION_RELATIONSHIP as R,
  MAP_ACQUISITION_IMPORT_BATCH as B,
  SOURCE_LINKEDIN_CONNECTIONS_EXPORT,
  VAL_RELATIONSHIP_STRENGTH,
  VAL_ACQUISITION_ROLE,
  VAL_ACQUISITION_RELATIONSHIP_STATUS,
  VAL_RESEARCH_STATUS,
  VAL_ACQUISITION_VISIBILITY,
} from "./field-map.js";
import {
  buildAcquisitionContactDedupeKey,
  buildRelationshipDedupeKey,
  formatPersonDisplayName,
  normalizeLinkedInProfileUrl,
} from "./linkedin-identity.js";
import { MAP_GTM_CONTACT } from "../gtm-owner-target/contact-field-map.js";

/**
 * Only write non-blank LinkedIn-derived values; never clear existing manual/research fields.
 * @param {Record<string, unknown>} existingFields
 * @param {Record<string, unknown>} incomingFields
 * @param {string[]} allowedKeys
 */
export function mergeNonBlankFields(existingFields, incomingFields, allowedKeys) {
  /** @type {Record<string, unknown>} */
  const patch = {};
  for (const key of allowedKeys) {
    const next = incomingFields[key];
    if (next == null || next === "") continue;
    const prev = existingFields?.[key];
    if (prev == null || prev === "" || String(prev) !== String(next)) {
      patch[key] = next;
    }
  }
  return patch;
}

/**
 * @param {object} row - parsed connection row
 * @param {string} sourceUserId
 * @param {{ importBatchId?: string, sourceFileName?: string, importedAt?: string }} meta
 */
export function buildRelationshipFieldsFromRow(row, sourceUserId, meta = {}) {
  const displayName = row.displayName || formatPersonDisplayName(row.firstName, row.lastName);
  const linkedInUrl = normalizeLinkedInProfileUrl(row.linkedInUrl || row.linkedInUrlRaw || "");
  const dedupeKey = buildRelationshipDedupeKey(sourceUserId, {
    linkedInUrl,
    firstName: row.firstName,
    lastName: row.lastName,
    company: row.company,
  });

  /** @type {Record<string, unknown>} */
  const fields = {
    [R.relationshipName]: displayName,
    [R.sourceUserId]: String(sourceUserId || "").trim(),
    [R.linkedInUrl]: linkedInUrl || undefined,
    [R.firstName]: row.firstName || undefined,
    [R.lastName]: row.lastName || undefined,
    [R.email]: row.email || undefined,
    [R.company]: row.company || undefined,
    [R.position]: row.position || undefined,
    [R.connectedOn]: row.connectedOn || undefined,
    [R.relationshipStrength]: VAL_RELATIONSHIP_STRENGTH[0], // Unknown
    [R.acquisitionRole]: VAL_ACQUISITION_ROLE[0], // Unclassified
    [R.importSource]: SOURCE_LINKEDIN_CONNECTIONS_EXPORT,
    [R.ingestionMethod]: "CSV_EXPORT",
    [R.sourceFileName]: meta.sourceFileName || undefined,
    [R.importedAt]: meta.importedAt || new Date().toISOString(),
    [R.lastLinkedInSyncAt]: meta.importedAt || new Date().toISOString(),
    [R.status]: VAL_ACQUISITION_RELATIONSHIP_STATUS[0],
    [R.researchStatus]: VAL_RESEARCH_STATUS[0],
    [R.relationshipDedupeKey]: dedupeKey,
    [R.visibility]: VAL_ACQUISITION_VISIBILITY[0],
  };

  if (meta.importBatchId) {
    fields[R.importBatch] = [meta.importBatchId];
  }

  // Strip undefined
  for (const k of Object.keys(fields)) {
    if (fields[k] === undefined) delete fields[k];
  }
  return fields;
}

/**
 * Contact create/update payload from LinkedIn row (LinkedIn-derived fields only).
 * @param {object} row
 * @param {string} [sourceFileName]
 */
export function buildContactFieldsFromLinkedInRow(row, sourceFileName = "") {
  const name = row.displayName || formatPersonDisplayName(row.firstName, row.lastName);
  const linkedInUrl = normalizeLinkedInProfileUrl(row.linkedInUrl || row.linkedInUrlRaw || "");
  const dedupeKey = buildAcquisitionContactDedupeKey({
    linkedInUrl,
    email: row.email,
    firstName: row.firstName,
    lastName: row.lastName,
    company: row.company,
  });

  /** @type {Record<string, unknown>} */
  const fields = {
    [MAP_GTM_CONTACT.name]: name,
    [MAP_GTM_CONTACT.linkedIn]: linkedInUrl || undefined,
    [MAP_GTM_CONTACT.email]: row.email || undefined,
    [MAP_GTM_CONTACT.company]: row.company || undefined,
    [MAP_GTM_CONTACT.title]: row.position || undefined,
    [MAP_GTM_CONTACT.contactDedupeKey]: dedupeKey || undefined,
    [MAP_GTM_CONTACT.sourceFile]: sourceFileName || undefined,
  };

  for (const k of Object.keys(fields)) {
    if (fields[k] === undefined || fields[k] === "") delete fields[k];
  }
  return fields;
}

/** Fields allowed to refresh on Contact re-import (never Notes / verification / outreach). */
export const CONTACT_LINKEDIN_REFRESH_FIELDS = [
  MAP_GTM_CONTACT.name,
  MAP_GTM_CONTACT.linkedIn,
  MAP_GTM_CONTACT.email,
  MAP_GTM_CONTACT.company,
  MAP_GTM_CONTACT.title,
  MAP_GTM_CONTACT.contactDedupeKey,
  MAP_GTM_CONTACT.sourceFile,
];

/** Relationship fields refreshed from LinkedIn CSV on re-import. */
export const RELATIONSHIP_LINKEDIN_REFRESH_FIELDS = [
  R.relationshipName,
  R.linkedInUrl,
  R.firstName,
  R.lastName,
  R.email,
  R.company,
  R.position,
  R.connectedOn,
  R.sourceFileName,
  R.lastLinkedInSyncAt,
  R.importBatch,
];

/**
 * @param {object[]} uniqueRows - parsed rows without intra-file duplicates
 * @param {string} sourceUserId
 * @param {{
 *   existingRelationshipsByDedupeKey?: Map<string, { id: string, fields: Record<string, unknown> }>,
 *   existingContactsByDedupeKey?: Map<string, { id: string, fields: Record<string, unknown> }>,
 *   existingContactsByLinkedIn?: Map<string, { id: string, fields: Record<string, unknown> }>,
 *   sourceFileName?: string,
 *   importedAt?: string,
 * }} [index]
 */
export function buildAcquisitionImportPlan(uniqueRows, sourceUserId, index = {}) {
  const relIndex = index.existingRelationshipsByDedupeKey || new Map();
  const contactByDedupe = index.existingContactsByDedupeKey || new Map();
  const contactByLinkedIn = index.existingContactsByLinkedIn || new Map();
  const sourceFileName = index.sourceFileName || "";
  const importedAt = index.importedAt || new Date().toISOString();

  const toCreateRelationships = [];
  const toUpdateRelationships = [];
  const toCreateContacts = [];
  const toUpdateContacts = [];
  const skipped = [];
  const invalid = [];

  /** Track contacts we plan to create in this batch to avoid duplicates. */
  const plannedContactKeys = new Set();

  for (const row of uniqueRows) {
    const relKey = buildRelationshipDedupeKey(sourceUserId, row);
    if (!relKey) {
      invalid.push({ row, reason: "missing_relationship_dedupe_key" });
      continue;
    }

    const contactFields = buildContactFieldsFromLinkedInRow(row, sourceFileName);
    const contactDedupe = String(contactFields[MAP_GTM_CONTACT.contactDedupeKey] || "");
    const linkedIn = normalizeLinkedInProfileUrl(row.linkedInUrl || "");

    let contactRef =
      (linkedIn && contactByLinkedIn.get(linkedIn)) ||
      (contactDedupe && contactByDedupe.get(contactDedupe)) ||
      null;

    if (!contactRef) {
      const planKey = contactDedupe || linkedIn || relKey;
      if (!plannedContactKeys.has(planKey)) {
        plannedContactKeys.add(planKey);
        toCreateContacts.push({
          planKey,
          fields: {
            ...contactFields,
            [MAP_GTM_CONTACT.outreachStatus]: "not_contacted",
          },
          row,
        });
      }
      contactRef = { id: null, planKey, fields: contactFields };
    } else {
      const patch = mergeNonBlankFields(
        contactRef.fields || {},
        contactFields,
        CONTACT_LINKEDIN_REFRESH_FIELDS
      );
      if (Object.keys(patch).length) {
        toUpdateContacts.push({
          id: contactRef.id,
          patch,
          row,
        });
      }
    }

    const relFields = buildRelationshipFieldsFromRow(row, sourceUserId, {
      sourceFileName,
      importedAt,
    });

    const existingRel = relIndex.get(relKey);
    if (existingRel) {
      const patch = mergeNonBlankFields(
        existingRel.fields || {},
        {
          ...relFields,
          // Preserve manual relationship strength / notes / acquisition role on re-import
        },
        RELATIONSHIP_LINKEDIN_REFRESH_FIELDS
      );
      // Always bump last sync timestamp when LinkedIn-derived data present
      patch[R.lastLinkedInSyncAt] = importedAt;
      if (Object.keys(patch).length <= 1 && !hasMaterialContactChange(patch)) {
        skipped.push({ row, reason: "unchanged", relationshipId: existingRel.id });
      } else {
        toUpdateRelationships.push({
          id: existingRel.id,
          patch,
          row,
          contactRef,
        });
      }
    } else {
      toCreateRelationships.push({
        fields: relFields,
        row,
        contactRef,
        relationshipDedupeKey: relKey,
      });
    }
  }

  return {
    sourceUserId,
    sourceFileName,
    importedAt,
    toCreateContacts,
    toUpdateContacts,
    toCreateRelationships,
    toUpdateRelationships,
    skipped,
    invalid,
    summary: {
      createContacts: toCreateContacts.length,
      updateContacts: toUpdateContacts.length,
      createRelationships: toCreateRelationships.length,
      updateRelationships: toUpdateRelationships.length,
      skipped: skipped.length,
      invalid: invalid.length,
    },
  };
}

function hasMaterialContactChange(patch) {
  return Object.keys(patch).some((k) => k !== R.lastLinkedInSyncAt);
}

/**
 * @param {object} plan
 * @param {object} previewStats
 * @param {string} sourceUserId
 * @param {string} fileName
 */
export function buildImportBatchFieldsFromPlan(plan, previewStats, sourceUserId, fileName) {
  const now = plan.importedAt || new Date().toISOString();
  return {
    [B.batchLabel]: `LinkedIn Connections — ${fileName} — ${now.slice(0, 10)}`,
    [B.sourceUserId]: sourceUserId,
    [B.sourceFileName]: fileName,
    [B.importSource]: SOURCE_LINKEDIN_CONNECTIONS_EXPORT,
    [B.ingestionMethod]: "CSV_EXPORT",
    [B.importedAt]: now,
    [B.status]: "Applied",
    [B.rowsDetected]: previewStats?.connectionsDetected ?? 0,
    [B.createdCount]: plan.summary.createRelationships,
    [B.updatedCount]: plan.summary.updateRelationships,
    [B.skippedCount]: plan.summary.skipped,
    [B.invalidCount]: (previewStats?.invalidRows || 0) + plan.summary.invalid,
    [B.duplicateCount]: previewStats?.potentialDuplicates ?? 0,
    [B.withCompanyCount]: previewStats?.recordsWithCompany ?? 0,
    [B.withPositionCount]: previewStats?.recordsWithPosition ?? 0,
    [B.withLinkedInCount]: previewStats?.recordsWithLinkedInUrl ?? 0,
    [B.withEmailCount]: previewStats?.recordsWithEmail ?? 0,
    [B.earliestConnectedOn]: previewStats?.earliestConnection || undefined,
    [B.latestConnectedOn]: previewStats?.latestConnection || undefined,
    [B.visibility]: "internal_only",
  };
}

/**
 * Field mapping snapshot for write-path audit (dev).
 */
export function getAcquisitionImportFieldMappingSnapshot() {
  return {
    contactTable: "Contacts",
    relationshipTable: "Acquisition Network Relationships",
    importBatchTable: "Acquisition Import Batches",
    contactFields: CONTACT_LINKEDIN_REFRESH_FIELDS,
    relationshipFields: Object.values(R),
    identityPreference: ["LinkedIn URL", "first+last+company"],
    source: SOURCE_LINKEDIN_CONNECTIONS_EXPORT,
  };
}
