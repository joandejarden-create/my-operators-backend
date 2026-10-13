/**
 * Airtable CRUD for MarketAlertStakeholders — Dealality identity only.
 * Validates payloads; never writes Surfe PII fields.
 */

import Airtable from "airtable";
import {
  MARKET_ALERT_STAKEHOLDERS_TABLE,
  map_marketAlertStakeholderFields as F,
  IDENTIFICATION_STATUS,
  CONTACT_LOOKUP_STATUS,
  STAKEHOLDER_CLASS_OPTIONS,
  CONFIDENCE_OPTIONS,
  FORBIDDEN_SURFE_PII_FIELDS,
  stripSurfeContactDetails,
} from "./stakeholder-schema.js";
import { logContactOp } from "./safe-log.js";

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID required");
  return new Airtable({ apiKey }).base(baseId);
}

function tableName() {
  return MARKET_ALERT_STAKEHOLDERS_TABLE;
}

/**
 * Validate stakeholder write — reject any Surfe PII keys.
 */
export function validateStakeholderWrite(fields = {}) {
  const failures = [];
  const sanitized = {};

  for (const banned of FORBIDDEN_SURFE_PII_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(fields, banned) && fields[banned] != null) {
      failures.push(`forbidden_field:${banned}`);
    }
  }
  const lowerKeys = Object.keys(fields).map((k) => k.toLowerCase());
  for (const k of ["email", "phone", "mobile", "provider person id", "surfe person id"]) {
    if (lowerKeys.includes(k)) failures.push(`forbidden_key:${k}`);
  }
  // LinkedIn only allowed with Dealality public-research source (never Surfe)
  const liSource =
    fields[F.linkedinResolutionSource] ||
    fields.linkedinResolutionSource ||
    fields["LinkedIn Resolution Source"];
  const liUrl = fields[F.linkedinUrl] || fields.linkedinUrl || fields["LinkedIn Profile URL"];
  if (liUrl && liSource !== "DEALALITY_PUBLIC_RESEARCH") {
    failures.push("linkedin_requires_dealality_public_research_source");
  }

  if (!fields[F.stakeholderId] && !fields.stakeholderId) {
    failures.push("missing_stakeholder_id");
  }

  const status = fields[F.identificationStatus] || fields.identificationStatus;
  if (status && !Object.values(IDENTIFICATION_STATUS).includes(status)) {
    failures.push(`invalid_identification_status:${status}`);
  }

  const conf = fields[F.confidence] || fields.confidence;
  if (conf && !CONFIDENCE_OPTIONS.includes(conf)) {
    failures.push(`invalid_confidence:${conf}`);
  }

  const cls = fields[F.stakeholderClass] || fields.stakeholderClass;
  if (cls && !STAKEHOLDER_CLASS_OPTIONS.includes(cls)) {
    failures.push(`invalid_stakeholder_class:${cls}`);
  }

  const lookup = fields[F.contactLookupStatus] || fields.contactLookupStatus;
  if (lookup && !Object.values(CONTACT_LOOKUP_STATUS).includes(lookup)) {
    failures.push(`invalid_contact_lookup_status:${lookup}`);
  }

  // Build sanitized Airtable field map from row shape or already-mapped fields
  const row = fields.stakeholderId ? fields : null;
  if (row) {
    Object.assign(sanitized, rowToAirtableFields(row));
  } else {
    for (const [k, v] of Object.entries(fields)) {
      if (FORBIDDEN_SURFE_PII_FIELDS.includes(k)) continue;
      sanitized[k] = v;
    }
  }

  return {
    ok: failures.length === 0,
    failures,
    sanitized,
    fieldMapping: "map_marketAlertStakeholderFields",
  };
}

export function rowToAirtableFields(row = {}) {
  const clean = stripSurfeContactDetails(row);
  const out = {
    [F.stakeholderId]: clean.stakeholderId,
    [F.personName]: clean.personName || undefined,
    [F.jobTitle]: clean.jobTitle || undefined,
    [F.company]: clean.company || clean.companyName || undefined,
    [F.stakeholderClass]: clean.stakeholderClass || undefined,
    [F.relationshipToProject]: clean.relationshipToProject || undefined,
    [F.whyRelevant]: clean.whyRelevant || undefined,
    [F.articleDerived]: clean.articleDerived === true,
    [F.quoted]: clean.quoted === true,
    [F.confidence]: clean.confidence || undefined,
    [F.identificationStatus]: clean.identificationStatus || undefined,
    [F.selectedPrimary]: clean.selectedPrimary === true,
    [F.selectedSecondary]: clean.selectedSecondary === true,
    [F.decisionOpen]: clean.decisionOpen !== false,
    [F.sourceType]: clean.sourceType || undefined,
    [F.identifiedAt]: clean.identifiedAt || undefined,
    [F.updatedAt]: clean.updatedAt || new Date().toISOString(),
    [F.articleMentionCount]:
      clean.articleMentionCount != null ? Number(clean.articleMentionCount) : undefined,
    [F.sourceArticleUrl]: clean.sourceArticleUrl || undefined,
    [F.sourceArticleTitle]: clean.sourceArticleTitle || undefined,
    [F.contactLookupStatus]: clean.contactLookupStatus || CONTACT_LOOKUP_STATUS.NOT_REQUESTED,
  };

  // Dealality-owned LinkedIn only — never copy Surfe linkedinUrl
  if (
    clean.linkedinUrl &&
    clean.linkedinResolutionSource === "DEALALITY_PUBLIC_RESEARCH" &&
    clean.linkedinResolutionConfidence === "HIGH" &&
    clean.linkedinResolutionStatus === "CONFIRMED"
  ) {
    out[F.linkedinUrl] = clean.linkedinUrl;
    out[F.linkedinResolutionConfidence] = "HIGH";
    out[F.linkedinResolutionSource] = "DEALALITY_PUBLIC_RESEARCH";
    out[F.linkedinResolutionStatus] = "CONFIRMED";
  }

  if (clean.alertId) {
    out[F.alert] = [clean.alertId];
  }

  // Drop undefined
  for (const k of Object.keys(out)) {
    if (out[k] === undefined) delete out[k];
  }
  return out;
}

export function airtableRecordToRow(rec) {
  const f = rec.fields || {};
  const alertIds = Array.isArray(f[F.alert]) ? f[F.alert] : [];
  return {
    recordId: rec.id,
    stakeholderId: f[F.stakeholderId] || null,
    alertId: alertIds[0] || null,
    personName: f[F.personName] || null,
    jobTitle: f[F.jobTitle] || null,
    company: f[F.company] || null,
    companyName: f[F.company] || null,
    stakeholderClass: f[F.stakeholderClass] || null,
    relationshipToProject: f[F.relationshipToProject] || null,
    whyRelevant: f[F.whyRelevant] || null,
    articleDerived: f[F.articleDerived] === true,
    quoted: f[F.quoted] === true,
    confidence: f[F.confidence] || null,
    identificationStatus: f[F.identificationStatus] || null,
    selectedPrimary: f[F.selectedPrimary] === true,
    selectedSecondary: f[F.selectedSecondary] === true,
    decisionOpen: f[F.decisionOpen] !== false,
    sourceType: f[F.sourceType] || null,
    identifiedAt: f[F.identifiedAt] || null,
    updatedAt: f[F.updatedAt] || null,
    articleMentionCount: f[F.articleMentionCount] || 0,
    sourceArticleUrl: f[F.sourceArticleUrl] || null,
    sourceArticleTitle: f[F.sourceArticleTitle] || null,
    contactLookupStatus: f[F.contactLookupStatus] || CONTACT_LOOKUP_STATUS.NOT_REQUESTED,
    linkedinUrl: f[F.linkedinUrl] || null,
    linkedinResolutionConfidence: f[F.linkedinResolutionConfidence] || null,
    linkedinResolutionSource: f[F.linkedinResolutionSource] || null,
    linkedinResolutionStatus: f[F.linkedinResolutionStatus] || null,
  };
}

export async function listStakeholdersForAlert(alertId) {
  if (!alertId) return [];
  try {
    const base = getBase();
    // Linked-record ARRAYJOIN returns primary field names (titles), NOT record IDs.
    // Stakeholder ID is keyed as mas_{alertIdLower}__… — filter on that instead.
    const keyNeedle = String(alertId).toLowerCase().replace(/"/g, '\\"');
    const formula = `FIND("${keyNeedle}", LOWER({${F.stakeholderId}}))`;
    const rows = [];
    await base(tableName())
      .select({
        filterByFormula: formula,
        pageSize: 100,
      })
      .eachPage((records, next) => {
        for (const r of records) {
          const row = airtableRecordToRow(r);
          // Defensive: ensure Alert link matches (or stable key prefix)
          if (row.alertId === alertId || String(row.stakeholderId || "").includes(keyNeedle)) {
            rows.push(row);
          }
        }
        next();
      });
    return rows;
  } catch (err) {
    logContactOp("stakeholder_list_failed", {
      message: String(err?.message || err).slice(0, 120),
    });
    return [];
  }
}

export async function findStakeholderByStableId(stakeholderId) {
  if (!stakeholderId) return null;
  try {
    const base = getBase();
    const formula = `{${F.stakeholderId}} = "${String(stakeholderId).replace(/"/g, '\\"')}"`;
    const records = await base(tableName())
      .select({ filterByFormula: formula, maxRecords: 1 })
      .firstPage();
    if (!records.length) return null;
    return airtableRecordToRow(records[0]);
  } catch (err) {
    logContactOp("stakeholder_find_failed", {
      message: String(err?.message || err).slice(0, 120),
    });
    return null;
  }
}

/**
 * Upsert by Stakeholder ID. Never writes Surfe PII.
 */
export async function upsertStakeholder(row, { dryRun = false } = {}) {
  const validation = validateStakeholderWrite(row);
  if (!validation.ok) {
    return {
      ok: false,
      validation,
      sanitizedPreview: null,
      error: "validation_failed",
    };
  }

  const fields = validation.sanitized;
  const preview = { ...fields };
  // Don't log anything that could be PII from Surfe — fields already clean

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      validation,
      sanitizedPreview: preview,
      fieldMapping: validation.fieldMapping,
      action: "would_upsert",
    };
  }

  try {
    const existing = await findStakeholderByStableId(row.stakeholderId);
    const base = getBase();
    if (existing?.recordId) {
      const updated = await base(tableName()).update(existing.recordId, fields);
      return {
        ok: true,
        action: "updated",
        validation,
        sanitizedPreview: preview,
        fieldMapping: validation.fieldMapping,
        row: airtableRecordToRow(updated),
      };
    }
    const created = await base(tableName()).create(fields);
    return {
      ok: true,
      action: "created",
      validation,
      sanitizedPreview: preview,
      fieldMapping: validation.fieldMapping,
      row: airtableRecordToRow(created),
    };
  } catch (err) {
    logContactOp("stakeholder_upsert_failed", {
      message: String(err?.message || err).slice(0, 120),
    });
    return {
      ok: false,
      validation,
      sanitizedPreview: preview,
      error: "airtable_write_failed",
      detail: String(err?.message || err).slice(0, 160),
    };
  }
}

export async function updateStakeholderLookupStatus(stakeholderId, status) {
  if (!Object.values(CONTACT_LOOKUP_STATUS).includes(status)) {
    return { ok: false, error: "invalid_status" };
  }
  const existing = await findStakeholderByStableId(stakeholderId);
  if (!existing?.recordId) return { ok: false, error: "not_found" };
  try {
    const base = getBase();
    const updated = await base(tableName()).update(existing.recordId, {
      [F.contactLookupStatus]: status,
      [F.updatedAt]: new Date().toISOString(),
    });
    return { ok: true, row: airtableRecordToRow(updated) };
  } catch (err) {
    return { ok: false, error: "airtable_write_failed", detail: String(err?.message || err).slice(0, 120) };
  }
}

/**
 * Persist identification result rows (Dealality only).
 */
export async function persistIdentificationResult(identification, { dryRun = false } = {}) {
  const results = [];
  for (const row of identification.stakeholders || []) {
    results.push(await upsertStakeholder(row, { dryRun }));
  }
  return {
    ok: results.every((r) => r.ok),
    surfeCalls: 0,
    results,
  };
}
