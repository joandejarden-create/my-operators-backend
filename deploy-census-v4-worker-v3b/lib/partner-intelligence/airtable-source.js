/**
 * Partner Intelligence — Source Library Airtable read/write helpers.
 */
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";
import {
  PARTNER_INTELLIGENCE_TABLES,
  PARTNER_INTELLIGENCE_FLAGS,
  PARTNER_INTELLIGENCE_LINKS,
  MAP_PARTNER_SOURCE,
} from "../../api/lib/partner-intelligence-field-map.js";
import { cellToString, escapeAirtableFormulaValue, extractLinkedRecordIds } from "../airtable-utils.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..", "..");

export function getPartnerIntelligenceConfig() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const tableName =
    process.env.PARTNER_INTELLIGENCE_SOURCE_TABLE_ID ||
    PARTNER_INTELLIGENCE_TABLES.sourceLibrary;
  return { baseId, apiKey, tableName };
}

export function resolveReferenceRoot() {
  const configured = nz(PARTNER_INTELLIGENCE_FLAGS.referenceRoot);
  if (configured && fs.existsSync(configured)) return configured;
  return path.join(REPO_ROOT, "data", "partner-sources");
}

export function resolveOperatorReferenceRoot() {
  const configured = nz(PARTNER_INTELLIGENCE_FLAGS.operatorReferenceRoot);
  if (configured && fs.existsSync(configured)) return configured;
  const brandRoot = resolveReferenceRoot();
  const sibling = path.join(path.dirname(brandRoot), "Operator Reference Material");
  if (fs.existsSync(sibling)) return sibling;
  return path.join(REPO_ROOT, "data", "operator-sources");
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function airtableUrl(baseId, tableName, recordId) {
  const table = encodeURIComponent(tableName);
  if (recordId) {
    return `https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(recordId)}`;
  }
  return `https://api.airtable.com/v0/${baseId}/${table}`;
}

async function airtableFetch(url, apiKey, init = {}) {
  const res = await fetch(url, {
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

export function normalizePartnerSourceRecord(record) {
  const f = record.fields || {};
  const attachments = f[MAP_PARTNER_SOURCE.sourceFile];
  return {
    id: record.id,
    profileType: cellToString(f[MAP_PARTNER_SOURCE.profileType]),
    parentCompanyId: extractLinkedRecordIds(f[MAP_PARTNER_SOURCE.parentCompany])[0] || null,
    brandId: extractLinkedRecordIds(f[MAP_PARTNER_SOURCE.brand])[0] || null,
    operatorId: extractLinkedRecordIds(f[MAP_PARTNER_SOURCE.operator])[0] || null,
    region: cellToString(f[MAP_PARTNER_SOURCE.region]),
    countryMarket: cellToString(f[MAP_PARTNER_SOURCE.countryMarket]),
    sourceTitle: cellToString(f[MAP_PARTNER_SOURCE.sourceTitle]),
    sourceType: cellToString(f[MAP_PARTNER_SOURCE.sourceType]),
    sourceUrl: cellToString(f[MAP_PARTNER_SOURCE.sourceUrl]),
    sourceFile:
      Array.isArray(attachments) && attachments.length
        ? attachments.map((a) => ({
            id: a.id,
            url: a.url,
            filename: a.filename,
            size: a.size,
            type: a.type,
          }))
        : [],
    fileType: cellToString(f[MAP_PARTNER_SOURCE.fileType]),
    sourceDate: cellToString(f[MAP_PARTNER_SOURCE.sourceDate]),
    captureDate: cellToString(f[MAP_PARTNER_SOURCE.captureDate]),
    sourceOrigin: cellToString(f[MAP_PARTNER_SOURCE.sourceOrigin]),
    visibility: cellToString(f[MAP_PARTNER_SOURCE.visibility]),
    verifiedSource: cellToString(f[MAP_PARTNER_SOURCE.verifiedSource]),
    sourceQuality: cellToString(f[MAP_PARTNER_SOURCE.sourceQuality]),
    status: cellToString(f[MAP_PARTNER_SOURCE.status]),
    notes: cellToString(f[MAP_PARTNER_SOURCE.notes]),
    lastReviewed: cellToString(f[MAP_PARTNER_SOURCE.lastReviewed]),
    approvedForExtraction: cellToString(f[MAP_PARTNER_SOURCE.approvedForExtraction]),
    approvedForExplorerUse: cellToString(f[MAP_PARTNER_SOURCE.approvedForExplorerUse]),
    confidentialityNotes: cellToString(f[MAP_PARTNER_SOURCE.confidentialityNotes]),
    permissionVisibilityNotes: cellToString(f[MAP_PARTNER_SOURCE.permissionVisibilityNotes]),
    relatedContactId: extractLinkedRecordIds(f[MAP_PARTNER_SOURCE.relatedContact])[0] || null,
    extractionRunId: cellToString(f[MAP_PARTNER_SOURCE.extractionRunId]),
    duplicateOfId: extractLinkedRecordIds(f[MAP_PARTNER_SOURCE.duplicateOf])[0] || null,
    localFilePath: cellToString(f[MAP_PARTNER_SOURCE.localFilePath]),
    createdTime: record.createdTime || null,
  };
}

export async function fetchLinkedPrimaryName(baseId, apiKey, tableName, recordId, fieldCandidates) {
  const url = airtableUrl(baseId, tableName, recordId);
  const { res, json } = await airtableFetch(url, apiKey);
  if (!res.ok) return null;
  const fields = json.fields || {};
  for (const key of fieldCandidates) {
    const value = cellToString(fields[key]);
    if (value) return value;
  }
  return null;
}

/**
 * Linked-record filters compare to the linked row primary field text, not record ids
 * (same pattern as brand-library presentation rows).
 */
async function resolveListQueryLinks(query) {
  const { baseId, apiKey } = getPartnerIntelligenceConfig();
  const resolved = { ...query };

  if (query.operatorId && /^rec[a-zA-Z0-9]+$/.test(query.operatorId)) {
    resolved.operatorLinkName = await fetchLinkedPrimaryName(
      baseId,
      apiKey,
      PARTNER_INTELLIGENCE_LINKS.operatorMaster,
      query.operatorId,
      ["company_name", "Company Name", "Operator Name"]
    );
  }

  if (query.brandId && /^rec[a-zA-Z0-9]+$/.test(query.brandId)) {
    resolved.brandLinkName = await fetchLinkedPrimaryName(
      baseId,
      apiKey,
      PARTNER_INTELLIGENCE_LINKS.brandBasics,
      query.brandId,
      ["Brand Name", "brand_name"]
    );
  }

  return resolved;
}

function buildListFormula(resolved) {
  const parts = [];
  if (resolved.operatorLinkName) {
    parts.push(
      `{${MAP_PARTNER_SOURCE.operator}} = '${escapeAirtableFormulaValue(resolved.operatorLinkName)}'`
    );
  }
  if (resolved.brandLinkName) {
    parts.push(
      `{${MAP_PARTNER_SOURCE.brand}} = '${escapeAirtableFormulaValue(resolved.brandLinkName)}'`
    );
  }
  if (resolved.profileType) {
    parts.push(
      `{${MAP_PARTNER_SOURCE.profileType}} = '${escapeAirtableFormulaValue(resolved.profileType)}'`
    );
  }
  if (resolved.status) {
    parts.push(`{${MAP_PARTNER_SOURCE.status}} = '${escapeAirtableFormulaValue(resolved.status)}'`);
  }
  if (parts.length === 0) return null;
  return parts.length === 1 ? parts[0] : `AND(${parts.join(", ")})`;
}

export async function listPartnerSources(query = {}) {
  const { baseId, apiKey, tableName } = getPartnerIntelligenceConfig();
  if (!baseId || !apiKey) {
    throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  }

  const params = new URLSearchParams();
  params.set("pageSize", String(Math.min(Number(query.limit) || 100, 100)));
  const resolvedQuery = await resolveListQueryLinks(query);
  const formula = buildListFormula(resolvedQuery);
  if (formula) params.set("filterByFormula", formula);
  if (query.offset) params.set("offset", query.offset);

  const url = `${airtableUrl(baseId, tableName)}?${params.toString()}`;
  const { res, json } = await airtableFetch(url, apiKey);
  if (!res.ok) {
    const msg = json.error?.message || JSON.stringify(json);
    throw new Error(`Airtable list failed: ${msg}`);
  }

  return {
    sources: (json.records || []).map(normalizePartnerSourceRecord),
    offset: json.offset || null,
  };
}

export async function getPartnerSourceById(recordId) {
  const { baseId, apiKey, tableName } = getPartnerIntelligenceConfig();
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  if (!/^rec[a-zA-Z0-9]+$/.test(recordId)) throw new Error("Invalid record id");

  const url = airtableUrl(baseId, tableName, recordId);
  const { res, json } = await airtableFetch(url, apiKey);
  if (res.status === 404) return null;
  if (!res.ok) {
    throw new Error(json.error?.message || "Airtable read failed");
  }
  return normalizePartnerSourceRecord(json);
}

export async function createPartnerSource(fields) {
  const { baseId, apiKey, tableName } = getPartnerIntelligenceConfig();
  const url = airtableUrl(baseId, tableName);
  const { res, json } = await airtableFetch(url, apiKey, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!res.ok) {
    throw new Error(json.error?.message || "Airtable create failed");
  }
  return normalizePartnerSourceRecord(json);
}

export async function patchPartnerSource(recordId, fields) {
  const { baseId, apiKey, tableName } = getPartnerIntelligenceConfig();
  const url = airtableUrl(baseId, tableName, recordId);
  const { res, json } = await airtableFetch(url, apiKey, {
    method: "PATCH",
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!res.ok) {
    throw new Error(json.error?.message || "Airtable patch failed");
  }
  return normalizePartnerSourceRecord(json);
}

export function sanitizeFolderName(name) {
  return nz(name).replace(/[<>:"/\\|?*]/g, "_").replace(/\s+/g, " ").trim() || "inbox";
}

export function resolveUploadFolderForSource(source, referenceFolderOverride) {
  const folder =
    sanitizeFolderName(referenceFolderOverride) ||
    (source?.operatorId === "recF5Z87OAqFgndoq" ? "Arbor Lodging" : "inbox");
  return path.join(resolveReferenceRoot(), folder);
}

export function relativeLocalFilePath(referenceFolder, filename) {
  return `${sanitizeFolderName(referenceFolder)}/${filename}`.replace(/\\/g, "/");
}
