/**
 * Partner Intelligence — Extracted Facts + Published Explorer Fields (Airtable).
 */
import {
  PARTNER_INTELLIGENCE_TABLES,
  PARTNER_INTELLIGENCE_LINKS,
  MAP_PARTNER_FACT,
  MAP_PARTNER_PUBLISHED,
} from "../../api/lib/partner-intelligence-field-map.js";
import { cellToString, escapeAirtableFormulaValue, extractLinkedRecordIds } from "../airtable-utils.js";
import { fetchLinkedPrimaryName, getPartnerIntelligenceConfig } from "./airtable-source.js";

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

function factsTableName() {
  return process.env.PARTNER_INTELLIGENCE_FACTS_TABLE_ID || PARTNER_INTELLIGENCE_TABLES.extractedFacts;
}

function publishedTableName() {
  return (
    process.env.PARTNER_INTELLIGENCE_PUBLISHED_TABLE_ID ||
    PARTNER_INTELLIGENCE_TABLES.publishedFields
  );
}

export function normalizePartnerFactRecord(record) {
  const f = record.fields || {};
  return {
    id: record.id,
    profileType: cellToString(f[MAP_PARTNER_FACT.profileType]),
    operatorId: extractLinkedRecordIds(f[MAP_PARTNER_FACT.operator])[0] || null,
    brandId: extractLinkedRecordIds(f[MAP_PARTNER_FACT.brand])[0] || null,
    sourceRecordId: extractLinkedRecordIds(f[MAP_PARTNER_FACT.sourceRecord])[0] || null,
    explorerType: cellToString(f[MAP_PARTNER_FACT.explorerType]),
    explorerSection: cellToString(f[MAP_PARTNER_FACT.explorerSection]),
    fieldName: cellToString(f[MAP_PARTNER_FACT.fieldName]),
    extractedValue: cellToString(f[MAP_PARTNER_FACT.extractedValue]),
    normalizedValue: cellToString(f[MAP_PARTNER_FACT.normalizedValue]),
    evidenceText: cellToString(f[MAP_PARTNER_FACT.evidenceText]),
    pageSectionAnchor: cellToString(f[MAP_PARTNER_FACT.pageSectionAnchor]),
    sourceType: cellToString(f[MAP_PARTNER_FACT.sourceType]),
    sourceQuality: cellToString(f[MAP_PARTNER_FACT.sourceQuality]),
    confidenceScore: f[MAP_PARTNER_FACT.confidenceScore] ?? null,
    confidenceLevel: cellToString(f[MAP_PARTNER_FACT.confidenceLevel]),
    extractionType: cellToString(f[MAP_PARTNER_FACT.extractionType]),
    publicVisibility: cellToString(f[MAP_PARTNER_FACT.publicVisibility]),
    humanReviewStatus: cellToString(f[MAP_PARTNER_FACT.humanReviewStatus]),
    approvedValue: cellToString(f[MAP_PARTNER_FACT.approvedValue]),
    reviewerNotes: cellToString(f[MAP_PARTNER_FACT.reviewerNotes]),
    dataGap: cellToString(f[MAP_PARTNER_FACT.dataGap]),
    followUpQuestion: cellToString(f[MAP_PARTNER_FACT.followUpQuestion]),
    extractionRunId: cellToString(f[MAP_PARTNER_FACT.extractionRunId]),
    lastUpdated: cellToString(f[MAP_PARTNER_FACT.lastUpdated]),
    createdTime: record.createdTime || null,
  };
}

export function normalizePublishedFieldRecord(record) {
  const f = record.fields || {};
  return {
    id: record.id,
    profileType: cellToString(f[MAP_PARTNER_PUBLISHED.profileType]),
    operatorId: extractLinkedRecordIds(f[MAP_PARTNER_PUBLISHED.operator])[0] || null,
    brandId: extractLinkedRecordIds(f[MAP_PARTNER_PUBLISHED.brand])[0] || null,
    supportingFactIds: extractLinkedRecordIds(f[MAP_PARTNER_PUBLISHED.supportingFacts]),
    primarySourceId: extractLinkedRecordIds(f[MAP_PARTNER_PUBLISHED.primarySource])[0] || null,
    explorerType: cellToString(f[MAP_PARTNER_PUBLISHED.explorerType]),
    explorerSection: cellToString(f[MAP_PARTNER_PUBLISHED.explorerSection]),
    fieldName: cellToString(f[MAP_PARTNER_PUBLISHED.fieldName]),
    approvedValue: cellToString(f[MAP_PARTNER_PUBLISHED.approvedValue]),
    displayLabel: cellToString(f[MAP_PARTNER_PUBLISHED.displayLabel]),
    publicVisibility: cellToString(f[MAP_PARTNER_PUBLISHED.publicVisibility]),
    overallSourceConfidence: cellToString(f[MAP_PARTNER_PUBLISHED.overallSourceConfidence]),
    publishStatus: cellToString(f[MAP_PARTNER_PUBLISHED.publishStatus]),
    publishedAt: cellToString(f[MAP_PARTNER_PUBLISHED.publishedAt]),
    stale: !!f[MAP_PARTNER_PUBLISHED.stale],
    dataGap: !!f[MAP_PARTNER_PUBLISHED.dataGap],
    registryVersion: f[MAP_PARTNER_PUBLISHED.registryVersion] ?? null,
  };
}

async function resolveFactListQuery(query) {
  const { baseId, apiKey } = getPartnerIntelligenceConfig();
  const resolved = { ...query };
  if (query.operatorId && /^rec[a-zA-Z0-9]+$/.test(query.operatorId)) {
    resolved.operatorLinkName = await fetchLinkedPrimaryName(
      baseId,
      apiKey,
      PARTNER_INTELLIGENCE_LINKS.operatorMaster,
      query.operatorId,
      ["company_name", "Company Name"]
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
  if (query.sourceRecordId && /^rec[a-zA-Z0-9]+$/.test(query.sourceRecordId)) {
    resolved.sourceRecordId = query.sourceRecordId;
  }
  return resolved;
}

function buildFactListFormula(resolved) {
  const parts = [];
  if (resolved.operatorLinkName) {
    parts.push(
      `{${MAP_PARTNER_FACT.operator}} = '${escapeAirtableFormulaValue(resolved.operatorLinkName)}'`
    );
  }
  if (resolved.brandLinkName) {
    parts.push(
      `{${MAP_PARTNER_FACT.brand}} = '${escapeAirtableFormulaValue(resolved.brandLinkName)}'`
    );
  }
  if (resolved.sourceRecordId) {
    parts.push(
      `{${MAP_PARTNER_FACT.sourceRecord}} = '${escapeAirtableFormulaValue(resolved.sourceRecordId)}'`
    );
  }
  if (resolved.humanReviewStatus) {
    parts.push(
      `{${MAP_PARTNER_FACT.humanReviewStatus}} = '${escapeAirtableFormulaValue(resolved.humanReviewStatus)}'`
    );
  }
  if (resolved.extractionRunId) {
    parts.push(
      `{${MAP_PARTNER_FACT.extractionRunId}} = '${escapeAirtableFormulaValue(resolved.extractionRunId)}'`
    );
  }
  if (parts.length === 0) return null;
  return parts.length === 1 ? parts[0] : `AND(${parts.join(", ")})`;
}

export async function listPartnerFacts(query = {}) {
  const { baseId, apiKey } = getPartnerIntelligenceConfig();
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resolved = await resolveFactListQuery(query);
  const params = new URLSearchParams();
  params.set("pageSize", String(Math.min(Number(query.limit) || 100, 100)));
  const formula = buildFactListFormula(resolved);
  if (formula) params.set("filterByFormula", formula);
  if (query.offset) params.set("offset", query.offset);

  const url = `${airtableUrl(baseId, factsTableName())}?${params.toString()}`;
  const { res, json } = await airtableFetch(url, apiKey);
  if (!res.ok) throw new Error(json.error?.message || "Airtable list facts failed");

  return {
    facts: (json.records || []).map(normalizePartnerFactRecord),
    offset: json.offset || null,
  };
}

export async function getPartnerFactById(recordId) {
  const { baseId, apiKey } = getPartnerIntelligenceConfig();
  if (!/^rec[a-zA-Z0-9]+$/.test(recordId || "")) throw new Error("Invalid record id");
  const url = airtableUrl(baseId, factsTableName(), recordId);
  const { res, json } = await airtableFetch(url, apiKey);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(json.error?.message || "Airtable read fact failed");
  return normalizePartnerFactRecord(json);
}

export async function createPartnerFact(fields) {
  const { baseId, apiKey } = getPartnerIntelligenceConfig();
  const url = airtableUrl(baseId, factsTableName());
  const { res, json } = await airtableFetch(url, apiKey, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!res.ok) throw new Error(json.error?.message || "Airtable create fact failed");
  return normalizePartnerFactRecord(json);
}

export async function patchPartnerFact(recordId, fields) {
  const { baseId, apiKey } = getPartnerIntelligenceConfig();
  const url = airtableUrl(baseId, factsTableName(), recordId);
  const { res, json } = await airtableFetch(url, apiKey, {
    method: "PATCH",
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!res.ok) throw new Error(json.error?.message || "Airtable patch fact failed");
  return normalizePartnerFactRecord(json);
}

export async function listPublishedFieldsForOperator(operatorId) {
  const { baseId, apiKey } = getPartnerIntelligenceConfig();
  const operatorLinkName = await fetchLinkedPrimaryName(
    baseId,
    apiKey,
    PARTNER_INTELLIGENCE_LINKS.operatorMaster,
    operatorId,
    ["company_name", "Company Name"]
  );
  if (!operatorLinkName) return [];

  const formula = `AND({${MAP_PARTNER_PUBLISHED.operator}} = '${escapeAirtableFormulaValue(operatorLinkName)}', {${MAP_PARTNER_PUBLISHED.publishStatus}} = 'Published', OR({${MAP_PARTNER_PUBLISHED.stale}} = FALSE(), {${MAP_PARTNER_PUBLISHED.stale}} = BLANK()))`;
  const params = new URLSearchParams({ filterByFormula: formula, pageSize: "100" });
  const url = `${airtableUrl(baseId, publishedTableName())}?${params.toString()}`;
  const { res, json } = await airtableFetch(url, apiKey);
  if (!res.ok) throw new Error(json.error?.message || "Airtable list published failed");
  return (json.records || []).map(normalizePublishedFieldRecord);
}

export async function upsertPublishedField(fields, existingId) {
  const { baseId, apiKey } = getPartnerIntelligenceConfig();
  if (existingId) {
    const url = airtableUrl(baseId, publishedTableName(), existingId);
    const { res, json } = await airtableFetch(url, apiKey, {
      method: "PATCH",
      body: JSON.stringify({ fields, typecast: true }),
    });
    if (!res.ok) throw new Error(json.error?.message || "Airtable patch published failed");
    return normalizePublishedFieldRecord(json);
  }
  const url = airtableUrl(baseId, publishedTableName());
  const { res, json } = await airtableFetch(url, apiKey, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!res.ok) throw new Error(json.error?.message || "Airtable create published failed");
  return normalizePublishedFieldRecord(json);
}

export async function findPublishedRowByFieldName(operatorLinkName, fieldName) {
  const { baseId, apiKey } = getPartnerIntelligenceConfig();
  const formula = `AND({${MAP_PARTNER_PUBLISHED.operator}} = '${escapeAirtableFormulaValue(operatorLinkName)}', {${MAP_PARTNER_PUBLISHED.fieldName}} = '${escapeAirtableFormulaValue(fieldName)}')`;
  const params = new URLSearchParams({ filterByFormula: formula, pageSize: "5" });
  const url = `${airtableUrl(baseId, publishedTableName())}?${params.toString()}`;
  const { res, json } = await airtableFetch(url, apiKey);
  if (!res.ok) return null;
  const rec = (json.records || [])[0];
  return rec ? normalizePublishedFieldRecord(rec) : null;
}
