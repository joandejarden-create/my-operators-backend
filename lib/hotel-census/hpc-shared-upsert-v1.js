/**
 * Shared HPC upsert / null-fill enrichment V1.
 * Idempotent resolve → dry-run → optional apply → readback.
 * Never overwrites populated production values. Never invents facts.
 */

import "dotenv/config";
import {
  HPC_TABLE_ID,
  HPC_TABLE_NAME,
  COMPUTED_FIELD_TYPES,
  isFieldEmpty,
  classifyFieldStatus,
  certifyHpcCompleteness,
  proposeNullFillsFromAdpProfile,
  HPC_MINIMUM_IDENTITY_FIELDS,
  HPC_ADP_ESSENTIAL_FIELDS,
  HPC_ENRICHMENT_TARGET_FIELDS,
} from "./hpc-full-onboarding-contract-v1.js";
import { getCensusRecordIdForAdpProperty } from "../ai-demand-positioning/census-link-registry.js";
import { resolveCanonicalHotelId } from "./adp-gdi-canonical-identity.js";

export const HPC_SHARED_UPSERT_VERSION = "HPC_SHARED_UPSERT_V1";

function airtableToken() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

function platformBaseId() {
  return (process.env.AIRTABLE_BASE_ID_ALT || "").trim();
}

async function airtableFetch(path, opts = {}) {
  const token = airtableToken();
  const baseId = platformBaseId();
  if (!token || !baseId) {
    throw new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT");
  }
  const url = path.startsWith("http")
    ? path
    : `https://api.airtable.com/v0/${baseId}/${path}`;
  const res = await fetch(url, {
    ...opts,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(opts.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(
      `Airtable ${res.status}: ${json?.error?.message || JSON.stringify(json).slice(0, 200)}`
    );
    err.status = res.status;
    err.body = json;
    throw err;
  }
  return json;
}

export async function fetchLiveHpcSchema() {
  const token = airtableToken();
  const baseId = platformBaseId();
  const res = await fetch(
    `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const j = await res.json();
  if (!res.ok) throw new Error(`meta ${res.status}`);
  const table = (j.tables || []).find(
    (t) => t.id === HPC_TABLE_ID || t.name === HPC_TABLE_NAME
  );
  if (!table) throw new Error("Hotel Property Census table not found");
  const fields = (table.fields || []).map((f) => ({
    id: f.id,
    name: f.name,
    type: f.type,
    computed: COMPUTED_FIELD_TYPES.includes(f.type),
    writable: !COMPUTED_FIELD_TYPES.includes(f.type),
    choices: (f.options?.choices || []).map((c) => c.name),
    description: f.description || "",
  }));
  return {
    baseId,
    tableId: table.id,
    tableName: table.name,
    fieldCount: fields.length,
    fields,
    fetchedAt: new Date().toISOString(),
  };
}

export async function fetchHpcRecord(recordId) {
  const id = String(recordId || "").trim();
  if (!/^rec[a-zA-Z0-9]{14}$/.test(id)) return null;
  const j = await airtableFetch(
    `${encodeURIComponent(HPC_TABLE_NAME)}/${id}`
  );
  return { id: j.id, fields: j.fields || {}, createdTime: j.createdTime };
}

/**
 * Resolve ADP property → canonical HPC id (registry first, never invent).
 */
export function resolveHpcIdForAdpProperty(adpPropertyId) {
  const fromLink = getCensusRecordIdForAdpProperty(adpPropertyId);
  if (fromLink) {
    return {
      censusRecordId: fromLink,
      resolution: "CENSUS_LINK_REGISTRY",
      confidence: "HIGH",
    };
  }
  const canonical = resolveCanonicalHotelId(adpPropertyId);
  if (canonical && /^rec[a-zA-Z0-9]{14}$/.test(canonical)) {
    return {
      censusRecordId: canonical,
      resolution: "ADP_GDI_ALIAS_MAP",
      confidence: "HIGH",
    };
  }
  return {
    censusRecordId: null,
    resolution: "UNRESOLVED",
    confidence: "NONE",
    nextAction: "SEARCH_HPC_THEN_CREATE_VIA_SHARED_UPSERT",
  };
}

export function auditHpcRecordFields(record, schema) {
  const fields = record?.fields || {};
  const byName = Object.fromEntries((schema?.fields || []).map((f) => [f.name, f]));
  const fieldStatuses = {};
  const applicableNames = [
    ...new Set([
      ...HPC_MINIMUM_IDENTITY_FIELDS,
      ...HPC_ADP_ESSENTIAL_FIELDS,
      ...HPC_ENRICHMENT_TARGET_FIELDS,
      ...(schema?.fields || [])
        .filter((f) => f.writable && !f.computed)
        .map((f) => f.name),
    ]),
  ];

  for (const name of applicableNames) {
    const schemaField = byName[name] || { name, type: "unknown", computed: false };
    fieldStatuses[name] = {
      field: name,
      valuePresent: !isFieldEmpty(fields[name]),
      ...classifyFieldStatus({
        fieldName: name,
        value: fields[name],
        schemaField,
        applicable: true,
        pairedLatitude: fields.Latitude,
        pairedLongitude: fields.Longitude,
      }),
    };
  }

  const populated = Object.values(fieldStatuses).filter((s) =>
    ["VERIFIED_EXISTING", "VERIFIED_POPULATED"].includes(s.status)
  ).length;
  const empty = Object.values(fieldStatuses).filter(
    (s) => s.status === "EMPTY_NO_REASON"
  ).length;

  return {
    recordId: record?.id || null,
    applicableFieldCount: applicableNames.length,
    verifiedPopulated: populated,
    emptyNoReason: empty,
    fieldStatuses,
  };
}

/**
 * Dry-run null-fill enrichment for one ADP hotel.
 */
export async function planHpcEnrichmentForAdpHotel({
  adpPropertyId,
  profile,
  schema,
} = {}) {
  const resolved = resolveHpcIdForAdpProperty(adpPropertyId);
  if (!resolved.censusRecordId) {
    return {
      ok: false,
      adpPropertyId,
      resolved,
      error: "NO_HPC_LINK",
      proposals: [],
    };
  }
  const record = await fetchHpcRecord(resolved.censusRecordId);
  if (!record) {
    return {
      ok: false,
      adpPropertyId,
      resolved,
      error: "HPC_RECORD_NOT_FOUND",
      proposals: [],
    };
  }
  const audit = auditHpcRecordFields(record, schema);
  // Full-schema evaluation (same path new ADP hotels must run) — not identity-only
  const { planAdpHpcFieldPopulation } = await import("./hpc-adp-field-population-v1.js");
  const { planExhaustiveHpcCompletion, loadBrandExplorerSlugMap } = await import(
    "./hpc-exhaustive-field-completion-v1.js"
  );
  let exhaustive;
  try {
    exhaustive = planExhaustiveHpcCompletion({
      propertyId: adpPropertyId,
      profile,
      hpcFields: record.fields,
      schema,
      slugMap: loadBrandExplorerSlugMap(),
    });
  } catch {
    exhaustive = planAdpHpcFieldPopulation({ profile, hpcFields: record.fields });
  }
  const legacy = proposeNullFillsFromAdpProfile(profile, record.fields);
  const byField = new Map();
  for (const p of [...legacy, ...(exhaustive.proposals || [])]) {
    if (!byField.has(p.field)) byField.set(p.field, p);
  }
  const proposals = [...byField.values()];
  // Drop proposals for computed fields
  const writable = new Set(
    (schema?.fields || []).filter((f) => f.writable).map((f) => f.name)
  );
  const safeProposals = proposals.filter((p) => writable.has(p.field));

  const cert = certifyHpcCompleteness({
    recordId: record.id,
    fieldStatuses: audit.fieldStatuses,
    linkedToAdp: true,
  });

  return {
    ok: true,
    adpPropertyId,
    resolved,
    recordId: record.id,
    propertyName: record.fields["Property Name"] || profile?.name || null,
    audit,
    certification: cert,
    proposals: safeProposals,
    exhaustive,
    dryRunPatch: Object.fromEntries(safeProposals.map((p) => [p.field, p.value])),
  };
}

/**
 * Plan + NULL_FILL apply + readback for one ADP hotel.
 * Planner alone is not completion — this is the permanent onboarding executor.
 */
export async function executeHpcEnrichmentForAdpHotel({
  adpPropertyId,
  profile,
  schema,
  dryRun = true,
  reason = "hpc_enrichment_execute_v1",
} = {}) {
  const plan = await planHpcEnrichmentForAdpHotel({
    adpPropertyId,
    profile,
    schema,
  });
  if (!plan.ok || !plan.recordId) {
    return { ...plan, applied: false, writeResult: null };
  }
  const patch = plan.dryRunPatch || {};
  if (!Object.keys(patch).length) {
    return {
      ...plan,
      applied: false,
      writeResult: { ok: true, skipped: true, reason: "no_null_fill_proposals" },
    };
  }
  const writeResult = await applyHpcNullFill({
    recordId: plan.recordId,
    patch,
    dryRun,
    reason,
  });
  const after = dryRun ? null : await fetchHpcRecord(plan.recordId);
  const unresolved = [];
  if (after && schema) {
    const audit = auditHpcRecordFields(after, schema);
    for (const [name, st] of Object.entries(audit.fieldStatuses || {})) {
      if (st.status === "EMPTY_NO_REASON") {
        unresolved.push({ field: name, reason: "STILL_EMPTY_AFTER_ENRICHMENT" });
      }
    }
  }
  return {
    ...plan,
    applied: !dryRun && writeResult && !writeResult.skipped,
    writeResult,
    postApplyUnresolved: unresolved,
  };
}

/**
 * Apply NULL_FILL patch with readback. Requires apply=true.
 */
export async function applyHpcNullFill({
  recordId,
  patch,
  dryRun = true,
  reason = "hpc_shared_upsert_null_fill",
} = {}) {
  const fields = {};
  for (const [k, v] of Object.entries(patch || {})) {
    if (v == null || v === "") continue;
    fields[k] = v;
  }
  if (!Object.keys(fields).length) {
    return { ok: true, skipped: true, reason: "empty_patch" };
  }

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      recordId,
      wouldWrite: fields,
      reason,
    };
  }

  // Re-fetch to ensure still null (race-safe null-fill)
  const before = await fetchHpcRecord(recordId);
  const safe = {};
  for (const [k, v] of Object.entries(fields)) {
    if (isFieldEmpty(before?.fields?.[k])) safe[k] = v;
  }
  if (!Object.keys(safe).length) {
    return { ok: true, skipped: true, reason: "all_fields_already_populated" };
  }

  await airtableFetch(`${encodeURIComponent(HPC_TABLE_NAME)}/${recordId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields: safe, typecast: true }),
  });

  const after = await fetchHpcRecord(recordId);
  const readback = {};
  for (const k of Object.keys(safe)) {
    const expected = safe[k];
    const actual = after?.fields?.[k] ?? null;
    // Airtable omits unchecked checkboxes from the fields payload — treat
    // missing/null as false when we intentionally wrote false.
    const match =
      actual === expected ||
      String(actual ?? "").trim() === String(expected).trim() ||
      (expected === false && (actual === false || actual == null || actual === ""));
    readback[k] = {
      expected,
      actual,
      match,
    };
  }
  const allMatch = Object.values(readback).every((r) => r.match);
  return {
    ok: allMatch,
    dryRun: false,
    recordId,
    written: safe,
    readback,
    reason,
  };
}

/**
 * Governed semantic correction — overwrites incorrect populated classification values.
 * Not NULL_FILL. Never typecasts new select options into existence.
 *
 * @param {object} opts
 * @param {string} opts.recordId
 * @param {Record<string, unknown>} opts.patch — field → new value
 * @param {object} [opts.schema] — live schema; select values must already exist
 * @param {boolean} [opts.dryRun=true]
 * @param {string} [opts.reason]
 */
export async function applyHpcGovernedSemanticCorrection({
  recordId,
  patch,
  schema = null,
  dryRun = true,
  reason = "hpc_governed_semantic_correction_v1",
} = {}) {
  const fields = {};
  for (const [k, v] of Object.entries(patch || {})) {
    if (v === undefined) continue;
    fields[k] = v;
  }
  if (!Object.keys(fields).length) {
    return { ok: true, skipped: true, reason: "empty_patch" };
  }

  // Reject inventing new single-select choices
  if (schema?.fields?.length) {
    for (const [name, value] of Object.entries(fields)) {
      const meta = schema.fields.find((f) => f.name === name);
      if (!meta || meta.type !== "singleSelect") continue;
      const choices = meta.choices || [];
      if (choices.length && value != null && value !== "" && !choices.includes(value)) {
        return {
          ok: false,
          skipped: true,
          reason: "SELECT_VALUE_NOT_IN_SCHEMA",
          field: name,
          value,
          allowed: choices,
        };
      }
    }
  }

  if (dryRun) {
    return { ok: true, dryRun: true, recordId, wouldWrite: fields, reason };
  }

  const before = await fetchHpcRecord(recordId);
  // typecast:false — never create unreviewed taxonomy options
  await airtableFetch(`${encodeURIComponent(HPC_TABLE_NAME)}/${recordId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields, typecast: false }),
  });

  const after = await fetchHpcRecord(recordId);
  const readback = {};
  for (const k of Object.keys(fields)) {
    const expected = fields[k];
    const actual = after?.fields?.[k] ?? null;
    const match =
      actual === expected ||
      String(actual ?? "").trim() === String(expected).trim() ||
      (expected === false && (actual === false || actual == null || actual === ""));
    readback[k] = {
      expected,
      actual: actual ?? null,
      before: before?.fields?.[k] ?? null,
      match,
    };
  }
  const allMatch = Object.values(readback).every((r) => r.match);
  return {
    ok: allMatch,
    dryRun: false,
    recordId,
    written: fields,
    readback,
    reason,
  };
}

/**
 * Governed coordinate correction — overwrites invalid existing values (0/0, missing).
 * Not NULL_FILL: Null Island is treated as corrupt data requiring replacement.
 */
export async function applyHpcCoordinateCorrection({
  recordId,
  latitude,
  longitude,
  provenance = {},
  dryRun = true,
  reason = "hpc_geographic_integrity_coordinate_correction",
} = {}) {
  const { hasValidHpcCoordinates, requiresCoordinateCorrection } = await import(
    "./hpc-geographic-integrity-v1.js"
  );
  if (!hasValidHpcCoordinates(latitude, longitude)) {
    return {
      ok: false,
      skipped: true,
      reason: "proposed_coordinates_invalid",
    };
  }

  const patch = {
    Latitude: Number(latitude),
    Longitude: Number(longitude),
  };
  if (provenance.coordinateSourceType) {
    patch["Coordinate Source Type"] = provenance.coordinateSourceType;
  }
  if (provenance.coordinateConfidence) {
    patch["Coordinate Confidence"] = provenance.coordinateConfidence;
  }
  if (provenance.geocodeProvider) {
    patch["Geocode Provider"] = provenance.geocodeProvider;
  }
  if (provenance.geocodeMethod) {
    patch["Geocode Method"] = provenance.geocodeMethod;
  }
  if (provenance.geocodeReviewedDate) {
    patch["Geocode Reviewed Date"] = provenance.geocodeReviewedDate;
  }

  if (dryRun) {
    return { ok: true, dryRun: true, recordId, wouldWrite: patch, reason };
  }

  const before = await fetchHpcRecord(recordId);
  const beforeLat = before?.fields?.Latitude;
  const beforeLng = before?.fields?.Longitude;
  if (!requiresCoordinateCorrection(beforeLat, beforeLng)) {
    return {
      ok: true,
      skipped: true,
      reason: "existing_coordinates_already_valid",
      before: { latitude: beforeLat, longitude: beforeLng },
    };
  }

  await airtableFetch(`${encodeURIComponent(HPC_TABLE_NAME)}/${recordId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields: patch, typecast: true }),
  });

  const after = await fetchHpcRecord(recordId);
  const actualLat = after?.fields?.Latitude;
  const actualLng = after?.fields?.Longitude;
  const match =
    hasValidHpcCoordinates(actualLat, actualLng) &&
    Math.abs(Number(actualLat) - Number(latitude)) < 1e-5 &&
    Math.abs(Number(actualLng) - Number(longitude)) < 1e-5;

  return {
    ok: match,
    dryRun: false,
    recordId,
    reason,
    before: { latitude: beforeLat, longitude: beforeLng },
    written: patch,
    after: { latitude: actualLat, longitude: actualLng },
    match,
  };
}

export async function searchHpcByName({ name, maxRecords = 5 } = {}) {
  const n = String(name || "").trim();
  if (!n) return [];
  const formula = `OR({Property Name}='${n.replace(/'/g, "\\'")}', FIND(LOWER('${n
    .replace(/'/g, "\\'")
    .toLowerCase()}'), LOWER({Property Name}&'')))`;
  const q = new URLSearchParams({
    maxRecords: String(maxRecords),
    filterByFormula: formula,
  });
  const j = await airtableFetch(
    `${encodeURIComponent(HPC_TABLE_NAME)}?${q.toString()}`
  );
  return (j.records || []).map((r) => ({
    id: r.id,
    name: r.fields?.["Property Name"] || null,
    brand: r.fields?.["Current Brand"] || null,
    city: r.fields?.City || null,
    country: r.fields?.Country || null,
  }));
}
