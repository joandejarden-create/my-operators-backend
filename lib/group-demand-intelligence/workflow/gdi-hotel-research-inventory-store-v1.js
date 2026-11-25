/**
 * GDI Hotel Research Inventory Airtable store V1.
 * Composite key (hotelId, opportunityId). Fail-closed on oversized payload.
 * Never writes GDO or publication packages.
 */

import Airtable from "airtable";
import { escapeAirtableFormulaValue } from "../../airtable-utils.js";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
} from "../../decision-outcomes/airtable-base.js";
import { airtableRequestTimeoutMs } from "../../http/with-timeout.js";
import {
  GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
  GDI_HOTEL_RESEARCH_OPPORTUNITY_SCHEMA_VERSION,
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F,
  VAL_RESEARCH_LIFECYCLE_STATUS,
  VAL_COMMERCIAL_ELIGIBILITY_STATUS,
  buildHotelResearchCompositeKey,
} from "../hotel-research-opportunity-field-map.js";
import {
  classifyOpportunityLifecycleLayer,
  extractDemandThesisEnum,
} from "./gdi-complete-research-persistence-v1.js";
import {
  buildSlimResearchPayload,
  collectNamedContacts,
  collectEvidenceUrls,
  SLIM_RESEARCH_PAYLOAD_MAX_CHARS,
} from "./gdi-research-inventory-slim-payload-v1.js";
import {
  mergeResearchSotLossless,
  GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
} from "./gdi-research-sot-lossless-merge-v1.js";

export const GDI_HOTEL_RESEARCH_INVENTORY_STORE_V1 =
  "gdi-hotel-research-inventory-store-v1";

/** Airtable multiline safe ceiling — exceed → fail closed (no truncate). */
export const RESEARCH_PAYLOAD_MAX_CHARS = SLIM_RESEARCH_PAYLOAD_MAX_CHARS;

const TELEMETRY_DROP_KEYS = Object.freeze([
  "orsRaw",
  "orsTranscript",
  "firecrawlRaw",
  "exaRawHits",
  "retrievalLog",
  "plannerTrace",
  "ledgerEntries",
  "deepResearchEnrichment",
  "supervisorV2",
]);

function text(v) {
  // Never coerce structured objects via String(obj) → "[object Object]".
  if (v == null) return "";
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
    return String(v).replace(/\s+/g, " ").trim();
  }
  return "";
}

/**
 * Extract one human-readable source evidence line from a source entry.
 * Prefers URL, then label/title/name, then relative path. Never fabricates.
 */
export function formatResearchSourceEvidencePart(source) {
  if (source == null) return "";
  if (typeof source === "string" || typeof source === "number") {
    return text(source);
  }
  if (typeof source !== "object") return "";

  const url = text(source.url || source.sourceUrl || source.href);
  if (url) return url;

  const label = text(
    source.label ||
      source.title ||
      source.name ||
      source.sourceName ||
      source.citation ||
      source.description,
  );
  const pathRel = text(source.pathRel || source.path || source.file);
  if (label && pathRel) return `${label} (${pathRel})`;
  if (label) return label;
  if (pathRel) return pathRel;
  return "";
}

/**
 * Build evidence-grounded sourceSummary for research inventory rows.
 * Rejects accidental "[object Object]" placeholders from prior bad coercion.
 */
export function buildResearchSourceSummary(opp = {}) {
  const rawSummary = opp.sourceSummary;
  let fromField = "";
  if (typeof rawSummary === "string" || typeof rawSummary === "number") {
    fromField = text(rawSummary);
  } else if (Array.isArray(rawSummary)) {
    fromField = rawSummary
      .map((s) => formatResearchSourceEvidencePart(s))
      .filter(Boolean)
      .slice(0, 5)
      .join(" | ");
  } else if (rawSummary && typeof rawSummary === "object") {
    fromField = formatResearchSourceEvidencePart(rawSummary);
  }
  if (fromField && !/\[object Object\]/i.test(fromField)) {
    return fromField;
  }

  const sources = Array.isArray(opp.sources) ? opp.sources : [];
  const fromSources = sources
    .map((s) => formatResearchSourceEvidencePart(s))
    .filter(Boolean)
    .slice(0, 5)
    .join(" | ");
  if (fromSources && !/\[object Object\]/i.test(fromSources)) {
    return fromSources;
  }

  const evidence = Array.isArray(opp.evidence) ? opp.evidence : [];
  const fromEvidence = evidence
    .map((s) => formatResearchSourceEvidencePart(s))
    .filter(Boolean)
    .slice(0, 5)
    .join(" | ");
  if (fromEvidence && !/\[object Object\]/i.test(fromEvidence)) {
    return fromEvidence;
  }
  return null;
}

function getToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

function getBase() {
  const apiKey = getToken();
  const baseId = getGdiOpportunitiesAirtableBaseId();
  if (!apiKey || !baseId) {
    const err = new Error("gdi_hotel_research_airtable_not_configured");
    err.code = "gdi_hotel_research_airtable_not_configured";
    throw err;
  }
  assertNotLegacyMvpCanonicalBase(baseId, {
    surface: "gdi_hotel_research_inventory",
  });
  return new Airtable({
    apiKey,
    requestTimeout: airtableRequestTimeoutMs(),
  }).base(baseId);
}

function table() {
  return getBase()(GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME);
}

function asSelect(value, allowed) {
  const v = text(value);
  if (!v) return undefined;
  return allowed.includes(v) ? v : undefined;
}

/**
 * Serialize research payload without silent research loss.
 * Default: Slice 2A slim commercial allowlist (preserves contacts/evidence).
 * Opt out with opp.__useRawResearchPayload === true for legacy callers.
 * If still oversized → fail closed (never truncate commercially essential fields).
 */
export function serializeResearchPayload(opp = {}) {
  if (opp?.__useRawResearchPayload === true) {
    const next = { ...opp };
    delete next.__useRawResearchPayload;
    delete next.__slim;
    delete next.__persistenceSource;
    delete next.__thinRowDegraded;
    for (const k of TELEMETRY_DROP_KEYS) {
      if (k in next) delete next[k];
    }
    if (next.demandThesis == null || next.demandThesis === "") {
      delete next.demandThesis;
    }
    let json;
    try {
      json = JSON.stringify(next);
    } catch (err) {
      return {
        ok: false,
        code: "RESEARCH_PAYLOAD_SERIALIZE_FAILED",
        error: String(err?.message || err),
        json: null,
        length: 0,
      };
    }
    if (json.length > RESEARCH_PAYLOAD_MAX_CHARS) {
      return {
        ok: false,
        code: "RESEARCH_PAYLOAD_TOO_LARGE",
        error: `payload ${json.length} chars exceeds ${RESEARCH_PAYLOAD_MAX_CHARS}; refuse truncate`,
        json: null,
        length: json.length,
      };
    }
    return { ok: true, code: null, json, length: json.length };
  }

  const slim = buildSlimResearchPayload(opp, {
    maxChars: RESEARCH_PAYLOAD_MAX_CHARS,
  });
  if (!slim.ok) {
    return {
      ok: false,
      code: slim.code,
      error: slim.error,
      json: null,
      length: slim.length || 0,
      preservation: slim.preservation || null,
    };
  }
  return {
    ok: true,
    code: null,
    json: slim.json,
    length: slim.length,
    preservation: slim.preservation,
    slimPayload: slim.payload,
  };
}

export function inferResearchLifecycleStatus(opp = {}) {
  const d = text(opp.disposition || opp.evaluationLabel).toUpperCase();
  const pop = text(opp.inventoryPopulationPrimary).toUpperCase();
  if (d === "WATCHLIST_RESEARCH" || pop === "WATCHLIST") return "WATCHLIST";
  if (d === "EXCLUDE" || d === "REJECTED" || text(opp.priority) === "DISQUALIFIED") {
    return "ARCHIVED_RESEARCH";
  }
  if (pop === "SALES_OPPORTUNITIES") return "ACTIVE_RESEARCH";
  if (!extractDemandThesisEnum(opp) && !opp.primaryContact) return "EXPLORATORY";
  return "ACTIVE_RESEARCH";
}

export function buildResearchInventoryFields(opp = {}, hotel = {}) {
  const hotelId = text(hotel.hotelId || opp.hotelId);
  const opportunityId = text(opp.opportunityId || opp.id);
  const compositeKey = buildHotelResearchCompositeKey(hotelId, opportunityId);
  if (!compositeKey) {
    return {
      ok: false,
      code: "MISSING_COMPOSITE_IDENTITY",
      fields: null,
    };
  }

  const payload = serializeResearchPayload(opp);
  if (!payload.ok) {
    return { ok: false, code: payload.code, detail: payload.error, fields: null };
  }

  const cls = classifyOpportunityLifecycleLayer(
    {
      ...opp,
      opportunityId,
      hotelId,
      demandThesis: extractDemandThesisEnum(opp),
    },
    { hotelId },
  );

  const sourceSummary = buildResearchSourceSummary(opp);

  const fields = {
    [F.compositeKey]: compositeKey,
    [F.opportunityId]: opportunityId,
    [F.hotelId]: hotelId,
    [F.organizationName]: text(opp.organizationName || opp.company) || null,
    [F.researchLifecycleStatus]: asSelect(
      inferResearchLifecycleStatus(opp),
      VAL_RESEARCH_LIFECYCLE_STATUS,
    ),
    [F.commercialEligibilityStatus]: asSelect(
      cls.eligibility?.commercialQualificationStatus || "RESEARCH_ONLY",
      VAL_COMMERCIAL_ELIGIBILITY_STATUS,
    ),
    [F.demandThesis]: extractDemandThesisEnum(opp) || null,
    [F.priority]: text(opp.priority) || null,
    [F.title]: text(opp.title || opp.displayTitle || opp.opportunityName) || null,
    [F.segment]: text(opp.segment || opp.segmentDisplayLabel) || null,
    [F.sourceSummary]: sourceSummary,
    [F.firstDiscoveredAt]:
      text(opp.firstDiscoveredAt || opp.firstSeenAt) || null,
    [F.firstDiscoveredRunId]:
      text(opp.firstDiscoveredRunId || opp.firstSeenRunId || opp.runId) || null,
    [F.lastResearchedAt]:
      text(opp.lastResearchedAt || opp.updatedAt || opp.lastSeenAt) || null,
    [F.lastMaterialChangeAt]: text(opp.lastMaterialChangeAt) || null,
    [F.dgGeneratorId]: text(opp.dgGeneratorId || opp.demandGeneratorId) || null,
    [F.researchPayloadJson]: payload.json,
    [F.schemaVersion]: GDI_HOTEL_RESEARCH_OPPORTUNITY_SCHEMA_VERSION,
  };

  // Drop null/undefined/empty
  const clean = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null || v === "") continue;
    clean[k] = v;
  }

  return {
    ok: true,
    code: null,
    compositeKey,
    hotelId,
    opportunityId,
    fields: clean,
    payloadLength: payload.length,
    commerciallyQualified: cls.commerciallyQualified === true,
  };
}

/**
 * Fields compared for material research change detection.
 * firstDiscovered* excluded — preserved operational provenance.
 * lastResearchedAt excluded — operational run stamp; alone must not force UPDATE.
 */
export const MATERIAL_RESEARCH_FIELDS = Object.freeze([
  F.opportunityId,
  F.hotelId,
  F.organizationName,
  F.researchLifecycleStatus,
  F.commercialEligibilityStatus,
  F.demandThesis,
  F.priority,
  F.title,
  F.segment,
  F.sourceSummary,
  // lastMaterialChangeAt intentionally excluded — stamp only when other
  // material fields change (never force UPDATE on timestamp alone).
  F.dgGeneratorId,
  F.researchPayloadJson,
  F.schemaVersion,
  F.compositeKey,
]);

/**
 * Operational / provenance stamps that must not alone force a research inventory
 * UPDATE. Typed Airtable columns remain the source of truth for firstDiscovered*;
 * bridge dry-runs often inject firstDiscoveredRunId / lastResearchedAt into the
 * serialized payload even when research content is unchanged.
 */
export const PAYLOAD_VOLATILE_KEYS = Object.freeze([
  "lastResearchedAt",
  "updatedAt",
  "lastSeenAt",
  "runId",
  "researchedAt",
  "firstDiscoveredRunId",
  "firstDiscoveredAt",
  "firstSeenRunId",
]);

function stripVolatilePayloadKeys(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return value;
  const out = { ...value };
  for (const k of PAYLOAD_VOLATILE_KEYS) delete out[k];
  return out;
}

function normalizeComparableValue(fieldName, value) {
  if (value === undefined || value === null || value === "") return null;
  if (fieldName === F.researchPayloadJson && typeof value === "string") {
    try {
      return canonicalizeJson(stripVolatilePayloadKeys(JSON.parse(value)));
    } catch {
      return String(value).trim();
    }
  }
  if (fieldName === F.researchPayloadJson && typeof value === "object") {
    return canonicalizeJson(stripVolatilePayloadKeys(value));
  }
  if (typeof value === "string") return value.replace(/\s+/g, " ").trim();
  if (typeof value === "object") return canonicalizeJson(value);
  return value;
}

/** Deterministic JSON for stable fingerprint/compare (sorted keys, drop null/empty). */
export function canonicalizeJson(value) {
  if (value === undefined || value === null || value === "") return null;
  if (Array.isArray(value)) {
    return value.map((v) => canonicalizeJson(v));
  }
  if (typeof value === "object") {
    const out = {};
    for (const k of Object.keys(value).sort()) {
      const v = canonicalizeJson(value[k]);
      if (v === null || v === undefined || v === "") continue;
      out[k] = v;
    }
    return out;
  }
  if (typeof value === "string") {
    const t = value.replace(/\s+/g, " ").trim();
    return t === "" ? null : t;
  }
  return value;
}

export function fingerprintResearchFields(fields = {}) {
  const slice = {};
  for (const name of MATERIAL_RESEARCH_FIELDS) {
    slice[name] = normalizeComparableValue(name, fields[name]);
  }
  return JSON.stringify(canonicalizeJson(slice));
}

/**
 * Compare proposed write fields against existing Airtable record fields.
 * Returns materialChanged + changed field names.
 */
export function diffMaterialResearchFields(proposed = {}, existing = {}) {
  const changed = [];
  for (const name of MATERIAL_RESEARCH_FIELDS) {
    const a = normalizeComparableValue(name, proposed[name]);
    const b = normalizeComparableValue(name, existing[name]);
    if (JSON.stringify(a) !== JSON.stringify(b)) {
      changed.push({
        field: name,
        beforeFingerprint: fingerprintResearchFields({ [name]: existing[name] }),
        afterFingerprint: fingerprintResearchFields({ [name]: proposed[name] }),
      });
    }
  }
  return {
    materialChanged: changed.length > 0,
    changedFields: changed.map((c) => c.field),
    changes: changed,
    beforeFingerprint: fingerprintResearchFields(existing),
    afterFingerprint: fingerprintResearchFields(proposed),
  };
}

function parsePayloadJson(raw) {
  if (!raw) return {};
  if (typeof raw === "object") return { ...raw };
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
}

function unionHttpUrls(...lists) {
  const out = [];
  for (const list of lists) {
    for (const u of Array.isArray(list) ? list : []) {
      const t = text(typeof u === "string" ? u : u?.url || u?.sourceUrl);
      if (!t || !/^https?:\/\//i.test(t)) continue;
      if (!out.includes(t)) out.push(t);
    }
  }
  return out;
}

function contactKey(c) {
  // Prefer name identity so distinct people sharing an org inbox are retained.
  const name = text(c?.name || c?.fullName || c).toLowerCase();
  const email = text(c?.email || c?.workEmail).toLowerCase();
  if (name && email) return `n:${name}|e:${email}`;
  if (name) return `n:${name}`;
  if (email) return `e:${email}`;
  return "";
}

function unionContacts(...lists) {
  const out = [];
  const seen = new Set();
  for (const list of lists) {
    for (const c of Array.isArray(list) ? list : []) {
      if (!c || typeof c !== "object") continue;
      const key = contactKey(c);
      if (!key || seen.has(key)) continue;
      seen.add(key);
      out.push(c);
    }
  }
  return out;
}

function unionSourceSummary(a, b) {
  const urls = unionHttpUrls(
    String(a || "").split(/\s*\|\s*/),
    String(b || "").split(/\s*\|\s*/),
  );
  if (urls.length) return urls.slice(0, 8).join(" | ");
  // Preserve non-URL historical attestations when no HTTP URLs exist.
  return text(a) || text(b) || null;
}

/**
 * Merge proposed research inventory fields with live Airtable values so
 * verified contacts, outreach, org names, and source URLs are never lost.
 * Prefer live research body; overlay FQO / disposition / fill-empty IDs.
 */
export function mergeProposedResearchInventoryFields(
  proposedFields = {},
  existingFields = {},
) {
  const prev = existingFields || {};
  const next = { ...(proposedFields || {}) };

  // Never clear populated typed identity / research columns.
  for (const fname of [
    F.organizationName,
    F.title,
    F.segment,
    F.priority,
    F.demandThesis,
  ]) {
    if (!text(next[fname]) && text(prev[fname])) {
      next[fname] = prev[fname];
    }
  }
  // Keep richer live titles when proposed shortens/corrupts (en-dash → period, dropped context).
  if (text(prev[F.title]) && text(next[F.title])) {
    const prevTitle = text(prev[F.title]);
    const nextTitle = text(next[F.title]);
    const corrupted =
      (/[–—]/.test(prevTitle) &&
        !/[–—]/.test(nextTitle) &&
        nextTitle.includes(". ")) ||
      nextTitle.length < prevTitle.length * 0.85;
    if (corrupted) next[F.title] = prev[F.title];
  }

  // dgGeneratorId: fill-if-empty only (never swap a different live id).
  if (text(prev[F.dgGeneratorId])) {
    next[F.dgGeneratorId] = prev[F.dgGeneratorId];
  }

  next[F.sourceSummary] = unionSourceSummary(
    prev[F.sourceSummary],
    next[F.sourceSummary],
  );

  // Lossless SoT merge: live fat Airtable payload is authoritative base.
  // Slim / STAGED / drawer representations must NEVER wholesale-replace SoT.
  const liveRaw = parsePayloadJson(prev[F.researchPayloadJson]);
  const propPayload = parsePayloadJson(next[F.researchPayloadJson]);

  const sotMerge = mergeResearchSotLossless({
    livePayload: liveRaw,
    proposedPayload: propPayload,
    maxChars: RESEARCH_PAYLOAD_MAX_CHARS,
    failOnUnresolvedConflicts: true,
  });

  if (!sotMerge.ok) {
    return {
      ok: false,
      code: sotMerge.code || "SOT_MERGE_BLOCKED",
      fields: null,
      sotMerge,
      lostContacts: [],
      lostEvidence: [],
      classification: sotMerge.classification,
      blockers: sotMerge.blockers,
      conflicts: sotMerge.conflicts,
    };
  }

  const mergedPayload = sotMerge.payload;
  const liveContacts = collectNamedContacts(liveRaw);
  const mergedContactNames = collectNamedContacts(mergedPayload);
  const lostContacts = liveContacts.filter(
    (n) =>
      !mergedContactNames.some((x) => x.toLowerCase() === n.toLowerCase()),
  );
  const liveHttp = collectEvidenceUrls(liveRaw).filter((u) =>
    /^https?:\/\//i.test(u),
  );
  const mergedHttp = collectEvidenceUrls(mergedPayload).filter((u) =>
    /^https?:\/\//i.test(u),
  );
  const lostEvidence = liveHttp.filter((u) => !mergedHttp.includes(u));

  if (lostContacts.length || lostEvidence.length) {
    return {
      ok: false,
      code: lostContacts.length
        ? "MERGE_WOULD_LOSE_CONTACTS"
        : "MERGE_WOULD_LOSE_EVIDENCE",
      fields: null,
      lostContacts,
      lostEvidence,
      sotMerge,
      classification: "BLOCKED_INFORMATION_LOSS",
    };
  }

  next[F.researchPayloadJson] = sotMerge.json;
  if (next[F.sourceSummary] == null && mergedPayload.sourceSummary) {
    next[F.sourceSummary] = mergedPayload.sourceSummary;
  }

  return {
    ok: true,
    code: null,
    fields: next,
    lostContacts: [],
    lostEvidence: [],
    preservedLiveContacts: liveContacts,
    preservedLiveEvidence: liveHttp,
    sotMerge,
    classification: sotMerge.classification,
    mergeVersion: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
  };
}

export async function findResearchInventoryByCompositeKey(compositeKey) {
  const key = text(compositeKey);
  if (!key) return null;
  const formula = `{${F.compositeKey}} = "${escapeAirtableFormulaValue(key)}"`;
  const records = await table()
    .select({ filterByFormula: formula, maxRecords: 5 })
    .firstPage();
  if (!records.length) return null;
  if (records.length > 1) {
    const err = new Error(`duplicate_research_inventory_key:${key}`);
    err.code = "DUPLICATE_COMPOSITE_KEY";
    err.records = records.map((r) => r.id);
    throw err;
  }
  return records[0];
}

export async function listResearchInventoryForHotel(hotelId) {
  const id = text(hotelId);
  if (!id) return [];
  const formula = `{${F.hotelId}} = "${escapeAirtableFormulaValue(id)}"`;
  const out = [];
  await table()
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((page, next) => {
      out.push(...page);
      next();
    });
  return out;
}

/**
 * Upsert one research inventory row. Preserves firstDiscovered* on update.
 */
export async function upsertResearchInventoryRecord({
  opportunity,
  hotel,
  dryRun = true,
} = {}) {
  const built = buildResearchInventoryFields(opportunity, hotel);
  if (!built.ok) {
    return {
      ok: false,
      action: "FAILED",
      code: built.code,
      detail: built.detail || null,
      compositeKey: null,
    };
  }

  let existing = null;
  try {
    existing = await findResearchInventoryByCompositeKey(built.compositeKey);
  } catch (err) {
    if (err.code === "DUPLICATE_COMPOSITE_KEY") {
      return {
        ok: false,
        action: "FAILED",
        code: err.code,
        detail: err.records,
        compositeKey: built.compositeKey,
      };
    }
    throw err;
  }

  let fields = { ...built.fields };
  let mergeMeta = null;
  if (existing) {
    // Preserve first discovery history. Never invent firstDiscovered* on UPDATE
    // when the live record lacked them — typed provenance is CREATE-time only.
    const prev = existing.fields || {};
    if (prev[F.firstDiscoveredAt]) {
      fields[F.firstDiscoveredAt] = prev[F.firstDiscoveredAt];
    } else {
      delete fields[F.firstDiscoveredAt];
    }
    if (prev[F.firstDiscoveredRunId]) {
      fields[F.firstDiscoveredRunId] = prev[F.firstDiscoveredRunId];
    } else {
      delete fields[F.firstDiscoveredRunId];
    }

    // Lossless merge: live contacts/evidence/org win over package replace.
    const merged = mergeProposedResearchInventoryFields(fields, prev);
    mergeMeta = {
      ok: merged.ok === true,
      code: merged.code || null,
      lostContacts: merged.lostContacts || [],
      lostEvidence: merged.lostEvidence || [],
      preservedLiveContacts: merged.preservedLiveContacts || [],
      preservedLiveEvidence: merged.preservedLiveEvidence || [],
    };
    if (!merged.ok) {
      return {
        ok: false,
        action: "FAILED",
        code: merged.code,
        detail: {
          lostContacts: merged.lostContacts || [],
          lostEvidence: merged.lostEvidence || [],
          length: merged.length || null,
        },
        compositeKey: built.compositeKey,
        hotelId: built.hotelId,
        opportunityId: built.opportunityId,
        airtableRecordId: existing.id,
        mergeMeta,
      };
    }
    fields = merged.fields;
  }

  let materialDiff = null;
  if (existing) {
    materialDiff = diffMaterialResearchFields(fields, existing.fields || {});
    // Only stamp lastMaterialChangeAt when a genuine material field changed.
    if (materialDiff.materialChanged) {
      fields[F.lastMaterialChangeAt] =
        text(opportunity?.lastMaterialChangeAt) || new Date().toISOString();
    } else {
      delete fields[F.lastMaterialChangeAt];
    }
  }

  if (dryRun) {
    let action = "WOULD_CREATE";
    if (existing) {
      action = materialDiff.materialChanged ? "WOULD_UPDATE" : "UNCHANGED";
    }
    return {
      ok: true,
      action,
      compositeKey: built.compositeKey,
      hotelId: built.hotelId,
      opportunityId: built.opportunityId,
      airtableRecordId: existing?.id || null,
      fields,
      payloadLength: text(fields[F.researchPayloadJson]).length || built.payloadLength,
      materialDiff,
      mergeMeta,
    };
  }

  if (existing) {
    if (!materialDiff.materialChanged) {
      return {
        ok: true,
        action: "UNCHANGED",
        compositeKey: built.compositeKey,
        hotelId: built.hotelId,
        opportunityId: built.opportunityId,
        airtableRecordId: existing.id,
        fields,
        payloadLength: text(fields[F.researchPayloadJson]).length || built.payloadLength,
        materialDiff,
        mergeMeta,
      };
    }
    const updated = await table().update(existing.id, fields);
    return {
      ok: true,
      action: "UPDATED",
      compositeKey: built.compositeKey,
      hotelId: built.hotelId,
      opportunityId: built.opportunityId,
      airtableRecordId: updated.id,
      fields,
      payloadLength: text(fields[F.researchPayloadJson]).length || built.payloadLength,
      materialDiff,
      mergeMeta,
    };
  }

  const created = await table().create(fields);
  return {
    ok: true,
    action: "CREATED",
    compositeKey: built.compositeKey,
    hotelId: built.hotelId,
    opportunityId: built.opportunityId,
    airtableRecordId: created.id,
    fields,
    payloadLength: built.payloadLength,
    materialDiff: null,
    mergeMeta: null,
  };
}

export async function readBackResearchInventory(airtableRecordId) {
  const rec = await table().find(airtableRecordId);
  return {
    id: rec.id,
    fields: rec.fields,
    compositeKey: rec.fields?.[F.compositeKey] || null,
    opportunityId: rec.fields?.[F.opportunityId] || null,
    hotelId: rec.fields?.[F.hotelId] || null,
  };
}

export default {
  GDI_HOTEL_RESEARCH_INVENTORY_STORE_V1,
  RESEARCH_PAYLOAD_MAX_CHARS,
  MATERIAL_RESEARCH_FIELDS,
  PAYLOAD_VOLATILE_KEYS,
  serializeResearchPayload,
  canonicalizeJson,
  fingerprintResearchFields,
  diffMaterialResearchFields,
  formatResearchSourceEvidencePart,
  buildResearchSourceSummary,
  buildResearchInventoryFields,
  findResearchInventoryByCompositeKey,
  listResearchInventoryForHotel,
  mergeProposedResearchInventoryFields,
  upsertResearchInventoryRecord,
  readBackResearchInventory,
};
