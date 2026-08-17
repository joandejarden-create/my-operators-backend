/**
 * Phase 4A / 4Q — Build and apply validation evidence for matched OSM staging candidates.
 * Writes only to Independent Hotel Source Evidence (when caller applies).
 */

import {
  EVIDENCE_FIELDS,
  EVIDENCE_TYPES,
  MATCH_CONFIDENCE,
} from "./fields.js";
import { PROPERTY_MATCH_ACTIONS } from "./match-brand-directory-properties.js";
import { MATCH_TYPE } from "./choice-property-id-reconciliation.js";
import { normalizeKey } from "./match-current-census.js";
import { WIKIDATA_ENTITY_BASE } from "./sources/wikidata.js";

export const EVIDENCE_CAPTURED_BY = "Phase 4A independent-census-evidence";
export const BRAND_DIRECTORY_EVIDENCE_CAPTURED_BY =
  "Phase 4Q brand-directory-evidence";
export const CHOICE_RECONCILIATION_EVIDENCE_CAPTURED_BY =
  "Phase 4U choice-property-id-corrected-evidence";

const RECONCILIATION_ALLOWED_ACTIONS = new Set([
  "needs_manual_review",
  "ready_for_choice_evidence",
]);

/** Source policy values eligible for brand-directory evidence (Phase 4Q). */
export const BRAND_DIRECTORY_ALLOWED_SOURCE_POLICY = new Set([
  "review_required",
  "approved_for_internal_staging",
]);

const CONFIDENCE_RANK = {
  none: 0,
  low: 1,
  medium: 2,
  high: 3,
};

/**
 * Primary-field dedupe key stored in Evidence `Name`.
 */
export function evidenceDedupeName(batchId, wikidataQid, osmSourceRecordId) {
  return `4A|${batchId}|${wikidataQid}|${osmSourceRecordId}`;
}

/**
 * Phase 4Q — stable Evidence `Name` for Choice brand-directory ↔ OSM candidate.
 */
export function brandDirectoryEvidenceDedupeName(
  batchId,
  choicePropertyId,
  matchedCandidateRecordId
) {
  return `4Q|${batchId}|${choicePropertyId}|${matchedCandidateRecordId}`;
}

/**
 * Phase 4U — stable Evidence `Name` for corrected Choice property ID evidence.
 */
export function correctedChoiceEvidenceDedupeName(
  batchId,
  choicePropertyId,
  candidateRecordId
) {
  return `4U|${batchId}|${choicePropertyId}|${candidateRecordId}`;
}

/** Cross-batch dedupe: same Choice property ID + OSM candidate (Phase 4U / 4X). */
export function correctedChoiceSemanticDedupeKey(propertyId, candidateRecordId) {
  return `${normalizeKey(propertyId)}|${candidateRecordId}`;
}

/**
 * Parse `4U|{batch}|{propertyId}|{candidateRecordId}` Evidence Name.
 * @returns {string|null} semantic key
 */
export function semanticKeyFromCorrectedChoiceEvidenceName(name) {
  const parts = String(name || "").split("|");
  if (parts.length < 4 || parts[0] !== "4U") return null;
  const propertyId = parts[2];
  const candidateRecordId = parts[3];
  if (!propertyId || !candidateRecordId) return null;
  return correctedChoiceSemanticDedupeKey(propertyId, candidateRecordId);
}

export function parseMatchTypesInclude(includeStr) {
  const defaults = [
    MATCH_TYPE.DIRECT_PROPERTY_ID,
    MATCH_TYPE.DIRECT_PROPERTY_URL,
  ];
  const raw = String(includeStr || "").trim();
  if (!raw) return new Set(defaults);
  return new Set(
    raw
      .split(",")
      .map((s) => normalizeKey(s.trim()))
      .filter(Boolean)
  );
}

export function parsePropertyIdFilter(propertyIdsStr) {
  const raw = String(propertyIdsStr || "").trim();
  if (!raw) return null;
  return new Set(
    raw
      .split(",")
      .map((s) => normalizeKey(s.trim()))
      .filter(Boolean)
  );
}

/**
 * Phase 4U — Select Phase 4T reconciliation rows for corrected evidence.
 * @param {object} reconciliationReport
 * @param {{ includeMatchTypes?: Set<string>, propertyIdsFilter?: Set<string>|null }} opts
 */
export function selectReconciliationEvidenceMatches(reconciliationReport, opts = {}) {
  const includeMatchTypes =
    opts.includeMatchTypes || parseMatchTypesInclude("");
  const propertyIdFilter = opts.propertyIdsFilter ?? null;
  const rows = reconciliationReport.reconciliationRows || [];
  const selected = [];
  const skipped = {
    wrongMatchType: [],
    wrongRecommendedAction: [],
    missingRequiredFields: [],
    propertyIdFiltered: [],
  };

  for (const r of rows) {
    const matchType = normalizeKey(r.matchType);
    if (!includeMatchTypes.has(matchType)) {
      skipped.wrongMatchType.push(r);
      continue;
    }

    const action = normalizeKey(r.recommendedAction);
    if (!RECONCILIATION_ALLOWED_ACTIONS.has(action)) {
      skipped.wrongRecommendedAction.push(r);
      continue;
    }

    const propertyId = normalizeKey(r.extractedChoicePropertyId);
    const candidateId = r.osmCandidateRecordId || "";
    const choiceUrl = r.matchedChoicePropertyUrl || "";

    if (!candidateId || !choiceUrl) {
      skipped.missingRequiredFields.push(r);
      continue;
    }
    if (
      matchType === MATCH_TYPE.DIRECT_PROPERTY_ID &&
      !propertyId
    ) {
      skipped.missingRequiredFields.push(r);
      continue;
    }

    if (propertyIdFilter && !propertyIdFilter.has(propertyId)) {
      skipped.propertyIdFiltered.push(r);
      continue;
    }

    selected.push(r);
  }

  return {
    selected,
    skipped,
    includeMatchTypes: [...includeMatchTypes],
    propertyIdFilter: propertyIdFilter ? [...propertyIdFilter] : null,
  };
}

function reconciliationConfidenceToScore(confidence) {
  const c = normalizeKey(confidence);
  if (c === "high") return 80;
  if (c === "medium") return 65;
  if (c === "low") return 50;
  return 45;
}

function reconciliationMatchReason(row) {
  if (row.matchType === MATCH_TYPE.DIRECT_PROPERTY_URL) {
    return "Direct property URL match from OSM website to Choice sitemap (Phase 4T reconciliation).";
  }
  return "Direct property ID match from OSM website to Choice sitemap (Phase 4T reconciliation).";
}

/**
 * @param {object} row — Phase 4T reconciliation row
 * @param {string} batchId
 */
export function buildCorrectedChoiceEvidenceText(row, batchId) {
  const lines = [
    "Corrected Choice brand-directory evidence (Phase 4U) — OSM website property ID / URL reconciled to Choice sitemap.",
    `Evidence batch: ${batchId}`,
    "",
    "Parent Company: Choice Hotels International",
    `Choice Property ID: ${row.extractedChoicePropertyId || ""}`,
    `Matched Choice Property URL: ${row.matchedChoicePropertyUrl || ""}`,
    `Match Type: ${row.matchType || ""}`,
    `Matched Choice Brand: ${row.matchedChoiceBrand || ""}`,
    `Matched Choice Country: ${row.matchedChoiceCountry || ""}`,
    `Matched Choice City Slug: ${row.matchedChoiceCitySlug || ""}`,
    "",
    `OSM Candidate Record ID: ${row.osmCandidateRecordId || ""}`,
    `OSM Candidate Name: ${row.osmCandidateName || ""}`,
    `OSM Website: ${row.osmWebsite || row.osmWebsiteUsed || ""}`,
    `OSM City: ${row.osmCity || ""}`,
    `OSM Country: ${row.osmCountry || ""}`,
    "",
    `Reconciliation confidence: ${row.reconciliationConfidence || ""}`,
    `Reconciliation recommended action: ${row.recommendedAction || ""}`,
    row.notes ? `Reconciliation notes: ${row.notes}` : null,
    "",
    "Source policy note: Corrected Choice sitemap evidence based on direct property ID / URL match from OSM website; no Choice property HTML fetched; requires human review before promotion.",
    "Prior Phase 4Q collision evidence (e.g. weak tr821–tr825 links) is not modified by this batch.",
    "Human approval required before Verified Independent Hotel Census promotion.",
    "No Hotel Census link created. Not auto-approved.",
  ];
  return lines.filter(Boolean).join("\n");
}

/**
 * @param {object} row
 * @param {string} batchId
 */
export function buildCorrectedChoiceEvidenceAirtableFields(row, batchId) {
  const propertyId = row.extractedChoicePropertyId || "";
  const name = correctedChoiceEvidenceDedupeName(
    batchId,
    propertyId,
    row.osmCandidateRecordId
  );

  const fields = {
    Name: name,
    [EVIDENCE_FIELDS.evidenceType]: EVIDENCE_TYPES.SOURCE_SNAPSHOT,
    [EVIDENCE_FIELDS.evidenceUrl]: String(row.matchedChoicePropertyUrl || ""),
    [EVIDENCE_FIELDS.evidenceText]: buildCorrectedChoiceEvidenceText(row, batchId),
    [EVIDENCE_FIELDS.capturedAt]: new Date().toISOString(),
    [EVIDENCE_FIELDS.capturedBy]: CHOICE_RECONCILIATION_EVIDENCE_CAPTURED_BY,
    [EVIDENCE_FIELDS.matchScore]: reconciliationConfidenceToScore(
      row.reconciliationConfidence
    ),
    [EVIDENCE_FIELDS.matchReason]: reconciliationMatchReason(row),
  };

  if (row.osmCandidateRecordId) {
    fields[EVIDENCE_FIELDS.candidate] = [row.osmCandidateRecordId];
  }

  return {
    name,
    fields,
    row,
    choicePropertyId: propertyId,
    matchedCandidateRecordId: row.osmCandidateRecordId,
  };
}

/**
 * @param {object} matchReport
 * @param {{ minConfidence?: string, includeMedium?: boolean }} opts
 */
export function selectEvidenceMatches(matchReport, opts = {}) {
  const minConfidence = (opts.minConfidence || MATCH_CONFIDENCE.HIGH).toLowerCase();
  const minRank = CONFIDENCE_RANK[minConfidence] ?? CONFIDENCE_RANK.high;
  const includeMedium = !!opts.includeMedium;

  const allowed = new Set();
  for (const [level, rank] of Object.entries(CONFIDENCE_RANK)) {
    if (rank >= minRank) allowed.add(level);
  }
  if (includeMedium) allowed.add(MATCH_CONFIDENCE.MEDIUM);

  const matches = matchReport.matches || [];
  const selected = [];
  const skipped = {
    noStagingRecordId: [],
    belowMinConfidence: [],
  };

  for (const m of matches) {
    if (!m.matchedStagingRecordId) {
      skipped.noStagingRecordId.push(m);
      continue;
    }
    const conf = String(m.matchConfidence || "none").toLowerCase();
    if (!allowed.has(conf)) {
      skipped.belowMinConfidence.push(m);
      continue;
    }
    selected.push(m);
  }

  return { selected, skipped, minConfidence, allowed: [...allowed] };
}

/**
 * Phase 4Q — Select Choice property URL ↔ OSM matches for evidence creation.
 * @param {object} matchReport — Phase 4P choice property match JSON
 * @param {{ minConfidence?: string }} opts
 */
export function selectBrandDirectoryEvidenceMatches(matchReport, opts = {}) {
  const minConfidence = (opts.minConfidence || MATCH_CONFIDENCE.MEDIUM).toLowerCase();
  const minRank = CONFIDENCE_RANK[minConfidence] ?? CONFIDENCE_RANK.medium;

  const allowedConf = new Set();
  for (const [level, rank] of Object.entries(CONFIDENCE_RANK)) {
    if (rank >= minRank && rank > 0) allowedConf.add(level);
  }

  const matches = matchReport.matches || [];
  const selected = [];
  const skipped = {
    noCandidateRecordId: [],
    belowMinConfidence: [],
    wrongRecommendedAction: [],
    wrongSourcePolicy: [],
    nonCala: [],
  };

  for (const m of matches) {
    if (!m.matchedCandidateRecordId) {
      skipped.noCandidateRecordId.push(m);
      continue;
    }

    const conf = String(m.candidateMatchConfidence || "none").toLowerCase();
    if (!allowedConf.has(conf)) {
      skipped.belowMinConfidence.push(m);
      continue;
    }

    const action = String(m.recommendedAction || "");
    if (action !== PROPERTY_MATCH_ACTIONS.LINK_CANDIDATE) {
      skipped.wrongRecommendedAction.push(m);
      continue;
    }

    const policy = String(m.sourcePolicyStatus || "").toLowerCase();
    if (!BRAND_DIRECTORY_ALLOWED_SOURCE_POLICY.has(policy)) {
      skipped.wrongSourcePolicy.push(m);
      continue;
    }

    if (!m.inferredCountry || m.inferredCountry === "(unknown)") {
      skipped.nonCala.push(m);
      continue;
    }

    selected.push(m);
  }

  return {
    selected,
    skipped,
    minConfidence,
    allowedConfidence: [...allowedConf],
  };
}

/**
 * @param {object} wikidataReport
 */
export function indexWikidataCandidatesByQid(wikidataReport) {
  const map = new Map();
  for (const c of wikidataReport.candidates || []) {
    const qid = c.sourceRecordId || c.wikidataQid;
    if (qid) map.set(qid, c);
  }
  return map;
}

function pickWikidataDetail(wikidata, match) {
  let payload = {};
  try {
    payload =
      typeof wikidata?.rawPayloadJson === "string"
        ? JSON.parse(wikidata.rawPayloadJson)
        : wikidata?.rawPayload || {};
  } catch {
    payload = {};
  }

  return {
    qid: match.wikidataQid,
    label: wikidata?.rawHotelName || match.wikidataName || payload.label || "",
    description: wikidata?.wikidataDescription || payload.description || "",
    city: wikidata?.rawCity || match.wikidataCity || payload.locatedInAdmin || "",
    country: wikidata?.rawCountry || match.wikidataCountry || payload.countryQuery || "",
    lat: wikidata?.rawLatitude ?? match.wikidataLatitude ?? payload.lat,
    lng: wikidata?.rawLongitude ?? match.wikidataLongitude ?? payload.lng,
    website: wikidata?.rawWebsite || match.wikidataWebsite || payload.website || "",
    operator: wikidata?.wikidataOperator || payload.operator || wikidata?.rawBrand || "",
    owner: wikidata?.wikidataOwner || payload.owner || "",
    wikipediaUrl:
      wikidata?.wikidataWikipediaUrl || payload.wikipediaUrl || "",
    entityUrl: `${WIKIDATA_ENTITY_BASE}${match.wikidataQid}`,
    license: wikidata?.sourceLicense || "CC0",
  };
}

/**
 * @param {object} match
 * @param {object|null} wikidata
 * @param {string} batchId
 */
export function buildEvidenceText(match, wikidata, batchId) {
  const d = pickWikidataDetail(wikidata, match);
  const lines = [
    "Wikidata validation evidence (Phase 4A) — corroborates OSM staging candidate.",
    `Evidence batch: ${batchId}`,
    "",
    "— Wikidata —",
    `QID: ${d.qid}`,
    `Label: ${d.label}`,
    d.description ? `Description: ${d.description}` : null,
    d.city ? `Location: ${d.city}` : null,
    d.country ? `Country: ${d.country}` : null,
    Number.isFinite(d.lat) && Number.isFinite(d.lng)
      ? `Coordinates: ${d.lat}, ${d.lng}`
      : null,
    d.website ? `Official website: ${d.website}` : null,
    d.operator ? `Operator: ${d.operator}` : null,
    d.owner ? `Owner: ${d.owner}` : null,
    d.wikipediaUrl ? `Wikipedia: ${d.wikipediaUrl}` : null,
    `Entity URL: ${d.entityUrl}`,
    `License: ${d.license}`,
    "",
    "— OSM staging candidate —",
    `Source Record ID: ${match.matchedStagingSourceRecordId}`,
    `Name: ${match.matchedStagingName}`,
    match.matchedStagingCity ? `City: ${match.matchedStagingCity}` : null,
    `Airtable record ID: ${match.matchedStagingRecordId}`,
    "",
    "— Match —",
    `Confidence: ${match.matchConfidence}`,
    `Score: ${match.matchScore}`,
    `Reason: ${match.matchReason}`,
    match.distanceMeters !== "" && match.distanceMeters != null
      ? `Distance: ${match.distanceMeters}m`
      : null,
    `Recommended action: ${match.recommendedAction}`,
    "",
    "Human approval required before Verified Independent Hotel Census promotion.",
    "No Hotel Census link created. Not auto-approved.",
  ];
  return lines.filter(Boolean).join("\n");
}

/**
 * @param {object} match
 * @param {object|null} wikidata
 * @param {string} batchId
 */
export function buildEvidenceAirtableFields(match, wikidata, batchId) {
  const d = pickWikidataDetail(wikidata, match);
  const name = evidenceDedupeName(
    batchId,
    match.wikidataQid,
    match.matchedStagingSourceRecordId
  );

  const fields = {
    Name: name,
    [EVIDENCE_FIELDS.evidenceType]: EVIDENCE_TYPES.SOURCE_SNAPSHOT,
    [EVIDENCE_FIELDS.evidenceUrl]: d.entityUrl,
    [EVIDENCE_FIELDS.evidenceText]: buildEvidenceText(match, wikidata, batchId),
    [EVIDENCE_FIELDS.capturedAt]: new Date().toISOString(),
    [EVIDENCE_FIELDS.capturedBy]: EVIDENCE_CAPTURED_BY,
    [EVIDENCE_FIELDS.matchScore]: Number(match.matchScore) || 0,
    [EVIDENCE_FIELDS.matchReason]: String(match.matchReason || ""),
  };

  if (match.matchedStagingRecordId) {
    fields[EVIDENCE_FIELDS.candidate] = [match.matchedStagingRecordId];
  }

  return { name, fields, match, wikidataQid: match.wikidataQid };
}

/**
 * @param {object} match — Phase 4P match row
 * @param {string} batchId
 */
export function buildBrandDirectoryEvidenceText(match, batchId) {
  const lines = [
    "Choice brand-directory sitemap URL evidence (Phase 4Q) — corroborates OSM staging candidate.",
    `Evidence batch: ${batchId}`,
    "",
    `Parent Company: ${match.parentCompany || "Choice Hotels International"}`,
    `Brand Setup Brand: ${match.brandSetupBrand || ""}`,
    `Choice Property URL: ${match.propertyUrl || ""}`,
    `Choice Property ID: ${match.propertyId || ""}`,
    `Inferred Country: ${match.inferredCountry || ""}`,
    `City Slug: ${match.citySlug || ""}`,
    `Candidate Match Confidence: ${match.candidateMatchConfidence || ""}`,
    `Candidate Match Reason: ${match.candidateMatchReason || ""}`,
    `Matched OSM Candidate Record ID: ${match.matchedCandidateRecordId || ""}`,
    `Matched OSM Candidate Name: ${match.matchedCandidateName || ""}`,
    "",
    "Source policy note: Brand-directory sitemap URL evidence; no property HTML fetched; requires human review before promotion.",
    "Human approval required before Verified Independent Hotel Census promotion.",
    "No Hotel Census link created. Not auto-approved.",
  ];
  return lines.filter(Boolean).join("\n");
}

/**
 * @param {object} match
 * @param {string} batchId
 */
export function buildBrandDirectoryEvidenceAirtableFields(match, batchId) {
  const name = brandDirectoryEvidenceDedupeName(
    batchId,
    match.propertyId,
    match.matchedCandidateRecordId
  );

  const fields = {
    Name: name,
    [EVIDENCE_FIELDS.evidenceType]: EVIDENCE_TYPES.SOURCE_SNAPSHOT,
    [EVIDENCE_FIELDS.evidenceUrl]: String(match.propertyUrl || ""),
    [EVIDENCE_FIELDS.evidenceText]: buildBrandDirectoryEvidenceText(match, batchId),
    [EVIDENCE_FIELDS.capturedAt]: new Date().toISOString(),
    [EVIDENCE_FIELDS.capturedBy]: BRAND_DIRECTORY_EVIDENCE_CAPTURED_BY,
    [EVIDENCE_FIELDS.matchScore]: Number(match.candidateMatchScore) || 0,
    [EVIDENCE_FIELDS.matchReason]: String(match.candidateMatchReason || ""),
  };

  if (match.matchedCandidateRecordId) {
    fields[EVIDENCE_FIELDS.candidate] = [match.matchedCandidateRecordId];
  }

  return {
    name,
    fields,
    match,
    choicePropertyId: match.propertyId,
    matchedCandidateRecordId: match.matchedCandidateRecordId,
  };
}

/**
 * @param {import('airtable').Base} base
 * @param {string} tableName
 * @param {string} batchId
 * @param {string} [dedupePrefix] — e.g. "4A" or "4Q"
 */
export async function loadExistingEvidenceDedupeNames(
  base,
  tableName,
  batchId,
  dedupePrefix = "4A"
) {
  const keys = new Set();
  const batchEsc = String(batchId).replace(/'/g, "\\'");
  const prefixEsc = String(dedupePrefix).replace(/'/g, "\\'");
  const formula = `FIND("${prefixEsc}|${batchEsc}|", {Name}) > 0`;

  await new Promise((resolve, reject) => {
    base(tableName)
      .select({
        filterByFormula: formula,
        fields: ["Name", EVIDENCE_FIELDS.capturedBy],
      })
      .eachPage(
        (records, fetchNextPage) => {
          for (const rec of records) {
            const n = rec.fields.Name;
            if (n) keys.add(n);
          }
          fetchNextPage();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  return keys;
}

/**
 * All prior Phase 4U/4X corrected Choice evidence (any batch) by property+candidate.
 * @param {import('airtable').Base} base
 */
export async function loadExistingCorrectedChoiceSemanticKeys(base, tableName) {
  const keys = new Set();
  const formula = `FIND("4U|", {Name}) > 0`;

  await new Promise((resolve, reject) => {
    base(tableName)
      .select({
        filterByFormula: formula,
        fields: ["Name"],
      })
      .eachPage(
        (records, fetchNextPage) => {
          for (const rec of records) {
            const sk = semanticKeyFromCorrectedChoiceEvidenceName(rec.fields.Name);
            if (sk) keys.add(sk);
          }
          fetchNextPage();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  return keys;
}

/**
 * Skip exact Name match or prior corrected evidence for same property+candidate.
 */
export function partitionCorrectedChoiceEvidenceByDuplicate(
  evidenceRows,
  existingNames,
  existingSemanticKeys
) {
  const toWrite = [];
  const skippedDuplicate = [];

  for (const row of evidenceRows) {
    const semantic = correctedChoiceSemanticDedupeKey(
      row.choicePropertyId,
      row.matchedCandidateRecordId
    );
    if (existingNames.has(row.name)) {
      skippedDuplicate.push({ ...row, duplicateReason: "exact_name" });
      continue;
    }
    if (existingSemanticKeys.has(semantic)) {
      skippedDuplicate.push({ ...row, duplicateReason: "prior_corrected_evidence" });
      continue;
    }
    toWrite.push(row);
  }

  return { toWrite, skippedDuplicate };
}

const CREATE_CHUNK = 10;

/**
 * @param {import('airtable').Base} base
 * @param {string} tableName
 * @param {Array<ReturnType<typeof buildEvidenceAirtableFields>>} rows
 * @param {Set<string>} existingNames
 */
export async function createEvidenceRecords(base, tableName, rows, existingNames) {
  const toWrite = [];
  const skippedDuplicate = [];

  for (const row of rows) {
    if (existingNames.has(row.name)) {
      skippedDuplicate.push(row);
      continue;
    }
    toWrite.push(row);
  }

  const created = [];
  for (let i = 0; i < toWrite.length; i += CREATE_CHUNK) {
    const chunk = toWrite.slice(i, i + CREATE_CHUNK);
    const payload = chunk.map((r) => ({ fields: r.fields }));
    const records = await base(tableName).create(payload, { typecast: true });
    for (const rec of records) {
      const src = chunk.find((c) => c.name === rec.fields.Name);
      created.push({
        airtableRecordId: rec.id,
        name: rec.fields.Name,
        wikidataQid: src?.wikidataQid,
        choicePropertyId: src?.choicePropertyId,
        matchedCandidateRecordId: src?.matchedCandidateRecordId,
      });
      existingNames.add(rec.fields.Name);
    }
  }

  return {
    created,
    skippedDuplicate,
    writtenCount: created.length,
  };
}
