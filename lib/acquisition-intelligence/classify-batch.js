/**
 * Batch classify Acquisition Network Relationships for one user.
 * Dry-run by default. Preserves Manual classification overrides.
 */

import {
  GTM_ACQUISITION_RELATIONSHIPS_TABLE,
  MAP_ACQUISITION_RELATIONSHIP as R,
  ACQUISITION_CLASSIFIER_VERSION,
} from "./field-map.js";
import {
  assertGtmBaseConfigured,
  assertNotProductBase,
  getGtmAirtableBase,
} from "../gtm-owner-target/platform-base.js";
import { GTM_OWNER_TARGET_TABLES, MAP_GTM_OWNER_TARGET } from "../gtm-owner-target/field-map.js";
import { normalizeOwnerKey } from "../gtm-owner-target/normalize.js";
import {
  classifyAcquisitionRelationship,
  classificationFieldsEqual,
  bandRank,
} from "./classify-relationship.js";

async function listAll(base, tableName, fields, filterByFormula) {
  const out = [];
  /** @type {Record<string, unknown>} */
  const selectOpts = { pageSize: 100 };
  if (Array.isArray(fields) && fields.length) {
    selectOpts.fields = fields;
  }
  // Airtable SDK rejects filterByFormula when the key is present but not a string
  // (e.g. explicit `undefined` from `filterByFormula || undefined`).
  if (typeof filterByFormula === "string" && filterByFormula.trim()) {
    selectOpts.filterByFormula = filterByFormula;
  }
  await base(tableName)
    .select(selectOpts)
    .eachPage((records, next) => {
      for (const rec of records) out.push({ id: rec.id, fields: rec.fields || {} });
      next();
    });
  return out;
}

/**
 * @returns {Promise<Map<string, { id: string, ownerName: string }>>}
 */
export async function loadOwnerTargetIndex() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();
  const rows = await listAll(base, GTM_OWNER_TARGET_TABLES.ownerTargets, [
    MAP_GTM_OWNER_TARGET.ownerName,
    MAP_GTM_OWNER_TARGET.ownerNameNormalized,
  ]);
  const map = new Map();
  for (const row of rows) {
    const name = String(row.fields[MAP_GTM_OWNER_TARGET.ownerName] || "").trim();
    const norm =
      String(row.fields[MAP_GTM_OWNER_TARGET.ownerNameNormalized] || "").trim() ||
      normalizeOwnerKey(name);
    if (!norm) continue;
    map.set(norm, { id: row.id, ownerName: name || norm });
  }
  return map;
}

function emptyBandCounts() {
  return { High: 0, Medium: 0, Low: 0, Unknown: 0 };
}

/**
 * @param {string} sourceUserId
 * @param {{ dryRun?: boolean, limit?: number }} [opts]
 */
export async function classifyAcquisitionNetworkForUser(sourceUserId, opts = {}) {
  const uid = String(sourceUserId || "").trim();
  if (!uid) {
    return {
      ok: false,
      error: "missing_source_user_id",
      message: "sourceUserId required.",
    };
  }

  const isDryRun = opts.dryRun !== false;
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const formula = `{${R.sourceUserId}} = "${uid.replace(/"/g, '\\"')}"`;
  const relFields = [
    R.relationshipName,
    R.sourceUserId,
    R.position,
    R.company,
    R.firstName,
    R.lastName,
    R.acquisitionRole,
    R.personCompanyClass,
    R.directProspectPotential,
    R.connectorPotential,
    R.decisionVisibility,
    R.calaRelevance,
    R.classificationConfidence,
    R.scoreExplanation,
    R.researchQueueEligibility,
    R.classificationSource,
    R.classifierVersion,
    R.classifiedAt,
    R.existingOwnerTargetMatch,
    R.existingOwnerTargetName,
    R.relationshipStrength,
    R.status,
  ];

  let relationships = await listAll(base, GTM_ACQUISITION_RELATIONSHIPS_TABLE, relFields, formula);
  if (opts.limit && Number(opts.limit) > 0) {
    relationships = relationships.slice(0, Number(opts.limit));
  }

  if (!relationships.length) {
    return {
      ok: true,
      dryRun: isDryRun,
      empty: true,
      validation: { pass: true, failedChecks: [] },
      summary: {
        total: 0,
        wouldUpdate: 0,
        updated: 0,
        skippedManual: 0,
        skippedUnchanged: 0,
        failed: 0,
        classifierVersion: ACQUISITION_CLASSIFIER_VERSION,
        dryRun: isDryRun,
        bands: {
          directProspect: emptyBandCounts(),
          connector: emptyBandCounts(),
          decisionVisibility: emptyBandCounts(),
        },
        cala: {},
        roles: {},
        researchQueue: {
          "Research Priority": 0,
          "Research Candidate": 0,
          "No Research Yet": 0,
        },
        existingOwnerTargetMatch: { Yes: 0, No: 0, Uncertain: 0 },
        relevantConnections: 0,
      },
      review: {
        topDirectProspects: [],
        topConnectors: [],
        topDecisionSignal: [],
        dedupedReviewPool: [],
      },
      fieldMapping: {
        classifier: ACQUISITION_CLASSIFIER_VERSION,
        overwrittenWhen: "Classification Source != Manual",
        neverTouched: ["Relationship Strength", "Notes"],
      },
      message:
        "No acquisition relationships to classify yet. Click Import Connections first (Preview alone does not write records).",
    };
  }

  const ownerIndexByKey = await loadOwnerTargetIndex();

  const summary = {
    total: relationships.length,
    wouldUpdate: 0,
    updated: 0,
    skippedManual: 0,
    skippedUnchanged: 0,
    failed: 0,
    classifierVersion: ACQUISITION_CLASSIFIER_VERSION,
    dryRun: isDryRun,
    bands: {
      directProspect: emptyBandCounts(),
      connector: emptyBandCounts(),
      decisionVisibility: emptyBandCounts(),
    },
    cala: {},
    roles: {},
    researchQueue: {
      "Research Priority": 0,
      "Research Candidate": 0,
      "No Research Yet": 0,
    },
    existingOwnerTargetMatch: { Yes: 0, No: 0, Uncertain: 0 },
    relevantConnections: 0,
  };

  /** @type {object[]} */
  const classifiedRows = [];
  /** @type {{ id: string, patch: Record<string, unknown> }[]} */
  const patches = [];

  for (const rel of relationships) {
    const classified = classifyAcquisitionRelationship({
      position: rel.fields[R.position],
      company: rel.fields[R.company],
      firstName: rel.fields[R.firstName],
      lastName: rel.fields[R.lastName],
      existingFields: rel.fields,
      ownerIndexByKey,
    });

    if (classified.skipped) {
      summary.skippedManual++;
      // Still count existing manual values in summary if present
      const role = String(rel.fields[R.acquisitionRole] || "Unclassified");
      summary.roles[role] = (summary.roles[role] || 0) + 1;
      continue;
    }

    const result = classified.result;
    summary.bands.directProspect[result.directProspectPotential]++;
    summary.bands.connector[result.connectorPotential]++;
    summary.bands.decisionVisibility[result.decisionVisibility]++;
    summary.cala[result.calaRelevance] = (summary.cala[result.calaRelevance] || 0) + 1;
    summary.roles[result.acquisitionRole] = (summary.roles[result.acquisitionRole] || 0) + 1;
    summary.researchQueue[result.researchQueueEligibility]++;
    summary.existingOwnerTargetMatch[result.existingOwnerTargetMatch]++;

    if (
      result.researchQueueEligibility === "Research Priority" ||
      result.researchQueueEligibility === "Research Candidate"
    ) {
      summary.relevantConnections++;
    }

    classifiedRows.push({
      id: rel.id,
      name: rel.fields[R.relationshipName] || "",
      position: rel.fields[R.position] || "",
      company: rel.fields[R.company] || "",
      acquisitionRole: result.acquisitionRole,
      directProspectPotential: result.directProspectPotential,
      connectorPotential: result.connectorPotential,
      decisionVisibility: result.decisionVisibility,
      calaRelevance: result.calaRelevance,
      classificationConfidence: result.classificationConfidence,
      researchQueueEligibility: result.researchQueueEligibility,
      scoreExplanation: result.scoreExplanation,
      existingOwnerTargetMatch: result.existingOwnerTargetMatch,
    });

    if (classificationFieldsEqual(classified.fields, rel.fields)) {
      summary.skippedUnchanged++;
      continue;
    }

    summary.wouldUpdate++;
    patches.push({ id: rel.id, patch: classified.fields });
  }

  if (!isDryRun && patches.length) {
    for (let i = 0; i < patches.length; i += 10) {
      const chunk = patches.slice(i, i + 10).map((p) => ({ id: p.id, fields: p.patch }));
      try {
        await base(GTM_ACQUISITION_RELATIONSHIPS_TABLE).update(chunk);
        summary.updated += chunk.length;
      } catch (err) {
        summary.failed += chunk.length;
        console.error(
          "[acquisition-intelligence:classify-batch] update_failed",
          err?.message || err
        );
      }
    }
  }

  const review = buildClassificationReviewSample(classifiedRows);

  return {
    ok: true,
    dryRun: isDryRun,
    validation: { pass: true, failedChecks: [] },
    summary,
    review,
    fieldMapping: {
      classifier: ACQUISITION_CLASSIFIER_VERSION,
      overwrittenWhen: "Classification Source != Manual",
      neverTouched: ["Relationship Strength", "Notes"],
    },
  };
}

/**
 * @param {object[]} rows
 */
export function buildClassificationReviewSample(rows) {
  const byDirect = [...rows].sort(
    (a, b) =>
      bandRank(b.directProspectPotential) - bandRank(a.directProspectPotential) ||
      bandRank(b.classificationConfidence) - bandRank(a.classificationConfidence)
  );
  const byConnector = [...rows].sort(
    (a, b) =>
      bandRank(b.connectorPotential) - bandRank(a.connectorPotential) ||
      bandRank(b.classificationConfidence) - bandRank(a.classificationConfidence)
  );
  const byDecisionCala = [...rows].sort((a, b) => {
    const calaBoost = (r) =>
      ["Mexico", "Dominican Republic", "Costa Rica", "Colombia", "Guatemala", "Wider CALA"].includes(
        r.calaRelevance
      )
        ? 1
        : 0;
    return (
      bandRank(b.decisionVisibility) + calaBoost(b) * 0.5 -
      (bandRank(a.decisionVisibility) + calaBoost(a) * 0.5) ||
      bandRank(b.connectorPotential) - bandRank(a.connectorPotential)
    );
  });

  const pick = (list, n = 20) =>
    list.slice(0, n).map((r) => ({
      name: r.name,
      position: r.position,
      company: r.company,
      acquisitionRole: r.acquisitionRole,
      directProspectPotential: r.directProspectPotential,
      connectorPotential: r.connectorPotential,
      decisionVisibility: r.decisionVisibility,
      calaRelevance: r.calaRelevance,
      confidence: r.classificationConfidence,
      reason: r.scoreExplanation,
      researchQueueEligibility: r.researchQueueEligibility,
    }));

  const topDirect = pick(byDirect);
  const topConnector = pick(byConnector);
  const topDecision = pick(byDecisionCala);

  const seen = new Set();
  const deduped = [];
  for (const row of [...topDirect, ...topConnector, ...topDecision]) {
    const key = `${row.name}|${row.company}|${row.position}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(row);
  }

  return {
    topDirectProspects: topDirect,
    topConnectors: topConnector,
    topDecisionSignal: topDecision,
    dedupedReviewPool: deduped,
  };
}

/**
 * List classified relationships for UI table (user-scoped).
 * @param {string} sourceUserId
 * @param {{ limit?: number, researchOnly?: boolean }} [opts]
 */
export async function listClassifiedRelationshipsForUser(sourceUserId, opts = {}) {
  const uid = String(sourceUserId || "").trim();
  if (!uid) return { ok: false, error: "missing_source_user_id", rows: [] };

  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();
  const limit = Math.min(Math.max(Number(opts.limit) || 50, 1), 200);

  let formula = `{${R.sourceUserId}} = "${uid.replace(/"/g, '\\"')}"`;
  if (opts.researchOnly) {
    formula = `AND(${formula}, OR({${R.researchQueueEligibility}} = "Research Priority", {${R.researchQueueEligibility}} = "Research Candidate"))`;
  }

  const rows = [];
  await base(GTM_ACQUISITION_RELATIONSHIPS_TABLE)
    .select({
      filterByFormula: formula,
      maxRecords: limit,
      sort: [{ field: R.relationshipName, direction: "asc" }],
      fields: [
        R.relationshipName,
        R.position,
        R.company,
        R.acquisitionRole,
        R.directProspectPotential,
        R.connectorPotential,
        R.decisionVisibility,
        R.calaRelevance,
        R.classificationConfidence,
        R.researchQueueEligibility,
        R.scoreExplanation,
      ],
    })
    .eachPage((records, next) => {
      for (const rec of records) {
        rows.push({
          id: rec.id,
          name: rec.fields[R.relationshipName] || "",
          position: rec.fields[R.position] || "",
          company: rec.fields[R.company] || "",
          acquisitionRole: rec.fields[R.acquisitionRole] || "Unclassified",
          directProspectPotential: rec.fields[R.directProspectPotential] || "Unknown",
          connectorPotential: rec.fields[R.connectorPotential] || "Unknown",
          decisionVisibility: rec.fields[R.decisionVisibility] || "Unknown",
          calaRelevance: rec.fields[R.calaRelevance] || "Unknown",
          classificationConfidence: rec.fields[R.classificationConfidence] || "Low",
          researchQueueEligibility: rec.fields[R.researchQueueEligibility] || "No Research Yet",
        });
      }
      next();
    });

  // Prefer research-priority ordering in memory
  rows.sort(
    (a, b) =>
      bandRank(b.directProspectPotential) + bandRank(b.connectorPotential) -
      (bandRank(a.directProspectPotential) + bandRank(a.connectorPotential))
  );

  return { ok: true, rows: rows.slice(0, limit) };
}

/**
 * Aggregate metrics for summary UI after classification.
 * @param {string} sourceUserId
 */
export async function getAcquisitionClassificationSummaryForUser(sourceUserId) {
  const uid = String(sourceUserId || "").trim();
  if (!uid) return { ok: false, error: "missing_source_user_id" };

  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();
  const formula = `{${R.sourceUserId}} = "${uid.replace(/"/g, '\\"')}"`;

  const rows = await listAll(
    base,
    GTM_ACQUISITION_RELATIONSHIPS_TABLE,
    [
      R.directProspectPotential,
      R.connectorPotential,
      R.decisionVisibility,
      R.calaRelevance,
      R.researchQueueEligibility,
      R.acquisitionRole,
      R.existingOwnerTargetMatch,
      R.status,
    ],
    formula
  );

  const metrics = {
    totalConnections: rows.length,
    relevantConnections: 0,
    highDirectProspects: 0,
    highConnectors: 0,
    highDecisionVisibility: 0,
    calaRelevant: 0,
    researchPriority: 0,
    researchCandidate: 0,
    unclassified: 0,
    lowRelevance: 0,
    existingOwnerTargetMatches: 0,
    bands: {
      directProspect: emptyBandCounts(),
      connector: emptyBandCounts(),
      decisionVisibility: emptyBandCounts(),
    },
  };

  const calaPriority = new Set([
    "Mexico",
    "Dominican Republic",
    "Costa Rica",
    "Colombia",
    "Guatemala",
    "Wider CALA",
  ]);

  for (const row of rows) {
    const d = row.fields[R.directProspectPotential] || "Unknown";
    const c = row.fields[R.connectorPotential] || "Unknown";
    const v = row.fields[R.decisionVisibility] || "Unknown";
    const cala = row.fields[R.calaRelevance] || "Unknown";
    const rq = row.fields[R.researchQueueEligibility] || "No Research Yet";
    const role = row.fields[R.acquisitionRole] || "Unclassified";

    if (metrics.bands.directProspect[d] != null) metrics.bands.directProspect[d]++;
    if (metrics.bands.connector[c] != null) metrics.bands.connector[c]++;
    if (metrics.bands.decisionVisibility[v] != null) metrics.bands.decisionVisibility[v]++;

    if (d === "High") metrics.highDirectProspects++;
    if (c === "High") metrics.highConnectors++;
    if (v === "High") metrics.highDecisionVisibility++;
    if (calaPriority.has(cala)) metrics.calaRelevant++;
    if (rq === "Research Priority") metrics.researchPriority++;
    if (rq === "Research Candidate") metrics.researchCandidate++;
    if (rq === "Research Priority" || rq === "Research Candidate") metrics.relevantConnections++;
    if (role === "Unclassified") metrics.unclassified++;
    if (role === "Low Relevance") metrics.lowRelevance++;
    if (row.fields[R.existingOwnerTargetMatch] === "Yes") metrics.existingOwnerTargetMatches++;
  }

  return { ok: true, metrics };
}
