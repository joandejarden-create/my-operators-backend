/**
 * Bethesda research consolidation V1 — reconcile preserved 86 vs canonical 59,
 * validate +27 research-only creates, prepare exact Airtable write fields.
 * Non-mutating. Production apply remains founder-gated elsewhere.
 */

import fs from "node:fs";
import path from "node:path";
import {
  buildHotelResearchCompositeKey,
  GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
  GDI_HOTEL_RESEARCH_OPPORTUNITY_SCHEMA_VERSION,
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F,
} from "../hotel-research-opportunity-field-map.js";
import { buildResearchInventoryFields } from "./gdi-hotel-research-inventory-store-v1.js";
import { ROCK_CREEK_VENUE_IDENTITY } from "./gdi-multi-hotel-airtable-reconciliation-v2.js";

export const GDI_BETHESDA_RESEARCH_CONSOLIDATION_V1 =
  "gdi-bethesda-research-consolidation-v1";

export const BETHESDA_HOTEL_ID = "recLuxvwwxID7U2B8";

export const BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION =
  "gdi-bethesda-research-inventory-27-creates-v1";

export const BETHESDA_SOURCE_OF_TRUTH = Object.freeze({
  decision:
    "CANONICAL_HEAD_59_PLUS_PRESERVED_86_SUPERSET_FOR_AUTHORIZED_CREATES",
  canonicalFilesystemOnMain:
    "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json",
  preservedSupersetSnapshot:
    "wt-gdi-isolation-post-365/data/group-demand-intelligence/_wip-preserve-20261118/recLuxvwwxID7U2B8-opportunities.86.json",
  airtableBaselineExpected: 59,
  rules: [
    "Never overwrite or delete the original 59 HEAD opportunities.",
    "Treat preserved 86 as additive research inventory candidate set.",
    "Authorize only the 27 composite keys absent from HEAD/Airtable baseline.",
    "Do not invent demandThesis or firstDiscoveredAt when absent.",
    "WATCHLIST / research-only rows are valid research-inventory destinations.",
    "GDO and publication remain separately gated and out of scope.",
  ],
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadOpportunityFile(filePath) {
  if (!fs.existsSync(filePath)) {
    return { present: false, path: filePath, count: 0, rows: [], byId: new Map() };
  }
  const j = JSON.parse(fs.readFileSync(filePath, "utf8"));
  const arr = Array.isArray(j) ? j : j.opportunities || [];
  const byId = new Map();
  const rows = [];
  for (const o of arr) {
    const id = text(o.id || o.opportunityId);
    if (!id) continue;
    const row = {
      ...o,
      id,
      opportunityId: id,
      hotelId: text(o.hotelId) || BETHESDA_HOTEL_ID,
    };
    byId.set(id, row);
    rows.push(row);
  }
  return {
    present: true,
    path: filePath,
    count: byId.size,
    rows,
    byId,
    hotelId: (!Array.isArray(j) && j.hotelId) || BETHESDA_HOTEL_ID,
    hotelName: (!Array.isArray(j) && j.hotelName) || null,
  };
}

function pickEvidenceSignals(row = {}) {
  const sources = Array.isArray(row.sources) ? row.sources : [];
  const sourceUrls = sources
    .map((s) => text(s?.url || s?.sourceUrl || s))
    .filter(Boolean)
    .slice(0, 8);
  return {
    hasSources: sourceUrls.length > 0,
    sourceUrlCount: sourceUrls.length,
    sourceUrlsSample: sourceUrls.slice(0, 3),
    hasThesisText: Boolean(
      text(row.hotelOpportunityThesis || row.bethesdaWinThesis || row.whyMonitor),
    ),
    hasOrganization: Boolean(text(row.organizationName || row.company)),
    hasTitle: Boolean(text(row.title || row.displayTitle || row.opportunityName)),
    demandThesisRaw: row.demandThesis ?? null,
    firstDiscoveredAtRaw:
      row.firstDiscoveredAt || row.firstSeenAt || row.createdAt || null,
    firstDiscoveredRunIdRaw:
      row.firstDiscoveredRunId || row.firstSeenRunId || row.runId || null,
    lastResearchedAtRaw:
      row.lastResearchedAt || row.updatedAt || row.lastSeenAt || null,
    priority: text(row.priority) || null,
    segment: text(row.segment || row.segmentDisplayLabel) || null,
    disposition: text(row.disposition || row.evaluationLabel) || null,
    inventoryPopulationPrimary: text(row.inventoryPopulationPrimary) || null,
  };
}

/**
 * Scan optional research-state / ledger dirs for opportunityId mentions.
 * Read-only; never invents values when absent.
 */
export function scanLedgersForOpportunityHints(repoRoot, opportunityIds = []) {
  const want = new Set(opportunityIds.map((id) => text(id)).filter(Boolean));
  const hits = {};
  for (const id of want) hits[id] = [];

  const roots = [
    path.join(
      repoRoot,
      "data/group-demand-intelligence/hotels",
      BETHESDA_HOTEL_ID,
    ),
    path.resolve(
      repoRoot,
      "../wt-gdi-isolation-post-365/data/group-demand-intelligence",
    ),
    path.resolve(
      repoRoot,
      "../wt-gdi-isolation-post-365/data/group-demand-intelligence/_wip-preserve-20261118",
    ),
  ];

  function walk(dir, depth = 0) {
    if (depth > 6 || !fs.existsSync(dir)) return;
    let entries = [];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const ent of entries) {
      const p = path.join(dir, ent.name);
      if (ent.isDirectory()) {
        if (
          /node_modules|\.git|from-primary-untracked/i.test(ent.name)
        ) {
          continue;
        }
        walk(p, depth + 1);
        continue;
      }
      if (!/\.(json|md)$/i.test(ent.name)) continue;
      // Skip the giant opportunities preserve itself for id-hit noise control
      if (/opportunities\.86\.json$/i.test(ent.name)) continue;
      let raw = "";
      try {
        const st = fs.statSync(p);
        if (st.size > 2_500_000) continue;
        raw = fs.readFileSync(p, "utf8");
      } catch {
        continue;
      }
      for (const id of want) {
        if (raw.includes(id)) {
          hits[id].push(path.relative(repoRoot, p).replace(/\\/g, "/"));
        }
      }
    }
  }

  for (const r of roots) walk(r, 0);
  return hits;
}

export function resolveBethesdaPaths(repoRoot) {
  return {
    canonical:
      path.join(
        repoRoot,
        "data/group-demand-intelligence/hotels",
        BETHESDA_HOTEL_ID,
        "opportunities.json",
      ),
    preserved: path.resolve(
      repoRoot,
      "../wt-gdi-isolation-post-365/data/group-demand-intelligence/_wip-preserve-20261118/recLuxvwwxID7U2B8-opportunities.86.json",
    ),
    priorManifest: path.join(
      repoRoot,
      "reports/gdi-research-consolidation-and-full-e2e-persistence-v1/NEW_OR_CHANGED_RESEARCH_MANIFEST.json",
    ),
  };
}

export function reconcileBethesdaResearchInventory(repoRoot, opts = {}) {
  const paths = resolveBethesdaPaths(repoRoot);
  const canonical = loadOpportunityFile(paths.canonical);
  const preserved = loadOpportunityFile(paths.preserved);

  const priorIds = new Set();
  if (fs.existsSync(paths.priorManifest)) {
    try {
      const prior = JSON.parse(fs.readFileSync(paths.priorManifest, "utf8"));
      for (const e of prior.entries || []) {
        if (text(e.opportunityId)) priorIds.add(text(e.opportunityId));
      }
    } catch {
      /* ignore */
    }
  }

  const shared = [];
  const onlyCanonical = [];
  const onlyPreserved = [];

  for (const id of canonical.byId.keys()) {
    if (preserved.byId.has(id)) shared.push(id);
    else onlyCanonical.push(id);
  }
  for (const id of preserved.byId.keys()) {
    if (!canonical.byId.has(id)) onlyPreserved.push(id);
  }
  shared.sort();
  onlyCanonical.sort();
  onlyPreserved.sort();

  const ledgerHits =
    opts.scanLedgers === false
      ? {}
      : scanLedgersForOpportunityHints(repoRoot, onlyPreserved);

  const creates = [];
  const validationErrors = [];

  for (const id of onlyPreserved) {
    const row = preserved.byId.get(id);
    const evidence = pickEvidenceSignals(row);
    const built = buildResearchInventoryFields(row, {
      hotelId: BETHESDA_HOTEL_ID,
      hotelName: preserved.hotelName || "Bethesda Marriott",
    });

    if (!built.ok) {
      validationErrors.push({
        opportunityId: id,
        code: built.code,
        detail: built.detail || null,
      });
    }

    const hotelMismatch =
      text(row.hotelId) && text(row.hotelId) !== BETHESDA_HOTEL_ID;

    creates.push({
      hotelId: BETHESDA_HOTEL_ID,
      opportunityId: id,
      compositeKey: buildHotelResearchCompositeKey(BETHESDA_HOTEL_ID, id),
      organizationName: text(row.organizationName || row.company) || null,
      title: text(row.title || row.displayTitle || row.opportunityName) || null,
      segment: evidence.segment,
      priority: evidence.priority,
      disposition: evidence.disposition,
      inventoryPopulationPrimary: evidence.inventoryPopulationPrimary,
      demandThesis: evidence.demandThesisRaw,
      firstDiscoveredAt: evidence.firstDiscoveredAtRaw,
      firstDiscoveredRunId: evidence.firstDiscoveredRunIdRaw,
      lastResearchedAt: evidence.lastResearchedAtRaw,
      inPrior372Manifest: priorIds.has(id),
      hotelRelationshipOk: !hotelMismatch,
      hotelMismatch: hotelMismatch ? text(row.hotelId) : null,
      evidence,
      ledgerPathHints: ledgerHits[id] || [],
      buildOk: built.ok === true,
      buildCode: built.code || null,
      payloadLength: built.payloadLength || null,
      exactAirtableFields: built.ok ? built.fields : null,
      commerciallyQualified: built.commerciallyQualified === true,
      plannedAction: "CREATE",
      destinationTable: GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
      destinationTableId: "tblbkDj0vhQLfrssV",
      gdoWrite: false,
      publicationWrite: false,
      notes: [
        evidence.demandThesisRaw == null || evidence.demandThesisRaw === ""
          ? "demandThesis_absent_preserved_as_null"
          : null,
        !evidence.firstDiscoveredAtRaw
          ? "firstDiscoveredAt_absent_do_not_invent"
          : null,
        (ledgerHits[id] || []).length
          ? "ledger_path_hints_found_for_manual_review"
          : "no_ledger_path_hint_located",
      ].filter(Boolean),
    });
  }

  const fingerprintDrift = [];
  for (const id of shared) {
    const a = canonical.byId.get(id);
    const b = preserved.byId.get(id);
    const aTitle = text(a.title || a.displayTitle);
    const bTitle = text(b.title || b.displayTitle);
    const aOrg = text(a.organizationName || a.company);
    const bOrg = text(b.organizationName || b.company);
    if (aTitle !== bTitle || aOrg !== bOrg) {
      fingerprintDrift.push({
        opportunityId: id,
        canonicalTitle: aTitle,
        preservedTitle: bTitle,
        canonicalOrg: aOrg,
        preservedOrg: bOrg,
      });
    }
  }

  return {
    version: GDI_BETHESDA_RESEARCH_CONSOLIDATION_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_HOTEL_ID,
    paths,
    sourceOfTruth: BETHESDA_SOURCE_OF_TRUTH,
    counts: {
      canonical: canonical.count,
      preserved: preserved.count,
      shared: shared.length,
      onlyCanonical: onlyCanonical.length,
      onlyPreserved: onlyPreserved.length,
      priorManifestCreates: priorIds.size,
      createsReady: creates.filter((c) => c.buildOk && c.hotelRelationshipOk)
        .length,
      validationErrors: validationErrors.length,
      sharedTitleOrgDrift: fingerprintDrift.length,
    },
    sharedIds: shared,
    onlyCanonicalIds: onlyCanonical,
    onlyPreservedIds: onlyPreserved,
    fingerprintDrift,
    creates,
    validationErrors,
    productionWritesAuthorized: false,
    authorizationId: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
    schemaVersion: GDI_HOTEL_RESEARCH_OPPORTUNITY_SCHEMA_VERSION,
  };
}

export function buildBethesda27ProductionManifest(repoRoot) {
  const recon = reconcileBethesdaResearchInventory(repoRoot);
  const entries = recon.creates.map((c) => ({
    hotelId: c.hotelId,
    opportunityId: c.opportunityId,
    compositeKey: c.compositeKey,
    plannedAction: "CREATE",
    destinationTable: c.destinationTable,
    destinationTableId: c.destinationTableId,
    gdoWrite: false,
    publicationWrite: false,
    fields: c.exactAirtableFields,
    fieldNames: c.exactAirtableFields ? Object.keys(c.exactAirtableFields) : [],
    validation: {
      buildOk: c.buildOk,
      hotelRelationshipOk: c.hotelRelationshipOk,
      commerciallyQualified: c.commerciallyQualified,
      notes: c.notes,
    },
    evidenceSummary: {
      organizationName: c.organizationName,
      title: c.title,
      segment: c.segment,
      priority: c.priority,
      demandThesis: c.demandThesis,
      firstDiscoveredAt: c.firstDiscoveredAt,
      firstDiscoveredRunId: c.firstDiscoveredRunId,
      sourceUrlCount: c.evidence?.sourceUrlCount || 0,
      hasThesisText: c.evidence?.hasThesisText === true,
      ledgerPathHints: c.ledgerPathHints,
    },
  }));

  const blockers = [];
  if (recon.counts.canonical !== 59) {
    blockers.push({
      code: "UNEXPECTED_CANONICAL_COUNT",
      expected: 59,
      actual: recon.counts.canonical,
    });
  }
  if (recon.counts.preserved !== 86) {
    blockers.push({
      code: "UNEXPECTED_PRESERVED_COUNT",
      expected: 86,
      actual: recon.counts.preserved,
    });
  }
  if (recon.counts.onlyPreserved !== 27) {
    blockers.push({
      code: "UNEXPECTED_CREATE_COUNT",
      expected: 27,
      actual: recon.counts.onlyPreserved,
    });
  }
  if (recon.counts.onlyCanonical !== 0) {
    blockers.push({
      code: "CANONICAL_IDS_MISSING_FROM_PRESERVED",
      ids: recon.onlyCanonicalIds,
    });
  }
  for (const e of entries) {
    if (!e.validation.buildOk || !e.validation.hotelRelationshipOk) {
      blockers.push({
        code: "CREATE_ENTRY_INVALID",
        opportunityId: e.opportunityId,
        validation: e.validation,
      });
    }
    // Ensure we never include firstDiscovered* unless present on source
    if (
      e.fields &&
      e.fields[F.firstDiscoveredAt] &&
      !e.evidenceSummary.firstDiscoveredAt
    ) {
      blockers.push({
        code: "INVENTED_FIRST_DISCOVERED_AT",
        opportunityId: e.opportunityId,
      });
    }
  }

  return {
    version: GDI_BETHESDA_RESEARCH_CONSOLIDATION_V1,
    authorizationId: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
    generatedAt: recon.generatedAt,
    baseId: "appa2cE7FTRmIbB32",
    tableName: GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
    tableId: "tblbkDj0vhQLfrssV",
    hotelId: BETHESDA_HOTEL_ID,
    productionWritesAuthorized: false,
    applied: false,
    gdoWrites: 0,
    publicationWrites: 0,
    expectedCreates: 27,
    entryCount: entries.length,
    readyForFounderAuthorization:
      blockers.length === 0 && entries.length === 27,
    blockers,
    sourceOfTruth: BETHESDA_SOURCE_OF_TRUTH,
    preservedSnapshotPath: recon.paths.preserved,
    canonicalPath: recon.paths.canonical,
    entries,
    reconciliationCounts: recon.counts,
  };
}

export function buildRockCreekCorrectionManifest() {
  return {
    version: GDI_BETHESDA_RESEARCH_CONSOLIDATION_V1,
    writeKind: "SINGLE_FIELD_VENUE_ID_CORRECTION",
    applied: false,
    productionWritesAuthorized: false,
    separateFromBethesdaResearchMigration: true,
    baseId: "appa2cE7FTRmIbB32",
    tableName: "Private Event Venues",
    airtableRecordId: ROCK_CREEK_VENUE_IDENTITY.airtableRecordId,
    field: "Venue ID",
    beforeKnownAliases: [
      ROCK_CREEK_VENUE_IDENTITY.structuredIdAfterPilot,
      ROCK_CREEK_VENUE_IDENTITY.aliasErroneousPilotId,
    ],
    after: ROCK_CREEK_VENUE_IDENTITY.authoritativeCanonicalId,
    preserveAllOtherFields: true,
    preserveNoLodging: true,
    venueName: ROCK_CREEK_VENUE_IDENTITY.venueName,
    officialDomain: ROCK_CREEK_VENUE_IDENTITY.officialDomain,
    note: ROCK_CREEK_VENUE_IDENTITY.divergenceCause,
  };
}

export default {
  GDI_BETHESDA_RESEARCH_CONSOLIDATION_V1,
  BETHESDA_HOTEL_ID,
  BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
  BETHESDA_SOURCE_OF_TRUTH,
  reconcileBethesdaResearchInventory,
  buildBethesda27ProductionManifest,
  buildRockCreekCorrectionManifest,
  resolveBethesdaPaths,
  scanLedgersForOpportunityHints,
};
