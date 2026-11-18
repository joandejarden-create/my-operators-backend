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
import {
  buildResearchInventoryFields,
  findResearchInventoryByCompositeKey,
  upsertResearchInventoryRecord,
  readBackResearchInventory,
  listResearchInventoryForHotel,
  RESEARCH_PAYLOAD_MAX_CHARS,
} from "./gdi-hotel-research-inventory-store-v1.js";
import { ROCK_CREEK_VENUE_IDENTITY } from "./gdi-multi-hotel-airtable-reconciliation-v2.js";

export const GDI_BETHESDA_RESEARCH_CONSOLIDATION_V1 =
  "gdi-bethesda-research-consolidation-v1";

export const BETHESDA_HOTEL_ID = "recLuxvwwxID7U2B8";

export const BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION =
  "gdi-bethesda-research-inventory-27-creates-v1";

/** Frozen exact allowlist — identity set authorized for production CREATE. */
export const BETHESDA_27_AUTHORIZED_CREATE_OPPORTUNITY_IDS = Object.freeze([
  "disp_recLuxvwwxID7U2B8_aledade",
  "disp_recLuxvwwxID7U2B8_city_of_rockville",
  "disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_",
  "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
  "disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup",
  "disp_recLuxvwwxID7U2B8_linder_global_events",
  "disp_recLuxvwwxID7U2B8_montgomery_county_shrm",
  "disp_recLuxvwwxID7U2B8_nature_forward",
  "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
  "disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter",
  "disp_recLuxvwwxID7U2B8_strathmore",
  "gdi_beth_v16_evt_0ab91e208405",
  "gdi_beth_v16_evt_90a6ddcb10ca",
  "gdi_e2d_bc0a94d7c6007f36",
  "gdi_pep_live_af0711e711ba",
  "gdi_pep_live_c165c897d233",
  "gdi_plp_live_4958569d826e",
  "gdi_plp_live_fa2063dc1195",
  "gdi_s4912_364713d41c67",
  "gdi_s4912_512deebd0963",
  "gdi_s4912_59382d1e71a2",
  "gdi_s4912_66a1f89e5afb",
  "gdi_s4912_6c8a62552bfa",
  "gdi_s4912_6ffc303bf55a",
  "gdi_s4912_732496c2f8aa",
  "gdi_s4912_b0ea89e0716d",
  "gdi_s4912_d55ee8a8e9ca",
]);

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

/**
 * Build the exact 27-record CREATE manifest from the frozen allowlist +
 * preserved 86 snapshot (additive source). Remains valid after canonical
 * filesystem is aligned to 86.
 */
export function buildBethesda27ProductionManifest(repoRoot) {
  const recon = reconcileBethesdaResearchInventory(repoRoot, {
    scanLedgers: false,
  });
  const paths = resolveBethesdaPaths(repoRoot);
  const preserved = loadOpportunityFile(paths.preserved);
  const allowIds = BETHESDA_27_AUTHORIZED_CREATE_OPPORTUNITY_IDS;
  const entries = [];
  const blockers = [];

  if (preserved.count !== 86) {
    blockers.push({
      code: "UNEXPECTED_PRESERVED_COUNT",
      expected: 86,
      actual: preserved.count,
    });
  }
  if (recon.counts.onlyCanonical !== 0) {
    blockers.push({
      code: "CANONICAL_IDS_MISSING_FROM_PRESERVED",
      ids: recon.onlyCanonicalIds,
    });
  }
  if (![59, 86].includes(recon.counts.canonical)) {
    blockers.push({
      code: "UNEXPECTED_CANONICAL_COUNT",
      expected: "59_or_86",
      actual: recon.counts.canonical,
    });
  }

  for (const id of allowIds) {
    const row = preserved.byId.get(id);
    if (!row) {
      blockers.push({ code: "ALLOWLIST_ID_MISSING_FROM_PRESERVED", opportunityId: id });
      continue;
    }
    const evidence = pickEvidenceSignals(row);
    const built = buildResearchInventoryFields(row, {
      hotelId: BETHESDA_HOTEL_ID,
      hotelName: preserved.hotelName || "Bethesda Marriott",
    });
    const hotelMismatch =
      text(row.hotelId) && text(row.hotelId) !== BETHESDA_HOTEL_ID;
    const notes = [
      evidence.demandThesisRaw == null || evidence.demandThesisRaw === ""
        ? "demandThesis_absent_preserved_as_null"
        : null,
      !evidence.firstDiscoveredAtRaw
        ? "firstDiscoveredAt_absent_do_not_invent"
        : null,
    ].filter(Boolean);

    const entry = {
      hotelId: BETHESDA_HOTEL_ID,
      opportunityId: id,
      compositeKey: buildHotelResearchCompositeKey(BETHESDA_HOTEL_ID, id),
      plannedAction: "CREATE",
      destinationTable: GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
      destinationTableId: "tblbkDj0vhQLfrssV",
      gdoWrite: false,
      publicationWrite: false,
      fields: built.ok ? built.fields : null,
      fieldNames: built.ok ? Object.keys(built.fields) : [],
      validation: {
        buildOk: built.ok === true,
        hotelRelationshipOk: !hotelMismatch,
        commerciallyQualified: built.commerciallyQualified === true,
        notes,
        buildCode: built.code || null,
        payloadLength: built.payloadLength || null,
      },
      evidenceSummary: {
        organizationName: text(row.organizationName || row.company) || null,
        title: text(row.title || row.displayTitle || row.opportunityName) || null,
        segment: evidence.segment,
        priority: evidence.priority,
        demandThesis: evidence.demandThesisRaw,
        firstDiscoveredAt: evidence.firstDiscoveredAtRaw,
        firstDiscoveredRunId: evidence.firstDiscoveredRunIdRaw,
        sourceUrlCount: evidence.sourceUrlCount || 0,
        hasThesisText: evidence.hasThesisText === true,
        ledgerPathHints: [],
      },
    };
    entries.push(entry);

    if (!entry.validation.buildOk || !entry.validation.hotelRelationshipOk) {
      blockers.push({
        code: "CREATE_ENTRY_INVALID",
        opportunityId: id,
        validation: entry.validation,
      });
    }
    if (
      entry.fields &&
      entry.fields[F.firstDiscoveredAt] &&
      !entry.evidenceSummary.firstDiscoveredAt
    ) {
      blockers.push({
        code: "INVENTED_FIRST_DISCOVERED_AT",
        opportunityId: id,
      });
    }
    if (
      entry.fields?.researchPayloadJson &&
      String(entry.fields.researchPayloadJson).length > RESEARCH_PAYLOAD_MAX_CHARS
    ) {
      blockers.push({
        code: "PAYLOAD_EXCEEDS_AIRTABLE_LIMIT",
        opportunityId: id,
        length: String(entry.fields.researchPayloadJson).length,
      });
    }
    const summary = entry.fields?.[F.sourceSummary];
    if (typeof summary === "string" && summary.includes("[object Object]")) {
      blockers.push({
        code: "SOURCE_SUMMARY_SERIALIZATION_PLACEHOLDER",
        opportunityId: id,
      });
    }
  }

  if (entries.length !== 27) {
    blockers.push({
      code: "UNEXPECTED_CREATE_COUNT",
      expected: 27,
      actual: entries.length,
    });
  }

  return {
    version: GDI_BETHESDA_RESEARCH_CONSOLIDATION_V1,
    authorizationId: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
    generatedAt: new Date().toISOString(),
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
    preservedSnapshotPath: paths.preserved,
    canonicalPath: paths.canonical,
    authorizedOpportunityIds: [...allowIds],
    entries,
    reconciliationCounts: recon.counts,
  };
}

export function validateBethesda27Authorization(input = {}) {
  const authorizeProductionWrites = input.authorizeProductionWrites === true;
  const exactWriteSetAuthorization = text(input.exactWriteSetAuthorization);
  const apply = input.apply === true;
  if (!authorizeProductionWrites) {
    return { ok: false, code: "MISSING_AUTHORIZE_PRODUCTION_WRITES" };
  }
  if (exactWriteSetAuthorization !== BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION) {
    return {
      ok: false,
      code: "INVALID_EXACT_WRITE_SET_AUTHORIZATION",
      expected: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
    };
  }
  if (!apply) return { ok: true, apply: false, code: "DRY_RUN_ONLY" };
  return {
    ok: true,
    apply: true,
    code: "AUTHORIZED_BETHESDA_27_RESEARCH_CREATES",
  };
}

/**
 * Live preflight against Airtable for the exact 27-key allowlist.
 * Never mutates.
 */
export async function preflightBethesda27ResearchCreates(repoRoot) {
  const manifest = buildBethesda27ProductionManifest(repoRoot);
  const uniqueKeys = new Set(manifest.entries.map((e) => e.compositeKey));
  const sourceSummaryPlaceholders = manifest.entries.filter((e) =>
    String(e.fields?.[F.sourceSummary] || "").includes("[object Object]"),
  );
  const payloadOverLimit = manifest.entries.filter(
    (e) =>
      String(e.fields?.researchPayloadJson || "").length >
      RESEARCH_PAYLOAD_MAX_CHARS,
  );

  let liveBethesda = [];
  let liveError = null;
  try {
    liveBethesda = await listResearchInventoryForHotel(BETHESDA_HOTEL_ID);
  } catch (err) {
    liveError = String(err?.message || err);
  }

  const liveByKey = new Map();
  for (const rec of liveBethesda) {
    const key = text(rec.fields?.[F.compositeKey]);
    if (key) liveByKey.set(key, rec);
  }

  const alreadyPresent = [];
  const missing = [];
  for (const e of manifest.entries) {
    const existing = liveByKey.get(e.compositeKey) || null;
    if (existing) {
      alreadyPresent.push({
        opportunityId: e.opportunityId,
        compositeKey: e.compositeKey,
        airtableRecordId: existing.id,
        researchLifecycleStatus:
          existing.fields?.[F.researchLifecycleStatus] || null,
        priority: existing.fields?.[F.priority] || null,
      });
    } else {
      missing.push({
        opportunityId: e.opportunityId,
        compositeKey: e.compositeKey,
      });
    }
  }

  const original59Ids = [...reconOriginal59Ids(repoRoot)];
  const liveOriginal59 = original59Ids.filter((id) =>
    liveByKey.has(buildHotelResearchCompositeKey(BETHESDA_HOTEL_ID, id)),
  );

  const stopReasons = [...manifest.blockers];
  if (liveError) {
    stopReasons.push({ code: "LIVE_AIRTABLE_QUERY_FAILED", detail: liveError });
  }
  if (uniqueKeys.size !== 27) {
    stopReasons.push({
      code: "NON_UNIQUE_COMPOSITE_KEYS",
      actual: uniqueKeys.size,
    });
  }
  if (sourceSummaryPlaceholders.length) {
    stopReasons.push({
      code: "SOURCE_SUMMARY_PLACEHOLDERS",
      count: sourceSummaryPlaceholders.length,
    });
  }
  if (payloadOverLimit.length) {
    stopReasons.push({
      code: "PAYLOAD_OVER_LIMIT",
      count: payloadOverLimit.length,
    });
  }
  if (!liveError && liveOriginal59.length !== 59) {
    stopReasons.push({
      code: "ORIGINAL_59_NOT_FULLY_PRESENT_IN_AIRTABLE",
      expected: 59,
      actual: liveOriginal59.length,
    });
  }

  return {
    ok: stopReasons.length === 0,
    authorizationId: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
    baseId: "appa2cE7FTRmIbB32",
    tableId: "tblbkDj0vhQLfrssV",
    hotelId: BETHESDA_HOTEL_ID,
    uniqueCompositeKeys: uniqueKeys.size,
    entryCount: manifest.entries.length,
    sourceSummaryPlaceholderCount: sourceSummaryPlaceholders.length,
    payloadOverLimitCount: payloadOverLimit.length,
    liveBethesdaCount: liveError ? null : liveBethesda.length,
    original59PresentInAirtable: liveOriginal59.length,
    alreadyPresentCount: alreadyPresent.length,
    proposedCreateCount: missing.length,
    alreadyPresent,
    missing,
    stopReasons,
    gdoWrites: 0,
    publicationWrites: 0,
    rockCreekExcluded: true,
    manifestReady: manifest.readyForFounderAuthorization === true,
    reconciliationCounts: manifest.reconciliationCounts,
  };
}

function reconOriginal59Ids(repoRoot) {
  const paths = resolveBethesdaPaths(repoRoot);
  const preserved = loadOpportunityFile(paths.preserved);
  const allow = new Set(BETHESDA_27_AUTHORIZED_CREATE_OPPORTUNITY_IDS);
  const ids = [];
  for (const id of preserved.byId.keys()) {
    if (!allow.has(id)) ids.push(id);
  }
  // Prefer canonical snapshot of pre-merge 59 when still 59
  const canonical = loadOpportunityFile(paths.canonical);
  if (canonical.count === 59) {
    return [...canonical.byId.keys()].sort();
  }
  return ids.sort();
}

/**
 * CREATE-only production apply for missing keys from the approved 27 allowlist.
 * Never updates existing rows. Never writes GDO/publication.
 */
export async function executeBethesda27ResearchCreates({
  repoRoot,
  apply = false,
  authorizeProductionWrites = false,
  exactWriteSetAuthorization = "",
  sleepMs = 220,
} = {}) {
  const auth = validateBethesda27Authorization({
    authorizeProductionWrites,
    exactWriteSetAuthorization,
    apply,
  });
  const preflight = await preflightBethesda27ResearchCreates(repoRoot);
  const manifest = buildBethesda27ProductionManifest(repoRoot);
  const paths = resolveBethesdaPaths(repoRoot);
  const preserved = loadOpportunityFile(paths.preserved);
  const hotel = {
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: preserved.hotelName || "Bethesda Marriott",
  };

  if (!preflight.ok) {
    return {
      ok: false,
      blocked: true,
      code: "PREFLIGHT_BLOCKED",
      auth,
      preflight,
      results: [],
      counts: { created: 0, alreadyPresent: 0, failed: 0, unchanged: 0 },
      gdoWrites: 0,
      publicationWrites: 0,
    };
  }

  const needsAuth = apply === true;
  if (needsAuth && !auth.ok) {
    return {
      ok: false,
      blocked: true,
      code: auth.code,
      auth,
      preflight,
      results: [],
      counts: { created: 0, alreadyPresent: 0, failed: 0, unchanged: 0 },
      gdoWrites: 0,
      publicationWrites: 0,
    };
  }

  const liveApply = apply && auth.apply === true;
  const results = [];
  const missingKeys = new Set(preflight.missing.map((m) => m.compositeKey));

  for (const entry of manifest.entries) {
    const opp = preserved.byId.get(entry.opportunityId);
    if (!opp) {
      results.push({
        ...entry,
        ok: false,
        action: "FAILED",
        code: "SOURCE_ROW_MISSING",
      });
      continue;
    }

    let existing = null;
    try {
      existing = await findResearchInventoryByCompositeKey(entry.compositeKey);
    } catch (err) {
      results.push({
        opportunityId: entry.opportunityId,
        compositeKey: entry.compositeKey,
        ok: false,
        action: "FAILED",
        code: err.code || "LOOKUP_FAILED",
        detail: String(err?.message || err),
      });
      continue;
    }

    if (existing) {
      results.push({
        opportunityId: entry.opportunityId,
        compositeKey: entry.compositeKey,
        ok: true,
        action: "ALREADY_PRESENT",
        airtableRecordId: existing.id,
        updated: false,
        gdoWrite: false,
        publicationWrite: false,
      });
      continue;
    }

    if (!missingKeys.has(entry.compositeKey) && liveApply) {
      results.push({
        opportunityId: entry.opportunityId,
        compositeKey: entry.compositeKey,
        ok: false,
        action: "FAILED",
        code: "NOT_IN_PREFLIGHT_MISSING_SET",
      });
      continue;
    }

    const result = await upsertResearchInventoryRecord({
      opportunity: opp,
      hotel,
      dryRun: !liveApply,
    });

    // CREATE-only: reject unexpected updates
    if (
      liveApply &&
      result.ok &&
      result.action !== "CREATED" &&
      result.action !== "WOULD_CREATE"
    ) {
      results.push({
        opportunityId: entry.opportunityId,
        compositeKey: entry.compositeKey,
        ok: false,
        action: "FAILED",
        code: "UNEXPECTED_NON_CREATE_ACTION",
        detail: result.action,
        airtableRecordId: result.airtableRecordId || null,
      });
      continue;
    }

    const row = {
      opportunityId: entry.opportunityId,
      compositeKey: entry.compositeKey,
      hotelId: BETHESDA_HOTEL_ID,
      gdoWrite: false,
      publicationWrite: false,
      ...result,
    };

    if (liveApply && result.ok && result.airtableRecordId && result.action === "CREATED") {
      try {
        const rb = await readBackResearchInventory(result.airtableRecordId);
        const summary = rb.fields?.[F.sourceSummary];
        row.readBack = {
          ok:
            rb.compositeKey === entry.compositeKey &&
            rb.hotelId === BETHESDA_HOTEL_ID &&
            rb.opportunityId === entry.opportunityId &&
            Boolean(rb.fields?.researchPayloadJson) &&
            !(typeof summary === "string" && summary.includes("[object Object]")),
          airtableRecordId: rb.id,
          researchLifecycleStatus: rb.fields?.[F.researchLifecycleStatus] || null,
          priority: rb.fields?.[F.priority] || null,
          payloadPresent: Boolean(rb.fields?.researchPayloadJson),
          sourceSummary: typeof summary === "string" ? summary.slice(0, 240) : null,
        };
        if (!row.readBack.ok) row.ok = false;
      } catch (err) {
        row.readBack = { ok: false, error: String(err?.message || err) };
        row.ok = false;
      }
    }

    results.push(row);
    if (sleepMs > 0 && liveApply) {
      await new Promise((r) => setTimeout(r, sleepMs));
    }
  }

  const created = results.filter((r) => r.action === "CREATED").length;
  const wouldCreate = results.filter((r) => r.action === "WOULD_CREATE").length;
  const alreadyPresent = results.filter((r) => r.action === "ALREADY_PRESENT").length;
  const failed = results.filter((r) => !r.ok);

  let postBethesdaCount = null;
  try {
    const rows = await listResearchInventoryForHotel(BETHESDA_HOTEL_ID);
    postBethesdaCount = rows.length;
  } catch (err) {
    postBethesdaCount = { error: String(err?.message || err) };
  }

  return {
    ok: failed.length === 0,
    blocked: false,
    code: liveApply
      ? "APPLIED_BETHESDA_27_RESEARCH_CREATES"
      : "DRY_RUN_BETHESDA_27_RESEARCH_CREATES",
    authorizationId: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
    applied: liveApply,
    auth,
    preflight,
    results,
    counts: {
      created: liveApply ? created : 0,
      wouldCreate: liveApply ? 0 : wouldCreate,
      alreadyPresent,
      failed: failed.length,
      plannedMissing: preflight.proposedCreateCount,
    },
    postBethesdaCount,
    gdoWrites: 0,
    publicationWrites: 0,
    rockCreekApplied: false,
    autoPersistenceEnabled: false,
  };
}

/**
 * Merge the authorized +27 into canonical opportunities.json without mutating
 * the original 59 shared rows (title/org drifts retained as informational).
 */
export function alignCanonicalBethesdaOpportunitiesTo86(repoRoot, { write = false } = {}) {
  const paths = resolveBethesdaPaths(repoRoot);
  const canonicalRaw = JSON.parse(fs.readFileSync(paths.canonical, "utf8"));
  const canonical = loadOpportunityFile(paths.canonical);
  const preserved = loadOpportunityFile(paths.preserved);
  const allow = new Set(BETHESDA_27_AUTHORIZED_CREATE_OPPORTUNITY_IDS);

  const originalRows = Array.isArray(canonicalRaw.opportunities)
    ? canonicalRaw.opportunities
    : [];
  const originalIds = new Set(
    originalRows.map((o) => text(o.id || o.opportunityId)).filter(Boolean),
  );

  const additive = [];
  for (const id of BETHESDA_27_AUTHORIZED_CREATE_OPPORTUNITY_IDS) {
    if (originalIds.has(id)) continue;
    const row = preserved.byId.get(id);
    if (!row) {
      throw new Error(`align_missing_preserved_row:${id}`);
    }
    additive.push(row);
  }

  const nextOpps = [...originalRows, ...additive];
  const nextIds = new Set(
    nextOpps.map((o) => text(o.id || o.opportunityId)).filter(Boolean),
  );

  const result = {
    ok:
      nextOpps.length === 86 &&
      nextIds.size === 86 &&
      [...allow].every((id) => nextIds.has(id)) &&
      [...originalIds].every((id) => nextIds.has(id)),
    beforeCount: canonical.count,
    afterCount: nextOpps.length,
    additiveCount: additive.length,
    originalPreservedCount: originalRows.length,
    path: paths.canonical,
    wrote: false,
  };

  if (!result.ok) {
    return result;
  }

  if (write) {
    const nextDoc = {
      ...canonicalRaw,
      hotelId: canonicalRaw.hotelId || BETHESDA_HOTEL_ID,
      opportunities: nextOpps,
      updatedAt: new Date().toISOString(),
      persistence: {
        ...(canonicalRaw.persistence || {}),
        bethesda27ResearchCreatesAlignment: {
          authorizationId: BETHESDA_27_EXACT_WRITE_SET_AUTHORIZATION,
          additiveCount: additive.length,
          alignedAt: new Date().toISOString(),
          note: "Additive merge of authorized 27; original 59 rows unchanged",
        },
      },
    };
    fs.writeFileSync(paths.canonical, `${JSON.stringify(nextDoc, null, 2)}\n`);
    result.wrote = true;
  }

  return result;
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
  BETHESDA_27_AUTHORIZED_CREATE_OPPORTUNITY_IDS,
  BETHESDA_SOURCE_OF_TRUTH,
  reconcileBethesdaResearchInventory,
  buildBethesda27ProductionManifest,
  buildRockCreekCorrectionManifest,
  validateBethesda27Authorization,
  preflightBethesda27ResearchCreates,
  executeBethesda27ResearchCreates,
  alignCanonicalBethesdaOpportunitiesTo86,
  resolveBethesdaPaths,
  scanLedgersForOpportunityHints,
};
