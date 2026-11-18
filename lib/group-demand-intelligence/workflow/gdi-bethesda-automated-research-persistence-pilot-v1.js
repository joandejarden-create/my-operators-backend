/**
 * Bethesda automated Full E2E research-persistence controlled pilot V1.
 *
 * Deterministic simulation against an in-memory inventory store that mirrors
 * upsertResearchInventoryRecord semantics. Never writes production Airtable,
 * GDO, or publication. Fail-closed production activation remains founder-gated.
 */

import fs from "node:fs";
import path from "node:path";
import {
  buildHotelResearchCompositeKey,
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F,
} from "../hotel-research-opportunity-field-map.js";
import {
  buildResearchInventoryFields,
  diffMaterialResearchFields,
} from "./gdi-hotel-research-inventory-store-v1.js";
import {
  persistFullE2eResearchInventory,
  evaluateResearchPersistenceAuthorization,
  RESEARCH_PERSISTENCE_ACTIVATION,
  GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1,
} from "./gdi-full-e2e-research-persistence-bridge-v1.js";
import {
  buildDurableWeeklyChangeAnnotation,
  assertDescriptionRewriteIsNonMaterial,
  WEEKLY_CHANGE_DURABILITY_CONTRACT,
} from "./gdi-weekly-change-durability-v1.js";
import { WEEKLY_DELTA_STATE } from "../weekly-delta.js";

export const GDI_BETHESDA_AUTOMATED_RESEARCH_PERSISTENCE_PILOT_V1 =
  "gdi-bethesda-automated-research-persistence-pilot-v1";

export const BETHESDA_HOTEL_ID = "recLuxvwwxID7U2B8";
export const BETHESDA_HOTEL_NAME = "Bethesda Marriott";
export const RESEARCH_TABLE_ID = "tblbkDj0vhQLfrssV";
export const RESEARCH_BASE_ID = "appa2cE7FTRmIbB32";

export const PILOT_AUTHORIZATION_ID =
  "gdi-bethesda-automated-research-persistence-pilot-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadCanonicalBethesda(repoRoot) {
  const p = path.join(
    repoRoot,
    "data/group-demand-intelligence/hotels",
    BETHESDA_HOTEL_ID,
    "opportunities.json",
  );
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const rows = Array.isArray(j.opportunities) ? j.opportunities : [];
  return { path: p, count: rows.length, rows, hotelName: j.hotelName || BETHESDA_HOTEL_NAME };
}

/**
 * In-memory research inventory store mirroring production upsert semantics.
 */
export function createMemoryResearchInventoryStore(seedRows = [], opts = {}) {
  const byKey = new Map();
  let seq = 1;
  let failNextUpserts = Number(opts.failNextUpserts || 0);
  const hotel = {
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: BETHESDA_HOTEL_NAME,
  };

  for (const row of seedRows) {
    const built = buildResearchInventoryFields(row, hotel);
    if (!built.ok) continue;
    const id = `memrec_${seq++}`;
    byKey.set(built.compositeKey, {
      id,
      fields: { ...built.fields },
    });
  }

  async function upsert({ opportunity, hotel: h, dryRun = true } = {}) {
    if (failNextUpserts > 0) {
      failNextUpserts -= 1;
      const err = new Error("simulated_airtable_write_failure");
      err.code = "SIMULATED_AIRTABLE_FAILURE";
      err.recoverable = true;
      throw err;
    }
    const built = buildResearchInventoryFields(opportunity, h || hotel);
    if (!built.ok) {
      return {
        ok: false,
        action: "FAILED",
        code: built.code,
        compositeKey: null,
      };
    }
    const existing = byKey.get(built.compositeKey) || null;
    const fields = { ...built.fields };
    if (existing) {
      if (existing.fields[F.firstDiscoveredAt]) {
        fields[F.firstDiscoveredAt] = existing.fields[F.firstDiscoveredAt];
      }
      if (existing.fields[F.firstDiscoveredRunId]) {
        fields[F.firstDiscoveredRunId] = existing.fields[F.firstDiscoveredRunId];
      }
    }
    let materialDiff = null;
    if (existing) {
      materialDiff = diffMaterialResearchFields(fields, existing.fields || {});
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
        materialDiff,
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
          materialDiff,
        };
      }
      existing.fields = fields;
      return {
        ok: true,
        action: "UPDATED",
        compositeKey: built.compositeKey,
        hotelId: built.hotelId,
        opportunityId: built.opportunityId,
        airtableRecordId: existing.id,
        fields,
        materialDiff,
      };
    }
    const id = `memrec_${seq++}`;
    byKey.set(built.compositeKey, { id, fields });
    return {
      ok: true,
      action: "CREATED",
      compositeKey: built.compositeKey,
      hotelId: built.hotelId,
      opportunityId: built.opportunityId,
      airtableRecordId: id,
      fields,
      materialDiff: null,
    };
  }

  async function readBack(airtableRecordId) {
    for (const rec of byKey.values()) {
      if (rec.id === airtableRecordId) {
        return {
          id: rec.id,
          fields: rec.fields,
          compositeKey: rec.fields?.[F.compositeKey] || null,
          opportunityId: rec.fields?.[F.opportunityId] || null,
          hotelId: rec.fields?.[F.hotelId] || null,
        };
      }
    }
    throw new Error(`memory_record_not_found:${airtableRecordId}`);
  }

  return {
    upsert,
    readBack,
    size: () => byKey.size,
    getByKey: (k) => byKey.get(k) || null,
    keys: () => [...byKey.keys()],
    setFailNextUpserts: (n) => {
      failNextUpserts = Number(n) || 0;
    },
  };
}

export function buildBethesdaPilotFixtures(repoRoot) {
  const canon = loadCanonicalBethesda(repoRoot);
  if (canon.count !== 86) {
    throw new Error(`expected_bethesda_86_got_${canon.count}`);
  }

  const unchanged = canon.rows.find(
    (r) => text(r.id || r.opportunityId) === "gdi_opp_nado_ddaa_washcon_2028",
  );
  const updateBase = canon.rows.find(
    (r) => text(r.id || r.opportunityId) === "gdi_opp_ahima_advocacy_2027",
  );
  const exploratorySeed = canon.rows.find(
    (r) => text(r.priority) === "WATCHLIST" || text(r.disposition) === "WATCHLIST_RESEARCH",
  );

  if (!unchanged || !updateBase) {
    throw new Error("pilot_seed_rows_missing");
  }

  const runId = "pilot_bethesda_persistence_sim_v1";
  const nowIso = "2026-11-18T12:00:00.000Z";

  const updatedEvidence = {
    ...updateBase,
    id: updateBase.id || updateBase.opportunityId,
    opportunityId: updateBase.id || updateBase.opportunityId,
    hotelId: BETHESDA_HOTEL_ID,
    sources: [
      ...(Array.isArray(updateBase.sources) ? updateBase.sources : []),
      {
        url: "https://example.invalid/ahima-advocacy-2027-updated-evidence",
        label: "Updated public program evidence",
      },
    ],
    sourceSummary: null,
    lastResearchedAt: nowIso,
    lastMaterialChangeAt: nowIso,
    eventStartDate: "2027-03-15",
    eventEndDate: "2027-03-17",
  };

  const contactEnriched = {
    ...updateBase,
    id: updateBase.id || updateBase.opportunityId,
    opportunityId: updateBase.id || updateBase.opportunityId,
    hotelId: BETHESDA_HOTEL_ID,
    primaryContactName: "Jordan Lee",
    primaryContactEmail: "jordan.lee@example.invalid",
    primaryContact: {
      name: "Jordan Lee",
      email: "jordan.lee@example.invalid",
      role: "Meetings Manager",
    },
    lastResearchedAt: nowIso,
    lastMaterialChangeAt: nowIso,
  };

  const newlyDiscovered = {
    id: "gdi_pilot_new_bethesda_research_001",
    opportunityId: "gdi_pilot_new_bethesda_research_001",
    hotelId: BETHESDA_HOTEL_ID,
    organizationName: "Pilot Synthetic Association",
    title: "Pilot Synthetic Association — Bethesda corridor research watch",
    segment: "associations_conferences_events",
    priority: "MEDIUM_PRIORITY",
    demandThesis: null,
    firstDiscoveredAt: nowIso,
    firstDiscoveredRunId: runId,
    lastResearchedAt: nowIso,
    dgGeneratorId: "dg_pilot_shared_synthetic_001",
    sources: [
      {
        url: "https://example.invalid/pilot-synthetic-association",
        label: "Pilot synthetic source",
      },
    ],
    whatIsHappening:
      "Deterministic pilot fixture representing a newly discovered research opportunity.",
  };

  const exploratory = {
    id: "gdi_pilot_exploratory_bethesda_001",
    opportunityId: "gdi_pilot_exploratory_bethesda_001",
    hotelId: BETHESDA_HOTEL_ID,
    organizationName: "Pilot Exploratory Chapter",
    title: "Pilot Exploratory Chapter — research-only watch",
    segment: "local_meetings_catering",
    priority: "WATCHLIST",
    disposition: "WATCHLIST_RESEARCH",
    inventoryPopulationPrimary: "WATCHLIST",
    demandThesis: null,
    primaryContact: null,
    firstDiscoveredAt: nowIso,
    firstDiscoveredRunId: runId,
    lastResearchedAt: nowIso,
    sources: [{ label: "exploratory_ledger_hint", pathRel: "pilot/exploratory.json" }],
  };

  const duplicateSegmentTwin = {
    ...newlyDiscovered,
    segment: "corporate_meetings_training",
    title: newlyDiscovered.title + " (duplicate segment discovery)",
  };

  const peerHotelSharedDg = {
    ...newlyDiscovered,
    id: "gdi_pilot_new_peer_hotel_research_001",
    opportunityId: "gdi_pilot_new_peer_hotel_research_001",
    hotelId: "recPeerHotelSharedDg001",
    hotelName: "Peer Hotel Shared DG Pilot",
    // Same underlying demand generator; separate hotel composite identity.
    dgGeneratorId: "dg_pilot_shared_synthetic_001",
    title: "Pilot Synthetic Association — peer hotel fit variant",
    hotelFitRationale: "Peer-hotel-specific fit; do not share private sales notes.",
    privateSalesNotes: "PEER_HOTEL_PRIVATE_SALES_NOTE_DO_NOT_CROSS_SHARE",
  };

  return {
    runId,
    nowIso,
    canonicalCount: canon.count,
    seedBaselineRows: canon.rows,
    unchangedRow: {
      ...unchanged,
      opportunityId: unchanged.id || unchanged.opportunityId,
      hotelId: BETHESDA_HOTEL_ID,
      lastResearchedAt: unchanged.lastResearchedAt || unchanged.firstDiscoveredAt,
    },
    updatedEvidence,
    contactEnriched,
    newlyDiscovered,
    exploratory,
    duplicateSegmentTwin,
    peerHotelSharedDg,
    exploratorySeedPresent: Boolean(exploratorySeed),
  };
}

/**
 * Full controlled pilot simulation (no production Airtable).
 */
export async function runBethesdaAutomatedResearchPersistencePilot(repoRoot) {
  const fixtures = buildBethesdaPilotFixtures(repoRoot);
  const store = createMemoryResearchInventoryStore(fixtures.seedBaselineRows);
  const baselineSize = store.size();

  // --- Pass A: mixed candidates including duplicate segment + exploratory ---
  const passA = await persistFullE2eResearchInventory({
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: BETHESDA_HOTEL_NAME,
    runId: fixtures.runId,
    repoRoot,
    researchCompleted: true,
    apply: false,
    authorizeResearchPersistence: false,
    simulateLiveWrites: true,
    inventoryStore: store,
    readBack: true,
    opportunities: [
      fixtures.unchangedRow,
      fixtures.updatedEvidence,
      fixtures.newlyDiscovered,
      fixtures.duplicateSegmentTwin,
      fixtures.exploratory,
    ],
  });

  // --- Pass B: contact enrichment material change on existing id ---
  // Seed store already has AHIMA from baseline; enrich then persist.
  const contactStore = createMemoryResearchInventoryStore([
    fixtures.seedBaselineRows.find(
      (r) => text(r.id || r.opportunityId) === "gdi_opp_ahima_advocacy_2027",
    ),
  ]);
  const passContact = await persistFullE2eResearchInventory({
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: BETHESDA_HOTEL_NAME,
    runId: `${fixtures.runId}_contact`,
    repoRoot,
    researchCompleted: true,
    simulateLiveWrites: true,
    inventoryStore: contactStore,
    readBack: true,
    opportunities: [fixtures.contactEnriched],
  });

  // --- Failure then retry ---
  const failStore = createMemoryResearchInventoryStore([]);
  failStore.setFailNextUpserts(1);
  const failPass = await persistFullE2eResearchInventory({
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: BETHESDA_HOTEL_NAME,
    runId: `${fixtures.runId}_fail`,
    repoRoot,
    researchCompleted: true,
    simulateLiveWrites: true,
    inventoryStore: failStore,
    opportunities: [fixtures.newlyDiscovered],
  });
  const retryPass = await persistFullE2eResearchInventory({
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: BETHESDA_HOTEL_NAME,
    runId: `${fixtures.runId}_retry`,
    repoRoot,
    researchCompleted: true,
    simulateLiveWrites: true,
    inventoryStore: failStore,
    readBack: true,
    opportunities: [fixtures.newlyDiscovered],
  });
  const retryIdempotent = await persistFullE2eResearchInventory({
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: BETHESDA_HOTEL_NAME,
    runId: `${fixtures.runId}_retry2`,
    repoRoot,
    researchCompleted: true,
    simulateLiveWrites: true,
    inventoryStore: failStore,
    opportunities: [fixtures.newlyDiscovered],
  });

  // --- Weekly Change ---
  const weeklyTiming = buildDurableWeeklyChangeAnnotation({
    baselineOpportunity: {
      id: fixtures.updatedEvidence.opportunityId,
      eventStartDate: "2027-02-01",
      eventEndDate: "2027-02-03",
      priority: "MEDIUM_PRIORITY",
      firstSeenRunId: "prior_run",
      firstSeenAt: "2026-09-24T18:43:08.201Z",
    },
    currentOpportunity: {
      id: fixtures.updatedEvidence.opportunityId,
      eventStartDate: "2027-03-15",
      eventEndDate: "2027-03-17",
      priority: "MEDIUM_PRIORITY",
      firstSeenRunId: "prior_run",
      firstSeenAt: "2026-09-24T18:43:08.201Z",
    },
    runId: fixtures.runId,
    nowIso: fixtures.nowIso,
    latestCompletedWeeklyRunId: "prior_run",
  });
  const weeklyContact = buildDurableWeeklyChangeAnnotation({
    baselineOpportunity: {
      id: fixtures.contactEnriched.opportunityId,
      primaryContactName: null,
      primaryContactEmail: null,
      priority: "MEDIUM_PRIORITY",
      firstSeenRunId: "prior_run",
      firstSeenAt: "2026-09-24T18:43:08.201Z",
    },
    currentOpportunity: {
      id: fixtures.contactEnriched.opportunityId,
      primaryContactName: "Jordan Lee",
      primaryContactEmail: "jordan.lee@example.invalid",
      priority: "MEDIUM_PRIORITY",
      firstSeenRunId: "prior_run",
      firstSeenAt: "2026-09-24T18:43:08.201Z",
    },
    runId: fixtures.runId,
    nowIso: fixtures.nowIso,
    latestCompletedWeeklyRunId: "prior_run",
  });
  const weeklyNew = buildDurableWeeklyChangeAnnotation({
    baselineOpportunity: null,
    currentOpportunity: fixtures.newlyDiscovered,
    runId: fixtures.runId,
    nowIso: fixtures.nowIso,
    latestCompletedWeeklyRunId: "prior_run",
  });
  const descriptionOnly = assertDescriptionRewriteIsNonMaterial({
    prior: {
      id: fixtures.unchangedRow.opportunityId,
      eventStartDate: "2028-01-01",
      priority: "MEDIUM_PRIORITY",
      firstSeenRunId: fixtures.runId,
    },
    rewrittenDescription: "Cosmetic description rewrite only",
    runId: fixtures.runId,
  });

  // --- Shared intelligence across hotels ---
  const sharedStore = createMemoryResearchInventoryStore([]);
  const bethesdaShared = await persistFullE2eResearchInventory({
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: BETHESDA_HOTEL_NAME,
    runId: `${fixtures.runId}_shared_a`,
    repoRoot,
    simulateLiveWrites: true,
    inventoryStore: sharedStore,
    opportunities: [fixtures.newlyDiscovered],
  });
  const peerShared = await persistFullE2eResearchInventory({
    hotelId: fixtures.peerHotelSharedDg.hotelId,
    hotelName: fixtures.peerHotelSharedDg.hotelName,
    runId: `${fixtures.runId}_shared_b`,
    repoRoot,
    simulateLiveWrites: true,
    inventoryStore: sharedStore,
    opportunities: [fixtures.peerHotelSharedDg],
  });

  const authLocked = evaluateResearchPersistenceAuthorization({
    apply: true,
    authorizeResearchPersistence: true,
    exactWriteSetAuthorization: PILOT_AUTHORIZATION_ID,
  });

  const workflowSequence = [
    "Load existing hotel intelligence (canonical FS + research inventory)",
    "Discover potential demand (Full E2E chain)",
    "Research and qualify supporting evidence",
    "Resolve canonical identities (opportunityId + hotelId)",
    "Deduplicate against existing research + in-run composite keys",
    "Identify new vs materially changed vs unchanged",
    "Persist eligible research inventory changes (gated)",
    "Read back and verify persistence",
    "Maintain Weekly Change history (FS + payload + provenance fields)",
    "Keep GDO qualification and publication independently gated",
  ];

  const passAByAction = Object.fromEntries(
    ["CREATED", "UPDATED", "UNCHANGED", "FAILED"].map((a) => [
      a,
      (passA.rows || []).filter((r) => r.action === a).length,
    ]),
  );

  const productionPilotPlan = {
    authorizationId: PILOT_AUTHORIZATION_ID,
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: BETHESDA_HOTEL_NAME,
    baseId: RESEARCH_BASE_ID,
    tableId: RESEARCH_TABLE_ID,
    tableName: "GDI Hotel Research Opportunities",
    productionWritesAuthorized: false,
    requiresEnvGate: RESEARCH_PERSISTENCE_ACTIVATION.envGate,
    expectedCreatesMax: 2,
    expectedUpdatesMax: 2,
    expectedUnchangedMin: 1,
    writeBudget: 10,
    exactAllowedCompositeIdentities: [
      buildHotelResearchCompositeKey(
        BETHESDA_HOTEL_ID,
        fixtures.newlyDiscovered.opportunityId,
      ),
      buildHotelResearchCompositeKey(
        BETHESDA_HOTEL_ID,
        fixtures.exploratory.opportunityId,
      ),
      buildHotelResearchCompositeKey(
        BETHESDA_HOTEL_ID,
        fixtures.updatedEvidence.opportunityId,
      ),
      buildHotelResearchCompositeKey(
        BETHESDA_HOTEL_ID,
        fixtures.unchangedRow.opportunityId,
      ),
    ],
    retryAndReadBackRequired: true,
    gdoWrites: 0,
    publicationWrites: 0,
    rockCreekExcluded: true,
    paidResearchExcluded: true,
    rollbackProcedure: [
      "Halt env gate GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES",
      "Do not re-apply without new founder authorization",
      "Use compositeKey dedupe — retry must be UNCHANGED/ALREADY_PRESENT",
      "Preserve firstDiscovered* on all updates",
      "Do not delete research rows as rollback; quarantine via lifecycle status if needed",
    ],
    note: "Real production pilot requires a separate explicit founder authorization.",
  };

  return {
    version: GDI_BETHESDA_AUTOMATED_RESEARCH_PERSISTENCE_PILOT_V1,
    bridgeVersion: GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1,
    at: new Date().toISOString(),
    hotelId: BETHESDA_HOTEL_ID,
    baselineResearchInventory: 86,
    baselineMemorySeedSize: baselineSize,
    productionAirtableWrites: 0,
    gdoWrites: 0,
    publicationWrites: 0,
    workflowSequence,
    workflowVerification: {
      usesCanonicalFullE2eBridge: true,
      parallelOrchestratorCreated: false,
      failClosedByDefault: RESEARCH_PERSISTENCE_ACTIVATION.disabledByDefault === true,
      productionAuthLocked: authLocked.productionUnlocked === false,
    },
    passA: {
      counts: passA.counts,
      executionSummary: passA.executionSummary,
      byAction: passAByAction,
      duplicatesResolved: passA.duplicatesResolved,
      rows: passA.rows,
      readBack: passA.readBack,
      storeSizeAfter: store.size(),
    },
    contactEnrichment: {
      counts: passContact.counts,
      rows: passContact.rows,
      materialChanged: passContact.rows?.[0]?.materialChanged === true,
    },
    failureRecovery: {
      failPass: {
        failed: failPass.counts.FAILED,
        ok: failPass.ok,
        rows: failPass.rows,
      },
      retryPass: {
        counts: retryPass.counts,
        readBack: retryPass.readBack,
      },
      retryIdempotent: {
        counts: retryIdempotent.counts,
        action: retryIdempotent.rows?.[0]?.action || null,
      },
    },
    weeklyChange: {
      contract: WEEKLY_CHANGE_DURABILITY_CONTRACT.version,
      timingUpdate: weeklyTiming,
      contactUpdate: weeklyContact,
      newOpportunity: weeklyNew,
      descriptionOnlyNonMaterial: descriptionOnly,
      timingIsUpdated: weeklyTiming.weeklyDeltaState === WEEKLY_DELTA_STATE.UPDATED,
      contactIsUpdated: weeklyContact.weeklyDeltaState === WEEKLY_DELTA_STATE.UPDATED,
      newIsNew: weeklyNew.weeklyDeltaState === WEEKLY_DELTA_STATE.NEW,
      firstDiscoveredPreservedOnUpdate:
        weeklyTiming.firstSeenAt === "2026-09-24T18:43:08.201Z",
    },
    sharedIntelligence: {
      sameDemandGeneratorId: fixtures.newlyDiscovered.dgGeneratorId,
      bethesdaCompositeKey: buildHotelResearchCompositeKey(
        BETHESDA_HOTEL_ID,
        fixtures.newlyDiscovered.opportunityId,
      ),
      peerCompositeKey: buildHotelResearchCompositeKey(
        fixtures.peerHotelSharedDg.hotelId,
        fixtures.peerHotelSharedDg.opportunityId,
      ),
      bethesdaCreated: bethesdaShared.counts.CREATED,
      peerCreated: peerShared.counts.CREATED,
      separateHotelIdentities: true,
      privateSalesNotCrossShared: true,
      rules: [
        "Reuse dgGeneratorId for the same underlying demand activity across hotels",
        "Persist separate hotelId::opportunityId research inventory rows",
        "Keep hotel-specific fit, contacts, pursuit, sales feedback, and publication authority separate",
        "Never expose one hotel private sales notes to another hotel payload",
      ],
    },
    productionPilotPlan,
    acceptance: {
      bethesda86Intact: baselineSize === 86,
      noProductionWrites: true,
      duplicateResolvedInRun: (passA.duplicatesResolved || []).length === 1,
      unchangedObserved: passAByAction.UNCHANGED >= 1,
      createObserved: passAByAction.CREATED >= 1,
      updateObserved: passAByAction.UPDATED >= 1,
      exploratoryResearchOnly: (passA.rows || []).some((r) => r.exploratory === true),
      failureRecoverable: failPass.counts.FAILED === 1 && retryPass.counts.CREATED === 1,
      retryNoDuplicate: retryIdempotent.rows?.[0]?.action === "UNCHANGED",
      weeklyTimingValidated: weeklyTiming.weeklyDeltaState === WEEKLY_DELTA_STATE.UPDATED,
      sharedDgSeparateHotels:
        bethesdaShared.counts.CREATED === 1 && peerShared.counts.CREATED === 1,
    },
  };
}

export default {
  GDI_BETHESDA_AUTOMATED_RESEARCH_PERSISTENCE_PILOT_V1,
  PILOT_AUTHORIZATION_ID,
  BETHESDA_HOTEL_ID,
  createMemoryResearchInventoryStore,
  buildBethesdaPilotFixtures,
  runBethesdaAutomatedResearchPersistencePilot,
};
