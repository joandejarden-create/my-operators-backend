/**
 * Full E2E → Hotel Research Inventory persistence bridge V1.
 *
 * Invoked from the canonical Full E2E entry after research completes.
 * Reuses gdi-hotel-research-inventory-store-v1. Never writes GDO or publication.
 *
 * Default: dry-run only. Production apply requires ALL of:
 *   - authorizeResearchPersistence === true
 *   - exactWriteSetAuthorization (non-empty founder token)
 *   - apply === true
 *   - process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES === "1"
 *
 * Research-run authorization alone never unlocks Airtable mutation.
 */

import fs from "node:fs";
import path from "node:path";
import {
  GDI_AIRTABLE_TABLES,
  AIRTABLE_PRODUCTION_WRITE_GUARD,
  WRITE_OUTCOME,
} from "./gdi-full-e2e-airtable-persistence-contract-v1.js";
import {
  upsertResearchInventoryRecord,
  readBackResearchInventory,
  buildResearchInventoryFields,
} from "./gdi-hotel-research-inventory-store-v1.js";
import { loadYotelForbiddenRecreateIds } from "./gdi-complete-research-persistence-v1.js";
import { YOTEL_LEGACY_DELETE_AUTH } from "./gdi-yotel-legacy-gdo-deletion-v1.js";

export const GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1 =
  "gdi-full-e2e-research-persistence-bridge-v1";

export const RESEARCH_PERSISTENCE_ACTIVATION = Object.freeze({
  disabledByDefault: true,
  envGate: "GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES",
  requiresExactWriteSetAuthorization: true,
  separateFromGdoWrites: true,
  separateFromPublication: true,
  separateFromPaidResearchAuth: true,
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function extractOpportunities(input = {}) {
  if (Array.isArray(input.opportunities) && input.opportunities.length) {
    return input.opportunities;
  }
  const chain = input.chain || {};
  const fromProcessed = [];
  for (const row of Array.isArray(chain.processed) ? chain.processed : []) {
    const opp = row?.opportunity || row?.opportunityRow || row;
    if (!opp || typeof opp !== "object") continue;
    const opportunityId = text(opp.opportunityId || opp.id || row.opportunityId);
    if (!opportunityId) continue;
    fromProcessed.push({
      ...opp,
      opportunityId,
      hotelId: text(opp.hotelId || input.hotelId),
      segment: opp.segment || opp.segmentId || row.segmentId || null,
      runId: text(opp.runId || input.runId || row.runId) || null,
    });
  }
  if (fromProcessed.length) return fromProcessed;

  const seed = Array.isArray(input.seedOpportunities)
    ? input.seedOpportunities
    : [];
  return seed
    .map((opp) => ({
      ...opp,
      opportunityId: text(opp?.opportunityId || opp?.id),
      hotelId: text(opp?.hotelId || input.hotelId),
    }))
    .filter((o) => o.opportunityId);
}

/**
 * Evaluate whether production research-inventory writes are unlocked.
 */
export function evaluateResearchPersistenceAuthorization(input = {}) {
  const authorizeResearchPersistence =
    input.authorizeResearchPersistence === true;
  const exactWriteSetAuthorization = text(input.exactWriteSetAuthorization);
  const apply = input.apply === true || input.liveWrite === true;
  const envUnlocked =
    String(process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES || "") === "1";
  const authorizePaidResearch = input.authorizePaidResearch === true;
  const authorizePublish = input.authorizePublish === true;
  const authorizeGdoPersistence = input.authorizeGdoPersistence === true;

  const blockers = [];
  if (!authorizeResearchPersistence) {
    blockers.push({ code: "AUTHORIZE_RESEARCH_PERSISTENCE_REQUIRED" });
  }
  if (!exactWriteSetAuthorization) {
    blockers.push({ code: "EXACT_WRITE_SET_AUTHORIZATION_REQUIRED" });
  }
  if (!envUnlocked) {
    blockers.push({
      code: "ENV_PRODUCTION_GATE_OFF",
      env: RESEARCH_PERSISTENCE_ACTIVATION.envGate,
    });
  }
  if (authorizePublish) {
    blockers.push({
      code: "PUBLICATION_FLAG_NOT_ACCEPTED_ON_RESEARCH_BRIDGE",
    });
  }
  if (authorizeGdoPersistence) {
    blockers.push({
      code: "GDO_FLAG_NOT_ACCEPTED_ON_RESEARCH_BRIDGE",
      note: "Use the separate GDO production-write guard.",
    });
  }

  return {
    applyRequested: apply,
    authorizeResearchPersistence,
    exactWriteSetAuthorization: exactWriteSetAuthorization || null,
    envUnlocked,
    authorizePaidResearch,
    productionUnlocked:
      apply &&
      authorizeResearchPersistence &&
      Boolean(exactWriteSetAuthorization) &&
      envUnlocked &&
      !authorizePublish &&
      !authorizeGdoPersistence,
    blockers,
    note:
      "Paid research authorization does not unlock Airtable research persistence.",
    activation: RESEARCH_PERSISTENCE_ACTIVATION,
  };
}

/**
 * Persist (or dry-run) research inventory rows for one Full E2E hotel run.
 */
export async function persistFullE2eResearchInventory(input = {}) {
  const hotelId = text(input.hotelId);
  const runId = text(input.runId) || null;
  const repoRoot = input.repoRoot || process.cwd();
  const auth = evaluateResearchPersistenceAuthorization(input);
  const dryRun = auth.productionUnlocked !== true;
  const destination = GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES;
  const budget = Math.max(0, Number(input.writeBudget || 0) || 0);
  const opportunities = extractOpportunities(input);

  const outcome = {
    version: GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1,
    at: nowIso(),
    hotelId,
    runId,
    destinationTable: destination.name,
    destinationTableId: destination.tableId,
    dryRun,
    authorization: auth,
    researchCompletedIndependently: input.researchCompleted === true,
    gdoWrites: 0,
    publicationWrites: 0,
    airtableProductionGuard: AIRTABLE_PRODUCTION_WRITE_GUARD,
    counts: {
      candidates: opportunities.length,
      WOULD_CREATE: 0,
      WOULD_UPDATE: 0,
      UNCHANGED: 0,
      CREATED: 0,
      UPDATED: 0,
      FAILED: 0,
      SKIPPED_FORBIDDEN: 0,
      SKIPPED_BUDGET: 0,
    },
    rows: [],
    readBack: [],
    ok: true,
  };

  if (!hotelId) {
    outcome.ok = false;
    outcome.code = "MISSING_HOTEL_ID";
    return outcome;
  }

  if (input.apply === true && auth.productionUnlocked !== true) {
    outcome.ok = true;
    outcome.blockedApply = true;
    outcome.code = "RESEARCH_PERSISTENCE_PRODUCTION_LOCKED";
    outcome.detail =
      "Apply requested but production gates are locked; continuing as dry-run.";
  }

  let forbidden = new Set();
  try {
    forbidden = loadYotelForbiddenRecreateIds(repoRoot);
  } catch {
    forbidden = new Set();
  }

  let writesUsed = 0;
  for (const opp of opportunities) {
    const opportunityId = text(opp.opportunityId || opp.id);
    if (!opportunityId) {
      outcome.counts.FAILED += 1;
      outcome.rows.push({
        action: "FAILED",
        code: "MISSING_OPPORTUNITY_ID",
        outcome: WRITE_OUTCOME.FAILED,
      });
      continue;
    }

    if (
      hotelId === YOTEL_LEGACY_DELETE_AUTH.hotelId &&
      forbidden.has(opportunityId)
    ) {
      outcome.counts.SKIPPED_FORBIDDEN += 1;
      outcome.rows.push({
        hotelId,
        opportunityId,
        action: "SKIPPED_FORBIDDEN_YOTEL_LEGACY",
        outcome: WRITE_OUTCOME.DEFERRED,
        gdoWrite: false,
      });
      continue;
    }

    if (budget > 0 && !dryRun && writesUsed >= budget) {
      outcome.counts.SKIPPED_BUDGET += 1;
      outcome.rows.push({
        hotelId,
        opportunityId,
        action: "SKIPPED_WRITE_BUDGET",
        outcome: WRITE_OUTCOME.DEFERRED,
      });
      continue;
    }

    const built = buildResearchInventoryFields(
      {
        ...opp,
        opportunityId,
        hotelId,
        firstDiscoveredRunId: opp.firstDiscoveredRunId || runId,
        lastResearchedAt: opp.lastResearchedAt || nowIso(),
      },
      { hotelId, hotelName: input.hotelName || null },
    );
    if (!built.ok) {
      outcome.counts.FAILED += 1;
      outcome.rows.push({
        hotelId,
        opportunityId,
        action: "FAILED",
        code: built.code,
        outcome: WRITE_OUTCOME.FAILED,
      });
      continue;
    }

    let result;
    try {
      result = await upsertResearchInventoryRecord({
        opportunity: {
          ...opp,
          opportunityId,
          hotelId,
          firstDiscoveredRunId: opp.firstDiscoveredRunId || runId,
          lastResearchedAt: opp.lastResearchedAt || nowIso(),
        },
        hotel: { hotelId, hotelName: input.hotelName || null },
        dryRun,
      });
    } catch (err) {
      outcome.counts.FAILED += 1;
      outcome.ok = false;
      outcome.rows.push({
        hotelId,
        opportunityId,
        action: "FAILED",
        code: "UPSERT_THREW",
        error: String(err?.message || err),
        outcome: WRITE_OUTCOME.FAILED,
        recoverable: true,
      });
      continue;
    }

    const action = result.action || "FAILED";
    if (outcome.counts[action] != null) outcome.counts[action] += 1;
    else if (action.startsWith("WOULD_") || action === "UNCHANGED") {
      /* counted above when key exists */
    } else {
      outcome.counts.FAILED += 1;
    }

    const row = {
      hotelId,
      opportunityId,
      compositeKey: result.compositeKey || built.compositeKey,
      action,
      airtableRecordId: result.airtableRecordId || null,
      materialChanged: result.materialDiff?.materialChanged === true,
      changedFields: result.materialDiff?.changedFields || [],
      outcome:
        action === "CREATED" || action === "WOULD_CREATE"
          ? WRITE_OUTCOME.CREATED
          : action === "UPDATED" || action === "WOULD_UPDATE"
            ? WRITE_OUTCOME.UPDATED
            : action === "UNCHANGED"
              ? WRITE_OUTCOME.UNCHANGED
              : WRITE_OUTCOME.FAILED,
      gdoWrite: false,
      publicationWrite: false,
    };
    outcome.rows.push(row);

    if (!dryRun && (action === "CREATED" || action === "UPDATED")) {
      writesUsed += 1;
      if (input.readBack === true && result.airtableRecordId) {
        try {
          const rb = await readBackResearchInventory(result.airtableRecordId);
          outcome.readBack.push({
            airtableRecordId: rb.id,
            compositeKey: rb.compositeKey,
            matched:
              rb.compositeKey === row.compositeKey &&
              rb.opportunityId === opportunityId &&
              rb.hotelId === hotelId,
          });
        } catch (err) {
          outcome.readBack.push({
            airtableRecordId: result.airtableRecordId,
            matched: false,
            error: String(err?.message || err),
          });
          outcome.ok = false;
        }
      }
    }
  }

  outcome.writesUsed = writesUsed;
  outcome.writeBudget = budget || null;
  return outcome;
}

/**
 * Optional durable ledger for a persistence attempt (reports/ or caller outDir).
 */
export function writeResearchPersistenceLedger(outDir, outcome) {
  if (!outDir) return null;
  fs.mkdirSync(outDir, { recursive: true });
  const p = path.join(outDir, "research-inventory-persistence-ledger.json");
  fs.writeFileSync(p, `${JSON.stringify(outcome, null, 2)}\n`);
  return p;
}

export default {
  GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1,
  RESEARCH_PERSISTENCE_ACTIVATION,
  evaluateResearchPersistenceAuthorization,
  persistFullE2eResearchInventory,
  writeResearchPersistenceLedger,
};
