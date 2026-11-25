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
import {
  collectEvidenceUrls,
  collectNamedContacts,
  assertSlimPayloadRoundTrip,
  buildSlimResearchPayload,
} from "./gdi-research-inventory-slim-payload-v1.js";
import { ensureDemandGeneratorIdOnOpportunity } from "./gdi-slice2b-relationship-wiring-v1.js";
import { evaluateFinalizedQualifiedOpportunity } from "./gdi-finalized-qualified-opportunity-contract-v1.js";

export const GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1 =
  "gdi-full-e2e-research-persistence-bridge-v1";

/**
 * Permanent founder policy for Full E2E research → Airtable persistence:
 * Discover broadly → research thoroughly → qualify rigorously →
 * persist validated opportunities only. Nonqualified research is preserved
 * as supporting intelligence, never as active hotel-opportunity writes.
 */
export const RESEARCH_PERSISTENCE_WRITE_SET_POLICY = Object.freeze({
  VALIDATED_FQO_ONLY: "VALIDATED_FQO_ONLY",
  RESEARCH_UNIVERSE_AUDIT: "RESEARCH_UNIVERSE_AUDIT",
});

/** Founder-facing operation classes for write manifests / Weekly Change plans. */
export const RESEARCH_PERSISTENCE_OPERATION_CLASS = Object.freeze({
  CREATE_NEW: "CREATE_NEW",
  UPDATE_MATERIAL: "UPDATE_MATERIAL",
  UNCHANGED: "UNCHANGED",
  DUPLICATE: "DUPLICATE",
  DUPLICATE_OR_MERGED: "DUPLICATE_OR_MERGED",
  RESEARCH_ONLY: "RESEARCH_ONLY",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  DISQUALIFIED_OR_HISTORICAL: "DISQUALIFIED_OR_HISTORICAL",
  GDO_QUALIFIED_PENDING_AUTHORIZATION: "GDO_QUALIFIED_PENDING_AUTHORIZATION",
  EXCLUDED: "EXCLUDED",
});

export const RESEARCH_PERSISTENCE_ACTIVATION = Object.freeze({
  disabledByDefault: true,
  envGate: "GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES",
  requiresExactWriteSetAuthorization: true,
  separateFromGdoWrites: true,
  separateFromPublication: true,
  separateFromPaidResearchAuth: true,
  defaultProductionWriteSetPolicy:
    RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
});

/**
 * Filter candidates to FQO-validated hotel opportunities only.
 * Historical schema-compat may assist evidence/pathway checks, but
 * finalizedQualified must still be true — historical alone cannot force inclusion.
 */
export function filterValidatedFqoPersistenceWriteSet(opportunities = [], opts = {}) {
  const allowHistoricalCompat = opts.allowHistoricalSchemaCompat !== false;
  const allowedIds = opts.allowedOpportunityIds
    ? new Set([...opts.allowedOpportunityIds].map((id) => text(id)).filter(Boolean))
    : null;
  const kept = [];
  const deferred = [];
  for (const opp of Array.isArray(opportunities) ? opportunities : []) {
    const opportunityId = text(opp?.opportunityId || opp?.id);
    if (allowedIds && opportunityId && !allowedIds.has(opportunityId)) {
      deferred.push({
        opportunityId,
        reason: "OUTSIDE_CERTIFIED_VALIDATED_SET",
        action: "PRESERVE_NOT_IN_WRITE_SET",
        founderOperationClass:
          RESEARCH_PERSISTENCE_OPERATION_CLASS.RESEARCH_ONLY,
      });
      continue;
    }
    const fqo = evaluateFinalizedQualifiedOpportunity(opp, {
      historicalApprovedInventory: allowHistoricalCompat,
    });
    // Genuine qualification required — historical compat cannot invent a pass.
    if (fqo.finalizedQualified !== true) {
      deferred.push({
        opportunityId,
        reason: "FQO_GATE_FAILED",
        fqoStatus: fqo.status || null,
        fqoReasons: fqo.reasons || [],
        action: "PRESERVE_NONQUALIFIED_RESEARCH",
        founderOperationClass:
          RESEARCH_PERSISTENCE_OPERATION_CLASS.RESEARCH_CONTINUATION,
      });
      continue;
    }
    kept.push({
      ...opp,
      opportunityId,
      finalizedQualified: true,
      fqoEvaluation: fqo,
      hotelSpecificQualification: {
        ...(opp.hotelSpecificQualification || {}),
        finalizedQualified: true,
        fqoStatus: fqo.status,
        fqoReasons: fqo.reasons,
      },
    });
  }
  return {
    policy: RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
    kept,
    deferred,
    keptCount: kept.length,
    deferredCount: deferred.length,
  };
}

/**
 * Explicit stage statuses for canonical Full E2E certificates.
 * Research persistence is independent of STAGED/CURRENT publication.
 */
export const RESEARCH_PERSISTENCE_STAGE_STATUS = Object.freeze({
  NOT_REQUESTED: "NOT_REQUESTED",
  DRY_RUN: "DRY_RUN",
  BLOCKED_AUTHORIZATION: "BLOCKED_AUTHORIZATION",
  BLOCKED_SCHEMA: "BLOCKED_SCHEMA",
  WRITE_IN_PROGRESS: "WRITE_IN_PROGRESS",
  PARTIAL_FAILURE: "PARTIAL_FAILURE",
  PERSISTED_AND_VERIFIED: "PERSISTED_AND_VERIFIED",
  FAILED: "FAILED",
});

/**
 * Map store/bridge actions into founder operation classes.
 */
export function classifyResearchPersistenceOperation(row = {}) {
  const action = text(row.action || row.outcome).toUpperCase();
  const disposition = text(
    row.disposition ||
      row.exclusionReason ||
      row.researchDisposition ||
      row.manifestDisposition ||
      "",
  ).toUpperCase();
  const eligibility = text(
    row.commercialEligibilityStatus ||
      row.eligibility ||
      row.currentEligibility ||
      "",
  ).toUpperCase();
  const lifecycle = text(row.researchLifecycleStatus || "").toUpperCase();

  if (
    /DISQUALIFIED|HISTORICAL|ARCHIVED/.test(disposition) ||
    lifecycle === "ARCHIVED_RESEARCH" ||
    row.historicalHold === true
  ) {
    return RESEARCH_PERSISTENCE_OPERATION_CLASS.DISQUALIFIED_OR_HISTORICAL;
  }
  if (
    disposition === "EXCLUDE" ||
    eligibility === "EXCLUDED" ||
    row.excluded === true
  ) {
    return RESEARCH_PERSISTENCE_OPERATION_CLASS.EXCLUDED;
  }
  if (
    action.includes("DUPLICATE") ||
    row.reason === "DUPLICATE_COMPOSITE_KEY_IN_RUN" ||
    /DUPLICATE|CANONICAL_MERGE|MERGED/.test(disposition)
  ) {
    return RESEARCH_PERSISTENCE_OPERATION_CLASS.DUPLICATE_OR_MERGED;
  }
  if (
    /CONTACT_RESEARCH|RESEARCH_INCOMPLETE|CONTINUATION|HOLD/.test(disposition) ||
    lifecycle === "CONTINUATION" ||
    row.researchContinuation === true
  ) {
    return RESEARCH_PERSISTENCE_OPERATION_CLASS.RESEARCH_CONTINUATION;
  }
  if (
    eligibility === "CUSTOMER_ELIGIBLE_CANDIDATE" ||
    eligibility === "QUALIFIED_GDO_PRESENT" ||
    row.gdoQualifiedPendingAuthorization === true
  ) {
    return RESEARCH_PERSISTENCE_OPERATION_CLASS.GDO_QUALIFIED_PENDING_AUTHORIZATION;
  }
  if (action === "CREATED" || action === "WOULD_CREATE") {
    return RESEARCH_PERSISTENCE_OPERATION_CLASS.CREATE_NEW;
  }
  if (
    action === "UPDATED" ||
    action === "WOULD_UPDATE" ||
    row.materialChanged === true
  ) {
    return RESEARCH_PERSISTENCE_OPERATION_CLASS.UPDATE_MATERIAL;
  }
  if (action === "UNCHANGED") {
    return RESEARCH_PERSISTENCE_OPERATION_CLASS.UNCHANGED;
  }
  if (
    lifecycle === "WATCHLIST" ||
    lifecycle === "EXPLORATORY" ||
    eligibility === "RESEARCH_ONLY" ||
    eligibility === "GLOBAL_ORG_ONLY"
  ) {
    return RESEARCH_PERSISTENCE_OPERATION_CLASS.RESEARCH_ONLY;
  }
  return RESEARCH_PERSISTENCE_OPERATION_CLASS.RESEARCH_ONLY;
}

/**
 * Derive certificate-facing stage status from a persistence outcome.
 * Never claims PERSISTED_AND_VERIFIED for dry-run / skipped / blocked runs.
 */
export function classifyResearchPersistenceStageStatus(outcome = null, opts = {}) {
  if (opts.requested !== true && !outcome) {
    return RESEARCH_PERSISTENCE_STAGE_STATUS.NOT_REQUESTED;
  }
  if (!outcome || typeof outcome !== "object") {
    return RESEARCH_PERSISTENCE_STAGE_STATUS.FAILED;
  }
  if (outcome.code === "BLOCKED_SCHEMA" || outcome.schemaBlocked === true) {
    return RESEARCH_PERSISTENCE_STAGE_STATUS.BLOCKED_SCHEMA;
  }
  // Isolated/injected simulation stores may mutate in-memory while production
  // Airtable gates remain locked. Prefer actual dryRun/write outcomes first.
  const simulatedLive =
    opts.simulationStore === true || outcome.simulationStore === true;
  if (
    !simulatedLive &&
    outcome.dryRun !== false &&
    (outcome.blockedApply === true ||
      outcome.code === "RESEARCH_PERSISTENCE_PRODUCTION_LOCKED" ||
      (Array.isArray(outcome.authorization?.blockers) &&
        outcome.authorization.blockers.length > 0 &&
        (opts.applyRequested === true || outcome.applyRequested === true)))
  ) {
    if (Number(outcome.counts?.FAILED || 0) > 0 && outcome.ok === false) {
      return RESEARCH_PERSISTENCE_STAGE_STATUS.PARTIAL_FAILURE;
    }
    if (Array.isArray(outcome.rows)) {
      return RESEARCH_PERSISTENCE_STAGE_STATUS.DRY_RUN;
    }
    return RESEARCH_PERSISTENCE_STAGE_STATUS.BLOCKED_AUTHORIZATION;
  }
  if (outcome.writeInProgress === true) {
    return RESEARCH_PERSISTENCE_STAGE_STATUS.WRITE_IN_PROGRESS;
  }
  const failed = Number(outcome.counts?.FAILED || 0);
  const wrote =
    Number(outcome.counts?.CREATED || 0) + Number(outcome.counts?.UPDATED || 0);
  if (outcome.dryRun === false) {
    if (failed > 0 && wrote > 0) {
      return RESEARCH_PERSISTENCE_STAGE_STATUS.PARTIAL_FAILURE;
    }
    if (failed > 0 && wrote === 0) {
      return RESEARCH_PERSISTENCE_STAGE_STATUS.FAILED;
    }
    const verified = Array.isArray(outcome.readBack)
      ? outcome.readBack.filter((r) => r.matched === true).length
      : 0;
    if (
      outcome.ok !== false &&
      wrote >= 0 &&
      (opts.requireReadBack !== true ||
        verified === wrote ||
        (wrote === 0 && failed === 0))
    ) {
      if (opts.requireReadBack === true && wrote > 0 && verified < wrote) {
        return RESEARCH_PERSISTENCE_STAGE_STATUS.PARTIAL_FAILURE;
      }
      return RESEARCH_PERSISTENCE_STAGE_STATUS.PERSISTED_AND_VERIFIED;
    }
    return RESEARCH_PERSISTENCE_STAGE_STATUS.FAILED;
  }
  if (failed > 0 && outcome.ok === false) {
    return RESEARCH_PERSISTENCE_STAGE_STATUS.PARTIAL_FAILURE;
  }
  return RESEARCH_PERSISTENCE_STAGE_STATUS.DRY_RUN;
}

/**
 * True only when live authorized writes succeeded and (when required) read-back matched.
 */
export function isResearchPersistenceCertified(outcome = null, opts = {}) {
  return (
    classifyResearchPersistenceStageStatus(outcome, {
      ...opts,
      requested: true,
      requireReadBack: opts.requireReadBack !== false,
    }) === RESEARCH_PERSISTENCE_STAGE_STATUS.PERSISTED_AND_VERIFIED
  );
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

/**
 * Load ORS opportunity JSON from hotel research-state directory.
 * Honors GDI_DATA_ROOT when set; otherwise repoRoot/cwd.
 */
export function loadOrsOpportunity(hotelId, opportunityId, opts = {}) {
  const hid = text(hotelId);
  const oid = text(opportunityId);
  if (!hid || !oid) return null;
  const dataRoot =
    text(opts.dataRoot) ||
    text(process.env.GDI_DATA_ROOT) ||
    path.join(opts.repoRoot || process.cwd(), "data/group-demand-intelligence");
  const file = path.join(
    dataRoot,
    "hotels",
    hid,
    "opportunity-research-state",
    `${oid}.json`,
  );
  if (!fs.existsSync(file)) return null;
  try {
    const doc = JSON.parse(fs.readFileSync(file, "utf8"));
    const opp = doc?.opportunity && typeof doc.opportunity === "object" ? doc.opportunity : doc;
    if (!opp || typeof opp !== "object") return null;
    return {
      ...opp,
      opportunityId: text(opp.opportunityId || opp.id || oid),
      hotelId: text(opp.hotelId || hid),
      __persistenceSource: "ors_file",
      __orsPath: file,
    };
  } catch {
    return null;
  }
}

function isThinProcessedRow(row = {}) {
  if (!row || typeof row !== "object") return true;
  if (row.__thinRowDegraded === true) return true;
  if (row.opportunity || row.opportunityRow) return false;
  // Processed scorecard rows carry contact counts without evidence arrays.
  const hasEvidence =
    (Array.isArray(row.evidence) && row.evidence.length > 0) ||
    (Array.isArray(row.sources) && row.sources.length > 0);
  const hasFullContact =
    row.primaryContact?.email ||
    row.primaryContact?.phone ||
    (Array.isArray(row.contacts) && row.contacts.length > 0);
  return (
    (row.contactsInvestigated != null || row.presentationSections != null) &&
    !hasEvidence &&
    !hasFullContact
  );
}

/**
 * Detect essential field loss before claiming persistence success.
 */
export function evaluatePersistenceLosslessGuard(sourceOpp = {}, builtPayload = null) {
  const sourceContacts = collectNamedContacts(sourceOpp);
  const sourceEvidence = collectEvidenceUrls(sourceOpp).filter((u) =>
    /^https?:\/\//i.test(u),
  );
  // Processed scorecard rows are never sufficient persistence sources.
  if (sourceOpp.__thinRowDegraded === true) {
    const scorecardSignal =
      Number(sourceOpp.contactsInvestigated || 0) > 0 ||
      Number(sourceOpp.salesReadyContacts || 0) > 0 ||
      Boolean(sourceOpp.primaryContactName) ||
      sourceOpp.presentationSections != null ||
      sourceOpp.commercialResearchComplete === true;
    if (scorecardSignal) {
      return {
        ok: false,
        code: "PERSISTENCE_THIN_ROW_NO_OPPORTUNITY",
        missingContacts: sourceContacts,
        missingEvidence: sourceEvidence,
      };
    }
  }
  if (!builtPayload) {
    return { ok: true, code: null, missingContacts: [], missingEvidence: [] };
  }
  const round = assertSlimPayloadRoundTrip(sourceOpp, builtPayload);
  if (!round.ok) {
    return {
      ok: false,
      code: round.missingContacts.length
        ? "CONTACTS_LOST_IN_PAYLOAD"
        : "EVIDENCE_LOST_IN_PAYLOAD",
      missingContacts: round.missingContacts,
      missingEvidence: round.missingEvidence,
    };
  }
  return {
    ok: true,
    code: null,
    missingContacts: [],
    missingEvidence: [],
    identityOk: round.identityOk !== false,
  };
}

function extractOpportunities(input = {}) {
  if (Array.isArray(input.opportunities) && input.opportunities.length) {
    return input.opportunities.map((opp) => ({
      ...opp,
      opportunityId: text(opp?.opportunityId || opp?.id),
      hotelId: text(opp?.hotelId || input.hotelId),
      __persistenceSource: opp?.__persistenceSource || "input.opportunities",
    })).filter((o) => o.opportunityId);
  }

  const chain = input.chain || {};
  const repoRoot = input.repoRoot || process.cwd();
  const hotelId = text(input.hotelId);

  if (
    Array.isArray(chain.persistenceOpportunities) &&
    chain.persistenceOpportunities.length
  ) {
    return chain.persistenceOpportunities
      .map((opp) => ({
        ...opp,
        opportunityId: text(opp?.opportunityId || opp?.id),
        hotelId: text(opp?.hotelId || hotelId),
        __persistenceSource: opp?.__persistenceSource || "chain.persistenceOpportunities",
      }))
      .filter((o) => o.opportunityId);
  }

  const fromProcessed = [];
  for (const row of Array.isArray(chain.processed) ? chain.processed : []) {
    if (!row || typeof row !== "object") continue;
    if (row.persistable === false) continue;
    const opportunityId = text(
      row.opportunity?.opportunityId ||
        row.opportunity?.id ||
        row.opportunityRow?.opportunityId ||
        row.opportunityId ||
        row.id,
    );
    if (!opportunityId) continue;

    let opp = row.opportunity || row.opportunityRow || null;
    let source = opp ? "processed.opportunity" : null;

    if (!opp) {
      const ors = loadOrsOpportunity(hotelId || row.hotelId, opportunityId, {
        repoRoot,
        dataRoot: input.dataRoot,
      });
      if (ors) {
        opp = ors;
        source = "ors_reload";
      }
    }

    if (!opp) {
      // Thin processed row — mark degraded so lossless guard can fail closed
      // when essential contact/evidence signals were present on the scorecard.
      opp = {
        ...row,
        opportunityId,
        hotelId: text(row.hotelId || hotelId),
        __thinRowDegraded: true,
        __persistenceSource: "processed_thin_row",
      };
      source = "processed_thin_row";
    }

    fromProcessed.push({
      ...opp,
      opportunityId,
      hotelId: text(opp.hotelId || hotelId || row.hotelId),
      segment: opp.segment || opp.segmentId || row.segmentId || null,
      runId: text(opp.runId || input.runId || row.runId) || null,
      researchContinuation: opp.researchContinuation || row.researchContinuation || null,
      researchStopClass: opp.researchStopClass || row.researchStopClass || null,
      primaryContactName:
        opp.primaryContactName || row.primaryContactName || opp.primaryContact?.name || null,
      __persistenceSource: source,
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
      __persistenceSource: "seedOpportunities",
    }))
    .filter((o) => o.opportunityId);
}

/**
 * Collapse same hotelId::opportunityId appearing from multiple discovery segments
 * within one Full E2E run. Keeps first occurrence; later ones are duplicates.
 */
export function dedupeResearchCandidatesByCompositeKey(
  opportunities = [],
  hotelId = "",
) {
  const kept = [];
  const duplicates = [];
  const seen = new Set();
  for (const opp of opportunities) {
    const opportunityId = text(opp?.opportunityId || opp?.id);
    const hid = text(opp?.hotelId || hotelId);
    if (!opportunityId || !hid) {
      kept.push(opp);
      continue;
    }
    const key = `${hid}::${opportunityId}`;
    if (seen.has(key)) {
      duplicates.push({
        hotelId: hid,
        opportunityId,
        compositeKey: key,
        segment: opp.segment || opp.segmentId || null,
        reason: "DUPLICATE_COMPOSITE_KEY_IN_RUN",
      });
      continue;
    }
    seen.add(key);
    kept.push(opp);
  }
  return { opportunities: kept, duplicates, uniqueCount: kept.length };
}

/**
 * Compact execution summary from completed persistence outcome (actuals).
 */
export function buildResearchPersistenceExecutionSummary(outcome = {}) {
  const rows = Array.isArray(outcome.rows) ? outcome.rows : [];
  const duplicatesResolved = Array.isArray(outcome.duplicatesResolved)
    ? outcome.duplicatesResolved.length
    : Number(outcome.counts?.DUPLICATES_RESOLVED || 0);
  const exploratory = rows.filter(
    (r) =>
      r.researchLifecycleStatus === "EXPLORATORY" ||
      r.exploratory === true,
  ).length;
  return {
    opportunitiesResearched: Number(outcome.counts?.candidatesBeforeDedupe ?? outcome.counts?.candidates ?? rows.length),
    uniqueCandidates: Number(outcome.counts?.candidates ?? rows.length),
    newResearchOpportunities:
      Number(outcome.counts?.CREATED || 0) +
      Number(outcome.counts?.WOULD_CREATE || 0),
    materiallyUpdatedOpportunities:
      Number(outcome.counts?.UPDATED || 0) +
      Number(outcome.counts?.WOULD_UPDATE || 0),
    unchangedOpportunities: Number(outcome.counts?.UNCHANGED || 0),
    duplicatesResolved,
    exploratoryResearchOnly: exploratory,
    airtable: {
      CREATE: Number(outcome.counts?.CREATED || 0),
      UPDATE: Number(outcome.counts?.UPDATED || 0),
      UNCHANGED: Number(outcome.counts?.UNCHANGED || 0),
      WOULD_CREATE: Number(outcome.counts?.WOULD_CREATE || 0),
      WOULD_UPDATE: Number(outcome.counts?.WOULD_UPDATE || 0),
      FAILED: Number(outcome.counts?.FAILED || 0),
    },
    failedWrites: Number(outcome.counts?.FAILED || 0),
    verifiedReadBacks: Array.isArray(outcome.readBack)
      ? outcome.readBack.filter((r) => r.matched === true).length
      : 0,
    dryRun: outcome.dryRun !== false,
    gdoWrites: Number(outcome.gdoWrites || 0),
    publicationWrites: Number(outcome.publicationWrites || 0),
  };
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
  // Injectable inventoryStore enables deterministic pilots without Airtable I/O.
  // It never unlocks production Airtable writes.
  const useInjectedStore =
    input.inventoryStore && typeof input.inventoryStore.upsert === "function";
  const productionUnlocked = auth.productionUnlocked === true && !useInjectedStore;
  const mutateStore =
    useInjectedStore === true
      ? input.simulateLiveWrites === true
      : productionUnlocked === true;
  const dryRun = !mutateStore;
  const destination = GDI_AIRTABLE_TABLES.GDI_HOTEL_RESEARCH_OPPORTUNITIES;
  const budget = Math.max(0, Number(input.writeBudget || 0) || 0);
  const extracted = extractOpportunities(input);
  const deduped = dedupeResearchCandidatesByCompositeKey(extracted, hotelId);
  const writeSetPolicy =
    text(input.writeSetPolicy) ||
    (productionUnlocked || input.requireValidatedFqoWriteSet === true
      ? RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY
      : RESEARCH_PERSISTENCE_WRITE_SET_POLICY.RESEARCH_UNIVERSE_AUDIT);
  const useValidatedOnly =
    writeSetPolicy === RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY ||
    input.requireValidatedFqoWriteSet === true;
  const filtered = useValidatedOnly
    ? filterValidatedFqoPersistenceWriteSet(deduped.opportunities, {
        allowHistoricalSchemaCompat: input.allowHistoricalSchemaCompat !== false,
        allowedOpportunityIds: input.allowedOpportunityIds || null,
      })
    : {
        policy: RESEARCH_PERSISTENCE_WRITE_SET_POLICY.RESEARCH_UNIVERSE_AUDIT,
        kept: deduped.opportunities,
        deferred: [],
        keptCount: deduped.opportunities.length,
        deferredCount: 0,
      };
  const opportunities = filtered.kept;

  const outcome = {
    version: GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1,
    at: nowIso(),
    hotelId,
    runId,
    destinationTable: destination.name,
    destinationTableId: destination.tableId,
    // Reflect actual mutate vs plan mode (injected simulation store can mutate in-memory).
    dryRun,
    simulationStore: useInjectedStore === true,
    authorization: auth,
    writeSetPolicy: filtered.policy,
    researchCompletedIndependently: input.researchCompleted === true,
    gdoWrites: 0,
    publicationWrites: 0,
    airtableProductionGuard: AIRTABLE_PRODUCTION_WRITE_GUARD,
    counts: {
      candidatesBeforeDedupe: extracted.length,
      candidatesAfterDedupe: deduped.opportunities.length,
      candidates: opportunities.length,
      DEFERRED_NONQUALIFIED: filtered.deferredCount,
      DUPLICATES_RESOLVED: deduped.duplicates.length,
      WOULD_CREATE: 0,
      WOULD_UPDATE: 0,
      UNCHANGED: 0,
      CREATED: 0,
      UPDATED: 0,
      FAILED: 0,
      SKIPPED_FORBIDDEN: 0,
      SKIPPED_BUDGET: 0,
    },
    duplicatesResolved: deduped.duplicates,
    deferredNonqualified: filtered.deferred,
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

    // Prefer opportunity-native provenance. Only seed firstDiscoveredRunId from
    // the current run when absent (CREATE path). Never invent a new discovery
    // run id solely for dry-run compare / UPDATE serialization.
    const seededFirstDiscoveredRunId =
      text(opp.firstDiscoveredRunId || opp.firstSeenRunId) || runId || null;
    // Slice 2B: stamp stable primary dgGeneratorId before inventory slim/upsert.
    // Does not write Demand Generators / Programs / Signals (offline plan only).
    const stampedOpp = ensureDemandGeneratorIdOnOpportunity({
      ...opp,
      opportunityId,
      hotelId,
    });
    const opportunityPayload = {
      ...stampedOpp,
      opportunityId,
      hotelId,
      firstDiscoveredRunId: seededFirstDiscoveredRunId,
      lastResearchedAt: text(opp.lastResearchedAt) || nowIso(),
    };

    // Pre-flight slim + lossless guard — never report ok on lost contacts/evidence.
    const slimProbe = buildSlimResearchPayload(opportunityPayload);
    const guard = evaluatePersistenceLosslessGuard(
      opportunityPayload,
      slimProbe.ok ? slimProbe.payload : null,
    );
    if (!slimProbe.ok || !guard.ok) {
      outcome.counts.FAILED += 1;
      outcome.ok = false;
      const failRow = {
        hotelId,
        opportunityId,
        action: "FAILED",
        code: slimProbe.ok === false ? slimProbe.code : guard.code,
        outcome: WRITE_OUTCOME.FAILED,
        disposition: opp.disposition || opp.manifestDisposition || null,
        researchLifecycleStatus: null,
        commercialEligibilityStatus: null,
        preservation: slimProbe.preservation || {
          missingContacts: guard.missingContacts,
          missingEvidence: guard.missingEvidence,
        },
        persistenceSource: opportunityPayload.__persistenceSource || null,
        recoverable: true,
      };
      failRow.founderOperationClass =
        classifyResearchPersistenceOperation(failRow);
      outcome.rows.push(failRow);
      continue;
    }

    const built = buildResearchInventoryFields(opportunityPayload, {
      hotelId,
      hotelName: input.hotelName || null,
    });
    if (!built.ok) {
      outcome.counts.FAILED += 1;
      outcome.ok = false;
      outcome.rows.push({
        hotelId,
        opportunityId,
        action: "FAILED",
        code: built.code,
        outcome: WRITE_OUTCOME.FAILED,
        recoverable: true,
      });
      continue;
    }
    const hotel = { hotelId, hotelName: input.hotelName || null };

    let result;
    try {
      if (useInjectedStore) {
        result = await input.inventoryStore.upsert({
          opportunity: opportunityPayload,
          hotel,
          dryRun,
        });
      } else {
        result = await upsertResearchInventoryRecord({
          opportunity: opportunityPayload,
          hotel,
          dryRun,
        });
      }
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
      researchLifecycleStatus: built.fields?.researchLifecycleStatus || null,
      exploratory: built.fields?.researchLifecycleStatus === "EXPLORATORY",
      commercialEligibilityStatus:
        built.fields?.commercialEligibilityStatus || null,
      disposition: opp.disposition || null,
      payloadChars: slimProbe.length || null,
      persistenceSource: opportunityPayload.__persistenceSource || null,
      preservation: slimProbe.preservation || null,
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
    if (action === "FAILED") {
      outcome.ok = false;
    }
    row.founderOperationClass = classifyResearchPersistenceOperation(row);
    outcome.rows.push(row);

    if (mutateStore && (action === "CREATED" || action === "UPDATED")) {
      writesUsed += 1;
      if (input.readBack === true && result.airtableRecordId) {
        try {
          const rb = useInjectedStore
            ? await input.inventoryStore.readBack(result.airtableRecordId)
            : await readBackResearchInventory(result.airtableRecordId);
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
  outcome.executionSummary = buildResearchPersistenceExecutionSummary(outcome);
  outcome.applyRequested = auth.applyRequested === true;
  outcome.stageStatus = classifyResearchPersistenceStageStatus(outcome, {
    requested: true,
    applyRequested: auth.applyRequested === true,
    requireReadBack: input.readBack === true,
    simulationStore: useInjectedStore === true && mutateStore === true,
  });
  outcome.persistenceCertified = isResearchPersistenceCertified(outcome, {
    requireReadBack: input.readBack === true,
    simulationStore: useInjectedStore === true && mutateStore === true,
  });
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
  RESEARCH_PERSISTENCE_WRITE_SET_POLICY,
  RESEARCH_PERSISTENCE_OPERATION_CLASS,
  RESEARCH_PERSISTENCE_STAGE_STATUS,
  filterValidatedFqoPersistenceWriteSet,
  classifyResearchPersistenceOperation,
  classifyResearchPersistenceStageStatus,
  isResearchPersistenceCertified,
  evaluateResearchPersistenceAuthorization,
  evaluatePersistenceLosslessGuard,
  loadOrsOpportunity,
  dedupeResearchCandidatesByCompositeKey,
  buildResearchPersistenceExecutionSummary,
  persistFullE2eResearchInventory,
  writeResearchPersistenceLedger,
};
