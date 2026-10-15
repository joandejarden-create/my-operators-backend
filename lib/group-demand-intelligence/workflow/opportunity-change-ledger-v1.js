/**
 * Authoritative opportunity-level change ledger V1.
 *
 * All scorecard unique totals must aggregate from this ledger — not from
 * double-counting task disposition rows or enrichment activities as cards.
 *
 * One opportunity/account/venue identity → one primary change row.
 * Contact enrichment is an activity flag, not automatically a new prospect.
 * Redisposition preserves originalDiscoveryTaskId; does not credit fresh discovery.
 */

export const OPPORTUNITY_CHANGE_LEDGER_VERSION = "gdi-opportunity-change-ledger-v1";

/** Mutually exclusive primary change type for unique opportunity totals. */
export const CHANGE_TYPE = Object.freeze({
  NEW_PROSPECT_FIRST_PUBLISHED: "new_prospect_first_published",
  EXISTING_PROSPECT_MATERIALLY_ENRICHED: "existing_prospect_materially_enriched",
  EXISTING_INTERNAL_PROMOTED: "existing_internal_promoted",
  RESTORED_REAPPLIED: "restored_reapplied",
  UNCHANGED: "unchanged",
  REJECTED: "rejected",
  INTERNAL_ONLY: "internal_only",
  UNRESOLVED: "unresolved",
  QUARANTINED: "quarantined",
});

export const TASK_EXECUTION_STATUS = Object.freeze({
  PLANNED: "planned",
  RUNNING: "running",
  COMPLETED: "completed",
  PARTIAL: "partial",
  FAILED: "failed",
  BLOCKED: "blocked",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function identityKey(row = {}) {
  return (
    text(row.canonicalOpportunityId) ||
    text(row.canonicalAccountId) ||
    text(row.canonicalVenueId) ||
    `${text(row.hotelId)}::${text(row.organizationOrVenueName).toLowerCase()}`
  );
}

/**
 * Create one change ledger row.
 */
export function createChangeLedgerRow({
  runId = null,
  taskId = null,
  hotelId = null,
  canonicalOpportunityId = null,
  canonicalAccountId = null,
  canonicalVenueId = null,
  organizationOrVenueName = null,
  primaryResearchSegment = null,
  useCaseTags = [],
  originalDiscoveryTaskId = null,
  recoveryOrReprocessTaskId = null,
  beforeExisted = null,
  beforeCustomerVisible = null,
  afterExisted = null,
  afterCustomerVisible = null,
  changeType = CHANGE_TYPE.UNRESOLVED,
  contactEnrichmentActivity = false,
  evidenceRefs = [],
  publicationResult = null,
  notes = null,
} = {}) {
  return {
    version: OPPORTUNITY_CHANGE_LEDGER_VERSION,
    runId,
    taskId,
    hotelId,
    canonicalOpportunityId,
    canonicalAccountId,
    canonicalVenueId,
    organizationOrVenueName,
    primaryResearchSegment,
    useCaseTags: useCaseTags || [],
    originalDiscoveryTaskId:
      originalDiscoveryTaskId || taskId || null,
    recoveryOrReprocessTaskId: recoveryOrReprocessTaskId || null,
    before: {
      existed: beforeExisted,
      customerVisible: beforeCustomerVisible,
    },
    after: {
      existed: afterExisted,
      customerVisible: afterCustomerVisible,
    },
    changeType,
    contactEnrichmentActivity: Boolean(contactEnrichmentActivity),
    evidenceRefs: evidenceRefs || [],
    publicationResult,
    notes,
    identityKey: null,
  };
}

function finalizeRow(row) {
  row.identityKey = identityKey(row);
  return row;
}

/**
 * Merge duplicate identities: prefer stronger change types; keep earliest discovery task.
 */
const CHANGE_STRENGTH = {
  [CHANGE_TYPE.NEW_PROSPECT_FIRST_PUBLISHED]: 80,
  [CHANGE_TYPE.EXISTING_INTERNAL_PROMOTED]: 70,
  [CHANGE_TYPE.EXISTING_PROSPECT_MATERIALLY_ENRICHED]: 60,
  [CHANGE_TYPE.RESTORED_REAPPLIED]: 50,
  [CHANGE_TYPE.INTERNAL_ONLY]: 40,
  [CHANGE_TYPE.UNRESOLVED]: 30,
  [CHANGE_TYPE.REJECTED]: 20,
  [CHANGE_TYPE.QUARANTINED]: 15,
  [CHANGE_TYPE.UNCHANGED]: 10,
};

export function dedupeChangeLedger(rows = []) {
  const byKey = new Map();
  for (const raw of rows) {
    const row = finalizeRow({ ...raw });
    const key = row.identityKey;
    if (!key) continue;
    const prev = byKey.get(key);
    if (!prev) {
      byKey.set(key, row);
      continue;
    }
    const next = { ...prev };
    // Preserve earliest original discovery provenance
    if (
      prev.originalDiscoveryTaskId &&
      row.originalDiscoveryTaskId &&
      prev.originalDiscoveryTaskId !== row.originalDiscoveryTaskId
    ) {
      // keep prev discovery; attach later as recovery if stronger publish
      if (
        (CHANGE_STRENGTH[row.changeType] || 0) >=
        (CHANGE_STRENGTH[prev.changeType] || 0)
      ) {
        next.recoveryOrReprocessTaskId =
          row.recoveryOrReprocessTaskId ||
          row.taskId ||
          prev.recoveryOrReprocessTaskId;
      }
    } else if (!prev.originalDiscoveryTaskId && row.originalDiscoveryTaskId) {
      next.originalDiscoveryTaskId = row.originalDiscoveryTaskId;
    }
    if (
      (CHANGE_STRENGTH[row.changeType] || 0) >
      (CHANGE_STRENGTH[prev.changeType] || 0)
    ) {
      next.changeType = row.changeType;
      next.after = row.after;
      next.publicationResult = row.publicationResult || prev.publicationResult;
      next.taskId = row.taskId || prev.taskId;
      next.runId = row.runId || prev.runId;
      next.hotelId = row.hotelId || prev.hotelId;
      next.primaryResearchSegment =
        row.primaryResearchSegment || prev.primaryResearchSegment;
    }
    if (row.contactEnrichmentActivity) next.contactEnrichmentActivity = true;
    if (row.recoveryOrReprocessTaskId) {
      next.recoveryOrReprocessTaskId = row.recoveryOrReprocessTaskId;
    }
    next.evidenceRefs = [
      ...new Set([...(prev.evidenceRefs || []), ...(row.evidenceRefs || [])]),
    ];
    next.notes = [prev.notes, row.notes].filter(Boolean).join(" | ") || null;
    byKey.set(key, finalizeRow(next));
  }
  return [...byKey.values()];
}

/**
 * Aggregate unique counts for scorecard (contact enrichment does not inflate new).
 */
export function summarizeChangeLedger(rows = []) {
  const deduped = dedupeChangeLedger(rows);
  const byType = {};
  for (const t of Object.values(CHANGE_TYPE)) byType[t] = 0;
  let contactEnrichmentActivities = 0;
  const byHotelSegment = new Map();

  for (const r of deduped) {
    byType[r.changeType] = (byType[r.changeType] || 0) + 1;
    if (r.contactEnrichmentActivity) contactEnrichmentActivities += 1;
    const key = `${r.hotelId || "unknown"}::${r.primaryResearchSegment || "unknown"}`;
    if (!byHotelSegment.has(key)) {
      byHotelSegment.set(key, {
        hotelId: r.hotelId || "unknown",
        segmentId: r.primaryResearchSegment || "unknown",
        newProspectFirstPublished: 0,
        existingInternalPromoted: 0,
        existingMateriallyEnriched: 0,
        restoredReapplied: 0,
        contactEnrichmentActivities: 0,
        uniqueRecords: 0,
      });
    }
    const agg = byHotelSegment.get(key);
    agg.uniqueRecords += 1;
    if (r.changeType === CHANGE_TYPE.NEW_PROSPECT_FIRST_PUBLISHED) {
      agg.newProspectFirstPublished += 1;
    }
    if (r.changeType === CHANGE_TYPE.EXISTING_INTERNAL_PROMOTED) {
      agg.existingInternalPromoted += 1;
    }
    if (r.changeType === CHANGE_TYPE.EXISTING_PROSPECT_MATERIALLY_ENRICHED) {
      agg.existingMateriallyEnriched += 1;
    }
    if (r.changeType === CHANGE_TYPE.RESTORED_REAPPLIED) {
      agg.restoredReapplied += 1;
    }
    if (r.contactEnrichmentActivity) agg.contactEnrichmentActivities += 1;
  }

  return {
    version: OPPORTUNITY_CHANGE_LEDGER_VERSION,
    uniqueRecords: deduped.length,
    byChangeType: byType,
    /** Net-new first published only — does not include enrichment or restores */
    netNewFirstPublished: byType[CHANGE_TYPE.NEW_PROSPECT_FIRST_PUBLISHED] || 0,
    promotedFromInternal: byType[CHANGE_TYPE.EXISTING_INTERNAL_PROMOTED] || 0,
    materiallyEnriched: byType[CHANGE_TYPE.EXISTING_PROSPECT_MATERIALLY_ENRICHED] || 0,
    restoredReapplied: byType[CHANGE_TYPE.RESTORED_REAPPLIED] || 0,
    contactEnrichmentActivities,
    byHotelSegment: [...byHotelSegment.values()],
    rows: deduped,
  };
}

/**
 * Map disposition + publication context → change type.
 */
export function changeTypeFromDisposition(d = {}, ctx = {}) {
  const disp = String(d.disposition || "").toLowerCase();
  if (/quarantine/.test(disp)) return CHANGE_TYPE.QUARANTINED;
  if (/reject/.test(disp)) return CHANGE_TYPE.REJECTED;
  if (ctx.isRestore || /restore|reappl/i.test(String(d.notes || d.reprocess || ""))) {
    return CHANGE_TYPE.RESTORED_REAPPLIED;
  }
  if (
    /contact_resolved|material.?enrich|named.?buyer/i.test(disp) ||
    ctx.contactEnrichmentOnly === true
  ) {
    return CHANGE_TYPE.EXISTING_PROSPECT_MATERIALLY_ENRICHED;
  }
  if (/internal_enrichment|internal/.test(disp) && !ctx.published) {
    return CHANGE_TYPE.INTERNAL_ONLY;
  }
  if (ctx.wasInternal && ctx.published) {
    return CHANGE_TYPE.EXISTING_INTERNAL_PROMOTED;
  }
  if (
    (/qualified_publishable/.test(disp) || ctx.published) &&
    !ctx.wasAlreadyFacing
  ) {
    if (ctx.wasInternal) return CHANGE_TYPE.EXISTING_INTERNAL_PROMOTED;
    return CHANGE_TYPE.NEW_PROSPECT_FIRST_PUBLISHED;
  }
  if (/needs_review|unresolved/.test(disp)) return CHANGE_TYPE.UNRESOLVED;
  if (ctx.wasAlreadyFacing && !ctx.materialChange) return CHANGE_TYPE.UNCHANGED;
  return CHANGE_TYPE.UNRESOLVED;
}

/**
 * Build ledger rows from paid-MS disposition artifacts + known publication facts.
 * Prefer explicit knownPublished / knownRestored lists over disposition string alone.
 */
export function buildLedgerFromPaidArtifacts({
  runId = "paid_ms_consolidated",
  dispositions = [],
  /** @type {Array<{opportunityId?, organizationName, hotelId, segment, originalDiscoveryTaskId?, recoveryTaskId?, changeType, contactEnrichmentActivity?}>} */
  authoritativeOutcomes = [],
} = {}) {
  const rows = [];

  for (const a of authoritativeOutcomes) {
    rows.push(
      finalizeRow(
        createChangeLedgerRow({
          runId,
          taskId: a.recoveryTaskId || a.originalDiscoveryTaskId || a.taskId,
          hotelId: a.hotelId,
          canonicalOpportunityId: a.opportunityId || null,
          canonicalVenueId: a.venueId || null,
          organizationOrVenueName: a.organizationName || a.venueName,
          primaryResearchSegment: a.segment,
          useCaseTags: a.useCaseTags || [],
          originalDiscoveryTaskId: a.originalDiscoveryTaskId || a.taskId,
          recoveryOrReprocessTaskId: a.recoveryTaskId || null,
          beforeExisted: a.beforeExisted ?? null,
          beforeCustomerVisible: a.beforeCustomerVisible ?? null,
          afterExisted: true,
          afterCustomerVisible: a.afterCustomerVisible ?? null,
          changeType: a.changeType,
          contactEnrichmentActivity: a.contactEnrichmentActivity === true,
          evidenceRefs: a.evidenceRefs || [],
          publicationResult: a.publicationResult || null,
          notes: a.notes || null,
        })
      )
    );
  }

  // Optional: fill gaps from dispositions not already in authoritative list
  const seen = new Set(rows.map((r) => identityKey(r)));
  for (const d of dispositions) {
    const name = d.organizationName || d.venueName || d.package?.organizationName;
    const hotelId = d.hotelId || d.package?.hotelId;
    const key = `${hotelId}::${text(name).toLowerCase()}`;
    if (!name || seen.has(key) || seen.has(d.opportunityId)) continue;
    // Skip raw disposition rows when authoritativeOutcomes provided — avoid double-count PE/scaling
    if (authoritativeOutcomes.length) continue;
    const changeType = changeTypeFromDisposition(d, {
      published: /qualified_publishable/.test(String(d.disposition || "")),
    });
    rows.push(
      finalizeRow(
        createChangeLedgerRow({
          runId,
          taskId: d.taskId,
          hotelId,
          canonicalOpportunityId: d.opportunityId || d.package?.opportunityId,
          organizationOrVenueName: name,
          primaryResearchSegment: d.segment,
          originalDiscoveryTaskId: d.taskId,
          changeType,
          evidenceRefs: d.evidenceUrls || [],
        })
      )
    );
  }

  return summarizeChangeLedger(rows);
}
