/**
 * Idempotent GDI → Airtable synchronization (research path).
 *
 * Reuses airtable-opportunity-store upsert-by-opportunityId.
 * Preserves sales-owned and publication-owned fields.
 * Defaults to dry-run. Never triggers Surfe / paid enrichment.
 * Never publishes.
 */
import {
  isGdiOpportunityAirtableConfigured,
  findOpportunityRecordById,
  upsertOpportunity,
  listOpportunitiesForHotel,
  opportunityToAirtableFields,
  airtableRecordToOpportunity,
} from "../airtable-opportunity-store.js";
import { MAP_GDI_OPPORTUNITY as F } from "../opportunity-field-map.js";
import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";
import {
  mergeResearchPayloadPreservingSales,
  applySalesOwnershipToAirtableFields,
  mapLifecycleToActionStatus,
  GDI_WORKFLOW_LIFECYCLE,
  GDI_AIRTABLE_FIELD_OWNERSHIP_V1,
  PUBLICATION_OWNED_PAYLOAD_KEYS,
} from "./gdi-airtable-field-ownership-v1.js";

export const GDI_AIRTABLE_SYNC_V1 = "gdi-airtable-sync-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function withRetry(fn, { attempts = 4, label = "airtable" } = {}) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const msg = String(err?.message || err);
      const retryable =
        /429|rate|timeout|ETIMEDOUT|ECONNRESET|503|502/i.test(msg) ||
        err?.statusCode === 429;
      if (!retryable || i === attempts - 1) throw err;
      await sleep(400 * Math.pow(2, i));
      console.warn(`[${label}] retry ${i + 1}/${attempts}: ${msg.slice(0, 160)}`);
    }
  }
  throw lastErr;
}

/**
 * Sync one hotel's canonical opportunities to Airtable.
 *
 * @param {object} input
 * @param {string} input.hotelId
 * @param {boolean} [input.dryRun=true]
 * @param {object[]} [input.opportunities] optional override set
 * @param {string} [input.runId]
 * @param {string} [input.lifecycleHint] conceptual lifecycle for empty actionStatus
 */
export async function syncHotelOpportunitiesToAirtable(input = {}) {
  const hotelId = text(input.hotelId);
  const dryRun = input.dryRun !== false;
  const runId = text(input.runId) || `gdi_sync_${Date.now().toString(36)}`;

  if (!hotelId) {
    return { ok: false, code: "HOTEL_ID_REQUIRED", dryRun };
  }
  if (!isGdiOpportunityAirtableConfigured()) {
    return {
      ok: false,
      code: "AIRTABLE_NOT_CONFIGURED",
      dryRun,
      hotelId,
      message:
        "Set AIRTABLE_API_KEY/PAT and AIRTABLE_GDI_BASE_ID / AIRTABLE_INTELLIGENCE_BASE_ID.",
    };
  }

  let opportunities = Array.isArray(input.opportunities)
    ? input.opportunities
    : null;
  if (!opportunities) {
    const doc = await loadOpportunitiesCanonical(hotelId, {
      bypassCache: true,
    });
    opportunities = doc?.opportunities || [];
  }

  const summary = {
    hotelId,
    dryRun,
    runId,
    ownershipContract: GDI_AIRTABLE_FIELD_OWNERSHIP_V1,
    syncContract: GDI_AIRTABLE_SYNC_V1,
    total: opportunities.length,
    created: 0,
    updated: 0,
    unchanged: 0,
    failed: 0,
    skippedExcluded: 0,
    errors: [],
    rows: [],
  };

  for (const raw of opportunities) {
    const opportunityId = text(raw?.id || raw?.opportunityId);
    if (!opportunityId) {
      summary.failed += 1;
      summary.errors.push({ error: "opportunity_id_required" });
      continue;
    }

    // Internal excluded stay in Airtable but flagged; never treated as publish set.
    const excluded =
      raw.customerVisible === false ||
      raw.prospectSuppressed === true ||
      raw.archived === true ||
      raw.isArchived === true;

    try {
      const existingRec = await withRetry(
        () => findOpportunityRecordById(opportunityId),
        { label: `find:${opportunityId}` },
      );
      const existingOpp = existingRec
        ? airtableRecordToOpportunity(existingRec)
        : null;
      const existingFields = existingRec?.fields || {};

      const mergedPayload = mergeResearchPayloadPreservingSales(
        existingOpp,
        {
          ...raw,
          id: opportunityId,
          hotelId,
        },
      );

      // Lifecycle hint only fills empty actionStatus (sales-owned when set).
      const lifecycleHint =
        input.lifecycleHint ||
        (excluded
          ? GDI_WORKFLOW_LIFECYCLE.EXCLUDED
          : GDI_WORKFLOW_LIFECYCLE.QUALIFIED);
      mergedPayload.actionStatus = mapLifecycleToActionStatus(
        lifecycleHint,
        existingFields[F.actionStatus] || existingOpp?.actionStatus,
      );

      if (dryRun) {
        // Validate field mapping without writes.
        opportunityToAirtableFields(mergedPayload, {
          hotelId,
          runId,
          researchVersion: input.researchVersion || runId,
        });
        applySalesOwnershipToAirtableFields(
          {},
          existingFields,
          { overwriteSalesOwned: input.overwriteSalesOwned === true },
        );
        const wouldCreate = !existingRec;
        summary.rows.push({
          opportunityId,
          action: wouldCreate ? "CREATE" : "UPDATE",
          dryRun: true,
          excluded,
        });
        if (wouldCreate) summary.created += 1;
        else summary.updated += 1;
        continue;
      }

      const result = await withRetry(
        () =>
          upsertOpportunity(
            { ...mergedPayload, id: opportunityId, hotelId },
            {
              hotelId,
              runId,
              researchVersion: input.researchVersion || runId,
              preserveSalesOwned: input.overwriteSalesOwned !== true,
            },
          ),
        { label: `upsert:${opportunityId}` },
      );

      // Read-back verification
      const readBack = await withRetry(
        () => findOpportunityRecordById(opportunityId),
        { label: `readback:${opportunityId}` },
      );
      if (!readBack) {
        summary.failed += 1;
        summary.errors.push({
          opportunityId,
          error: "read_back_missing_after_upsert",
        });
        continue;
      }
      const rbId = readBack.fields?.[F.opportunityId];
      if (String(rbId) !== opportunityId) {
        summary.failed += 1;
        summary.errors.push({
          opportunityId,
          error: "read_back_id_mismatch",
          got: rbId,
        });
        continue;
      }

      if (result.created) summary.created += 1;
      else summary.updated += 1;
      if (excluded) summary.skippedExcluded += 1;
      summary.rows.push({
        opportunityId,
        action: result.created ? "CREATED" : "UPDATED",
        airtableRecordId: result.airtableRecordId,
        excluded,
        salesFieldsPreserved: true,
      });
    } catch (err) {
      summary.failed += 1;
      summary.errors.push({
        opportunityId,
        error: String(err?.message || err).slice(0, 300),
        code: err?.code || null,
      });
    }
  }

  // Detect Airtable orphans vs local set (report only — never delete).
  let airtableCount = null;
  try {
    const listed = await listOpportunitiesForHotel(hotelId);
    airtableCount = listed.length;
    const localIds = new Set(
      opportunities.map((o) => text(o.id || o.opportunityId)).filter(Boolean),
    );
    summary.airtableOnlyIds = listed
      .map((o) => o.id)
      .filter((id) => id && !localIds.has(id))
      .slice(0, 50);
  } catch {
    /* optional */
  }

  return {
    ok: summary.failed === 0,
    dryRun,
    hotelId,
    airtableCount,
    summary,
    fieldMap: Object.keys(F),
  };
}

/**
 * After verified publication: stamp publication-owned payload keys only.
 * Does not reconstruct approved presentation from Airtable.
 * Queues reconciliation on failure (returns reconcileNeeded).
 */
export async function reconcilePublicationStatusToAirtable(input = {}) {
  const hotelId = text(input.hotelId);
  const dryRun = input.dryRun !== false;
  const publishedIds = Array.isArray(input.publishedOpportunityIds)
    ? input.publishedOpportunityIds.map(text).filter(Boolean)
    : [];
  if (!hotelId) {
    return { ok: false, code: "HOTEL_ID_REQUIRED", dryRun };
  }
  if (!isGdiOpportunityAirtableConfigured()) {
    return {
      ok: false,
      code: "AIRTABLE_NOT_CONFIGURED",
      dryRun,
      reconcileNeeded: true,
      hotelId,
    };
  }

  const stamp = {
    gdiPublicationStatus: "PUBLISHED",
    gdiPublicationVersion: input.publicationVersion || null,
    gdiPublicationId: input.publicationId || null,
    gdiPublishedAt: input.publishedAt || new Date().toISOString(),
    gdiPublishedOpportunityIds: publishedIds,
    gdiCustomerShareReady: input.customerShareReady !== false,
    gdiFullContentParityStatus: input.fullContentParityStatus || null,
    gdiFullContentParityVerifiedAt: input.parityVerifiedAt || null,
    gdiPublicationProvenance: input.provenance || null,
  };

  const results = [];
  const errors = [];

  for (const opportunityId of publishedIds) {
    try {
      const existingRec = await withRetry(
        () => findOpportunityRecordById(opportunityId),
        { label: `pub-find:${opportunityId}` },
      );
      if (!existingRec) {
        errors.push({
          opportunityId,
          error: "airtable_row_missing_queue_reconcile",
        });
        continue;
      }
      const existing = airtableRecordToOpportunity(existingRec) || {};
      const nextPayload = { ...existing };
      for (const key of PUBLICATION_OWNED_PAYLOAD_KEYS) {
        if (stamp[key] != null) nextPayload[key] = stamp[key];
      }
      nextPayload.actionStatus = mapLifecycleToActionStatus(
        GDI_WORKFLOW_LIFECYCLE.PUBLISHED,
        // Prefer keeping sales actionStatus if it is a sales follow-up value
        /SALES_FOLLOW_UP|CONTACTED|WORKING/i.test(
          String(existing.actionStatus || ""),
        )
          ? existing.actionStatus
          : null,
      );
      nextPayload.gdiPublicationStatus = "PUBLISHED";

      if (dryRun) {
        results.push({ opportunityId, action: "WOULD_STAMP_PUBLISHED" });
        continue;
      }
      await withRetry(
        () =>
          upsertOpportunity(
            { ...nextPayload, id: opportunityId, hotelId },
            { hotelId, runId: input.runId || null },
          ),
        { label: `pub-upsert:${opportunityId}` },
      );
      results.push({ opportunityId, action: "STAMPED_PUBLISHED" });
    } catch (err) {
      errors.push({
        opportunityId,
        error: String(err?.message || err).slice(0, 300),
      });
    }
  }

  return {
    ok: errors.length === 0,
    dryRun,
    hotelId,
    stamped: results.length,
    errors,
    reconcileNeeded: errors.length > 0,
    // Successful publication must NOT be rolled back for Airtable failures.
    publicationRollbackRequired: false,
    stamp,
  };
}

// Re-export map for docs/tests without circular naming confusion
export { F as MAP_GDI_OPPORTUNITY };
