/**
 * Central GDI canonical promotion service.
 * All lanes should promote TRUE/qualified opportunities through this module —
 * not via ad-hoc Airtable writes.
 *
 * Semantics (Yield Recovery V1.1):
 * - NEW_TO_GDI_ON_PROMOTION: first time the row enters canonical GDI
 * - firstDiscoveredAt / firstDiscoveredRunId preserve earlier discovery when known
 * - firstSeenRunId = promotion run (when entered canonical bag) for weekly NEW_TO_GDI
 * - Never invent facts; never create duplicates
 */

import { upsertOpportunity, findOpportunityRecordById } from "./airtable-opportunity-store.js";
import { invalidateGdiHotelReadCache } from "./read-cache.js";
import { attachDiscoveryProvenance } from "./research-coverage/entities.js";
import {
  enrichGdiOpportunityForCustomer,
  shouldHideAfterEnrichment,
} from "./enrich-gdi-opportunity-for-customer.js";
import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "./future-watch/is-valid-future-watch-v1.js";

function optsNowDate() {
  return new Date().toISOString().slice(0, 10);
}

export const PROMOTION_ACTION = Object.freeze({
  PROMOTE_NEW: "PROMOTE_NEW",
  UPDATE_EXISTING: "UPDATE_EXISTING",
  HOLD_WATCH: "HOLD_WATCH",
  REJECT_STALE: "REJECT_STALE",
  DUPLICATE_EXISTING: "DUPLICATE_EXISTING",
  SKIP: "SKIP",
});

export const NEWNESS_SEMANTICS = Object.freeze({
  /** First write to canonical GDI — customer NEW; discovery may be earlier. */
  NEW_TO_GDI_ON_PROMOTION: "NEW_TO_GDI_ON_PROMOTION",
  /** Material update to existing canonical row — not NEW. */
  UPDATE_EXISTING: "UPDATE_EXISTING",
});

/**
 * Dedupe against hotel bag by id / title+org / event series key.
 */
export function findCanonicalDuplicate(candidate, existingOpps = []) {
  const id = candidate.id || candidate.opportunityId;
  if (id) {
    const byId = existingOpps.find((o) => o.id === id || o.opportunityId === id);
    if (byId) return { match: byId, reason: "id" };
  }
  const title = String(candidate.title || candidate.opportunityName || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  const org = String(candidate.organizationName || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  const start = String(candidate.eventStartDate || "").slice(0, 10);
  for (const o of existingOpps) {
    const ot = String(o.title || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    const oo = String(o.organizationName || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    const os = String(o.eventStartDate || "").slice(0, 10);
    if (title && ot && (ot.includes(title.slice(0, 24)) || title.includes(ot.slice(0, 24)))) {
      if (!org || !oo || oo.includes(org.slice(0, 12)) || org.includes(oo.slice(0, 12))) {
        if (!start || !os || start === os) {
          return { match: o, reason: "title_org_date" };
        }
      }
    }
  }
  return null;
}

/**
 * Build provenance for a promotion write.
 * @param {object} opts
 * @param {string} opts.promotionRunId — run that writes to canonical
 * @param {string} [opts.discoveryRunId] — earlier discovery run (dry-run audit, etc.)
 * @param {string} [opts.discoveryAt] — ISO discovery timestamp
 */
export function buildPromotionProvenance({
  promotionRunId,
  discoveryRunId = null,
  discoveryAt = null,
  targetId = null,
  targetRunId = null,
  method = "controlled_promote",
  playbook = null,
  source = null,
  newnessSemantics = NEWNESS_SEMANTICS.NEW_TO_GDI_ON_PROMOTION,
  at = null,
} = {}) {
  const iso = at || new Date().toISOString();
  const discoveredAt = discoveryAt || iso;
  const discoveredRun = discoveryRunId || promotionRunId;
  return {
    firstDiscoveredAt: discoveredAt,
    firstDiscoveredRunId: discoveredRun,
    // firstSeen* = when entered canonical GDI (promotion), for NEW_TO_GDI weekly semantics
    firstSeenAt: iso,
    firstSeenRunId: promotionRunId,
    discoveryTargetId: targetId,
    discoveryTargetRunId: targetRunId,
    discoveryMethod: method,
    discoveryPlaybook: playbook,
    discoverySource: source,
    lastMaterialChangeAt: iso,
    lastMaterialChangeRunId: promotionRunId,
    newnessSemantics,
    weeklyDeltaState:
      newnessSemantics === NEWNESS_SEMANTICS.NEW_TO_GDI_ON_PROMOTION ? "NEW" : "UPDATED",
    isNewThisWeek: newnessSemantics === NEWNESS_SEMANTICS.NEW_TO_GDI_ON_PROMOTION,
  };
}

/**
 * Merge provenance onto opportunity without inventing empty required fields.
 */
export function applyProvenanceToOpportunity(opp, provenance = {}) {
  return {
    ...opp,
    ...provenance,
    firstDiscoveredAt: provenance.firstDiscoveredAt || opp.firstDiscoveredAt,
    firstDiscoveredRunId:
      provenance.firstDiscoveredRunId || opp.firstDiscoveredRunId,
    firstSeenAt: provenance.firstSeenAt || opp.firstSeenAt,
    firstSeenRunId: provenance.firstSeenRunId || opp.firstSeenRunId,
  };
}

/**
 * Promote or update a qualified GDI opportunity.
 *
 * @returns {{ action, dryRun, opportunity, recordId, duplicate, validation }}
 */
export async function promoteQualifiedGdiOpportunity({
  candidate,
  existingOpps = [],
  hotelId,
  runId,
  discoveryRunId = null,
  discoveryAt = null,
  targetId = null,
  targetRunId = null,
  method = "controlled_promote",
  playbook = null,
  source = null,
  dryRun = true,
  forceUpdateId = null,
  materialUpdateOnly = false,
} = {}) {
  if (!candidate || !(candidate.id || candidate.opportunityId)) {
    return {
      action: PROMOTION_ACTION.SKIP,
      dryRun,
      error: "missing_opportunity_id",
      validation: { ok: false, failed: ["id"] },
    };
  }

  const validation = materialUpdateOnly
    ? { ok: true, failed: [], skippedForMaterialUpdate: true }
    : validateQualifiedCandidate(candidate);
  if (!validation.ok) {
    return {
      action: PROMOTION_ACTION.HOLD_WATCH,
      dryRun,
      validation,
      opportunity: candidate,
    };
  }

  const dup = forceUpdateId
    ? {
        match:
          existingOpps.find((o) => o.id === forceUpdateId) || {
            id: forceUpdateId,
          },
        reason: "forced",
      }
    : findCanonicalDuplicate(candidate, existingOpps);

  // Duplicate found and caller did not request an update → do not create second row
  if (dup?.match?.id && !forceUpdateId && !materialUpdateOnly) {
    return {
      action: PROMOTION_ACTION.DUPLICATE_EXISTING,
      dryRun,
      duplicate: dup,
      opportunity: dup.match,
      validation,
    };
  }

  const updateMatch =
    (forceUpdateId && existingOpps.find((o) => o.id === forceUpdateId)) ||
    (materialUpdateOnly && dup?.match) ||
    null;
  const isUpdate = Boolean(updateMatch || forceUpdateId);
  const promoRun = runId || `gdi_promote_${Date.now()}`;
  const provenance = buildPromotionProvenance({
    promotionRunId: promoRun,
    discoveryRunId,
    discoveryAt,
    targetId,
    targetRunId,
    method,
    playbook,
    source: source || candidate.officialSource || candidate.discoverySource,
    newnessSemantics: isUpdate
      ? NEWNESS_SEMANTICS.UPDATE_EXISTING
      : NEWNESS_SEMANTICS.NEW_TO_GDI_ON_PROMOTION,
  });

  let opportunity = {
    ...candidate,
    hotelId: candidate.hotelId || hotelId,
    id: candidate.id || candidate.opportunityId,
    opportunityId: candidate.id || candidate.opportunityId,
  };

  if (isUpdate) {
    const prior = updateMatch || dup?.match || { id: forceUpdateId };
    const preserveNew =
      candidate.weeklyDeltaState === "NEW" ||
      candidate.isNewThisWeek === true ||
      prior.weeklyDeltaState === "NEW";
    opportunity = {
      ...prior,
      ...candidate,
      id: prior.id,
      opportunityId: prior.id,
      hotelId: prior.hotelId || hotelId,
      firstDiscoveredAt: prior.firstDiscoveredAt || provenance.firstDiscoveredAt,
      firstDiscoveredRunId:
        prior.firstDiscoveredRunId || provenance.firstDiscoveredRunId,
      firstSeenRunId: prior.firstSeenRunId || prior.firstDiscoveredRunId,
      firstSeenAt: prior.firstSeenAt || prior.firstDiscoveredAt,
      // Contact-only / material updates must not invent NEW, but must not wipe existing NEW.
      weeklyDeltaState: preserveNew ? "NEW" : "UPDATED",
      isNewThisWeek: preserveNew ? true : false,
      newnessSemantics: preserveNew
        ? NEWNESS_SEMANTICS.NEW_TO_GDI_ON_PROMOTION
        : NEWNESS_SEMANTICS.UPDATE_EXISTING,
      lastMaterialChangeAt: provenance.lastMaterialChangeAt,
      lastMaterialChangeRunId: promoRun,
      discoveryMethod: method,
      discoveryPlaybook: playbook || prior.discoveryPlaybook,
      discoverySource: source || provenance.discoverySource,
      updatedAt: provenance.lastMaterialChangeAt,
    };
    const urls = new Set(
      (Array.isArray(prior.sources) ? prior.sources : [])
        .map((s) => s.url)
        .filter(Boolean)
    );
    const merged = [...(prior.sources || [])];
    for (const s of candidate.sources || []) {
      if (s?.url && !urls.has(s.url)) {
        urls.add(s.url);
        merged.push(s);
      }
    }
    opportunity.sources = merged;
  } else {
    opportunity = applyProvenanceToOpportunity(opportunity, provenance);
    opportunity.createdAt = opportunity.createdAt || provenance.firstSeenAt;
    opportunity.updatedAt = provenance.lastMaterialChangeAt;
  }

  // Canonical customer enrichment — same path as mature opportunities (no thin tier)
  opportunity = enrichGdiOpportunityForCustomer(opportunity, {
    hotelId: hotelId || opportunity.hotelId,
    enforceCustomerGate: true,
  });
  if (shouldHideAfterEnrichment(opportunity)) {
    opportunity.customerVisible = false;
  }

  // Global readiness: summary ADEQUATE+ and WHO research attempted
  const ready = isGdiCustomerOpportunityReady(opportunity, {
    nowDate: optsNowDate(),
  });
  opportunity.customerReadiness = ready;
  if (!ready.ok) {
    const intendWatch =
      String(opportunity.customerFacingState || "").toUpperCase() === "FUTURE_WATCH" ||
      String(opportunity.salesPartitionV11 || "").toUpperCase() === "FUTURE_WATCH" ||
      String(candidate.customerFacingState || "").toUpperCase() === "FUTURE_WATCH";
    const watchGate = isValidFutureWatch(opportunity, { nowDate: optsNowDate() });
    opportunity.watchGate = {
      ok: Boolean(watchGate.ok),
      class: watchGate.class || null,
      reasons: watchGate.reasons || [],
    };
    if (intendWatch && watchGate.ok && candidate.customerVisible !== false) {
      // Customer-visible Future Watch only when the real Watch gate passes.
      // HOLD_WATCH / research intent alone must NOT publish.
      opportunity.customerVisible = true;
      opportunity.customerFacingState = "FUTURE_WATCH";
      opportunity.salesPartitionV11 = "FUTURE_WATCH";
      opportunity.priority = opportunity.priority || "WATCHLIST";
      opportunity.customerReadinessHold = ready.state;
    } else if (intendWatch) {
      // Internal research watch — persist for pipeline, never customer-facing.
      opportunity.customerVisible = false;
      opportunity.customerFacingState = "FUTURE_WATCH";
      opportunity.salesPartitionV11 = "FUTURE_WATCH";
      opportunity.priority = opportunity.priority || "WATCHLIST";
      opportunity.customerReadinessHold = ready.state;
      opportunity.publicationHoldReason = watchGate.ok
        ? "explicit_customer_hide"
        : watchGate.class || "WATCH_GATE_FAILED";
    } else {
      opportunity.customerVisible = false;
      opportunity.customerReadinessHold = ready.state;
    }
    if (dryRun) {
      return {
        action: PROMOTION_ACTION.HOLD_WATCH,
        dryRun: true,
        opportunity,
        duplicate: dup,
        validation,
        customerReadiness: ready,
        watchGate: opportunity.watchGate,
        drawerReadiness: opportunity.drawerReadiness || null,
      };
    }
    // Still persist for research history / Future Watch (internal unless gate passed)
  }

  if (dryRun) {
    return {
      action: isUpdate ? PROMOTION_ACTION.UPDATE_EXISTING : PROMOTION_ACTION.PROMOTE_NEW,
      dryRun: true,
      opportunity,
      duplicate: dup,
      validation,
      drawerReadiness: opportunity.drawerReadiness || null,
      fieldsPreview: {
        id: opportunity.id,
        title: opportunity.title,
        weeklyDeltaState: opportunity.weeklyDeltaState,
        firstDiscoveredRunId: opportunity.firstDiscoveredRunId,
        firstSeenRunId: opportunity.firstSeenRunId,
        opportunityQualification: opportunity.opportunityQualification,
        segment: opportunity.segment,
        venueSourcingStatus: opportunity.venueSourcingStatus,
        customerVisible: opportunity.customerVisible,
      },
    };
  }

  const written = await upsertOpportunity(opportunity, {
    hotelId: hotelId || opportunity.hotelId,
    runId: promoRun,
  });
  invalidateGdiHotelReadCache(hotelId || opportunity.hotelId);

  let readback = null;
  try {
    readback = await findOpportunityRecordById(opportunity.id);
  } catch {
    readback = null;
  }

  return {
    action: isUpdate ? PROMOTION_ACTION.UPDATE_EXISTING : PROMOTION_ACTION.PROMOTE_NEW,
    dryRun: false,
    opportunity: written.opportunity || opportunity,
    recordId: written.recordId || readback?.id || null,
    duplicate: dup,
    validation,
    readbackOk: Boolean(readback),
  };
}

/**
 * Minimal qualification gate before write — does not lower TRUE bar;
 * requires future date, source URL, lodging thesis, geo, qualification.
 */
export function validateQualifiedCandidate(candidate = {}) {
  const failed = [];
  const id = candidate.id || candidate.opportunityId;
  if (!id) failed.push("id");
  if (!candidate.title && !candidate.opportunityName) failed.push("title");
  if (!candidate.hotelId) failed.push("hotelId");
  const q = candidate.opportunityQualification || candidate.qualification;
  if (!q || q === "CLOSED" || q === "WEAK") failed.push("qualification");
  const start = candidate.eventStartDate || candidate.timing;
  if (!start) failed.push("eventStartDate");
  else {
    const d = new Date(String(start).slice(0, 10));
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    if (Number.isFinite(d.getTime()) && d < today) failed.push("past_event");
  }
  const sources = Array.isArray(candidate.sources) ? candidate.sources : [];
  const hasSource =
    sources.some((s) => s?.url) ||
    candidate.officialSource ||
    candidate.discoverySource ||
    candidate.primarySourceUrl;
  if (!hasSource) failed.push("source");
  const lodging =
    candidate.roomDemandStatus ||
    candidate.lodgingEvidence ||
    candidate.housingStatus ||
    candidate.housingEvidence;
  if (!lodging) failed.push("lodging_evidence");
  return { ok: failed.length === 0, failed };
}

/** Re-export attach for callers that already use coverage provenance. */
export { attachDiscoveryProvenance };
