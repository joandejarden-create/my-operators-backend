/**
 * reuse_prior_intelligence — read existing GDI stores + Demand Generators
 * before any paid discovery. Emits a disposition ledger; never auto-qualifies.
 */

import fs from "node:fs";
import path from "node:path";
import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";
import { loadFeedback, hotelDir } from "../repository.js";
import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import { isCustomerFacingOpportunity } from "../customer-visibility.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { annotateWebhoundHistorical } from "./webhound-retired-v1.js";
import { buildPrivateEventIntelligence } from "./private-event-intelligence-v1.js";
import {
  selectResearchPaths,
  researchPathSelectionToCsv,
} from "./research-path-selection-v1.js";

export const REUSE_PRIOR_INTELLIGENCE_VERSION = "gdi-reuse-prior-intelligence-v1";

export const REUSE_DISPOSITION = Object.freeze({
  ALREADY_FACING: "already_facing",
  ENRICHMENT_NEEDED: "enrichment_needed",
  FIT_ASSESSMENT_NEEDED: "fit_assessment_needed",
  STALE_EVIDENCE: "stale_evidence",
  REJECTED_WITH_REASON: "rejected_with_reason",
  UNRESOLVED: "unresolved",
  DEMAND_GENERATOR_CANDIDATE: "demand_generator_candidate",
  FEEDBACK_DISPOSITION: "feedback_disposition",
  PRIVATE_EVENT_INTELLIGENCE: "private_event_intelligence",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function normalizeOrgIdentityKey(name = "") {
  return text(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function occasionIdentityKey(opp = {}) {
  const org = normalizeOrgIdentityKey(opp.organizationName || opp.company);
  const start = String(opp.eventStartDate || "").slice(0, 10);
  const title = text(opp.title || opp.opportunityName)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .slice(0, 80);
  return `${org}|${start || "undated"}|${title}`;
}

function sourceFreshness(opp = {}, nowIso = new Date().toISOString()) {
  const stamps = [
    opp.lastVerifiedAt,
    opp.lastResearchedAt,
    opp.updatedAt,
    opp.createdAt,
    opp.firstSeenAt,
  ]
    .map((s) => Date.parse(s))
    .filter((n) => Number.isFinite(n));
  if (!stamps.length) {
    return { freshestAt: null, ageDays: null, stale: true, reason: "no_timestamp" };
  }
  const freshest = Math.max(...stamps);
  const ageDays = Math.floor((Date.parse(nowIso) - freshest) / 86400000);
  const stale = ageDays > 180;
  return {
    freshestAt: new Date(freshest).toISOString(),
    ageDays,
    stale,
    reason: stale ? "older_than_180d" : "current",
  };
}

function contactStatus(opp = {}) {
  const email = text(opp.primaryContact?.email || opp.functionalContactEmail);
  const phone = text(opp.primaryContact?.phone);
  const name = text(opp.primaryContact?.name);
  const hasNamed = Boolean(name && /\s/.test(name));
  if (email || phone) {
    return {
      status: hasNamed ? "usable_named_or_functional" : "usable_route",
      hasEmail: Boolean(email),
      hasPhone: Boolean(phone),
      hasNamedPerson: hasNamed,
    };
  }
  return {
    status: "missing_route",
    hasEmail: false,
    hasPhone: false,
    hasNamedPerson: hasNamed,
  };
}

function loadShareValidationLocal(hotelId) {
  try {
    const p = path.join(hotelDir(hotelId), "share-validation.json");
    if (!fs.existsSync(p)) return { items: [] };
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return { items: [] };
  }
}

/**
 * Read-only Demand Generator fits for a hotel (Airtable when configured).
 * Degrades gracefully when AT is unavailable.
 */
export async function loadDemandGeneratorFitsForHotel(hotelId, opts = {}) {
  const id = text(hotelId);
  if (!id) return { ok: false, reason: "hotelId_required", fits: [] };
  if (opts.skipAirtable === true) {
    return { ok: true, reason: "skipped", fits: [], generators: [] };
  }
  try {
    const Airtable = (await import("airtable")).default;
    const { getGdiOpportunitiesAirtableBaseId } =
      await import("../airtable-opportunity-store.js");
    const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
    const baseId = getGdiOpportunitiesAirtableBaseId();
    if (!token || !baseId) {
      return { ok: false, reason: "airtable_not_configured", fits: [], generators: [] };
    }
    const base = new Airtable({ apiKey: token }).base(baseId);
    const fits = [];
    await base("Hotel Demand Generator Fit")
      .select({
        filterByFormula: `{Hotel ID} = "${id.replace(/"/g, '\\"')}"`,
        maxRecords: opts.maxRecords || 100,
      })
      .eachPage((records, next) => {
        for (const r of records) {
          const f = r.fields || {};
          fits.push({
            airtableRecordId: r.id,
            fitId: f["Hotel Generator Fit ID"] || null,
            demandGeneratorId: f["Demand Generator ID"] || null,
            organizationName: f["Organization Name"] || null,
            relevanceClass: f["Relevance Class"] || null,
            generatorPriority: f["Generator Priority"] || null,
            fitRationale: f["Fit Rationale"] || null,
            hotelId: f["Hotel ID"] || id,
            lastEvaluated: f["Last Evaluated"] || null,
          });
        }
        next();
      });
    return { ok: true, reason: "airtable", fits, generators: [] };
  } catch (err) {
    return {
      ok: false,
      reason: err?.message || "airtable_read_failed",
      fits: [],
      generators: [],
    };
  }
}

function classifyOpportunityRow(opp, hotelId, nowIso, feedbackByOpp, shareByOpp) {
  const annotated = annotateWebhoundHistorical(opp);
  const elig = evaluateProspectPublicationEligibility(annotated);
  const facing = isCustomerFacingOpportunity(annotated, { hotelId });
  const ready = isGdiCustomerOpportunityReady(annotated);
  const fresh = sourceFreshness(annotated, nowIso);
  const contact = contactStatus(annotated);
  const fb = feedbackByOpp.get(annotated.id) || null;
  const share = shareByOpp.get(annotated.id) || null;

  let disposition = REUSE_DISPOSITION.UNRESOLVED;
  let reason = "";

  if (annotated.priority === "DISQUALIFIED" || annotated.customerVisible === false) {
    disposition = REUSE_DISPOSITION.REJECTED_WITH_REASON;
    reason =
      annotated.cqDowngradeReason ||
      annotated.disqualificationReason ||
      "disqualified_or_not_customer_visible";
  } else if (facing) {
    disposition = REUSE_DISPOSITION.ALREADY_FACING;
    reason = ready.ok ? "ready_or_legacy_or_reviewed_facing" : "reviewed_or_legacy_facing";
  } else if (elig.enrichmentQueue === true) {
    disposition = REUSE_DISPOSITION.ENRICHMENT_NEEDED;
    reason = (elig.reasons || []).join(";") || "enrichment_queue";
  } else if (fresh.stale) {
    disposition = REUSE_DISPOSITION.STALE_EVIDENCE;
    reason = fresh.reason;
  } else if (!annotated.hotelOpportunityThesis && !annotated.fitExplanation) {
    disposition = REUSE_DISPOSITION.FIT_ASSESSMENT_NEEDED;
    reason = "missing_hotel_specific_thesis";
  } else {
    disposition = REUSE_DISPOSITION.UNRESOLVED;
    reason = (elig.reasons || []).join(";") || "not_facing_not_enrichment";
  }

  if (fb || share) {
    // Feedback does not override disposition class; surface separately.
  }

  return {
    sourceType: "opportunity",
    sourceRecordId: annotated.id,
    hotelId,
    organizationName: annotated.organizationName || annotated.company || null,
    organizationIdentityKey: normalizeOrgIdentityKey(
      annotated.organizationName || annotated.company
    ),
    occasionIdentityKey: occasionIdentityKey(annotated),
    eventStartDate: annotated.eventStartDate || null,
    title: annotated.title || null,
    disposition,
    reason,
    sourceFreshness: fresh,
    contactStatus: contact,
    eligibility: elig.eligibility || null,
    hotelFacingElig: elig.hotelFacing === true,
    enrichmentQueue: elig.enrichmentQueue === true,
    customerFacing: facing,
    ready: ready.ok === true,
    prospectPublicationBasis: annotated.prospectPublicationBasis || null,
    webhoundHistorical: annotated.webhoundUsed === true,
    feedback: fb,
    shareValidation: share
      ? {
          familiarityStatus: share.familiarityStatus || null,
          commercialValue: share.commercialValue || null,
        }
      : null,
    nextResearchNeed:
      disposition === REUSE_DISPOSITION.ENRICHMENT_NEEDED
        ? "targeted_contact_or_sourcing_research"
        : disposition === REUSE_DISPOSITION.FIT_ASSESSMENT_NEEDED
          ? "independent_hotel_fit"
          : disposition === REUSE_DISPOSITION.STALE_EVIDENCE
            ? "refresh_evidence_optional"
            : disposition === REUSE_DISPOSITION.ALREADY_FACING
              ? "none_reuse"
              : "human_review",
  };
}

/**
 * Build prior-intelligence ledger for a hotel (read-only).
 */
export async function buildReusePriorIntelligenceLedger(hotelId, opts = {}) {
  const id = text(hotelId);
  if (!id) throw new Error("hotelId_required");
  const nowIso = opts.nowIso || new Date().toISOString();

  const doc = await loadOpportunitiesCanonical(id);
  const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];

  const feedbackDoc = loadFeedback(id) || {};
  const feedbackItems = Array.isArray(feedbackDoc.items)
    ? feedbackDoc.items
    : Array.isArray(feedbackDoc)
      ? feedbackDoc
      : [];
  const feedbackByOpp = new Map();
  for (const it of feedbackItems) {
    const oid = it.opportunityId || it.subjectId || it.id;
    if (oid) feedbackByOpp.set(oid, it);
  }

  const shareDoc = loadShareValidationLocal(id);
  const shareByOpp = new Map();
  const shareItems = Array.isArray(shareDoc.items)
    ? shareDoc.items
    : shareDoc.items && typeof shareDoc.items === "object"
      ? Object.values(shareDoc.items)
      : [];
  for (const it of shareItems) {
    if (it?.opportunityId) shareByOpp.set(it.opportunityId, it);
  }

  const rows = ops.map((o) =>
    classifyOpportunityRow(o, id, nowIso, feedbackByOpp, shareByOpp)
  );

  const dg = await loadDemandGeneratorFitsForHotel(id, {
    skipAirtable: opts.skipAirtable === true,
    maxRecords: opts.maxDgRecords || 100,
  });

  const facingOrgs = new Set(
    rows
      .filter((r) => r.disposition === REUSE_DISPOSITION.ALREADY_FACING)
      .map((r) => r.organizationIdentityKey)
      .filter(Boolean)
  );

  for (const fit of dg.fits || []) {
    const orgKey = normalizeOrgIdentityKey(fit.organizationName);
    rows.push({
      sourceType: "demand_generator_fit",
      sourceRecordId: fit.fitId || fit.airtableRecordId,
      hotelId: id,
      organizationName: fit.organizationName,
      organizationIdentityKey: orgKey,
      occasionIdentityKey: `${orgKey}|undated|generator`,
      eventStartDate: null,
      title: null,
      disposition: facingOrgs.has(orgKey)
        ? REUSE_DISPOSITION.ALREADY_FACING
        : REUSE_DISPOSITION.DEMAND_GENERATOR_CANDIDATE,
      reason: facingOrgs.has(orgKey)
        ? "org_already_has_facing_opportunity"
        : "dg_fit_present_not_auto_qualified",
      sourceFreshness: {
        freshestAt: fit.lastEvaluated || null,
        ageDays: null,
        stale: false,
        reason: "dg_fit",
      },
      contactStatus: {
        status: "unknown",
        hasEmail: false,
        hasPhone: false,
        hasNamedPerson: false,
      },
      eligibility: null,
      hotelFacingElig: false,
      enrichmentQueue: false,
      customerFacing: false,
      ready: false,
      prospectPublicationBasis: null,
      webhoundHistorical: false,
      feedback: null,
      shareValidation: null,
      relevanceClass: fit.relevanceClass || null,
      generatorPriority: fit.generatorPriority || null,
      demandGeneratorId: fit.demandGeneratorId || null,
      nextResearchNeed: facingOrgs.has(orgKey)
        ? "none_reuse"
        : "human_occasion_discovery_or_fit_confirm",
      note: "Demand Generator fit is not automatic qualification for a dated occasion",
    });
  }

  // Private Event Venues / Hotel Venue Fit / Private Event Signals
  const pe = await buildPrivateEventIntelligence(id, {
    skipAirtable: opts.skipAirtable === true,
  });
  for (const peRow of pe.rows || []) {
    rows.push({
      sourceType: peRow.sourceType,
      sourceRecordId: peRow.sourceRecordId,
      hotelId: id,
      organizationName: peRow.organizationName,
      organizationIdentityKey: peRow.organizationIdentityKey,
      occasionIdentityKey: peRow.occasionIdentityKey || null,
      eventStartDate: null,
      title: peRow.eventName || null,
      disposition: REUSE_DISPOSITION.PRIVATE_EVENT_INTELLIGENCE,
      peDisposition: peRow.disposition,
      reason: peRow.rationale,
      sourceFreshness: {
        freshestAt: null,
        ageDays: null,
        stale: false,
        reason: "pe_graph",
      },
      contactStatus: { status: "unknown", hasEmail: false, hasPhone: false, hasNamedPerson: false },
      eligibility: peRow.eligibility || null,
      hotelFacingElig: false,
      enrichmentQueue: false,
      customerFacing: false,
      ready: false,
      prospectPublicationBasis: null,
      webhoundHistorical: false,
      feedback: null,
      shareValidation: null,
      peVenueId: peRow.peVenueId,
      hotelVenueFitId: peRow.hotelVenueFitId,
      peSignalId: peRow.peSignalId,
      preserveIds: peRow.preserveIds,
      nextResearchNeed:
        peRow.disposition === "venue_referral_partnership_account"
          ? "human_partnership_shortlist_or_contact"
          : peRow.disposition === "specific_event_related_lodging_prospect"
            ? "evidence_travel_cohort_and_procurement"
            : peRow.disposition === "background_market_intelligence_only" ||
                peRow.disposition === "rejected_insufficient_travel_or_procurement"
              ? "none_internal_keep"
              : "human_review",
      note: "PE venue/signal is not automatic customer-facing qualification",
    });
  }

  const counts = {};
  for (const d of Object.values(REUSE_DISPOSITION)) counts[d] = 0;
  for (const r of rows) counts[r.disposition] = (counts[r.disposition] || 0) + 1;

  const pathSelection = selectResearchPaths(id, {
    runDiscovery: opts.runDiscovery === true,
    discoveryMode: opts.discoveryMode || null,
    evidenceHints: {
      opportunityCount: ops.length,
      dgFitCount: (dg.fits || []).length,
      dgCount: (dg.fits || []).length,
      programCount: 0,
      dgSignalCount: 0,
      peVenueCount: pe.counts?.venues || 0,
      peFitCount: pe.counts?.fits || 0,
      peSignalCount: pe.counts?.signals || 0,
      peFixtureUsed: String(pe.source || "").includes("fixture"),
      airtableBlocked: dg.ok === false || pe.airtableOk === false,
      airtableBlockReason: dg.reason || pe.airtableReason || null,
      feedbackCount: feedbackItems.length,
      contactTargets: counts[REUSE_DISPOSITION.ENRICHMENT_NEEDED] || 0,
    },
  });

  return {
    version: REUSE_PRIOR_INTELLIGENCE_VERSION,
    hotelId: id,
    asOf: nowIso,
    opportunityCount: ops.length,
    demandGeneratorFit: {
      ok: dg.ok,
      reason: dg.reason,
      count: (dg.fits || []).length,
    },
    privateEventIntelligence: {
      source: pe.source,
      airtableOk: pe.airtableOk,
      airtableReason: pe.airtableReason,
      counts: pe.counts,
    },
    researchPathSelection: pathSelection,
    feedbackItemCount: feedbackItems.length,
    shareValidationItemCount: shareItems.length,
    counts,
    rows,
    gapsBeforePaidDiscovery: {
      enrichmentNeeded: counts[REUSE_DISPOSITION.ENRICHMENT_NEEDED] || 0,
      fitAssessmentNeeded: counts[REUSE_DISPOSITION.FIT_ASSESSMENT_NEEDED] || 0,
      dgCandidatesNotFacing: counts[REUSE_DISPOSITION.DEMAND_GENERATOR_CANDIDATE] || 0,
      privateEventRows: counts[REUSE_DISPOSITION.PRIVATE_EVENT_INTELLIGENCE] || 0,
      unresolved: counts[REUSE_DISPOSITION.UNRESOLVED] || 0,
      stale: counts[REUSE_DISPOSITION.STALE_EVIDENCE] || 0,
    },
    notes: [
      "A source record or DG fit is not automatic qualification.",
      "A rejected occasion does not exclude the organization for all future occasions.",
      "Webhound is retired as a live provider; historical evidence is preserved.",
      "Private Event Venues / Hotel Venue Fit / Private Event Signals are first-class reuse paths.",
      "Nearby venue event ≠ traveling guests or open hotel procurement.",
    ],
  };
}

export { researchPathSelectionToCsv };

export function reuseLedgerToCsv(ledger) {
  const header = [
    "hotelId",
    "sourceType",
    "sourceRecordId",
    "organizationName",
    "organizationIdentityKey",
    "occasionIdentityKey",
    "eventStartDate",
    "disposition",
    "reason",
    "nextResearchNeed",
    "contactStatus",
    "eligibility",
    "customerFacing",
    "freshestAt",
    "demandGeneratorId",
  ];
  const lines = [header.join(",")];
  const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
  for (const r of ledger.rows || []) {
    lines.push(
      [
        r.hotelId,
        r.sourceType,
        r.sourceRecordId,
        esc(r.organizationName),
        r.organizationIdentityKey,
        esc(r.occasionIdentityKey),
        r.eventStartDate || "",
        r.disposition,
        esc(r.reason),
        r.nextResearchNeed,
        r.contactStatus?.status || "",
        r.eligibility || "",
        r.customerFacing,
        r.sourceFreshness?.freshestAt || "",
        r.demandGeneratorId || "",
      ].join(",")
    );
  }
  return lines.join("\n") + "\n";
}
