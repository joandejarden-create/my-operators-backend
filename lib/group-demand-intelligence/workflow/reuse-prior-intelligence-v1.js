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
 * Distinguishes empty_success vs not_configured vs failed. Never substitutes fixtures.
 */
export async function loadDemandGeneratorFitsForHotel(hotelId, opts = {}) {
  const id = text(hotelId);
  if (!id) return { ok: false, reason: "hotelId_required", fits: [], status: "failed" };
  if (opts.skipAirtable === true) {
    return {
      ok: true,
      reason: "skipped",
      status: "skipped",
      fits: [],
      generators: [],
    };
  }
  try {
    const Airtable = (await import("airtable")).default;
    const { getGdiOpportunitiesAirtableBaseId } =
      await import("../airtable-opportunity-store.js");
    const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
    const baseId = getGdiOpportunitiesAirtableBaseId();
    if (!token || !baseId) {
      return {
        ok: false,
        reason: "airtable_not_configured",
        status: "failed",
        fits: [],
        generators: [],
      };
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
    return {
      ok: true,
      reason: fits.length ? "airtable" : "empty_success_zero_records",
      status: fits.length ? "executed" : "empty_success",
      fits,
      generators: [],
    };
  } catch (err) {
    return {
      ok: false,
      reason: err?.message || "airtable_read_failed",
      status: "failed",
      fits: [],
      generators: [],
    };
  }
}

/**
 * Load Demand Programs into the reuse ledger (universe sample when hotel-filter unavailable).
 * Does not silently fall back to fixtures.
 */
export async function loadDemandProgramsForReuse(hotelId, opts = {}) {
  const id = text(hotelId);
  if (!id) return { ok: false, reason: "hotelId_required", status: "failed", programs: [] };
  if (opts.skipAirtable === true) {
    return { ok: true, reason: "skipped", status: "skipped", programs: [] };
  }
  try {
    const Airtable = (await import("airtable")).default;
    const { getGdiOpportunitiesAirtableBaseId } =
      await import("../airtable-opportunity-store.js");
    const {
      DEMAND_PROGRAMS_TABLE_NAME,
    } = await import("../demand-generators/constants.js");
    const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
    const baseId = getGdiOpportunitiesAirtableBaseId();
    if (!token || !baseId) {
      return {
        ok: false,
        reason: "airtable_not_configured",
        status: "failed",
        programs: [],
      };
    }
    const base = new Airtable({ apiKey: token }).base(baseId);
    const programs = [];
    const generatorIds = new Set(
      (opts.generatorIds || []).map((g) => text(g)).filter(Boolean)
    );
    await base(DEMAND_PROGRAMS_TABLE_NAME)
      .select({ maxRecords: opts.maxRecords || 80 })
      .eachPage((records, next) => {
        for (const r of records) {
          const f = r.fields || {};
          const dgId = f["Demand Generator ID"] || null;
          if (generatorIds.size && dgId && !generatorIds.has(String(dgId))) {
            continue;
          }
          programs.push({
            airtableRecordId: r.id,
            programId: f["Program ID"] || null,
            demandGeneratorId: dgId,
            programName: f["Program Name"] || null,
            programType: f["Program Type"] || null,
            market: f["Market / Geography"] || null,
            recurrenceStatus: f["Recurrence Status"] || null,
            typicalTiming: f["Typical Timing"] || null,
            lastVerified: f["Last Verified"] || null,
          });
        }
        next();
      });
    return {
      ok: true,
      reason: programs.length
        ? generatorIds.size
          ? "airtable_filtered_by_fitted_generators"
          : "airtable_universe_sample"
        : "empty_success_zero_records",
      status: programs.length ? "executed" : "empty_success",
      programs,
    };
  } catch (err) {
    return {
      ok: false,
      reason: err?.message || "airtable_read_failed",
      status: "failed",
      programs: [],
    };
  }
}

/**
 * Load Demand Generator Signals into the reuse ledger.
 */
export async function loadDemandGeneratorSignalsForReuse(hotelId, opts = {}) {
  const id = text(hotelId);
  if (!id) return { ok: false, reason: "hotelId_required", status: "failed", signals: [] };
  if (opts.skipAirtable === true) {
    return { ok: true, reason: "skipped", status: "skipped", signals: [] };
  }
  try {
    const Airtable = (await import("airtable")).default;
    const { getGdiOpportunitiesAirtableBaseId } =
      await import("../airtable-opportunity-store.js");
    const {
      DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
    } = await import("../demand-generators/constants.js");
    const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
    const baseId = getGdiOpportunitiesAirtableBaseId();
    if (!token || !baseId) {
      return {
        ok: false,
        reason: "airtable_not_configured",
        status: "failed",
        signals: [],
      };
    }
    const base = new Airtable({ apiKey: token }).base(baseId);
    const signals = [];
    const generatorIds = new Set(
      (opts.generatorIds || []).map((g) => text(g)).filter(Boolean)
    );
    await base(DEMAND_GENERATOR_SIGNALS_TABLE_NAME)
      .select({ maxRecords: opts.maxRecords || 80 })
      .eachPage((records, next) => {
        for (const r of records) {
          const f = r.fields || {};
          const dgId = f["Demand Generator ID"] || null;
          const signalHotel = f["Hotel ID"] || null;
          // When hotel has fitted generators: keep hotel-scoped + fitted-gen signals.
          // When no fits: keep universe sample (do not discard as empty merely because
          // signals are tagged to other hotels — empty fit ≠ path irrelevance).
          if (generatorIds.size > 0) {
            const hotelScoped = signalHotel === id;
            const fittedGen = dgId && generatorIds.has(String(dgId));
            if (!hotelScoped && !fittedGen) continue;
          }
          signals.push({
            airtableRecordId: r.id,
            signalId: f["Demand Generator Signal ID"] || null,
            demandGeneratorId: dgId,
            programId: f["Program ID"] || null,
            hotelId: signalHotel || null,
            title: f["Title"] || null,
            eventStartDate: f["Event Start Date"] || null,
            eventEndDate: f["Event End Date"] || null,
            market: f["Market"] || null,
            hotelDemandThesis: f["Hotel Demand Thesis"] || null,
            signalStatus: f["Signal Status"] || null,
            recommendedAction: f["Recommended Action"] || null,
            sourceUrl: f["Source URL"] || null,
            gdiOpportunityId: f["GDI Opportunity ID"] || null,
          });
        }
        next();
      });
    return {
      ok: true,
      reason: signals.length
        ? generatorIds.size
          ? "airtable_filtered_by_hotel_or_fitted_generators"
          : "airtable_universe_sample"
        : "empty_success_zero_records",
      status: signals.length ? "executed" : "empty_success",
      signals,
    };
  } catch (err) {
    return {
      ok: false,
      reason: err?.message || "airtable_read_failed",
      status: "failed",
      signals: [],
    };
  }
}

/**
 * Map operator feedback / share-validation into nextResearchNeed + discovery hints.
 */
export function resolveNextResearchNeedFromFeedback({
  baseNeed,
  feedback = null,
  share = null,
  organizationName = null,
} = {}) {
  const fam =
    share?.familiarityStatus ||
    feedback?.familiarityStatus ||
    feedback?.familiarity ||
    null;
  const commercial =
    share?.commercialValue ||
    feedback?.commercialValue ||
    feedback?.valueAssessment ||
    null;
  const org = text(organizationName) || "organization";

  if (fam === "NOT_RELEVANT" || commercial === "NOT_WORTH_PURSUING") {
    return {
      nextResearchNeed: "stop_org_occasion",
      researchPriority: "suppress",
      discoveryQueryHint: null,
      feedbackDriver: fam || commercial,
    };
  }
  if (fam === "BOOKED_WON" || fam === "ALREADY_KNOWN") {
    return {
      nextResearchNeed: "none_reuse_known",
      researchPriority: "defer",
      discoveryQueryHint: null,
      feedbackDriver: fam,
    };
  }
  if (fam === "ACTIVELY_PURSUING" || commercial === "WORTH_PURSUING_NOW") {
    return {
      nextResearchNeed: "prioritize_targeted_contact",
      researchPriority: "high",
      discoveryQueryHint: `${org} meeting planner OR housing coordinator OR group sales contact`,
      feedbackDriver: fam || commercial,
    };
  }
  if (commercial === "WORTH_WATCHING" || fam === "PREVIOUSLY_PURSUED_LOST") {
    return {
      nextResearchNeed: "refresh_timing_and_procurement",
      researchPriority: "medium",
      discoveryQueryHint: `${org} 2026 OR 2027 conference lodging OR room block OR RFP`,
      feedbackDriver: fam || commercial,
    };
  }
  if (fam === "NEVER_SEEN_BEFORE") {
    return {
      nextResearchNeed: "independent_hotel_fit_then_contact",
      researchPriority: "medium",
      discoveryQueryHint: `${org} annual meeting OR conference hotel`,
      feedbackDriver: fam,
    };
  }
  return {
    nextResearchNeed: baseNeed,
    researchPriority: "inherit",
    discoveryQueryHint: null,
    feedbackDriver: null,
  };
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

  const baseNeed =
    disposition === REUSE_DISPOSITION.ENRICHMENT_NEEDED
      ? "targeted_contact_or_sourcing_research"
      : disposition === REUSE_DISPOSITION.FIT_ASSESSMENT_NEEDED
        ? "independent_hotel_fit"
        : disposition === REUSE_DISPOSITION.STALE_EVIDENCE
          ? "refresh_evidence_optional"
          : disposition === REUSE_DISPOSITION.ALREADY_FACING
            ? "none_reuse"
            : "human_review";

  const feedbackResolved = resolveNextResearchNeedFromFeedback({
    baseNeed,
    feedback: fb,
    share,
    organizationName: annotated.organizationName || annotated.company,
  });

  // Feedback can suppress or escalate research; does not invent a facing disposition.
  if (
    feedbackResolved.feedbackDriver &&
    feedbackResolved.nextResearchNeed === "stop_org_occasion" &&
    disposition !== REUSE_DISPOSITION.ALREADY_FACING
  ) {
    disposition = REUSE_DISPOSITION.FEEDBACK_DISPOSITION;
    reason = `feedback_${feedbackResolved.feedbackDriver}`;
  } else if (
    feedbackResolved.feedbackDriver &&
    disposition === REUSE_DISPOSITION.UNRESOLVED
  ) {
    disposition = REUSE_DISPOSITION.FEEDBACK_DISPOSITION;
    reason = `feedback_${feedbackResolved.feedbackDriver}`;
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
    nextResearchNeed: feedbackResolved.nextResearchNeed,
    researchPriority: feedbackResolved.researchPriority,
    discoveryQueryHint: feedbackResolved.discoveryQueryHint,
    feedbackDriver: feedbackResolved.feedbackDriver,
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

  const fittedGeneratorIds = [
    ...new Set(
      (dg.fits || [])
        .map((f) => f.demandGeneratorId)
        .filter(Boolean)
        .map(String)
    ),
  ];

  const programsDoc = await loadDemandProgramsForReuse(id, {
    skipAirtable: opts.skipAirtable === true,
    maxRecords: opts.maxProgramRecords || 80,
    generatorIds: fittedGeneratorIds,
  });
  const signalsDoc = await loadDemandGeneratorSignalsForReuse(id, {
    skipAirtable: opts.skipAirtable === true,
    maxRecords: opts.maxSignalRecords || 80,
    generatorIds: fittedGeneratorIds,
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

  for (const prog of programsDoc.programs || []) {
    const orgKey = normalizeOrgIdentityKey(prog.programName || prog.demandGeneratorId);
    rows.push({
      sourceType: "demand_program",
      sourceRecordId: prog.programId || prog.airtableRecordId,
      hotelId: id,
      organizationName: prog.programName,
      organizationIdentityKey: orgKey,
      occasionIdentityKey: `${orgKey}|undated|program`,
      eventStartDate: null,
      title: prog.programName,
      disposition: REUSE_DISPOSITION.DEMAND_GENERATOR_CANDIDATE,
      reason: "demand_program_loaded_not_auto_qualified",
      sourceFreshness: {
        freshestAt: prog.lastVerified || null,
        ageDays: null,
        stale: false,
        reason: "demand_program",
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
      demandGeneratorId: prog.demandGeneratorId || null,
      programId: prog.programId || null,
      programType: prog.programType || null,
      nextResearchNeed: "confirm_dated_cycle_or_hotel_fit",
      note: "Demand Program is reuse evidence, not automatic qualification",
    });
  }

  for (const sig of signalsDoc.signals || []) {
    const orgKey = normalizeOrgIdentityKey(sig.title || sig.demandGeneratorId);
    const alreadyFacingOpp = Boolean(sig.gdiOpportunityId);
    rows.push({
      sourceType: "demand_generator_signal",
      sourceRecordId: sig.signalId || sig.airtableRecordId,
      hotelId: id,
      organizationName: sig.title,
      organizationIdentityKey: orgKey,
      occasionIdentityKey: `${orgKey}|${String(sig.eventStartDate || "").slice(0, 10) || "undated"}|signal`,
      eventStartDate: sig.eventStartDate || null,
      title: sig.title,
      disposition: alreadyFacingOpp
        ? REUSE_DISPOSITION.ALREADY_FACING
        : REUSE_DISPOSITION.DEMAND_GENERATOR_CANDIDATE,
      reason: alreadyFacingOpp
        ? "signal_already_linked_gdi_opportunity"
        : "dg_signal_loaded_not_auto_qualified",
      sourceFreshness: {
        freshestAt: sig.eventStartDate || null,
        ageDays: null,
        stale: false,
        reason: "demand_generator_signal",
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
      demandGeneratorId: sig.demandGeneratorId || null,
      programId: sig.programId || null,
      signalStatus: sig.signalStatus || null,
      nextResearchNeed: alreadyFacingOpp
        ? "none_reuse"
        : "evidence_and_independent_hotel_fit",
      note: "Demand Generator Signal is reuse evidence, not automatic qualification",
    });
  }

  // Private Event Venues / Hotel Venue Fit / Private Event Signals
  const pe = await buildPrivateEventIntelligence(id, {
    skipAirtable: opts.skipAirtable === true,
    // undefined → PE module default: fixture only when live AT unavailable (never as live success)
    // true → explicit empty-live fixture enrich; false → never fixture
    allowFixtureFallback: opts.allowPeFixtureFallback,
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

  const feedbackDrivenHints = [
    ...new Set(
      rows
        .map((r) => r.discoveryQueryHint)
        .filter(Boolean)
    ),
  ].slice(0, 12);

  const airtableBlocked =
    dg.status === "failed" ||
    programsDoc.status === "failed" ||
    signalsDoc.status === "failed" ||
    (pe.liveAirtable?.attempted === true && pe.liveAirtable?.ok === false) ||
    (pe.airtableOk === false && pe.fixtureFallback?.used !== true);

  const pathSelection = selectResearchPaths(id, {
    runDiscovery: opts.runDiscovery === true,
    discoveryMode: opts.discoveryMode || null,
    evidenceHints: {
      opportunityCount: ops.length,
      dgFitCount: (dg.fits || []).length,
      dgCount: (dg.fits || []).length,
      programCount: (programsDoc.programs || []).length,
      dgSignalCount: (signalsDoc.signals || []).length,
      peVenueCount: pe.counts?.venues || 0,
      peFitCount: pe.counts?.fits || 0,
      peSignalCount: pe.counts?.signals || 0,
      peFixtureUsed: pe.fixtureFallback?.used === true,
      peLiveEmptySuccess:
        pe.liveAirtable?.ok === true &&
        pe.liveAirtable?.reason === "empty_success_zero_records",
      airtableBlocked,
      airtableBlockReason:
        dg.status === "failed"
          ? dg.reason
          : programsDoc.status === "failed"
            ? programsDoc.reason
            : signalsDoc.status === "failed"
              ? signalsDoc.reason
              : pe.liveAirtable?.ok === false
                ? pe.liveAirtable?.reason || pe.airtableReason
                : null,
      feedbackCount: feedbackItems.length + shareItems.length,
      feedbackDrivenHintCount: feedbackDrivenHints.length,
      contactTargets: counts[REUSE_DISPOSITION.ENRICHMENT_NEEDED] || 0,
      contactExecuted: opts.contactExecuted === true,
      nativePlannedOrExecuted: opts.nativePlannedOrExecuted === true,
      nativeMode: opts.nativeMode || null,
      geographicPlanned: opts.geographicPlanned === true,
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
      status: dg.status,
      count: (dg.fits || []).length,
    },
    demandPrograms: {
      ok: programsDoc.ok,
      reason: programsDoc.reason,
      status: programsDoc.status,
      count: (programsDoc.programs || []).length,
    },
    demandGeneratorSignals: {
      ok: signalsDoc.ok,
      reason: signalsDoc.reason,
      status: signalsDoc.status,
      count: (signalsDoc.signals || []).length,
    },
    privateEventIntelligence: {
      source: pe.source,
      airtableOk: pe.airtableOk,
      airtableReason: pe.airtableReason,
      liveAirtable: pe.liveAirtable || null,
      fixtureFallback: pe.fixtureFallback || null,
      counts: pe.counts,
    },
    researchPathSelection: pathSelection,
    feedbackItemCount: feedbackItems.length,
    shareValidationItemCount: shareItems.length,
    feedbackDrivenResearch: {
      hintCount: feedbackDrivenHints.length,
      discoveryQueryHints: feedbackDrivenHints,
      note: "Feedback/share-validation drives nextResearchNeed and optional discovery hints; does not auto-qualify",
    },
    counts,
    rows,
    gapsBeforePaidDiscovery: {
      enrichmentNeeded: counts[REUSE_DISPOSITION.ENRICHMENT_NEEDED] || 0,
      fitAssessmentNeeded: counts[REUSE_DISPOSITION.FIT_ASSESSMENT_NEEDED] || 0,
      dgCandidatesNotFacing: counts[REUSE_DISPOSITION.DEMAND_GENERATOR_CANDIDATE] || 0,
      privateEventRows: counts[REUSE_DISPOSITION.PRIVATE_EVENT_INTELLIGENCE] || 0,
      unresolved: counts[REUSE_DISPOSITION.UNRESOLVED] || 0,
      stale: counts[REUSE_DISPOSITION.STALE_EVIDENCE] || 0,
      feedbackDisposition: counts[REUSE_DISPOSITION.FEEDBACK_DISPOSITION] || 0,
    },
    notes: [
      "A source record or DG fit is not automatic qualification.",
      "A rejected occasion does not exclude the organization for all future occasions.",
      "Webhound is retired as a live provider; historical evidence is preserved.",
      "Private Event Venues / Hotel Venue Fit / Private Event Signals are first-class reuse paths.",
      "Demand Programs and Demand Generator Signals are loaded into the reuse ledger when Airtable is configured.",
      "Empty Airtable reads are empty_success, not failures, and fixtures are never reported as live success.",
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
