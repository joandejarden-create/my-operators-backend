/**
 * GDI Slice 2B — Demand Generator / Partner / Program / Signal relationship wiring.
 *
 * Offline ($0) contract: plan + stamp stable identities onto opportunities,
 * upsert into a memory DG graph (CREATE/UPDATE/UNCHANGED/DUPLICATE_LINKED).
 * No new Airtable tables. No production writes.
 */

import {
  buildDemandGeneratorEntity,
  buildProgramEntity,
  computeDemandGeneratorId,
  domainFromUrl,
} from "../demand-generators/entities.js";
import {
  buildHotelDemandGeneratorFit,
  computeHotelGeneratorFitId,
} from "../demand-generators/fit-relevance.js";
import {
  buildDemandGeneratorSignal,
  computeSignalId,
} from "../demand-generators/qualify-promote.js";
import {
  stripHotelPrivateFields,
  assertNoHotelPrivateLeak,
} from "../global-intelligence/gdi-global-intelligence-privacy-v1.js";
import { stripHotelPrivateKeys } from "./gdi-commercial-payload-durability-v1.js";
import {
  applySharedDemandIdentityToOpportunity,
  assertCrossHotelSharedIdentityIsolation,
  extractSharedDemandIdentity,
  mayUpsertDemandProgram,
} from "./gdi-shared-demand-identity-v1.js";
import { WRITE_OUTCOME } from "./gdi-full-e2e-airtable-persistence-contract-v1.js";
import { createMemoryDemandGeneratorGraphStore } from "./gdi-demand-generator-memory-store-v1.js";

export const GDI_SLICE2B_RELATIONSHIP_WIRING_V1 =
  "gdi-slice2b-relationship-wiring-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function firstSourceUrl(opp = {}) {
  if (typeof opp.officialSource === "string" && /^https?:\/\//i.test(opp.officialSource)) {
    return opp.officialSource;
  }
  if (Array.isArray(opp.sources)) {
    for (const s of opp.sources) {
      if (typeof s === "string" && /^https?:\/\//i.test(s)) return s;
      if (s?.url && /^https?:\/\//i.test(s.url)) return s.url;
    }
  }
  if (typeof opp.sourceSummary === "string" && /^https?:\/\//i.test(opp.sourceSummary)) {
    return opp.sourceSummary;
  }
  return null;
}

function collectSourceUrls(opp = {}) {
  const out = [];
  const push = (u) => {
    const t = text(u);
    if (t && /^https?:\/\//i.test(t) && !out.includes(t)) out.push(t);
  };
  push(opp.officialSource);
  if (Array.isArray(opp.sources)) {
    for (const s of opp.sources) push(typeof s === "string" ? s : s?.url);
  }
  if (typeof opp.sourceSummary === "string") {
    for (const m of opp.sourceSummary.match(/https?:\/\/[^\s|,;]+/gi) || []) {
      push(m);
    }
  }
  return out;
}

/**
 * Extract primary org + optional partner/planner intermediary candidates.
 */
export function extractRelationshipCandidates(opp = {}) {
  const primaryName =
    text(opp.organizationName) ||
    text(opp.primaryContact?.organization) ||
    null;
  const partnerName =
    text(opp.partnerOrganizationName) ||
    text(opp.plannerCompany) ||
    text(opp.intermediaryOrganizationName) ||
    null;

  const candidates = [];
  if (primaryName) {
    candidates.push({
      role: "PRIMARY_ORGANIZATION",
      organizationName: primaryName,
      website: text(opp.organizationWebsite) || firstSourceUrl(opp),
      organizationType: opp.organizationType || null,
    });
  }
  if (
    partnerName &&
    primaryName &&
    partnerName.toLowerCase() !== primaryName.toLowerCase()
  ) {
    candidates.push({
      role: "PARTNER_OR_PLANNER",
      organizationName: partnerName,
      website: text(opp.partnerWebsite) || null,
      organizationType: opp.partnerOrganizationType || "OTHER",
    });
  } else if (partnerName && !primaryName) {
    candidates.push({
      role: "PARTNER_OR_PLANNER",
      organizationName: partnerName,
      website: text(opp.partnerWebsite) || null,
      organizationType: opp.partnerOrganizationType || "OTHER",
    });
  }
  return candidates;
}

export function ensureDemandGeneratorIdOnOpportunity(opp = {}) {
  const existing = text(opp.dgGeneratorId || opp.demandGeneratorId);
  if (existing) {
    return {
      ...opp,
      dgGeneratorId: existing,
      demandGeneratorId: existing,
    };
  }
  const name = text(opp.organizationName);
  if (!name) return { ...opp };
  const website = text(opp.organizationWebsite) || firstSourceUrl(opp);
  const id = computeDemandGeneratorId({
    organizationName: name,
    website,
    officialDomain: domainFromUrl(website),
  });
  return {
    ...opp,
    dgGeneratorId: id,
    demandGeneratorId: id,
  };
}

function stripEntity(entity) {
  const cleaned = stripHotelPrivateFields(stripHotelPrivateKeys(entity || {}));
  if (cleaned.payload) {
    cleaned.payload = stripHotelPrivateFields(
      stripHotelPrivateKeys(cleaned.payload),
    );
  }
  return cleaned;
}

/**
 * Build dry-run relationship plan for one opportunity (no Airtable writes).
 */
export function planOpportunityRelationshipGraph(opp = {}, opts = {}) {
  const hotelId = text(opp.hotelId || opts.hotelId);
  const stamped = ensureDemandGeneratorIdOnOpportunity(opp);
  const candidates = extractRelationshipCandidates(stamped);
  const sourceUrls = collectSourceUrls(stamped);
  const plans = [];
  const deferred = [];

  let primaryDgId = text(stamped.dgGeneratorId) || null;
  let partnerDgId = null;

  for (const c of candidates) {
    const website = c.website || firstSourceUrl(stamped);
    const entity = stripEntity(
      buildDemandGeneratorEntity({
        organizationName: c.organizationName,
        website,
        officialDomain: domainFromUrl(website),
        organizationType: c.organizationType,
        sourceUrls,
        // Shared DG payload must stay opportunity-agnostic so cross-hotel
        // reuse classifies UNCHANGED / DUPLICATE_LINKED (not hotel-row UPDATED).
        payload: {
          role: c.role,
          marketOpportunityId: stamped.marketOpportunityId || null,
          // Intentionally plant-strip confidential if present on opp
          hotelSalesFeedback: stamped.hotelSalesFeedback,
          salesOwner: stamped.salesOwner,
        },
      }),
    );
    const privacy = assertNoHotelPrivateLeak(entity);
    plans.push({
      kind: "DEMAND_GENERATOR",
      role: c.role,
      identityKey: entity.demandGeneratorId,
      entity,
      privacyOk: privacy.ok,
      privacyViolations: privacy.violations,
      table: "Demand Generators",
    });
    if (c.role === "PRIMARY_ORGANIZATION") primaryDgId = entity.demandGeneratorId;
    if (c.role === "PARTNER_OR_PLANNER") partnerDgId = entity.demandGeneratorId;
  }

  // Program — only with recurrence evidence
  const programGate = mayUpsertDemandProgram({
    recurrenceStatus: stamped.recurrenceStatus || opts.recurrenceStatus,
    evidenceConfirmed: Boolean(
      stamped.recurrenceEvidenceConfirmed ?? opts.recurrenceEvidenceConfirmed,
    ),
    onboardingMirror: stamped.onboardingMirror === true,
  });
  let programId = text(stamped.dgProgramId) || null;
  if (primaryDgId && programGate.allowed) {
    const programName =
      text(stamped.programName) ||
      text(stamped.eventName) ||
      text(stamped.title) ||
      "Program";
    const program = stripEntity(
      buildProgramEntity({
        demandGeneratorId: primaryDgId,
        programName,
        programType: stamped.programType,
        recurrenceStatus: stamped.recurrenceStatus || opts.recurrenceStatus,
        sourceUrls,
        market: stamped.market || stamped.metro || null,
        inventFromHistoryOnly: false,
      }),
    );
    programId = program.programId;
    plans.push({
      kind: "DEMAND_PROGRAM",
      identityKey: program.programId,
      entity: program,
      privacyOk: assertNoHotelPrivateLeak(program).ok,
      table: "Demand Programs",
      gate: programGate,
    });
  } else if (primaryDgId) {
    deferred.push({
      kind: "DEMAND_PROGRAM",
      outcome: WRITE_OUTCOME.DEFERRED,
      reason: programGate.reason,
    });
  }

  // Hotel fit — hotel-scoped
  let fitId = text(stamped.hotelGeneratorFitId) || null;
  if (primaryDgId && hotelId) {
    const fit = stripEntity(
      buildHotelDemandGeneratorFit({
        hotelId,
        hotelName: stamped.hotelName || opts.hotelName,
        demandGeneratorId: primaryDgId,
        organizationName: stamped.organizationName,
        factors: stamped.generatorFitFactors || opts.generatorFitFactors || {},
      }),
    );
    fitId = fit.fitId;
    plans.push({
      kind: "HOTEL_DEMAND_GENERATOR_FIT",
      identityKey: fit.fitId,
      entity: fit,
      privacyOk: assertNoHotelPrivateLeak(fit).ok,
      table: "Hotel Demand Generator Fit",
      hotelScoped: true,
    });
  }

  // Signal — when dated event + source exist
  let signalId = text(stamped.dgSignalId) || null;
  if (primaryDgId && (stamped.eventStartDate || stamped.title) && sourceUrls[0]) {
    const signal = stripEntity(
      buildDemandGeneratorSignal({
        demandGeneratorId: primaryDgId,
        programId,
        hotelId,
        hotelGeneratorFitId: fitId,
        title: stamped.title || stamped.eventName,
        eventStartDate: stamped.eventStartDate,
        sourceUrl: sourceUrls[0],
        sourceUrls,
        triggerType: stamped.triggerType || "ANNUAL_CYCLE",
        lodgingDemandThesis: true,
        hasCredibleLodgingDemand: true,
        hotelFitOk: true,
        marketRelevance: "MEDIUM",
        gdiOpportunityId: stamped.opportunityId || stamped.id || null,
        hotelDemandThesis: stamped.demandThesis || stamped.summaryWhat || null,
      }),
    );
    signalId = signal.signalId;
    plans.push({
      kind: "DEMAND_GENERATOR_SIGNAL",
      identityKey: signal.signalId,
      entity: signal,
      privacyOk: assertNoHotelPrivateLeak(signal).ok,
      table: "Demand Generator Signals",
    });
  } else if (primaryDgId) {
    deferred.push({
      kind: "DEMAND_GENERATOR_SIGNAL",
      outcome: WRITE_OUTCOME.DEFERRED,
      reason: "MISSING_DATED_EVENT_OR_SOURCE",
    });
  }

  const identity = {
    dgGeneratorId: primaryDgId,
    dgProgramId: programId,
    dgSignalId: signalId,
    hotelGeneratorFitId: fitId,
    partnerDgGeneratorId: partnerDgId,
  };

  const stampedOpp = applySharedDemandIdentityToOpportunity(stamped, identity);

  return {
    version: GDI_SLICE2B_RELATIONSHIP_WIRING_V1,
    opportunityId: stamped.opportunityId || stamped.id || null,
    hotelId: hotelId || null,
    marketOpportunityId: stamped.marketOpportunityId || null,
    candidates,
    plans,
    deferred,
    identity,
    stampedOpportunity: stampedOpp,
    airtableWrites: 0,
    productionWrites: 0,
    newTables: 0,
  };
}

/**
 * Execute relationship plan against memory store (offline).
 */
export function applyRelationshipPlanToMemoryStore(plan, store) {
  const results = [];
  const seenDg = new Set();
  for (const step of plan.plans || []) {
    let result;
    if (step.kind === "DEMAND_GENERATOR") {
      const linkOnly = seenDg.has(step.identityKey);
      result = store.upsertDemandGenerator(step.entity, { linkOnly });
      seenDg.add(step.identityKey);
    } else if (step.kind === "DEMAND_PROGRAM") {
      result = store.upsertDemandProgram(step.entity);
    } else if (step.kind === "DEMAND_GENERATOR_SIGNAL") {
      result = store.upsertDemandGeneratorSignal(step.entity);
    } else if (step.kind === "HOTEL_DEMAND_GENERATOR_FIT") {
      result = store.upsertHotelGeneratorFit(step.entity);
    } else {
      result = {
        ok: false,
        outcome: WRITE_OUTCOME.FAILED,
        reason: `UNKNOWN_KIND:${step.kind}`,
      };
    }
    results.push({
      kind: step.kind,
      identityKey: step.identityKey,
      role: step.role || null,
      outcome: result.outcome,
      ok: result.ok,
      readBackId: result.id || null,
    });
  }
  for (const d of plan.deferred || []) {
    results.push({
      kind: d.kind,
      outcome: d.outcome || WRITE_OUTCOME.DEFERRED,
      reason: d.reason,
      ok: true,
    });
  }
  return {
    version: GDI_SLICE2B_RELATIONSHIP_WIRING_V1,
    opportunityId: plan.opportunityId,
    results,
    airtableWrites: 0,
    productionWrites: 0,
  };
}

/**
 * Full offline wiring pass for one opportunity.
 */
export function wireOpportunityRelationshipsOffline(opp = {}, opts = {}) {
  const store = opts.store || createMemoryDemandGeneratorGraphStore();
  const plan = planOpportunityRelationshipGraph(opp, opts);
  const applied = applyRelationshipPlanToMemoryStore(plan, store);
  const readBack = {
    demandGenerator: plan.identity.dgGeneratorId
      ? store.readBackDemandGenerator(plan.identity.dgGeneratorId)
      : null,
    program: plan.identity.dgProgramId
      ? store.readBackProgram(plan.identity.dgProgramId)
      : null,
    signal: plan.identity.dgSignalId
      ? store.readBackSignal(plan.identity.dgSignalId)
      : null,
    hotelFit: plan.identity.hotelGeneratorFitId
      ? store.readBackHotelFit(plan.identity.hotelGeneratorFitId)
      : null,
  };
  return {
    version: GDI_SLICE2B_RELATIONSHIP_WIRING_V1,
    plan,
    applied,
    readBack,
    stampedOpportunity: plan.stampedOpportunity,
    store,
    airtableWrites: 0,
    productionWrites: 0,
    newTables: 0,
  };
}

export function assertPartnerReuseAcrossOpportunities(opps = []) {
  const byOrg = new Map();
  for (const opp of opps) {
    const name = text(opp.organizationName).toLowerCase();
    const id = text(opp.dgGeneratorId || opp.demandGeneratorId);
    if (!name || !id) continue;
    if (!byOrg.has(name)) byOrg.set(name, new Set());
    byOrg.get(name).add(id);
  }
  const collisions = [...byOrg.entries()].filter(([, ids]) => ids.size > 1);
  return {
    ok: collisions.length === 0,
    collisions: collisions.map(([org, ids]) => ({
      organizationName: org,
      dgGeneratorIds: [...ids],
    })),
  };
}

export function buildRelationshipManifest(runs = []) {
  return {
    version: GDI_SLICE2B_RELATIONSHIP_WIRING_V1,
    at: new Date().toISOString(),
    airtableWrites: 0,
    productionWrites: 0,
    newTables: 0,
    runs: runs.map((r) => ({
      opportunityId: r.plan?.opportunityId,
      hotelId: r.plan?.hotelId,
      identity: r.plan?.identity,
      outcomes: (r.applied?.results || []).map((x) => ({
        kind: x.kind,
        outcome: x.outcome,
        identityKey: x.identityKey,
      })),
      privacyAllOk: (r.plan?.plans || []).every((p) => p.privacyOk !== false),
    })),
  };
}

export {
  createMemoryDemandGeneratorGraphStore,
  extractSharedDemandIdentity,
  assertCrossHotelSharedIdentityIsolation,
  computeDemandGeneratorId,
  computeHotelGeneratorFitId,
  computeSignalId,
  WRITE_OUTCOME,
};

export default {
  GDI_SLICE2B_RELATIONSHIP_WIRING_V1,
  extractRelationshipCandidates,
  ensureDemandGeneratorIdOnOpportunity,
  planOpportunityRelationshipGraph,
  applyRelationshipPlanToMemoryStore,
  wireOpportunityRelationshipsOffline,
  assertPartnerReuseAcrossOpportunities,
  buildRelationshipManifest,
  createMemoryDemandGeneratorGraphStore,
};
