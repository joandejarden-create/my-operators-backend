/**
 * Bethesda Full-Demand Challenge — mandatory database reconciliation preflight.
 *
 * Unifies canonical opportunities, research-candidate memory, weekly staged bags,
 * run opportunity-id indexes, and event-series graph BEFORE any paid research.
 *
 * Entity resolution (org) and opportunity resolution (org + program + cycle + geo
 * + sourcing) are separate. Distinct programs under one org are NOT merged.
 *
 * No Airtable / customer / paid writes in this module.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { canonicalizeSegmentId, SUPERVISOR_EXPECTED_SEGMENTS } from "./research-constants-v1.js";

export const BETHESDA_DB_RECONCILIATION_V1 = "gdi-bethesda-full-demand-database-reconciliation-v1";
export const BETHESDA_CANONICAL_HOTEL_ID = "recLuxvwwxID7U2B8";
export const BETHESDA_CANONICAL_HOTEL_NAME = "Bethesda Marriott";
export const BETHESDA_FULL_DEMAND_CHALLENGE_V1 = "gdi-bethesda-full-demand-challenge-v1";
export const BETHESDA_FULL_DEMAND_REPORTS_DIR = "reports/gdi-bethesda-full-demand-challenge-v1";
/** Must stay aligned with challenge package budget — do not diverge. */
export const BETHESDA_CHALLENGE_BUDGET_USD = Object.freeze({
  discoveryPhaseUsd: 1.5,
  deepResearchPhaseUsd: 7.5,
  aggregateCeilingUsd: 9.0,
  perOpportunityDeepResearchMaxUsd: 0.55,
  surfePdl: "SEPARATE_AUTHORIZATION_REQUIRED",
});

export const CHANGE_LEDGER_DECISION = Object.freeze({
  EXISTING_UPDATED: "EXISTING_UPDATED",
  EXISTING_UNCHANGED: "EXISTING_UNCHANGED",
  NEW_OPPORTUNITY: "NEW_OPPORTUNITY",
  DUPLICATE_LINKED: "DUPLICATE_LINKED",
  PREVIOUSLY_REJECTED_REASSESSED: "PREVIOUSLY_REJECTED_REASSESSED",
  DEFERRED: "DEFERRED",
  EXCLUDED: "EXCLUDED",
});

export const RECORD_CLASS = Object.freeze({
  ACTIVE_OPPORTUNITY: "ACTIVE_OPPORTUNITY",
  PREVIOUSLY_RESEARCHED: "PREVIOUSLY_RESEARCHED",
  UNPUBLISHED_INCOMPLETE: "UNPUBLISHED_INCOMPLETE",
  ARCHIVED_OR_REJECTED: "ARCHIVED_OR_REJECTED",
  RELATED_PROGRAM_SAME_ORG: "RELATED_PROGRAM_SAME_ORG",
  POTENTIAL_DUPLICATE: "POTENTIAL_DUPLICATE",
});

export const RESOLUTION_KIND = Object.freeze({
  EXISTING_WITH_NEW_EVIDENCE: "EXISTING_WITH_NEW_EVIDENCE",
  NEW_PROGRAM_EXISTING_ORG: "NEW_PROGRAM_EXISTING_ORG",
  GENUINELY_NEW: "GENUINELY_NEW",
  DUPLICATE: "DUPLICATE",
  PREVIOUSLY_REJECTED_REQUIRES_REASSESSMENT: "PREVIOUSLY_REJECTED_REQUIRES_REASSESSMENT",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const HOTEL_DIR = path.join(ROOT, "data/group-demand-intelligence/hotels", BETHESDA_CANONICAL_HOTEL_ID);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadJson(p, fallback = null) {
  if (!fs.existsSync(p)) return fallback;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}

function asOppArray(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;
  if (Array.isArray(raw.opportunities)) return raw.opportunities;
  if (Array.isArray(raw.candidates)) return raw.candidates;
  return [];
}

function oppId(o = {}) {
  return text(o.id || o.opportunityId || o.canonicalOpportunityId) || null;
}

/** Normalize organization for soft matching — strip legal suffixes and trailing program labels. */
export function normalizeOrganizationIdentity(name = "") {
  let s = text(name).toLowerCase();
  // Split org - program patterns common in disposition IDs
  if (s.includes(" - ")) s = s.split(" - ")[0];
  s = s
    .replace(/\([^)]*\)/g, " ")
    .replace(/\b(inc|llc|ltd|corp|corporation|association|assoc|society|foundation|club)\b\.?/g, " ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return s;
}

export function extractProgramIdentity(o = {}) {
  return text(
    o.canonicalEventName
    || o.normalizedEventName
    || o.eventName
    || o.programName
    || o.title
    || o.displayTitle
    || o.namedProgramOrMechanism
    || "",
  );
}

export function normalizeProgramIdentity(program = "") {
  return text(program)
    .toLowerCase()
    .replace(/\([^)]*\)/g, " ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(20\d{2}|annual|lodging opportunity|hotel opportunity)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function extractCycleIdentity(o = {}) {
  const year = o.eventYear || (String(o.eventStartDate || "").match(/^(20\d{2})/) || [])[1] || null;
  const cycle = text(o.eventCycleId || o.nextCycleStatus || o.eventDateDisplay || "") || null;
  return { year: year ? String(year) : null, cycleLabel: cycle };
}

export function extractGeoIdentity(o = {}) {
  return text(
    o.eventLocationSummary
    || o.geographicRelevance?.reason
    || o.localConnection
    || o.destinationStatus
    || "",
  ).toLowerCase().slice(0, 160);
}

export function extractSourcingContext(o = {}) {
  return {
    room: o.roomDemandStatus || null,
    venue: o.venueSourcingStatus || null,
    sourcing: o.sourcingStatus || null,
    opportunityType: o.opportunityType || null,
  };
}

/**
 * Opportunity resolution key — org + program + cycle year (not org alone).
 */
export function buildOpportunityResolutionKey(o = {}) {
  const org = normalizeOrganizationIdentity(o.organizationName || o.company || o.name);
  const program = normalizeProgramIdentity(extractProgramIdentity(o));
  const { year } = extractCycleIdentity(o);
  const geoHint = extractGeoIdentity(o).slice(0, 40);
  return `org:${org}|prog:${program || "_account"}|year:${year || "_recurring"}|geo:${geoHint || "_"}`;
}

export function classifyRecordLifecycle(o = {}, { sourceBag = "" } = {}) {
  const type = String(o.opportunityType || "");
  const researchStatus = String(o.researchStatus || "");
  const disposition = String(o.disposition || o.researchLayerDisposition || "");
  const customerVisible = o.customerVisible === true;
  const staged = o._stagedNotPublished === true || sourceBag.includes("research-candidates") || sourceBag.includes("staged");
  const rejected = /CLOSED_DISQUALIFIED|REJECTED|ARCHIVED|DISQUALIFIED/i.test(type)
    || /REJECTED|ARCHIVED/i.test(disposition)
    || String(o.priority || "").toUpperCase() === "DISQUALIFIED";

  if (rejected) return RECORD_CLASS.ARCHIVED_OR_REJECTED;

  if (staged || customerVisible === false && sourceBag.includes("research-candidates")) {
    if (/STOPPED|RESEARCH_EXHAUSTED|RESEARCHED/i.test(researchStatus) || (o.researchAttempts || []).length) {
      return RECORD_CLASS.PREVIOUSLY_RESEARCHED;
    }
    return RECORD_CLASS.UNPUBLISHED_INCOMPLETE;
  }

  if (/QUALIFIED|ACTIVE/i.test(researchStatus) || o._airtableRecordId) {
    return RECORD_CLASS.ACTIVE_OPPORTUNITY;
  }

  if ((o.researchAttempts || []).length || o.deepResearchJobId || o.whoResearchState) {
    return RECORD_CLASS.PREVIOUSLY_RESEARCHED;
  }

  return RECORD_CLASS.UNPUBLISHED_INCOMPLETE;
}

/**
 * Inventory evidence already on the record — for incremental gap research.
 */
export function inventoryExistingEvidence(o = {}) {
  const sources = Array.isArray(o.sources) ? o.sources : [];
  const evidence = Array.isArray(o.evidence) ? o.evidence : [];
  const urls = new Set([
    ...sources.map((s) => s.url || s.sourceUrl || s).filter(Boolean),
    ...evidence.map((e) => e.url || e.sourceUrl).filter(Boolean),
    ...(o.sourceUrls || []),
    o.officialSource,
    o.eventDateEvidenceUrl,
    o.attendanceEvidenceUrl,
    o.peakRoomsEvidenceUrl,
    o.contactSourceUrl,
  ].filter(Boolean).map(String));

  const researchedAt = o.updatedAt || o.lastMaterialChangeAt || o.lastContactResearchAt || o.startedAt || null;
  const ageDays = researchedAt
    ? Math.floor((Date.now() - Date.parse(researchedAt)) / 86400000)
    : null;

  return {
    sourceUrlCount: urls.size,
    evidenceCount: evidence.length,
    investigatedUrlCount: (o.investigatedUrls || []).length,
    researchAttempts: (o.researchAttempts || []).length,
    researchStatus: o.researchStatus || null,
    stopReason: o.stopReason || null,
    deepResearchJobId: o.deepResearchJobId || null,
    whoResearchState: o.whoResearchState || o.whoHowResearchState || null,
    contactResearchState: o.contactResearchState || null,
    lastResearchedAt: researchedAt,
    ageDays,
    likelyStale: ageDays != null && ageDays > 45,
    hasNamedContact: Boolean(o.contactName || o.primaryContactName || o.primaryContact?.name),
    hasContactRoute: Boolean(
      o.contactEmail || o.primaryContactEmail || o.primaryContact?.email
      || o.contactUrl || o.organizationContactUrl || o.publicContactPath,
    ),
  };
}

export function identifyUnresolvedCommercialQuestions(o = {}, evidence = null) {
  const ev = evidence || inventoryExistingEvidence(o);
  const gaps = [];
  if (!extractProgramIdentity(o) && !/CORPORATE_ACCOUNT|ACCOUNT/i.test(String(o.opportunityType || ""))) {
    gaps.push("program_or_event_identity_unconfirmed");
  }
  if (!o.eventStartDate && !o.eventDateDisplay && !o.eventYear && !o.nextCycleStatus) {
    gaps.push("timing_or_cycle_unconfirmed");
  }
  if (!o.roomDemandStatus || /UNKNOWN|NULL|SPECULATIVE/i.test(String(o.roomDemandStatus))) {
    gaps.push("room_demand_status_unverified");
  }
  if (!ev.hasContactRoute) gaps.push("usable_contact_route_missing");
  if (!ev.hasNamedContact) gaps.push("named_buyer_unverified");
  if (ev.sourceUrlCount < 2) gaps.push("independent_corroboration_thin");
  if (ev.likelyStale) gaps.push("evidence_may_be_stale");
  if (/OVERFLOW/i.test(String(o.opportunityType || "")) && !/verified|confirmed/i.test(String(o.hotelOpportunityThesis || ""))) {
    gaps.push("overflow_thesis_needs_verification");
  }
  if (o._publicExhaustedPaidBlocked === true) {
    gaps.push("public_research_exhausted_paid_blocked");
  }
  return gaps;
}

/**
 * Decide how a candidate relates to an existing canonical opportunity.
 */
export function resolveAgainstExisting(candidate = {}, existingPool = []) {
  const candOrg = normalizeOrganizationIdentity(candidate.organizationName || candidate.company);
  const candProg = normalizeProgramIdentity(extractProgramIdentity(candidate));
  const candKey = buildOpportunityResolutionKey(candidate);
  const candCycle = extractCycleIdentity(candidate);

  const sameOrg = existingPool.filter((e) => {
    const eOrg = normalizeOrganizationIdentity(e.organizationName || e.company);
    return eOrg && candOrg && (eOrg === candOrg || eOrg.includes(candOrg) || candOrg.includes(eOrg));
  });

  if (!sameOrg.length) {
    return {
      resolutionKind: RESOLUTION_KIND.GENUINELY_NEW,
      matchOpportunityId: null,
      reason: "no_matching_organization_in_canonical_bag",
    };
  }

  const exactKey = sameOrg.find((e) => buildOpportunityResolutionKey(e) === candKey);
  if (exactKey) {
    return {
      resolutionKind: RESOLUTION_KIND.DUPLICATE,
      matchOpportunityId: oppId(exactKey),
      reason: "org_program_cycle_key_collision",
    };
  }

  const sameProgram = sameOrg.find((e) => {
    const p = normalizeProgramIdentity(extractProgramIdentity(e));
    return p && candProg && (p === candProg || p.includes(candProg) || candProg.includes(p));
  });

  if (sameProgram) {
    const existingYear = extractCycleIdentity(sameProgram).year;
    if (existingYear && candCycle.year && existingYear === candCycle.year) {
      return {
        resolutionKind: RESOLUTION_KIND.DUPLICATE,
        matchOpportunityId: oppId(sameProgram),
        reason: "same_org_program_same_year",
      };
    }
    // Distinct dated cycle of a related program → keep as separate opportunity (do not merge years)
    if (existingYear && candCycle.year && existingYear !== candCycle.year) {
      return {
        resolutionKind: RESOLUTION_KIND.NEW_PROGRAM_EXISTING_ORG,
        matchOpportunityId: oppId(sameProgram),
        relatedOrganizationOpportunityIds: sameOrg.map(oppId).filter(Boolean),
        reason: "same_org_related_program_distinct_cycle_year",
      };
    }
    // Same program identity but missing year on one side — possible evidence refresh of existing row
    return {
      resolutionKind: RESOLUTION_KIND.EXISTING_WITH_NEW_EVIDENCE,
      matchOpportunityId: oppId(sameProgram),
      reason: "same_org_program_possible_evidence_refresh",
    };
  }

  // Org exists, distinct program
  if (/CLOSED_DISQUALIFIED|DISQUALIFIED/i.test(String(candidate.opportunityType || ""))) {
    return {
      resolutionKind: RESOLUTION_KIND.PREVIOUSLY_REJECTED_REQUIRES_REASSESSMENT,
      matchOpportunityId: oppId(sameOrg[0]),
      reason: "rejected_label_on_related_org_family",
    };
  }

  return {
    resolutionKind: RESOLUTION_KIND.NEW_PROGRAM_EXISTING_ORG,
    matchOpportunityId: oppId(sameOrg[0]),
    relatedOrganizationOpportunityIds: sameOrg.map(oppId).filter(Boolean),
    reason: "distinct_program_under_known_organization",
  };
}

function ledgerDecisionForResolution(resolutionKind, recordClass) {
  if (resolutionKind === RESOLUTION_KIND.DUPLICATE) return CHANGE_LEDGER_DECISION.DUPLICATE_LINKED;
  if (resolutionKind === RESOLUTION_KIND.PREVIOUSLY_REJECTED_REQUIRES_REASSESSMENT) {
    return CHANGE_LEDGER_DECISION.PREVIOUSLY_REJECTED_REASSESSED;
  }
  if (resolutionKind === RESOLUTION_KIND.EXISTING_WITH_NEW_EVIDENCE) {
    return CHANGE_LEDGER_DECISION.EXISTING_UPDATED; // proposed — applied only after live evidence
  }
  if (resolutionKind === RESOLUTION_KIND.GENUINELY_NEW) return CHANGE_LEDGER_DECISION.NEW_OPPORTUNITY;
  if (resolutionKind === RESOLUTION_KIND.NEW_PROGRAM_EXISTING_ORG) return CHANGE_LEDGER_DECISION.NEW_OPPORTUNITY;
  if (recordClass === RECORD_CLASS.ARCHIVED_OR_REJECTED) {
    return CHANGE_LEDGER_DECISION.PREVIOUSLY_REJECTED_REASSESSED;
  }
  if (recordClass === RECORD_CLASS.UNPUBLISHED_INCOMPLETE) return CHANGE_LEDGER_DECISION.DEFERRED;
  return CHANGE_LEDGER_DECISION.EXISTING_UNCHANGED;
}

/**
 * Load every Bethesda GDI source bag relevant to reconciliation.
 */
export function loadBethesdaGdiSourceCorpus({ root = ROOT } = {}) {
  const hotelDir = path.join(root, "data/group-demand-intelligence/hotels", BETHESDA_CANONICAL_HOTEL_ID);
  const sources = [];

  const canonicalRaw = loadJson(path.join(hotelDir, "opportunities.json"), []);
  const canonical = asOppArray(canonicalRaw).map((o) => ({
    ...o,
    _corpusSource: "canonical_opportunities",
    _sourceBag: "opportunities.json",
  }));
  sources.push({
    name: "canonical_opportunities",
    path: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/opportunities.json`,
    count: canonical.length,
  });

  const rcRaw = loadJson(path.join(hotelDir, "research-candidates.json"), null);
  const researchCandidates = asOppArray(rcRaw).map((o) => ({
    ...o,
    _corpusSource: o._corpusSource || "research_memory",
    _sourceBag: "research-candidates.json",
  }));
  sources.push({
    name: "research_candidates",
    path: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/research-candidates.json`,
    count: researchCandidates.length,
  });

  const weeklyDir = path.join(hotelDir, "weekly-runs");
  const weeklyOpps = [];
  if (fs.existsSync(weeklyDir)) {
    for (const run of fs.readdirSync(weeklyDir)) {
      for (const file of ["opportunities.staged.json", "opportunities.pre-stage.json"]) {
        const p = path.join(weeklyDir, run, file);
        if (!fs.existsSync(p)) continue;
        const rows = asOppArray(loadJson(p, [])).map((o) => ({
          ...o,
          _corpusSource: "weekly_staged",
          _sourceBag: `weekly-runs/${run}/${file}`,
        }));
        weeklyOpps.push(...rows);
        sources.push({
          name: `weekly_${run}_${file}`,
          path: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/weekly-runs/${run}/${file}`,
          count: rows.length,
        });
      }
    }
  }

  const runsDir = path.join(hotelDir, "runs");
  const runOpportunityIds = new Set();
  const runIndex = [];
  if (fs.existsSync(runsDir)) {
    for (const run of fs.readdirSync(runsDir)) {
      const p = path.join(runsDir, run, "run.json");
      const j = loadJson(p, null);
      if (!j) continue;
      const ids = Array.isArray(j.opportunityIds) ? j.opportunityIds : [];
      ids.forEach((id) => runOpportunityIds.add(id));
      runIndex.push({ runId: j.id || run, opportunityIdCount: ids.length, status: j.status || null });
    }
  }
  sources.push({
    name: "historical_runs_opportunity_id_index",
    path: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/runs/`,
    runCount: runIndex.length,
    uniqueOpportunityIdsReferenced: runOpportunityIds.size,
  });

  const eventSeries = loadJson(path.join(hotelDir, "event-series-graph.json"), null);
  const feedback = loadJson(path.join(hotelDir, "feedback.json"), null);
  const contacts = loadJson(path.join(hotelDir, "canonical-contacts.json"), null);

  sources.push({
    name: "event_series_graph",
    present: Boolean(eventSeries),
    organizationCount: eventSeries?.organizations?.length || Object.keys(eventSeries?.organizations || {}).length || null,
    seriesCount: eventSeries?.series?.length || null,
  });
  sources.push({
    name: "feedback",
    present: Boolean(feedback),
    itemCount: feedback?.items?.length || 0,
  });
  sources.push({
    name: "canonical_contacts",
    present: Boolean(contacts),
  });

  return {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    sources,
    canonical,
    researchCandidates,
    weeklyOpps,
    runOpportunityIds: [...runOpportunityIds],
    runIndex,
    eventSeries,
    feedback,
    contacts,
  };
}

/**
 * Build unified inventory + change ledger (offline preflight; no paid, no Airtable).
 */
export function runBethesdaDatabaseReconciliationPreflight({ root = ROOT } = {}) {
  const corpus = loadBethesdaGdiSourceCorpus({ root });
  const canonicalById = new Map(corpus.canonical.map((o) => [oppId(o), o]));
  const changeLedger = [];
  const unified = [];
  const seenIds = new Set();

  // 1) Canonical active bag — preserve IDs
  for (const o of corpus.canonical) {
    const id = oppId(o);
    if (!id || seenIds.has(id)) continue;
    seenIds.add(id);
    const recordClass = classifyRecordLifecycle(o, { sourceBag: o._sourceBag });
    const evidence = inventoryExistingEvidence(o);
    const gaps = identifyUnresolvedCommercialQuestions(o, evidence);
    const segment = canonicalizeSegmentId(o.researchSegment || o.segment || o.primarySegment)
      || o.segment
      || null;

    let decision = CHANGE_LEDGER_DECISION.EXISTING_UNCHANGED;
    let resolutionKind = null;
    if (recordClass === RECORD_CLASS.ARCHIVED_OR_REJECTED) {
      decision = CHANGE_LEDGER_DECISION.PREVIOUSLY_REJECTED_REASSESSED;
      resolutionKind = RESOLUTION_KIND.PREVIOUSLY_REJECTED_REQUIRES_REASSESSMENT;
    } else if (gaps.length === 0 && !evidence.likelyStale) {
      decision = CHANGE_LEDGER_DECISION.EXISTING_UNCHANGED;
    } else {
      // Eligible for incremental refresh during live — still "unchanged" until evidence lands
      decision = CHANGE_LEDGER_DECISION.EXISTING_UNCHANGED;
    }

    const entry = {
      canonicalOpportunityId: id,
      organizationName: o.organizationName,
      programIdentity: extractProgramIdentity(o) || null,
      resolutionKey: buildOpportunityResolutionKey(o),
      segment,
      recordClass,
      resolutionKind,
      decision,
      sourceBag: o._sourceBag,
      corpusSource: o._corpusSource,
      airtableRecordId: o._airtableRecordId || null,
      opportunityType: o.opportunityType || null,
      customerVisible: o.customerVisible !== false,
      evidence,
      unresolvedCommercialQuestions: gaps,
      incrementalResearchPlan: {
        researchOnlyGaps: true,
        avoidUnnecessaryProviderCalls: true,
        gapTargets: gaps,
        preservePriorEvidence: true,
        doNotOverwriteStrongerWithWeaker: true,
      },
      proposedInternalUpdate: null,
      oldVsNew: {
        note: "Preflight — no live evidence applied yet",
        commerciallyNewIntelligence: false,
      },
      reason: gaps.length
        ? `Canonical record retained; ${gaps.length} commercial gap(s) queued for incremental research`
        : "Canonical record retained; no material gaps identified offline",
    };
    changeLedger.push(entry);
    unified.push({
      ...o,
      _reconciliation: {
        decision,
        recordClass,
        resolutionKey: entry.resolutionKey,
        gaps,
      },
    });
  }

  // 2) Research-candidate memory + weekly — resolve vs canonical
  const secondary = [
    ...corpus.researchCandidates,
    ...corpus.weeklyOpps,
  ];

  for (const o of secondary) {
    const id = oppId(o);
    if (!id) continue;
    if (seenIds.has(id)) {
      changeLedger.push({
        canonicalOpportunityId: id,
        organizationName: o.organizationName,
        programIdentity: extractProgramIdentity(o) || null,
        decision: CHANGE_LEDGER_DECISION.DUPLICATE_LINKED,
        resolutionKind: RESOLUTION_KIND.DUPLICATE,
        recordClass: RECORD_CLASS.POTENTIAL_DUPLICATE,
        reason: "identical_opportunity_id_already_in_unified_inventory",
        sourceBag: o._sourceBag,
        commerciallyNewIntelligence: false,
      });
      continue;
    }

    const resolution = resolveAgainstExisting(o, corpus.canonical);
    const recordClass = classifyRecordLifecycle(o, { sourceBag: o._sourceBag });
    let decision = ledgerDecisionForResolution(resolution.resolutionKind, recordClass);

    // Soft org/program match to a canonical ID → link duplicate, do not invent second active row
    if (resolution.resolutionKind === RESOLUTION_KIND.DUPLICATE
      || resolution.resolutionKind === RESOLUTION_KIND.EXISTING_WITH_NEW_EVIDENCE) {
      decision = resolution.resolutionKind === RESOLUTION_KIND.DUPLICATE
        ? CHANGE_LEDGER_DECISION.DUPLICATE_LINKED
        : CHANGE_LEDGER_DECISION.EXISTING_UPDATED;
      seenIds.add(id);
      const evidence = inventoryExistingEvidence(o);
      const matched = canonicalById.get(resolution.matchOpportunityId);
      const matchedEvidence = matched ? inventoryExistingEvidence(matched) : null;
      const commerciallyNew = evidence.sourceUrlCount > (matchedEvidence?.sourceUrlCount || 0)
        || Boolean(o.contactEmail && matched && !matched.contactEmail);

      changeLedger.push({
        canonicalOpportunityId: resolution.matchOpportunityId || id,
        linkedSourceOpportunityId: id,
        organizationName: o.organizationName,
        programIdentity: extractProgramIdentity(o) || null,
        resolutionKey: buildOpportunityResolutionKey(o),
        segment: o.researchSegment || o.segment || null,
        recordClass: RECORD_CLASS.POTENTIAL_DUPLICATE,
        resolutionKind: resolution.resolutionKind,
        decision,
        sourceBag: o._sourceBag,
        corpusSource: o._corpusSource,
        evidence,
        unresolvedCommercialQuestions: identifyUnresolvedCommercialQuestions(o, evidence),
        oldVsNew: {
          matchedCanonicalId: resolution.matchOpportunityId,
          candidateSourceUrls: evidence.sourceUrlCount,
          canonicalSourceUrls: matchedEvidence?.sourceUrlCount ?? null,
          commerciallyNewIntelligence: commerciallyNew,
          note: commerciallyNew
            ? "Candidate may carry additional evidence — propose merge into canonical; do not overwrite stronger verified fields"
            : "No commercially new intelligence beyond canonical bag",
        },
        proposedInternalUpdate: commerciallyNew
          ? {
            targetOpportunityId: resolution.matchOpportunityId,
            action: "MERGE_EVIDENCE_PRESERVE_PROVENANCE",
            airtableWrite: false,
            status: "PROPOSED_AWAITING_LIVE_VALIDATION",
          }
          : null,
        reason: resolution.reason,
      });
      continue;
    }

    // Genuinely new or new program under known org — add to unified inventory (internal only)
    seenIds.add(id);
    const evidence = inventoryExistingEvidence(o);
    const gaps = identifyUnresolvedCommercialQuestions(o, evidence);
    if (recordClass === RECORD_CLASS.UNPUBLISHED_INCOMPLETE && gaps.includes("room_demand_status_unverified")) {
      // keep as NEW but may DEFER weak speculative rows
      if (!evidence.hasContactRoute && evidence.sourceUrlCount < 1) {
        decision = CHANGE_LEDGER_DECISION.DEFERRED;
      }
    }
    if (recordClass === RECORD_CLASS.ARCHIVED_OR_REJECTED) {
      decision = CHANGE_LEDGER_DECISION.PREVIOUSLY_REJECTED_REASSESSED;
    }

    const entry = {
      canonicalOpportunityId: id,
      organizationName: o.organizationName,
      programIdentity: extractProgramIdentity(o) || null,
      resolutionKey: buildOpportunityResolutionKey(o),
      segment: o.researchSegment || o.segment || null,
      recordClass: resolution.resolutionKind === RESOLUTION_KIND.NEW_PROGRAM_EXISTING_ORG
        ? RECORD_CLASS.RELATED_PROGRAM_SAME_ORG
        : recordClass,
      resolutionKind: resolution.resolutionKind,
      decision,
      sourceBag: o._sourceBag,
      corpusSource: o._corpusSource,
      relatedCanonicalOpportunityId: resolution.matchOpportunityId || null,
      evidence,
      unresolvedCommercialQuestions: gaps,
      incrementalResearchPlan: {
        researchOnlyGaps: true,
        avoidUnnecessaryProviderCalls: true,
        gapTargets: gaps,
        preservePriorEvidence: true,
        doNotOverwriteStrongerWithWeaker: true,
      },
      proposedInternalUpdate: {
        action: decision === CHANGE_LEDGER_DECISION.NEW_OPPORTUNITY
          ? "ADD_TO_INTERNAL_UNIFIED_INVENTORY"
          : decision === CHANGE_LEDGER_DECISION.DEFERRED
            ? "HOLD_INTERNAL_DEFERRED"
            : "REASSESS_REJECTED",
        airtableWrite: false,
        customerFacing: false,
        status: "PROPOSED_INTERNAL_ONLY",
      },
      oldVsNew: {
        commerciallyNewIntelligence: decision === CHANGE_LEDGER_DECISION.NEW_OPPORTUNITY,
        note: "Recovered from research memory / staged bags — not in prior 54-only inventory",
      },
      reason: resolution.reason || `Recovered from ${o._sourceBag}`,
    };
    changeLedger.push(entry);

    if (decision !== CHANGE_LEDGER_DECISION.EXCLUDED
      && decision !== CHANGE_LEDGER_DECISION.DUPLICATE_LINKED) {
      unified.push({
        ...o,
        _reconciliation: {
          decision,
          recordClass: entry.recordClass,
          resolutionKind: resolution.resolutionKind,
          resolutionKey: entry.resolutionKey,
          gaps,
          relatedCanonicalOpportunityId: resolution.matchOpportunityId || null,
        },
      });
    }
  }

  // 3) Run ID orphan check — IDs referenced historically but missing from bags
  const unifiedIds = new Set(unified.map(oppId));
  const orphanRunIds = corpus.runOpportunityIds.filter((id) => !unifiedIds.has(id) && !canonicalById.has(id));
  for (const id of orphanRunIds.slice(0, 50)) {
    changeLedger.push({
      canonicalOpportunityId: id,
      decision: CHANGE_LEDGER_DECISION.DEFERRED,
      recordClass: RECORD_CLASS.UNPUBLISHED_INCOMPLETE,
      reason: "opportunity_id_referenced_in_historical_run_but_payload_not_in_current_bags",
      commerciallyNewIntelligence: false,
    });
  }

  const counts = {
    totalExistingOpportunitiesRecovered: corpus.canonical.length,
    researchMemoryCandidatesRecovered: corpus.researchCandidates.length,
    weeklyStagedRowsInspected: corpus.weeklyOpps.length,
    unifiedInventorySize: unified.length,
    existingUnchanged: changeLedger.filter((e) => e.decision === CHANGE_LEDGER_DECISION.EXISTING_UNCHANGED).length,
    existingUpdatedProposed: changeLedger.filter((e) => e.decision === CHANGE_LEDGER_DECISION.EXISTING_UPDATED).length,
    newOpportunities: changeLedger.filter((e) => e.decision === CHANGE_LEDGER_DECISION.NEW_OPPORTUNITY).length,
    duplicatesLinked: changeLedger.filter((e) => e.decision === CHANGE_LEDGER_DECISION.DUPLICATE_LINKED).length,
    previouslyRejectedReassessed: changeLedger.filter((e) => e.decision === CHANGE_LEDGER_DECISION.PREVIOUSLY_REJECTED_REASSESSED).length,
    deferred: changeLedger.filter((e) => e.decision === CHANGE_LEDGER_DECISION.DEFERRED).length,
    excluded: changeLedger.filter((e) => e.decision === CHANGE_LEDGER_DECISION.EXCLUDED).length,
    withUsableContactRoute: unified.filter((o) => inventoryExistingEvidence(o).hasContactRoute).length,
    orphanRunOpportunityIds: orphanRunIds.length,
  };

  const bySegment = {};
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) bySegment[seg] = 0;
  for (const o of unified) {
    const seg = canonicalizeSegmentId(o.researchSegment || o.segment || o.primarySegment)
      || o._reconciliation?.segment
      || null;
    if (seg && bySegment[seg] != null) bySegment[seg] += 1;
    else if (seg) bySegment[seg] = (bySegment[seg] || 0) + 1;
  }

  const segmentsStillEmpty = SUPERVISOR_EXPECTED_SEGMENTS.filter((s) => !bySegment[s]);

  const safety = {
    airtableWrites: false,
    customerFacingPublication: false,
    productionPersistence: false,
    proposedUpdatesOnly: true,
    preserveOriginalOpportunityIds: true,
    preservePriorEvidenceProvenanceTimestamps: true,
    doNotOverwriteStrongerWithWeaker: true,
  };

  const gate = {
    mandatoryBeforePaidResearch: true,
    reconciliationPreflightComplete: true,
    requiresUnifiedInventoryNotFiftyFourOnly: true,
    priorFiftyFourOnlyInsufficient: corpus.researchCandidates.length > 0,
    aggregateBudgetUsdUnchanged: BETHESDA_CHALLENGE_BUDGET_USD.aggregateCeilingUsd,
    providerRestrictionsUnchanged: true,
    paidResearchAuthorized: false,
    executePaidResearch: false,
    blockersRemaining: [
      "FOUNDER_GO_REQUIRED_FOR_PAID_EXA_FIRECRAWL",
      "SURFE_PDL_SEPARATE_AUTHORIZATION_FOR_NAMED_BUYER_ENRICHMENT",
      "NO_AIRTABLE_OR_CUSTOMER_PUBLICATION_IN_THIS_PHASE",
      "LIVE_RESEARCH_MUST_CALL_assertBethesdaReconciliationGate",
    ],
  };

  return {
    version: BETHESDA_DB_RECONCILIATION_V1,
    challengeVersion: BETHESDA_FULL_DEMAND_CHALLENGE_V1,
    preparedAt: new Date().toISOString(),
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    corpusSummary: {
      sources: corpus.sources,
      runIndexCount: corpus.runIndex.length,
    },
    counts,
    bySegment,
    segmentsStillEmptyAfterReconciliation: segmentsStillEmpty,
    changeLedger,
    unifiedInventoryPreview: unified.slice(0, 30).map((o) => ({
      id: oppId(o),
      organizationName: o.organizationName,
      program: extractProgramIdentity(o) || null,
      segment: o.researchSegment || o.segment || null,
      decision: o._reconciliation?.decision,
      recordClass: o._reconciliation?.recordClass,
      sourceBag: o._sourceBag,
    })),
    unifiedOpportunityIds: unified.map(oppId).filter(Boolean),
    safety,
    gate,
    finalDeliveryRule: {
      selectFinalTenToFifteenFrom: "unified_inventory_of_existing_refreshed_and_newly_discovered",
      notFrom: "isolated_three_opportunity_certifications_or_fifty_four_only_bag",
    },
    goNoGo: {
      decision: "GO_FOR_RECONCILIATION_PREFLIGHT_ONLY",
      paidLiveResearch: "NO_GO_UNTIL_FOUNDER_AUTHORIZES_AND_RECONCILIATION_GATE_PASSES",
      note: "Database reconciliation preflight complete. Stop before paid research.",
    },
  };
}

/**
 * Hard gate for live execution — must pass before any paid provider call.
 */
export function assertBethesdaReconciliationGate(preflight = null, opts = {}) {
  const report = preflight || runBethesdaDatabaseReconciliationPreflight(opts);
  const errors = [];

  if (!report?.gate?.mandatoryBeforePaidResearch) {
    errors.push("reconciliation_not_marked_mandatory");
  }
  if (!report?.gate?.reconciliationPreflightComplete) {
    errors.push("reconciliation_preflight_incomplete");
  }
  if (!report?.counts?.unifiedInventorySize || report.counts.unifiedInventorySize < report.counts.totalExistingOpportunitiesRecovered) {
    errors.push("unified_inventory_smaller_than_canonical_bag");
  }
  if (report?.gate?.executePaidResearch === true && opts.founderAuthorizedPaid !== true) {
    errors.push("execute_paid_flag_set_without_founder_authorization");
  }
  if (report?.safety?.airtableWrites === true) {
    errors.push("airtable_writes_must_remain_false");
  }
  if (Number(report?.gate?.aggregateBudgetUsdUnchanged) !== BETHESDA_CHALLENGE_BUDGET_USD.aggregateCeilingUsd) {
    errors.push("budget_ceiling_must_remain_9_usd");
  }
  // Live paid still blocked unless explicit founder flag
  if (opts.allowPaidResearch === true && opts.founderAuthorizedPaid !== true) {
    errors.push("founder_authorized_paid_required");
  }

  const ok = errors.length === 0;
  return {
    ok,
    errors,
    paidResearchAllowed: ok && opts.allowPaidResearch === true && opts.founderAuthorizedPaid === true,
    reconciliationVersion: report.version,
    unifiedInventorySize: report.counts?.unifiedInventorySize,
    reportsDir: BETHESDA_FULL_DEMAND_REPORTS_DIR,
    note: ok
      ? (opts.founderAuthorizedPaid
        ? "Reconciliation gate passed; paid research may proceed under $9 ceiling"
        : "Reconciliation gate structurally OK — paid still NO-GO until founderAuthorizedPaid")
      : "Reconciliation gate FAILED — do not start paid research",
  };
}
