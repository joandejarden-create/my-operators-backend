/**
 * GDI 20-opportunity controlled experiment: existing ORS/eligibility baseline
 * vs bounded adaptive public investigation (reuse-only, $0 paid providers).
 *
 * Does NOT publish, write Airtable, or authorize paid Exa/SerpAPI/PDL/Surfe.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { evaluateOpportunity } from "../research/opportunity-research-supervisor-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import {
  buildAdaptiveResearchPreflight,
} from "../adaptive-research-preflight-v1.js";
import {
  selectAdaptiveNextAction,
  PHASE2F_PILOT_LIMITS,
  DENIED_METERED_PROVIDERS,
} from "../adaptive-public-research-controller-v1.js";
import { recoverCommercialEvidence } from "../commercial-evidence-recovery-v1.js";
import { redactOpportunityForShadowExport } from "../gdi2-shadow-publication-evaluation-v1.js";

export const EXPERIMENT_ID = "gdi_20opp_adaptive_investigation_experiment_v1";
export const EXPERIMENT_VERSION = "2026-11-19.1";
export const MAX_PAID_EXTERNAL_USD = 0;
export const OPTIONAL_LIVE_BUDGET_USD_GATED = 20;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const COHORT_QUOTAS = Object.freeze({
  corporate_meetings: 4,
  association_event: 4,
  projects_workforce: 3,
  medical_pharma: 3,
  sports_university: 2,
  rejected_unsupported: 2,
  cross_hotel_booked: 2,
});

/**
 * Preferred hotels from the brief. Mayflower / Falls Church / Nobu lack local
 * opportunities.json in this workspace — substitutes are documented in the
 * cohort manifest.
 */
export const HOTEL_POOL = Object.freeze([
  { hotelId: "recLuxvwwxID7U2B8", name: "Bethesda Marriott", preferred: true },
  { hotelId: "recnPB3Q02NO8Ma1o", name: "Wyndham San Diego Bayside", preferred: true },
  { hotelId: "recrPQcZg7SFARRb2", name: "YOTEL Geneva Lake", preferred: true },
  { hotelId: "recH9h3McgMNFQEla", name: "Radisson Blu Toronto Downtown", preferred: true },
  {
    hotelId: "rece0or38cxo3Fymb",
    name: "W Rome",
    preferred: true,
    note: "cross-hotel / already-placed contracted venue cases",
  },
  {
    hotelId: "recgMYovrrZDJMqzX",
    name: "Waterstone Resort & Marina",
    preferred: false,
    substituteFor: "Mayflower Autograph (rec0eTEnCqlsGJJt9 has profile only; no opportunities.json)",
  },
  {
    hotelId: "rec82D9zpB8fede1I",
    name: "Castillo Hotel Son Vida",
    preferred: false,
    substituteFor: "Falls Church Marriott (no local opportunities.json in ws-gdi)",
  },
  {
    hotelId: "recCAGqkERSXgiku4",
    name: "Sheraton Guadalajara Expo",
    preferred: false,
    substituteFor: "Nobu London (no local opportunities.json in ws-gdi)",
  },
  {
    hotelId: "recXh4NgF8sDbi9cQ",
    name: "Wyndham Alltra Punta Cana",
    preferred: false,
    substituteFor: "additional coastal / leisure inventory diversity",
  },
]);

export const RUBRIC_DIMENSIONS = Object.freeze([
  "commercialEligibilityCorrectness",
  "evidenceAccuracyProvenance",
  "meaningfulNewVerifiedFindings",
  "contactQualityRelevance",
  "thesisCompleteness",
  "hotelSpecificRelevance",
  "salesActionability",
  "duplicateDetection",
  "falsePositiveNegativeRisk",
  "researchCostEfficiency",
]);

function sha256(text) {
  return createHash("sha256").update(String(text || "")).digest("hex");
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function fold(v) {
  return text(v).toLowerCase();
}

export function classifyDemandCategory(opportunity = {}) {
  const statusBlob = fold(
    [
      opportunity.status,
      opportunity.demandStatus,
      opportunity.publicationStatus,
      opportunity.eligibilityStatus,
      opportunity.commercialEligibilityStatus,
      opportunity.qualificationStatus,
      opportunity.qualificationOutcome,
      opportunity.lifecycleStatus,
      opportunity.stopReason,
      opportunity.rejectionReason,
    ].join(" "),
  );
  const segmentBlob = fold(
    [
      opportunity.segment,
      opportunity.demandSegment,
      opportunity.demandFamily,
      opportunity.demandType,
      opportunity.opportunityType,
      opportunity.demandSignalType,
      opportunity.category,
      opportunity.demandCategory,
      opportunity.workflow,
      opportunity.researchSegment,
    ].join(" "),
  );
  const blob = fold(
    [
      segmentBlob,
      opportunity.title,
      opportunity.name,
      opportunity.organizationName,
      opportunity.companyName,
      opportunity.orgName,
      opportunity.thesis,
      opportunity.summary,
      opportunity.whatIsHappening,
      opportunity.notes,
    ].join(" "),
  );

  // Mutually exclusive priority: contracted/cross-hotel → rejected → domain categories
  if (
    opportunity.alreadyContracted === true ||
    opportunity.venueContracted === true ||
    opportunity.bookedElsewhere === true ||
    /already.?book|booked elsewhere|other hotel|competitor hotel|cross.?hotel|overflow.?vip|venue operator|organizer\)/i.test(
      blob,
    ) ||
    /housing.?overflow|overflow/.test(segmentBlob)
  ) {
    return "cross_hotel_booked";
  }
  if (
    opportunity.publicationBlocked === true ||
    opportunity.unsupportedPromotionAttempt === true ||
    /^(speculative_watch|rejected|unsupported|commercially_ineligible|not_pursue)$/i.test(
      statusBlob.trim(),
    ) ||
    /\bspeculative_watch\b|\brejected\b|\bunsupported\b|\bcommercially_ineligible\b|\bnot_pursue\b/.test(
      statusBlob,
    )
  ) {
    return "rejected_unsupported";
  }
  if (
    /medical|scientific|pharma|nih|hospital|clinical|biotech|health|rna biology|ophthalmology|ahima|infection|isct|aao\b|amwa|nci\b/.test(
      blob,
    ) ||
    /education_scientific|medical.?scientific/.test(segmentBlob)
  ) {
    return "medical_pharma";
  }
  if (
    /sport|university|ncaa|alumni|athletic|soccer|tournament|fifa|cheer|higher education|academic/.test(
      blob,
    ) ||
    /sports_entertainment|youth sports|higher education/.test(segmentBlob)
  ) {
    return "sports_university";
  }
  if (
    /project|infrastructure|workforce|construction|relocation|extended stay|development district|nado/.test(
      blob,
    ) ||
    /projects_infrastructure/.test(segmentBlob)
  ) {
    return "projects_workforce";
  }
  if (
    /association|conference|congress|society|event rotation|annual meeting|asae|itu|ias\b|associations_conferences/.test(
      blob,
    ) ||
    /association_events|associations_conferences|association/.test(segmentBlob)
  ) {
    return "association_event";
  }
  if (
    /corporate|meeting|training|incentive|expansion|account|local meetings|catering|kiwanis|cmaa|partnership|scaling/.test(
      blob,
    ) ||
    /corporate_meetings|local_catering|scaling_companies|private events|mice/.test(segmentBlob)
  ) {
    return "corporate_meetings";
  }
  return "other";
}

function opportunityIdOf(o = {}) {
  return text(o.id || o.opportunityId || o.dispId || o.externalId);
}

function orgOf(o = {}) {
  return text(o.organizationName || o.companyName || o.orgName || o.name || o.title);
}

function loadHotelOpportunities(hotelId, root = ROOT) {
  const p = path.join(root, "data/group-demand-intelligence/hotels", hotelId, "opportunities.json");
  if (!fs.existsSync(p)) return { path: p, opportunities: [], missing: true };
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  const opportunities = Array.isArray(raw)
    ? raw
    : raw.opportunities || raw.items || raw.records || [];
  return { path: p, opportunities, missing: false, fileSha256: sha256(fs.readFileSync(p)) };
}

function freezeSnapshot(opportunity, hotelMeta) {
  const id = opportunityIdOf(opportunity);
  const clone = JSON.parse(JSON.stringify(opportunity));
  return {
    opportunityId: id,
    hotelId: hotelMeta.hotelId,
    hotelName: hotelMeta.name,
    organization: orgOf(opportunity),
    demandCategory: classifyDemandCategory(opportunity),
    overlappingDemandCharacteristics: {
      segment: opportunity.segment || opportunity.demandSegment || null,
      workflow: opportunity.workflow || null,
      hasContact: Boolean(opportunity.contact?.email || opportunity.contact?.name || opportunity.primaryContact),
      alreadyContracted: Boolean(opportunity.alreadyContracted || opportunity.venueContracted),
    },
    originalResearchDate:
      opportunity.researchedAt ||
      opportunity.updatedAt ||
      opportunity.createdAt ||
      opportunity.lastResearchedAt ||
      null,
    historicalResearchStatus:
      opportunity.status ||
      opportunity.researchStatus ||
      opportunity.qualificationStatus ||
      null,
    originalPublicationStatus:
      opportunity.publicationStatus ||
      opportunity.visibility ||
      opportunity.customerVisibility ||
      null,
    availableSourceEvidenceCount: Array.isArray(opportunity.evidence)
      ? opportunity.evidence.length
      : Array.isArray(opportunity.sources)
        ? opportunity.sources.length
        : Array.isArray(opportunity.acceptedClaims)
          ? opportunity.acceptedClaims.length
          : 0,
    relevantContacts: summarizeContacts(opportunity),
    evaluationLabelsHeldOut: {
      note: "Labels for human scoring only — not passed into investigator inputs",
      knownSalesReadyHint: opportunity.salesReady ?? null,
      knownEligibilityHint: opportunity.commercialEligibilityStatus ?? null,
    },
    snapshotSha256: sha256(JSON.stringify(clone)),
    frozenOpportunity: clone,
  };
}

function summarizeContacts(opportunity = {}) {
  const contacts = [];
  const c = opportunity.contact || opportunity.primaryContact || {};
  if (c.name || c.email || c.fullName) {
    contacts.push({
      name: c.name || c.fullName || null,
      email: c.email || null,
      role: c.role || c.title || null,
      verifiedHint: Boolean(c.verified || c.emailVerified),
    });
  }
  for (const x of opportunity.contacts || opportunity.namedContacts || []) {
    contacts.push({
      name: x.name || x.fullName || null,
      email: x.email || null,
      role: x.role || x.title || null,
      verifiedHint: Boolean(x.verified || x.emailVerified),
    });
  }
  return contacts.slice(0, 5);
}

/**
 * Deterministic cohort selection with quotas + substitutions.
 */
export function buildTwentyCaseCohort({ root = ROOT, now = new Date().toISOString() } = {}) {
  const substitutions = [];
  const byCategory = Object.fromEntries(Object.keys(COHORT_QUOTAS).map((k) => [k, []]));
  const other = [];

  for (const hotel of HOTEL_POOL) {
    const loaded = loadHotelOpportunities(hotel.hotelId, root);
    if (loaded.missing) {
      substitutions.push({
        hotelId: hotel.hotelId,
        hotelName: hotel.name,
        reason: "opportunities.json missing",
        substituteFor: hotel.substituteFor || null,
      });
      continue;
    }
    if (!hotel.preferred && hotel.substituteFor) {
      substitutions.push({
        hotelId: hotel.hotelId,
        hotelName: hotel.name,
        reason: "included as substitute for preferred hotel without local opp store",
        substituteFor: hotel.substituteFor,
      });
    }
    for (const opp of loaded.opportunities) {
      const id = opportunityIdOf(opp);
      if (!id) continue;
      const cat = classifyDemandCategory(opp);
      const frozen = freezeSnapshot(opp, hotel);
      frozen.sourceFile = path.relative(root, loaded.path).replace(/\\/g, "/");
      frozen.sourceFileSha256 = loaded.fileSha256;
      if (byCategory[cat]) byCategory[cat].push(frozen);
      else other.push(frozen);
    }
  }

  const preferredIds = new Set(HOTEL_POOL.filter((h) => h.preferred).map((h) => h.hotelId));

  // Prefer preferred hotels, then diversify by hotel, then stable id order
  function diversifySort(rows) {
    const byHotel = new Map();
    for (const row of rows) {
      if (!byHotel.has(row.hotelId)) byHotel.set(row.hotelId, []);
      byHotel.get(row.hotelId).push(row);
    }
    for (const list of byHotel.values()) {
      list.sort((a, b) => a.opportunityId.localeCompare(b.opportunityId));
    }
    const hotelOrder = [...byHotel.keys()].sort((a, b) => {
      const ap = preferredIds.has(a) ? 0 : 1;
      const bp = preferredIds.has(b) ? 0 : 1;
      if (ap !== bp) return ap - bp;
      return a.localeCompare(b);
    });
    const out = [];
    let idx = 0;
    let progressed = true;
    while (progressed) {
      progressed = false;
      for (const hid of hotelOrder) {
        const list = byHotel.get(hid);
        if (list && list[idx]) {
          out.push(list[idx]);
          progressed = true;
        }
      }
      idx += 1;
    }
    return out;
  }

  for (const cat of Object.keys(byCategory)) {
    byCategory[cat] = diversifySort(byCategory[cat]);
  }
  const otherSorted = diversifySort(other);

  const selected = [];
  const used = new Set();
  const shortfalls = [];
  const hotelUseCounts = new Map();

  function take(cat, n) {
    const pool = byCategory[cat] || [];
    let took = 0;
    // Pass 1: prefer unused hotels for diversity
    const passes = [
      (row) => (hotelUseCounts.get(row.hotelId) || 0) === 0,
      (row) => (hotelUseCounts.get(row.hotelId) || 0) < 3,
      () => true,
    ];
    for (const allow of passes) {
      for (const row of pool) {
        if (took >= n) break;
        const key = `${row.hotelId}::${row.opportunityId}`;
        if (used.has(key)) continue;
        if (!allow(row)) continue;
        used.add(key);
        hotelUseCounts.set(row.hotelId, (hotelUseCounts.get(row.hotelId) || 0) + 1);
        selected.push({ ...row, cohortSlot: cat, caseIndex: selected.length + 1 });
        took += 1;
      }
    }
    if (took < n) shortfalls.push({ category: cat, needed: n, got: took });
    return took;
  }

  for (const [cat, n] of Object.entries(COHORT_QUOTAS)) take(cat, n);

  // Fill shortfalls from other / remaining pools without breaking mutual exclusivity of slots
  if (selected.length < 20) {
    const filler = diversifySort([
      ...otherSorted,
      ...Object.values(byCategory).flat(),
    ]).filter((r) => !used.has(`${r.hotelId}::${r.opportunityId}`));
    for (const row of filler) {
      if (selected.length >= 20) break;
      const key = `${row.hotelId}::${row.opportunityId}`;
      used.add(key);
      hotelUseCounts.set(row.hotelId, (hotelUseCounts.get(row.hotelId) || 0) + 1);
      selected.push({
        ...row,
        cohortSlot: row.demandCategory === "other" ? "corporate_meetings" : row.demandCategory,
        caseIndex: selected.length + 1,
        fillReason: "quota_shortfall_backfill",
      });
    }
  }

  // Reindex
  selected.forEach((row, i) => {
    row.caseIndex = i + 1;
  });

  const manifest = {
    schema: EXPERIMENT_ID,
    version: EXPERIMENT_VERSION,
    createdAt: now,
    historicalCutoffNote:
      "Frozen from local opportunities.json snapshots at cohort build time; evaluation labels held out from model inputs.",
    maxPaidExternalUsd: MAX_PAID_EXTERNAL_USD,
    optionalLiveBudgetUsdGated: OPTIONAL_LIVE_BUDGET_USD_GATED,
    quotas: COHORT_QUOTAS,
    substitutions,
    shortfalls,
    caseCount: selected.length,
    cases: selected.map((c) => ({
      caseIndex: c.caseIndex,
      opportunityId: c.opportunityId,
      organization: c.organization,
      hotelId: c.hotelId,
      hotelName: c.hotelName,
      demandCategory: c.demandCategory,
      cohortSlot: c.cohortSlot,
      originalResearchDate: c.originalResearchDate,
      historicalResearchStatus: c.historicalResearchStatus,
      originalPublicationStatus: c.originalPublicationStatus,
      availableSourceEvidenceCount: c.availableSourceEvidenceCount,
      relevantContacts: c.relevantContacts,
      evaluationLabelsHeldOut: c.evaluationLabelsHeldOut,
      overlappingDemandCharacteristics: c.overlappingDemandCharacteristics,
      sourceFile: c.sourceFile,
      sourceFileSha256: c.sourceFileSha256,
      snapshotSha256: c.snapshotSha256,
      fillReason: c.fillReason || null,
    })),
    frozenOpportunitiesByCase: Object.fromEntries(
      selected.map((c) => [`case_${c.caseIndex}`, c.frozenOpportunity]),
    ),
  };

  return { manifest, selected };
}

/** Evidence challenger — verification pass before eligibility (no publication). */
export function runEvidenceChallenger(opportunity = {}) {
  const claims = [];
  const evidence = opportunity.evidence || opportunity.sources || opportunity.acceptedClaims || [];
  const evidenceCount = Array.isArray(evidence) ? evidence.length : 0;
  const demandThesis = text(
    opportunity.demandThesis || opportunity.thesis || opportunity.summary || opportunity.opportunityThesis,
  );
  const timing = text(opportunity.needDate || opportunity.eventDate || opportunity.timingNotes);

  let confirmedDemand = false;
  let inferredDemand = false;
  let speculative = false;

  if (evidenceCount === 0 && demandThesis) {
    speculative = true;
    claims.push({
      type: "speculation",
      detail: "Demand thesis present without attached evidence array",
    });
  }
  if (/might|maybe|possibly|could host|potential opportunity(?! evidence)/i.test(demandThesis)) {
    speculative = true;
    claims.push({ type: "speculation", detail: "Speculative language in thesis" });
  }
  if (evidenceCount > 0) {
    inferredDemand = true;
    if (evidenceCount >= 2 || opportunity.futureDemand === true) confirmedDemand = true;
  }
  if (timing && !/\d{4}|q[1-4]|january|february|march|april|may|june|july|august|september|october|november|december/i.test(timing)) {
    claims.push({ type: "unsupported_timing", detail: "Timing claim lacks concrete date markers" });
  }

  const contacts = summarizeContacts(opportunity);
  const verifiedNamed = contacts.filter((c) => c.name && (c.verifiedHint || c.email));
  const contactShapedOnly = contacts.filter((c) => c.name && !c.email && !c.verifiedHint);

  if (opportunity.duplicateOf || opportunity.isDuplicate) {
    claims.push({ type: "duplicate", detail: "Marked duplicate" });
  }

  const commercialWeaknesses = [];
  if (!verifiedNamed.length) commercialWeaknesses.push("missing_verified_named_buyer_or_entry_point");
  if (speculative) commercialWeaknesses.push("speculative_demand");
  if (!demandThesis) commercialWeaknesses.push("missing_thesis");

  return {
    schema: "gdi-evidence-challenger-v1",
    confirmedDemand,
    reliablyInferredDemand: inferredDemand && !speculative,
    speculative,
    contactRelevance: {
      verifiedNamedCount: verifiedNamed.length,
      nameShapedUnverifiedCount: contactShapedOnly.length,
    },
    unsupportedTiming: claims.some((c) => c.type === "unsupported_timing"),
    duplicateOrContradiction: claims.some((c) => c.type === "duplicate"),
    commercialWeaknesses,
    claims,
    salesReadyBlockedByChallenger:
      speculative || verifiedNamed.length === 0 || commercialWeaknesses.includes("missing_thesis"),
  };
}

function scoreArm(opportunity, evaluation, challenger, costs, opts = {}) {
  const elig = evaluation.shared?.status || evaluation.shared?.eligibilityStatus || null;
  const salesReady = elig === "SALES_READY" || evaluation.shared?.salesReady === true;
  const evidenceN =
    (Array.isArray(opportunity.evidence) ? opportunity.evidence.length : 0) ||
    (Array.isArray(opportunity.sources) ? opportunity.sources.length : 0) ||
    (Array.isArray(opportunity.acceptedClaims) ? opportunity.acceptedClaims.length : 0);
  const thesisOk = Boolean(
    text(
      opportunity.thesis ||
        opportunity.demandThesis ||
        opportunity.summary ||
        opportunity.whatIsHappening ||
        opportunity.potentialHotelBusiness,
    ),
  );
  // Challenger gate: speculative or unverified buyer cannot score as sales-ready actionability
  const actionabilityOk = salesReady && !challenger.salesReadyBlockedByChallenger;
  const scores = {
    commercialEligibilityCorrectness: salesReady
      ? challenger.salesReadyBlockedByChallenger
        ? 2
        : 4
      : elig === "RESEARCH_CONTINUATION"
        ? 3
        : elig === "COMMERCIALLY_INELIGIBLE"
          ? 4
          : 2,
    evidenceAccuracyProvenance: Math.min(5, 1 + Math.min(4, evidenceN)),
    meaningfulNewVerifiedFindings: opts.newFindings ? 4 : opts.strategyProposed ? 2 : 1,
    contactQualityRelevance: Math.min(
      5,
      1 + (challenger.contactRelevance.verifiedNamedCount || 0) * 2,
    ),
    thesisCompleteness: thesisOk ? (challenger.speculative ? 2 : 4) : 1,
    hotelSpecificRelevance: opportunity.hotelCatchmentMismatch === true ? 1 : 3,
    salesActionability: actionabilityOk ? 4 : salesReady ? 2 : 2,
    duplicateDetection: opportunity.duplicateOf || opportunity.isDuplicate ? 4 : 3,
    falsePositiveNegativeRisk: challenger.speculative
      ? 1
      : actionabilityOk
        ? 3
        : elig === "COMMERCIALLY_INELIGIBLE"
          ? 4
          : 3,
    researchCostEfficiency: costs.actualPaidUsd === 0 ? 5 : 2,
  };
  const total = Object.values(scores).reduce((a, b) => a + b, 0);
  return { scores, total, maxTotal: RUBRIC_DIMENSIONS.length * 5 };
}

export function runBaselineCase(frozenOpportunity, meta = {}) {
  const t0 = Date.now();
  const opp = JSON.parse(JSON.stringify(frozenOpportunity));
  const supervisor = evaluateOpportunity(opp, { requireFutureDemand: false });
  const shared = evaluateSharedCommercialEligibility(opp);
  const challenger = runEvidenceChallenger(opp);
  const costs = {
    actualPaidUsd: 0,
    estimatedUsd: 0,
    providerCalls: [],
    deniedPaidAttempts: 0,
    mode: "baseline_frozen_evaluation",
  };
  const scored = scoreArm(opp, { shared }, challenger, costs);
  return {
    arm: "baseline_current_gdi",
    executionKind: "fresh_deterministic_evaluation_on_frozen_snapshot",
    opportunityId: meta.opportunityId || opportunityIdOf(opp),
    durationMs: Date.now() - t0,
    supervisor: {
      blockers: (supervisor.blockers || []).map((b) => b.key || b),
      publishable: supervisor.publishable === true,
      exploratory: supervisor.exploratory === true,
      maturityHint: supervisor.maturityHint || null,
      hardStopReason: supervisor.hardStop?.reason || null,
    },
    sharedEligibility: {
      status: shared.status || shared.eligibilityStatus || null,
      salesReady: shared.salesReady === true,
      gaps: shared.gaps || [],
    },
    challenger,
    thesis: text(
      opp.thesis || opp.demandThesis || opp.summary || opp.whatIsHappening || opp.title,
    ),
    contacts: summarizeContacts(opp),
    costs,
    rubric: scored,
    redacted: redactOpportunityForShadowExport(opp),
  };
}

/**
 * Agentic arm: adaptive next-action planning + evidence recovery + challenger.
 * Default: no external retrieval (offline / $0). Optional public fetch only when
 * explicitly authorized via opts.authorizeExternalRetrieval === true (still $0 paid).
 */
export async function runAgenticCase(frozenOpportunity, meta = {}, opts = {}) {
  const t0 = Date.now();
  const authorizeExternal = opts.authorizeExternalRetrieval === true;
  const costs = {
    actualPaidUsd: 0,
    estimatedUsd: 0,
    providerCalls: [],
    deniedPaidAttempts: 0,
    externalRetrievals: 0,
    mode: authorizeExternal
      ? "agentic_bounded_public_fetch_zero_paid"
      : "agentic_offline_adaptive_planning_zero_paid",
  };

  let working = JSON.parse(JSON.stringify(frozenOpportunity));
  const recovered = recoverCommercialEvidence(working);
  working = recovered.recoveredOpportunity || working;

  const steps = [];
  const maxSteps = Number(opts.maxSteps) || 3;
  // Keep pilot limits in the cost log for auditability (authorizePaidResearch always false).
  costs.pilotLimits = { ...PHASE2F_PILOT_LIMITS, ...(opts.limits || {}), authorizePaidResearch: false };

  for (let i = 0; i < maxSteps; i += 1) {
    const preflight = buildAdaptiveResearchPreflight(working, { skipRecovery: true });
    const next = selectAdaptiveNextAction(working, {
      useBuyerDiscoveryStrategies: true,
      hotelCommercialAlignment: true,
    });
    const step = {
      index: i + 1,
      continue: Boolean(next.continue),
      decision: next.decision || null,
      reason: next.reason || null,
      strategyId: next.strategy?.id || next.approach?.id || null,
      gapType: next.gapType || null,
      preflightGaps: preflight?.sharedGaps || [],
      preflightDecision: preflight?.stopOrContinuation?.decision || null,
    };

    const strategyId = step.strategyId || "";
    if (
      strategyId &&
      DENIED_METERED_PROVIDERS.some((p) => new RegExp(p, "i").test(strategyId))
    ) {
      costs.deniedPaidAttempts += 1;
      step.deniedPaid = true;
      steps.push(step);
      break;
    }

    if (!authorizeExternal) {
      step.externalRetrieval = "skipped_zero_spend_offline_mode";
      steps.push(step);
      break;
    }

    step.externalRetrieval = "authorized_but_deferred_to_gated_live_protocol";
    steps.push(step);
    break;
  }

  // Challenger before eligibility re-check (eligibility remains authoritative for sales-ready)
  const challenger = runEvidenceChallenger(working);
  const supervisor = evaluateOpportunity(working, { requireFutureDemand: false });
  let shared = evaluateSharedCommercialEligibility(working);
  if (challenger.salesReadyBlockedByChallenger && shared.salesReady === true) {
    shared = {
      ...shared,
      status: "RESEARCH_CONTINUATION",
      salesReady: false,
      commercialEligible: false,
      challengerOverride: "blocked_unsupported_or_unverified_buyer",
      gaps: [...(shared.gaps || []), "CHALLENGER_BLOCKED_SALES_READY"],
    };
  }

  const recoveryApplied =
    recovered?.overlaysApplied?.demand === true ||
    recovered?.overlaysApplied?.buyer === true ||
    recovered?.evidenceBackedNewSalesReady === true;
  const strategyProposed = steps.some((s) => s.strategyId);
  const newFindings = recoveryApplied;

  const scored = scoreArm(working, { shared }, challenger, costs, {
    newFindings,
    strategyProposed,
  });

  return {
    arm: "agentic_enhanced_gdi",
    executionKind: authorizeExternal
      ? "fresh_model_adaptive_with_optional_public_fetch"
      : "fresh_deterministic_adaptive_planning_on_frozen_snapshot",
    opportunityId: meta.opportunityId || opportunityIdOf(working),
    durationMs: Date.now() - t0,
    steps,
    recovery: {
      applied: recoveryApplied,
      evidenceBackedNewSalesReady: recovered?.evidenceBackedNewSalesReady === true,
      category: recovered?.category || null,
      overlaysApplied: recovered?.overlaysApplied || null,
      beforeStatus: recovered?.before?.status || null,
      afterStatus: recovered?.after?.status || null,
    },
    supervisor: {
      blockers: (supervisor.blockers || []).map((b) => b.key || b),
      publishable: supervisor.publishable === true,
      exploratory: supervisor.exploratory === true,
      maturityHint: supervisor.maturityHint || null,
      hardStopReason: supervisor.hardStop?.reason || null,
    },
    sharedEligibility: {
      status: shared.status || shared.eligibilityStatus || null,
      salesReady: shared.salesReady === true,
      gaps: shared.gaps || [],
      challengerOverride: shared.challengerOverride || null,
    },
    challenger,
    thesis: text(
      working.thesis ||
        working.demandThesis ||
        working.summary ||
        working.whatIsHappening ||
        working.title,
    ),
    contacts: summarizeContacts(working),
    costs,
    rubric: scored,
    redacted: redactOpportunityForShadowExport(working),
  };
}

export function compareCase(baseline, agentic) {
  const deltaTotal = agentic.rubric.total - baseline.rubric.total;
  const regressions = [];
  const improvements = [];
  for (const dim of RUBRIC_DIMENSIONS) {
    const d = (agentic.rubric.scores[dim] || 0) - (baseline.rubric.scores[dim] || 0);
    if (d < 0) regressions.push({ dimension: dim, delta: d });
    if (d > 0) improvements.push({ dimension: dim, delta: d });
  }
  return {
    opportunityId: baseline.opportunityId,
    baselineTotal: baseline.rubric.total,
    agenticTotal: agentic.rubric.total,
    deltaTotal,
    baselineStatus: baseline.sharedEligibility.status,
    agenticStatus: agentic.sharedEligibility.status,
    improvements,
    regressions,
    agenticWorse: deltaTotal < 0,
    agenticBetter: deltaTotal > 0,
    actualPaidUsd: (baseline.costs.actualPaidUsd || 0) + (agentic.costs.actualPaidUsd || 0),
  };
}

export async function runFullExperiment({
  root = ROOT,
  authorizeExternalRetrieval = false,
  outDir = null,
} = {}) {
  const { manifest, selected } = buildTwentyCaseCohort({ root });
  if (selected.length !== 20) {
    throw new Error(`Cohort must contain exactly 20 cases; got ${selected.length}`);
  }

  const caseResults = [];
  for (const row of selected) {
    const baseline = runBaselineCase(row.frozenOpportunity, row);
    const agentic = await runAgenticCase(row.frozenOpportunity, row, {
      authorizeExternalRetrieval,
      maxSteps: 3,
    });
    const comparison = compareCase(baseline, agentic);
    caseResults.push({
      caseIndex: row.caseIndex,
      opportunityId: row.opportunityId,
      organization: row.organization,
      hotelId: row.hotelId,
      hotelName: row.hotelName,
      cohortSlot: row.cohortSlot,
      demandCategory: row.demandCategory,
      baseline,
      agentic,
      comparison,
    });
  }

  const totals = {
    cases: caseResults.length,
    baselineAvgScore:
      caseResults.reduce((s, c) => s + c.baseline.rubric.total, 0) / caseResults.length,
    agenticAvgScore:
      caseResults.reduce((s, c) => s + c.agentic.rubric.total, 0) / caseResults.length,
    improvedCount: caseResults.filter((c) => c.comparison.agenticBetter).length,
    worsenedCount: caseResults.filter((c) => c.comparison.agenticWorse).length,
    unchangedCount: caseResults.filter((c) => c.comparison.deltaTotal === 0).length,
    actualPaidUsdTotal: caseResults.reduce((s, c) => s + c.comparison.actualPaidUsd, 0),
    deniedPaidAttempts: caseResults.reduce(
      (s, c) => s + (c.agentic.costs.deniedPaidAttempts || 0),
      0,
    ),
  };

  const decision = decideGoNoGo(totals, caseResults, { authorizeExternalRetrieval });

  const report = {
    schema: EXPERIMENT_ID,
    version: EXPERIMENT_VERSION,
    executedAt: new Date().toISOString(),
    authorizeExternalRetrieval,
    maxPaidExternalUsd: MAX_PAID_EXTERNAL_USD,
    optionalLiveBudgetUsdGated: OPTIONAL_LIVE_BUDGET_USD_GATED,
    executionDisclaimer:
      "Default run uses frozen snapshots + deterministic ORS/eligibility + offline adaptive planning. No paid providers. Historical publication statuses are not claimed as newly executed baselines.",
    cohort: {
      version: manifest.version,
      caseCount: manifest.caseCount,
      substitutions: manifest.substitutions,
      shortfalls: manifest.shortfalls,
      cases: manifest.cases,
    },
    totals,
    decision,
    caseResults: caseResults.map((c) => ({
      caseIndex: c.caseIndex,
      opportunityId: c.opportunityId,
      organization: c.organization,
      hotelId: c.hotelId,
      hotelName: c.hotelName,
      cohortSlot: c.cohortSlot,
      comparison: c.comparison,
      baseline: {
        executionKind: c.baseline.executionKind,
        sharedEligibility: c.baseline.sharedEligibility,
        challenger: c.baseline.challenger,
        rubric: c.baseline.rubric,
        costs: c.baseline.costs,
        durationMs: c.baseline.durationMs,
      },
      agentic: {
        executionKind: c.agentic.executionKind,
        steps: c.agentic.steps,
        recovery: c.agentic.recovery,
        sharedEligibility: c.agentic.sharedEligibility,
        challenger: c.agentic.challenger,
        rubric: c.agentic.rubric,
        costs: c.agentic.costs,
        durationMs: c.agentic.durationMs,
      },
    })),
    detailedExamples: pickDetailedExamples(caseResults),
    moatAssessment: assessMoat(caseResults),
    gatedLiveProtocol: {
      status: "NOT_EXECUTED",
      maxIncrementalProviderBudgetUsd: OPTIONAL_LIVE_BUDGET_USD_GATED,
      requiresFounderAuthorization: true,
      intendedCalls: [
        "optional official_page / direct_public fetches only",
        "still deny Exa/SerpAPI/PDL/Surfe/Firecrawl/Parallel",
      ],
    },
  };

  const dir =
    outDir ||
    path.join(root, "reports", "gdi-20opp-adaptive-investigation-experiment-v1");
  fs.mkdirSync(dir, { recursive: true });
  const cohortPath = path.join(dir, "COHORT_MANIFEST.json");
  // Strip bulky frozen bodies from published cohort file; keep hashes + metadata
  const cohortPublic = { ...manifest };
  delete cohortPublic.frozenOpportunitiesByCase;
  fs.writeFileSync(cohortPath, JSON.stringify(cohortPublic, null, 2));
  fs.writeFileSync(path.join(dir, "RESULTS.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(dir, "CASE_RESULTS_FULL.json"),
    JSON.stringify(caseResults, null, 2),
  );
  fs.writeFileSync(path.join(dir, "EXECUTIVE_REPORT.md"), renderExecutiveMarkdown(report));
  fs.writeFileSync(path.join(dir, "TECHNICAL_FINDINGS.md"), renderTechnicalMarkdown(report));
  fs.writeFileSync(
    path.join(dir, "COST_AND_TOOL_LOG.json"),
    JSON.stringify(
      {
        actualPaidUsdTotal: totals.actualPaidUsdTotal,
        estimatedUsdTotal: 0,
        deniedPaidAttempts: totals.deniedPaidAttempts,
        perCase: caseResults.map((c) => ({
          opportunityId: c.opportunityId,
          baseline: c.baseline.costs,
          agentic: c.agentic.costs,
        })),
      },
      null,
      2,
    ),
  );

  // Fixture-sized frozen snapshots for reproducible harness (not full hotel dumps)
  const fixtureDir = path.join(root, "fixtures", "gdi-experiments", "gdi-20opp-adaptive-investigation-v1");
  fs.mkdirSync(fixtureDir, { recursive: true });
  fs.writeFileSync(
    path.join(fixtureDir, "cohort-manifest.public.json"),
    JSON.stringify(cohortPublic, null, 2),
  );
  const slimFrozen = {};
  for (const row of selected) {
    slimFrozen[`case_${row.caseIndex}`] = {
      opportunityId: row.opportunityId,
      hotelId: row.hotelId,
      snapshotSha256: row.snapshotSha256,
      frozenOpportunity: row.frozenOpportunity,
    };
  }
  fs.writeFileSync(
    path.join(fixtureDir, "frozen-case-snapshots.json"),
    JSON.stringify(slimFrozen, null, 2),
  );

  return { report, manifest: cohortPublic, outDir: dir, fixtureDir };
}

function decideGoNoGo(totals, caseResults, opts = {}) {
  const avgDelta = totals.agenticAvgScore - totals.baselineAvgScore;
  const worseRate = totals.worsenedCount / totals.cases;
  const recoveryOnlyLift = caseResults.every(
    (c) =>
      (c.comparison.deltaTotal > 0 &&
        (c.agentic.recovery?.applied === true ||
          (c.agentic.steps || []).some((s) => s.strategyId))) ||
      c.comparison.deltaTotal === 0,
  );
  const eligibilityFlipCount = caseResults.filter(
    (c) => c.comparison.baselineStatus !== c.comparison.agenticStatus,
  ).length;
  const contactLiftCount = caseResults.filter(
    (c) =>
      (c.agentic.challenger?.contactRelevance?.verifiedNamedCount || 0) >
      (c.baseline.challenger?.contactRelevance?.verifiedNamedCount || 0),
  ).length;

  if (totals.actualPaidUsdTotal > 0) {
    return {
      recommendation: "NO-GO",
      rationale: "Experiment violated zero paid-spend constraint",
      avgDelta,
    };
  }

  // Offline $0 runs cannot claim full live-agent validation. Require eligibility
  // or contact lift beyond recovery/thesis remapping for GO.
  if (opts.authorizeExternalRetrieval !== true) {
    if (
      avgDelta > 0 &&
      (eligibilityFlipCount >= 2 || contactLiftCount >= 2) &&
      worseRate <= 0.2
    ) {
      return {
        recommendation: "ITERATE",
        rationale:
          "Offline adaptive planning + recovery shows measurable structure lift, but live public-fetch validation is still required before production promotion",
        avgDelta,
        eligibilityFlipCount,
        contactLiftCount,
        nextPhase:
          "Founder-gated ≤$20 public-fetch live validation on a 5-case subset; keep ORS as sole supervisor",
      };
    }
    return {
      recommendation: "ITERATE",
      rationale:
        recoveryOnlyLift
          ? "Offline arm improves local recovery/planning scores, but lift is not yet evidence of new verified web findings or eligibility flips — do not treat as live-agent validation"
          : "Promising offline adaptive planning / recovery improvements, but not yet decisive for production promotion",
      avgDelta,
      eligibilityFlipCount,
      contactLiftCount,
      nextPhase:
        "Tighten challenger→eligibility coupling; run gated ≤$20 public-fetch subset; re-score with blind human review",
    };
  }

  if (
    avgDelta >= 1.5 &&
    worseRate <= 0.15 &&
    totals.improvedCount >= 5 &&
    (eligibilityFlipCount >= 2 || contactLiftCount >= 2)
  ) {
    return {
      recommendation: "GO",
      rationale:
        "Adaptive arm improved average rubric with limited regressions and material eligibility/contact lift under authorized retrieval",
      avgDelta,
      eligibilityFlipCount,
      contactLiftCount,
      nextPhase:
        "Promote challenger + bounded adaptive planning behind feature flag; keep paid providers denied by default",
    };
  }
  if (avgDelta > 0 || totals.improvedCount >= 3) {
    return {
      recommendation: "ITERATE",
      rationale:
        "Promising adaptive improvements under authorized retrieval, but quality/reliability still need optimization",
      avgDelta,
      nextPhase:
        "Reduce false thesis inflation; require challenger-confirmed demand before scoring lift",
    };
  }
  return {
    recommendation: "NO-GO",
    rationale:
      "Agentic arm did not outperform existing GDI evaluation enough to justify added complexity at this checkpoint",
    avgDelta,
    nextPhase: "Keep current ORS + eligibility; revisit after richer frozen evidence packs",
  };
}

function pickDetailedExamples(caseResults) {
  const sorted = [...caseResults].sort(
    (a, b) => b.comparison.deltaTotal - a.comparison.deltaTotal,
  );
  const best = sorted[0];
  const worst = sorted[sorted.length - 1];
  const contactImprove =
    caseResults.find(
      (c) =>
        (c.agentic.challenger.contactRelevance.verifiedNamedCount || 0) >
        (c.baseline.challenger.contactRelevance.verifiedNamedCount || 0),
    ) ||
    caseResults.find((c) => (c.agentic.contacts || []).length > (c.baseline.contacts || []).length);
  const crossHotel = caseResults.find((c) => c.cohortSlot === "cross_hotel_booked");
  const rejection = caseResults.find(
    (c) =>
      c.cohortSlot === "rejected_unsupported" ||
      c.agentic.challenger.speculative ||
      c.agentic.sharedEligibility.status === "COMMERCIALLY_INELIGIBLE",
  );
  const noValue =
    caseResults.find((c) => c.comparison.agenticWorse) ||
    caseResults.find((c) => c.comparison.deltaTotal === 0) ||
    worst;
  return {
    strongestImprovement: summarizeExample(best, "strongest_improvement"),
    strongestRejectionOrCorrection: summarizeExample(
      rejection || worst,
      "strongest_rejection_correction",
    ),
    contactImprovement: summarizeExample(
      contactImprove || best,
      "contact_improvement",
    ),
    crossHotelReuse: summarizeExample(crossHotel || best, "cross_hotel_reuse"),
    noValueOrWorse: summarizeExample(noValue, "no_value_or_worse"),
  };
}

function summarizeExample(c, label) {
  if (!c) return { label, missing: true };
  return {
    label,
    opportunityId: c.opportunityId,
    organization: c.organization,
    hotelId: c.hotelId,
    hotelName: c.hotelName,
    deltaTotal: c.comparison.deltaTotal,
    baselineStatus: c.comparison.baselineStatus,
    agenticStatus: c.comparison.agenticStatus,
    regressions: c.comparison.regressions,
    improvements: c.comparison.improvements,
  };
}

function assessMoat(caseResults) {
  return {
    newReusableIntelligence: {
      orgsTouched: new Set(caseResults.map((c) => c.organization)).size,
      adaptiveStrategiesProposed: caseResults.reduce(
        (n, c) => n + (c.agentic.steps || []).filter((s) => s.strategyId).length,
        0,
      ),
      note: "Offline mode proposes strategies and applies local recovery; does not mint new verified web evidence without gated fetch",
    },
    crossHotelReuse: {
      crossHotelCases: caseResults.filter((c) => c.cohortSlot === "cross_hotel_booked").length,
      multiHotelOrgs: countMultiHotelOrgs(caseResults),
    },
    learningReadiness: {
      stableOpportunityIds: caseResults.every((c) => c.opportunityId),
      evidenceRevisionHooks: "ORS maturation + opportunity-research-state / change ledger (existing)",
      salesFeedbackCapture: "Not executed in this experiment (no production writes)",
    },
  };
}

function countMultiHotelOrgs(caseResults) {
  const map = new Map();
  for (const c of caseResults) {
    const k = fold(c.organization);
    if (!k) continue;
    if (!map.has(k)) map.set(k, new Set());
    map.get(k).add(c.hotelId);
  }
  return [...map.values()].filter((s) => s.size > 1).length;
}

function renderExecutiveMarkdown(report) {
  const t = report.totals;
  const d = report.decision;
  return `# GDI 20-Opportunity Adaptive Investigation Experiment — Executive Report

**Experiment:** ${report.schema}  
**Version:** ${report.version}  
**Executed:** ${report.executedAt}  
**Paid spend (actual):** $${t.actualPaidUsdTotal} (cap $${report.maxPaidExternalUsd})

## Recommendation

**${d.recommendation}** — ${d.rationale}

Next: ${d.nextPhase || "n/a"}

## Headline metrics

| Metric | Current GDI (baseline) | Agentic GDI | Difference |
|--------|------------------------|-------------|------------|
| Avg rubric total | ${t.baselineAvgScore.toFixed(2)} | ${t.agenticAvgScore.toFixed(2)} | ${(t.agenticAvgScore - t.baselineAvgScore).toFixed(2)} |
| Cases improved | — | ${t.improvedCount} | — |
| Cases worsened | — | ${t.worsenedCount} | — |
| Cases unchanged | — | ${t.unchangedCount} | — |
| Actual paid USD | 0 | 0 | 0 |

## Execution fidelity

${report.executionDisclaimer}

Gated live protocol: **${report.gatedLiveProtocol.status}** (max incremental $${report.gatedLiveProtocol.maxIncrementalProviderBudgetUsd}, founder auth required).

## Cohort

${report.cohort.cases.map((c) => `- **Case ${c.caseIndex}** \`${c.opportunityId}\` — ${c.organization} @ ${c.hotelName} [${c.cohortSlot}]`).join("\n")}

## Detailed examples

${Object.entries(report.detailedExamples)
  .map(
    ([k, ex]) =>
      `### ${k}\n- Opportunity: \`${ex.opportunityId}\` (${ex.organization})\n- Hotel: ${ex.hotelName}\n- Delta: ${ex.deltaTotal}\n`,
  )
  .join("\n")}

## Moat

- Orgs touched: ${report.moatAssessment.newReusableIntelligence.orgsTouched}
- Multi-hotel orgs in cohort: ${report.moatAssessment.crossHotelReuse.multiHotelOrgs}
- Adaptive strategies proposed: ${report.moatAssessment.newReusableIntelligence.adaptiveStrategiesProposed}

## Production note

Do **not** treat archived/frozen experiment outputs as Airtable production persistence or customer publication.
`;
}

function renderTechnicalMarkdown(report) {
  return `# Technical findings — ${report.schema}

## Arms

- **Baseline:** \`evaluateOpportunity\` + \`evaluateSharedCommercialEligibility\` + evidence challenger on frozen snapshots.
- **Agentic:** commercial evidence recovery + \`buildAdaptiveResearchPreflight\` + \`selectAdaptiveNextAction\` + challenger + eligibility (offline by default).

## Hard denies

Paid providers denied: ${DENIED_METERED_PROVIDERS.join(", ")}

## Per-case comparison (compact)

| Case | Opportunity ID | Δ score | Baseline status | Agentic status | Worse? |
|------|----------------|---------|-----------------|----------------|--------|
${report.caseResults
  .map(
    (c) =>
      `| ${c.caseIndex} | \`${c.opportunityId}\` | ${c.comparison.deltaTotal} | ${c.comparison.baselineStatus} | ${c.comparison.agenticStatus} | ${c.comparison.agenticWorse} |`,
  )
  .join("\n")}

## Substitutions / shortfalls

\`\`\`json
${JSON.stringify({ substitutions: report.cohort.substitutions, shortfalls: report.cohort.shortfalls }, null, 2)}
\`\`\`

## Decision object

\`\`\`json
${JSON.stringify(report.decision, null, 2)}
\`\`\`
`;
}
