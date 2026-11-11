/**
 * Commercial Depth Hardening — controlled live validation.
 * Aggregate $1.35 · hotel budgets Wyndham $0.45 / Bethesda $0.90 · isolated ledgers.
 * No Surfe/PDL/Parallel · no Airtable · publication OFF · weekly stays 0.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runDeepResearchOnExistingGdiOpportunity,
  captureOpportunityBeforeSnapshot,
} from "./deep-research-gdi-integration-v1.js";
import {
  evaluateDeepResearchEligibility,
  tierBudgetDefaults,
} from "./deep-research-eligibility-v1.js";
import {
  prepareOpportunityForAutonomousDeepResearch,
  buildAlternativeSourceCandidates,
  hasRoleRelevantDecisionMaker,
  buildBuyerDiscoveryQueries,
} from "./autonomous-deep-research-recovery-v1.js";
import {
  selectCrossOpportunityCertificationSet,
  COMMERCIAL_OUTCOME,
} from "./autonomous-longform-gdi-integration-v1.js";
import {
  HARDENING_REPORTS_DIR,
  COMMERCIAL_DEPTH_HARDENING_V1,
} from "./commercial-depth-hardening-v1.js";
import {
  assertLiveDeepResearchLedger,
  loadOrCreateLiveDeepResearchLedger,
  createLiveDeepResearchResearcher,
  preflightLiveDeepResearchPricing,
  classifyEvidenceBuckets,
  LIVE_LEDGER_RELATIVE,
} from "./deep-research-gdi-live-certification-v1.js";
import {
  assertPaidContactProvidersDisabled,
  isConsumerEmailDomain,
} from "./commercial-spine-restoration-v41.js";
import {
  assessCommercialImprovement,
  IMPROVEMENT_OUTCOME,
} from "./commercial-improvement-measurement-v1.js";

export const HARDENING_LIVE_VERSION = "gdi-commercial-depth-hardening-live-v1";
export const HARDENING_LIVE_AGGREGATE_USD = 1.35;
export const HARDENING_LIVE_LEDGER_VERSION = "gdi-commercial-depth-hardening-live-ledger-v1";

export const HOTEL_BUDGETS_USD = Object.freeze({
  recnPB3Q02NO8Ma1o: 0.45, // Wyndham San Diego Bayside
  recLuxvwwxID7U2B8: 0.90, // Bethesda Marriott
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const FIXTURE_ONLY_NAMES = /^(jordan\s+hale|chris\s+delgado|riley\s+chen|alex\s+morgan)$/i;
const PRINCIPAL_NOBU_LEAK = /Principal\.|Nobu Portman|Rosewood London|central-London luxury|Marylebone\s*\/\s*West End/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function stripFixtureStakeholders(list = []) {
  return (list || []).filter((p) => {
    const name = text(p?.name || p?.personName);
    return name && !FIXTURE_ONLY_NAMES.test(name);
  });
}

function seedLiveOpportunityFromSelection(sel) {
  const bag = sel.bag || {};
  const id = bag.id;
  const org = bag.organizationName;
  const contactEmail = bag.contactEmail || bag.primaryContactEmail || null;
  const official = bag.officialSource || bag.contactUrl || null;
  const investigated = [];
  if (official) investigated.push(official);
  if (sel.competingHotel) {
    investigated.push("https://www.marriott.com/en-us/hotels/wascr-crystal-gateway-marriott/overview/");
  }

  const evidence = [];
  if (text(bag.hotelOpportunityThesis)) {
    evidence.push({
      url: official || `canonical://${id}/thesis`,
      statement: text(bag.hotelOpportunityThesis).slice(0, 500),
      quote: text(bag.hotelOpportunityThesis).slice(0, 500),
      provenance: "canonical_gdi_bag",
      accepted: true,
      previouslyHeld: true,
    });
  }

  let host = sel.orgHost;
  try {
    if (official) host = new URL(official).hostname.replace(/^www\./, "");
  } catch { /* keep */ }

  return prepareOpportunityForAutonomousDeepResearch({
    id,
    opportunityId: id,
    hotelId: sel.hotelId,
    hotelName: sel.hotelName,
    organizationName: org,
    organizationDomain: host,
    pathway: sel.pathway,
    internalDisposition: sel.internalDisposition,
    commercialStage: sel.internalDisposition,
    segment: sel.segment || bag.segment,
    opportunityType: bag.opportunityType || null,
    venueSourcingStatus: bag.venueSourcingStatus || null,
    roomDemandStatus: bag.roomDemandStatus || null,
    hotelOpportunityThesis: bag.hotelOpportunityThesis || null,
    futureCycleEvidenceState: bag.futureCycleEvidenceState || null,
    futureCycleSignals: bag.futureCycleSignals || [],
    eventLocationSummary: bag.eventLocationSummary || null,
    contactEmail,
    contact: {
      businessEmail: contactEmail,
      personName: bag.contactIsNamedPerson ? (bag.contactName || bag.primaryContactName || null) : null,
      roleTitle: bag.contactRole || bag.primaryContactRole || null,
    },
    officialSource: official,
    evidence,
    investigatedUrls: investigated,
    seedUrls: investigated.filter(Boolean),
    unresolvedQuestions: [
      `Who influences hotel sourcing / housing / contracting at ${org}?`,
      `What programs or next-cycle activity create lodging demand relevant to ${sel.hotelName}?`,
      "Can independent or official sources substantiate commercial activity without inventing RFPs?",
      ...buildBuyerDiscoveryQueries({ organizationName: org, hotelName: sel.hotelName }, sel.hotelName).slice(0, 1),
    ],
    competingHotel: sel.competingHotel || null,
    namedHostHotel: sel.competingHotel || null,
    namedStakeholders: [],
  });
}

/**
 * Honest commercial classification via multidimensional improvement measurement.
 * Named DM / room block not required for MATERIAL; prior contacts never count as new.
 */
export function classifyHardeningLiveCommercialOutcome({
  before,
  result,
  technicalPass,
  relatedExecutedCount = 0,
  newLiveUrls = 0,
  salesBriefClean = false,
  opportunity = null,
} = {}) {
  const assessment = assessCommercialImprovement({
    before,
    after: result.after || {},
    opportunity: opportunity || {
      hotelId: before.hotelId,
      hotelName: before.hotelName,
      contactEmail: before.contactEmail,
      hotelOpportunityThesis: before.description,
      namedStakeholders: before.knownStakeholders,
      competingHotel: before.competingHotel,
      namedHostHotel: before.namedHostHotel,
    },
    result,
    technicalPass,
    salesBriefClean,
  });

  // Attach effort context without letting page counts manufacture MATERIAL
  if (assessment.outcome === IMPROVEMENT_OUTCOME.NO_IMPROVEMENT
    && (relatedExecutedCount >= 1 || newLiveUrls >= 1)) {
    return COMMERCIAL_OUTCOME.LIMITED_IMPROVEMENT;
  }

  if (assessment.outcome === IMPROVEMENT_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT) {
    return COMMERCIAL_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT;
  }
  if (assessment.outcome === IMPROVEMENT_OUTCOME.RESEARCH_BLOCKED) {
    return COMMERCIAL_OUTCOME.RESEARCH_BLOCKED;
  }
  if (assessment.outcome === IMPROVEMENT_OUTCOME.LIMITED_IMPROVEMENT) {
    return COMMERCIAL_OUTCOME.LIMITED_IMPROVEMENT;
  }
  return COMMERCIAL_OUTCOME.NO_IMPROVEMENT;
}

export function assessHardeningLiveCommercialImprovement(args = {}) {
  return assessCommercialImprovement(args);
}

export function buildHardeningLiveValidationPackage() {
  const selections = selectCrossOpportunityCertificationSet();
  const opportunities = selections.map((sel) => {
    const hotelCeiling = HOTEL_BUDGETS_USD[sel.hotelId] || 0.45;
    const oppsAtHotel = selections.filter((s) => s.hotelId === sel.hotelId).length;
    const proposedCeilingUsd = Number((hotelCeiling / oppsAtHotel).toFixed(2));
    return {
      opportunityId: sel.bag.id,
      hotelId: sel.hotelId,
      hotelName: sel.hotelName,
      organizationName: sel.bag.organizationName,
      pathway: sel.pathway,
      role: sel.role,
      proposedCeilingUsd,
      hotelCeilingUsd: hotelCeiling,
      nontransferableHotelBudget: true,
      researchQuestions: [
        `Who influences hotel sourcing / housing / contracting at ${sel.bag.organizationName}?`,
        `What programs or next-cycle activity create lodging demand relevant to ${sel.hotelName}?`,
        "Can independent or official sources substantiate commercial activity without inventing RFPs?",
      ],
    };
  });

  return {
    version: HARDENING_LIVE_VERSION,
    hardeningOfflineVersion: COMMERCIAL_DEPTH_HARDENING_V1,
    status: "PREPARED_AWAITING_EXECUTE",
    executePaidResearch: false,
    totalProposedCeilingUsd: HARDENING_LIVE_AGGREGATE_USD,
    hotelBudgetsUsd: {
      "Wyndham San Diego Bayside": HOTEL_BUDGETS_USD.recnPB3Q02NO8Ma1o,
      "Bethesda Marriott": HOTEL_BUDGETS_USD.recLuxvwwxID7U2B8,
    },
    opportunities,
    authorizedProviders: ["direct_public", "exa_lite_optional", "firecrawl_thin_optional"],
    forbiddenUnlessSeparateGo: ["parallel", "surfe", "pdl", "paid_contact_enrichment"],
    isolatedLedgers: opportunities.map((o) =>
      `${HARDENING_REPORTS_DIR}/live-ledgers/${o.opportunityId}.json`),
    usesPriorResearchBalances: false,
    priorLedgersMustRemainUnchanged: [
      "reports/gdi-deep-research-gdi-integration-v1/live-ledger.json",
      "reports/gdi-evidence-driven-longform-validation-v1/live-ledger.json",
      "reports/gdi-autonomous-longform-gdi-integration-v1/live-ledgers/",
    ],
    publication: "OFF",
    airtableWrites: false,
    weeklyActivation: false,
    GDI_WEEKLY_DEEP_RESEARCH_ENABLED: "0",
    successTarget: "MATERIAL commercial improvement on ≥2/3 pathways with intact safeguards",
  };
}

export function authorizeHardeningLivePackage(packagePath) {
  const abs = path.isAbsolute(packagePath) ? packagePath : path.join(ROOT, packagePath);
  const pkg = JSON.parse(fs.readFileSync(abs, "utf8"));
  const pricing = preflightLiveDeepResearchPricing();
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });

  const hotelBudgetOk = Number(pkg.hotelBudgetsUsd?.["Wyndham San Diego Bayside"]) === 0.45
    && Number(pkg.hotelBudgetsUsd?.["Bethesda Marriott"]) === 0.90;
  const oppCeilingsSum = (pkg.opportunities || [])
    .reduce((s, o) => s + Number(o.proposedCeilingUsd || 0), 0);

  const checks = {
    packagePresent: Boolean(pkg),
    aggregateCeiling: Number(pkg.totalProposedCeilingUsd) === HARDENING_LIVE_AGGREGATE_USD,
    hotelBudgets: hotelBudgetOk,
    opportunityCeilingsWithinAggregate: Math.abs(oppCeilingsSum - HARDENING_LIVE_AGGREGATE_USD) < 0.02,
    threeOpportunities: (pkg.opportunities || []).length === 3,
    isolatedLedgersListed: (pkg.isolatedLedgers || []).length === 3,
    ledgersUnderHardeningDir: (pkg.isolatedLedgers || []).every((p) =>
      String(p).includes("gdi-commercial-depth-hardening-v1/live-ledgers/")),
    noPriorBalances: pkg.usesPriorResearchBalances === false,
    pricingOk: pricing.ok === true,
    surfePdlDisabled: paidGuard.ok === true,
    weeklyOff: pkg.GDI_WEEKLY_DEEP_RESEARCH_ENABLED === "0" || pkg.weeklyActivation === false,
    publicationOff: pkg.publication === "OFF",
  };
  const ok = Object.entries(checks).every(([, v]) => v === true);

  const authorized = {
    ...pkg,
    status: ok ? "AUTHORIZED" : "PREFLIGHT_FAILED",
    authorizedAt: new Date().toISOString(),
    executePaidResearch: ok,
    founderAuthorizedAggregateCeilingUsd: HARDENING_LIVE_AGGREGATE_USD,
    pricingPreflight: { live: pricing.checks, ok: pricing.ok },
    preflightChecks: checks,
    note: ok
      ? "Founder GO for commercial-depth hardening live validation within hotel ceilings."
      : "Preflight failed — do not spend.",
  };
  fs.writeFileSync(abs, `${JSON.stringify(authorized, null, 2)}\n`);
  return { ok, authorized, pricing, checks };
}

export async function executeHardeningLiveOpportunity({
  selection,
  ceilingUsd,
  liveDir,
} = {}) {
  const opportunityId = selection.bag.id;
  const ledgerRel = `${HARDENING_REPORTS_DIR}/live-ledgers/${opportunityId}.json`;
  const ledgerPath = path.join(ROOT, ledgerRel);
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });

  assertLiveDeepResearchLedger(ledgerPath);
  if (String(ledgerPath).replace(/\\/g, "/").endsWith(LIVE_LEDGER_RELATIVE)) {
    throw new Error("hardening_live_must_use_isolated_ledger");
  }
  if (String(ledgerPath).includes("gdi-autonomous-longform-gdi-integration-v1/live-ledgers")) {
    throw new Error("hardening_live_must_not_reuse_prior_pilot_ledgers");
  }

  // Fresh ledger only — refuse non-empty prior spend on this path
  if (fs.existsSync(ledgerPath)) {
    const existing = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
    if (Number(existing.estimatedSpentUsd || 0) > 0) {
      throw new Error(`hardening_live_ledger_already_spent:${opportunityId}`);
    }
  }
  loadOrCreateLiveDeepResearchLedger(ledgerPath, {
    budgetUsd: ceilingUsd,
    version: HARDENING_LIVE_LEDGER_VERSION,
  });

  let opportunity = seedLiveOpportunityFromSelection(selection);
  opportunity.namedStakeholders = stripFixtureStakeholders(opportunity.namedStakeholders);

  const eligibility = evaluateDeepResearchEligibility(opportunity, {
    remainingBudgetUsd: ceilingUsd,
    providersAvailable: true,
  });
  const tier = tierBudgetDefaults(eligibility.tier);
  const before = captureOpportunityBeforeSnapshot(opportunity);
  before.hotelName = opportunity.hotelName;
  before.hotelId = opportunity.hotelId;

  const oppLiveDir = path.join(liveDir, opportunityId);
  fs.mkdirSync(oppLiveDir, { recursive: true });
  fs.writeFileSync(path.join(oppLiveDir, "BEFORE.json"), JSON.stringify(before, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "ELIGIBILITY.json"), JSON.stringify(eligibility, null, 2));

  const researcher = createLiveDeepResearchResearcher({
    ledgerPath,
    additionalSkipUrls: opportunity.investigatedUrls || [],
    allowInvestigatedReread: false,
  });

  const alts = buildAlternativeSourceCandidates(opportunity);
  const result = await runDeepResearchOnExistingGdiOpportunity({
    opportunity,
    researcher,
    corpus: null,
    offlineLongform: false,
    options: {
      allowEarlyBuyerStop: false,
      allowReplan: true,
      allowVoiContinue: true,
      ensureBuyerWho: true,
      resetResearchAttempts: true,
      candidateQueue: opportunity.candidateQueue || alts.map((a) => a.suggestedUrl),
      replanAlternativeCandidates: alts,
      maxIterations: Math.min(5, tier.maxIterations || 5),
      maxLeadFollowUpsPerOpportunity: tier.maxLeadFollowUps || 3,
      remainingBudgetUsd: ceilingUsd,
      checkpointDir: path.join(oppLiveDir, "checkpoints"),
      paidBlocked: true,
      forbidPublication: true,
      forbidAirtable: true,
      authorizeSurfe: false,
      authorizePdl: false,
    },
  });

  if (result.after?.namedStakeholders) {
    result.after.namedStakeholders = stripFixtureStakeholders(result.after.namedStakeholders);
  }
  if (result.after?.opportunity) {
    result.after.opportunity.namedStakeholders = stripFixtureStakeholders(
      result.after.opportunity.namedStakeholders || [],
    );
  }
  if (result.hotelSalesBrief?.whoShouldIContact) {
    const pn = text(result.hotelSalesBrief.whoShouldIContact.namedPerson
      || result.hotelSalesBrief.whoShouldIContact.personName);
    if (FIXTURE_ONLY_NAMES.test(pn)) {
      result.hotelSalesBrief.whoShouldIContact = {
        ...result.hotelSalesBrief.whoShouldIContact,
        namedPerson: null,
        personName: null,
        role: null,
        note: "Fixture identity rejected — use verified org route only",
      };
    }
  }

  const ledger = researcher.getLedger();
  const spentUsd = Number(ledger.estimatedSpentUsd || 0);
  const evidenceBuckets = classifyEvidenceBuckets({
    before,
    afterEvidence: result.after?.opportunity?.evidence || [],
    trace: researcher.trace,
  });

  const newLiveUrls = (researcher.trace.executed || []).filter((e) => !e.heldPreviously);
  // Count only executed related-entity retrievals — not queued/discovered
  const relatedExecuted = (researcher.trace.relatedEntity?.executed || []).length;

  const briefBlob = JSON.stringify(result.hotelSalesBrief || {});
  const salesBriefClean = !PRINCIPAL_NOBU_LEAK.test(briefBlob)
    && (result.hotelSalesBrief?.hotelName === selection.hotelName
      || result.hotelSalesBrief?.hotelId === selection.hotelId
      || result.hotelSalesBrief?.provenanceBound?.hotelId === selection.hotelId);

  const technicalPass =
    result.orsEntrypoint === "matureOpportunity"
    && result.gdiRoundTrip?.sameOpportunityId === true
    && result.publicationEligibilityUnchanged === true
    && spentUsd - ceilingUsd <= 1e-9
    && salesBriefClean;

  const commercialOutcome = classifyHardeningLiveCommercialOutcome({
    before,
    result,
    technicalPass,
    relatedExecutedCount: relatedExecuted,
    newLiveUrls: newLiveUrls.length,
    salesBriefClean,
  });

  const realNamed = stripFixtureStakeholders(result.after?.namedStakeholders || []);
  const roleRelevant = realNamed.filter((s) => hasRoleRelevantDecisionMaker([s]));
  const ownership = result.after?.contactOwnership || {};

  const pathwayNotes = [];
  if (selection.role === "ACCOUNT_LED") {
    pathwayNotes.push("Do not promote nightlife/venue-listing activity into hotel group demand without meeting/guestroom evidence.");
  }
  if (selection.role === "EVENT_LED") {
    pathwayNotes.push("Do not invent overnight demand, sourcing, or room blocks for Potomac Memorial.");
  }
  if (selection.role === "COMPETITIVE_FUTURE_CYCLE") {
    pathwayNotes.push("Crystal Gateway commitment is competitor context — not open Bethesda demand; no overflow inference without evidence.");
  }

  const unanswered = (result.hotelSalesBrief?.whatDoWeStillNotKnow || opportunity.unresolvedQuestions || []);
  const certification = {
    version: HARDENING_LIVE_VERSION,
    opportunityId,
    hotelId: selection.hotelId,
    hotelName: selection.hotelName,
    organizationName: selection.bag.organizationName,
    pathway: selection.pathway,
    role: selection.role,
    eligibility,
    costs: {
      spentUsd,
      remainingCeilingUsd: Number((ceilingUsd - spentUsd).toFixed(6)),
      hardCeilingUsd: ceilingUsd,
      hotelCeilingUsd: HOTEL_BUDGETS_USD[selection.hotelId],
      ledgerPath: ledgerRel,
      nontransferable: true,
      exaCalls: ledger.exaCalls || 0,
      firecrawlCalls: ledger.firecrawlCalls || 0,
      directRetrievals: ledger.directRetrievals || 0,
      entries: ledger.entries || [],
    },
    researchQuestions: opportunity.unresolvedQuestions,
    questionsAnsweredSummary: {
      newLiveUrls: newLiveUrls.map((e) => e.url),
      relatedEntityExecuted: (researcher.trace.relatedEntity?.executed || []).map((e) => e.url),
      relatedEntityQueuedNotCounted: (researcher.trace.relatedEntity?.discovered || []).length,
      exaQueries: researcher.trace.exaQueries || [],
      buyerWhoFallback: result.buyerWhoFallback || result.after?.buyerWhoFallback || null,
      voiContinue: result.voiContinuePass || null,
      replan: result.replanPass || null,
    },
    retrievalTrace: researcher.trace,
    evidenceBuckets,
    realNamedStakeholders: realNamed,
    roleRelevantDecisionMakers: roleRelevant,
    fixtureIdentitiesRejected: ["Jordan Hale", "Chris Delgado", "Riley Chen", "Alex Morgan"],
    contactOwnership: ownership,
    consumerEmailRejected: Boolean(
      ownership.consumerDomain
      || (ownership.route && isConsumerEmailDomain(ownership.route) && !ownership.salesUsableAsProspectContact),
    ),
    targetedEnrichmentRequest: result.targetedEnrichmentRequest || result.after?.targetedEnrichmentRequest || null,
    buyerIntelligence: result.buyerIntelligence || result.after?.publicBuyerContactEnrichment || null,
    relatedEntityExecutedCount: relatedExecuted,
    newLiveUrlsRetrieved: newLiveUrls.length,
    researchCompleteness: result.researchCompleteness || result.after?.researchCompleteness || null,
    technicalStop: result.after?.orsStopReason || null,
    unresolvedCommercialQuestions: unanswered,
    gdiRoundTrip: result.gdiRoundTrip,
    publicationEligibilityUnchanged: result.publicationEligibilityUnchanged,
    salesBriefIntegrity: {
      clean: salesBriefClean,
      hotelBound: result.hotelSalesBrief?.hotelName === selection.hotelName,
      opportunityBound: result.hotelSalesBrief?.opportunityId === opportunityId,
      noPrincipalNobuLeak: !PRINCIPAL_NOBU_LEAK.test(briefBlob),
    },
    pathwayNotes,
    technicalCertification: technicalPass ? "PASS" : "FAIL",
    commercialOutcome,
    hotelSalesBrief: result.hotelSalesBrief,
    remainingResearchFailure: commercialOutcome === COMMERCIAL_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT
      ? null
      : [
        !roleRelevant.length ? "role_relevant_named_buyer_not_established" : null,
        relatedExecuted < 1 ? "related_entity_executed_count_zero_or_low" : null,
        !salesBriefClean ? "sales_brief_integrity_failed" : null,
        newLiveUrls.length < 1 ? "no_new_live_urls" : null,
      ].filter(Boolean),
  };

  fs.writeFileSync(path.join(oppLiveDir, "AFTER.json"), JSON.stringify(result.after, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "SALES_BRIEF.json"), JSON.stringify(result.hotelSalesBrief, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "DOSSIER.json"), JSON.stringify(result.dossierResult || null, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "RETRIEVAL_TRACE.json"), JSON.stringify(researcher.trace, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "CERTIFICATION.json"), JSON.stringify(certification, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "COSTS.json"), JSON.stringify(certification.costs, null, 2));
  if (certification.targetedEnrichmentRequest) {
    fs.writeFileSync(
      path.join(oppLiveDir, "TARGETED_ENRICHMENT_REQUEST.json"),
      JSON.stringify(certification.targetedEnrichmentRequest, null, 2),
    );
  }

  return {
    executed: true,
    certification,
    before,
    after: result.after,
    hotelSalesBrief: result.hotelSalesBrief,
    spentUsd,
  };
}

export async function executeHardeningLiveValidation({
  packagePath = path.join(ROOT, HARDENING_REPORTS_DIR, "LIVE_VALIDATION_PACKAGE.json"),
  liveDir = path.join(ROOT, HARDENING_REPORTS_DIR, "live"),
} = {}) {
  fs.mkdirSync(liveDir, { recursive: true });

  const auth = authorizeHardeningLivePackage(packagePath);
  if (!auth.ok) {
    fs.writeFileSync(path.join(liveDir, "PREFLIGHT_FAILED.json"), JSON.stringify(auth, null, 2));
    return { executed: false, reason: "preflight_failed", auth, spentUsd: 0 };
  }

  const priorLedgers = [
    "reports/gdi-deep-research-gdi-integration-v1/live-ledger.json",
    "reports/gdi-evidence-driven-longform-validation-v1/live-ledger.json",
    "reports/gdi-autonomous-longform-gdi-integration-v1/live-ledgers/gdi_lmc_97fb727376abb83b.json",
    "reports/gdi-autonomous-longform-gdi-integration-v1/live-ledgers/gdi_opp_potomac_memorial_2027.json",
    "reports/gdi-autonomous-longform-gdi-integration-v1/live-ledgers/gdi_opp_nado_ddaa_washcon_2028.json",
  ].map((p) => path.join(ROOT, p));
  const priorSnapshots = priorLedgers
    .filter((p) => fs.existsSync(p))
    .map((p) => ({ path: p, bytes: fs.readFileSync(p) }));

  const selections = selectCrossOpportunityCertificationSet();
  const byId = new Map(selections.map((s) => [s.bag?.id, s]));
  const ordered = auth.authorized.opportunities || [];

  const hotelSpent = {};
  const opportunityResults = [];
  let aggregateSpent = 0;

  for (const oppSpec of ordered) {
    const sel = byId.get(oppSpec.opportunityId);
    if (!sel?.bag) {
      opportunityResults.push({ opportunityId: oppSpec.opportunityId, executed: false, reason: "selection_missing" });
      continue;
    }
    const hotelId = sel.hotelId;
    const hotelCeiling = HOTEL_BUDGETS_USD[hotelId] || oppSpec.hotelCeilingUsd || 0.45;
    const hotelUsed = hotelSpent[hotelId] || 0;
    const hotelRemain = Number((hotelCeiling - hotelUsed).toFixed(6));
    const aggregateRemain = Number((HARDENING_LIVE_AGGREGATE_USD - aggregateSpent).toFixed(6));
    const ceilingUsd = Math.min(Number(oppSpec.proposedCeilingUsd || 0.45), hotelRemain, aggregateRemain);

    if (ceilingUsd < 0.004) {
      opportunityResults.push({
        opportunityId: oppSpec.opportunityId,
        executed: false,
        reason: "hotel_or_aggregate_budget_exhausted",
      });
      continue;
    }

    console.log(JSON.stringify({
      phase: "hardening_live_opportunity",
      opportunityId: oppSpec.opportunityId,
      hotelId,
      ceilingUsd,
      hotelRemain,
      aggregateRemain,
    }));

    const run = await executeHardeningLiveOpportunity({
      selection: sel,
      ceilingUsd,
      liveDir,
    });
    aggregateSpent = Number((aggregateSpent + run.spentUsd).toFixed(6));
    hotelSpent[hotelId] = Number(((hotelSpent[hotelId] || 0) + run.spentUsd).toFixed(6));
    opportunityResults.push(run);

    if (aggregateSpent - HARDENING_LIVE_AGGREGATE_USD > 1e-9) break;
  }

  const priorUnchanged = priorSnapshots.every(({ path: p, bytes }) => {
    try {
      return fs.readFileSync(p).equals(bytes);
    } catch {
      return false;
    }
  });

  const materialCount = opportunityResults.filter((r) =>
    r.certification?.commercialOutcome === COMMERCIAL_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT).length;
  const technicalAllPass = opportunityResults
    .filter((r) => r.executed !== false)
    .every((r) => r.certification?.technicalCertification === "PASS");
  const successTargetMet = materialCount >= 2 && technicalAllPass;

  const remainingFailures = opportunityResults
    .filter((r) => r.certification?.commercialOutcome !== COMMERCIAL_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT)
    .map((r) => ({
      opportunityId: r.certification?.opportunityId || r.opportunityId,
      commercialOutcome: r.certification?.commercialOutcome || null,
      failures: r.certification?.remainingResearchFailure || [r.reason || "not_executed"],
    }));

  const weeklyDecision = successTargetMet
    ? {
      decision: "CONDITIONAL_GO_LIMITED_INTERNAL_WEEKLY_PILOT",
      reason: "MATERIAL on ≥2/3 pathways with safeguards intact. Limited internal weekly pilot only after separate per-hotel weekly budget authorization; publication remains OFF; GDI_WEEKLY_DEEP_RESEARCH_ENABLED stays 0 until ops runbook signed.",
    }
    : {
      decision: "NO_GO",
      reason: `Success target not met (MATERIAL count=${materialCount}/3). Specific remaining failures listed — not an architecture redesign.`,
      remainingFailures,
    };

  const summary = {
    version: HARDENING_LIVE_VERSION,
    certifiedAt: new Date().toISOString(),
    aggregateCeilingUsd: HARDENING_LIVE_AGGREGATE_USD,
    aggregateSpentUsd: aggregateSpent,
    aggregateRemainingUsd: Number((HARDENING_LIVE_AGGREGATE_USD - aggregateSpent).toFixed(6)),
    hotelSpentUsd: hotelSpent,
    priorLedgersUnchanged: priorUnchanged,
    surfeInvoked: false,
    pdlInvoked: false,
    parallelInvoked: false,
    airtableWritePerformed: false,
    publicationOff: true,
    weeklyFlagRemainsZero: true,
    opportunities: opportunityResults.map((r) => ({
      opportunityId: r.certification?.opportunityId || r.opportunityId,
      executed: r.executed !== false,
      spentUsd: r.spentUsd ?? 0,
      technical: r.certification?.technicalCertification || null,
      commercialOutcome: r.certification?.commercialOutcome || null,
      namedBuyers: (r.certification?.realNamedStakeholders || []).map((s) => s.name),
      roleRelevantBuyers: (r.certification?.roleRelevantDecisionMakers || []).map((s) => s.name),
      newLiveUrls: r.certification?.newLiveUrlsRetrieved ?? 0,
      relatedExecuted: r.certification?.relatedEntityExecutedCount ?? 0,
      salesBriefClean: r.certification?.salesBriefIntegrity?.clean ?? null,
      remainingFailures: r.certification?.remainingResearchFailure || null,
    })),
    successTarget: {
      required: "MATERIAL on ≥2/3",
      materialCount,
      met: successTargetMet,
    },
    weeklyInternalPilot: weeklyDecision,
    finalAssessment: {
      technicalAllPass,
      operatesAcrossPathwaysWithoutManualIntervention: technicalAllPass
        && opportunityResults.every((r) => r.executed !== false),
      sufficientlyReliableForLimitedInternalWeeklyPilot: successTargetMet,
      remainingResearchFailures: remainingFailures,
    },
  };

  fs.writeFileSync(path.join(liveDir, "VALIDATION_SUMMARY.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(
    path.join(ROOT, HARDENING_REPORTS_DIR, "GO_NO_GO_LIVE.md"),
    `# Commercial Depth Hardening — Live Validation

| Item | Result |
|---|---|
| Aggregate spent | **$${aggregateSpent}** / $${HARDENING_LIVE_AGGREGATE_USD} |
| MATERIAL count | **${materialCount}/3** |
| Technical (all) | **${technicalAllPass ? "PASS" : "FAIL"}** |
| Prior ledgers unchanged | **${priorUnchanged}** |
| Limited weekly pilot | **${weeklyDecision.decision}** |

${weeklyDecision.reason}

See \`live/VALIDATION_SUMMARY.json\` and per-opportunity \`CERTIFICATION.json\`.
`,
  );

  return {
    executed: true,
    summary,
    aggregateSpentUsd: aggregateSpent,
    opportunityResults,
  };
}
