/**
 * Full Commercial Deep Research — controlled live pilot ($1.25 aggregate).
 * Founder-authorized ceilings: Alexandria $0.55 · PRA $0.45 · AAD $0.25.
 * Isolated ledgers under reports/gdi-full-commercial-deep-research-validation-v1/live-ledgers/
 * Surfe/PDL/Parallel OFF · Airtable OFF · publication OFF · weekly = 0.
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
  buildAlternativeSourceCandidates,
  buildBuyerDiscoveryQueries,
  hasRoleRelevantDecisionMaker,
} from "./autonomous-deep-research-recovery-v1.js";
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
import {
  FULL_COMMERCIAL_DEEP_RESEARCH_VALIDATION_V1,
  FULL_COMMERCIAL_REPORTS_DIR,
  FULL_COMMERCIAL_LIVE_LEDGER_VERSION,
  loadCanonicalCohortOpportunity,
  buildSourcingChainAnalysis,
  loadBuyerIntelligenceAudit,
} from "./full-commercial-deep-research-validation-v1.js";
import { FINDING_PROVENANCE } from "./buyer-intelligence-shortlist-offline-v1.js";

export const FULL_COMMERCIAL_LIVE_V1 = "gdi-full-commercial-deep-research-live-v1";
export const FULL_COMMERCIAL_LIVE_AGGREGATE_USD = 1.25;

export const OPPORTUNITY_CEILINGS_USD = Object.freeze({
  gdi_opp_alexandria_soccer_kickoff_2027: 0.55,
  gdi_lmc_4d2f10eab2d44499: 0.45,
  gdi_opp_aad_2028_overflow: 0.25,
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

function demoteUnverifiedBenHawkins(named = [], brief = {}) {
  const nextNamed = (named || []).map((p) => {
    if (!/ben\s+hawkins/i.test(p?.name || "")) return p;
    const hasLiveEmail = Boolean(p.email) && !isConsumerEmailDomain(p.email);
    const liveSource = /live|firecrawl|direct_public|exa/i.test(String(p.sourceUrl || p.provenance || p.contactDataOrigin || ""));
    if (hasLiveEmail && liveSource) return p;
    return {
      ...p,
      decisionMakerClass: "UNVERIFIED_PRIOR_INFERENCE",
      salesUsableIdentity: false,
      verifiedDecisionMaker: false,
      findingProvenance: FINDING_PROVENANCE.UNVERIFIED,
      note: "Ben Hawkins remains UNVERIFIED without live-confirmed email/ownership — not promoted as verified buyer",
    };
  });
  const nextBrief = { ...brief };
  // Always rewrite Ben-Hawkins asks unless already demoted — "Do not invent an RFP" must not block demotion
  if (/ben\s+hawkins/i.test(nextBrief.whatShouldISayOrAsk || "")
    && !/do not pitch ben hawkins|ben hawkins remains unverified|not a verified decision-maker/i.test(
      nextBrief.whatShouldISayOrAsk || "",
    )) {
    nextBrief.whatShouldISayOrAsk =
      "Do not pitch Ben Hawkins as a verified decision-maker. Pursue tournament/housing office or stay-to-play partner inventory path; ask who owns hotel list eligibility for Kickoff. Do not invent an RFP, room block, or current Bethesda Marriott award.";
  }
  if (nextBrief.whoShouldIContact && /ben\s+hawkins/i.test(nextBrief.whoShouldIContact.namedPerson || "")) {
    nextBrief.whoShouldIContact = {
      ...nextBrief.whoShouldIContact,
      namedPerson: null,
      role: null,
      decisionMakerClass: null,
      contactKind: nextBrief.whoShouldIContact.route ? "GENERAL_ORG_ROUTE" : "UNRESOLVED",
      note: "Ben Hawkins demoted — UNVERIFIED without live confirmation",
    };
  }
  // Named ask must not promote functional entities as person DMs
  if (nextBrief.whoShouldIContact?.namedPerson
    && !/^[A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+)+$/.test(text(nextBrief.whoShouldIContact.namedPerson))) {
    nextBrief.whoShouldIContact = {
      ...nextBrief.whoShouldIContact,
      namedPerson: null,
      role: null,
      decisionMakerClass: null,
      contactKind: nextBrief.whoShouldIContact.route ? "GENERAL_ORG_ROUTE" : "FUNCTIONAL_ENTITY_OR_UNRESOLVED",
      note: "Functional entity / intermediary is not a named individual decision-maker",
    };
    if (/ask\s+maritz|ask\s+aad meetings/i.test(nextBrief.whatShouldISayOrAsk || "")) {
      nextBrief.whatShouldISayOrAsk = nextBrief.whoShouldIContact.route
        ? `Use ${nextBrief.whoShouldIContact.route} to reach the official housing / registration channel; ask who owns 2028 overflow / housing list inclusion. Do not invent Bethesda host or overflow demand.`
        : nextBrief.whatShouldISayOrAsk;
    }
  }
  return { named: nextNamed, brief: nextBrief };
}

function isLikelyPersonName(name) {
  const n = text(name);
  if (!n || n.length < 3) return false;
  if (/association|academy|events|housing|provider|channel|tournaments|inc\.|llc|global|maritz|staff|tbd|meetings\s*\/\s*housing/i.test(n)) {
    return false;
  }
  return /^[A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+)+$/.test(n);
}

/**
 * Authorize the prepared live package after founder GO.
 */
export function authorizeFullCommercialLivePackage(packagePath, {
  founderAggregateUsd = FULL_COMMERCIAL_LIVE_AGGREGATE_USD,
  founderCeilings = OPPORTUNITY_CEILINGS_USD,
} = {}) {
  const abs = path.isAbsolute(packagePath) ? packagePath : path.join(ROOT, packagePath);
  const pkg = JSON.parse(fs.readFileSync(abs, "utf8"));
  const pricing = preflightLiveDeepResearchPricing();
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });

  const oppSum = (pkg.opportunities || [])
    .reduce((s, o) => s + Number(o.proposedCeilingUsd || 0), 0);
  const ceilingsMatch = (pkg.opportunities || []).every((o) =>
    Math.abs(Number(o.proposedCeilingUsd) - Number(founderCeilings[o.opportunityId])) < 0.001);

  const checks = {
    packagePresent: Boolean(pkg),
    cohortThree: (pkg.cohort || []).length === 3,
    aggregateCeiling: Math.abs(Number(pkg.totalProposedCeilingUsd) - founderAggregateUsd) < 0.02
      && Math.abs(oppSum - founderAggregateUsd) < 0.02,
    opportunityCeilingsMatchFounderGo: ceilingsMatch,
    isolatedLedgersListed: (pkg.isolatedLedgers || []).length === 3,
    ledgersUnderFullCommercialDir: (pkg.isolatedLedgers || []).every((p) =>
      String(p).includes("gdi-full-commercial-deep-research-validation-v1/live-ledgers/")),
    noPriorBalances: pkg.usesPriorResearchBalances === false,
    pricingOk: pricing.ok === true,
    surfePdlDisabled: paidGuard.ok === true,
    weeklyOff: pkg.GDI_WEEKLY_DEEP_RESEARCH_ENABLED === "0" || pkg.weeklyActivation === false,
    publicationOff: pkg.publication === "OFF",
    airtableOff: pkg.airtableWrites === false,
    allowedProvidersOnly: (pkg.allowedProviders || []).every((p) =>
      ["exa", "firecrawl", "direct_public"].includes(p)),
    surfePdlParallelForbidden: (pkg.forbiddenUnlessSeparateGo || []).includes("surfe")
      && (pkg.forbiddenUnlessSeparateGo || []).includes("pdl")
      && (pkg.forbiddenUnlessSeparateGo || []).includes("parallel"),
  };
  const ok = Object.values(checks).every((v) => v === true);

  const authorized = {
    ...pkg,
    status: ok ? "AUTHORIZED" : "PREFLIGHT_FAILED",
    authorizedAt: new Date().toISOString(),
    executePaidResearch: ok,
    founderAuthorizedAggregateCeilingUsd: founderAggregateUsd,
    founderCeilingsUsd: { ...founderCeilings },
    pricingPreflight: { live: pricing.checks, ok: pricing.ok },
    preflightChecks: checks,
    note: ok
      ? "Founder GO for full commercial deep-research live pilot within nontransferable opportunity ceilings."
      : "Preflight failed — do not spend.",
  };
  fs.writeFileSync(abs, `${JSON.stringify(authorized, null, 2)}\n`);
  return { ok, authorized, pricing, checks };
}

function seedLiveOpportunity(opportunityId) {
  const opportunity = loadCanonicalCohortOpportunity(opportunityId);
  // Live: do not pre-mark URLs investigated — allow live fetch; held bag evidence stays previouslyHeld
  const seeded = {
    ...opportunity,
    investigatedUrls: [],
    researchAttempts: [],
    relatedEntityLeadsExecuted: 0,
    namedStakeholders: stripFixtureStakeholders(opportunity.namedStakeholders || []),
    pendingDiscoverySearches: buildAlternativeSourceCandidates(opportunity).slice(0, 6),
    candidateQueue: [
      ...(opportunity.seedUrls || []),
      ...buildAlternativeSourceCandidates(opportunity).map((a) => a.suggestedUrl),
    ].filter(Boolean).slice(0, 10),
    unresolvedQuestions: [
      ...(opportunity.unresolvedQuestions || []),
      ...buildBuyerDiscoveryQueries(opportunity, opportunity.hotelName).slice(0, 2),
    ],
  };
  return seeded;
}

/**
 * Execute one live opportunity within its nontransferable ceiling.
 */
export async function executeFullCommercialLiveOpportunity({
  opportunityId,
  ceilingUsd,
  liveDir,
  buyerAssessment = null,
} = {}) {
  const authorizedCeiling = OPPORTUNITY_CEILINGS_USD[opportunityId];
  if (authorizedCeiling == null) throw new Error(`unknown_cohort_opportunity:${opportunityId}`);
  if (Math.abs(Number(ceilingUsd) - authorizedCeiling) > 0.001) {
    throw new Error(`ceiling_mismatch:${opportunityId}:got=${ceilingUsd}:expected=${authorizedCeiling}`);
  }

  const ledgerRel = `${FULL_COMMERCIAL_REPORTS_DIR}/live-ledgers/${opportunityId}.json`;
  const ledgerPath = path.join(ROOT, ledgerRel);
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });

  assertLiveDeepResearchLedger(ledgerPath);
  if (String(ledgerPath).replace(/\\/g, "/").endsWith(LIVE_LEDGER_RELATIVE)) {
    throw new Error("full_commercial_live_must_use_isolated_ledger");
  }
  for (const banned of [
    "gdi-autonomous-longform-gdi-integration-v1/live-ledgers",
    "gdi-commercial-depth-hardening-v1/live-ledgers",
    "gdi-deep-research-gdi-integration-v1/live-ledger",
    "gdi-evidence-driven-longform-validation-v1/live-ledger",
  ]) {
    if (String(ledgerPath).replace(/\\/g, "/").includes(banned)) {
      throw new Error(`full_commercial_live_must_not_reuse_prior_ledger:${banned}`);
    }
  }

  if (fs.existsSync(ledgerPath)) {
    const existing = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
    if (Number(existing.estimatedSpentUsd || 0) > 0) {
      throw new Error(`full_commercial_live_ledger_already_spent:${opportunityId}`);
    }
  }
  loadOrCreateLiveDeepResearchLedger(ledgerPath, {
    budgetUsd: ceilingUsd,
    version: FULL_COMMERCIAL_LIVE_LEDGER_VERSION,
  });

  let opportunity = seedLiveOpportunity(opportunityId);
  const eligibility = evaluateDeepResearchEligibility(opportunity, {
    remainingBudgetUsd: ceilingUsd,
    providersAvailable: true,
  });
  const tier = tierBudgetDefaults(eligibility.tier);
  const before = captureOpportunityBeforeSnapshot(opportunity);
  before.hotelName = opportunity.hotelName;
  before.hotelId = opportunity.hotelId;
  before.contactRoutes = [
    ...(before.contactRoutes || []),
    opportunity.contactEmail,
    opportunity.primaryContactEmail,
    opportunity.primaryContact?.email,
  ].filter(Boolean);

  const oppLiveDir = path.join(liveDir, opportunityId);
  fs.mkdirSync(oppLiveDir, { recursive: true });
  fs.writeFileSync(path.join(oppLiveDir, "BEFORE.json"), JSON.stringify(before, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "ELIGIBILITY.json"), JSON.stringify(eligibility, null, 2));

  const researcher = createLiveDeepResearchResearcher({
    ledgerPath,
    additionalSkipUrls: [],
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

  // Strip fixtures; demote Ben Hawkins without live confirmation
  if (result.after?.namedStakeholders) {
    result.after.namedStakeholders = stripFixtureStakeholders(result.after.namedStakeholders);
  }
  if (result.after?.opportunity?.namedStakeholders) {
    result.after.opportunity.namedStakeholders = stripFixtureStakeholders(
      result.after.opportunity.namedStakeholders,
    );
  }
  if (result.buyerIntelligence?.namedStakeholders) {
    result.buyerIntelligence.namedStakeholders = stripFixtureStakeholders(
      result.buyerIntelligence.namedStakeholders,
    );
  }

  const demoted = demoteUnverifiedBenHawkins(
    result.after?.namedStakeholders || [],
    result.hotelSalesBrief || {},
  );
  if (result.after) result.after.namedStakeholders = demoted.named;
  result.hotelSalesBrief = demoted.brief;

  const ledger = researcher.getLedger();
  const spentUsd = Number(ledger.estimatedSpentUsd || 0);
  if (spentUsd - ceilingUsd > 1e-6) {
    throw new Error(`budget_exceeded:${opportunityId}:spent=${spentUsd}:ceiling=${ceilingUsd}`);
  }

  const evidenceBuckets = classifyEvidenceBuckets({
    before,
    afterEvidence: result.after?.opportunity?.evidence || [],
    trace: researcher.trace,
  });

  const newLiveUrls = (researcher.trace.executed || []).filter((e) => !e.heldPreviously);
  const relatedExecuted = (researcher.trace.relatedEntity?.executed || []).length;

  const briefBlob = JSON.stringify(result.hotelSalesBrief || {});
  const salesBriefClean = !PRINCIPAL_NOBU_LEAK.test(briefBlob)
    && result.hotelSalesBrief?.hotelName === opportunity.hotelName
    && result.hotelSalesBrief?.hotelId === opportunity.hotelId
    && result.hotelSalesBrief?.opportunityId === opportunityId;

  const technicalPass =
    result.orsEntrypoint === "matureOpportunity"
    && result.gdiRoundTrip?.sameOpportunityId === true
    && result.gdiRoundTrip?.sameHotelId === true
    && result.publicationEligibilityUnchanged === true
    && result.gdiRoundTrip?.airtableWritePerformed !== true
    && spentUsd - ceilingUsd <= 1e-9
    && salesBriefClean;

  const commercialAssessment = assessCommercialImprovement({
    before,
    after: result.after || {},
    opportunity,
    result,
    technicalPass,
    salesBriefClean,
  });

  // Reject MATERIAL if novel evidence is only offline_fixture provenance (should not happen live)
  const dimEvidence = Object.values(commercialAssessment.dimensions || {}).flatMap((d) => d?.evidence || []);
  const fixtureLeak = dimEvidence.some((e) => /offline_fixture|OFFLINE_FIXTURE/i.test(String(e.provenance || "")));
  let commercialOutcome = commercialAssessment.outcome;
  if (fixtureLeak && commercialOutcome === IMPROVEMENT_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT) {
    commercialOutcome = IMPROVEMENT_OUTCOME.LIMITED_IMPROVEMENT;
    commercialAssessment.fixtureLeakDowngrade = true;
  }

  const realNamed = stripFixtureStakeholders(result.after?.namedStakeholders || []);
  const roleRelevant = realNamed.filter((s) =>
    isLikelyPersonName(s?.name || s?.personName)
    && hasRoleRelevantDecisionMaker([s])
    && !/UNVERIFIED/i.test(s.decisionMakerClass || "")
    && s.findingProvenance !== FINDING_PROVENANCE.UNVERIFIED);
  const functionalIntermediaries = realNamed.filter((s) =>
    !isLikelyPersonName(s?.name || s?.personName)
    && /housing|registration|tournament|meetings|provider|maritz/i.test(
      `${s?.name || ""} ${s?.title || ""}`,
    ));
  const ownership = result.after?.contactOwnership || {};

  const sourcingChain = buildSourcingChainAnalysis({
    opportunity,
    before,
    result,
    buyerIntelAssessment: buyerAssessment,
  });
  // Tag live retrievals in sourcing evidence
  sourcingChain.liveRetrievalUrls = newLiveUrls.map((e) => e.url);
  sourcingChain.liveRelatedEntityUrls = (researcher.trace.relatedEntity?.executed || []).map((e) => e.url);
  sourcingChain.findingProvenanceNote =
    "Live retrievals are VERIFIED_LIVE candidates only when sourced from executed URLs; prior bag claims remain VERIFIED_FROM_STORED_EVIDENCE or UNVERIFIED.";

  const unanswered = result.hotelSalesBrief?.whatDoWeStillNotKnow
    || opportunity.unresolvedQuestions
    || [];

  const certification = {
    version: FULL_COMMERCIAL_LIVE_V1,
    opportunityId,
    hotelId: opportunity.hotelId,
    hotelName: opportunity.hotelName,
    organizationName: opportunity.organizationName,
    pathway: opportunity.pathway,
    eligibility,
    costs: {
      spentUsd,
      remainingCeilingUsd: Number((ceilingUsd - spentUsd).toFixed(6)),
      hardCeilingUsd: ceilingUsd,
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
    sourcingChain,
    realNamedStakeholders: realNamed,
    roleRelevantDecisionMakers: roleRelevant,
    functionalIntermediaries,
    fixtureIdentitiesRejected: ["Jordan Hale", "Chris Delgado", "Riley Chen", "Alex Morgan"],
    benHawkinsPolicy: opportunityId === "gdi_opp_alexandria_soccer_kickoff_2027"
      ? "UNVERIFIED unless live email+ownership established — demoted if not"
      : null,
    contactOwnership: ownership,
    consumerEmailRejected: Boolean(
      ownership.consumerDomain
      || (ownership.route && isConsumerEmailDomain(ownership.route) && !ownership.salesUsableAsProspectContact),
    ),
    targetedEnrichmentRequest: result.targetedEnrichmentRequest || result.after?.targetedEnrichmentRequest || null,
    buyerIntelligence: result.buyerIntelligence || null,
    relatedEntityExecutedCount: relatedExecuted,
    newLiveUrlsRetrieved: newLiveUrls.length,
    researchCompleteness: result.researchCompleteness || result.after?.researchCompleteness || null,
    technicalStop: result.after?.orsStopReason || null,
    unresolvedCommercialQuestions: unanswered,
    gdiRoundTrip: result.gdiRoundTrip,
    publicationEligibilityUnchanged: result.publicationEligibilityUnchanged,
    salesBriefIntegrity: {
      clean: salesBriefClean,
      hotelBound: result.hotelSalesBrief?.hotelName === opportunity.hotelName,
      opportunityBound: result.hotelSalesBrief?.opportunityId === opportunityId,
      noPrincipalNobuLeak: !PRINCIPAL_NOBU_LEAK.test(briefBlob),
    },
    commercialImprovement: {
      ...commercialAssessment,
      outcome: commercialOutcome,
    },
    technicalCertification: technicalPass ? "PASS" : "FAIL",
    commercialOutcome,
    hotelSalesBrief: result.hotelSalesBrief,
    mode: "live",
    simulatedVsLive: "LIVE_RETRIEVAL",
    remainingResearchFailure: commercialOutcome === IMPROVEMENT_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT
      ? null
      : [
        !roleRelevant.length ? "role_relevant_named_buyer_not_established" : null,
        relatedExecuted < 1 ? "related_entity_executed_count_zero_or_low" : null,
        !salesBriefClean ? "sales_brief_integrity_failed" : null,
        newLiveUrls.length < 1 ? "no_new_live_urls" : null,
        commercialOutcome === IMPROVEMENT_OUTCOME.NO_IMPROVEMENT ? "no_commercial_dimension_advanced" : null,
      ].filter(Boolean),
  };

  fs.writeFileSync(path.join(oppLiveDir, "AFTER.json"), JSON.stringify(result.after, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "SALES_BRIEF.json"), JSON.stringify(result.hotelSalesBrief, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "DOSSIER.json"), JSON.stringify(result.dossierResult || result.contract?.dossier || null, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "RETRIEVAL_TRACE.json"), JSON.stringify(researcher.trace, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "SOURCING_CHAIN.json"), JSON.stringify(sourcingChain, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "COMMERCIAL_IMPROVEMENT.json"), JSON.stringify(certification.commercialImprovement, null, 2));
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

function diagnoseLimitedRootCause(cert = {}) {
  const failures = cert.remainingResearchFailure || [];
  const ownership = cert.contactOwnership || {};
  const completeness = cert.researchCompleteness || {};
  if (failures.includes("role_relevant_named_buyer_not_established")
    && !ownership.salesUsableAsProspectContact) {
    return "buyer_accessibility";
  }
  if (failures.includes("role_relevant_named_buyer_not_established")) {
    return "sourcing_chain_intelligence";
  }
  if (failures.includes("no_new_live_urls") || failures.includes("related_entity_executed_count_zero_or_low")) {
    return "research_strategy_or_public_evidence";
  }
  if (/INSUFFICIENT|PARTIAL|GAP/i.test(String(completeness.researchCompleteness || completeness.status || ""))) {
    return "available_public_evidence";
  }
  if (cert.commercialOutcome === IMPROVEMENT_OUTCOME.NO_IMPROVEMENT) {
    return "opportunity_quality_or_public_evidence";
  }
  return "available_public_evidence";
}

/**
 * Execute the full three-opportunity live pilot.
 */
export async function executeFullCommercialLivePilot({
  packagePath = path.join(ROOT, FULL_COMMERCIAL_REPORTS_DIR, "LIVE_AUTHORIZATION_PACKAGE.json"),
  liveDir = path.join(ROOT, FULL_COMMERCIAL_REPORTS_DIR, "live"),
} = {}) {
  fs.mkdirSync(liveDir, { recursive: true });

  const auth = authorizeFullCommercialLivePackage(packagePath);
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
    "reports/gdi-commercial-depth-hardening-v1/live-ledgers/gdi_lmc_97fb727376abb83b.json",
    "reports/gdi-commercial-depth-hardening-v1/live-ledgers/gdi_opp_potomac_memorial_2027.json",
    "reports/gdi-commercial-depth-hardening-v1/live-ledgers/gdi_opp_nado_ddaa_washcon_2028.json",
  ].map((p) => path.join(ROOT, p));
  const priorSnapshots = priorLedgers
    .filter((p) => fs.existsSync(p))
    .map((p) => ({ path: p, bytes: fs.readFileSync(p) }));

  let buyerIntel = null;
  try {
    buyerIntel = loadBuyerIntelligenceAudit();
  } catch {
    buyerIntel = null;
  }

  const ordered = auth.authorized.opportunities || [];
  const opportunityResults = [];
  let aggregateSpent = 0;

  for (const oppSpec of ordered) {
    const ceiling = OPPORTUNITY_CEILINGS_USD[oppSpec.opportunityId];
    if (ceiling == null) {
      opportunityResults.push({
        opportunityId: oppSpec.opportunityId,
        executed: false,
        reason: "not_in_founder_ceiling_map",
      });
      continue;
    }
    if (aggregateSpent + ceiling > FULL_COMMERCIAL_LIVE_AGGREGATE_USD + 1e-6) {
      opportunityResults.push({
        opportunityId: oppSpec.opportunityId,
        executed: false,
        reason: "aggregate_would_exceed",
      });
      break;
    }

    const buyerAssessment = (buyerIntel?.assessments || [])
      .find((a) => a.opportunityId === oppSpec.opportunityId) || null;

    // eslint-disable-next-line no-await-in-loop
    const run = await executeFullCommercialLiveOpportunity({
      opportunityId: oppSpec.opportunityId,
      ceilingUsd: ceiling,
      liveDir,
      buyerAssessment,
    });

    aggregateSpent = Number((aggregateSpent + run.spentUsd).toFixed(6));
    opportunityResults.push(run);

    if (aggregateSpent > FULL_COMMERCIAL_LIVE_AGGREGATE_USD + 1e-6) {
      throw new Error(`aggregate_exceeded_after:${oppSpec.opportunityId}:${aggregateSpent}`);
    }
  }

  const priorLedgersUnchanged = priorSnapshots.every((s) => {
    if (!fs.existsSync(s.path)) return false;
    return Buffer.compare(fs.readFileSync(s.path), s.bytes) === 0;
  });

  const materialCount = opportunityResults.filter((r) =>
    r.certification?.commercialOutcome === IMPROVEMENT_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT).length;
  const limitedCount = opportunityResults.filter((r) =>
    r.certification?.commercialOutcome === IMPROVEMENT_OUTCOME.LIMITED_IMPROVEMENT).length;
  const technicalAllPass = opportunityResults.every((r) =>
    r.certification?.technicalCertification === "PASS");

  const rootCauses = opportunityResults
    .filter((r) => r.certification
      && r.certification.commercialOutcome !== IMPROVEMENT_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT)
    .map((r) => ({
      opportunityId: r.certification.opportunityId,
      commercialOutcome: r.certification.commercialOutcome,
      rootCauseClass: diagnoseLimitedRootCause(r.certification),
      failures: r.certification.remainingResearchFailure || [],
    }));

  const summary = {
    version: FULL_COMMERCIAL_LIVE_V1,
    offlinePackageVersion: FULL_COMMERCIAL_DEEP_RESEARCH_VALIDATION_V1,
    executedAt: new Date().toISOString(),
    aggregateCeilingUsd: FULL_COMMERCIAL_LIVE_AGGREGATE_USD,
    aggregateSpentUsd: aggregateSpent,
    remainingAggregateUsd: Number((FULL_COMMERCIAL_LIVE_AGGREGATE_USD - aggregateSpent).toFixed(6)),
    priorLedgersUnchanged,
    surfePdlParallel: "OFF",
    airtableWrites: false,
    publication: "OFF",
    GDI_WEEKLY_DEEP_RESEARCH_ENABLED: "0",
    opportunities: opportunityResults.map((r) => ({
      opportunityId: r.certification?.opportunityId || r.opportunityId,
      hotelId: r.certification?.hotelId,
      hotelName: r.certification?.hotelName,
      organizationName: r.certification?.organizationName,
      spentUsd: r.spentUsd,
      ceilingUsd: r.certification?.costs?.hardCeilingUsd,
      remainingUsd: r.certification?.costs?.remainingCeilingUsd,
      technical: r.certification?.technicalCertification,
      commercialOutcome: r.certification?.commercialOutcome,
      materialDimensions: r.certification?.commercialImprovement?.materialDimensionCount,
      limitedDimensions: r.certification?.commercialImprovement?.limitedDimensionCount,
      roleRelevantBuyers: (r.certification?.roleRelevantDecisionMakers || []).map((p) => p.name),
      contactRoute: r.certification?.contactOwnership?.route || null,
      relatedExecuted: r.certification?.relatedEntityExecutedCount,
      newLiveUrls: r.certification?.newLiveUrlsRetrieved,
      salesBriefClean: r.certification?.salesBriefIntegrity?.clean,
      enrichmentStatus: r.certification?.targetedEnrichmentRequest?.status || null,
      researchCompleteness: r.certification?.researchCompleteness?.researchCompleteness
        || r.certification?.researchCompleteness?.status
        || null,
      stopReason: r.certification?.technicalStop || null,
    })),
    successTarget: {
      materialCount,
      limitedCount,
      met: materialCount >= 1,
      note: "Success target: MATERIAL on ≥1/3 with honest LIMITED elsewhere",
    },
    finalAssessment: {
      technicalAllPass,
      engineProducesMateriallyBetterSalesIntelligence: materialCount >= 1,
      ifLimitedRootCauses: rootCauses,
      weeklyInternalPilot: {
        decision: "NO_GO",
        reason: "Live pilot complete — weekly activation remains OFF pending separate founder review; do not auto-promote.",
      },
    },
  };

  fs.writeFileSync(path.join(liveDir, "SUMMARY.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(path.join(liveDir, "AUTHORIZED_PACKAGE.json"), JSON.stringify(auth.authorized, null, 2));

  return {
    executed: true,
    summary,
    opportunityResults,
    auth,
  };
}
