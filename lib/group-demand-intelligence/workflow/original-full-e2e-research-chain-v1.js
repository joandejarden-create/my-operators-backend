/**
 * Original GDI Full E2E research chain restoration V1.
 *
 * Connects EXISTING modules — does not replace them:
 *   Hotel profile → Discovery candidates → Deep research (ORS) →
 *   Contact research → Commercial qualification → Ten-section presentation →
 *   Durable research-state
 *
 * Paid Exa discovery/deep-research requires explicit authorization + budget.
 * Default mode is fixture/offline/zero-cost continuation (no provider spend).
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { resolveHotelGdiProfileForResearch } from "../hotel-gdi-profile-resolution-v1.js";
import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";
import {
  executeFourteenSegmentDiscoveryMatrix,
} from "./full-e2e-fourteen-segment-discovery-execution-v1.js";
import {
  evaluateDeepResearchEligibility,
  RESEARCH_DEPTH_TIER,
} from "../research/deep-research-eligibility-v1.js";
import {
  runDeepResearchOnExistingGdiOpportunity,
  createFixtureResearcher,
  mergeDeepResearchIntoOpportunity,
} from "../research/deep-research-gdi-integration-v1.js";
import { classifyContactEligibility } from "../contact-eligibility-standard-v1.js";
import {
  qualifyCommercialProspectV41,
  buildHonestSalesBriefV41,
  extractPublicContactIntelligenceForOrs,
} from "../research/commercial-spine-restoration-v41.js";
import { buildSharedTenSectionDrawerDto } from "../demand-segments/shared-ten-section-drawer-contract-v1.js";
import {
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
} from "../research/exa-agent/exa-agent-provider-adapter-v1.js";
import { DISCOVERY_EXECUTION_STATUS } from "../research/segment-discovery-executor-v1.js";
import {
  persistDiscoveryTrace,
  loadDiscoveryState,
  shouldSkipLiveProviderInvocation,
} from "../research/full-e2e-fourteen-segment-discovery-state-v1.js";

export const ORIGINAL_FULL_E2E_RESEARCH_CHAIN_V1 =
  "gdi-original-full-e2e-research-chain-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const CHAIN_STAGES = Object.freeze([
  "HOTEL_PROFILE",
  "DISCOVERY",
  "DEEP_RESEARCH",
  "CONTACT_RESEARCH",
  "COMMERCIAL_QUALIFICATION",
  "SALES_INTELLIGENCE_OUTPUT",
  "PERSISTENCE_AND_CONTINUATION",
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function sha12(s) {
  return createHash("sha1").update(String(s || "")).digest("hex").slice(0, 12);
}

function now() {
  return new Date().toISOString();
}

/**
 * Map a discovery-trace / research-state candidate into an ORS opportunity shape.
 */
export function mapCandidateToOpportunity(candidate = {}, meta = {}) {
  const organizationName =
    text(
      candidate.organizationName ||
        candidate.organizingEntity ||
        candidate.companyName ||
        ""
    ) || null;
  const title =
    text(
      candidate.title ||
        candidate.activityTitle ||
        candidate.programmeName ||
        candidate.eventName ||
        ""
    ) || null;
  const urls = [
    ...(candidate.evidenceUrls || []),
    ...(candidate.urls || []),
    candidate.sourceUrl,
    candidate.url,
  ].filter((u) => /^https?:\/\//i.test(String(u || "")));
  const uniqueUrls = [...new Set(urls.map(String))];
  const id =
    candidate.candidateId ||
    candidate.opportunityId ||
    `cand_${sha12(`${meta.hotelId}|${organizationName}|${title}`)}`;

  return {
    id,
    opportunityId: id,
    hotelId: meta.hotelId || candidate.hotelId,
    hotelName: meta.hotelName || candidate.hotelName || null,
    organizationName,
    title,
    programmeName: title,
    researchSegment: meta.segmentId || candidate.segmentId || null,
    demandType: candidate.demandFamily || candidate.segmentId || meta.segmentId,
    hotelOpportunityThesis:
      candidate.hotelOpportunityThesis ||
      (organizationName && title
        ? `${organizationName} — ${title}: investigate hotel-relevant group / lodging / meetings demand for ${meta.hotelName || "target hotel"}.`
        : null),
    evidence: uniqueUrls.map((url, i) => ({
      evidenceId: `ev_${i}_${sha12(url)}`,
      url,
      statement: text(candidate.activityTitle || candidate.title || organizationName || url).slice(0, 280),
      accepted: true,
      provenance: "discovery_evidence",
    })),
    acceptedClaims: uniqueUrls.map((url, i) => ({
      evidenceId: `ev_${i}_${sha12(url)}`,
      url,
      statement: text(candidate.activityTitle || title || "").slice(0, 280),
      accepted: true,
    })),
    seedUrls: uniqueUrls,
    investigatedUrls: [],
    namedStakeholders: [],
    namedStakeholdersFound: [],
    unresolvedQuestions: [
      "named_buyer_not_verified",
      "sourcing_accessibility_unconfirmed",
    ],
    customerPublicationReady: false,
    publicationOccurred: false,
    airtableWrites: 0,
    discoveryProviderRunId: candidate.providerRunId || null,
    chainSource: "original_full_e2e_research_chain_v1",
  };
}

/**
 * Build a zero-cost offline corpus from candidate evidence statements.
 */
export function buildOfflineCorpusFromOpportunity(opportunity = {}) {
  const corpus = {};
  for (const e of [...(opportunity.evidence || []), ...(opportunity.acceptedClaims || [])]) {
    const url = e?.url;
    if (!url || !/^https?:\/\//i.test(url)) continue;
    const body = [
      opportunity.organizationName,
      opportunity.title || opportunity.programmeName,
      e.statement || e.quote || "",
      "Hotel lodging housing meetings catering sourcing planner contact",
    ]
      .filter(Boolean)
      .join(". ");
    corpus[url] = body;
  }
  if (!Object.keys(corpus).length && opportunity.organizationName) {
    corpus[`https://example.invalid/fixture/${sha12(opportunity.id)}`] =
      `${opportunity.organizationName} ${opportunity.title || ""} hotel demand fixture page`;
  }
  return corpus;
}

function persistOpportunityResearchState(opportunity, extra = {}) {
  const hotelId = opportunity.hotelId;
  if (!hotelId || !opportunity.id) return null;
  const dir = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunity-research-state"
  );
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${opportunity.id}.json`);
  const prior = fs.existsSync(file)
    ? JSON.parse(fs.readFileSync(file, "utf8"))
    : {};
  const next = {
    ...prior,
    ...opportunity,
    ...extra,
    version: ORIGINAL_FULL_E2E_RESEARCH_CHAIN_V1,
    updatedAt: now(),
    customerPublicationReady: false,
    publicationOccurred: false,
    airtableWrites: 0,
    lastSuccessfulCheckpoint: {
      kind: extra.checkpointKind || "FULL_E2E_CHAIN_STEP",
      at: now(),
      stage: extra.stage || null,
    },
  };
  fs.writeFileSync(file, `${JSON.stringify(next, null, 2)}\n`);
  return file;
}

/**
 * Run contact classification against any named stakeholders already present
 * (from deep research fixture extraction or prior seed state). No paid Surfe.
 */
export function runContactResearchPass(opportunity = {}) {
  const people = [
    ...(opportunity.namedStakeholders || []),
    ...(opportunity.namedStakeholdersFound || []),
    ...(opportunity.contacts || []).map((c) => ({
      name: c.name,
      role: c.title || c.role,
      title: c.title || c.role,
      organizationName: c.organization || opportunity.organizationName,
      sourceUrl: c.sourceUrl,
    })),
  ];

  const classified = [];
  for (const person of people) {
    if (!text(person.name)) continue;
    const result = classifyContactEligibility(person, opportunity);
    classified.push({
      name: text(person.name),
      role: text(person.role || person.title || ""),
      class: result.class,
      eligible: result.eligible === true,
      salesReadyContactOk: result.salesReadyContactOk === true,
      reason: result.reason,
      sourceUrl: person.sourceUrl || null,
    });
  }

  // Also attempt public-text extraction from evidence statements (zero-cost).
  for (const e of opportunity.evidence || []) {
    if (!e?.statement) continue;
    try {
      const ci = extractPublicContactIntelligenceForOrs({
        textContent: e.statement,
        url: e.url,
        organizationName: opportunity.organizationName || "",
      });
      for (const p of ci.namedStakeholdersFound || []) {
        if (!text(p.name)) continue;
        if (classified.some((c) => c.name === text(p.name))) continue;
        const result = classifyContactEligibility(p, opportunity);
        classified.push({
          name: text(p.name),
          role: text(p.title || p.role || ""),
          class: result.class,
          eligible: result.eligible === true,
          salesReadyContactOk: result.salesReadyContactOk === true,
          reason: result.reason,
          sourceUrl: e.url || null,
        });
      }
    } catch {
      /* ignore extract failures */
    }
  }

  return {
    contactsInvestigated: classified.length,
    verifiedEligible: classified.filter((c) => c.eligible).length,
    salesReadyContacts: classified.filter((c) => c.salesReadyContactOk).length,
    classifiedContacts: classified,
    contactResearchComplete: true,
    paidContactProvidersUsed: false,
  };
}

/**
 * Process one opportunity through deep research → contacts → qualification → presentation.
 * authorizePaidResearch=false uses offline fixture researcher only ($0).
 */
export async function processOpportunityThroughOriginalChain(
  opportunityInput,
  opts = {}
) {
  const authorizePaidResearch = opts.authorizePaidResearch === true;
  const remainingBudgetUsd = Number(opts.remainingBudgetUsd ?? 0);
  const stages = [];

  let opportunity = { ...opportunityInput };
  stages.push({ stage: "CANDIDATE_MAPPED", at: now(), opportunityId: opportunity.id });

  const eligibility = evaluateDeepResearchEligibility(opportunity, {
    remainingBudgetUsd: authorizePaidResearch ? remainingBudgetUsd : 0,
  });
  stages.push({
    stage: "DEEP_RESEARCH_ELIGIBILITY",
    at: now(),
    decision: eligibility.decision || eligibility.eligibilityDecision,
    tier: eligibility.tier || eligibility.recommendedTier,
  });

  let deepResult = null;
  let deepCostUsd = 0;
  if (opts.skipDeepResearch === true) {
    stages.push({ stage: "DEEP_RESEARCH", skipped: true, reason: "explicit_skip" });
  } else if (!authorizePaidResearch) {
    const corpus = buildOfflineCorpusFromOpportunity(opportunity);
    const researcher = createFixtureResearcher(corpus);
    deepResult = await runDeepResearchOnExistingGdiOpportunity({
      opportunity,
      researcher,
      corpus,
      offlineLongform: true,
      options: {
        paidBlocked: true,
        authorizeSurfe: false,
        authorizePdl: false,
        maxIterations: opts.maxIterations ?? 3,
        remainingBudgetUsd: 0,
        forbidPublication: true,
      },
    });
    opportunity = deepResult.opportunity || opportunity;
    deepCostUsd = Number(deepResult.researchCostUsd || 0);
    stages.push({
      stage: "DEEP_RESEARCH",
      mode: "OFFLINE_FIXTURE_ORS",
      costUsd: deepCostUsd,
      at: now(),
    });
  } else {
    // Paid deep research path — reserved for authorized budget run.
    // Still routes through the same ORS integration; caller must supply live researcher.
    if (!opts.liveResearcher?.execute) {
      stages.push({
        stage: "DEEP_RESEARCH",
        blocked: true,
        reason: "PAID_AUTHORIZED_BUT_LIVE_RESEARCHER_NOT_SUPPLIED",
      });
    } else {
      deepResult = await runDeepResearchOnExistingGdiOpportunity({
        opportunity,
        researcher: opts.liveResearcher,
        offlineLongform: false,
        options: {
          paidBlocked: false,
          remainingBudgetUsd,
          maxIterations: opts.maxIterations ?? 4,
          forbidPublication: true,
        },
      });
      opportunity = deepResult.opportunity || opportunity;
      deepCostUsd = Number(deepResult.researchCostUsd || 0);
      stages.push({
        stage: "DEEP_RESEARCH",
        mode: "AUTHORIZED_PAID",
        costUsd: deepCostUsd,
        at: now(),
      });
    }
  }

  const contactPass = runContactResearchPass(opportunity);
  opportunity.chainContactResearch = contactPass;
  if (contactPass.classifiedContacts.length) {
    opportunity.namedStakeholders = contactPass.classifiedContacts.map((c) => ({
      name: c.name,
      title: c.role,
      role: c.role,
      decisionMakerClass: c.salesReadyContactOk
        ? "VERIFIED_DECISION_MAKER"
        : c.eligible
          ? "POTENTIAL_DECISION_MAKER"
          : "RESEARCH_CANDIDATE",
      sourceUrl: c.sourceUrl,
    }));
  }
  stages.push({
    stage: "CONTACT_RESEARCH",
    investigated: contactPass.contactsInvestigated,
    eligible: contactPass.verifiedEligible,
    at: now(),
  });

  const pageTexts = (opportunity.evidence || []).map(
    (e) => e.statement || e.quote || ""
  );
  const qualification = qualifyCommercialProspectV41({
    opportunity: {
      ...opportunity,
      allowUnresolvedIdentityQualification: true,
    },
    acceptedClaims: opportunity.acceptedClaims || [],
    pageTexts,
    targetHotel: opportunity.hotelName || "",
    pageUrl: (opportunity.seedUrls || [])[0] || null,
  });
  const salesBrief = buildHonestSalesBriefV41({
    targetHotel: opportunity.hotelName || "",
    organizationName: opportunity.organizationName || "",
    activity: opportunity.title || opportunity.programmeName || "",
    disposition: qualification?.internalDisposition || null,
    contactOwnership: qualification?.contactOwnership || null,
    geoFit: qualification?.geoFit || null,
    verifiedFacts: (opportunity.evidence || [])
      .map((e) => e.statement)
      .filter(Boolean)
      .slice(0, 5),
  });
  opportunity.commercialQualificationV41 = qualification;
  opportunity.honestSalesBriefV41 = salesBrief;
  opportunity.internalDisposition =
    qualification?.internalDisposition ||
    opportunity.internalDisposition ||
    null;
  stages.push({
    stage: "COMMERCIAL_QUALIFICATION",
    disposition: opportunity.internalDisposition,
    at: now(),
  });

  const presentation = buildSharedTenSectionDrawerDto(opportunity, {
    hotelId: opportunity.hotelId,
    segmentId: opportunity.researchSegment,
  });
  opportunity.tenSectionDrawer = {
    version: presentation.version,
    sectionIds: presentation.sectionIds,
    sectionTitles: presentation.sectionTitles,
    opportunityId: presentation.opportunityId,
    segmentId: presentation.segmentId,
    hasSections: Boolean(presentation.sections),
  };
  stages.push({
    stage: "SALES_INTELLIGENCE_OUTPUT",
    sections: (presentation.sectionIds || []).length,
    at: now(),
  });

  const statePath = persistOpportunityResearchState(opportunity, {
    stage: "PERSISTENCE_AND_CONTINUATION",
    checkpointKind: "ORIGINAL_FULL_E2E_CHAIN_COMPLETE_STEP",
    chainStages: stages,
    researchCostUsd: deepCostUsd,
    stopReason: authorizePaidResearch
      ? "CHAIN_STEP_COMPLETE_PAID_PATH"
      : "CHAIN_STEP_COMPLETE_ZERO_COST_FIXTURE_PATH",
  });
  stages.push({ stage: "PERSISTENCE_AND_CONTINUATION", path: statePath, at: now() });

  return {
    opportunity,
    deepResult,
    contactPass,
    qualification,
    salesBrief,
    presentation,
    stages,
    researchCostUsd: deepCostUsd,
    commercialResearchComplete: Boolean(
      deepResult && contactPass.contactResearchComplete && qualification
    ),
    customerPublicationReady: false,
    paidSpendUsd: authorizePaidResearch ? deepCostUsd : 0,
  };
}

/**
 * Collect candidates from discovery matrix cells + optional seed inventories.
 */
export function collectRetainedCandidatesFromDiscovery(discoveryExecution, opts = {}) {
  const out = [];
  const seen = new Set();
  const includeFixtureCandidates = opts.includeFixtureCandidates === true;
  for (const cell of discoveryExecution?.cells || []) {
    for (const c of cell.candidates || []) {
      // Deterministic fixture-only hits prove discovery wiring but are not commercial
      // research candidates unless explicitly included.
      if (c.fixtureOnly && !includeFixtureCandidates) continue;
      if (c.unsupportedNameOnly && !c.hasSourceEvidence && !includeFixtureCandidates) {
        continue;
      }
      const opp = mapCandidateToOpportunity(c, {
        hotelId: cell.hotelId,
        hotelName: cell.hotelName,
        segmentId: cell.segmentId,
      });
      if (!opp.organizationName && !opp.title) continue;
      if (seen.has(opp.id)) continue;
      seen.add(opp.id);
      out.push(opp);
    }
  }
  // Optional: include prior inventory / live pilot state files as starting research state
  for (const seed of opts.seedOpportunities || []) {
    const opp = mapCandidateToOpportunity(seed, {
      hotelId: seed.hotelId,
      hotelName: seed.hotelName,
      segmentId: seed.segmentId || seed.researchSegment,
    });
    if (seen.has(opp.id)) continue;
    seen.add(opp.id);
    out.push(opp);
  }
  return out;
}

/**
 * Build bounded three-hotel Full E2E budget manifest (no spend).
 */
export function buildThreeHotelFullE2eBudgetManifest({
  hotelIds = [
    "recCAGqkERSXgiku4",
    "recTk5I8VOzsPhxDz",
    "recXh4NgF8sDbi9cQ",
  ],
  discoveryEffort = EXA_AGENT_EFFORT.MEDIUM,
  deepResearchEffort = EXA_AGENT_EFFORT.MEDIUM,
  discoveryRunsPerSegment = 2,
  deepResearchRunsPerEligibleCandidateEstimate = 1,
  estimatedEligibleCandidatesPerHotel = 25,
  priorLiveSpendUsd = 0.4,
} = {}) {
  const segmentIds = listFourteenWorkflowSegmentIds();
  const discoveryQuote = quoteExaAgentRun({ effort: discoveryEffort });
  const deepQuote = quoteExaAgentRun({ effort: deepResearchEffort });
  const discoveryUnit = Number(discoveryQuote.maxUsd || 0);
  const deepUnit = Number(deepQuote.maxUsd || 0);

  const discoveryCells = hotelIds.length * segmentIds.length; // 42
  const discoveryRuns = discoveryCells * discoveryRunsPerSegment;
  const discoveryMaxUsd = Number((discoveryRuns * discoveryUnit).toFixed(2));

  const deepCandidates =
    hotelIds.length * estimatedEligibleCandidatesPerHotel;
  const deepRuns =
    deepCandidates * deepResearchRunsPerEligibleCandidateEstimate;
  const deepMaxUsd = Number((deepRuns * deepUnit).toFixed(2));

  const contingencyUsd = Number(
    ((discoveryMaxUsd + deepMaxUsd) * 0.15).toFixed(2)
  );
  const hardCeilingUsd = Number(
    (discoveryMaxUsd + deepMaxUsd + contingencyUsd).toFixed(2)
  );

  return {
    version: ORIGINAL_FULL_E2E_RESEARCH_CHAIN_V1,
    at: now(),
    authorizationStatus: "NOT_AUTHORIZED",
    hotelIds,
    segmentIds,
    priorLiveSpendUsd,
    pricingAssumptions: {
      discoveryEffort,
      deepResearchEffort,
      discoveryUnitUsd: discoveryUnit,
      deepResearchUnitUsd: deepUnit,
      note: "Exa Agent fixed-effort list prices; auto/max require separate ceilings. $0.10/run is not guaranteed for all operations.",
      quoteAsOf: discoveryQuote.asOf || null,
    },
    discovery: {
      cells: discoveryCells,
      runsPerSegment: discoveryRunsPerSegment,
      plannedRuns: discoveryRuns,
      maxUsd: discoveryMaxUsd,
      resumeSkipsCompletedLiveCells: true,
    },
    deepResearch: {
      estimatedEligibleCandidates: deepCandidates,
      runsPerCandidate: deepResearchRunsPerEligibleCandidateEstimate,
      plannedRuns: deepRuns,
      maxUsd: deepMaxUsd,
      usesExistingOrsIntegration: true,
      module: "deep-research-gdi-integration-v1.runDeepResearchOnExistingGdiOpportunity",
    },
    contactResearch: {
      paidSurfe: false,
      paidPdl: false,
      mode: "public_evidence_and_ors_extraction",
      estimatedUsd: 0,
    },
    contingencyUsd,
    hardCeilingUsd,
    requestedAuthorizationUsd: hardCeilingUsd,
    stopConditions: [
      "hard_ceiling_reached",
      "quote_unbounded",
      "provider_error_budget",
      "resume_duplicate_live_call_blocked",
      "founder_revoke",
    ],
    excludes: [
      "production_airtable_writes",
      "customer_publication",
      "share_token_changes",
      "railway_deploy",
      "surfe_enrichment",
    ],
    note: "One bounded Full E2E authorization requested — not per-segment micro-approvals. Paid execution must not start until founder authorizes this ceiling.",
  };
}

/**
 * Execute the restored original Full E2E chain for hotels.
 * Default: discovery (historical/fixture unless authorizePaidDiscovery) +
 * offline deep research/contacts/qualification/presentation for retained candidates.
 */
export async function executeOriginalFullE2eResearchChain(opts = {}) {
  const hotelIds = opts.hotelIds || [
    "recCAGqkERSXgiku4",
    "recTk5I8VOzsPhxDz",
    "recXh4NgF8sDbi9cQ",
  ];
  const authorizePaidDiscovery = opts.authorizePaidDiscovery === true;
  const authorizePaidResearch = opts.authorizePaidResearch === true;
  const budgetCeilingUsd = Number(opts.budgetCeilingUsd || 0);
  const maxCandidates =
    opts.maxCandidates == null ? null : Number(opts.maxCandidates);
  const outDir =
    opts.outDir ||
    path.join(ROOT, "reports/gdi-original-full-e2e-research-chain-v1");

  const chainTrace = {
    version: ORIGINAL_FULL_E2E_RESEARCH_CHAIN_V1,
    at: now(),
    stages: [...CHAIN_STAGES],
    hotelIds,
    authorizePaidDiscovery,
    authorizePaidResearch,
    budgetCeilingUsd,
    paidSpendUsd: 0,
    publicationOccurred: false,
    airtableWrites: 0,
  };

  // STAGE 1 — hotel profiles
  const profiles = [];
  for (const hotelId of hotelIds) {
    // eslint-disable-next-line no-await-in-loop
    const resolved = await resolveHotelGdiProfileForResearch(hotelId, {
      skipHpc: opts.skipHpc !== false,
      preferLive: false,
    });
    profiles.push({
      hotelId,
      ok: resolved.ok !== false,
      displayName:
        resolved.hotel?.displayName ||
        resolved.hotel?.name ||
        resolved.profile?.displayName ||
        null,
      city: resolved.hotel?.city || resolved.profile?.city || null,
    });
  }
  chainTrace.hotelProfiles = profiles;

  // STAGE 2 — discovery (existing fourteen-segment matrix)
  const discoveryExecution = await executeFourteenSegmentDiscoveryMatrix({
    hotelIds,
    authorizePaidDiscovery,
    useFixtureProvider: opts.useFixtureProvider !== false && !authorizePaidDiscovery,
    allowHistoricalReplay: opts.allowHistoricalReplay !== false,
    persistState: opts.persistState !== false,
    runQualificationHandoff: false, // replaced by full chain below
  });
  chainTrace.discovery = {
    cellCount: discoveryExecution.summary?.cellCount || 0,
    extracted: discoveryExecution.summary?.totalExtractedCandidates || 0,
    costUsd: discoveryExecution.summary?.totalResearchCostUsd || 0,
    freshRetrievalCells: discoveryExecution.summary?.freshRetrievalCells || 0,
  };
  chainTrace.paidSpendUsd += Number(chainTrace.discovery.costUsd || 0);

  let opportunities = collectRetainedCandidatesFromDiscovery(discoveryExecution, {
    seedOpportunities: opts.seedOpportunities || [],
  });
  // No 10–15 cap. Optional safety bound only when explicitly set.
  if (maxCandidates != null && Number.isFinite(maxCandidates) && maxCandidates > 0) {
    opportunities = opportunities.slice(0, maxCandidates);
    chainTrace.candidateCapApplied = maxCandidates;
  } else {
    chainTrace.candidateCapApplied = null;
  }
  chainTrace.retainedCandidates = opportunities.length;

  // STAGES 3–7 per candidate
  const processed = [];
  let remaining =
    authorizePaidResearch && budgetCeilingUsd > 0
      ? Math.max(0, budgetCeilingUsd - chainTrace.paidSpendUsd)
      : 0;

  for (const opp of opportunities) {
    if (authorizePaidResearch && remaining <= 0) {
      processed.push({
        opportunityId: opp.id,
        blocked: true,
        reason: "BUDGET_EXHAUSTED",
      });
      continue;
    }
    // eslint-disable-next-line no-await-in-loop
    const result = await processOpportunityThroughOriginalChain(opp, {
      authorizePaidResearch,
      remainingBudgetUsd: remaining,
      skipDeepResearch: opts.skipDeepResearch === true,
      maxIterations: opts.maxIterations,
      liveResearcher: opts.liveResearcher,
    });
    chainTrace.paidSpendUsd += Number(result.paidSpendUsd || 0);
    remaining = Math.max(0, remaining - Number(result.paidSpendUsd || 0));
    processed.push({
      opportunityId: result.opportunity.id,
      organizationName: result.opportunity.organizationName,
      segmentId: result.opportunity.researchSegment,
      disposition: result.opportunity.internalDisposition,
      contactsInvestigated: result.contactPass.contactsInvestigated,
      salesReadyContacts: result.contactPass.salesReadyContacts,
      presentationSections: (result.presentation.sectionIds || []).length,
      commercialResearchComplete: result.commercialResearchComplete,
      paidSpendUsd: result.paidSpendUsd,
      stages: result.stages.map((s) => s.stage),
    });
  }

  // Update discovery state cells: qualification is underway, not discovery-complete
  for (const hotelId of hotelIds) {
    for (const segmentId of listFourteenWorkflowSegmentIds()) {
      const related = processed.filter(
        (p) =>
          p.segmentId === segmentId &&
          opportunities.find((o) => o.id === p.opportunityId)?.hotelId === hotelId
      );
      if (!related.length) continue;
      persistDiscoveryTrace(
        {
          hotelId,
          segmentId,
          researchRunId: opts.researchRunId || "original_full_e2e_chain",
          executionStatus: DISCOVERY_EXECUTION_STATUS.QUALIFICATION_PARTIAL,
          retrievalMode: authorizePaidResearch
            ? "AUTHORIZED_CHAIN"
            : "ZERO_COST_CHAIN",
          providerCallCount: 0,
          freshRetrievals: 0,
          researchCostUsd: 0,
          stopReason:
            "ORIGINAL_FULL_E2E_CHAIN_DEEP_RESEARCH_CONTACTS_QUALIFICATION_EXECUTED",
          candidates: related.map((r) => ({
            candidateId: r.opportunityId,
            organizationName: r.organizationName,
            hasSourceEvidence: true,
            stage: "CHAIN_COMPLETE_STEP",
            disposition: r.disposition,
          })),
          discoveryCompleted: false,
        },
        { researchRunId: opts.researchRunId || "original_full_e2e_chain" }
      );
    }
  }

  const budget = buildThreeHotelFullE2eBudgetManifest({
    hotelIds,
    priorLiveSpendUsd: opts.priorLiveSpendUsd ?? 0.4,
  });

  const summary = {
    ...chainTrace,
    processedCount: processed.filter((p) => !p.blocked).length,
    blockedCount: processed.filter((p) => p.blocked).length,
    withPresentation: processed.filter((p) => (p.presentationSections || 0) >= 10)
      .length,
    withContactInvestigation: processed.filter(
      (p) => (p.contactsInvestigated || 0) > 0
    ).length,
    commercialResearchCompleteClaimAllowed: false,
    fullE2eCompleteClaimAllowed: false,
    fullE2eCompleteClaimBlockedReason: authorizePaidDiscovery && authorizePaidResearch
      ? "Paid path executed only if authorized; COMPLETE still requires founder verification of coverage/horizon/contacts"
      : "Fixture/zero-cost chain proves module connectivity — live Exa Full E2E requires authorized budget",
    processed,
    budgetManifest: budget,
  };

  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "chain-execution-trace.json"),
    `${JSON.stringify(summary, null, 2)}\n`
  );
  fs.writeFileSync(
    path.join(outDir, "budget-manifest.json"),
    `${JSON.stringify(budget, null, 2)}\n`
  );

  return summary;
}

export default {
  ORIGINAL_FULL_E2E_RESEARCH_CHAIN_V1,
  CHAIN_STAGES,
  mapCandidateToOpportunity,
  buildOfflineCorpusFromOpportunity,
  processOpportunityThroughOriginalChain,
  collectRetainedCandidatesFromDiscovery,
  buildThreeHotelFullE2eBudgetManifest,
  executeOriginalFullE2eResearchChain,
  shouldSkipLiveProviderInvocation,
  loadDiscoveryState,
};
