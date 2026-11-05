/**
 * Bethesda Weekly Net-New Opportunity Discovery V1
 *
 * Corrects the Full-Demand Challenge mission:
 * - Baseline inventory (≈103 source records) is starting intelligence, NOT the deliverable.
 * - Weekly success = ~10–15 NET_NEW commercially actionable opportunities (aspirational).
 * - Parallel stream: refresh / reactivate / enrich existing opportunities.
 * - Uses existing commercial-demand-discovery-v4 + ORS deep research — no new engine.
 *
 * Offline only in this module. $9 aggregate is a proposed ceiling, not spend authorization.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "./research-constants-v1.js";
import { RESEARCH_SEGMENT_LABEL } from "../workflow/hotel-segment-research-plan-v1.js";
import {
  generateHotelDemandDiscoveryStrategies,
  loadHotelGdiConfig,
  dedupeDiscoveryAgainstMemory,
  normalizeDiscoveryHit,
} from "./commercial-demand-discovery-v4.js";
import { preflightLiveDeepResearchPricing } from "./deep-research-gdi-live-certification-v1.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
  BETHESDA_CHALLENGE_BUDGET_USD,
  BETHESDA_FULL_DEMAND_CHALLENGE_V1,
  BETHESDA_FULL_DEMAND_REPORTS_DIR,
  CHANGE_LEDGER_DECISION,
  buildOpportunityResolutionKey,
  normalizeOrganizationIdentity,
  normalizeProgramIdentity,
  extractProgramIdentity,
  extractCycleIdentity,
  extractGeoIdentity,
  extractSourcingContext,
  inventoryExistingEvidence,
  resolveAgainstExisting,
  runBethesdaDatabaseReconciliationPreflight,
  RESOLUTION_KIND,
  loadBethesdaGdiSourceCorpus,
} from "./bethesda-full-demand-database-reconciliation-v1.js";

export const BETHESDA_WEEKLY_NET_NEW_V1 = "gdi-bethesda-weekly-net-new-discovery-v1";

/** Weekly finding classification — distinct from reconciliation NEW_OPPORTUNITY (memory recovery). */
export const WEEKLY_FINDING_CLASS = Object.freeze({
  NET_NEW_OPPORTUNITY: "NET_NEW_OPPORTUNITY",
  REACTIVATED_OPPORTUNITY: "REACTIVATED_OPPORTUNITY",
  EXISTING_ENRICHED: "EXISTING_ENRICHED",
  EXISTING_UNCHANGED: "EXISTING_UNCHANGED",
  DUPLICATE: "DUPLICATE",
  EXCLUDED: "EXCLUDED",
});

export const WEEKLY_TARGET = Object.freeze({
  netNewActionableMin: 10,
  netNewActionableMax: 15,
  aspirational: true,
  notAQuota: true,
  note: "Target ≈10–15 net-new per hotel per weekly cycle — report actual yield honestly; do not pad",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function oppId(o = {}) {
  return text(o.id || o.opportunityId || o.canonicalOpportunityId) || null;
}

function loadProfile() {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    BETHESDA_CANONICAL_HOTEL_ID,
    "profile.json",
  );
  if (!fs.existsSync(p)) return {};
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Build immutable baseline identity manifest from reconciled corpus.
 * Research-memory recoveries are PREVIOUSLY_KNOWN — never NET_NEW market discoveries.
 */
export function buildBethesdaImmutableBaselineManifest({
  reconciliation = null,
  root = ROOT,
} = {}) {
  const recon = reconciliation || runBethesdaDatabaseReconciliationPreflight({ root });
  const corpus = loadBethesdaGdiSourceCorpus({ root });

  // Index every source record that belongs in the baseline (canonical + memory + weekly)
  const sourceRecords = [];
  const pushRecord = (o, provenance) => {
    const id = oppId(o);
    if (!id) return;
    sourceRecords.push({
      sourceRecordId: id,
      organizationName: o.organizationName || o.company || null,
      organizationKey: normalizeOrganizationIdentity(o.organizationName || o.company),
      programIdentity: extractProgramIdentity(o) || null,
      programKey: normalizeProgramIdentity(extractProgramIdentity(o)),
      opportunityResolutionKey: buildOpportunityResolutionKey(o),
      cycle: extractCycleIdentity(o),
      geography: extractGeoIdentity(o) || null,
      sourcingContext: extractSourcingContext(o),
      segment: o.researchSegment || o.segment || o.primarySegment || null,
      opportunityType: o.opportunityType || null,
      researchStatus: o.researchStatus || null,
      qualificationState: o.disposition || o.researchLayerDisposition || o.demandStatus || null,
      customerVisible: o.customerVisible,
      contact: {
        name: o.contactName || o.primaryContactName || o.primaryContact?.name || null,
        email: o.contactEmail || o.primaryContactEmail || o.primaryContact?.email || null,
        url: o.contactUrl || o.organizationContactUrl || o.publicContactPath || null,
      },
      evidence: inventoryExistingEvidence(o),
      lastInvestigatedAt: o.updatedAt || o.lastMaterialChangeAt || o.lastContactResearchAt || o.startedAt || null,
      provenance,
      isResearchMemoryRecovery: provenance.bag === "research-candidates.json",
      isCanonicalBag: provenance.bag === "opportunities.json",
    });
  };

  for (const o of corpus.canonical) {
    pushRecord(o, { bag: "opportunities.json", class: "canonical" });
  }
  for (const o of corpus.researchCandidates) {
    pushRecord(o, { bag: "research-candidates.json", class: "research_memory_previously_known" });
  }
  // Weekly staged — baseline-known if already in bags; still index IDs for dedupe
  const seenIds = new Set(sourceRecords.map((r) => r.sourceRecordId));
  for (const o of corpus.weeklyOpps) {
    const id = oppId(o);
    if (!id || seenIds.has(id)) continue;
    seenIds.add(id);
    pushRecord(o, { bag: o._sourceBag || "weekly-staged", class: "weekly_staged" });
  }

  const byResolutionKey = new Map();
  for (const r of sourceRecords) {
    const key = r.opportunityResolutionKey;
    if (!byResolutionKey.has(key)) byResolutionKey.set(key, []);
    byResolutionKey.get(key).push(r.sourceRecordId);
  }

  const uniqueOpportunities = [...byResolutionKey.entries()].map(([key, ids]) => {
    const primary = sourceRecords.find((r) => r.sourceRecordId === ids[0]);
    return {
      opportunityResolutionKey: key,
      primarySourceRecordId: ids[0],
      linkedSourceRecordIds: ids,
      organizationName: primary?.organizationName || null,
      organizationKey: primary?.organizationKey || null,
      programIdentity: primary?.programIdentity || null,
      segment: primary?.segment || null,
      cycle: primary?.cycle || null,
      hasResearchMemoryLink: ids.some((id) => {
        const row = sourceRecords.find((r) => r.sourceRecordId === id);
        return row?.isResearchMemoryRecovery === true;
      }),
    };
  });

  const orgs = new Map();
  for (const r of sourceRecords) {
    if (!r.organizationKey) continue;
    if (!orgs.has(r.organizationKey)) {
      orgs.set(r.organizationKey, {
        organizationKey: r.organizationKey,
        displayName: r.organizationName,
        opportunityKeys: new Set(),
        sourceRecordIds: [],
      });
    }
    const org = orgs.get(r.organizationKey);
    org.opportunityKeys.add(r.opportunityResolutionKey);
    org.sourceRecordIds.push(r.sourceRecordId);
  }

  const researchMemoryRecovered = sourceRecords.filter((r) => r.isResearchMemoryRecovery);
  const reconNewFromMemory = (recon.changeLedger || []).filter(
    (e) => e.decision === CHANGE_LEDGER_DECISION.NEW_OPPORTUNITY
      && String(e.sourceBag || "").includes("research-candidates"),
  );

  const baselineOpportunityIds = new Set(uniqueOpportunities.map((u) => u.primarySourceRecordId));
  const baselineResolutionKeys = new Set(uniqueOpportunities.map((u) => u.opportunityResolutionKey));
  const baselineOrgKeys = new Set([...orgs.keys()]);
  const baselineProgramKeys = new Set(
    sourceRecords.map((r) => `${r.organizationKey}::${r.programKey || "_account"}`).filter(Boolean),
  );

  const manifest = {
    version: BETHESDA_WEEKLY_NET_NEW_V1,
    manifestKind: "IMMUTABLE_BETHESDA_WEEKLY_BASELINE",
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    frozenAt: new Date().toISOString(),
    immutable: true,
    challengeVersion: BETHESDA_FULL_DEMAND_CHALLENGE_V1,
    counts: {
      sourceRecords: sourceRecords.length,
      distinctOrganizations: orgs.size,
      distinctCanonicalOpportunities: uniqueOpportunities.length,
      duplicateLinkedSourceRecords: sourceRecords.length - uniqueOpportunities.length,
      researchMemoryPreviouslyKnownRecords: researchMemoryRecovered.length,
      researchMemoryMustNotCountAsNetNew: researchMemoryRecovered.length,
      reconciliationNewOpportunityDecisionsThatAreNotMarketNetNew: reconNewFromMemory.length,
    },
    corrections: {
      sourceRecordsAreNotUniqueOpportunities: true,
      researchMemoryRecoveriesArePreviouslyKnown: true,
      researchMemoryRecoveriesAreNotNetNewMarketDiscoveries: true,
      selectingTenToFifteenFromBaselineIsNotWeeklySuccess: true,
    },
    indexes: {
      baselineOpportunityIds: [...baselineOpportunityIds],
      baselineResolutionKeys: [...baselineResolutionKeys],
      baselineOrganizationKeys: [...baselineOrgKeys],
      baselineOrgProgramKeys: [...baselineProgramKeys],
    },
    distinctOrganizations: [...orgs.values()].map((o) => ({
      organizationKey: o.organizationKey,
      displayName: o.displayName,
      distinctOpportunityCount: o.opportunityKeys.size,
      sourceRecordCount: o.sourceRecordIds.length,
    })),
    distinctOpportunities: uniqueOpportunities,
    sourceRecords,
    knownOrganizationsForDiscoveryMemory: [...orgs.values()].map((o) => o.displayName).filter(Boolean),
    knownProgramsForDiscoveryMemory: sourceRecords
      .map((r) => r.programIdentity)
      .filter(Boolean),
  };

  return manifest;
}

/**
 * Classify a discovery/enrichment finding against the immutable baseline.
 */
export function classifyFindingAgainstBethesdaBaseline(finding = {}, baseline = null) {
  const manifest = baseline || buildBethesdaImmutableBaselineManifest();
  const orgKey = normalizeOrganizationIdentity(finding.organizationName || finding.company);
  const programKey = normalizeProgramIdentity(
    finding.programName || finding.eventName || extractProgramIdentity(finding),
  );
  const resolutionKey = buildOpportunityResolutionKey({
    organizationName: finding.organizationName || finding.company,
    eventName: finding.programName || finding.eventName || extractProgramIdentity(finding),
    eventYear: finding.eventYear || extractCycleIdentity(finding).year,
    eventLocationSummary: finding.geography || finding.eventLocationSummary,
  });

  const id = oppId(finding);

  // Research-memory provenance must never be labeled market net-new (check before soft-match)
  if (finding._corpusSource === "research_memory"
    || finding.provenance?.class === "research_memory_previously_known"
    || finding.isResearchMemoryRecovery === true) {
    return {
      class: WEEKLY_FINDING_CLASS.DUPLICATE,
      resolutionKey,
      matchedBaselineId: id || null,
      reason: "research_memory_recovery_is_previously_known_not_net_new",
      countsAsNetNew: false,
    };
  }

  if (id && manifest.indexes.baselineOpportunityIds.includes(id)) {
    if (finding.materialNewEvidence === true || finding.newlyActionable === true) {
      return {
        class: WEEKLY_FINDING_CLASS.REACTIVATED_OPPORTUNITY,
        resolutionKey,
        matchedBaselineId: id,
        reason: "baseline_id_with_material_new_actionability",
        countsAsNetNew: false,
      };
    }
    if (finding.enriched === true || finding.improvedContact === true || finding.improvedResearch === true) {
      return {
        class: WEEKLY_FINDING_CLASS.EXISTING_ENRICHED,
        resolutionKey,
        matchedBaselineId: id,
        reason: "baseline_id_enriched_not_net_new",
        countsAsNetNew: false,
      };
    }
    return {
      class: WEEKLY_FINDING_CLASS.EXISTING_UNCHANGED,
      resolutionKey,
      matchedBaselineId: id,
      reason: "baseline_id_no_material_change",
      countsAsNetNew: false,
    };
  }

  if (manifest.indexes.baselineResolutionKeys.includes(resolutionKey)) {
    return {
      class: WEEKLY_FINDING_CLASS.DUPLICATE,
      resolutionKey,
      matchedBaselineId: manifest.distinctOpportunities.find((u) => u.opportunityResolutionKey === resolutionKey)?.primarySourceRecordId || null,
      reason: "resolution_key_already_in_baseline",
      countsAsNetNew: false,
    };
  }

  // Soft match via resolveAgainstExisting using baseline distinct opportunity primaries
  const existingPool = manifest.sourceRecords
    .filter((r) => r.isCanonicalBag || r.isResearchMemoryRecovery)
    .map((r) => ({
      id: r.sourceRecordId,
      organizationName: r.organizationName,
      eventName: r.programIdentity,
      eventYear: r.cycle?.year,
      eventLocationSummary: r.geography,
      opportunityType: r.opportunityType,
    }));

  const resolution = resolveAgainstExisting({
    ...finding,
    eventName: finding.programName || finding.eventName,
  }, existingPool);

  if (resolution.resolutionKind === RESOLUTION_KIND.DUPLICATE) {
    return {
      class: WEEKLY_FINDING_CLASS.DUPLICATE,
      resolutionKey,
      matchedBaselineId: resolution.matchOpportunityId,
      reason: resolution.reason,
      countsAsNetNew: false,
    };
  }

  if (resolution.resolutionKind === RESOLUTION_KIND.EXISTING_WITH_NEW_EVIDENCE) {
    return {
      class: finding.newlyActionable === true
        ? WEEKLY_FINDING_CLASS.REACTIVATED_OPPORTUNITY
        : WEEKLY_FINDING_CLASS.EXISTING_ENRICHED,
      resolutionKey,
      matchedBaselineId: resolution.matchOpportunityId,
      reason: resolution.reason,
      countsAsNetNew: false,
    };
  }

  if (finding.excluded === true || finding.insufficientDemand === true || finding.hotelFit === "poor") {
    return {
      class: WEEKLY_FINDING_CLASS.EXCLUDED,
      resolutionKey,
      matchedBaselineId: null,
      reason: finding.excludeReason || "insufficient_demand_or_poor_fit",
      countsAsNetNew: false,
    };
  }

  // Genuinely new org OR new program under known org with distinct cycle → NET_NEW
  if (
    resolution.resolutionKind === RESOLUTION_KIND.GENUINELY_NEW
    || resolution.resolutionKind === RESOLUTION_KIND.NEW_PROGRAM_EXISTING_ORG
  ) {
    const hasDemandSignal = Boolean(
      finding.programName || finding.eventName || finding.demandSignalSummary || finding.demandMechanism,
    );
    if (!hasDemandSignal && !finding.allowAccountLedNetNew) {
      return {
        class: WEEKLY_FINDING_CLASS.EXCLUDED,
        resolutionKey,
        matchedBaselineId: null,
        reason: "no_specific_demand_mechanism",
        countsAsNetNew: false,
      };
    }
    return {
      class: WEEKLY_FINDING_CLASS.NET_NEW_OPPORTUNITY,
      resolutionKey,
      matchedBaselineId: null,
      relatedBaselineId: resolution.matchOpportunityId || null,
      reason: resolution.reason || "not_in_baseline",
      countsAsNetNew: true,
      orgWasKnown: resolution.resolutionKind === RESOLUTION_KIND.NEW_PROGRAM_EXISTING_ORG,
    };
  }

  return {
    class: WEEKLY_FINDING_CLASS.EXCLUDED,
    resolutionKey,
    matchedBaselineId: null,
    reason: "unclassified_or_insufficient",
    countsAsNetNew: false,
  };
}

/**
 * Build discovery memory object for V4 dedupe against baseline.
 */
export function buildDiscoveryMemoryFromBaseline(baseline = null) {
  const manifest = baseline || buildBethesdaImmutableBaselineManifest();
  return {
    knownOrganizations: manifest.knownOrganizationsForDiscoveryMemory,
    investigatedOrganizations: manifest.knownOrganizationsForDiscoveryMemory,
    knownPrograms: manifest.knownProgramsForDiscoveryMemory,
    discoveryIds: manifest.indexes.baselineOpportunityIds,
    baselineResolutionKeys: manifest.indexes.baselineResolutionKeys,
  };
}

/**
 * Eleven-segment fresh discovery plan — mandatory every cycle; beyond baseline.
 */
export function buildBethesdaElevenSegmentFreshDiscoveryPlan({
  baseline = null,
  maxPerSegment = 3,
  productiveSegmentExtraQueries = 1,
} = {}) {
  const manifest = baseline || buildBethesdaImmutableBaselineManifest();
  const config = loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID) || {};
  const profile = loadProfile();
  const strategyPack = generateHotelDemandDiscoveryStrategies({
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
    city: "Bethesda",
    country: "US",
    demandTerritory: profile.demandTerritory || config.demandTerritory,
    commercialPriorities: profile.commercialPriorityConfiguration || config.commercialPriorities,
    capabilityProfile: {
      totalGuestrooms: profile.guestrooms?.totalGuestrooms,
      meetingRooms: profile.meetingCapability?.meetingRooms,
      largestBallroomSqFt: profile.meetingCapability?.ballroomSqFt,
    },
  }, { maxPerSegment });

  // Productivity bias from baseline segment density (more effort where historically productive)
  const orgSegCounts = {};
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) orgSegCounts[seg] = 0;
  for (const u of manifest.distinctOpportunities) {
    const seg = u.segment;
    if (seg && orgSegCounts[seg] != null) orgSegCounts[seg] += 1;
  }
  const productive = SUPERVISOR_EXPECTED_SEGMENTS
    .filter((s) => (orgSegCounts[s] || 0) >= 3)
    .slice(0, 4);

  const bySegment = {};
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) {
    const strategies = (strategyPack.strategies || []).filter((s) => s.segment === seg);
    const extra = productive.includes(seg)
      ? [
        {
          segment: seg,
          researchQuestion: `Bethesda Marriott ${RESEARCH_SEGMENT_LABEL[seg] || seg} NEW programs OR 2026 OR 2027 OR 2028 hotel housing OR room block -exclude known inventory`,
          demandSeeking: true,
          bonusForProductiveSegment: true,
        },
      ].slice(0, productiveSegmentExtraQueries)
      : [];
    bySegment[seg] = {
      segmentId: seg,
      label: RESEARCH_SEGMENT_LABEL[seg] || seg,
      baselineOpportunityCount: orgSegCounts[seg] || 0,
      productiveBias: productive.includes(seg),
      mandatoryFreshDiscovery: true,
      stopDiscoveryBecauseBaselineHasCandidates: false,
      strategies: [...strategies.map((s) => s.researchQuestion), ...extra.map((e) => e.researchQuestion)],
      expectedSignalKinds: ["PROGRAM_OR_EVENT", "ORGANIZATION_CAPABILITY", "RECURRING_CYCLE"],
      yieldTracking: {
        rawHits: null,
        afterBaselineDedupe: null,
        netNewCandidates: null,
        deepResearchEligible: null,
        actionableNetNew: null,
      },
    };
  }

  return {
    version: BETHESDA_WEEKLY_NET_NEW_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    allElevenSegmentsCovered: SUPERVISOR_EXPECTED_SEGMENTS.every((s) => bySegment[s].strategies.length > 0),
    equalVolumeNotRequired: true,
    productiveSegmentsExtraEffort: productive,
    strategyCount: Object.values(bySegment).reduce((n, s) => n + s.strategies.length, 0),
    discoveryEntrypoint: "runCommercialDemandDiscoveryCycleV4 + generateHotelDemandDiscoveryStrategies",
    deepResearchEntrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
    newInternalCandidateEntrypoint: "normalizeDiscoveryHit → enqueueDiscoveryIntoOrsQueue → matureOpportunity",
    mustSearchBeyondBaseline: true,
    bySegment,
  };
}

/**
 * Discovery → screen → deep-research funnel plan (volume sized for ~10–15 yield after attrition).
 */
export function buildBethesdaDiscoveryToDeepResearchFunnel({
  weeklyTarget = WEEKLY_TARGET,
} = {}) {
  // Honest attrition assumptions for planning (not guarantees)
  const planned = {
    freshDiscoveryStrategies: 36,
    expectedRawHits: 80,
    afterBaselineDedupeUnique: 45,
    afterCommercialScreen: 28,
    deepResearchEligible: 20,
    expectedActionableNetNewRange: [weeklyTarget.netNewActionableMin, weeklyTarget.netNewActionableMax],
    expectedAttritionNote: "Plan volume large enough that ~10–15 remain after honest attrition; do not pad finals",
  };

  const screenCriteria = [
    "Credible demand mechanism",
    "Bethesda hotel fit",
    "Potential guestroom / meeting / catering revenue",
    "Commercial timing",
    "Sourcing accessibility",
    "Program specificity",
    "Researchability",
    "Buyer/contact potential",
  ];

  return {
    version: BETHESDA_WEEKLY_NET_NEW_V1,
    operatingStreams: {
      netNewDiscovery: {
        primary: true,
        successMetric: "actionable_net_new_yield",
        target: weeklyTarget,
      },
      existingOpportunityIntelligence: {
        primary: false,
        includes: [
          WEEKLY_FINDING_CLASS.REACTIVATED_OPPORTUNITY,
          WEEKLY_FINDING_CLASS.EXISTING_ENRICHED,
          WEEKLY_FINDING_CLASS.EXISTING_UNCHANGED,
        ],
      },
    },
    plannedVolume: planned,
    screenCriteria,
    doNotManufactureDemand: true,
    preserveHonestFunnelAttrition: true,
    deepResearch: {
      entrypointExisting: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
      entrypointNewInternal: "ORS queue after normalizeDiscoveryHit / enqueueDiscoveryIntoOrsQueue",
      gapOnlyIncremental: true,
      avoidUnnecessaryProviderCalls: true,
    },
    hotelFacingAcceptance: [
      "Specific organization + demand mechanism",
      "Credible demand evidence with inference boundaries",
      "Defensible Bethesda fit",
      "Legitimate buyer/contact route (named preferred)",
      "Salesperson knows exact next action",
      "Would save research time or reveal missed business",
    ],
  };
}

/**
 * Empty weekly output scaffold — filled during live execution.
 */
export function buildEmptyWeeklyOutputsScaffold() {
  return {
    A_newlyActionableNetNew: {
      target: WEEKLY_TARGET,
      items: [],
      count: 0,
      note: "Do not fill with baseline leads or duplicate sourcing cycles",
    },
    B_reactivated: {
      items: [],
      count: 0,
    },
    C_enrichedExisting: {
      items: [],
      count: 0,
    },
    D_excludedOrDeferred: {
      items: [],
      count: 0,
    },
    secondaryMetrics: {
      reactivatedCount: 0,
      enrichedCount: 0,
      buyerContactCompleteness: null,
      researchQuality: null,
      segmentCoverage: null,
      salesUsefulness: null,
      costPerActionableNetNewUsd: null,
    },
    primarySuccessMetric: "new_credible_contactable_opportunity_yield",
  };
}

/**
 * Proposed research budget allocation — ceiling not authorization.
 */
export function buildBethesdaWeeklyNetNewBudgetAllocation({ pricing = null } = {}) {
  const pricingLive = pricing || preflightLiveDeepResearchPricing();
  const quotes = pricingLive.checks?.quotes || {};
  const exa = Number(quotes.exaLiteSearchUsd) || 0.004;
  const fc = Number(quotes.firecrawlScrapeUsd) || 0.005;

  const discoveryStrategies = 36;
  const discoveryScrapes = 45;
  const deepNetNewCandidates = 18;
  const deepExistingRefresh = 4;

  const estDiscovery = Number(((discoveryStrategies * exa) + (discoveryScrapes * fc)).toFixed(2));
  const estDeep = Number(
    (((deepNetNewCandidates + deepExistingRefresh)
      * ((3 * exa) + (6 * fc))).toFixed(2)),
  );

  return {
    status: "PROPOSED_MAXIMUM_NOT_AUTHORIZATION",
    executePaidResearch: false,
    aggregateCeilingUsd: BETHESDA_CHALLENGE_BUDGET_USD.aggregateCeilingUsd,
    allocationUsd: {
      discoveryPhaseMax: BETHESDA_CHALLENGE_BUDGET_USD.discoveryPhaseUsd,
      deepResearchPhaseMax: BETHESDA_CHALLENGE_BUDGET_USD.deepResearchPhaseUsd,
      perOpportunityDeepResearchMax: BETHESDA_CHALLENGE_BUDGET_USD.perOpportunityDeepResearchMaxUsd,
      preferredSplit: {
        netNewDiscoveryAndDeepResearch: 7.0,
        existingRefreshReactivate: 2.0,
      },
    },
    estimatedSpendUsd: {
      discovery: Math.min(estDiscovery, BETHESDA_CHALLENGE_BUDGET_USD.discoveryPhaseUsd),
      deepResearch: Math.min(estDeep, BETHESDA_CHALLENGE_BUDGET_USD.deepResearchPhaseUsd),
    },
    unitCostsUsd: { exaLiteSearch: exa, firecrawlScrape: fc },
    guards: {
      perOpportunityMaxUsd: BETHESDA_CHALLENGE_BUDGET_USD.perOpportunityDeepResearchMaxUsd,
      aggregateMaxUsd: BETHESDA_CHALLENGE_BUDGET_USD.aggregateCeilingUsd,
      stopEarlyWhenCommerciallyComplete: true,
      doNotBurnUnusedCeiling: true,
      isolatedLedger: `${BETHESDA_FULL_DEMAND_REPORTS_DIR}/live-ledgers/bethesda-weekly-net-new.json`,
      usesPriorResearchBalances: false,
    },
    forbiddenUnlessSeparateGo: ["surfe", "pdl", "parallel"],
    weeklyActivation: false,
    GDI_WEEKLY_DEEP_RESEARCH_ENABLED: "0",
    airtableWrites: false,
    publication: "OFF",
    pricingPreflight: pricingLive,
  };
}

/**
 * Offline classify a batch of mock/live hits through baseline gate + V4 memory dedupe.
 */
export function runOfflineNetNewDedupeGateCertification({
  baseline = null,
  mockHits = null,
} = {}) {
  const manifest = baseline || buildBethesdaImmutableBaselineManifest();
  const memory = buildDiscoveryMemoryFromBaseline(manifest);

  const fixtures = mockHits || [
    {
      organizationName: "Completely Novel Bethesda Biotech Consortium",
      programName: "2027 Regional Translational Science Summit",
      eventYear: 2027,
      demandSignalSummary: "annual scientific meeting seeking hotel housing near NIH",
      segment: "medical_academic",
    },
    {
      organizationName: "Aledade",
      programName: "Aledade lodging opportunity",
      demandSignalSummary: "corporate account",
      isResearchMemoryRecovery: true,
      _corpusSource: "research_memory",
    },
    {
      organizationName: "Bethesda Soccer Club",
      programName: "Bethesda Premier Cup",
      eventYear: 2026,
      demandSignalSummary: "tournament lodging",
    },
    {
      organizationName: "National Association of REALTORS® (NAR)",
      id: manifest.indexes.baselineOpportunityIds[0],
      enriched: true,
      improvedContact: true,
    },
    {
      organizationName: "Unknown Weak Listing Corp",
      excluded: true,
      excludeReason: "directory_listing_only_no_demand",
    },
  ];

  const normalized = fixtures.map((h) => normalizeDiscoveryHit(h, {
    hotel: { hotelId: BETHESDA_CANONICAL_HOTEL_ID, displayName: BETHESDA_CANONICAL_HOTEL_NAME },
    strategy: { segment: h.segment || "medical_academic" },
  }));

  const { unique, duplicates } = dedupeDiscoveryAgainstMemory(normalized, memory);
  const classified = fixtures.map((f) => ({
    organizationName: f.organizationName,
    programName: f.programName || f.eventName || null,
    classification: classifyFindingAgainstBethesdaBaseline(f, manifest),
  }));

  const netNew = classified.filter((c) => c.classification.countsAsNetNew);
  const memoryBlocked = classified.filter((c) =>
    c.classification.reason === "research_memory_recovery_is_previously_known_not_net_new"
    || (c.classification.class === WEEKLY_FINDING_CLASS.DUPLICATE && /memory|baseline/i.test(c.classification.reason || "")));

  return {
    version: BETHESDA_WEEKLY_NET_NEW_V1,
    ok: netNew.length >= 1
      && memoryBlocked.length >= 1
      && classified.some((c) => c.classification.class === WEEKLY_FINDING_CLASS.EXCLUDED),
    memoryDedupe: {
      input: normalized.length,
      uniqueAfterMemory: unique.length,
      duplicates: duplicates.length,
    },
    classified,
    netNewCount: netNew.length,
    gates: {
      researchMemoryNotCountedAsNetNew: memoryBlocked.length >= 1,
      atLeastOneGenuineNetNewFixture: netNew.length >= 1,
      excludedPathWorks: classified.some((c) => c.classification.class === WEEKLY_FINDING_CLASS.EXCLUDED),
      enrichedBaselineNotNetNew: classified.some((c) => c.classification.class === WEEKLY_FINDING_CLASS.EXISTING_ENRICHED),
    },
  };
}

/**
 * Full offline package: baseline + discovery plan + funnel + budget + gates.
 */
export function runBethesdaWeeklyNetNewOfflinePackage({ root = ROOT } = {}) {
  const reconciliation = runBethesdaDatabaseReconciliationPreflight({ root });
  const baseline = buildBethesdaImmutableBaselineManifest({ reconciliation, root });
  const discoveryPlan = buildBethesdaElevenSegmentFreshDiscoveryPlan({ baseline });
  const funnel = buildBethesdaDiscoveryToDeepResearchFunnel();
  const budget = buildBethesdaWeeklyNetNewBudgetAllocation();
  const weeklyOutputs = buildEmptyWeeklyOutputsScaffold();
  const dedupeCert = runOfflineNetNewDedupeGateCertification({ baseline });

  const liveAuthorization = {
    version: BETHESDA_WEEKLY_NET_NEW_V1,
    status: "PREPARED_AWAITING_FOUNDER_GO",
    executePaidResearch: false,
    mission: "Weekly net-new discovery + deep research across 11 segments — not baseline shortlist selection",
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    successDefinition:
      "Approximately 10–15 NET_NEW credible contactable opportunities per weekly cycle (aspirational); report actual yield honestly",
    notSuccess: [
      "Selecting 10–15 from the 103-record baseline",
      "Counting research-memory recoveries as net-new",
      "Technical PASS without commercial yield",
    ],
    mandatoryGates: [
      "immutable_baseline_manifest_loaded",
      "assertBethesdaReconciliationGate",
      "classifyFindingAgainstBethesdaBaseline for every hit",
      "fresh discovery across all 11 segments",
    ],
    budget,
    weeklyActivation: false,
    airtableWrites: false,
    publication: "OFF",
    surfePdlParallel: "NO_GO_WITHOUT_SEPARATE_AUTHORIZATION",
  };

  return {
    version: BETHESDA_WEEKLY_NET_NEW_V1,
    challengeVersion: BETHESDA_FULL_DEMAND_CHALLENGE_V1,
    preparedAt: new Date().toISOString(),
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    baseline: {
      frozenAt: baseline.frozenAt,
      immutable: true,
      counts: baseline.counts,
      corrections: baseline.corrections,
      distinctOrganizationCount: baseline.counts.distinctOrganizations,
      distinctOpportunityCount: baseline.counts.distinctCanonicalOpportunities,
      sourceRecordCount: baseline.counts.sourceRecords,
      researchMemoryPreviouslyKnown: baseline.counts.researchMemoryPreviouslyKnownRecords,
    },
    // Full baseline written to disk by prepare/test scripts
    _baselineManifest: baseline,
    discoveryPlan: {
      allElevenSegmentsCovered: discoveryPlan.allElevenSegmentsCovered,
      strategyCount: discoveryPlan.strategyCount,
      productiveSegmentsExtraEffort: discoveryPlan.productiveSegmentsExtraEffort,
      mustSearchBeyondBaseline: discoveryPlan.mustSearchBeyondBaseline,
      discoveryEntrypoint: discoveryPlan.discoveryEntrypoint,
      deepResearchEntrypoint: discoveryPlan.deepResearchEntrypoint,
      bySegment: discoveryPlan.bySegment,
    },
    funnel,
    weeklyOutputs,
    dedupeGateCertification: dedupeCert,
    liveAuthorization,
    goNoGo: {
      decision: "GO_FOR_OFFLINE_BASELINE_AND_NET_NEW_PLAN_ONLY",
      paidLiveResearch: "NO_GO_UNTIL_FOUNDER_AUTHORIZES",
      weeklyActivation: "NO_GO",
      surfePdlParallel: "NO_GO",
      airtablePublication: "NO_GO",
      note: "Baseline corrected; net-new discovery integrated offline. Stop before paid execution. $9 is proposed maximum only.",
    },
  };
}
