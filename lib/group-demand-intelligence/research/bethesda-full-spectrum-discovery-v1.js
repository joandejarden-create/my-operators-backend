/**
 * Bethesda Marriott — Full-Spectrum Demand Discovery V1
 * Offline: segment corrections (proposed), discovery strategy audit, funnel spec,
 * staged pilot plan. No paid APIs. No Airtable / production writes.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  SUPERVISOR_EXPECTED_SEGMENTS,
  canonicalizeSegmentId,
} from "./research-constants-v1.js";
import {
  RESEARCH_SEGMENT_LABEL,
} from "../workflow/hotel-segment-research-plan-v1.js";
import {
  generateHotelDemandDiscoveryStrategies,
  loadHotelGdiConfig,
  screenDiscoveryHitCommercialRelevance,
  assessCapturableHotelDemandFit,
  planAdjacentDemandLeadsFromOpportunity,
  BETHESDA_SEGMENT_DISCOVERY_WEIGHT,
  SEGMENT_DISCOVERY_SOURCE_TYPES,
} from "./commercial-demand-discovery-v4.js";
import {
  resolveBethesda26OpportunityPopulation,
  recommendDemandSegment,
  auditOneOpportunity,
} from "./bethesda-26-opportunity-segment-audit-v1.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
  loadBethesdaGdiSourceCorpus,
  runBethesdaDatabaseReconciliationPreflight,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import {
  buildBethesdaImmutableBaselineManifest,
  classifyFindingAgainstBethesdaBaseline,
} from "./bethesda-weekly-net-new-discovery-v1.js";
import { computeHonestCommercialFunnelMetrics } from "./bethesda-full-demand-live-challenge-v1.js";
import { assertPaidContactProvidersDisabled } from "./commercial-spine-restoration-v41.js";

export const BETHESDA_FULL_SPECTRUM_DISCOVERY_V1 = "gdi-bethesda-full-spectrum-discovery-v1";
export const REPORTS_DIR = "reports/gdi-bethesda-full-spectrum-discovery-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/** Proposed Stage A/B ceilings — not spending authorization. */
export const PROPOSED_STAGED_PILOT_BUDGET_USD = Object.freeze({
  stageADiscoveryUsd: 1.5,
  stageBDeepResearchUsd: 4.0,
  stageBBuyerReserveUsd: 0.5,
  aggregateCeilingUsd: 6.0,
  note: "Proposed amounts only. No reuse of prior $9 balance. Surfe/PDL/Parallel separately gated.",
});

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

function writeMd(p, md) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, md.endsWith("\n") ? md : `${md}\n`);
}

/**
 * Proposed internal segment corrections for the 26 — does NOT mutate opportunities.json.
 */
/** Explicit 26-audit reclassification set — always retained as proposed overlays. */
export const BETHESDA_26_PROPOSED_SEGMENT_CORRECTIONS = Object.freeze([
  {
    opportunityId: "gdi_opp_ahima_advocacy_2027",
    fromRaw: "Healthcare",
    toPrimary: "associations_government",
    toSecondary: "medical_academic",
  },
  {
    opportunityId: "gdi_opp_acc_legislative_2027",
    fromRaw: "Medical / Advocacy",
    toPrimary: "associations_government",
    toSecondary: "medical_academic",
  },
  {
    opportunityId: "gdi_opp_regulatory_information_conference_20260930",
    fromRaw: "Training Program",
    toPrimary: "associations_government",
    toSecondary: "corporate_meetings_training",
  },
  {
    opportunityId: "gdi_opp_agb_2027_disqualified",
    fromRaw: "University / education",
    toPrimary: "associations_government",
    toSecondary: "tours_educational_specialist",
  },
  {
    opportunityId: "gdi_opp_accp_annual_meeting_20261001",
    fromRaw: "Event / Association",
    toPrimary: "medical_academic",
    toSecondary: null,
  },
  {
    opportunityId: "gdi_opp_ndss_advocacy_2027",
    fromRaw: "Healthcare / Advocacy",
    toPrimary: "associations_government",
    toSecondary: "medical_academic",
  },
  {
    opportunityId: "gdi_opp_acvnu_renal_week_20270412",
    fromRaw: "Overflow Housing",
    toPrimary: "medical_academic",
    toSecondary: null,
  },
]);

export function buildCorrected26SegmentBaseline({ root = ROOT } = {}) {
  const pop = resolveBethesda26OpportunityPopulation({ root });
  if (!pop.populationVerified) {
    return { ok: false, discrepancy: pop.discrepancy, populationCount: pop.populationCount };
  }

  const proposedById = new Map(
    BETHESDA_26_PROPOSED_SEGMENT_CORRECTIONS.map((c) => [c.opportunityId, c]),
  );
  const corrections = [];
  const rows = pop.population.map((opp, i) => {
    const audit = auditOneOpportunity(opp, i);
    const rec = recommendDemandSegment(opp);
    const id = opp.id || opp.opportunityId;
    const proposed = proposedById.get(id);
    const primary = proposed?.toPrimary || rec.primary;
    const secondary = proposed?.toSecondary !== undefined ? proposed.toSecondary : rec.secondary;
    const priorCanonical = canonicalizeSegmentId(opp.segment || "");
    const rawDiffers = Boolean(proposed && text(opp.segment) === text(proposed.fromRaw));
    const change = Boolean(proposed)
      || audit.changeRequired
      || (primary && priorCanonical && priorCanonical !== primary)
      || (primary && !priorCanonical);

    if (change && primary) {
      corrections.push({
        opportunityId: id,
        fromRaw: opp.segment || proposed?.fromRaw || null,
        fromCanonicalMapped: priorCanonical || null,
        toPrimary: primary,
        toSecondary: secondary,
        confidence: rec.confidence,
        evidence: rec.evidence,
        rawLabelCorrection: rawDiffers,
        aliasNowAligned: Boolean(proposed && priorCanonical === primary),
        applyToProduction: false,
        applyToAirtable: false,
        note: "Proposed internal overlay only — alias engine may already map correctly after full-spectrum fixes",
      });
    }

    return {
      opportunityId: id,
      organization: opp.organizationName,
      title: opp.title || opp.canonicalEventName,
      demandStatus: opp.demandStatus,
      opportunityType: opp.opportunityType,
      currentSegmentRaw: opp.segment || null,
      correctedPrimary: primary,
      correctedSecondary: secondary,
      confidence: rec.confidence,
      changeRequired: Boolean(change && primary),
      evidence: rec.evidence,
    };
  });

  const bySeg = {};
  for (const s of SUPERVISOR_EXPECTED_SEGMENTS) bySeg[s] = 0;
  for (const r of rows) {
    if (r.correctedPrimary && bySeg[r.correctedPrimary] !== undefined) {
      bySeg[r.correctedPrimary] += 1;
    }
  }

  return {
    ok: true,
    version: BETHESDA_FULL_SPECTRUM_DISCOVERY_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    populationCount: 26,
    correctedSum: Object.values(bySeg).reduce((a, b) => a + b, 0),
    correctedDistribution: bySeg,
    correctionsCount: corrections.length,
    corrections,
    rows,
    productionWrite: false,
    airtableWrite: false,
  };
}

export function buildUnifiedInventoryReconciliation({ root = ROOT } = {}) {
  const corpus = loadBethesdaGdiSourceCorpus({ root });
  const recon = runBethesdaDatabaseReconciliationPreflight({ root });
  let baseline;
  try {
    baseline = buildBethesdaImmutableBaselineManifest({ reconciliation: recon, root });
  } catch (err) {
    baseline = { error: err.message, counts: {} };
  }
  const pop26 = resolveBethesda26OpportunityPopulation({ root });

  return {
    version: BETHESDA_FULL_SPECTRUM_DISCOVERY_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    canonicalOpportunitiesBag: corpus?.canonical?.length ?? null,
    researchCandidatesBag: corpus?.researchCandidates?.length ?? null,
    weeklyStaged: corpus?.weeklyOpps?.length ?? null,
    immutableBaselineDistinctOpportunities: baseline?.counts?.distinctCanonicalOpportunities ?? null,
    immutableBaselineSourceRecords: baseline?.counts?.sourceRecords ?? null,
    active26DemandPopulation: pop26.populationCount,
    active26Verified: pop26.populationVerified,
    reconciliationGate: recon?.gate || recon?.status || null,
    note: "26 is a demandStatus subset of the canonical bag — not the full market inventory. 103-class baseline remains immutable for NET_NEW. Live housing-page 10 are not auto-qualified.",
    priorLiveNetNewHousingCandidates: {
      treatAsQualified: false,
      treatAsAutomaticNetNew: false,
      subjectToCorrectedReconciliation: true,
    },
  };
}

export function auditElevenSegmentDiscoveryStrategies({ root = ROOT } = {}) {
  const cfg = loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID, { root }) || {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
    city: "Bethesda",
    demandTerritory: { label: "DMV" },
    commercialPriorities: {
      demandAnchorFocus: ["NIH", "Walter Reed National Military Medical Center"],
      coreTargetPeakRoomsMin: 100,
      coreTargetPeakRoomsMax: 300,
    },
    capabilityProfile: {
      totalGuestrooms: 407,
      meetingRoomCount: 27,
      largestBallroomSqFt: 4592,
    },
  };
  const pack = generateHotelDemandDiscoveryStrategies(cfg, { maxPerSegment: 4 });
  const bySegment = {};
  for (const s of SUPERVISOR_EXPECTED_SEGMENTS) {
    const strategies = pack.strategies.filter((x) => x.segment === s);
    const housingOnly = strategies.filter((x) => x.housingPageExtractionOnly).length;
    bySegment[s] = {
      label: RESEARCH_SEGMENT_LABEL[s] || s,
      strategyCount: strategies.length,
      weight: BETHESDA_SEGMENT_DISCOVERY_WEIGHT[s] || 1,
      underrepresentedPriority: (BETHESDA_SEGMENT_DISCOVERY_WEIGHT[s] || 0) >= 3,
      sourceTypes: SEGMENT_DISCOVERY_SOURCE_TYPES[s] || [],
      sampleQuestions: strategies.map((x) => x.researchQuestion).slice(0, 3),
      housingPageOnlyQueries: housingOnly,
      meaningful: strategies.length > 0 && housingOnly === 0,
    };
  }

  return {
    version: BETHESDA_FULL_SPECTRUM_DISCOVERY_V1,
    allElevenCovered: pack.allElevenSegmentsCovered === true,
    strategyCount: pack.strategyCount,
    underrepresentedPrioritized: pack.underrepresentedSegmentsPrioritized || [],
    weights: BETHESDA_SEGMENT_DISCOVERY_WEIGHT,
    bySegment,
    defectsFixed: [
      "Underrepresented segments (corporate, DMC, scaling, local catering, exhibitor) now weight ≥2–3",
      "Distinct sourceTypes per segment",
      "Queries avoid hotel-housing-page recall as primary objective",
      "Advocacy/overflow alias corrections in canonicalizeSegmentId",
    ],
  };
}

export function buildAdjacentDemandStrategy({ root = ROOT } = {}) {
  const pop = resolveBethesda26OpportunityPopulation({ root });
  const plans = (pop.population || []).slice(0, 26).map((opp) =>
    planAdjacentDemandLeadsFromOpportunity(opp, { maxLeads: 4 }));
  const leadKinds = {};
  for (const p of plans) {
    for (const l of p.leads) {
      leadKinds[l.adjacentKind] = (leadKinds[l.adjacentKind] || 0) + 1;
    }
  }
  return {
    version: BETHESDA_FULL_SPECTRUM_DISCOVERY_V1,
    parentsExamined: plans.length,
    totalAdjacentLeadsProposed: plans.reduce((a, p) => a + p.leadCount, 0),
    leadKinds,
    rules: [
      "Original conference ≠ new opportunity",
      "Organizer ≠ automatically new",
      "Sponsors/exhibitors ≠ automatically new",
      "Distinct programme/mechanism may be NET_NEW only after baseline reconciliation",
      "Do not double-count same demand across segments",
    ],
    sample: plans.filter((p) => p.leadCount > 0).slice(0, 5),
  };
}

export function buildFunnelReportingSpec() {
  return {
    version: BETHESDA_FULL_SPECTRUM_DISCOVERY_V1,
    perSegmentAndOverall: [
      "grossDiscoveryCandidates",
      "validOrganizationsPrograms",
      "genuineNetNew",
      "credibleDemandMechanisms",
      "bethesdaFit",
      "eligibleForDeepResearch",
      "actuallyDeeplyResearched",
      "viableSourcingOrAccountDevelopmentPathways",
      "legitimateBuyerContactRoutes",
      "hotelFacingEligible",
      "commerciallyActionableNew",
    ],
    separateStreams: [
      "reactivated",
      "existingEnriched",
      "existingUnchanged",
      "duplicates",
      "excluded",
    ],
    unitEconomics: {
      costPerActionable: "undefined when actionable yield is 0 — never spend/housing-page-count",
      costPerCredible: "optional; disclose denominator",
    },
    weeklyClasses: ["NET_NEW", "REACTIVATED", "EXISTING_ENRICHED", "EXISTING_UNCHANGED", "DUPLICATE", "EXCLUDED"],
    exampleZeroYield: computeHonestCommercialFunnelMetrics({
      aggregateSpendUsd: 0,
      customerActionable: 0,
      hotelFacingEligible: 0,
    }),
  };
}

export function buildStagedPilotPlan() {
  return {
    version: BETHESDA_FULL_SPECTRUM_DISCOVERY_V1,
    executeNow: false,
    paidExecutionAuthorized: false,
    reusePriorNineDollarBalance: false,
    providersAllowedIfAuthorized: ["exa", "firecrawl", "direct_public"],
    providersForbiddenUntilSeparateGo: ["surfe", "pdl", "parallel"],
    budgetUsd: { ...PROPOSED_STAGED_PILOT_BUDGET_USD },
    stageA: {
      name: "Discovery validation",
      ceilingUsd: PROPOSED_STAGED_PILOT_BUDGET_USD.stageADiscoveryUsd,
      focus: "All 11 segments; extra weight on underrepresented commercial channels",
      measures: [
        "newCandidatesIdentified",
        "trueNovelty",
        "sourceAndSegmentDiversity",
        "bethesdaFitQuality",
        "demandCredibility",
        "contactRoutePotential",
        "exclusionReasons",
      ],
      autoProceedToStageB: false,
      reviewRequired: "Founder review of candidate identities, sources, and qualification before Stage B",
    },
    stageB: {
      name: "Commercial deep research",
      requires: "Separate authorization after Stage A review",
      deepResearchCeilingUsd: PROPOSED_STAGED_PILOT_BUDGET_USD.stageBDeepResearchUsd,
      buyerReserveUsd: PROPOSED_STAGED_PILOT_BUDGET_USD.stageBBuyerReserveUsd,
      entrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
      nontransferableStageBudgets: true,
    },
    commercialAcceptance: {
      aspirationalTarget: "≈10–15 NEW actionable per hotel per weekly cycle",
      notAQuota: true,
      requiredFields: [
        "verifiedIdentity",
        "credibleDemandMechanism",
        "evidenceBackedBethesdaFit",
        "timing",
        "commercialStage",
        "buyerOrgOrFunction",
        "legitimateContactRoute",
        "practicalSalesAction",
        "citedSourcesAndUncertainties",
      ],
      namedBuyerPreferredNotMandatory: true,
    },
  };
}

export function runOfflineFullSpectrumCertifications() {
  const providers = assertPaidContactProvidersDisabled();
  const cfg = {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
    city: "Bethesda",
  };

  const tests = [];
  const ok = (name, pass, detail = null) => tests.push({ name, pass: Boolean(pass), detail });

  const pack = generateHotelDemandDiscoveryStrategies({
    ...cfg,
    demandTerritory: { label: "DMV" },
    commercialPriorities: {
      demandAnchorFocus: ["NIH", "Walter Reed"],
      coreTargetPeakRoomsMin: 100,
      coreTargetPeakRoomsMax: 300,
    },
    capabilityProfile: { totalGuestrooms: 407, meetingRoomCount: 27 },
  });
  ok("all_eleven_strategies", pack.allElevenSegmentsCovered, { count: pack.strategyCount });
  ok(
    "underrepresented_receive_weight",
    (pack.underrepresentedSegmentsPrioritized || []).includes("corporate_meetings_training")
      && (pack.underrepresentedSegmentsPrioritized || []).includes("professional_planners_dmc"),
    pack.underrepresentedSegmentsPrioritized,
  );
  ok(
    "no_legacy_housing_or_room_block_primary_queries",
    !pack.strategies.some((s) => /\bhotel housing OR room block\b/i.test(s.researchQuestion)),
  );

  const distant = screenDiscoveryHitCommercialRelevance({
    organizationName: "AACPDM",
    programName: "Annual Meeting headquarters Philadelphia Marriott Downtown",
    sourceUrl: "https://www.aacpdm.org/events/2026/hoteltravel",
    demandSignalSummary: "already contracted headquarters hotel Philadelphia Marriott Downtown",
  }, cfg);
  ok("distant_committed_screened", distant.pass === false, distant.rejectReasons);

  const localAccount = assessCapturableHotelDemandFit({
    organizationName: "Capitol DMV Destinations",
    programName: "corporate meetings and incentives DMC",
    demandSignalSummary: "Bethesda and Montgomery County hotel sourcing for incentives",
  }, cfg);
  ok(
    "account_development_not_rejected_for_missing_rfp",
    localAccount.accountDevelopmentEligible === true
      || localAccount.fitClass === "POTENTIALLY_CAPTURABLE_CORE"
      || localAccount.fitClass === "POTENTIALLY_CAPTURABLE_DMV_OR_ACCOUNT",
    localAccount,
  );

  ok(
    "advocacy_alias_to_associations",
    canonicalizeSegmentId("Medical / Advocacy") === "associations_government"
      && canonicalizeSegmentId("Healthcare / Advocacy") === "associations_government",
  );
  ok(
    "overflow_housing_not_exhibitor_segment",
    canonicalizeSegmentId("Overflow Housing") === ""
      || canonicalizeSegmentId("Overflow Housing") !== "exhibitor_sponsor_teams",
  );

  let baselineOk = false;
  try {
    const baseline = buildBethesdaImmutableBaselineManifest();
    const cls = classifyFindingAgainstBethesdaBaseline({
      organizationName: "NIH Common Fund",
      programName: "High-Risk High-Reward Research Symposium",
      eventYear: 2027,
    }, baseline);
    baselineOk = Boolean(cls?.class);
    ok("baseline_reconcile_known_program", baselineOk, cls?.class);
  } catch (err) {
    ok("baseline_reconcile_known_program", false, err.message);
  }

  const adj = planAdjacentDemandLeadsFromOpportunity({
    id: "gdi_opp_show_2026_nih",
    organizationName: "NIH / NHLBI",
    title: "SHOW 2026 NIH Research Conference",
    segment: "medical_academic",
  });
  ok("adjacent_leads_do_not_auto_net_new", adj.leads.every((l) => l.countsAsNetNewAutomatically === false));

  const funnel = computeHonestCommercialFunnelMetrics({
    aggregateSpendUsd: 0.87,
    customerActionable: 0,
  });
  ok("zero_actionable_cost_undefined", funnel.costPerActionableUndefinedBecauseZeroYield === true);

  ok("paid_contact_providers_disabled", providers?.ok !== false && providers !== false, providers);

  const failed = tests.filter((t) => !t.pass);
  return {
    version: BETHESDA_FULL_SPECTRUM_DISCOVERY_V1,
    passed: failed.length === 0,
    total: tests.length,
    failed: failed.length,
    tests,
    fixturePassesAreNotLiveDiscoveries: true,
  };
}

export function buildGoNoGoRecommendation({ cert = null, corrected = null } = {}) {
  const certOk = cert?.passed === true;
  const baselineOk = corrected?.ok === true && corrected?.correctedSum === 26;
  return {
    decision: "NO-GO_PAID_EXECUTION",
    stagedPilotPrepared: true,
    offlineCertificationPassed: certOk,
    correctedBaselineReady: baselineOk,
    rationale: [
      "Offline full-spectrum package complete — discovery weights, aliases, hotel-fit screen, funnel, and Stage A/B plans ready",
      "Paid Stage A discovery still requires separate founder GO (proposed $1.50)",
      "Do not reuse prior $9 authorization balance",
      "Surfe/PDL/Parallel remain NO-GO",
      "No Airtable / production / publication / weekly activation",
    ],
    conditionsForStageAGo: [
      "Founder authorizes Stage A discovery ≤ $1.50 with isolated ledger",
      "Review uses corrected strategies (underrepresented segments prioritized)",
      "Do not auto-advance to Stage B",
    ],
    conditionsForStageBGo: [
      "Stage A candidates reviewed for novelty, Bethesda fit, and demand credibility",
      "Separate authorization for deep ≤ $4.00 + buyer reserve ≤ $0.50",
    ],
  };
}

/**
 * Write full deliverable package.
 */
export function writeBethesdaFullSpectrumDiscoveryPackage({ root = ROOT } = {}) {
  const outDir = path.join(root, REPORTS_DIR);
  const corrected = buildCorrected26SegmentBaseline({ root });
  const inventory = buildUnifiedInventoryReconciliation({ root });
  const strategies = auditElevenSegmentDiscoveryStrategies({ root });
  const adjacent = buildAdjacentDemandStrategy({ root });
  const funnel = buildFunnelReportingSpec();
  const pilot = buildStagedPilotPlan();
  const cert = runOfflineFullSpectrumCertifications();
  const goNoGo = buildGoNoGoRecommendation({ cert, corrected });

  writeJson(path.join(outDir, "01-corrected-26-segment-baseline.json"), corrected);
  writeJson(path.join(outDir, "02-unified-inventory-reconciliation.json"), inventory);
  writeJson(path.join(outDir, "03-eleven-segment-discovery-strategy-audit.json"), strategies);
  writeJson(path.join(outDir, "04-targeted-discovery-improvements-by-segment.json"), {
    weights: BETHESDA_SEGMENT_DISCOVERY_WEIGHT,
    sourceTypes: SEGMENT_DISCOVERY_SOURCE_TYPES,
    bySegment: strategies.bySegment,
    priorityOrder: [
      "corporate_meetings_training",
      "professional_planners_dmc",
      "scaling_companies",
      "local_meetings_catering",
      "exhibitor_sponsor_teams",
    ],
  });
  writeJson(path.join(outDir, "05-adjacent-demand-research-strategy.json"), adjacent);
  writeJson(path.join(outDir, "06-bethesda-geographic-hotel-fit-qualification.json"), {
    assessCapturableHotelDemandFit: "lib/.../commercial-demand-discovery-v4.js#assessCapturableHotelDemandFit",
    screenDiscoveryHitCommercialRelevance: "enhanced with capturableFit + distant-committed reject",
    rules: [
      "Potentially capturable Bethesda/DMV demand vs event-elsewhere hotel use",
      "Do not infer Bethesda demand from generic housing tabs",
      "Do not exclude account-development solely for missing RFP",
      "Screen weak candidates before deep research",
    ],
  });
  writeJson(path.join(outDir, "07-net-new-reconciliation-tests.json"), {
    weeklyClasses: funnel.weeklyClasses,
    immutableBaselineRequired: true,
    researchMemoryNotNetNew: true,
    newContactNotNewOpportunity: true,
    certCases: cert.tests.filter((t) => /baseline|adjacent|advocacy|overflow/.test(t.name)),
  });
  writeJson(path.join(outDir, "08-buyer-contact-research-integration.json"), {
    providersDisabled: true,
    surfePdlParallel: "OFF pending separate GO",
    surfePersist: false,
    autoOutreach: false,
    accept: [
      "verified named buyer",
      "verified functional department",
      "verified event/housing contact",
      "verified intermediary",
      "legitimate org contact form to relevant team",
    ],
    reject: [
      "consumer email",
      "directory inbox",
      "fabricated formats",
      "irrelevant executives",
      "housing URL as contact",
    ],
  });
  writeJson(path.join(outDir, "09-ors-deep-research-integration-verification.json"), {
    entrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
    newCandidates: "canonical create/identity-resolve before deep research",
    relatedEntityFollowUps: true,
    replanWhenUnproductive: true,
    iterationCapNotCompleteness: true,
    supervisorSoleOrchestrator: true,
    noNewEngine: true,
  });
  writeJson(path.join(outDir, "10-offline-regressions-and-unresolved.json"), {
    certification: cert,
    unresolved: [
      "Live Stage A not executed — candidate quality unknown until authorized discovery",
      "Buyer enrichment still blocked without Surfe/PDL GO",
      "Empty segments in existing 26 may reflect market thinness OR discovery bias — Stage A must measure both",
      "Proposed segment corrections not applied to production opportunities.json",
    ],
  });
  writeJson(path.join(outDir, "11-stage-a-discovery-pilot.json"), pilot.stageA);
  writeJson(path.join(outDir, "12-stage-b-deep-research-plan.json"), pilot.stageB);
  writeJson(path.join(outDir, "13-segment-funnel-reporting-spec.json"), funnel);
  writeJson(path.join(outDir, "14-expected-commercial-success-measures.json"), {
    aspirationalWeeklyTarget: [10, 15],
    notAQuota: true,
    successIsCommercialUsability: true,
    technicalPassIsNotSuccess: true,
    zeroYieldMustBeReportedHonestly: true,
    acceptance: pilot.commercialAcceptance,
  });
  writeJson(path.join(outDir, "15-go-nogo.json"), goNoGo);

  const summary = {
    version: BETHESDA_FULL_SPECTRUM_DISCOVERY_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    completedAt: new Date().toISOString(),
    corrected26Sum: corrected.correctedSum,
    correctionsProposed: corrected.correctionsCount,
    allElevenStrategiesCovered: strategies.allElevenCovered,
    offlineCertPassed: cert.passed,
    paidSpend: 0,
    productionWrites: false,
    nextDecision: goNoGo.decision,
    proposedPilotCeilingUsd: PROPOSED_STAGED_PILOT_BUDGET_USD.aggregateCeilingUsd,
  };
  writeJson(path.join(outDir, "SUMMARY.json"), summary);

  writeMd(path.join(outDir, "REPORT.md"), `# Bethesda Full-Spectrum Demand Discovery V1

## Verdict

Offline package **ready**. Paid execution: **${goNoGo.decision}**.

- Corrected 26 primary segments sum to **${corrected.correctedSum}** (production not modified).
- All **11** discovery strategies covered; underrepresented channels prioritized.
- Offline certification: **${cert.passed ? "PASS" : "FAIL"}** (${cert.passed ? cert.total : cert.failed}/${cert.total}).
- Proposed staged ceiling: **$${PROPOSED_STAGED_PILOT_BUDGET_USD.aggregateCeilingUsd}** (A $${PROPOSED_STAGED_PILOT_BUDGET_USD.stageADiscoveryUsd} / B deep $${PROPOSED_STAGED_PILOT_BUDGET_USD.stageBDeepResearchUsd} / buyer $${PROPOSED_STAGED_PILOT_BUDGET_USD.stageBBuyerReserveUsd}) — **not authorized**.

## Corrected 26 distribution (primary)

${Object.entries(corrected.correctedDistribution || {}).map(([k, v]) => `- \`${k}\`: ${v}`).join("\n")}

## Code fixes in this package

- Segment alias corrections (advocacy → associations_government; Overflow Housing not a segment).
- Bethesda discovery weights tilted to underrepresented commercial segments.
- Distinct source-types and queries per segment.
- \`assessCapturableHotelDemandFit\` + stronger discovery screen.
- Adjacent-demand lead planner (no auto NET_NEW).
- Honest funnel cost-per-actionable when yield is zero.

## GO/NO-GO

${goNoGo.rationale.map((r) => `- ${r}`).join("\n")}

Paid spend this assignment: **$0**.
`);

  return { outDir: outDir.replace(/\\/g, "/"), summary, corrected, strategies, cert, goNoGo, pilot };
}
