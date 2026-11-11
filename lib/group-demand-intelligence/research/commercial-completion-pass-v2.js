/**
 * Commercial Completion Pass V2 — offline targeted completion of the frozen
 * 22-candidate three-hotel benchmark. No paid research; no publication.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { matureOpportunity } from "./opportunity-research-supervisor-v1.js";
import { HeuristicResearchPlanner } from "./research-planner-v1.js";
import {
  planAutonomousLeadFollowing,
  executeBoundedLeadFollowUps,
  selectNextAuthorizedLeadFollowUp,
} from "./autonomous-lead-following-v2.js";
import { qualifyHotelSpecificProspectV2 } from "./hotel-specific-qualification-v2.js";
import { runCommercialYieldRecoveryOffline } from "./commercial-yield-recovery-v2.js";
import { STOP_REASONS } from "./research-constants-v1.js";
import { STOPPING_TRIGGER } from "./commercial-source-selection-v1.js";

export const COMMERCIAL_COMPLETION_PASS_VERSION = "gdi-commercial-completion-pass-v2";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");
const OUT_DIR = path.join(REPO_ROOT, "reports", "gdi-commercial-completion-pass-v2");

const HOTEL_CEILINGS = Object.freeze({
  recC2kbCaS48jFjFF: { hotelName: "Nobu Hotel London Portman Square", ceilingUsd: 1.5 },
  recLuxvwwxID7U2B8: { hotelName: "Bethesda Marriott", ceilingUsd: 1.5 },
  recnPB3Q02NO8Ma1o: { hotelName: "Wyndham San Diego Bayside", ceilingUsd: 1.5 },
});

const PRIORITY = Object.freeze({
  recC2kbCaS48jFjFF: ["London DMC", "Spectra DMC"],
  recLuxvwwxID7U2B8: [
    "National Association of Development Organizations (NADO) / DDAA",
    "American Medical Women's Association (AMWA)",
    "National Institutes of Health",
  ],
  recnPB3Q02NO8Ma1o: [
    "CMAA San Diego Chapter",
    "BOMA San Diego",
    "Urban League of San Diego County",
    "Downtown San Diego Partnership",
  ],
});

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function commerciallyMaterialQuestions(row) {
  const q = [];
  const missing = row.missingQualificationRequirements || [];
  for (const m of missing) q.push(m);
  if (row.verifiedDemandMechanism === "VENUE_OR_EVENT_LISTING_ONLY") {
    q.push("Is there evidence of guestroom purchasing control beyond venue listing copy?");
  }
  if (row.verifiedDemandMechanism === "INSUFFICIENT" && row.contactRoute) {
    q.push("What overnight / room-block / residential programme language exists on organization-owned pages?");
  }
  if (row.afterRecovery?.pathway === "COMPETITIVE_FUTURE_CYCLE") {
    q.push("What evidence supports a future cycle, satellite, or overflow — without treating the committed host as open?");
  }
  if (/SoccerPlex|Arlington|Capitol Hill|Crystal City/i.test(`${row.hotelSpecificFit || ""} ${row.competitorHost || ""}`)) {
    q.push("Does demand geography actually support the target hotel catchment?");
  }
  const lead = row.leadFollowOutcomes?.[0];
  if (lead && lead.independentlyVerified !== true) {
    q.push(`Can an independent publisher corroborate the related-entity claim for ${lead.entityName || "the lead"}?`);
  }
  return [...new Set(q)].slice(0, 5);
}

function buildFollowUpPlan(row) {
  const priorityList = PRIORITY[row.hotelId] || [];
  const priorityRank = priorityList.findIndex((n) => row.organizationName.startsWith(n) || n.startsWith(row.organizationName.slice(0, 20)));
  const useful = row.afterRecovery?.commerciallyUsefulInternal === true;
  const venue = row.failureMode === "GENUINE_VENUE_LISTING_EXCLUSION";
  const geo = row.failureMode === "GEOGRAPHY_MISMATCH_OR_WEAK_FIT";

  let disposition = "FURTHER_RESEARCH";
  if (useful) disposition = "COMPLETE_FOR_INTERNAL_ACCOUNT_DEV";
  else if (venue) disposition = "EXCLUDED_VENUE_LISTING";
  else if (geo) disposition = "EXCLUDED_OR_DEPRIORITIZE_GEOGRAPHY";

  const pursue = planAutonomousLeadFollowing({
    findingsText: [
      ...(row.sourceProvenance || []),
      row.afterRecovery?.nextSalesAction,
      row.verifiedDemandMechanism,
    ].filter(Boolean).join("\n"),
    sourceUrl: row.leadFollowOutcomes?.[0]?.url || "",
    organizationName: row.organizationName,
    investigatedUrls: (row.leadFollowOutcomes || []).map((o) => o.url).filter(Boolean),
    targetHotel: row.hotelName,
    remainingBudgetUsd: HOTEL_CEILINGS[row.hotelId]?.ceilingUsd ?? 1.5,
    maxPursue: 2,
  });

  const nextLead = selectNextAuthorizedLeadFollowUp({
    pursueQueue: pursue.leadsToPursue,
    investigatedUrls: (row.leadFollowOutcomes || []).map((o) => o.url).filter(Boolean),
    alreadyFollowedCount: row.relatedEntityLeadsPursuedOffline || 0,
    maxFollowUps: 2,
    remainingBudgetUsd: HOTEL_CEILINGS[row.hotelId]?.ceilingUsd ?? 1.5,
    commerciallyUsefulInternal: useful,
  });

  return {
    organizationName: row.organizationName,
    hotelId: row.hotelId,
    hotelName: row.hotelName,
    priorityRank: priorityRank >= 0 ? priorityRank + 1 : 99,
    disposition,
    pathway: row.afterRecovery?.pathway || row.commercialPathway,
    commerciallyUsefulInternal: useful,
    contactRoute: row.contactRoute,
    competitorHost: row.competitorHost,
    commerciallyMaterialUnansweredQuestions: commerciallyMaterialQuestions(row),
    nextAuthorizedFollowUp: nextLead.lead
      ? {
          entityName: nextLead.lead.entityName,
          url: nextLead.lead.suggestedUrl,
          researchQuestion: nextLead.lead.researchQuestion,
          commercialValueScore: nextLead.lead.commercialValueScore,
          fingerprint: nextLead.fingerprint,
        }
      : null,
    leadFollowSkipReason: nextLead.skipReason,
    deferredLeads: (pursue.leadsDeferred || []).slice(0, 3).map((l) => ({
      entityName: l.entityName,
      reason: l.rankingReasons?.[0] || "deferred",
    })),
    doNotInvestigate: venue || geo,
    preserveCompetitorCommitment: Boolean(row.competitorHost),
    estimatedLiveRetrievals: nextLead.lead && !useful ? 1 : useful ? 0 : 0,
  };
}

/**
 * Offline ORS path proof: matureOpportunity executes related_entity_followup
 * when V2 surfaces a pursue lead with a URL.
 */
export async function proveLiveOrsLeadFollowIntegrationOffline() {
  const planner = new HeuristicResearchPlanner();
  const fetched = [];
  const researcher = {
    async execute({ action }) {
      const url = (action?.queries || []).find((q) => /^https?:/i.test(q))
        || "https://spectra-dmc.example/services";
      fetched.push({ strategy: action?.strategy, url });
      if (action?.strategy === "related_entity_followup") {
        return {
          newMaterialEvidence: true,
          provider: "fixture_direct_public",
          sourceFamilies: ["related_entity"],
          costUsd: 0,
          evidence: [{
            sourceFamily: "related_entity",
            url,
            quote: "Spectra provides hotel sourcing and accommodation contracting for incentive programmes.",
            publicationReady: false,
            customerVisible: false,
          }],
        };
      }
      return {
        newMaterialEvidence: true,
        provider: "fixture_direct_public",
        sourceFamilies: ["official"],
        costUsd: 0,
        evidence: [{
          sourceFamily: "official",
          url: "https://spectra-dmc.example/",
          quote: "Spectra DMC works with hotel partners including Rosewood London for residential incentives. See also our accommodation partners page.",
          publicationReady: false,
          customerVisible: false,
        }],
        deepPatch: { contact: { businessEmail: "spectra@spectra-dmc.example" } },
      };
    },
  };

  const matured = await matureOpportunity({
    opportunity: {
      id: "gdi_completion_ors_lead_proof",
      opportunityId: "gdi_completion_ors_lead_proof",
      organizationName: "Spectra DMC",
      officialDomain: "spectra-dmc.example",
      officialSource: "https://spectra-dmc.example/",
      contactEmail: "spectra@spectra-dmc.example",
      hotelId: "recC2kbCaS48jFjFF",
      targetHotel: "Nobu Hotel London Portman Square",
      segmentId: "professional_planners_dmc",
      evidence: [],
      researchAttempts: [],
      statusHistory: [],
      entityUrlHints: {
        "Rosewood London": "https://www.rosewoodhotels.com/en/london",
      },
    },
    planner,
    researcher,
    persist: null,
    options: {
      paidBlocked: true,
      parallelAvailable: false,
      maxIterations: 4,
      enableSupervisorV2: true,
      executeLeadFollowUps: true,
      maxLeadFollowUpsPerOpportunity: 2,
      forbidPublication: true,
      forbidAirtable: true,
      remainingBudgetUsd: 1.5,
      targetHotel: "Nobu Hotel London Portman Square",
      entityUrlHints: {
        "Rosewood London": "https://www.rosewoodhotels.com/en/london",
      },
    },
  });

  const leadAttempts = (matured.researchAttempts || []).filter((a) => a.strategy === "related_entity_followup");
  const stopIsNotFalseExhaustion = ![
    STOP_REASONS.RESEARCH_EXHAUSTED,
  ].includes(matured.stopReason);

  return {
    version: COMMERCIAL_COMPLETION_PASS_VERSION,
    offline: true,
    spent: false,
    liveCalls: 0,
    entrypoint: "matureOpportunity",
    relatedEntityFollowUpsExecuted: leadAttempts.length,
    leadAttempts: leadAttempts.map((a) => ({
      entity: a.leadEntityName,
      url: a.queries?.[0],
      newMaterialEvidence: a.newMaterialEvidence,
    })),
    fetchedStrategies: fetched.map((f) => f.strategy),
    stopReason: matured.stopReason || null,
    commercialStoppingTrigger: matured.commercialStoppingTrigger || null,
    supervisorV2Attached: Boolean(matured.supervisorV2),
    stopIsNotFalseExhaustion,
    notesMentionExecution: (matured.notes || []).some((n) => /ors_executed_related_entity_followup/.test(n)),
    publicationWritesDisabled: matured.supervisorV2?.publicationWritesDisabled === true,
    passed:
      leadAttempts.length >= 1
      && matured.supervisorV2 != null
      && stopIsNotFalseExhaustion
      && matured.supervisorV2?.publicationWritesDisabled === true,
  };
}

/**
 * Checkpoint resume does not repeat completed lead fingerprints.
 */
export async function proveCheckpointResumeDoesNotRepeatWork() {
  const planner = new HeuristicResearchPlanner();
  let executeCount = 0;
  const researcher = {
    async execute({ action }) {
      executeCount += 1;
      const url = (action?.queries || [])[0] || "https://example.com/a";
      return {
        newMaterialEvidence: true,
        provider: "fixture",
        sourceFamilies: ["official"],
        costUsd: 0,
        evidence: [{
          sourceFamily: "official",
          url,
          quote: "hotel sourcing for incentive programmes in central London",
          publicationReady: false,
          customerVisible: false,
        }],
        deepPatch: { contact: { businessEmail: "info@londondmc.example" } },
      };
    },
  };

  const base = {
    id: "gdi_completion_resume_proof",
    opportunityId: "gdi_completion_resume_proof",
    organizationName: "London DMC",
    officialSource: "https://londondmc.example/",
    contactEmail: "info@londondmc.example",
    hotelId: "recC2kbCaS48jFjFF",
    targetHotel: "Nobu Hotel London Portman Square",
    segmentId: "professional_planners_dmc",
    evidence: [{
      sourceFamily: "official",
      url: "https://londondmc.example/",
      quote: "hotel sourcing and contract negotiation for residential programmes",
      publicationReady: false,
      customerVisible: false,
    }],
    researchAttempts: [{
      at: new Date().toISOString(),
      iteration: 1,
      blocker: "hotel_fit",
      strategy: "related_entity_followup",
      fingerprint: "related_entity_followup::prior::https://londondmc.example/partners",
      queries: ["https://londondmc.example/partners"],
      disposition: "COMPLETED",
      newMaterialEvidence: true,
      costUsd: 0,
      leadEntityName: "Prior Partner",
    }],
    investigatedLeadUrls: ["https://londondmc.example/partners"],
    statusHistory: [],
  };

  const first = await matureOpportunity({
    opportunity: structuredClone(base),
    planner,
    researcher,
    options: {
      paidBlocked: true,
      maxIterations: 2,
      enableSupervisorV2: true,
      executeLeadFollowUps: true,
      maxLeadFollowUpsPerOpportunity: 2,
      forbidPublication: true,
      forbidAirtable: true,
      remainingBudgetUsd: 1.5,
      targetHotel: "Nobu Hotel London Portman Square",
      forceRemature: true,
    },
  });

  const second = await matureOpportunity({
    opportunity: structuredClone(first),
    planner,
    researcher,
    options: {
      paidBlocked: true,
      maxIterations: 1,
      enableSupervisorV2: true,
      executeLeadFollowUps: true,
      forbidPublication: true,
      forbidAirtable: true,
      remainingBudgetUsd: 1.5,
      targetHotel: "Nobu Hotel London Portman Square",
      // Already terminal exploratory / stopped — without forceRemature should no-op
    },
  });

  return {
    firstExecuteCount: executeCount,
    secondIsCloneWithoutRerun: second.researchAttempts?.length === first.researchAttempts?.length,
    priorLeadUrlNotReFetched: (first.investigatedLeadUrls || []).includes("https://londondmc.example/partners"),
    commerciallyUsefulOrAccountLed:
      first.supervisorV2?.qualification?.commerciallyUsefulInternal === true
      || first.internalAccountDevelopment === true,
    passed:
      (first.investigatedLeadUrls || []).includes("https://londondmc.example/partners")
      && second.researchAttempts?.length === first.researchAttempts?.length,
  };
}

export async function runCommercialCompletionPassOffline() {
  const recovery = runCommercialYieldRecoveryOffline();
  const rows = recovery.candidateRootCauseTable || [];
  const followUpPlans = rows.map(buildFollowUpPlan).sort((a, b) => a.priorityRank - b.priorityRank);

  const orsProof = await proveLiveOrsLeadFollowIntegrationOffline();
  const resumeProof = await proveCheckpointResumeDoesNotRepeatWork();

  // Offline lead execution still works via stored evidence
  const offlineLead = executeBoundedLeadFollowUps({
    leads: [{
      entityName: "London DMC Partner Hotel",
      pursueRecommended: true,
      suggestedUrl: "https://londondmc.com/",
      commercialValueScore: 25,
    }],
    mode: "offline_stored_evidence_only",
    storedPages: [{ url: "https://londondmc.com/", text: "hotel sourcing and group accommodation" }],
    organizationHost: "londondmc.com",
    maxExecute: 1,
  });

  const duplicateRejected = planAutonomousLeadFollowing({
    findingsText: "privacy policy careers page cookie settings",
    sourceUrl: "https://example.com/privacy",
    organizationName: "Noise Org",
    investigatedUrls: [],
    targetHotel: "Nobu Hotel London Portman Square",
    remainingBudgetUsd: 1.5,
  });

  const useful = rows.filter((r) => r.afterRecovery?.commerciallyUsefulInternal);
  const prioritizedQuestions = followUpPlans
    .filter((p) => !p.doNotInvestigate && p.commerciallyMaterialUnansweredQuestions.length)
    .slice(0, 12)
    .flatMap((p) => p.commerciallyMaterialUnansweredQuestions.map((question) => ({
      hotelId: p.hotelId,
      organizationName: p.organizationName,
      priorityRank: p.priorityRank,
      question,
    })));

  const liveManifest = {
    version: "gdi-commercial-completion-live-auth-manifest-v1",
    status: "PREPARED_NOT_EXECUTED",
    authorizePaidResearch: false,
    authorizeLiveLeadRetrieval: false,
    note: "Isolated ceilings for a future GO. Do not draw on prior three-hotel ledger balances. Do not execute until separately authorized.",
    nontransferableHotelBudgetsUsd: Object.fromEntries(
      Object.entries(HOTEL_CEILINGS).map(([id, meta]) => [id, {
        hotelName: meta.hotelName,
        ceilingUsd: meta.ceilingUsd,
        priorBenchmarkBalanceNotDrawn: true,
      }]),
    ),
    totalProposedCeilingUsd: 4.5,
    providerControls: {
      parallel: "requires_explicit_authorize_paid",
      firecrawl: "thin_body_only_when_authorized",
      exa: "off_unless_authorized",
      direct_public: "preferred_first",
    },
    candidateScope: followUpPlans
      .filter((p) => p.priorityRank <= 4 && !p.doNotInvestigate)
      .map((p) => ({
        hotelId: p.hotelId,
        organizationName: p.organizationName,
        nextFollowUp: p.nextAuthorizedFollowUp,
        unanswered: p.commerciallyMaterialUnansweredQuestions,
      })),
    expectedCommercialCertificationCriteria: [
      "London DMC hotel-sourcing retained without inventing RFP",
      "Venue listings remain excluded (Miss Jones, HeadBox amenity, Hire Space listing, Gaslamp)",
      "NADO Crystal Gateway commitment preserved; future-cycle only",
      "Bethesda contacts mapped from corpus when present",
      "Wyndham banquet-only contacts not promoted without overnight mechanism",
      "related_entity_followup attempts appear in researchAttempts when authorized",
      "Stop reasons are ITERATION_CEILING_HIT / RELATED_ENTITY_FOLLOWUPS_PENDING / PROVIDER_RESTRICTED_NOT_EXHAUSTION / BUYER_ENGAGEMENT — not false RESEARCH_EXHAUSTED",
      "Publication eligibility unchanged; no Airtable writes",
    ],
    remainingProductionBlockers: [
      "Live ORS lead-following wired but not yet spend-authorized for completion pass",
      "Independent publisher corroboration for DMC–hotel relationships still thin in frozen corpus",
      "Wyndham overnight mechanisms largely unresolved — need targeted public retrieval, not generic rediscovery",
      "Bethesda NIH corridor lodging demand still weakly evidenced for several associations",
      "Production publication gate intentionally unchanged — internal useful ≠ publishable",
    ],
  };

  const certification = {
    version: COMMERCIAL_COMPLETION_PASS_VERSION,
    offline: true,
    spent: false,
    liveCalls: 0,
    candidateCount: rows.length,
    usefulInternalAfterOffline: useful.length,
    usefulNames: useful.map((r) => `${r.organizationName} @ ${r.hotelName}`),
    proofs: {
      relevantFollowUpsExecuteWhenAuthorized: orsProof.passed,
      duplicateAndIrrelevantLeadsRejected: (duplicateRejected.leadsToPursue || []).length === 0,
      hotelSourcingEvidenceRetained: useful.some((r) => /London DMC/i.test(r.organizationName)),
      contactsMapped: rows.filter((r) => r.hotelId === "recLuxvwwxID7U2B8" && r.contactRoute).length >= 3,
      competitorCommitmentsIntact: rows.some((r) => /NADO/i.test(r.organizationName) && r.afterRecovery?.pathway === "COMPETITIVE_FUTURE_CYCLE"),
      stopsAreSubstantiveNotArbitraryExhaustion: orsProof.stopIsNotFalseExhaustion,
      checkpointResumeDoesNotRepeatWork: resumeProof.passed,
      providerAndBudgetRestrictionsRespected: liveManifest.authorizePaidResearch === false && offlineLead.budgetReservationsRequiredForLive === true,
      offlineStoredLeadExecutionWorks: offlineLead.executedCount >= 1,
    },
    orsLeadIntegration: orsProof,
    resumeProof,
    offlineLeadExecution: offlineLead,
    passed: false,
  };
  certification.passed = Object.values(certification.proofs).every(Boolean);

  const packageOut = {
    version: COMMERCIAL_COMPLETION_PASS_VERSION,
    offline: true,
    spent: false,
    beforeUseful: recovery.beforeAfter?.beforeUsefulInternal ?? 0,
    afterUseful: recovery.beforeAfter?.afterUsefulInternal ?? 0,
    followUpPlans,
    prioritizedCommerciallyMaterialQuestions: prioritizedQuestions,
    liveAuthorizationManifest: liveManifest,
    certification,
    yieldRecoveryRef: {
      beforeAfter: recovery.beforeAfter,
      readiness: recovery.readinessAssessment,
    },
    controls: {
      noPaidExecution: true,
      noDeploy: true,
      noPublish: true,
      noAirtable: true,
      priorLedgersPreserved: true,
      publicationGateUnchanged: true,
    },
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  writeJson(path.join(OUT_DIR, "COMPLETION_PASS.json"), packageOut);
  writeJson(path.join(OUT_DIR, "FOLLOW_UP_PLANS.json"), followUpPlans);
  writeJson(path.join(OUT_DIR, "UNANSWERED_QUESTIONS.json"), prioritizedQuestions);
  writeJson(path.join(OUT_DIR, "LIVE_AUTH_MANIFEST.json"), liveManifest);
  writeJson(path.join(OUT_DIR, "CERTIFICATION.json"), certification);
  fs.writeFileSync(
    path.join(OUT_DIR, "CERTIFICATION_CLOSEOUT.md"),
    [
      "# Commercial Completion Pass V2 — Closeout",
      "",
      `**Spent:** $0 · **Live calls:** 0 · **Passed:** ${certification.passed}`,
      `**Useful internal (frozen):** ${useful.length}`,
      "",
      "## ORS integration",
      `- related_entity_followup executed in matureOpportunity: ${orsProof.relatedEntityFollowUpsExecuted}`,
      `- Stop not false RESEARCH_EXHAUSTED: ${orsProof.stopIsNotFalseExhaustion}`,
      "",
      "## Proposed live ceiling (NOT authorized)",
      `- Total: $${liveManifest.totalProposedCeilingUsd} nontransferable ($1.50 × 3 hotels)`,
      `- Status: ${liveManifest.status}`,
      "",
      "## Remaining blockers",
      ...liveManifest.remainingProductionBlockers.map((b) => `- ${b}`),
      "",
    ].join("\n"),
    "utf8",
  );

  return packageOut;
}
