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
} from "../research/deep-research-eligibility-v1.js";
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
import {
  resolveExaAgentRoutingForSegment,
  buildExaOperationRoutingMetadata,
} from "../research/exa-agent/gdi-segment-aware-exa-agent-routing-v1.js";
import { DISCOVERY_EXECUTION_STATUS } from "../research/segment-discovery-executor-v1.js";
import {
  persistDiscoveryTrace,
  loadDiscoveryState,
  shouldSkipLiveProviderInvocation,
} from "../research/full-e2e-fourteen-segment-discovery-state-v1.js";
import {
  createPaidBudgetLedger,
  loadPaidBudgetLedger,
  completedDeepResearchOpportunityIds,
  canAffordQuotedCall,
  reserveQuotedCall,
  settleReservedCall,
  releaseReservation,
  remainingBudgetUsd,
  persistPaidBudgetLedger,
} from "../research/gdi-authorized-paid-budget-ledger-v1.js";
import {
  buildSegmentHotelFairDeepResearchQueue,
  summarizeFairQueueCoverage,
} from "./gdi-segment-hotel-fair-research-scheduler-v1.js";
import {
  runRestoredOrsDeepResearchPass,
  runRestoredContactInvestigationPass,
  runRestoredContactInvestigationPassAsync,
  runRestoredCommercialEligibilityPass,
  runRestoredNeedDatePass,
} from "./gdi-original-full-e2e-execution-restore-v1.js";
import { rehydratePersistedExaEvidence } from "./gdi-persisted-exa-evidence-rehydrate-v1.js";
import { reconcileCommercialDispositionV1 } from "./gdi-commercial-disposition-reconcile-v1.js";
import {
  orgDeepKey,
  evaluatePrePaidDeepDedupe,
  reconcilePostDeepIdentity,
  buildInventoryDedupeContextForHotels,
} from "../research/gdi-paid-deep-two-stage-dedupe-v1.js";

const AIMBRIDGE_HOTEL_DISPLAY_NAMES = Object.freeze({
  recCAGqkERSXgiku4: "Sheraton Guadalajara Expo",
  recTk5I8VOzsPhxDz: "voco Cancún Zona Hotelera",
  recXh4NgF8sDbi9cQ: "Wyndham Alltra Punta Cana",
});

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
        : organizationName &&
            (meta.segmentId === "preferred_lodging_partnerships" ||
              meta.segmentId === "professional_event_planners")
          ? `${organizationName} — ${
              meta.segmentId === "preferred_lodging_partnerships"
                ? "Independent event venue — preferred lodging partnership / overnight guest referral prospect"
                : "Professional event planner — hotel lodging referral and group accommodation pathway"
            }: investigate hotel-relevant lodging / meetings / catering demand for ${meta.hotelName || "target hotel"}.`
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

function peopleFromExaContactFields(opportunity = {}) {
  const out = [];
  const push = (c, fallbackRole = "") => {
    if (!c) return;
    const name = text(c.name || c.personName || "");
    if (!name) return;
    out.push({
      name,
      role: text(c.role || c.title || c.currentTitle || fallbackRole || ""),
      title: text(c.title || c.currentTitle || c.role || fallbackRole || ""),
      organizationName: text(
        c.organization || c.company || opportunity.organizationName || ""
      ),
      sourceUrl: c.sourceUrl || (c.supportingSourceUrls || [])[0] || null,
      commercialClass: c.commercialClass || null,
      contactRelevanceLabel: c.contactRelevanceLabel || null,
    });
  };
  push(opportunity.primaryContact);
  push(opportunity.secondaryContact);
  if (opportunity.contactName) {
    push({
      name: opportunity.contactName,
      title: opportunity.contactRole || opportunity.contactTitle || "",
      role: opportunity.contactRole || opportunity.contactTitle || "",
      sourceUrl: opportunity.contactUrl || opportunity.organizationRouteUrl || null,
    });
  }
  const sci = opportunity.scalingContactIntelligence || {};
  push(sci.primaryContact);
  push(sci.secondaryContact);
  for (const c of sci.additionalCandidates || []) push(c);
  return out;
}

/**
 * Run contact classification against any named stakeholders already present
 * (from deep research fixture extraction, Exa ORS mapping, or prior seed state).
 * No paid Surfe.
 */
export function runContactResearchPass(opportunity = {}) {
  const contactList = Array.isArray(opportunity.contacts)
    ? opportunity.contacts
    : [];
  const people = [
    ...peopleFromExaContactFields(opportunity),
    ...(Array.isArray(opportunity.namedStakeholders)
      ? opportunity.namedStakeholders
      : []),
    ...(Array.isArray(opportunity.namedStakeholdersFound)
      ? opportunity.namedStakeholdersFound
      : []),
    ...contactList.map((c) => ({
      name: c?.name,
      role: c?.title || c?.role,
      title: c?.title || c?.role,
      organizationName: c?.organization || opportunity.organizationName,
      sourceUrl: c?.sourceUrl,
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
  const remainingBudgetForStep = Number(opts.remainingBudgetUsd ?? 0);
  const stages = [];

  let opportunity = { ...opportunityInput };
  stages.push({ stage: "CANDIDATE_MAPPED", at: now(), opportunityId: opportunity.id });

  // Recover Exa packs into evidence/claims when Aimbridge left arrays empty.
  const rehydrate = rehydratePersistedExaEvidence(opportunity);
  opportunity = rehydrate.opportunity;
  if (rehydrate.recovered || rehydrate.gap) {
    stages.push({
      stage: "EXA_EVIDENCE_REHYDRATE",
      recovered: rehydrate.recovered === true,
      evidenceAdded: rehydrate.evidenceAdded || 0,
      claimsAdded: rehydrate.claimsAdded || 0,
      gap: rehydrate.gap || null,
      at: now(),
    });
  }

  let deepResult = null;
  let deepCostUsd = 0;
  let eligibility = null;

  const segmentId =
    opportunity.researchSegment || opportunity.segmentId || null;
  const routing = resolveExaAgentRoutingForSegment(segmentId, {
    stage: "DEEP_RESEARCH",
  });
  const routingMeta = buildExaOperationRoutingMetadata({
    hotel: {
      hotelId: opportunity.hotelId,
      displayName: opportunity.hotelName,
    },
    segmentId,
    stage: "DEEP_RESEARCH",
  });
  // Segment identity survives every stage — including skipped/unsupported deep.
  opportunity.exaAgentRouting = {
    ...(opportunity.exaAgentRouting || {}),
    ...routingMeta,
    segmentId: routing.segmentId || segmentId,
    demandSegment: routing.segmentId || segmentId,
    support: routing.support,
    rationale: routing.rationale,
  };

  if (opts.skipDeepResearch === true) {
    stages.push({
      stage: "DEEP_RESEARCH",
      skipped: true,
      reason:
        routing.paidExaAllowed === false
          ? routing.blockReason || "AGENT_FAMILY_UNSUPPORTED_FOR_SEGMENT"
          : "explicit_skip",
      agentRouting: opportunity.exaAgentRouting,
      at: now(),
    });
    eligibility = evaluateDeepResearchEligibility(opportunity, {
      remainingBudgetUsd: authorizePaidResearch ? remainingBudgetForStep : 0,
    });
    stages.push({
      stage: "DEEP_RESEARCH_ELIGIBILITY",
      at: now(),
      decision: eligibility.decision,
      tier: eligibility.tier,
    });
  } else {
    const ledger = opts.paidBudgetLedger || null;
    const alreadyPaidDeep =
      Boolean(opportunity.exaAgentDeepResearch) ||
      Boolean(
        opts.completedDeepResearchIds &&
          opts.completedDeepResearchIds.has(String(opportunity.id))
      );

    if (
      authorizePaidResearch &&
      ledger &&
      !opts.liveResearcher?.execute &&
      !alreadyPaidDeep &&
      routing.paidExaAllowed === false
    ) {
      stages.push({
        stage: "DEEP_RESEARCH",
        blocked: true,
        reason: routing.blockReason || "AGENT_FAMILY_UNSUPPORTED_FOR_SEGMENT",
        agentRouting: opportunity.exaAgentRouting,
        at: now(),
      });
      eligibility = evaluateDeepResearchEligibility(opportunity, {
        remainingBudgetUsd: remainingBudgetForStep,
      });
    } else {
      let reservedQuote = null;
      const willSeedLiveExa =
        authorizePaidResearch === true &&
        opts.authorizeExaAgentLive === true &&
        opts.allowLiveExaDeepSeed === true &&
        !alreadyPaidDeep &&
        !opts.liveResearcher?.execute;
      // Reserve ledger capacity only when a live Exa deep seed may spend.
      // Offline ORS continuation on fixtures / already-paid evidence is $0.
      if (willSeedLiveExa && ledger) {
        const gate = canAffordQuotedCall(ledger);
        if (!gate.ok) {
          stages.push({
            stage: "DEEP_RESEARCH",
            blocked: true,
            reason: gate.reason,
            remainingBudgetUsd: remainingBudgetUsd(ledger),
          });
          eligibility = evaluateDeepResearchEligibility(opportunity, {
            remainingBudgetUsd: 0,
          });
        } else {
          const reserved = reserveQuotedCall(ledger, {
            quotedMaxUsd: gate.proposedMax,
          });
          if (!reserved.ok) {
            stages.push({
              stage: "DEEP_RESEARCH",
              blocked: true,
              reason: reserved.reason,
            });
          } else {
            reservedQuote = gate.proposedMax;
          }
        }
      }

      if (
        !stages.some(
          (s) => s.stage === "DEEP_RESEARCH" && (s.blocked || s.skipped)
        )
      ) {
        try {
          const restored = await runRestoredOrsDeepResearchPass(opportunity, {
            authorizePaidResearch,
            authorizeExaAgentLive: opts.authorizeExaAgentLive === true,
            // Live Exa seed only when explicitly authorized for a new paid cycle.
            allowLiveExaDeepSeed:
              authorizePaidResearch === true &&
              opts.authorizeExaAgentLive === true &&
              opts.allowLiveExaDeepSeed === true &&
              !alreadyPaidDeep,
            skipExaBecauseAlreadyPaid: alreadyPaidDeep,
            remainingBudgetUsd: remainingBudgetForStep,
            maxIterations: opts.maxIterations ?? 4,
            liveResearcher: opts.liveResearcher || null,
            researcher: opts.researcher || null,
            buildCorpus: buildOfflineCorpusFromOpportunity,
            corpus: buildOfflineCorpusFromOpportunity(opportunity),
            exaEffort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
            checkpointDir: opts.checkpointDir || null,
            forceOrsPaidBlocked: alreadyPaidDeep === true,
          });
          opportunity = restored.opportunity;
          deepResult = restored.deepResult;
          deepCostUsd = Number(restored.deepCostUsd || 0);
          eligibility = restored.eligibility;
          for (const st of restored.stages || []) stages.push(st);

          if (ledger && reservedQuote != null) {
            settleReservedCall(ledger, {
              quotedMaxUsd: reservedQuote,
              actualCostUsd: deepCostUsd,
              hotelId: opportunity.hotelId,
              segmentId,
              opportunityId: opportunity.id,
              stage: "DEEP_RESEARCH",
              mode: restored.exaSeeded
                ? "EXA_DEEP_SEED_PLUS_ORS"
                : restored.orsInvoked
                  ? "ORS_SUPERVISOR_MATURE_OPPORTUNITY_NO_EXA_SEED"
                  : "DEEP_RESEARCH_SKIPPED",
              demandSegment: segmentId,
              agentFamily: routing.agentFamily,
              alreadyPaidDeep,
              exaSeeded: restored.exaSeeded === true,
              providerRunId:
                restored.exaProviderRunId ||
                opportunity?.exaAgentRouting?.providerRunId ||
                opportunity?.researchJobId ||
                opportunity?.exaRunId ||
                null,
              exaSeedReason: restored.exaSeedReason || null,
            });
          } else if (ledger && alreadyPaidDeep && deepCostUsd === 0) {
            // Zero-cost ORS continuation on previously paid evidence — no new settle.
            stages.push({
              stage: "DEEP_RESEARCH_IDEMPOTENT",
              note: "Reused prior paid Exa evidence; ORS supervisor continuation at $0",
              at: now(),
            });
          }
        } catch (err) {
          if (ledger && reservedQuote != null) {
            releaseReservation(ledger, reservedQuote);
          }
          stages.push({
            stage: "DEEP_RESEARCH",
            blocked: true,
            reason: String(err?.message || err),
          });
        }
      }
    }
  }

  const contactBundle =
    opts.allowOfficialSourceContactFollowUp === false
      ? runRestoredContactInvestigationPass(opportunity)
      : await runRestoredContactInvestigationPassAsync(opportunity, {
          allowOfficialSourceFollowUp: true,
          maxOfficialFetches: opts.maxOfficialContactFetches ?? 4,
        });
  opportunity = contactBundle.opportunity;
  const contactPass = contactBundle.pass;
  opportunity.chainContactResearch = contactPass;
  stages.push({
    stage: "CONTACT_RESEARCH",
    investigated: contactPass.contactsInvestigated,
    eligible: contactPass.verifiedEligible,
    secondContactAttempted: contactPass.secondContactAttempted === true,
    secondContactFound: contactPass.secondContactFound === true,
    contactGaps: contactPass.contactGaps || [],
    at: now(),
  });

  // Commercial eligibility contract BEFORE customer-readiness / V41 disposition finalization.
  const eligibilityBundle = runRestoredCommercialEligibilityPass(opportunity, {
    hotelName: opportunity.hotelName,
  });
  opportunity = eligibilityBundle.opportunity;
  stages.push({
    stage: "COMMERCIAL_ELIGIBILITY_CONTRACT",
    status: eligibilityBundle.result?.status || null,
    salesReady: eligibilityBundle.result?.salesReady === true,
    researchContinuation:
      eligibilityBundle.result?.researchContinuation === true,
    at: now(),
  });

  const needDateBundle = runRestoredNeedDatePass(opportunity, {
    hotelNeedDateConfig: opts.hotelNeedDateConfig || null,
  });
  opportunity = needDateBundle.opportunity;
  stages.push({
    stage: "NEED_DATE_INTEGRATION",
    hotelNeedDateStatus: needDateBundle.hotelNeedDateStatus || null,
    indicatorState: opportunity.needDateIndicator?.state || null,
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
  // Preserve historical V41 classification for audit before reconcile.
  opportunity.historicalInternalDisposition =
    opportunity.historicalInternalDisposition ||
    opportunity.internalDisposition ||
    qualification?.internalDisposition ||
    null;

  const dispositionReconcile = reconcileCommercialDispositionV1({
    eligibilityResult: eligibilityBundle.result,
    v41Qualification: qualification,
    priorDisposition: opportunity.internalDisposition,
  });
  opportunity.dispositionReconcile = dispositionReconcile;
  opportunity.internalDisposition = dispositionReconcile.finalDisposition;
  opportunity.customerPublicationReady = false;
  opportunity.autoPromoted = false;
  if (dispositionReconcile.conflict) {
    opportunity.dispositionConflict = true;
  }
  if (
    eligibilityBundle.result?.researchContinuation === true ||
    dispositionReconcile.eligibilityStatus === "RESEARCH_CONTINUATION"
  ) {
    opportunity.researchContinuationRequired = true;
  }

  stages.push({
    stage: "COMMERCIAL_QUALIFICATION",
    disposition: opportunity.internalDisposition,
    v41Disposition: dispositionReconcile.v41Disposition,
    commercialEligibilityStatus: opportunity.commercialEligibilityStatus || null,
    dispositionConflict: dispositionReconcile.conflict === true,
    dispositionExplanation: dispositionReconcile.explanation,
    at: now(),
  });

  const presentation = buildSharedTenSectionDrawerDto(opportunity, {
    hotelId: opportunity.hotelId,
    segmentId: opportunity.researchSegment,
  });
  const recommendedNextAction =
    salesBrief?.recommendedNextAction ||
    salesBrief?.nextStep ||
    salesBrief?.recommendedAction ||
    qualification?.recommendedNextAction ||
    (opportunity.primaryContact?.name || opportunity.contactName
      ? `Research outreach route to ${opportunity.primaryContact?.name || opportunity.contactName}${opportunity.primaryContact?.role || opportunity.contactRole ? ` (${opportunity.primaryContact?.role || opportunity.contactRole})` : ""} — verify lodging decision role before commercial outreach.`
      : "Continue public research for a named commercially relevant buyer or entry point before outreach.");

  opportunity.recommendedNextSalesAction = recommendedNextAction;
  opportunity.tenSectionDrawer = {
    version: presentation.version,
    sectionIds: presentation.sectionIds,
    sectionTitles: presentation.sectionTitles,
    opportunityId: presentation.opportunityId,
    segmentId: presentation.segmentId,
    hasSections: Boolean(presentation.sections),
    // Full customer presentation content — not metadata-only.
    sections: presentation.sections || null,
    overviewModel: presentation.overviewModel || null,
    salespersonPacket: {
      who:
        opportunity.primaryContact?.name ||
        opportunity.contactName ||
        null,
      whoRole:
        opportunity.primaryContact?.role ||
        opportunity.contactRole ||
        null,
      secondaryContact: opportunity.secondaryContact?.name || null,
      why:
        opportunity.hotelOpportunityThesis ||
        opportunity.title ||
        null,
      when:
        opportunity.needDateIndicator?.state ||
        opportunity.eventStartDate ||
        null,
      evidenceUrls: (opportunity.evidence || [])
        .map((e) => e.url)
        .filter(Boolean)
        .slice(0, 8),
      recommendedNextAction,
      qualificationRationale:
        dispositionReconcile.explanation ||
        qualification?.rationale ||
        null,
      commercialEligibilityStatus:
        opportunity.commercialEligibilityStatus || null,
      finalDisposition: opportunity.internalDisposition,
      uncertainties: opportunity.unresolvedQuestions || [],
    },
    completePersistedContent: Boolean(presentation.sections),
  };
  stages.push({
    stage: "SALES_INTELLIGENCE_OUTPUT",
    sections: (presentation.sectionIds || []).length,
    completePersistedContent:
      opportunity.tenSectionDrawer.completePersistedContent === true,
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
function resolveHotelDisplayName(hotelId, cellOrSeed = {}, hotelNameById = {}) {
  return (
    text(cellOrSeed.hotelName) ||
    text(hotelNameById[hotelId]) ||
    text(AIMBRIDGE_HOTEL_DISPLAY_NAMES[hotelId]) ||
    null
  );
}

/**
 * Round-robin opportunities across hotels so one hotel cannot consume the
 * entire remaining deep-research budget before others are investigated.
 */
export function interleaveOpportunitiesByHotel(opportunities = []) {
  const buckets = new Map();
  for (const opp of opportunities) {
    const key = String(opp.hotelId || "_unknown");
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(opp);
  }
  const queues = [...buckets.values()];
  const out = [];
  let progressed = true;
  while (progressed) {
    progressed = false;
    for (const q of queues) {
      if (!q.length) continue;
      out.push(q.shift());
      progressed = true;
    }
  }
  return out;
}

function looksLikeDestinationConferenceForLocalCatering(opp = {}) {
  const blob = `${opp.organizationName || ""} ${opp.title || ""} ${opp.programmeName || ""} ${opp.hotelOpportunityThesis || ""}`;
  return /annual conference|national conference|world congress|international congress|multi-?day conference|destination conference|annual meeting 20\d{2}/i.test(
    blob
  ) && !/chapter (meeting|luncheon|dinner)|monthly meeting|local training|half-?day|reception only/i.test(blob);
}

export function collectRetainedCandidatesFromDiscovery(discoveryExecution, opts = {}) {
  const out = [];
  const seen = new Set();
  const includeFixtureCandidates = opts.includeFixtureCandidates === true;
  const hotelNameById = opts.hotelNameById || {};
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
        hotelName: resolveHotelDisplayName(cell.hotelId, cell, hotelNameById),
        segmentId: cell.segmentId,
      });
      if (!opp.organizationName && !opp.title) continue;
      if (seen.has(opp.id)) continue;
      // LCM quality gate: destination conferences are not local catering demand.
      if (
        String(cell.segmentId) === "local_catering_day_meetings" &&
        looksLikeDestinationConferenceForLocalCatering(opp)
      ) {
        opp.discoveryRejected = true;
        opp.discoveryRejectReason =
          "DESTINATION_CONFERENCE_NOT_LOCAL_CATERING_DAY_MEETING";
        opp.internalDisposition = "EXCLUDE";
        opp.hotelOpportunityThesis = [
          opp.hotelOpportunityThesis,
          "Rejected for Local Catering & Day Meetings: appears to be a destination/national conference — route to Associations/Education if hotel-fit exists.",
        ]
          .filter(Boolean)
          .join(" ");
        // Keep for audit trail but do not queue for paid deep by default.
        if (opts.includeRejectedLcmDestinationConferences !== true) {
          seen.add(opp.id);
          continue;
        }
      }
      seen.add(opp.id);
      out.push(opp);
    }
  }
  // Optional: include prior inventory / live pilot state files as starting research state
  for (const seed of opts.seedOpportunities || []) {
    const opp = mapCandidateToOpportunity(seed, {
      hotelId: seed.hotelId,
      hotelName: resolveHotelDisplayName(seed.hotelId, seed, hotelNameById),
      segmentId: seed.segmentId || seed.researchSegment,
    });
    if (seen.has(opp.id)) continue;
    seen.add(opp.id);
    out.push(opp);
  }
  if (opts.useFairSegmentHotelScheduler === false && opts.interleaveHotels === false) {
    return out;
  }
  if (opts.useFairSegmentHotelScheduler === false) {
    return interleaveOpportunitiesByHotel(out);
  }
  // Default: segment-fair + hotel-fair (prevents early-segment starvation of late workflows).
  const scheduled = buildSegmentHotelFairDeepResearchQueue(out, {
    hotelIds: opts.hotelIds || [...new Set(out.map((o) => o.hotelId).filter(Boolean))],
    completedOpportunityIds: opts.completedOpportunityIds || [],
  });
  return scheduled.queue;
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

  const ledgerPath = path.join(outDir, "paid-budget-ledger.json");
  let paidBudgetLedger = opts.paidBudgetLedger || null;
  if (
    !paidBudgetLedger &&
    (authorizePaidDiscovery || authorizePaidResearch) &&
    budgetCeilingUsd > 0
  ) {
    paidBudgetLedger =
      opts.resumeLedger !== false
        ? loadPaidBudgetLedger(ledgerPath, {
            authorizationId:
              opts.authorizationId || "GDI_AIMBRIDGE_FULL_E2E_LIVE_V1",
            hardCeilingUsd: budgetCeilingUsd,
            effort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
          })
        : null;
    if (!paidBudgetLedger) {
      paidBudgetLedger = createPaidBudgetLedger({
        authorizationId:
          opts.authorizationId || "GDI_AIMBRIDGE_FULL_E2E_LIVE_V1",
        hardCeilingUsd: budgetCeilingUsd,
        effort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
        priorSpendUsd: opts.priorLiveSpendUsd || 0,
        outPath: ledgerPath,
      });
    }
  }
  if (paidBudgetLedger) persistPaidBudgetLedger(paidBudgetLedger);
  const alreadyDeepResearchedIds = paidBudgetLedger
    ? completedDeepResearchOpportunityIds(paidBudgetLedger)
    : new Set();

  const chainTrace = {
    version: ORIGINAL_FULL_E2E_RESEARCH_CHAIN_V1,
    at: now(),
    stages: [...CHAIN_STAGES],
    hotelIds,
    authorizePaidDiscovery,
    authorizePaidResearch,
    budgetCeilingUsd,
    authorizationId: paidBudgetLedger?.authorizationId || null,
    paidSpendUsd: 0,
    publicationOccurred: false,
    airtableWrites: 0,
  };

  // STAGE 1 — hotel profiles
  const profiles = [];
  const hotelNameById = { ...AIMBRIDGE_HOTEL_DISPLAY_NAMES };
  for (const hotelId of hotelIds) {
    // eslint-disable-next-line no-await-in-loop
    const resolved = await resolveHotelGdiProfileForResearch(hotelId, {
      skipHpc: opts.skipHpc !== false,
      preferLive: false,
    });
    const displayName =
      resolved.hotel?.displayName ||
      resolved.hotel?.name ||
      resolved.profile?.displayName ||
      hotelNameById[hotelId] ||
      null;
    if (displayName) hotelNameById[hotelId] = displayName;
    profiles.push({
      hotelId,
      ok: resolved.ok !== false,
      displayName,
      city: resolved.hotel?.city || resolved.profile?.city || null,
    });
  }
  chainTrace.hotelProfiles = profiles;

  // STAGE 2 — discovery (existing fourteen-segment matrix)
  // On resume, live cells already completed are skipped by durable discovery-state.
  const discoveryExecution = await executeFourteenSegmentDiscoveryMatrix({
    hotelIds,
    authorizePaidDiscovery,
    useFixtureProvider: opts.useFixtureProvider !== false && !authorizePaidDiscovery,
    allowHistoricalReplay: opts.allowHistoricalReplay !== false,
    persistState: opts.persistState !== false,
    runQualificationHandoff: false, // replaced by full chain below
    paidBudgetLedger,
    exaEffort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
  });
  // Discovery-only spend portion: total ledger spend at this point may include
  // prior deep research when resuming — report both.
  chainTrace.discovery = {
    cellCount: discoveryExecution.summary?.cellCount || 0,
    extracted: discoveryExecution.summary?.totalExtractedCandidates || 0,
    costUsd: Number(
      (paidBudgetLedger?.operations || [])
        .filter((op) => op.stage === "DISCOVERY")
        .reduce((n, op) => n + Number(op.actualCostUsd || 0), 0)
        .toFixed(6)
    ),
    freshRetrievalCells: discoveryExecution.summary?.freshRetrievalCells || 0,
    resumedCells: discoveryExecution.summary?.resumedCells || 0,
    paidBudgetRemainingUsd: paidBudgetLedger
      ? remainingBudgetUsd(paidBudgetLedger)
      : null,
  };
  chainTrace.paidSpendUsd = paidBudgetLedger
    ? Number(paidBudgetLedger.spentUsd || 0)
    : Number(chainTrace.discovery.costUsd || 0);

  let opportunities = collectRetainedCandidatesFromDiscovery(discoveryExecution, {
    seedOpportunities: opts.seedOpportunities || [],
    hotelNameById,
    hotelIds,
    completedOpportunityIds:
      opts.skipCompletedDeepResearch !== false ? alreadyDeepResearchedIds : [],
    useFairSegmentHotelScheduler: opts.useFairSegmentHotelScheduler !== false,
    interleaveHotels: opts.interleaveHotels !== false,
  });
  const fairCoverage = summarizeFairQueueCoverage({
    queue: opportunities,
    totalQueued: opportunities.length,
    firstOccurrenceBySegment: Object.fromEntries(
      listFourteenWorkflowSegmentIds().map((seg) => [
        seg,
        opportunities.findIndex((o) => String(o.researchSegment) === seg),
      ])
    ),
  });
  chainTrace.fairScheduler = fairCoverage;
  // No 10–15 cap. Optional safety bound only when explicitly set.
  if (maxCandidates != null && Number.isFinite(maxCandidates) && maxCandidates > 0) {
    opportunities = opportunities.slice(0, maxCandidates);
    chainTrace.candidateCapApplied = maxCandidates;
  } else {
    chainTrace.candidateCapApplied = null;
  }
  chainTrace.retainedCandidates = opportunities.length;
  chainTrace.skippedAlreadyDeepResearched = 0;

  // STAGES 3–7 per candidate
  const processed = [];
  let remaining = paidBudgetLedger
    ? remainingBudgetUsd(paidBudgetLedger)
    : authorizePaidResearch && budgetCeilingUsd > 0
      ? Math.max(0, budgetCeilingUsd - chainTrace.paidSpendUsd)
      : 0;

  const writeProgressSnapshot = (reason = "INTERVAL") => {
    try {
      fs.mkdirSync(outDir, { recursive: true });
      const byHotel = {};
      const bySegment = {};
      const dispositions = {};
      for (const p of processed) {
        const h = p.hotelId || "_unknown";
        const s = p.segmentId || "_unknown";
        byHotel[h] = byHotel[h] || { processed: 0, blocked: 0, skipped: 0 };
        bySegment[s] = bySegment[s] || { processed: 0, blocked: 0, skipped: 0 };
        if (p.skipped) {
          byHotel[h].skipped += 1;
          bySegment[s].skipped += 1;
        } else if (p.blocked) {
          byHotel[h].blocked += 1;
          bySegment[s].blocked += 1;
        } else {
          byHotel[h].processed += 1;
          bySegment[s].processed += 1;
          const d = String(p.disposition || "UNKNOWN");
          dispositions[d] = (dispositions[d] || 0) + 1;
        }
      }
      const pendingQueue = opportunities
        .slice(processed.length)
        .slice(0, 40)
        .map((o) => ({
          opportunityId: o.id,
          hotelId: o.hotelId,
          segmentId: o.researchSegment,
          organizationName: o.organizationName,
        }));
      const snap = {
        version: ORIGINAL_FULL_E2E_RESEARCH_CHAIN_V1,
        kind: "CHAIN_PROGRESS_SNAPSHOT",
        reason,
        at: now(),
        authorizationId: chainTrace.authorizationId,
        hotelIds,
        paidSpendUsd: paidBudgetLedger
          ? Number(paidBudgetLedger.spentUsd || 0)
          : chainTrace.paidSpendUsd,
        remainingBudgetUsd: paidBudgetLedger
          ? remainingBudgetUsd(paidBudgetLedger)
          : remaining,
        retainedCandidates: opportunities.length,
        skippedAlreadyDeepResearched: chainTrace.skippedAlreadyDeepResearched,
        processedCount: processed.filter((p) => !p.blocked && !p.skipped).length,
        blockedCount: processed.filter((p) => p.blocked).length,
        skippedCount: processed.filter((p) => p.skipped).length,
        lastProcessed: processed.length
          ? processed[processed.length - 1]
          : null,
        perHotel: byHotel,
        perSegment: bySegment,
        dispositions,
        fairScheduler: chainTrace.fairScheduler || null,
        pendingQueue,
        stopReason: reason,
        finalReportPending: reason !== "LOOP_COMPLETE",
        founderFacingPartial: true,
      };
      fs.writeFileSync(
        path.join(outDir, "chain-progress-snapshot.json"),
        `${JSON.stringify(snap, null, 2)}\n`
      );
      // Durable partial founder summary on interrupt or periodic checkpoints
      if (
        reason.startsWith("SIGNAL_") ||
        reason === "LOOP_COMPLETE" ||
        processed.length % 25 === 0
      ) {
        fs.writeFileSync(
          path.join(outDir, "founder-partial-progress-report.json"),
          `${JSON.stringify(snap, null, 2)}\n`
        );
      }
    } catch (err) {
      console.error(
        "[gdi-full-e2e] progress snapshot failed:",
        String(err?.message || err)
      );
    }
  };
  writeProgressSnapshot("LOOP_START");
  if (typeof process !== "undefined" && process.on) {
    const onSignal = (sig) => {
      writeProgressSnapshot(`SIGNAL_${sig}`);
    };
    process.once("SIGINT", () => onSignal("SIGINT"));
    process.once("SIGTERM", () => onSignal("SIGTERM"));
  }

  /** Same org across segments must not receive repeated paid Exa deep seeds. */
  const paidDeepOrgKeys = new Set();
  const inventoryDedupe = buildInventoryDedupeContextForHotels(hotelIds);
  chainTrace.inventoryDedupe = {
    totalOrgKeys: inventoryDedupe.totalOrgKeys,
    totalOpportunityIds: inventoryDedupe.totalOpportunityIds,
    perHotel: inventoryDedupe.perHotel,
  };
  chainTrace.skippedExistingInventory = 0;
  chainTrace.skippedCrossSegmentOrg = 0;
  const postDeepPeers = [];

  for (const opp of opportunities) {
    if (
      authorizePaidResearch &&
      alreadyDeepResearchedIds.has(String(opp.id)) &&
      opts.skipCompletedDeepResearch !== false
    ) {
      chainTrace.skippedAlreadyDeepResearched += 1;
      processed.push({
        opportunityId: opp.id,
        organizationName: opp.organizationName,
        segmentId: opp.researchSegment,
        hotelId: opp.hotelId,
        skipped: true,
        reason: "DEEP_RESEARCH_ALREADY_SETTLED_IN_LEDGER",
        continuation: false,
      });
      continue;
    }
    const preDedupe = evaluatePrePaidDeepDedupe(opp, {
      completedDeepResearchIds: alreadyDeepResearchedIds,
      paidDeepOrgKeys,
      inventoryOrgKeys: inventoryDedupe.inventoryOrgKeys,
      inventoryOpportunityIds: inventoryDedupe.inventoryOpportunityIds,
    });
    if (
      authorizePaidResearch &&
      preDedupe.skipPaidDeep &&
      preDedupe.reasonCode === "EXISTING_HOTEL_INVENTORY_ORG"
    ) {
      chainTrace.skippedExistingInventory += 1;
      processed.push({
        opportunityId: opp.id,
        organizationName: opp.organizationName,
        segmentId: opp.researchSegment,
        hotelId: opp.hotelId,
        skipped: true,
        reason: "EXISTING_HOTEL_INVENTORY_ORG",
        dedupe: preDedupe,
        continuation: false,
      });
      continue;
    }
    if (authorizePaidResearch && paidBudgetLedger && !canAffordQuotedCall(paidBudgetLedger).ok) {
      processed.push({
        opportunityId: opp.id,
        organizationName: opp.organizationName,
        segmentId: opp.researchSegment,
        blocked: true,
        reason: "BUDGET_EXHAUSTED_OR_FROZEN",
        continuation: true,
      });
      continue;
    }
    if (authorizePaidResearch && !paidBudgetLedger && remaining <= 0) {
      processed.push({
        opportunityId: opp.id,
        blocked: true,
        reason: "BUDGET_EXHAUSTED",
        continuation: true,
      });
      continue;
    }
    if (!opp.hotelName && hotelNameById[opp.hotelId]) {
      opp.hotelName = hotelNameById[opp.hotelId];
    }
    const deepOrgKey = orgDeepKey(opp);
    const crossSegmentOrgAlreadyPaidDeep =
      Boolean(deepOrgKey) && paidDeepOrgKeys.has(deepOrgKey);
    if (crossSegmentOrgAlreadyPaidDeep) {
      chainTrace.skippedCrossSegmentOrg += 1;
    }
    const allowLiveExaDeepSeedForOpp =
      opts.allowLiveExaDeepSeed === true &&
      !crossSegmentOrgAlreadyPaidDeep &&
      preDedupe.allowLiveExaDeepSeed !== false;
    // eslint-disable-next-line no-await-in-loop
    const result = await processOpportunityThroughOriginalChain(opp, {
      authorizePaidResearch,
      remainingBudgetUsd: paidBudgetLedger
        ? remainingBudgetUsd(paidBudgetLedger)
        : remaining,
      skipDeepResearch: opts.skipDeepResearch === true,
      maxIterations: opts.maxIterations,
      liveResearcher: opts.liveResearcher,
      paidBudgetLedger,
      exaEffort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
      // Idempotent Exa: ORS may continue offline; never re-seed paid deep.
      completedDeepResearchIds: alreadyDeepResearchedIds,
      authorizeExaAgentLive: opts.authorizeExaAgentLive === true,
      allowLiveExaDeepSeed: allowLiveExaDeepSeedForOpp,
      hotelNeedDateConfig: opts.hotelNeedDateConfig || null,
      checkpointDir: opts.checkpointDir || null,
      crossSegmentOrgAlreadyPaidDeep,
    });
    if (
      allowLiveExaDeepSeedForOpp &&
      Number(result.paidSpendUsd || 0) > 0 &&
      deepOrgKey
    ) {
      paidDeepOrgKeys.add(deepOrgKey);
    } else if (result?.opportunity?.exaAgentDeepResearch && deepOrgKey) {
      paidDeepOrgKeys.add(deepOrgKey);
    }
    chainTrace.paidSpendUsd = paidBudgetLedger
      ? Number(paidBudgetLedger.spentUsd || 0)
      : chainTrace.paidSpendUsd + Number(result.paidSpendUsd || 0);
    remaining = paidBudgetLedger
      ? remainingBudgetUsd(paidBudgetLedger)
      : Math.max(0, remaining - Number(result.paidSpendUsd || 0));
    const postIdentity = reconcilePostDeepIdentity(
      result.opportunity,
      postDeepPeers
    );
    postDeepPeers.push(result.opportunity);
    processed.push({
      opportunityId: result.opportunity.id,
      organizationName: result.opportunity.organizationName,
      hotelId: result.opportunity.hotelId || opp.hotelId,
      segmentId: result.opportunity.researchSegment,
      disposition: result.opportunity.internalDisposition,
      contactsInvestigated: result.contactPass.contactsInvestigated,
      salesReadyContacts: result.contactPass.salesReadyContacts,
      presentationSections: (result.presentation.sectionIds || []).length,
      commercialResearchComplete: result.commercialResearchComplete,
      paidSpendUsd: result.paidSpendUsd,
      stages: result.stages.map((s) => s.stage),
      blocked: result.stages.some((s) => s.blocked),
      postDeepIdentity: postIdentity,
      prePaidDeepDedupe: preDedupe,
    });
    if (processed.length === 1 || processed.length % 10 === 0) {
      writeProgressSnapshot(`AFTER_${processed.length}`);
    }
  }
  writeProgressSnapshot("LOOP_COMPLETE");

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
