/**
 * Autonomous portfolio ownership V1 — portfolio supervisor only.
 *
 * Selects / schedules / persists opportunities. Authoritative commercial
 * investigation is mature FullE2E:
 *   runDeepResearchOnExistingGdiOpportunity → matureOpportunity (ORS)
 *   → Scaling investigation loop / Association long-form
 *   → FullE2E gold brief adapters
 *
 * Ownership must not replace ORS with a truncated investigation-only path.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunities, saveOpportunities } from "../repository.js";
import { invalidateGdiHotelReadCache } from "../read-cache.js";
import {
  DOS_GRADE,
  assessOutreachRoutes,
  buildImprovedHotelThesis,
  assessGoldBriefCompleteness,
} from "../commercial-sales-quality-v1.js";
import { runScalingIntelligentInvestigationLoop } from "./scaling-intelligent-investigation-loop-v1.js";
import { runAssociationEventsLongFormInvestigation } from "./association-events-long-form-investigation-v1.js";
import {
  maybeExaSearch,
  createLiveDeepResearchResearcher,
} from "./deep-research-gdi-live-certification-v1.js";
import {
  runDeepResearchOnExistingGdiOpportunity,
  createFixtureResearcher,
} from "./deep-research-gdi-integration-v1.js";
import { applyAssociationEventsGoldBrief } from "./association-events-gold-brief-v1.js";
import { applyScalingSalesPresentation } from "../scaling-companies-sales-presentation-v1.js";
import { applyScalingTwoContactStandard } from "../scaling-two-contact-standard-v1.js";
import { attachSecondaryNamedContact } from "../secondary-named-contact-v1.js";
import {
  campaignAggregateLedgerPath,
  remainingAggregateUsd,
  assertPaidExecutionAuthorized,
  loadAuthorizationStatus,
  AUTHORIZATION_STATUS,
  WATERSTONE_HOTEL_ID,
  CHAPULTEPEC_HOTEL_ID,
} from "./waterstone-chapultepec-shared-budget-v1.js";

export const AUTONOMOUS_PORTFOLIO_OWNERSHIP_V1 = "gdi-autonomous-portfolio-ownership-v1";
/** Authoritative research executor restored in run-039 */
export const AUTHORITATIVE_RESEARCH_EXECUTOR_V1 =
  "fulle2e_ors_matureOpportunity_plus_segment_investigation";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadHotelConfig(hotelId) {
  const p = path.join(ROOT, "config/group-demand-intelligence/hotels", `${hotelId}.json`);
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Score research-ownership priority from the six commercial outcomes.
 * Higher = deepen next. Deterministic from evidence state — no named ID lists.
 */
export function assessSixCommercialOutcomes(opp = {}, hotel = {}) {
  const brief = opp.salesBrief || opp.commercialIntelligenceBrief || {};
  const sources = [
    ...(Array.isArray(opp.sources) ? opp.sources : []),
    ...(Array.isArray(opp.evidenceUrls) ? opp.evidenceUrls : []),
    ...(Array.isArray(brief?.evidenceAndUncertainty?.sources)
      ? brief.evidenceAndUncertainty.sources
      : []),
  ];
  const contact = opp.primaryContact || brief?.whoToContact?.primary || {};
  // run-051: official /contact pages on the opportunity are credible org routes.
  // Homepage-only URLs (e.g. medicosalfrente.com/) must NOT clear the contact gap.
  const contactPageUrl = (() => {
    const candidates = [
      contact.url,
      opp.contactUrl,
      opp.organizationRouteUrl,
      opp.organizationContactUrl,
    ]
      .map((u) => String(u || "").trim())
      .filter(Boolean);
    for (const u of candidates) {
      if (/\/(contact|contacts|enquire|inquiry|enquiries|meetings|groups|group-sales|sales)(\/|$|\?)/i.test(u)) {
        return u;
      }
    }
    return null;
  })();
  const routes = assessOutreachRoutes(
    {
      ...contact,
      url: contact.url || contactPageUrl || null,
    },
    {
      organizationName: opp.organizationName,
      evidenceUrls: sources,
      contactPageUrl,
    },
  );
  const thesis = text(
    opp.hotelOpportunityThesis ||
      brief.whyThisHotel ||
      brief.potentialBusinessForThisHotel ||
      "",
  );
  const demand = text(
    opp.roomDemandRationale ||
      brief.potentialBusinessForThisHotel ||
      brief.companyAndLocalActivity ||
      "",
  );
  const activity = text(
    brief.opportunityOverview ||
      brief.companyAndLocalActivity ||
      opp.title ||
      opp.eventName ||
      "",
  );
  const strategy = text(
    brief?.recommendedSalesApproach?.nextAction ||
      opp.salesActionRecommended ||
      "",
  );
  const genericFit =
    /same city|same commercial market|in the (same )?city|located in|generic hotel|meeting rooms and/i.test(
      thesis,
    ) &&
    (thesis.length < 280 ||
      !/(reforma|polanco|chapultepec|corridor|overflow|room.?block|visitor lodging|project-team)/i.test(
        thesis,
      ));
  const past =
    opp.dosGrade === DOS_GRADE.DO_NOT_PURSUE ||
    opp.priority === "DISQUALIFIED" ||
    /historical|paris — historical|do not pursue/i.test(String(opp.title || ""));

  const outcomes = {
    opportunity: {
      addressed: activity.length >= 40,
      gap: activity.length < 40 ? "WHAT_IS_HAPPENING_UNCLEAR" : null,
    },
    demand: {
      addressed: demand.length >= 40 && !/every company|automatically creates/i.test(demand),
      gap: demand.length < 40 ? "DEMAND_MECHANISM_UNCLEAR" : null,
    },
    hotelFit: {
      addressed: thesis.length >= 60 && !genericFit,
      gap: !thesis || genericFit ? "HOTEL_FIT_GENERIC_OR_MISSING" : null,
    },
    people: {
      addressed:
        routes.hasVerifiedNamedRelevantContact === true ||
        routes.hasCredibleOutreachRoute === true,
      gap:
        routes.hasVerifiedNamedRelevantContact || routes.hasCredibleOutreachRoute
          ? null
          : "NO_CREDIBLE_CONTACT_ROUTE",
    },
    salesStrategy: {
      addressed: strategy.length >= 40 && !/validate before outreach/i.test(strategy),
      gap: strategy.length < 40 ? "SALES_STRATEGY_GENERIC_OR_MISSING" : null,
    },
    evidence: {
      addressed: sources.length >= 1,
      gap: sources.length < 1 ? "NO_SOURCE_EVIDENCE" : null,
    },
  };

  const gaps = Object.values(outcomes)
    .map((o) => o.gap)
    .filter(Boolean);

  return {
    outcomes,
    gaps,
    past,
    routes,
    sourceCount: sources.length,
    completeness: assessGoldBriefCompleteness({
      ...opp,
      salesBrief: brief,
      primaryContact: contact,
      evidenceUrls: sources,
      hotelOpportunityThesis: thesis,
    }),
  };
}

export function scoreOwnershipPriority(opp = {}, hotel = {}) {
  const assess = assessSixCommercialOutcomes(opp, hotel);
  if (assess.past && opp.commercialProspectTier !== "RESEARCH_CONTINUATION") {
    return { score: -100, reason: "historical_or_disqualified", assess };
  }
  let score = 0;
  // Prefer incomplete but promising research
  score += assess.gaps.length * 12;
  if (assess.outcomes.demand.addressed) score += 8;
  if (assess.outcomes.opportunity.addressed) score += 8;
  if (!assess.outcomes.hotelFit.addressed) score += 15;
  if (!assess.outcomes.people.addressed) score += 14;
  if (!assess.outcomes.salesStrategy.addressed) score += 10;
  if (!assess.outcomes.evidence.addressed) score += 18;
  if (opp.commercialProspectTier === "PRIORITY_SALES_PROSPECT") score += 20;
  if (opp.commercialProspectTier === "EXPLORATORY_SALES_PROSPECT") score += 12;
  if (opp.commercialProspectTier === "RESEARCH_CONTINUATION") score += 16;
  if (opp.dosGrade === DOS_GRADE.RESEARCH_FIRST) score += 10;
  if (opp.dosGrade === DOS_GRADE.ASSIGN_FOR_DISCOVERY) score += 6;
  if (opp.liveResearchRun) score += 4; // recently touched — still deepen if gaps
  // Future timing boost
  if (/2027|2028|2026-1[1-2]|2026-0[9]|jun|oct|nov|dec/i.test(
    `${opp.eventDateDisplay || ""} ${opp.eventStartDate || ""} ${opp.title || ""}`,
  )) {
    score += 8;
  }
  // Suppress Cancún-for-Waterstone noise already marked
  if (
    hotel?.hotelId === WATERSTONE_HOTEL_ID &&
    /canc[uú]n/i.test(`${opp.title || ""} ${opp.eventLocationSummary || ""}`)
  ) {
    score -= 80;
  }
  // Prefer future editions over clearly past-year titles (agent still may investigate once)
  const yearHit = String(opp.title || "").match(/\b(20\d{2})\b/);
  if (yearHit) {
    const y = Number(yearHit[1]);
    const nowY = new Date().getUTCFullYear();
    if (Number.isFinite(y) && y < nowY) score -= 40;
  }
  return {
    score,
    reason: assess.gaps[0] || "deepen_commercial_outcomes",
    assess,
    nextQuestionHint: assess.gaps[0] || "IMPROVE_HOTEL_SPECIFIC_DEMAND_THESIS",
  };
}

/**
 * Autonomously select portfolio candidates — no hardcoded opportunity IDs.
 */
export function selectOwnershipCandidates(opportunities = [], hotel = {}, { limit = 4 } = {}) {
  const ranked = (opportunities || [])
    .map((o) => ({ opportunity: o, ...scoreOwnershipPriority(o, hotel) }))
    .filter((r) => r.score > 0)
    .sort((a, b) => b.score - a.score);
  // Diversify segments when possible
  const picked = [];
  const seenSeg = new Set();
  for (const row of ranked) {
    if (picked.length >= limit) break;
    const seg = String(
      row.opportunity.researchSegment || row.opportunity.segment || row.opportunity.demandFamily || "",
    ).toLowerCase();
    if (picked.length >= 2 && seenSeg.has(seg) && ranked.length > limit) {
      // Prefer diversity for first slots; allow same segment later
      const hasOther = ranked.some(
        (r) =>
          !picked.includes(r) &&
          String(r.opportunity.researchSegment || r.opportunity.segment || "").toLowerCase() !== seg &&
          r.score > 0,
      );
      if (hasOther && picked.length < Math.min(3, limit)) continue;
    }
    picked.push(row);
    seenSeg.add(seg);
  }
  // Fill remaining from ranked
  for (const row of ranked) {
    if (picked.length >= limit) break;
    if (!picked.includes(row)) picked.push(row);
  }
  return picked.slice(0, limit);
}

function isAssociationLike(opp = {}) {
  const blob = `${opp.researchSegment || ""} ${opp.segment || ""} ${opp.demandFamily || ""} ${opp.opportunityType || ""}`.toLowerCase();
  return /associat|conference|event|congress|e2d/.test(blob) || String(opp.id || "").includes("gdi_e2d_");
}

function isScalingLike(opp = {}) {
  const blob = `${opp.researchSegment || ""} ${opp.segment || ""} ${opp.demandFamily || ""}`.toLowerCase();
  return /scaling|corporate|company|sc_/.test(blob) || String(opp.id || "").startsWith("gdi_sc_");
}

function mapDosFromInvestigation(opp, assess, stopReason) {
  if (opp.dosGrade === DOS_GRADE.DO_NOT_PURSUE || assess.past) return DOS_GRADE.DO_NOT_PURSUE;
  if (/HOST_CONTRACTED_NO_OPEN_DEMAND|ENTITY_INVALID|CONFIRMED_NOT_COMMERCIALLY/i.test(stopReason || "")) {
    return DOS_GRADE.DO_NOT_PURSUE;
  }
  const named = assess.routes?.hasVerifiedNamedRelevantContact === true;
  const route = assess.routes?.hasCredibleOutreachRoute === true;
  const demandOk = assess.outcomes?.demand?.addressed;
  const fitOk = assess.outcomes?.hotelFit?.addressed;
  if (named && demandOk && fitOk && assess.sourceCount >= 1) return DOS_GRADE.ASSIGN_NOW;
  if ((named || route) && (demandOk || fitOk) && assess.sourceCount >= 1) {
    return DOS_GRADE.ASSIGN_FOR_DISCOVERY;
  }
  if (assess.gaps?.length >= 3) return DOS_GRADE.RESEARCH_FIRST;
  return DOS_GRADE.ASSIGN_FOR_DISCOVERY;
}

/**
 * Build a zero-spend ORS corpus from already-retrieved opportunity evidence.
 * Enables mature FullE2E replay without paid discovery.
 */
export function buildHeldEvidenceCorpus(opp = {}) {
  const corpus = {};
  const org = text(opp.organizationName || opp.title);
  const push = (url, body) => {
    const u = text(url);
    if (!u || !/^https?:/i.test(u)) return;
    let b = text(body);
    // URL-only held sources still seed ORS fixture pages so matureOpportunity runs.
    if (b.length < 40) {
      b = `Held public source for ${org || "organization"} at ${u}. Prior GDI research retained this URL for commercial follow-up.`;
    }
    if (!corpus[u] || corpus[u].length < b.length) corpus[u] = b.slice(0, 12000);
  };
  for (const s of [...(opp.sources || []), ...(opp.evidenceUrls || [])]) {
    if (typeof s === "string") push(s, "");
    else push(s?.url, s?.excerpt || s?.statement || s?.quote || s?.text);
  }
  for (const e of opp.evidence || []) {
    push(e?.url, e?.statement || e?.quote || e?.excerpt);
  }
  for (const c of opp.acceptedClaims || []) {
    push(c?.url, c?.statement || c?.excerpt);
  }
  for (const u of opp.investigatedUrls || opp.seedUrls || []) {
    push(u, "");
  }
  return corpus;
}

function ledgerPaidHalted(ledgerPath) {
  try {
    if (!ledgerPath || !fs.existsSync(ledgerPath)) return false;
    return JSON.parse(fs.readFileSync(ledgerPath, "utf8")).haltedPaidExecution === true;
  } catch {
    return false;
  }
}

function buildOwnershipSalesBrief(opp, hotel, assess, investigation) {
  const hotelLabel = hotel.displayName || hotel.name || "this hotel";
  const thesisBuild = buildImprovedHotelThesis(
    {
      ...opp,
      hotelOpportunityThesis: opp.hotelOpportunityThesis,
      eventLocationSummary: opp.eventLocationSummary,
    },
    hotelLabel,
  );
  // Prefer FullE2E gold brief / commercial intelligence — ownership only annotates history.
  const priorRaw =
    opp.associationEventsGoldBrief ||
    opp.commercialIntelligenceBrief ||
    opp.salesBrief ||
    {};
  const prior = {
    ...priorRaw,
    potentialBusinessForThisHotel:
      priorRaw.potentialBusinessForThisHotel || priorRaw.potentialBusinessForHotel,
    companyAndLocalActivity:
      priorRaw.companyAndLocalActivity || priorRaw.eventHistoryAndActivity,
  };
  const isFullE2eBrief =
    /gold|scaling|association|commercial.?intelligence|v41|honest/i.test(
      String(prior.version || prior.briefVersion || ""),
    ) || Boolean(opp.commercialIntelligenceBrief || opp.associationEventsGoldBrief);
  return {
    ...prior,
    version: isFullE2eBrief
      ? prior.version || "gdi-fulle2e-gold-brief-preserved-v1"
      : "gdi-autonomous-portfolio-ownership-brief-v1",
    opportunityOverview:
      prior.opportunityOverview ||
      text(opp.title) ||
      `${opp.organizationName || "Organization"} — commercial research ownership pass`,
    companyAndLocalActivity:
      prior.companyAndLocalActivity ||
      prior.eventHistoryAndActivity ||
      text(opp.eventLocationSummary),
    potentialBusinessForThisHotel:
      prior.potentialBusinessForThisHotel ||
      opp.roomDemandRationale ||
      "Demand mechanism under investigation — see evidence and gaps.",
    whyThisHotel: prior.whyThisHotel || thesisBuild.thesis || opp.hotelOpportunityThesis,
    buyingAndSourcingStatus:
      prior.buyingAndSourcingStatus || "Hotel sourcing unconfirmed; no fabricated contract status.",
    recommendedSalesApproach: {
      ...(prior.recommendedSalesApproach || {}),
      nextAction:
        prior?.recommendedSalesApproach?.nextAction ||
        (assess.routes?.hasCredibleOutreachRoute
          ? "Approach verified contact route; confirm lodging/meeting needs and timing."
          : "Continue contact/demand research before sales assignment."),
      qualificationQuestions:
        prior?.recommendedSalesApproach?.qualificationQuestions ||
        assess.gaps.map((g) => `Resolve: ${g}`).slice(0, 5),
      conversationOpener: prior?.recommendedSalesApproach?.conversationOpener || null,
    },
    evidenceAndUncertainty: {
      uncertainties: [
        ...(prior?.evidenceAndUncertainty?.uncertainties || []),
        ...assess.gaps,
      ].slice(0, 10),
      sources: (
        prior?.evidenceAndUncertainty?.sources ||
        opp.sources ||
        opp.evidenceUrls ||
        []
      ).slice(0, 12),
    },
    whoToContact: prior.whoToContact || {
      primary: opp.primaryContact || null,
    },
    investigationHistory: {
      ...(prior.investigationHistory || {}),
      ownershipVersion: AUTONOMOUS_PORTFOLIO_OWNERSHIP_V1,
      authoritativeExecutor: AUTHORITATIVE_RESEARCH_EXECUTOR_V1,
      pathUsed: investigation?.pathUsed || [],
      stopReason: investigation?.stopReason || investigation?.disposition || null,
      attempts: investigation?.attempts || investigation?.metrics || null,
      nextQuestionSelected: investigation?.nextQuestion || assess.gaps[0] || null,
      sixOutcomes: assess.outcomes,
      at: new Date().toISOString(),
    },
    commercialOutcomesAddressed: Object.fromEntries(
      Object.entries(assess.outcomes).map(([k, v]) => [k, v.addressed]),
    ),
  };
}

export async function deepenOpportunityOwnership({
  opportunity,
  hotel,
  ledgerPath,
  allowPaid = true,
  maxPaidCalls = 4,
} = {}) {
  const assessBefore = assessSixCommercialOutcomes(opportunity, hotel);
  const nextQuestion = assessBefore.gaps[0] || "IMPROVE_COMMERCIAL_THESIS";
  const pathUsed = [];
  let paidCalls = 0;
  const paidHalted = ledgerPaidHalted(ledgerPath);
  const canPay = allowPaid === true && !paidHalted && Boolean(ledgerPath);
  // Investigation loop calls paidSearch({ query, opportunity, strategy }).
  // Direct FullE2E destructures { query }; ownership previously passed the whole
  // object into maybeExaSearch → String(obj) === "[object Object]" → Exa 400 hold.
  const paidSearch = canPay
    ? async (queryOrOpts) => {
        if (paidCalls >= maxPaidCalls) return { status: "call_cap", hits: [], costUsd: 0 };
        const query =
          typeof queryOrOpts === "string"
            ? queryOrOpts
            : queryOrOpts?.query || queryOrOpts?.q || "";
        if (!String(query || "").trim()) {
          return { status: "invalid_query", hits: [], costUsd: 0 };
        }
        paidCalls += 1;
        return maybeExaSearch(query, ledgerPath);
      }
    : null;

  let investigation = null;
  let nextOpp = {
    ...opportunity,
    hotelId: hotel.hotelId || opportunity.hotelId,
    hotelName: hotel.displayName || hotel.name || opportunity.hotelName,
  };
  const corpus = buildHeldEvidenceCorpus(nextOpp);

  // --- Path A (authoritative): FullE2E ORS matureOpportunity ---
  try {
    let researcher = null;
    if (canPay) {
      researcher = createLiveDeepResearchResearcher({ ledgerPath });
      pathUsed.push("live_deep_research_researcher");
    } else if (Object.keys(corpus).length) {
      researcher = createFixtureResearcher(corpus);
      pathUsed.push("held_evidence_fixture_researcher");
    }
    if (researcher) {
      const deep = await runDeepResearchOnExistingGdiOpportunity({
        opportunity: nextOpp,
        researcher,
        corpus: Object.keys(corpus).length ? corpus : null,
        offlineLongform: Object.keys(corpus).length > 0,
        options: {
          authorizeSurfe: false,
          authorizePdl: false,
          maxIterations: 6,
          paidBlocked: !canPay,
          resetResearchAttempts: true,
          executeLeadFollowUps: true,
        },
      });
      const deepOpp =
        deep?.after?.opportunity || deep?.opportunity || deep?.matured || null;
      if (deepOpp) {
        nextOpp = { ...nextOpp, ...deepOpp, id: nextOpp.id };
        pathUsed.push("ors_matureOpportunity");
      }
      investigation = {
        ...(investigation || {}),
        orsOk: deep?.ok !== false,
        orsStopReason:
          deep?.after?.orsStopReason || deep?.stopReason || deep?.stoppingReason || null,
        dossier: deep?.dossierResult?.dossier || deep?.dossier || null,
      };
    } else {
      pathUsed.push("ors_skipped_no_researcher_or_corpus");
    }
  } catch (err) {
    pathUsed.push(`ors_error:${text(err?.message || err).slice(0, 120)}`);
  }

  // --- FullE2E segment investigation (same post-ORS stage as hotel runners) ---
  if (isAssociationLike(opportunity)) {
    investigation = await runAssociationEventsLongFormInvestigation({
      opportunity: nextOpp,
      hotel,
      allowFreeFetch: true,
      maxIterations: 8,
      maxPageFetches: 10,
      paidSearch,
    });
    nextOpp = investigation.opportunity || investigation || nextOpp;
    pathUsed.push("association_events_long_form_investigation");
    nextOpp = applyAssociationEventsGoldBrief(nextOpp, { hotel });
    // Map FullE2E association gold brief into salesBrief (drawer contract).
    if (nextOpp.associationEventsGoldBrief) {
      const g = nextOpp.associationEventsGoldBrief;
      nextOpp.salesBrief = {
        ...g,
        version: g.version || "gdi-association-events-gold-brief-v1",
        potentialBusinessForThisHotel:
          g.potentialBusinessForThisHotel || g.potentialBusinessForHotel,
        companyAndLocalActivity:
          g.companyAndLocalActivity || g.eventHistoryAndActivity,
      };
    }
    pathUsed.push("association_events_gold_brief");
  } else {
    investigation = await runScalingIntelligentInvestigationLoop({
      opportunity: nextOpp,
      hotel,
      allowFreeFetch: true,
      maxIterations: 8,
      maxPageFetches: 10,
      paidSearch,
    });
    nextOpp = investigation.opportunity || nextOpp;
    pathUsed.push("scaling_intelligent_investigation_loop");
    nextOpp = attachSecondaryNamedContact(
      {
        ...nextOpp,
        hotelId: hotel.hotelId,
        researchSegment: nextOpp.researchSegment || "scaling_companies",
      },
      { allowSavedEvidence: true },
    );
    nextOpp = applyScalingTwoContactStandard(
      applyScalingSalesPresentation(nextOpp, {
        hotelDisplayName: hotel.displayName || hotel.name || null,
        hotel,
        applyCommercialIntelligenceBrief: true,
      }),
      { force: true },
    );
    pathUsed.push("scaling_sales_presentation_gold_brief");
  }

  const assessAfter = assessSixCommercialOutcomes(nextOpp, hotel);
  const stopReason =
    investigation?.orsStopReason ||
    investigation?.stopReason ||
    investigation?.disposition ||
    investigation?.investigationLoopDisposition ||
    null;
  const dosGrade = mapDosFromInvestigation(nextOpp, assessAfter, stopReason);
  const salesBrief = buildOwnershipSalesBrief(nextOpp, hotel, assessAfter, {
    stopReason,
    pathUsed,
    attempts:
      investigation?.attempts ||
      nextOpp.buyerResearchAttempts ||
      nextOpp.associationEventsInvestigationAttempts ||
      nextOpp.researchAttempts,
    nextQuestion,
    metrics: investigation?.metrics,
  });

  const updated = {
    ...nextOpp,
    hotelId: hotel.hotelId || opportunity.hotelId,
    dosGrade,
    salesBrief,
    hotelOpportunityThesis:
      salesBrief.whyThisHotel || nextOpp.hotelOpportunityThesis,
    commercialProspectTier:
      dosGrade === DOS_GRADE.DO_NOT_PURSUE
        ? "REJECTED_STALE"
        : dosGrade === DOS_GRADE.ASSIGN_NOW
          ? "PRIORITY_SALES_PROSPECT"
          : dosGrade === DOS_GRADE.RESEARCH_FIRST
            ? "RESEARCH_CONTINUATION"
            : opportunity.commercialProspectTier || "EXPLORATORY_SALES_PROSPECT",
    // Local shadow workspace: keep researched rows drawer-visible.
    // Does NOT grant production publication / hotelFacingEligible.
    customerVisible: nextOpp.customerVisible === false ? false : true,
    isTestData: false,
    localShadowPortfolio: true,
    prospectSuppressed:
      dosGrade === DOS_GRADE.DO_NOT_PURSUE
        ? true
        : nextOpp.prospectSuppressed === true && !assessAfter.outcomes.opportunity.addressed
          ? true
          : false,
    ownershipRun: AUTONOMOUS_PORTFOLIO_OWNERSHIP_V1,
    ownershipUpdatedAt: new Date().toISOString(),
    authoritativeResearchExecutor: AUTHORITATIVE_RESEARCH_EXECUTOR_V1,
    researchPathUsed: pathUsed,
    hasVerifiedNamedRelevantContact: assessAfter.routes.hasVerifiedNamedRelevantContact,
    hasCredibleOutreachRoute: assessAfter.routes.hasCredibleOutreachRoute,
    contactPersonhood: assessAfter.routes.personhood,
    evidenceConfidence: Math.min(
      95,
      35 + assessAfter.sourceCount * 8 + (assessAfter.outcomes.demand.addressed ? 10 : 0),
    ),
  };

  return {
    before: {
      id: opportunity.id,
      gaps: assessBefore.gaps,
      dosGrade: opportunity.dosGrade,
      nextQuestion,
    },
    after: {
      id: updated.id,
      gaps: assessAfter.gaps,
      dosGrade: updated.dosGrade,
      stopReason,
      paidCalls,
      pathUsed,
    },
    opportunity: updated,
    trace: {
      hotelId: hotel.hotelId,
      opportunityId: opportunity.id,
      title: opportunity.title,
      segment: opportunity.researchSegment || opportunity.segment,
      initialHypothesis: text(opportunity.hotelOpportunityThesis || opportunity.title).slice(0, 240),
      importantUnknowns: assessBefore.gaps,
      nextResearchQuestion: nextQuestion,
      strategySelected: AUTHORITATIVE_RESEARCH_EXECUTOR_V1,
      pathUsed,
      stopReason,
      paidCalls,
      finalDos: dosGrade,
      outcomesAfter: assessAfter.outcomes,
    },
  };
}

export async function runAutonomousPortfolioOwnership({
  hotelIds = [WATERSTONE_HOTEL_ID, CHAPULTEPEC_HOTEL_ID],
  candidatesPerHotel = 3,
  softSpendCapUsd = 8,
  campaignRootRel = "reports/gdi-waterstone-chapultepec-live-campaign-v1",
} = {}) {
  assertPaidExecutionAuthorized(campaignRootRel);
  const auth = loadAuthorizationStatus(campaignRootRel);
  if (auth.status !== AUTHORIZATION_STATUS.AUTHORIZED) {
    throw new Error("campaign_not_authorized");
  }
  const ledgerPath = campaignAggregateLedgerPath(campaignRootRel);
  const remaining = remainingAggregateUsd(ledgerPath);
  const spendCeiling = Math.min(Number(softSpendCapUsd), Math.max(0, remaining));
  const spentBefore = Number(
    JSON.parse(fs.readFileSync(ledgerPath, "utf8")).estimatedSpentUsd || 0,
  );

  const hotelResults = [];
  const traces = [];

  for (const hotelId of hotelIds) {
    const hotel = { ...loadHotelConfig(hotelId), hotelId };
    const doc = loadOpportunities(hotelId);
    const ops = doc.opportunities || [];
    const selected = selectOwnershipCandidates(ops, hotel, { limit: candidatesPerHotel });
    const deepened = [];

    for (const row of selected) {
      const ledgerNow = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
      const spentSoFar = Number(ledgerNow.estimatedSpentUsd || 0) - spentBefore;
      if (spentSoFar >= spendCeiling - 1e-9) {
        traces.push({
          hotelId,
          opportunityId: row.opportunity.id,
          skipped: true,
          reason: "ownership_soft_spend_cap_reached",
        });
        break;
      }
      const remainingNow = remainingAggregateUsd(ledgerPath);
      if (remainingNow <= 0) {
        traces.push({
          hotelId,
          opportunityId: row.opportunity.id,
          skipped: true,
          reason: "aggregate_budget_exhausted",
        });
        break;
      }

      const result = await deepenOpportunityOwnership({
        opportunity: row.opportunity,
        hotel,
        ledgerPath,
        allowPaid: true,
        maxPaidCalls: 4,
      });
      deepened.push(result);
      traces.push(result.trace);
    }

    // Persist updates
    const byId = new Map(ops.map((o) => [o.id, o]));
    for (const d of deepened) {
      if (d.opportunity?.id) byId.set(d.opportunity.id, d.opportunity);
    }
    const nextOps = [...byId.values()];
    saveOpportunities(hotelId, {
      ...doc,
      hotelId,
      opportunities: nextOps,
      lastOwnershipRun: AUTONOMOUS_PORTFOLIO_OWNERSHIP_V1,
      lastOwnershipAt: new Date().toISOString(),
    });
    invalidateGdiHotelReadCache(hotelId);

    hotelResults.push({
      hotelId,
      displayName: hotel.displayName,
      storeBefore: ops.length,
      storeAfter: nextOps.length,
      selected: selected.map((s) => ({
        id: s.opportunity.id,
        title: s.opportunity.title,
        score: s.score,
        reason: s.reason,
        gaps: s.assess.gaps,
      })),
      deepened: deepened.map((d) => ({
        id: d.after.id,
        dosBefore: d.before.dosGrade,
        dosAfter: d.after.dosGrade,
        gapsBefore: d.before.gaps,
        gapsAfter: d.after.gaps,
        paidCalls: d.after.paidCalls,
        stopReason: d.after.stopReason,
      })),
    });
  }

  const ledgerAfter = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  return {
    version: AUTONOMOUS_PORTFOLIO_OWNERSHIP_V1,
    authorization: auth.status,
    spentBeforeUsd: spentBefore,
    spentAfterUsd: Number(ledgerAfter.estimatedSpentUsd || 0),
    spentThisPassUsd: Number(
      (Number(ledgerAfter.estimatedSpentUsd || 0) - spentBefore).toFixed(6),
    ),
    remainingUsd: remainingAggregateUsd(ledgerPath),
    softSpendCapUsd: spendCeiling,
    hotelResults,
    traces,
  };
}
