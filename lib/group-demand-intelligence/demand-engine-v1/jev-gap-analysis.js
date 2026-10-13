/**
 * Jev gap analysis — ADVISORY ONLY.
 * Deterministic recommendations always; Jev may refine routing when available.
 * Jev never writes verified facts or promotes opportunities.
 */

import { decide } from "../jev/jev-decision-service.js";
import { JEV_DECISION_TYPE } from "../jev/jev-types.js";
import { isJevEnabled } from "../jev/jev-config.js";
import { COVERAGE_STATE } from "./taxonomy.js";
import { getGdiNextBestResearchAction } from "../discovery-expansion-v3/next-best-research.js";

const SOURCE_BY_ENGINE = {
  ASSOCIATION_NGO: "OFFICIAL_ASSOCIATION_EVENT_CALENDAR",
  CORPORATE: "CORPORATE_NEWS_AND_IR",
  PHARMA_LIFE_SCIENCES: "MEDICAL_SOCIETY_AND_CONGRESS",
  GOVERNMENT_INTL_ORGANIZATIONS: "IGO_NGO_OFFICIAL",
  UNIVERSITY_EDUCATION: "UNIVERSITY_EVENTS",
  PROCUREMENT_RFP: "PUBLIC_PROCUREMENT_PORTAL",
  SPORTS_ENTERTAINMENT_SOCIAL: "FEDERATION_TOURNAMENT",
  PROJECT_CREW_EXTENDED_GROUP: "PROJECT_ANNOUNCEMENT",
  TOUR_DMC_INCENTIVE: "DMC_INCENTIVE_CASE_STUDY",
  TECH_FINANCIAL_PROFESSIONAL_SERVICES: "INDUSTRY_CONFERENCE",
};

/**
 * Deterministic gap recommendations from coverage + funnel.
 */
export function buildDeterministicGapRecommendations(coverage = {}, opts = {}) {
  const rows = coverage.rows || [];
  const under = rows
    .filter((r) => r.underResearched)
    .sort((a, b) => a.priorityRank - b.priorityRank);

  const langProfile = opts.langProfile || {};
  const primaryLang = langProfile.primaryLanguage || "en";
  const feeder = (langProfile.feederMarkets || [])[0] || null;

  const recs = under.slice(0, opts.maxRecs ?? 5).map((r, i) => ({
    jevRecommendation: "DETERMINISTIC_GAP",
    recommendedDemandEngine: r.demandEngine,
    recommendedSourceFamily: SOURCE_BY_ENGINE[r.demandEngine] || "WEB_SERP",
    recommendedLanguage: primaryLang,
    recommendedFeederMarket: i === 0 ? feeder : null,
    recommendedResearchQuestion: `What ${r.label} demand with lodging motion exists for ${coverage.marketId || coverage.hotelKey}?`,
    rationale: `Coverage ${r.coverageState}; gaps: ${(r.remainingKnownGaps || []).join(", ") || "under-researched"}`,
    confidence: r.coverageState === COVERAGE_STATE.NOT_RESEARCHED ? 0.7 : 0.55,
    costEstimate: 0.25,
    source: "DETERMINISTIC",
    acceptedByPolicy: true,
  }));

  // Low-yield stop advisories
  for (const r of rows.filter((x) => x.coverageState === COVERAGE_STATE.SATURATED || (x.candidateCount >= 5 && x.yield === 0))) {
    if (r.candidateCount >= 5 && r.yield === 0) {
      recs.push({
        jevRecommendation: "STOP_LOW_YIELD",
        recommendedDemandEngine: r.demandEngine,
        recommendedSourceFamily: null,
        recommendedLanguage: null,
        recommendedFeederMarket: null,
        recommendedResearchQuestion: null,
        rationale: `Engine ${r.demandEngine} has ${r.candidateCount} candidates and 0 useful yield — pause or switch source`,
        confidence: 0.65,
        costEstimate: 0,
        source: "DETERMINISTIC",
        acceptedByPolicy: true,
      });
    }
  }

  // Deepen existing candidate (single next-best)
  for (const cand of opts.promisingCandidates || []) {
    const action = getGdiNextBestResearchAction(cand, {
      marketPlace: opts.marketPlace,
    });
    if (!action.query) continue;
    recs.push({
      jevRecommendation: "DEEPEN_EXISTING_CANDIDATE",
      recommendedDemandEngine: cand.demandEngine || null,
      recommendedSourceFamily: action.bestSourceFamily,
      recommendedLanguage: primaryLang,
      recommendedFeederMarket: null,
      recommendedResearchQuestion: action.exactQuestion,
      rationale: `Single-blocker near-miss: ${action.blocker}`,
      confidence: 0.6,
      costEstimate: action.estimatedCostUsd,
      source: "DETERMINISTIC",
      acceptedByPolicy: true,
      opportunityId: cand.id || cand.opportunityId,
      promotionCondition: action.promotionCondition,
    });
    break;
  }

  return recs;
}

/**
 * Optional Jev refinement — advisory only. Falls back to deterministic on any failure.
 */
export async function runJevGapAnalysis(coverage = {}, opts = {}) {
  const deterministic = buildDeterministicGapRecommendations(coverage, opts);
  const out = {
    hotelId: coverage.hotelId,
    hotelKey: coverage.hotelKey,
    recommendations: deterministic,
    jevCalled: false,
    jevAccepted: 0,
    jevRejectedByPolicy: 0,
    jevError: null,
  };

  if (opts.skipJev === true || !isJevEnabled()) {
    out.note = "Jev disabled or skipped — deterministic gap analysis only";
    return out;
  }

  const topUnder = (coverage.underResearchedEngines || []).slice(0, 3).join(",");
  try {
    const decision = await decide({
      decisionType: JEV_DECISION_TYPE.STOP_CONTINUE,
      context: {
        targetType: "DEMAND_ENGINE_COVERAGE",
        entityType: "HOTEL_MARKET",
        playbookHint: topUnder || "ASSOCIATION_NGO",
        queriesAlreadyRun: opts.queriesAlreadyRun || 0,
        costSoFarUsd: opts.costSoFarUsd || 0,
        signalsFound: opts.signalsFound || 0,
        opportunitiesCreated: opts.readyCount || 0,
        lastResult: coverage.summary || {},
        evidenceSummary: `under_researched=${topUnder}; summary=${JSON.stringify(coverage.summary || {})}`,
        decisionHint: "RESEARCH_DIRECTION_ONLY",
      },
      existingDecision: {
        choice: deterministic[0]?.jevRecommendation === "STOP_LOW_YIELD" ? "STOP" : "CONTINUE",
      },
      shadow: true,
    });

    out.jevCalled = true;
    const choice = decision?.choice || decision?.jevChoice || null;
    // Policy: only accept CONTINUE-like routing; never treat as truth
    if (choice && /STOP|PAUSE|RETIRE/i.test(String(choice))) {
      // May accept STOP for saturated engines only
      const stopRec = deterministic.find((r) => r.jevRecommendation === "STOP_LOW_YIELD");
      if (stopRec) {
        out.jevAccepted += 1;
        out.recommendations = [
          {
            ...stopRec,
            jevRecommendation: "STOP_LOW_YIELD",
            rationale: `${stopRec.rationale} | Jev choice=${choice} (advisory)`,
            source: "JEV_ADVISORY+DETERMINISTIC",
            jevChoice: choice,
            jevConfidence: decision?.confidence ?? null,
          },
          ...deterministic.filter((r) => r !== stopRec),
        ];
      } else {
        out.jevRejectedByPolicy += 1;
        out.recommendations.push({
          jevRecommendation: "JEV_STOP_REJECTED_BY_POLICY",
          rationale: `Jev suggested ${choice} but no saturated/low-yield engine — ignored for research continuation`,
          source: "JEV_ADVISORY",
          acceptedByPolicy: false,
          jevChoice: choice,
          confidence: decision?.confidence ?? null,
          costEstimate: 0,
        });
      }
    } else if (choice) {
      out.jevAccepted += 1;
      if (deterministic[0]) {
        deterministic[0] = {
          ...deterministic[0],
          source: "JEV_ADVISORY+DETERMINISTIC",
          jevChoice: choice,
          jevConfidence: decision?.confidence ?? null,
          rationale: `${deterministic[0].rationale} | Jev routing=${choice}`,
        };
        out.recommendations = deterministic;
      }
    }
  } catch (err) {
    out.jevError = String(err?.message || err).slice(0, 160);
    out.note = "Jev call failed — deterministic recommendations retained";
  }

  // Hard policy stamp on every row
  out.recommendations = (out.recommendations || []).map((r) => ({
    ...r,
    writesVerifiedFacts: false,
    promotesOpportunities: false,
    advisoryOnly: true,
  }));

  return out;
}
