/**
 * Map opportunities / SERP hits to demand engines (heuristic, deterministic).
 */

import { DEMAND_ENGINE } from "./taxonomy.js";

const RULES = [
  {
    engine: DEMAND_ENGINE.PROCUREMENT_RFP,
    re: /\b(rfp|tender|licitación|appel d['']offres|procurement|solicitation|contrato público|ausschreibung)\b/i,
  },
  {
    engine: DEMAND_ENGINE.PHARMA_LIFE_SCIENCES,
    re: /\b(pharma|biotech|medtech|investigator|advisory board|clinical|medical congress|oncology|life sciences)\b/i,
  },
  {
    engine: DEMAND_ENGINE.GOVERNMENT_INTL_ORGANIZATIONS,
    re: /\b(united nations|\bun\b|who\b|wto|ilo|ngo|diplomatic|embassy|ministry|gouvernement|gobierno|international organization)\b/i,
  },
  {
    engine: DEMAND_ENGINE.UNIVERSITY_EDUCATION,
    re: /\b(university|université|universidad|academic|faculty|executive education|colloque|symposium|campus|EPFL|UNIL|UNIGE)\b/i,
  },
  {
    engine: DEMAND_ENGINE.SPORTS_ENTERTAINMENT_SOCIAL,
    re: /\b(tournament|federation|training camp|olymp|fifa|uefa|broadcast|festival|wedding|referee|sports? team|championship)\b/i,
  },
  {
    engine: DEMAND_ENGINE.PROJECT_CREW_EXTENDED_GROUP,
    re: /\b(construction|infrastructure|commissioning|workforce|relocation|engineering crew|airport project|data center|chantier)\b/i,
  },
  {
    engine: DEMAND_ENGINE.TOUR_DMC_INCENTIVE,
    re: /\b(incentive|dmc|tour operator|group travel|pre.?post cruise|luxury tour|voyage incentive)\b/i,
  },
  {
    engine: DEMAND_ENGINE.TECH_FINANCIAL_PROFESSIONAL_SERVICES,
    re: /\b(fintech|bank|private equity|consulting|law firm|tech summit|partner conference|analyst day|saas)\b/i,
  },
  {
    engine: DEMAND_ENGINE.ASSOCIATION_NGO,
    re: /\b(association|society|congress|congrès|congreso|annual meeting|assemblée|federation|chamber|foundation)\b/i,
  },
  {
    engine: DEMAND_ENGINE.CORPORATE,
    re: /\b(corporate|offsite|kickoff|sales meeting|leadership|board meeting|product launch|M&A|integration|training program|client event)\b/i,
  },
];

/**
 * @returns {{ demandEngine, subsegment, confidence }}
 */
export function classifyDemandEngine(opp = {}) {
  const blob = [
    opp.title,
    opp.opportunityName,
    opp.organizationName,
    opp.company,
    opp.segment,
    opp.demandType,
    opp.opportunityType,
    opp.summaryWhat,
    opp.hotelOpportunityThesis,
    opp.discoveryMeta?.scoutFamily,
    opp.discoveryMeta?.canonicalIntent,
  ]
    .map((x) => String(x || ""))
    .join(" | ");

  for (const rule of RULES) {
    if (rule.re.test(blob)) {
      return {
        demandEngine: rule.engine,
        subsegment: guessSubsegment(rule.engine, blob),
        confidence: "MEDIUM",
      };
    }
  }
  return {
    demandEngine: DEMAND_ENGINE.ASSOCIATION_NGO,
    subsegment: "unknown",
    confidence: "LOW",
  };
}

function guessSubsegment(engine, blob) {
  const t = String(blob || "").toLowerCase();
  if (engine === DEMAND_ENGINE.CORPORATE) {
    if (/kickoff/.test(t)) return "sales_kickoff";
    if (/board/.test(t)) return "board_meeting";
    if (/offsite|retreat/.test(t)) return "offsite";
    if (/training/.test(t)) return "training";
    if (/launch/.test(t)) return "product_launch";
    return "leadership_meeting";
  }
  if (engine === DEMAND_ENGINE.ASSOCIATION_NGO) {
    if (/congress|congrès|congreso/.test(t)) return "congress";
    if (/annual/.test(t)) return "annual_meeting";
    if (/overflow|housing|room block/.test(t)) return "overflow";
    return "rotating_conference";
  }
  if (engine === DEMAND_ENGINE.PHARMA_LIFE_SCIENCES) {
    if (/investigator/.test(t)) return "investigator_meeting";
    if (/advisory/.test(t)) return "advisory_board";
    if (/congress/.test(t)) return "medical_congress";
    return "training";
  }
  if (engine === DEMAND_ENGINE.PROCUREMENT_RFP) {
    if (/university/.test(t)) return "university_tender";
    if (/government|public/.test(t)) return "government_tender";
    return "lodging_tender";
  }
  if (engine === DEMAND_ENGINE.TOUR_DMC_INCENTIVE) {
    if (/dmc/.test(t)) return "dmc_program";
    if (/incentive/.test(t)) return "incentive_group";
    return "luxury_tour_series";
  }
  return "general";
}
