/**
 * Reusable discovery scout modules — feed one canonical opportunity universe.
 */

import { CANONICAL_INTENTS, buildLocalizedQuery } from "./localized-intents.js";

export const SCOUT_FAMILY = Object.freeze({
  ASSOCIATION: "AssociationScout",
  PROCUREMENT: "ProcurementScout",
  CORPORATE_TRIGGER: "CorporateTriggerScout",
  PROJECT_WORKFORCE: "ProjectWorkforceScout",
  MEDICAL_RESEARCH: "MedicalResearchScout",
  SPORTS_HOUSING: "SportsHousingScout",
  UNIVERSITY: "UniversityDemandScout",
  TOUR_DMC: "TourDmcScout",
  HIDDEN_DEMAND: "HiddenDemandExtractor",
});

function places(hotelCtx) {
  return hotelCtx.placeNames || hotelCtx.marketPlaceNames || [hotelCtx.market].filter(Boolean);
}

function intentsFor(scout) {
  switch (scout) {
    case SCOUT_FAMILY.ASSOCIATION:
      return [
        CANONICAL_INTENTS.ASSOCIATION_CONGRESS,
        CANONICAL_INTENTS.ANNUAL_MEETING,
        CANONICAL_INTENTS.ROOM_BLOCK,
      ];
    case SCOUT_FAMILY.PROCUREMENT:
      return [CANONICAL_INTENTS.PROCUREMENT, CANONICAL_INTENTS.LODGING_TENDER];
    case SCOUT_FAMILY.CORPORATE_TRIGGER:
      return [CANONICAL_INTENTS.CORPORATE_OFFSITE, CANONICAL_INTENTS.TRAINING_PROGRAM];
    case SCOUT_FAMILY.PROJECT_WORKFORCE:
      return [CANONICAL_INTENTS.PROJECT_WORKFORCE];
    case SCOUT_FAMILY.MEDICAL_RESEARCH:
      return [CANONICAL_INTENTS.MEDICAL_CONGRESS, CANONICAL_INTENTS.INVESTIGATOR_MEETING];
    case SCOUT_FAMILY.SPORTS_HOUSING:
      return [CANONICAL_INTENTS.SPORTS_HOUSING];
    case SCOUT_FAMILY.UNIVERSITY:
      return [CANONICAL_INTENTS.UNIVERSITY_PROGRAM, CANONICAL_INTENTS.BOARD_MEETING];
    case SCOUT_FAMILY.TOUR_DMC:
      return [CANONICAL_INTENTS.TOUR_SERIES, CANONICAL_INTENTS.INCENTIVE_GROUP];
    case SCOUT_FAMILY.HIDDEN_DEMAND:
      return [CANONICAL_INTENTS.HIDDEN_EXHIBITOR, CANONICAL_INTENTS.HIDDEN_DELEGATION];
    default:
      return [CANONICAL_INTENTS.GROUP_HOUSING];
  }
}

/** Extra scout-specific query seeds (natural language, not literal). */
function scoutSeeds(scout, hotelCtx, language) {
  const p = places(hotelCtx).slice(0, 2).join(" ");
  const seeds = {
    [SCOUT_FAMILY.PROCUREMENT]: {
      en: [`government hotel accommodation tender ${p} 2026`, `university lodging RFP ${p}`],
      fr: [`appel d'offres hébergement hôtel ${p}`, `marché public logement ${p}`],
      es: [`licitación alojamiento hotel ${p}`, `contrato alojamiento congreso ${p}`],
      gl: [`licitación aloxamento ${p}`],
    },
    [SCOUT_FAMILY.CORPORATE_TRIGGER]: {
      en: [
        `new office opening ${p} training offsite hotel`,
        `merger integration meeting ${p} hotel`,
        `facility opening ${p} corporate lodging`,
      ],
    },
    [SCOUT_FAMILY.PROJECT_WORKFORCE]: {
      en: [
        `construction project workforce lodging ${p}`,
        `airport OR rail OR energy project temporary accommodation ${p}`,
      ],
      es: [`alojamiento temporal obra infraestructura ${p}`],
      fr: [`hébergement temporaire chantier ${p}`],
    },
    [SCOUT_FAMILY.MEDICAL_RESEARCH]: {
      en: [`investigator meeting ${p} 2026 OR 2027`, `medical society congress housing ${p}`],
      fr: [`congrès médical hébergement ${p}`, `réunion investigateurs ${p}`],
      es: [`congreso médico alojamiento ${p}`],
    },
    [SCOUT_FAMILY.SPORTS_HOUSING]: {
      en: [`tournament hotel block ${p} 2026 OR 2027`, `federation training camp lodging ${p}`],
      fr: [`hébergement équipe compétition ${p}`],
      es: [`alojamiento equipo torneo ${p}`],
    },
    [SCOUT_FAMILY.UNIVERSITY]: {
      en: [`university conference accommodations ${p} 2026`, `executive education residential ${p}`],
      fr: [`colloque universitaire hébergement ${p}`],
      es: [`congreso universitario alojamiento ${p}`],
    },
    [SCOUT_FAMILY.TOUR_DMC]: {
      en: [
        `DMC incentive group hotel ${p}`,
        `luxury tour operator group booking ${p}`,
        `wedding planner group lodging ${p}`,
      ],
    },
    [SCOUT_FAMILY.HIDDEN_DEMAND]: {
      en: [
        `exhibitor housing ${p} preferred hotel`,
        `national pavilion delegation hotel ${p}`,
        `sponsor VIP program lodging ${p}`,
      ],
    },
  };
  const byLang = seeds[scout] || {};
  return byLang[language] || byLang.en || [];
}

/**
 * Build scout query plan for a hotel context.
 */
export function buildScoutQueryPlan(scoutFamily, hotelCtx = {}, opts = {}) {
  const languages = opts.languages || ["en"];
  const maxPerLang = opts.maxPerLang ?? 3;
  const plan = [];
  for (const language of languages) {
    let n = 0;
    for (const intent of intentsFor(scoutFamily)) {
      if (n >= maxPerLang) break;
      plan.push({
        scoutFamily,
        ...buildLocalizedQuery({
          intent,
          language,
          marketPlaceNames: places(hotelCtx),
          yearHints: opts.yearHints || ["2026", "2027"],
          queryFamily: "SCOUT_" + scoutFamily,
        }),
      });
      n++;
    }
    for (const q of scoutSeeds(scoutFamily, hotelCtx, language)) {
      if (n >= maxPerLang + 1) break;
      plan.push({
        scoutFamily,
        canonicalIntent: "SCOUT_SEED",
        queryLanguage: language,
        localizedQuery: q,
        market: places(hotelCtx).join(" "),
        sourceLanguage: language,
        queryFamily: "SCOUT_SEED",
        serpHl: language === "en" ? "en" : language,
        serpGl: language === "fr" ? "fr" : language === "es" || language === "gl" ? "es" : language === "de" ? "de" : "us",
      });
      n++;
    }
  }
  return plan;
}

/**
 * Hidden demand extraction from a major event candidate — only evidence-backed sub-motions.
 */
export function extractHiddenDemandCandidates(parentEvent = {}, opts = {}) {
  const max = opts.maxSub ?? 4;
  const evidenceBlob = [
    parentEvent.title,
    parentEvent.summaryWhat,
    parentEvent.hotelOpportunityThesis,
    parentEvent.pageText,
    ...(parentEvent.sources || []).map((s) => s?.url || s),
  ]
    .map((x) => String(x || ""))
    .join(" ");

  const patterns = [
    { key: "exhibitors", re: /\bexhibitor|trade\s*show|booth\b/i, intent: CANONICAL_INTENTS.HIDDEN_EXHIBITOR },
    { key: "sponsors", re: /\bsponsor/i, intent: CANONICAL_INTENTS.HIDDEN_EXHIBITOR },
    { key: "delegations", re: /\bdelegation|pavilion|national\s+team\b/i, intent: CANONICAL_INTENTS.HIDDEN_DELEGATION },
    { key: "overflow", re: /\boverflow|host\s+hotel|housing\s+bureau|room\s+block\b/i, intent: CANONICAL_INTENTS.ROOM_BLOCK },
    { key: "vip", re: /\bVIP|speaker|advisory\s+board\b/i, intent: CANONICAL_INTENTS.BOARD_MEETING },
    { key: "crew", re: /\bproduction\s+crew|broadcast|technical\s+team\b/i, intent: CANONICAL_INTENTS.PROJECT_WORKFORCE },
  ];

  const out = [];
  for (const p of patterns) {
    if (out.length >= max) break;
    if (!p.re.test(evidenceBlob)) continue;
    // Require identifiable parent org + lodging plausibility signal in same blob for overflow/housing
    const lodgingPlausible =
      p.key === "overflow" ||
      /\bhotel|housing|accommodation|lodging|room\b/i.test(evidenceBlob);
    if (!lodgingPlausible) continue;
    if (!parentEvent.organizationName && !parentEvent.organization) continue;
    out.push({
      parentOpportunityId: parentEvent.id || parentEvent.opportunityId || null,
      subDemandType: p.key,
      canonicalIntent: p.intent,
      organizationName: parentEvent.organizationName || parentEvent.organization,
      title: `${parentEvent.title || "Event"} — ${p.key} lodging motion`,
      evidenceBasis: "parent_page_signal",
      speculative: false,
    });
  }
  return out;
}

/**
 * Corporate trigger → lodging motion filter (trigger alone is not an opportunity).
 */
export function corporateTriggerImpliesLodgingMotion(text = "") {
  const t = String(text || "");
  const trigger =
    /\b(new office|facility opening|merger|acquisition|integration|product launch|regional expansion|hiring surge|sales.?force|executive summit|clinical program|capital project)\b/i.test(
      t
    );
  if (!trigger) return false;
  return /\b(training|offsite|meeting|summit|kickoff|integration meeting|dealer meeting|temporary lodging|project team)\b/i.test(
    t
  );
}

export function defaultScoutPriorityForHotel(hotelKey = "") {
  const k = String(hotelKey).toUpperCase();
  if (k === "SPICE" || k === "CAMBRIDGE") {
    return [
      SCOUT_FAMILY.TOUR_DMC,
      SCOUT_FAMILY.ASSOCIATION,
      SCOUT_FAMILY.PROCUREMENT,
      SCOUT_FAMILY.CORPORATE_TRIGGER,
      SCOUT_FAMILY.SPORTS_HOUSING,
      SCOUT_FAMILY.MEDICAL_RESEARCH,
      SCOUT_FAMILY.UNIVERSITY,
      SCOUT_FAMILY.PROJECT_WORKFORCE,
      SCOUT_FAMILY.HIDDEN_DEMAND,
    ];
  }
  if (k === "YOTEL" || k === "AC") {
    return [
      SCOUT_FAMILY.ASSOCIATION,
      SCOUT_FAMILY.PROCUREMENT,
      SCOUT_FAMILY.MEDICAL_RESEARCH,
      SCOUT_FAMILY.UNIVERSITY,
      SCOUT_FAMILY.CORPORATE_TRIGGER,
      SCOUT_FAMILY.PROJECT_WORKFORCE,
      SCOUT_FAMILY.SPORTS_HOUSING,
      SCOUT_FAMILY.HIDDEN_DEMAND,
      SCOUT_FAMILY.TOUR_DMC,
    ];
  }
  // NOW NOW / default urban
  return [
    SCOUT_FAMILY.ASSOCIATION,
    SCOUT_FAMILY.CORPORATE_TRIGGER,
    SCOUT_FAMILY.PROCUREMENT,
    SCOUT_FAMILY.UNIVERSITY,
    SCOUT_FAMILY.MEDICAL_RESEARCH,
    SCOUT_FAMILY.HIDDEN_DEMAND,
    SCOUT_FAMILY.PROJECT_WORKFORCE,
    SCOUT_FAMILY.SPORTS_HOUSING,
    SCOUT_FAMILY.TOUR_DMC,
  ];
}
