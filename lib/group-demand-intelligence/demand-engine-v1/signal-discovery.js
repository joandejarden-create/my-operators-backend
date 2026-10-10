/**
 * Continuous demand-signal discovery — signals are NOT opportunities.
 * Pipeline: SIGNAL → ORGANIZATION → DEMAND MOTION → TIMING → LODGING → FIT → WHO → OPPORTUNITY
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { DEMAND_ENGINE, defaultEnginesForHotel } from "./taxonomy.js";
import { classifyDemandEngine } from "./classify-engine.js";
import { resolveGdiMarketLanguages } from "../discovery-expansion-v3/market-languages.js";

export const SIGNAL_STAGE = Object.freeze({
  SIGNAL: "SIGNAL",
  ORGANIZATION: "ORGANIZATION",
  DEMAND_MOTION: "DEMAND_MOTION",
  FUTURE_TIMING: "FUTURE_TIMING",
  LODGING_BASIS: "LODGING_BASIS",
  HOTEL_FIT: "HOTEL_FIT",
  WHO: "WHO",
  OPPORTUNITY: "OPPORTUNITY",
});

const ENGINE_SIGNAL_QUERIES = {
  [DEMAND_ENGINE.ASSOCIATION_NGO]: {
    en: "association congress calendar OR annual meeting",
    fr: "calendrier congrès association OR assemblée annuelle",
    es: "congreso asociación calendario OR asamblea anual",
  },
  [DEMAND_ENGINE.CORPORATE]: {
    en: "corporate kickoff OR leadership offsite OR sales meeting hotel",
    fr: "séminaire entreprise OR kickoff commercial hôtel",
    es: "kickoff corporativo OR offsite dirección hotel",
  },
  [DEMAND_ENGINE.PHARMA_LIFE_SCIENCES]: {
    en: "investigator meeting OR medical congress housing",
    fr: "réunion investigateurs OR congrès médical hébergement",
    es: "reunión investigadores OR congreso médico alojamiento",
  },
  [DEMAND_ENGINE.GOVERNMENT_INTL_ORGANIZATIONS]: {
    en: "UN OR NGO conference OR international organization meeting",
    fr: "ONU OR ONG conférence OR organisation internationale",
    es: "ONU OR ONG conferencia OR organización internacional",
  },
  [DEMAND_ENGINE.UNIVERSITY_EDUCATION]: {
    en: "university conference OR executive education residential",
    fr: "colloque universitaire OR formation executive hébergement",
    es: "congreso universitario OR formación executive alojamiento",
  },
  [DEMAND_ENGINE.PROCUREMENT_RFP]: {
    en: "hotel accommodation tender OR lodging RFP",
    fr: "appel d'offres hébergement hôtel",
    es: "licitación alojamiento hotel",
  },
  [DEMAND_ENGINE.SPORTS_ENTERTAINMENT_SOCIAL]: {
    en: "tournament hotel block OR federation meeting lodging",
    fr: "compétition hébergement équipe OR fédération",
    es: "torneo alojamiento equipo OR federación",
  },
  [DEMAND_ENGINE.PROJECT_CREW_EXTENDED_GROUP]: {
    en: "project workforce temporary lodging OR construction crew hotel",
    fr: "hébergement temporaire chantier",
    es: "alojamiento temporal obra",
  },
  [DEMAND_ENGINE.TOUR_DMC_INCENTIVE]: {
    en: "incentive travel group hotel OR DMC group program",
    fr: "voyage incentive groupe hôtel",
    es: "viaje incentivo grupo hotel",
  },
  [DEMAND_ENGINE.TECH_FINANCIAL_PROFESSIONAL_SERVICES]: {
    en: "fintech summit OR consulting offsite OR partner conference",
    fr: "sommet fintech OR séminaire conseil",
    es: "cumbre fintech OR offsite consultoría",
  },
};

function hasSerp() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

/**
 * Assess transformation stage for a raw signal (deterministic).
 */
export function assessSignalStage(signal = {}) {
  const stages = [SIGNAL_STAGE.SIGNAL];
  if (signal.organizationName || signal.organization) stages.push(SIGNAL_STAGE.ORGANIZATION);
  if (signal.demandMotion || signal.demandEngine) stages.push(SIGNAL_STAGE.DEMAND_MOTION);
  if (signal.eventStartDate || signal.eventYear || signal.timingState) {
    stages.push(SIGNAL_STAGE.FUTURE_TIMING);
  }
  if (
    signal.lodgingEvidence ||
    signal.lodgingBasis ||
    /housing|room block|accommodation|hébergement|alojamiento/i.test(
      `${signal.title || ""} ${signal.snippet || ""} ${signal.url || ""}`
    )
  ) {
    stages.push(SIGNAL_STAGE.LODGING_BASIS);
  }
  if (signal.hotelFitScore != null || signal.hotelFit) stages.push(SIGNAL_STAGE.HOTEL_FIT);
  if (signal.primaryContact || signal.organizationContactUrl) stages.push(SIGNAL_STAGE.WHO);
  if (signal.promotedToOpportunityId) stages.push(SIGNAL_STAGE.OPPORTUNITY);
  return {
    highestStage: stages[stages.length - 1],
    stagesReached: stages,
    isOpportunity: stages.includes(SIGNAL_STAGE.OPPORTUNITY),
  };
}

/**
 * Discover demand signals for a hotel (bounded SERP). Does not promote.
 */
export async function discoverGdiDemandSignals(hotelCtx = {}, opts = {}) {
  const langProfile = resolveGdiMarketLanguages(hotelCtx);
  const engines = (opts.engines || defaultEnginesForHotel(hotelCtx.hotelKey)).slice(
    0,
    opts.maxEngines ?? 5
  );
  const languages = [
    langProfile.primaryLanguage,
    ...(langProfile.secondaryLanguages || []).slice(0, 1),
  ];
  const places = (hotelCtx.placeNames || []).slice(0, 2).join(" ");
  const feeder = (langProfile.feederMarkets || []).slice(0, 2);

  const budget = {
    queriesLeft: opts.maxQueries ?? 8,
    queriesRun: 0,
    costUsd: 0,
    errors: [],
  };
  const signals = [];

  if (!hasSerp()) {
    return {
      hotelId: hotelCtx.hotelId,
      hotelKey: hotelCtx.hotelKey,
      signals: [],
      queriesRun: 0,
      costUsd: 0,
      serpEnabled: false,
      note: "SERPAPI_KEY missing — signal discovery skipped",
    };
  }

  for (const engine of engines) {
    if (budget.queriesLeft <= 0) break;
    for (const language of languages) {
      if (budget.queriesLeft <= 0) break;
      const phraseMap = ENGINE_SIGNAL_QUERIES[engine] || ENGINE_SIGNAL_QUERIES[DEMAND_ENGINE.ASSOCIATION_NGO];
      const phrase = phraseMap[language] || phraseMap.en;
      const q = `${phrase} ${places} 2026 OR 2027`;
      budget.queriesLeft -= 1;
      budget.queriesRun += 1;
      budget.costUsd += 0.05;
      try {
        const serp = await serpapiSearch({
          engine: "google",
          q,
          num: 5,
          hl: language === "en" ? "en" : language,
          gl: language === "fr" ? "fr" : language === "es" || language === "gl" ? "es" : "us",
        });
        for (const hit of serp?.data?.organic_results || []) {
          const title = String(hit.title || "").trim();
          const url = String(hit.link || "").trim();
          const snippet = String(hit.snippet || "").trim();
          if (!title || !url) continue;
          if (/\b(booking\.com|expedia|tripadvisor|trivago)\b/i.test(url)) continue;
          const cls = classifyDemandEngine({ title, summaryWhat: snippet });
          const signal = {
            signalId: `sig_${hotelCtx.hotelKey}_${Buffer.from(url).toString("base64url").slice(0, 16)}`,
            hotelId: hotelCtx.hotelId,
            hotelKey: hotelCtx.hotelKey,
            demandEngine: engine,
            classifiedEngine: cls.demandEngine,
            title,
            organizationName: title.split(/[|\-—:]/)[0].trim().slice(0, 100),
            demandMotion: cls.subsegment,
            url,
            snippet,
            queryLanguage: language,
            localizedQuery: q,
            originMarket: null,
            destinationMarket: langProfile.market,
            buyerLocation: null,
            eventLocation: places,
            lodgingLocation: places,
            sourceFamily: "WEB_SERP",
            createdAt: new Date().toISOString(),
          };
          const stage = assessSignalStage(signal);
          signals.push({ ...signal, ...stage });
        }
      } catch (err) {
        budget.errors.push(String(err?.message || err).slice(0, 120));
      }
    }
  }

  // Feeder-market signals (EN)
  for (const fm of feeder) {
    if (budget.queriesLeft <= 0) break;
    const engine = engines[0];
    const phrase = ENGINE_SIGNAL_QUERIES[engine]?.en || "group lodging";
    const q = `${phrase} ${fm} ${places} 2026 OR 2027`;
    budget.queriesLeft -= 1;
    budget.queriesRun += 1;
    budget.costUsd += 0.05;
    try {
      const serp = await serpapiSearch({ engine: "google", q, num: 4, hl: "en", gl: "us" });
      for (const hit of serp?.data?.organic_results || []) {
        const title = String(hit.title || "").trim();
        const url = String(hit.link || "").trim();
        if (!title || !url) continue;
        const signal = {
          signalId: `sig_feeder_${hotelCtx.hotelKey}_${Buffer.from(url).toString("base64url").slice(0, 12)}`,
          hotelId: hotelCtx.hotelId,
          hotelKey: hotelCtx.hotelKey,
          demandEngine: engine,
          title,
          organizationName: title.split(/[|\-—:]/)[0].trim().slice(0, 100),
          url,
          snippet: String(hit.snippet || ""),
          queryLanguage: "en",
          localizedQuery: q,
          originMarket: fm,
          destinationMarket: langProfile.market,
          buyerLocation: fm,
          eventLocation: places,
          lodgingLocation: places,
          sourceFamily: "FEEDER_SERP",
          createdAt: new Date().toISOString(),
        };
        const stage = assessSignalStage(signal);
        signals.push({ ...signal, ...stage });
      }
    } catch (err) {
      budget.errors.push(String(err?.message || err).slice(0, 120));
    }
  }

  return {
    hotelId: hotelCtx.hotelId,
    hotelKey: hotelCtx.hotelKey,
    signals,
    queriesRun: budget.queriesRun,
    costUsd: budget.costUsd,
    serpEnabled: true,
    errors: budget.errors,
  };
}
