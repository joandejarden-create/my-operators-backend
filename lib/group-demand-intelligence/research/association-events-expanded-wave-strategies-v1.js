/**
 * Wave-indexed discovery strategy expansions — avoid recycling the same 5–7 queries.
 */

import { RESEARCH_SEGMENT } from "../workflow/hotel-segment-research-plan-v1.js";
import {
  EVENT_DEMAND_DOMAIN,
  primarySegmentForDomain,
} from "./event-to-demand/event-to-demand-constants-v1.js";
import {
  ASSOCIATION_EVENTS_DISCOVERY_V1,
  ASSOCIATION_EVENTS_NEGATIVE_TERMS,
  buildAssociationEventsDiscoveryStrategies,
} from "./association-events-discovery-strategies-v1.js";

export const ASSOCIATION_EVENTS_EXPANDED_WAVE_STRATEGIES_V1 =
  "gdi-association-events-expanded-wave-strategies-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isCancun(hotel = {}) {
  return /canc[uú]n|riviera maya|condesa/i.test(
    `${hotel.displayName || ""} ${hotel.city || ""} ${hotel.demandTerritory?.label || ""}`,
  );
}

function isCdmx(hotel = {}) {
  return /mexico city|ciudad de m[eé]xico|cdmx|chapultepec|polanco|reforma/i.test(
    `${hotel.displayName || ""} ${hotel.city || ""} ${hotel.demandTerritory?.label || ""}`,
  );
}

function isLondon(hotel = {}) {
  return /london|marylebone|portman square|west end/i.test(
    `${hotel.displayName || ""} ${hotel.city || ""} ${hotel.demandTerritory?.label || ""}`,
  );
}

/**
 * Build strategies for a campaign wave. Wave 1 = base. Later waves add unused source families.
 */
export function buildAssociationEventsStrategiesForWave(hotel = {}, waveIndex = 1, usedIntents = []) {
  const used = new Set((usedIntents || []).map(String));
  const base = buildAssociationEventsDiscoveryStrategies(hotel);
  const neg = ASSOCIATION_EVENTS_NEGATIVE_TERMS;
  const city = text(hotel.city || "market");
  const extras = [];
  const push = (row) => {
    if (used.has(row.intent)) return;
    extras.push({
      ...row,
      version: ASSOCIATION_EVENTS_DISCOVERY_V1,
      waveStrategyVersion: ASSOCIATION_EVENTS_EXPANDED_WAVE_STRATEGIES_V1,
      researchSegment:
        row.researchSegment ||
        primarySegmentForDomain(row.domain) ||
        RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      pathway: "EVENT_LED",
      demandSeeking: true,
      track: "NEW_DISCOVERY",
    });
  };

  if (waveIndex >= 2) {
    if (isCancun(hotel)) {
      push({
        intent: "wave2_cancun_calendar_en",
        domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
        researchQuestion: `Cancún OR Cancun (2026 OR 2027) ("annual conference" OR "annual meeting" OR congress OR "asamblea") (association OR society OR institute OR federation) (venue OR lodging OR registration OR "call for") ${neg}`,
        discoveryWeight: 8,
      });
      push({
        intent: "wave2_cancun_medical_residential_es",
        domain: EVENT_DEMAND_DOMAIN.MEDICAL_SCIENTIFIC_ACADEMIC,
        researchSegment: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
        researchQuestion: `Cancún (2026 OR 2027) (congreso OR simposio OR jornadas) (médico OR cirugía OR cardiología OR oncología OR pediatría OR anestesia) (asociación OR colegio OR sociedad) (sede OR hospedaje OR inscripción) ${neg}`,
        discoveryWeight: 8,
      });
      push({
        intent: "wave2_cancun_intl_assoc_en",
        domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
        researchQuestion: `("Cancun" OR Cancún) (2026 OR 2027) ("Latin America" OR Iberoamerican OR international OR worldwide) (congress OR conference OR symposium) (hotel OR resort OR "meeting venue") ${neg}`,
        discoveryWeight: 7,
      });
      push({
        intent: "wave2_cancun_training_retreat_en",
        domain: EVENT_DEMAND_DOMAIN.CORPORATE_EVENT_MEETING,
        researchQuestion: `Cancún (2026 OR 2027) ("professional training" OR "certification course" OR "leadership retreat" OR "association retreat" OR "board retreat") (hotel OR resort OR all-inclusive) ${neg}`,
        discoveryWeight: 6,
      });
    }
    if (isCdmx(hotel)) {
      push({
        intent: "wave2_cdmx_scientific_en",
        domain: EVENT_DEMAND_DOMAIN.MEDICAL_SCIENTIFIC_ACADEMIC,
        researchSegment: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
        researchQuestion: `("Mexico City" OR CDMX OR "Ciudad de México") (2026 OR 2027) (symposium OR workshop OR congress OR "annual meeting") (society OR association OR college OR institute) (speakers OR lodging OR "meeting rooms") ${neg}`,
        discoveryWeight: 8,
      });
      push({
        intent: "wave2_cdmx_professional_es",
        domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
        researchQuestion: `("Ciudad de México" OR CDMX) (2026 OR 2027) (asamblea OR congreso OR convención OR "reunión nacional") (colegio OR cámara OR asociación OR federación) (Polanco OR Reforma OR Chapultepec OR Anzures OR Insurgentes) ${neg}`,
        discoveryWeight: 8,
      });
      push({
        intent: "wave2_cdmx_ancillary_speakers_en",
        domain: EVENT_DEMAND_DOMAIN.DESTINATION_CITYWIDE,
        researchQuestion: `("Mexico City" OR CDMX) (2026 OR 2027) (congress OR "trade show" OR exposition) ("speaker hotel" OR "overflow lodging" OR "room block" OR "preferred hotel" OR hospedaje OR convenio) ${neg}`,
        discoveryWeight: 7,
      });
      push({
        intent: "wave2_cdmx_committee_board_en",
        domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
        researchQuestion: `("Mexico City" OR CDMX) (association OR sociedad OR colegio) ("board meeting" OR "comité ejecutivo" OR "consejo directivo" OR "annual board") (2026 OR 2027) (venue OR hotel OR meeting) ${neg}`,
        discoveryWeight: 6,
      });
    }
    if (isLondon(hotel)) {
      push({
        intent: "wave2_london_assoc_congress_en",
        domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
        researchQuestion: `London (2026 OR 2027 OR 2028) (association OR institute OR society OR federation OR "royal college") ("annual conference" OR "annual meeting" OR congress OR convention) ("hotel accommodation" OR "room block" OR "delegate hotel" OR Marylebone OR "Portman Square" OR "West End") ${neg}`,
        discoveryWeight: 9,
      });
      push({
        intent: "wave2_london_professional_institute_en",
        domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
        researchQuestion: `(UK OR United Kingdom OR London) (2026 OR 2027 OR 2028) (institute OR "professional body" OR "learned society" OR "trade association") ("annual conference" OR congress OR "AGM") (London) (venue OR lodging OR "hotel block" OR accommodation) ${neg}`,
        discoveryWeight: 8,
      });
      push({
        intent: "wave2_london_medical_legal_finance_en",
        domain: EVENT_DEMAND_DOMAIN.MEDICAL_SCIENTIFIC_ACADEMIC,
        researchSegment: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
        researchQuestion: `London (2026 OR 2027 OR 2028) (medical OR legal OR financial OR academic OR scientific) (association OR society OR college OR institute) (congress OR conference OR symposium) ("hotel block" OR accommodation OR "speaker hotel" OR Marylebone) ${neg}`,
        discoveryWeight: 8,
      });
      push({
        intent: "wave2_london_overflow_competitive_en",
        domain: EVENT_DEMAND_DOMAIN.DESTINATION_CITYWIDE,
        researchQuestion: `London (2026 OR 2027 OR 2028) (ExCeL OR Olympia OR "QEII Centre" OR "ICC London" OR "conference centre") (congress OR exhibition OR conference) ("official hotel" OR "preferred hotel" OR overflow OR "room block" OR "delegate accommodation") ${neg}`,
        discoveryWeight: 7,
      });
      push({
        intent: "wave2_london_board_executive_en",
        domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
        researchQuestion: `London (2026 OR 2027 OR 2028) (association OR institute OR society) ("board meeting" OR "executive dinner" OR "council meeting" OR "trustees meeting" OR "committee meeting") (Marylebone OR "West End" OR "private dining" OR hotel OR venue) ${neg}`,
        discoveryWeight: 7,
      });
    }
  }

  if (waveIndex >= 3) {
    push({
      intent: "wave3_organizer_directory_en",
      domain: EVENT_DEMAND_DOMAIN.EVENT_INTERMEDIARY,
      researchSegment: RESEARCH_SEGMENT.PROFESSIONAL_PLANNERS_DMC,
      researchQuestion: `"${city}" (Intermeeting OR Messis OR Metaplanners OR Congrex OR MCI OR "professional congress organizer" OR PCO) (2026 OR 2027) (congreso OR conference) (hotel OR hospedaje) ${neg}`,
      discoveryWeight: 7,
    });
    push({
      intent: "wave3_future_edition_announce_en",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `"${city}" ("save the date" OR "próxima edición" OR "next edition" OR "announcing" OR "will be held" OR "se celebrará") (2026 OR 2027) (congress OR congreso OR conference) (association OR asociación) ${neg}`,
      discoveryWeight: 7,
    });
    push({
      intent: "wave3_federation_ibero_en",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `(${isCancun(hotel) ? "Cancún OR Cancun" : `"${city}"`}) (FEILAQ OR FELAC OR ALAT OR FLASOG OR "Federación" OR Iberolatinoamericana) (2026 OR 2027) (congreso OR congress) ${neg}`,
      discoveryWeight: 6,
    });
  }

  if (waveIndex >= 4) {
    push({
      intent: "wave4_industry_vertical_en",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `"${city}" (2026 OR 2027) (fintech OR accounting OR contadores OR engineering OR ingeniería OR logistics OR logística OR energy OR energía OR tourism OR turismo) (association OR cámara OR colegio) (congress OR convention OR asamblea) ${neg}`,
      discoveryWeight: 7,
    });
    push({
      intent: "wave4_univ_academic_en",
      domain: EVENT_DEMAND_DOMAIN.MEDICAL_SCIENTIFIC_ACADEMIC,
      researchSegment: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      researchQuestion: `"${city}" (2026 OR 2027) (UNAM OR IPN OR university OR universidad) (symposium OR congress OR "scientific meeting" OR colloquium) (lodging OR hotel OR venue) ${neg}`,
      discoveryWeight: 5,
    });
    push({
      intent: "wave4_calendar_sites_en",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `"${city}" (2026 OR 2027) (congreso OR conference) (site:congresos OR site:events OR "call for abstracts" OR "registro" OR inscripción) (asociación OR association OR society) ${neg}`,
      discoveryWeight: 6,
    });
    push({
      intent: "wave4_hospedaje_convenio_es",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `"${city}" (2026 OR 2027) (hospedaje OR "hotel sede" OR "tarifas preferenciales" OR convenio) (congreso OR convención) (asociación OR colegio OR sociedad) ${neg}`,
      discoveryWeight: 6,
    });
  }

  if (waveIndex >= 5) {
    push({
      intent: `wave${waveIndex}_fresh_en_${waveIndex}`,
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `"${city}" (2026 OR 2027) (congress OR congreso OR "annual meeting") (association OR asociación OR society OR colegio) ("room block" OR lodging OR hospedaje OR venue OR sede) -past -2024 ${neg}`,
      discoveryWeight: 6,
    });
    push({
      intent: `wave${waveIndex}_fresh_es_${waveIndex}`,
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `"${city}" (2026 OR 2027) (próximo congreso OR "edición 2026" OR "edición 2027" OR "se realizará") (asociación OR colegio OR federación) ${neg}`,
      discoveryWeight: 6,
    });
    push({
      intent: `wave${waveIndex}_pco_follow_en_${waveIndex}`,
      domain: EVENT_DEMAND_DOMAIN.EVENT_INTERMEDIARY,
      researchSegment: RESEARCH_SEGMENT.PROFESSIONAL_PLANNERS_DMC,
      researchQuestion: `"${city}" (2026 OR 2027) (PCO OR "agencia de congresos" OR Intermeeting OR Congrex) (association OR asociación) (hotel OR venue) ${neg}`,
      discoveryWeight: 5,
    });
  }

  let combined = [...base, ...extras].filter((s) => !used.has(s.intent));
  // Never return fewer than 3 strategies for a campaign wave — rotate unused base with wave suffix
  if (combined.length < 3) {
    for (const s of base) {
      if (combined.length >= 5) break;
      const rotated = {
        ...s,
        intent: `${s.intent}_w${waveIndex}_rot`,
        researchQuestion: `${s.researchQuestion} (2026 OR 2027)`,
        discoveryWeight: Math.max(1, (s.discoveryWeight || 3) - 1),
        waveStrategyVersion: ASSOCIATION_EVENTS_EXPANDED_WAVE_STRATEGIES_V1,
      };
      if (!used.has(rotated.intent)) combined.push(rotated);
    }
  }
  // Prefer unused high-weight first
  return combined.sort((a, b) => (b.discoveryWeight || 0) - (a.discoveryWeight || 0));
}
