/**
 * Multilingual + feeder-market queries per base.
 * Do not multiply every query by every language.
 */

import { GDI_BASE_OF_DEMAND } from "./taxonomy.js";

const B = GDI_BASE_OF_DEMAND;

const LANG_TERMS = {
  en: {
    hotelBlock: "hotel block OR room block OR official hotel OR accommodation",
    congress: "conference OR congress OR summit",
    housing: "housing OR lodging OR group accommodation",
    rfp: "RFP OR site selection OR host city",
    kickoff: "sales kickoff OR leadership summit OR partner conference",
    trigger: "acquisition OR merger OR office opening OR expansion",
    pharma: "investigator meeting OR advisory board OR medical affairs",
    project: "temporary accommodation OR crew hotel OR project lodging",
    sports: "team hotel OR production crew OR tournament housing",
  },
  fr: {
    hotelBlock: "hôtel officiel OR bloc chambres OR hébergement",
    congress: "conférence OR congrès OR sommet",
    housing: "hébergement OR logement groupe",
    rfp: "appel d'offres OR ville hôte OR sélection de site",
    kickoff: "kick-off commercial OR séminaire dirigeants",
    trigger: "acquisition OR fusion OR ouverture bureau",
    pharma: "réunion investigateurs OR board médical",
    project: "hébergement chantier OR hôtel équipes",
    sports: "hôtel équipe OR équipe production",
  },
  es: {
    hotelBlock:
      "hotel oficial OR bloque de habitaciones OR alojamiento OR hospedaje OR hoteles recomendados OR tarifa preferencial OR convenio hotelero OR hotel sede",
    congress:
      "congreso OR convención OR foro OR cumbre OR seminario OR jornadas OR asamblea OR simposio OR encuentro OR feria",
    housing: "alojamiento OR hospedaje grupo OR reserva hotelera OR agencia oficial de viajes",
    rfp: "licitación OR contratación OR sede OR selección de sede",
    kickoff: "reunión corporativa OR evento empresarial OR kickoff comercial OR reunión directiva OR capacitación",
    trigger: "adquisición OR fusión OR apertura oficina OR misión OR delegación",
    pharma: "reunión investigadores OR advisory board OR congreso médico OR farmacéutica",
    project: "alojamiento temporal OR hotel de obra OR infraestructura OR energía OR producción",
    sports: "hotel equipo OR producción evento OR deportivo OR bloque de habitaciones",
    exhibitors: "expositores OR patrocinadores OR secretaría técnica OR comité organizador",
  },
  gl: {
    hotelBlock:
      "hotel oficial OR bloque de habitacións OR aloxamento OR hoteis recomendados",
    congress: "congreso OR xornadas OR encontro OR asemblea OR feira OR simposio OR foro",
    housing: "aloxamento OR reserva hoteleira",
    rfp: "licitación OR contratación OR sede OR universidade",
    kickoff: "encontro empresarial OR formación OR evento empresarial",
    trigger: "expansión OR apertura OR proxecto OR infraestrutura",
    pharma: "reunión médica OR investigación",
    project: "aloxamento temporal OR enerxía OR naval OR marítimo OR infraestrutura OR proxecto",
    sports: "hotel equipo OR producción",
    exhibitors: "expositores OR patrocinadores OR secretaría técnica",
  },
  de: {
    hotelBlock:
      "offizielles Hotel OR Partnerhotel OR Tagungshotel OR Hotelkontingent OR Zimmerkontingent OR Unterkunft OR Übernachtung OR Hotelbuchung",
    congress:
      "Kongress OR Tagung OR Jahrestagung OR Konferenz OR Messe OR Fachmesse OR Symposium OR Forum OR Veranstaltung",
    housing:
      "Unterkunft OR Übernachtung OR Hotelkontingent OR Zimmerkontingent OR Partnerhotel OR Tagungshotel OR Hotelreservierung",
    rfp: "Ausschreibung OR Vergabe OR Beschaffung OR Rahmenvertrag OR Gastgeberstadt",
    kickoff:
      "Firmentagung OR Vertriebstagung OR Schulung OR Incentive OR Führungskräftetagung OR Kickoff",
    trigger: "Übernahme OR Fusion OR Büroeröffnung OR Projektteam OR Baustelle OR Infrastruktur",
    pharma: "Pharma OR Medizin OR Kongress OR Investigator Meeting OR Advisory Board OR Symposium",
    project: "Projektteam OR Baustelle OR Infrastruktur OR Projektunterkunft OR Crew Hotel",
    sports: "Mannschaftshotel OR Sport OR Produktionsteam OR Veranstaltungsteam",
    exhibitors: "Aussteller OR Sponsoren OR Teilnehmer OR Delegation OR Referenten OR PCO OR Eventagentur",
  },
};

function termsFor(lang) {
  return LANG_TERMS[lang] || LANG_TERMS.en;
}

/**
 * Build bounded query set for a hotel × base.
 * HIGH priority gets more queries; uses primary + one feeder + selective languages.
 */
export function buildBaseQueries(hotel = {}, base, opts = {}) {
  const max = opts.maxQueries ?? (opts.priority === "HIGH" ? 4 : 2);
  const langs = hotel.languages || ["en"];
  const primaryLang = langs[0] || "en";
  const place = hotel.placeNames?.[0] || hotel.destinationMarket || hotel.market || "";
  const feeders = (hotel.feederMarkets || []).slice(0, 2);
  const comps = (hotel.competitors || []).slice(0, 2);
  const queries = [];

  const push = (q, meta = {}) => {
    if (queries.length >= max) return;
    queries.push({
      baseOfDemand: base,
      query: q,
      language: meta.language || primaryLang,
      marketRole: meta.marketRole || "DESTINATION",
      originMarket: meta.originMarket || place,
      feederMarket: meta.feederMarket || null,
    });
  };

  const t = termsFor(primaryLang);
  const tEn = LANG_TERMS.en;

  switch (base) {
    case B.PUBLISHED_EVENT_DECOMPOSITION:
      push(`${place} ${t.congress} 2026 OR 2027 ${t.hotelBlock}`, { language: primaryLang });
      push(`${place} sponsors exhibitors speakers list conference 2026`, { language: "en" });
      if (feeders[0]) {
        push(`${feeders[0]} delegation ${place} congress hotel`, {
          language: "en",
          marketRole: "FEEDER",
          feederMarket: feeders[0],
          originMarket: feeders[0],
        });
      }
      if (primaryLang === "fr") {
        push(`${place} ${termsFor("fr").congress} sponsors 2026`, { language: "fr" });
      }
      if (primaryLang === "es" && t.exhibitors) {
        push(`${place} ${t.congress} ${t.exhibitors} 2026 OR 2027`, { language: "es" });
      }
      break;

    case B.INTERNATIONAL_ORG_RECURRING_GROUPS:
      push(`${place} UNECE OR WHO OR WIPO OR WTO OR ILO working group meeting 2026 hotel`, {
        language: "en",
      });
      push(`${place} national delegation meeting secretariat accommodation`, { language: primaryLang });
      if (primaryLang === "fr") {
        push(`${place} délégation nationale réunion groupe de travail hébergement`, { language: "fr" });
      }
      if (primaryLang === "es") {
        push(`${place} organismo internacional OR ONU OR ministerio OR cámara asamblea 2026 OR 2027 alojamiento`, {
          language: "es",
        });
      }
      break;

    case B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING:
      push(`${place} conference exhibitor list OR sponsor list 2026 OR 2027`, { language: "en" });
      push(`${place} ${t.hotelBlock} sponsor pavilion`, { language: primaryLang });
      if (primaryLang === "es" && t.exhibitors) {
        push(`${place} lista de expositores OR lista de patrocinadores congreso 2026 OR 2027`, {
          language: "es",
        });
      }
      break;

    case B.HISTORIC_ROTATION_PREDICTION:
      push(`${place} ${t.rfp} OR rotation OR "next host" congress 2027 OR 2028`, {
        language: primaryLang,
      });
      push(`"site selection" OR "host city" ${place} association annual meeting`, { language: "en" });
      break;

    case B.RECURRING_CORPORATE_MEETINGS:
      push(`${place} ${t.kickoff} OR "annual sales meeting" OR "partner conference" hotel`, {
        language: primaryLang,
      });
      if (feeders[0]) {
        push(`${feeders[0]} company ${tEn.kickoff} ${place}`, {
          language: "en",
          marketRole: "FEEDER",
          feederMarket: feeders[0],
          originMarket: feeders[0],
        });
      }
      break;

    case B.CORPORATE_TRIGGER_DEMAND:
      push(`${place} ${t.trigger} leadership meeting OR integration OR training`, {
        language: primaryLang,
      });
      if (feeders[0]) {
        push(`${feeders[0]} acquisition expansion ${place} office`, {
          language: "en",
          marketRole: "FEEDER",
          feederMarket: feeders[0],
        });
      }
      break;

    case B.PHARMA_MEDICAL_ECOSYSTEM:
      push(`${place} ${t.pharma} OR medical congress sponsor 2026 OR 2027`, { language: primaryLang });
      push(`${place} CRO "investigator meeting" OR "advisory board" hotel`, { language: "en" });
      break;

    case B.PROJECT_WORKFORCE_DEMAND:
      push(`${place} ${t.project} OR construction OR "data center" OR airport project hotel`, {
        language: primaryLang,
      });
      push(`${place} contractor engineer temporary accommodation`, { language: "en" });
      break;

    case B.SPORTS_ENTERTAINMENT_PRODUCTION:
      push(`${place} ${t.sports} OR tournament OR production crew hotel block`, {
        language: primaryLang,
      });
      push(`${place} federation team housing OR broadcast crew hotel`, { language: "en" });
      break;

    case B.HOTEL_HISTORY_LOOKALIKE:
      for (const c of comps.slice(0, 2)) {
        push(`"${c}" ${tEn.hotelBlock} conference OR training OR kickoff`, { language: "en" });
      }
      push(`${place} group hotel "room block" training OR incentive OR kickoff`, {
        language: primaryLang,
      });
      break;

    default:
      push(`${place} ${tEn.hotelBlock} group 2026`, { language: "en" });
  }

  // Selective secondary language (not every query)
  if (langs.includes("de") && base === B.INTERNATIONAL_ORG_RECURRING_GROUPS && queries.length < max) {
    push(`${place} Kongress Delegation Unterkunft 2026`, { language: "de" });
  }
  if (langs.includes("gl") && queries.length < max) {
    const tg = termsFor("gl");
    if (base === B.PUBLISHED_EVENT_DECOMPOSITION) {
      push(`${place} ${tg.congress} ${tg.hotelBlock} 2026 OR 2027`, { language: "gl" });
    } else if (base === B.PROJECT_WORKFORCE_DEMAND) {
      push(`${place} ${tg.project} aloxamento 2026 OR 2027`, { language: "gl" });
    } else if (base === B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING && tg.exhibitors) {
      push(`${place} ${tg.exhibitors} congreso 2026 OR 2027`, { language: "gl" });
    }
  }

  return queries.slice(0, max);
}
