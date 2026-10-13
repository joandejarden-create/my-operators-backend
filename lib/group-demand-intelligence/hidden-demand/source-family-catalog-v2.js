/**
 * GDI Hidden Demand Source Expansion V2 — source family catalog + motion query shapes.
 * Hotel-specific query text is generated from patterns; not hardcoded opportunities.
 */

export const SOURCE_FAMILY = Object.freeze({
  EXHIBITOR_VENDOR_ROSTER: "EXHIBITOR_VENDOR_ROSTER",
  PROJECT_PROCUREMENT: "PROJECT_PROCUREMENT",
  CORPORATE_TRAINING: "CORPORATE_TRAINING",
  AGENCY_PRODUCTION: "AGENCY_PRODUCTION",
  DMC_INCENTIVE: "DMC_INCENTIVE",
  WEDDING_PLANNER: "WEDDING_PLANNER",
  UNIVERSITY_FIELD: "UNIVERSITY_FIELD",
  SPORTS_TEAM_TRAVEL: "SPORTS_TEAM_TRAVEL",
  MARINE_YACHT: "MARINE_YACHT",
  TOUR_TRAVEL_SERIES: "TOUR_TRAVEL_SERIES",
  GOVERNMENT_DELEGATION: "GOVERNMENT_DELEGATION",
  ASSOCIATION_SUBGROUP: "ASSOCIATION_SUBGROUP",
});

export const FAMILY_PRIOR_STATUS = Object.freeze({
  SATURATED: "SATURATED",
  LOW_YIELD: "LOW_YIELD",
  PROMISING: "PROMISING",
  NOT_TESTED: "NOT_TESTED",
  BLOCKED: "BLOCKED",
});

export const ADDRESSABLE_MOTION = Object.freeze({
  PROJECT_TEAM: "PROJECT_TEAM",
  TRAINING_COHORT: "TRAINING_COHORT",
  VENDOR_TEAM: "VENDOR_TEAM",
  EXHIBITOR_TEAM: "EXHIBITOR_TEAM",
  INCENTIVE_GROUP: "INCENTIVE_GROUP",
  EXECUTIVE_RETREAT: "EXECUTIVE_RETREAT",
  WEDDING_GROUP: "WEDDING_GROUP",
  PRODUCTION_CREW: "PRODUCTION_CREW",
  SPORTS_TEAM: "SPORTS_TEAM",
  TOUR_SERIES: "TOUR_SERIES",
  DELEGATION: "DELEGATION",
  UNIVERSITY_COHORT: "UNIVERSITY_COHORT",
  MARINE_GROUP: "MARINE_GROUP",
  ASSOCIATION_SUBGROUP: "ASSOCIATION_SUBGROUP",
  OTHER: "OTHER",
});

export const LODGING_EVIDENCE = Object.freeze({
  DIRECT: "DIRECT",
  STRONG_INFERENCE: "STRONG_INFERENCE",
  WEAK_INFERENCE: "WEAK_INFERENCE",
  NONE: "NONE",
});

export const TIMING_CLASS = Object.freeze({
  CONFIRMED_FUTURE: "CONFIRMED_FUTURE",
  ACTIVE_CURRENT: "ACTIVE_CURRENT",
  RECURRING_NEXT_CYCLE: "RECURRING_NEXT_CYCLE",
  FUTURE_WATCH: "FUTURE_WATCH",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
  UNKNOWN: "UNKNOWN",
});

export const PLAYBOOK_VERDICT = Object.freeze({
  PROMOTE_TO_STANDARD_PLAYBOOK: "PROMOTE_TO_STANDARD_PLAYBOOK",
  PROMISING_NEEDS_ONE_MORE_TEST: "PROMISING_NEEDS_ONE_MORE_TEST",
  LOW_YIELD: "LOW_YIELD",
  NOISY: "NOISY",
  MARKET_SPECIFIC: "MARKET_SPECIFIC",
  DO_NOT_USE: "DO_NOT_USE",
});

/** Prior inventory from AC/Spice prior cycles (registry + calendar discovery). */
export const PRIOR_FAMILY_STATUS = {
  rec2PVBDavppGpenm: {
    EVENT_CALENDAR: FAMILY_PRIOR_STATUS.SATURATED,
    ASSOCIATION_REGISTRY: FAMILY_PRIOR_STATUS.SATURATED,
    ACADEMIC_CONFERENCE: FAMILY_PRIOR_STATUS.LOW_YIELD,
    CULTURAL_FESTIVAL: FAMILY_PRIOR_STATUS.LOW_YIELD,
    SPORTS_OVERFLOW: FAMILY_PRIOR_STATUS.LOW_YIELD,
    REGISTRY_URL_HARVEST: FAMILY_PRIOR_STATUS.SATURATED,
    [SOURCE_FAMILY.EXHIBITOR_VENDOR_ROSTER]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.PROJECT_PROCUREMENT]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.CORPORATE_TRAINING]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.AGENCY_PRODUCTION]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.DMC_INCENTIVE]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.WEDDING_PLANNER]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.UNIVERSITY_FIELD]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.SPORTS_TEAM_TRAVEL]: FAMILY_PRIOR_STATUS.PROMISING,
    [SOURCE_FAMILY.MARINE_YACHT]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.TOUR_TRAVEL_SERIES]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.GOVERNMENT_DELEGATION]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.ASSOCIATION_SUBGROUP]: FAMILY_PRIOR_STATUS.PROMISING,
  },
  recKRJjcPnb4tVDDS: {
    EVENT_CALENDAR: FAMILY_PRIOR_STATUS.SATURATED,
    TOURISM_MICE: FAMILY_PRIOR_STATUS.LOW_YIELD,
    DESTINATION_WEDDING_BROCHURE: FAMILY_PRIOR_STATUS.LOW_YIELD,
    YACHT_MENTION: FAMILY_PRIOR_STATUS.LOW_YIELD,
    REGISTRY_URL_HARVEST: FAMILY_PRIOR_STATUS.SATURATED,
    [SOURCE_FAMILY.EXHIBITOR_VENDOR_ROSTER]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.PROJECT_PROCUREMENT]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.CORPORATE_TRAINING]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.AGENCY_PRODUCTION]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.DMC_INCENTIVE]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.WEDDING_PLANNER]: FAMILY_PRIOR_STATUS.PROMISING,
    [SOURCE_FAMILY.UNIVERSITY_FIELD]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.SPORTS_TEAM_TRAVEL]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.MARINE_YACHT]: FAMILY_PRIOR_STATUS.PROMISING,
    [SOURCE_FAMILY.TOUR_TRAVEL_SERIES]: FAMILY_PRIOR_STATUS.PROMISING,
    [SOURCE_FAMILY.GOVERNMENT_DELEGATION]: FAMILY_PRIOR_STATUS.NOT_TESTED,
    [SOURCE_FAMILY.ASSOCIATION_SUBGROUP]: FAMILY_PRIOR_STATUS.NOT_TESTED,
  },
};

const AC_FAMILIES = [
  SOURCE_FAMILY.PROJECT_PROCUREMENT,
  SOURCE_FAMILY.MARINE_YACHT,
  SOURCE_FAMILY.CORPORATE_TRAINING,
  SOURCE_FAMILY.UNIVERSITY_FIELD,
  SOURCE_FAMILY.SPORTS_TEAM_TRAVEL,
  SOURCE_FAMILY.AGENCY_PRODUCTION,
  SOURCE_FAMILY.ASSOCIATION_SUBGROUP,
  SOURCE_FAMILY.GOVERNMENT_DELEGATION,
  SOURCE_FAMILY.EXHIBITOR_VENDOR_ROSTER,
  SOURCE_FAMILY.DMC_INCENTIVE,
  SOURCE_FAMILY.TOUR_TRAVEL_SERIES,
  SOURCE_FAMILY.WEDDING_PLANNER,
];

const SPICE_FAMILIES = [
  SOURCE_FAMILY.DMC_INCENTIVE,
  SOURCE_FAMILY.WEDDING_PLANNER,
  SOURCE_FAMILY.MARINE_YACHT,
  SOURCE_FAMILY.TOUR_TRAVEL_SERIES,
  SOURCE_FAMILY.AGENCY_PRODUCTION,
  SOURCE_FAMILY.ASSOCIATION_SUBGROUP,
  SOURCE_FAMILY.GOVERNMENT_DELEGATION,
  SOURCE_FAMILY.CORPORATE_TRAINING,
  SOURCE_FAMILY.UNIVERSITY_FIELD,
  SOURCE_FAMILY.SPORTS_TEAM_TRAVEL,
  SOURCE_FAMILY.EXHIBITOR_VENDOR_ROSTER,
  SOURCE_FAMILY.PROJECT_PROCUREMENT,
];

function motionPatterns(hotelId, family, cfg) {
  const city = cfg?.city || "";
  const country = cfg?.country || "";
  const isAc = hotelId === "rec2PVBDavppGpenm";
  const loc = isAc ? "A Coruña Galicia" : "Grenada Caribbean";
  const y = new Date().getUTCFullYear() + 1;

  const P = {
    [SOURCE_FAMILY.EXHIBITOR_VENDOR_ROSTER]: isAc
      ? [
          `"listado expositores" OR "directorio expositores" ${loc} ${y}`,
          `"exhibitor list" OR "vendor roster" Expocoruña OR Palexco ${y}`,
          `"participantes" feria ${city} filetype:pdf`,
        ]
      : [
          `"exhibitor directory" OR "sponsor roster" Grenada conference ${y}`,
          `"trade show" vendor list Caribbean ${y}`,
        ],
    [SOURCE_FAMILY.PROJECT_PROCUREMENT]: isAc
      ? [
          `"licitación" OR "adjudicación" proyecto A Coruña ${y}`,
          `"contrato" obra infraestructura Galicia equipo desplazamiento`,
          `"project team" contractor A Coruña port ${y}`,
        ]
      : [
          `"contract award" OR "tender" Grenada infrastructure ${y}`,
          `"project team" Grenada construction`,
        ],
    [SOURCE_FAMILY.CORPORATE_TRAINING]: isAc
      ? [
          `"programa formación" empresa A Coruña ${y}`,
          `"sales meeting" OR "training academy" Galicia ${y}`,
          `"curso certificación" corporativo A Coruña alojamiento`,
        ]
      : [
          `"corporate training" OR "leadership program" Grenada ${y}`,
          `"incentive meeting" company retreat Grenada`,
        ],
    [SOURCE_FAMILY.AGENCY_PRODUCTION]: isAc
      ? [
          `"rodaje" OR "producción audiovisual" A Coruña ${y}`,
          `"shoot" OR "production company" Galicia location ${y}`,
          `"agencia" campaña cliente A Coruña equipo`,
        ]
      : [
          `"photo shoot" OR "film production" Grenada ${y}`,
          `"production crew" Grenada location`,
        ],
    [SOURCE_FAMILY.DMC_INCENTIVE]: isAc
      ? [
          `"DMC" OR "destination management" Galicia incentive ${y}`,
          `"programa incentivo" empresa Galicia`,
        ]
      : [
          `"DMC Grenada" incentive program case study`,
          `"incentive travel" Grenada group itinerary ${y}`,
          `"hosted buyer" Caribbean Grenada ${y}`,
        ],
    [SOURCE_FAMILY.WEDDING_PLANNER]: isAc
      ? [
          `"wedding planner" A Coruña portfolio ${y}`,
          `"bodas" planner Galicia grupo alojamiento`,
        ]
      : [
          `"destination wedding planner" Grenada portfolio ${y}`,
          `"wedding" planner "Grand Anse" group ${y}`,
        ],
    [SOURCE_FAMILY.UNIVERSITY_FIELD]: isAc
      ? [
          `"programa movilidad" universidad A Coruña ${y}`,
          `"field school" OR "visiting cohort" Galicia ${y}`,
          `"congreso" investigación A Coruña alojamiento grupo`,
        ]
      : [
          `"university" field program Grenada ${y}`,
          `"study tour" cohort Grenada ${y}`,
          `"medical field" program Grenada accommodation`,
        ],
    [SOURCE_FAMILY.SPORTS_TEAM_TRAVEL]: isAc
      ? [
          `"equipo" torneo desplazamiento A Coruña ${y}`,
          `"tournament" team travel Galicia accommodation`,
          `"campamento" entrenamiento A Coruña`,
        ]
      : [
          `"regatta" participants Grenada ${y}`,
          `"sports team" Grenada tournament travel`,
        ],
    [SOURCE_FAMILY.MARINE_YACHT]: isAc
      ? [
          `"autoridad portuaria" A Coruña delegación ${y}`,
          `"regata" participantes A Coruña alojamiento`,
          `"maritime" crew A Coruña`,
        ]
      : [
          `"yacht" OR "regatta" Grenada participants list ${y}`,
          `"sailing" event Grenada crew accommodation`,
          `"marina" Grenada group event ${y}`,
        ],
    [SOURCE_FAMILY.TOUR_TRAVEL_SERIES]: isAc
      ? [
          `"tour operator" salida A Coruña ${y}`,
          `"FAM trip" Galicia advisor ${y}`,
        ]
      : [
          `"escorted tour" Grenada departure ${y}`,
          `"luxury advisor" FAM Grenada ${y}`,
          `"group departure" tour operator Grenada ${y}`,
        ],
    [SOURCE_FAMILY.GOVERNMENT_DELEGATION]: isAc
      ? [
          `"misión comercial" Galicia A Coruña ${y}`,
          `"delegación" institucional A Coruña taller ${y}`,
        ]
      : [
          `"trade mission" Grenada delegation ${y}`,
          `"NGO" OR "development bank" mission Grenada ${y}`,
        ],
    [SOURCE_FAMILY.ASSOCIATION_SUBGROUP]: isAc
      ? [
          `"junta directiva" OR "comité" reunión A Coruña ${y}`,
          `"board meeting" association Galicia ${y}`,
          `"capítulo" asociación A Coruña alojamiento`,
        ]
      : [
          `"board meeting" OR "committee meeting" Grenada association ${y}`,
          `"chapter" meeting Caribbean organization Grenada`,
        ],
  };
  return P[family] || [];
}

export function familiesForHotel(hotelId) {
  return hotelId === "rec2PVBDavppGpenm" ? AC_FAMILIES : SPICE_FAMILIES;
}

export function buildMotionQueries(hotelId, family, cfg, { max = 4 } = {}) {
  const prior = PRIOR_FAMILY_STATUS[hotelId]?.[family];
  if (prior === FAMILY_PRIOR_STATUS.SATURATED || prior === FAMILY_PRIOR_STATUS.BLOCKED) {
    return [];
  }
  const patterns = motionPatterns(hotelId, family, cfg);
  return patterns.slice(0, max).map((query, i) => ({
    queryId: `${family}_${i}`,
    family,
    query,
  }));
}

export function classifyMotionFromText(text = "", family = "") {
  const t = String(text).toLowerCase();
  if (/project|obra|contractor|procurement|licitaci|adjudic/i.test(t)) {
    return ADDRESSABLE_MOTION.PROJECT_TEAM;
  }
  if (/training|formaci|certification|academy|cohort/i.test(t)) {
    return ADDRESSABLE_MOTION.TRAINING_COHORT;
  }
  if (/exhibitor|vendor|booth|stand|expositor/i.test(t)) {
    return ADDRESSABLE_MOTION.EXHIBITOR_TEAM;
  }
  if (/incentive|retreat|executive/i.test(t)) {
    return family === SOURCE_FAMILY.DMC_INCENTIVE
      ? ADDRESSABLE_MOTION.INCENTIVE_GROUP
      : ADDRESSABLE_MOTION.EXECUTIVE_RETREAT;
  }
  if (/wedding|boda|planner/i.test(t)) {
    return ADDRESSABLE_MOTION.WEDDING_GROUP;
  }
  if (/production|rodaje|shoot|film|photo/i.test(t)) {
    return ADDRESSABLE_MOTION.PRODUCTION_CREW;
  }
  if (/team|tournament|regata|camp|fixture/i.test(t)) {
    return ADDRESSABLE_MOTION.SPORTS_TEAM;
  }
  if (/tour|departure|itinerary|FAM|advisor/i.test(t)) {
    return ADDRESSABLE_MOTION.TOUR_SERIES;
  }
  if (/delegation|misión|mission|institutional/i.test(t)) {
    return ADDRESSABLE_MOTION.DELEGATION;
  }
  if (/university|universidad|field school|mobilidad/i.test(t)) {
    return ADDRESSABLE_MOTION.UNIVERSITY_COHORT;
  }
  if (/yacht|marina|maritime|sailing|crew/i.test(t)) {
    return ADDRESSABLE_MOTION.MARINE_GROUP;
  }
  if (/board|committee|junta|comité|chapter|asociaci/i.test(t)) {
    return ADDRESSABLE_MOTION.ASSOCIATION_SUBGROUP;
  }
  return ADDRESSABLE_MOTION.OTHER;
}

export function classifyLodgingEvidence(text = "") {
  const t = String(text).toLowerCase();
  if (
    /hotel block|room block|official housing|alojamiento|hospedaje|accommodation|lodging|overnight|multi.?night|pernocta/i.test(
      t
    )
  ) {
    return LODGING_EVIDENCE.DIRECT;
  }
  if (
    /traveling team|desplazamiento|delegation|crew|visiting|out.?of.?town|from abroad|international participants/i.test(
      t
    )
  ) {
    return LODGING_EVIDENCE.STRONG_INFERENCE;
  }
  if (/group|participants|attendees|equipo|cohort/i.test(t)) {
    return LODGING_EVIDENCE.WEAK_INFERENCE;
  }
  return LODGING_EVIDENCE.NONE;
}

export function classifyTiming(text = "", year = new Date().getUTCFullYear() + 1) {
  const t = String(text);
  if (/\b(201[0-9]|202[0-4])\b/.test(t) && !/\b202[6-9]\b/.test(t)) {
    return TIMING_CLASS.HISTORICAL_ONLY;
  }
  if (/\b202[6-9]\b/.test(t) || String(year) === "2027") {
    return TIMING_CLASS.CONFIRMED_FUTURE;
  }
  if (/annual|recurring|each year|every year/i.test(t)) {
    return TIMING_CLASS.RECURRING_NEXT_CYCLE;
  }
  if (/watch|tbd|to be announced/i.test(t)) {
    return TIMING_CLASS.FUTURE_WATCH;
  }
  return TIMING_CLASS.UNKNOWN;
}

export function scoreFamilyVerdict(metrics = {}) {
  const {
    fetches = 0,
    validMotions = 0,
    lodgingSupported = 0,
    fitMatches = 0,
    ready = 0,
    falsePositives = 0,
  } = metrics;
  if (fetches === 0) return PLAYBOOK_VERDICT.DO_NOT_USE;
  if (ready > 0) return PLAYBOOK_VERDICT.PROMOTE_TO_STANDARD_PLAYBOOK;
  const motionRate = validMotions / Math.max(1, fetches);
  const lodgingRate = lodgingSupported / Math.max(1, fetches);
  if (falsePositives > validMotions && fetches >= 3) return PLAYBOOK_VERDICT.NOISY;
  if (lodgingRate >= 0.15 && motionRate >= 0.2) {
    return PLAYBOOK_VERDICT.PROMISING_NEEDS_ONE_MORE_TEST;
  }
  if (motionRate >= 0.1 || fitMatches > 0) {
    return PLAYBOOK_VERDICT.MARKET_SPECIFIC;
  }
  if (fetches >= 5 && validMotions === 0) return PLAYBOOK_VERDICT.LOW_YIELD;
  return PLAYBOOK_VERDICT.LOW_YIELD;
}
