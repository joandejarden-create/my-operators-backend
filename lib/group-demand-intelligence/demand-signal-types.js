/**
 * GDI New Opportunities V1 — demand signal types beyond traditional events.
 *
 * All signals feed the same canonical opportunity model.
 * NEW = NEW_TO_GDI (firstSeenRunId == current weekly run). Never NEW_TO_HOTEL
 * from public research alone.
 */

export const NEW_OPPORTUNITIES_V1 = "gdi_new_opportunities_v1";

/** Canonical demand signal types (V1). */
export const DEMAND_SIGNAL_TYPE = Object.freeze({
  EVENT: "EVENT",
  TRAINING_PROGRAM: "TRAINING_PROGRAM",
  PROJECT_TEAM: "PROJECT_TEAM",
  CORPORATE_RELOCATION: "CORPORATE_RELOCATION",
  MEDICAL_HEALTHCARE_PROGRAM: "MEDICAL_HEALTHCARE_PROGRAM",
  UNIVERSITY_ACADEMIC_PROGRAM: "UNIVERSITY_ACADEMIC_PROGRAM",
  SPORTS_ACADEMIC_COMPETITION: "SPORTS_ACADEMIC_COMPETITION",
  CONSULTING_ADVISORY_TEAM: "CONSULTING_ADVISORY_TEAM",
  CORPORATE_MEETING: "CORPORATE_MEETING",
  BOARD_COMMITTEE_MEETING: "BOARD_COMMITTEE_MEETING",
  NONPROFIT_PROGRAM: "NONPROFIT_PROGRAM",
  INSTITUTIONAL_GATHERING: "INSTITUTIONAL_GATHERING",
  GOVERNMENT_CONTRACTOR_PROGRAM: "GOVERNMENT_CONTRACTOR_PROGRAM",
  PROFESSIONAL_PROJECT_CREW: "PROFESSIONAL_PROJECT_CREW",
  INCENTIVE_RETREAT: "INCENTIVE_RETREAT",
  OVERFLOW_HOUSING: "OVERFLOW_HOUSING",
  // Private Events V1
  WEDDING: "WEDDING",
  PRIVATE_EVENT: "PRIVATE_EVENT",
  FAMILY_REUNION: "FAMILY_REUNION",
  RELIGIOUS_CELEBRATION: "RELIGIOUS_CELEBRATION",
  SOCIAL_EVENT: "SOCIAL_EVENT",
  DESTINATION_WEDDING: "DESTINATION_WEDDING",
  VENUE_PARTNERSHIP: "VENUE_PARTNERSHIP",
  EVENT_PLANNER_PARTNERSHIP: "EVENT_PLANNER_PARTNERSHIP",
  OTHER_PRIVATE_EVENT: "OTHER_PRIVATE_EVENT",
  OTHER: "OTHER",
});

/** Customer-facing short labels. */
export const DEMAND_SIGNAL_TYPE_LABEL = Object.freeze({
  EVENT: "Event / Association",
  TRAINING_PROGRAM: "Training Program",
  PROJECT_TEAM: "Project Team",
  CORPORATE_RELOCATION: "Corporate Relocation",
  MEDICAL_HEALTHCARE_PROGRAM: "Medical / Healthcare Program",
  UNIVERSITY_ACADEMIC_PROGRAM: "University / Academic Program",
  SPORTS_ACADEMIC_COMPETITION: "Sports / Academic Competition",
  CONSULTING_ADVISORY_TEAM: "Consulting / Advisory Team",
  CORPORATE_MEETING: "Corporate Meeting",
  BOARD_COMMITTEE_MEETING: "Board / Committee Meeting",
  NONPROFIT_PROGRAM: "Nonprofit Program",
  INSTITUTIONAL_GATHERING: "Institutional Gathering",
  GOVERNMENT_CONTRACTOR_PROGRAM: "Government Contractor Program",
  PROFESSIONAL_PROJECT_CREW: "Professional Project Crew",
  INCENTIVE_RETREAT: "Incentive / Retreat",
  OVERFLOW_HOUSING: "Overflow Housing",
  WEDDING: "Wedding",
  PRIVATE_EVENT: "Private Event",
  FAMILY_REUNION: "Family Reunion",
  RELIGIOUS_CELEBRATION: "Religious Celebration",
  SOCIAL_EVENT: "Social Event",
  DESTINATION_WEDDING: "Destination Wedding",
  VENUE_PARTNERSHIP: "Venue Partnership",
  EVENT_PLANNER_PARTNERSHIP: "Event Planner Partnership",
  OTHER_PRIVATE_EVENT: "Other Private Event",
  OTHER: "Other Demand Signal",
});

/** Customer-facing demand families (UI grouping). */
export const DEMAND_FAMILY = Object.freeze({
  GROUP_MEETINGS: "GROUP_MEETINGS",
  TRAINING_PROGRAMS: "TRAINING_PROGRAMS",
  PROJECT_TEAMS: "PROJECT_TEAMS",
  RELOCATION: "RELOCATION",
  PRIVATE_EVENTS: "PRIVATE_EVENTS",
  OTHER: "OTHER",
});

export const DEMAND_FAMILY_LABEL = Object.freeze({
  GROUP_MEETINGS: "Group Meetings",
  TRAINING_PROGRAMS: "Training Programs",
  PROJECT_TEAMS: "Project Teams",
  RELOCATION: "Relocation",
  PRIVATE_EVENTS: "Private Events",
  OTHER: "Other",
});

export const PRIVATE_EVENT_SIGNAL_TYPES = Object.freeze([
  DEMAND_SIGNAL_TYPE.WEDDING,
  DEMAND_SIGNAL_TYPE.PRIVATE_EVENT,
  DEMAND_SIGNAL_TYPE.FAMILY_REUNION,
  DEMAND_SIGNAL_TYPE.RELIGIOUS_CELEBRATION,
  DEMAND_SIGNAL_TYPE.SOCIAL_EVENT,
  DEMAND_SIGNAL_TYPE.DESTINATION_WEDDING,
  DEMAND_SIGNAL_TYPE.VENUE_PARTNERSHIP,
  DEMAND_SIGNAL_TYPE.EVENT_PLANNER_PARTNERSHIP,
  DEMAND_SIGNAL_TYPE.OTHER_PRIVATE_EVENT,
]);

export function demandFamilyForSignalType(type) {
  const t = String(type || "");
  if (PRIVATE_EVENT_SIGNAL_TYPES.includes(t)) return DEMAND_FAMILY.PRIVATE_EVENTS;
  if (t === DEMAND_SIGNAL_TYPE.TRAINING_PROGRAM) return DEMAND_FAMILY.TRAINING_PROGRAMS;
  if (
    t === DEMAND_SIGNAL_TYPE.PROJECT_TEAM ||
    t === DEMAND_SIGNAL_TYPE.CONSULTING_ADVISORY_TEAM ||
    t === DEMAND_SIGNAL_TYPE.PROFESSIONAL_PROJECT_CREW ||
    t === DEMAND_SIGNAL_TYPE.GOVERNMENT_CONTRACTOR_PROGRAM
  ) {
    return DEMAND_FAMILY.PROJECT_TEAMS;
  }
  if (t === DEMAND_SIGNAL_TYPE.CORPORATE_RELOCATION) return DEMAND_FAMILY.RELOCATION;
  if (
    t === DEMAND_SIGNAL_TYPE.EVENT ||
    t === DEMAND_SIGNAL_TYPE.CORPORATE_MEETING ||
    t === DEMAND_SIGNAL_TYPE.BOARD_COMMITTEE_MEETING ||
    t === DEMAND_SIGNAL_TYPE.OVERFLOW_HOUSING ||
    t === DEMAND_SIGNAL_TYPE.MEDICAL_HEALTHCARE_PROGRAM ||
    t === DEMAND_SIGNAL_TYPE.UNIVERSITY_ACADEMIC_PROGRAM ||
    t === DEMAND_SIGNAL_TYPE.SPORTS_ACADEMIC_COMPETITION ||
    t === DEMAND_SIGNAL_TYPE.NONPROFIT_PROGRAM ||
    t === DEMAND_SIGNAL_TYPE.INSTITUTIONAL_GATHERING ||
    t === DEMAND_SIGNAL_TYPE.INCENTIVE_RETREAT
  ) {
    return DEMAND_FAMILY.GROUP_MEETINGS;
  }
  return DEMAND_FAMILY.OTHER;
}

/**
 * Newness semantics — public research may only assert GDI newness.
 * NEW_TO_HOTEL / LAPSED / PAST_CUSTOMER require hotel-supplied CRM/CI.
 */
export const NEWNESS_STATUS = Object.freeze({
  NEW_TO_GDI: "NEW_TO_GDI",
  EXISTING_IN_GDI: "EXISTING_IN_GDI",
  UNKNOWN: "UNKNOWN",
});

export const RECURRENCE_CLASS = Object.freeze({
  ONE_TIME: "ONE_TIME",
  RECURRING_CONFIRMED: "RECURRING_CONFIRMED",
  RECURRING_HISTORICAL: "RECURRING_HISTORICAL",
  POSSIBLE_RECURRING: "POSSIBLE_RECURRING",
  UNKNOWN: "UNKNOWN",
});

export const BUYER_ACCESSIBILITY = Object.freeze({
  NAMED_BUYER: "NAMED_BUYER",
  FUNCTIONAL_BUYER: "FUNCTIONAL_BUYER",
  ORGANIZATION_PATH: "ORGANIZATION_PATH",
  NO_BUYER_PATH: "NO_BUYER_PATH",
});

export const TIMING_CLASS = Object.freeze({
  ACTIVE_NOW: "ACTIVE_NOW",
  NEAR_TERM: "NEAR_TERM",
  FUTURE_CONFIRMED: "FUTURE_CONFIRMED",
  FUTURE_WATCH: "FUTURE_WATCH",
  PAST: "PAST",
  UNKNOWN: "UNKNOWN",
});

export const ROOM_NIGHTS_STATUS = Object.freeze({
  CONFIRMED: "CONFIRMED",
  ESTIMATED: "ESTIMATED",
  UNKNOWN: "UNKNOWN",
});

export const LANE_YIELD_CLASS = Object.freeze({
  HIGH_YIELD: "HIGH_YIELD",
  MEDIUM_YIELD: "MEDIUM_YIELD",
  LOW_YIELD: "LOW_YIELD",
  ZERO_YIELD: "ZERO_YIELD",
});

/** Discovery lanes (query families) — map to demandSignalType. */
export const DEMAND_LANE = Object.freeze({
  EVENT_ASSOCIATION: "EVENT_ASSOCIATION",
  TRAINING: "TRAINING",
  GOVERNMENT_CONTRACTOR: "GOVERNMENT_CONTRACTOR",
  PROJECT_TEAM: "PROJECT_TEAM",
  RELOCATION: "RELOCATION",
  MEDICAL: "MEDICAL",
  UNIVERSITY: "UNIVERSITY",
  SPORTS_ACADEMIC: "SPORTS_ACADEMIC",
  CONSULTING_PROJECT: "CONSULTING_PROJECT",
  CORPORATE_MEETING: "CORPORATE_MEETING",
  BOARD_COMMITTEE: "BOARD_COMMITTEE",
  NONPROFIT: "NONPROFIT",
  INSTITUTIONAL: "INSTITUTIONAL",
  PROFESSIONAL_CREW: "PROFESSIONAL_CREW",
  INCENTIVE_RETREAT: "INCENTIVE_RETREAT",
  OVERFLOW: "OVERFLOW",
  PRIVATE_EVENTS: "PRIVATE_EVENTS",
  OTHER: "OTHER",
});

export const LANE_TO_SIGNAL = Object.freeze({
  [DEMAND_LANE.EVENT_ASSOCIATION]: DEMAND_SIGNAL_TYPE.EVENT,
  [DEMAND_LANE.TRAINING]: DEMAND_SIGNAL_TYPE.TRAINING_PROGRAM,
  [DEMAND_LANE.GOVERNMENT_CONTRACTOR]: DEMAND_SIGNAL_TYPE.GOVERNMENT_CONTRACTOR_PROGRAM,
  [DEMAND_LANE.PROJECT_TEAM]: DEMAND_SIGNAL_TYPE.PROJECT_TEAM,
  [DEMAND_LANE.RELOCATION]: DEMAND_SIGNAL_TYPE.CORPORATE_RELOCATION,
  [DEMAND_LANE.MEDICAL]: DEMAND_SIGNAL_TYPE.MEDICAL_HEALTHCARE_PROGRAM,
  [DEMAND_LANE.UNIVERSITY]: DEMAND_SIGNAL_TYPE.UNIVERSITY_ACADEMIC_PROGRAM,
  [DEMAND_LANE.SPORTS_ACADEMIC]: DEMAND_SIGNAL_TYPE.SPORTS_ACADEMIC_COMPETITION,
  [DEMAND_LANE.CONSULTING_PROJECT]: DEMAND_SIGNAL_TYPE.CONSULTING_ADVISORY_TEAM,
  [DEMAND_LANE.CORPORATE_MEETING]: DEMAND_SIGNAL_TYPE.CORPORATE_MEETING,
  [DEMAND_LANE.BOARD_COMMITTEE]: DEMAND_SIGNAL_TYPE.BOARD_COMMITTEE_MEETING,
  [DEMAND_LANE.NONPROFIT]: DEMAND_SIGNAL_TYPE.NONPROFIT_PROGRAM,
  [DEMAND_LANE.INSTITUTIONAL]: DEMAND_SIGNAL_TYPE.INSTITUTIONAL_GATHERING,
  [DEMAND_LANE.PROFESSIONAL_CREW]: DEMAND_SIGNAL_TYPE.PROFESSIONAL_PROJECT_CREW,
  [DEMAND_LANE.INCENTIVE_RETREAT]: DEMAND_SIGNAL_TYPE.INCENTIVE_RETREAT,
  [DEMAND_LANE.OVERFLOW]: DEMAND_SIGNAL_TYPE.OVERFLOW_HOUSING,
  [DEMAND_LANE.PRIVATE_EVENTS]: DEMAND_SIGNAL_TYPE.PRIVATE_EVENT,
  [DEMAND_LANE.OTHER]: DEMAND_SIGNAL_TYPE.OTHER,
});

/**
 * Archetype → ordered demand lanes (hotel-profile driven, no hotel hardcodes).
 */
export const ARCHETYPE_DEMAND_LANES = Object.freeze({
  URBAN_BUSINESS_MEETINGS: [
    DEMAND_LANE.EVENT_ASSOCIATION,
    DEMAND_LANE.CORPORATE_MEETING,
    DEMAND_LANE.TRAINING,
    DEMAND_LANE.GOVERNMENT_CONTRACTOR,
    DEMAND_LANE.PROJECT_TEAM,
    DEMAND_LANE.CONSULTING_PROJECT,
    DEMAND_LANE.RELOCATION,
    DEMAND_LANE.MEDICAL,
    DEMAND_LANE.UNIVERSITY,
    DEMAND_LANE.BOARD_COMMITTEE,
    DEMAND_LANE.NONPROFIT,
    DEMAND_LANE.SPORTS_ACADEMIC,
    DEMAND_LANE.PROFESSIONAL_CREW,
    DEMAND_LANE.INSTITUTIONAL,
    DEMAND_LANE.PRIVATE_EVENTS,
    DEMAND_LANE.OVERFLOW,
  ],
  LUXURY_DESTINATION_SMALL: [
    DEMAND_LANE.INCENTIVE_RETREAT,
    DEMAND_LANE.BOARD_COMMITTEE,
    DEMAND_LANE.CORPORATE_MEETING,
    DEMAND_LANE.CONSULTING_PROJECT,
    DEMAND_LANE.EVENT_ASSOCIATION,
    DEMAND_LANE.PRIVATE_EVENTS,
    DEMAND_LANE.NONPROFIT,
    DEMAND_LANE.TRAINING,
  ],
  RESORT_DESTINATION: [
    DEMAND_LANE.INCENTIVE_RETREAT,
    DEMAND_LANE.PRIVATE_EVENTS,
    DEMAND_LANE.EVENT_ASSOCIATION,
    DEMAND_LANE.SPORTS_ACADEMIC,
    DEMAND_LANE.CORPORATE_MEETING,
    DEMAND_LANE.BOARD_COMMITTEE,
    DEMAND_LANE.NONPROFIT,
    DEMAND_LANE.OVERFLOW,
  ],
  CORPORATE_SECONDARY: [
    DEMAND_LANE.CORPORATE_MEETING,
    DEMAND_LANE.TRAINING,
    DEMAND_LANE.EVENT_ASSOCIATION,
    DEMAND_LANE.CONSULTING_PROJECT,
    DEMAND_LANE.PROJECT_TEAM,
    DEMAND_LANE.RELOCATION,
    DEMAND_LANE.PRIVATE_EVENTS,
    DEMAND_LANE.UNIVERSITY,
    DEMAND_LANE.MEDICAL,
    DEMAND_LANE.OVERFLOW,
  ],
  MIXED: [
    DEMAND_LANE.EVENT_ASSOCIATION,
    DEMAND_LANE.CORPORATE_MEETING,
    DEMAND_LANE.TRAINING,
    DEMAND_LANE.PROJECT_TEAM,
    DEMAND_LANE.PRIVATE_EVENTS,
    DEMAND_LANE.INCENTIVE_RETREAT,
    DEMAND_LANE.MEDICAL,
    DEMAND_LANE.OVERFLOW,
  ],
});

/** English query hints per lane (locale packs may extend). */
export const LANE_QUERY_HINTS_EN = Object.freeze({
  [DEMAND_LANE.EVENT_ASSOCIATION]: [
    "upcoming annual meeting hotel housing 2026 OR 2027",
    "conference accommodations registration open",
    "association summit official hotels",
    "convention housing block RFP",
  ],
  [DEMAND_LANE.TRAINING]: [
    "multi-day training hotel accommodations 2026 OR 2027",
    "certification academy lodging registration",
    "regional training institute housing",
    "government training program hotel block",
    "professional education residential hotel",
  ],
  [DEMAND_LANE.GOVERNMENT_CONTRACTOR]: [
    "implementation team mobilization lodging onsite",
    "program deployment traveling team hotel",
    "system rollout project office temporary housing",
    "technical assistance team hotel multi-week",
  ],
  [DEMAND_LANE.RELOCATION]: [
    "company relocating headquarters employees temporary housing",
    "new regional office opening workforce hotel",
    "office move employees relocating lodging",
  ],
  [DEMAND_LANE.MEDICAL]: [
    "medical annual meeting hotel housing 2026 OR 2027",
    "clinical conference accommodations upcoming",
    "healthcare symposium official hotels",
    "hospital training program multi-day lodging",
  ],
  [DEMAND_LANE.UNIVERSITY]: [
    "executive education residential hotel 2026 OR 2027",
    "visiting faculty program lodging upcoming",
    "university conference housing block",
  ],
  [DEMAND_LANE.SPORTS_ACADEMIC]: [
    "tournament hotel housing block 2026 OR 2027",
    "championship accommodations traveling teams",
    "youth competition room block official hotels",
  ],
  [DEMAND_LANE.CONSULTING_PROJECT]: [
    "consulting engagement traveling team hotel multi-week",
    "transformation project onsite consultants lodging",
    "system implementation advisors temporary housing",
  ],
  [DEMAND_LANE.PROJECT_TEAM]: [
    "capital project team temporary housing hotel",
    "technology deployment traveling crew lodging",
    "commissioning team multi-week hotel",
  ],
  [DEMAND_LANE.CORPORATE_MEETING]: [
    "regional sales meeting hotel block 2026 OR 2027",
    "leadership offsite lodging upcoming",
    "dealer meeting room block accommodations",
  ],
  [DEMAND_LANE.BOARD_COMMITTEE]: [
    "board retreat hotel multi-day 2026 OR 2027",
    "trustee meeting accommodations offsite",
    "board of directors annual meeting hotel",
  ],
  [DEMAND_LANE.NONPROFIT]: [
    "nonprofit annual meeting hotel housing 2026 OR 2027",
    "leadership institute residential accommodations",
    "foundation symposium official hotels",
  ],
  [DEMAND_LANE.INSTITUTIONAL]: [
    "agency workshop multi-day hotel lodging",
    "institutional gathering accommodations 2026 OR 2027",
  ],
  [DEMAND_LANE.PROFESSIONAL_CREW]: [
    "commissioning team hotel temporary housing",
    "engineering field team lodging multi-week",
    "project mobilization hotel rooms",
  ],
  [DEMAND_LANE.INCENTIVE_RETREAT]: [
    "executive retreat hotel buyout 2026 OR 2027",
    "leadership retreat lodging destination",
    "incentive travel hotel group",
  ],
  [DEMAND_LANE.OVERFLOW]: [
    "overflow housing official hotels 2026 OR 2027",
    "conference housing bureau accepting hotels",
    "room block additional hotels RFP",
  ],
  [DEMAND_LANE.PRIVATE_EVENTS]: [
    "wedding venue room block hotel recommendations",
    "private event venue preferred hotel partnership",
    "destination wedding lodging guest accommodations",
    "country club wedding hotel block nearby",
    "event venue no onsite rooms lodging partners",
  ],
  [DEMAND_LANE.OTHER]: ["group lodging demand hotel block upcoming"],
});

const EVENT_MARKERS =
  /\b(?:conference|congress|symposium|forum|summit|convention|expo|exhibition|annual\s+meeting|trade\s+show|tradeshow|assembly)\b/i;

const TRAINING_MARKERS =
  /\b(?:training\s+program|certification\s+program|academy\s+program|professional\s+development|field\s+training|implementation\s+training|multi[- ]?day\s+workshop|regional\s+training)\b/i;

const RELOCATION_MARKERS =
  /\b(?:office\s+relocation|headquarters\s+move|workforce\s+relocation|facility\s+opening|temporary\s+relocation|corporate\s+expansion\s+move)\b/i;

const GOV_CONTRACT_MARKERS =
  /\b(?:contract\s+award|awarded\s+(?:contract|task\s+order)|program\s+management\s+office|contractor\s+mobilization|federal\s+contractor|implementation\s+program)\b/i;

const MEDICAL_MARKERS =
  /\b(?:clinical\s+education|medical\s+training|residency|fellowship\s+program|visiting\s+specialist|hospital\s+(?:program|training)|medical\s+association)\b/i;

const UNIVERSITY_MARKERS =
  /\b(?:executive\s+education|visiting\s+faculty|faculty\s+retreat|academic\s+conference|commencement|admissions\s+(?:tour|program)|research\s+collaboration\s+program)\b/i;

const SPORTS_MARKERS =
  /\b(?:tournament|championship|robotics\s+competition|debate\s+(?:tournament|competition)|traveling\s+teams?|youth\s+(?:soccer|basketball|hockey)|academic\s+competition)\b/i;

const CONSULTING_MARKERS =
  /\b(?:consulting\s+engagement|transformation\s+project|system\s+implementation|integration\s+program|advisory\s+team|multi[- ]?week\s+(?:project|engagement)\s+team)\b/i;

const CORP_MEETING_MARKERS =
  /\b(?:sales\s+meeting|leadership\s+meeting|regional\s+meeting|dealer\s+meeting|distributor\s+meeting|franchise\s+meeting|planning\s+session|corporate\s+offsite)\b/i;

const BOARD_MARKERS =
  /\b(?:board\s+meeting|board\s+retreat|committee\s+meeting|trustee\s+meeting|governance\s+session)\b/i;

const NONPROFIT_MARKERS =
  /\b(?:nonprofit|foundation)\b.*\b(?:annual\s+meeting|leadership\s+program|member\s+gathering|training)\b|\b(?:annual\s+meeting|leadership\s+institute)\b.*\b(?:nonprofit|foundation|association)\b/i;

const CREW_MARKERS =
  /\b(?:commissioning\s+team|project\s+mobilization|construction\s+management\s+team|technical\s+field\s+team|installation\s+crew)\b/i;

const INCENTIVE_MARKERS =
  /\b(?:incentive\s+(?:travel|program|trip)|executive\s+retreat|leadership\s+retreat|team\s+offsite|buyout)\b/i;

const PROJECT_TEAM_MARKERS =
  /\b(?:project\s+team|implementation\s+team|deployment\s+team|traveling\s+team|program\s+team)\b/i;

const DESTINATION_WEDDING_MARKERS =
  /\b(?:destination\s+wedding|out[- ]?of[- ]?town\s+guests?|travel(?:ing)?\s+guests?|guest\s+travel|airport\s+(?:shuttle|transfer|info)|hotel\s+block\s+for\s+(?:wedding|guests?))\b/i;

const PRIVATE_EVENT_MARKERS =
  /\b(?:wedding|bar\s+mitzvah|bat\s+mitzvah|quincea(?:ñ|n)era|family\s+reunion|private\s+(?:gala|event|celebration)|social\s+event|bridal)\b/i;

const VENUE_PARTNERSHIP_MARKERS =
  /\b(?:preferred\s+hotel|hotel\s+partner(?:ship)?|room[- ]?block\s+partner|venue\s+partnership|exclusive\s+hotel)\b/i;

/** Lodging-plausible language for non-event demand signals. */
export const LODGING_PLAUSIBLE_RE =
  /\b(?:room\s+block|hotel\s+(?:rooms?|block|lodging)|temporary\s+housing|multi[- ]?(?:night|week|day)\s+(?:stay|lodging|housing|team)|sleeping\s+rooms?|attendee\s+housing|tournament\s+housing|traveling\s+(?:team|staff|consultants?)|workforce\s+lodging|relocation\s+housing|peak\s+rooms?|room\s+nights?|official\s+hotels?|housing\s+required|requires?\s+lodging)\b/i;

/** Local / no-room patterns — do not treat as hotel demand. */
export const LOCAL_NO_ROOM_RE =
  /\b(?:one[- ]hour|1[- ]hour|half[- ]day\s+seminar|lunch\s+(?:and\s+)?learn|webinar|virtual\s+only|local\s+(?:day|afternoon)\s+seminar|no\s+overnight|commuter\s+only)\b/i;

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function collectBlob(opp = {}) {
  const bits = [
    opp.title,
    opp.organizationName,
    opp.demandSignalType,
    opp.segment,
    opp.summaryWhat,
    opp.whyNow,
    opp.hotelOpportunityThesis,
    opp.hotelDemandThesis,
    opp.housingEvidence,
    opp.roomDemandEvidence,
    opp.commercialThesis,
    opp.location,
    opp.destinationStatus,
  ];
  for (const e of opp.evidence || opp.evidenceSources || []) {
    if (!e) continue;
    if (typeof e === "string") bits.push(e);
    else bits.push(e.url, e.title, e.value, e.snippet, e.sourceTitle);
  }
  return bits.filter(Boolean).join(" \n ");
}

/**
 * Classify demandSignalType from explicit field or evidence text.
 */
export function classifyDemandSignalType(opp = {}) {
  const explicit = String(opp.demandSignalType || opp.signalType || "")
    .toUpperCase()
    .replace(/[\s-]+/g, "_");
  if (explicit && DEMAND_SIGNAL_TYPE[explicit]) return DEMAND_SIGNAL_TYPE[explicit];
  if (explicit && Object.values(DEMAND_SIGNAL_TYPE).includes(explicit)) return explicit;

  const title = norm(opp.title);
  const blob = `${title} ${collectBlob(opp)}`;

  if (/\boverflow\b/i.test(blob) && /\b(?:housing|hotel|room)\b/i.test(blob)) {
    return DEMAND_SIGNAL_TYPE.OVERFLOW_HOUSING;
  }
  if (DESTINATION_WEDDING_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.DESTINATION_WEDDING;
  if (VENUE_PARTNERSHIP_MARKERS.test(blob) && PRIVATE_EVENT_MARKERS.test(blob)) {
    return DEMAND_SIGNAL_TYPE.VENUE_PARTNERSHIP;
  }
  if (/\bfamily\s+reunion\b/i.test(blob)) return DEMAND_SIGNAL_TYPE.FAMILY_REUNION;
  if (/\b(?:bar|bat)\s+mitzvah|quincea(?:ñ|n)era|religious\s+celebration\b/i.test(blob)) {
    return DEMAND_SIGNAL_TYPE.RELIGIOUS_CELEBRATION;
  }
  if (PRIVATE_EVENT_MARKERS.test(blob) && !EVENT_MARKERS.test(title)) {
    if (/\bwedding\b/i.test(blob)) return DEMAND_SIGNAL_TYPE.WEDDING;
    if (/\bsocial\s+event|private\s+gala\b/i.test(blob)) return DEMAND_SIGNAL_TYPE.SOCIAL_EVENT;
    return DEMAND_SIGNAL_TYPE.PRIVATE_EVENT;
  }
  if (INCENTIVE_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.INCENTIVE_RETREAT;
  if (BOARD_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.BOARD_COMMITTEE_MEETING;
  if (RELOCATION_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.CORPORATE_RELOCATION;
  if (TRAINING_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.TRAINING_PROGRAM;
  if (GOV_CONTRACT_MARKERS.test(blob)) {
    return DEMAND_SIGNAL_TYPE.GOVERNMENT_CONTRACTOR_PROGRAM;
  }
  if (CONSULTING_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.CONSULTING_ADVISORY_TEAM;
  if (CREW_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.PROFESSIONAL_PROJECT_CREW;
  if (PROJECT_TEAM_MARKERS.test(blob) && !EVENT_MARKERS.test(title)) {
    return DEMAND_SIGNAL_TYPE.PROJECT_TEAM;
  }
  if (MEDICAL_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.MEDICAL_HEALTHCARE_PROGRAM;
  if (SPORTS_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.SPORTS_ACADEMIC_COMPETITION;
  if (UNIVERSITY_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.UNIVERSITY_ACADEMIC_PROGRAM;
  if (CORP_MEETING_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.CORPORATE_MEETING;
  if (NONPROFIT_MARKERS.test(blob)) return DEMAND_SIGNAL_TYPE.NONPROFIT_PROGRAM;
  if (EVENT_MARKERS.test(blob) || EVENT_MARKERS.test(title)) {
    return DEMAND_SIGNAL_TYPE.EVENT;
  }
  if (/\b(?:institutional|agency)\b.*\b(?:gathering|workshop|program)\b/i.test(blob)) {
    return DEMAND_SIGNAL_TYPE.INSTITUTIONAL_GATHERING;
  }
  return DEMAND_SIGNAL_TYPE.OTHER;
}

export function demandSignalTypeLabel(type) {
  const t = String(type || "").toUpperCase();
  return DEMAND_SIGNAL_TYPE_LABEL[t] || DEMAND_SIGNAL_TYPE_LABEL.OTHER;
}

/**
 * Non-EVENT lodging-plausible demand signals may enter hygiene (not auto-INVALID).
 * PLAN/REPORT without lodging stay invalid via caller.
 */
export function isLodgingPlausibleDemandSignal(opp = {}, signalType = null) {
  const type = signalType || classifyDemandSignalType(opp);
  if (type === DEMAND_SIGNAL_TYPE.EVENT || type === DEMAND_SIGNAL_TYPE.OVERFLOW_HOUSING) {
    return true;
  }
  if (type === DEMAND_SIGNAL_TYPE.OTHER) return false;

  const blob = collectBlob(opp);
  if (LOCAL_NO_ROOM_RE.test(blob) && !LODGING_PLAUSIBLE_RE.test(blob)) {
    return false;
  }
  // Explicit lodging evidence OR multi-day / traveling program markers
  if (LODGING_PLAUSIBLE_RE.test(blob)) return true;
  if (opp.hotelDemandEvidence || opp.housingEvidence || opp.roomDemandEvidence) {
    return true;
  }
  if (
    opp.eventStartDate &&
    opp.eventEndDate &&
    String(opp.eventStartDate).slice(0, 10) !== String(opp.eventEndDate).slice(0, 10)
  ) {
    return true;
  }
  // Peak rooms / room nights already estimated
  const peak = Number(opp.peakRooms ?? opp.estimatedPeakRooms ?? 0);
  if (peak > 0) return true;
  return false;
}

/**
 * Whether this signal type can bypass NON_EVENT_CONTENT INVALID when lodging-plausible.
 */
export function isExpandableDemandSignalType(type) {
  const t = String(type || "").toUpperCase();
  return (
    t &&
    t !== DEMAND_SIGNAL_TYPE.EVENT &&
    t !== DEMAND_SIGNAL_TYPE.OTHER &&
    Boolean(DEMAND_SIGNAL_TYPE[t] || Object.values(DEMAND_SIGNAL_TYPE).includes(t))
  );
}

export function classifyRecurrence(opp = {}) {
  const hint = String(opp.recurrenceClass || opp.recurrence || "").toUpperCase();
  if (hint && RECURRENCE_CLASS[hint]) return RECURRENCE_CLASS[hint];
  const blob = collectBlob(opp);
  if (/\b(?:annual|yearly|every\s+year|recurring\s+confirmed)\b/i.test(blob)) {
    if (/\b(?:confirmed|scheduled)\s+(?:for\s+)?20\d{2}\b/i.test(blob)) {
      return RECURRENCE_CLASS.RECURRING_CONFIRMED;
    }
    return RECURRENCE_CLASS.POSSIBLE_RECURRING;
  }
  if (/\b(?:historically|past\s+years?|previous\s+editions?)\b/i.test(blob)) {
    return RECURRENCE_CLASS.RECURRING_HISTORICAL;
  }
  if (/\b(?:one[- ]time|single\s+engagement|pilot\s+only)\b/i.test(blob)) {
    return RECURRENCE_CLASS.ONE_TIME;
  }
  return RECURRENCE_CLASS.UNKNOWN;
}

export function classifyBuyerAccessibility(opp = {}) {
  const hint = String(opp.buyerAccessibility || "").toUpperCase();
  if (hint && BUYER_ACCESSIBILITY[hint]) return BUYER_ACCESSIBILITY[hint];
  const contact = opp.primaryContact || {};
  if (contact.name && (contact.email || contact.phone || contact.role || contact.title)) {
    return BUYER_ACCESSIBILITY.NAMED_BUYER;
  }
  if (contact.name || opp.whoClues?.name) return BUYER_ACCESSIBILITY.NAMED_BUYER;
  if (
    opp.whoClues?.role ||
    opp.functionalBuyerRole ||
    /\b(?:program\s+manager|training\s+director|housing\s+coordinator)\b/i.test(
      collectBlob(opp)
    )
  ) {
    return BUYER_ACCESSIBILITY.FUNCTIONAL_BUYER;
  }
  if (opp.organizationName || opp.organization) {
    return BUYER_ACCESSIBILITY.ORGANIZATION_PATH;
  }
  return BUYER_ACCESSIBILITY.NO_BUYER_PATH;
}

export function classifyTiming(opp = {}, { nowDate } = {}) {
  const hint = String(opp.timingClass || "").toUpperCase();
  if (hint && TIMING_CLASS[hint]) return TIMING_CLASS[hint];
  const start = String(opp.eventStartDate || "").slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(start)) return TIMING_CLASS.UNKNOWN;
  const now = String(nowDate || new Date().toISOString()).slice(0, 10);
  const startMs = Date.parse(`${start}T00:00:00Z`);
  const nowMs = Date.parse(`${now}T00:00:00Z`);
  if (Number.isNaN(startMs) || Number.isNaN(nowMs)) return TIMING_CLASS.UNKNOWN;
  const days = Math.floor((startMs - nowMs) / 86400000);
  if (days < -1) return TIMING_CLASS.PAST;
  if (days <= 60) return TIMING_CLASS.ACTIVE_NOW;
  if (days <= 180) return TIMING_CLASS.NEAR_TERM;
  if (days <= 540) return TIMING_CLASS.FUTURE_CONFIRMED;
  return TIMING_CLASS.FUTURE_WATCH;
}

/**
 * potentialRoomNights when peakRooms + nights are known or defensibly estimated.
 * Never fabricate.
 */
export function computePotentialRoomNights(opp = {}) {
  const peakRaw = opp.peakRooms ?? opp.estimatedPeakRooms ?? opp.publishedPeakRooms;
  const peak = Number(peakRaw);
  let nights = Number(opp.nights ?? opp.lengthOfStayNights ?? 0);
  if (!nights) {
    const a = String(opp.eventStartDate || "").slice(0, 10);
    const b = String(opp.eventEndDate || "").slice(0, 10);
    if (/^\d{4}-\d{2}-\d{2}$/.test(a) && /^\d{4}-\d{2}-\d{2}$/.test(b)) {
      const days =
        Math.floor(
          (Date.parse(`${b}T00:00:00Z`) - Date.parse(`${a}T00:00:00Z`)) / 86400000
        ) + 1;
      if (days >= 2) nights = days - 1; // nights ≈ span days - 1 for lodging
      else if (days === 1) nights = 0;
    }
  }
  if (!Number.isFinite(peak) || peak <= 0 || !Number.isFinite(nights) || nights <= 0) {
    return {
      potentialRoomNights: null,
      potentialRoomNightsStatus: ROOM_NIGHTS_STATUS.UNKNOWN,
      peakRooms: Number.isFinite(peak) && peak > 0 ? peak : null,
      nights: Number.isFinite(nights) && nights > 0 ? nights : null,
    };
  }
  const statusHint = String(
    opp.peakRoomsStatus || opp.roomDemandStatus || ""
  ).toUpperCase();
  const status = /CONFIRM|VERIFIED|PUBLISHED/i.test(statusHint)
    ? ROOM_NIGHTS_STATUS.CONFIRMED
    : ROOM_NIGHTS_STATUS.ESTIMATED;
  return {
    potentialRoomNights: Math.round(peak * nights),
    potentialRoomNightsStatus: status,
    peakRooms: peak,
    nights,
  };
}

/**
 * Structured commercial factors — NOT a proprietary composite score.
 */
export function evaluateCommercialFactors(opp = {}, opts = {}) {
  const signalType = classifyDemandSignalType(opp);
  const roomNights = computePotentialRoomNights(opp);
  const recurrence = classifyRecurrence(opp);
  const buyerAccessibility = classifyBuyerAccessibility(opp);
  const timing = classifyTiming(opp, opts);
  const lodgingPlausible = isLodgingPlausibleDemandSignal(opp, signalType);

  return {
    demandSignalType: signalType,
    demandSignalTypeLabel: demandSignalTypeLabel(signalType),
    demandValue: {
      peakRooms: roomNights.peakRooms,
      nights: roomNights.nights,
      potentialRoomNights: roomNights.potentialRoomNights,
      potentialRoomNightsStatus: roomNights.potentialRoomNightsStatus,
      groupSize: opp.attendance ?? opp.estimatedAttendance ?? null,
      status:
        roomNights.potentialRoomNightsStatus === ROOM_NIGHTS_STATUS.UNKNOWN
          ? "UNKNOWN"
          : roomNights.potentialRoomNightsStatus,
    },
    recurrence,
    buyerAccessibility,
    productFit: {
      status: opp.productFitStatus || (opp.hotelFitScore != null ? "SCORED" : "UNKNOWN"),
      hotelFitScore: opp.hotelFitScore ?? null,
      notes: opp.productFitNotes || null,
    },
    geographicFit: {
      status: opp.demandTerritoryFit || opp.geoClass || "UNKNOWN",
      label: opp.demandTerritoryFitLabel || null,
    },
    timing,
    contactability: {
      status: opp.contactPathClass || buyerAccessibility,
      hasEmail: Boolean(opp.primaryContact?.email),
      hasPhone: Boolean(opp.primaryContact?.phone),
    },
    evidenceQuality: {
      sourceTier: opp.sourceTier || null,
      lodgingPlausible,
      hasOfficialSource: Boolean(opp.officialSource || (opp.sources || []).length),
    },
  };
}

/**
 * Attach demand-signal + commercial factor fields onto an opportunity record.
 */
export function enrichOpportunityDemandSignal(opp = {}, opts = {}) {
  const factors = evaluateCommercialFactors(opp, opts);
  const newnessStatus =
    opp.newnessStatus ||
    (opp.isNewThisWeek === true || opp.weeklyDeltaState === "NEW"
      ? NEWNESS_STATUS.NEW_TO_GDI
      : opp.firstSeenRunId
        ? NEWNESS_STATUS.EXISTING_IN_GDI
        : NEWNESS_STATUS.UNKNOWN);

  // Hard guard: never invent hotel CRM newness from public research.
  // Poison CRM labels → UNKNOWN (not NEW_TO_GDI) so we do not falsely claim new.
  let safeNewness = newnessStatus;
  if (
    safeNewness === "NEW_TO_HOTEL" ||
    safeNewness === "LAPSED" ||
    safeNewness === "LAPSED_ACCOUNT" ||
    safeNewness === "PAST_CUSTOMER" ||
    safeNewness === "REACTIVATION_ACCOUNT" ||
    safeNewness === "NEW_CUSTOMER"
  ) {
    safeNewness = NEWNESS_STATUS.UNKNOWN;
  }

  return {
    ...opp,
    demandSignalType: factors.demandSignalType,
    demandSignalTypeLabel: factors.demandSignalTypeLabel,
    newnessStatus: safeNewness,
    recurrenceClass: factors.recurrence,
    buyerAccessibility: factors.buyerAccessibility,
    timingClass: factors.timing,
    potentialRoomNights: factors.demandValue.potentialRoomNights,
    potentialRoomNightsStatus: factors.demandValue.potentialRoomNightsStatus,
    commercialFactors: factors,
    whyHotelDemand:
      opp.whyHotelDemand ||
      opp.hotelDemandThesis ||
      opp.hotelOpportunityThesis ||
      null,
  };
}

/**
 * Resolve newness from weekly delta — NEW_TO_GDI only.
 */
export function resolveNewnessStatus({ weeklyDeltaState, isNewThisWeek } = {}) {
  if (isNewThisWeek === true || weeklyDeltaState === "NEW") {
    return NEWNESS_STATUS.NEW_TO_GDI;
  }
  if (weeklyDeltaState) return NEWNESS_STATUS.EXISTING_IN_GDI;
  return NEWNESS_STATUS.UNKNOWN;
}

/**
 * Forbidden CRM-derived labels under public-data-only mode.
 */
export const FORBIDDEN_PUBLIC_CRM_LABELS = Object.freeze([
  "NEW_TO_HOTEL",
  "LAPSED",
  "LAPSED_ACCOUNT",
  "PAST_CUSTOMER",
  "REACTIVATION_ACCOUNT",
  "NEW_CUSTOMER",
]);

export function assertNoCrmInference(payload = {}) {
  const blob = JSON.stringify(payload);
  const hits = FORBIDDEN_PUBLIC_CRM_LABELS.filter((lab) => {
    const re = new RegExp(`"${lab}"|'${lab}'|\\b${lab}\\b`);
    // Allow mentioning the constant name in docs/guards; flag assignment values
    return (
      payload.newnessStatus === lab ||
      payload.incrementalValueStatus === lab ||
      payload.customerStatus === lab ||
      (payload.commercialFactors && payload.commercialFactors.newnessStatus === lab)
    );
  });
  return { ok: hits.length === 0, hits, scanned: Boolean(blob) };
}

export function classifyLaneYield({
  queries = 0,
  urls = 0,
  candidates = 0,
  trueActionable = 0,
  newOpportunities = 0,
} = {}) {
  if (trueActionable >= 1 || newOpportunities >= 1 || candidates >= 3) {
    return LANE_YIELD_CLASS.HIGH_YIELD;
  }
  if (candidates >= 1) return LANE_YIELD_CLASS.MEDIUM_YIELD;
  if (urls >= 4 || queries >= 6) return LANE_YIELD_CLASS.LOW_YIELD;
  return LANE_YIELD_CLASS.ZERO_YIELD;
}

/**
 * Build lane-stratified search tasks for expanded demand discovery.
 * Caps per lane to control cost.
 */
export function buildDemandLaneSearchTasks({
  geos = [],
  archetype = "MIXED",
  yearPriority = ["2026", "2027"],
  maxLanes = 10,
  maxQueriesPerLane = 4,
} = {}) {
  const lanes =
    ARCHETYPE_DEMAND_LANES[archetype] || ARCHETYPE_DEMAND_LANES.MIXED;
  const yearNear = `(${yearPriority.join(" OR ")})`;
  const geoList = (geos || []).filter(Boolean).slice(0, 4);
  const selectedLanes = lanes.slice(0, maxLanes);
  const tasks = [];

  for (const lane of selectedLanes) {
    const hints = LANE_QUERY_HINTS_EN[lane] || LANE_QUERY_HINTS_EN[DEMAND_LANE.OTHER];
    const queries = [];
    for (const geo of geoList.slice(0, 2)) {
      for (const hint of hints.slice(0, 2)) {
        queries.push(`"${geo}" ${hint} ${yearNear}`);
        if (queries.length >= maxQueriesPerLane) break;
      }
      if (queries.length >= maxQueriesPerLane) break;
    }
    tasks.push({
      lane,
      vertical: lane,
      note: lane,
      demandSignalType: LANE_TO_SIGNAL[lane] || DEMAND_SIGNAL_TYPE.OTHER,
      queries: [...new Set(queries)].slice(0, maxQueriesPerLane),
    });
  }

  return {
    version: NEW_OPPORTUNITIES_V1,
    archetype,
    tasks,
    laneCount: tasks.length,
    queryBudget: tasks.reduce((n, t) => n + t.queries.length, 0),
  };
}

/**
 * Compare small-recurring fit vs large-no-fit for qualification preference.
 * Returns which candidate is commercially preferred (not a black-box score).
 */
export function preferCommercialFit(a = {}, b = {}) {
  const score = (opp) => {
    const factors = evaluateCommercialFactors(opp);
    let s = 0;
    if (factors.evidenceQuality.lodgingPlausible) s += 3;
    if (factors.recurrence === RECURRENCE_CLASS.RECURRING_CONFIRMED) s += 3;
    if (factors.recurrence === RECURRENCE_CLASS.POSSIBLE_RECURRING) s += 2;
    if (factors.buyerAccessibility === BUYER_ACCESSIBILITY.NAMED_BUYER) s += 2;
    if (factors.buyerAccessibility === BUYER_ACCESSIBILITY.FUNCTIONAL_BUYER) s += 1;
    const fit = Number(opp.hotelFitScore ?? 0);
    if (fit >= 70) s += 3;
    else if (fit >= 50) s += 2;
    else if (fit > 0) s += 1;
    // Cap: large attendance without fit does not dominate
    const peak = Number(opp.peakRooms ?? opp.estimatedPeakRooms ?? 0);
    if (peak > 0 && peak <= 80 && factors.evidenceQuality.lodgingPlausible) s += 2;
    if (peak > 400 && fit < 40) s -= 2;
    return s;
  };
  const sa = score(a);
  const sb = score(b);
  if (sa === sb) return { preferred: null, reason: "tie", scores: { a: sa, b: sb } };
  return {
    preferred: sa > sb ? "a" : "b",
    reason: sa > sb ? "a_stronger_commercial_fit" : "b_stronger_commercial_fit",
    scores: { a: sa, b: sb },
  };
}

/**
 * Public trigger classes — reasons to investigate, never auto-opportunities.
 * Flow: TRIGGER → RESEARCH → HOTEL DEMAND THESIS → QUALIFICATION → OPPORTUNITY
 */
export const PUBLIC_TRIGGER_CLASS = Object.freeze({
  CONTRACT_AWARD: "CONTRACT_AWARD",
  PROJECT_MOBILIZATION: "PROJECT_MOBILIZATION",
  OFFICE_OPENING: "OFFICE_OPENING",
  CORPORATE_RELOCATION: "CORPORATE_RELOCATION",
  FACILITY_OPENING: "FACILITY_OPENING",
  TRAINING_ROLLOUT: "TRAINING_ROLLOUT",
  SYSTEM_DEPLOYMENT: "SYSTEM_DEPLOYMENT",
  MERGER_INTEGRATION: "MERGER_INTEGRATION",
  HIRING_EXPANSION: "HIRING_EXPANSION",
  UNIVERSITY_PROGRAM_LAUNCH: "UNIVERSITY_PROGRAM_LAUNCH",
  HOSPITAL_RESEARCH_INITIATIVE: "HOSPITAL_RESEARCH_INITIATIVE",
  TOURNAMENT_CYCLE: "TOURNAMENT_CYCLE",
  BOARD_GATHERING: "BOARD_GATHERING",
  UNKNOWN: "UNKNOWN",
});

const TRIGGER_PATTERNS = Object.freeze([
  [PUBLIC_TRIGGER_CLASS.CONTRACT_AWARD, /\b(?:contract\s+award|awarded\s+(?:contract|task\s+order)|won\s+(?:a\s+)?contract)\b/i],
  [PUBLIC_TRIGGER_CLASS.PROJECT_MOBILIZATION, /\b(?:project\s+mobilization|mobilizing\s+(?:the\s+)?(?:team|program)|kick[- ]?off\s+mobilization)\b/i],
  [PUBLIC_TRIGGER_CLASS.OFFICE_OPENING, /\b(?:opens?\s+(?:a\s+)?(?:new\s+)?(?:regional\s+)?office|office\s+opening|opens?\s+(?:its\s+)?headquarters)\b/i],
  [PUBLIC_TRIGGER_CLASS.CORPORATE_RELOCATION, /\b(?:relocating\s+(?:headquarters|HQ|workforce)|headquarters\s+move|moving\s+(?:its\s+)?HQ)\b/i],
  [PUBLIC_TRIGGER_CLASS.FACILITY_OPENING, /\b(?:facility\s+opening|opens?\s+(?:a\s+)?(?:new\s+)?(?:campus|facility|plant))\b/i],
  [PUBLIC_TRIGGER_CLASS.TRAINING_ROLLOUT, /\b(?:training\s+rollout|academy\s+launch|certification\s+program\s+launch)\b/i],
  [PUBLIC_TRIGGER_CLASS.SYSTEM_DEPLOYMENT, /\b(?:system\s+deployment|ERP\s+(?:go[- ]?live|implementation)|EHR\s+implementation)\b/i],
  [PUBLIC_TRIGGER_CLASS.MERGER_INTEGRATION, /\b(?:merger\s+integration|post[- ]?merger|acquisition\s+integration)\b/i],
  [PUBLIC_TRIGGER_CLASS.HIRING_EXPANSION, /\b(?:hiring\s+(?:spree|expansion)|workforce\s+expansion|adding\s+\d+\s+(?:jobs|employees))\b/i],
  [PUBLIC_TRIGGER_CLASS.UNIVERSITY_PROGRAM_LAUNCH, /\b(?:launches?\s+(?:a\s+)?(?:new\s+)?(?:degree|executive\s+education|academic)\s+program)\b/i],
  [PUBLIC_TRIGGER_CLASS.HOSPITAL_RESEARCH_INITIATIVE, /\b(?:research\s+initiative|clinical\s+trial\s+launch|hospital\s+program\s+launch)\b/i],
  [PUBLIC_TRIGGER_CLASS.TOURNAMENT_CYCLE, /\b(?:tournament\s+(?:cycle|announced)|championship\s+(?:dates|housing))\b/i],
  [PUBLIC_TRIGGER_CLASS.BOARD_GATHERING, /\b(?:board\s+(?:retreat|meeting)\s+(?:announced|scheduled)|trustee\s+gathering)\b/i],
]);

/**
 * Classify a public news/snippet as a research trigger (not an opportunity).
 * @returns {{ triggerClass, isOpportunity: false, requiresLodgingThesis: true, reason }}
 */
export function classifyPublicTrigger(text = "") {
  const blob = String(text || "");
  for (const [cls, re] of TRIGGER_PATTERNS) {
    if (re.test(blob)) {
      return {
        triggerClass: cls,
        isOpportunity: false,
        requiresLodgingThesis: true,
        reason: "public_trigger_requires_hotel_demand_thesis",
      };
    }
  }
  return {
    triggerClass: PUBLIC_TRIGGER_CLASS.UNKNOWN,
    isOpportunity: false,
    requiresLodgingThesis: true,
    reason: "no_recognized_public_trigger",
  };
}

/**
 * In-memory org research reuse cache (process-local).
 * Avoid re-fetching official domain / staff / program pages for same org key.
 */
export function createOrgResearchReuseCache() {
  const byOrg = new Map();
  return {
    get(orgKey) {
      const k = String(orgKey || "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();
      if (!k) return null;
      return byOrg.get(k) || null;
    },
    set(orgKey, meta = {}) {
      const k = String(orgKey || "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();
      if (!k) return null;
      const prev = byOrg.get(k) || {};
      const next = {
        organizationKey: k,
        officialDomain: meta.officialDomain || prev.officialDomain || null,
        staffPages: meta.staffPages || prev.staffPages || [],
        programPages: meta.programPages || prev.programPages || [],
        sourceFamilies: meta.sourceFamilies || prev.sourceFamilies || [],
        reusedCount: (prev.reusedCount || 0) + (meta.incrementReuse ? 1 : 0),
        updatedAt: new Date().toISOString(),
      };
      byOrg.set(k, next);
      return next;
    },
    size() {
      return byOrg.size;
    },
    stats() {
      let reusedHits = 0;
      for (const v of byOrg.values()) reusedHits += Number(v.reusedCount || 0);
      return { organizationsCached: byOrg.size, reusedHits };
    },
  };
}

/**
 * Demand-generator record (research target, not a customer lead).
 * Must produce timing + location + room thesis + fit + source before opportunity.
 */
export function buildDemandGeneratorStub({
  organizationName,
  organizationType,
  market,
  recurrenceClass = RECURRENCE_CLASS.UNKNOWN,
  programs = [],
} = {}) {
  return {
    kind: "DEMAND_GENERATOR",
    isCustomerFacing: false,
    organizationName: organizationName || null,
    organizationType: organizationType || null,
    market: market || null,
    recurrenceClass,
    programs: Array.isArray(programs) ? programs : [],
    note: "Research target only — not an opportunity until lodging demand thesis qualifies",
  };
}
