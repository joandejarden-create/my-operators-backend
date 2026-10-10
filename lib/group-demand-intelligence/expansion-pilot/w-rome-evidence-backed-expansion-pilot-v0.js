/**
 * W Rome Evidence-Backed Expansion Pilot V0
 *
 * Narrow corpus for SailGP Rome 2027 / Maker Faire Rome 2026 / Rome Film Fest 2026.
 * Evaluates SIGNAL → CANDIDATE → QUALIFIED → ACTIONABLE without lowering Ready gates.
 *
 * Room bands are MODELED. lodgingVerified / headcountVerified stay false unless
 * actual lodging / traveler-count evidence exists (none in this pilot corpus).
 */

import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { ACCOUNT_QUALITY_CLASS } from "../account-quality-taxonomy-v1.js";
import { BOOKING_WINDOW } from "../claim-types.js";
import {
  W_ROME_HOTEL_ID,
  W_ROME_EXPANSION_PILOT_ID,
  W_ROME_EXPANSION_PILOT_ENV,
  isWRomeExpansionPilotEnabled,
  isWRomePilotQualifiedCustomerVisible,
} from "./w-rome-pilot-visibility-v0.js";

export {
  W_ROME_HOTEL_ID,
  W_ROME_EXPANSION_PILOT_ID,
  W_ROME_EXPANSION_PILOT_ENV,
  isWRomeExpansionPilotEnabled,
  isWRomePilotQualifiedCustomerVisible,
};

export const GDI_MATURITY_STATE = Object.freeze({
  SIGNAL: "SIGNAL",
  CANDIDATE: "CANDIDATE",
  QUALIFIED: "QUALIFIED",
  ACTIONABLE: "ACTIONABLE",
  REJECTED: "REJECTED",
});

export const LODGING_CONTROL = Object.freeze({
  ACCOUNT_DIRECT: "ACCOUNT_DIRECT",
  ACTIVATION_AGENCY: "ACTIVATION_AGENCY",
  EVENT_ORGANIZER: "EVENT_ORGANIZER",
  TMC: "TMC",
  UNKNOWN: "UNKNOWN",
});

/** Parent demand signals for this pilot only. */
export const PILOT_DEMAND_SIGNALS = Object.freeze([
  {
    id: "wrome_signal_sailgp_rome_2027",
    label: "SailGP Rome 2027",
    eventStartDate: "2027-09-11",
    eventEndDate: "2027-09-12",
    venue: "Porto Turistico di Roma (Ostia)",
    officialSource:
      "https://sailgp.com/news/26/rolex-sailgp-championship-2027-season-rome-debut/",
  },
  {
    id: "wrome_signal_maker_faire_rome_2026",
    label: "Maker Faire Rome 2026",
    eventStartDate: "2026-10-23",
    eventEndDate: "2026-10-25",
    venue: "Gazometro Ostiense",
    officialSource: "https://makerfairerome.eu/en/2026/",
  },
  {
    id: "wrome_signal_rome_film_fest_2026",
    label: "Rome Film Fest 2026",
    eventStartDate: "2026-10-13",
    eventEndDate: "2026-10-25",
    venue: "Auditorium Parco della Musica",
    officialSource: "https://www.romacinemafest.it/en/banca-ifis-main-partner/",
  },
]);

/**
 * Evaluation targets researched for the pilot.
 * Only rows with evidenceStatus ACCEPTED become opportunity candidates.
 */
export const PILOT_ACCOUNT_RESEARCH = Object.freeze([
  // —— SailGP ——
  {
    accountName: "Red Bull",
    parentDemandSignalId: "wrome_signal_sailgp_rome_2027",
    accountRole: "TEAM_PARTNER",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        type: "official_league_news",
        url: "https://sailgp.com/news/26/rolex-sailgp-championship-2027-season-rome-debut/",
        note: "SailGP announces Rome debut 11–12 Sep 2027",
      },
      {
        type: "official_team_announcement",
        url: "https://www.linkedin.com/posts/italysailgp_s7-rome-italy-sail-grand-prix-announcement-activity-7464620030270271489-h95Z",
        note: "Red Bull Italy SailGP Team announces Rome GP with Red Bull / Azimut partners",
      },
    ],
    travelingCohortType: "TEAM_AND_SPONSOR_HOSPITALITY",
    travelingCohortSummary:
      "Italy SailGP race team + Red Bull brand hospitality / VIP guest program for the Rome GP weekend",
    travelingCohortConfidence: "MEDIUM",
    lodgingControlHypothesis: LODGING_CONTROL.ACCOUNT_DIRECT,
    lodgingControlSummary:
      "Team / brand hospitality typically books or directs preferred hotels for race week; buyer desk not yet identified",
    lodgingControlConfidence: "LOW",
    modeledRoomsMin: 10,
    modeledRoomsMax: 25,
    modeledNightsMin: 3,
    modeledNightsMax: 5,
    modeledDemandBasis:
      "Lifestyle / sports VIP team band for compact SailGP race weekend — not a published room block",
    modeledAncillary: ["VIP reception", "private dining", "suite preference"],
    hotelFitScore: 82,
    buyerEntity: "Red Bull Italy SailGP Team — hospitality / partner services",
    buyerRole: "Team hospitality / Partner services",
    publicContactPath: "https://sailgp.com/teams/italy/",
  },
  {
    accountName: "Azimut",
    parentDemandSignalId: "wrome_signal_sailgp_rome_2027",
    accountRole: "SPONSOR",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        type: "official_press_release",
        url: "https://pressmare.it/en/team/red-bull-ita-sailgp-team/2026-01-15/red-bull-italy-sailgp-welcomes-azimut-as-global-partner-87770",
        note: "Azimut joins Red Bull Italy SailGP as Global Partner; logo on Italian F50",
      },
      {
        type: "official_team_announcement",
        url: "https://www.linkedin.com/posts/italysailgp_s7-rome-italy-sail-grand-prix-announcement-activity-7464620030270271489-h95Z",
        note: "Rome GP announcement lists Azimut Italia among partners",
      },
    ],
    travelingCohortType: "SPONSOR_ACTIVATION_AND_CLIENT_HOSPITALITY",
    travelingCohortSummary:
      "Azimut sponsor activation / client hospitality guests supporting the Italian SailGP team at Rome GP",
    travelingCohortConfidence: "MEDIUM",
    lodgingControlHypothesis: LODGING_CONTROL.UNKNOWN,
    lodgingControlSummary:
      "Global partner may use team hospitality, an activation agency, or own TMC — lodging controller not evidenced",
    lodgingControlConfidence: "LOW",
    modeledRoomsMin: 6,
    modeledRoomsMax: 18,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis:
      "Modeled sponsor hospitality band for a SailGP race weekend — lodging not verified",
    modeledAncillary: ["client entertainment", "brand activation staging"],
    hotelFitScore: 78,
    buyerEntity: "Azimut — sponsorship / hospitality liaison",
    buyerRole: "Sponsorship / Client hospitality",
    publicContactPath:
      "https://pressmare.it/en/team/red-bull-ita-sailgp-team/2026-01-15/red-bull-italy-sailgp-welcomes-azimut-as-global-partner-87770",
  },
  {
    accountName: "Oracle",
    parentDemandSignalId: "wrome_signal_sailgp_rome_2027",
    accountRole: "SPONSOR",
    evidenceStatus: "REJECTED",
    rejectReason:
      "Oracle titles Perth SailGP (Oracle Perth Sail Grand Prix); no official Rome 2027 sponsor/partner page found",
    evidenceItems: [],
  },
  {
    accountName: "Emirates",
    parentDemandSignalId: "wrome_signal_sailgp_rome_2027",
    accountRole: "SPONSOR",
    evidenceStatus: "REJECTED",
    rejectReason:
      "Emirates is league airline partner / GBR team brand / Dubai GP presenter — no Rome 2027 event-specific sponsor evidence",
    evidenceItems: [],
  },
  {
    accountName: "KPMG",
    parentDemandSignalId: "wrome_signal_sailgp_rome_2027",
    accountRole: "SPONSOR",
    evidenceStatus: "REJECTED",
    rejectReason:
      "KPMG evidenced on Asia-Pacific SailGP events (Perth/Sydney/Auckland) — not Rome 2027",
    evidenceItems: [],
  },
  {
    accountName: "DP World",
    parentDemandSignalId: "wrome_signal_sailgp_rome_2027",
    accountRole: "SPONSOR",
    evidenceStatus: "REJECTED",
    rejectReason:
      "DP World presents Emirates Dubai Sail Grand Prix — no Rome 2027 event-specific evidence",
    evidenceItems: [],
  },

  // —— Maker Faire ——
  {
    accountName: "ABB S.p.A.",
    parentDemandSignalId: "wrome_signal_maker_faire_rome_2026",
    accountRole: "PARTNER",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        type: "official_exhibitor_directory",
        url: "https://makerfairerome.eu/en/exhibitors/?order=asc",
        note: "Official MFR2026 exhibitor directory lists Partner — ABB S.p.A.",
      },
    ],
    travelingCohortType: "EXHIBITOR_PARTNER_TEAM",
    travelingCohortSummary:
      "ABB partner / demo staff and visiting technical specialists supporting Maker Faire Rome booth activation",
    travelingCohortConfidence: "MEDIUM",
    lodgingControlHypothesis: LODGING_CONTROL.ACCOUNT_DIRECT,
    lodgingControlSummary:
      "Corporate partner booths usually book own travel or via corporate TMC; no published hotel program",
    lodgingControlConfidence: "LOW",
    modeledRoomsMin: 5,
    modeledRoomsMax: 15,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis:
      "Modeled partner booth team for a 3-day innovation fair — lodging not verified",
    modeledAncillary: ["small meeting / demo prep"],
    hotelFitScore: 72,
    buyerEntity: "ABB Italy — events / partner marketing",
    buyerRole: "Events / Partner marketing",
    publicContactPath: "https://makerfairerome.eu/en/exhibitors/?order=asc",
    // Entity-truth: bare "ABB" fails no_addressable_entity_signals; use directory legal name
    organizationNameCanonical: "ABB S.p.A.",
  },
  {
    accountName: "Anycubic",
    parentDemandSignalId: "wrome_signal_maker_faire_rome_2026",
    accountRole: "PARTNER",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        type: "official_exhibitor_directory",
        url: "https://makerfairerome.eu/en/exhibitors/?order=asc",
        note: "Official MFR2026 exhibitor directory lists Partner — Anycubic",
      },
    ],
    travelingCohortType: "EXHIBITOR_PARTNER_TEAM",
    travelingCohortSummary:
      "Anycubic international partner / product demo team traveling for Maker Faire Rome activation",
    travelingCohortConfidence: "MEDIUM",
    lodgingControlHypothesis: LODGING_CONTROL.UNKNOWN,
    lodgingControlSummary:
      "International 3D-print brand partner — lodging likely agency or regional office; controller unknown",
    lodgingControlConfidence: "LOW",
    modeledRoomsMin: 4,
    modeledRoomsMax: 12,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis:
      "Modeled international partner demo team — lodging not verified",
    modeledAncillary: ["product showcase staging"],
    hotelFitScore: 70,
    buyerEntity: "Anycubic — event / brand activation",
    buyerRole: "Brand activation / Events",
    publicContactPath: "https://makerfairerome.eu/en/exhibitors/?order=asc",
  },
  {
    accountName: "Eni",
    parentDemandSignalId: "wrome_signal_maker_faire_rome_2026",
    accountRole: "PARTNER",
    evidenceStatus: "REJECTED",
    rejectReason:
      "No official MFR2026 exhibitor/sponsor listing for Eni corporate; only Joule (Eni business school) participation language found — see separate Joule (Eni) acceptance",
    evidenceItems: [],
  },
  {
    accountName: "Joule (Eni)",
    parentDemandSignalId: "wrome_signal_maker_faire_rome_2026",
    accountRole: "PARTNER",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        type: "official_event_site",
        url: "https://makerfairerome.eu/en/",
        note: "Maker Faire Rome site describes Joule, Eni's business school, as recurring participant / hackathon contributor",
      },
    ],
    travelingCohortType: "PROGRAM_AND_HACKATHON_SUPPORT",
    travelingCohortSummary:
      "Joule (Eni) program staff / mentors supporting Maker Faire Rome hackathon and partner presence",
    travelingCohortConfidence: "LOW",
    lodgingControlHypothesis: LODGING_CONTROL.UNKNOWN,
    lodgingControlSummary:
      "Eni/Joule Rome-based school may be largely local — travel cohort for overnight lodging is weaker",
    lodgingControlConfidence: "LOW",
    modeledRoomsMin: 3,
    modeledRoomsMax: 10,
    modeledNightsMin: 1,
    modeledNightsMax: 3,
    modeledDemandBasis:
      "Modeled overnight support band; many Joule staff may be Rome-local — lodging not verified",
    modeledAncillary: [],
    hotelFitScore: 58,
    buyerEntity: "Joule (Eni) — program / events",
    buyerRole: "Program / Events",
    publicContactPath: "https://makerfairerome.eu/en/",
  },

  // —— Rome Film Fest ——
  {
    accountName: "Banca Ifis",
    parentDemandSignalId: "wrome_signal_rome_film_fest_2026",
    accountRole: "SPONSOR",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        type: "official_festival_announcement",
        url: "https://www.romacinemafest.it/en/banca-ifis-main-partner/",
        note: "Fondazione Cinema per Roma: Banca Ifis Main Partner for three years from 2026 edition",
      },
      {
        type: "official_press_pdf",
        url: "https://www.romacinemafest.it/wp-content/uploads/2026/05/Comunicato-Banca-Ifis-alla-Festa-del-Cinema.pdf",
        note: "Official Banca Ifis Main Partner press release",
      },
    ],
    travelingCohortType: "SPONSOR_VIP_AND_EXECUTIVE_HOSPITALITY",
    travelingCohortSummary:
      "Banca Ifis executive / client hospitality and Music Award ceremony guests around Rome Film Fest opening week",
    travelingCohortConfidence: "MEDIUM",
    lodgingControlHypothesis: LODGING_CONTROL.ACCOUNT_DIRECT,
    lodgingControlSummary:
      "Main Partner hospitality typically coordinated by bank events / communications; desk not named publicly",
    lodgingControlConfidence: "LOW",
    modeledRoomsMin: 8,
    modeledRoomsMax: 20,
    modeledNightsMin: 2,
    modeledNightsMax: 5,
    modeledDemandBasis:
      "Modeled Main Partner VIP / executive hospitality band for festival opening week — lodging not verified",
    modeledAncillary: ["award ceremony hospitality", "private dining", "suites"],
    hotelFitScore: 84,
    buyerEntity: "Banca Ifis — events / corporate communications / Ifis art",
    buyerRole: "Corporate events / Sponsorship hospitality",
    publicContactPath: "https://www.romacinemafest.it/en/banca-ifis-main-partner/",
  },
]);

function signalById(id) {
  return PILOT_DEMAND_SIGNALS.find((s) => s.id === id) || null;
}

function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

export function pilotOpportunityId(accountName, signalId) {
  return `gdi_opp_wrome_pilot_v0_${slug(accountName)}_${slug(signalId).replace(
    /^wrome_signal_/,
    ""
  )}`;
}

/**
 * Maturity evaluation — no forced ACTIONABLE.
 */
export function evaluatePilotMaturity(account, readyProbe = null) {
  if (account.evidenceStatus === "REJECTED") {
    return {
      state: GDI_MATURITY_STATE.REJECTED,
      reasons: [account.rejectReason || "rejected"],
    };
  }
  if (!account.evidenceItems?.length) {
    return { state: GDI_MATURITY_STATE.SIGNAL, reasons: ["no_account_evidence"] };
  }

  const hasCohort =
    Boolean(account.travelingCohortType) &&
    Boolean(account.travelingCohortSummary) &&
    account.travelingCohortConfidence !== "NONE";

  if (!hasCohort) {
    return {
      state: GDI_MATURITY_STATE.CANDIDATE,
      reasons: ["missing_traveling_cohort"],
    };
  }

  const fit = Number(account.hotelFitScore) || 0;
  const hasModeled =
    account.modeledRoomsMin != null && account.modeledRoomsMax != null;
  const hasLodgingHypothesis = Boolean(account.lodgingControlHypothesis);

  // Local-heavy / weak fit stays CANDIDATE (honest — Joule pattern)
  if (fit < 65 || account.travelingCohortConfidence === "LOW" && fit < 70) {
    return {
      state: GDI_MATURITY_STATE.CANDIDATE,
      reasons: ["hotel_fit_or_cohort_confidence_insufficient_for_qualified"],
    };
  }

  if (!hasModeled || !hasLodgingHypothesis) {
    return {
      state: GDI_MATURITY_STATE.CANDIDATE,
      reasons: ["missing_modeled_demand_or_lodging_hypothesis"],
    };
  }

  // QUALIFIED bar: evidence + cohort + fit + modeled honesty fields
  if (readyProbe?.ok === true) {
    return { state: GDI_MATURITY_STATE.ACTIONABLE, reasons: ["ready_gate_pass"] };
  }

  return {
    state: GDI_MATURITY_STATE.QUALIFIED,
    reasons: ["evidence_cohort_fit_modeled_ready_gate_not_met"],
  };
}

export function buildModeledDemandDisclaimer(account) {
  const min = account.modeledRoomsMin;
  const max = account.modeledRoomsMax;
  if (min == null || max == null) {
    return "Modeled demand — lodging not verified.";
  }
  return `Estimated ${min}–${max} rooms. Modeled demand — lodging not verified.`;
}

export function buildPilotOpportunityCandidate(account) {
  const signal = signalById(account.parentDemandSignalId);
  if (!signal || account.evidenceStatus !== "ACCEPTED") return null;

  const id = pilotOpportunityId(account.accountName, account.parentDemandSignalId);
  const modeledDisclaimer = buildModeledDemandDisclaimer(account);
  const missingValidation = [
    "lodging_block_unverified",
    "headcount_unverified",
    "named_buyer_person_missing",
    account.lodgingControlHypothesis === LODGING_CONTROL.UNKNOWN
      ? "lodging_controller_unknown"
      : null,
  ].filter(Boolean);

  const title = `${account.accountName} — ${signal.label} ${String(account.accountRole || "")
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase())}`;

  const summaryWhat = [
    `${account.accountName} is a published ${String(account.accountRole || "participant")
      .replace(/_/g, " ")
      .toLowerCase()} for ${signal.label}.`,
    `Traveling cohort: ${account.travelingCohortSummary}.`,
    modeledDisclaimer,
    `W Rome fit: lifestyle / VIP hospitality corridor for compact premium groups — not a mass attendee play.`,
    `Next: ${account.recommendedNextAction || "Confirm hospitality / travel desk and whether preferred-hotel inventory is open."}`,
  ].join(" ");

  const sources = (account.evidenceItems || []).map((e) => ({
    url: e.url,
    title: e.note || e.type,
    type: e.type,
  }));

  const draft = {
    id,
    hotelId: W_ROME_HOTEL_ID,
    title,
    organizationName: account.organizationNameCanonical || account.accountName,
    buyerEntity: account.buyerEntity || account.accountName,
    buyerRole: account.buyerRole || account.accountRole,
    primaryContactRole: account.buyerRole || account.accountRole,
    publicContactPath: account.publicContactPath || signal.officialSource,
    participationRole: account.accountRole,
    accountQualityClass: ACCOUNT_QUALITY_CLASS.TRUE_PARTICIPATING_ACCOUNT,
    parentDemandSignalId: signal.id,
    parentDemandSignalLabel: signal.label,
    parentCampaignId: signal.id,
    eventStartDate: signal.eventStartDate,
    eventEndDate: signal.eventEndDate,
    eventName: signal.label,
    canonicalEventName: signal.label,
    geography: "Rome, Italy",
    market: "Rome / Centro destination",
    venue: signal.venue,
    segment: String(account.accountRole || "").replace(/_/g, " "),
    summaryWhat,
    summaryWhyHotel:
      "W Rome (Via Liguria / Via Veneto lifestyle luxury) fits compact VIP, sponsor hospitality, and executive groups that want centro positioning rather than Fiera/Ostia mass housing.",
    fitExplanation:
      "Prefer 5–30 room premium groups, sponsor/VIP hospitality, suites and private dining — down-rank mass attendees and venue operators.",
    hotelFitScore: account.hotelFitScore,
    whyNow: `${signal.label} is a confirmed published cycle (${signal.eventStartDate}–${signal.eventEndDate}). Engage hospitality / sponsorship desks while partner programs and overflow lists are still open.`,
    recommendedAction: `Contact ${account.buyerEntity || account.accountName} (${account.buyerRole || "events / hospitality"}) to confirm Rome travel / hospitality arrangements for ${signal.label} and ask whether preferred-hotel or centro overflow inventory is still open. Do not pitch invented room counts — use modeled band only as planning context.`,
    recommendedNextStep: null,
    bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
    priority: "WATCHLIST",
    researchStatus: "QUALIFIED",
    expansionPilotId: W_ROME_EXPANSION_PILOT_ID,
    gdiMaturityState: GDI_MATURITY_STATE.CANDIDATE, // stamped after evaluate
    accountRole: account.accountRole,
    travelingCohortType: account.travelingCohortType,
    travelingCohortSummary: account.travelingCohortSummary,
    travelingCohortConfidence: account.travelingCohortConfidence,
    lodgingControlHypothesis: account.lodgingControlHypothesis,
    lodgingControlSummary: account.lodgingControlSummary,
    lodgingControlConfidence: account.lodgingControlConfidence,
    modeledRoomsMin: account.modeledRoomsMin,
    modeledRoomsMax: account.modeledRoomsMax,
    modeledNightsMin: account.modeledNightsMin,
    modeledNightsMax: account.modeledNightsMax,
    modeledDemandBasis: account.modeledDemandBasis,
    modeledAncillary: account.modeledAncillary || [],
    modeledDemandDisclaimer: modeledDisclaimer,
    lodgingVerified: false,
    headcountVerified: false,
    estimatedPeakRooms: null,
    publishedPeakRooms: null,
    peakRooms: null,
    missingValidation,
    buyerResearchStatus: "NOT_STARTED",
    sources,
    evidenceUrls: sources.map((s) => s.url),
    demandFamily: "EVENT_SPONSOR_EXHIBITOR",
    opportunityType: "GROUP_DEMAND",
    contactResearchAttempted: false,
    whoResearchAttempted: false,
    customerSurfaceDisposition: "KEEP_ACTIVE",
    gdiPilotCorpus: true,
    isTestData: false,
    isDemandGenerator: false,
    demandGeneratorOnly: false,
  };

  draft.recommendedNextStep = draft.recommendedAction;
  return draft;
}

/**
 * Build evaluated candidates for accepted research rows.
 */
export function buildEvaluatedPilotCandidates(opts = {}) {
  const nowDate = opts.nowDate || "2026-10-05";
  const accepted = [];
  const rejected = [];

  for (const row of PILOT_ACCOUNT_RESEARCH) {
    if (row.evidenceStatus === "REJECTED") {
      rejected.push({
        accountName: row.accountName,
        parentDemandSignalId: row.parentDemandSignalId,
        reason: row.rejectReason,
      });
      continue;
    }
    const candidate = buildPilotOpportunityCandidate(row);
    if (!candidate) continue;

    const readyProbe = isGdiCustomerOpportunityReady(
      {
        ...candidate,
        customerVisible: true,
        customerActiveEligible: true,
        customerSurfaceDisposition: "KEEP_ACTIVE",
        customerFacingState: "ACTIVE",
        whoResearchAttempted: true,
        contactResearchAttempted: true,
      },
      { nowDate }
    );

    const maturity = evaluatePilotMaturity(row, readyProbe);
    candidate.gdiMaturityState = maturity.state;
    candidate.maturityReasons = maturity.reasons;
    candidate.customerReadiness = readyProbe;

    if (maturity.state === GDI_MATURITY_STATE.ACTIONABLE) {
      candidate.customerVisible = true;
      candidate.customerFacingState = "ACTIVE";
      candidate.customerActiveEligible = true;
      candidate.priority = "HIGH";
    } else if (maturity.state === GDI_MATURITY_STATE.QUALIFIED) {
      candidate.customerVisible = true; // gated at visibility layer by pilot flag
      candidate.customerFacingState = "QUALIFIED";
      candidate.customerActiveEligible = true;
      candidate.priority = "WATCHLIST";
      // CONTACT (not PURSUE NOW) — distinguishes QUALIFIED from ACTIONABLE/Ready
      candidate.bookingWindowStatus = BOOKING_WINDOW.QUALIFY_NOW;
      candidate.qualificationFailureReason = `pilot_qualified_not_actionable:${(readyProbe.failed || []).join("|")}`;
    } else {
      candidate.customerVisible = false;
      candidate.customerFacingState = "CANDIDATE";
      candidate.customerActiveEligible = false;
      candidate.bookingWindowStatus = BOOKING_WINDOW.WATCH;
    }

    accepted.push({ research: row, candidate, maturity, readyProbe });
  }

  return { accepted, rejected, signals: PILOT_DEMAND_SIGNALS };
}
