/**
 * Frozen five-campaign publication monitors (AC Coruña + Radisson SD).
 * Known official / directly affiliated sources only — no broad discovery seeds.
 */

import { MONITOR_STATUS, PUBLICATION_TRIGGER_TYPE } from "./constants.js";

export const AC_HOTEL_ID = "rec2PVBDavppGpenm";
export const RAD_HOTEL_ID = "recUOyzOXn2Zdp98I";

const T = PUBLICATION_TRIGGER_TYPE;

/**
 * Canonical monitor seeds for publication-trigger monitoring.
 * One monitor row per (campaign × primary source URL × primary trigger family).
 */
export function buildFiveCampaignMonitorSeeds(now = new Date()) {
  const today = new Date(now).toISOString().slice(0, 10);

  return [
    // ——— IAPS ———
    {
      monitorId: "gdi_pm_iaps_programme_speakers_v1",
      campaignId: "accamp_iaps_spaces_in_transition_symposium_2027_2027",
      hotelId: AC_HOTEL_ID,
      campaignKey: "IAPS",
      triggerType: T.PROGRAMME_PUBLISHED,
      watchForTypes: [
        T.PROGRAMME_PUBLISHED,
        T.SPEAKER_LIST_PUBLISHED,
        T.ACCEPTED_PAPERS_PUBLISHED,
        T.PARTICIPANT_LIST_PUBLISHED,
        T.LODGING_PAGE_PUBLISHED,
      ],
      triggerSourceUrl: "https://iaps-association.org/",
      alternateSourceUrls: [
        "https://iaps-association.org/networks/sustainability/",
      ],
      sourceLanguage: "en",
      currentSourceState: "LIST_NOT_YET_PUBLISHED",
      expectedPublicationWindowStart: "2026-12-15",
      expectedPublicationWindowEnd: "2027-05-31",
      eventEndDate: "2027-06-16",
      milestoneDates: {
        abstractDeadline: "2026-12-31",
      },
      priorityRank: 3,
      monitoringStatus: MONITOR_STATUS.ACTIVE,
      customerMonitoringFor: ["Programme", "Speaker programme", "Accommodation guidance"],
      notes:
        "After 2026-12-31 abstract deadline, increase for accepted abstracts / programme / speakers / accommodation.",
      scheduleRationaleSeed: "Abstract deadline 2026-12-31 → higher cadence from mid-Dec 2026",
      seededAt: today,
    },
    // ——— BioCultura ———
    {
      monitorId: "gdi_pm_biocultura_exhibitor_directory_v1",
      campaignId: "accamp_biocultura_a_coruna_2027_2027",
      hotelId: AC_HOTEL_ID,
      campaignKey: "BIOCULTURA",
      triggerType: T.EXHIBITOR_LIST_PUBLISHED,
      watchForTypes: [
        T.EXHIBITOR_LIST_PUBLISHED,
        T.EXHIBITOR_MANUAL_PUBLISHED,
        T.SPONSOR_LIST_PUBLISHED,
        T.LODGING_PAGE_PUBLISHED,
        T.HOTEL_LIST_PUBLISHED,
      ],
      triggerSourceUrl: "https://www.biocultura.org/acoruna",
      alternateSourceUrls: [
        "https://www.biocultura.org/acoruna/viajes",
        "https://vidasana.org/",
      ],
      sourceLanguage: "es",
      currentSourceState: "LIST_NOT_YET_PUBLISHED",
      expectedPublicationWindowStart: "2026-10-01",
      expectedPublicationWindowEnd: "2027-03-01",
      eventEndDate: "2027-03-31",
      milestoneDates: {
        standAllocation: "2027-01-10",
      },
      priorityRank: 2,
      monitoringStatus: MONITOR_STATUS.ACTIVE,
      customerMonitoringFor: ["Exhibitor list", "Exhibitor manual", "Accommodation guidance"],
      notes:
        "~130 exhibitors announced; directory not public. Stand allocation ~2027-01-10 — monitor directory before/after.",
      scheduleRationaleSeed: "Q4 2026–Q1 2027 exhibitor directory window; stand process 2027-01-10",
      seededAt: today,
    },
    // ——— RIF ———
    {
      monitorId: "gdi_pm_rif_programme_papers_v1",
      campaignId: "radisscamp_vii_congreso_iberoamericano_de_filosofia_2027_2027",
      hotelId: RAD_HOTEL_ID,
      campaignKey: "RIF",
      triggerType: T.ACCEPTED_PAPERS_PUBLISHED,
      watchForTypes: [
        T.ACCEPTED_PAPERS_PUBLISHED,
        T.PROGRAMME_PUBLISHED,
        T.SPEAKER_LIST_PUBLISHED,
        T.DELEGATION_LIST_PUBLISHED,
        T.PARTICIPANT_LIST_PUBLISHED,
        T.LODGING_PAGE_PUBLISHED,
        T.HOTEL_LIST_PUBLISHED,
      ],
      triggerSourceUrl: "https://rediberoamericanafilosofia.com/",
      alternateSourceUrls: [
        "https://rediberoamericanafilosofia.com/wp-content/uploads/2026/05/Segunda-convocatoria-Congreso-RIF.pdf",
      ],
      sourceLanguage: "es",
      currentSourceState: "LIST_NOT_YET_PUBLISHED",
      expectedPublicationWindowStart: "2026-11-01",
      expectedPublicationWindowEnd: "2027-03-01",
      eventEndDate: "2027-03-31",
      priorityRank: 2,
      monitoringStatus: MONITOR_STATUS.ACTIVE,
      customerMonitoringFor: ["Accepted papers", "Programme", "Speaker programme"],
      notes:
        "Convocatoria PDF compressed — no extractable university list yet. Monitor through pre-Mar 2027 for papers/programme/universities.",
      scheduleRationaleSeed: "Pre-March 2027 publication period for papers / programme / delegations",
      seededAt: today,
    },
    // ——— CIELO (highest near-term priority — Dec 2026) ———
    {
      monitorId: "gdi_pm_cielo_speakers_lodging_v1",
      campaignId: "radisscamp_6_congreso_mundial_cielo_laboral_2026_2026",
      hotelId: RAD_HOTEL_ID,
      campaignKey: "CIELO",
      triggerType: T.SPEAKER_LIST_PUBLISHED,
      watchForTypes: [
        T.SPEAKER_LIST_PUBLISHED,
        T.PARTICIPANT_LIST_PUBLISHED,
        T.PROGRAMME_PUBLISHED,
        T.HOTEL_LIST_PUBLISHED,
        T.LODGING_PAGE_PUBLISHED,
      ],
      triggerSourceUrl:
        "https://www.cielolaboral.com/6o-congreso-mundial-cielo-laboral-santodomingo/",
      alternateSourceUrls: [],
      sourceLanguage: "es",
      currentSourceState: "LIST_NOT_YET_PUBLISHED",
      expectedPublicationWindowStart: "2026-10-01",
      expectedPublicationWindowEnd: "2026-12-10",
      eventEndDate: "2026-12-15",
      graceDaysAfterWindow: 21,
      priorityRank: 1,
      monitoringStatus: MONITOR_STATUS.ACTIVE,
      customerMonitoringFor: [
        "Speaker programme",
        "Participant list",
        "Hotel list",
        "Accommodation guidance",
      ],
      notes:
        "Highest priority — Dec 2026 event. Watch accepted speakers, programme, recommended-hotels PDF. If hotel list publishes, classify whether Radisson can still enter selection.",
      scheduleRationaleSeed: "Near-term Dec 2026 — HIGH frequency inside Oct–Dec window",
      seededAt: today,
    },
    // ——— AUTOAMERICAS (exhibitor directory; lodging already DIRECT) ———
    {
      monitorId: "gdi_pm_autoamericas_exhibitor_directory_v1",
      campaignId: "radisscamp_autoamericas_2027_2027",
      hotelId: RAD_HOTEL_ID,
      campaignKey: "AUTOAMERICAS",
      triggerType: T.EXHIBITOR_LIST_PUBLISHED,
      watchForTypes: [
        T.EXHIBITOR_LIST_PUBLISHED,
        T.SPONSOR_LIST_PUBLISHED,
        T.EXHIBITOR_MANUAL_PUBLISHED,
        T.HOTEL_LIST_PUBLISHED,
        T.LODGING_PAGE_PUBLISHED,
      ],
      triggerSourceUrl: "https://www.autoamericas.show/es/",
      alternateSourceUrls: [
        "https://www.autoamericas.show/es/expo/alojamiento.html",
      ],
      sourceLanguage: "es",
      currentSourceState: "LIST_PARTIAL",
      expectedPublicationWindowStart: "2027-01-01",
      expectedPublicationWindowEnd: "2027-04-01",
      eventEndDate: "2027-04-24",
      priorityRank: 2,
      monitoringStatus: MONITOR_STATUS.ACTIVE,
      customerMonitoringFor: ["Exhibitor list", "Sponsor list", "Hotel list"],
      notes:
        "DIRECT lodging (Dominican Fiesta official) already known — do NOT treat as Radisson opportunity. Monitor exhibitor directory; only secondary/overflow lodging with evidence.",
      autoAmericasSpecial: {
        officialHotel: "Hotel Dominican Fiesta",
        treatOfficialHotelAsRadissonOpportunity: false,
        requireOverflowEvidenceForRadisson: true,
      },
      scheduleRationaleSeed: "Exhibitor directory expected Q1 2027; lodging page already live",
      seededAt: today,
    },
  ];
}

export function seedMonitorsForHotel(hotelId, now = new Date()) {
  return buildFiveCampaignMonitorSeeds(now).filter((m) => m.hotelId === hotelId);
}
