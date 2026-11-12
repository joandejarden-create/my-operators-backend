/**
 * Evidence-backed CMI ledger — a seed is a research pointer, not proof.
 * Verified CMI requires an attached source supporting the exact competitor
 * venue/block claim for the cited edition.
 */

import {
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
  COMMERCIAL_ACTIONABILITY,
  COMPETITOR_VENUE_STATUS_LABEL,
  TARGET_HOTEL_SOURCING_STATUS_LABEL,
  recommendCommercialAction,
} from "./commercial-status-dimensions-v1.js";

export const CMI_EVIDENCE_LEDGER_V1 = "gdi-cmi-evidence-ledger-v1";

export const CMI_CLAIM_TYPE = Object.freeze({
  COMPETITOR_ROOM_BLOCK: "COMPETITOR_ROOM_BLOCK",
  COMPETITOR_EVENT_VENUE: "COMPETITOR_EVENT_VENUE",
  COMPETITOR_HOST_HOTEL: "COMPETITOR_HOST_HOTEL",
  TARGET_OVERFLOW: "TARGET_OVERFLOW",
  TARGET_OPEN_SOURCING: "TARGET_OPEN_SOURCING",
  ATTENDANCE: "ATTENDANCE",
  EVENT_DATES: "EVENT_DATES",
});

/**
 * Audited ledger for the seven Bethesda CMI candidates.
 * verifiedCmi=true only when a competitor booking/venue claim has supporting URLs.
 */
export const BETHESDA_CMI_EVIDENCE_LEDGER = Object.freeze({
  gdi_opp_acvnu_renal_week_20270412: Object.freeze({
    verifiedCmi: true,
    competitorHotel: "The Bethesdan Hotel (Tapestry Collection by Hilton)",
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    commercialActionability: COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
    advertisedAttendance: "250+",
    eventDates: "2027-04-12/2027-04-17",
    roomBlockDates: "2027-04-11/2027-04-17",
    verifiedAt: "2026-11-12",
    claims: Object.freeze([
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.COMPETITOR_ROOM_BLOCK,
        status: "VERIFIED",
        evidenceUrls: Object.freeze([
          "https://acvnurenalweek.org/Plan-Your-Trip/Hotel-Accommodations",
          "https://acvnurenalweek.org/",
        ]),
        statement:
          "Official ACVNU room block at The Bethesdan Hotel for stays April 11–17, 2027; event venue The Bethesdan.",
      }),
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.ATTENDANCE,
        status: "VERIFIED",
        evidenceUrls: Object.freeze(["https://acvnurenalweek.org/"]),
        statement: "Advertised attendance 250+ specialists, veterinarians, and technicians.",
      }),
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.TARGET_OPEN_SOURCING,
        status: "NOT_SUPPORTED",
        evidenceUrls: Object.freeze([
          "https://acvnurenalweek.org/Plan-Your-Trip/Hotel-Accommodations",
        ]),
        statement:
          "No verified open Bethesda Marriott RFP or overflow requirement on the official housing page.",
      }),
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.TARGET_OVERFLOW,
        status: "NOT_SUPPORTED",
        evidenceUrls: Object.freeze([
          "https://acvnurenalweek.org/Plan-Your-Trip/Hotel-Accommodations",
        ]),
        statement: "Overflow for Bethesda Marriott is not evidenced.",
      }),
    ]),
    remediation: Object.freeze({
      title: "ACVNU Renal Week 2027 — competitor room block at The Bethesdan",
      whyNow:
        "Competitor contracted room block confirmed at The Bethesdan (Apr 11–17, 2027). Bethesda Marriott sourcing for this edition is not established.",
      summaryWhat:
        "ACVNU Renal Week (Apr 12–17, 2027) is hosted with an official room block at The Bethesdan Hotel. Advertised attendance 250+. No verified open Marriott RFP/overflow.",
      summaryWhyMatters:
        "Verified competitor booking in Bethesda corridor — useful CMI; not an open host RFP for Bethesda Marriott.",
      lodgingEvidence:
        "Official room block at The Bethesdan Hotel (Tapestry); block dates Apr 11–17, 2027.",
      housingStatus: "COMPETITOR_BLOCK_CONFIRMED",
      roomDemandStatus: "COMPETITOR_BLOCK_CONFIRMED",
      venueSourcingStatus: "COMPETITOR_BLOCK_CONFIRMED_TARGET_NOT_ESTABLISHED",
      opportunityType: "COMPETITIVE_MARKET_INTELLIGENCE",
      opportunityTypeLabel: "Competitive Market Intelligence",
      demandType: "Competitive Market Intelligence",
      contactRoleNote:
        "JD Foster is an activity-connected conference chair / ACVNU President-Elect — not verified hotel-booking buyer authority.",
    }),
  }),

  gdi_opp_nado_ddaa_washcon_2027_overflow: Object.freeze({
    verifiedCmi: true,
    competitorHotel: "Crystal Gateway Marriott",
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.UNKNOWN,
    commercialActionability: COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
    verifiedAt: "2026-11-12",
    claims: Object.freeze([
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.COMPETITOR_HOST_HOTEL,
        status: "VERIFIED",
        evidenceUrls: Object.freeze([
          "https://www.nado.org/2027washcon/",
          "https://www.nado.org/2026washcon/",
        ]),
        statement:
          "Prior recovery + organizer pages: 2027 WashCon hosted at Crystal Gateway Marriott with published housing rates.",
      }),
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.TARGET_OVERFLOW,
        status: "NOT_SUPPORTED",
        evidenceUrls: Object.freeze(["https://www.nado.org/2027washcon/"]),
        statement:
          "Overflow need for Bethesda Marriott is hypothesized, not evidenced.",
      }),
    ]),
    remediation: Object.freeze({
      title:
        "NADO & DDAA Washington Conference 2027 — Crystal Gateway Marriott host",
      competitorHotel: "Crystal Gateway Marriott",
      whyNow:
        "Competitor host confirmed at Crystal Gateway Marriott. Overflow for Bethesda Marriott remains unconfirmed — do not present as an open lodging path.",
      summaryWhat:
        "2027 WashCon is hosted at Crystal Gateway Marriott with published housing rates. Bethesda Marriott overflow is hypothesized, not evidenced.",
      summaryWhyMatters:
        "Verified competitor host booking — useful CMI; overflow to Bethesda Marriott is not confirmed.",
      lodgingEvidence:
        "Crystal Gateway Marriott named host with published rates (nado.org/2027washcon).",
      housingStatus: "COMPETITOR_HOST_CONFIRMED",
      venueSourcingStatus: "COMPETITOR_HOST_CONFIRMED_OVERFLOW_UNCONFIRMED",
      opportunityType: "COMPETITIVE_MARKET_INTELLIGENCE",
      opportunityTypeLabel: "Competitive Market Intelligence",
      demandType: "Competitive Market Intelligence",
      recommendedAction:
        "Competitor booking confirmed at Crystal Gateway Marriott. Identify the housing decision-maker and qualify whether any overflow or secondary lodging is needed — do not request an RFP as host.",
    }),
  }),

  gdi_opp_nado_ddaa_washcon_2028: Object.freeze({
    // Edition-specific confirmation: official 2028 WashCon page names Crystal Gateway
    // Marriott as conference hotel with published room rates (not a 2027 historical pattern).
    verifiedCmi: true,
    competitorHotel: "Crystal Gateway Marriott",
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    commercialActionability: COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
    verifiedAt: "2026-11-12",
    evidenceBasis: "CONFIRMED_2028_COMPETITOR_BOOKING",
    claims: Object.freeze([
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.COMPETITOR_HOST_HOTEL,
        status: "VERIFIED",
        evidenceUrls: Object.freeze([
          "https://www.nado.org/2028washcon/",
        ]),
        statement:
          "Official 2028 NADO & DDAA WashCon page (March 19–22, 2028) names Crystal Gateway Marriott as the conference hotel with published weekend/weekday room rates.",
      }),
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.COMPETITOR_ROOM_BLOCK,
        status: "VERIFIED",
        evidenceUrls: Object.freeze(["https://www.nado.org/2028washcon/"]),
        statement:
          "Accommodations section publishes Crystal Gateway Marriott room rates for the 2028 edition (weekend $215 / weekday $300 + tax).",
      }),
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.TARGET_OVERFLOW,
        status: "NOT_SUPPORTED",
        evidenceUrls: Object.freeze(["https://www.nado.org/2028washcon/"]),
        statement:
          "No Bethesda Marriott overflow or open-sourcing requirement evidenced on the 2028 organizer page.",
      }),
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.TARGET_OPEN_SOURCING,
        status: "NOT_SUPPORTED",
        evidenceUrls: Object.freeze(["https://www.nado.org/2028washcon/"]),
        statement:
          "Host hotel already named — Bethesda Marriott is not an open host RFP for this edition.",
      }),
    ]),
    remediation: Object.freeze({
      competitorHotel: "Crystal Gateway Marriott",
      title:
        "NADO & DDAA Washington Conference 2028 — Crystal Gateway Marriott host (confirmed)",
      whyNow:
        "Confirmed 2028 competitor booking at Crystal Gateway Marriott with published rates. Bethesda Marriott sourcing for this edition is not established — qualify overflow only; do not request an RFP as host.",
      summaryWhat:
        "NADO & DDAA WashCon March 19–22, 2028 is hosted at Crystal Gateway Marriott (Arlington) with published room rates on the official organizer page.",
      summaryWhyMatters:
        "Verified competitor host booking for the 2028 edition — useful CMI; not an open host bid for Bethesda Marriott.",
      lodgingEvidence:
        "Official 2028 accommodations: Crystal Gateway Marriott; weekend $215 / weekday $300 + tax (nado.org/2028washcon).",
      housingStatus: "COMPETITOR_BLOCK_CONFIRMED",
      roomDemandStatus: "COMPETITOR_BLOCK_CONFIRMED",
      venueSourcingStatus: "COMPETITOR_HOST_CONFIRMED_TARGET_NOT_ESTABLISHED",
      opportunityType: "COMPETITIVE_MARKET_INTELLIGENCE",
      opportunityTypeLabel: "Competitive Market Intelligence",
      demandType: "Competitive Market Intelligence",
      recommendedAction:
        "Competitor booking confirmed at Crystal Gateway Marriott for March 19–22, 2028. Identify the housing decision-maker and qualify whether any overflow or secondary lodging is needed — do not request an RFP as host.",
    }),
  }),

  gdi_opp_afcea_hits_2027: Object.freeze({
    // 2027 summit page was unavailable (500); 2026 official lodging is Bethesda North Marriott.
    // Hyatt Regency Bethesda is unsupported. Do not keep verified CMI on wrong competitor.
    verifiedCmi: false,
    priorIncorrectCompetitorHotel: "Hyatt Regency Bethesda",
    competitorHotel: "Bethesda North Marriott Hotel & Conference Center",
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.VERIFIED_HISTORICAL_VENUE,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR,
    commercialActionability: COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION,
    verifiedAt: "2026-11-12",
    demotionReason:
      "2026 HITS lodging is Bethesda North Marriott; 2027 official page unavailable at audit time. Hyatt Regency Bethesda claim unsupported. Historical venue ≠ verified 2027 contracted block.",
    claims: Object.freeze([
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.COMPETITOR_HOST_HOTEL,
        status: "HISTORICAL_ONLY",
        evidenceUrls: Object.freeze([
          "https://bethesdaevents.afceachapters.org/HITS26",
        ]),
        statement:
          "2026 Health IT Summit lodging/venue: Bethesda North Marriott (not Hyatt Regency Bethesda).",
      }),
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.COMPETITOR_ROOM_BLOCK,
        status: "NOT_SUPPORTED",
        evidenceUrls: Object.freeze([]),
        statement: "No verified 2027 contracted block source at audit time.",
      }),
    ]),
    remediation: Object.freeze({
      competitorHotel: "Bethesda North Marriott Hotel & Conference Center",
      title:
        "AFCEA Bethesda Health IT Summit 2027 — verify 2027 lodging (2026 = Bethesda North Marriott)",
      whyNow:
        "Correct competitor identity: 2026 official lodging was Bethesda North Marriott, not Hyatt Regency Bethesda. Re-verify 2027 housing before CMI verification.",
      venueSourcingStatus: "FUTURE_CYCLE_TO_MONITOR",
      recommendedAction:
        "Re-verify the 2027 AFCEA HITS housing page when available; do not pursue Hyatt-based CMI or open RFP claims.",
    }),
  }),

  gdi_opp_ecs_251_2027_overflow: Object.freeze({
    verifiedCmi: true,
    competitorHotel: "Marriott Marquis Washington, DC",
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    commercialActionability: COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
    verifiedAt: "2026-11-12",
    priorIncorrectCompetitorHotel:
      "Bethesda North Marriott Hotel & Conference Center",
    claims: Object.freeze([
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.COMPETITOR_EVENT_VENUE,
        status: "VERIFIED",
        evidenceUrls: Object.freeze(["https://www.electrochem.org/251/"]),
        statement:
          "251st ECS Meeting: Walter E. Washington Convention Center and Marriott Marquis, May 30–June 3, 2027.",
      }),
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.TARGET_OVERFLOW,
        status: "NOT_SUPPORTED",
        evidenceUrls: Object.freeze(["https://www.electrochem.org/251/"]),
        statement:
          "Bethesda North Marriott is not the organizer-named lodging venue; overflow to Bethesda Marriott not evidenced.",
      }),
    ]),
    remediation: Object.freeze({
      competitorHotel: "Marriott Marquis Washington, DC",
      title:
        "251st ECS Meeting 2027 — WEWCC + Marriott Marquis (not Bethesda North)",
      whyNow:
        "Organizer confirms WEWCC + Marriott Marquis. Correct competitor attribution; Bethesda Marriott overflow not established.",
      venueSourcingStatus: "COMPETITOR_VENUE_CONFIRMED_TARGET_NOT_ESTABLISHED",
      eventLocationSummary: "Washington, DC — WEWCC + Marriott Marquis",
    }),
  }),

  gdi_opp_world_biomaterials_2028_overflow: Object.freeze({
    verifiedCmi: false,
    competitorHotel: null,
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.ANNOUNCED_VENUE,
    primaryVenue: "Walter E. Washington Convention Center",
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    commercialActionability: COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION,
    verifiedAt: "2026-11-12",
    demotionReason:
      "ExpoFP/venue listing confirms convention center only. Bethesda North Marriott hotel-block claim unsupported.",
    priorIncorrectCompetitorHotel:
      "Bethesda North Marriott Hotel & Conference Center",
    claims: Object.freeze([
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.COMPETITOR_EVENT_VENUE,
        status: "VERIFIED",
        evidenceUrls: Object.freeze([
          "https://expofp.com/walter-e-washington-convention-center/world-biomaterials-congress",
        ]),
        statement:
          "World Biomaterials Congress 2028 listed at Walter E. Washington Convention Center (Apr 24–29, 2028).",
      }),
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.COMPETITOR_ROOM_BLOCK,
        status: "NOT_SUPPORTED",
        evidenceUrls: Object.freeze([
          "https://expofp.com/walter-e-washington-convention-center/world-biomaterials-congress",
        ]),
        statement: "No hotel room-block evidence for Bethesda North Marriott.",
      }),
    ]),
    remediation: Object.freeze({
      competitorHotel: null,
      title:
        "World Biomaterials Congress 2028 — WEWCC venue (hotel block not verified)",
      whyNow:
        "Convention center venue announced; no verified hotel block. Keep as research continuation, not verified CMI. Do not claim lodging demand path open.",
      summaryWhat:
        "World Biomaterials Congress 2028 is listed at Walter E. Washington Convention Center. No verified hotel room block.",
      summaryWhyMatters:
        "Venue announced only — hotel sourcing and competitor hotel block remain unverified.",
      lodgingEvidence:
        "No verified hotel block; convention-center listing only.",
      housingStatus: "HOTEL_BLOCK_NOT_VERIFIED",
      roomDemandStatus: "NOT_VERIFIED",
      venueSourcingStatus: "NOT_ESTABLISHED",
      opportunityType: "RESEARCH_CONTINUATION",
      opportunityTypeLabel: "Research Continuation / Monitor",
      demandType: "Research Continuation",
      recommendedAction:
        "Monitor official housing announcement; do not present Bethesda North Marriott as a verified competitor booking and do not request an RFP as host.",
    }),
  }),

  gdi_opp_aad_2028_overflow: Object.freeze({
    verifiedCmi: false,
    competitorHotel: null,
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.NOT_VERIFIED,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    commercialActionability: COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION,
    verifiedAt: "2026-11-12",
    demotionReason:
      "AAD 2028 Washington listing does not evidence a Bethesda North Marriott contracted block.",
    priorIncorrectCompetitorHotel:
      "Bethesda North Marriott Hotel & Conference Center",
    claims: Object.freeze([
      Object.freeze({
        claimType: CMI_CLAIM_TYPE.COMPETITOR_ROOM_BLOCK,
        status: "NOT_SUPPORTED",
        evidenceUrls: Object.freeze([
          "https://www.aad.org/member/meetings/events/locations",
          "https://www.showsbee.com/fairs/97402-AAD-Annual-Meeting-2028.html",
        ]),
        statement:
          "City/location listings only — no verified Bethesda North Marriott block.",
      }),
    ]),
    remediation: Object.freeze({
      competitorHotel: null,
      title: "AAD Annual Meeting 2028 — Washington, DC (hotel block not verified)",
      whyNow:
        "No verified competitor hotel block for Bethesda North Marriott. Demote from verified CMI.",
      venueSourcingStatus: "NOT_ESTABLISHED",
      recommendedAction:
        "Wait for official AAD housing list; do not attribute a competitor hotel without a block source.",
    }),
  }),
});

export function getCmiEvidenceEntry(opportunityId) {
  const id = String(opportunityId || "").trim();
  return BETHESDA_CMI_EVIDENCE_LEDGER[id] || null;
}

export function buildCmiClassificationFromLedger(opp = {}, entry = null) {
  const id = String(opp.id || opp.opportunityId || "");
  const ledger = entry || getCmiEvidenceEntry(id);
  if (!ledger) return null;

  const competitorHotel =
    ledger.remediation?.competitorHotel !== undefined
      ? ledger.remediation.competitorHotel
      : ledger.competitorHotel;

  if (ledger.verifiedCmi === true && competitorHotel) {
    return {
      class: "COMPETITIVE_MARKET_INTELLIGENCE",
      label: "Competitive Market Intelligence",
      verified: true,
      competitorHotel,
      competitorVenueStatus: ledger.competitorVenueStatus,
      competitorVenueStatusLabel:
        COMPETITOR_VENUE_STATUS_LABEL[ledger.competitorVenueStatus] || null,
      targetHotelSourcingStatus: ledger.targetHotelSourcingStatus,
      targetHotelSourcingStatusLabel:
        TARGET_HOTEL_SOURCING_STATUS_LABEL[ledger.targetHotelSourcingStatus] ||
        null,
      commercialActionability: ledger.commercialActionability,
      reason: (ledger.claims || [])
        .filter((c) => c.status === "VERIFIED")
        .map((c) => c.statement)
        .join(" "),
      evidenceUrls: [
        ...new Set(
          (ledger.claims || []).flatMap((c) => c.evidenceUrls || [])
        ),
      ],
      verifiedAt: ledger.verifiedAt || null,
      seedId: id,
      ledgerVersion: CMI_EVIDENCE_LEDGER_V1,
      demotionReason: null,
    };
  }

  return {
    class: "NEEDS_FURTHER_VERIFICATION",
    label: "Needs further verification",
    verified: false,
    competitorHotel: competitorHotel || null,
    competitorVenueStatus:
      ledger.competitorVenueStatus || COMPETITOR_VENUE_STATUS.NOT_VERIFIED,
    competitorVenueStatusLabel:
      COMPETITOR_VENUE_STATUS_LABEL[
        ledger.competitorVenueStatus || COMPETITOR_VENUE_STATUS.NOT_VERIFIED
      ] || null,
    targetHotelSourcingStatus:
      ledger.targetHotelSourcingStatus ||
      TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    targetHotelSourcingStatusLabel:
      TARGET_HOTEL_SOURCING_STATUS_LABEL[
        ledger.targetHotelSourcingStatus ||
          TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED
      ] || null,
    commercialActionability:
      ledger.commercialActionability ||
      COMMERCIAL_ACTIONABILITY.RESEARCH_CONTINUATION,
    reason: ledger.demotionReason || "Ledger requires further verification",
    evidenceUrls: [
      ...new Set((ledger.claims || []).flatMap((c) => c.evidenceUrls || [])),
    ],
    verifiedAt: ledger.verifiedAt || null,
    seedId: id,
    ledgerVersion: CMI_EVIDENCE_LEDGER_V1,
    demotionReason: ledger.demotionReason || null,
  };
}

export function applyCmiLedgerRemediation(opp = {}) {
  const id = String(opp.id || opp.opportunityId || "");
  const ledger = getCmiEvidenceEntry(id);
  if (!ledger) return opp;

  const cmi = buildCmiClassificationFromLedger(opp, ledger);
  const rem = ledger.remediation || {};
  const competitorHotel =
    rem.competitorHotel !== undefined ? rem.competitorHotel : ledger.competitorHotel;
  const targetHotelSourcingStatus = ledger.targetHotelSourcingStatus;
  const recommendedAction =
    rem.recommendedAction ||
    recommendCommercialAction({
      competitorVenueStatus: ledger.competitorVenueStatus,
      targetHotelSourcingStatus,
      commercialActionability: ledger.commercialActionability,
      competitorHotel,
      contactHint: opp.primaryContactName || opp.organizationContactRoute || "",
    });

  const prior = {
    competitorHotel: opp.competitorHotel || opp.likelyCompetitor || null,
    venueSourcingStatus: opp.venueSourcingStatus || null,
    recommendedAction: opp.recommendedAction || null,
    title: opp.title || null,
    housingStatus: opp.housingStatus || null,
  };

  return {
    ...opp,
    ...(rem.title ? { title: rem.title, opportunityName: rem.title } : {}),
    ...(rem.whyNow ? { whyNow: rem.whyNow } : {}),
    ...(rem.summaryWhat ? { summaryWhat: rem.summaryWhat } : {}),
    ...(rem.summaryWhyMatters
      ? { summaryWhyMatters: rem.summaryWhyMatters }
      : {}),
    ...(rem.lodgingEvidence ? { lodgingEvidence: rem.lodgingEvidence } : {}),
    ...(rem.housingStatus ? { housingStatus: rem.housingStatus } : {}),
    ...(rem.roomDemandStatus ? { roomDemandStatus: rem.roomDemandStatus } : {}),
    ...(rem.eventLocationSummary
      ? { eventLocationSummary: rem.eventLocationSummary }
      : {}),
    ...(rem.opportunityType
      ? {
          opportunityType: rem.opportunityType,
          opportunityTypeLabel: rem.opportunityTypeLabel || rem.opportunityType,
          demandType: rem.demandType || rem.opportunityTypeLabel,
        }
      : {}),
    competitorHotel: competitorHotel || null,
    competitorVenue: competitorHotel || null,
    likelyCompetitor: competitorHotel || null,
    competitorVenueStatus: ledger.competitorVenueStatus,
    competitorVenueStatusLabel:
      COMPETITOR_VENUE_STATUS_LABEL[ledger.competitorVenueStatus] || null,
    targetHotelSourcingStatus,
    targetHotelSourcingStatusLabel:
      TARGET_HOTEL_SOURCING_STATUS_LABEL[targetHotelSourcingStatus] || null,
    venueSourcingStatus:
      rem.venueSourcingStatus ||
      `TARGET_${targetHotelSourcingStatus}`,
    venueSourcingStatusLabel:
      TARGET_HOTEL_SOURCING_STATUS_LABEL[targetHotelSourcingStatus] || null,
    hotelSourcingStatusLabel:
      TARGET_HOTEL_SOURCING_STATUS_LABEL[targetHotelSourcingStatus] || null,
    hotelSourcingStatusNormalized: targetHotelSourcingStatus,
    commercialActionability: ledger.commercialActionability,
    advertisedAttendance:
      ledger.advertisedAttendance || opp.advertisedAttendance || null,
    attendance:
      ledger.advertisedAttendance || opp.attendance || opp.estimatedAttendance,
    recommendedAction,
    suggestedAction: recommendedAction,
    cmiEvidenceLedger: {
      version: CMI_EVIDENCE_LEDGER_V1,
      verified: cmi.verified,
      claims: ledger.claims,
      demotionReason: ledger.demotionReason || null,
      priorIncorrectCompetitorHotel:
        ledger.priorIncorrectCompetitorHotel || null,
    },
    commercialConsistencyPrior: prior,
    commercialConsistencyRemediatedAt: ledger.verifiedAt || null,
    contactRoleNote: rem.contactRoleNote || opp.contactRoleNote || null,
  };
}
