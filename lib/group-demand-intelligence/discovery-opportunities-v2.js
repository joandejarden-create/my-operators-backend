/**
 * Association discovery candidates from Webhound session
 * ab352b86-4c20-47b5-86ec-11b6fae181a1 ($5).
 * Only include specific named meetings with dates + hotel/venue gap.
 * Cap-Hill advocacy geography is discounted for Bethesda Marriott fit.
 */

import { buildOpportunity } from "./opportunity-factory.js";
import { CLAIM_KIND, DEMAND_STATUS, BOOKING_WINDOW } from "./claim-types.js";
import { PILOT_HOTEL_ID } from "./hotel-profile.js";

const ACCESS = "2026-09-12";
const WH = "ab352b86-4c20-47b5-86ec-11b6fae181a1";

function ev(field, value, claimKind, sourceUrl, sourceTitle, extra = {}) {
  let domain = null;
  try {
    domain = sourceUrl ? new URL(sourceUrl).hostname : null;
  } catch {
    domain = null;
  }
  return {
    field,
    value,
    claimKind,
    sourceUrl,
    sourceTitle,
    sourceDomain: domain,
    sourceType: extra.sourceType || "official_web",
    sourceAuthority: extra.sourceAuthority || "Tier_A",
    accessDate: ACCESS,
    extractedText: extra.extractedText || null,
    confidence: extra.confidence ?? 78,
    researchMethodId: extra.researchMethodId || "GDI-ASSOC-EVENT-01",
    researchProvider: extra.researchProvider || "webhound",
    researchLevel: extra.researchLevel || "L5_WEBHOUND",
    webhoundSessionId: WH,
  };
}

/**
 * @returns {object[]}
 */
export function buildAssociationDiscoveryOpportunitiesV2() {
  const hotelId = PILOT_HOTEL_ID;
  const out = [];

  // ACTS Translational Science 2027 — strongest NIH-adjacent discovery
  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_acts_ts27",
      organizationName: "Association for Clinical and Translational Science (ACTS)",
      title: "ACTS Translational Science 2027 (TS27) — Washington, DC (hotel/venue not named)",
      segment: "Medical / Scientific",
      demandType: "annual_conference",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2027-04-20",
      eventEndDate: "2027-04-23",
      destinationStatus: "Washington, DC confirmed",
      venueStatus: "Hotel/venue not listed publicly",
      estimatedAttendance: "500-800",
      estimatedAttendanceClaimKind: CLAIM_KIND.ESTIMATED,
      estimatedPeakRooms: "150-250",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
      whyNow:
        "CONTACT NOW: DC confirmed for Apr 20–23, 2027 with no public hotel. Strong CTSA/NCATS/NIH audience overlap. Recurring DC pattern (2025, 2023, 2019).",
      fitExplanation:
        "Upper-upscale full-service fit for research/academic medicine. Bethesda Marriott is plausible for NIH-corridor attendees even when sessions are downtown (INFERENCE). Peak rooms estimated 150–250 — inside target band.",
      summaryWhat: "ACTS TS27 in Washington, DC April 20–23, 2027; venue/hotel not announced.",
      summaryWhyMatters: "Named future medical research meeting with hotel gap and NIH-adjacent audience.",
      summaryWhyHotel: "Full-service Marriott near NIH/Bethesda research corridor; room-block size estimated in band.",
      recommendedAction:
        "Contact ACTS meetings staff via actscience.org this week. Ask whether hotel RFP is open and whether a Bethesda/Rockville overflow or headquarters hotel is under consideration alongside downtown DC.",
      primaryContact: {
        name: "ACTS meetings / Translational Science staff",
        role: "Conference organizers",
        organization: "ACTS",
        email: null,
        claimKind: CLAIM_KIND.INFERENCE,
        sourceUrl: "https://www.actscience.org/Translational-Science",
        confidence: 55,
      },
      likelyCompetitor: "Downtown DC convention hotels; prior DC hosts",
      meetingHistory: [
        { year: 2025, city: "Washington, DC", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT },
        { year: 2023, city: "Washington, DC", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT },
      ],
      evidence: [
        ev(
          "dates_city",
          "April 20–23, 2027 — Washington, DC",
          CLAIM_KIND.FACT,
          "https://www.actscience.org/Translational-Science",
          "ACTS Translational Science"
        ),
        ev(
          "hotel_status",
          "No venue/hotel listed on upcoming meetings page",
          CLAIM_KIND.FACT,
          "https://www.actscience.org/Translational-Science/Upcoming-Past-Meetings",
          "ACTS upcoming/past meetings"
        ),
        ev(
          "peak_rooms_estimate",
          "150–250 peak rooms (from historical ~500–800 attendance)",
          CLAIM_KIND.ESTIMATED,
          "https://www.actscience.org/Translational-Science",
          "ACTS / Webhound estimate",
          { confidence: 55 }
        ),
      ],
      fitComponents: {
        physicalFit: 82,
        geographyFit: 78,
        timing: 84,
        commercialValue: 80,
        historicalFit: 72,
        competitiveAccessibility: 68,
        contactability: 58,
      },
      confidenceInput: {
        sourceAuthority: 88,
        independentSourceCount: 2,
        directness: 85,
        recency: 90,
        verifiedFieldRatio: 0.55,
        firstPartyShare: 85,
        completeness: 58,
        conflictPenalty: 0,
      },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01", "GDI-MED-SCI-01"],
      webhoundUsed: true,
      labels: ["discovery_v2", "nih_adjacent", "new_this_week", "entering_booking_window"],
    })
  );

  // AHIMA Advocacy Summit 2027 — DC, hotel TBA; Cap Hill tilt → Medium
  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_ahima_advocacy_2027",
      organizationName: "AHIMA",
      title: "AHIMA Advocacy Summit 2027 — Washington, DC (hotel not specified)",
      segment: "Healthcare",
      demandType: "advocacy_summit",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2027-03-15",
      eventEndDate: "2027-03-16",
      destinationStatus: "Washington, DC",
      venueStatus: "Hotel not specified; 2026 used Hilton Washington DC National Mall",
      estimatedPeakRooms: "100-200",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
      whyNow:
        "CONTACT NOW: 2027 dates and DC city announced; lodging not named. Health-information policy audience with federal healthcare adjacency.",
      fitExplanation:
        "Size fits upper-upscale inventory. Geography is Cap-Hill oriented (2026 Hilton National Mall) — Bethesda is a secondary/overflow pitch unless planners want Metro-accessible NIH-corridor lodging (INFERENCE).",
      summaryWhat: "AHIMA Advocacy Summit March 15–16, 2027 in Washington, DC; hotel TBA.",
      summaryWhyMatters: "Named advocacy meeting with lodging gap in the 100–200 room band.",
      summaryWhyHotel: "Possible recommended/overflow hotel if downtown inventory is tight.",
      recommendedAction:
        "Email AHIMA meetings/advocacy staff offering Bethesda Marriott as recommended lodging or overflow with Metro guidance. Do not claim to displace Hilton National Mall without evidence of open RFP.",
      likelyCompetitor: "Hilton Washington DC National Mall (2026 host pattern)",
      evidence: [
        ev(
          "2027_city_dates",
          "Washington, DC — March 15–16, 2027",
          CLAIM_KIND.FACT,
          "https://www.ahima.org/education-events/events/2026-advocacy-summit/",
          "AHIMA advocacy events (2027 referenced via discovery)",
          { confidence: 70 }
        ),
        ev(
          "2026_hotel",
          "Hilton Washington DC National Mall (2026)",
          CLAIM_KIND.FACT,
          "https://www.ahima.org/education-events/events/2026-advocacy-summit/",
          "AHIMA 2026 Advocacy Summit",
          { confidence: 75 }
        ),
      ],
      fitComponents: {
        physicalFit: 78,
        geographyFit: 55,
        timing: 80,
        commercialValue: 70,
        historicalFit: 48,
        competitiveAccessibility: 55,
        contactability: 50,
      },
      confidenceInput: {
        sourceAuthority: 80,
        independentSourceCount: 1,
        directness: 75,
        recency: 85,
        verifiedFieldRatio: 0.45,
        firstPartyShare: 70,
        completeness: 50,
        conflictPenalty: 0,
      },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01"],
      webhoundUsed: true,
      labels: ["discovery_v2", "advocacy", "new_this_week"],
    })
  );

  // NDSS 2027 — explicit "check back for hotel"
  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_ndss_advocacy_2027",
      organizationName: "National Down Syndrome Society (NDSS)",
      title: "NDSS Down Syndrome Advocacy Conference 2027 — hotel block TBA",
      segment: "Healthcare / Advocacy",
      demandType: "advocacy_conference",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2027-04-19",
      eventEndDate: "2027-04-21",
      destinationStatus: "Washington, DC (Hill Day pattern)",
      venueStatus: "Official FAQ: check back for 2027 hotel room-block information",
      estimatedPeakRooms: "150-300",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
      whyNow:
        "CONTACT NOW: NDSS explicitly says hotel room-block details for 2027 are forthcoming. 2026 used Omni Shoreham.",
      fitExplanation:
        "Banquet/full-service pattern at Omni Shoreham supports upper-upscale. Bethesda is secondary to downtown for Hill Day unless overflow or family lodging package is needed (INFERENCE).",
      summaryWhat: "NDSS Advocacy Conference April 19–21, 2027; hotel block not yet published.",
      summaryWhyMatters: "Public signal that lodging is still open; NIH-funded Down syndrome research adjacency.",
      summaryWhyHotel: "Possible overflow / family lodging if Omni pattern continues downtown.",
      recommendedAction:
        "Contact NDSS conference housing team now requesting inclusion on recommended lodging list or overflow block proposal for Bethesda Marriott.",
      likelyCompetitor: "Omni Shoreham (2026)",
      evidence: [
        ev(
          "hotel_tba",
          "Check back for 2027 hotel room-block information",
          CLAIM_KIND.FACT,
          "https://ndss.org/faq-ndss-down-syndrome-advocacy-conference",
          "NDSS FAQ"
        ),
        ev(
          "dates",
          "April 19–21, 2027",
          CLAIM_KIND.FACT,
          "https://ndss.org/faq-ndss-down-syndrome-advocacy-conference",
          "NDSS FAQ",
          { confidence: 80 }
        ),
      ],
      fitComponents: {
        physicalFit: 80,
        geographyFit: 58,
        timing: 82,
        commercialValue: 72,
        historicalFit: 55,
        competitiveAccessibility: 60,
        contactability: 62,
      },
      confidenceInput: {
        sourceAuthority: 85,
        independentSourceCount: 1,
        directness: 88,
        recency: 90,
        verifiedFieldRatio: 0.5,
        firstPartyShare: 90,
        completeness: 52,
        conflictPenalty: 0,
      },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01"],
      webhoundUsed: true,
      labels: ["discovery_v2", "advocacy", "new_this_week", "entering_booking_window"],
    })
  );

  // ACC Legislative Conference 2027 — Cap Hill heavy → Medium with geo discount
  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_acc_legislative_2027",
      organizationName: "American College of Cardiology (ACC)",
      title: "ACC Legislative Conference 2027 — Washington, DC (hotel not named)",
      segment: "Medical / Advocacy",
      demandType: "legislative_conference",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2027-10-24",
      eventEndDate: "2027-10-26",
      destinationStatus: "Washington, DC",
      venueStatus: "City only; prior Hyatt Regency Capitol Hill pattern",
      estimatedAttendance: "450+",
      estimatedAttendanceClaimKind: CLAIM_KIND.ESTIMATED,
      estimatedPeakRooms: "200-300",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      whyNow:
        "WATCH / early CONTACT: Oct 2027 DC dates public with no hotel named. Cap-Hill advocacy format historically downtown — Bethesda is overflow/secondary unless RFP opens broadly.",
      fitExplanation:
        "Room-band fit is good; geography favors Capitol Hill hotels. Bethesda Marriott is not the primary host candidate without overflow evidence (INFERENCE).",
      summaryWhat: "ACC Legislative Conference Oct 24–26, 2027 in Washington, DC; hotel TBA.",
      summaryWhyMatters: "Large medical advocacy meeting; lodging gap exists but downtown bias is strong.",
      summaryWhyHotel: "Overflow or Metro-corridor lodging only unless planners seek Bethesda.",
      recommendedAction:
        "Monitor ACC housing RFP. Soft outreach as overflow/recommended lodging — do not position as primary Cap-Hill host without evidence.",
      likelyCompetitor: "Hyatt Regency Washington on Capitol Hill",
      evidence: [
        ev(
          "dates",
          "October 24–26, 2027 — Washington, DC",
          CLAIM_KIND.FACT,
          "https://www.acc.org/tools-and-practice-support/advocacy-at-the-acc/acc-legislative-conference",
          "ACC Legislative Conference",
          { confidence: 75 }
        ),
      ],
      fitComponents: {
        physicalFit: 78,
        geographyFit: 42,
        timing: 70,
        commercialValue: 75,
        historicalFit: 40,
        competitiveAccessibility: 45,
        contactability: 48,
      },
      confidenceInput: {
        sourceAuthority: 82,
        independentSourceCount: 1,
        directness: 70,
        recency: 85,
        verifiedFieldRatio: 0.4,
        firstPartyShare: 75,
        completeness: 45,
        conflictPenalty: 5,
      },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01"],
      webhoundUsed: true,
      labels: ["discovery_v2", "advocacy", "cap_hill_geo_risk"],
    })
  );

  // CMSS Spring 2027 — small leadership → Watchlist
  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_cmss_spring_2027",
      organizationName: "Council of Medical Specialty Societies (CMSS)",
      title: "CMSS Spring Meeting 2027 — hotel info forthcoming (small leadership)",
      segment: "Medical / Leadership",
      demandType: "leadership_meeting",
      demandStatus: DEMAND_STATUS.SPECULATIVE_WATCH,
      eventStartDate: "2027-03-30",
      eventEndDate: "2027-04-02",
      destinationStatus: "DC area (HQ Washington)",
      venueStatus: "Hotel information to be shared closer to event; 2026 at ASCO HQ Alexandria",
      estimatedPeakRooms: "50-100",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      whyNow: "WATCH: Small senior meeting; hotel TBD. Below primary 100–300 band but high rate quality.",
      fitExplanation: "Excellent for full-service executive inventory if they choose a hotel vs campus HQ.",
      summaryWhat: "CMSS Spring Meeting Mar 30–Apr 2, 2027; lodging TBA.",
      summaryWhyMatters: "Medical society CEO audience; relationship value if hotel-hosted.",
      summaryWhyHotel: "Fits executive meetings if planners leave ASCO HQ pattern.",
      recommendedAction: "Light outreach to CMSS meetings contact offering Bethesda Marriott for 2027 if hotel-hosted.",
      likelyCompetitor: "ASCO HQ Alexandria / downtown boutique hotels",
      evidence: [
        ev(
          "dates",
          "March 30 – April 2, 2027",
          CLAIM_KIND.FACT,
          "https://cmss.org/event/cmss-spring-meeting-2027/",
          "CMSS Spring Meeting 2027"
        ),
      ],
      fitComponents: {
        physicalFit: 85,
        geographyFit: 70,
        timing: 55,
        commercialValue: 48,
        historicalFit: 45,
        competitiveAccessibility: 60,
        contactability: 45,
      },
      confidenceInput: {
        sourceAuthority: 85,
        independentSourceCount: 1,
        directness: 80,
        recency: 88,
        verifiedFieldRatio: 0.45,
        firstPartyShare: 85,
        completeness: 48,
        conflictPenalty: 0,
      },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01"],
      webhoundUsed: true,
      labels: ["discovery_v2", "leadership", "small_block"],
    })
  );

  // Corporate public-source finding — update is applied in deepen-pass; no fabricated corp leads.

  return out;
}

export const DISCOVERY_WEBHOUND_SESSION_ID = WH;
export const DEEPEN_WEBHOUND_SESSION_ID = "1716a70c-1e4c-45d9-b905-525c4c97a933";
export const WAVE1_WEBHOUND_SESSION_ID = "4f99b00b-ca62-44af-b45c-3af61a6d325d";
