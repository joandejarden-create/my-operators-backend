/**
 * DMV Expansion Pass — L3 standard-web discovery outside Bethesda-centric sources.
 * $0 Webhound. Does not invent weak opportunities. Does not touch ADP.
 *
 * Research allocation targets (search effort, not final quotas):
 * CORE / NORTH_DC context retained from prior set; this pass focuses discovery
 * on DMV Competitive + DMV Stretch source pools.
 */

import { buildOpportunity } from "./opportunity-factory.js";
import {
  BOOKING_WINDOW,
  CLAIM_KIND,
  COMPETITOR_CLASS,
  DEMAND_STATUS,
  DEMAND_TERRITORY_FIT,
  PRIORITY,
} from "./claim-types.js";
import { PILOT_HOTEL_ID } from "./hotel-profile.js";

const ACCESS = "2026-09-13";
const METHOD = "GDI-DMV-EXPANSION-01";

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
    confidence: extra.confidence ?? 76,
    researchMethodId: METHOD,
    researchProvider: "standard_web",
    researchLevel: "L3_STANDARD_WEB",
  };
}

function marketComp(name, submarket, reason) {
  return {
    name,
    class: COMPETITOR_CLASS.OPPORTUNITY_MARKET,
    classLabel: "Opportunity Market Competitor",
    submarket,
    reason,
    historicalEventRelationship: null,
  };
}

/**
 * New qualified / watch opportunities discovered outside the prior Bethesda-centric set.
 * @returns {object[]}
 */
export function buildDmvExpansionOpportunities() {
  const hotelId = PILOT_HOTEL_ID;
  const out = [];

  // --- DMV COMPETITIVE ---
  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_nar_gad_institute_2027",
      organizationName: "National Association of REALTORS® (NAR)",
      title: "NAR GAD Institute 2027 — Arlington, VA (hotel/venue not named)",
      segment: "Association / Advocacy",
      demandType: "annual_institute",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2027-07-27",
      eventEndDate: "2027-07-29",
      destinationStatus: "Arlington, Virginia",
      venueStatus: "City announced; hotel/venue not named on NAR event page",
      estimatedAttendance: "250-450",
      estimatedAttendanceClaimKind: CLAIM_KIND.ESTIMATED,
      estimatedPeakRooms: "120-220",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_COMPETITIVE,
      demandTerritoryFitLocked: true,
      demandTerritoryRationale:
        "Northern Virginia city announced without a contracted host hotel — Bethesda is not obvious geography but is a credible full-service alternative for a multi-day association institute.",
      bethesdaWinThesis:
        "Although marketed in Arlington, the host hotel is not named and NAR maintains a DC office. Bethesda Marriott can compete as a full-service suburban Marriott with parking, Red Line Metro access to Capitol Hill / downtown NAR offices, and likely lower total stay cost than Crystal City / Pentagon City peak rates.",
      whyNow:
        "CONTACT NOW: Dates and Arlington destination are public; hotel not named; registration listed for May 2027 — sourcing window is open now.",
      fitExplanation:
        "Full-service meeting + sleeping rooms in target band. Geography is Northern Virginia, not Montgomery — Hotel Fit depends on positioning as a DMV alternative, not a local Arlington hotel.",
      summaryWhat:
        "NAR Government Affairs Directors Institute, July 27–29, 2027, Arlington VA; hotel/venue TBD.",
      summaryWhyMatters:
        "Named future association institute with city confirmed and lodging unresolved — classic DMV Competitive pattern.",
      summaryWhyHotel:
        "Full-service Marriott with parking and Metro access for advocacy staff who need DC access without requiring Crystal City proximity.",
      recommendedAction:
        "Email GADInst@nar.realtor / Jami Sims (202-383-1221) this week. Ask whether hotel RFP is open and whether a Bethesda / Montgomery full-service option is under consideration alongside Arlington / Crystal City properties.",
      primaryContact: {
        name: "Jami Sims",
        role: "NAR contact listed on GAD Institute page",
        organization: "National Association of REALTORS®",
        email: "GADInst@nar.realtor",
        phone: "202-383-1221",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://www.nar.realtor/events/2027-gad-institute",
        confidence: 88,
      },
      likelyCompetitor: "Crystal Gateway Marriott; Hilton Arlington; Ritz-Carlton Pentagon City",
      likelyStrCompetitor: "Hyatt Regency Bethesda",
      likelyGroupCompetitor: "Bethesda North Marriott Hotel & Conference Center",
      competitorRationale:
        "Primary market competitors are Arlington / Crystal City / Pentagon City hotels. STR set remains Bethesda-local context; group alternatives remain Marriott-area.",
      marketCompetitors: [
        marketComp(
          "Crystal Gateway Marriott",
          "Crystal City / Arlington",
          "Default Northern Virginia association host pattern"
        ),
        marketComp(
          "Hilton Arlington",
          "Ballston / Arlington",
          "Metro-adjacent full-service used by other 2027 Arlington conferences"
        ),
        marketComp(
          "Ritz-Carlton, Pentagon City",
          "Pentagon City",
          "Premium Arlington legislative / association host"
        ),
      ],
      fitComponents: {
        physicalFit: 78,
        geographyFit: 62,
        timing: 88,
        commercialValue: 74,
        historicalFit: 48,
        competitiveAccessibility: 58,
        contactability: 86,
      },
      confidenceInput: {
        sourceAuthority: 90,
        independentSourceCount: 2,
        directness: 85,
        recency: 95,
        verifiedFieldRatio: 0.7,
        firstPartyShare: 0.9,
        completeness: 72,
        conflictPenalty: 0,
      },
      evidence: [
        ev(
          "dates_city",
          "July 27–29, 2027 — Arlington, Virginia",
          CLAIM_KIND.FACT,
          "https://www.nar.realtor/events/2027-gad-institute",
          "NAR 2027 GAD Institute"
        ),
        ev(
          "hotel_status",
          "Location listed as Arlington, VA only — no hotel named",
          CLAIM_KIND.FACT,
          "https://www.nar.realtor/events/2027-gad-institute",
          "NAR 2027 GAD Institute"
        ),
        ev(
          "contact",
          "GADInst@nar.realtor · Jami Sims · 202-383-1221",
          CLAIM_KIND.FACT,
          "https://www.nar.realtor/events/2027-gad-institute",
          "NAR 2027 GAD Institute"
        ),
      ],
      labels: ["dmv_expansion_v1", "dmv_competitive"],
    })
  );

  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_nado_ddaa_washcon_2028",
      organizationName: "National Association of Development Organizations (NADO) / DDAA",
      title: "NADO & DDAA Washington Conference 2028 — Arlington, VA (hotel not yet named)",
      segment: "Association / Government",
      demandType: "policy_conference",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2028-03-19",
      eventEndDate: "2028-03-22",
      destinationStatus: "Arlington, Virginia",
      venueStatus: "City announced; 2028 hotel not yet published (2026/2027 pattern = Crystal Gateway Marriott)",
      estimatedAttendance: "300-500",
      estimatedAttendanceClaimKind: CLAIM_KIND.ESTIMATED,
      estimatedPeakRooms: "150-250",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_COMPETITIVE,
      demandTerritoryFitLocked: true,
      demandTerritoryRationale:
        "Recurring Arlington policy conference with hotel not yet named for 2028 — Bethesda can compete as overflow/alternative given documented Crystal City sell-outs.",
      bethesdaWinThesis:
        "NADO’s Arlington Washington Conference historically uses Crystal Gateway Marriott, and the 2026 block sold out with no official overflow hotel. For 2028 (hotel not yet named), Bethesda Marriott can compete as a full-service Metro-accessible overflow or alternative when Crystal City inventory/rate compresses.",
      whyNow:
        "QUALIFY NOW: 2028 dates and Arlington city are public while hotel is not yet locked — early engagement before Crystal City default re-contracts.",
      fitExplanation:
        "Peak rooms estimated in band. Win path is alternative/overflow positioning vs Crystal City, not pretending Bethesda is the natural Arlington HQ hotel.",
      summaryWhat:
        "NADO & DDAA Washington Conference March 19–22, 2028 in Arlington; hotel TBA.",
      summaryWhyMatters:
        "Recurring federal/regional development conference with a documented sold-out host pattern — lodging pressure creates Competitive opportunity.",
      summaryWhyHotel:
        "Full-service Marriott with parking and Metro for Hill Day attendees when Crystal City is full or expensive.",
      recommendedAction:
        "Contact NADO meetings (info@nado.org / Jamie McCormick pattern from 2026 sold-out notice). Ask whether 2028 hotel selection is open and whether an overflow Marriott in Bethesda/Rockville would be considered.",
      primaryContact: {
        name: "NADO conference staff",
        role: "Meetings / housing",
        organization: "NADO",
        email: "info@nado.org",
        claimKind: CLAIM_KIND.INFERENCE,
        sourceUrl: "https://www.nado.org/nado-event-categories/conference/",
        confidence: 60,
      },
      likelyCompetitor: "Crystal Gateway Marriott",
      marketCompetitors: [
        marketComp(
          "Crystal Gateway Marriott",
          "Crystal City / Arlington",
          "Historical NADO Washington Conference host; 2026 block sold out"
        ),
        marketComp(
          "Hilton Arlington",
          "Ballston / Arlington",
          "Nearby Northern Virginia full-service alternative"
        ),
      ],
      fitComponents: {
        physicalFit: 76,
        geographyFit: 58,
        timing: 70,
        commercialValue: 72,
        historicalFit: 55,
        competitiveAccessibility: 52,
        contactability: 62,
      },
      confidenceInput: {
        sourceAuthority: 88,
        independentSourceCount: 3,
        directness: 70,
        recency: 90,
        verifiedFieldRatio: 0.65,
        firstPartyShare: 0.85,
        completeness: 68,
        conflictPenalty: 5,
      },
      evidence: [
        ev(
          "dates_city",
          "March 19–22, 2028 — Arlington, VA",
          CLAIM_KIND.FACT,
          "https://www.nado.org/nado-event-categories/conference/",
          "NADO Conference Archives"
        ),
        ev(
          "historical_host",
          "2026 and 2027 Washington Conferences listed at Crystal Gateway Marriott",
          CLAIM_KIND.FACT,
          "https://www.nado.org/2027washcon/",
          "NADO 2027 Washington Conference"
        ),
        ev(
          "sold_out_pattern",
          "2026 Crystal Gateway Marriott group block sold out; no official overflow hotel",
          CLAIM_KIND.FACT,
          "https://www.nado.org/2026washcon/",
          "NADO 2026 Washington Conference accommodations"
        ),
      ],
      labels: ["dmv_expansion_v1", "dmv_competitive"],
    })
  );

  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_bebpa_usb_2027",
      organizationName: "BioPharmaceutical Emerging Best Practices Association (BEBPA)",
      title: "BEBPA US Bioassay Conference 2027 — College Park, MD (room block coming soon)",
      segment: "Medical / Scientific",
      demandType: "scientific_conference",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2027-03-08",
      eventEndDate: "2027-03-10",
      destinationStatus: "College Park, Maryland (Prince George's County)",
      venueStatus:
        "College Park Marriott Hotel & Conference Center named as home base; rooming block info coming soon",
      estimatedAttendance: "150-300",
      estimatedAttendanceClaimKind: CLAIM_KIND.ESTIMATED,
      estimatedPeakRooms: "80-160",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_COMPETITIVE,
      demandTerritoryFitLocked: true,
      demandTerritoryRationale:
        "Prince George's / College Park scientific meeting — not Montgomery Core, but Maryland suburban biotech demand where Bethesda can compete for overflow and for attendees tied to FDA/NIH.",
      bethesdaWinThesis:
        "Sessions are anchored at College Park Marriott, but the room block is not yet published and the audience includes FDA / USP / biopharma scientists. Bethesda Marriott can compete for overflow, preferred full-service lodging, and NIH/FDA-corridor attendees who prefer Bethesda/Rockville over College Park.",
      whyNow:
        "QUALIFY NOW: Dates confirmed; rooming block 'coming soon' — engage before the College Park Marriott block fills and before overflow is assigned elsewhere.",
      fitExplanation:
        "Scientific mid-size meeting in PG County. Primary host is College Park Marriott; Bethesda win path is overflow / preferred lodging, not stealing the contracted meeting space.",
      summaryWhat:
        "BEBPA US Bioassay Conference March 8–10, 2027 in College Park; College Park Marriott home base; room block TBA.",
      summaryWhyMatters:
        "FDA-adjacent scientific conference in the Maryland suburbs with lodging still forming — Competitive overflow opportunity.",
      summaryWhyHotel:
        "Full-service Marriott near NIH/FDA patterns; stronger brand for some industry attendees than campus-adjacent options when overflow opens.",
      recommendedAction:
        "Contact contactus@bebpa.org to ask whether overflow housing will be needed and whether Bethesda/Rockville Marriott properties can be added to the housing list when the College Park block opens.",
      primaryContact: {
        name: "BEBPA office",
        role: "Conference organizers",
        organization: "BEBPA",
        email: "contactus@bebpa.org",
        phone: "206-651-4542",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://bebpa.org/2027-usb/",
        confidence: 82,
      },
      likelyCompetitor: "College Park Marriott Hotel & Conference Center",
      marketCompetitors: [
        marketComp(
          "College Park Marriott Hotel & Conference Center",
          "College Park / Prince George's",
          "Named home-base hotel; room block forthcoming"
        ),
        marketComp(
          "The Hotel at the University of Maryland",
          "College Park",
          "Campus full-service competitor for UMD-area meetings"
        ),
        marketComp(
          "Cambria Hotel College Park",
          "College Park",
          "Select-service College Park alternative"
        ),
      ],
      fitComponents: {
        physicalFit: 72,
        geographyFit: 64,
        timing: 80,
        commercialValue: 68,
        historicalFit: 50,
        competitiveAccessibility: 55,
        contactability: 78,
      },
      confidenceInput: {
        sourceAuthority: 92,
        independentSourceCount: 1,
        directness: 88,
        recency: 95,
        verifiedFieldRatio: 0.75,
        firstPartyShare: 1,
        completeness: 74,
        conflictPenalty: 8,
      },
      evidence: [
        ev(
          "dates_city",
          "March 8–10, 2027 — College Park, Maryland",
          CLAIM_KIND.FACT,
          "https://bebpa.org/2027-usb/",
          "BEBPA 2027 USB"
        ),
        ev(
          "hotel_status",
          "College Park Marriott named; rooming block info coming soon",
          CLAIM_KIND.FACT,
          "https://bebpa.org/2027-usb/",
          "BEBPA 2027 USB hotel information"
        ),
        ev(
          "audience",
          "Participating companies include U.S. FDA and US Pharmacopeia",
          CLAIM_KIND.FACT,
          "https://bebpa.org/2027-usb/",
          "BEBPA 2027 USB"
        ),
      ],
      labels: ["dmv_expansion_v1", "dmv_competitive"],
    })
  );

  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_alexandria_soccer_kickoff_2027",
      organizationName: "Alexandria Soccer Association",
      title: "Alexandria Soccer Kickoff 2027 — stay-to-play housing (Traveling Teams)",
      segment: "Sports / Weekend Group",
      demandType: "sports_tournament_housing",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2027-03-06",
      eventEndDate: "2027-03-21",
      destinationStatus: "Alexandria, Virginia (fields across city / surrounding communities)",
      venueStatus: "Fields-based; hotel reservations required through Traveling Teams (stay-to-play)",
      estimatedAttendance: "UNKNOWN",
      estimatedAttendanceClaimKind: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: "80-200",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_COMPETITIVE,
      demandTerritoryFitLocked: true,
      demandTerritoryRationale:
        "Northern Virginia tournament housing distributed via housing partner — Bethesda can compete for team blocks that prefer Maryland / Beltway lodging and parking.",
      bethesdaWinThesis:
        "Games are in Alexandria, but lodging is managed through a stay-to-play partner rather than a single contracted host. Teams from Maryland / Mid-Atlantic often prefer Beltway full-service hotels with parking and motorcoach access; Bethesda Marriott can bid into the Traveling Teams inventory as a Maryland-side option.",
      whyNow:
        "QUALIFY NOW: 2027 tournament weekends posted; housing is stay-to-play through Traveling Teams — ask to be added to the hotel list before inventory locks.",
      fitExplanation:
        "Weekend sports housing fit for full-service inventory. Geography is Alexandria fields; win path is housing-list inclusion, not host-hotel status.",
      summaryWhat:
        "Alexandria Soccer Kickoff March 2027 weekends; stay-to-play via Traveling Teams.",
      summaryWhyMatters:
        "Named regional tournament with mandatory partner housing — Competitive lodging inventory opportunity outside Montgomery fields.",
      summaryWhyHotel:
        "Full-service parking + motorcoach-friendly inventory for traveling teams.",
      recommendedAction:
        "Contact Alexandria Soccer Association / Traveling Teams housing desk to request Bethesda Marriott inclusion on the 2027 hotel list for full-sided and small-sided weekends.",
      primaryContact: {
        name: "Alexandria Soccer Association tournaments",
        role: "Tournament / housing",
        organization: "Alexandria Soccer Association",
        email: null,
        claimKind: CLAIM_KIND.INFERENCE,
        sourceUrl: "https://alexandria-soccer.org/tournaments/alexandria-soccer-kickoff/",
        confidence: 55,
      },
      likelyCompetitor: "Alexandria / Crystal City / National Harbor hotels in Traveling Teams list",
      marketCompetitors: [
        marketComp(
          "Alexandria Old Town hotels",
          "Alexandria",
          "Closest lodging to many tournament fields"
        ),
        marketComp(
          "Crystal City hotels",
          "Arlington / Crystal City",
          "Metro-adjacent Northern Virginia inventory"
        ),
        marketComp(
          "National Harbor hotels",
          "National Harbor / Prince George's",
          "Beltway sports-housing alternative cluster"
        ),
      ],
      fitComponents: {
        physicalFit: 70,
        geographyFit: 55,
        timing: 78,
        commercialValue: 66,
        historicalFit: 40,
        competitiveAccessibility: 50,
        contactability: 58,
      },
      confidenceInput: {
        sourceAuthority: 88,
        independentSourceCount: 2,
        directness: 80,
        recency: 90,
        verifiedFieldRatio: 0.6,
        firstPartyShare: 0.9,
        completeness: 62,
        conflictPenalty: 5,
      },
      evidence: [
        ev(
          "dates",
          "March 6–7 and March 20–21, 2027 weekends listed",
          CLAIM_KIND.FACT,
          "https://alexandria-soccer.org/tournaments/alexandria-soccer-kickoff/",
          "Alexandria Soccer Kickoff"
        ),
        ev(
          "housing",
          "Stay-to-play — reservations must be made through Traveling Teams",
          CLAIM_KIND.FACT,
          "https://alexandria-soccer.org/tournaments/alexandria-soccer-kickoff/",
          "Alexandria Soccer Kickoff hotel info"
        ),
      ],
      labels: ["dmv_expansion_v1", "dmv_competitive", "sports"],
    })
  );

  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_arlington_spring_tournament_2027",
      organizationName: "Arlington Soccer Association",
      title: "Arlington Spring Tournament 2027 — hotel information forthcoming",
      segment: "Sports / Weekend Group",
      demandType: "sports_tournament_housing",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2027-02-26",
      eventEndDate: "2027-03-07",
      destinationStatus: "Arlington, Virginia",
      venueStatus: "All-weather turf fields; hotel details 'more information to come'",
      estimatedPeakRooms: "60-180",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_COMPETITIVE,
      demandTerritoryFitLocked: true,
      demandTerritoryRationale:
        "Arlington tournament with lodging not yet published — Competitive for Beltway hotels once housing opens.",
      bethesdaWinThesis:
        "Arlington fields are the game venue, but hotel information is still forthcoming. Visiting Mid-Atlantic teams often accept Beltway lodging with parking; Bethesda can compete for a Maryland-side room block once housing partners are selected.",
      whyNow:
        "QUALIFY NOW: 2027 girls/boys weekends posted and lodging still open — engage tournaments@arlingtonsoccer.com before a Crystal City–only list locks.",
      fitExplanation:
        "Sports housing opportunity; geography is Arlington; win path is early housing-list inclusion.",
      summaryWhat:
        "Arlington Spring Tournament late Feb / early March 2027; hotel details forthcoming.",
      summaryWhyMatters:
        "Named Northern Virginia tournament with unresolved lodging — Competitive inventory window.",
      summaryWhyHotel:
        "Full-service parking inventory for traveling youth teams.",
      recommendedAction:
        "Email tournaments@arlingtonsoccer.com asking to be considered for the 2027 hotel partner list and whether Maryland Beltway properties are accepted.",
      primaryContact: {
        name: "Arlington Soccer tournaments desk",
        role: "Tournament director contact",
        organization: "Arlington Soccer Association",
        email: "tournaments@arlingtonsoccer.com",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl:
          "https://arlingtonsoccer.ottosport.ai/tournaments/arlington-spring-tournament-presented-by-orthovirginia/2027-arlington-spring-tournament-presented-by-orthovirginia",
        confidence: 80,
      },
      likelyCompetitor: "Arlington / Crystal City hotels",
      marketCompetitors: [
        marketComp(
          "Arlington hotels near fields",
          "Arlington",
          "Closest lodging once hotel list opens"
        ),
        marketComp(
          "Crystal City hotels",
          "Crystal City",
          "Common Northern Virginia tournament housing cluster"
        ),
      ],
      fitComponents: {
        physicalFit: 68,
        geographyFit: 52,
        timing: 82,
        commercialValue: 64,
        historicalFit: 35,
        competitiveAccessibility: 48,
        contactability: 75,
      },
      confidenceInput: {
        sourceAuthority: 85,
        independentSourceCount: 1,
        directness: 75,
        recency: 92,
        verifiedFieldRatio: 0.55,
        firstPartyShare: 1,
        completeness: 58,
        conflictPenalty: 5,
      },
      evidence: [
        ev(
          "dates",
          "Girls Feb 26–28, 2027; Boys March 5–7, 2027",
          CLAIM_KIND.FACT,
          "https://arlingtonsoccer.ottosport.ai/tournaments/arlington-spring-tournament-presented-by-orthovirginia/2027-arlington-spring-tournament-presented-by-orthovirginia",
          "Arlington Spring Tournament 2027"
        ),
        ev(
          "hotel_status",
          "More information to come — hotel details not yet published",
          CLAIM_KIND.FACT,
          "https://arlingtonsoccer.ottosport.ai/tournaments/arlington-spring-tournament-presented-by-orthovirginia/2027-arlington-spring-tournament-presented-by-orthovirginia",
          "Arlington Spring Tournament 2027"
        ),
      ],
      labels: ["dmv_expansion_v1", "dmv_competitive", "sports"],
    })
  );

  // --- DMV STRETCH ---
  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_nado_ddaa_washcon_2027_overflow",
      organizationName: "National Association of Development Organizations (NADO) / DDAA",
      title: "NADO & DDAA Washington Conference 2027 — Crystal Gateway host (overflow play)",
      segment: "Association / Government",
      demandType: "policy_conference_overflow",
      demandStatus: DEMAND_STATUS.KNOWN_DEMAND,
      eventStartDate: "2027-03-07",
      eventEndDate: "2027-03-10",
      destinationStatus: "Crystal Gateway Marriott, Arlington, VA",
      venueStatus: "Host hotel contracted — Crystal Gateway Marriott",
      estimatedPeakRooms: "40-120 overflow",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_STRETCH,
      demandTerritoryFitLocked: true,
      demandTerritoryRationale:
        "Host already contracted in Crystal City; Bethesda is stretch overflow only, justified by prior sold-out pattern.",
      bethesdaWinThesis:
        "The conference hotel is already Crystal Gateway Marriott, so Bethesda cannot win host status. It can still compete for overflow if the 2027 block fills like 2026, when NADO publicly stated there was no official overflow hotel.",
      whyNow:
        "QUALIFY NOW as overflow only: confirm 2027 block pace; offer Bethesda as named overflow if Crystal City compresses.",
      fitExplanation:
        "Stretch overflow — do not pursue as primary host.",
      summaryWhat:
        "NADO 2027 Washington Conference at Crystal Gateway Marriott; overflow opportunity if block fills.",
      summaryWhyMatters:
        "Documented sold-out history creates a Stretch overflow path without inventing a primary host bid.",
      summaryWhyHotel:
        "Metro-accessible full-service overflow when Crystal City sells out.",
      recommendedAction:
        "Ask NADO whether an official 2027 overflow hotel will be designated if Crystal Gateway fills; propose Bethesda Marriott as a Marriott-family overflow option.",
      primaryContact: {
        name: "NADO conference staff",
        role: "Meetings / housing",
        organization: "NADO",
        email: "info@nado.org",
        claimKind: CLAIM_KIND.INFERENCE,
        sourceUrl: "https://www.nado.org/2027washcon/",
        confidence: 55,
      },
      likelyCompetitor: "Crystal Gateway Marriott (host)",
      marketCompetitors: [
        marketComp(
          "Crystal Gateway Marriott",
          "Crystal City",
          "Contracted host hotel"
        ),
        marketComp(
          "Hilton Arlington",
          "Ballston",
          "Nearby overflow alternative"
        ),
      ],
      fitComponents: {
        physicalFit: 70,
        geographyFit: 42,
        timing: 75,
        commercialValue: 55,
        historicalFit: 60,
        competitiveAccessibility: 35,
        contactability: 60,
      },
      confidenceInput: {
        sourceAuthority: 90,
        independentSourceCount: 2,
        directness: 80,
        recency: 90,
        verifiedFieldRatio: 0.7,
        firstPartyShare: 0.9,
        completeness: 70,
        conflictPenalty: 10,
      },
      evidence: [
        ev(
          "host_hotel",
          "Crystal Gateway Marriott named for March 7–10, 2027",
          CLAIM_KIND.FACT,
          "https://www.nado.org/2027washcon/",
          "NADO 2027 Washington Conference"
        ),
        ev(
          "sold_out_precedent",
          "2026 Crystal Gateway block sold out with no official overflow hotel",
          CLAIM_KIND.FACT,
          "https://www.nado.org/2026washcon/",
          "NADO 2026 accommodations"
        ),
      ],
      labels: ["dmv_expansion_v1", "dmv_stretch", "overflow"],
    })
  );

  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_loudoun_college_showcase_2027",
      organizationName: "Loudoun Soccer",
      title: "Loudoun Soccer College Showcase 2027 — stay-to-play (HBC housing)",
      segment: "Sports / Weekend Group",
      demandType: "sports_tournament_housing",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2027-03-05",
      eventEndDate: "2027-03-07",
      destinationStatus: "Loudoun County, Northern Virginia",
      venueStatus: "Multiple Loudoun turf venues; stay-to-play via HBC Event Services; hotel deadline TBA",
      estimatedPeakRooms: "50-150",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_STRETCH,
      demandTerritoryFitLocked: true,
      demandTerritoryRationale:
        "Loudoun / Dulles corridor fields — stretch for Bethesda vs local Loudoun inventory, but housing partner model allows distant properties if teams accept drive time.",
      bethesdaWinThesis:
        "This is not a natural Bethesda event. It is Stretch only because stay-to-play housing is agency-managed and some Mid-Atlantic teams may accept a Beltway Marriott with parking if Loudoun inventory is tight — only pursue if HBC will list Maryland properties.",
      whyNow:
        "QUALIFY selectively: hotel booking link / deadline still TBA — ask HBC whether Maryland Beltway hotels are eligible.",
      fitExplanation:
        "Stretch sports housing — long drive to Loudoun fields; only pursue if housing partner confirms eligibility.",
      summaryWhat:
        "Loudoun College Showcase March 5–7, 2027; stay-to-play via HBC; hotel deadline TBA.",
      summaryWhyMatters:
        "Named Northern Virginia showcase with unresolved housing list — Stretch only.",
      summaryWhyHotel:
        "Only if agency accepts Beltway inventory; otherwise watch.",
      recommendedAction:
        "Contact HBC Event Services (505-346-0522 / support@hbceventservices.com) to ask whether Bethesda/Rockville hotels can be listed for non-commuting teams.",
      primaryContact: {
        name: "HBC Event Services",
        role: "Tournament housing partner",
        organization: "HBC Event Services",
        email: "support@hbceventservices.com",
        phone: "505-346-0522",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://www.loudounsoccer.com/college-showcase",
        confidence: 78,
      },
      likelyCompetitor: "Loudoun / Dulles corridor hotels",
      marketCompetitors: [
        marketComp(
          "Loudoun / Dulles hotels",
          "Reston / Dulles / Leesburg",
          "Natural lodging for Loudoun fields"
        ),
        marketComp(
          "Tysons hotels",
          "Tysons / McLean",
          "Closer Beltway alternative than Bethesda for many teams"
        ),
      ],
      fitComponents: {
        physicalFit: 65,
        geographyFit: 32,
        timing: 76,
        commercialValue: 50,
        historicalFit: 25,
        competitiveAccessibility: 30,
        contactability: 70,
      },
      confidenceInput: {
        sourceAuthority: 88,
        independentSourceCount: 1,
        directness: 78,
        recency: 90,
        verifiedFieldRatio: 0.6,
        firstPartyShare: 1,
        completeness: 60,
        conflictPenalty: 5,
      },
      evidence: [
        ev(
          "dates_housing",
          "March 5–7, 2027; stay-to-play via HBC; hotel deadline TBA",
          CLAIM_KIND.FACT,
          "https://www.loudounsoccer.com/college-showcase",
          "Loudoun Soccer College Showcase"
        ),
      ],
      labels: ["dmv_expansion_v1", "dmv_stretch", "sports"],
    })
  );

  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_aad_2028_overflow",
      organizationName: "American Academy of Dermatology (AAD)",
      title: "AAD Annual Meeting 2028 — Washington, DC (convention-scale overflow watch)",
      segment: "Medical / Scientific",
      demandType: "annual_meeting_overflow",
      demandStatus: DEMAND_STATUS.KNOWN_DEMAND,
      eventStartDate: "2028-03-17",
      eventEndDate: "2028-03-21",
      destinationStatus: "Washington, DC (Walter E. Washington Convention Center pattern)",
      venueStatus: "City confirmed on AAD locations page; multi-hotel downtown housing expected",
      estimatedPeakRooms: "100-300 overflow share",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_STRETCH,
      demandTerritoryFitLocked: true,
      demandTerritoryRationale:
        "Convention-scale downtown medical meeting — Stretch overflow only, not a host bid.",
      bethesdaWinThesis:
        "AAD 2028 will almost certainly use downtown convention housing. Bethesda cannot win host status, but medical attendees with NIH/suburban preferences and sold-out downtown blocks create a Stretch overflow path once official housing opens.",
      whyNow:
        "WATCH: City/dates confirmed; monitor official housing release before outbound.",
      fitExplanation:
        "Stretch overflow for a mega medical congress — quality watch, not primary pursuit.",
      summaryWhat:
        "AAD Annual Meeting March 17–21, 2028 in Washington, DC.",
      summaryWhyMatters:
        "Large medical congress returning to DC — Stretch overflow if downtown blocks tighten.",
      summaryWhyHotel:
        "Suburban medical/overflow lodging only.",
      recommendedAction:
        "Monitor AAD official housing; do not chase unofficial brokers. Revisit when housing list opens for overflow inclusion.",
      primaryContact: {
        name: "AAD meetings / housing (official channel TBD)",
        role: "Official housing provider (not yet named publicly for 2028)",
        organization: "American Academy of Dermatology",
        email: null,
        claimKind: CLAIM_KIND.UNKNOWN,
        sourceUrl: "https://www.aad.org/member/meetings/events/locations",
        confidence: 40,
      },
      likelyCompetitor: "Downtown DC convention hotels / Marriott Marquis cluster",
      marketCompetitors: [
        marketComp(
          "Marriott Marquis Washington, DC",
          "Downtown / Convention Center",
          "Typical WEWCC headquarter hotel"
        ),
        marketComp(
          "Westin Washington, DC Downtown",
          "Downtown / Convention Center",
          "Common WEWCC headquarter pair"
        ),
      ],
      fitComponents: {
        physicalFit: 60,
        geographyFit: 38,
        timing: 55,
        commercialValue: 58,
        historicalFit: 45,
        competitiveAccessibility: 28,
        contactability: 35,
      },
      confidenceInput: {
        sourceAuthority: 95,
        independentSourceCount: 2,
        directness: 70,
        recency: 90,
        verifiedFieldRatio: 0.5,
        firstPartyShare: 0.7,
        completeness: 55,
        conflictPenalty: 10,
      },
      evidence: [
        ev(
          "dates_city",
          "2028: Washington, D.C., March 17–21",
          CLAIM_KIND.FACT,
          "https://www.aad.org/member/meetings/events/locations",
          "AAD meeting locations and dates"
        ),
        ev(
          "venue_pattern",
          "Third-party directories list Walter E. Washington Convention Center for AAD 2028",
          CLAIM_KIND.ESTIMATED,
          "https://www.showsbee.com/fairs/97402-AAD-Annual-Meeting-2028.html",
          "Showsbee AAD 2028 listing"
        ),
      ],
      labels: ["dmv_expansion_v1", "dmv_stretch", "overflow"],
    })
  );

  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_ecs_251_2027_overflow",
      organizationName: "The Electrochemical Society (ECS)",
      title: "251st ECS Meeting 2027 — WEWCC + Marriott Marquis (overflow watch)",
      segment: "Medical / Scientific",
      demandType: "scientific_meeting_overflow",
      demandStatus: DEMAND_STATUS.KNOWN_DEMAND,
      eventStartDate: "2027-05-30",
      eventEndDate: "2027-06-03",
      destinationStatus: "Walter E. Washington Convention Center and Marriott Marquis, Washington, DC",
      venueStatus: "Convention center + Marriott Marquis named",
      estimatedPeakRooms: "80-200 overflow share",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_STRETCH,
      demandTerritoryFitLocked: true,
      demandTerritoryRationale:
        "Scientific society meeting already paired with downtown convention + Marquis — Stretch overflow only.",
      bethesdaWinThesis:
        "ECS has already named Marriott Marquis with the Convention Center, so Bethesda cannot win the headquarters hotel. Stretch value appears only if the Marquis block fills and scientific attendees accept a Metro Red Line suburban Marriott with parking.",
      whyNow:
        "WATCH until hotel registration opens (target window around May 2027 deadlines on ECS page).",
      fitExplanation:
        "Stretch overflow behind a named downtown Marriott headquarters hotel.",
      summaryWhat:
        "ECS 251st Meeting May 30–June 3, 2027 at WEWCC + Marriott Marquis.",
      summaryWhyMatters:
        "Large scientific meeting with named downtown HQ — Stretch overflow only.",
      summaryWhyHotel:
        "Overflow only if Marquis inventory constrains.",
      recommendedAction:
        "Monitor ECS housing open; pursue only if overflow list is requested. Do not bid against the named Marquis headquarters package.",
      primaryContact: {
        name: "ECS meetings",
        role: "Meeting organizers",
        organization: "The Electrochemical Society",
        email: null,
        claimKind: CLAIM_KIND.INFERENCE,
        sourceUrl: "https://www.electrochem.org/251/",
        confidence: 50,
      },
      likelyCompetitor: "Marriott Marquis Washington, DC",
      marketCompetitors: [
        marketComp(
          "Marriott Marquis Washington, DC",
          "Downtown / Convention Center",
          "Named headquarters hotel"
        ),
        marketComp(
          "Other downtown DC hotels",
          "Downtown DC",
          "Typical WEWCC overflow cluster"
        ),
      ],
      fitComponents: {
        physicalFit: 62,
        geographyFit: 36,
        timing: 50,
        commercialValue: 55,
        historicalFit: 40,
        competitiveAccessibility: 25,
        contactability: 40,
      },
      confidenceInput: {
        sourceAuthority: 95,
        independentSourceCount: 1,
        directness: 90,
        recency: 90,
        verifiedFieldRatio: 0.7,
        firstPartyShare: 1,
        completeness: 68,
        conflictPenalty: 12,
      },
      evidence: [
        ev(
          "dates_venue",
          "May 30–June 3, 2027 — Walter E. Washington Convention Center and Marriott Marquis",
          CLAIM_KIND.FACT,
          "https://www.electrochem.org/251/",
          "251st ECS Meeting"
        ),
      ],
      labels: ["dmv_expansion_v1", "dmv_stretch", "overflow"],
    })
  );

  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_asae_annual_2029",
      organizationName: "American Society of Association Executives (ASAE)",
      title: "ASAE Annual Meeting & Exposition 2029 — Washington, DC (early watch)",
      segment: "Association",
      demandType: "annual_meeting",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2029-08-11",
      eventEndDate: "2029-08-14",
      destinationStatus: "Washington, DC",
      venueStatus: "City secured via Destination DC; hotel package not yet detailed for 2029",
      estimatedPeakRooms: "200-400+",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.TOO_EARLY,
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_STRETCH,
      demandTerritoryFitLocked: true,
      demandTerritoryRationale:
        "Large association annual returning to DC — Stretch/early watch for suburban overflow and satellite meetings.",
      bethesdaWinThesis:
        "ASAE’s 2029 DC annual will center downtown/convention inventory. Bethesda’s Stretch path is early relationship for overflow, board/satellite meetings, or sold-out downtown packages — not headquarters contention.",
      whyNow:
        "TOO EARLY for aggressive outbound; capture as Stretch watch and monitor Destination DC / ASAE housing milestones.",
      fitExplanation:
        "Early Stretch watch — commercially real city award, lodging package years out.",
      summaryWhat:
        "ASAE Annual Meeting August 11–14, 2029 in Washington, DC.",
      summaryWhyMatters:
        "Major association annual secured for DC — long-lead Stretch overflow / satellite opportunity.",
      summaryWhyHotel:
        "Suburban overflow / satellite meetings only.",
      recommendedAction:
        "Watch Destination DC / ASAE housing announcements; do not spend sales time until hotel package process opens.",
      primaryContact: {
        name: "ASAE meetings",
        role: "Annual meeting organizers",
        organization: "ASAE",
        email: "ASAEservice@asaecenter.org",
        claimKind: CLAIM_KIND.INFERENCE,
        sourceUrl: "https://annual.asaecenter.org/about-annual/",
        confidence: 55,
      },
      likelyCompetitor: "Walter E. Washington Convention Center hotel package",
      marketCompetitors: [
        marketComp(
          "Downtown DC convention hotels",
          "Downtown DC",
          "Expected headquarters package"
        ),
        marketComp(
          "National Harbor / Crystal City overflow clusters",
          "Regional DMV",
          "Typical large-meeting overflow geography"
        ),
      ],
      fitComponents: {
        physicalFit: 58,
        geographyFit: 34,
        timing: 25,
        commercialValue: 60,
        historicalFit: 40,
        competitiveAccessibility: 22,
        contactability: 45,
      },
      confidenceInput: {
        sourceAuthority: 92,
        independentSourceCount: 2,
        directness: 75,
        recency: 85,
        verifiedFieldRatio: 0.55,
        firstPartyShare: 0.8,
        completeness: 50,
        conflictPenalty: 5,
      },
      evidence: [
        ev(
          "dates_city",
          "2029 Washington, DC August 11–14",
          CLAIM_KIND.FACT,
          "https://annual.asaecenter.org/about-annual/",
          "ASAE Annual Meeting locations"
        ),
        ev(
          "destination_dc",
          "Destination DC announced DC as 2029 ASAE Annual host",
          CLAIM_KIND.FACT,
          "https://washington.org/press/washington-dc-named-host-2029-ASAE-annual-meeting-exposition",
          "Destination DC press release"
        ),
      ],
      labels: ["dmv_expansion_v1", "dmv_stretch", "early_watch"],
    })
  );

  out.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_world_biomaterials_2028_overflow",
      organizationName: "World Biomaterials Congress / biomaterials community",
      title: "World Biomaterials Congress 2028 — WEWCC (medical overflow watch)",
      segment: "Medical / Scientific",
      demandType: "congress_overflow",
      demandStatus: DEMAND_STATUS.KNOWN_DEMAND,
      eventStartDate: "2028-04-24",
      eventEndDate: "2028-04-29",
      destinationStatus: "Walter E. Washington Convention Center, Washington, DC",
      venueStatus: "Convention center listed",
      estimatedPeakRooms: "80-220 overflow share",
      estimatedPeakRoomsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_STRETCH,
      demandTerritoryFitLocked: true,
      demandTerritoryRationale:
        "Convention-center scientific congress — Stretch overflow with NIH/medical attendee rationale.",
      bethesdaWinThesis:
        "Sessions will be downtown at WEWCC. Bethesda’s Stretch case is medical/research attendees who prefer NIH-corridor lodging and parking if official downtown blocks fill — not a headquarters bid.",
      whyNow:
        "WATCH until official housing opens.",
      fitExplanation:
        "Stretch medical overflow only.",
      summaryWhat:
        "World Biomaterials Congress April 24–29, 2028 at WEWCC.",
      summaryWhyMatters:
        "Named scientific congress downtown — Stretch overflow if housing opens broadly.",
      summaryWhyHotel:
        "NIH-corridor overflow for scientific attendees.",
      recommendedAction:
        "Monitor official congress housing; pursue only if overflow list is published.",
      primaryContact: {
        name: "Congress organizers (housing TBD)",
        role: "Housing",
        organization: "World Biomaterials Congress",
        email: null,
        claimKind: CLAIM_KIND.UNKNOWN,
        sourceUrl:
          "https://expofp.com/walter-e-washington-convention-center/world-biomaterials-congress",
        confidence: 45,
      },
      likelyCompetitor: "Downtown DC convention hotels",
      marketCompetitors: [
        marketComp(
          "Downtown DC convention hotels",
          "Downtown / Convention Center",
          "Primary WEWCC lodging cluster"
        ),
      ],
      fitComponents: {
        physicalFit: 58,
        geographyFit: 36,
        timing: 45,
        commercialValue: 52,
        historicalFit: 35,
        competitiveAccessibility: 25,
        contactability: 30,
      },
      confidenceInput: {
        sourceAuthority: 70,
        independentSourceCount: 1,
        directness: 75,
        recency: 80,
        verifiedFieldRatio: 0.45,
        firstPartyShare: 0.4,
        completeness: 48,
        conflictPenalty: 8,
      },
      evidence: [
        ev(
          "dates_venue",
          "April 24–29, 2028 — Walter E. Washington Convention Center",
          CLAIM_KIND.FACT,
          "https://expofp.com/walter-e-washington-convention-center/world-biomaterials-congress",
          "ExpoFP World Biomaterials Congress 2028"
        ),
      ],
      labels: ["dmv_expansion_v1", "dmv_stretch", "overflow"],
    })
  );

  return out;
}

/**
 * Researched but rejected — proves filtering, not scraping.
 */
export function listRejectedDmvExpansionCandidates() {
  return [
    {
      event: "CARH 2027 Annual Meeting & Legislative Conference",
      organization: "Council for Affordable and Rural Housing",
      geography: "Arlington, VA",
      reason:
        "Already contracted at The Ritz-Carlton, Pentagon City — no credible Bethesda host or overflow thesis.",
      sourceUrl: "https://www.carh.org/events/2027-annual-meeting-legislative-conference/",
    },
    {
      event: "ASOR Annual Meeting 2027",
      organization: "American Society of Overseas Research",
      geography: "Arlington, VA",
      reason: "Already contracted at Hyatt Regency Crystal City.",
      sourceUrl: "https://www.asor.org/am/meetings/",
    },
    {
      event: "IPWatchdog LIVE 2028",
      organization: "IPWatchdog",
      geography: "Arlington, VA",
      reason: "Already contracted at Renaissance Arlington Capital View.",
      sourceUrl:
        "https://ipwatchdog.com/event/ipwatchdog-live-2028-at-the-renaissance-arlington-capital-view/",
    },
    {
      event: "TEI Midyear Conference 2027/2028",
      organization: "Tax Executives Institute",
      geography: "Washington, DC",
      reason: "Already returning to Grand Hyatt Washington with published room block.",
      sourceUrl: "https://www.tei.org/events-education/events/2027-midyear-conference",
    },
    {
      event: "ASSA Annual Meeting 2027",
      organization: "Allied Social Science Associations / AEA",
      geography: "Washington, DC",
      reason: "Headquarters hotel already Marriott Marquis Washington, DC.",
      sourceUrl: "https://www.aeaweb.org/conference/2027",
    },
    {
      event: "NASPA Annual Conference 2027",
      organization: "NASPA",
      geography: "National Harbor, MD",
      reason:
        "Headquarters at Gaylord National with a full National Harbor hotel package — Bethesda is not a credible alternative.",
      sourceUrl: "https://events.naspa.org/e/2027-naspa-annual-conference/page/plan-your-trip",
    },
    {
      event: "Society for Prevention Research Annual Meeting 2026–2029",
      organization: "SPR",
      geography: "Washington, DC",
      reason: "Explicitly contracted to Hyatt Regency Washington on Capitol Hill for DC years.",
      sourceUrl: "https://preventionresearch.org/meeting/future-conferences/",
    },
    {
      event: "Sacred Space Conference 2027",
      organization: "Sacred Space Foundation",
      geography: "College Park, MD",
      reason: "Host hotel already The Hotel at UMD; not an open lodging competition for Bethesda.",
      sourceUrl: "https://www.sacredspacefoundation.org/plan-your-trip-2/",
    },
    {
      event: "ONS Congress 2027",
      organization: "Oncology Nursing Society",
      geography: "Washington, DC",
      reason: "Headquarter hotels already Marriott Marquis and Westin Downtown.",
      sourceUrl: "http://www.ons.org/education-hub/events/ons-congress/location",
    },
    {
      event: "Data Center Americas 2027",
      organization: "Data Center Americas",
      geography: "Washington, DC",
      reason: "Official event hotel already Marriott Marquis; Connections Housing locked.",
      sourceUrl: "https://datacenteramericas.com/why-attend/hotel-travel/",
    },
    {
      event: "ACG Next Conference 2027",
      organization: "ACG National Capital",
      geography: "Tysons, VA",
      reason: "Single-day event at Capital One Tower — not a room-block lodging opportunity.",
      sourceUrl: "https://www.acg.org/national-capital/events/2027-acg-next-conference",
    },
    {
      event: "NCCAN 2027",
      organization: "ACF / HHS",
      geography: "Arlington, VA",
      reason: "Already contracted at Hyatt Regency Crystal City.",
      sourceUrl: "https://acf.gov/cb/focus-areas/child-abuse-neglect/nccan",
    },
  ];
}

/**
 * Merge expansion opportunities into an existing list (dedupe by id).
 */
export function applyDmvExpansionPass(existingOpportunities = []) {
  const byId = new Map();
  for (const o of existingOpportunities || []) {
    if (o?.id) byId.set(o.id, o);
  }
  const added = [];
  for (const o of buildDmvExpansionOpportunities()) {
    if (byId.has(o.id)) continue;
    byId.set(o.id, o);
    added.push(o.id);
  }
  return {
    opportunities: [...byId.values()],
    addedIds: added,
    rejectedExamples: listRejectedDmvExpansionCandidates(),
    researchNote:
      "L3 standard-web DMV expansion; $0 Webhound. Prior Bethesda pilot Webhound cap remains exhausted at $15.",
  };
}
