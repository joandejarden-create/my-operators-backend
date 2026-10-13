/**
 * Bethesda Marriott pilot seed opportunities — Level 1–3 researched candidates.
 * Attendance/room blocks marked UNKNOWN unless sourced. FACT vs INFERENCE explicit.
 * Webhound Level-5 findings may merge later via orchestrator.
 */

import { PILOT_HOTEL_ID } from "./hotel-profile.js";
import { buildOpportunity } from "./opportunity-factory.js";
import {
  BOOKING_WINDOW,
  CLAIM_KIND,
  DEMAND_STATUS,
} from "./claim-types.js";

const ACCESS = "2026-09-12";

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
    confidence: extra.confidence ?? 80,
    researchMethodId: extra.researchMethodId || "GDI-ASSOC-EVENT-01",
    researchProvider: extra.researchProvider || "dealality_web_fetch",
    researchLevel: extra.researchLevel || "L3_STANDARD_APPROVED_WEB_RESEARCH",
  };
}

export function buildBethesdaPilotOpportunities() {
  const hotelId = PILOT_HOTEL_ID;
  const candidates = [];

  // 1 — AMWA 2027 Annual Meeting (HIGH candidate)
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_amwa_2027_annual",
      organizationName: "American Medical Women's Association (AMWA)",
      title: "AMWA 112th Annual Meeting 2027 — Washington, DC area (hotel/venue not named)",
      segment: "Medical",
      demandType: "annual_meeting",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2027-03-11",
      eventEndDate: "2027-03-14",
      destinationStatus: "Washington, DC area announced; specific city/hotel not named",
      venueStatus: "NOT_ANNOUNCED",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      estimatedNights: 3,
      estimatedNightsClaimKind: CLAIM_KIND.INFERENCE,
      bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
      whyNow:
        "CONTACT NOW: Official AMWA pages announce March 11–14, 2027 in the Washington, D.C. area and open speaker/poster calls, but do not name a host hotel. Historical AMWA annual meetings use full-service hotels in major cities; venue selection window is likely open.",
      fitExplanation:
        "Full-service Marriott with substantial meeting space near NIH/Walter Reed and DC advocacy access supports a medical association annual meeting that includes Advocacy Day on the Hill.",
      summaryWhat:
        "AMWA’s 112th Annual Meeting (March 11–14, 2027) is announced for the Washington, D.C. area, beginning with Advocacy Day on the Hill.",
      summaryWhyMatters:
        "Medical association annual meeting with policy/advocacy programming — aligned with Bethesda Marriott’s NIH/medical positioning and DC access.",
      summaryWhyHotel:
        "Proximity to NIH and federal health ecosystem, full-service meeting inventory, and Marriott brand familiarity for association planners.",
      recommendedAction:
        "Contact AMWA meetings leadership this week via associatedirector@amwa-doc.org. Confirm whether a 2027 host hotel has been selected. Lead with NIH adjacency, full-service meeting space, and Advocacy Day logistics support.",
      likelyCompetitor: "Hyatt Regency Bethesda",
      competitorRationale:
        "Downtown Bethesda Metro location is a frequent planner alternative for DC-area medical groups; Gaylord National / downtown DC hotels also compete for advocacy-heavy programs.",
      primaryContact: {
        name: CLAIM_KIND.UNKNOWN,
        role: "Association meetings contact (public)",
        organization: "AMWA",
        email: "associatedirector@amwa-doc.org",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/",
        confidence: 70,
      },
      contacts: [
        {
          name: CLAIM_KIND.UNKNOWN,
          role: "Association Director / meetings inbox",
          organization: "AMWA",
          email: "associatedirector@amwa-doc.org",
          claimKind: CLAIM_KIND.FACT,
          sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/",
        },
      ],
      meetingHistory: [
        { year: 2026, city: "Burlingame, CA", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT, sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/" },
        { year: 2025, city: "Boston, MA", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT, sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/" },
        { year: 2023, city: "Philadelphia, PA", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT, sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/" },
        { year: 2016, city: "Miami, FL", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT, sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/" },
        { year: 2015, city: "Chicago, IL", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT, sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/" },
        { year: 2014, city: "Washington, DC", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT, sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/" },
      ],
      evidence: [
        ev("event_dates", "March 11–14, 2027", CLAIM_KIND.FACT, "https://amwa-doc.org/news/amwas-112th-annual-meeting-heads-to-washington-dc/", "AMWA announces 112th Annual Meeting", { extractedText: "March 11–14, 2027, in the Washington, D.C. area" }),
        ev("destination", "Washington, D.C. area", CLAIM_KIND.FACT, "https://amwa-doc.org/news/amwas-112th-annual-meeting-heads-to-washington-dc/", "AMWA news", {}),
        ev("venue", "Not named", CLAIM_KIND.FACT, "https://amwa-doc.org/event/amwa-2027-annual-meeting/", "AMWA 2027 event page", { extractedText: "SAVE THE DATE" }),
        ev("contact_email", "associatedirector@amwa-doc.org", CLAIM_KIND.FACT, "https://amwa-doc.org/news-events/amwa-meetings/", "AMWA Meetings", {}),
        ev("historical_rotation", "Prior annual cities include Boston, Philadelphia, Miami, Chicago, Washington DC", CLAIM_KIND.FACT, "https://amwa-doc.org/news-events/amwa-meetings/", "AMWA Meetings history", {}),
        ev("size_fit", "Mid-size medical association annual (not mega-society)", CLAIM_KIND.INFERENCE, "https://amwa-doc.org/news-events/amwa-meetings/", "AMWA Meetings", { sourceAuthority: "Tier_D", confidence: 45 }),
      ],
      fitComponents: {
        physicalFit: 82,
        geographyFit: 90,
        timing: 88,
        commercialValue: 70,
        historicalFit: 72,
        competitiveAccessibility: 68,
        contactability: 75,
      },
      confidenceInput: {
        sourceAuthority: 90,
        independentSourceCount: 3,
        directness: 85,
        recency: 95,
        verifiedFieldRatio: 0.7,
        firstPartyShare: 90,
        completeness: 55,
        conflictPenalty: 0,
      },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01", "GDI-MED-SCI-01", "GDI-HIST-MEETING-01", "GDI-CONTACT-01"],
      labels: ["new_this_week", "entering_booking_window", "medical", "association"],
    })
  );

  // 2 — AMWA 2027 Interim Meeting TBD
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_amwa_2027_interim",
      organizationName: "American Medical Women's Association (AMWA)",
      title: "AMWA 2027 Interim Meeting — Sep 11–12, 2027 (destination TBD)",
      segment: "Medical",
      demandType: "interim_meeting",
      demandStatus: DEMAND_STATUS.RECURRING_PREDICTED,
      eventStartDate: "2027-09-11",
      eventEndDate: "2027-09-12",
      destinationStatus: "TBD",
      venueStatus: "NOT_ANNOUNCED",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      estimatedNights: 2,
      estimatedNightsClaimKind: CLAIM_KIND.INFERENCE,
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      whyNow:
        "WATCH: Dates listed; destination TBD. AMWA interim meetings have repeatedly used Washington, DC historically — early relationship building now, venue push later.",
      fitExplanation:
        "Smaller interim format may fit Bethesda Marriott more easily than a mega downtown convention hotel.",
      summaryWhat: "AMWA lists 2027 Interim Meeting Sep 11–12 with destination TBD.",
      summaryWhyMatters: "Recurring medical association interim often lands in DC; early positioning opportunity.",
      summaryWhyHotel: "Historical DC interim pattern + Bethesda medical ecosystem.",
      recommendedAction:
        "Log as watchlist. Ask AMWA whether 2027 interim destination process has started; offer Bethesda for a compact board/leadership-style interim.",
      likelyCompetitor: "Downtown Washington hotels (prior interim pattern)",
      competitorRationale: "AMWA interim meetings have repeatedly been listed in Washington, DC.",
      primaryContact: {
        name: CLAIM_KIND.UNKNOWN,
        role: "Association meetings contact",
        organization: "AMWA",
        email: "associatedirector@amwa-doc.org",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/",
      },
      meetingHistory: [
        { year: 2025, city: "Washington, DC", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT, sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/" },
        { year: 2024, city: "Washington, DC", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT, sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/" },
        { year: 2023, city: "Washington, DC", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT, sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/" },
        { year: 2015, city: "Washington, DC", venue: CLAIM_KIND.UNKNOWN, claimKind: CLAIM_KIND.FACT, sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/" },
      ],
      evidence: [
        ev("event_dates", "September 11–12, 2027 TBD", CLAIM_KIND.FACT, "https://amwa-doc.org/news-events/amwa-meetings/", "AMWA Meetings", {}),
        ev("dc_interim_pattern", "Multiple prior interim meetings listed in Washington, DC", CLAIM_KIND.FACT, "https://amwa-doc.org/news-events/amwa-meetings/", "AMWA Meetings", {}),
      ],
      fitComponents: { physicalFit: 78, geographyFit: 85, timing: 55, commercialValue: 55, historicalFit: 80, competitiveAccessibility: 70, contactability: 70 },
      confidenceInput: { sourceAuthority: 88, independentSourceCount: 1, directness: 80, recency: 90, verifiedFieldRatio: 0.5, firstPartyShare: 90, completeness: 40, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01", "GDI-HIST-MEETING-01"],
      labels: ["watchlist", "medical", "recurring"],
    })
  );

  // 3 — AMWA 2028 Annual TBD
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_amwa_2028_annual",
      organizationName: "American Medical Women's Association (AMWA)",
      title: "AMWA 113th Annual Meeting 2028 — destination TBD",
      segment: "Medical",
      demandType: "annual_meeting",
      demandStatus: DEMAND_STATUS.RECURRING_PREDICTED,
      eventStartDate: "2028-03-23",
      eventEndDate: "2028-03-26",
      destinationStatus: "TBD",
      venueStatus: "NOT_ANNOUNCED",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      bookingWindowStatus: BOOKING_WINDOW.TOO_EARLY,
      whyNow: "TOO EARLY for aggressive pursuit, but useful for multi-year association relationship if engaging on 2027.",
      fitExplanation: "Same physical/geo fit thesis as 2027 if destination opens to DC metro.",
      summaryWhat: "AMWA lists March 23–26, 2028 annual meeting with destination TBD.",
      summaryWhyMatters: "Long-cycle association demand; relationship leverage from 2027 pursuit.",
      summaryWhyHotel: "Same Bethesda medical/NIH thesis if geography opens.",
      recommendedAction: "Do not lead with 2028. Mention multi-year interest only after 2027 conversation starts.",
      likelyCompetitor: CLAIM_KIND.UNKNOWN,
      primaryContact: {
        name: CLAIM_KIND.UNKNOWN,
        role: "Association meetings contact",
        organization: "AMWA",
        email: "associatedirector@amwa-doc.org",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://amwa-doc.org/news-events/amwa-meetings/",
      },
      evidence: [
        ev("event_dates", "March 23–26, 2028 TBD", CLAIM_KIND.FACT, "https://amwa-doc.org/news-events/amwa-meetings/", "AMWA Meetings", {}),
      ],
      fitComponents: { physicalFit: 80, geographyFit: 60, timing: 35, commercialValue: 65, historicalFit: 70, competitiveAccessibility: 55, contactability: 70 },
      confidenceInput: { sourceAuthority: 85, independentSourceCount: 1, directness: 75, recency: 90, verifiedFieldRatio: 0.35, firstPartyShare: 90, completeness: 30, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01"],
      labels: ["watchlist", "too_early"],
    })
  );

  // 4 — Bethesda Premier Cup 2026
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_bethesda_premier_cup_2026",
      organizationName: "Bethesda Soccer Club",
      title: "Bethesda Premier Cup 2026 — stay-to-play weekend hotel demand (Nov 13–15 & 20–22)",
      segment: "Weekend group",
      demandType: "sports_tournament",
      demandStatus: DEMAND_STATUS.CONFIRMED_DEMAND,
      eventStartDate: "2026-11-13",
      eventEndDate: "2026-11-22",
      destinationStatus: "Bethesda / Montgomery County",
      venueStatus: "Fields-based; hotels via official housing partner",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      estimatedNights: 2,
      estimatedNightsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
      whyNow:
        "CONTACT NOW: Tournament dates are sanctioned for November 2026; official hotel program is managed by HBC Event Services. Hotel should confirm participation / block availability before booking links fully allocate demand.",
      fitExplanation:
        "Full-service suburban hotel with coach access potential and local Bethesda branding is a natural weekend sports overflow candidate versus downtown-only properties.",
      summaryWhat:
        "Large local youth soccer tournament with official stay-to-play hotel blocks across two November weekends.",
      summaryWhyMatters:
        "Weekend group room nights with a formal housing partner — actionable sales outreach path.",
      summaryWhyHotel:
        "Local Bethesda identity, room inventory for team blocks, parking/coach access thesis (parking details still UNKNOWN in Dealality profile).",
      recommendedAction:
        "Contact HBC Event Services (support@hbceventservices.com / 505-346-0522) and tournament director Brad Roos (broos@bethesdasoccer.org) to confirm whether Bethesda Marriott is in the 2026 housing set and negotiate block terms.",
      likelyCompetitor: "Other Bethesda / Rockville hotels in HBC housing set",
      competitorRationale: "Stay-to-play programs distribute teams across an official hotel list.",
      primaryContact: {
        name: "Brad Roos",
        role: "Tournament Director",
        organization: "Bethesda Soccer Club / Bethesda Premier Cup",
        email: "broos@bethesdasoccer.org",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://www.msysa.org/2026-2027-sanctioned-tournaments/",
        confidence: 85,
      },
      contacts: [
        {
          name: "Brad Roos",
          role: "Tournament Director",
          organization: "Bethesda Soccer Club",
          email: "broos@bethesdasoccer.org",
          claimKind: CLAIM_KIND.FACT,
          sourceUrl: "https://www.msysa.org/2026-2027-sanctioned-tournaments/",
        },
        {
          name: "HBC Event Services",
          role: "Official housing company",
          organization: "HBC Event Services",
          email: "support@hbceventservices.com",
          phone: "505-346-0522",
          claimKind: CLAIM_KIND.FACT,
          sourceUrl: "https://bethesdapremiercuphotels.com/events/",
        },
      ],
      evidence: [
        ev("event_dates", "Nov 13–15 & 20–22, 2026", CLAIM_KIND.FACT, "https://www.msysa.org/2026-2027-sanctioned-tournaments/", "USYS Maryland sanctioned tournaments", { researchMethodId: "GDI-WEEKEND-01" }),
        ev("housing_partner", "HBC Event Services via bethesdapremiercuphotels.com", CLAIM_KIND.FACT, "https://bethesdapremiercuphotels.com/events/", "Bethesda Premier Cup Hotels", { researchMethodId: "GDI-WEEKEND-01" }),
        ev("tournament_director", "Brad Roos broos@bethesdasoccer.org", CLAIM_KIND.FACT, "https://www.msysa.org/2026-2027-sanctioned-tournaments/", "USYS Maryland", { researchMethodId: "GDI-CONTACT-01" }),
      ],
      fitComponents: { physicalFit: 75, geographyFit: 92, timing: 85, commercialValue: 72, historicalFit: 60, competitiveAccessibility: 70, contactability: 88 },
      confidenceInput: { sourceAuthority: 85, independentSourceCount: 2, directness: 90, recency: 90, verifiedFieldRatio: 0.65, firstPartyShare: 70, completeness: 55, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-WEEKEND-01", "GDI-CONTACT-01"],
      labels: ["weekend", "sports", "entering_booking_window", "high_actionability"],
    })
  );

  // 5 — NIH Research Festival 2026 overflow
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_nih_research_festival_2026",
      organizationName: "National Institutes of Health (NIH)",
      title: "2026 NIH Research Festival — campus event with vendor/attendee overflow lodging potential",
      segment: "Scientific",
      demandType: "campus_festival_overflow",
      demandStatus: DEMAND_STATUS.CONFIRMED_DEMAND,
      eventStartDate: "2026-09-14",
      eventEndDate: "2026-09-18",
      destinationStatus: "NIH Main Campus, Bethesda",
      venueStatus: "Campus Building 10 (not hotel-hosted)",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      bookingWindowStatus: BOOKING_WINDOW.LIKELY_TOO_LATE,
      whyNow:
        "LIKELY TOO LATE for primary planning: festival starts Sep 14, 2026 (days away). May still capture last-minute vendor overflow; better used as pattern learning for 2027.",
      fitExplanation:
        "Closest full-service Marriott to NIH campus for overnight vendors/guests — but primary venue is campus, not hotel RFP.",
      summaryWhat: "Multi-day NIH intramural science festival on Bethesda campus with vendor exhibit days.",
      summaryWhyMatters: "Validates NIH adjacency demand; limited room-block RFP opportunity this cycle.",
      summaryWhyHotel: "Pooks Hill location relative to NIH campus.",
      recommendedAction:
        "If not already sold out, offer last-minute vendor rate outreach via TVA/R&W contacts. Capture 2027 festival dates early next year.",
      likelyCompetitor: "Bethesda North Marriott; downtown Bethesda hotels",
      competitorRationale: "Multiple Bethesda hotels compete for NIH campus overflow.",
      primaryContact: {
        name: "David Browne",
        role: "R&W vendor coordination contact (public)",
        organization: "Recreation & Welfare Association / TVA vendor exhibit",
        email: "browned2@mail.nih.gov",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://researchfestival.nih.gov/2026",
        confidence: 75,
      },
      disqualifyReasons: ["Event imminent — primary booking window closed for planned group solicitation"],
      evidence: [
        ev("event_dates", "September 14–18, 2026", CLAIM_KIND.FACT, "https://researchfestival.nih.gov/2026", "NIH Research Festival 2026", { researchMethodId: "GDI-MED-SCI-01" }),
        ev("venue", "NIH Main Bethesda Campus Building 10", CLAIM_KIND.FACT, "https://researchfestival.nih.gov/2026/general-schedule-events-0", "General Schedule", { researchMethodId: "GDI-MED-SCI-01" }),
        ev("vendor_contact", "David Browne browned2@mail.nih.gov", CLAIM_KIND.FACT, "https://researchfestival.nih.gov/2026", "NIH Research Festival", { researchMethodId: "GDI-CONTACT-01" }),
      ],
      fitComponents: { physicalFit: 60, geographyFit: 95, timing: 20, commercialValue: 40, historicalFit: 50, competitiveAccessibility: 65, contactability: 70 },
      confidenceInput: { sourceAuthority: 95, independentSourceCount: 2, directness: 90, recency: 99, verifiedFieldRatio: 0.7, firstPartyShare: 95, completeness: 50, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-MED-SCI-01", "GDI-CONTACT-01", "GDI-ANCHOR-01"],
      labels: ["nih", "pattern_learning", "disqualified_timing"],
    })
  );

  // 6 — AAN 2027 — too large
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_aan_2027_disqualified",
      organizationName: "American Academy of Neurology",
      title: "2027 AAN Annual Meeting — Washington, DC (likely convention-scale; not a Bethesda Marriott host fit)",
      segment: "Medical",
      demandType: "annual_meeting",
      demandStatus: DEMAND_STATUS.CONFIRMED_DEMAND,
      eventStartDate: "2027-05-01",
      eventEndDate: "2027-05-05",
      destinationStatus: "Washington, DC",
      venueStatus: "City announced; housing typically convention-scale",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      bookingWindowStatus: BOOKING_WINDOW.RESEARCH_FURTHER,
      whyNow: "City announced; however physical capacity vs mega-society annual makes Bethesda Marriott an unlikely primary host.",
      fitExplanation: "407 rooms / ~18–24k sq ft meeting space is below typical AAN annual host profile.",
      summaryWhat: "Premier neurology annual meeting in Washington, DC May 1–5, 2027.",
      summaryWhyMatters: "Important market demand signal; overflow only if housing bureau opens suburban hotels.",
      summaryWhyHotel: "Unlikely primary; possible overflow if official housing expands.",
      recommendedAction: "Watch official housing bureau list only. Do not pursue as primary host.",
      likelyCompetitor: "Downtown DC convention hotels / Walter E. Washington Convention Center complex",
      disqualifyReasons: ["Likely incompatible event scale for primary host at 407-room suburban hotel"],
      evidence: [
        ev("event_dates", "May 1–5, 2027 Washington DC", CLAIM_KIND.FACT, "https://www.aan.com/events/annual-meeting", "AAN Annual Meeting", { researchMethodId: "GDI-MED-SCI-01" }),
      ],
      fitComponents: { physicalFit: 25, geographyFit: 55, timing: 60, commercialValue: 40, historicalFit: 30, competitiveAccessibility: 20, contactability: 30 },
      confidenceInput: { sourceAuthority: 90, independentSourceCount: 1, directness: 80, recency: 90, verifiedFieldRatio: 0.4, firstPartyShare: 90, completeness: 35, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-MED-SCI-01"],
      labels: ["disqualified_scale"],
    })
  );

  // 7 — ASA ADVANCE 2027 — venue already Gaylord
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_asa_advance_2027_disqualified",
      organizationName: "American Society of Anesthesiologists",
      title: "ASA ADVANCE 2027 — already contracted at Gaylord National (National Harbor)",
      segment: "Medical",
      demandType: "conference",
      demandStatus: DEMAND_STATUS.CONFIRMED_DEMAND,
      eventStartDate: "2027-01-22",
      eventEndDate: "2027-01-24",
      destinationStatus: "Washington DC Metro Area",
      venueStatus: "Gaylord National Resort & Convention Center (announced)",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      bookingWindowStatus: BOOKING_WINDOW.LIKELY_TOO_LATE,
      whyNow: "Host hotel already published on ASA travel services page.",
      fitExplanation: "Bethesda Marriott is not the host; overflow unlikely to be material vs on-site Gaylord inventory (~2,000 rooms).",
      summaryWhat: "ASA ADVANCE 2027 Jan 22–24 at Gaylord National.",
      summaryWhyMatters: "Useful negative example — contracted venue detection.",
      summaryWhyHotel: "Not competitive for primary; do not pursue as host.",
      recommendedAction: "Do not pursue. Archive as contracted-elsewhere learning case.",
      likelyCompetitor: "Gaylord National Resort & Convention Center",
      disqualifyReasons: ["Event already contracted at named competing venue"],
      evidence: [
        ev("venue", "Gaylord National Resort & Convention Center", CLAIM_KIND.FACT, "https://www.asahq.org/advance/attend/travelservices", "ASA ADVANCE Travel Services", { researchMethodId: "GDI-MED-SCI-01" }),
        ev("event_dates", "January 22–24, 2027", CLAIM_KIND.FACT, "https://www.asahq.org/meetings/asa-advance", "ASA ADVANCE", { researchMethodId: "GDI-MED-SCI-01" }),
      ],
      fitComponents: { physicalFit: 40, geographyFit: 35, timing: 15, commercialValue: 20, historicalFit: 40, competitiveAccessibility: 10, contactability: 40 },
      confidenceInput: { sourceAuthority: 95, independentSourceCount: 2, directness: 95, recency: 90, verifiedFieldRatio: 0.8, firstPartyShare: 95, completeness: 70, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-MED-SCI-01"],
      labels: ["disqualified_contracted"],
    })
  );

  // 8 — FBA Qui Tam 2027 — contracted Hilton Capitol Hill
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_fba_qui_tam_2027_disqualified",
      organizationName: "Federal Bar Association — Qui Tam Section",
      title: "2027 Hybrid Qui Tam Conference — venue already Hilton Capitol Hill",
      segment: "Government",
      demandType: "conference",
      demandStatus: DEMAND_STATUS.CONFIRMED_DEMAND,
      eventStartDate: "2027-02-18",
      eventEndDate: "2027-02-19",
      venueStatus: "Hilton Capitol Hill Washington DC (announced)",
      bookingWindowStatus: BOOKING_WINDOW.LIKELY_TOO_LATE,
      whyNow: "Venue announced.",
      fitExplanation: "Downtown advocacy/legal conference already placed.",
      summaryWhat: "FBA Qui Tam conference Feb 18–19, 2027 at Hilton Capitol Hill.",
      summaryWhyMatters: "Government/legal demand exists in market but not available.",
      summaryWhyHotel: "Wrong micro-location vs Capitol Hill legal set.",
      recommendedAction: "Do not pursue.",
      likelyCompetitor: "Hilton Capitol Hill",
      disqualifyReasons: ["Event already contracted at named competing venue", "Wrong geography micro-cluster"],
      evidence: [
        ev("venue", "Hilton Capitol Hill", CLAIM_KIND.FACT, "https://www.fedbar.org/event/2027-hybrid-qui-tam-conference-save-the-date/", "FBA Qui Tam Save the Date", { researchMethodId: "GDI-GOV-EVENT-01" }),
      ],
      fitComponents: { physicalFit: 50, geographyFit: 30, timing: 10, commercialValue: 35, historicalFit: 40, competitiveAccessibility: 15, contactability: 40 },
      confidenceInput: { sourceAuthority: 90, independentSourceCount: 1, directness: 95, recency: 85, verifiedFieldRatio: 0.75, firstPartyShare: 90, completeness: 60, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-GOV-EVENT-01"],
      labels: ["disqualified_contracted"],
    })
  );

  // 9 — NTCA Legislative Conference 2027 — Hyatt Capitol Hill contracted
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_ntca_2027_disqualified",
      organizationName: "NTCA — The Rural Broadband Association",
      title: "2027 NTCA Legislative and Policy Conference — Hyatt Regency Washington on Capitol Hill",
      segment: "Associations",
      demandType: "advocacy_conference",
      demandStatus: DEMAND_STATUS.CONFIRMED_DEMAND,
      eventStartDate: "2027-04-18",
      eventEndDate: "2027-04-20",
      venueStatus: "Hyatt Regency Washington on Capitol Hill (announced)",
      bookingWindowStatus: BOOKING_WINDOW.LIKELY_TOO_LATE,
      whyNow: "Venue announced on official NTCA page.",
      fitExplanation: "Capitol Hill advocacy hotel already selected.",
      summaryWhat: "NTCA legislative conference April 18–20, 2027 at Hyatt Regency Washington on Capitol Hill.",
      summaryWhyMatters: "Association advocacy demand pattern; not available for Bethesda host.",
      summaryWhyHotel: "Advocacy Day pattern favors Capitol Hill hotels.",
      recommendedAction: "Do not pursue. Note advocacy meetings often prefer Capitol Hill — relevant when pitching AMWA.",
      likelyCompetitor: "Hyatt Regency Washington on Capitol Hill",
      disqualifyReasons: ["Event already contracted at named competing venue"],
      evidence: [
        ev("venue", "Hyatt Regency Washington on Capitol Hill", CLAIM_KIND.FACT, "https://www.ntca.org/learn/events/save-date-legislative-policy-conference", "NTCA Save the Date", { researchMethodId: "GDI-ASSOC-EVENT-01" }),
      ],
      fitComponents: { physicalFit: 55, geographyFit: 28, timing: 10, commercialValue: 45, historicalFit: 35, competitiveAccessibility: 15, contactability: 50 },
      confidenceInput: { sourceAuthority: 90, independentSourceCount: 1, directness: 95, recency: 85, verifiedFieldRatio: 0.7, firstPartyShare: 90, completeness: 55, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01"],
      labels: ["disqualified_contracted"],
    })
  );

  // 10 — PTAB Bar Association 2027 — Ritz contracted
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_ptab_2027_disqualified",
      organizationName: "PTAB Bar Association",
      title: "2027 PTAB Bar Association Annual Conference — Ritz-Carlton Washington, DC",
      segment: "Associations",
      demandType: "annual_conference",
      demandStatus: DEMAND_STATUS.CONFIRMED_DEMAND,
      eventStartDate: "2027-03-17",
      eventEndDate: "2027-03-19",
      venueStatus: "Ritz-Carlton, Washington, DC (announced)",
      bookingWindowStatus: BOOKING_WINDOW.LIKELY_TOO_LATE,
      whyNow: "Venue announced.",
      fitExplanation: "Luxury downtown venue already selected.",
      summaryWhat: "PTAB Bar Association Annual Conference March 17–19, 2027 at Ritz-Carlton DC.",
      summaryWhyMatters: "Legal association demand exists; not transferable.",
      summaryWhyHotel: "Service-level / location mismatch vs Ritz DC.",
      recommendedAction: "Do not pursue.",
      likelyCompetitor: "Ritz-Carlton Washington, DC",
      disqualifyReasons: ["Event already contracted at named competing venue"],
      evidence: [
        ev("venue", "Ritz-Carlton, Washington, DC", CLAIM_KIND.FACT, "https://www.ptabbar.org/2027_ptab_bar_association_annu.php", "PTAB Bar Association", { researchMethodId: "GDI-ASSOC-EVENT-01" }),
      ],
      fitComponents: { physicalFit: 45, geographyFit: 25, timing: 10, commercialValue: 40, historicalFit: 30, competitiveAccessibility: 10, contactability: 45 },
      confidenceInput: { sourceAuthority: 88, independentSourceCount: 1, directness: 95, recency: 85, verifiedFieldRatio: 0.7, firstPartyShare: 90, completeness: 55, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01"],
      labels: ["disqualified_contracted"],
    })
  );

  // 11 — AGB Trusteeship 2027 — Washington Hilton contracted
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_agb_2027_disqualified",
      organizationName: "Association of Governing Boards of Universities and Colleges (AGB)",
      title: "2027 National Conference on Trusteeship — Washington Hilton (contracted)",
      segment: "University / education",
      demandType: "national_conference",
      demandStatus: DEMAND_STATUS.CONFIRMED_DEMAND,
      eventStartDate: "2027-03-13",
      eventEndDate: "2027-03-15",
      venueStatus: "Washington Hilton (announced with room block)",
      bookingWindowStatus: BOOKING_WINDOW.LIKELY_TOO_LATE,
      whyNow: "Hotel and room block published.",
      fitExplanation: "Large downtown conference hotel already selected.",
      summaryWhat: "AGB National Conference on Trusteeship March 13–15, 2027 at Washington Hilton.",
      summaryWhyMatters: "Education governance demand — contracted.",
      summaryWhyHotel: "Not competitive once Washington Hilton block is live.",
      recommendedAction: "Do not pursue.",
      likelyCompetitor: "Washington Hilton",
      disqualifyReasons: ["Event already contracted at named competing venue"],
      evidence: [
        ev("venue", "Washington Hilton", CLAIM_KIND.FACT, "https://agb.org/events/national-conference-on-trusteeship-2027/2027-national-conference-on-trusteeship-hotel-and-travel/", "AGB Hotel and Travel", { researchMethodId: "GDI-ASSOC-EVENT-01" }),
      ],
      fitComponents: { physicalFit: 40, geographyFit: 30, timing: 10, commercialValue: 50, historicalFit: 35, competitiveAccessibility: 10, contactability: 40 },
      confidenceInput: { sourceAuthority: 92, independentSourceCount: 1, directness: 95, recency: 85, verifiedFieldRatio: 0.8, firstPartyShare: 95, completeness: 65, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01"],
      labels: ["disqualified_contracted"],
    })
  );

  // 12 — Experimental: medical association watch — Society-scale DC overflow from ADA (disqualify primary)
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_ada_2027_disqualified",
      organizationName: "American Diabetes Association",
      title: "ADA 2027 Scientific Sessions — Washington D.C. (convention-scale; primary host unfit)",
      segment: "Healthcare",
      demandType: "scientific_sessions",
      demandStatus: DEMAND_STATUS.CONFIRMED_DEMAND,
      destinationStatus: "Washington D.C.",
      venueStatus: "City-level listing; convention-scale expected",
      bookingWindowStatus: BOOKING_WINDOW.RESEARCH_FURTHER,
      whyNow: "City presence creates overflow possibility only; not a primary host play.",
      fitExplanation: "Scientific Sessions are typically convention-center scale.",
      summaryWhat: "ADA 87th Scientific Sessions listed for Washington D.C. in 2027.",
      summaryWhyMatters: "Healthcare demand density in market.",
      summaryWhyHotel: "Overflow-only thesis; weak evidence of suburban housing need.",
      recommendedAction: "Watch official housing list only. Do not pitch as host.",
      likelyCompetitor: "Downtown DC / National Harbor convention hotels",
      disqualifyReasons: ["Likely incompatible event scale for primary host"],
      evidence: [
        ev("destination", "Washington D.C.", CLAIM_KIND.FACT, "https://ada2027.org/index.html", "ADA 2027 site", { researchMethodId: "GDI-MED-SCI-01", sourceAuthority: "Tier_B" }),
      ],
      fitComponents: { physicalFit: 20, geographyFit: 50, timing: 50, commercialValue: 35, historicalFit: 25, competitiveAccessibility: 15, contactability: 20 },
      confidenceInput: { sourceAuthority: 60, independentSourceCount: 1, directness: 60, recency: 70, verifiedFieldRatio: 0.25, firstPartyShare: 40, completeness: 25, conflictPenalty: 10 },
      researchMethodsAttempted: ["GDI-MED-SCI-01"],
      labels: ["disqualified_scale"],
    })
  );

  // 13 — Corporate/gov contractor watch: recurring NIH vendor ecosystem
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_nih_sbpo_vos_pattern",
      organizationName: "NIH Small Business Program Office",
      title: "NIH SBPO Vendor Outreach Sessions — recurring Bethesda on-site pattern (next date TBD after April 2026 session)",
      segment: "Government contractor",
      demandType: "vendor_outreach",
      demandStatus: DEMAND_STATUS.RECURRING_PREDICTED,
      eventStartDate: null,
      eventEndDate: null,
      destinationStatus: "Bethesda (prior session at 6700 Rockledge Drive)",
      venueStatus: "Government facility for session; hotel demand = traveling vendors",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      bookingWindowStatus: BOOKING_WINDOW.RESEARCH_FURTHER,
      whyNow:
        "RESEARCH FURTHER: April 29, 2026 session already occurred. Monitor SAM.gov / NIH SBPO for next Vendor Outreach Session and offer preferred vendor lodging packages.",
      fitExplanation:
        "Bethesda location near Rockledge / NIH leased facilities is a natural lodging base for traveling small-business vendors.",
      summaryWhat:
        "NIH SBPO runs Vendor Outreach Sessions in Bethesda (documented April 29, 2026 at 6700 Rockledge Drive).",
      summaryWhyMatters:
        "Government contractor ecosystem generates intermittent room nights when sessions bring out-of-town vendors.",
      summaryWhyHotel: "Local to Rockledge/NIH leased corridor vs downtown DC.",
      recommendedAction:
        "Set SAM.gov / NIH SBPO watch. When next VOS is posted, contact NIHSmallBusiness@od.nih.gov with a vendor lodging offer within 48 hours.",
      likelyCompetitor: "Bethesda North Marriott; Rockville hotels",
      competitorRationale: "Proximity to Rockledge Drive / White Flint corridor.",
      primaryContact: {
        name: CLAIM_KIND.UNKNOWN,
        role: "NIH Small Business Program Office",
        organization: "NIH SBPO",
        email: "NIHSmallBusiness@od.nih.gov",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://sam.gov/workspace/contract/opp/f4e0c6bfa2a14c2195c8476fb81d5f9c/view",
        confidence: 70,
      },
      evidence: [
        ev("prior_session", "April 29, 2026 Vendor Outreach Session at 6700 Rockledge Drive, Bethesda", CLAIM_KIND.FACT, "https://sam.gov/workspace/contract/opp/f4e0c6bfa2a14c2195c8476fb81d5f9c/view", "SAM.gov NIH SBPO VOS", { researchMethodId: "GDI-GOV-EVENT-01", sourceAuthority: "Tier_A" }),
        ev("contact", "NIHSmallBusiness@od.nih.gov", CLAIM_KIND.FACT, "https://sam.gov/workspace/contract/opp/f4e0c6bfa2a14c2195c8476fb81d5f9c/view", "SAM.gov", { researchMethodId: "GDI-CONTACT-01" }),
      ],
      fitComponents: { physicalFit: 70, geographyFit: 90, timing: 55, commercialValue: 45, historicalFit: 55, competitiveAccessibility: 75, contactability: 65 },
      confidenceInput: { sourceAuthority: 90, independentSourceCount: 1, directness: 80, recency: 70, verifiedFieldRatio: 0.45, firstPartyShare: 90, completeness: 35, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-GOV-EVENT-01", "GDI-CONTACT-01"],
      labels: ["government_contractor", "recurring_pattern"],
    })
  );

  // 14 — Social/university weekend placeholder carefully labeled speculative
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_umd_alumni_weekend_watch",
      organizationName: "University of Maryland Alumni Association (watch)",
      title: "University of Maryland alumni / parents weekend demand — monitoring for 2027 dates",
      segment: "University / education",
      demandType: "alumni_weekend",
      demandStatus: DEMAND_STATUS.SPECULATIVE_WATCH,
      destinationStatus: "College Park / Bethesda drive market",
      venueStatus: "Campus-centric; hotels capture drive-in overflow",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      whyNow: "WATCH: No specific 2027 alumni weekend hotel RFP evidenced in this pilot pass.",
      fitExplanation: "Drive-market weekend demand may spill into Bethesda hotels when College Park sells out — evidence currently weak.",
      summaryWhat: "Speculative watch for UMD-related weekend room demand affecting Bethesda.",
      summaryWhyMatters: "Weekend fill opportunity if evidenced; not yet actionable.",
      summaryWhyHotel: "Suburban Bethesda vs College Park inventory.",
      recommendedAction: "Do not cold-call yet. Revisit when 2027 alumni calendar publishes housing partner.",
      likelyCompetitor: "College Park Marriott Hotel & Conference Center",
      evidence: [
        ev("status", "No verified 2027 alumni hotel block found in pilot pass", CLAIM_KIND.INFERENCE, null, "GDI pilot research note", { sourceAuthority: "Tier_D", confidence: 30, researchMethodId: "GDI-WEEKEND-01" }),
      ],
      fitComponents: { physicalFit: 65, geographyFit: 55, timing: 40, commercialValue: 40, historicalFit: 40, competitiveAccessibility: 50, contactability: 20 },
      confidenceInput: { sourceAuthority: 20, independentSourceCount: 0, directness: 20, recency: 40, verifiedFieldRatio: 0.05, firstPartyShare: 0, completeness: 15, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-WEEKEND-01"],
      labels: ["speculative", "weekend"],
    })
  );

  // 15 — Corporate meetings near Marriott HQ / Bethesda — careful
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_marriott_hq_adjacent_corporate_watch",
      organizationName: "Corporate meetings near Marriott HQ / Bethesda business district (category watch)",
      title: "Corporate offsite / training demand near Marriott HQ corridor — needs account-level evidence",
      segment: "Corporate",
      demandType: "corporate_meeting",
      demandStatus: DEMAND_STATUS.SPECULATIVE_WATCH,
      destinationStatus: "Bethesda / Marriott HQ area",
      venueStatus: "UNKNOWN",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      bookingWindowStatus: BOOKING_WINDOW.RESEARCH_FURTHER,
      whyNow:
        "RESEARCH FURTHER: Local corporate density is real, but public event calendars rarely publish specific 100–300 room offsites. Requires CRM/lost-business data or Ampfy/Apify enrichment — not invented here.",
      fitExplanation: "Full-service Marriott with meeting space is suitable for corporate training/offsites when evidenced.",
      summaryWhat: "Category watch for corporate group demand near Bethesda business district / Marriott HQ.",
      summaryWhyMatters: "Commercially important segment; public web evidence is thin by design.",
      summaryWhyHotel: "Brand alignment and meeting inventory.",
      recommendedAction:
        "Do not invent leads. Pull hotel CRM need dates / Delphi production groups if available. Optionally escalate Level-4 enrichment for named employers later.",
      likelyCompetitor: "Marriott Bethesda Downtown at Marriott HQ; Hyatt Regency Bethesda",
      evidence: [
        ev("category_note", "No specific public corporate offsite with dates/size verified in pilot", CLAIM_KIND.INFERENCE, null, "GDI pilot research note", { sourceAuthority: "Tier_D", confidence: 25 }),
      ],
      fitComponents: { physicalFit: 80, geographyFit: 85, timing: 40, commercialValue: 60, historicalFit: 50, competitiveAccessibility: 55, contactability: 15 },
      confidenceInput: { sourceAuthority: 15, independentSourceCount: 0, directness: 15, recency: 50, verifiedFieldRatio: 0.0, firstPartyShare: 0, completeness: 10, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-ANCHOR-01"],
      labels: ["corporate", "needs_crm", "speculative"],
    })
  );

  // 16 — Experimental Planner Consideration attached to AMWA (clone reference on AMWA opp via separate field - already on first)
  // Add a second strong mid opportunity: healthcare association with DC advocacy pattern - use AMWA-style second org
  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_ajas_hq_rockville_watch",
      organizationName: "Association of Jewish Aging Services (AJAS)",
      title: "AJAS Annual Conference 2027 — Fort Lauderdale (HQ Rockville; future DC-year watch)",
      segment: "Healthcare",
      demandType: "annual_conference",
      demandStatus: DEMAND_STATUS.SPECULATIVE_WATCH,
      eventStartDate: "2027-03-30",
      eventEndDate: "2027-04-02",
      destinationStatus: "Fort Lauderdale, FL (announced)",
      venueStatus: "Announced city; not Bethesda",
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      whyNow: "WATCH for future years: HQ is Rockville, MD — relationship value even when 2027 is elsewhere.",
      fitExplanation: "2027 geography is Florida; Bethesda unfit this cycle. HQ proximity may matter for board/leadership meetings.",
      summaryWhat: "AJAS 2027 Annual Conference March 30–April 2 in Fort Lauderdale; organization HQ in Rockville.",
      summaryWhyMatters: "Local healthcare association HQ may generate smaller leadership meetings in Montgomery County.",
      summaryWhyHotel: "Not for 2027 annual; possible smaller HQ-area meetings (unverified).",
      recommendedAction: "Do not pursue 2027 annual. Optionally ask about board/leadership meeting needs in Rockville/Bethesda.",
      likelyCompetitor: "Fort Lauderdale host hotels (2027)",
      evidence: [
        ev("hq", "Rockville, Maryland", CLAIM_KIND.FACT, "https://www.linkedin.com/company/association-of-jewish-aging-services-ajas-", "AJAS LinkedIn", { sourceAuthority: "Tier_B", researchMethodId: "GDI-ASSOC-EVENT-01" }),
        ev("2027_destination", "Fort Lauderdale, Florida", CLAIM_KIND.FACT, "https://www.linkedin.com/company/association-of-jewish-aging-services-ajas-", "AJAS LinkedIn save-the-date post", { sourceAuthority: "Tier_B" }),
      ],
      fitComponents: { physicalFit: 70, geographyFit: 25, timing: 35, commercialValue: 40, historicalFit: 45, competitiveAccessibility: 30, contactability: 25 },
      confidenceInput: { sourceAuthority: 55, independentSourceCount: 1, directness: 60, recency: 70, verifiedFieldRatio: 0.4, firstPartyShare: 20, completeness: 35, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-ASSOC-EVENT-01"],
      labels: ["hq_local", "watch"],
    })
  );

  // --- Webhound L5 incremental opportunities (session 4f99b00b…, $5.00) ---

  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_nice_2027",
      organizationName: "NIST / National Initiative for Cybersecurity Education (NICE)",
      title: "18th Annual NICE Conference and Expo 2027 — location TBD (Jun 7–9)",
      segment: "Government",
      demandType: "conference_expo",
      demandStatus: DEMAND_STATUS.EMERGING_DEMAND,
      eventStartDate: "2027-06-07",
      eventEndDate: "2027-06-09",
      destinationStatus: "Location to be announced",
      venueStatus: "NOT_ANNOUNCED",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      estimatedNights: 3,
      estimatedNightsClaimKind: CLAIM_KIND.INFERENCE,
      bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
      whyNow:
        "CONTACT NOW: Official NIST page publishes June 7–9, 2027 with location still TBA — open site-selection window for a mid-size federal cybersecurity conference.",
      fitExplanation:
        "Full-service meeting inventory on the I-270 / Montgomery County federal-tech corridor is a plausible host profile if NICE stays in the DC metro / Maryland area (INFERENCE).",
      summaryWhat: "NIST NICE 18th Annual Conference and Expo, June 7–9, 2027; location TBA.",
      summaryWhyMatters: "Open-venue government conference — rare clean RFP window vs already-contracted DC citywides.",
      summaryWhyHotel: "Meeting space + suburban federal-tech access thesis (INFERENCE until geography announced).",
      recommendedAction:
        "Contact nice@nist.gov this week. Ask whether 2027 site selection is open and whether Maryland / Bethesda is under consideration. Lead with meeting capacity and federal-visitor logistics.",
      likelyCompetitor: "Gaithersburg / North Bethesda conference hotels",
      competitorRationale: "NIST proximity competitors if Maryland is shortlisted (INFERENCE).",
      primaryContact: {
        name: CLAIM_KIND.UNKNOWN,
        role: "NICE program contact",
        organization: "NIST NICE",
        email: "nice@nist.gov",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://www.nist.gov/news-events/events/18th-annual-nice-conference-and-expo",
        confidence: 80,
      },
      evidence: [
        ev("event_dates", "June 7–9, 2027", CLAIM_KIND.FACT, "https://www.nist.gov/news-events/events/18th-annual-nice-conference-and-expo", "NIST NICE 2027", { researchMethodId: "GDI-GOV-EVENT-01", researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH" }),
        ev("venue", "Location to be announced", CLAIM_KIND.FACT, "https://www.nist.gov/news-events/events/18th-annual-nice-conference-and-expo", "NIST NICE 2027", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH" }),
        ev("contact", "nice@nist.gov", CLAIM_KIND.FACT, "https://www.nist.gov/news-events/events/18th-annual-nice-conference-and-expo", "NIST NICE", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH" }),
      ],
      fitComponents: { physicalFit: 78, geographyFit: 70, timing: 86, commercialValue: 68, historicalFit: 55, competitiveAccessibility: 65, contactability: 80 },
      confidenceInput: { sourceAuthority: 92, independentSourceCount: 1, directness: 90, recency: 90, verifiedFieldRatio: 0.6, firstPartyShare: 95, completeness: 45, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-GOV-EVENT-01"],
      webhoundUsed: true,
      labels: ["webhound", "government", "entering_booking_window"],
    })
  );

  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_potomac_memorial_2027",
      organizationName: "Potomac Soccer Association",
      title: "47th Annual Potomac Memorial Tournament 2027 — stay-to-play weekend demand",
      segment: "Weekend group",
      demandType: "sports_tournament",
      demandStatus: DEMAND_STATUS.RECURRING_PREDICTED,
      eventStartDate: null,
      eventEndDate: null,
      destinationStatus: "Maryland SoccerPlex / Montgomery County",
      venueStatus: "Fields-based; housing via stay-to-play platform",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      estimatedNights: 3,
      estimatedNightsClaimKind: CLAIM_KIND.ESTIMATED,
      bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
      whyNow:
        "CONTACT NOW: 2026 ran Memorial Day weekend (~450 teams, stay-to-play). 2027 housing list planning should happen before Roomvy inventory locks.",
      fitExplanation:
        "Large recurring Montgomery County tournament with mandatory hotel booking for out-of-area teams — strong weekend group path if added to housing inventory.",
      summaryWhat: "Elite boys soccer tournament (~450 teams pattern) at Maryland SoccerPlex with stay-to-play housing.",
      summaryWhyMatters: "Policy-driven hotel demand across a holiday weekend — commercially actionable if on the housing list.",
      summaryWhyHotel: "I-270 access to SoccerPlex; full-service option vs select-service Germantown/Silver Spring competitors.",
      recommendedAction:
        "Contact Tournament Director Kathy Hauschild (tournament@potomacsoccer.org) and request inclusion on 2027 Roomvy / housing inventory.",
      likelyCompetitor: "DoubleTree Silver Spring; Fairfield Inn Germantown; Spark by Hilton Germantown",
      competitorRationale: "Named on 2026 Roomvy housing inventory (FACT per Webhound).",
      primaryContact: {
        name: "Kathy Hauschild",
        role: "Tournament Director",
        organization: "Potomac Soccer Association",
        email: "tournament@potomacsoccer.org",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://potomacsoccer.org/memorial_tournament/potomac-memorial-tournament/",
        confidence: 85,
      },
      evidence: [
        ev("team_count_pattern", "~450 teams (2026 materials)", CLAIM_KIND.FACT, "https://potomacsoccer.org/memorial_tournament/potomac-memorial-tournament/", "Potomac Memorial Tournament", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH", researchMethodId: "GDI-WEEKEND-01" }),
        ev("stay_to_play", "Teams outside 100 miles must book through housing system", CLAIM_KIND.FACT, "https://potomacsoccer.org/memorial_tournament/potomac-memorial-tournament/", "Potomac Memorial", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH" }),
        ev("contact", "Kathy Hauschild tournament@potomacsoccer.org", CLAIM_KIND.FACT, "https://potomacsoccer.org/memorial_tournament/potomac-memorial-tournament/", "Potomac Memorial", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH" }),
        ev("2027_dates", "Exact 2027 dates not yet posted; Memorial Day pattern", CLAIM_KIND.INFERENCE, "https://potomacsoccer.org/memorial_tournament/potomac-memorial-tournament/", "Potomac Memorial", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH", sourceAuthority: "Tier_D" }),
      ],
      fitComponents: { physicalFit: 74, geographyFit: 88, timing: 80, commercialValue: 78, historicalFit: 65, competitiveAccessibility: 72, contactability: 85 },
      confidenceInput: { sourceAuthority: 85, independentSourceCount: 2, directness: 85, recency: 80, verifiedFieldRatio: 0.55, firstPartyShare: 75, completeness: 50, conflictPenalty: 5 },
      researchMethodsAttempted: ["GDI-WEEKEND-01", "GDI-CONTACT-01"],
      webhoundUsed: true,
      labels: ["webhound", "weekend", "sports"],
    })
  );

  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_show_2026_nih",
      organizationName: "NIH / NHLBI",
      title: "SHOW 2026 — NIH Research Conference on Sleep and the Health of Women (Oct 14–16)",
      segment: "Scientific",
      demandType: "scientific_conference",
      demandStatus: DEMAND_STATUS.CONFIRMED_DEMAND,
      eventStartDate: "2026-10-14",
      eventEndDate: "2026-10-16",
      destinationStatus: "NIH Main Campus, Bethesda",
      venueStatus: "Natcher Conference Center (campus — not hotel-hosted)",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
      whyNow:
        "CONTACT NOW for overflow lodging: campus venue confirmed for Oct 14–16, 2026; remaining speaker/attendee room nights still sellable.",
      fitExplanation: "Closest full-service Marriott adjacency to NIH campus for overnight guests (INFERENCE).",
      summaryWhat: "NIH/NHLBI scientific conference on NIH Bethesda campus Oct 14–16, 2026.",
      summaryWhyMatters: "Near-term NIH-adjacent lodging demand with official dates.",
      summaryWhyHotel: "Campus adjacency vs downtown DC hotels.",
      recommendedAction:
        "Offer a short NIH-campus conference rate window for SHOW 2026 attendees/speakers. Capture organizer contact via NHLBI event page; do not claim host-hotel status.",
      likelyCompetitor: "Bethesda North Marriott; downtown Bethesda hotels",
      primaryContact: {
        name: CLAIM_KIND.UNKNOWN,
        role: "NHLBI event registration (planner name not public in research)",
        organization: "NIH / NHLBI",
        claimKind: CLAIM_KIND.UNKNOWN,
        sourceUrl: "https://www.nhlbi.nih.gov/events/2026/2026-nih-research-conference-sleep-and-health-women-show-2026",
      },
      evidence: [
        ev("event_dates", "October 14–16, 2026", CLAIM_KIND.FACT, "https://www.nhlbi.nih.gov/events/2026/2026-nih-research-conference-sleep-and-health-women-show-2026", "NHLBI SHOW 2026", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH", researchMethodId: "GDI-MED-SCI-01" }),
        ev("venue", "Natcher Conference Center, NIH Main Campus", CLAIM_KIND.FACT, "https://www.nhlbi.nih.gov/events/2026/2026-nih-research-conference-sleep-and-health-women-show-2026", "NHLBI SHOW 2026", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH" }),
      ],
      fitComponents: { physicalFit: 62, geographyFit: 95, timing: 78, commercialValue: 48, historicalFit: 50, competitiveAccessibility: 70, contactability: 35 },
      confidenceInput: { sourceAuthority: 95, independentSourceCount: 2, directness: 90, recency: 90, verifiedFieldRatio: 0.55, firstPartyShare: 90, completeness: 40, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-MED-SCI-01"],
      webhoundUsed: true,
      labels: ["webhound", "nih", "scientific"],
    })
  );

  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_afcea_hits_2027",
      organizationName: "AFCEA Bethesda Chapter",
      title: "AFCEA Bethesda Health IT Summit 2027 — booked at Bethesda North Marriott (overflow / 2028 cycle)",
      segment: "Government contractor",
      demandType: "summit",
      demandStatus: DEMAND_STATUS.CONFIRMED_DEMAND,
      eventStartDate: "2027-01-25",
      eventEndDate: "2027-01-26",
      destinationStatus: "Rockville / North Bethesda",
      venueStatus: "Bethesda North Marriott Hotel & Conference Center (announced)",
      estimatedAttendance: "1400+",
      estimatedAttendanceClaimKind: CLAIM_KIND.FACT,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      bookingWindowStatus: BOOKING_WINDOW.RESEARCH_FURTHER,
      whyNow:
        "RESEARCH FURTHER: 2027 host is competitor Bethesda North Marriott. Pursue overflow / exhibitor lodging and begin 2028 host conversation now.",
      fitExplanation:
        "Not primary host for 2027. 1,400+ registration pattern (2025 recap) implies overflow and future-cycle value.",
      summaryWhat: "AFCEA Bethesda Health IT Summit Jan 25–26, 2027 at Bethesda North Marriott.",
      summaryWhyMatters: "Largest local federal health-IT gathering; competitor already won 2027.",
      summaryWhyHotel: "Same micro-market; overflow + 2028 RFP positioning.",
      recommendedAction:
        "Contact registrat@afceabethesda.org: (1) ask about overflow lodging needs for Jan 2027; (2) request to be considered for 2028 host RFP.",
      likelyCompetitor: "Bethesda North Marriott Hotel & Conference Center",
      competitorRationale: "Announced 2027 venue (FACT).",
      primaryContact: {
        name: CLAIM_KIND.UNKNOWN,
        role: "AFCEA Bethesda registration",
        organization: "AFCEA Bethesda",
        email: "registrat@afceabethesda.org",
        phone: "571-323-2587",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://bethesda.afceachapters.org/event/2027-health-it-summit/",
        confidence: 80,
      },
      evidence: [
        ev("venue", "Bethesda North Marriott Hotel & Conference Center", CLAIM_KIND.FACT, "https://bethesda.afceachapters.org/event/2027-health-it-summit/", "AFCEA Bethesda 2027 HITS", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH" }),
        ev("attendance_pattern", "1,400+ registrations in 2025 recap", CLAIM_KIND.FACT, "https://bethesdaevents.afceachapters.org/HITS26", "AFCEA HITS26", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH" }),
      ],
      fitComponents: { physicalFit: 70, geographyFit: 80, timing: 60, commercialValue: 55, historicalFit: 60, competitiveAccessibility: 40, contactability: 75 },
      confidenceInput: { sourceAuthority: 90, independentSourceCount: 2, directness: 90, recency: 85, verifiedFieldRatio: 0.7, firstPartyShare: 85, completeness: 55, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-GOV-EVENT-01", "GDI-CONTACT-01"],
      webhoundUsed: true,
      labels: ["webhound", "competitor_risk", "government_contractor"],
    })
  );

  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_georgetown_homecoming_2027",
      organizationName: "Georgetown University",
      title: "Georgetown Homecoming Weekend 2027 — recommended-hotel outreach window",
      segment: "University / education",
      demandType: "homecoming",
      demandStatus: DEMAND_STATUS.RECURRING_PREDICTED,
      eventStartDate: null,
      eventEndDate: null,
      destinationStatus: "Georgetown University campus",
      venueStatus: "Campus-centric; 2027 hotel list not yet evidenced",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      whyNow:
        "WATCH / early outreach: 2026 Homecoming Oct 2–3 evidenced; 2027 date TBD and no official hotel list found yet — earlier opening than Reunion (already blocked).",
      fitExplanation: "Suburban full-service option for alumni who prefer Bethesda vs Georgetown/Dupont inventory (INFERENCE).",
      summaryWhat: "Recurring Georgetown Homecoming weekend; 2027 dates not yet posted.",
      summaryWhyMatters: "Weekend university demand with possible recommended-hotel list formation.",
      summaryWhyHotel: "Full-service Bethesda alternative for drive-market alumni (INFERENCE).",
      recommendedAction:
        "Email advancementevents@georgetown.edu to ask about 2027 Homecoming recommended hotels before the list locks.",
      likelyCompetitor: "Georgetown / Rosslyn / Arlington hotels used for Reunion blocks",
      primaryContact: {
        name: CLAIM_KIND.UNKNOWN,
        role: "Georgetown Advancement Events",
        organization: "Georgetown University",
        email: "advancementevents@georgetown.edu",
        phone: "202-687-2064",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://homecoming.georgetown.edu/",
        confidence: 75,
      },
      evidence: [
        ev("2026_dates", "October 2–3, 2026 Homecoming", CLAIM_KIND.FACT, "https://homecoming.georgetown.edu/", "Georgetown Homecoming", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH", researchMethodId: "GDI-WEEKEND-01" }),
        ev("contact", "advancementevents@georgetown.edu", CLAIM_KIND.FACT, "https://reunion.georgetown.edu/", "Georgetown Reunion", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH" }),
      ],
      fitComponents: { physicalFit: 68, geographyFit: 55, timing: 58, commercialValue: 50, historicalFit: 45, competitiveAccessibility: 45, contactability: 70 },
      confidenceInput: { sourceAuthority: 80, independentSourceCount: 1, directness: 70, recency: 80, verifiedFieldRatio: 0.35, firstPartyShare: 80, completeness: 30, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-WEEKEND-01", "GDI-CONTACT-01"],
      webhoundUsed: true,
      labels: ["webhound", "university", "weekend"],
    })
  );

  candidates.push(
    buildOpportunity({
      hotelId,
      id: "gdi_opp_msysa_state_cup_2027",
      organizationName: "Maryland State Youth Soccer Association (MSYSA)",
      title: "2027 MSYSA Spring State Cup Championships — multi-weekend SoccerPlex demand",
      segment: "Sports",
      demandType: "championship_tournament",
      demandStatus: DEMAND_STATUS.RECURRING_PREDICTED,
      destinationStatus: "Maryland SoccerPlex / Liberty Sports Park pattern",
      venueStatus: "Fields-based",
      estimatedAttendance: CLAIM_KIND.UNKNOWN,
      estimatedPeakRooms: CLAIM_KIND.UNKNOWN,
      bookingWindowStatus: BOOKING_WINDOW.RESEARCH_FURTHER,
      whyNow:
        "RESEARCH FURTHER: 2026 State Cup used April–May weekends at SoccerPlex/Liberty. 2027 schedule not posted — recommended-hotel outreach before applications ramp.",
      fitExplanation: "Multi-weekend youth championship travel; more open than stay-to-play events if no mandatory housing partner (INFERENCE).",
      summaryWhat: "MSYSA Spring State Cup championship weekends — 2027 dates TBD.",
      summaryWhyMatters: "Recurring multi-weekend room demand from out-of-area teams/families.",
      summaryWhyHotel: "I-270 access to SoccerPlex corridor.",
      recommendedAction:
        "Contact Brad Roos, MSYSA Cups Director (cups@msysa.org) about 2027 recommended hotels / team travel partners.",
      likelyCompetitor: "Germantown / Gaithersburg hotels",
      primaryContact: {
        name: "Brad Roos",
        role: "MSYSA Cups Director",
        organization: "MSYSA",
        email: "cups@msysa.org",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://www.msysa.org/programs-landing-page/state-cup/",
        confidence: 80,
      },
      evidence: [
        ev("contact", "Brad Roos cups@msysa.org", CLAIM_KIND.FACT, "https://www.msysa.org/programs-landing-page/state-cup/", "MSYSA State Cup", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH", researchMethodId: "GDI-WEEKEND-01" }),
        ev("2026_venue_pattern", "Maryland SoccerPlex and Liberty Sports Park", CLAIM_KIND.FACT, "https://www.msysa.org/winter-spring-state-cup-event-details/", "MSYSA State Cup details", { researchProvider: "webhound", researchLevel: "L5_WEBHOUND_DEEP_RESEARCH" }),
      ],
      fitComponents: { physicalFit: 70, geographyFit: 82, timing: 62, commercialValue: 58, historicalFit: 55, competitiveAccessibility: 68, contactability: 80 },
      confidenceInput: { sourceAuthority: 80, independentSourceCount: 1, directness: 75, recency: 75, verifiedFieldRatio: 0.4, firstPartyShare: 80, completeness: 35, conflictPenalty: 0 },
      researchMethodsAttempted: ["GDI-WEEKEND-01", "GDI-CONTACT-01"],
      webhoundUsed: true,
      labels: ["webhound", "sports", "weekend"],
    })
  );

  // Attach experimental planner consideration to AMWA high priority
  const amwa = candidates.find((c) => c.id === "gdi_opp_amwa_2027_annual");
  if (amwa) {
    amwa.webhoundUsed = true;
    amwa.plannerConsideration = {
      experimental: true,
      label: "EXPERIMENTAL",
      note: "Does not affect official ADP. Simulated planner discovery scenario for product validation.",
      provider: "dealality_gdi_experimental_v1",
      prompt:
        "Best Bethesda hotels for a 150–250 person medical association conference near NIH with meeting space and approximately 100–175 guestrooms.",
      timestamp: new Date().toISOString(),
      hotelsReturned: [
        "Hyatt Regency Bethesda",
        "Bethesda Marriott",
        "The Bethesdan Hotel, Tapestry Collection by Hilton",
        "Bethesda North Marriott Hotel & Conference Center",
      ],
      bethesdaAppeared: true,
      bethesdaRank: 2,
      competitors: ["Hyatt Regency Bethesda", "Bethesda North Marriott Hotel & Conference Center"],
      citations: [],
      observations:
        "EXPERIMENTAL only: In this controlled scenario framing, Bethesda Marriott appears in the local medical/NIH consideration set behind Hyatt Regency Bethesda’s Metro-core positioning. Not an official ADP Presence Index observation.",
    };
  }

  const premier = candidates.find((c) => c.id === "gdi_opp_bethesda_premier_cup_2026");
  if (premier) premier.webhoundUsed = true;

  return candidates;
}
