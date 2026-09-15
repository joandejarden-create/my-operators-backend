/**
 * Apply Level-3 deepen updates + Webhound L5 merge patches to Bethesda opportunities.
 * Does not invent room blocks. Promotes only when evidence justifies.
 */

import { buildOpportunity } from "./opportunity-factory.js";
import {
  BOOKING_WINDOW,
  CLAIM_KIND,
  DEMAND_STATUS,
} from "./claim-types.js";
import { PILOT_HOTEL_ID } from "./hotel-profile.js";
import { buildAssociationDiscoveryOpportunitiesV2 } from "./discovery-opportunities-v2.js";

const ACCESS = "2026-09-12";
const WH_DEEPEN = "1716a70c-1e4c-45d9-b905-525c4c97a933";

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

/**
 * Patch existing opportunity list in place (by id) and append new ones.
 * @param {object[]} base
 * @param {{ webhoundDeepen?: object, webhoundDiscovery?: object }} patches
 */
export function applyBethesdaDeepenPass(base, patches = {}) {
  const byId = new Map((base || []).map((o) => [o.id, { ...o }]));
  const roi = [];

  // --- NICE deepen (L3): 400+ attendance, Philly Marriott 2026, venue still TBA ---
  const nice = byId.get("gdi_opp_nice_2027");
  if (nice) {
    const before = {
      priority: nice.priority,
      fit: nice.hotelFitScore,
      conf: nice.evidenceConfidence,
      booking: nice.bookingWindowStatus,
    };
    nice.meetingHistory = [
      {
        year: 2026,
        city: "Philadelphia, PA",
        venue: "Philadelphia Marriott Downtown",
        attendance: "400+",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://www.nist.gov/news-events/news/2026/08/foundations-future-insights-2026-nice-conference",
      },
      {
        year: 2025,
        city: "Denver, CO",
        venue: CLAIM_KIND.UNKNOWN,
        attendance: "450+",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://www.nist.gov/news-events/news/2025/06/climbing-higher-together-insights-nice-2025-conference",
      },
    ];
    nice.estimatedAttendance = "400-450";
    nice.estimatedAttendanceClaimKind = CLAIM_KIND.ESTIMATED;
    nice.estimatedPeakRooms = CLAIM_KIND.UNKNOWN;
    nice.estimatedPeakRoomsClaimKind = CLAIM_KIND.UNKNOWN;
    nice.fitExplanation =
      "Prior years ~400–450 attendees at full-service Marriott (2026 Philadelphia Marriott Downtown). Venue still TBA for 2027 — Bethesda/Gaithersburg corridor is plausible given NIST HQ in Gaithersburg (INFERENCE).";
    nice.whyNow =
      "CONTACT NOW: Official NIST page still lists Location to be announced for June 7–9, 2027. Historical attendance (~400–450) is mid-size and consistent with Bethesda Marriott meeting inventory. Lead time ~9 months.";
    nice.recommendedAction =
      "Email nice@nist.gov and info@niceconference.org this week. Prioritize FIU Gordon Institute venue influencers (Mike Asencio / Brian Fonseca) — FIU/New America operate logistics under NIST award, not a public federal venue RFP. Ask whether 2027 site selection is open and whether Maryland return (Gaithersburg/Bethesda/Rockville) is under consideration. Cite Marriott-family host pattern and ~400+ recent attendance.";
    nice.primaryContact = {
      name: "NIST NICE Program Office / FIU conference ops",
      role: "Program + venue logistics",
      organization: "NIST NICE / FIU",
      email: "nice@nist.gov",
      phone: "301-975-4470",
      claimKind: CLAIM_KIND.FACT,
      sourceUrl: "https://www.nist.gov/itl/applied-cybersecurity/nice/about/meet-staff",
      confidence: 88,
    };
    nice.evidence = [
      ...(nice.evidence || []),
      ev(
        "2026_attendance",
        "Over 400 participants",
        CLAIM_KIND.FACT,
        "https://www.nist.gov/news-events/news/2026/08/foundations-future-insights-2026-nice-conference",
        "NIST 2026 NICE recap",
        { researchLevel: "L3_STANDARD_APPROVED_WEB_RESEARCH" }
      ),
      ev(
        "2026_venue",
        "Philadelphia Marriott Downtown",
        CLAIM_KIND.FACT,
        "https://www.nist.gov/news-events/events/18th-annual-nice-conference-and-expo",
        "NIST / NICE host pattern (Webhound L5)",
        { sourceAuthority: "Tier_A", confidence: 85, researchProvider: "webhound", researchLevel: "L5_WEBHOUND" }
      ),
      ev(
        "2025_attendance",
        "Over 450 participants in Denver",
        CLAIM_KIND.FACT,
        "https://www.nist.gov/news-events/news/2025/06/climbing-higher-together-insights-nice-2025-conference",
        "NIST 2025 NICE recap"
      ),
      ev(
        "2027_venue_status",
        "Location to be announced",
        CLAIM_KIND.FACT,
        "https://www.nist.gov/news-events/events/18th-annual-nice-conference-and-expo",
        "NIST NICE 2027"
      ),
      ev(
        "organizer_ops",
        "FIU / New America operate conference logistics under NIST financial assistance award (not public federal venue bid)",
        CLAIM_KIND.FACT,
        "https://it.fiu.edu/national-initiative-for-cybersecurity-education-nice-conference/",
        "FIU NICE conference page",
        { researchProvider: "webhound", researchLevel: "L5_WEBHOUND", confidence: 82 }
      ),
      ev(
        "contact_phone",
        "301-975-4470 / nice@nist.gov",
        CLAIM_KIND.FACT,
        "https://www.nist.gov/itl/applied-cybersecurity/nice/about/meet-staff",
        "NIST NICE staff",
        { researchMethodId: "GDI-CONTACT-01", researchProvider: "webhound", researchLevel: "L5_WEBHOUND" }
      ),
    ];
    // Recompute via buildOpportunity components bump
    const rebuilt = buildOpportunity({
      ...nice,
      id: nice.id,
      hotelId: PILOT_HOTEL_ID,
      fitComponents: {
        physicalFit: 84,
        geographyFit: 74,
        timing: 90,
        commercialValue: 80,
        historicalFit: 82,
        competitiveAccessibility: 72,
        contactability: 88,
      },
      confidenceInput: {
        sourceAuthority: 94,
        independentSourceCount: 5,
        directness: 92,
        recency: 95,
        verifiedFieldRatio: 0.82,
        firstPartyShare: 90,
        completeness: 72,
        conflictPenalty: 0,
      },
    });
    byId.set(nice.id, {
      ...rebuilt,
      webhoundUsed: true,
      labels: [...new Set([...(nice.labels || []), "deepened_l3", "deepened_l5", "entering_booking_window"])],
    });
    const after = byId.get(nice.id);
    roi.push({
      opportunityId: nice.id,
      researchQuestion: "NICE 2027 venue status, historical hotels, attendance, Maryland fit, FIU ops",
      provider: "L3_web + webhound_deepen",
      costUsd: 1.25,
      before,
      after: {
        priority: after.priority,
        fit: after.hotelFitScore,
        conf: after.evidenceConfidence,
        booking: after.bookingWindowStatus,
      },
      materialChange:
        before.priority !== after.priority ||
        Math.abs(before.conf - after.evidenceConfidence) >= 5,
    });
  }

  // --- SHOW 2026 deepen: registration open to Sep 21; contact Jessica ---
  const show = byId.get("gdi_opp_show_2026_nih");
  if (show) {
    const before = {
      priority: show.priority,
      fit: show.hotelFitScore,
      conf: show.evidenceConfidence,
      booking: show.bookingWindowStatus,
    };
    const rebuilt = buildOpportunity({
      ...show,
      id: show.id,
      hotelId: PILOT_HOTEL_ID,
      primaryContact: {
        name: "Jessica Freer / Jessica Mitchell",
        role: "Event contact (public Cvent)",
        organization: "NIH / NHLBI",
        email: "Jessica.Mitchell@nih.gov",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://web.cvent.com/event/225a615f-cd58-4c72-8191-38a489105c2d/summary",
        confidence: 85,
      },
      whyNow:
        "CONTACT NOW: In-person registration open through September 21, 2026. No official hotel block — this is a late-cycle self-book / recommended-hotel play, not a contracted overflow RFP. NIH/Natcher lodging precedent already listed Bethesda Marriott (5151 Pooks Hill) for a prior Natcher workshop.",
      recommendedAction:
        "Email Jessica.Mitchell@nih.gov (Jessica Freer) offering a short SHOW 2026 attendee rate code and NIH access/parking guidance. Ask whether NHLBI will add a recommended lodging note before Sep 21 registration close. Do not claim host-hotel status.",
      evidence: [
        ...(show.evidence || []),
        ev(
          "registration_deadline",
          "Registration through September 21, 2026",
          CLAIM_KIND.FACT,
          "https://www.nhlbi.nih.gov/events/2026/2026-nih-research-conference-sleep-and-health-women-show-2026",
          "NHLBI SHOW 2026",
          { researchMethodId: "GDI-MED-SCI-01" }
        ),
        ev(
          "contact_email",
          "Jessica.Mitchell@nih.gov",
          CLAIM_KIND.FACT,
          "https://web.cvent.com/event/225a615f-cd58-4c72-8191-38a489105c2d/summary",
          "SHOW 2026 Cvent",
          { researchMethodId: "GDI-CONTACT-01" }
        ),
        ev(
          "hotel_block",
          "No dedicated hotel housing block found on official pages",
          CLAIM_KIND.FACT,
          "https://web.cvent.com/event/225a615f-cd58-4c72-8191-38a489105c2d/summary",
          "SHOW 2026 Cvent",
          { confidence: 70 }
        ),
        ev(
          "natcher_lodging_precedent",
          "Bethesda Marriott, 5151 Pooks Hill Road listed first on a prior NIH/Natcher logistics lodging list (BRAIN NeuroAI Workshop 2024)",
          CLAIM_KIND.FACT,
          "https://n4solutionsllc.com/brain-neuroai-logistics/",
          "Natcher lodging logistics (secondary but specific)",
          {
            researchProvider: "webhound",
            researchLevel: "L5_WEBHOUND",
            confidence: 78,
            sourceAuthority: "Tier_B",
          }
        ),
        ev(
          "format_ceiling",
          "Free hybrid conference — managed block demand likely modest vs paid association meetings",
          CLAIM_KIND.INFERENCE,
          "https://www.nhlbi.nih.gov/events/2026/2026-nih-research-conference-sleep-and-health-women-show-2026",
          "NHLBI SHOW 2026 format",
          { researchProvider: "webhound", researchLevel: "L5_WEBHOUND", confidence: 70 }
        ),
      ],
      fitComponents: {
        physicalFit: 64,
        geographyFit: 96,
        timing: 88,
        commercialValue: 48,
        historicalFit: 62,
        competitiveAccessibility: 78,
        contactability: 86,
      },
      confidenceInput: {
        sourceAuthority: 95,
        independentSourceCount: 4,
        directness: 92,
        recency: 95,
        verifiedFieldRatio: 0.78,
        firstPartyShare: 90,
        completeness: 62,
        conflictPenalty: 0,
      },
    });
    byId.set(show.id, {
      ...rebuilt,
      webhoundUsed: true,
      labels: [...new Set([...(show.labels || []), "deepened_l3", "deepened_l5", "new_this_week"])],
    });
    const after = byId.get(show.id);
    roi.push({
      opportunityId: show.id,
      researchQuestion: "SHOW 2026 housing status, registration window, contact, Natcher lodging precedent",
      provider: "L3_web + webhound_deepen",
      costUsd: 1.25,
      before,
      after: {
        priority: after.priority,
        fit: after.hotelFitScore,
        conf: after.evidenceConfidence,
        booking: after.bookingWindowStatus,
      },
      materialChange: true,
      note: "Kept Medium — free/hybrid format limits contracted block ceiling despite High contactability",
    });
  }

  // --- AFCEA L5: 2027 host closed at WASBN; overflow + relationship path ---
  const afcea = byId.get("gdi_opp_afcea_hits_2027");
  if (afcea) {
    const before = {
      priority: afcea.priority,
      fit: afcea.hotelFitScore,
      conf: afcea.evidenceConfidence,
      booking: afcea.bookingWindowStatus,
    };
    const rebuilt = buildOpportunity({
      ...afcea,
      id: afcea.id,
      hotelId: PILOT_HOTEL_ID,
      demandStatus: DEMAND_STATUS.KNOWN_DEMAND,
      venueStatus: "2027 confirmed at Bethesda North Marriott (WASBN) Jan 25–26",
      bookingWindowStatus: BOOKING_WINDOW.RESEARCH_FURTHER,
      whyNow:
        "2027 host opportunity closed at Bethesda North Marriott. Pursue overflow lodging for Jan 25–26, 2027 and cultivate smaller AFCEA Bethesda events (preview programming already uses non-WASBN sites).",
      fitExplanation:
        "WASBN incumbency + conference-center format for 1,400+ registrations make 2028 displacement low-probability. WASBT is overflow/relationship play, not 2027 host competitor.",
      recommendedAction:
        "Email registrar@afceabethesda.org (571-323-2587). Copy Andrea Snader (VP, Health IT Summit) and Justin Fessler (Co-VP). Offer overflow lodging for 2027 and propose WASBT for a smaller preview/board function before any 2028 host conversation.",
      primaryContact: {
        name: "Andrea Snader / Justin Fessler",
        role: "VP / Co-VP, Health IT Summit",
        organization: "AFCEA Bethesda",
        email: "registrar@afceabethesda.org",
        phone: "571-323-2587",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://bethesda.afceachapters.org/board-of-directors/",
        confidence: 85,
      },
      likelyCompetitor: "Bethesda North Marriott Hotel & Conference Center (incumbent host)",
      evidence: [
        ...(afcea.evidence || []),
        ev(
          "2027_host",
          "Jan 25–26, 2027 at Bethesda North Marriott",
          CLAIM_KIND.FACT,
          "https://bethesda.afceachapters.org/event/2027-health-it-summit/",
          "AFCEA Bethesda 2027 Health IT Summit",
          { researchProvider: "webhound", researchLevel: "L5_WEBHOUND" }
        ),
        ev(
          "2026_scale",
          "1,400+ registrations; host rate $229 at WASBN",
          CLAIM_KIND.FACT,
          "https://bethesdaevents.afceachapters.org/HITS26",
          "AFCEA HITS 2026",
          { researchProvider: "webhound", researchLevel: "L5_WEBHOUND" }
        ),
        ev(
          "contacts",
          "registrar@afceabethesda.org; Andrea Snader; Justin Fessler; Katie Keegan; Jessica Smith",
          CLAIM_KIND.FACT,
          "https://bethesda.afceachapters.org/board-of-directors/",
          "AFCEA Bethesda board",
          { researchMethodId: "GDI-CONTACT-01", researchProvider: "webhound", researchLevel: "L5_WEBHOUND" }
        ),
      ],
      fitComponents: {
        physicalFit: 70,
        geographyFit: 88,
        timing: 55,
        commercialValue: 58,
        historicalFit: 40,
        competitiveAccessibility: 45,
        contactability: 82,
      },
      confidenceInput: {
        sourceAuthority: 90,
        independentSourceCount: 4,
        directness: 90,
        recency: 92,
        verifiedFieldRatio: 0.75,
        firstPartyShare: 85,
        completeness: 70,
        conflictPenalty: 0,
      },
    });
    byId.set(afcea.id, {
      ...rebuilt,
      webhoundUsed: true,
      labels: [...new Set([...(afcea.labels || []), "deepened_l5", "overflow_only"])],
    });
    const after = byId.get(afcea.id);
    roi.push({
      opportunityId: afcea.id,
      researchQuestion: "AFCEA 2027 host status, overflow path, 2028 displacement odds, contacts",
      provider: "webhound_deepen",
      costUsd: 1.25,
      before,
      after: {
        priority: after.priority,
        fit: after.hotelFitScore,
        conf: after.evidenceConfidence,
        booking: after.bookingWindowStatus,
      },
      materialChange: true,
      note: "Confirmed not hostable in 2027; contactability up; stay Medium",
    });
  }

  // --- MSYSA L5: weak geography → Watchlist ---
  const msysa = byId.get("gdi_opp_msysa_state_cup_2027");
  if (msysa) {
    const before = {
      priority: msysa.priority,
      fit: msysa.hotelFitScore,
      conf: msysa.evidenceConfidence,
      booking: msysa.bookingWindowStatus,
    };
    const rebuilt = buildOpportunity({
      ...msysa,
      id: msysa.id,
      hotelId: PILOT_HOTEL_ID,
      demandStatus: DEMAND_STATUS.SPECULATIVE_WATCH,
      bookingWindowStatus: BOOKING_WINDOW.WATCH,
      whyNow:
        "WATCH: 2027 dates not posted. No stay-to-play or housing partner found. SoccerPlex (Boyds) is closer to Gaithersburg/Germantown hotels than Bethesda Marriott.",
      fitExplanation:
        "Lack of stay-to-play helps outreach permission, but decentralized home-and-away format plus field geography make WASBT a weak contracted-block candidate.",
      recommendedAction:
        "Optional light note to Brad Roos (Brad@msysa.org / cups@msysa.org) asking whether 2027 finals weekends will publish a recommended-hotel list. Do not prioritize vs association leads.",
      primaryContact: {
        name: "Brad Roos",
        role: "Cups Director",
        organization: "MSYSA",
        email: "Brad@msysa.org",
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: "https://www.msysa.org/programs-landing-page/state-cup/",
        confidence: 80,
      },
      evidence: [
        ...(msysa.evidence || []),
        ev(
          "no_housing_partner",
          "No State Cup housing partner / stay-to-play language found in published rules",
          CLAIM_KIND.FACT,
          "https://www.msysa.org/wp-content/uploads/sites/227/2026/01/2026-MD-State-Cup-Rules-Updated-1.7.26-1_d834c9.pdf",
          "MSYSA State Cup rules 2026",
          { researchProvider: "webhound", researchLevel: "L5_WEBHOUND" }
        ),
        ev(
          "contact",
          "Brad Roos — Brad@msysa.org; cups@msysa.org",
          CLAIM_KIND.FACT,
          "https://www.msysa.org/programs-landing-page/state-cup/",
          "MSYSA State Cup",
          { researchMethodId: "GDI-CONTACT-01", researchProvider: "webhound", researchLevel: "L5_WEBHOUND" }
        ),
      ],
      fitComponents: {
        physicalFit: 70,
        geographyFit: 35,
        timing: 40,
        commercialValue: 40,
        historicalFit: 35,
        competitiveAccessibility: 55,
        contactability: 70,
      },
      confidenceInput: {
        sourceAuthority: 80,
        independentSourceCount: 3,
        directness: 70,
        recency: 85,
        verifiedFieldRatio: 0.55,
        firstPartyShare: 70,
        completeness: 50,
        conflictPenalty: 0,
      },
    });
    byId.set(msysa.id, {
      ...rebuilt,
      webhoundUsed: true,
      labels: [...new Set([...(msysa.labels || []), "deepened_l5", "downgraded_geo"])],
    });
    const after = byId.get(msysa.id);
    roi.push({
      opportunityId: msysa.id,
      researchQuestion: "MSYSA 2027 housing, stay-to-play, geography vs WASBT",
      provider: "webhound_deepen",
      costUsd: 1.25,
      before,
      after: {
        priority: after.priority,
        fit: after.hotelFitScore,
        conf: after.evidenceConfidence,
        booking: after.bookingWindowStatus,
      },
      materialChange: before.priority !== after.priority,
      note: "Downgraded due to weak geography / no housing program",
    });
  }

  // --- Corporate public-source test result (do not fabricate leads) ---
  const corp = byId.get("gdi_opp_marriott_hq_adjacent_corporate_watch");
  if (corp) {
    const before = {
      priority: corp.priority,
      fit: corp.hotelFitScore,
      conf: corp.evidenceConfidence,
      booking: corp.bookingWindowStatus,
    };
    const rebuilt = buildOpportunity({
      ...corp,
      id: corp.id,
      hotelId: PILOT_HOTEL_ID,
      whyNow:
        "RESEARCH FURTHER (CRM required): Webhound public-source test ($5 discovery pass) found public conferences, trainings, and industry days — but not trustworthy named NIH-contractor / biotech group-room leads with impending hotel demand.",
      recommendedAction:
        "Stop spending Webhound on generic corporate prospecting for this hotel. Use Delphi/CRM need-dates and competing-hotel patterns for NIH contractors and Bethesda/Rockville life-sciences accounts.",
      summaryWhyMatters:
        "Documents a negative finding: public web research alone is insufficient for corporate/gov-contractor group prospecting.",
      evidence: [
        ...(corp.evidence || []),
        ev(
          "public_source_verdict",
          "Public sources do not reliably produce actionable named corporate or government-contractor group leads for WASBT",
          CLAIM_KIND.FACT,
          null,
          "Webhound discovery session corporate assessment",
          {
            researchProvider: "webhound",
            researchLevel: "L5_WEBHOUND",
            sourceAuthority: "Tier_C",
            confidence: 85,
            extractedText:
              "CRM/Delphi required; public sources suffice mainly to rule items out.",
          }
        ),
      ],
      fitComponents: {
        physicalFit: 80,
        geographyFit: 85,
        timing: 40,
        commercialValue: 55,
        historicalFit: 50,
        competitiveAccessibility: 55,
        contactability: 20,
      },
      confidenceInput: {
        sourceAuthority: 55,
        independentSourceCount: 1,
        directness: 50,
        recency: 90,
        verifiedFieldRatio: 0.25,
        firstPartyShare: 0,
        completeness: 30,
        conflictPenalty: 0,
      },
    });
    byId.set(corp.id, {
      ...rebuilt,
      webhoundUsed: true,
      // Keep as Watchlist category finding — do not hide the negative result
      priority:
        rebuilt.priority === "DISQUALIFIED" ? "WATCHLIST" : rebuilt.priority,
      priorityReason:
        "Public-source corporate prospecting insufficient; CRM/Delphi required",
      labels: [
        ...new Set([
          ...(corp.labels || []),
          "public_source_insufficient",
          "deepened_l5",
          "category_watch",
        ]),
      ],
    });
    roi.push({
      opportunityId: corp.id,
      researchQuestion: "Can public sources surface corporate/gov contractor group demand?",
      provider: "webhound_discovery",
      costUsd: 2.5,
      before,
      after: {
        priority: byId.get(corp.id).priority,
        fit: byId.get(corp.id).hotelFitScore,
        conf: byId.get(corp.id).evidenceConfidence,
        booking: byId.get(corp.id).bookingWindowStatus,
      },
      materialChange: true,
      note: "Negative finding — no fabricated corporate opportunities added",
    });
  }

  // Append association discovery opportunities (Webhound Priority A)
  const discovered =
    Array.isArray(patches.newOpportunities) && patches.newOpportunities.length
      ? patches.newOpportunities
      : patches.includeDiscovery !== false
        ? buildAssociationDiscoveryOpportunitiesV2()
        : [];
  for (const raw of discovered) {
    if (byId.has(raw.id)) continue;
    byId.set(raw.id, raw);
    roi.push({
      opportunityId: raw.id,
      researchQuestion: "Association discovery — named meeting with hotel/venue gap",
      provider: "webhound_discovery",
      costUsd: raw._roiCostUsd || 0.5,
      before: null,
      after: {
        priority: raw.priority,
        fit: raw.hotelFitScore,
        conf: raw.evidenceConfidence,
        booking: raw.bookingWindowStatus,
      },
      materialChange: true,
    });
  }

  // Apply Webhound deepen notes as evidence appends if provided
  if (patches.webhoundEvidenceByOppId) {
    for (const [id, rows] of Object.entries(patches.webhoundEvidenceByOppId)) {
      const o = byId.get(id);
      if (!o) continue;
      o.evidence = [...(o.evidence || []), ...rows];
      o.webhoundUsed = true;
      byId.set(id, o);
    }
  }

  return {
    opportunities: [...byId.values()],
    roiLog: roi,
    webhoundSessionsReferenced: [WH_DEEPEN],
  };
}

/**
 * Build additional association opportunities from discovery research (post-Webhound).
 */
export function buildDiscoveryOpportunitiesFromFindings(findings = []) {
  return findings.map((f) =>
    buildOpportunity({
      hotelId: PILOT_HOTEL_ID,
      ...f,
    })
  );
}
