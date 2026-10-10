/**
 * Curated named child seeds for YOTEL Geneva Lake campaigns.
 * Only publicly evidenced organizations — no speculative exhibitors.
 * AI for Good children are reused from canonical bag (not re-seeded here).
 */

/** @typedef {{ organizationName: string, role: string, participantType: string, travelingGroup: string, buyerEntity: string, buyerRole: string, publicContactPath: string, lodgingState: string, lodgingNote: string, evidenceUrl: string, forceClass?: string }} CampaignChildSeed */

/**
 * @param {string} campaignId
 * @returns {CampaignChildSeed[]}
 */
export function getYotelCampaignEvidencePack(campaignId) {
  const packs = {
    ycamp_aidex_geneva_2026: [
      {
        organizationName: "Clarion Events / AidEx Geneva Limited",
        role: "ORGANIZER",
        participantType: "ORGANIZER",
        travelingGroup: "Show organizer / operations / exhibitor services team",
        buyerEntity: "AidEx / Clarion Events — exhibitor services / housing liaison",
        buyerRole: "Exhibitor Services / Housing",
        publicContactPath: "https://aid-expo.com/when-where",
        lodgingState: "WEAK",
        lodgingNote:
          "Palexpo hosts AidEx; organizer may source overflow beyond onsite Ibis/Hilton — no published YOTEL block",
        evidenceUrl: "https://aid-expo.com/when-where",
      },
      {
        organizationName: "Palexpo SA",
        role: "VENUE_OPERATOR",
        participantType: "VENUE",
        travelingGroup: "Venue operations (mostly local) — housing partner path only if published",
        buyerEntity: "Palexpo SA",
        buyerRole: "Venue operations",
        publicContactPath: "https://www.palexpo.ch/",
        lodgingState: "UNKNOWN",
        lodgingNote:
          "Venue context only — do not stamp Housing desk without a public housing/hotel-reservation URL",
        evidenceUrl: "https://aid-expo.com/when-where",
      },
    ],
    ycamp_geneva_health_forum_2026: [
      {
        organizationName: "Geneva Health Forum",
        role: "ORGANIZER",
        participantType: "ORGANIZER",
        travelingGroup: "Conference organizing / programme team",
        buyerEntity: "Geneva Health Forum secretariat",
        buyerRole: "Secretariat / Events",
        publicContactPath: "https://genevahealthforum.com/events/ghf-conference-2026/",
        lodgingState: "UNKNOWN",
        lodgingNote: "Campus Biotech venue; lodging not published on landing page",
        evidenceUrl: "https://genevahealthforum.com/events/ghf-conference-2026/",
      },
      {
        organizationName: "University of Geneva — Institute of Global Health",
        role: "UNIVERSITY_HOST",
        participantType: "UNIVERSITY",
        travelingGroup: "Faculty / visiting researchers / partner university delegations",
        buyerEntity: "UNIGE Institute of Global Health / conference admin",
        buyerRole: "Academic events / administration",
        publicContactPath: "https://genevahealthforum.com/events/ghf-conference-2026/",
        lodgingState: "WEAK",
        lodgingNote: "Host institute may be local; visiting partners plausible travelers",
        evidenceUrl: "https://genevahealthforum.com/events/ghf-conference-2026/",
      },
    ],
    ycamp_chi_geneva_centennial_2026: [
      {
        organizationName: "CHI de Genève / Concours Hippique International",
        role: "EVENT_ORGANIZER",
        participantType: "ORGANIZER",
        travelingGroup: "Organizer / federation operations (not spectators)",
        buyerEntity: "CHI Geneva organizing committee / accommodations liaison",
        buyerRole: "Event operations / hospitality",
        publicContactPath:
          "https://www.chi-geneve.ch/en/Edition-2026/Edition-2026-CHI-Geneva.html",
        lodgingState: "WEAK",
        lodgingNote: "Centennial edition at Palexpo — team/crew lodging plausible; no published blocks scraped",
        evidenceUrl:
          "https://www.chi-geneve.ch/en/Edition-2026/Edition-2026-CHI-Geneva.html",
      },
      {
        organizationName: "Palexpo SA",
        role: "VENUE_OPERATOR",
        participantType: "VENUE",
        travelingGroup: "Venue context for participating teams/crews — housing path unconfirmed",
        buyerEntity: "Palexpo SA",
        buyerRole: "Venue operations",
        publicContactPath: "https://www.palexpo.ch/",
        lodgingState: "UNKNOWN",
        lodgingNote: "Palexpo venue context only — no public housing URL evidenced in this pack",
        evidenceUrl:
          "https://www.chi-geneve.ch/en/Edition-2026/Edition-2026-CHI-Geneva.html",
      },
    ],
    ycamp_who_eb_160_2027: [
      {
        organizationName: "World Health Organization",
        role: "SECRETARIAT",
        participantType: "SECRETARIAT",
        travelingGroup:
          "Secretariat support (Geneva HQ heavily local) — visiting experts / overflow only",
        buyerEntity: "WHO Governing Bodies / conference services",
        buyerRole: "Governing Bodies / Conference Services",
        publicContactPath: "https://www.who.int/gb/gov/en/dates-of-meetings-eb_en.html",
        lodgingState: "UNKNOWN",
        lodgingNote: "WHO HQ Geneva — many staff local; member-state lodging separate and unnamed here",
        evidenceUrl: "https://www.who.int/gb/gov/en/dates-of-meetings-eb_en.html",
        forceClass: "SIGNAL_ONLY",
      },
    ],
    ycamp_art_geneve_2027: [
      {
        organizationName: "Art Genève",
        role: "ORGANIZER",
        participantType: "ORGANIZER",
        travelingGroup: "Fair organizer / gallery relations / production",
        buyerEntity: "Art Genève / Palexpo fair management",
        buyerRole: "Fair management / exhibitor services",
        publicContactPath: "https://artgeneve.ch/en/home/",
        lodgingState: "WEAK",
        lodgingNote: "Galleries/installers travel; organizer lodging unproven",
        evidenceUrl: "https://artgeneve.ch/en/home/",
      },
      {
        organizationName: "Palexpo SA",
        role: "VENUE_OPERATOR",
        participantType: "VENUE",
        travelingGroup: "Venue context for galleries / handlers — housing path unconfirmed",
        buyerEntity: "Palexpo SA",
        buyerRole: "Venue operations",
        publicContactPath: "https://www.palexpo.ch/",
        lodgingState: "UNKNOWN",
        lodgingNote: "Fair at Palexpo — do not assert Housing desk without public housing URL",
        evidenceUrl: "https://artgeneve.ch/en/home/",
      },
    ],
    ycamp_watches_wonders_2027: [
      {
        organizationName: "Watches and Wonders",
        role: "ORGANIZER",
        participantType: "ORGANIZER",
        travelingGroup: "Show organizer / brand relations / production coordination",
        buyerEntity: "Watches and Wonders — exhibitor / hospitality services",
        buyerRole: "Exhibitor services / hospitality",
        publicContactPath: "https://www.watchesandwonders.com/en",
        lodgingState: "WEAK",
        lodgingNote: "Brand teams travel heavily; official housing not confirmed in this pack",
        evidenceUrl: "https://www.watchesandwonders.com/en",
      },
      {
        organizationName: "Palexpo SA",
        role: "VENUE_OPERATOR",
        participantType: "VENUE",
        travelingGroup: "Venue context for brand / production crews — housing path unconfirmed",
        buyerEntity: "Palexpo SA",
        buyerRole: "Venue operations",
        publicContactPath: "https://www.palexpo.ch/",
        lodgingState: "UNKNOWN",
        lodgingNote: "Palexpo + city activation — overflow not evidenced without housing URL",
        evidenceUrl: "https://www.watchesandwonders.com/en",
      },
    ],
    ycamp_setac_europe_37_2027: [
      {
        organizationName: "SETAC Europe",
        role: "ORGANIZER",
        participantType: "ORGANIZER",
        travelingGroup: "Society organizing / scientific programme / exhibitor services",
        buyerEntity: "SETAC Europe meetings / exhibitor services",
        buyerRole: "Meetings / Exhibitor services",
        publicContactPath:
          "https://www.setac.org/discover-events/global-meetings/setac-europe-37th-annual-meeting.html",
        lodgingState: "WEAK",
        lodgingNote: "Scientific society meetings usually publish housing later — not yet evidenced",
        evidenceUrl:
          "https://www.setac.org/discover-events/global-meetings/setac-europe-37th-annual-meeting.html",
      },
      {
        organizationName: "Society of Environmental Toxicology and Chemistry",
        role: "PARENT_SOCIETY",
        participantType: "ASSOCIATION",
        travelingGroup: "Society leadership / lab exhibitors / consultants",
        buyerEntity: "SETAC global meetings office",
        buyerRole: "Meetings",
        publicContactPath:
          "https://www.setac.org/discover-events/global-meetings/setac-europe-37th-annual-meeting.html",
        lodgingState: "UNKNOWN",
        lodgingNote: "Parent society path; lodging TBD",
        evidenceUrl:
          "https://www.setac.org/discover-events/global-meetings/setac-europe-37th-annual-meeting.html",
      },
    ],
    ycamp_wha_80_2027: [
      {
        organizationName: "World Health Organization",
        role: "SECRETARIAT",
        participantType: "SECRETARIAT",
        travelingGroup: "WHA secretariat (Geneva) — member-state lodging is the sales motion",
        buyerEntity: "WHO Governing Bodies / WHA conference services",
        buyerRole: "Governing Bodies",
        publicContactPath: "https://apps.who.int/gb/ebwha/pdf_files/EB159/B159_(5)-en.pdf",
        lodgingState: "UNKNOWN",
        lodgingNote: "Member-state delegations unnamed without official list — not invented",
        evidenceUrl: "https://apps.who.int/gb/ebwha/pdf_files/EB159/B159_(5)-en.pdf",
        forceClass: "SIGNAL_ONLY",
      },
    ],
    ycamp_ecosoc_has_2027: [
      {
        organizationName: "United Nations ECOSOC / OCHA",
        role: "SECRETARIAT",
        participantType: "SECRETARIAT",
        travelingGroup: "HAS secretariat / humanitarian partner delegations",
        buyerEntity: "ECOSOC / OCHA conference support",
        buyerRole: "Conference support / Humanitarian affairs",
        publicContactPath: "https://sdg.iisd.org/events/ecosoc-humanitarian-affairs-segment-2027/",
        lodgingState: "WEAK",
        lodgingNote: "2027 HAS Geneva — NGO/delegation lodging plausible; unnamed orgs not invented",
        evidenceUrl: "https://sdg.iisd.org/events/ecosoc-humanitarian-affairs-segment-2027/",
      },
    ],
    // AI for Good — empty pack; continuity reuses canonical children
    ycamp_ai_for_good_2027: [],
  };
  return packs[campaignId] || [];
}
