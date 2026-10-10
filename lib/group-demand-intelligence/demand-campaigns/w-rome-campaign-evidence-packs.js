/**
 * Curated named child seeds for W Rome demand campaigns.
 * Public organizers / venue paths only — no speculative exhibitors.
 */

/**
 * @param {string} campaignId
 * @returns {Array<{ organizationName: string, role: string, participantType: string, travelingGroup: string, buyerEntity: string, buyerRole: string, publicContactPath: string, lodgingState: string, lodgingNote: string, evidenceUrl: string, forceClass?: string }>}
 */
export function getWRomeCampaignEvidencePack(campaignId) {
  const packs = {
    wrcamp_festa_cinema_roma_2026: [
      {
        organizationName: "Fondazione Cinema per Roma",
        role: "ORGANIZER",
        participantType: "ORGANIZER",
        travelingGroup: "Festival organizing / guest programming / hospitality team",
        buyerEntity: "Fondazione Cinema per Roma — hospitality / guest services",
        buyerRole: "Guest services / Hospitality",
        publicContactPath: "https://www.romacinemafest.it/",
        lodgingState: "WEAK",
        lodgingNote:
          "International talent / press travel to Rome; no published W Rome block — lifestyle overflow plausible",
        evidenceUrl: "https://www.romacinemafest.it/",
      },
      {
        organizationName: "Auditorium Parco della Musica",
        role: "VENUE_OPERATOR",
        participantType: "VENUE",
        travelingGroup: "Venue operations (mostly local) — guest hotel path only",
        buyerEntity: "Auditorium / festival housing liaison",
        buyerRole: "Housing / Guest liaison",
        publicContactPath: "https://www.auditorium.com/",
        lodgingState: "STRONG_INFERENCE",
        lodgingNote: "Primary festival venues drive guest lodging in centro — overflow corridor relevant",
        evidenceUrl: "https://www.romacinemafest.it/",
      },
    ],
    wrcamp_altaroma_2027: [
      {
        organizationName: "Altaroma",
        role: "ORGANIZER",
        participantType: "ORGANIZER",
        travelingGroup: "Fashion week organizing / brand relations / production",
        buyerEntity: "Altaroma — exhibitor / brand hospitality",
        buyerRole: "Exhibitor services / Hospitality",
        publicContactPath: "https://www.altaroma.it/",
        lodgingState: "WEAK",
        lodgingNote: "Brand teams and buyers travel; official housing not confirmed in this pack",
        evidenceUrl: "https://www.altaroma.it/",
      },
    ],
    wrcamp_maker_faire_rome_2026: [
      {
        organizationName: "Maker Faire Rome / Innova Camera",
        role: "ORGANIZER",
        participantType: "ORGANIZER",
        travelingGroup: "Show organizer / exhibitor services / maker relations",
        buyerEntity: "Maker Faire Rome — exhibitor services",
        buyerRole: "Exhibitor services",
        publicContactPath: "https://makerfairerome.eu/",
        lodgingState: "WEAK",
        lodgingNote: "Exhibitors travel; venue at Fiera di Roma — centro overflow for select teams",
        evidenceUrl: "https://makerfairerome.eu/",
      },
      {
        organizationName: "Fiera Roma",
        role: "VENUE_OPERATOR",
        participantType: "VENUE",
        travelingGroup: "Venue housing channel for exhibitors",
        buyerEntity: "Fiera Roma hotel / housing partners",
        buyerRole: "Housing",
        publicContactPath: "https://www.fieraroma.it/",
        lodgingState: "STRONG_INFERENCE",
        lodgingNote: "Trade-fair venue typically routes exhibitor lodging — centro lifestyle overflow selective",
        evidenceUrl: "https://makerfairerome.eu/",
      },
    ],
    wrcamp_fao_conference_2026: [
      {
        organizationName: "Food and Agriculture Organization of the United Nations",
        role: "SECRETARIAT",
        participantType: "SECRETARIAT",
        travelingGroup:
          "FAO secretariat (Rome HQ heavily local) — visiting member-state / partner delegations",
        buyerEntity: "FAO Conference / Governing Bodies services",
        buyerRole: "Governing Bodies / Conference Services",
        publicContactPath: "https://www.fao.org/home/en",
        lodgingState: "UNKNOWN",
        lodgingNote:
          "Rome HQ — many staff local; member-state lodging is the sales motion and must stay unnamed without lists",
        evidenceUrl: "https://www.fao.org/home/en",
        forceClass: "SIGNAL_ONLY",
      },
    ],
    wrcamp_wfp_executive_board_2026: [
      {
        organizationName: "World Food Programme",
        role: "SECRETARIAT",
        participantType: "SECRETARIAT",
        travelingGroup: "WFP EB secretariat (Rome) — visiting board / partner delegations",
        buyerEntity: "WFP Executive Board / conference services",
        buyerRole: "Executive Board services",
        publicContactPath: "https://www.wfp.org/executive-board",
        lodgingState: "UNKNOWN",
        lodgingNote: "Rome HQ — delegation lodging unnamed without official list",
        evidenceUrl: "https://www.wfp.org/executive-board",
        forceClass: "SIGNAL_ONLY",
      },
    ],
    wrcamp_ifad_governing_council_2027: [
      {
        organizationName: "International Fund for Agricultural Development",
        role: "SECRETARIAT",
        participantType: "SECRETARIAT",
        travelingGroup: "IFAD Governing Council support — visiting member delegations",
        buyerEntity: "IFAD Governing Council / conference support",
        buyerRole: "Governing Council services",
        publicContactPath: "https://www.ifad.org/",
        lodgingState: "UNKNOWN",
        lodgingNote: "Rome HQ — named delegations not invented",
        evidenceUrl: "https://www.ifad.org/",
        forceClass: "SIGNAL_ONLY",
      },
    ],
    wrcamp_romaeuropa_2027: [
      {
        organizationName: "Fondazione Romaeuropa",
        role: "ORGANIZER",
        participantType: "ORGANIZER",
        travelingGroup: "Festival programming / artist hospitality / production",
        buyerEntity: "Romaeuropa — artist hospitality",
        buyerRole: "Artist hospitality / Production",
        publicContactPath: "https://romaeuropa.net/",
        lodgingState: "WEAK",
        lodgingNote: "International artists/crews travel; no published W Rome block",
        evidenceUrl: "https://romaeuropa.net/",
      },
    ],
    wrcamp_luiss_executive_2027: [
      {
        organizationName: "LUISS Business School",
        role: "PROGRAM_HOST",
        participantType: "UNIVERSITY",
        travelingGroup: "Executive education cohorts / visiting faculty / corporate partners",
        buyerEntity: "LUISS Business School — executive programs / events",
        buyerRole: "Program / Events administration",
        publicContactPath: "https://businessschool.luiss.it/",
        lodgingState: "WEAK",
        lodgingNote: "Executive cohorts often need centro lodging; counts not invented",
        evidenceUrl: "https://businessschool.luiss.it/",
      },
    ],
  };
  return packs[campaignId] || [];
}
