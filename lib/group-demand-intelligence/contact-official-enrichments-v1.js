/**
 * Official-source contact enrichments for qualified Bethesda/DMV GDI opportunities.
 * L1 only — curated from event/org pages already in the research corpus.
 * $0 Webhound / $0 paid enrichment. Do not invent phones or inferred emails as verified.
 *
 * Keys = opportunity id.
 */

/** @typedef {import('./contact-resolution.js')} _ */

const VERIFIED = "2026-09-15";

/**
 * @type {Record<string, { targetRole: string, primary: object, backups?: object[], auditNote?: string, costUsd?: number }>}
 */
export const OFFICIAL_CONTACT_ENRICHMENTS_V1 = Object.freeze({
  gdi_opp_potomac_memorial_2027: {
    targetRole: "Housing / sourcing contact (stay-to-play) + tournament director backup",
    costUsd: 0,
    auditNote:
      "OVERFLOW_HOUSING: HBC controls hotel-list inclusion; Kathy remains tournament ops backup. Org main 301-519-8070 is MAIN, not direct.",
    primary: {
      name: "HBC Event Services",
      role: "Official stay-to-play housing partner",
      organization: "HBC Event Services",
      email: "support@hbceventservices.com",
      phone: "505-346-0522",
      phoneLineHint: "EVENT",
      claimKind: "FACT",
      sourceUrl: "https://potomacsoccer.org/memorial_tournament/potomac-memorial-tournament/",
      relationshipToEvent: "Manages mandatory stay-to-play hotel program for traveling teams",
      relationshipToOpportunity: "Controls whether Bethesda Marriott can join the official hotel list",
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      whyThisContact:
        "HBC Event Services runs the official stay-to-play hotel program for Potomac Memorial; housing-list inclusion is the actionable hotel-sales path, ahead of the association’s general office.",
      firstSeenAt: VERIFIED,
      lastVerifiedAt: VERIFIED,
      emailSource: "https://potomacsoccer.org/memorial_tournament/potomac-memorial-tournament/",
      phoneSource: "Prior official HBC tournament housing path (same partner as Premier Cup)",
    },
    backups: [
      {
        name: "Kathy Hauschild",
        role: "Tournament Director",
        organization: "Potomac Soccer Association",
        email: "tournament@potomacsoccer.org",
        phone: "301-519-8070",
        phoneLineHint: "MAIN",
        claimKind: "FACT",
        sourceUrl: "https://potomacsoccer.org/memorial_tournament/potomac-memorial-tournament/",
        relationshipToEvent: "Named tournament director on official Memorial page",
        targetRoleMatch: "DIRECT_DECISION_MAKER",
        whyThisContact:
          "Kathy Hauschild is listed by Potomac Soccer Association as Tournament Director on the official Memorial tournament page (role-based tournament@ inbox).",
        firstSeenAt: VERIFIED,
        lastVerifiedAt: VERIFIED,
      },
    ],
  },

  gdi_opp_bethesda_premier_cup_2026: {
    targetRole: "Housing partner (HBC) with tournament director backup",
    costUsd: 0,
    auditNote: "OVERFLOW_HOUSING — prefer HBC over association ED for hotel inclusion.",
    primary: {
      name: "HBC Event Services",
      role: "Official housing partner / hotel program",
      organization: "HBC Event Services",
      email: "support@hbceventservices.com",
      phone: "505-346-0522",
      phoneLineHint: "EVENT",
      claimKind: "FACT",
      sourceUrl: "https://bethesdapremiercuphotels.com/events/",
      relationshipToEvent: "Runs official Premier Cup stay-to-play hotel portal",
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      whyThisContact:
        "HBC manages the official stay-to-play hotel program for Bethesda Premier Cup, making its housing desk more relevant for hotel inclusion than a general club office.",
      firstSeenAt: VERIFIED,
      lastVerifiedAt: VERIFIED,
    },
    backups: [
      {
        name: "Brad Roos",
        role: "Tournament Director",
        organization: "Bethesda Soccer Club / Bethesda Premier Cup",
        email: "broos@bethesdasoccer.org",
        claimKind: "FACT",
        sourceUrl: "https://www.msysa.org/2026-2027-sanctioned-tournaments/",
        relationshipToEvent: "Named tournament director on USYS Maryland sanctioned list",
        targetRoleMatch: "DIRECT_DECISION_MAKER",
        whyThisContact:
          "Brad Roos is listed as Tournament Director on the USYS Maryland sanctioned tournaments page; use as backup when HBC does not respond on inventory.",
        firstSeenAt: VERIFIED,
        lastVerifiedAt: VERIFIED,
      },
    ],
  },

  gdi_opp_nice_2027: {
    targetRole: "NICE program leadership (location / conference influence)",
    costUsd: 0,
    auditNote:
      "Replaced generic nice@nist.gov-as-primary with named NIST NICE Director from official staff page. nice@ remains routing backup. Phone on staff page treated as OFFICE (not invented mobile).",
    primary: {
      name: "Karen Wetzel",
      role: "Director of NICE",
      organization: "NIST / NICE",
      email: "karen.wetzel@nist.gov",
      phone: "240-439-0767",
      phoneLineHint: "OFFICE",
      claimedDirectPhone: true,
      claimKind: "FACT",
      sourceUrl: "https://www.nist.gov/itl/applied-cybersecurity/nice/about/meet-staff",
      relationshipToEvent: "Director of NICE program that owns the annual conference",
      relationshipToOpportunity: "Executive / program authority for conference location strategy",
      targetRoleMatch: "DIRECT_DECISION_MAKER",
      whyThisContact:
        "Karen Wetzel is listed on the official NIST NICE staff page as Director of NICE — the program that owns the annual NICE Conference and Expo while the 2027 host city remains TBD.",
      firstSeenAt: VERIFIED,
      lastVerifiedAt: VERIFIED,
    },
    backups: [
      {
        name: "Susana Barraza",
        role: "Program Manager, Lead for Communications and Engagement",
        organization: "NIST / NICE",
        email: "susana.barraza@nist.gov",
        phone: "240-457-2638",
        phoneLineHint: "OFFICE",
        claimedDirectPhone: true,
        claimKind: "FACT",
        sourceUrl: "https://www.nist.gov/itl/applied-cybersecurity/nice/about/meet-staff",
        relationshipToEvent: "NICE communications & engagement lead",
        targetRoleMatch: "EVENT_OPERATIONS_CONTACT",
        whyThisContact:
          "Susana Barraza is the official NICE Lead for Communications and Engagement on the NIST staff directory — a practical ops path alongside program leadership.",
        firstSeenAt: VERIFIED,
        lastVerifiedAt: VERIFIED,
      },
      {
        name: "NICE Program Office",
        role: "General program inbox",
        organization: "NIST / NICE",
        email: "nice@nist.gov",
        phone: "301-975-4470",
        phoneLineHint: "MAIN",
        claimKind: "FACT",
        sourceUrl: "https://www.nist.gov/itl/applied-cybersecurity/nice/about/meet-staff",
        relationshipToEvent: "Published general NICE program contact",
        targetRoleMatch: "GENERAL_ORGANIZATION_CONTACT",
        whyThisContact:
          "nice@nist.gov remains the published general NICE Program Office channel; use only if named staff do not respond.",
        firstSeenAt: VERIFIED,
        lastVerifiedAt: VERIFIED,
      },
    ],
  },

  gdi_opp_nar_gad_institute_2027: {
    targetRole: "Named NAR event contact on official GAD Institute page",
    costUsd: 0,
    auditNote:
      "Jami Sims is named; GADInst@nar.realtor is ROLE-BASED (not personal direct). Phone treated as OFFICE.",
    primary: {
      name: "Jami Sims",
      role: "NAR contact — GAD Institute",
      organization: "National Association of REALTORS®",
      email: "GADInst@nar.realtor",
      phone: "202-383-1221",
      phoneLineHint: "OFFICE",
      claimKind: "FACT",
      sourceUrl: "https://www.nar.realtor/events/2027-gad-institute",
      relationshipToEvent: "Named contact person on the official 2027 GAD Institute event page",
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      whyThisContact:
        "Jami Sims is listed as the contact person on NAR’s official 2027 GAD Institute page, with the GADInst@nar.realtor role inbox and DC office phone.",
      firstSeenAt: VERIFIED,
      lastVerifiedAt: VERIFIED,
    },
  },

  gdi_opp_afcea_hits_2027: {
    targetRole: "Chapter VP / Co-VP for Health IT Summit",
    costUsd: 0,
    auditNote:
      "Named summit VPs from board page; registrar@ is ROLE-BASED registration desk; 571-323-2587 is chapter MAIN line.",
    primary: {
      name: "Andrea Snader",
      role: "Vice President, Health IT Summit",
      organization: "AFCEA Bethesda",
      email: "registrar@afceabethesda.org",
      phone: "571-323-2587",
      phoneLineHint: "MAIN",
      claimKind: "FACT",
      sourceUrl: "https://bethesda.afceachapters.org/board-of-directors/",
      relationshipToEvent: "Chapter VP responsible for the Health IT Summit",
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      whyThisContact:
        "Andrea Snader is listed on the AFCEA Bethesda board as Vice President, Health IT Summit — the volunteer lead for this event. Outreach still routes via the chapter registrar inbox / main line until a direct work email is published.",
      firstSeenAt: VERIFIED,
      lastVerifiedAt: VERIFIED,
    },
    backups: [
      {
        name: "Justin Fessler",
        role: "Co-Vice President, Health IT Summit",
        organization: "AFCEA Bethesda",
        email: "registrar@afceabethesda.org",
        phone: "571-323-2587",
        phoneLineHint: "MAIN",
        claimKind: "FACT",
        sourceUrl: "https://bethesda.afceachapters.org/board-of-directors/",
        relationshipToEvent: "Co-VP for Health IT Summit",
        targetRoleMatch: "EVENT_MEETINGS_OWNER",
        whyThisContact:
          "Justin Fessler is listed as Co-Vice President, Health IT Summit on the same official board page.",
        firstSeenAt: VERIFIED,
        lastVerifiedAt: VERIFIED,
      },
    ],
  },

  gdi_opp_show_2026_nih: {
    targetRole: "Public Cvent event contact",
    costUsd: 0,
    primary: {
      name: "Jessica Mitchell",
      role: "Event contact (public Cvent)",
      organization: "NIH / NHLBI",
      email: "Jessica.Mitchell@nih.gov",
      claimKind: "FACT",
      sourceUrl: "https://web.cvent.com/event/225a615f-cd58-4c72-8191-38a489105c2d/summary",
      relationshipToEvent: "Named event contact on public Cvent listing",
      targetRoleMatch: "EVENT_OPERATIONS_CONTACT",
      whyThisContact:
        "Jessica Mitchell is the named event contact on the public SHOW 2026 Cvent page (NIH / NHLBI).",
      firstSeenAt: VERIFIED,
      lastVerifiedAt: VERIFIED,
    },
  },

  gdi_opp_acts_ts27: {
    targetRole: "Association meetings / conference desk (named planner not yet public)",
    costUsd: 0,
    auditNote:
      "Official ACTS site publishes info@ + main phone only — Grade D until a named planner appears. Do not fabricate names.",
    primary: {
      name: null,
      role: "ACTS conference desk",
      organization: "Association for Clinical and Translational Science",
      email: "info@actscience.org",
      phone: "202-367-1119",
      phoneLineHint: "MAIN",
      claimKind: "FACT",
      sourceUrl: "https://www.actscience.org/Translational-Science",
      relationshipToEvent: "Published association main contact on TS27 save-the-date page",
      targetRoleMatch: "GENERAL_ORGANIZATION_CONTACT",
      whyThisContact:
        "ACTS has not yet published a named hotel / meetings planner for TS27; only the association main inbox and phone appear on the official Translational Science page.",
      firstSeenAt: VERIFIED,
      lastVerifiedAt: VERIFIED,
    },
  },

  gdi_opp_bebpa_usb_2027: {
    targetRole: "Conference organizers (role inbox)",
    costUsd: 0,
    primary: {
      name: null,
      role: "Conference organizers",
      organization: "BEBPA",
      email: "contactus@bebpa.org",
      phone: "206-651-4542",
      phoneLineHint: "MAIN",
      claimKind: "FACT",
      sourceUrl: "https://bebpa.org/2027-usb/",
      relationshipToEvent: "Published conference contact on BEBPA USB 2027 page",
      targetRoleMatch: "ASSOCIATION_MANAGEMENT_CONTACT",
      whyThisContact:
        "BEBPA publishes a conference organizer inbox and main phone on the 2027 USB page; a named meetings decision maker is not yet listed.",
      firstSeenAt: VERIFIED,
      lastVerifiedAt: VERIFIED,
    },
  },
});

export const CONTACT_ENRICHMENT_PASS_ID = "gdi_contact_resolution_v1";
