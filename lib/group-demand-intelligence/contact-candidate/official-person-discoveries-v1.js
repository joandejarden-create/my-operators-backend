/**
 * Official-source person discoveries for previously unresolved Bethesda GDI opportunities.
 * Research date: 2026-09-15. $0 Surfe / paid. Do not invent names.
 *
 * Keys = opportunity id. Empty array = remains unresolved after research.
 */

import {
  EMPLOYMENT_STATUS,
  EVENT_RELATIONSHIP,
  SOURCE_TYPE,
} from "./person-discovery-states.js";

const V = "2026-09-15";

/** @type {Record<string, object[]>} */
export const OFFICIAL_PERSON_DISCOVERIES_V1 = Object.freeze({
  gdi_opp_acts_ts27: [
    {
      name: "Elizabeth Lancaster",
      role: "Events Manager",
      organization: "Association for Clinical and Translational Science",
      email: "elancaster@actscience.org",
      claimKind: "FACT",
      sourceUrl: "https://www.actscience.org/About/Contact",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      evidenceQuote:
        "Event Services — Elizabeth Lancaster – Events Manager (elancaster@actscience.org)",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Listed as Events Manager under Event Services on the official ACTS contact page — the staff role most likely to influence TS27 hotel/venue logistics.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
    },
  ],

  gdi_opp_bebpa_usb_2027: [
    {
      name: "Karen Bertani",
      role: "Director of Events",
      organization: "BEBPA",
      email: "karen.bertani@bebpa.org",
      claimKind: "FACT",
      sourceUrl: "https://bebpa.org/exhibitors/why-exhibit-at-a-bebpa-conference/",
      sourceType: SOURCE_TYPE.OFFICIAL_ORG_SITE,
      evidenceQuote: "Contact: Karen Bertani, Director of Events karen.bertani@bebpa.org",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Named Director of Events on BEBPA’s official exhibitor/conference contact path for BEBPA conferences including the 2027 US Bioassay Conference.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
    },
  ],

  gdi_opp_nado_ddaa_washcon_2028: [
    {
      name: "Jamie McCormick",
      role: "Events Manager",
      organization: "NADO",
      email: "jmccormick@nado.org",
      claimKind: "FACT",
      sourceUrl: "https://www.nado.org/2026washcon/",
      sourceType: SOURCE_TYPE.PRIOR_YEAR_EVENT,
      evidenceQuote:
        "For hotel reservation assistance, contact Jamie McCormick at jmccormick@nado.org (2026 WashCon page).",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_PROBABLE,
      eventRelationship: EVENT_RELATIONSHIP.HISTORICAL_EVENT_CONTACT,
      priorEventInvolvement: true,
      historicalOnly: true,
      reactivationBoost: true,
      stillEmployed: true,
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      gdiContactRole: "HOUSING_OWNER",
      whyThisPerson:
        "Named NADO Events Manager on the official 2026 WashCon page for hotel reservation assistance at Crystal Gateway Marriott — same venue series as 2027/2028; treated as current-probable successor path for 2028.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      eventSpecificEvidence: true,
    },
  ],

  gdi_opp_nado_ddaa_washcon_2027_overflow: [
    {
      name: "Jamie McCormick",
      role: "Events Manager",
      organization: "NADO",
      email: "jmccormick@nado.org",
      claimKind: "FACT",
      sourceUrl: "https://www.nado.org/2026washcon/",
      sourceType: SOURCE_TYPE.PRIOR_YEAR_EVENT,
      evidenceQuote:
        "Questions about accommodations? Contact Jamie McCormick (jmccormick@nado.org).",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_PROBABLE,
      eventRelationship: EVENT_RELATIONSHIP.HISTORICAL_EVENT_CONTACT,
      priorEventInvolvement: true,
      historicalOnly: true,
      reactivationBoost: true,
      stillEmployed: true,
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      gdiContactRole: "HOUSING_OWNER",
      whyThisPerson:
        "Official 2026 WashCon hotel/accommodations contact; Crystal Gateway Marriott is already named for 2027 — Jamie is the probable housing/ops path for overflow conversations.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      eventSpecificEvidence: true,
    },
    {
      name: "Brittany Salazar",
      role: "Senior Meetings & Membership Manager (prior WashCon housing)",
      organization: "NADO",
      email: "bsalazar@nado.org",
      claimKind: "FACT",
      sourceUrl: "https://www.nado.org/2025-washcon/",
      sourceType: SOURCE_TYPE.PRIOR_YEAR_EVENT,
      evidenceQuote:
        "2025 WashCon hotel assistance: Brittany Salazar, Senior Meetings & Membership Manager.",
      employmentStatus: EMPLOYMENT_STATUS.UNKNOWN,
      eventRelationship: EVENT_RELATIONSHIP.HISTORICAL_EVENT_CONTACT,
      priorEventInvolvement: true,
      historicalOnly: true,
      stillEmployed: false,
      rejected: true,
      rejectReason: "superseded_by_current_events_manager_jamie_mccormick",
      forceReject: true,
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      whyThisPerson:
        "Historical 2025 housing contact; 2026 materials name Jamie McCormick instead — do not use as primary without current-employment confirmation.",
      firstSeenAt: V,
      lastVerifiedAt: V,
    },
  ],

  gdi_opp_asae_annual_2029: [
    {
      name: "Kelly Frere",
      role: "Director, Meeting Operations and Engagement",
      organization: "ASAE",
      email: "kfrere@asaecenter.org",
      claimKind: "FACT",
      sourceUrl: "https://annual.asaecenter.org/about-annual/contact-us/",
      sourceType: SOURCE_TYPE.OFFICIAL_EVENT_SITE,
      evidenceQuote:
        "Meeting Operations Team — Kelly Frere, CAE, CMP, Director, Meeting Operations and Engagement, kfrere@asaecenter.org",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Official ASAE Annual Meeting contact page lists Kelly Frere as Director of Meeting Operations and Engagement — the meetings owner for Annual (including 2029 DC).",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
    },
    {
      name: "Expovision",
      role: "Official ASAE Annual housing bureau",
      organization: "Expovision",
      email: "asaehotels@expovision.com",
      phone: "866-812-8749",
      claimKind: "FACT",
      sourceUrl: "https://annual.asaecenter.org/hotel-travel/",
      sourceType: SOURCE_TYPE.HOUSING_PAGE,
      evidenceQuote:
        "ASAE’s only authorized hotel vendor is Expovision — asaehotels@expovision.com / (866) 812-8749",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT,
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      gdiContactRole: "HOUSING_OWNER",
      whyThisPerson:
        "Official housing bureau for ASAE Annual — functional entity for room-block / overflow hotel inclusion (parallel to HBC pattern).",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
      functionalEntity: true,
    },
  ],

  gdi_opp_alexandria_soccer_kickoff_2027: [
    {
      name: "Traveling Teams",
      role: "Official stay-to-play housing partner",
      organization: "Traveling Teams",
      claimKind: "FACT",
      sourceUrl: "https://alexandria-soccer.org/tournaments/alexandria-soccer-kickoff/",
      sourceType: SOURCE_TYPE.HOUSING_PAGE,
      evidenceQuote:
        "Alexandria Soccer has teamed up with Traveling Teams — stay-to-play; all hotel reservations must be made through Traveling Teams.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT,
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      gdiContactRole: "HOUSING_OWNER",
      whyThisPerson:
        "Official stay-to-play housing partner named on the Kickoff tournament page — primary hotel-list inclusion path.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
      functionalEntity: true,
    },
    {
      name: "Ben Hawkins",
      role: "Tournament Director",
      organization: "Alexandria Soccer Association",
      claimKind: "FACT",
      sourceUrl: "https://alexandria-soccer.org/tournaments/alexandria-soccer-kickoff/",
      evidenceUrls: [
        "https://alexandria-soccer.org/tournaments/alexandria-soccer-kickoff/",
        "https://alexandria-soccer.org/staff/",
      ],
      sourceType: SOURCE_TYPE.OFFICIAL_EVENT_SITE,
      evidenceQuote:
        "For general questions, check-in or payment inquiries please contact Tournament Director Ben Hawkins.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT,
      targetRoleMatch: "DIRECT_DECISION_MAKER",
      gdiContactRole: "EVENT_OWNER",
      whyThisPerson:
        "Named Tournament Director on the official Kickoff page and ASA staff directory — backup when housing partner does not respond.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
    },
  ],

  gdi_opp_arlington_spring_tournament_2027: [
    // No named person on 2027 tournament page — only tournaments@ inbox.
    // Do not import Texas API travel Babette Haddox (wrong geography/event).
  ],

  gdi_opp_ahima_advocacy_2027: [
    {
      name: "Sabrina Bracken",
      role: "Advocacy Summit prospectus / partner contact (Spargo)",
      organization: "Spargo, Inc. (AHIMA Advocacy Summit AMC)",
      email: "sabrina.bracken@spargoinc.com",
      phone: "571-207-8392",
      claimKind: "FACT",
      sourceUrl: "https://www.ahima.org/media/o5yfklmm/ahima2026advocacyprospectusv2.pdf",
      sourceType: SOURCE_TYPE.PROSPECTUS_PDF,
      evidenceQuote:
        "AHIMA 2026 Advocacy prospectus contact: Sabrina Bracken | 571.207.8392 | sabrina.bracken@spargoinc.com",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_PROBABLE,
      eventRelationship: EVENT_RELATIONSHIP.HISTORICAL_EVENT_CONTACT,
      priorEventInvolvement: true,
      historicalOnly: true,
      reactivationBoost: true,
      stillEmployed: true,
      targetRoleMatch: "ASSOCIATION_MANAGEMENT_CONTACT",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Named contact on the official AHIMA 2026 Advocacy Summit prospectus PDF (Spargo). 2027 summit dates are published; treat as historical event contact still probably current at Spargo pending 2027 prospectus.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      eventSpecificEvidence: true,
    },
    {
      name: "Vicky Betzig",
      role: "Director, Meetings (LinkedIn-only)",
      organization: "AHIMA",
      claimKind: "INFERENCE",
      sourceUrl: "https://www.linkedin.com/in/vicky-betzig-a05bbb1a5",
      sourceType: SOURCE_TYPE.PUBLIC_PROFESSIONAL_PROFILE,
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_PROBABLE,
      eventRelationship: EVENT_RELATIONSHIP.ORGANIZATION_ROLE_ONLY,
      rejected: true,
      forceReject: true,
      rejectReason: "linkedin_only_insufficient_for_event_ownership",
      whyThisPerson:
        "LinkedIn lists Director, Meetings at AHIMA, but no ahima.org staff/event page corroboration — blocked per LinkedIn-only rule.",
      firstSeenAt: V,
      lastVerifiedAt: V,
    },
  ],

  gdi_opp_acc_legislative_2027: [
    {
      name: "Meg Novak",
      role: "ACC Legislative Conference contact",
      organization: "American College of Cardiology",
      email: "mnovak@acc.org",
      claimKind: "FACT",
      sourceUrl:
        "https://www.accmi.org/index.php?Itemid=115&day=04&evid=50&month=10&option=com_jevents&task=icalrepeat.detail&title=acc-legislative-conference-2026&uid=c0caaee2f874e55803f0a63155c50591&year=2026",
      sourceType: SOURCE_TYPE.PRIOR_YEAR_EVENT,
      evidenceQuote: "Email Meg Novak (mnovak@acc.org) with questions (ACC Legislative Conference tips).",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_PROBABLE,
      eventRelationship: EVENT_RELATIONSHIP.HISTORICAL_EVENT_CONTACT,
      priorEventInvolvement: true,
      historicalOnly: true,
      reactivationBoost: true,
      stillEmployed: true,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Named as the conference questions contact on an official ACC chapter calendar entry for Legislative Conference — used as historical/current-probable path for 2027 until ACC publishes a 2027 staff page.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      eventSpecificEvidence: true,
    },
  ],

  gdi_opp_cmss_spring_2027: [
    {
      name: "Mary Sanders",
      role: "Sales Manager",
      organization: "CMSS",
      email: "msanders@cmss.org",
      claimKind: "FACT",
      sourceUrl: "https://cmss.org/healthcare-innovation-roundtable/",
      sourceType: SOURCE_TYPE.OFFICIAL_EVENT_SITE,
      evidenceQuote:
        "Hotel Arrangements will be shared closer to the event. Questions regarding the Healthcare Innovation Roundtable: Mary Sanders, CMSS Sales Manager msanders@cmss.org.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT,
      targetRoleMatch: "EVENT_OPERATIONS_CONTACT",
      gdiContactRole: "PARTNERSHIPS_OWNER",
      whyThisPerson:
        "Named on the official Spring 2027 Healthcare Innovation Roundtable page (Alexandria venue + hotel arrangements forthcoming) as the partnership/event contact.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
    },
    {
      name: "Shaniece Rigans",
      role: "Operations Manager — Annual Meeting Program Committee liaison",
      organization: "CMSS",
      claimKind: "FACT",
      sourceUrl: "https://cmss.org/about-cmss/staff/",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      evidenceQuote:
        "Shaniece Rigans serves as the primary liaison to the CMSS Annual Meeting Program Committee.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.ORGANIZATION_ROLE_ONLY,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Official CMSS staff page — Annual Meeting program liaison; relevant meetings function but Spring Meeting hotel owner not explicitly named.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
    },
  ],

  gdi_opp_ndss_advocacy_2027: [
    {
      name: "Amy Drow",
      role: "Director of Events",
      organization: "National Down Syndrome Society",
      claimKind: "FACT",
      sourceUrl: "https://ndss.org/meet-our-staff",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      evidenceQuote:
        "Amy provides strategic leadership and oversees the planning, execution, and success of NDSS events, including … and national programs.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "EVENT_OWNER",
      whyThisPerson:
        "Listed on the official NDSS staff page as the events lead overseeing NDSS national programs/events — most probable internal owner while 2027 hotel block is still TBA.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
    },
    {
      name: "Lauren Donahue",
      role: "Manager of Special Events",
      organization: "National Down Syndrome Society",
      claimKind: "FACT",
      sourceUrl: "https://ndss.org/meet-our-staff",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      evidenceQuote:
        "Lauren plans and oversees the execution of NDSS signature events…",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.ORGANIZATION_ROLE_ONLY,
      targetRoleMatch: "EVENT_OPERATIONS_CONTACT",
      gdiContactRole: "EVENT_OWNER",
      whyThisPerson:
        "Official NDSS staff — special events manager; backup to Director of Events.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
    },
  ],

  gdi_opp_amwa_2027_annual: [
    {
      name: "Elizabeth McGovern",
      role: "Assistant Director",
      organization: "AMWA",
      claimKind: "FACT",
      sourceUrl: "https://amwa-doc.org/about-amwa/our-people/",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      evidenceQuote:
        "Elizabeth McGovern is Assistant Director … experience includes association management … event planning, and member engagement.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.ORGANIZATION_ROLE_ONLY,
      targetRoleMatch: "EVENT_OPERATIONS_CONTACT",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Official AMWA Our People page — Assistant Director with event-planning scope; meetings still route via program@ / associatedirector@ until a named meetings director is published.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
    },
  ],

  gdi_opp_amwa_2027_interim: [
    {
      name: "Elizabeth McGovern",
      role: "Assistant Director",
      organization: "AMWA",
      claimKind: "FACT",
      sourceUrl: "https://amwa-doc.org/about-amwa/our-people/",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.ORGANIZATION_ROLE_ONLY,
      targetRoleMatch: "EVENT_OPERATIONS_CONTACT",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Same AMWA Assistant Director (official staff page) used as organization meetings-path for Interim 2027.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
    },
  ],

  gdi_opp_amwa_2028_annual: [
    {
      name: "Elizabeth McGovern",
      role: "Assistant Director",
      organization: "AMWA",
      claimKind: "FACT",
      sourceUrl: "https://amwa-doc.org/about-amwa/our-people/",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.ORGANIZATION_ROLE_ONLY,
      targetRoleMatch: "EVENT_OPERATIONS_CONTACT",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Same AMWA Assistant Director (official staff page) for 2028 annual cycle watch.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
    },
  ],

  gdi_opp_aad_2028_overflow: [
    {
      name: "Maritz Global Events",
      role: "Official AAD registration & housing provider (eventshq)",
      organization: "Maritz Global Events",
      email: "registration@aad.org",
      claimKind: "FACT",
      sourceUrl:
        "https://staging.aad.org/member/meetings-education/am26/housing/official-provider",
      sourceType: SOURCE_TYPE.HOUSING_PAGE,
      evidenceQuote:
        "Maritz Global Events (eventshq) is the ONLY official registration and housing provider for AAD.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT,
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      gdiContactRole: "HOUSING_OWNER",
      whyThisPerson:
        "AAD’s official housing provider pattern (confirmed for Annual Meeting housing) — functional entity for 2028 DC overflow room-block conversations.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
      functionalEntity: true,
    },
  ],

  gdi_opp_world_biomaterials_2028_overflow: [
    {
      name: "Jessica Goodone",
      role: "Meeting Manager",
      organization: "Society For Biomaterials",
      email: "jgoodone@biomaterials.org",
      phone: "856-380-6878",
      claimKind: "FACT",
      sourceUrl: "https://biomaterials.org/about-society/society-headquarters",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      evidenceQuote:
        "Jessica Goodone Meeting Manager jgoodone@biomaterials.org +1 856-380-6878",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Official SFB headquarters lists Jessica Goodone as Meeting Manager — SFB hosts WBC 2028 in Washington, DC.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
    },
  ],

  gdi_opp_ajas_hq_rockville_watch: [
    {
      name: "Rachel Stevens",
      role: "Director of Operations",
      organization: "Association of Jewish Aging Services",
      claimKind: "FACT",
      sourceUrl:
        "https://www.causeiq.com/organizations/association-of-jewish-aging-services,521967481/",
      sourceType: SOURCE_TYPE.STANDARD_WEB,
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_PROBABLE,
      eventRelationship: EVENT_RELATIONSHIP.ORGANIZATION_ROLE_ONLY,
      targetRoleMatch: "EVENT_OPERATIONS_CONTACT",
      gdiContactRole: "PROGRAM_OWNER",
      whyThisPerson:
        "Listed as Director of Operations (secondary directory). 2027 conference is Fort Lauderdale — weak Bethesda hotel authority; keep low confidence / watch only.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
    },
  ],

  // Explicit empty — researched, no credible named person without fabrication
  gdi_opp_ecs_251_2027_overflow: [],
  gdi_opp_georgetown_homecoming_2027: [],
  gdi_opp_marriott_hq_adjacent_corporate_watch: [],
  gdi_opp_nih_sbpo_vos_pattern: [],
});

export const OFFICIAL_PERSON_DISCOVERY_PASS_ID = "gdi_official_person_discovery_v1";
