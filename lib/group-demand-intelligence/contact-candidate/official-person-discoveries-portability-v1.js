/**
 * Official-source person discoveries for Waterstone + Renaissance GDI WHO gaps.
 * Research: 2026-09-18. $0 Surfe/PDL for WHO. Do not invent names.
 *
 * Precision-first: only Tier-1/2 official staff/event pages.
 */

import {
  EMPLOYMENT_STATUS,
  EVENT_RELATIONSHIP,
  SOURCE_TYPE,
} from "./person-discovery-states.js";

const V = "2026-09-18";

/** @type {Record<string, object[]>} */
export const OFFICIAL_PERSON_DISCOVERIES_PORTABILITY_V1 = Object.freeze({
  // --- Waterstone ---
  gdi_opp_uca_convergence_2027_overflow_vip_housing_9: [
    {
      name: "Dedra Benjamin",
      role: "Meetings & Events Manager",
      organization: "Urgent Care Association",
      phone: "331-472-3750",
      claimKind: "FACT",
      sourceUrl: "https://urgentcareassociation.org/about/the-people-behind-uca",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      sourceAuthorityTier: 1,
      evidenceQuote:
        "Dedra Benjamin — Meetings & Events Manager — Phone: 331-472-3750 on UCA The People Behind UCA staff page.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Official UCA staff directory lists Meetings & Events Manager — the role most likely to influence Convergence hotel/overflow logistics.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
      qualityAudit: "SUPPORTED",
    },
  ],

  gdi_opp_iaadfs_summit_of_the_americas_2027_6: [
    {
      name: "Olivia Jallits",
      role: "Event Director",
      organization: "IAADFS",
      claimKind: "FACT",
      sourceUrl: "https://www.iaadfs.org/page/Contact",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      sourceAuthorityTier: 1,
      evidenceQuote:
        "IAADFS Contact page staff list includes Olivia Jallits, Event Director.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "EVENT_OWNER",
      whyThisPerson:
        "Named Event Director on official IAADFS contact page for Summit of the Americas series (2027 Palm Beach).",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
      qualityAudit: "SUPPORTED",
    },
    {
      name: "Sarah Appleton",
      role: "Sales Director",
      organization: "IAADFS",
      email: "sappleton@iaadfs.org",
      claimKind: "FACT",
      sourceUrl: "https://www.iaadfs.org/page/Contact",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      sourceAuthorityTier: 1,
      evidenceQuote: "Sarah Appleton, Sales Director on IAADFS Contact page.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.ORGANIZATION_ROLE_ONLY,
      targetRoleMatch: "GENERAL_ORGANIZATION_CONTACT",
      gdiContactRole: "SALES_OWNER",
      whyThisPerson:
        "Official Sales Director — useful secondary for exhibitor/sponsor path; Event Director remains primary for hotel logistics.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      qualityAudit: "SUPPORTED",
      backupPreferred: true,
    },
  ],

  gdi_opp_operation_sailfish_2027_4: [
    {
      name: "Chris Caravello",
      role: "Tournament / series contact",
      organization: "Bluewater Movements",
      email: "Chris@bluewatermovements.com",
      phone: "954-725-4010",
      claimKind: "FACT",
      sourceUrl:
        "https://www.bluewatermovements.com/20-years-of-sailfish-challenge/",
      sourceType: SOURCE_TYPE.OFFICIAL_ORG_SITE,
      sourceAuthorityTier: 1,
      evidenceQuote:
        "Official Bluewater Movements press/contact listings use Chris@bluewatermovements.com and 954-725-4010 for Sailfish Series / Operation Sailfish.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_PROBABLE,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "DIRECT_DECISION_MAKER",
      gdiContactRole: "EVENT_OWNER",
      whyThisPerson:
        "Named contact on official Bluewater Movements releases for Sailfish Series tournaments including Operation Sailfish.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
      qualityAudit: "PLAUSIBLE_NEEDS_VALIDATION",
    },
  ],

  gdi_opp_bluewater_movements_2027_series_partnership_17: [
    {
      name: "Chris Caravello",
      role: "Tournament / series contact",
      organization: "Bluewater Movements",
      email: "Chris@bluewatermovements.com",
      phone: "954-725-4010",
      claimKind: "FACT",
      sourceUrl: "https://www.bluewatermovements.com/",
      sourceType: SOURCE_TYPE.OFFICIAL_ORG_SITE,
      sourceAuthorityTier: 1,
      evidenceQuote:
        "Bluewater Movements official contact path lists Chris@bluewatermovements.com / 954-725-4010.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_PROBABLE,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "DIRECT_DECISION_MAKER",
      gdiContactRole: "EVENT_OWNER",
      whyThisPerson:
        "Same official Bluewater Movements series contact reused across Sailfish Series opportunities.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      qualityAudit: "PLAUSIBLE_NEEDS_VALIDATION",
    },
  ],

  gdi_opp_palm_beach_international_boat_show_2027_vip_over_3: [
    {
      name: "onPeak",
      role: "Official hotel provider",
      organization: "onPeak",
      email: "boatshows@onpeaksupport.com",
      functionalEntity: true,
      claimKind: "FACT",
      sourceUrl: "https://www.pbboatshow.com/",
      sourceType: SOURCE_TYPE.HOUSING_PAGE,
      sourceAuthorityTier: 2,
      evidenceQuote:
        "PBIBS official site: Book your stay with onPeak — the official hotel provider; hotel bookings boatshows@onpeaksupport.com.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT,
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      gdiContactRole: "HOUSING_OWNER",
      whyThisPerson:
        "Official housing vendor for PBIBS VIP/overflow — event owner ≠ hotel operational contact.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      qualityAudit: "SUPPORTED",
      housingVendor: true,
    },
  ],

  gdi_opp_athletic_championships_west_palm_beach_nationals_1: [
    {
      name: "Team Travel Source",
      role: "Varsity Stay Smart housing desk",
      organization: "Team Travel Source",
      phone: "502.354.9103",
      functionalEntity: true,
      claimKind: "FACT",
      sourceUrl: "https://www.teamtravelsource.com/varsity/stay-smart-overview/",
      sourceType: SOURCE_TYPE.HOUSING_PAGE,
      sourceAuthorityTier: 2,
      evidenceQuote:
        "Official Varsity Stay Smart housing overview via Team Travel Source.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT,
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      gdiContactRole: "HOUSING_OWNER",
      whyThisPerson:
        "Housing vendor owns operational hotel outreach for Athletic Championships — retain as functional/housing primary when no named person.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      qualityAudit: "SUPPORTED",
      housingVendor: true,
    },
  ],

  // --- Renaissance ---
  gdi_opp_76th_annual_meeting_3: [
    {
      name: "Michele Smith Koontz",
      role: "Administrative Officer & Meeting Manager",
      organization: "Society for the Study of Social Problems",
      email: "mkoontz3@utk.edu",
      phone: "865-689-1531",
      claimKind: "FACT",
      sourceUrl: "https://www.sssp1.org/index.cfm/m/396/Contact_Us",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      sourceAuthorityTier: 1,
      evidenceQuote:
        "Michele Smith Koontz, Administrative Officer & Meeting Manager — mkoontz3@utk.edu on SSSP Contact Us.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Official Meeting Manager for SSSP Annual Meeting (Westin New York at Times Square 2026).",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
      qualityAudit: "SUPPORTED",
    },
  ],

  gdi_opp_sior_fall_event_7: [
    {
      name: "Katie Hollis",
      role: "Director of Events and Programs",
      organization: "SIOR",
      email: "khollis@sior.com",
      phone: "+1.202.449.8218",
      claimKind: "FACT",
      sourceUrl: "https://sior.com/who-we-are/hq-team",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      sourceAuthorityTier: 1,
      evidenceQuote:
        "Katie Hollis CMP — Director of Events and Programs — khollis@sior.com on SIOR HQ team page.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Official SIOR Director of Events and Programs for Fall Event hotel/block coordination path.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
      qualityAudit: "SUPPORTED",
    },
  ],

  gdi_opp_forum_on_education_abroad_annual_conference_14: [
    {
      name: "Genesis Jardinico",
      role: "Director, Events & Conferences",
      organization: "The Forum on Education Abroad",
      email: "jardinig@forumea.org",
      phone: "717-245-1718",
      claimKind: "FACT",
      sourceUrl: "https://www.forumea.org/the-forum-team.html",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      sourceAuthorityTier: 1,
      evidenceQuote:
        "Genesis Jardinico — Director, Events & Conferences — jardinig@forumea.org on Forum team page; responsible for Annual Conference logistics.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Official Events & Conferences Director for Forum Annual Conference — resolves prior NO_WHO.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      eventSpecificEvidence: true,
      qualityAudit: "SUPPORTED",
    },
    {
      name: "Elizabeth Frohlich",
      role: "Senior Director, Learning & Event Engagement",
      organization: "The Forum on Education Abroad",
      email: "frohlice@forumea.org",
      phone: "717-245-1507",
      claimKind: "FACT",
      sourceUrl: "https://www.forumea.org/the-forum-team.html",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      sourceAuthorityTier: 1,
      evidenceQuote:
        "Elizabeth Frohlich leads the events team and oversees content for the Annual Conference.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "CONFERENCE_DIRECTOR",
      whyThisPerson:
        "Senior events lead — strong secondary/influencer for Annual Conference.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      qualityAudit: "SUPPORTED",
      backupPreferred: true,
    },
  ],

  gdi_opp_nyssba_annual_convention_education_expo_15: [
    {
      name: "Jennifer Kearney",
      role: "Convention and Events",
      organization: "New York State School Boards Association",
      email: "jennifer.kearney@nyssba.org",
      phone: "518.783.3714",
      claimKind: "FACT",
      sourceUrl:
        "https://www.nyssba.org/index.php?src=directory&srctype=staff_lister&submenu=about_nyssba10&view=staff",
      sourceType: SOURCE_TYPE.STAFF_DIRECTORY,
      sourceAuthorityTier: 1,
      evidenceQuote:
        "Jennifer Kearney listed under Convention and Events on NYSSBA staff directory; prior convention materials list her as Convention & Events Manager.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "NYSSBA Convention & Events staff contact; housing remains via Connections Housing partner.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      stillEmployed: true,
      qualityAudit: "SUPPORTED",
    },
    {
      name: "Connections Housing",
      role: "Exclusive convention housing partner",
      organization: "Connections Housing",
      phone: "800-262-9974",
      functionalEntity: true,
      claimKind: "FACT",
      sourceUrl: "https://www.nyssba.org/convention/",
      sourceType: SOURCE_TYPE.HOUSING_PAGE,
      sourceAuthorityTier: 2,
      evidenceQuote:
        "NYSSBA convention page: exclusive housing partner is Connections Housing — do not contact hotels directly.",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT,
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      gdiContactRole: "HOUSING_OWNER",
      whyThisPerson:
        "Official housing vendor for NYSSBA block — secondary/backup to named convention staff.",
      firstSeenAt: V,
      lastVerifiedAt: V,
      qualityAudit: "SUPPORTED",
      housingVendor: true,
      backupPreferred: true,
    },
  ],
});

export const OFFICIAL_PERSON_DISCOVERY_PORTABILITY_PASS_ID =
  "gdi_official_person_discovery_portability_v1";
