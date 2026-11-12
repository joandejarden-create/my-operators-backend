/**
 * Bethesda 29 — targeted commercial research completion (zero-paid first).
 *
 * Applies independently verified public evidence to priority research-stage rows.
 * Never writes opportunities.json. Never Sales Ready without named buyer + thesis.
 * Paid Exa batch requires explicit founder approval (see proposedPaidBatch).
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import { applyHistoricalCommercialCategoryProjection } from "./bethesda-historical-commercial-category-map-v1.js";
import {
  persistResearchStageOpportunityBag,
  researchStageHotelDir,
  writeJsonAtomic,
} from "./gdi-research-stage-persistence-v1.js";
import { evaluateAccountDevelopmentEligibility } from "../account-development-eligibility-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";

export const BETHESDA_29_TARGETED_COMMERCIAL_RESEARCH_V1 =
  "gdi-bethesda-29-targeted-commercial-research-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const FIXTURE_REL = "fixtures/gdi/bethesda-29-historical-reconstruction-v1.json";
const REPORT_DIR_REL = "reports/gdi-bethesda-29-targeted-commercial-research-v1";
const EXPECTED_STORE_SHA =
  "fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad";

export const PRIORITY_TARGET_IDS = Object.freeze([
  "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
  "disp_recLuxvwwxID7U2B8_montgomery_county_shrm",
  "disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter",
  "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
  "disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup",
  "gdi_pep_live_c165c897d233",
  "disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_",
]);

/** First paid batch — STOP until founder approval. Not authorized. */
export const PROPOSED_PAID_BATCH_V1 = Object.freeze({
  authorized: false,
  ceilingUsd: 25,
  batchMaxUsd: 12,
  hardStopUsd: 12,
  provider: "Exa (existing GDI Full E2E demand/contact deep research)",
  expectedCalls: 3,
  opportunityIds: [
    "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
    "disp_recLuxvwwxID7U2B8_montgomery_county_shrm",
    "disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup",
  ],
  questionsById: {
    disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_: [
      "Named person who owns hotel / lodging recommendations for PDA TRI Bethesda training attendees",
      "Whether any preferred-hotel list or housing block exists beyond public nearby-hotel suggestions",
      "Confirm Dec 2026 (and next) Bethesda TRI courses still open for lodging outreach",
    ],
    disp_recLuxvwwxID7U2B8_montgomery_county_shrm: [
      "Named VP Programs / event chair with venue-sourcing authority",
      "Whether any 2027 chapter dinner/training remains venue-open for Bethesda Marriott banquet space",
      "Independent contact route beyond mcshrm@gmail.com",
    ],
    disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup: [
      "Named Halpern Travel housing manager for IWLCA Champions Cup 2027",
      "Whether Bethesda Marriott can be added to residual/overflow inventory under stay-to-play",
      "Confirm Boyds SoccerPlex dates and housing policy still bind teams through Halpern",
    ],
  },
  note: "NOT AUTHORIZATION. Do not reuse historical paid balances. Execute only after explicit founder approval of these three IDs.",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadStagedOrFixture({ root = ROOT } = {}) {
  const staged = path.join(
    researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
    "reconstructed-opportunities-v1.json"
  );
  if (fs.existsSync(staged)) return JSON.parse(fs.readFileSync(staged, "utf8"));
  const fixture = path.join(root, FIXTURE_REL);
  if (fs.existsSync(fixture)) return JSON.parse(fs.readFileSync(fixture, "utf8"));
  return null;
}

/**
 * Zero-paid public evidence recovered 2026-11-12 (Cursor WebSearch).
 * Facts only — no invented room nights or partnership contracts.
 */
export const ZERO_PAID_EVIDENCE_BY_ID = Object.freeze({
  disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce: {
    organizationName: "Greater Bethesda Chamber of Commerce",
    demandActivity:
      "Greater Bethesda Chamber publishes a continuous local events calendar (75+ events/meetings annually per membership materials), including Dec 2026 Legislative Reception, Dec 15 Holiday Off-the-Clock networking, and Jan 26 2027 Board Installation Reception / Annual Membership Meeting.",
    hotelFit:
      "Chamber HQ is in Bethesda (7910 Woodmont Ave). Banquet / reception / day-meeting inventory at Bethesda Marriott is a same-market venue option for Chamber-hosted programmes that are not already venue-committed. Guestrooms optional for catering-led events.",
    primaryContact: {
      name: "Clarissa Harris",
      title: "Vice President, Marketing Communications & Events",
      email: "charris@greaterbethesdachamber.org",
      namedPerson: true,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    },
    secondaryContact: {
      name: "Allie Williams",
      title: "President and CEO",
      email: "awilliams@greaterbethesdachamber.org",
      namedPerson: true,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    },
    sources: [
      {
        label: "Chamber Our Team",
        url: "https://www.greaterbethesdachamber.org/our-team.html",
      },
      {
        label: "Chamber Contact",
        url: "https://www.greaterbethesdachamber.org/contact-us.html",
      },
      {
        label: "Chamber Events Calendar",
        url: "https://web.greaterbethesdachamber.org/mobile/events.aspx",
      },
    ],
    recommendedAction:
      "Introduce Bethesda Marriott banquet/day-meeting inventory to Clarissa Harris for upcoming Chamber receptions and networking programmes; ask which 2027 events remain venue-open and whether Marriott can host one Chamber signature reception.",
    // Shared AD classifier currently marks "Marketing Communications & Events"
    // as BUYER_ROLE_IRRELEVANT — keep Research Continuation despite named contact.
    maturityHint: "RESEARCH_CONTINUATION",
    watchNote:
      "Named events VP verified; AD eligibility currently rejects role taxonomy — do not force AD.",
  },
  disp_recLuxvwwxID7U2B8_montgomery_county_shrm: {
    organizationName: "Montgomery County SHRM",
    demandActivity:
      "MCSHRM runs recurring chapter meetings (typically 3rd Wednesday) plus dated 2026 programmes. Winter Holiday Party is Dec 2 2026 at Manor Country Club (Rockville) — competitor/country-club committed. Multiple recent programmes also used Manor Country Club or Pinstripes North Bethesda.",
    hotelFit:
      "Bethesda Marriott banquet space could compete for future chapter dinners/training when venues are not already committed. Dec 2026 holiday party is not an open Marriott pursuit. Need a 2027 open-venue path before active banquet pursuit.",
    primaryContact: {
      name: null,
      title: null,
      email: "mcshrm@gmail.com",
      namedPerson: false,
      contactClass: "FUNCTIONAL_INBOX",
      route: "mcshrm@gmail.com",
    },
    sources: [
      { label: "MCSHRM Home", url: "https://mcshrm.com/" },
      {
        label: "MCSHRM Holiday Party Detail",
        url: "https://mcshrm.starchapter.com/meetinginfo.php?id=67&ts=1778696788",
      },
      {
        label: "MCSHRM Past Meetings",
        url: "https://mcshrm.starchapter.com/meetinginfo.php?p_or_f=p",
      },
    ],
    recommendedAction:
      "Retain Research Continuation — ask chapter leadership via mcshrm@gmail.com who owns 2027 venue sourcing; do not pursue the Dec 2026 Manor Country Club holiday party as Marriott demand.",
    maturityHint: "RESEARCH_CONTINUATION",
    watchNote: "Competitor-committed holiday party; named venue owner still missing.",
  },
  disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter: {
    organizationName: "PMI Montgomery County Maryland Chapter",
    demandActivity:
      "Chapter runs monthly dinner meetings (e.g. Nov 4 2026 Member Appreciation Dinner at That's Amore, Rockville) and industry events (FedTALKS 2026 at Waldorf Astoria DC). VP Programs seat currently listed as vacant on the board page.",
    hotelFit:
      "Recent dinners use restaurants, not hotel banquet packages. Bethesda Marriott could pitch a future monthly dinner or half-day education meeting if the chapter wants hotel meeting space — not evidenced as current booking intent.",
    primaryContact: {
      name: "Matt Lowy",
      title: "Chapter President",
      email: "president@pmimontcomdchapter.org",
      namedPerson: true,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    },
    sources: [
      {
        label: "PMI-MC Board",
        url: "https://pmimontcomdchapter.org/about/board-members/",
      },
      {
        label: "PMI-MC Events List",
        url: "https://pmimontcomdchapter.org/events/list/",
      },
      {
        label: "Nov 2026 Dinner",
        url: "https://pmimontcomdchapter.org/product/november-2026-pmi-mc-monthly-chapter-dinner-meeting-annual-member-appreciation-dinner/",
      },
    ],
    recommendedAction:
      "Contact Matt Lowy / vpprograms@pmimontcomdchapter.org to learn 2027 programme venue preferences; offer a sample chapter-dinner banquet package only if they confirm interest in hotel space vs restaurant pattern.",
    maturityHint: "RESEARCH_CONTINUATION",
    watchNote: "Named president present; venue pattern currently restaurant-led.",
  },
  disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_: {
    organizationName: "Parenteral Drug Association (PDA)",
    demandActivity:
      "PDA Training and Research Institute (TRI) in Bethesda hosts multiple Dec 2026 courses (e.g. Isolator Systems Dec 16–17 2026; Quality Risk Management Dec 1; Risk Assessment Tools Dec 2–4) at 4350 East West Highway, Bethesda. Public page lists nearby hotels (Hilton Garden Inn, Hyatt Regency Bethesda, Residence Inn) — Bethesda Marriott not listed.",
    hotelFit:
      "Strong same-city lodging demand for multi-day TRI attendees. Bethesda Marriott can pursue overflow / preferred training housing if PDA or attendees need rooms beyond listed options. Not a confirmed Marriott block.",
    primaryContact: {
      name: null,
      title: "Training Course Inquiries",
      email: "training@pda.org",
      namedPerson: false,
      contactClass: "FUNCTIONAL_INBOX",
      route: "training@pda.org",
    },
    secondaryContact: {
      name: "David Hall",
      title: "Exhibition/Sponsorship",
      email: "hall@pda.org",
      namedPerson: true,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      note: "Exhibition contact — not verified as lodging buyer.",
    },
    sources: [
      {
        label: "PDA Isolator Systems Dec 2026",
        url: "https://www.pda.org/global-event-calendar/event-detail/design-validation-and-operation-of-isolator-systems-training-course-pda-557-2",
      },
      {
        label: "PDA TRI Aseptic Events",
        url: "https://www.pda.org/global-event-calendar/training/pda-tri-aseptic-processing-events",
      },
      { label: "PDA Contact", url: "https://www.pda.org/contact" },
    ],
    recommendedAction:
      "Use training@pda.org / programs@pda.org to identify who owns attendee lodging recommendations for TRI Bethesda courses; propose Bethesda Marriott as an additional nearby housing option for Dec 2026 and 2027 TRI weeks.",
    maturityHint: "RESEARCH_CONTINUATION",
    watchNote: "Demand verified; named lodging owner still missing (functional inbox only).",
  },
  disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup: {
    organizationName: "IWLCA / Elite Tournaments - IWLCA Champions Cup",
    demandActivity:
      "2027 IWLCA Champions Cup confirmed June 19–20 2027 at Maryland SoccerPlex, Boyds MD. Stay-to-play: teams must book minimum 8 rooms / 2 nights through Halpern Travel (75-mile commute exemption). Published blocks include SpringHill Suites Gaithersburg, Sheraton Rockville, Cambria Rockville — not Bethesda Marriott.",
    hotelFit:
      "Montgomery County lodging demand is real but housing is controlled by Halpern Travel stay-to-play. Bethesda Marriott is residual/overflow only if Halpern adds the property or teams need rooms outside contracted blocks. Do not treat as primary host-hotel demand.",
    primaryContact: {
      name: null,
      title: "Halpern Travel Reservations",
      email: "reservations@halperntravel.com",
      namedPerson: false,
      contactClass: "FUNCTIONAL_INBOX",
      route: "reservations@halperntravel.com / 888.640.6400",
    },
    sources: [
      {
        label: "IWLCA Champions Cup Travel",
        url: "https://iwlcarecruiting.com/tournaments/iwlca-champions-cup-v2/",
      },
      {
        label: "Mass Elite housing blocks",
        url: "https://www.masselite.com/team/schedules/tournament-details/iwlca-champions-cup/",
      },
      {
        label: "LeagueApps 2027 Champions Cup",
        url: "https://iwlcarecruiting.leagueapps.com/tournaments/5018385-2027-iwlca-champions-cup",
      },
    ],
    recommendedAction:
      "Retain Overflow Watch — contact Halpern Travel to ask whether Bethesda Marriott can be added as residual inventory for Champions Cup 2027; do not solicit teams to break stay-to-play.",
    maturityHint: "OVERFLOW_WATCH",
    watchNote: "Stay-to-play locked to Halpern; Marriott not on published block list.",
  },
  gdi_pep_live_c165c897d233: {
    organizationName: "Potomac Meetings and Events (Potomac Events)",
    demandActivity:
      "Potomac Meetings and Events (brand: Potomac Events) is a DMV conference/meeting consultancy founded by Carrie Rice, CMP, CIS. Public site describes association/corporate conference production (speaker management, BEOs, receptions) — referral/partnership mechanism, not a single dated hotel block.",
    hotelFit:
      "Credible planner intermediary for Bethesda corridor meetings if programmes need hotel space. No evidenced preferred-hotel agreement with Bethesda Marriott.",
    primaryContact: {
      name: "Carrie Rice",
      title: "Founder / Convention Planner (CMP, CIS)",
      email: null,
      namedPerson: true,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      route: "https://potomacevents.com/",
      note: "Named founder verified on public site; direct booking email not published on homepage.",
    },
    sources: [
      { label: "Potomac Events", url: "https://potomacevents.com/" },
      {
        label: "Carrie Rice IDSA profile",
        url: "https://www.idsa.org/profile/carrie-rice/",
      },
    ],
    recommendedAction:
      "Continue Partnership Watch — introduce Bethesda Marriott meeting inventory to Carrie Rice as a Bethesda option for association/corporate programmes; do not present an existing partnership.",
    maturityHint: "PARTNERSHIP_WATCH",
    watchNote: "Named planner found; relationship not established.",
  },
  disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_: {
    organizationName: "Cohera (formerly CSI DMC and 360DG)",
    demandActivity:
      "Cohera is an active DMV destination management company (merger of CSI DMC + 360DG) offering corporate events, museum programmes, and DMC services in Washington DC / nearby markets.",
    hotelFit:
      "DMC can route association/corporate programmes into Bethesda/NIH corridor hotels when clients need lodging near Bethesda activity. Partnership/referral path — not a dated block.",
    primaryContact: {
      name: "Annette Hawks",
      title: "GM, Orlando & Washington, D.C.",
      email: "annette.hawks@meetcohera.com",
      namedPerson: true,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      phone: "407-615-1111",
    },
    secondaryContact: {
      name: null,
      title: "Washington DC destination inbox",
      email: "WashingtonDC@meetcohera.com",
      namedPerson: false,
      contactClass: "FUNCTIONAL_INBOX",
    },
    sources: [
      {
        label: "Annette Hawks team page",
        url: "https://meetcohera.com/our-team/annette-hawks/",
      },
      { label: "Cohera Contact", url: "https://meetcohera.com/contact-us/" },
      {
        label: "Destination DC listing",
        url: "https://washington.org/meetings/find-dc-listings/csi-washington-dc",
      },
    ],
    recommendedAction:
      "Continue Partnership Watch — introduce Bethesda Marriott as a Bethesda/NIH corridor lodging option to Annette Hawks / WashingtonDC@meetcohera.com; request how Cohera currently selects Bethesda hotels.",
    maturityHint: "PARTNERSHIP_WATCH",
    watchNote: "Named DC GM verified; no Marriott preferred agreement evidenced.",
  },
});

function applyZeroPaidEnrichment(row, evidence) {
  const before = {
    maturity: row.commercialMaturity || null,
    contactClass: row.contactRecovery?.contactClass || null,
    namedPerson: row.contactRecovery?.namedPerson === true,
    what: text(row.whatIsHappening).slice(0, 120),
  };

  const contact = evidence.primaryContact || {};
  const named = contact.namedPerson === true && text(contact.name);
  let maturity = evidence.maturityHint || "RESEARCH_CONTINUATION";
  // Never Sales Ready from zero-paid enrichment alone.
  if (maturity === "SALES_READY") maturity = "RESEARCH_CONTINUATION";

  const sources = [
    ...(Array.isArray(row.sources) ? row.sources : []),
    ...evidence.sources.map((s) => ({
      ...s,
      verification: "ZERO_PAID_WEBSEARCH_2026_11_12",
    })),
  ];

  let next = {
    ...row,
    organizationName: evidence.organizationName || row.organizationName,
    whatIsHappening: evidence.demandActivity,
    summaryWhat: evidence.demandActivity,
    tileCustomerSummary: evidence.demandActivity,
    hotelFitRationale: evidence.hotelFit,
    whyThisHotel: evidence.hotelFit,
    summaryWhyHotel: evidence.hotelFit,
    hotelOpportunityThesis: evidence.hotelFit,
    recommendedAction: evidence.recommendedAction,
    nextBestAction: evidence.recommendedAction,
    sources,
    contactRecovery: {
      raw: contact.email || contact.route || contact.name || null,
      namedPerson: Boolean(named),
      contactClass: contact.contactClass || "UNVERIFIED",
      usableRoute: Boolean(contact.email || contact.route),
      email: contact.email || null,
      note: contact.note || "Zero-paid public verification 2026-11-12.",
    },
    primaryContact: named
      ? {
          name: contact.name,
          title: contact.title || null,
          role: contact.title || null,
          email: contact.email || null,
          namedPerson: true,
          contactClass: contact.contactClass,
        }
      : contact.email
        ? {
            email: contact.email,
            role: contact.title || "functional_inbox",
            namedPerson: false,
            contactClass: contact.contactClass || "FUNCTIONAL_INBOX",
          }
        : row.primaryContact || null,
    secondaryContact: evidence.secondaryContact || row.secondaryContact || null,
    zeroPaidResearch: {
      version: BETHESDA_29_TARGETED_COMMERCIAL_RESEARCH_V1,
      verifiedAt: "2026-11-12",
      paidResearch: false,
      provider: "cursor-websearch-zero-paid",
    },
    paidResearch: false,
    customerFacing: false,
    customerVisible: false,
    salesReady: false,
    publicationApproved: false,
    publication: "NOT_AUTHORIZED",
    furtherResearchRequired: true,
  };

  next = applyHistoricalCommercialCategoryProjection(next);

  const missing = new Set(
    (row.missingResearchRequirements || []).filter(
      (m) =>
        m !== "DEMAND_THESIS_INCOMPLETE" &&
        m !== "HOTEL_FIT_DETAIL" &&
        m !== "ORIGINAL_RESEARCH_BODY"
    )
  );
  if (!named) missing.add("VERIFIED_NAMED_COMMERCIAL_CONTACT");
  else missing.delete("VERIFIED_NAMED_COMMERCIAL_CONTACT");
  if (maturity === "OVERFLOW_WATCH") {
    missing.add("INDEPENDENT_OVERFLOW_DEMAND_EVIDENCE");
  }
  missing.add("CUSTOMER_PUBLICATION_NOT_AUTHORIZED");
  next.missingResearchRequirements = [...missing];
  next.unresolvedResearchGaps = [...missing].filter(
    (m) => m !== "CUSTOMER_PUBLICATION_NOT_AUTHORIZED"
  );

  const ad = evaluateAccountDevelopmentEligibility(next, {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
  });
  const shared = evaluateSharedCommercialEligibility(next, {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
  });

  // Enforce disposition honesty. Evidence maturityHint is a ceiling —
  // eligibility may confirm AD only when the hint allows it.
  if (maturity === "OVERFLOW_WATCH") {
    next.historicalRecoveryDisposition = "OVERFLOW_WATCH";
    next.commercialMaturity = "OVERFLOW_WATCH";
    next.commercialStagePill = "Overflow Watch";
    next.priorityPillLabel = "Overflow Watch";
  } else if (maturity === "PARTNERSHIP_WATCH") {
    next.historicalRecoveryDisposition = "PARTNERSHIP_WATCH";
    next.commercialMaturity = "PARTNERSHIP_WATCH";
    next.commercialStagePill = "Partnership Watch";
    next.priorityPillLabel = "Partnership Watch";
  } else if (
    maturity === "ACCOUNT_DEVELOPMENT" &&
    ad.status === "ACCOUNT_DEVELOPMENT" &&
    named
  ) {
    next.commercialMaturity = "ACCOUNT_DEVELOPMENT";
    next.commercialStagePill = "Account Development";
    next.priorityPillLabel = "Account Development";
    next.outreachReadiness = "ACCOUNT_DEVELOPMENT";
  } else {
    next.commercialMaturity = "RESEARCH_CONTINUATION";
    next.commercialStagePill = "Research Continuation";
    next.priorityPillLabel = "Research Continuation";
  }

  // Hard refuse Sales Ready.
  next.salesReady = false;
  if (shared?.status === "SALES_READY") {
    next.commercialEligibility = {
      ...(next.commercialEligibility || {}),
      sharedStatus: shared.status,
      blockedSalesReadyElevation: true,
      note: "Zero-paid enrichment refuses auto Sales Ready elevation.",
    };
    if (next.commercialMaturity === "ACCOUNT_DEVELOPMENT") {
      // keep AD
    } else if (
      next.commercialMaturity !== "OVERFLOW_WATCH" &&
      next.commercialMaturity !== "PARTNERSHIP_WATCH"
    ) {
      next.commercialMaturity = "RESEARCH_CONTINUATION";
    }
  }

  next.accountDevelopmentEligibility = {
    status: ad.status,
    ok: ad.ok === true,
    gaps: ad.gaps || [],
  };

  next.qualityDelta = {
    before,
    after: {
      maturity: next.commercialMaturity,
      contactClass: next.contactRecovery?.contactClass || null,
      namedPerson: next.contactRecovery?.namedPerson === true,
      what: text(next.whatIsHappening).slice(0, 120),
    },
    evidenceImproved: true,
    salesReady: false,
  };

  return next;
}

export function buildBethesda29TargetedCommercialResearch({ root = ROOT } = {}) {
  const staged = loadStagedOrFixture({ root });
  const rows = staged?.opportunities || [];
  const byId = new Map(rows.map((r) => [r.id || r.opportunityId, r]));
  const enriched = [];
  const scorecards = [];

  for (const id of PRIORITY_TARGET_IDS) {
    const before = byId.get(id);
    if (!before) {
      scorecards.push({
        id,
        ok: false,
        error: "MISSING_STAGED_BODY",
      });
      continue;
    }
    const evidence = ZERO_PAID_EVIDENCE_BY_ID[id];
    const after = applyZeroPaidEnrichment(before, evidence);
    enriched.push(after);
    scorecards.push({
      id,
      organizationName: after.organizationName,
      category: after.opportunityTypeLabel,
      beforeMaturity: before.commercialMaturity || null,
      afterMaturity: after.commercialMaturity,
      namedContact: after.primaryContact?.name || null,
      contactClass: after.contactRecovery?.contactClass || null,
      accountDevelopment: after.accountDevelopmentEligibility?.status || null,
      salesReady: false,
      unresolved: after.unresolvedResearchGaps || [],
      sourcesAdded: evidence.sources.length,
      qualityDelta: after.qualityDelta,
    });
  }

  // Non-priority rows pass through unchanged for merge completeness.
  const mergedAll = rows.map((r) => {
    const id = r.id || r.opportunityId;
    const hit = enriched.find((e) => e.id === id);
    return hit || r;
  });

  const oppPath = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    BETHESDA_CANONICAL_HOTEL_ID,
    "opportunities.json"
  );
  const storeSha = fs.existsSync(oppPath)
    ? crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex")
    : null;

  const maturityCounts = {
    ACCOUNT_DEVELOPMENT: 0,
    RESEARCH_CONTINUATION: 0,
    PARTNERSHIP_WATCH: 0,
    OVERFLOW_WATCH: 0,
    SALES_READY: 0,
  };
  for (const s of scorecards) {
    const m = s.afterMaturity || "RESEARCH_CONTINUATION";
    if (maturityCounts[m] != null) maturityCounts[m] += 1;
  }

  return {
    version: BETHESDA_29_TARGETED_COMMERCIAL_RESEARCH_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    safety: {
      paidResearch: false,
      paidResearchAuthorized: false,
      opportunitiesJsonWrites: false,
      shareRegistryWrites: false,
      salesReadyPromotions: 0,
      customerFacing: false,
      researchStageOnly: true,
    },
    priorityIds: [...PRIORITY_TARGET_IDS],
    counts: {
      priorityTargets: PRIORITY_TARGET_IDS.length,
      enriched: enriched.length,
      accountDevelopment: maturityCounts.ACCOUNT_DEVELOPMENT,
      researchContinuation: maturityCounts.RESEARCH_CONTINUATION,
      partnershipWatch: maturityCounts.PARTNERSHIP_WATCH,
      overflowWatch: maturityCounts.OVERFLOW_WATCH,
      salesReady: 0,
    },
    maturityCounts,
    scorecards,
    proposedPaidBatch: PROPOSED_PAID_BATCH_V1,
    opportunityStore: {
      pathRel: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/opportunities.json`,
      sha256: storeSha,
      expectedSha256: EXPECTED_STORE_SHA,
      untouched: storeSha === EXPECTED_STORE_SHA,
    },
    opportunities: mergedAll,
    enrichedOnly: enriched,
  };
}

export function stageBethesda29TargetedCommercialResearch({
  root = ROOT,
  writeFixture = true,
  writeReports = true,
} = {}) {
  const doc = buildBethesda29TargetedCommercialResearch({ root });
  const targetPath = path.join(
    researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
    "reconstructed-opportunities-v1.json"
  );
  const persist = persistResearchStageOpportunityBag({
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    targetPath,
    incomingDoc: {
      version: BETHESDA_29_TARGETED_COMMERCIAL_RESEARCH_V1,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
      safety: doc.safety,
      counts: doc.counts,
      proposedPaidBatch: doc.proposedPaidBatch,
      opportunityStore: doc.opportunityStore,
      generatedAt: doc.generatedAt,
      opportunities: doc.opportunities,
    },
    protectedIds: [...BETHESDA_29_ATTESTED_IDS],
    root,
    label: "bethesda-29-targeted-commercial-research",
    mode: "merge",
  });

  let fixtureWrite = null;
  if (writeFixture) {
    const fixturePath = path.join(root, FIXTURE_REL);
    const prev = fs.existsSync(fixturePath)
      ? JSON.parse(fs.readFileSync(fixturePath, "utf8"))
      : {};
    fixtureWrite = writeJsonAtomic(fixturePath, {
      ...prev,
      version: BETHESDA_29_TARGETED_COMMERCIAL_RESEARCH_V1,
      generatedAt: doc.generatedAt,
      safety: { ...(prev.safety || {}), ...doc.safety },
      targetedCommercialResearch: {
        version: BETHESDA_29_TARGETED_COMMERCIAL_RESEARCH_V1,
        priorityIds: doc.priorityIds,
        proposedPaidBatchAuthorized: false,
      },
      opportunityStore: doc.opportunityStore,
      opportunities: doc.opportunities,
    });
  }

  if (writeReports) {
    const outDir = path.join(root, REPORT_DIR_REL);
    fs.mkdirSync(outDir, { recursive: true });
    writeJsonAtomic(path.join(outDir, "00-zero-paid-summary.json"), {
      version: doc.version,
      generatedAt: doc.generatedAt,
      counts: doc.counts,
      maturityCounts: doc.maturityCounts,
      scorecards: doc.scorecards,
      opportunityStore: doc.opportunityStore,
      safety: doc.safety,
      persistOk: persist.ok === true,
    });
    writeJsonAtomic(path.join(outDir, "01-proposed-paid-batch.json"), {
      ...doc.proposedPaidBatch,
      stop: true,
      message:
        "STOP — awaiting explicit founder approval before any Exa/paid provider calls.",
    });
    writeJsonAtomic(path.join(outDir, "02-priority-scorecards.json"), {
      scorecards: doc.scorecards,
    });
  }

  return { ok: persist.ok === true, persist, fixtureWrite, doc, stagedPath: targetPath };
}
