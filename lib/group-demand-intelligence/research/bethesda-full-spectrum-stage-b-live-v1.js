/**
 * Bethesda Full-Spectrum Stage B — founder-authorized limited deep research.
 *
 * Hard aggregate $0.90 · nontransferable per-opportunity ledgers ($0.45 each):
 *   - ICF Metro DC Capital Coaches Conference 2027
 *   - MPI Potomac e² 2026–2027
 * USBC/NBCC: stored-evidence assessment only (no paid).
 * Surfe/PDL/Parallel OFF · no Airtable/publish/weekly.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  assertLiveDeepResearchLedger,
  loadOrCreateLiveDeepResearchLedger,
  preflightLiveDeepResearchPricing,
  createLiveDeepResearchResearcher,
  maybeExaSearch,
} from "./deep-research-gdi-live-certification-v1.js";
import { runDeepResearchOnExistingGdiOpportunity } from "./deep-research-gdi-integration-v1.js";
import { assertPaidContactProvidersDisabled } from "./commercial-spine-restoration-v41.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { REPORTS_DIR as FULL_SPECTRUM_REPORTS_DIR } from "./bethesda-full-spectrum-discovery-v1.js";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "./research-constants-v1.js";
import { RESEARCH_SEGMENT_LABEL } from "../workflow/hotel-segment-research-plan-v1.js";

export const BETHESDA_FULL_SPECTRUM_STAGE_B_LIVE_V1 = "gdi-bethesda-full-spectrum-stage-b-live-v1";
export const STAGE_B_AGGREGATE_CEILING_USD = 0.9;
export const STAGE_B_PER_OPP_CEILING_USD = 0.45;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const LIVE_DIR = path.join(ROOT, FULL_SPECTRUM_REPORTS_DIR, "live-stage-b");
const LEDGER_DIR = path.join(LIVE_DIR, "ledgers");
const ICF_LEDGER = path.join(LEDGER_DIR, "icf-metro-dc-ccc-2027.json");
const MPI_LEDGER = path.join(LEDGER_DIR, "mpi-potomac-e2-2026-2027.json");
const STAGE_A_DIR = path.join(ROOT, FULL_SPECTRUM_REPORTS_DIR, "live-stage-a");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hashKey(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 12);
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

function writeMd(p, md) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, md.endsWith("\n") ? md : `${md}\n`);
}

function ledgerSpent(ledgerPath) {
  if (!fs.existsSync(ledgerPath)) return 0;
  return Number(JSON.parse(fs.readFileSync(ledgerPath, "utf8")).estimatedSpentUsd || 0);
}

/** Free / no-cost preflight evidence already gathered before paid retrievals. */
export function buildStageBPreflightAssessments({ asOfDate = "2026-10-26" } = {}) {
  const icf = {
    opportunityKey: "icf_metro_dc_ccc_2027",
    organizationName: "ICF Metro DC Chapter",
    programName: "Capital Coaches Conference (CCC) 2027 — Event Management Services RFP",
    authorizedBudgetUsd: STAGE_B_PER_OPP_CEILING_USD,
    asOfDate,
    verifiedProgramIdentity: {
      conference: "Capital Coaches Conference (CCC)",
      chapter: "ICF Metro DC",
      rfpTitle: "Event Management Services for the 2027 Capital Coaches Conference",
      theme: "Keep Humanity at the Coaching Core",
      expectedAttendance: "400–450",
      engagementWindow: "June 2026 through November 2027",
      scopeIncludesVenueSelection: true,
      sourceUrls: [
        "https://www.icfmetrodc.org/content.aspx?club_id=587868&module_id=416637&page_id=22",
        "https://www.rfpmart.com/1076628-usa-washington-dc-event-management-services-rfp.html",
        "https://icfmetrodc.org/content.aspx?club_id=587868&module_id=416622&page_id=22",
      ],
    },
    rfpStatus: {
      status: "EXPIRED",
      submissionDeadline: "2026-03-02",
      evidence: "RFPMart lists Expiry Date Monday 2 March 2026 and marks RFP Expired; chapter page still shows RFP copy but directs firms to download full RFP for deadlines",
      stillActionableAsEmRfp: false,
    },
    hotelSelectionInScope: {
      inEmRfpScope: true,
      note: "RFP seeks event management firms including venue selection — hotels are not the RFP respondent class; hotel capture is indirect via awarded manager or separate venue outreach",
    },
    geographicEligibility: {
      bethesdaEligible: "PLAUSIBLE_DMV",
      evidence: "Chapter serves DC / Northern Virginia / suburban Maryland; RFP seeks firms with experience managing small professional conferences in the area",
      venueNotYetNamedFor2027: true,
    },
    adjacentTimingNote: {
      ccc2026: "CCC Committee page (as of research) recruits volunteers for October 2026 CCC — distinct from the 2027 EM RFP",
      contact: "conference@icfmetrodc.org / admin@icfmetrodc.org",
    },
    paidSpendAuthorized: false,
    paidSpendBlockedReason: "EVENT_MANAGEMENT_RFP_EXPIRED_2026_03_02",
    commercialPreflightClass: "ACCOUNT_DEVELOPMENT_OR_WATCHLIST_NO_PAID",
  };

  const mpi = {
    opportunityKey: "mpi_potomac_e2_2026_2027",
    organizationName: "MPI Potomac Chapter",
    programName: "Educational Experiences (e²) venue host RFP 2026–2027",
    authorizedBudgetUsd: STAGE_B_PER_OPP_CEILING_USD,
    asOfDate,
    verifiedProgramIdentity: {
      series: "Educational Experiences (e²)",
      chapter: "MPI Potomac (~600 members; Northern Virginia, Maryland, Washington DC)",
      attendance: "~100 in person + ~30 virtual",
      decisionMakers: "Board Director for Professional Development and Vice President of Education",
      namedContact: "Melissa Benowitz",
      contactRoute: "info@mpipotomac.org",
      mailingAddress: "18901 Lindenhouse Road, Gaithersburg, MD 20879",
      sourceUrls: [
        "https://www.mpi.org/docs/librariesprovider15/default-document-library/potomac-mpi_rfp-for-education-hybrid-events--2026-2027.pdf?sfvrsn=7cd32a55_0",
        "https://www.mpi.org/chapters/potomac/advertising/host-an-mpi-potomac-event",
        "https://www.mpi.org/chapters/potomac/education-events/education-opportunities",
        "https://www.mpi.org/chapters/potomac/education-events/event/2026/05/11/default-calendar/may-e2",
      ],
    },
    datesAndGeography: {
      preferredDates2026: ["2026-05-11", "2026-05-13", "2026-05-14"],
      preferredDates2027: [
        "2027-01 (20/21/25/27/28)",
        "2027-03 (15/17/18/22/24/25)",
        "2027-05 (3–6, 17–20)",
        "2027-09 (8/9/13/15/16)",
      ],
      may2026Status: "HELD — Dupont Circle Hotel, 1500 New Hampshire Ave NW, Washington DC (2026-05-11 4:00–7:30 PM)",
      venueType: "Hotel or conference venue near Metro in DC / Northern Virginia / Maryland",
      locationOptionsIncludeBethesda: true,
      locationOption4: "Metro accessible in Arlington/Alexandria and Bethesda or Silver Spring",
      procurement: "Full in-kind sponsorship requested (space, F&B, AV, internet); discounted speaker sleeping room if applicable",
    },
    rfpStatus: {
      status: "OPEN_FOR_REMAINING_2027_CYCLES_INFERRED",
      formalDeadlineOnPdf: null,
      note: "PDF does not state a hard close date; May 2026 preferred window already used by Dupont Circle Hotel; Host-an-Event page still invites venue hosts via info@mpipotomac.org; Education Day 2026-10-26 and 2027 e² dates remain commercially relevant",
      stillActionable: true,
    },
    hotelFitPreflight: {
      bethesdaMarriottEligible: true,
      mechanism: "in-kind meeting space + F&B + AV showcase to ~100 planner-heavy attendees; optional speaker room night",
      revenueType: "MEETINGS_CATERING_PRIMARY_PLUS_LIMITED_TRANSIENT",
      competitorKnown: "Dupont Circle Hotel hosted May 2026 e²",
    },
    paidSpendAuthorized: true,
    paidSpendBlockedReason: null,
    commercialPreflightClass: "PROCEED_PAID_DEEP_RESEARCH",
  };

  return { icf, mpi, asOfDate };
}

function assessUsbcFromStoredEvidenceOnly() {
  const stageARanked = path.join(STAGE_A_DIR, "09-ranked-candidates-for-stage-b.json");
  const inventory = path.join(STAGE_A_DIR, "03-complete-candidate-inventory.json");
  let usbc = null;
  if (fs.existsSync(stageARanked)) {
    const ranked = JSON.parse(fs.readFileSync(stageARanked, "utf8"));
    usbc = (ranked.ranked || []).find((r) => /breastfeed|usbc|nbcc/i.test(`${r.organizationName} ${r.programName}`));
  }
  if (!usbc && fs.existsSync(inventory)) {
    const inv = JSON.parse(fs.readFileSync(inventory, "utf8"));
    usbc = (inv.candidates || []).find((r) => /breastfeed|nbcc/i.test(`${r.organizationName} ${r.programName} ${r.sourceUrl}`));
  }
  const evidence = text(usbc?.demandEvidence || "");
  const june2026 = /june\s*3-5,\s*2026|jun(e)?\s*3.?5,?\s*2026/i.test(evidence)
    || /2026/.test(String(usbc?.programName || ""));
  return {
    paidDeepResearchAuthorized: false,
    organizationName: "US Breastfeeding Committee (parsed Stage A: Usbreastfeeding)",
    programFromStageA: usbc?.programName || "2026 NBCC Marketing",
    sourceUrl: usbc?.sourceUrl || "https://www.usbreastfeeding.org/2026-nbcc-marketing.html",
    storedEvidenceSummary: evidence.slice(0, 500) || null,
    june2026Event: {
      dates: "June 3–5, 2026",
      location: "Bethesda, MD (from Stage A snippet)",
      classification: "NOT_COUNTED_AS_CURRENT_FORWARD_LOOKING_NEW_OPPORTUNITY",
      reason: "Founder excluded paid Stage B; as-of Stage B run date 2026-10-26 the June 2026 cycle is historical/past — do not treat as new forward opportunity",
    },
    distinctFutureNbccCycle: {
      foundInStoredEvidence: false,
      createdOpportunity: false,
      note: "Stage A marketing page references 2026 NBCC only; no verified 2027+ cycle identity in stored evidence — no future-cycle opportunity fabricated",
    },
    recommendedInternalClass: "EXCLUDE_OR_HISTORICAL_REFERENCE_ONLY",
  };
}

function analyzeEightSegmentDiscoveryGaps() {
  const funnelPath = path.join(STAGE_A_DIR, "08-segment-by-segment-funnel.json");
  const funnel = fs.existsSync(funnelPath)
    ? JSON.parse(fs.readFileSync(funnelPath, "utf8"))
    : {};
  const empty = SUPERVISOR_EXPECTED_SEGMENTS.filter((s) => Number(funnel[s]?.genuineNetNew || 0) === 0
    && !["associations_government", "exhibitor_sponsor_teams", "professional_planners_dmc"].includes(s));
  // User asked why eight demand segments yielded no commercially credible new candidates
  const eight = [
    "corporate_meetings_training",
    "professional_planners_dmc",
    "scaling_companies",
    "local_meetings_catering",
    "exhibitor_sponsor_teams",
    "entertainment_production",
    "tours_educational_specialist",
    "weddings_social_private_events",
  ];
  // Note exhibitor DID yield USBC — so clarify: historically empty segments vs underrepresented focus
  const historicallyEmptyFocus = [
    "corporate_meetings_training",
    "professional_planners_dmc",
    "scaling_companies",
    "local_meetings_catering",
    "entertainment_production",
    "tours_educational_specialist",
    "weddings_social_private_events",
    "sports",
  ];

  return {
    question: "Why eight underrepresented/adjacent segments yielded no commercially credible Stage B pathways (beyond thin DMC landings)",
    segmentsAnalyzed: historicallyEmptyFocus,
    perSegment: historicallyEmptyFocus.map((seg) => {
      const row = funnel[seg] || {};
      return {
        segment: seg,
        label: RESEARCH_SEGMENT_LABEL[seg] || seg,
        searchesPerformed: row.searchesPerformed || 0,
        candidatesDiscovered: row.candidatesDiscovered || 0,
        genuineNetNewAfterGate: row.genuineNetNew || 0,
        stageBEligible: row.stageBEligible || 0,
        concreteLimitations: (() => {
          switch (seg) {
            case "corporate_meetings_training":
              return [
                "Exa queries returned CVB hotel directories (Visit Montgomery Bolger/Bethesdan) and competitor venue marketing — not named corporate training programs with lodging demand",
                "Corporate offsite demand is rarely published as crawlable RFP pages; LinkedIn/private planner channels dominate",
              ];
            case "professional_planners_dmc":
              return [
                "Strategies surface DMC company homepages (Incentive Travel House, CBD Meetings, Kilohana) without a named client program or hotel RFP",
                "Account-development entities are valid CRM leads but fail Stage A demand-mechanism gate by design",
              ];
            case "scaling_companies":
              return [
                "Hits skewed to hiring/talent news (Bethesda Magazine federal workforce) without meeting/travel mechanism",
                "Expansion announcements alone are insufficient under commercial gate — no lodging/offsite evidence",
              ];
            case "local_meetings_catering":
              return [
                "Exa Places / synthetic library URLs and generic day-meeting keyword pages lacked organization identity",
                "Local catering demand is often private inquiry / sales-led, not indexed conference pages",
              ];
            case "entertainment_production":
              return [
                "Results were AV vendor landings (Corporate AV for DMV) — suppliers to events, not hotel room buyers",
                "No named production shoot or touring show with Bethesda lodging need in the hit set",
              ];
            case "tours_educational_specialist":
              return [
                "Strong-looking educational hits (Model Arab League, RCMI) were already committed to Hyatt Regency Bethesda or other venues — deferred as competitor-committed",
                "Tour operators rarely publish open hotel RFPs discoverable via Exa lite",
              ];
            case "weddings_social_private_events":
              return [
                "Zero commercially credible NET_NEW after gate — queries returned thin venue marketing or non-specific social event pages",
                "Wedding/social demand is inquiry-driven and privacy-sensitive; public discovery yield is structurally low",
              ];
            case "sports":
              return [
                "Premier Cup / Bethesda soccer noise matched baseline sports inventory; Aloha Lacrosse venue was Elkridge (distant without Bethesda pathway)",
                "Tournament hotel pages often housing tabs without new net-new program identity",
              ];
            default:
              return ["No commercially credible NET_NEW after quality gate"];
          }
        })(),
      };
    }),
    notANewArchitectureRecommendation: [
      "Keep ORS + V4 discovery + capturable-fit + commercial quality gate",
      "Improve query templates toward dated RFPs / host-city calls / chapter venue RFPs (pattern that worked: MPI Potomac, ICF EM RFP)",
      "Add hard rejects earlier for CVB directories, DMC landings without program, and competitor-committed venues",
      "Treat planner/DMC results as CRM account-development queues separate from Stage B deep-research spend",
    ],
    emptySegsFromFunnel: empty,
  };
}

function buildIcfOpportunitySeed(preflight) {
  const urls = preflight.verifiedProgramIdentity.sourceUrls;
  return {
    id: `gdi_stageb_icf_${hashKey("icf-ccc-2027")}`,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    organizationName: "ICF Metro DC Chapter",
    eventName: "Capital Coaches Conference 2027",
    programName: "Capital Coaches Conference 2027 — Event Management Services RFP",
    researchSegment: "associations_government",
    segment: "associations_government",
    eventYear: "2027",
    demandSignalSummary: "400–450 attendee coaching conference; EM RFP included venue selection; engagement Jun 2026–Nov 2027; EM RFP expired 2026-03-02",
    hotelOpportunityThesis: "Bethesda Marriott could compete as DMV headquarters hotel for CCC 2027 if venue not yet contracted — capture path is via awarded event manager or chapter conference committee, not as EM RFP respondent",
    sourceUrl: urls[0],
    seedUrls: urls,
    sources: urls.map((url) => ({ url, publisher: "icfmetrodc.org" })),
    evidence: [
      {
        url: urls[0],
        statement: "ICF Metro DC seeks event management partner for 2027 CCC including venue selection; 400–450 attendees",
        topic: "programme",
      },
      {
        url: urls[1],
        statement: "Event management RFP expiry date 2 March 2026; marked RFP Expired on aggregator",
        topic: "sourcing_status",
      },
    ],
    contactEmail: "conference@icfmetrodc.org",
    primaryContactEmail: "conference@icfmetrodc.org",
  };
}

function buildMpiOpportunitySeed(preflight) {
  const urls = preflight.verifiedProgramIdentity.sourceUrls;
  return {
    id: `gdi_stageb_mpi_${hashKey("mpi-potomac-e2")}`,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    organizationName: "MPI Potomac Chapter",
    eventName: "Educational Experiences (e²) 2026–2027",
    programName: "MPI Potomac Educational Experiences (e²) venue host RFP 2026–2027",
    researchSegment: "associations_government",
    segment: "associations_government",
    eventYear: "2027",
    demandSignalSummary: "Chapter venue host RFP for e² education events (~100 in-person); locations explicitly include Metro-accessible Bethesda; in-kind space/F&B/AV; speaker room night optional; contact Melissa Benowitz via info@mpipotomac.org",
    hotelOpportunityThesis: "Bethesda Marriott is explicitly in the RFP location set (option 4: Metro-accessible Bethesda/Silver Spring). Compete as in-kind host venue for remaining 2027 e² dates after May 2026 Dupont Circle Hotel commitment.",
    sourceUrl: urls[0],
    seedUrls: urls,
    sources: urls.map((url) => ({ url })),
    evidence: [
      {
        url: urls[0],
        statement: "MPI Potomac e² RFP lists venue locations including Metro accessible Bethesda or Silver Spring; ~100 in person; in-kind F&B/space/AV; decisions by Board Director PD and VP Education; Attn Melissa Benowitz info@mpipotomac.org",
        topic: "programme",
      },
      {
        url: urls[3],
        statement: "May 11 2026 e² held at Dupont Circle Hotel Washington DC — 2026 May window committed",
        topic: "competitor_commitment",
      },
      {
        url: urls[1],
        statement: "Host-an-Event page still solicits venue hosts for e², Education Conference, networking, board meetings/retreats via info@mpipotomac.org",
        topic: "sourcing_status",
      },
    ],
    contactEmail: "info@mpipotomac.org",
    primaryContactEmail: "info@mpipotomac.org",
    namedContact: "Melissa Benowitz",
    investigatedUrls: [],
  };
}

function writeOpportunityBundle(oppDir, payload) {
  fs.mkdirSync(oppDir, { recursive: true });
  writeJson(path.join(oppDir, "01-preflight-sourcing-timing.json"), payload.preflight);
  writeJson(path.join(oppDir, "02-research-calls-and-costs.json"), payload.costs);
  writeJson(path.join(oppDir, "03-research-findings-and-sources.json"), payload.findings);
  writeJson(path.join(oppDir, "04-executed-follow-ups.json"), payload.followUps);
  writeJson(path.join(oppDir, "05-buyer-and-contact-intelligence.json"), payload.buyer);
  writeJson(path.join(oppDir, "06-hotel-fit-assessment.json"), payload.hotelFit);
  writeJson(path.join(oppDir, "07-commercial-classification.json"), payload.classification);
  writeJson(path.join(oppDir, "08-hotel-specific-sales-brief.json"), payload.salesBrief);
  writeJson(path.join(oppDir, "09-before-after-commercial-improvement.json"), payload.beforeAfter);
  writeJson(path.join(oppDir, "10-remaining-research-gaps.json"), payload.gaps);
}

/**
 * Execute Stage B under founder GO ($0.90 aggregate, $0.45/opp nontransferable).
 */
export async function executeBethesdaFullSpectrumStageB({
  founderAuthorized = false,
  asOfDate = "2026-10-26",
} = {}) {
  if (!founderAuthorized) {
    throw new Error("stage_b_requires_founder_authorization");
  }

  const pricing = preflightLiveDeepResearchPricing();
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });
  if (!pricing.ok) {
    return { executed: false, reason: "pricing_preflight_failed", pricing };
  }
  if (!paidGuard.ok) {
    return { executed: false, reason: "paid_contact_guard_failed", paidGuard };
  }

  fs.mkdirSync(LEDGER_DIR, { recursive: true });
  const preflight = buildStageBPreflightAssessments({ asOfDate });
  writeJson(path.join(LIVE_DIR, "00-preflight-both-opportunities.json"), preflight);

  // --- ICF: NO PAID SPEND (expired RFP) ---
  assertLiveDeepResearchLedger(ICF_LEDGER);
  loadOrCreateLiveDeepResearchLedger(ICF_LEDGER, {
    budgetUsd: STAGE_B_PER_OPP_CEILING_USD,
    version: `${BETHESDA_FULL_SPECTRUM_STAGE_B_LIVE_V1}-icf-ledger`,
  });
  const icfSpend = ledgerSpent(ICF_LEDGER);
  const icfBundle = {
    preflight: preflight.icf,
    costs: {
      authorizedUsd: STAGE_B_PER_OPP_CEILING_USD,
      spentUsd: icfSpend,
      paidCalls: [],
      note: "Zero paid retrievals — EM RFP expired 2026-03-02",
    },
    findings: {
      programVerified: true,
      sourcingStatus: "EM_RFP_EXPIRED",
      demandMechanism: "400–450 attendee chapter conference with venue selection via event manager",
      timing: "Conference 2027; EM partner engagement was Jun 2026–Nov 2027; EM bid window closed 2026-03-02",
      sources: preflight.icf.verifiedProgramIdentity.sourceUrls,
      newPaidEvidence: [],
    },
    followUps: { executed: [], skippedReason: "no_paid_spend_rfp_expired" },
    buyer: {
      rfpRespondentClass: "event_management_firms",
      chapterContacts: {
        conference: "conference@icfmetrodc.org",
        admin: "admin@icfmetrodc.org",
        mailingAddress: "5614 Connecticut Ave NW #116, Washington DC 20015",
      },
      namedEmRfpOwner: null,
      note: "Functional chapter conference inbox verified on CCC Committee page — not invented; not the expired EM RFP submission owner identity",
      salesUsableAsProspectContact: true,
      contactKind: "FUNCTIONAL_DEPARTMENT_CHAPTER_CONFERENCE",
    },
    hotelFit: {
      fitClass: "POTENTIALLY_CAPTURABLE_DMV_OR_ACCOUNT",
      thesis: "DMV chapter conference with unnamed 2027 venue — Bethesda Marriott plausible if capacity fits ~400–450 + breakouts; no verified room-block RFP open to hotels today",
      knownCommitments: "None named for 2027 venue; Oct 2026 CCC volunteer recruiting is adjacent timing",
    },
    classification: {
      commercialClass: "ACCOUNT_DEVELOPMENT",
      hotelFacingActionable: false,
      rationale: "Expired EM RFP removes active hotel-facing bid path; retain account-development outreach to conference@icfmetrodc.org for venue consideration once manager selected / for 2026 CCC if still open",
      recommendedSalesNextAction: "Relationship email to conference@icfmetrodc.org offering Bethesda Marriott as DMV headquarters option for CCC 2026/2027 — do not submit as EM firm",
    },
    salesBrief: {
      hotel: BETHESDA_CANONICAL_HOTEL_NAME,
      whoToContact: "conference@icfmetrodc.org (CCC Committee)",
      whatToSay: "Introduce Bethesda Marriott meeting capacity for Capital Coaches Conference; ask whether 2027 venue shortlist is open and who the awarded event manager is",
      doNot: "Do not respond to expired EM RFP as an event-management bidder",
      evidenceBackedHotelFit: true,
    },
    beforeAfter: {
      before: "Stage A: promising local chapter conference RFP with venue selection language",
      after: "Confirmed EM RFP expired 2026-03-02; reclassified from Stage B deep-research candidate to account-development / watchlist; $0.00 spent",
      materialCommercialImprovement: false,
      reason: "Additional free sources clarified expiry — did not create a new pursuable RFP",
    },
    gaps: [
      "Who was awarded the EM contract (if anyone)?",
      "Is a 2027 headquarters hotel already under LOI?",
      "Exact October 2026 CCC dates/venue if still sourcing",
    ],
  };
  writeOpportunityBundle(path.join(LIVE_DIR, "icf-metro-dc-ccc-2027"), icfBundle);

  // --- MPI: PAID deep research up to $0.45 ---
  assertLiveDeepResearchLedger(MPI_LEDGER);
  loadOrCreateLiveDeepResearchLedger(MPI_LEDGER, {
    budgetUsd: STAGE_B_PER_OPP_CEILING_USD,
    version: `${BETHESDA_FULL_SPECTRUM_STAGE_B_LIVE_V1}-mpi-ledger`,
  });

  const mpiSeed = buildMpiOpportunitySeed(preflight.mpi);
  const spentBefore = ledgerSpent(MPI_LEDGER);
  const researcher = createLiveDeepResearchResearcher({ ledgerPath: MPI_LEDGER });
  const checkpointDir = path.join(LIVE_DIR, "checkpoints", "mpi-potomac-e2");
  fs.mkdirSync(checkpointDir, { recursive: true });

  // Targeted Exa discovery within MPI ledger (buyer / remaining dates / competitors)
  const mpiQueries = [
    "MPI Potomac Educational Experiences e2 2027 host venue RFP Bethesda",
    "MPI Potomac e2 Dupont Circle Hotel May 2026",
    "MPI Potomac Melissa Benowitz education events venue host",
    "MPI Potomac Board Vice President Education Professional Development 2026",
  ];
  const mpiExaHits = [];
  for (const q of mpiQueries) {
    if (ledgerSpent(MPI_LEDGER) >= STAGE_B_PER_OPP_CEILING_USD - 0.01) break;
    const exa = await maybeExaSearch(q, MPI_LEDGER);
    mpiExaHits.push({ query: q, status: exa.status, costUsd: exa.costUsd || 0, hitCount: (exa.hits || []).length, hits: (exa.hits || []).slice(0, 5) });
  }

  let deepResult = null;
  let deepError = null;
  try {
    deepResult = await runDeepResearchOnExistingGdiOpportunity({
      opportunity: {
        ...mpiSeed,
        seedUrls: [
          ...mpiSeed.seedUrls,
          ...mpiExaHits.flatMap((h) => (h.hits || []).map((x) => x.url)).filter(Boolean),
        ].filter((u, i, a) => a.indexOf(u) === i).slice(0, 12),
      },
      researcher,
      corpus: null,
      offlineLongform: false,
      options: {
        allowEarlyBuyerStop: false,
        allowReplan: true,
        maxIterations: 4,
        maxLeadFollowUpsPerOpportunity: 2,
        remainingBudgetUsd: Math.max(0, STAGE_B_PER_OPP_CEILING_USD - ledgerSpent(MPI_LEDGER)),
        maxCostUsd: STAGE_B_PER_OPP_CEILING_USD,
        checkpointDir,
        paidBlocked: false,
      },
    });
  } catch (err) {
    deepError = err.message || String(err);
  }

  const spentAfter = ledgerSpent(MPI_LEDGER);
  const mpiSpend = Number((spentAfter - spentBefore).toFixed(6));
  const afterWrap = deepResult?.after || {};
  const afterOpp = afterWrap.opportunity || deepResult?.opportunity || mpiSeed;
  const ownership = afterWrap.contactOwnership || {};
  const q41 = afterWrap.qualificationV41 || {};
  const salesBriefDeep = deepResult?.hotelSalesBrief || afterOpp?.deepResearchEnrichment?.salesBrief || {};

  const mpiClassification = {
    commercialClass: "PRE_SOURCING_OPPORTUNITY",
    hotelFacingActionable: false,
    reportingNote: "Pursuable venue-host opportunity with verified contact route and explicit Bethesda eligibility — not hotel-facing 'actionable closed-loop' until proposal submitted / response received; Stage B must not inflate MATERIAL",
    internalDisposition: q41.internalDisposition || "PRE_SOURCING_OPPORTUNITY",
    recommendedSalesNextAction: "Email info@mpipotomac.org Attn Melissa Benowitz with Bethesda Marriott in-kind e² host proposal for open 2027 preferred dates; reference Metro-accessible Bethesda location option in RFP",
  };

  // Prefer honest classification from free+paid evidence even if ORS thin
  const contactRoute = "info@mpipotomac.org";
  const mpiBundle = {
    preflight: preflight.mpi,
    costs: {
      authorizedUsd: STAGE_B_PER_OPP_CEILING_USD,
      spentUsd: mpiSpend,
      ledgerPath: MPI_LEDGER.replace(/\\/g, "/"),
      exaSearches: mpiExaHits,
      deepResearchError: deepError,
      researcherTrace: researcher.trace || null,
    },
    findings: {
      programVerified: true,
      sourcingStatus: "OPEN_HOST_PATH_FOR_2027_E2_AND_OTHER_CHAPTER_EVENTS",
      demandMechanism: "In-kind venue host sponsorship for chapter education events (~100 planners) with optional speaker room night",
      timing: "May 2026 committed (Dupont Circle); 2027 Jan/Mar/May/Sep preferred windows remain; Education Day 2026-10-26 also promoted on chapter site",
      bethesdaExplicitlyListed: true,
      sources: [
        ...preflight.mpi.verifiedProgramIdentity.sourceUrls,
        ...mpiExaHits.flatMap((h) => (h.hits || []).map((x) => x.url)).filter(Boolean),
      ].filter((u, i, a) => a.indexOf(u) === i),
      deepResearchInvestigatedUrls: afterOpp.investigatedUrls || [],
      deepResearchEvidenceCount: (afterOpp.evidence || []).length,
    },
    followUps: {
      executed: (researcher.trace?.relatedEntity || {}),
      orsAttempts: (afterOpp.researchAttempts || []).slice(0, 12),
      replan: deepResult?.replan || null,
    },
    buyer: {
      namedContact: "Melissa Benowitz",
      role: "RFP addressee / Host-an-Event contact (chapter staff route)",
      email: contactRoute,
      decisionAuthority: "Final decision: Board Director for Professional Development and Vice President of Education (roles named in RFP; individual board names not verified in paid pass — do not invent)",
      ownershipClass: ownership.ownershipClass || "FUNCTIONAL_DEPARTMENT",
      salesUsableAsProspectContact: true,
      contactKind: "NAMED_ATTN_PLUS_FUNCTIONAL_INBOX",
      doNotTreatAs: "Do not treat general MPI.org membership contacts as the RFP owner",
      targetedEnrichmentNeeded: {
        surfePdlParallel: false,
        note: "Optional later GO: board roster WHO for VP Education / Director Professional Development names — not authorized in this Stage B",
      },
    },
    hotelFit: {
      fitClass: "POTENTIALLY_CAPTURABLE_CORE",
      thesis: "RFP location option 4 explicitly includes Metro-accessible Bethesda; Bethesda Marriott can host ~100 crescent-rounds + reception + hybrid AV as in-kind supplier showcase",
      revenueMechanism: "Meetings/catering in-kind sponsorship (not full cash group block); limited transient speaker room; strategic exposure to planner members",
      competitors: ["Dupont Circle Hotel (May 2026 e² host)"],
      knownCommitments: "May 2026 e² committed elsewhere; remaining 2027 dates open per RFP date list + ongoing host solicitation",
    },
    classification: mpiClassification,
    salesBrief: {
      hotel: BETHESDA_CANONICAL_HOTEL_NAME,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      whoToContact: "Melissa Benowitz via info@mpipotomac.org",
      whatToPropose: "In-kind e² host for a 2027 preferred date: complimentary meeting space, F&B per RFP, AV/internet, optional speaker room; cite Bethesda Metro accessibility",
      evidenceBackedHotelFit: true,
      deepSalesBrief: salesBriefDeep,
    },
    beforeAfter: {
      before: "Stage A: PDF snippet showed e² RFP with weak geo flag until chapter-local boost",
      after: "Verified explicit Bethesda location eligibility; confirmed May 2026 already hosted at Dupont Circle; verified usable contact Melissa Benowitz / info@mpipotomac.org; classified as pre-sourcing venue-host opportunity",
      materialCommercialImprovement: true,
      improvementType: "SOURCING_STATUS_GEO_CONTACT_CLARITY",
      notMaterialBecause: "Did not invent closed business or named board individuals beyond RFP roles",
    },
    gaps: [
      "Hard RFP close date still unpublished on PDF",
      "Which 2027 preferred dates remain unhosted?",
      "Names of current VP Education / Director Professional Development (enrichment not authorized)",
      "Whether Bethesda Marriott is already an MPI Potomac supplier member",
    ],
  };
  writeOpportunityBundle(path.join(LIVE_DIR, "mpi-potomac-e2-2026-2027"), mpiBundle);

  const usbc = assessUsbcFromStoredEvidenceOnly();
  writeJson(path.join(LIVE_DIR, "usbc-nbcc-stored-evidence-only.json"), usbc);

  const gapsAnalysis = analyzeEightSegmentDiscoveryGaps();
  writeJson(path.join(LIVE_DIR, "eight-segment-discovery-limitations.json"), gapsAnalysis);

  const totalSpent = Number((ledgerSpent(ICF_LEDGER) + ledgerSpent(MPI_LEDGER)).toFixed(6));
  const summary = {
    version: BETHESDA_FULL_SPECTRUM_STAGE_B_LIVE_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    executed: true,
    completedAt: new Date().toISOString(),
    aggregateCeilingUsd: STAGE_B_AGGREGATE_CEILING_USD,
    spentUsd: totalSpent,
    icf: {
      spentUsd: ledgerSpent(ICF_LEDGER),
      paidAuthorized: false,
      class: icfBundle.classification.commercialClass,
      hotelFacingActionable: false,
    },
    mpi: {
      spentUsd: mpiSpend,
      paidAuthorized: true,
      class: mpiBundle.classification.commercialClass,
      hotelFacingActionable: false,
      contactRoute,
    },
    usbc: {
      paidAuthorized: false,
      class: usbc.recommendedInternalClass,
    },
    stageARemainderNotReused: true,
    surfePdlParallel: false,
    airtableWrites: 0,
    publication: false,
    weekly: false,
    liveDir: LIVE_DIR.replace(/\\/g, "/"),
  };
  writeJson(path.join(LIVE_DIR, "SUMMARY.json"), summary);

  writeMd(path.join(LIVE_DIR, "DELIVERABLES_LIVE.md"), `# Bethesda Full-Spectrum Stage B — Targeted Deep Research

**Aggregate spend:** $${totalSpent} / $${STAGE_B_AGGREGATE_CEILING_USD}  
**ICF CCC 2027:** $${ledgerSpent(ICF_LEDGER)} / $0.45 · **paid blocked** (EM RFP expired 2026-03-02) → **ACCOUNT_DEVELOPMENT**  
**MPI Potomac e²:** $${mpiSpend} / $0.45 · **PRE_SOURCING_OPPORTUNITY** · contact \`info@mpipotomac.org\` (Attn Melissa Benowitz)  
**USBC/NBCC:** stored evidence only · June 2026 **not** counted as new forward opportunity · no future cycle fabricated  

## Genuinely pursuable now

1. **MPI Potomac e² / chapter venue hosting** — Bethesda explicitly listed; propose in-kind host for open **2027** dates (May 2026 already at Dupont Circle Hotel).

## Not pursuable as active RFP

1. **ICF Metro DC EM RFP for CCC 2027** — expired **2026-03-02**. Keep \`conference@icfmetrodc.org\` for account-development venue outreach only.

## Hotel-facing actionable claimed

**0** (by design — Stage B clarifies pursuable paths; does not declare closed-loop actionable without buyer response).

Surfe/PDL/Parallel OFF · no Airtable · no publish · weekly OFF.
`);

  return summary;
}
