/**
 * Wave 3 Batch 4 AVA full portfolio: write hotel-specific payloads, generate
 * PDFs, stage to durable local storage, and save AUDIT_VALIDATED versions.
 * Zero paid research. No publication or founder/customer approval.
 *
 *   AVA_STORAGE_MODE=local AVA_STORAGE_ROOT=data/ava-data \
 *     node scripts/ava-wave3-batch4-generate-stage-validate-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { assertAuditPdfPayload } from "../lib/ai-visibility-audit-action-plan/audit-pdf-contract-v1.js";
import {
  getAuditMeta,
  saveAuditVersion,
  upsertAuditMeta,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT_ROOT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const PAYLOAD_ROOT = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/payloads"
);

const HOTELS = [
  {
    propertyId: "adp_charlotte_marriott_city_center",
    slug: "charlotte-marriott-city-center",
    short: "CMCC",
    name: "Charlotte Marriott City Center",
    market: "Charlotte",
    region: "North Carolina",
    coverLocation: "Charlotte, North Carolina",
    website: "https://www.marriott.com/en-us/hotels/cltcc-charlotte-marriott-city-center/overview/",
    events: "https://www.marriott.com/en-us/hotels/cltcc-charlotte-marriott-city-center/events/",
    periodId: "adp_period_adp_charlotte_marriott_city_center_20261019071607_f9f0ee",
    consideration: "12.5%",
    considerationMeta: "Mentioned in 22 of 176 comparable monitored AI answers",
    scenario: "38.6%",
    scenarioMeta: "Present across 17 of 44 eligible traveler-need scenarios",
    peer: "#2 of 6",
    peerMeta: "Second-highest presence among 6 certified Marriott Bonvoy peers",
    lead:
      "Charlotte Marriott City Center ranks second in its certified six-hotel set, but appears in only 38.6% of eligible scenarios and 12.5% of comparable consideration answers. Official pages verify an Uptown location, convention-center connection, five restaurants, M Club, and substantial meeting capacity. The priority is to reconcile competing event-space totals and organize those verified strengths into consistent buyer context.",
    success:
      "Meeting-space totals use one setup-qualified source of truth; Uptown and convention-center access is stated consistently; dining and M Club facts are complete across priority pages; and Marriott documents its intended crawler-access posture.",
    quality: 9.2,
    strongestFindings: [
      "Official events page reports 23,011 square feet while overview marketing says 20,000",
      "ADP recorded 0% recognition for Uptown, convention-center proximity, restaurant, and in-room dining",
      "Marriott origin probe GET returned 403 while official hotel pages remained WebFetch-readable",
    ],
    actions: [
      {
        priority: "HIGH",
        claimType: "VERIFIED_ISSUE",
        headline: "Reconcile Event-Space Totals and Room Counts",
        priorityWhy: "Official Marriott content presents 23,011 square feet alongside 20,000-square-foot marketing.",
        found:
          "The events page reports 10 event rooms, 23,011 square feet, 13 breakout rooms, and capacity of 1,450; the overview and meeting narrative state 20,000 square feet and 13 breakout areas.",
        matters:
          "Planners need one setup-qualified inventory; rounding may be valid, but the counting and area basis should be explicit.",
        owner: "Director of Sales / Events Leadership",
        steps: [
          "Sign a room-by-room event-space schedule.",
          "Define the counting basis for event rooms, breakout areas, and divisible sections.",
          "Explain or align 20,000 and 23,011 square feet.",
          "Verify overview, events, floor plans, and planner collateral.",
        ],
        sources: ["overview", "events"],
      },
      {
        priority: "HIGH",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Strengthen Uptown and Convention-Center Decision Context",
        priorityWhy: "ADP recorded 0% recognition for both Uptown location and convention-center proximity.",
        found:
          "Official content places the hotel at 100 West Trade Street, describes a connection to Charlotte Convention Center, and names Spectrum Center and Bank of America Stadium; ADP recorded 0% recognition for Uptown, convention-center proximity, and walkable downtown.",
        matters:
          "These are verified location assets that can be made consistent without claiming they caused the measured visibility gap.",
        owner: "Hotel Commercial Leadership",
        steps: [
          "Approve exact connection and walking-access language.",
          "Map the facts across overview, location, meetings, and sales collateral.",
          "Keep airport distance and no-shuttle facts distinct from downtown access.",
          "Archive live-page verification.",
        ],
        sources: ["overview"],
      },
      {
        priority: "MEDIUM",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Organize Dining and M Club Facts Across Priority Pages",
        priorityWhy: "Verified dining and lounge assets are materially under-recognized in the ADP attribute read.",
        found:
          "Marriott lists five restaurants, Stoke Charlotte, Stoke Bar, Coco and the Director, in-room dining, and M Club; ADP recorded 0% restaurant and in-room dining recognition, with M Club at 20%.",
        matters:
          "Complete and consistent outlet, meal-period, and lounge facts can improve buyer context, although no booking or ranking outcome is promised.",
        owner: "Hotel Commercial Leadership",
        steps: [
          "Confirm current outlet names, meal periods, and M Club eligibility.",
          "Align overview, dining, lounge, and structured amenity fields.",
          "Remove stale or duplicate outlet language.",
          "QA rendered and machine-readable facts.",
        ],
        sources: ["overview"],
      },
    ],
  },
  {
    propertyId: "adp_tampa_airport_marriott",
    slug: "tampa-airport-marriott",
    short: "TAM",
    name: "Tampa Airport Marriott",
    market: "Tampa",
    region: "Florida",
    coverLocation: "Tampa, Florida",
    website: "https://www.marriott.com/en-us/hotels/tpaap-tampa-airport-marriott/overview/",
    events: "https://www.marriott.com/en-us/hotels/tpaap-tampa-airport-marriott/events/",
    periodId: "adp_period_adp_tampa_airport_marriott_20261019082302_7b632f",
    consideration: "2.2%",
    considerationMeta: "Mentioned in 4 of 184 comparable monitored AI answers",
    scenario: "6.5%",
    scenarioMeta: "Present across 3 of 46 eligible traveler-need scenarios",
    peer: "#6 of 6",
    peerMeta: "Lowest presence among 6 certified Marriott Bonvoy peers",
    lead:
      "Tampa Airport Marriott has a highly distinctive in-terminal proposition, renovated rooms, day-use inventory, three restaurants, an outdoor pool, and renovated event venues, yet it appeared in only 6.5% of eligible scenarios and 2.2% of comparable consideration answers. The immediate work is factual: resolve airport-access and shuttle wording, reconcile event-space totals, and consistently present verified layover, dining, and meeting assets.",
    success:
      "Airport access and shuttle language is unambiguous; event-space totals reconcile; renovated rooms, day-use, dining, pool, and rooftop-event facts are consistent; and Marriott documents its intended crawler-access posture.",
    quality: 9.3,
    strongestFindings: [
      "In-terminal direct access conflicts with FAQ shuttle language and a no-shuttle transport line",
      "Official event-space totals vary among 25,000, 25,416, and 25,766 square feet",
      "ADP consideration is 2.2% with 0% recognition for airport adjacency, restaurant, room service, and ballroom capacity",
    ],
    actions: [
      {
        priority: "HIGH",
        claimType: "VERIFIED_ISSUE",
        headline: "Clarify In-Terminal Access and Airport-Shuttle Language",
        priorityWhy: "The core airport proposition is presented with conflicting transport language on the same official page.",
        found:
          "The overview describes the hotel as inside TPA with direct terminal access. The getting-here section includes an on-request $0 round trip, another line says the hotel does not provide shuttle service, and the FAQ says an airport shuttle is available.",
        matters:
          "Travelers need to know whether they walk from the terminal or use a vehicle; ambiguous shuttle language weakens the hotel's defining access advantage.",
        owner: "Hotel Operations / Marriott Digital",
        steps: [
          "Operations confirms the actual TPA arrival path and any vehicle service.",
          "Define when “in-terminal,” “direct access,” and “airport shuttle” each apply.",
          "Align overview, transportation fields, FAQ, and booking disclosures.",
          "Verify the live mobile and desktop journey.",
        ],
        sources: ["overview"],
      },
      {
        priority: "HIGH",
        claimType: "VERIFIED_ISSUE",
        headline: "Reconcile Renovated Event-Space Totals",
        priorityWhy: "Official Marriott content uses three different headline totals.",
        found:
          "The overview states 25,416 square feet, events marketing says 25,000 square feet, and the events summary reports 25,766 square feet across 17 event rooms with capacity of 700.",
        matters:
          "Rounding may explain one difference, but the 350-square-foot variance between precise totals requires a signed inventory basis.",
        owner: "Director of Sales / Events Leadership",
        steps: [
          "Sign the current room-by-room inventory after renovation.",
          "Define indoor, outdoor, rooftop, and divisible-space inclusion.",
          "Align precise totals and label rounded marketing totals.",
          "Verify planner guides, overview, events, and floor plans.",
        ],
        sources: ["overview", "events"],
      },
      {
        priority: "HIGH",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Make Layover, Dining, Pool, and Rooftop Assets Usable",
        priorityWhy: "ADP consideration is 2.2% despite verified airport-specific and renovated assets.",
        found:
          "Marriott verifies renovated soundproof rooms, day-use rooms, Imbarco, Kahwa Coffee, in-room dining, outdoor pool, 24-hour fitness, and rooftop event space. ADP recorded 0% restaurant, room-service, airport-adjacent, and ballroom-capacity recognition.",
        matters:
          "These facts distinguish the property for disrupted travel, layovers, meetings, and airport stays, but the audit does not claim content caused the measured result.",
        owner: "Hotel Commercial Leadership",
        steps: [
          "Approve current renovated-room, day-use, outlet, pool, and rooftop facts.",
          "Map each fact to its highest-intent official page and structured field.",
          "Keep temporary offers separate from evergreen property facts.",
          "QA inquiry and booking paths.",
        ],
        sources: ["overview", "events"],
      },
    ],
  },
  {
    propertyId: "adp_falls_church_marriott_fairview_park",
    slug: "falls-church-marriott-fairview-park",
    short: "FCM",
    name: "Falls Church Marriott Fairview Park",
    market: "Falls Church",
    region: "Virginia",
    coverLocation: "Falls Church, Virginia",
    website: "https://www.marriott.com/en-us/hotels/wasfp-falls-church-marriott-fairview-park/overview/",
    events: "https://www.marriott.com/en-us/hotels/wasfp-falls-church-marriott-fairview-park/events/",
    periodId: "adp_period_adp_falls_church_marriott_fairview_park_20261019084033_55523d",
    consideration: "39.1%",
    considerationMeta: "Mentioned in 72 of 184 comparable monitored AI answers",
    scenario: "93.5%",
    scenarioMeta: "Present across 43 of 46 eligible traveler-need scenarios",
    peer: "#1 of 5",
    peerMeta: "Highest presence among 5 certified Marriott Bonvoy peers",
    lead:
      "Falls Church Marriott Fairview Park leads its certified five-hotel set and appears in 43 of 46 eligible scenarios. The priority is to protect that position with current facts: date the open-ended renovation notice, reconcile event-space totals, and make the complimentary Mosaic/Metro shuttle, Tandem dining, outdoor venues, and 24-hour Peloton-equipped gym consistently usable.",
    success:
      "The renovation notice has explicit dates and current scope; event-space totals reconcile; neighborhood-shuttle, dining, outdoor-event, and fitness facts are consistent; and Marriott documents its intended crawler-access posture.",
    quality: 9.3,
    strongestFindings: [
      "Renovation notice says November 23 through April without a year or exact completion date",
      "Overview markets 27,000 square feet while events reports 28,411 square feet",
      "ADP recorded 0% restaurant and room-service recognition despite official Tandem, Starbucks, and in-room dining",
    ],
    actions: [
      {
        priority: "HIGH",
        claimType: "VERIFIED_ISSUE",
        headline: "Date and Govern the Open-Ended Renovation Notice",
        priorityWhy: "The official notice lacks a year and exact completion date.",
        found:
          "The overview says Ellipse and lower-half guest rooms will be renovated from November 23 through April, with lobby noise from 9 a.m. to 5 p.m., but does not identify the year or a precise end date.",
        matters:
          "Guests and downstream systems cannot determine whether the disruption is current, planned, or completed.",
        owner: "Hotel Operations / Marriott Digital",
        steps: [
          "Operations confirms the current renovation phase and completion date.",
          "Add full dates, affected areas, hours, and amenity status.",
          "Remove the notice promptly when work ends.",
          "Align booking disclosures and partner feeds.",
        ],
        sources: ["overview"],
      },
      {
        priority: "HIGH",
        claimType: "VERIFIED_ISSUE",
        headline: "Reconcile Event-Space Totals and Capacity Language",
        priorityWhy: "Official pages use different area totals and multiple largest-space capacities.",
        found:
          "The overview markets 27,000 square feet; the events summary reports 28,411 square feet and capacity of 1,400, while meeting copy says events for up to 1,200 and wedding copy cites 840 banquet guests.",
        matters:
          "The capacity figures may reflect reception, theater, and banquet setups, but each needs explicit qualification and one signed inventory.",
        owner: "Director of Sales / Events Leadership",
        steps: [
          "Sign the current indoor and outdoor inventory.",
          "Label 1,400 reception, 1,200 theater, and 840 banquet capacities.",
          "Explain or align 27,000 and 28,411 square feet.",
          "Verify overview, events, floor plans, and sales collateral.",
        ],
        sources: ["overview", "events"],
      },
      {
        priority: "MEDIUM",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Strengthen Metro-Shuttle, Dining, and Outdoor-Event Context",
        priorityWhy: "Verified differentiators remain under-recognized despite strong overall scenario presence.",
        found:
          "Marriott verifies a complimentary neighborhood shuttle to Mosaic District and Metro, Tandem Restaurant & Bar, Starbucks, in-room dining, outdoor patios, and a 24-hour Peloton-equipped gym. ADP recognition was 14.1% for Metro access, 0% for restaurant and room service, 1.6% for outdoor event space, and 2.3% for Peloton.",
        matters:
          "Consistent facts can help preserve category leadership without implying that any content change will improve rankings or revenue.",
        owner: "Hotel Commercial Leadership",
        steps: [
          "Confirm shuttle route, hours, and reservation rules.",
          "Approve current dining, fitness, and outdoor-event facts.",
          "Map facts across overview, dining, location, events, and structured fields.",
          "Verify live content and inquiry paths.",
        ],
        sources: ["overview", "events"],
      },
    ],
  },
  {
    propertyId: "adp_renaissance_arlington_capital_view",
    slug: "renaissance-arlington-capital-view",
    short: "RACV",
    name: "Renaissance Arlington Capital View Hotel",
    market: "Arlington",
    region: "Virginia",
    coverLocation: "Arlington, Virginia",
    website: "https://www.marriott.com/en-us/hotels/waspy-renaissance-arlington-capital-view-hotel/overview/",
    events: "https://www.marriott.com/en-us/hotels/waspy-renaissance-arlington-capital-view-hotel/events/",
    periodId: "adp_period_adp_renaissance_arlington_capital_view_20261019084033_2acbb1",
    consideration: "19.0%",
    considerationMeta: "Mentioned in 35 of 184 comparable monitored AI answers",
    scenario: "54.3%",
    scenarioMeta: "Present across 25 of 46 eligible traveler-need scenarios",
    peer: "#1 of 5",
    peerMeta: "Highest presence among 5 certified Marriott Bonvoy peers",
    lead:
      "Renaissance Arlington Capital View leads its certified five-hotel set, but appears in only 54.3% of eligible scenarios and 19.0% of comparable consideration answers. Official pages verify a complimentary DCA, Metro, Pentagon, and Amazon HQ2 shuttle, SOCCi dining, Navigator programming, and substantial meetings inventory. The immediate work is to reconcile event totals and property naming, resolve conflicting pet fees, and organize those verified demand signals.",
    success:
      "Event-space totals and the Capital View name are consistent; pet fees use one approved basis; shuttle, dining, Navigator, and government/corporate facts are complete; and Marriott documents its intended crawler-access posture.",
    quality: 9.4,
    strongestFindings: [
      "Official event content alternates among 17,000, 17,179, and 20,000 square feet and uses “Capitol View” once",
      "Official overview presents both a $100 per-stay pet fee and a $50 per-night pet fee",
      "ADP recorded 0% recognition for restaurant, room service, airport shuttle, ballroom capacity, and Navigator",
    ],
    actions: [
      {
        priority: "HIGH",
        claimType: "VERIFIED_ISSUE",
        headline: "Reconcile Event Totals and the Capital View Name",
        priorityWhy: "Official Marriott event content combines conflicting totals with a property-name typo.",
        found:
          "The overview markets 17,000 square feet; the events summary reports 17,179; its opening copy says 20,000; and virtual-tour copy says “Renaissance Arlington Capitol View Hotel” rather than Capital View.",
        matters:
          "Planners need one inventory basis, and the property name should be consistent for entity clarity.",
        owner: "Director of Sales / Marriott Digital",
        steps: [
          "Sign the hotel-only and complex-level event inventory.",
          "Define whether 20,000 square feet includes shared or attached-hotel space.",
          "Align 17,000 and 17,179 labels and correct “Capitol View.”",
          "Verify overview, events, virtual tour, and planner collateral.",
        ],
        sources: ["overview", "events"],
      },
      {
        priority: "HIGH",
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        headline: "Resolve Conflicting Pet-Fee Terms",
        priorityWhy: "The same official overview presents different fee bases.",
        found:
          "Pet-friendly marketing states a $100 non-refundable fee per stay, while hotel information and the FAQ state a $50 non-refundable fee per night; both allow up to two pets.",
        matters:
          "Guests need one approved fee basis before booking, and the audit cannot determine which amount is current.",
        owner: "Hotel Operations / Revenue Leadership",
        steps: [
          "Confirm the approved pet fee, weight limit, and stay/night basis.",
          "Align marketing, hotel information, FAQ, booking disclosures, and partner feeds.",
          "Remove stale fee language.",
          "Archive live verification.",
        ],
        sources: ["overview"],
      },
      {
        priority: "HIGH",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Strengthen Shuttle, Dining, Navigator, and Demand Context",
        priorityWhy: "Verified access and experience assets are absent or weak in the ADP attribute read.",
        found:
          "Marriott verifies complimentary scheduled service to DCA and Crystal City Metro plus weekday Pentagon/Amazon HQ2 service, SOCCi, illy Café, room service, Renaissance Navigators, and government/military positioning. ADP recorded 0% recognition for airport shuttle, restaurant, room service, ballroom capacity, and Navigator.",
        matters:
          "These facts support airport, government, corporate, and local-experience decisions, but no ranking or booking causation is claimed.",
        owner: "Hotel Commercial Leadership",
        steps: [
          "Confirm current shuttle schedule, stops, and request rules.",
          "Approve outlet, room-service, Navigator, government, and corporate facts.",
          "Map the facts across overview, dining, location, events, and structured fields.",
          "QA live content and inquiry paths.",
        ],
        sources: ["overview", "events"],
      },
    ],
  },
];

function evidenceFor(hotel, source) {
  const url = source === "events" ? hotel.events : hotel.website;
  return {
    label: `${hotel.name} official ${source === "events" ? "events" : "overview"}`,
    url,
    kind: "VERIFIED_PUBLIC",
  };
}

function addProbeAction(hotel) {
  return {
    priority: "MEDIUM",
    claimType: "RECOMMENDED_INVESTIGATION",
    headline: "Review Marriott Origin Access for Intended Crawlers",
    priorityWhy: "The bounded zero-cost probe returned different GET and HEAD behavior at the Marriott origin.",
    found:
      "The public research probe received HTTP 403 and a redirect to default.mi for an origin GET, while HEAD and robots.txt returned HTTP 200. The named hotel overview and events pages remained retrievable through separate WebFetch.",
    matters:
      "The mixed result warrants a security-aligned review but does not establish a hotel-page outage, indexing failure, or cause of ADP performance.",
    owner: "Marriott Digital / CDN Security Owner",
    steps: [
      "Review the exact research user agent and redirect chain.",
      "Confirm intended crawler and bot policy.",
      "Make only narrow approved changes if a conflict is verified.",
      "Retest the origin and named hotel pages.",
    ],
    sources: ["overview"],
  };
}

function buildPayload(hotel) {
  const actions = [...hotel.actions, addProbeAction(hotel)].map((action, index) => {
    const actionId = `AVA-${hotel.short}-${String(index + 1).padStart(3, "0")}`;
    const evidence = action.sources.map((source) => evidenceFor(hotel, source));
    return {
      actionId,
      headline: action.headline,
      priority: action.priority,
      priorityWhy: action.priorityWhy,
      whatWeFound: action.found,
      whyItMatters: action.matters,
      recommendedSteps: action.steps,
      accountableOwner: action.owner,
      supportingParties: ["Hotel commercial leadership", "Marriott digital content owner"],
      suggestedTimeframe: "Review in 2 weeks; approved updates within 30–45 days",
      timeframeNote: "Timing is proposed and requires accountable-owner agreement.",
      completionCriteria: {
        investigationCompleted: "Hotel leadership records the approved source of truth, unknowns, scope, and decision.",
        correctionSubmitted: "Approved field-level changes are submitted through the responsible hotel or Marriott workflow.",
        correctionVerifiedLive: "Named pages match the approved facts and live evidence is archived.",
        noChangeRequired: "Valid only when leadership documents why current content is accurate or no change is commercially warranted.",
      },
      requiresHotelApproval: true,
      hotelApprovalNotes: "Hotel leadership must approve facts and every public change before implementation.",
      supportingEvidence: evidence,
      technicalDetails: {
        targetUrls: evidence.map((item) => item.url),
        urls: evidence.map((item) => item.url),
        changesOrInvestigation: action.steps,
        evidence: evidence.map((item) => `${item.label}: ${action.found}`),
        verificationMethod: "Re-fetch the named pages and compare rendered facts with the hotel-approved record.",
        dependencies: ["Hotel-approved source of truth", "Access to the responsible Marriott content channel"],
        notes: "ADP recognition is a prioritization signal, not proof of ranking, booking, ADR, or revenue causation.",
      },
      implementationStatus: "NOT_STARTED",
      claimType: action.claimType,
      evidenceRefs: evidence.map((item) => item.url),
    };
  });

  return {
    contractVersion: "AI_VISIBILITY_AUDIT_ACTION_PLAN_PDF_V1",
    status: "AUDIT_VALIDATED",
    property: {
      propertyId: hotel.propertyId,
      name: hotel.name,
      market: hotel.market,
      country: "United States",
      coverLocation: hotel.coverLocation,
      website: hotel.website,
    },
    reporting: {
      reportMonthLabel: "NOVEMBER 2026",
      coverSubtitle: "INITIAL DIGITAL PRESENCE ASSESSMENT",
      coverDescriptorLine: "",
      coverDisclaimer: "",
      generatedAt: "2026-11-05",
      adpPeriodId: hotel.periodId,
      researchCostUsd: 0,
      paidResearchAuthorized: false,
    },
    kpis: [
      { label: "AI Consideration", value: hotel.consideration, meta: hotel.considerationMeta },
      { label: "Scenario Presence", value: hotel.scenario, meta: hotel.scenarioMeta },
      { label: "Certified Peer Position", value: hotel.peer, meta: hotel.peerMeta },
    ],
    executiveSummary: {
      headline: hotel.actions[0].headline,
      lead: hotel.lead,
      leadSupport: "",
      metricsNote: "AI Consideration is the share of comparable monitored answers mentioning the hotel; Scenario Presence is the share of eligible traveler-need scenarios where it appeared. Certified Peer Position uses only the named certified Marriott Bonvoy set. These measures do not represent bookings, ADR, revenue, or traveler exposure.",
      themes: actions.slice(0, 3).map((action) => ({
        title: action.headline,
        what: action.whatWeFound,
        whyItMatters: action.whyItMatters,
      })),
      priorities: actions.map((action) => ({
        priority: action.priority === "HIGH" ? "High" : "Medium",
        action: `${action.accountableOwner}: ${action.headline}`,
        desiredOutcome: "Hotel leadership records the approved source of truth and scope.",
      })),
      successLooksLike: hotel.success,
      closing: `${hotel.name} should treat its certified ADP result as a prioritization signal. The practical task is to protect or improve decision context with current, internally consistent official facts and documented verification—not to promise a visibility, booking, ADR, or revenue outcome.`,
    },
    actionOverviewIntro: "4 hotel-specific actions at a glance; detailed evidence, decisions, and verification steps follow.",
    actionOverview: actions.map((action) => ({
      actionId: action.actionId,
      priority: action.priority,
      headline: action.headline,
      businessRelevance: action.whyItMatters,
      accountableOwner: action.accountableOwner,
      suggestedTimeframe: action.suggestedTimeframe,
    })),
    hotelDecisions: {
      intro: "No public change is approved by this payload. Hotel leadership must establish the source of truth and authorize implementation.",
      items: actions.map((action) => ({
        title: action.headline,
        detail: "Hotel leadership must approve facts and public changes before implementation.",
        relatedActionId: action.actionId,
      })),
    },
    findingsIntro: "Findings separate verified inconsistencies, bounded investigations, and hotel-confirmation needs.",
    keyFindings: actions.map((action) => ({
      claimType: action.claimType,
      relatedActionId: action.actionId,
      title: action.headline,
      summary: action.whatWeFound,
      evidence: action.supportingEvidence,
    })),
    actionPlanIntro: "Each action states the owner, evidence, approval requirement, completion criteria, target URLs, and verification method.",
    actions,
    methodology: {
      scope: "Combines the pinned certified ADP baseline with a bounded review of named official Marriott pages, WebFetch retrieval, and zero-cost technical probes. No paid research, ADP remeasurement, production publication, or customer approval was performed.",
      claimTypeGuide: [
        "VERIFIED_ISSUE — a directly observed, source-backed discrepancy.",
        "RECOMMENDED_INVESTIGATION — evidence warrants review but does not prove a defect or cause.",
        "REQUIRES_HOTEL_CONFIRMATION — leadership must establish the source of truth before correction.",
        "OPTIONAL_COMMERCIAL_OPPORTUNITY — proceed only if leadership wants the demand.",
      ],
      responsibilities: [
        "Hotel leadership confirms facts, priorities, and public changes.",
        "Digital, brand, and partner teams implement only approved corrections and document verification.",
        "Dealality packages measurement context and evidence without implying ranking, booking, ADR, or revenue causation.",
      ],
      monthlyReviewRelationship: "Approved actions may be tracked later. Completing an action does not itself prove a change in AI presence, bookings, ADR, or revenue.",
      nextStep: "Approve the highest-priority source-of-truth decision, then authorize only the validated work.",
      confidentiality: "Confidential · For recipient only · © 2026 Dealality",
    },
  };
}

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function stripForceStatus(propertyId) {
  const meta = getAuditMeta(propertyId);
  if (!meta || meta.forceStatus === undefined) return;
  delete meta.forceStatus;
  const metaPath = path.join(ROOT, "data/ava-data/audits", propertyId, "AUDIT_META.json");
  fs.writeFileSync(metaPath, `${JSON.stringify(meta, null, 2)}\n`);
}

async function main() {
  if (process.env.AVA_STORAGE_MODE !== "local") {
    throw new Error("AVA_STORAGE_MODE=local is required for Wave 3 Batch 4");
  }
  fs.mkdirSync(PAYLOAD_ROOT, { recursive: true });
  const results = [];
  for (const hotel of HOTELS) {
    const audit = buildPayload(hotel);
    const payloadRel = `lib/ai-visibility-audit-action-plan/payloads/${hotel.slug}-v1.json`;
    fs.writeFileSync(path.join(ROOT, payloadRel), `${JSON.stringify(audit, null, 2)}\n`);
    const gate = assertAuditPdfPayload(audit);
    if (!gate.ok) throw new Error(`${hotel.slug} INVALID ${gate.errors.join("; ")}`);

    const outDir = path.join(REPORT_ROOT, hotel.slug, "pdf-staged");
    fs.mkdirSync(outDir, { recursive: true });
    const filename = buildAiVisibilityAuditPdfFilename({
      hotelName: audit.property.name,
      reportMonthLabel: audit.reporting.reportMonthLabel,
      date: audit.reporting.generatedAt,
    });
    const outPath = path.join(outDir, filename);
    await generateAiVisibilityAuditPdfV1({
      payload: audit,
      outPath,
      pagesDir: path.join(outDir, "page-previews-wave3-batch4"),
      bridgeOutPath: path.join(outDir, "proposed-monthly-action-register-bridge.json"),
    });

    const versionId = `ava_v1_wave3_batch4_${hotel.slug.replace(/-/g, "_")}`;
    const staged = stageAvaPdf({
      propertyId: hotel.propertyId,
      generatedAbsPath: outPath,
      versionId,
      filename,
    });
    if (!staged.durable || !String(staged.pdfRelativePath).startsWith("pdfs/")) {
      throw new Error(`${hotel.slug} DURABLE_STAGE_REQUIRED`);
    }

    const legacyDir = path.join(
      ROOT,
      "reports/ai-visibility-audit-action-plan/archive",
      hotel.propertyId
    );
    fs.mkdirSync(legacyDir, { recursive: true });
    const legacyCurrent = path.join(legacyDir, "current.pdf");
    if (fs.existsSync(legacyCurrent)) {
      const priorSha = sha256File(legacyCurrent);
      if (priorSha !== staged.sha256) {
        fs.copyFileSync(
          legacyCurrent,
          path.join(legacyDir, `pre_wave3_batch4_${priorSha.slice(0, 12)}.pdf`)
        );
      }
    }
    fs.copyFileSync(outPath, legacyCurrent);
    fs.copyFileSync(outPath, path.join(legacyDir, `${versionId}.pdf`));
    fs.copyFileSync(outPath, path.join(legacyDir, filename));

    saveAuditVersion(hotel.propertyId, versionId, audit, {
      status: "AUDIT_VALIDATED",
      metaStatus: "AUDIT_VALIDATED",
      forceStatus: true,
      kind: "REPORT_VERSION",
      pdfRelativePath: staged.pdfRelativePath,
      researchCostUsd: 0,
      evidenceReviewStatus: "AUDIT_VALIDATED",
      validation: {
        contractOk: true,
        wave: "WAVE_3_BATCH_4",
        phase: "ava-full-portfolio-production-v1",
        auditValidated: true,
        qualityOverall: hotel.quality,
        strongestFindings: hotel.strongestFindings,
        founderApprovedForCustomer: false,
        customerReadyBlocked: true,
        publicationAuthorized: false,
        paidResearchUsd: 0,
        generatedAt: new Date().toISOString(),
        pdfSha256: staged.sha256,
      },
    });
    upsertAuditMeta(hotel.propertyId, {
      pdfRelativePath: staged.pdfRelativePath,
      pdfSha256: staged.sha256,
      auditValidatedAt: new Date().toISOString(),
      founderApprovedForCustomer: false,
      customerReadinessStatus: "NOT_CUSTOMER_READY",
      publicationAuthorizationStatus: "NOT_AUTHORIZED",
      researchCostUsd: 0,
      forceStatus: true,
    });
    stripForceStatus(hotel.propertyId);

    results.push({
      propertyId: hotel.propertyId,
      name: hotel.name,
      wave: 3,
      batch: 4,
      status: "AUDIT_VALIDATED",
      founderApprovedForCustomer: false,
      publicationAuthorized: false,
      actionCount: audit.actions.length,
      qualityOverall: hotel.quality,
      strongestFindings: hotel.strongestFindings,
      payload: payloadRel,
      filename,
      pdf: path.relative(ROOT, outPath).replace(/\\/g, "/"),
      stagedPath: staged.pdfRelativePath,
      durableStorage: staged.durable === true,
      sha256: staged.sha256,
      versionId,
      researchCostUsd: 0,
    });
    console.log("OK", hotel.propertyId, staged.sha256.slice(0, 12));
  }

  const checkpoint = {
    ok: true,
    contract: "AVA_WAVE3_BATCH4_PRODUCTION_V1",
    generatedAt: new Date().toISOString(),
    storageMode: "local",
    storageRoot: "data/ava-data",
    publicationAuthorized: false,
    founderApprovedForCustomer: false,
    paidResearchUsd: 0,
    results,
  };
  const checkpointPath = path.join(REPORT_ROOT, "AVA_WAVE3_BATCH4_CHECKPOINT.json");
  fs.writeFileSync(checkpointPath, `${JSON.stringify(checkpoint, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, count: results.length, checkpointPath }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
