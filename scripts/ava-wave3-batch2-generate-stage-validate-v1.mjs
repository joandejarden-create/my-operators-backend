/**
 * Wave 3 Batch 2 AVA: build hotel-specific payloads, generate PDFs, stage them
 * in durable local storage, and save AUDIT_VALIDATED versions.
 *
 * Zero paid research. No publication or founder/customer approval.
 *
 *   AVA_STORAGE_MODE=local AVA_STORAGE_ROOT=data/ava-data \
 *     node scripts/ava-wave3-batch2-generate-stage-validate-v1.mjs
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
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
const REPORT_ROOT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const OFFICIAL = "VERIFIED_PUBLIC";
const TECHNICAL = "TECHNICAL_OBSERVATION";

const HOTELS = [
  {
    propertyId: "adp_cambridge_beaches_bermuda",
    slug: "cambridge-beaches-bermuda",
    payloadFile: "cambridge-beaches-bermuda-v1.json",
    name: "Cambridge Beaches Resort & Spa",
    market: "Sandys Parish",
    country: "Bermuda",
    website: "https://www.cambridgebeaches.com/",
    periodId: "adp_period_adp_cambridge_beaches_bermuda_20260903010829_044437",
    kpis: [
      ["AI Consideration", "78.6%", "Mentioned in 66 of 84 comparable monitored AI answers"],
      ["Scenario Presence", "100.0%", "Present across 21 of 21 eligible traveler-need scenarios"],
      ["Certified Peer Position", "#1 of 4", "Highest presence among 4 certified Independent Positioning hotels"],
    ],
    metricLead:
      "The hotel reaches all 21 eligible scenarios and leads its certified four-hotel peer set, but appears in 78.6% of comparable answers and has only 28.6% Reality Coverage.",
    quality: 9.2,
    strongestFindings: [
      "The official site explicitly presents multiple waterfront restaurants and bars while on-site restaurant recognition was 0%.",
      "The official homepage presents four private beaches, two private coves, cottage accommodations, and luxury-resort positioning while several core resort attributes were at or near 0% recognition.",
      "The staged review prioritizes Business District Location at 0% for a Sandys Parish oceanfront resort, requiring an intent check before any content change.",
    ],
    actions: [
      {
        id: "AVA-CAMB-001",
        priority: "HIGH",
        headline: "Reconcile Official Dining Facts With the 0% Recognition Gap",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Hotel Commercial Leadership / Digital Content Owner",
        found:
          "The official homepage describes multiple waterview restaurants and bars, including poolside, oceanfront, dockside, and tropical-bar experiences; the certified ADP read recorded 0% recognition for On-Site Restaurant.",
        matters:
          "The gap is commercially material because dining is explicit first-party content, but it does not prove that page copy caused measured AI performance.",
        steps: [
          "Approve the current outlet names, meal periods, and operating-season facts.",
          "Map the approved facts across dining, overview, structured data, and priority partner profiles.",
          "Check whether each outlet is visible in rendered and machine-readable content.",
          "Archive live evidence and review the gap at the next governed measurement.",
        ],
        urls: ["https://www.cambridgebeaches.com/"],
        evidence: [
          ["Cambridge Beaches homepage", "https://www.cambridgebeaches.com/", OFFICIAL, "Describes multiple waterview restaurants and bars across poolside, oceanfront, and dockside settings."],
        ],
      },
      {
        id: "AVA-CAMB-002",
        priority: "HIGH",
        headline: "Strengthen the Resort, Beach, and Cottage Fact Architecture",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Hotel Commercial Leadership",
        found:
          "The official homepage states that the property is a Bermuda luxury resort with cottage accommodations, four private beaches, and two private coves. The staged reality-gap read recorded 0% for Luxury Positioning, Resort Experience, and Outdoor Pool, and 1.2% for Island Resort.",
        matters:
          "These are defining product facts. Consistent, approved presentation can improve decision context without promising a ranking, booking, ADR, or revenue outcome.",
        steps: [
          "Approve one canonical property-description block with resort, cottage, beach, and cove facts.",
          "Separate permanent facts from seasonal or package-specific claims.",
          "Apply the approved block to overview, accommodations, experience, and structured fields.",
          "Verify wording and source consistency across priority channels.",
        ],
        urls: ["https://www.cambridgebeaches.com/"],
        evidence: [
          ["Cambridge Beaches homepage", "https://www.cambridgebeaches.com/", OFFICIAL, "Presents luxury-resort positioning, cottage accommodations, four private beaches, and two private coves."],
        ],
      },
      {
        id: "AVA-CAMB-003",
        priority: "HIGH",
        headline: "Validate the Business-District Attribute Before Acting",
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        owner: "Hotel Commercial Leadership / Dealality Measurement Steward",
        found:
          "The staged review marks Business District Location as the first actionable 0% gap, while the certified property identity is an oceanfront resort in Sandys Parish.",
        matters:
          "A potentially inapplicable attribute should not trigger public copy changes until hotel leadership and the measurement steward confirm the intended demand context.",
        steps: [
          "Confirm whether business-district relevance is genuinely part of the hotel's target demand.",
          "Review the prompts and evidence associated with the attribute without changing methodology.",
          "Document either an approved positioning fact or a no-change rationale.",
          "Keep the outcome in the monthly action register.",
        ],
        urls: ["https://www.cambridgebeaches.com/"],
        evidence: [
          ["Cambridge Beaches homepage", "https://www.cambridgebeaches.com/", OFFICIAL, "Positions the property as an oceanfront Bermuda resort rather than a business-district hotel."],
        ],
      },
      {
        id: "AVA-CAMB-004",
        priority: "MEDIUM",
        headline: "Preserve the Healthy Public-Crawl Baseline",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Website Platform Owner",
        found:
          "The zero-cost probe received HTTP 200 for robots.txt, sitemap.xml, HEAD, and homepage GET; the homepage also exposed JSON-LD.",
        matters:
          "This is a healthy bounded observation, not proof of complete indexing. Preserving it provides a useful technical baseline while content facts are improved.",
        steps: [
          "Record the current robots, sitemap, response, and structured-data baseline.",
          "Include these endpoints in release regression checks.",
          "Avoid broad crawler-policy changes without a verified issue.",
          "Re-run the bounded probe after material site releases.",
        ],
        urls: [
          "https://www.cambridgebeaches.com/",
          "https://www.cambridgebeaches.com/robots.txt",
          "https://www.cambridgebeaches.com/sitemap.xml",
        ],
        evidence: [
          ["Zero-cost public research probe", "https://www.cambridgebeaches.com/", TECHNICAL, "Returned HTTP 200 for the tested homepage, HEAD, robots, and sitemap requests."],
        ],
      },
    ],
  },
  {
    propertyId: "adp_nobu_london_portman",
    slug: "nobu-london-portman",
    payloadFile: "nobu-london-portman-v1.json",
    name: "Nobu Hotel London Portman Square",
    market: "London",
    country: "England",
    website: "https://www.nobuhotels.com/london-portman/",
    periodId: "adp_period_adp_nobu_london_portman_20261017005955_8dde1d",
    kpis: [
      ["AI Consideration", "19.6%", "Mentioned in 36 of 184 comparable monitored AI answers"],
      ["Scenario Presence", "41.3%", "Present across 19 of 46 eligible traveler-need scenarios"],
      ["Reality Coverage", "13.3%", "Only 2 of 15 monitored attributes consistently recognized"],
    ],
    metricLead:
      "The hotel appears in 19.6% of comparable answers and 41.3% of eligible scenarios, while official pages explicitly support several attributes recorded at 0%.",
    quality: 9.4,
    strongestFindings: [
      "The official events page supports receptions up to 700 guests, but Large Ballroom / Event Capacity recognition was 0%.",
      "The official homepage explicitly places the hotel in Marylebone and presents four dining destinations, while Marylebone location, on-site restaurant, private dining, and Nobu Restaurant recognition were all 0%.",
      "The official event-buyout copy unifies 243 rooms, three event spaces, dining, Pilates, and wellness, providing a strong first-party fact block that is not reflected in 13.3% Reality Coverage.",
    ],
    actions: [
      {
        id: "AVA-NOBU-001",
        priority: "HIGH",
        headline: "Make the 700-Guest Event Proposition Consistent and Machine-Usable",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Director of Sales / Digital Content Owner",
        found:
          "The official events page describes receptions for up to 700 guests and a Nobu Ballroom with a 9-meter LED screen; Large Ballroom / Event Capacity recognition was 0%.",
        matters:
          "The ballroom is a material group-sales asset. Clear setup-qualified capacity facts can improve planner context without implying measured-performance causation.",
        steps: [
          "Approve capacity by setup and the exact ballroom naming convention.",
          "Align event landing pages, fact sheets, structured fields, and partner profiles.",
          "Keep the 700 figure explicitly tied to reception setup.",
          "Verify rendered and machine-readable facts after publication.",
        ],
        urls: ["https://www.nobuhotels.com/london-portman/meetings-events/"],
        evidence: [
          ["Nobu meetings and events", "https://www.nobuhotels.com/london-portman/meetings-events/", OFFICIAL, "States receptions for up to 700 guests and describes the Nobu Ballroom and 9-meter LED screen."],
        ],
      },
      {
        id: "AVA-NOBU-002",
        priority: "HIGH",
        headline: "Unify Marylebone, Dining, and Nobu Hospitality Positioning",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Hotel Commercial Leadership",
        found:
          "The official homepage places the hotel in Marylebone and presents four dining destinations plus Omotenashi hospitality. The certified read recorded 0% for Marylebone / Portman Square Location, On-Site Restaurant, Nobu Restaurant, Private Dining, and Japanese Hospitality Positioning.",
        matters:
          "The missing signals are central to the hotel's identity and entry into relevant traveler-need answers.",
        steps: [
          "Approve a concise canonical block covering Marylebone, Nobu dining, private dining, and Omotenashi.",
          "Map it across overview, dining, events, location, and structured fields.",
          "Ensure terminology is consistent rather than repetitive or promotional.",
          "Verify priority pages and partner profiles against the approved block.",
        ],
        urls: [
          "https://www.nobuhotels.com/london-portman/",
          "https://www.nobuhotels.com/london-portman/meetings-events/",
        ],
        evidence: [
          ["Nobu hotel homepage", "https://www.nobuhotels.com/london-portman/", OFFICIAL, "Explicitly presents Marylebone, four dining destinations, Nobu Restaurant, private dining, and Omotenashi."],
          ["Nobu meetings and events", "https://www.nobuhotels.com/london-portman/meetings-events/", OFFICIAL, "Presents Marylebone event positioning and Nobu catering/private dining."],
        ],
      },
      {
        id: "AVA-NOBU-003",
        priority: "HIGH",
        headline: "Connect the Full Hotel-Buyout Fact Set to Group Demand",
        claimType: "OPTIONAL_COMMERCIAL_OPPORTUNITY",
        owner: "Director of Sales",
        found:
          "The official events page offers exclusive hotel buyout access to 243 rooms and suites, three event spaces, Nobu Restaurant, The Lounge, Nobu Bar & Terrace, Nobu Pilates & Wellness, and a dedicated check-in experience.",
        matters:
          "This differentiated group proposition is useful for conferences, incentives, and celebrations if leadership wants that demand.",
        steps: [
          "Confirm which buyout facts and use cases are commercially prioritized.",
          "Create one approved buyer-facing summary with qualification rules.",
          "Link the summary from relevant meetings, incentive, and celebration pages.",
          "Track qualified inquiries separately from AI-presence measures.",
        ],
        urls: ["https://www.nobuhotels.com/london-portman/meetings-events/"],
        evidence: [
          ["Nobu meetings and events", "https://www.nobuhotels.com/london-portman/meetings-events/", OFFICIAL, "Lists the 243-room exclusive-buyout proposition and included hotel facilities."],
        ],
      },
      {
        id: "AVA-NOBU-004",
        priority: "MEDIUM",
        headline: "Correct the Probe's Root-Redirect Blind Spot",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Website Platform Owner",
        found:
          "The zero-cost probe accepted the supplied hotel URL but tested the site origin, whose homepage redirected to the Nobu Hotels root. Separate web fetches successfully retrieved the hotel and meetings pages.",
        matters:
          "The result is a probe-scope limitation, not evidence that the hotel pages are unavailable or unindexable.",
        steps: [
          "Add canonical hotel-path checks to the bounded technical regression.",
          "Record redirect chains separately from hotel-page status.",
          "Preserve the current HTTP 200 robots and sitemap baseline.",
          "Escalate only if a canonical hotel path fails independently.",
        ],
        urls: [
          "https://www.nobuhotels.com/london-portman/",
          "https://www.nobuhotels.com/london-portman/meetings-events/",
        ],
        evidence: [
          ["Zero-cost public research probe", "https://www.nobuhotels.com/", TECHNICAL, "Origin GET returned HTTP 200 at the Nobu Hotels root; hotel-path pages were separately retrievable."],
        ],
      },
    ],
  },
  {
    propertyId: "adp_hilton_times_square",
    slug: "hilton-times-square",
    payloadFile: "hilton-times-square-v1.json",
    name: "Hilton New York Times Square",
    market: "New York",
    country: "United States",
    website: "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/",
    periodId: "adp_period_adp_hilton_times_square_20261020121725_48a06f",
    kpis: [
      ["AI Consideration", "10.4%", "Mentioned in 22 of 212 comparable monitored AI answers"],
      ["Scenario Presence", "28.3%", "Present across 15 of 53 eligible traveler-need scenarios"],
      ["Certified Peer Position", "#2 of 6", "Second-highest presence among 6 certified Hilton Honors peers"],
    ],
    metricLead:
      "The hotel ranks #2 of 6 in its certified Hilton Honors set but appears in only 10.4% of comparable answers and 28.3% of eligible scenarios.",
    quality: 9.3,
    strongestFindings: [
      "Hilton's official hotel-information page explicitly lists Connecting Rooms, on-site restaurant, room service, and Digital Key while each registered 0% recognition.",
      "Celebrations & Events had 0% scenario capture even though Hilton lists meeting rooms and the property has a strong Times Square identity.",
      "The bounded origin GET returned 403 after redirect while robots, sitemap, and HEAD returned 200; the official hotel-information page remained retrievable through WebFetch.",
    ],
    actions: [
      {
        id: "AVA-HILTS-001",
        priority: "HIGH",
        headline: "Reconcile Hilton Amenities With Four 0% Recognition Gaps",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Hotel Operations / Hilton Digital",
        found:
          "Hilton's official hotel-information page lists on-site restaurant, room service, Digital Key, and Connecting Rooms; the certified reality-gap read recorded 0% recognition for all four.",
        matters:
          "These facts affect family, dining, and convenience decisions, but the gap does not prove that official-page content caused low consideration.",
        steps: [
          "Operations confirms current availability and restrictions for each amenity.",
          "Align overview, hotel-info, rooms, dining, and structured amenity fields.",
          "Verify confirmed-connecting-room and room-service wording is precise.",
          "Archive live evidence for the next governed measurement.",
        ],
        urls: ["https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/hotel-info/"],
        evidence: [
          ["Hilton hotel information", "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/hotel-info/", OFFICIAL, "Lists on-site restaurant, room service, Digital Key, and Connecting Rooms."],
        ],
      },
      {
        id: "AVA-HILTS-002",
        priority: "HIGH",
        headline: "Build a Source-Backed Celebrations and Events Entry Path",
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        owner: "Director of Sales / Hotel Commercial Leadership",
        found:
          "Celebrations & Events was the softest monitored territory at 0% scenario capture. Hilton's official hotel-information page lists meeting rooms, but the available evidence does not establish a complete celebration proposition.",
        matters:
          "The hotel should confirm actual event inventory and target occasions before publishing expanded positioning.",
        steps: [
          "Confirm current meeting-room inventory, capacities, and supported social occasions.",
          "Approve one setup-qualified events fact sheet.",
          "Publish only verified spaces, capacities, and services.",
          "Track whether the hotel wants celebration demand before expanding content.",
        ],
        urls: [
          "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/",
          "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/hotel-info/",
        ],
        evidence: [
          ["Hilton hotel information", "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/hotel-info/", OFFICIAL, "Lists meeting rooms but does not, in the fetched page, provide a full celebration inventory."],
        ],
      },
      {
        id: "AVA-HILTS-003",
        priority: "HIGH",
        headline: "Strengthen City-View and Walkable-Location Decision Context",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Hotel Commercial Leadership",
        found:
          "The certified read recorded 0% for City Views and Walkable Downtown while the property identity is Hilton New York Times Square at 234 West 42nd Street.",
        matters:
          "Location identity and view claims are material, but exact view categories and walking-distance language require hotel-approved facts.",
        steps: [
          "Approve exact room/view categories and defensible walking-distance facts.",
          "Separate address/location facts from room-specific view promises.",
          "Map approved facts to overview, rooms, location, and structured fields.",
          "Verify partner profiles do not generalize limited view categories.",
        ],
        urls: [
          "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/",
          "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/hotel-info/",
        ],
        evidence: [
          ["Hilton hotel information", "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/hotel-info/", OFFICIAL, "Confirms the property identity and 234 West 42nd Street address."],
        ],
      },
      {
        id: "AVA-HILTS-004",
        priority: "MEDIUM",
        headline: "Review Hilton Origin Access for Intended Crawlers",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Hilton Digital / CDN Security Owner",
        found:
          "The zero-cost probe received HTTP 403 for the Hilton origin GET after redirect to /en/, while robots.txt, sitemap.xml, and HEAD returned HTTP 200. A separate web fetch retrieved the hotel's official information page.",
        matters:
          "The mixed result warrants a bounded security-aligned review but does not establish a hotel-page outage or general indexing failure.",
        steps: [
          "Review the exact user agent, redirect chain, and WAF response.",
          "Test the canonical hotel paths independently from the origin.",
          "Confirm intended crawler policy with the platform owner.",
          "Make only narrow approved changes if a conflict is verified.",
        ],
        urls: [
          "https://www.hilton.com/",
          "https://www.hilton.com/robots.txt",
          "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/hotel-info/",
        ],
        evidence: [
          ["Hilton origin probe", "https://www.hilton.com/", TECHNICAL, "Origin GET returned 403 after redirect while HEAD, robots, and sitemap returned 200."],
          ["Hilton hotel information", "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/hotel-info/", OFFICIAL, "Was retrievable through the separate public web fetch."],
        ],
      },
    ],
  },
  {
    propertyId: "adp_radisson_blu_toronto_downtown",
    slug: "radisson-blu-toronto-downtown",
    payloadFile: "radisson-blu-toronto-downtown-v1.json",
    name: "Radisson Blu Toronto Downtown",
    market: "Toronto",
    country: "Canada",
    website: "https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41",
    periodId: "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b",
    kpis: [
      ["AI Consideration", "29.0%", "Mentioned in 73 of 252 comparable monitored AI answers"],
      ["Scenario Presence", "38.1%", "Present across 24 of 63 eligible traveler-need scenarios"],
      ["Certified Peer Position", "#1 of 6", "Highest presence among 6 certified Choice Privileges peers"],
    ],
    metricLead:
      "The hotel leads its certified six-hotel peer set but appears in 29.0% of comparable answers and 38.1% of eligible scenarios, with 19.0% Reality Coverage.",
    quality: 9.4,
    strongestFindings: [
      "The official Choice page lists Family Friendly and city-attraction proximity while recognition was 8.2% for Family-Friendly and 9.6% for City Views.",
      "The page explicitly lists EV Charging, Pet Friendly, and Metro Toronto Convention Centre proximity while recognition was 0%, 0%, and 1.4%, respectively.",
      "The official page says Restaurant and Lounge closed until further notice, creating an open-ended operating-status fact that should be current and dated.",
    ],
    actions: [
      {
        id: "AVA-RBTD-001",
        priority: "HIGH",
        headline: "Resolve the Open-Ended Restaurant and Lounge Closure Notice",
        claimType: "VERIFIED_ISSUE",
        owner: "Hotel Operations / Choice Digital",
        found:
          "The official Choice page says the Restaurant and Lounge are closed until further notice without a dated status or expected review point.",
        matters:
          "An open-ended operating-status notice affects dining decisions and can persist downstream after conditions change.",
        steps: [
          "Operations confirms the current restaurant and lounge status.",
          "Remove the notice or add an accurate effective date and approved status.",
          "Align dining, amenities, booking disclosures, and partner feeds.",
          "Archive live verification and an owner for future status updates.",
        ],
        urls: ["https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41"],
        evidence: [
          ["Choice official hotel page", "https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41", OFFICIAL, "States “Restaurant and Lounge closed until further notice.”"],
        ],
      },
      {
        id: "AVA-RBTD-002",
        priority: "HIGH",
        headline: "Unify Family, Harbourfront, and City-Access Facts",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Hotel Commercial Leadership",
        found:
          "The official page lists Family Friendly and positions the hotel near Toronto Harbour, Rogers Centre, and Metro Toronto Convention Centre. Recognition was 8.2% for Family-Friendly, 9.6% for City Views, and 1.4% for Near Convention Center.",
        matters:
          "These facts support family, leisure, and event-planner decisions but require precise wording, especially for room-specific views.",
        steps: [
          "Approve exact family amenities, room-view categories, and walking-distance claims.",
          "Create one location block distinguishing Harbourfront, attractions, and convention access.",
          "Map it to overview, rooms, location, and structured fields.",
          "Verify partner profiles against the approved facts.",
        ],
        urls: ["https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41"],
        evidence: [
          ["Choice official hotel page", "https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41", OFFICIAL, "Lists Family Friendly and proximity to Toronto Harbour, Rogers Centre, and Metro Toronto Convention Centre."],
        ],
      },
      {
        id: "AVA-RBTD-003",
        priority: "HIGH",
        headline: "Reconcile Pet and EV-Charging Facts With 0% Recognition",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Hotel Operations / Choice Digital",
        found:
          "The official page lists Pet Friendly and EV Charging Station, including a detailed pet fee and weight policy; both attributes registered 0% recognition.",
        matters:
          "These are high-intent travel filters. Consistency matters, but the gap does not establish a causal content defect.",
        steps: [
          "Confirm the current pet and EV-charging policies, costs, and availability.",
          "Align summary amenities with detailed policy content.",
          "Expose approved facts in structured amenity fields and partner feeds.",
          "Verify rendered facts and archive evidence.",
        ],
        urls: ["https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41"],
        evidence: [
          ["Choice official hotel page", "https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41", OFFICIAL, "Lists Pet Friendly, EV Charging Station, and detailed pet terms."],
        ],
      },
      {
        id: "AVA-RBTD-004",
        priority: "MEDIUM",
        headline: "Review Choice Origin Timeouts Without Overstating Impact",
        claimType: "RECOMMENDED_INVESTIGATION",
        owner: "Choice Digital / CDN Security Owner",
        found:
          "The zero-cost origin probe timed out on robots, sitemap, HEAD, and homepage GET, while the separate web fetch successfully retrieved the canonical hotel page.",
        matters:
          "The mixed observation may reflect origin-level bot handling or transient latency; it does not establish a canonical hotel-page outage or indexing failure.",
        steps: [
          "Retest the origin and canonical hotel path with logged timing and user agent.",
          "Review CDN/WAF telemetry for the bounded probe window.",
          "Confirm intended crawler policy.",
          "Make only narrow approved changes if repeatable impact is verified.",
        ],
        urls: [
          "https://www.choicehotels.com/",
          "https://www.choicehotels.com/robots.txt",
          "https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41",
        ],
        evidence: [
          ["Choice origin probe", "https://www.choicehotels.com/", TECHNICAL, "Timed out for the bounded origin requests."],
          ["Choice official hotel page", "https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41", OFFICIAL, "Was retrievable through the separate public web fetch."],
        ],
      },
    ],
  },
];

function buildPayload(hotel) {
  const completionCriteria = {
    investigationCompleted: "Hotel leadership records the approved source of truth and scope.",
    correctionSubmitted: "Approved field-level changes are submitted to the responsible channel.",
    correctionVerifiedLive: "Named pages match the approved facts and live evidence is archived.",
    noChangeRequired: "Valid only when leadership documents why the current presentation is accurate and sufficient.",
  };
  const actions = hotel.actions.map((action) => ({
    actionId: action.id,
    headline: action.headline,
    priority: action.priority,
    priorityWhy: action.found,
    whatWeFound: action.found,
    whyItMatters: action.matters,
    recommendedSteps: action.steps,
    accountableOwner: action.owner,
    supportingParties: ["Hotel commercial leadership", "Digital content owner"],
    suggestedTimeframe: "Review in 2 weeks; approved updates within 30–45 days",
    completionCriteria,
    requiresHotelApproval: true,
    hotelApprovalNotes: "Hotel leadership must approve facts and public changes before implementation.",
    supportingEvidence: action.evidence.map(([label, url, , note]) => ({ label, url, note })),
    technicalDetails: {
      targetUrls: action.urls,
      urls: action.urls,
      changesOrInvestigation: action.steps,
      evidence: action.evidence.map(([label, , , note]) => `${label}: ${note}`),
      verificationMethod: "Re-fetch the named pages and compare rendered facts with the approved record.",
      dependencies: ["Hotel-approved source of truth", "Access to the responsible content channel"],
      notes: "ADP recognition is a prioritization signal, not proof of ranking, booking, ADR, or revenue causation.",
    },
    implementationStatus: "NOT_STARTED",
    claimType: action.claimType,
  }));
  return {
    contractVersion: "AI_VISIBILITY_AUDIT_ACTION_PLAN_PDF_V1",
    status: "AUDIT_VALIDATED",
    property: {
      propertyId: hotel.propertyId,
      name: hotel.name,
      market: hotel.market,
      country: hotel.country,
      coverLocation: `${hotel.market}, ${hotel.country}`,
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
    kpis: hotel.kpis.map(([label, value, meta]) => ({ label, value, meta })),
    executiveSummary: {
      headline: hotel.actions[0].headline,
      lead: `${hotel.metricLead} The immediate task is to reconcile explicit first-party facts with the staged reality gaps, while treating technical-probe results as bounded observations rather than causal proof.`,
      leadSupport: "",
      metricsNote: "AI Consideration is the share of comparable monitored answers mentioning the hotel; Scenario Presence is the share of eligible traveler-need scenarios where it appeared. Peer position is certified-set context. These measures do not represent bookings, ADR, revenue, or traveler exposure.",
      themes: hotel.actions.slice(0, 3).map((action) => ({
        title: action.headline,
        what: action.found,
        whyItMatters: action.matters,
      })),
      priorities: hotel.actions.map((action) => ({
        priority: action.priority === "HIGH" ? "High" : "Medium",
        action: `${action.owner}: ${action.headline}`,
        desiredOutcome: "Hotel leadership records the approved source of truth and scope.",
      })),
      successLooksLike: "Hotel-approved facts are consistent across named official pages and priority feeds; bounded technical questions are resolved; and live verification is archived without implying customer publication or performance causation.",
      closing: "The practical next step is one approved source-of-truth decision at a time, followed by narrow implementation and verification.",
    },
    actionOverviewIntro: `${actions.length} hotel-specific actions at a glance; detailed evidence, decisions, and verification steps follow.`,
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
        detail: action.hotelApprovalNotes,
        relatedActionId: action.actionId,
      })),
    },
    findingsIntro: "Findings separate verified inconsistencies, bounded investigations, confirmation needs, and optional commercial choices.",
    keyFindings: hotel.actions.map((action) => ({
      claimType: action.claimType,
      relatedActionId: action.id,
      title: action.headline,
      summary: action.found,
      evidence: action.evidence.map(([label, url, kind, note]) => ({ label, url, kind, note })),
    })),
    actionPlanIntro: "Each action states the owner, evidence, approval requirement, completion criteria, target URLs, and verification method.",
    actions,
    methodology: {
      scope: "Combines the pinned certified ADP baseline with the staged monthly-review reality gaps, a bounded review of named public pages, and zero-cost technical probes. No paid research, ADP remeasurement, production publication, or customer approval was performed.",
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

async function main() {
  if (process.env.AVA_STORAGE_MODE !== "local") {
    throw new Error("AVA_STORAGE_MODE=local is required");
  }
  fs.mkdirSync(PAYLOAD_DIR, { recursive: true });
  const results = [];
  for (const hotel of HOTELS) {
    const audit = buildPayload(hotel);
    const payloadAbs = path.join(PAYLOAD_DIR, hotel.payloadFile);
    fs.writeFileSync(payloadAbs, `${JSON.stringify(audit, null, 2)}\n`);
    const gate = assertAuditPdfPayload(audit);
    if (!gate.ok) throw new Error(`${hotel.slug} INVALID ${gate.errors.join("; ")}`);
    if (hotel.quality < 9) throw new Error(`${hotel.slug} QUALITY_BELOW_9`);

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
      pagesDir: path.join(outDir, "page-previews-wave3-batch2"),
      bridgeOutPath: path.join(outDir, "proposed-monthly-action-register-bridge.json"),
    });

    const versionId = `ava_v1_wave3_batch2_${hotel.slug.replace(/-/g, "_")}`;
    const staged = stageAvaPdf({
      propertyId: hotel.propertyId,
      generatedAbsPath: outPath,
      versionId,
      filename,
    });

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
          path.join(legacyDir, `pre_wave3_batch2_${priorSha.slice(0, 12)}.pdf`)
        );
      }
    }
    fs.copyFileSync(outPath, legacyCurrent);
    fs.copyFileSync(outPath, path.join(legacyDir, `${versionId}.pdf`));
    fs.copyFileSync(outPath, path.join(legacyDir, filename));

    const now = new Date().toISOString();
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
        wave: "WAVE_3_BATCH_2",
        phase: "ava-full-portfolio-production-v1",
        auditValidated: true,
        qualityOverall: hotel.quality,
        strongestFindings: hotel.strongestFindings,
        founderApprovedForCustomer: false,
        customerReadyBlocked: true,
        paidResearchUsd: 0,
        generatedAt: now,
        pdfSha256: staged.sha256,
      },
    });
    upsertAuditMeta(hotel.propertyId, {
      pdfRelativePath: staged.pdfRelativePath,
      pdfSha256: staged.sha256,
      auditValidatedAt: now,
      founderApprovedForCustomer: false,
      customerReadinessStatus: "NOT_CUSTOMER_READY",
      publicationAuthorizationStatus: "NOT_AUTHORIZED",
      researchCostUsd: 0,
      forceStatus: true,
    });
    const meta = getAuditMeta(hotel.propertyId);
    if (meta?.forceStatus !== undefined) {
      delete meta.forceStatus;
      fs.writeFileSync(
        path.join(ROOT, "data/ava-data/audits", hotel.propertyId, "AUDIT_META.json"),
        `${JSON.stringify(meta, null, 2)}\n`
      );
    }

    results.push({
      propertyId: hotel.propertyId,
      name: hotel.name,
      wave: 3,
      batch: 2,
      status: "AUDIT_VALIDATED",
      founderApprovedForCustomer: false,
      actionCount: audit.actions.length,
      qualityOverall: hotel.quality,
      strongestFindings: hotel.strongestFindings,
      filename,
      pdf: path.relative(ROOT, outPath).replace(/\\/g, "/"),
      stagedPath: staged.pdfRelativePath,
      sha256: staged.sha256,
      versionId,
      researchCostUsd: 0,
      durable: staged.durable === true,
    });
    console.log("OK", hotel.propertyId, filename, staged.sha256.slice(0, 12));
  }

  const checkpoint = {
    ok: true,
    contract: "AVA_WAVE3_BATCH2_PRODUCTION_V1",
    generatedAt: new Date().toISOString(),
    storageMode: process.env.AVA_STORAGE_MODE,
    storageRoot: process.env.AVA_STORAGE_ROOT || "data/ava-data",
    publicationAuthorized: false,
    founderApprovedForCustomer: false,
    paidResearchUsd: 0,
    results,
  };
  const checkpointAbs = path.join(REPORT_ROOT, "AVA_WAVE3_BATCH2_CHECKPOINT.json");
  fs.writeFileSync(checkpointAbs, `${JSON.stringify(checkpoint, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, count: results.length, checkpointAbs }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
