/**
 * Wave 3 Batch 3 AVA: build hotel-specific payloads, generate PDFs, stage to
 * AVA local durable storage, and set AUDIT_VALIDATED.
 *
 * Zero paid research. No publication or founder/customer approval.
 *
 *   $env:AVA_STORAGE_MODE="local"
 *   node scripts/ava-wave3-batch3-generate-stage-validate-v1.mjs
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
const GENERATED_AT = "2026-11-05";
const REPORT_MONTH = "NOVEMBER 2026";

const HOTELS = [
  {
    propertyId: "adp_st_regis_mexico_city",
    slug: "st-regis-mexico-city",
    payloadFile: "st-regis-mexico-city-v1.json",
    name: "The St. Regis Mexico City",
    market: "Mexico City",
    country: "Mexico",
    coverLocation: "Mexico City, Mexico",
    website:
      "https://www.marriott.com/en-us/hotels/mexxr-the-st-regis-mexico-city/overview/",
    adpPeriodId: "adp_period_adp_st_regis_mexico_city_20261020184110_b23fe3",
    kpis: [
      ["AI Consideration", "71.9%", "Mentioned in 138 of 192 comparable monitored AI answers"],
      ["Scenario Presence", "91.7%", "Present across 44 of 48 eligible traveler-need scenarios"],
      ["Certified Peer Position", "#2 of 6", "Second-highest presence among 6 certified Marriott Bonvoy peers"],
    ],
    quality: 9.2,
    strongestFindings: [
      "Official events copy alternates between 13 meeting rooms and 12 event rooms",
      "Dining requires a stable bridge between 10 restaurants, named outlets, bars, and private dining",
      "ADP recognition is 0% for on-site restaurant, private dining, room service, and ballroom capacity",
    ],
    headline: "Reconcile Event and Dining Inventories While Protecting Strong AI Presence",
    lead:
      "The St. Regis Mexico City has strong measured presence—71.9% AI Consideration, 91.7% Scenario Presence, and the second position among six certified Marriott Bonvoy peers. The priority is not broad awareness; it is source-of-truth precision where official event-room and dining frames differ, plus clearer representation of verified assets that remain weak in the certified attribute read.",
    success:
      "Official event-room and dining inventories use approved definitions, distinctive hotel assets are consistently described on priority pages, and Marriott documents the intended automated-access posture.",
    closing:
      "The hotel already performs strongly in monitored demand. Precision around inventory, categories, and signature experiences is the highest-value next step.",
    actions: [
      {
        id: "AVA-SRMC-001",
        priority: "HIGH",
        claimType: "VERIFIED_ISSUE",
        headline: "Reconcile the 13-Room and 12-Room Event Inventory",
        priorityWhy: "Two official event-page sections use different room counts.",
        what:
          "The events page invites planners to review 13 meeting rooms, while later copy and the FAQ state that the hotel has 12 event rooms.",
        why:
          "Planners need one setup-qualified inventory that explains whether pre-function or another space is included in only one count.",
        owner: "Director of Sales / Marriott Digital",
        steps: [
          "Sales signs the active event-space roster.",
          "Define meeting room, event room, ballroom, terrace, and pre-function counting.",
          "Align page narrative, capacity chart, FAQ, and sales collateral.",
          "Archive the approved inventory and live verification.",
        ],
        evidence: [
          ["St. Regis Mexico City events", "https://www.marriott.com/en-us/hotels/mexxr-the-st-regis-mexico-city/events/", "Contains both the 13-meeting-room statement and 12-event-room statement."],
        ],
      },
      {
        id: "AVA-SRMC-002",
        priority: "HIGH",
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        headline: "Publish One Dining Venue and Category Map",
        priorityWhy: "The overview presents a 10-restaurant headline alongside restaurants, bars, a café, private dining, and other named concepts.",
        what:
          "The official overview states 10 restaurants, while its dining journey names multiple restaurants and bars and separately presents private dining and in-room dining.",
        why:
          "A signed category map can prevent omissions and apparent count conflicts across overview, dining, amenities, and partner feeds.",
        owner: "Director of Food & Beverage / Marriott Digital",
        steps: [
          "Confirm the active outlet roster and operating status.",
          "Classify restaurants, bars, cafés, concessions, private dining, and in-room dining.",
          "Align headline counts and outlet-level modules.",
          "Verify names, hours, reservation links, and seasonal qualifiers.",
        ],
        evidence: [
          ["St. Regis Mexico City overview", "https://www.marriott.com/en-us/hotels/mexxr-the-st-regis-mexico-city/overview/", "Shows the 10-restaurant amenity and the named dining concepts."],
        ],
      },
      {
        id: "AVA-SRMC-003",
        priority: "HIGH",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Strengthen Under-Recognized Dining, Family, and Event Facts",
        priorityWhy: "Several verified differentiators remain absent or weak in the certified attribute read.",
        what:
          "ADP recorded 0% recognition for on-site restaurant, private dining, room service, and ballroom capacity; family-friendly recognition was 5.2%, despite detailed official dining, children’s, and event content.",
        why:
          "Consistent, machine-readable facts can improve buyer context, without implying booking, ranking, ADR, or revenue causation.",
        owner: "Hotel Commercial Leadership",
        steps: [
          "Approve concise source-of-truth statements for each priority asset.",
          "Place the same facts on the most relevant first-party pages.",
          "Check headings, internal links, and structured data for consistency.",
          "Re-review in the next governed monitoring period.",
        ],
        evidence: [
          ["St. Regis Mexico City overview", "https://www.marriott.com/en-us/hotels/mexxr-the-st-regis-mexico-city/overview/", "Documents dining, family, spa, fitness, and meeting assets."],
          ["St. Regis Mexico City events", "https://www.marriott.com/en-us/hotels/mexxr-the-st-regis-mexico-city/events/", "Documents event rooms, total space, capacity, and wedding services."],
        ],
      },
      {
        id: "AVA-SRMC-004",
        priority: "MEDIUM",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Review Marriott Origin Access for Intended Crawlers",
        priorityWhy: "The bounded zero-cost probe returned different GET and HEAD behavior.",
        what:
          "The public probe received HTTP 403 and a redirect to default.mi for an origin GET, while HEAD and robots.txt returned HTTP 200. The hotel pages remained retrievable through separate WebFetch.",
        why:
          "The mixed result warrants a narrow security-aligned review but does not establish an indexing outage.",
        owner: "Marriott Digital / CDN Security Owner",
        steps: [
          "Review the exact request and redirect behavior.",
          "Confirm intended crawler policy.",
          "Approve only narrow changes if a conflict is verified.",
          "Retest the origin and hotel URLs.",
        ],
        evidence: [
          ["Marriott origin", "https://www.marriott.com/", "Bounded GET returned 403 and redirected to default.mi."],
          ["Marriott robots.txt", "https://www.marriott.com/robots.txt", "Returned HTTP 200 in the same probe."],
        ],
      },
    ],
  },
  {
    propertyId: "adp_w_rome",
    slug: "w-rome",
    payloadFile: "w-rome-v1.json",
    name: "W Rome",
    market: "Rome",
    country: "Italy",
    coverLocation: "Rome, Italy",
    website: "https://www.marriott.com/en-us/hotels/romwv-w-rome/overview/",
    adpPeriodId: "adp_period_adp_w_rome_20261019224104_a23f6f",
    kpis: [
      ["AI Consideration", "11.5%", "Mentioned in 22 of 192 comparable monitored AI answers"],
      ["Scenario Presence", "41.7%", "Present across 20 of 48 eligible traveler-need scenarios"],
      ["Certified Peer Position", "#3 of 6", "Third-highest presence among 6 certified Marriott Bonvoy peers"],
    ],
    quality: 9.1,
    strongestFindings: [
      "Formal event-room headline tops out at 40 while ancillary private-event venues list capacities up to 100",
      "ADP recognition is 0% for on-site restaurant, small meeting space, private dining, pet friendly, and lifestyle",
      "Marriott origin GET returned 403 while hotel pages remained available through WebFetch",
    ],
    headline: "Bridge Formal Meeting Metrics to W Rome’s Broader Private-Event Offer",
    lead:
      "W Rome appears in 20 of 48 eligible scenarios and ranks third among six certified Marriott Bonvoy peers, but AI Consideration is 11.5%. Its official pages show a differentiated lifestyle proposition—148 rooms and suites, rooftop dining, W Lounge, WET Deck, private-event venues, design, and city programming—while several of those assets remain absent or weak in the certified attribute read.",
    success:
      "Formal meeting-room metrics and broader private-event capacities are explicitly scoped, dining categories are stable, and priority lifestyle facts are consistent across official pages.",
    closing:
      "W Rome has distinctive assets; the immediate opportunity is to make their scope and commercial use easier to interpret and verify.",
    actions: [
      {
        id: "AVA-WROM-001",
        priority: "HIGH",
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        headline: "Bridge Formal Meeting Metrics to Private-Event Capacities",
        priorityWhy: "The official page uses different capacity frames for formal event rooms and ancillary venues.",
        what:
          "The events headline lists 2 event rooms, 60 square meters, and a largest-space capacity of 40. The same page separately offers W Lounge and Giardino Clandestino for up to 100 standing, WET Deck for 90, and other private-event settings.",
        why:
          "A clear formal-room versus buyout/ancillary-venue bridge helps planners compare the right capacity frame.",
        owner: "Director of Sales / Events",
        steps: [
          "Confirm the formal meeting-room inventory and gross/sellable area.",
          "Classify ancillary venues, buyouts, outdoor spaces, and seasonal restrictions.",
          "Label capacity headlines by space type and setup.",
          "Align page copy, floor plans, and sales collateral.",
        ],
        evidence: [
          ["W Rome events", "https://www.marriott.com/en-us/hotels/romwv-w-rome/events/", "Shows the formal-room headline and larger ancillary private-event capacities."],
        ],
      },
      {
        id: "AVA-WROM-002",
        priority: "HIGH",
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        headline: "Publish One Dining and Social-Venue Category Map",
        priorityWhy: "The overview headline says four restaurants while the journey mixes restaurant, rooftop, pastry, lounge, and garden concepts.",
        what:
          "Official copy presents Giano, Citrico Rooftop, Zucchero, W Lounge, and Giardino Clandestino across dining and social contexts, while the amenity headline states four restaurants and one bar.",
        why:
          "Stable categories help guests and partners distinguish restaurants, bars, lounges, pastry concepts, and seasonal spaces.",
        owner: "Director of Food & Beverage / Marriott Digital",
        steps: [
          "Confirm the active venue roster.",
          "Classify each concept and seasonal operating status.",
          "Align overview, dining, amenities, and events copy.",
          "Verify reservation and private-event links.",
        ],
        evidence: [
          ["W Rome overview", "https://www.marriott.com/en-us/hotels/romwv-w-rome/overview/", "Names the dining and social concepts and states the amenity counts."],
          ["W Rome events", "https://www.marriott.com/en-us/hotels/romwv-w-rome/events/", "Describes private use of dining, lounge, garden, rooftop, and pool spaces."],
        ],
      },
      {
        id: "AVA-WROM-003",
        priority: "HIGH",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Strengthen Restaurant, Pool, Pet, and Lifestyle Facts",
        priorityWhy: "Verified hotel assets remain weak in the certified attribute read.",
        what:
          "ADP recorded 0% for on-site restaurant, small meeting space, private dining, pet friendly, and lifestyle; outdoor pool recognition was 1.4%, despite detailed official content.",
        why:
          "Consistent facts can improve buyer context without promising a change in AI rank, bookings, ADR, or revenue.",
        owner: "Hotel Commercial Leadership",
        steps: [
          "Approve concise source-of-truth statements for the priority assets.",
          "Align the facts across overview, dining, events, and policy modules.",
          "Improve descriptive headings and internal links where approved.",
          "Re-review in the next governed monitoring period.",
        ],
        evidence: [
          ["W Rome overview", "https://www.marriott.com/en-us/hotels/romwv-w-rome/overview/", "Documents dining, WET Deck, pet policy, design, and lifestyle programming."],
          ["W Rome events", "https://www.marriott.com/en-us/hotels/romwv-w-rome/events/", "Documents formal and private-event spaces."],
        ],
      },
      {
        id: "AVA-WROM-004",
        priority: "MEDIUM",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Review Marriott Origin Access for Intended Crawlers",
        priorityWhy: "The bounded probe returned different GET and HEAD behavior.",
        what:
          "The public probe received HTTP 403 and a redirect to default.mi for an origin GET, while HEAD and robots.txt returned HTTP 200. Hotel pages remained retrievable through separate WebFetch.",
        why:
          "The mixed result warrants a narrow security-aligned review but does not establish an indexing outage.",
        owner: "Marriott Digital / CDN Security Owner",
        steps: [
          "Review the exact request and redirect behavior.",
          "Confirm intended crawler policy.",
          "Approve only narrow changes if a conflict is verified.",
          "Retest the origin and hotel URLs.",
        ],
        evidence: [
          ["Marriott origin", "https://www.marriott.com/", "Bounded GET returned 403 and redirected to default.mi."],
          ["Marriott robots.txt", "https://www.marriott.com/robots.txt", "Returned HTTP 200 in the same probe."],
        ],
      },
    ],
  },
  {
    propertyId: "adp_mayflower_hotel_washington_dc",
    slug: "mayflower-hotel-washington-dc",
    payloadFile: "mayflower-hotel-washington-dc-v1.json",
    name: "The Mayflower Hotel, Autograph Collection",
    market: "Washington, DC",
    country: "United States",
    coverLocation: "Washington, DC",
    website: "https://www.themayflowerhotel.com/",
    adpPeriodId: "adp_period_adp_mayflower_hotel_washington_dc_20261013213022_f9bdd0",
    kpis: [
      ["AI Consideration", "23.4%", "Mentioned in 43 of 184 comparable monitored AI answers"],
      ["Scenario Presence", "41.3%", "Present across 19 of 46 eligible traveler-need scenarios"],
      ["Certified Peer Position", "#2 of 5", "Second-highest presence among 5 certified Marriott Bonvoy peers"],
    ],
    quality: 9.1,
    strongestFindings: [
      "Meetings page mixes exact 42,580-square-foot and rounded 43,000-square-foot frames",
      "Wedding venue recognition is 5.2% despite 27 rooms, up to 1,000 capacity, and extensive official event content",
      "On-site restaurant recognition is 0% despite first-party Edgar Bar & Kitchen content",
    ],
    headline: "Translate a Century of Landmark Authority Into Clear Event and Dining Facts",
    lead:
      "The Mayflower ranks second among five certified Marriott Bonvoy peers, yet appears in 19 of 46 eligible scenarios and 23.4% of comparable answers. Its first-party site documents a 581-room landmark, 100 years of history, Edgar Bar & Kitchen, and a major events platform; the priority is to make exact inventory, rounded marketing figures, and decision-ready facts consistently legible.",
    success:
      "Exact and rounded event-space figures are labeled, wedding and dining facts are complete across priority pages, and first-party structured-data coverage is reviewed and documented.",
    closing:
      "The Mayflower’s historic authority is established. The next step is converting that authority into precise, reusable commercial facts.",
    actions: [
      {
        id: "AVA-MAYF-001",
        priority: "HIGH",
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        headline: "Label Exact and Rounded Event-Space Totals",
        priorityWhy: "The official meetings page uses both 42,580 and 43,000 square feet.",
        what:
          "The meetings headline reports 42,580 square feet of total event space, while body copy describes a newly restored 43,000 square feet.",
        why:
          "Labeling exact inventory and rounded marketing language prevents the figures from appearing inconsistent.",
        owner: "Director of Sales / Marriott Digital",
        steps: [
          "Confirm the signed event-space schedule.",
          "Identify exact, rounded, gross, and sellable figures.",
          "Align page headline, narrative, floor plans, and sales collateral.",
          "Archive the approved inventory and live verification.",
        ],
        evidence: [
          ["Mayflower meetings and events", "https://www.themayflowerhotel.com/meetings-events/", "Shows 42,580 square feet in the headline and rounded 43,000-square-foot body copy."],
        ],
      },
      {
        id: "AVA-MAYF-002",
        priority: "HIGH",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Strengthen Wedding and Ballroom Decision Facts",
        priorityWhy: "A major official events platform remains weak in the certified attribute read.",
        what:
          "The official page lists 27 event rooms, up to 1,000 capacity, a 7,000-square-foot Grand Ballroom, and extensive wedding positioning. ADP wedding-venue recognition was 5.2% and large-ballroom recognition was 13.4%.",
        why:
          "Setup-qualified facts can improve planner context without implying a change in booking or measurement outcomes.",
        owner: "Director of Sales / Weddings",
        steps: [
          "Approve a concise room, capacity, and ballroom fact set.",
          "Add setup and floor-plan qualifiers.",
          "Align meetings, weddings, and venue-detail pages.",
          "Re-review in the next governed monitoring period.",
        ],
        evidence: [
          ["Mayflower meetings and events", "https://www.themayflowerhotel.com/meetings-events/", "Documents event-room count, total area, largest capacity, ballroom area, and wedding positioning."],
        ],
      },
      {
        id: "AVA-MAYF-003",
        priority: "HIGH",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Make Edgar Bar & Kitchen a Reusable Hotel Fact",
        priorityWhy: "ADP on-site restaurant recognition was 0%.",
        what:
          "The official homepage identifies Edgar Bar & Kitchen as the hotel’s on-site American brasserie using local ingredients, while the certified attribute read recorded 0% on-site restaurant recognition.",
        why:
          "Consistent outlet identity, location, cuisine, and service facts can reduce ambiguity across hotel and partner pages.",
        owner: "Director of Food & Beverage / Digital Content Owner",
        steps: [
          "Confirm outlet name, cuisine, location, and current meal periods.",
          "Align homepage, dining page, amenities, and partner feeds.",
          "Use consistent headings and internal links.",
          "Verify live copy and archive evidence.",
        ],
        evidence: [
          ["Mayflower homepage", "https://www.themayflowerhotel.com/", "Identifies Edgar Bar & Kitchen and its brasserie positioning."],
        ],
      },
      {
        id: "AVA-MAYF-004",
        priority: "MEDIUM",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Review First-Party Hotel Structured Data",
        priorityWhy: "The healthy zero-cost probe did not detect JSON-LD on the homepage response.",
        what:
          "The public probe returned HTTP 200 for robots.txt, sitemap.xml, HEAD, and homepage GET, but did not detect an application/ld+json block in the fetched homepage HTML.",
        why:
          "A technical review can determine whether current rendered markup already covers Hotel, address, restaurant, and event facts before any change is considered.",
        owner: "Marriott Digital / Website Technical Owner",
        steps: [
          "Inspect server-rendered and client-rendered structured data.",
          "Validate existing entities and canonical URLs.",
          "Add or amend markup only if a documented gap exists.",
          "Retest with standard validators and archive results.",
        ],
        evidence: [
          ["Mayflower homepage", "https://www.themayflowerhotel.com/", "Homepage GET returned 200; bounded raw-response probe did not detect JSON-LD."],
          ["Mayflower sitemap", "https://www.themayflowerhotel.com/sitemap.xml", "Returned HTTP 200 in the same probe."],
        ],
      },
    ],
  },
  {
    propertyId: "adp_hotel_phillips_kansas_city",
    slug: "hotel-phillips-kansas-city",
    payloadFile: "hotel-phillips-kansas-city-v1.json",
    name: "Hotel Phillips Kansas City, Curio Collection by Hilton",
    market: "Kansas City",
    country: "United States",
    coverLocation: "Kansas City, Missouri",
    website: "https://hotelphillips.com/",
    adpPeriodId: "adp_period_adp_hotel_phillips_kansas_city_20260903003324_88084d",
    kpis: [
      ["AI Consideration", "32.1%", "Mentioned in 77 of 240 comparable monitored AI answers"],
      ["Scenario Presence", "75.0%", "Present across 45 of 60 eligible traveler-need scenarios"],
      ["Certified Peer Position", "#2 of 6", "Second-highest presence among 6 certified Hilton Honors peers"],
    ],
    quality: 9.0,
    strongestFindings: [
      "Canonical hotelphillips.com redirects to Hilton and the bounded probe received a 403/reference-code response",
      "Meeting-space recognition is 34.2% and ballroom recognition is 12.7%, with the meetings decision package still incomplete",
      "Historic building, Art Deco, restaurant, speakeasy, and downtown attributes recorded 0% recognition",
    ],
    headline: "Restore a Verifiable Canonical Source and Complete Core Decision Facts",
    lead:
      "Hotel Phillips reaches 45 of 60 eligible scenarios and ranks second among six certified Hilton Honors peers, but AI Consideration is 32.1%. The immediate constraint is source control: the supplied canonical domain redirects to Hilton and returned a reference-code/403 response in the bounded probe, while meeting, historic, dining, and location attributes remain incomplete or absent in the certified read.",
    success:
      "The intended canonical hotel URL is documented and retrievable for intended users, meetings facts are complete, and approved historic, dining, and downtown facts are consistent across first-party surfaces.",
    closing:
      "The hotel has broad scenario reach. A stable canonical source and complete decision facts are the foundation for improving representation consistency.",
    actions: [
      {
        id: "AVA-HPKC-001",
        priority: "HIGH",
        claimType: "RECOMMENDED_INVESTIGATION",
        headline: "Confirm and Restore the Intended Canonical Hotel URL",
        priorityWhy: "The supplied hotel domain did not deliver a normal hotel page in the bounded review.",
        what:
          "hotelphillips.com redirected to the Hilton property URL. The zero-cost probe then received HTTP 403 with a Hilton reference-code page; separate WebFetch attempts timed out.",
        why:
          "The hotel should have one documented canonical source that intended users and approved crawlers can reliably retrieve.",
        owner: "Hilton Digital / Hotel Website Owner",
        steps: [
          "Confirm whether the redirect is intentional and permanent.",
          "Document the canonical property URL.",
          "Review the 403/reference-code behavior with the exact request context.",
          "Retest the domain and canonical destination.",
        ],
        evidence: [
          ["Hotel Phillips supplied domain", "https://hotelphillips.com/", "Redirected to the Hilton property URL in the bounded probe."],
          ["Hotel Phillips Hilton page", "https://www.hilton.com/en/hotels/mkccuqq-hotel-phillips-kansas-city/", "Probe received HTTP 403/reference-code response; WebFetch timed out."],
        ],
      },
      {
        id: "AVA-HPKC-002",
        priority: "HIGH",
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        headline: "Complete the Meetings Decision Package",
        priorityWhy: "Meeting and ballroom recognition remain partial and the canonical page could not be reviewed.",
        what:
          "ADP meeting-space recognition was 34.2% and ballroom recognition was 12.7%. Total space, room count, largest capacity, AV, sales contact, and floor-plan availability require hotel confirmation.",
        why:
          "A complete setup-qualified package helps planners verify fit without relying on inferred or stale facts.",
        owner: "Director of Sales / Hilton Digital",
        steps: [
          "Sign the current meeting-space and capacity schedule.",
          "Confirm AV, sales contact, and floor-plan links.",
          "Publish the approved facts on the canonical page.",
          "Verify partner and group-sales surfaces.",
        ],
        evidence: [
          ["Hotel Phillips Hilton page", "https://www.hilton.com/en/hotels/mkccuqq-hotel-phillips-kansas-city/", "Canonical destination to review after access behavior is resolved."],
        ],
      },
      {
        id: "AVA-HPKC-003",
        priority: "HIGH",
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        headline: "Establish the Approved Historic and Downtown Fact Set",
        priorityWhy: "Several identity and location attributes recorded 0% recognition.",
        what:
          "ADP recorded 0% for historic building, historic Art Deco, National Register listing, downtown Kansas City, Power & Light District, streetcar access, and nearby venue attributes.",
        why:
          "Hotel-approved identity and proximity facts can create a consistent source of truth without overstating walking times or landmark relationships.",
        owner: "Hotel General Manager / Commercial Leadership",
        steps: [
          "Confirm approved historic designations and language.",
          "Verify addresses, distances, and transit relationships.",
          "Publish concise facts on the canonical first-party surface.",
          "Align partner descriptions and archive evidence.",
        ],
        evidence: [
          ["Hotel Phillips Hilton page", "https://www.hilton.com/en/hotels/mkccuqq-hotel-phillips-kansas-city/", "Target first-party surface for approved identity and location facts."],
        ],
      },
      {
        id: "AVA-HPKC-004",
        priority: "HIGH",
        claimType: "REQUIRES_HOTEL_CONFIRMATION",
        headline: "Align Dining, Speakeasy, Fitness, and Service Facts",
        priorityWhy: "Multiple core hotel amenities recorded 0% recognition.",
        what:
          "ADP recorded 0% recognition for on-site restaurant, speakeasy, fitness center, room service, and valet parking, while pet-friendly and business-center recognition were 2.5%.",
        why:
          "A signed amenity record can separate active, seasonal, third-party, and unavailable services before public copy is changed.",
        owner: "Hotel Operations / Hilton Digital",
        steps: [
          "Confirm current amenity and outlet operating status.",
          "Document hours, access, fees, and third-party relationships.",
          "Correct only missing, conflicting, or outdated first-party facts.",
          "Verify live pages and partner feeds.",
        ],
        evidence: [
          ["Hotel Phillips Hilton page", "https://www.hilton.com/en/hotels/mkccuqq-hotel-phillips-kansas-city/", "Target first-party surface for the hotel-approved amenity record."],
        ],
      },
    ],
  },
];

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function supportingEvidence(rows) {
  return rows.map(([label, url, note]) => ({ label, url, note }));
}

function actionFromConfig(action) {
  const evidence = supportingEvidence(action.evidence);
  return {
    actionId: action.id,
    headline: action.headline,
    priority: action.priority,
    priorityWhy: action.priorityWhy,
    whatWeFound: action.what,
    whyItMatters: action.why,
    recommendedSteps: action.steps,
    accountableOwner: action.owner,
    supportingParties: ["Hotel commercial leadership", "Digital content owner"],
    suggestedTimeframe: "Review in 2 weeks; approved updates within 30–45 days",
    completionCriteria: {
      investigationCompleted: "Hotel leadership records the approved source of truth and scope.",
      correctionSubmitted: "Approved field-level changes are submitted to the responsible channel.",
      correctionVerifiedLive: "Named pages match the approved facts and live evidence is archived.",
      noChangeRequired: "Valid only when leadership documents why the current presentation is accurate and sufficient.",
    },
    requiresHotelApproval: true,
    hotelApprovalNotes: "Hotel leadership must approve facts and public changes before implementation.",
    supportingEvidence: evidence,
    technicalDetails: {
      targetUrls: evidence.map((e) => e.url),
      urls: evidence.map((e) => e.url),
      changesOrInvestigation: ["Compare current public copy with the hotel-approved source of truth."],
      evidence: evidence.map((e) => `${e.label}: ${e.note}`),
      verificationMethod: "Re-fetch the named pages and compare rendered facts with the approved record.",
      dependencies: ["Hotel-approved source of truth", "Access to the responsible content channel"],
      notes: "ADP recognition is a prioritization signal, not proof of ranking, booking, ADR, or revenue causation.",
    },
    implementationStatus: "NOT_STARTED",
    claimType: action.claimType,
  };
}

function buildPayload(hotel) {
  const actions = hotel.actions.map(actionFromConfig);
  return {
    contractVersion: "AI_VISIBILITY_AUDIT_ACTION_PLAN_PDF_V1",
    status: "AUDIT_VALIDATED",
    property: {
      propertyId: hotel.propertyId,
      name: hotel.name,
      market: hotel.market,
      country: hotel.country,
      coverLocation: hotel.coverLocation,
      website: hotel.website,
    },
    reporting: {
      reportMonthLabel: REPORT_MONTH,
      coverSubtitle: "INITIAL DIGITAL PRESENCE ASSESSMENT",
      coverDescriptorLine: "",
      coverDisclaimer: "",
      generatedAt: GENERATED_AT,
      adpPeriodId: hotel.adpPeriodId,
      researchCostUsd: 0,
      paidResearchAuthorized: false,
    },
    kpis: hotel.kpis.map(([label, value, meta]) => ({ label, value, meta })),
    executiveSummary: {
      headline: hotel.headline,
      lead: hotel.lead,
      leadSupport: "",
      metricsNote: `${hotel.kpis[0][0]} is the share of comparable monitored answers mentioning the hotel; Scenario Presence is the share of eligible traveler-need scenarios where it appeared. Certified Peer Position uses the named governed peer set. These measures do not represent bookings, ADR, revenue, or traveler exposure.`,
      themes: hotel.actions.slice(0, 3).map((a) => ({
        title: a.headline,
        what: a.what,
        whyItMatters: a.why,
      })),
      priorities: hotel.actions.map((a) => ({
        priority: a.priority === "HIGH" ? "High" : "Medium",
        action: `${a.owner}: ${a.headline}`,
        desiredOutcome: "Hotel leadership records the approved source of truth and scope.",
      })),
      successLooksLike: hotel.success,
      closing: hotel.closing,
    },
    actionOverviewIntro: `${actions.length} hotel-specific actions at a glance; detailed evidence, decisions, and verification steps follow.`,
    actionOverview: actions.map((a) => ({
      actionId: a.actionId,
      priority: a.priority,
      headline: a.headline,
      businessRelevance: a.whyItMatters,
      accountableOwner: a.accountableOwner,
      suggestedTimeframe: a.suggestedTimeframe,
    })),
    hotelDecisions: {
      intro: "No public change is approved by this payload. Hotel leadership must establish the source of truth and authorize implementation.",
      items: actions.map((a) => ({
        title: a.headline,
        detail: a.hotelApprovalNotes,
        relatedActionId: a.actionId,
      })),
    },
    findingsIntro: "Findings separate verified inconsistencies, bounded investigations, and hotel-confirmation requirements.",
    keyFindings: hotel.actions.map((a) => ({
      claimType: a.claimType,
      relatedActionId: a.id,
      title: a.headline,
      summary: a.what,
      evidence: supportingEvidence(a.evidence).map((e) => ({
        ...e,
        kind: a.claimType === "VERIFIED_ISSUE" ? "VERIFIED_PUBLIC" : "TECHNICAL_OR_SOURCE_OBSERVATION",
      })),
    })),
    actionPlanIntro: "Each action states the owner, evidence, approval requirement, completion criteria, target URLs, and verification method.",
    actions,
    methodology: {
      scope: "Combines the pinned certified ADP lean baseline with a bounded review of named public pages, WebFetch retrieval, and zero-cost technical probes. No paid research, ADP remeasurement, production write, customer publication, or founder/customer approval was performed.",
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

async function main() {
  if (String(process.env.AVA_STORAGE_MODE || "").toLowerCase() !== "local") {
    throw new Error("AVA_STORAGE_MODE=local is required for Wave 3 Batch 3");
  }

  const payloadDir = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
  fs.mkdirSync(payloadDir, { recursive: true });
  const results = [];

  for (const hotel of HOTELS) {
    const audit = buildPayload(hotel);
    const payloadAbs = path.join(payloadDir, hotel.payloadFile);
    fs.writeFileSync(payloadAbs, `${JSON.stringify(audit, null, 2)}\n`);

    const gate = assertAuditPdfPayload(audit);
    if (!gate.ok) {
      throw new Error(`${hotel.slug} INVALID ${gate.errors.join("; ")}`);
    }

    const outDir = path.join(
      ROOT,
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1",
      hotel.slug,
      "pdf-staged"
    );
    fs.mkdirSync(outDir, { recursive: true });
    const filename = buildAiVisibilityAuditPdfFilename({
      hotelName: audit.property.name,
      reportMonthLabel: audit.reporting.reportMonthLabel,
      date: audit.reporting.generatedAt,
    });
    const outPath = path.join(outDir, filename);

    console.log("Generating", hotel.propertyId, "...");
    await generateAiVisibilityAuditPdfV1({
      payload: audit,
      outPath,
      pagesDir: path.join(outDir, "page-previews-wave3-batch3"),
      bridgeOutPath: path.join(outDir, "proposed-monthly-action-register-bridge.json"),
    });

    const versionId = `ava_v1_wave3_batch3_${hotel.slug.replace(/-/g, "_")}`;
    const staged = stageAvaPdf({
      propertyId: hotel.propertyId,
      generatedAbsPath: outPath,
      versionId,
      filename,
    });
    if (!staged.durable) {
      throw new Error(`${hotel.propertyId} PDF_NOT_DURABLE`);
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
          path.join(legacyDir, `pre_wave3_batch3_${priorSha.slice(0, 12)}.pdf`)
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
        wave: "WAVE_3_BATCH_3",
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

    const meta = getAuditMeta(hotel.propertyId);
    if (meta?.forceStatus !== undefined) {
      upsertAuditMeta(hotel.propertyId, { forceStatus: undefined });
    }

    results.push({
      propertyId: hotel.propertyId,
      name: hotel.name,
      wave: 3,
      batch: 3,
      status: "AUDIT_VALIDATED",
      founderApprovedForCustomer: false,
      customerReadinessStatus: "NOT_CUSTOMER_READY",
      publicationAuthorizationStatus: "NOT_AUTHORIZED",
      actionCount: audit.actions.length,
      qualityOverall: hotel.quality,
      strongestFindings: hotel.strongestFindings,
      payload: path.relative(ROOT, payloadAbs).replace(/\\/g, "/"),
      filename,
      pdf: path.relative(ROOT, outPath).replace(/\\/g, "/"),
      stagedPath: staged.pdfRelativePath,
      durable: staged.durable,
      sha256: staged.sha256,
      versionId,
      researchCostUsd: 0,
    });
    console.log("OK", filename, staged.sha256.slice(0, 12));
  }

  const summary = {
    ok: true,
    contract: "AVA_WAVE3_BATCH3_PRODUCTION_V1",
    generatedAt: new Date().toISOString(),
    storageMode: "local",
    publicationAuthorized: false,
    founderApprovedForCustomer: false,
    paidResearchUsd: 0,
    results,
  };
  const outJson = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_WAVE3_BATCH3_CHECKPOINT.json"
  );
  fs.writeFileSync(outJson, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, count: results.length, outJson }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
