#!/usr/bin/env node
/**
 * Early Signal classification tests — no network.
 */
import { inferMarketAlertEvent } from "../lib/market-alerts-event-infer.js";
import { computeMarketAlertIntelligence } from "../lib/market-alerts-intelligence.js";
import { inferSignalTiming } from "../lib/market-alerts-signal-timing.js";
import { buildProjectLabel } from "../lib/market-alerts-project-label.js";
import { classifyEarlySignalCandidate } from "../lib/market-alerts-early-signals.js";
import { listEarlySignalQueries, buildGoogleNewsRssUrl } from "../lib/market-alerts-early-signal-queries.js";
import { inferProjectDirection } from "../lib/market-alerts-project-direction.js";
import { detectStaleEarlySignal } from "../lib/market-alerts-early-signal-stale.js";
import { isUsableEntityName, inferPublisherTokens } from "../lib/market-alerts-qualification-gate.js";

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else console.log("OK:", msg);
}

{
  const q = listEarlySignalQueries("planning");
  assert(q.length >= 2, "planning family has queries");
  const url = buildGoogleNewsRssUrl(q[0].query);
  assert(/news\.google\.com\/rss\/search/.test(url), "Google News RSS URL");
  assert(/when%3A/.test(url) || /when:/.test(decodeURIComponent(url)), "recency bound in query");
}

{
  const m = computeMarketAlertIntelligence({
    title: "Developer acquires waterfront parcel for planned 180-room hotel",
    summary: "ABC Development bought a waterfront site for a planned 180-room hotel. No brand was announced.",
  });
  assert(m.meta.event.eventType === "Site Acquisition", "site acquisition event");
  assert(m.meta.signalTiming === "Pre-Decision", "site acquisition Pre-Decision");
  assert(m.meta.audience.owner.worthReviewing, "site acquisition owner WR");
  assert(m.meta.audience.brand.signalType === "Potential Development Opportunity", "unflagged site = brand opportunity");
  assert(m.meta.audience.operator.signalType === "Potential Management Opportunity", "unflagged site = operator potential");
  assert(m.meta.audience.brand.signalType !== "Competitive Brand Move", "not competitive brand move");
}

{
  const m = computeMarketAlertIntelligence({
    title: "Developer buys 10-acre industrial parcel",
    summary: "The buyer acquired industrial land with no hotel or lodging component disclosed.",
  });
  assert(m.meta.event.eventType !== "Site Acquisition", "generic land is not Site Acquisition");
  assert(m.meta.event.eventType !== "Acquisition" || m.meta.treatment === "STANDARD", "generic land not hotel acquisition WR");
  const c = classifyEarlySignalCandidate({
    title: "Developer buys 10-acre industrial parcel",
    summary: "Industrial land sale. No hotel.",
    source: "Google News (EARLY_SIGNAL_LAND)",
  });
  assert(c.treatment === "REJECTED" || c.treatment === "STANDARD", "generic land rejected or STANDARD");
  assert(!c.earlyWr, "generic land is not early WR");
}

{
  const m = computeMarketAlertIntelligence({
    title: "Plans submitted for 220-room hotel in Madrid",
    summary: "A planning application was filed for a 220-room hotel. No operator or brand named.",
  });
  assert(m.meta.event.eventType === "Planning Application", "planning application event");
  assert(["Pre-Decision", "Decision Forming"].includes(m.meta.signalTiming), "planning timing early");
  assert(m.meta.audience.brand.signalType === "Potential Development Opportunity", "unflagged planning = brand potential");
}

{
  const m = computeMarketAlertIntelligence({
    title: "Plans submitted for 220-room Hilton hotel in Madrid",
    summary: "Planning application filed for a 220-room Hilton hotel.",
  });
  assert(m.meta.audience.owner.worthReviewing, "branded planning still owner supply");
  assert(m.meta.audience.brand.signalType === "Competitive Brand Move", "Hilton planning = competitive brand");
  assert(m.meta.audience.brand.signalType !== "Potential Development Opportunity", "not open brand opportunity");
}

{
  const m = computeMarketAlertIntelligence({
    title: "Developer unveils mixed-use project with 150-room hotel and residences",
    summary: "The scheme includes apartments plus a 150-room hotel. Brand not announced.",
  });
  assert(m.meta.event.eventType === "Development Proposal", "mixed-use hotel proposal");
  assert(!m.meta.entities.hotelProject || !/future hotel/i.test(m.meta.entities.hotelProject), "no fake Future Hotel name");
  const label = m.meta.projectLabel || buildProjectLabel({
    eventType: m.meta.event.eventType,
    rooms: m.meta.entities.rooms,
    title: "Developer unveils mixed-use project with 150-room hotel and residences",
    summary: "",
  });
  assert(label && /mixed-use|150-key|hotel/i.test(label), "project label for unnamed mixed-use hotel");
  assert(m.meta.audience.brand.signalType === "Potential Development Opportunity", "unflagged mixed-use brand potential");
}

{
  const m = computeMarketAlertIntelligence({
    title: "Developer secures $80m construction financing for unflagged hotel",
    summary: "Construction financing closed for a hotel project. No brand or operator named.",
  });
  assert(m.meta.event.eventType === "Financing", "construction financing event");
  assert(m.meta.signalTiming === "Decision Forming", "construction financing Decision Forming");
  assert(m.meta.audience.brand.signalType === "Potential Development Opportunity", "unflagged financing brand potential");
  assert(m.meta.audience.operator.signalType === "Potential Management Opportunity", "unflagged financing operator potential");
}

{
  const m = computeMarketAlertIntelligence({
    title: "New hotel opens downtown",
    summary: "The property celebrated its opening this week.",
  });
  assert(m.meta.signalTiming === "Post-Decision" || m.meta.treatment === "STANDARD", "opening is not early signal");
  assert(!isEarlyWr(m), "opening not early WR");
}

{
  const m = computeMarketAlertIntelligence({
    title: "IHG Hotels & Resorts to Bring voco to the City of Lakes With New Signing in Udaipur",
    summary: "Management agreement signed.",
  });
  assert(m.meta.signalTiming === "Decision Announced", "management agreement Decision Announced");
  assert(m.meta.audience.operator.signalType !== "Potential Management Opportunity", "signed MA not open operator");
}

{
  const m = computeMarketAlertIntelligence({
    title: "Planning filed to convert office building into 190-room hotel",
    summary: "Adaptive reuse of an obsolete office into a 190-room hotel. Brand not named.",
  });
  assert(m.meta.event.eventType === "Adaptive Reuse Proposal", "office conversion proposal");
  assert(["Pre-Decision", "Decision Forming"].includes(m.meta.signalTiming), "adaptive reuse early timing");
  assert(m.meta.audience.brand.signalType === "Potential Development Opportunity", "unflagged conversion brand potential");
}

function isEarlyWr(m) {
  const t = m.meta.signalTiming;
  return m.meta.treatment === "REVIEW" && (t === "Pre-Decision" || t === "Decision Forming");
}

{
  const event = inferMarketAlertEvent({ title: "Crown Hotel Camden sold", summary: "" });
  assert(event.eventType === "Sale", "existing sale inference unchanged");
  const timing = inferSignalTiming({ eventType: "Sale", title: "Crown Hotel Camden sold" });
  assert(timing === "Decision Announced", "completed sale Decision Announced");
}

// --- V1.1 false-negative regressions ---
{
  const m = computeMarketAlertIntelligence({
    title: "New Orleans City Council approves Omni Hotel zoning action",
    summary: "The council approved a zoning action for the Omni Hotel project.",
  });
  assert(m.meta.event.eventType === "Planning Approval", "Omni zoning → Planning Approval");
  assert(m.meta.signalTiming === "Decision Forming", "Omni zoning Decision Forming");
  assert(m.meta.projectDirection === "Advancing", "Omni zoning Advancing");
  assert(m.meta.audience.brand.signalType === "Competitive Brand Move", "Omni = Competitive Brand Move");
  assert(m.meta.audience.brand.signalType !== "Potential Development Opportunity", "Omni not open brand opportunity");
}

{
  const m = computeMarketAlertIntelligence({
    title: "Windham Planning Board Signals Support for Mountain Club Hotel Plan",
    summary: "The planning board signaled support for the Mountain Club Hotel Plan at a public meeting.",
  });
  assert(
    ["Planning Application", "Development Proposal", "Planning Approval"].includes(m.meta.event.eventType),
    "Windham planning event recognized"
  );
  assert(["Under Review", "Advancing"].includes(m.meta.projectDirection), "Windham direction Under Review or Advancing");
  assert(m.meta.treatment === "REVIEW", "Windham not rejected for weak context");
  const c = classifyEarlySignalCandidate({
    title: "Windham Planning Board Signals Support for Mountain Club Hotel Plan",
    summary: "The planning board signaled support for the Mountain Club Hotel Plan.",
    family: "planning",
    source: "Google News (EARLY_SIGNAL_PLANNING)",
  });
  assert(c.validHospitality, "Windham valid hospitality");
  assert(c.rejection !== "weak context/entity", "Windham not weak-context rejected");
}

{
  const m = computeMarketAlertIntelligence({
    title: "Afreximbank advances credit approval for Abaco hotel project",
    summary: "Afreximbank advanced credit approval for a hotel project in Abaco. No brand was named.",
  });
  assert(m.meta.event.eventType === "Financing", "Abaco credit approval → Financing");
  assert(m.meta.signalTiming === "Decision Forming", "Abaco Decision Forming");
  assert(m.meta.projectDirection === "Advancing", "Abaco Advancing");
  assert(m.meta.audience.brand.signalType === "Potential Development Opportunity", "Abaco unflagged brand potential");
  assert(m.meta.audience.operator.signalType === "Potential Management Opportunity", "Abaco unflagged operator potential");
}

{
  const m = computeMarketAlertIntelligence({
    title: "Vacant Phoenix office tower headed for second act as JW Marriott",
    summary: "A vacant downtown office tower is headed for a second act as a JW Marriott.",
  });
  assert(m.meta.event.eventType === "Adaptive Reuse Proposal", "Phoenix JW → Adaptive Reuse Proposal");
  assert(["Advancing", "Under Review"].includes(m.meta.projectDirection), "Phoenix JW direction");
  assert(m.meta.audience.brand.signalType === "Competitive Brand Move", "Phoenix JW Competitive Brand Move");
  assert(m.meta.audience.brand.signalType !== "Potential Development Opportunity", "Phoenix JW not open brand opp");
}

{
  const m = computeMarketAlertIntelligence({
    title: "Downtown Phoenix office tower transforming into a luxury resort",
    summary: "The vacant office tower is transforming into a luxury resort. Brand not announced.",
  });
  assert(m.meta.event.eventType === "Adaptive Reuse Proposal", "Phoenix luxury resort adaptive reuse");
  assert(m.meta.audience.brand.signalType === "Potential Development Opportunity", "unbranded reuse brand potential");
}

// --- V1.1 false-positive / entity / stale ---
{
  const c = classifyEarlySignalCandidate({
    title: "World Cup fans flock to Caribbean hotels",
    summary: "Sports travel around the FIFA World Cup.",
    family: "earlyDevelopment",
    cala: true,
    source: "Google News (EARLY_SIGNAL_DEVELOPMENT)",
  });
  assert(c.treatment === "REJECTED" || c.rejection === "off-topic", "World Cup sports rejected");
  assert(!c.earlyWr, "World Cup not early WR");
}

{
  const c = classifyEarlySignalCandidate({
    title: "Cruise hotel guide to the Caribbean",
    summary: "Best cruise ship hotels and ports.",
    family: "earlyDevelopment",
    cala: true,
    source: "Google News (EARLY_SIGNAL_DEVELOPMENT)",
  });
  assert(c.rejection === "off-topic" || c.treatment === "REJECTED", "cruise hotel guide rejected");
}

{
  const c = classifyEarlySignalCandidate({
    title: "Investor buys 14 acres near airport",
    summary: "The buyer acquired land next to the runway. No hotel use disclosed.",
    family: "landSite",
    source: "Google News (EARLY_SIGNAL_LAND)",
  });
  assert(!c.earlyWr, "generic land near airport not early WR");
}

{
  const c = classifyEarlySignalCandidate({
    title: "Hospitality software company acquired by technology vendor",
    summary: "Guest experience platform merger.",
    family: "capitalFormation",
    source: "Google News (EARLY_SIGNAL_CAPITAL)",
  });
  assert(!c.earlyWr, "vendor acquisition not early WR");
}

{
  const stale = detectStaleEarlySignal({
    title: "Royal Decameron Indigo Beach Resort Planning February 2013 Opening",
    summary: "",
    now: new Date("2026-08-14"),
  });
  assert(stale.stale === true, "2013 opening headline is stale");
  const c = classifyEarlySignalCandidate({
    title: "Royal Decameron Indigo Beach Resort Planning February 2013 Opening",
    summary: "The resort is planning its February 2013 opening.",
    family: "earlyDevelopment",
    source: "Google News (EARLY_SIGNAL_DEVELOPMENT)",
  });
  assert(c.rejection === "stale", "stale classified as rejection=stale");
}

{
  assert(!isUsableEntityName("MaltaToday Hotel", { publishers: ["MaltaToday"] }), "reject publisher+Hotel");
  assert(!isUsableEntityName("Proposed Hotel"), "reject Proposed Hotel as proper name");
  assert(!isUsableEntityName("Future Hotel"), "reject Future Hotel as proper name");
  assert(!isUsableEntityName("Man Today Hotel", { publishers: ["Isle of Man Today"] }), "reject publisher-suffix hotel fragment");
  const pubs = inferPublisherTokens("Local news about a hotel - MaltaToday", "Google News (EARLY_SIGNAL_DEVELOPMENT)");
  assert(pubs.some((p) => /maltatoday/i.test(p)), "publisher token from title suffix");
}

{
  const m = computeMarketAlertIntelligence({
    title: "MaltaToday Hotel expansion rumoured",
    summary: "",
    sourceName: "MaltaToday",
  });
  assert(!m.meta.entities.hotelProject || m.meta.entities.hotelProject !== "MaltaToday Hotel", "publisher hotel name stripped");
}

// --- Project direction ---
{
  assert(
    inferProjectDirection({
      eventType: "Planning Application",
      title: "Planning application submitted for proposed hotel",
    }) === "Under Review" ||
      inferProjectDirection({
        eventType: "Planning Application",
        title: "Planning application submitted for proposed hotel",
      }) === "Advancing",
    "planning submitted → Under Review/Advancing"
  );
  assert(
    inferProjectDirection({
      eventType: "Planning Approval",
      title: "Planning approved for 200-room hotel",
    }) === "Advancing",
    "planning approved → Advancing"
  );
  assert(
    inferProjectDirection({
      title: "City council denied the hotel planning application",
    }) === "Rejected / Blocked",
    "planning denied → Rejected / Blocked"
  );
  assert(
    inferProjectDirection({
      title: "Public hearing scheduled for proposed hotel",
    }) === "Under Review",
    "public hearing → Under Review"
  );
  assert(
    inferProjectDirection({
      title: "NGT files suo moto case over proposed hotel construction",
    }) === "Challenged",
    "environmental case → Challenged"
  );
  assert(
    inferProjectDirection({
      eventType: "Financing",
      title: "Hotel construction loan secured",
    }) === "Advancing",
    "construction loan secured → Advancing"
  );
  assert(
    inferProjectDirection({
      title: "Hotel project postponed after financing delay",
    }) === "Delayed",
    "project postponed → Delayed"
  );
  assert(
    inferProjectDirection({
      title: "Proposed hotel project cancelled by developer",
    }) === "Rejected / Blocked",
    "project cancelled → Rejected / Blocked"
  );
  assert(
    inferProjectDirection({
      title: "Santa Fe City Council rejects appeal of proposed hotel",
    }) !== "Rejected / Blocked",
    "appeal rejected is not project rejected"
  );
}

{
  const blocked = computeMarketAlertIntelligence({
    title: "City council denied the hotel planning application",
    summary: "The proposed hotel application was rejected. No brand named.",
  });
  assert(blocked.meta.projectDirection === "Rejected / Blocked", "denied application direction");
  assert(
    blocked.meta.audience.brand.signalType !== "Potential Development Opportunity",
    "rejected project is not Potential Development Opportunity"
  );
  assert(/rejected or blocked/i.test(blocked.fields["Why It Matters — Owner"] || ""), "owner copy reflects rejected");
}

{
  const challenged = computeMarketAlertIntelligence({
    title: "NGT files suo moto case over proposed hotel construction",
    summary: "The tribunal opened an environmental case over a proposed hotel. No brand named.",
  });
  assert(challenged.meta.projectDirection === "Challenged", "NGT challenged");
  assert(/regulatory or planning challenges|environmental scrutiny|facing legal/i.test(
    `${challenged.fields["Why It Matters — Brand"] || ""} ${challenged.fields["Why It Matters — Owner"] || ""}`
  ), "challenged copy is cautious");
}

{
  const land = listEarlySignalQueries("landSite");
  assert(land.some((q) => /land acquired for hotel/i.test(q.query)), "land family uses short exact queries");
  assert(!land.some((q) => q.query.split("OR").length > 4), "land queries are not large nested ORs");
  const open = listEarlySignalQueries("openDecision");
  assert(open.length >= 6, "open decision family has compact queries");
}

if (failed) {
  console.error(`\n${failed} assertion(s) failed`);
  process.exit(1);
}
console.log("\nAll early-signal tests passed.");
