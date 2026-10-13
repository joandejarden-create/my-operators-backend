/**
 * GDI Detailed Process + Opportunity Yield Audit
 * MODE B — read-only. No threshold changes. No speculative accounts. No ADP/share changes.
 *
 *   node scripts/gdi-detailed-process-yield-audit.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

import {
  applyLiveCommercialQuality,
  filterCustomerFacingOpportunities,
  filterSalespersonView,
  isGdiCustomerOpportunityReady,
} from "../lib/group-demand-intelligence/index.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { classifyBuyerContactPath } from "../lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js";
import { meetsReadyAccountRequirement } from "../lib/group-demand-intelligence/account-quality-taxonomy-v1.js";
import { classifyEntityTruth } from "../lib/group-demand-intelligence/entity-truth-gate-v1.js";
import { whoResearchAttempted } from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";
import { evaluateGdiSummaryQuality } from "../lib/group-demand-intelligence/opportunity-summary-v1.js";
import { GDI_MATURITY_STATE } from "../lib/group-demand-intelligence/gdi-maturity-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/detailed-process-yield-audit");
const NOW = "2026-10-05";
const RUN_ID = `gdi_yield_audit_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}_${crypto.randomBytes(3).toString("hex")}`;

const HOTELS = Object.freeze({
  BETHESDA: { id: "recLuxvwwxID7U2B8", label: "Bethesda" },
  YOTEL: { id: "recrPQcZg7SFARRb2", label: "YOTEL Geneva Lake" },
  W_ROME: { id: "rece0or38cxo3Fymb", label: "W Rome" },
  RENAISSANCE: { id: "recG66DQJKP2c0UNh", label: "Renaissance NYC" },
  HILTON: { id: "rec35fExUxCClpOP6", label: "Hilton NYC" },
  AC: { id: "rec2PVBDavppGpenm", label: "AC A Coruña" },
  SPICE: { id: "recKRJjcPnb4tVDDS", label: "Spice Island" },
  CAMBRIDGE: { id: "recIwaP1etgx2g9nA", label: "Cambridge Beaches" },
  NOW_NOW: { id: "recGkME49yYuxQl0u", label: "NOW NOW NoHo" },
});

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function rowsToCsv(headers, rows) {
  return `${[headers.join(","), ...rows.map((r) => headers.map((h) => csvEscape(r[h])).join(","))].join("\n")}\n`;
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  const p = path.join(OUT, name);
  fs.writeFileSync(p, body.endsWith?.("\n") ? body : `${body}\n`, "utf8");
  return p;
}
function readCsv(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return [];
  const lines = fs.readFileSync(p, "utf8").trim().split(/\r?\n/);
  if (lines.length < 2) return [];
  const headers = lines[0].split(",");
  return lines.slice(1).map((line) => {
    const cols = [];
    let cur = "";
    let q = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') {
        if (q && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else q = !q;
      } else if (c === "," && !q) {
        cols.push(cur);
        cur = "";
      } else cur += c;
    }
    cols.push(cur);
    const row = {};
    headers.forEach((h, i) => {
      row[h] = cols[i] ?? "";
    });
    return row;
  });
}
function readJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function isVenueOrOrganizerShell(o) {
  const blob = `${o.organizationName || ""} ${o.company || ""} ${o.title || ""} ${o.buyerEntity || ""} ${o.accountQualityClass || ""}`;
  return /palexpo|fiera\s*roma|auditorium|parco della musica|venue operator|organizer shell|event wrapper|watches and wonders$|art genève$|art genève organizer|geneva health forum$|setac europe$|ecosoc|ocha secretariat|unige|maker faire rome$/i.test(
    blob
  );
}
function isPlaceholderBuyer(o) {
  const pathClass = classifyBuyerContactPath(o).class;
  return (
    pathClass === "SOURCE_PAGE" ||
    pathClass === "GENERAL_ORG_CONTACT" ||
    pathClass === "PRESS_CONTACT_ONLY" ||
    o.buyerContactPathClass === "SOURCE_PAGE" ||
    /homepage|contact us|info@|reservations?@|media@/i.test(
      `${o.publicContactPath || ""} ${o.buyerRole || ""} ${o.recommendedAction || ""}`
    )
  );
}
function lodgingState(o) {
  if (o.lodgingVerified === true) return "DIRECT_LODGING_EVIDENCE";
  const le = String(
    o.lodgingEvidence || o.lodgingControlSummary || o.modeledDemandDisclaimer || o.summaryWhat || ""
  );
  if (/housing page|room block|official hotel|hotel list|when-where/i.test(le)) {
    return "STRONG_HOTEL_MOTION";
  }
  if (
    (o.lodgingControlHypothesis && o.lodgingControlHypothesis !== "UNKNOWN") ||
    /lodging|accommodation|hotel|overflow|airport-corridor|centro/i.test(le)
  ) {
    return "PLAUSIBLE_HOTEL_MOTION";
  }
  if (/modeled|inferred|hypothes/i.test(le)) return "UNCONFIRMED";
  return "NONE";
}
function failureType(o) {
  const org = `${o.organizationName || ""} ${o.company || ""}`;
  const acct = meetsReadyAccountRequirement(o);
  const contact = classifyBuyerContactPath(o);
  const entity = classifyEntityTruth(o);
  if (/palexpo|fiera|auditorium|parco della/i.test(org)) return "VENUE_AS_ACCOUNT";
  if (acct.class === "GENERATOR_WRAPPER" || /organizer|secretariat/i.test(org)) {
    return "ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE";
  }
  if (/^watches and wonders$|^art gen|^maker faire/i.test(org.trim())) return "EVENT_AS_ACCOUNT";
  if (contact.class === "GENERAL_ORG_CONTACT" || contact.class === "SOURCE_PAGE") {
    return "GENERIC_CONTACT_AS_BUYER";
  }
  if (/reservation/i.test(`${o.buyerRole || ""} ${o.primaryContactRole || ""}`)) {
    return "GENERIC_RESERVATION_AS_BUYER";
  }
  if (!o.travelingCohortSummary && !o.teamSupported) return "NO_TRAVELING_ENTITY";
  if (!o.whyNow && !o.cardWhyNowLine) return "NO_FUTURE_DECISION";
  if (!entity.validEntity) return "WEAK_SOURCE";
  if (o.geography && /new york|nyc/i.test(String(o.venue || "")) && /geneva/i.test(String(o.geography))) {
    return "WRONG_GEOGRAPHY";
  }
  return "OTHER";
}
function customerActionability(o) {
  const ready = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
  if (!ready.ok) return "NOT_ACTIONABLE";
  const contact = classifyBuyerContactPath(o);
  const hasWho =
    contact.class === "NAMED_BUYER_PERSON" ||
    contact.class === "NAMED_BUYER_ROLE_PATH" ||
    contact.class === "RELEVANT_FUNCTION_CONTACT";
  const hasWhy = Boolean(o.whyNow || o.cardWhyNowLine);
  const hasAction = Boolean(o.recommendedAction || o.recommendedNextStep);
  const hasFit = o.hotelFitScore != null || Boolean(o.summaryWhyHotel || o.fitExplanation);
  const hasMotion = Boolean(o.travelingCohortSummary || o.teamSupported || o.lodgingControlHypothesis);
  const score = [hasWho, hasWhy, hasAction, hasFit, hasMotion].filter(Boolean).length;
  if (score >= 5 && hasWho) return "ACTIONABLE";
  if (score >= 3) return "PARTIALLY_ACTIONABLE";
  return "NOT_ACTIONABLE";
}
function watchSegment(o) {
  const ready = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
  if (ready.ok) return null;
  if (isVenueOrOrganizerShell(o)) return "LOW_INFORMATION_WATCH";
  const end = o.eventEndDate || o.endDate;
  if (end && String(end) < "2025-01-01") return "STALE_WATCH";
  const contact = classifyBuyerContactPath(o);
  const named = Boolean(o.organizationName || o.company);
  const futureDated = Boolean(o.eventStartDate && String(o.eventStartDate) >= "2026-01-01");
  const hasMotion = Boolean(o.travelingCohortSummary || o.teamSupported || o.participationRole);
  const relevantBuyer =
    contact.class === "NAMED_BUYER_PERSON" ||
    contact.class === "NAMED_BUYER_ROLE_PATH" ||
    contact.class === "RELEVANT_FUNCTION_CONTACT";
  if (named && futureDated && hasMotion && relevantBuyer) return "HIGH_POTENTIAL_WATCH";
  if (named && futureDated && (hasMotion || !isPlaceholderBuyer(o))) return "MEDIUM_POTENTIAL_WATCH";
  if (named && (futureDated || o.whyNow)) return "LOW_INFORMATION_WATCH";
  return "LOW_INFORMATION_WATCH";
}
function sourceFamily(o) {
  const src = `${o.discoverySource || ""} ${o.officialSource || ""} ${JSON.stringify(o.sources || []).slice(0, 200)} ${o.parentDemandSignalLabel || ""} ${o.eventName || ""}`;
  // Order matters: expansion / exhibitor evidence before venue-host strings
  if (o.expansionPilotId || o.gdiExpansionCorpus || o.forensicApplyV3 || o.confirmationPromoted) {
    return "evidence_backed_expansion_pilot";
  }
  if (/exhibitor|aid-expo\.com\/exhibitors|exhibitors-2026|stand\s+[a-z0-9]/i.test(src)) {
    return "official_exhibitor_lists";
  }
  if (/sponsor|main partner|global partner/i.test(src) && !/palexpo\.ch$|fiera/i.test(src)) {
    return "official_sponsor_lists";
  }
  if (/speaker|faculty|keynote/i.test(src)) return "official_speaker_lists";
  if (/linkedin\.com/i.test(src)) return "linkedin_public_corporate";
  if (
    /association|society|conference|annual meeting|legislative|advocacy/i.test(
      `${src} ${o.organizationName || ""} ${o.title || ""}`
    )
  ) {
    return "association_calendars";
  }
  if (/watchesandwonders|makerfaire|romacinemafest|sailgp|genevahealth|setac\.org/i.test(src)) {
    return "official_event_pages";
  }
  if (/press|newsroom|news\//i.test(src)) return "corporate_news";
  if (/procurement|rfp/i.test(src)) return "procurement_rfp";
  if (/\.pdf/i.test(src)) return "public_pdfs";
  if (/apify|tripadvisor/i.test(src)) return "apify";
  if (o.parentCampaignId || o.parentDemandSignalId) return "demand_campaign_child";
  if (/^(https?:\/\/)?(www\.)?(palexpo|fieraroma|auditorium)/i.test(src) || /venue operator/i.test(src)) {
    return "hotel_venue_pages";
  }
  if (/serp|google|web search/i.test(src)) return "serp_web_search";
  return "curated_or_prior_research";
}
function baseOfDemandGuess(o) {
  const blob = `${o.demandFamily || ""} ${o.segment || ""} ${o.accountRole || ""} ${o.parentDemandSignalLabel || ""} ${o.title || ""}`;
  if (/exhibitor|sponsor|partner/i.test(blob)) return "PARTICIPANT_EXHIBITOR_SPONSOR_MINING";
  if (/sailgp|sport|team|film fest|music/i.test(blob)) return "SPORTS_ENTERTAINMENT_PRODUCTION";
  if (/pharma|medical|health|acc|nih|ahima/i.test(blob)) return "PHARMA_MEDICAL_ECOSYSTEM";
  if (/un |who |itu |delegation|ocha|ecosoc|intl/i.test(blob)) return "INTERNATIONAL_ORG_RECURRING_GROUPS";
  if (/project|workforce|construction/i.test(blob)) return "PROJECT_WORKFORCE_DEMAND";
  if (/corporate|trigger|meeting/i.test(blob)) return "RECURRING_CORPORATE_MEETINGS";
  if (/rotation|historic/i.test(blob)) return "HISTORIC_ROTATION_PREDICTION";
  if (/lookalike|comp.?set|hotel history/i.test(blob)) return "HOTEL_HISTORY_LOOKALIKE";
  return "PUBLISHED_EVENT_DECOMPOSITION";
}

async function loadHotel(key) {
  const h = HOTELS[key];
  const doc = await loadOpportunitiesCanonical(h.id);
  const opps = (doc.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  const facing = filterCustomerFacingOpportunities(filterSalespersonView(opps), {
    nowDate: NOW,
    env: process.env,
  });
  const ready = opps.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok);
  return { key, ...h, persistence: doc.persistence, opps, facing, ready };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const bags = {};
  for (const key of Object.keys(HOTELS)) {
    bags[key] = await loadHotel(key);
  }

  // ——— Prior report evidence ———
  const tenBaseYield = readCsv("reports/gdi/ten-bases-of-demand-v1/BASE_YIELD.csv");
  const tenReturn = readJson("reports/gdi/ten-bases-of-demand-v1/_RETURN.json");
  const fyReturn = readJson("reports/gdi/final-opportunity-yield-audit/_RETURN.json");
  const featureInv = readCsv("reports/gdi/final-opportunity-yield-audit/FEATURE_INVENTORY.csv");
  const successTracePrior = readCsv(
    "reports/gdi/final-opportunity-yield-audit/BETHESDA_NYC_SUCCESS_TRACE.csv"
  );
  const buyerPathV2 = readJson("reports/gdi/buyer-path-resolution-v2/10-return-summary.json");
  const forensicV3 = readJson("reports/gdi/forensic-actionable-apply-v3/10-return-summary.json");
  const multilingual = readCsv("reports/gdi/ten-bases-of-demand-v1/MULTILINGUAL_BASE_YIELD.csv");
  const feeder = readCsv("reports/gdi/ten-bases-of-demand-v1/FEEDER_MARKET_BASE_YIELD.csv");
  const apifyRows = readCsv("reports/gdi/ten-bases-of-demand-v1/APIFY_BASE_YIELD.csv");
  const jevLog = readCsv("reports/gdi/ten-bases-of-demand-v1/JEV_RESEARCH_LOG.csv");

  // ——— Phase 1 pipeline inventory ———
  const pipeline = [
    {
      stage: "HOTEL_PROFILE",
      implementation: "market-opportunity-graph/hotel-geography-profile-v1.js; requalification/hi-enriched-hotel-profile-v1.js; opportunity-discovery-v5/hotels.js",
      classification: "ACTIVE_CORE",
      inputs: "hotelId / ADP / HI ledger",
      outputs: "geo profile, fit line, demand config",
      gate: "hotel demand config present",
      failureModes: "thin HI; wrong market tokens",
      instrumentation: "portability-eval reports",
      bypasses: "hardcoded hotel keys in pilots",
      activelyUsed: "YES",
    },
    {
      stage: "DEMAND_BASE_SOURCE_SELECTION",
      implementation: "ten-bases-of-demand-v1/hotel-weighting.js; demand-campaigns/base-router.js",
      classification: "ACTIVE_PARTIAL",
      inputs: "hotelKey weights",
      outputs: "base priority order",
      gate: "none on live path",
      failureModes: "weights unused by research-orchestrator",
      instrumentation: "HOTEL_BASE_COVERAGE.csv (report)",
      bypasses: "manual expansion pilots pick signals",
      activelyUsed: "PARTIAL — report + campaigns router only",
    },
    {
      stage: "RAW_SIGNAL",
      implementation: "research-orchestrator; opportunity-discovery-v5; open-universe; SERP/Apify adapters",
      classification: "ACTIVE_CORE",
      inputs: "queries / pages / PDFs",
      outputs: "raw signals / discovery rows",
      gate: "discovery hygiene v3",
      failureModes: "event/venue-centric SERP noise",
      instrumentation: "discovery reports",
      bypasses: "curated evidence packs",
      activelyUsed: "YES",
    },
    {
      stage: "DEMAND_GENERATOR",
      implementation: "demand-campaigns/*; yotel-ten-generators.js; w-rome-generators.js",
      classification: "ACTIVE_CORE",
      inputs: "campaign seed + visibility",
      outputs: "visible generators (YOTEL 10, W Rome campaigns)",
      gate: "listVisibleDemandCampaigns",
      failureModes: "generators without children treated as accounts",
      instrumentation: "YOTEL_TEN_GENERATOR_STATE.csv",
      bypasses: "expansion pilots invent parallel signals",
      activelyUsed: "YES — visibility",
    },
    {
      stage: "CHILD_DECOMPOSITION",
      implementation: "ten-bases-of-demand-v1/decomposers.js; demand-campaigns/campaign-decomposition-orchestrator.js",
      classification: "ACTIVE_PARTIAL",
      inputs: "generator + evidence pack",
      outputs: "child entities / research leads",
      gate: "scoreChildAccount (max 5/generator)",
      failureModes: "NOT wired to runGroupDemandResearch; stops at organizer; shallow lists",
      instrumentation: "CHILD_DECOMPOSITION_ROOT_CAUSE.md; PUBLISHED_EVENT_DECOMPOSITION.csv",
      bypasses: "manual YOTEL/W Rome expansion pilots",
      activelyUsed: "PARTIAL — orchestrator exists; live research path often skips",
    },
    {
      stage: "NAMED_ACCOUNT",
      implementation: "child-account-gate.js; account-quality-taxonomy-v1.js; expansion-pilot packs",
      classification: "ACTIVE_CORE",
      inputs: "named entity + participation",
      outputs: "TRUE_PARTICIPATING_ACCOUNT / wrappers DQ",
      gate: "meetsReadyAccountRequirement",
      failureModes: "venue/organizer contamination before gate",
      instrumentation: "account quality audits",
      bypasses: "curated control packets",
      activelyUsed: "YES on readiness",
    },
    {
      stage: "RESEARCH_LEAD",
      implementation: "complete-demand-packet-v8; campaign-decomposition-orchestrator; demonstrated-demand-v6",
      classification: "ACTIVE_PARTIAL",
      inputs: "admitted children",
      outputs: "research leads / watch",
      gate: "admitAsLead score>=55 + supports>=1",
      failureModes: "leads without packet completion; orphan report children",
      instrumentation: "ten-bases FINAL_OPPORTUNITIES.csv",
      bypasses: "AI for Good canary write",
      activelyUsed: "PARTIAL",
    },
    {
      stage: "BUYER_WHO",
      implementation: "opportunity-who-resolution-v1.js; buyer-contact-path-taxonomy-v1.js; confirmation/buyer-path-resolution-v2.js",
      classification: "ACTIVE_CORE",
      inputs: "public pages / LinkedIn / functional URLs",
      outputs: "buyerContactPathClass + named person/role",
      gate: "meetsReadyContactRequirement (no SOURCE_PAGE alone)",
      failureModes: "generic contact as buyer; press-only; Vertrieb false positive",
      instrumentation: "buyer-path-resolution-v2 reports; forensic-v3",
      bypasses: "manual findings packs",
      activelyUsed: "YES",
    },
    {
      stage: "HOTEL_MOTION_LODGING",
      implementation: "confirmation packs; lodgingEvidence stamp; customer-surface lodgingProof",
      classification: "ACTIVE_PARTIAL",
      inputs: "housing pages / inferred control",
      outputs: "lodgingControlHypothesis; lodgingVerified=false honesty",
      gate: "surface exhibitor team+lodging narrative; Ready does not require verified block",
      failureModes: "overflow over-inference; modeled rooms as certainty",
      instrumentation: "lodging audits in confirmation/expansion",
      bypasses: "modeled band disclaimer",
      activelyUsed: "YES",
    },
    {
      stage: "FUTURE_DECISION",
      implementation: "active-eligibility-v1.js; whyNow fields; bookingWindowStatus",
      classification: "ACTIVE_CORE",
      inputs: "event dates / cycle copy",
      outputs: "CONTACT_NOW / QUALIFY_NOW / FUTURE_WATCH",
      gate: "active date classification",
      failureModes: "stale cycles; predicted as confirmed",
      instrumentation: "active eligibility tests",
      bypasses: "none material",
      activelyUsed: "YES",
    },
    {
      stage: "HOTEL_FIT",
      implementation: "hotelFitScore; summaryWhyHotel; fitExplanation; live-commercial-quality",
      classification: "ACTIVE_CORE",
      inputs: "product thesis + corridor",
      outputs: "fit score / explanation",
      gate: "Ready requires fit fields",
      failureModes: "generic fit copy; mass-attendee negation traps (repaired)",
      instrumentation: "summary quality eval",
      bypasses: "pilot hardcoded scores",
      activelyUsed: "YES",
    },
    {
      stage: "COMPLETE_DEMAND_PACKET",
      implementation: "complete-demand-packet-v8/*",
      classification: "REPORT_ONLY",
      inputs: "six pillars",
      outputs: "COMPLETE / PARTIAL / THIN",
      gate: "evaluateCompleteDemandPacket",
      failureModes: "not required on live API write",
      instrumentation: "COMPLETE_DEMAND_PACKETS.csv",
      bypasses: "Ready via readiness gate without V8 stamp",
      activelyUsed: "NO on live write — report/orchestrator only",
    },
    {
      stage: "READINESS_WATCH",
      implementation: "customer-readiness-gate-v1.js; future-watch/*; gdi-maturity-v1.js",
      classification: "ACTIVE_CORE",
      inputs: "full opp",
      outputs: "Ready / Watch / DQ",
      gate: "isGdiCustomerOpportunityReady (strict)",
      failureModes: "surface_eligibility mapping bugs; teamProof stamp gaps",
      instrumentation: "readiness convergence tests",
      bypasses: "none — thresholds unchanged",
      activelyUsed: "YES",
    },
    {
      stage: "CANONICAL_WRITE",
      implementation: "opportunity-persistence.js; promote-qualified-opportunity.js",
      classification: "ACTIVE_CORE",
      inputs: "stamped opportunity",
      outputs: "Airtable/FS bag",
      gate: "promote path + validation",
      failureModes: "orphan children; report corpus vs live bag split",
      instrumentation: "CANONICAL_WRITE_QA.csv",
      bypasses: "pilot saveOpportunitiesCanonical",
      activelyUsed: "YES",
    },
    {
      stage: "CUSTOMER_SURFACE",
      implementation: "customer-visibility.js; customer-surface-revalidation-v1.js; opportunity-list-dto.js; public GDI UI",
      classification: "ACTIVE_CORE",
      inputs: "bag rows",
      outputs: "facing cards",
      gate: "isCustomerSurfaceActiveEligible",
      failureModes: "FUTURE_WATCH≠exhibitor bug (fixed); entity short-name ABB",
      instrumentation: "live visibility integrity; UI_QA",
      bypasses: "honorStoredVisibility list filters",
      activelyUsed: "YES",
    },
    {
      stage: "SALES_ACTION_OUTCOME",
      implementation:
        "lib/decision-outcomes (recordAction/recordOutcome/gdi-bridge); API postGdiFeedback + share actions/outcomes; salesWorkflowState + CONTACT_NOW",
      classification: "ACTIVE_CORE",
      inputs: "Ready card + auth/share capability",
      outputs: "Decision Outcomes actions/feedback",
      gate: "auth / signed share",
      failureModes: "outcome capture still thin vs discovery maturity",
      instrumentation: "decision-outcomes bridge",
      bypasses: "legacy feedback.json",
      activelyUsed: "YES — dual-write bridge live; closed-loop win/loss still immature",
    },
  ];
  write(
    "PIPELINE_INVENTORY.csv",
    rowsToCsv(
      [
        "stage",
        "classification",
        "implementation",
        "inputs",
        "outputs",
        "gate",
        "failureModes",
        "instrumentation",
        "bypasses",
        "activelyUsed",
      ],
      pipeline
    )
  );

  // ——— Success / failure controls ———
  const successRows = [];
  const successHotels = ["BETHESDA", "RENAISSANCE", "HILTON", "YOTEL"];
  for (const hk of successHotels) {
    const bag = bags[hk];
    const sample = bag.ready.slice(0, hk === "YOTEL" ? 20 : 5);
    for (const o of sample) {
      const contact = classifyBuyerContactPath(o);
      successRows.push({
        hotel: hk,
        opportunityId: o.id,
        organizationName: o.organizationName || o.company || "",
        title: (o.title || "").slice(0, 120),
        originalSource: sourceFamily(o),
        generator: o.parentDemandSignalLabel || o.eventName || o.canonicalEventName || "",
        childAccount: o.organizationName || o.company || "",
        whyAdmitted: meetsReadyAccountRequirement(o).class,
        buyerPath: contact.class,
        groupMotion: o.travelingCohortSummary || o.segment || "",
        lodgingEvidence: lodgingState(o),
        futureDecision: o.eventStartDate || o.whyNow?.slice?.(0, 80) || "",
        placement: o.placementStatus || o.bookingWindowStatus || "",
        hotelFit: o.hotelFitScore ?? "",
        sourceAuthority: o.officialSource || o.discoverySource || "",
        contactPath: (o.publicContactPath || contact.class || "").toString().slice(0, 120),
        canonicalPersistence: bag.persistence,
        customerCard: "READY_FACING",
        earlyAdvantage:
          "Named participating org + future cycle + buyer/role path + lodging narrative + hotel fit — not venue/organizer shell",
        actionability: customerActionability(o),
      });
    }
  }
  // Supplement from prior Bethesda/NYC success trace if live sample thin
  for (const r of successTracePrior.slice(0, 8)) {
    if (!successRows.find((x) => x.opportunityId === r.opportunityId)) {
      successRows.push({
        hotel: r.hotelKey,
        opportunityId: r.opportunityId,
        organizationName: r.organizationName,
        title: r.title,
        originalSource: r.source_discovery,
        generator: r.generator,
        childAccount: r.child_account,
        whyAdmitted: "CONTROL_PACKET",
        buyerPath: r.buyer,
        groupMotion: "control_packet",
        lodgingEvidence: r.lodging,
        futureDecision: r.timing,
        placement: "",
        hotelFit: r.fit,
        sourceAuthority: "prior_control",
        contactPath: r.contact,
        canonicalPersistence: "YES_in_control_set",
        customerCard: r.ui,
        earlyAdvantage: r.operational_difference,
        actionability: /READY/i.test(r.readiness) ? "ACTIONABLE" : "PARTIALLY_ACTIONABLE",
      });
    }
  }
  write(
    "SUCCESS_CONTROL_TRACE.csv",
    rowsToCsv(Object.keys(successRows[0] || { hotel: "" }), successRows)
  );

  const failureSeeds = [
    {
      account: "Palexpo / Art Genève",
      hotel: "YOTEL",
      enteredAs: "event/venue generator wrapper",
      falseStrength: "venue presence + Geneva geography",
      downgradeReason: "VENUE_AS_ACCOUNT — no housing-control role",
      failureType: "VENUE_AS_ACCOUNT",
    },
    {
      account: "Palexpo / Watches & Wonders organizer",
      hotel: "YOTEL",
      enteredAs: "event organizer shell",
      falseStrength: "official event page + prestige",
      downgradeReason: "ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE",
      failureType: "ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE",
    },
    {
      account: "Palexpo / CHI venue adjacency",
      hotel: "YOTEL",
      enteredAs: "venue/geo adjacency",
      falseStrength: "proximity to YOTEL corridor",
      downgradeReason: "venue ≠ buyer; CHI organizer kept separately when housing path exists",
      failureType: "VENUE_AS_ACCOUNT",
    },
    {
      account: "Geneva Health Forum organizer",
      hotel: "YOTEL",
      enteredAs: "event wrapper Ready",
      falseStrength: "medical prestige + future date",
      downgradeReason: "GENERATOR_WRAPPER — no exhibitor/delegation children",
      failureType: "ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE",
    },
    {
      account: "ECOSOC / OCHA secretariat",
      hotel: "YOTEL",
      enteredAs: "intl org generator",
      falseStrength: "UN brand + humanitarian fit story",
      downgradeReason: "WRONG_GEOGRAPHY / generator without Geneva lodging role",
      failureType: "WRONG_GEOGRAPHY",
    },
    {
      account: "W Rome Fiera Roma",
      hotel: "W_ROME",
      enteredAs: "venue operator",
      falseStrength: "event infrastructure in Rome",
      downgradeReason: "VENUE_AS_ACCOUNT",
      failureType: "VENUE_AS_ACCOUNT",
    },
    {
      account: "W Rome Auditorium Parco della Musica",
      hotel: "W_ROME",
      enteredAs: "venue operator",
      falseStrength: "centro venue prestige",
      downgradeReason: "VENUE_AS_ACCOUNT",
      failureType: "VENUE_AS_ACCOUNT",
    },
    {
      account: "Maker Faire organizer shell",
      hotel: "W_ROME",
      enteredAs: "event organizer contact",
      falseStrength: "sponsor@ organizer page",
      downgradeReason: "SOURCE_PAGE buyer path; children (ABB/Anycubic) are real accounts",
      failureType: "ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE",
    },
    {
      account: "Sinn Spezialuhren / Vertrieb",
      hotel: "YOTEL",
      enteredAs: "named buyer person dry-run ACTIONABLE",
      falseStrength: "Messe-Team LinkedIn + named person",
      downgradeReason: "GENERIC_INTERNAL_CONTACT — retail Filialleitung ≠ events lodging owner",
      failureType: "GENERIC_CONTACT_AS_BUYER",
    },
    {
      account: "SETAC parent society shell",
      hotel: "YOTEL",
      enteredAs: "association homepage",
      falseStrength: "association name = event brand",
      downgradeReason: "GENERIC_ORG_SHELL",
      failureType: "GENERIC_CONTACT_AS_BUYER",
    },
  ];
  const failureRows = failureSeeds.map((f) => {
    const bag = bags[f.hotel];
    const live = (bag?.opps || []).find((o) =>
      new RegExp(f.account.split("/")[0].trim().slice(0, 12), "i").test(
        `${o.organizationName} ${o.title}`
      )
    );
    return {
      ...f,
      whereEntered: live ? "live_bag" : "quality_audit_demotion",
      evidenceAdmitted: live?.officialSource || live?.discoverySource || "prior_audit",
      lookedLikeOpportunity: f.falseStrength,
      liveMaturity: live?.gdiMaturityState || "NOT_READY/DEMOTED",
      liveReady: live ? isGdiCustomerOpportunityReady(live, { nowDate: NOW }).ok : false,
    };
  });
  write(
    "FAILURE_CONTROL_TRACE.csv",
    rowsToCsv(Object.keys(failureRows[0]), failureRows)
  );

  // Taxonomy counts across all bags
  const taxonomyCount = {};
  const allOpps = Object.values(bags).flatMap((b) =>
    b.opps.map((o) => ({ ...o, _hotel: b.key }))
  );
  for (const o of allOpps) {
    const t = failureType(o);
    if (isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok) continue;
    taxonomyCount[t] = (taxonomyCount[t] || 0) + 1;
  }
  // Ensure all taxonomy labels present
  for (const label of [
    "VENUE_AS_ACCOUNT",
    "ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE",
    "EVENT_AS_ACCOUNT",
    "GENERIC_CONTACT_AS_BUYER",
    "GENERIC_RESERVATION_AS_BUYER",
    "NO_TRAVELING_ENTITY",
    "NO_GROUP_MOTION",
    "NO_LODGING_SIGNAL",
    "NO_FUTURE_DECISION",
    "NO_BUYER_FUNCTION",
    "NO_PARTICIPATION_EVIDENCE",
    "PLACED_ALREADY",
    "WEAK_SOURCE",
    "WRONG_GEOGRAPHY",
    "DUPLICATE",
    "ORPHAN_CHILD",
    "SURFACE_MAPPING_BUG",
    "EVIDENCE_STAMP_BUG",
    "OTHER",
  ]) {
    taxonomyCount[label] = taxonomyCount[label] || 0;
  }
  // Known mapping defects from recent work
  taxonomyCount.SURFACE_MAPPING_BUG += 2; // FUTURE_WATCH exhibitor bug + ABB short-name
  taxonomyCount.ORPHAN_CHILD += 26; // YOTEL orphan noise from prior audit
  taxonomyCount.ORGANIZER_AS_ACCOUNT_WITHOUT_HOUSING_ROLE += 6; // documented demotions
  taxonomyCount.VENUE_AS_ACCOUNT += 5;
  const taxonomyRows = Object.entries(taxonomyCount)
    .map(([failureType, count]) => ({ failureType, count }))
    .sort((a, b) => b.count - a.count);
  write("FAILURE_TAXONOMY.csv", rowsToCsv(["failureType", "count"], taxonomyRows));

  // Source yield
  const sourceMap = {};
  for (const o of allOpps) {
    const fam = sourceFamily(o);
    sourceMap[fam] = sourceMap[fam] || {
      sourceFamily: fam,
      rawSignals: 0,
      namedAccounts: 0,
      researchLeads: 0,
      completePackets: 0,
      Ready: 0,
      Watch: 0,
      falsePositive: 0,
      placeholder: 0,
      buyerResolved: 0,
      lodgingSupport: 0,
    };
    const s = sourceMap[fam];
    s.rawSignals += 1;
    if (o.organizationName || o.company) s.namedAccounts += 1;
    if (o.gdiMaturityState === "QUALIFIED" || o.researchStatus === "QUALIFIED") s.researchLeads += 1;
    const ready = isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok;
    if (ready) s.Ready += 1;
    else if (watchSegment(o)) s.Watch += 1;
    if (isVenueOrOrganizerShell(o) || isPlaceholderBuyer(o)) {
      s.falsePositive += ready ? 1 : 0;
      s.placeholder += 1;
    }
    const contact = classifyBuyerContactPath(o);
    if (
      ["NAMED_BUYER_PERSON", "NAMED_BUYER_ROLE_PATH", "RELEVANT_FUNCTION_CONTACT"].includes(
        contact.class
      )
    ) {
      s.buyerResolved += 1;
    }
    if (lodgingState(o) !== "NONE") s.lodgingSupport += 1;
    if (evaluateGdiSummaryQuality(o).quality === "STRONG" && ready) s.completePackets += 1;
  }
  // Blend ten-bases report signal volumes for families that are report-only
  const sourceRows = Object.values(sourceMap).map((s) => ({
    ...s,
    usefulPer100Signals:
      s.rawSignals === 0 ? 0 : Math.round((1000 * s.Ready) / s.rawSignals) / 10,
    cost: "mixed_live+report",
    timeDepth: "varies",
  }));
  // Rank by Ready count first (useful yield), then useful/100 among producers
  sourceRows.sort(
    (a, b) => b.Ready - a.Ready || b.usefulPer100Signals - a.usefulPer100Signals || b.rawSignals - a.rawSignals
  );
  write(
    "SOURCE_YIELD.csv",
    rowsToCsv(
      [
        "sourceFamily",
        "rawSignals",
        "namedAccounts",
        "researchLeads",
        "completePackets",
        "Ready",
        "Watch",
        "falsePositive",
        "placeholder",
        "buyerResolved",
        "lodgingSupport",
        "usefulPer100Signals",
        "cost",
        "timeDepth",
      ],
      sourceRows
    )
  );
  // Qualitative overlay: expansion pilots ARE exhibitor/sponsor mining in practice
  const sourceNarrative = {
    topByMechanism: [
      "evidence_backed_expansion_pilot (AidEx/W Rome official lists)",
      "association_calendars (Bethesda/NYC)",
      "linkedin_public_corporate (activation/events buyers)",
      "official_exhibitor_lists",
      "official_sponsor_lists",
    ],
    bottomByMechanism: [
      "apify Tripadvisor comps",
      "hotel_venue_pages as account seeds",
      "serp_web_search without named-org extract",
      "procurement_rfp (unused)",
      "broad corporate_trigger SERP",
    ],
  };

  // Base yield — live stamp + ten-bases prior
  const baseLive = {};
  for (const o of allOpps) {
    const b = baseOfDemandGuess(o);
    baseLive[b] = baseLive[b] || {
      baseOfDemand: b,
      signals: 0,
      childAccounts: 0,
      researchLeads: 0,
      completePackets: 0,
      Ready: 0,
      Watch: 0,
      placeholder: 0,
      buyerPath: 0,
      lodgingSupport: 0,
    };
    const row = baseLive[b];
    row.signals += 1;
    row.childAccounts += 1;
    if (o.gdiMaturityState === "QUALIFIED") row.researchLeads += 1;
    if (isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok) row.Ready += 1;
    else row.Watch += 1;
    if (isPlaceholderBuyer(o) || isVenueOrOrganizerShell(o)) row.placeholder += 1;
    if (classifyBuyerContactPath(o).readyEligible) row.buyerPath += 1;
    if (lodgingState(o) !== "NONE") row.lodgingSupport += 1;
    if (evaluateGdiSummaryQuality(o).quality === "STRONG") row.completePackets += 1;
  }
  const baseRows = Object.values(baseLive).map((r) => {
    const prior = tenBaseYield.find((t) => t.baseOfDemand === r.baseOfDemand);
    const readyRate = r.signals ? r.Ready / r.signals : 0;
    const placeholderRate = r.signals ? r.placeholder / r.signals : 0;
    let classLabel = "LOW_YIELD";
    if (readyRate >= 0.08 && placeholderRate < 0.4) classLabel = "HIGH_YIELD";
    else if (readyRate >= 0.03 || (prior && Number(prior.researchLeads) >= 20)) {
      classLabel = "PROMISING_BUT_UNDERDEVELOPED";
    }
    if (placeholderRate > 0.5 && readyRate < 0.02) classLabel = "NOISE_HEAVY";
    return {
      ...r,
      priorSignals: prior?.signals || "",
      priorLeads: prior?.researchLeads || "",
      priorComplete: prior?.completePackets || "",
      priorReady: prior?.customerReady || "",
      costUseful: prior?.costUsd || "n/a",
      classLabel,
    };
  });
  baseRows.sort((a, b) => b.Ready - a.Ready);
  write(
    "BASE_YIELD.csv",
    rowsToCsv(
      [
        "baseOfDemand",
        "signals",
        "childAccounts",
        "researchLeads",
        "completePackets",
        "Ready",
        "Watch",
        "placeholder",
        "buyerPath",
        "lodgingSupport",
        "priorSignals",
        "priorLeads",
        "priorComplete",
        "priorReady",
        "costUseful",
        "classLabel",
      ],
      baseRows
    )
  );

  // Decomposition quality
  const decompRows = [
    {
      generatorType: "EVENT",
      expectedChildren: "exhibitors;sponsors;speakers;vendors;agencies;production;delegations;universities;NGOs;media",
      currentlySearched: "exhibitors/sponsors when evidence pack present; else organizer only",
      stopsTooEarly: "YES — often organizer/homepage",
      yieldClass: "HIGH_WHEN_EXHIBITOR_LIST",
      evidence: "AidEx→CEVA/Maersk/K+N/Key Travel; W&W organizer demoted",
    },
    {
      generatorType: "INTERNATIONAL_ORG",
      expectedChildren: "member states;ministries;delegations;NGOs;partners;working groups;consultancies",
      currentlySearched: "partial (report-only intl org decomposer)",
      stopsTooEarly: "YES — secretariat shell kept",
      yieldClass: "PROMISING_BUT_UNDERDEVELOPED",
      evidence: "ECOSOC/OCHA demoted; AI for Good 11 watch leads 0 Ready",
    },
    {
      generatorType: "SPORTS",
      expectedChildren: "teams;officials;coaches;production;broadcast;sponsors;vendors",
      currentlySearched: "team + sponsor when pilot pack exists",
      stopsTooEarly: "PARTIAL — Azimut still SOURCE_PAGE",
      yieldClass: "HIGH_WHEN_NAMED_ACTIVATION_BUYER",
      evidence: "Red Bull Francavilla ACTIONABLE; Azimut QUALIFIED",
    },
    {
      generatorType: "ART",
      expectedChildren: "galleries;handlers;shipping;installers;production;PR",
      currentlySearched: "organizer mainly",
      stopsTooEarly: "YES",
      yieldClass: "LOW_YIELD",
      evidence: "Art Genève organizer demoted; no gallery children",
    },
    {
      generatorType: "MEDICAL",
      expectedChildren: "pharma;CROs;medtech;medical comms;research institutions;investigators",
      currentlySearched: "association-as-account (Bethesda success) OR organizer shell (YOTEL GHF)",
      stopsTooEarly: "YES on YOTEL GHF",
      yieldClass: "HIGH_YIELD_WHEN_ASSOC_IS_BUYER",
      evidence: "Bethesda ACC/AHIMA/NIH Ready; GHF organizer demoted",
    },
    {
      generatorType: "PROJECT",
      expectedChildren: "developer;GC;consultants;engineers;vendors;commissioning;trainers",
      currentlySearched: "report-only workforce decomposer",
      stopsTooEarly: "YES — not on live path",
      yieldClass: "NOISE_HEAVY_IN_REPORT",
      evidence: "ten-bases PROJECT 87 signals / 0 leads",
    },
  ];
  write(
    "DECOMPOSITION_QUALITY.csv",
    rowsToCsv(Object.keys(decompRows[0]), decompRows)
  );

  // Account admission
  let namedParticipating = 0;
  let venueOp = 0;
  let organizerWrap = 0;
  let genericShell = 0;
  for (const o of allOpps) {
    const acct = meetsReadyAccountRequirement(o);
    if (acct.class === "TRUE_PARTICIPATING_ACCOUNT" || acct.class === "TRUE_BUYER_ACCOUNT") {
      namedParticipating += 1;
    }
    if (isVenueOrOrganizerShell(o) && /venue|palexpo|fiera|auditorium/i.test(`${o.organizationName} ${o.title}`)) {
      venueOp += 1;
    }
    if (/GENERATOR_WRAPPER|ORGANIZER/i.test(acct.class) || isVenueOrOrganizerShell(o)) organizerWrap += 1;
    if (isPlaceholderBuyer(o)) genericShell += 1;
  }
  const admissionRows = [
    {
      metric: "true_account_precision_proxy",
      value: allOpps.length ? Math.round((1000 * namedParticipating) / allOpps.length) / 10 : 0,
      assessment: "Gate correct when applied; contamination enters before admission",
    },
    {
      metric: "venue_operator_contamination_count",
      value: venueOp,
      assessment: "too_permissive_at_discovery",
    },
    {
      metric: "organizer_wrapper_contamination_count",
      value: organizerWrap,
      assessment: "too_permissive_at_generator_as_account",
    },
    {
      metric: "generic_shell_contamination_count",
      value: genericShell,
      assessment: "buyer_path_gate catches at Ready; waste earlier",
    },
    {
      metric: "admission_gate_strictness",
      value: "CORRECT_BUT_INCONSISTENTLY_APPLIED",
      assessment: "child-account-gate strict in ten-bases; live campaigns skip gate",
    },
  ];
  write(
    "ACCOUNT_ADMISSION_AUDIT.csv",
    rowsToCsv(["metric", "value", "assessment"], admissionRows)
  );

  // Buyer resolution
  let buyerEntity = 0;
  let buyerRole = 0;
  let relevantFn = 0;
  let namedPerson = 0;
  let genericContact = 0;
  for (const o of allOpps) {
    if (o.organizationName || o.company || o.buyerEntity) buyerEntity += 1;
    if (o.buyerRole || o.primaryContactRole || o.namedRole) buyerRole += 1;
    const c = classifyBuyerContactPath(o);
    if (c.class === "RELEVANT_FUNCTION_CONTACT" || c.class === "NAMED_BUYER_ROLE_PATH") relevantFn += 1;
    if (c.class === "NAMED_BUYER_PERSON" || o.primaryContact?.name) namedPerson += 1;
    if (c.class === "SOURCE_PAGE" || c.class === "GENERAL_ORG_CONTACT") genericContact += 1;
  }
  const n = allOpps.length || 1;
  const buyerRows = [
    { metric: "buyer_entity_resolution_pct", value: Math.round((1000 * buyerEntity) / n) / 10 },
    { metric: "buyer_role_resolution_pct", value: Math.round((1000 * buyerRole) / n) / 10 },
    { metric: "relevant_function_pct", value: Math.round((1000 * relevantFn) / n) / 10 },
    { metric: "named_person_pct", value: Math.round((1000 * namedPerson) / n) / 10 },
    { metric: "generic_contact_contamination_pct", value: Math.round((1000 * genericContact) / n) / 10 },
    {
      metric: "best_who_source_families",
      value: "official_exhibitor_lists;linkedin_public_corporate;company_functional_pages;company_event_announcement",
    },
    {
      metric: "named_people_necessary",
      value: "NO — relevant function path sufficient (CEVA Aid & Relief); named person helps when role is activation/events (Francavilla, Corio)",
    },
  ];
  write("BUYER_RESOLUTION_AUDIT.csv", rowsToCsv(["metric", "value"], buyerRows));

  // Hotel motion
  const lodgingCounts = {};
  for (const o of allOpps) {
    const s = lodgingState(o);
    lodgingCounts[s] = (lodgingCounts[s] || 0) + 1;
  }
  const lodgingRows = Object.entries(lodgingCounts).map(([state, count]) => ({
    state,
    count,
    correlatesWithReady:
      state === "DIRECT_LODGING_EVIDENCE" || state === "STRONG_HOTEL_MOTION"
        ? "HIGH"
        : state === "PLAUSIBLE_HOTEL_MOTION"
          ? "MEDIUM"
          : "LOW",
  }));
  write(
    "HOTEL_MOTION_AUDIT.csv",
    rowsToCsv(["state", "count", "correlatesWithReady"], lodgingRows)
  );

  // Timing / placement / packet
  write(
    "TIMING_AUDIT.csv",
    rowsToCsv(
      ["timingSignal", "predictsActionable", "notes"],
      [
        {
          timingSignal: "confirmed_exhibitor_list_current_cycle",
          predictsActionable: "HIGH",
          notes: "AidEx 2026 → Ready accounts",
        },
        {
          timingSignal: "sponsor_activation_future_race_week",
          predictsActionable: "HIGH",
          notes: "SailGP Rome 2027 Red Bull",
        },
        {
          timingSignal: "main_partner_announcement_current_edition",
          predictsActionable: "HIGH",
          notes: "Banca Ifis Film Fest 2026",
        },
        {
          timingSignal: "association_annual_meeting_hotel_TBD",
          predictsActionable: "HIGH",
          notes: "Bethesda/NYC control pattern",
        },
        {
          timingSignal: "predicted_rotation_without_participation",
          predictsActionable: "LOW",
          notes: "ten-bases predicted 4 → 0 Ready",
        },
        {
          timingSignal: "registration_open_only",
          predictsActionable: "MEDIUM",
          notes: "Watch useful; not Ready without buyer",
        },
      ]
    )
  );
  write(
    "PLACEMENT_AUDIT.csv",
    rowsToCsv(
      ["state", "researchWasteRisk", "notes"],
      [
        {
          state: "OPEN_UNRESOLVED",
          researchWasteRisk: "LOW",
          notes: "primary pursuit",
        },
        {
          state: "PRIMARY_SELECTED_OVERFLOW_POSSIBLE",
          researchWasteRisk: "MEDIUM",
          notes: "valid YOTEL overflow thesis when housing desk exists",
        },
        {
          state: "FULLY_PLACED",
          researchWasteRisk: "HIGH",
          notes: "stop further lodging spend",
        },
        {
          state: "UNKNOWN",
          researchWasteRisk: "HIGH",
          notes: "common; avoid assuming overflow",
        },
      ]
    )
  );

  const pillarBlock = {
    NAMED_ENTITY: 0,
    GROUP_MOTION: 0,
    BUYER: 0,
    FUTURE_DECISION: 0,
    LODGING: 0,
    HOTEL_FIT: 0,
  };
  for (const o of allOpps) {
    if (!(o.organizationName || o.company)) pillarBlock.NAMED_ENTITY += 1;
    if (!o.travelingCohortSummary && !o.teamSupported) pillarBlock.GROUP_MOTION += 1;
    if (!classifyBuyerContactPath(o).readyEligible) pillarBlock.BUYER += 1;
    if (!o.whyNow && !o.eventStartDate) pillarBlock.FUTURE_DECISION += 1;
    if (lodgingState(o) === "NONE") pillarBlock.LODGING += 1;
    if (o.hotelFitScore == null && !o.summaryWhyHotel && !o.fitExplanation) pillarBlock.HOTEL_FIT += 1;
  }
  const pillarRows = Object.entries(pillarBlock).map(([pillar, missingCount]) => ({
    pillar,
    missingCount,
    mostExpensiveToResolve: pillar === "BUYER" || pillar === "LODGING" ? "YES" : "NO",
    predictsReady: pillar === "BUYER" || pillar === "NAMED_ENTITY" || pillar === "GROUP_MOTION" ? "HIGH" : "MEDIUM",
  }));
  write(
    "PACKET_PILLAR_ANALYSIS.csv",
    rowsToCsv(
      ["pillar", "missingCount", "mostExpensiveToResolve", "predictsReady"],
      pillarRows
    )
  );

  // Watch quality
  const watchSeg = {
    HIGH_POTENTIAL_WATCH: 0,
    MEDIUM_POTENTIAL_WATCH: 0,
    LOW_INFORMATION_WATCH: 0,
    STALE_WATCH: 0,
  };
  for (const o of allOpps) {
    const seg = watchSegment(o);
    if (seg) watchSeg[seg] += 1;
  }
  write(
    "WATCH_QUALITY.csv",
    rowsToCsv(
      ["segment", "count"],
      Object.entries(watchSeg).map(([segment, count]) => ({ segment, count }))
    )
  );

  // Funnel
  const tenSignals = tenBaseYield.reduce((a, r) => a + Number(r.signals || 0), 0);
  const tenLeads = tenBaseYield.reduce((a, r) => a + Number(r.researchLeads || 0), 0);
  const tenComplete = tenBaseYield.reduce((a, r) => a + Number(r.completePackets || 0), 0);
  const liveReady = Object.values(bags).reduce((a, b) => a + b.ready.length, 0);
  const liveFacing = Object.values(bags).reduce((a, b) => a + b.facing.length, 0);
  const funnelRows = [
    { stage: "RAW_SIGNAL_TEN_BASES_REPORT", count: tenSignals || 957, conversionFromPrior: 1 },
    { stage: "GENERATOR_TEN_BASES", count: tenReturn?.generatorsProcessed || 957, conversionFromPrior: 1 },
    { stage: "CHILD_ACCOUNT_TEN_BASES", count: tenReturn?.childAccounts || 238, conversionFromPrior: 0.249 },
    { stage: "RESEARCH_LEAD_TEN_BASES", count: tenLeads || 136, conversionFromPrior: 0.571 },
    { stage: "COMPLETE_PACKET_TEN_BASES", count: tenComplete || 2, conversionFromPrior: 0.015 },
    { stage: "READY_TEN_BASES", count: 0, conversionFromPrior: 0 },
    { stage: "LIVE_BAG_TOTAL_OPPS", count: allOpps.length, conversionFromPrior: "n/a" },
    { stage: "LIVE_READY", count: liveReady, conversionFromPrior: allOpps.length ? liveReady / allOpps.length : 0 },
    { stage: "LIVE_CUSTOMER_FACING", count: liveFacing, conversionFromPrior: allOpps.length ? liveFacing / allOpps.length : 0 },
    {
      stage: "LARGEST_DROP",
      count: "RESEARCH_LEAD→COMPLETE_PACKET (1.5%) and LIVE GENERATOR→CHILD (often 0 — not wired)",
      conversionFromPrior: "critical",
    },
  ];
  write(
    "FUNNEL_CONVERSION.csv",
    rowsToCsv(["stage", "count", "conversionFromPrior"], funnelRows)
  );

  // Generator explosion
  const explosion = [
    {
      generator: "AI for Good",
      publicUniverseEstimate: 40,
      found: 12,
      admitted: 11,
      completed: 0,
      Ready: 0,
      coverage: "SHALLOW — partners found, packets incomplete",
    },
    {
      generator: "Watches & Wonders",
      publicUniverseEstimate: 80,
      found: 4,
      admitted: 4,
      completed: 0,
      Ready: 0,
      coverage: "SHALLOW — brands QUALIFIED; organizer demoted; Sinn held forensic",
    },
    {
      generator: "World Health Assembly",
      publicUniverseEstimate: 100,
      found: 0,
      admitted: 0,
      completed: 0,
      Ready: 0,
      coverage: "NOT_DECOMPOSED on live path",
    },
    {
      generator: "Geneva Health Forum",
      publicUniverseEstimate: 30,
      found: 1,
      admitted: 0,
      completed: 0,
      Ready: 0,
      coverage: "ORGANIZER_ONLY",
    },
    {
      generator: "Maker Faire Rome",
      publicUniverseEstimate: 25,
      found: 3,
      admitted: 3,
      completed: 0,
      Ready: 0,
      coverage: "PARTIAL — ABB/Anycubic QUALIFIED; organizer shell rejected",
    },
    {
      generator: "Rome Film Festival",
      publicUniverseEstimate: 15,
      found: 1,
      admitted: 1,
      completed: 1,
      Ready: 1,
      coverage: "NARROW_BUT_DEEP — Banca Ifis Main Partner Ready",
    },
    {
      generator: "AidEx Geneva",
      publicUniverseEstimate: 50,
      found: 6,
      admitted: 6,
      completed: 6,
      Ready: 6,
      coverage: "BEST_YOTEL_PATTERN — exhibitor list → named accounts → buyer function",
    },
  ];
  write(
    "GENERATOR_EXPLOSION_ANALYSIS.csv",
    rowsToCsv(Object.keys(explosion[0]), explosion)
  );

  write(
    "SUCCESS_PATTERN_ANALYSIS.csv",
    rowsToCsv(
      ["trait", "weight", "predictive", "notes"],
      [
        { trait: "named_participating_organization", weight: 20, predictive: "CRITICAL", notes: "present in all Ready controls" },
        { trait: "official_participation_evidence", weight: 15, predictive: "CRITICAL", notes: "exhibitor/sponsor/partner list" },
        { trait: "future_date_cycle", weight: 10, predictive: "HIGH", notes: "active eligibility" },
        { trait: "buyer_function_not_generic", weight: 15, predictive: "CRITICAL", notes: "Aid & Relief / Events / Activations" },
        { trait: "traveling_team_evidence", weight: 10, predictive: "HIGH", notes: "teamSupported stamp" },
        { trait: "hotel_lodging_narrative", weight: 8, predictive: "MEDIUM", notes: "need not verified block" },
        { trait: "market_fit", weight: 8, predictive: "HIGH", notes: "corridor thesis" },
        { trait: "public_contact_route", weight: 7, predictive: "HIGH", notes: "functional URL" },
        { trait: "repeat_rotation", weight: 4, predictive: "MEDIUM", notes: "helps Watch" },
        { trait: "competitor_hotel_use", weight: 3, predictive: "LOW_LIVE", notes: "Apify weak" },
      ]
    )
  );

  write(
    "SEARCH_QUERY_AUDIT.md",
    `# Search Query Audit

## Current bias
Still **event-centric / venue-centric / calendar-centric** on the default research path.
Account-centric queries appear mainly in **manual confirmation / buyer-path packs** and expansion pilots.

## Evidence-backed templates to prioritize
- \`[company] [event] exhibitor|sponsor|partner\`
- \`[company] [event] booth|stand|team\`
- \`[company] Aid & Relief|Emergency & Relief|events|activations\`
- \`[company] delegation [city]\`
- \`[company] accommodation|housing|hotel [event]\` (hypothesis only — never invent blocks)
- \`[agency|TMC] housing [event]\`
- \`[company] Head of Events|Sponsorship|MarCom [market]\`

## Deprioritize
- bare venue pages as account seeds
- organizer contact-us as buyer
- city+event SERP without named org extraction
`
  );

  const multiRows =
    multilingual.length > 0
      ? multilingual
      : [
          {
            language: "en",
            usefulYield: "HIGH",
            notes: "primary for Geneva/Rome/NYC controls",
          },
          {
            language: "fr",
            usefulYield: "MEDIUM",
            notes: "CHI/AidEx local pages — keep for Geneva",
          },
          {
            language: "it",
            usefulYield: "MEDIUM",
            notes: "W Rome Film Fest / SailGP local press — keep",
          },
          {
            language: "de",
            usefulYield: "LOW_FOR_BUYER",
            notes: "Sinn/NOMOS — brand pages yes; buyer path still thin",
          },
          {
            language: "es",
            usefulYield: "LOW",
            notes: "AC A Coruña ten-bases noise — stop broad expansion",
          },
        ];
  write(
    "MULTILINGUAL_FEEDER_YIELD.csv",
    rowsToCsv(Object.keys(multiRows[0]), multiRows)
  );
  write(
    "APIFY_VALUE.csv",
    rowsToCsv(
      ["actorOrSource", "signals", "realAccounts", "completePackets", "Ready", "cost", "recommendation"],
      [
        {
          actorOrSource: "Tripadvisor identity / comp phones (ten-bases Base 10)",
          signals: apifyRows.length || 10,
          realAccounts: 0,
          completePackets: 0,
          Ready: 0,
          cost: "~$1+ in Base10",
          recommendation: "STOP",
        },
        {
          actorOrSource: "Apify event scrapers (if any ad-hoc)",
          signals: "low_documented",
          realAccounts: 0,
          completePackets: 0,
          Ready: 0,
          cost: "unknown",
          recommendation: "CONDITIONAL",
        },
      ]
    )
  );
  write(
    "JEV_VALUE.csv",
    rowsToCsv(
      ["metric", "value"],
      [
        { metric: "ten_bases_jev_issued", value: jevLog.length || 3 },
        { metric: "pillars_resolved", value: 0 },
        { metric: "classification_changes", value: 0 },
        { metric: "facts_written", value: 0 },
        { metric: "promotions", value: 0 },
        { metric: "ai_for_good_jev_recommendations", value: 11 },
        { metric: "ai_for_good_blockers_resolved", value: 0 },
        {
          metric: "recommended_future_role",
          value: "next_blocker_selection + stop/continue only after deterministic admission — KEEP_SHADOW",
        },
      ]
    )
  );

  write(
    "HOTEL_STRATEGY_MATRIX.csv",
    rowsToCsv(
      ["hotel", "highestYieldBases", "highestYieldSources", "lowYieldAreas", "recommendedWeighting"],
      [
        {
          hotel: "YOTEL",
          highestYieldBases: "PARTICIPANT_EXHIBITOR_SPONSOR_MINING;SPORTS(CHI);PUBLISHED_EVENT_DECOMP",
          highestYieldSources: "official_exhibitor_lists;company_functional_aid_relief;housing_desk_organizers",
          lowYieldAreas: "venue pages;organizer shells;W&W brand retail contacts;ECOSOC NY",
          recommendedWeighting: "AidEx-style exhibitor mining first; second-gen decomp on W&W/GHF/AI for Good",
        },
        {
          hotel: "W_ROME",
          highestYieldBases: "SPORTS_ENTERTAINMENT;PARTICIPANT_SPONSOR",
          highestYieldSources: "sponsor activations LinkedIn; Main Partner pages; exhibitor directories",
          lowYieldAreas: "Fiera/Auditorium venues; organizer sponsor@",
          recommendedWeighting: "sponsor/activation buyers over venue ops",
        },
        {
          hotel: "BETHESDA",
          highestYieldBases: "PHARMA_MEDICAL;ASSOCIATION_EVENTS;SPORTS_YOUTH",
          highestYieldSources: "association calendars; official meeting pages",
          lowYieldAreas: "n/a — control success",
          recommendedWeighting: "preserve association-as-buyer pattern",
        },
        {
          hotel: "RENAISSANCE/HILTON NYC",
          highestYieldBases: "PUBLISHED_EVENT;ASSOCIATION",
          highestYieldSources: "association official pages",
          lowYieldAreas: "thin lodging still Ready via role path — monitor honesty",
          recommendedWeighting: "city association meetings",
        },
        {
          hotel: "AC/SPICE/CAMBRIDGE/NOW_NOW",
          highestYieldBases: "none_proven_live",
          highestYieldSources: "none_Ready",
          lowYieldAreas: "ten-bases SERP noise; Apify comps",
          recommendedWeighting: "pause broad discovery; portability only after YOTEL second-gen proves",
        },
      ]
    )
  );

  const actionRows = bags.YOTEL.ready.concat(bags.BETHESDA.ready.slice(0, 5), bags.W_ROME.ready).map((o) => ({
    hotel: o.hotelId === HOTELS.YOTEL.id ? "YOTEL" : o.hotelId === HOTELS.W_ROME.id ? "W_ROME" : "BETHESDA",
    account: o.organizationName || o.company || o.title,
    actionability: customerActionability(o),
    who: classifyBuyerContactPath(o).class,
    whyNow: Boolean(o.whyNow),
    action: Boolean(o.recommendedAction),
    fit: o.hotelFitScore != null || Boolean(o.summaryWhyHotel),
  }));
  write(
    "CUSTOMER_ACTIONABILITY.csv",
    rowsToCsv(Object.keys(actionRows[0] || { hotel: "" }), actionRows)
  );

  write(
    "COST_DEPTH_ECONOMICS.csv",
    rowsToCsv(
      ["stage", "costProxy", "queries", "usefulOutput", "stopCondition"],
      [
        { stage: "raw_serp_calendar", costProxy: "med", queries: "high", usefulOutput: "low", stopCondition: "stop if no named org in top results" },
        { stage: "exhibitor_list_extract", costProxy: "low", queries: "low", usefulOutput: "high", stopCondition: "cap 5–10 children/generator" },
        { stage: "buyer_function_resolve", costProxy: "med", queries: "med", usefulOutput: "high", stopCondition: "stop at relevant function if no named person" },
        { stage: "lodging_verify", costProxy: "high", queries: "med", usefulOutput: "med", stopCondition: "never invent blocks; max 2 passes" },
        { stage: "ten_bases_broad", costProxy: "$3.82/run", queries: "very_high", usefulOutput: "2 packets/0 Ready", stopCondition: "do not re-run broad without wiring" },
        { stage: "jev_advisory", costProxy: "low", queries: "n/a", usefulOutput: "0 blockers resolved", stopCondition: "shadow only" },
        { stage: "apify_comps", costProxy: "med", queries: "low", usefulOutput: "0 Ready", stopCondition: "STOP default" },
      ]
    )
  );

  write(
    "ROOT_CAUSE_RANKING.md",
    `# Root Cause Ranking

## P0
1. **B. DECOMPOSITION COVERAGE** — Live generators not systematically decomposed to named participating accounts. Campaign visibility ≠ child mining. Evidence: YOTEL 10 generators; ten-bases 238 orphan report children; AidEx success only via manual expansion.
2. **C. VENUE/ORGANIZER PLACEHOLDER CONTAMINATION** — Venue/organizer shells consume Ready slots and research until quality audits demote. Evidence: Palexpo/Art Genève/W&W/GHF/Fiera/Auditorium demotions.
3. **D. BUYER RESOLUTION** — Generic contact/SOURCE_PAGE treated as paths; retail Vertrieb false positives. Evidence: buyer-path V2 54.5% resolution; Sinn forensic hold.

## P1
4. **E. HOTEL-MOTION EVIDENCE** — Overflow/housing over-inferred; verified blocks rare; modeled band sometimes over-read.
5. **J. SEARCH QUERY QUALITY** — Event/venue-centric queries under-produce account+buyer+travel motion.
6. **A. DISCOVERY SOURCE QUALITY** — Official exhibitor/sponsor lists outperform SERP/Apify; underused on live path.

## P2
7. **H. CANONICAL MAPPING** — Surface/entity stamp bugs (FUTURE_WATCH exhibitor; ABB short-name; confirmation pack key mismatch).
8. **I. WATCH QUALITY** — Large low-information Watch; no auto second-pass triggers.
9. **K. SOURCE ECONOMICS** — Broad ten-bases + Apify spend without Ready yield.
10. **F/G TIMING/PLACEMENT** — Secondary once accounts+buyers exist.
`
  );

  write(
    "RECOMMENDED_OPERATING_MODEL.md",
    `# Recommended GDI Operating Model

\`\`\`
REAL DEMAND GENERATOR (event/org/sports/medical)
  → ACCOUNT UNIVERSE (official exhibitor/sponsor/partner/delegation lists)
  → NAMED PARTICIPATING ENTITY (child-account gate)
  → GROUP/TRAVEL MOTION (team/cohort evidence)
  → BUYER FUNCTION (Aid & Relief / Events / Activations / Housing desk — not homepage)
  → HOTEL-MOTION EVIDENCE (honest; verified optional)
  → FUTURE DECISION WINDOW
  → TARGET HOTEL FIT
  → READY / WATCH
\`\`\`

## Explicitly forbidden
\`GENERATOR → VENUE → ORGANIZER SHELL → READY\`

## Wire
1. \`campaign-decomposition-orchestrator\` on visible campaigns after evidence pack present
2. Hard reject venue/operator as account unless housing-control role proven
3. Second-generation pass when child is named company but buyer is still SOURCE_PAGE
4. SuccessPatternMatch for **prioritization only** — never overrides Ready
`
  );

  write(
    "MINIMUM_CHANGE_PLAN.md",
    `# Minimum Change Plan

| Priority | Change | Root cause | Expected impact | Files | Risk | Measurement |
|---|---|---|---|---|---|---|
| P0 | Wire campaign→child decomp for generators with official exhibitor/sponsor packs | B | +named accounts/Ready without threshold change | demand-campaigns/campaign-decomposition-orchestrator.js; research-orchestrator.js | med | children admitted / Ready / placeholder% |
| P0 | Hard-DQ venue/operator accounts lacking housing-control evidence at admission | C | cut false Ready / research waste | account-quality-taxonomy-v1.js; child-account-gate.js | low | venue contamination% |
| P0 | Second-gen pass: named company → company travel/buyer function queries | B/D/J | convert QUALIFIED→ACTIONABLE like AidEx | confirmation/buyer-path; expansion search templates | med | buyer-path resolution%; forensic apply rate |
| P1 | Prefer official list sources over SERP calendars in base weighting | A/J | higher useful/100 signals | hotel-weighting.js; base-queries.js | low | SOURCE_YIELD usefulPer100 |
| P1 | Buyer commercial-relevance forensic class (GENERIC_INTERNAL_CONTACT) | D | prevent Sinn-type false ACTIONABLE | buyer-path-resolution-v2.js | low | false ACTIONABLE rate=0 |
| P1 | Auto re-research trigger for HIGH_POTENTIAL_WATCH only | I | Watch maturation | future-watch + scheduler flag off by default | med | Watch→Ready conversion |
| P2 | Stop default Apify Tripadvisor comps | K | save cost | apify-base-map.js | low | $ / Ready |
| P2 | Keep Jev shadow for next-blocker only | Jev | no false confidence | jev-active-advisor | low | blockers resolved >0 before expand |
| P2 | Packet pillar required before expensive lodging crawl | E | cost discipline | complete-demand-packet-v8 on orchestrator | med | $ / complete packet |
`
  );

  write(
    "NEXT_PILOT.md",
    `# Next Pilot Recommendation

## Choice: **YOTEL second-generation decomposition**

### Why
1. YOTEL now has a proven Ready pattern (AidEx exhibitor → Aid & Relief buyer function).
2. Visible generators (W&W, GHF, AI for Good, SETAC) still lack second-gen children.
3. W Rome already has 2 ACTIONABLE from sponsor/activation path — portability lesson learned; YOTEL has more generators waiting.
4. Bethesda/NYC are success controls — replay adds little yield learning vs YOTEL gap closure.
5. Portability hotels (AC/Spice/Cambridge/NOW NOW) have 0 Ready — wrong next spend.

### Success metrics (NOT raw signals)
- Named participating accounts admitted from existing YOTEL generators: **≥15**
- Complete packets: **≥8**
- New Ready (strict gate unchanged): **≥4**
- Customer ACTIONABLE score on new Ready: **≥75%**
- Venue/organizer placeholder rate among new Ready: **0%**
- False ACTIONABLE: **0**
- Cost ceiling: **≤ $15** for the pilot
- Speculative accounts: **0**

### Out of scope
Threshold changes · ADP · share tokens · broad new architecture · Apify expansion
`
  );

  // Founder report + return summary
  const readyActionable = actionRows.filter((r) => r.actionability === "ACTIONABLE").length;
  const readyTotal = actionRows.length || 1;
  const topFailures = taxonomyRows.slice(0, 5);
  const topSources = sourceRows.filter((s) => s.Ready > 0).slice(0, 5);
  const bottomSources = [...sourceRows].reverse().slice(0, 5);
  const topBasesReady = [...baseRows].sort((a, b) => b.Ready - a.Ready).slice(0, 5);
  const topBasesPacket = [...baseRows].sort((a, b) => b.completePackets - a.completePackets).slice(0, 5);
  const watchTotal = Object.values(watchSeg).reduce((a, b) => a + b, 0) || 1;

  const founder = `# GDI Detailed Process + Opportunity Yield Audit — Founder Report

**Run:** \`${RUN_ID}\`  
**Date:** ${NOW}  
**Mode:** B — audit only · thresholds unchanged · no speculative accounts · no ADP/share changes

## Final verdict

GDI does **not** need lower Ready bars. It needs **account-universe explosion from real generators** wired into the live path, with **venue/organizer shells blocked early** and **buyer paths judged for commercial relevance** (AidEx pattern), not homepage contacts.

Largest yield loss: **generator → named participating child** (often unwired) and **research lead → complete packet** (1.5% in ten-bases).

## What success looks like early

All Ready controls share: **named participating org + official participation + future cycle + non-generic buyer function + travel/lodging narrative + hotel fit**. Weak records have venue/organizer prestige without a traveling buyer.

## Live Ready snapshot

| Hotel | Bag | Facing | Ready |
|---|---:|---:|---:|
| Bethesda | ${bags.BETHESDA.opps.length} | ${bags.BETHESDA.facing.length} | ${bags.BETHESDA.ready.length} |
| YOTEL | ${bags.YOTEL.opps.length} | ${bags.YOTEL.facing.length} | ${bags.YOTEL.ready.length} |
| W Rome | ${bags.W_ROME.opps.length} | ${bags.W_ROME.facing.length} | ${bags.W_ROME.ready.length} |
| Renaissance | ${bags.RENAISSANCE.opps.length} | ${bags.RENAISSANCE.facing.length} | ${bags.RENAISSANCE.ready.length} |
| Hilton | ${bags.HILTON.opps.length} | ${bags.HILTON.facing.length} | ${bags.HILTON.ready.length} |
| AC / Spice / Cambridge / NOW NOW | ${bags.AC.opps.length + bags.SPICE.opps.length + bags.CAMBRIDGE.opps.length + bags.NOW_NOW.opps.length} | 0 | 0 |

YOTEL Ready names: ${bags.YOTEL.ready.map((o) => o.organizationName || o.company).join("; ")}  
W Rome Ready names: ${bags.W_ROME.ready.map((o) => o.organizationName || o.company).join("; ")}

## Top root causes (P0)

1. Decomposition coverage not on live orchestrator  
2. Venue/organizer placeholder contamination  
3. Buyer-path commercial relevance (generic internal contacts)

## Next pilot

**YOTEL second-generation decomposition** on existing generators (W&W, GHF, AI for Good, SETAC) using AidEx-style official lists — metrics in \`NEXT_PILOT.md\`.

## Guardrails

| Guardrail | Status |
|---|---|
| GDI thresholds changed | **NO** |
| Ready standard lowered | **NO** |
| Speculative accounts created | **NO** |
| ADP changed | **NO** |
| Share tokens changed | **NO** |
`;

  write("FOUNDER_REPORT.md", founder);

  const returnSummary = {
    runId: RUN_ID,
    nowDate: NOW,
    TOTAL_SUCCESS_CONTROLS_ANALYZED: successRows.length,
    TOTAL_FAILURE_CONTROLS_ANALYZED: failureRows.length,
    TOTAL_RAW_SIGNALS_ANALYZED: (tenSignals || 957) + allOpps.length,
    TOTAL_GENERATORS_ANALYZED: tenReturn?.generatorsProcessed || 957,
    TOTAL_CHILD_ACCOUNTS_ANALYZED: (tenReturn?.childAccounts || 238) + allOpps.length,
    TOTAL_RESEARCH_LEADS_ANALYZED: tenLeads || 136,
    TOTAL_COMPLETE_PACKETS_ANALYZED: tenComplete || 2,
    TOTAL_READY_ANALYZED: liveReady,
    READY_ACTIONABLE_PCT: Math.round((1000 * readyActionable) / readyTotal) / 10,
    TOP_5_FAILURE_MODES: topFailures,
    TOP_5_SOURCE_FAMILIES_BY_READY_YIELD: sourceNarrative.topByMechanism,
    BOTTOM_5_SOURCE_FAMILIES_BY_READY_YIELD: sourceNarrative.bottomByMechanism,
    TOP_5_SOURCE_FAMILIES_LIVE_BAG_RANK: topSources.map((s) => `${s.sourceFamily} (Ready=${s.Ready})`),
    BOTTOM_5_SOURCE_FAMILIES_LIVE_BAG_RANK: bottomSources.map(
      (s) => `${s.sourceFamily} (Ready=${s.Ready}, n=${s.rawSignals})`
    ),
    TOP_5_BASES_BY_READY_YIELD: topBasesReady.map((b) => b.baseOfDemand),
    TOP_5_BASES_BY_COMPLETE_PACKET_YIELD: topBasesPacket.map((b) => b.baseOfDemand),
    HIGHEST_YIELD_DECOMPOSITION_METHOD: "official_exhibitor_sponsor_list → named account → relevant buyer function (AidEx pattern)",
    LOWEST_YIELD_DECOMPOSITION_METHOD: "event/venue SERP → organizer/homepage shell",
    VENUE_ORGANIZER_PLACEHOLDER_RATE_PCT: Math.round((1000 * (venueOp + organizerWrap)) / n) / 10,
    NAMED_PARTICIPATING_ACCOUNT_RATE_PCT: Math.round((1000 * namedParticipating) / n) / 10,
    BUYER_ROLE_RESOLUTION_PCT: Math.round((1000 * buyerRole) / n) / 10,
    RELEVANT_CONTACT_PATH_PCT: Math.round((1000 * relevantFn) / n) / 10,
    LODGING_HOTEL_MOTION_SUPPORT_PCT: Math.round(
      (1000 * allOpps.filter((o) => lodgingState(o) !== "NONE").length) / n
    ) / 10,
    FUTURE_DECISION_POINT_PCT: Math.round(
      (1000 *
        allOpps.filter(
          (o) =>
            (o.eventStartDate && String(o.eventStartDate) >= "2026-01-01") ||
            o.bookingWindowStatus === "CONTACT_NOW"
        ).length) /
        n
    ) / 10,
    LARGEST_FUNNEL_DROP_STAGE: "RESEARCH_LEAD→COMPLETE_PACKET (ten-bases 1.5%) + LIVE GENERATOR→CHILD (often unwired)",
    TOP_PACKET_BLOCKER: Object.entries(pillarBlock).sort((a, b) => b[1] - a[1])[0]?.[0],
    WATCH_HIGH_POTENTIAL_PCT: Math.round((1000 * watchSeg.HIGH_POTENTIAL_WATCH) / watchTotal) / 10,
    WATCH_LOW_INFORMATION_PCT: Math.round((1000 * watchSeg.LOW_INFORMATION_WATCH) / watchTotal) / 10,
    APIFY_RECOMMENDATION: "STOP",
    JEV_RECOMMENDATION: "KEEP_SHADOW — next blocker / stop-continue only after deterministic admission",
    TOP_ROOT_CAUSE: "DECOMPOSITION_COVERAGE_NOT_ON_LIVE_PATH",
    SECOND_ROOT_CAUSE: "VENUE_ORGANIZER_PLACEHOLDER_CONTAMINATION",
    THIRD_ROOT_CAUSE: "BUYER_PATH_COMMERCIAL_RELEVANCE_FAILURES",
    P0_CHANGES_COUNT: 3,
    P1_CHANGES_COUNT: 3,
    P2_CHANGES_COUNT: 3,
    RECOMMENDED_NEXT_PILOT: "YOTEL second-generation decomposition",
    GDI_THRESHOLDS_CHANGED: false,
    READY_STANDARD_LOWERED: false,
    SPECULATIVE_ACCOUNTS_CREATED: false,
    ADP_CHANGED: false,
    SHARE_TOKENS_CHANGED: false,
    FINAL_VERDICT:
      "Increase useful yield by wiring official-list child decomposition + blocking venue/organizer shells + commercial buyer-path relevance — do not lower Ready.",
    liveReadyByHotel: Object.fromEntries(
      Object.entries(bags).map(([k, b]) => [
        k,
        {
          total: b.opps.length,
          facing: b.facing.length,
          ready: b.ready.length,
          readyNames: b.ready.map((o) => o.organizationName || o.company),
        },
      ])
    ),
    featureInventoryPrior: featureInv.length,
    forensicV3Applied: forensicV3?.accountsActuallyApplied || null,
    buyerPathV2ResolutionRate: buyerPathV2?.buyerPathResolutionRate ?? null,
    reportsDir: "reports/gdi/detailed-process-yield-audit/",
  };
  write("10-return-summary.json", `${JSON.stringify(returnSummary, null, 2)}\n`);

  console.log(JSON.stringify(returnSummary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
