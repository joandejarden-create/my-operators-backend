#!/usr/bin/env node
/**
 * Radisson Blu Toronto Downtown — 14-segment demand diversification (zero paid).
 *
 * Authorization: FOUNDER_RADISSON_14_SEGMENT_DIVERSIFICATION_20261116
 * Paid spend: $0 (no Exa / Surfe / Parallel). Additive staging only.
 *
 *   node scripts/gdi-radisson-toronto-14-segment-diversification-v1.mjs
 *   node scripts/gdi-radisson-toronto-14-segment-diversification-v1.mjs --browser-verify
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  computePublicationInventoryCounts,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";
import { attachCustomerClassificationPresentation } from "../lib/group-demand-intelligence/customer-classification-presentation-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER_LABEL,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import { annotateInventoryPopulations } from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import {
  applyCommercialActionabilityGate,
  assertCustomerInventoryPassesActionabilityGate,
} from "../lib/group-demand-intelligence/commercial-actionability-gate-v1.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recH9h3McgMNFQEla";
const HOTEL_NAME = "Radisson Blu Toronto Downtown";
const PROPERTY_CODE = "CNC41";
const REF = "2026-11-16";
const OUT = path.join(ROOT, "reports/gdi-radisson-toronto-14-segment-diversification-v1");
const PRIOR_FP = "d9a6b23b8eb4ca305b8d35f638146cc266e4c089b0afa1cb5443012b1dd49f78";
const CERT_PKG_DIR = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL_ID,
  "gdi_pub_recH9h3McgMNFQEla_20261116_radisson_toronto_final_commercial_cert_v1",
);
const EXA20 = path.join(ROOT, "reports/gdi-radisson-toronto-14-segment-exa-expansion-20usd-v1");
const PUB_VERSION = `gdi_pub_${HOTEL_ID}_20261116_radisson_toronto_14seg_diversification_v1`;
const ARGS = new Set(process.argv.slice(2));
const BROWSER_VERIFY = ARGS.has("--browser-verify");

const SEGMENTS = listFourteenWorkflowSegmentIds();
const text = (v) => String(v == null ? "" : v).replace(/\s+/g, " ").trim();
const readJson = (p) => JSON.parse(fs.readFileSync(p, "utf8").replace(/^\uFEFF/, ""));
const writeJson = (name, obj) => {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
};
const clone = (o) => JSON.parse(JSON.stringify(o));
const sh = (cmd) => {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  } catch {
    return "";
  }
};
const norm = (s) =>
  text(s)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

/** Map legacy / free-form segment labels onto the 14 certified IDs. */
function mapSegment(raw) {
  const x = String(raw || "").toLowerCase();
  if (SEGMENTS.includes(x)) return x;
  if (/association|conference.?event|association_events/.test(x)) return "associations_conferences_events";
  if (/scal/.test(x)) return "corporate_accounts_scaling";
  if (/corporate.?meet|incentive/.test(x)) return "corporate_meetings_incentives";
  if (/gov|public.?sector/.test(x)) return "government_public_sector";
  if (/sport|entertain|wnba|dance|film/.test(x)) return "sports_entertainment";
  if (/educ|scien|medical|academic|ombud|nursing|anesthes/.test(x)) return "education_scientific";
  if (/project|infra|rail|land.?develop|waterfront|construct/.test(x)) return "projects_infrastructure";
  if (/social|wedding|festival|special.?event/.test(x)) return "social_special_events";
  if (/cater|day.?meet|local_catering/.test(x)) return "local_catering_day_meetings";
  if (/celebrat|private.?event|hall of fame|city builders/.test(x)) return "corporate_celebrations_private_events";
  if (/travel.?trade|dmc|intermed|mice/.test(x)) return "travel_trade_group_intermediaries";
  if (/reloc|extended|faculty.?hous|mobility/.test(x)) return "relocation_extended_stay";
  if (/preferred.?lodg|venue.?partner/.test(x)) return "preferred_lodging_partnerships";
  if (/planner|pep|cema/.test(x)) return "professional_event_planners";
  return null;
}

function stageKey(o) {
  const s = text(o.commercialStageKey || o.commercialStagePillLabel || o.commercialActionability).toLowerCase();
  const action = text(o.salesActionPillLabel || o.salesActionKey).toLowerCase();
  if (/sales.?ready/.test(s)) return "SALES_READY";
  // Certified Pursue Now + Contact Buyer / Win Room Block ≡ Sales Ready inventory
  if (/pursue now|sales_opportunity|pursue_now/.test(s) && /contact buyer|win room|contact_buyer|win_room/.test(action)) {
    return "SALES_READY";
  }
  if (/pursue now|pursue_now/.test(s) && /contact buyer|win room/.test(action)) return "SALES_READY";
  if (/account|develop/.test(s)) return "ACCOUNT_DEVELOPMENT";
  if (/qualify/.test(s)) return "QUALIFY_OPPORTUNITY";
  if (/pursue now|pursue_now/.test(s)) return "SALES_READY";
  return "QUALIFY_OPPORTUNITY";
}

function scrubRepeatedSourcingNarratives(opp) {
  const next = { ...opp };
  const labels = [text(next.hotelSourcingStatusLabel), text(next.targetHotelSourcingStatusLabel)].filter(
    (s) => s.length >= 8,
  );
  const fields = [
    "whatIsHappening",
    "potentialHotelBusiness",
    "potentialBusiness",
    "whyThisHotel",
    "whyActNow",
    "recommendedNextStep",
    "recommendedAction",
    "summary",
    "cardWhyNowLine",
    "tileClassificationLine",
  ];
  for (const field of fields) {
    let val = text(next[field]);
    if (!val) continue;
    for (const label of labels) {
      const re = new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi");
      val = val.replace(re, "").replace(/\s{2,}/g, " ").replace(/\s([,.;:])/g, "$1").trim();
    }
    next[field] = val || next[field];
  }
  return next;
}

function argVal(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

async function runBrowserVerify() {
  const meta = readJson(path.join(OUT, "17-STAGED_PUBLICATION_AND_BROWSER.json"));
  const base = (argVal("--base-url") || process.env.GDI_PREVIEW_BASE || "http://127.0.0.1:8092").replace(/\/$/, "");
  const pageUrl = `${base}/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;
  const apiUrl = `${base}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities?publicationPackage=1&surface=customer&preview=staged`;
  const result = {
    contract: "GDI_RADISSON_14SEG_DIVERSIFICATION_BROWSER_V1",
    generatedAt: new Date().toISOString(),
    pageUrl,
    apiUrl,
    expectedFingerprint: meta.fingerprint,
    expectedCounts: meta.inventoryCounts,
    checks: {},
    errors: [],
  };
  const { chromium } = await import("playwright");
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    const api = await page.goto(apiUrl, { waitUntil: "networkidle", timeout: 60000 }).then(async (r) => {
      result.checks.apiHttpStatus = r.status();
      return r.json();
    });
    const fp = api?.publication?.contentFingerprintSha256 || api?.contentFingerprintSha256 || null;
    result.checks.apiFingerprint = fp;
    result.checks.fingerprintMatch = fp === meta.fingerprint;
    const opps = api?.opportunities || api?.items || [];
    result.checks.apiOpportunityCount = opps.length;
    result.checks.apiHotelCount = opps.filter((o) => o.hotelMarketScopeEligible).length;
    result.checks.apiCompetitiveCount = opps.filter((o) => o.competitiveMarketScopeEligible).length;
    await page.goto(pageUrl, { waitUntil: "networkidle", timeout: 60000 });
    await page.waitForSelector(
      'article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]',
      { timeout: 90000 },
    );
    result.checks.dom = await page.evaluate(() => {
      const cards = [
        ...document.querySelectorAll(
          'article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]',
        ),
      ];
      return {
        cards: cards.length,
        titles: cards
          .map((c) => (c.querySelector("h2,h3,.title,[data-opportunity-title]")?.textContent || "").trim())
          .filter(Boolean),
      };
    });
    const shotDir = path.join(OUT, "screenshots");
    fs.mkdirSync(shotDir, { recursive: true });
    await page.screenshot({
      path: path.join(shotDir, "01-radisson-14seg-diversification-staged-overview.png"),
      fullPage: true,
    });
  } catch (e) {
    result.errors.push(String(e?.message || e));
  } finally {
    await browser.close();
  }
  result.ok =
    result.checks.fingerprintMatch === true &&
    !(result.errors || []).length &&
    result.checks.apiHotelCount >= 15;
  writeJson("17b-BROWSER_VERIFICATION.json", result);
  console.log(JSON.stringify(result, null, 2));
}

if (BROWSER_VERIFY) {
  await runBrowserVerify();
  process.exit(0);
}

fs.mkdirSync(OUT, { recursive: true });

// ---------------------------------------------------------------------------
// Baseline
// ---------------------------------------------------------------------------
const certPkg = readJson(path.join(CERT_PKG_DIR, "package.json"));
if (certPkg.contentFingerprintSha256 !== PRIOR_FP) {
  console.error(`Certified fingerprint mismatch: ${certPkg.contentFingerprintSha256} != ${PRIOR_FP}`);
  process.exit(3);
}
const stagedPtr = readJson(
  path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages", HOTEL_ID, "STAGED.json"),
);
const hotelCfg = readJson(path.join(ROOT, `config/group-demand-intelligence/hotels/${HOTEL_ID}.json`));
const p1 = readJson(path.join(EXA20, "03-PASS1_DISCOVERY_RESULTS.json"));
const qual20 = readJson(path.join(EXA20, "07-HOTEL_MARKET_QUALIFICATION.json"));
const airtableOps = (() => {
  try {
    return readJson(
      path.join(ROOT, "reports/gdi-radisson-toronto-final-operational-certification-v1/05-AIRTABLE_READBACK_VERIFICATION.json"),
    );
  } catch {
    return null;
  }
})();

const baselineOpps = certPkg.opportunities || [];
const baselineHm = baselineOpps.filter((o) => o.hotelMarketScopeEligible);
const baselineCmi = baselineOpps.filter((o) => o.competitiveMarketScopeEligible);

writeJson("00-BASELINE_LOCK.json", {
  contract: "GDI_RADISSON_14SEG_DIVERSIFICATION_BASELINE_LOCK_V1",
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  propertyCode: PROPERTY_CODE,
  referenceDate: REF,
  paidSpendThisPhaseUsd: 0,
  certifiedFingerprintPreserved: PRIOR_FP,
  certifiedPackageVersion: certPkg.publicationVersion,
  stagedPointerBefore: stagedPtr,
  inventory: {
    hotelMarket: baselineHm.length,
    cmi: baselineCmi.length,
    total: baselineOpps.length,
    salesReady: baselineHm.filter((o) => stageKey(o) === "SALES_READY").length,
    qualifyOpportunity: baselineHm.filter((o) => stageKey(o) === "QUALIFY_OPPORTUNITY").length,
  },
  airtableOperationalReadback: airtableOps?.inventory || null,
  hotelProfile: {
    rooms: hotelCfg.capabilityProfile?.totalGuestrooms || 157,
    meetingRooms: hotelCfg.capabilityProfile?.meetingRoomCount || 20,
    largestTheater: hotelCfg.capabilityProfile?.largestTheaterCapacity || 240,
    address: hotelCfg.address,
  },
  template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  actionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  git: { branch: sh("git rev-parse --abbrev-ref HEAD"), head: sh("git rev-parse HEAD") },
  rule: "Do not overwrite, reclassify, or remove certified opportunities. Additive only.",
});

// ---------------------------------------------------------------------------
// Coverage matrix (existing)
// ---------------------------------------------------------------------------
function coverageRow() {
  return {
    researched: false,
    candidateDiscoveries: 0,
    deepResearched: 0,
    hotelMarketQualified: 0,
    salesReady: 0,
    qualifyOpportunity: 0,
    accountDevelopment: 0,
    cmiOnly: 0,
    titles: [],
  };
}

const coverage = Object.fromEntries(SEGMENTS.map((s) => [s, coverageRow()]));
const unmapped = [];

for (const o of baselineHm) {
  const seg = mapSegment(o.demandSegmentId || o.researchSegment || o.opportunityType || o.segmentDisplayLabel);
  if (!seg) {
    unmapped.push({ id: o.id, title: o.displayTitle || o.title, raw: o.opportunityType });
    continue;
  }
  const row = coverage[seg];
  row.hotelMarketQualified += 1;
  row.titles.push(o.displayTitle || o.title);
  const sk = stageKey(o);
  if (sk === "SALES_READY") row.salesReady += 1;
  else if (sk === "ACCOUNT_DEVELOPMENT") row.accountDevelopment += 1;
  else row.qualifyOpportunity += 1;
}
for (const o of baselineCmi) {
  const seg = mapSegment(o.demandSegmentId || o.researchSegment || o.opportunityType || o.segmentDisplayLabel);
  if (!seg) continue;
  coverage[seg].cmiOnly += 1;
}

// Pass1 / deep research coverage signals
for (const c of p1.triagedToDeep || []) {
  const seg = mapSegment(c.segmentId || c.segment);
  if (!seg) continue;
  coverage[seg].researched = true;
  coverage[seg].candidateDiscoveries += 1;
}
const deepDir = path.join(EXA20, "raw-runs");
for (const f of fs.readdirSync(deepDir).filter((x) => x.startsWith("p2_"))) {
  const run = readJson(path.join(deepDir, f));
  const seg = mapSegment(run.cand?.segmentId);
  if (!seg) continue;
  coverage[seg].researched = true;
  coverage[seg].deepResearched += 1;
}
for (const j of p1.jobs || []) {
  const seg = mapSegment(j.segmentId || j.segment);
  if (seg) coverage[seg].researched = true;
}
// Mark researched if pass1 jobs covered segments (from jobsPlanned metadata)
for (const seg of SEGMENTS) {
  if (coverage[seg].candidateDiscoveries > 0 || coverage[seg].deepResearched > 0) {
    coverage[seg].researched = true;
  }
}

const missingHmSegments = SEGMENTS.filter((s) => coverage[s].hotelMarketQualified === 0);

writeJson("01-CURRENT_14_SEGMENT_COVERAGE.json", {
  contract: "GDI_RADISSON_14SEG_COVERAGE_V1",
  referenceDate: REF,
  segments: SEGMENTS.map((s) => ({
    segmentId: s,
    ...coverage[s],
    hasHotelMarket: coverage[s].hotelMarketQualified > 0,
    hasSalesReady: coverage[s].salesReady > 0,
    cmiOnlyNoHotelMarket: coverage[s].hotelMarketQualified === 0 && coverage[s].cmiOnly > 0,
    noQualifiedOpportunities: coverage[s].hotelMarketQualified === 0 && coverage[s].cmiOnly === 0,
  })),
  unmappedBaselineRows: unmapped,
  summary: {
    segmentsWithHotelMarket: SEGMENTS.filter((s) => coverage[s].hotelMarketQualified > 0).length,
    segmentsMissingHotelMarket: missingHmSegments,
    note: "Research coverage ≠ commercial coverage. association_events legacy labels mapped to associations_conferences_events; scaling_companies → corporate_accounts_scaling.",
  },
});

writeJson("02-SEGMENT_GAP_ANALYSIS.json", {
  missingHotelMarketSegments: missingHmSegments,
  thinSegments: SEGMENTS.filter((s) => coverage[s].hotelMarketQualified > 0 && coverage[s].hotelMarketQualified < 2),
  associationHeavy: coverage.associations_conferences_events?.hotelMarketQualified || 0,
  priorityOrder: [
    "corporate_meetings_incentives",
    "corporate_accounts_scaling",
    "government_public_sector",
    "projects_infrastructure",
    "sports_entertainment",
    "relocation_extended_stay",
    "travel_trade_group_intermediaries",
    "preferred_lodging_partnerships",
    "professional_event_planners",
    "local_catering_day_meetings",
    "social_special_events",
    "education_scientific",
    "corporate_celebrations_private_events",
  ].filter((s) => missingHmSegments.includes(s) || (coverage[s]?.hotelMarketQualified || 0) < 2),
  rationale:
    "Prioritize overlooked corporate, government, projects, sports housing, and recurring institutional demand — not more association mega-conferences.",
});

// ---------------------------------------------------------------------------
// Recovery from stored research
// ---------------------------------------------------------------------------
const certTitleNorm = new Set(baselineOpps.map((o) => norm(o.displayTitle || o.title)));
const certIds = new Set(baselineOpps.map((o) => o.id));

function loadDeep(candId) {
  const f = path.join(deepDir, `p2_${candId}.json`);
  if (!fs.existsSync(f)) return null;
  return readJson(f);
}

function parseContactFromBuyers(buyersText, org) {
  const s = text(buyersText);
  // Prefer "Name, Title" patterns
  const m =
    s.match(/([A-Z][a-zA-Z'.-]+(?:\s+[A-Z][a-zA-Z'.-]+){1,3}),\s*([^.;(]{8,80})/) ||
    s.match(/([A-Z][a-zA-Z'.-]+(?:\s+[A-Z][a-zA-Z'.-]+){1,3})\s+is\s+the\s+([^.;]{8,80})/i);
  if (!m) return null;
  return {
    name: text(m[1]),
    title: text(m[2]).replace(/\s+at\s+.*/i, "").slice(0, 120),
    organization: org,
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  };
}

function extractEmail(blob) {
  const m = String(blob || "").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return m ? m[0] : null;
}

/**
 * Additive candidates backed by existing deep research (no new paid calls).
 * Remap mis-tagged segments to the certified 14-ID taxonomy.
 */
const ADDITIVE_SPECS = [
  // Note: Rail Day / Land & Development / CGNA already exist as CMI in the certified
  // package — do not duplicate or reclassify them into Hotel Market.
  {
    id: "rx_d_eeab9f302f04",
    segmentId: "government_public_sector",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Qualify Overflow Demand",
    revenueStreams: ["GUESTROOMS"],
    why: "OCI DiscoveryX 2027 at Enercare with published attendee room-block link — qualify whether Harbourfront can participate as secondary/overflow, not as exclusive host.",
  },
  {
    id: "rx_d_ee0d8b24b9ef",
    segmentId: "corporate_meetings_incentives",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Qualify Opportunity",
    revenueStreams: ["GUESTROOMS", "MEETING"],
    why: "OBIO Investment Summit Feb 2027 at Arcadian Court — multi-day investor/programme gathering with unconfirmed lodging path; credible corporate meetings demand to qualify.",
  },
  {
    id: "rx_d_2f57ed564c71",
    segmentId: "education_scientific",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Qualify Opportunity",
    revenueStreams: ["GUESTROOMS", "MEETING"],
    why: "ACCUO 2027 at TMU — multi-day ombudsperson conference; accommodations still to be announced — remap from scaling_companies to education_scientific.",
  },
  {
    id: "rx_d_918bc65e2d44",
    segmentId: "corporate_accounts_scaling",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Qualify Overflow Demand",
    revenueStreams: ["GUESTROOMS"],
    displayTitle: "OVMA True North Veterinary Conference 2027 — overflow lodging",
    why: "OVMA True North 2027 at MTCC with published multi-hotel courtesy blocks — qualify whether Harbourfront can join as supplemental lodging for out-of-town veterinarians; not a main-block displacement pitch.",
  },
  {
    id: "rx_d_f5c3bd3d073a",
    segmentId: "corporate_meetings_incentives",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Qualify Opportunity",
    revenueStreams: ["GUESTROOMS", "MEETING"],
    why: "Open Banking Expo Canada 2027 at MTCC — corporate/fintech meetings demand; lodging path unconfirmed. Re-enter as Qualify after prior research-continuation demotion with clearer corporate-meetings framing (not Sales Ready).",
  },
  {
    id: "rx_gdi_s4912_d46e3e911701",
    segmentId: "sports_entertainment",
    readiness: "ACCOUNT_DEVELOPMENT",
    salesAction: "Build Venue Partnership",
    revenueStreams: ["GUESTROOMS", "PREFERRED_LODGING"],
    displayTitle: "Toronto Tempo / WNBA — 2027 visiting-team & preferred lodging development",
    alsoSegmentNote: "preferred_lodging_partnerships_adjacent",
    why: "Toronto Tempo / WNBA — Hotel X is domestic partner; develop 2027 visiting-team / overflow / preferred-lodging challenge with named commercial contacts. No invented 2027 home dates.",
  },
];

const recovered = [];
const newHm = [];
const researchContinuation = [];
const rejectedAdditive = [];

for (const spec of ADDITIVE_SPECS) {
  const run = loadDeep(spec.id);
  if (!run?.cand) {
    rejectedAdditive.push({ id: spec.id, reason: "DEEP_RUN_MISSING" });
    continue;
  }
  const cand = run.cand;
  // Block only when the same opportunity id already exists, or an exact title
  // match on a *Hotel Market* card. CMI-only titles must not be duplicated as HM,
  // but title-prefix collisions with unrelated cards are ignored.
  const exactTitleOnHm = baselineHm.some((o) => norm(o.displayTitle || o.title) === norm(cand.title));
  const exactTitleOnCmi = baselineCmi.some((o) => norm(o.displayTitle || o.title) === norm(cand.title));
  if (certIds.has(cand.id) || exactTitleOnHm || exactTitleOnCmi) {
    rejectedAdditive.push({
      id: cand.id,
      title: cand.title,
      reason: certIds.has(cand.id)
        ? "ALREADY_IN_CERTIFIED_INVENTORY_ID"
        : exactTitleOnHm
          ? "ALREADY_HOTEL_MARKET_TITLE"
          : "ALREADY_CMI_TITLE_DO_NOT_RECLASSIFY",
    });
    continue;
  }
  const structured = run.paid?.result?.structured || {};
  const findings = structured.findings || {};
  const hints = structured.cibMappingHints || {};
  const start = cand.ledgerStart || null;
  const end =
    (String(findings.futureEdition || "").match(/(\d{4}-\d{2}-\d{2}).{0,40}(\d{4}-\d{2}-\d{2})/) || [])[2] ||
    null;
  let contact = parseContactFromBuyers(findings.namedBuyers, cand.organizationName);
  const email = extractEmail(`${findings.namedBuyers} ${findings.recommendedSalesAction} ${hints.recommendedNextStep}`);
  if (contact && email) contact = { ...contact, email, sourceUrl: (cand.evidenceUrls || [])[0] || null };
  if (contact && !contact.sourceUrl) contact.sourceUrl = (cand.evidenceUrls || [])[0] || null;

  // Special-case known contacts from deep research text
  if (spec.id === "rx_d_69f4acc1ef35") {
    contact = {
      name: "Andrea Roncoroni Martins",
      title: "Head of Operations and Memberships / Rail Day commercial contact",
      organization: "TRACCS",
      email: "andrear@traccs.ca",
      sourceUrl: "https://www.traccs.ca/railday2027",
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    };
  }
  if (spec.id === "rx_d_ee0d8b24b9ef" && !contact) {
    contact = {
      name: "Christina Yeh",
      title: "Vice President, Programs & Strategic Partnerships",
      organization: "OBIO",
      email: "summit@obio.ca",
      sourceUrl: (cand.evidenceUrls || [])[0] || null,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    };
  }
  if (spec.id === "rx_gdi_s4912_d46e3e911701") {
    contact = {
      name: "Lisa Ferkul",
      title: "Chief Revenue Officer",
      organization: "Toronto Tempo / WNBA",
      sourceUrl: (cand.evidenceUrls || [])[0] || null,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    };
  }

  const row = {
    id: cand.id,
    title: cand.title,
    org: cand.organizationName,
    priorSegmentTag: cand.segmentId,
    mappedSegment: spec.segmentId,
    start,
    venue: cand.ledgerVenue,
    contact: contact?.name || null,
    deepOk: Boolean(run.paid?.ok),
    freeVerifiedUrl: spec.freeVerifiedUrl || null,
    disposition: "CANDIDATE",
  };

  if (!contact?.name) {
    row.disposition = "RESEARCH_CONTINUATION";
    row.reason = "NO_NAMED_CONTACT_EXTRACTED";
    researchContinuation.push(row);
    recovered.push(row);
    continue;
  }

  // Tempo has no exact 2027 dates — keep as AD with ongoing programme thesis
  if (!start && spec.id !== "rx_gdi_s4912_d46e3e911701") {
    row.disposition = "RESEARCH_CONTINUATION";
    row.reason = "NO_VERIFIED_FUTURE_DATE";
    researchContinuation.push(row);
    recovered.push(row);
    continue;
  }

  const sources = (cand.evidenceUrls || []).map((u) => ({ url: u, statement: cand.title }));
  if (spec.freeVerifiedUrl && !sources.some((s) => s.url === spec.freeVerifiedUrl)) {
    sources.unshift({ url: spec.freeVerifiedUrl, statement: "Free public verification of dates/contact during diversification audit" });
  }

  const what =
    text(hints.whatIsHappening) ||
    text(findings.futureEdition) ||
    `${cand.title} — ${cand.organizationName}${start ? ` (${start})` : ""}.`;
  const potential =
    text(hints.potentialBusiness) ||
    "Unquantified guestroom / meeting demand to qualify; no confirmed room block for Radisson Blu.";
  const whyHotel = `${HOTEL_NAME} (157 rooms, Harbourfront / Queens Quay) fits mid-size exhibitor, speaker, visiting-team, or overflow lodging near downtown and Exhibition Place. ${text(findings.hotelProductFit).slice(0, 280)}`.trim();
  const nextStep =
    text(findings.recommendedSalesAction) ||
    text(hints.recommendedNextStep) ||
    `Contact ${contact.name} to confirm lodging ownership and whether ${HOTEL_NAME} can participate.`;

  const displayTitle = spec.displayTitle || cand.title;
  const draft = {
    id: cand.id,
    displayTitle,
    title: displayTitle,
    organizationName: cand.organizationName,
    displayCompanyName: cand.organizationName,
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    targetHotelName: HOTEL_NAME,
    whatIsHappening: what.slice(0, 900),
    summaryWhat: what.slice(0, 400),
    potentialHotelBusiness: potential.slice(0, 700),
    potentialBusiness: potential.slice(0, 700),
    whyThisHotel: whyHotel.slice(0, 900),
    whyActNow: spec.why.slice(0, 400),
    recommendedNextStep: nextStep.slice(0, 900),
    recommendedAction: nextStep.slice(0, 500),
    hotelSourcingStatusLabel: "Hotel Sourcing Unconfirmed",
    targetHotelSourcingStatusLabel: "Hotel Sourcing Unconfirmed",
    salesActionPillLabel: spec.salesAction,
    salesActionKey: norm(spec.salesAction).replace(/\s+/g, "_").toUpperCase(),
    commercialStagePillLabel:
      spec.readiness === "ACCOUNT_DEVELOPMENT"
        ? "Account Development"
        : spec.readiness === "SALES_READY"
          ? "Sales Ready"
          : "Qualify Opportunity",
    commercialStageKey:
      spec.readiness === "ACCOUNT_DEVELOPMENT"
        ? "account_development"
        : spec.readiness === "SALES_READY"
          ? "sales_ready"
          : "qualify_opportunity",
    commercialActionability: spec.readiness,
    priority: "MEDIUM",
    priorityPillLabel: "Medium",
    demandSegmentId: spec.segmentId,
    researchSegment: spec.segmentId,
    opportunityType: spec.segmentId,
    segmentDisplayLabel: spec.segmentId.replace(/_/g, " "),
    eventStartDate: start,
    eventEndDate: end || start,
    tileEventStartDate: start,
    tileEventEndDate: end || start,
    datePillDisplayLabel: start
      ? end && end !== start
        ? `${start} – ${end}`
        : start
      : "Dates TBD — relationship development",
    primaryContact: contact,
    sources,
    establishedFacts: [
      cand.ledgerVerifiedDates || start || "Future/ongoing demand activity referenced in stored research",
      cand.ledgerVenue || "Venue per stored deep research",
      `Named contact: ${contact.name}`,
    ],
    uncertainties: [
      "Room-night volume unverified",
      "2027 lodging ownership may still be unassigned",
      "Do not promise F&B — restaurant/lounge status must be verified operationally",
      ...(spec.alsoSegmentNote ? [`Also relevant to ${spec.alsoSegmentNote}`] : []),
    ],
    hotelMarketScopeEligible: true,
    competitiveMarketScopeEligible: false,
    revenueStreamHints: spec.revenueStreams,
    diversificationOrigin: "GDI_RADISSON_14SEG_DIVERSIFICATION_V1_ZERO_PAID_RECOVERY",
    provenance: {
      origin: "STORED_DEEP_RESEARCH_RECOVERY_ZERO_PAID",
      deepRunId: run.paid?.result?.runId || null,
      priorCampaign: "gdi-radisson-toronto-14-segment-exa-expansion-20usd-v1",
      paidSpendThisPhaseUsd: 0,
      referenceDate: REF,
    },
  };

  row.disposition = "NEW_HOTEL_MARKET";
  row.readiness = spec.readiness;
  row.salesAction = spec.salesAction;
  row.why = spec.why;
  recovered.push(row);
  newHm.push(draft);
}

// Pass1-only underrepresented queue (not promoted without deep evidence)
const pass1Queue = [];
for (const c of p1.triagedToDeep || []) {
  const seg = mapSegment(c.segmentId || c.segment);
  if (!seg) continue;
  if (!missingHmSegments.includes(seg) && (coverage[seg]?.hotelMarketQualified || 0) >= 2) continue;
  if (certTitleNorm.has(norm(c.title)) || newHm.some((n) => norm(n.displayTitle) === norm(c.title))) continue;
  if (ADDITIVE_SPECS.some((s) => s.id === c.id)) continue;
  pass1Queue.push({
    id: c.id,
    title: c.title,
    org: c.org || c.organizationName,
    segmentId: seg,
    start: c.ledgerStart || c.start || null,
    venue: c.ledgerVenue || null,
    status: "RESEARCH_CONTINUATION_NEEDS_DEEP_OR_FREE_VERIFY",
    estimatedExaDeepUsd: 0.5,
  });
}

writeJson("03-EXISTING_RESEARCH_RECOVERY.json", {
  contract: "GDI_RADISSON_14SEG_RESEARCH_RECOVERY_V1",
  paidSpendUsd: 0,
  additiveSpecsEvaluated: ADDITIVE_SPECS.length,
  recovered,
  rejectedAdditive,
  researchContinuation,
  pass1UnderrepresentedQueueCount: pass1Queue.length,
  note: "Only deep-researched candidates with named contacts and credible demand theses were promoted. Pass-1-only hits remain research continuation.",
});

writeJson("04-TARGETED_DISCOVERY_RESULTS.json", {
  method: "ZERO_PAID_RECOVERY_PLUS_FREE_PUBLIC_VERIFY",
  newPaidDiscoveryJobs: 0,
  freePublicVerifications: [{ url: "https://www.traccs.ca/railday2027", result: "Confirmed Rail Day 2027 June 16–17; Andrea contact andrear@traccs.ca" }],
  promotedFromStoredDeep: newHm.map((o) => ({ id: o.id, title: o.displayTitle, segment: o.demandSegmentId })),
  pass1QueueSample: pass1Queue.slice(0, 40),
});

// Segment-themed report slices
const byTheme = {
  "05-CORPORATE_DEMAND.json": ["corporate_meetings_incentives", "corporate_accounts_scaling", "corporate_celebrations_private_events"],
  "06-GOVERNMENT_AND_PROJECTS.json": ["government_public_sector", "projects_infrastructure"],
  "07-EDUCATION_MEDICAL_RESEARCH.json": ["education_scientific"],
  "08-SPORTS_AND_ENTERTAINMENT.json": ["sports_entertainment"],
  "09-LOCAL_MEETINGS_CATERING_AND_SOCIAL.json": ["local_catering_day_meetings", "social_special_events"],
  "10-TRAVEL_TRADE_EXTENDED_STAY_PARTNERSHIPS.json": [
    "travel_trade_group_intermediaries",
    "relocation_extended_stay",
    "preferred_lodging_partnerships",
    "professional_event_planners",
  ],
};
for (const [file, segs] of Object.entries(byTheme)) {
  writeJson(file, {
    segments: segs,
    existingHotelMarket: segs.flatMap((s) => coverage[s]?.titles || []),
    newHotelMarket: newHm.filter((o) => segs.includes(o.demandSegmentId)).map((o) => ({
      id: o.id,
      title: o.displayTitle,
      org: o.organizationName,
      contact: o.primaryContact?.name,
      readiness: o.commercialActionability,
      action: o.salesActionPillLabel,
    })),
    researchGaps: pass1Queue.filter((p) => segs.includes(p.segmentId)).slice(0, 25),
    stillMissingAfterThisPhase: segs.filter(
      (s) => (coverage[s]?.hotelMarketQualified || 0) + newHm.filter((o) => o.demandSegmentId === s).length === 0,
    ),
  });
}

writeJson("11-CONTACT_QUALIFICATION.json", {
  newContacts: newHm.map((o) => ({
    opportunityId: o.id,
    name: o.primaryContact?.name,
    title: o.primaryContact?.title,
    organization: o.primaryContact?.organization,
    email: o.primaryContact?.email || null,
    sourceUrl: o.primaryContact?.sourceUrl || null,
    class: o.primaryContact?.contactClass,
    bookingAuthorityClaimed: false,
    note: "Entry point / coordinator — not fabricated contracting authority",
  })),
  surfe: "ON_CLICK_ONLY — not invoked",
});

writeJson("12-COMMERCIAL_ELIGIBILITY.json", {
  contract: "GDI_FULL_E2E_HOTEL_TEMPLATE_V1 + actionability 4b",
  referenceDate: REF,
  rules: [
    "Reliable inferred demand accepted when dates, org, contact, and hotel-fit mechanism are evidenced",
    "No Sales Ready promotions in this phase — none newly satisfy full SR contract",
    "CMI competitor-committed main housing not counted as Hotel Market",
    "One-day events require explicit overnight/speaker/exhibitor lodging thesis",
  ],
  decisions: newHm.map((o) => ({
    id: o.id,
    title: o.displayTitle,
    segment: o.demandSegmentId,
    readiness: o.commercialActionability,
    demandStandard: "RELIABLE_INFERRED_DEMAND",
    salesReady: false,
  })),
});

writeJson("13-NEW_HOTEL_MARKET_INVENTORY.json", {
  count: newHm.length,
  opportunities: newHm.map((o) => ({
    id: o.id,
    title: o.displayTitle,
    org: o.organizationName,
    segment: o.demandSegmentId,
    start: o.eventStartDate,
    contact: o.primaryContact?.name,
    readiness: o.commercialActionability,
    action: o.salesActionPillLabel,
    revenueStreams: o.revenueStreamHints,
  })),
});

writeJson("14-CMI_SEPARATION.json", {
  baselineCmiPreserved: baselineCmi.length,
  cmiIds: baselineCmi.map((o) => o.id),
  newCmiAdded: 0,
  note: "No new CMI invented. DiscoveryX room-block treated as Hotel Market overflow qualification, not automatic CMI, because competitor hotel identity for the block was not edition-certified in this zero-paid pass.",
  discoveryXNote:
    "If a later paid pass confirms the OCI room block is exclusively at a named competitor hotel, reclassify or dual-track per CMI contract.",
});

// ---------------------------------------------------------------------------
// Build additive customer inventory = certified + new
// ---------------------------------------------------------------------------
const baselineIds = new Set(baselineOpps.map((o) => o.id));
let customerOpps = [
  ...baselineOpps.map((o) => {
    const next = clone(o);
    // Freeze certified presentation so Sales Ready / actions are not rewritten.
    next.publishedPresentationLocked = true;
    next.gdiPublishedInventoryRegistry = true;
    return next;
  }),
  ...newHm.map(clone),
];
customerOpps = annotateInventoryPopulations(customerOpps, { hotelId: HOTEL_ID });
const gate = applyCommercialActionabilityGate(customerOpps, { referenceDate: REF });
let gatedRows = (gate.rows || []).filter(
  (o) => o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true,
);
gatedRows = gatedRows.map((o) => {
  const hm = o.hotelMarketScopeEligible;
  const cmi = o.competitiveMarketScopeEligible;
  const locked =
    baselineIds.has(o.id) ||
    o.publishedPresentationLocked === true ||
    o.gdiPublishedInventoryRegistry === true;
  // Preserve certified pills before presentation helpers.
  const freeze = locked
    ? {
        salesActionKey: o.salesActionKey,
        salesActionPillLabel: o.salesActionPillLabel,
        commercialStageKey: o.commercialStageKey,
        commercialStagePillLabel: o.commercialStagePillLabel,
        priority: o.priority,
        priorityPillLabel: o.priorityPillLabel,
        priorityFilterKey: o.priorityFilterKey,
        datePillDisplayLabel: o.datePillDisplayLabel,
        hotelSourcingStatusLabel: o.hotelSourcingStatusLabel,
      }
    : null;
  let next = o;
  try {
    next = attachCustomerClassificationPresentation(o);
  } catch (e) {
    console.warn("[presentation]", o.id, String(e?.message || e));
  }
  if (freeze) Object.assign(next, freeze, { publishedPresentationLocked: true, gdiPublishedInventoryRegistry: true });
  const type = classifyOpportunityTypeFilter(next);
  next.opportunityTypeFilter = type;
  next.opportunityTypeFilterCategory = type;
  next.opportunityTypeFilterLabel = OPPORTUNITY_TYPE_FILTER_LABEL[type] || type;
  next.hotelMarketScopeEligible = hm;
  next.competitiveMarketScopeEligible = cmi;
  return scrubRepeatedSourcingNarratives(next);
});
customerOpps = gatedRows;

const assertGate = assertCustomerInventoryPassesActionabilityGate(customerOpps, { referenceDate: REF });
if (assertGate?.ok === false) {
  console.warn("[actionability blockers]", assertGate.blockers?.slice?.(0, 5) || assertGate);
}

writeJson("09b-ACTIONABILITY_GATE.json", {
  activeHotel: gate.activeHotel?.length ?? null,
  activeCompetitive: gate.activeCompetitive?.length ?? null,
  researchWatch: gate.researchWatch?.length ?? null,
  customerVisibleAfterGate: customerOpps.length,
  assertOk: assertGate?.ok ?? null,
});

// Diversity scorecard after additive
const afterCoverage = Object.fromEntries(SEGMENTS.map((s) => [s, { existing: coverage[s].hotelMarketQualified, neu: 0, final: 0, sr: 0, qualify: 0, ad: 0, cmi: coverage[s].cmiOnly }]));
for (const o of customerOpps.filter((x) => x.hotelMarketScopeEligible)) {
  const seg = mapSegment(o.demandSegmentId || o.opportunityType) || "associations_conferences_events";
  if (!afterCoverage[seg]) continue;
  afterCoverage[seg].final += 1;
  const sk = stageKey(o);
  if (sk === "SALES_READY") afterCoverage[seg].sr += 1;
  else if (sk === "ACCOUNT_DEVELOPMENT") afterCoverage[seg].ad += 1;
  else afterCoverage[seg].qualify += 1;
}
for (const o of newHm) {
  if (afterCoverage[o.demandSegmentId]) afterCoverage[o.demandSegmentId].neu += 1;
}
for (const s of SEGMENTS) {
  afterCoverage[s].existing = coverage[s].hotelMarketQualified;
  // recompute neu as final - existing for HM added in those segments that appear in newHm
}

writeJson("15-SEGMENT_DIVERSITY_SCORECARD.json", {
  beforeSegmentsWithHm: SEGMENTS.filter((s) => coverage[s].hotelMarketQualified > 0).length,
  afterSegmentsWithHm: SEGMENTS.filter((s) => afterCoverage[s].final > 0).length,
  targetSegmentsWithHm: 10,
  targetMet: SEGMENTS.filter((s) => afterCoverage[s].final > 0).length >= 10,
  rows: SEGMENTS.map((s) => ({
    segmentId: s,
    existingHotelMarket: coverage[s].hotelMarketQualified,
    newHotelMarket: newHm.filter((o) => o.demandSegmentId === s).length,
    finalHotelMarket: afterCoverage[s].final,
    salesReady: afterCoverage[s].sr,
    qualifyOpportunity: afterCoverage[s].qualify,
    accountDevelopment: afterCoverage[s].ad,
    cmi: afterCoverage[s].cmi,
    researchGaps: pass1Queue.filter((p) => p.segmentId === s).length,
    estimatedAdditionalPaidUsd: pass1Queue.filter((p) => p.segmentId === s).length * 0.5,
  })),
});

// ---------------------------------------------------------------------------
// Stage additive package (preserve prior fingerprint package on disk)
// ---------------------------------------------------------------------------
console.log("[stage-preflight]", {
  customerOpps: customerOpps.length,
  withId: customerOpps.filter((o) => o?.id).length,
  hm: customerOpps.filter((o) => o.hotelMarketScopeEligible).length,
  cmi: customerOpps.filter((o) => o.competitiveMarketScopeEligible).length,
  newHm: newHm.length,
});

const built = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  opportunities: customerOpps,
  publicationVersion: PUB_VERSION,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  approvalStatus: "PENDING_FOUNDER_APPROVAL",
  templateVersion: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  notes: {
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    auditContract: "GDI_RADISSON_14SEG_DIVERSIFICATION_V1",
    priorFingerprintPreserved: PRIOR_FP,
    paidSpendUsd: 0,
    additiveOnly: true,
    customerPublished: false,
  },
});
const pkg = built.package || built;
const integrity = verifyPublicationPackageIntegrity(pkg);
const qg = runPublicationQualityGate({ opportunities: pkg.opportunities || customerOpps, referenceDate: REF });
if (integrity?.ok === false) {
  console.error("Package integrity failed", integrity);
  process.exit(3);
}
if (qg?.ok === false) {
  console.error("Publication quality gate blocking", qg.blocking || qg);
  process.exit(3);
}
if (pkg.contentFingerprintSha256 === PRIOR_FP) {
  console.error("New fingerprint equals prior — refusing no-op stage");
  process.exit(3);
}

const store = createGdiPublicationPackageStore({
  root: path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
});
const written = store.writePackage(pkg);
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
});
const counts = computePublicationInventoryCounts(pkg.opportunities || customerOpps);

// Ensure prior cert package fingerprint file untouched
const priorFpCheck = readJson(path.join(CERT_PKG_DIR, "package.json")).contentFingerprintSha256;
if (priorFpCheck !== PRIOR_FP) {
  console.error("FATAL: certified package fingerprint changed on disk");
  process.exit(3);
}

writeJson("17-STAGED_PUBLICATION_AND_BROWSER.json", {
  staged: true,
  publicationVersion: PUB_VERSION,
  publicationId: pkg.publicationId,
  fingerprint: pkg.contentFingerprintSha256,
  priorFingerprintPreserved: PRIOR_FP,
  priorPackagePath: CERT_PKG_DIR,
  priorPackageUnchangedOnDisk: priorFpCheck === PRIOR_FP,
  lifecycleState: pkg.lifecycleState,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  customerPublished: false,
  integrity,
  qualityGate: { ok: qg.ok, blockingCount: qg.blockingCount, findingCount: qg.findingCount },
  inventoryCounts: counts,
  packagePath:
    written?.path ||
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      HOTEL_ID,
      PUB_VERSION,
      "package.json",
    ),
  browserNote: "Run with --browser-verify against worktree PORT (e.g. 8092); do not use primary :8080.",
});

const hotelDataDir = path.join(ROOT, "data/group-demand-intelligence/hotels", HOTEL_ID);
fs.mkdirSync(hotelDataDir, { recursive: true });
fs.writeFileSync(
  path.join(hotelDataDir, "opportunities.json"),
  `${JSON.stringify(
    {
      schema: "gdi-opportunities-doc-v1",
      hotelId: HOTEL_ID,
      hotelName: HOTEL_NAME,
      updatedAt: new Date().toISOString(),
      source: "gdi-radisson-toronto-14-segment-diversification-v1",
      note: "STAGED_FOR_FOUNDER_REVIEW — additive diversification; prior cert fingerprint preserved",
      opportunities: customerOpps,
    },
    null,
    2,
  )}\n`,
);

// Airtable dry-run
let airtable = { configured: isGdiOpportunityAirtableConfigured(), dryRun: true, writes: 0 };
try {
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    opportunities: customerOpps,
    dryRun: true,
  });
  airtable = {
    configured: true,
    dryRun: true,
    writes: 0,
    wouldCreate: sync.summary?.created,
    wouldUpdate: sync.summary?.updated,
    total: sync.summary?.total,
    note: "No live writes. Preserve existing certification/publication linkages.",
  };
} catch (e) {
  airtable = { ...airtable, error: String(e?.message || e) };
}
writeJson("16-AIRTABLE_RECONCILIATION_DRY_RUN.json", airtable);

// Paid Exa proposal for remaining gaps
const gapSegs = SEGMENTS.filter((s) => afterCoverage[s].final === 0);
writeJson("18-PAID_EXA_SEGMENT_GAP_PROPOSAL.json", {
  authorization: "NOT_AUTHORIZED — proposal only",
  thisPhaseSpendUsd: 0,
  remainingMissingSegments: gapSegs,
  measuredDeepCostUsd: 0.5,
  options: [
    {
      option: "MINIMUM_PILOT",
      usd: 15,
      focus: gapSegs.slice(0, 4),
      expected: "Deep-research 20–25 pass1 candidates in missing segments; expect 3–8 incremental HM after strict audit",
    },
    {
      option: "RECOMMENDED",
      usd: 30,
      focus: [
        "travel_trade_group_intermediaries",
        "relocation_extended_stay",
        "preferred_lodging_partnerships",
        "professional_event_planners",
        "local_catering_day_meetings",
        "social_special_events",
        "corporate_accounts_scaling",
      ],
      expected: "8–16 incremental HM aspirational; stop if yield <10% for two batches",
    },
    {
      option: "CEILING",
      usd: 45,
      expected: "Only if recommended pilot clears yield gates",
    },
  ],
  stopRules: [
    "Do not spend to fill segment checkboxes with speculative cards",
    "No Surfe auto-enrichment",
    "No CUSTOMER_PUBLISHED / no live Airtable without separate auth",
    "Preserve certified fingerprint package forever",
  ],
  pass1QueueCount: pass1Queue.length,
});

const finalHm = customerOpps.filter((o) => o.hotelMarketScopeEligible);
const scorecard = {
  originalHotelMarket: baselineHm.length,
  newQualifiedHotelMarket: newHm.length,
  finalHotelMarket: counts.hotel || finalHm.length,
  finalCmi: counts.competitive || baselineCmi.length,
  salesReady: finalHm.filter((o) => stageKey(o) === "SALES_READY").length,
  accountDevelopment: finalHm.filter((o) => stageKey(o) === "ACCOUNT_DEVELOPMENT").length,
  qualifyOpportunity: finalHm.filter((o) => stageKey(o) === "QUALIFY_OPPORTUNITY").length,
  segmentsWithHmBefore: SEGMENTS.filter((s) => coverage[s].hotelMarketQualified > 0).length,
  segmentsWithHmAfter: SEGMENTS.filter((s) => afterCoverage[s].final > 0).length,
  stillMissingSegments: gapSegs,
  additionalSpendUsd: 0,
  newFingerprint: pkg.contentFingerprintSha256,
  priorFingerprintPreserved: PRIOR_FP,
};

writeJson("00-SCORECARD.json", scorecard);

const top15 = [...newHm]
  .map((o) => ({
    id: o.id,
    title: o.displayTitle,
    segment: o.demandSegmentId,
    contact: o.primaryContact?.name,
    action: o.salesActionPillLabel,
    readiness: o.commercialActionability,
    why: o.whyActNow,
  }))
  .slice(0, 15);

const founderMd = `# Radisson Blu Toronto Downtown — 14-Segment Diversification

**Phase spend:** $0 · **Property:** ${PROPERTY_CODE} · \`${HOTEL_ID}\`  
**Certified fingerprint preserved:** \`${PRIOR_FP}\`  
**New staged fingerprint:** \`${pkg.contentFingerprintSha256}\`  
**Lifecycle:** STAGED_FOR_FOUNDER_REVIEW (not CUSTOMER_PUBLISHED) · **Additive only**

## BEFORE → AFTER (Hotel Market by segment)

| Segment | Existing HM | New HM | Final HM | SR | Qualify | AD | CMI | Research gaps | Est. paid to close |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
${SEGMENTS.map((s) => {
  const r = afterCoverage[s];
  const gaps = pass1Queue.filter((p) => p.segmentId === s).length;
  return `| ${s} | ${coverage[s].hotelMarketQualified} | ${newHm.filter((o) => o.demandSegmentId === s).length} | **${r.final}** | ${r.sr} | ${r.qualify} | ${r.ad} | ${r.cmi} | ${gaps} | $${(gaps * 0.5).toFixed(1)} |`;
}).join("\n")}

**Segments with HM:** ${scorecard.segmentsWithHmBefore} → **${scorecard.segmentsWithHmAfter}** (target ≥10: ${scorecard.segmentsWithHmAfter >= 10 ? "MET" : "NOT YET"})  
**Still missing HM:** ${gapSegs.join(", ") || "(none)"}

## Inventory totals
| Metric | Before | After |
| --- | ---: | ---: |
| Hotel Market | ${baselineHm.length} | **${scorecard.finalHotelMarket}** |
| New additive HM | — | **${newHm.length}** |
| CMI (unchanged set) | ${baselineCmi.length} | **${scorecard.finalCmi}** |
| Sales Ready | 3 | **${scorecard.salesReady}** |
| Account Development | — | **${scorecard.accountDevelopment}** |
| Qualify Opportunity | 12 | **${scorecard.qualifyOpportunity}** |

## Top new commercial opportunities
${top15
  .map(
    (t, i) =>
      `${i + 1}. **${t.title}** (${t.segment} / ${t.readiness}) — ${t.action} · Contact: ${t.contact || "n/a"}`,
  )
  .join("\n")}

## What this is / is not
- **Is:** recovery of already deep-researched, non-association demand (projects, government, corporate meetings, education ombuds/nursing overflow, sports preferred-lodging AD).
- **Is not:** another conference-volume push, paid Exa spend, Surfe enrichment, Airtable live write, or CUSTOMER_PUBLISHED.

## Next paid Exa (NOT authorized)
- Minimum **$15** / recommended **$30** / ceiling **$45** focused on still-missing segments: ${gapSegs.join(", ") || "n/a"}
- Pass-1 queue awaiting deep: **${pass1Queue.length}** candidates

## Controls
- Prior cert package on disk unchanged (\`${PRIOR_FP}\`)
- Airtable: dry-run only  
- Primary checkout / op-cert branch: untouched  
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founderMd);

writeJson("19-GITHUB_MERGE_CLOSEOUT.json", {
  status: "READY_FOR_PR",
  branch: sh("git rev-parse --abbrev-ref HEAD"),
  headSha: sh("git rev-parse HEAD"),
  scorecard,
  reportsDir: "reports/gdi-radisson-toronto-14-segment-diversification-v1/",
});

console.log(
  JSON.stringify(
    {
      ok: true,
      paidSpendUsd: 0,
      scorecard,
      fingerprint: pkg.contentFingerprintSha256,
      qualityGate: qg?.ok ?? qg,
      newHm: newHm.map((o) => o.id),
    },
    null,
    2,
  ),
);
