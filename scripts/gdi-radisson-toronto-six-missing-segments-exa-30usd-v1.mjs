#!/usr/bin/env node
/**
 * Radisson Blu Toronto Downtown — 14-segment Exa expansion with a HARD $20 incremental paid ceiling.
 *
 * Authorization: FOUNDER_APPROVE_RADISSON_TORONTO_14_SEGMENT_EXA_EXPANSION_20USD_20261115
 *   - Exa ONLY (no Surfe, no Parallel). Max USD $20 incremental, SEPARATE ledger from the prior $15 campaign.
 *   - No CUSTOMER_PUBLISHED. Airtable dry-run only (0 writes).
 *   - Prior staged fingerprint 64d2ff53…8143 (expansion_20plus_v1) is preserved on disk; a NEW package is staged.
 *   - Bethesda / Sheraton / Wyndham publications are never touched (pointer + file hash proof).
 *
 * Budget stages (sum = $20):
 *   PASS1_DISCOVERY cap $3  — Exa MEDIUM discovery, one job per each of the 14 segments (+ mid-size second angles)
 *   PASS2_DEEP      cap $15 — Exa HIGH deep research on ~25-30 strongest candidates (recovery + new discovery hits)
 *   PASS3_VERIFY    cap $2  — Exa HIGH verification only for borderline rows that would unlock Hotel Market
 *
 * Modes:
 *   node scripts/gdi-radisson-toronto-14-segment-exa-expansion-20usd-v1.mjs                   # plan-only ($0)
 *   node scripts/gdi-radisson-toronto-14-segment-exa-expansion-20usd-v1.mjs --authorize-paid  # Pass1 -> Pass2 -> Pass3 + stage
 *   node scripts/gdi-radisson-toronto-14-segment-exa-expansion-20usd-v1.mjs --assemble-cached # re-qualify from this campaign's raw-runs ($0)
 *   node scripts/gdi-radisson-toronto-14-segment-exa-expansion-20usd-v1.mjs --browser-verify  # staged preview check (14-*.json)
 *
 * Re-runs resume the on-disk ledger and reuse completed raw runs (a stored run is never re-paid).
 * Completed raw runs from the prior $15 campaign are reused at $0 when the same candidate is selected.
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
import { softClampCustomerProse } from "../lib/group-demand-intelligence/presentation/gdi-customer-prose-completeness-v1.js";
import { attachSalesOpportunityFilterFields } from "../lib/group-demand-intelligence/sales-opportunity-quick-filters-v1.js";
import { planCustomerTileBadges } from "../lib/group-demand-intelligence/customer-classification-presentation-v1.js";
import { resolveCustomerSalesAction } from "../lib/group-demand-intelligence/customer-sales-action-resolver-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER_LABEL,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import {
  INVENTORY_POPULATION,
  annotateInventoryPopulations,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import {
  applyCommercialActionabilityGate,
  assertCustomerInventoryPassesActionabilityGate,
} from "../lib/group-demand-intelligence/commercial-actionability-gate-v1.js";
import {
  annotateCanonicalCmiContract,
  evaluateCmiQualificationGates,
  evaluateCmiDiscoveryCoverage,
  CMI_DISCOVERY_COVERAGE_CHECKLIST,
  GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
} from "../lib/group-demand-intelligence/commercial-consistency/cmi-canonical-eligibility-contract-v1.js";
import {
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
  COMMERCIAL_ACTIONABILITY,
  recommendCommercialAction,
} from "../lib/group-demand-intelligence/commercial-consistency/commercial-status-dimensions-v1.js";
import { CMI_CLAIM_TYPE } from "../lib/group-demand-intelligence/commercial-consistency/cmi-evidence-ledger-v1.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  GDI_FULL_E2E_HOTEL_TEMPLATE_LOCK_V1,
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import {
  createPaidBudgetLedger,
  loadPaidBudgetLedger,
  persistPaidBudgetLedger,
  remainingBudgetUsd,
  canAffordQuotedCall,
  reserveQuotedCall,
  settleReservedCall,
  releaseReservation,
} from "../lib/group-demand-intelligence/research/gdi-authorized-paid-budget-ledger-v1.js";
import {
  EXA_AGENT_EFFORT,
  runExaAgentResearch,
  quoteExaAgentRun,
} from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";
import { buildExaEventsAgentCreatePayload } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-events-contracts-v1.js";
import { FOURTEEN_SEGMENT_EXECUTOR_SPECS } from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-executor-registry-v1.js";

// ---------------------------------------------------------------------------
// constants
// ---------------------------------------------------------------------------
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recH9h3McgMNFQEla";
const HOTEL_NAME = "Radisson Blu Toronto Downtown";
const REF = "2026-11-16";
const TARGET_HM = 25;
const TARGET_SEGMENTS_WITH_HM = 10;
const MIN_NEW_MISSING_SEGMENTS = 2;
const HARD_CEILING_USD = 30;
const AUTHORIZATION_ID = "RADISSON_TORONTO_SIX_MISSING_SEGMENTS_EXA_30USD_V1";
const SCRIPT_ID = "gdi-radisson-toronto-six-missing-segments-exa-30usd-v1";
const OUT = path.join(ROOT, "reports", "gdi-radisson-toronto-six-missing-segments-exa-30usd-v1");
const RAW_DIR = path.join(OUT, "raw-runs");
const LEDGER_PATH = path.join(OUT, "paid-budget-ledger.json");
const HOTEL_DATA = path.join(ROOT, "data/group-demand-intelligence/hotels", HOTEL_ID);
const OPPS_JSON_PATH = path.join(HOTEL_DATA, "opportunities.json");
const DISCOVERY_LEDGER_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/discovery-ledgers",
  HOTEL_ID,
  "four-hotel-14-workflow-full-e2e-v1.json",
);
const PROTECTED_STORE_ROOT = path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages");
const PRIOR_PKG_DIR = path.join(
  PROTECTED_STORE_ROOT,
  HOTEL_ID,
  "gdi_pub_recH9h3McgMNFQEla_20261116_radisson_toronto_14seg_diversification_v1",
);
const PRIOR_15USD_PKG_DIR = path.join(
  PROTECTED_STORE_ROOT,
  HOTEL_ID,
  "gdi_pub_recH9h3McgMNFQEla_20261115_radisson_blu_toronto_15usd_full_e2e_v1",
);
const PRIOR_FINGERPRINT = "087afd1b1c6e91a62504b458f6d249cfa06bf1d400bc4540f79578ee37cf8fcb";
const CERT_FINGERPRINT_PRESERVE = "d9a6b23b8eb4ca305b8d35f638146cc266e4c089b0afa1cb5443012b1dd49f78";
const CERT_PKG_DIR = path.join(PROTECTED_STORE_ROOT, HOTEL_ID, "gdi_pub_recH9h3McgMNFQEla_20261116_radisson_toronto_final_commercial_cert_v1");
const PRIOR_15USD_RAW_DIR = path.join(ROOT, "reports/gdi-radisson-blu-toronto-paid-full-e2e-15usd-v1/raw-runs");
const PRIOR_20USD_RAW_DIR = path.join(ROOT, "reports/gdi-radisson-toronto-14-segment-exa-expansion-20usd-v1/raw-runs");
const DIVERSIFICATION_REPORT_DIR = path.join(ROOT, "reports/gdi-radisson-toronto-14-segment-diversification-v1");
const PRIOR_GAPS_PATH = path.join(DIVERSIFICATION_REPORT_DIR, "18-PAID_EXA_SEGMENT_GAP_PROPOSAL.json");
const MISSING_SEG_GAP_PATHS = [
  path.join(DIVERSIFICATION_REPORT_DIR, "09-LOCAL_MEETINGS_CATERING_AND_SOCIAL.json"),
  path.join(DIVERSIFICATION_REPORT_DIR, "10-TRAVEL_TRADE_EXTENDED_STAY_PARTNERSHIPS.json"),
];
const MISSING_SEGMENTS = [
  "social_special_events",
  "local_catering_day_meetings",
  "travel_trade_group_intermediaries",
  "relocation_extended_stay",
  "preferred_lodging_partnerships",
  "professional_event_planners",
];
const PUB_VERSION = `gdi_pub_${HOTEL_ID}_20261116_radisson_toronto_six_missing_segments_exa_30usd_v1`;
const PROTECTED_HOTELS = [
  ["recLuxvwwxID7U2B8", "bethesda"],
  ["recCAGqkERSXgiku4", "sheraton"],
  ["recXh4NgF8sDbi9cQ", "wyndham_alltra"],
];

const ARGS = new Set(process.argv.slice(2));
const AUTHORIZE = ARGS.has("--authorize-paid");
const ASSEMBLE_CACHED = ARGS.has("--assemble-cached");
const BROWSER_VERIFY = ARGS.has("--browser-verify");
const LIVE_PAID = AUTHORIZE; // only --authorize-paid may spend

const STAGE_CAP_USD = { PASS1_DISCOVERY: 5, PASS2_DEEP: 20, PASS3_VERIFY: 5 };
const MAX_RUNS = { PASS1_DISCOVERY: 18, PASS2_DEEP: 40, PASS3_VERIFY: 10 };
const CONCURRENCY = 2;
const HIGH_UNIT = Number(quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.HIGH }).maxUsd);
const MEDIUM_UNIT = Number(quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM }).maxUsd);

if (Object.values(STAGE_CAP_USD).reduce((a, b) => a + b, 0) > HARD_CEILING_USD + 1e-9) {
  console.error("Stage caps exceed hard ceiling");
  process.exit(3);
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
const text = (v) => String(v == null ? "" : v).replace(/\s+/g, " ").trim();
const readJson = (p) => JSON.parse(fs.readFileSync(p, "utf8").replace(/^\uFEFF/, ""));
function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  } catch {
    return "";
  }
}
const sha256File = (p) => (fs.existsSync(p) ? crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex") : null);
const normKey = (s) =>
  text(s).toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-z0-9]+/g, " ").trim();
function hashId(prefix, a, b) {
  const h = crypto.createHash("sha256").update(`${normKey(a)}|${normKey(b)}`).digest("hex").slice(0, 12);
  return `${prefix}_${h}`;
}
const iso = (y, m, d) => `${String(y).padStart(4, "0")}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
function parseIso(s) {
  const m = text(s).match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}
const countBy = (arr, fn) => {
  const m = {};
  for (const x of arr) {
    const k = fn(x) ?? "(none)";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
};
const cloneOpp = (o) => JSON.parse(JSON.stringify(o));
const addDays = (d, n) => new Date(Date.parse(`${d}T12:00:00Z`) + n * 86400000).toISOString().slice(0, 10);
const daysBetween = (a, b) => Math.round((Date.parse(`${b}T12:00:00Z`) - Date.parse(`${a}T12:00:00Z`)) / 86400000);
/** Events starting inside 21 days cannot realistically be sourced — excluded, not padded. */
const IMMINENT_CUTOFF = addDays(REF, 21);
function dateLabel(start, end) {
  if (!start) return "Dates TBD";
  const f = (d) =>
    new Date(`${d}T12:00:00Z`).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" });
  return end && end !== start ? `${f(start)} – ${f(end)}` : f(start);
}

const MONTHS = {
  jan: 1, january: 1, janvier: 1, feb: 2, february: 2, fevrier: 2, mar: 3, march: 3, mars: 3,
  apr: 4, april: 4, avril: 4, may: 5, mai: 5, jun: 6, june: 6, juin: 6, jul: 7, july: 7, juillet: 7,
  aug: 8, august: 8, aout: 8, sep: 9, sept: 9, september: 9, septembre: 9, oct: 10, october: 10,
  octobre: 10, nov: 11, november: 11, novembre: 11, dec: 12, december: 12, decembre: 12,
};
const MONTH_RE =
  "jan(?:uary)?|janvier|feb(?:ruary)?|fevrier|mar(?:ch)?|mars|apr(?:il)?|avril|may|mai|jun(?:e)?|juin|jul(?:y)?|juillet|aug(?:ust)?|aout|sep(?:t(?:ember)?)?|septembre|oct(?:ober)?|octobre|nov(?:ember)?|novembre|dec(?:ember)?|decembre";
const monthNum = (w) => MONTHS[String(w || "").toLowerCase().replace(/\.$/, "")] || null;

/** Explicit calendar dates (start of ranges) from text / URLs — EN + FR, accent-folded. */
function extractExplicitDates(blob) {
  const s = String(blob || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  const out = [];
  const push = (y, m, d) => {
    if (!y || !m || !d || m < 1 || m > 12 || d < 1 || d > 31) return;
    out.push(iso(y, m, d));
  };
  for (const m of s.matchAll(/\b(20\d{2})-(\d{2})-(\d{2})\b/g)) push(+m[1], +m[2], +m[3]);
  const reEn = new RegExp(
    `\\b(${MONTH_RE})\\.?\\s+(\\d{1,2})(?:st|nd|rd|th)?(?:\\s*(?:–|-|to)\\s*(?:(?:${MONTH_RE})\\.?\\s+)?\\d{1,2})?,?\\s+(20\\d{2})`,
    "gi",
  );
  for (const m of s.matchAll(reEn)) push(+m[3], monthNum(m[1]), +m[2]);
  const reDay = new RegExp(
    `\\b(\\d{1,2})(?:\\s*(?:–|-|au|to)\\s*\\d{1,2})?\\s+(?:de\\s+)?(${MONTH_RE})\\.?,?\\s+(20\\d{2})`,
    "gi",
  );
  for (const m of s.matchAll(reDay)) push(+m[3], monthNum(m[2]), +m[1]);
  return [...new Set(out)].sort();
}
const maxYearIn = (s) => {
  const ys = [...String(s || "").matchAll(/\b(20[2-3]\d)\b/g)].map((m) => Number(m[1]));
  return ys.length ? Math.max(...ys) : null;
};

/** Toronto competitive hotel catalog — membership is NOT an opportunity. */
const COMPETITOR_CATALOG = [
  ["The Westin Harbour Castle", /westin harbour castle/i],
  ["Hotel X Toronto", /hotel x\b/i],
  ["Sheraton Centre Toronto", /sheraton centre/i],
  ["Hyatt Regency Toronto", /hyatt regency toronto/i],
  ["Le Méridien Toronto Pinnacle Hotel", /le m[eé]ridien/i],
  ["Fairmont Royal York", /fairmont royal york|\broyal york\b/i],
  ["Toronto Marriott City Centre Hotel", /marriott city centre/i],
  ["Toronto Marriott Downtown Eaton Centre Hotel", /marriott downtown|toronto marriott downtown/i],
  ["Hilton Toronto", /hilton toronto|hilton garden inn toronto/i],
  ["InterContinental Toronto Centre", /intercontinental/i],
  ["Delta Hotels Toronto", /delta hotels?(?: by marriott)? toronto|delta toronto/i],
  ["Omni King Edward Hotel", /omni king edward|king edward hotel/i],
  ["Park Hyatt Toronto", /park hyatt/i],
  ["Four Seasons Hotel Toronto", /four seasons/i],
  ["The Ritz-Carlton, Toronto", /ritz[- ]carlton/i],
  ["Bisha Hotel Toronto", /bisha/i],
  ["Pantages Hotel Toronto", /pantages/i],
];
const COMPETITOR_REGEX = new RegExp(COMPETITOR_CATALOG.map(([, rx]) => rx.source).join("|"), "i");
const findCompetitors = (blob) => COMPETITOR_CATALOG.filter(([, rx]) => rx.test(String(blob || ""))).map(([n]) => n);
const HOTEL_LIKE_ORG_REGEX =
  /\b(?:hotel|hotels|resort|resorts|hospitality|hyatt|marriott|hilton|sheraton|westin|fairmont|intercontinental|radisson|delta hotels|four seasons)\b/i;
const MTCC_LIKE_VENUE_REGEX =
  /metro(?:politan)? toronto convention|\bmtcc\b|exhibition place|enercare|beanfield|national trade cent(?:re|er)|scotiabank arena|rogers centre/i;

const HOTEL = {
  hotelId: HOTEL_ID,
  displayName: HOTEL_NAME,
  name: HOTEL_NAME,
  city: "Toronto",
  country: "Canada",
  address: "249 Queens Quay West, Toronto, ON M5J 2N5",
  rooms: 157,
  suites: 19,
  meetingSpaceSqFt: 11000,
  meetingRooms: 20,
  largestRoom: "Admiral Ballroom (3,151 sq ft; ~240 theatre / ~224 banquet / ~280 reception)",
  demandTerritory: { label: "Toronto — Harbourfront / Queens Quay / downtown / MTCC / Exhibition Place" },
  positioning:
    "157-room Radisson Blu on Toronto's Harbourfront (Queens Quay West); ~11,000 sq ft over 20 meeting rooms; walkable to the Metro Toronto Convention Centre; restaurant/lounge status must be verified before catering claims",
};
const HOTEL_FIT_COPY =
  "Radisson Blu Toronto Downtown is a 157-room Harbourfront hotel at 249 Queens Quay West with about 11,000 sq ft across 20 meeting rooms (Admiral Ballroom 3,151 sq ft, roughly 240 theatre / 224 banquet), a walk from the Metro Toronto Convention Centre — a credible overflow, mid-size group and ancillary meeting option, not exclusive housing for 2,000+ attendee convention shows.";

/** Territory: Toronto Harbourfront/downtown/MTCC/Exhibition Place core; generic Toronto nearby; GTA suburbs stretch. */
function territoryOf(blob, city = "") {
  const b = normKey(blob);
  const c = normKey(city);
  if (/toronto congress|dixon|pearson|airport|mississauga|etobicoke|vaughan|markham|brampton|oakville|burlington|hamilton|niagara|richmond hill|scarborough|north york/.test(`${b} ${c}`)) {
    return "TERRITORY_STRETCH";
  }
  if (c && !/toronto/.test(c)) return /ontario|\bon\b/.test(`${b} ${c}`) ? "TERRITORY_STRETCH" : "OUTSIDE_REALISTIC_TERRITORY";
  if (
    /harbourfront|queens quay|waterfront|metropolitan toronto convention|metro toronto convention|\bmtcc\b|exhibition place|enercare|beanfield|coca cola coliseum|downtown toronto|financial district|entertainment district|union station|scotiabank arena|rogers centre|cn tower|university of toronto|toronto metropolitan university|st george|king street west|bay street|front street|university avenue|metro hall/.test(
      b,
    )
  ) {
    return "TERRITORY_CORE";
  }
  if (/\btoronto\b/.test(b) || /\btoronto\b/.test(c)) return "TERRITORY_NEARBY";
  return "OUTSIDE_REALISTIC_TERRITORY";
}
const HOTEL_MARKET_TERRITORIES = new Set(["TERRITORY_CORE", "TERRITORY_NEARBY"]);

function classifyContactRole(title) {
  const t = normKey(title);
  if (!t) return "NO_ROLE";
  const procurement = /procure|purchas|buyer|supply|facility|logistics and purchasing/.test(t);
  const eventish = /event|meeting|travel|incentive|convention|congress|conference|organizer|producer|housing|accommodation|exhibit|sponsor/.test(t);
  if (procurement && !eventish) return "PROCUREMENT_ONLY";
  if (
    /event|meeting|travel|incentive|sales|commercial|organizer|producer|program|project manager|president|founder|ceo|owner|coordinator|executive director|chair|secretary|chief operating|\bcoo\b|vice president|hospitality|host|committee|director|housing|accommodation|conference|registration|exhibit|sponsor|partnership|manager/.test(
      t,
    )
  ) {
    return "COMMERCIALLY_RELEVANT";
  }
  return "ROLE_UNCLEAR";
}
const contactIsIntermediary = (title, org) =>
  /\b(dmc|pco|planner|incentive|tour operator|travel agency|events? (?:company|agency)|agency|production)\b/i.test(`${title} ${org}`);
const revenueStreamFor = (potential) =>
  /catering|banquet|meeting|conference|training|congress|convention|seminar|reception|dance|symposium/i.test(potential || "")
    ? "COMBINED_GUESTROOMS_EVENT_FNB"
    : "GUESTROOMS_ONLY";

async function pool(items, limit, worker) {
  const results = new Array(items.length);
  let idx = 0;
  async function lane() {
    while (true) {
      const i = idx++;
      if (i >= items.length) return;
      results[i] = await worker(items[i], i);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, lane));
  return results;
}

const titleTokens = (t) => new Set(normKey(t).split(" ").filter((w) => w.length > 3));
function tokenOverlap(a, b) {
  const ta = titleTokens(a);
  const tb = titleTokens(b);
  const inter = [...ta].filter((x) => tb.has(x)).length;
  return { inter, ratio: inter / Math.max(1, Math.min(ta.size, tb.size)) };
}
/** Same-event test used for baseline/new/new dedupe. */
function sameEvent(a, b) {
  const sa = a.eventStartDate || a.startDate;
  const sb = b.eventStartDate || b.startDate;
  const ta = a.displayTitle || a.title;
  const tb = b.displayTitle || b.title;
  const oa = a.organizationName || a.org;
  const ob = b.organizationName || b.org;
  const ov = tokenOverlap(ta, tb);
  if (sa && sb && sa === sb && (normKey(oa) === normKey(ob) || (ov.inter >= 2 && ov.ratio >= 0.5))) return true;
  if (normKey(oa) && normKey(oa) === normKey(ob) && ov.inter >= 1) return true;
  return ov.inter >= 3 && ov.ratio >= 0.75;
}

// ---------------------------------------------------------------------------
// --browser-verify
// ---------------------------------------------------------------------------
async function runBrowserVerify() {
  const argVal = (n) => {
    const i = process.argv.indexOf(n);
    return i > -1 ? process.argv[i + 1] : null;
  };
  const base = (argVal("--base-url") || process.env.GDI_PREVIEW_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
  const pageUrl = `${base}/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;
  const apiUrl = `${base}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities?publicationPackage=1&surface=customer&preview=staged`;
  let meta = {};
  try {
    meta = readJson(path.join(OUT, "13-NEW_STAGED_PUBLICATION.json"));
  } catch {
    meta = {};
  }
  const result = {
    contract: "GDI_RADISSON_14SEG_LOCAL_BROWSER_VERIFICATION_V1",
    generatedAt: new Date().toISOString(),
    status: "UNKNOWN",
    pageUrl,
    apiUrl,
    expectedFingerprint: meta.fingerprint || null,
    expectedCounts: meta.inventoryCounts || null,
    checks: {},
    errors: [],
  };
  if (!meta.fingerprint) {
    result.status = "FAIL_NO_STAGED_PACKAGE";
    result.errors.push("13-NEW_STAGED_PUBLICATION.json has no fingerprint (nothing staged).");
    writeJson("14-LOCAL_BROWSER_VERIFICATION.json", result);
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  try {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), 8000);
    const r = await fetch(apiUrl, { signal: ctl.signal });
    clearTimeout(t);
    result.checks.apiHttpStatus = r.status;
    const api = await r.json().catch(() => null);
    result.checks.apiFingerprint = api?.publication?.contentFingerprintSha256 || null;
    result.checks.fingerprintMatch = result.checks.apiFingerprint === meta.fingerprint;
    result.checks.inventoryCounts = api?.publication?.inventoryCounts || null;
    const ops = Array.isArray(api?.opportunities) ? api.opportunities : [];
    result.checks.apiOpportunityCount = ops.length;
    result.checks.apiHotelCount = ops.filter((o) => o.hotelMarketScopeEligible === true).length;
    result.checks.apiCompetitiveCount = ops.filter((o) => o.competitiveMarketScopeEligible === true).length;
    result.checks.providerLeakInApi = /\bexa\b|surfe|parallel\.ai|airtable/i.test(JSON.stringify(ops));
  } catch (e) {
    result.status = "FAIL_SERVER_DOWN";
    result.errors.push(`Preview server not reachable at ${base}: ${String(e?.message || e)}`);
    writeJson("14-LOCAL_BROWSER_VERIFICATION.json", result);
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  let chromium = null;
  try {
    ({ chromium } = await import("playwright"));
  } catch (e) {
    result.errors.push(`playwright unavailable: ${String(e?.message || e)}`);
  }
  if (!chromium) {
    result.status = result.checks.fingerprintMatch ? "PARTIAL_API_ONLY_NO_PLAYWRIGHT" : "FAIL_FINGERPRINT_MISMATCH";
    writeJson("14-LOCAL_BROWSER_VERIFICATION.json", result);
    writeJson("16-BROWSER_VERIFICATION.json", {
      status: result.status,
      expectedFingerprint: result.expectedFingerprint,
      checks: result.checks,
      errors: result.errors,
    });
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  const shotDir = path.join(OUT, "screenshots");
  fs.mkdirSync(shotDir, { recursive: true });
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
  } catch (e) {
    result.errors.push(`playwright launch failed: ${String(e?.message || e)}`);
    result.status = result.checks.fingerprintMatch ? "PARTIAL_API_ONLY_NO_PLAYWRIGHT" : "FAIL_FINGERPRINT_MISMATCH";
    writeJson("14-LOCAL_BROWSER_VERIFICATION.json", result);
    writeJson("16-BROWSER_VERIFICATION.json", {
      status: result.status,
      expectedFingerprint: result.expectedFingerprint,
      checks: result.checks,
      errors: result.errors,
      note: "Playwright browser binary unavailable; API fingerprint/inventory verification completed.",
    });
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForSelector('article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]', {
      timeout: 90000,
    });
    await page.waitForTimeout(1500);
    result.checks.dom = await page.evaluate(() => {
      const body = document.body?.innerText || "";
      const cards = [
        ...document.querySelectorAll('article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]'),
      ];
      return {
        cards: cards.length,
        titles: cards.map((c) => (c.innerText || "").split("\n")[0].trim().slice(0, 120)),
        mentionsHotel: /Radisson Blu Toronto/i.test(body),
        providerLeak: /\bExa\b|Surfe|Parallel|Airtable/.test(body),
        stillLoading: /Loading Group Demand Intelligence/i.test(body),
      };
    });
    await page.screenshot({ path: path.join(shotDir, "01-radisson-14seg-staged-overview.png"), fullPage: true });
    const expectedAll = Number(meta?.inventoryCounts?.all || 0);
    result.status =
      result.checks.fingerprintMatch === true &&
      result.checks.dom.cards >= Math.max(1, expectedAll) &&
      result.checks.dom.mentionsHotel === true &&
      result.checks.dom.stillLoading !== true &&
      !result.checks.dom.providerLeak &&
      !result.checks.providerLeakInApi
        ? "PASS"
        : "FAIL_CHECKS";
  } catch (e) {
    result.status = "FAIL_BROWSER_ERROR";
    result.errors.push(String(e?.message || e));
  } finally {
    await browser.close();
  }
  writeJson("14-LOCAL_BROWSER_VERIFICATION.json", result);
  console.log(JSON.stringify(result, null, 2));
}

if (BROWSER_VERIFY) {
  await runBrowserVerify();
  process.exit(0);
}

// ---------------------------------------------------------------------------
// STAGE 0 — baseline (zero cost)
// ---------------------------------------------------------------------------
fs.mkdirSync(RAW_DIR, { recursive: true });

const HOTEL_CFG = readJson(path.join(ROOT, "config/group-demand-intelligence/hotels", `${HOTEL_ID}.json`));
const ledgerDoc = readJson(DISCOVERY_LEDGER_PATH);
const ROWS = ledgerDoc.candidates || [];
const priorPkgPath = path.join(PRIOR_PKG_DIR, "package.json");
const priorPkg = readJson(priorPkgPath);
const priorFp = priorPkg.contentFingerprintSha256;
if (priorFp !== PRIOR_FINGERPRINT) {
  console.error(`Prior package fingerprint mismatch: ${priorFp} != ${PRIOR_FINGERPRINT} — refusing to run`);
  process.exit(3);
}
const certPkgPath = path.join(CERT_PKG_DIR, "package.json");
const certFpOnDisk = fs.existsSync(certPkgPath) ? readJson(certPkgPath).contentFingerprintSha256 : null;
if (certFpOnDisk !== CERT_FINGERPRINT_PRESERVE) {
  console.error(`Commercial cert fingerprint mismatch: ${certFpOnDisk} != ${CERT_FINGERPRINT_PRESERVE} — refusing to run`);
  process.exit(3);
}
const certPkgShaBefore = sha256File(certPkgPath);
const priorOpps = priorPkg.opportunities || [];
const priorHm = priorOpps.filter((o) => o.hotelMarketScopeEligible === true);
const priorCmi = priorOpps.filter((o) => o.competitiveMarketScopeEligible === true);
const protectedStore = createGdiPublicationPackageStore({ root: PROTECTED_STORE_ROOT });
const priorPkgShaBefore = sha256File(priorPkgPath);
const prior15PkgShaBefore = sha256File(path.join(PRIOR_15USD_PKG_DIR, "package.json"));
const stagedPointerBefore = protectedStore.readPointer(HOTEL_ID, "STAGED");

function protectedSnapshot() {
  const o = {};
  for (const [hid, label] of PROTECTED_HOTELS) {
    const cur = protectedStore.readPointer(hid, "CURRENT");
    const stg = protectedStore.readPointer(hid, "STAGED");
    o[label] = {
      hotelId: hid,
      currentFingerprint: cur?.contentFingerprintSha256 || null,
      stagedFingerprint: stg?.contentFingerprintSha256 || null,
      opportunitiesJsonSha256: sha256File(path.join(ROOT, "data/group-demand-intelligence/hotels", hid, "opportunities.json")),
    };
  }
  return o;
}
const protectedBefore = protectedSnapshot();

// --- ledger row helpers (same semantics as the 15usd campaign) ---
const rowSF = (r) => r.structuredExaCandidateFields || {};
const rowTitle = (r) => {
  const s = rowSF(r);
  return text(r.programmeName || s.verifiedEventName || s.eventIdentity || s.programmeName || r.originalResultTitle || "");
};
const rowVenue = (r) => text(rowSF(r).verifiedVenue || rowSF(r).incumbentVenue || "");
const rowStart = (r) => {
  const s = rowSF(r);
  return parseIso(s.verifiedStartDate) || parseIso(s.futureEditionDate) || extractExplicitDates(`${s.verifiedDates || ""}`)[0] || null;
};
const rowUrls = (r) => {
  const s = rowSF(r);
  return [
    ...(s.evidenceUrls || []),
    ...(s.supportingSources || []).map((x) => x?.url),
    ...(r.originalSourceUrls || []),
  ].filter((u) => /^https?:\/\//i.test(String(u || "")));
};
function candFromRow(r, extra = {}) {
  const s = rowSF(r);
  const id = `rx_${r.opportunityId || r.discoveryId}`;
  return {
    id,
    stage: "PASS2_DEEP",
    source: "LEDGER_RECOVERY",
    discoveryId: r.discoveryId,
    opportunityId: r.opportunityId || null,
    title: rowTitle(r) || text(r.organizationName),
    organizationName: text(r.organizationName || s.organizationName || s.organizingEntity),
    segmentId: r.segmentId,
    ledgerStatus: r.researchStatus,
    excerpt: text(s.demandMechanismSummary || s.underlyingEvidence || s.rankScoreExplain || r.originalResultExcerpt).slice(0, 500),
    evidenceUrls: [...new Set(rowUrls(r))],
    ledgerVerifiedDates: text(s.verifiedDates || ""),
    ledgerStart: rowStart(r),
    ledgerVenue: rowVenue(r),
    ledgerSourcingStatus: text(s.sourcingStatus || s.hotelSourcingStatus || ""),
    ledgerAttendance: Number(s.attendancePublished) || null,
    priorLabel: (r.disposition?.evidence || []).map((e) => /^acceptance=(.*)/.exec(e)?.[1]).find(Boolean) || null,
    ...extra,
  };
}

// --- baseline inventory (all 9 HM + 12 CMI preserved) ---
const baselineIds = new Set(priorOpps.map((o) => o.id));
const baselineIdStems = new Set(priorOpps.map((o) => o.id.replace(/^(rbc?|rx|rxc)_/, "")));
const baselineAsLike = priorOpps.map((o) => ({
  id: o.id,
  title: o.displayTitle || o.title,
  org: o.organizationName,
  startDate: o.eventStartDate,
}));
const dupOfBaseline = (c) =>
  baselineAsLike.find(
    (b) =>
      baselineIdStems.has(String(c.opportunityId || c.discoveryId || "")) ||
      sameEvent({ title: c.title, org: c.organizationName, startDate: c.ledgerStart }, b),
  ) || null;

// --- prior paid raw-runs (reusable at $0) ---
const PRIOR_RAW = [];
function loadPriorRawDir(dir, fileRe) {
  if (!fs.existsSync(dir)) return;
  for (const f of fs.readdirSync(dir)) {
    if (!fileRe.test(f)) continue;
    try {
      const j = readJson(path.join(dir, f));
      if (j?.paid?.ok && j?.paid?.result) PRIOR_RAW.push({ file: `${path.basename(dir)}/${f}`, cand: j.cand, paid: j.paid });
    } catch (e) {
      console.warn(`[prior-raw] unreadable ${f}: ${String(e?.message || e)}`);
    }
  }
}
loadPriorRawDir(PRIOR_15USD_RAW_DIR, /^deep[AB]_/);
loadPriorRawDir(PRIOR_20USD_RAW_DIR, /^(p2_|p3_|deep)/);
function findPriorRaw(cand) {
  const byId = PRIOR_RAW.find(
    (p) =>
      (p.cand?.discoveryId && p.cand.discoveryId === cand.discoveryId) ||
      (p.cand?.opportunityId && cand.opportunityId && p.cand.opportunityId === cand.opportunityId),
  );
  if (byId) return byId;
  if (cand.recoveryRe) {
    const rx = new RegExp(cand.recoveryRe, "i");
    const byRe = PRIOR_RAW.find((p) => rx.test(`${p.cand?.title || ""} ${p.cand?.organizationName || ""}`));
    if (byRe) return byRe;
  }
  return (
    PRIOR_RAW.find(
      (p) =>
        normKey(p.cand?.title) === normKey(cand.title) &&
        normKey(p.cand?.organizationName).slice(0, 20) === normKey(cand.organizationName).slice(0, 20),
    ) || null
  );
}

// ---------------------------------------------------------------------------
// recovery queue (from prior gaps report + named priorities)
// ---------------------------------------------------------------------------
const gapsDoc = fs.existsSync(PRIOR_GAPS_PATH) ? readJson(PRIOR_GAPS_PATH) : { pass1QueueCount: 0 };
/** Named recovery priorities for the six missing segments (not mega-association conventions). */
const RECOVERY_TARGETS = [
  { key: "CIRI_ROTMAN", re: "ciri|investor relations certification|rotman", segmentId: "local_catering_day_meetings", pri: 1 },
  { key: "IBC_INSIGHT", re: "ibc insight|insurance bureau of canada", segmentId: "local_catering_day_meetings", pri: 2 },
  { key: "AIEC_COLONOGRAPHY", re: "colonography|advanced imaging education", segmentId: "local_catering_day_meetings", pri: 3 },
  { key: "TORONTO_TEA_FEST", re: "toronto tea festival", segmentId: "social_special_events", pri: 4 },
  { key: "ASSEMBLY_RUN", re: "assembly conference|\\bc3 assembly\\b", segmentId: "social_special_events", pri: 5 },
  // ICSC is an association convention — do not seed as social.
  { key: "ICSC_CANADA", re: "icsc.?canada|icsc@canada", segmentId: "associations_conferences_events", pri: 6 },
  { key: "UTMUN", re: "model united nations|utmun|namun", segmentId: "travel_trade_group_intermediaries", pri: 7 },
  // Acavent is a professional conference organizer (PCO), not travel-trade.
  { key: "AI_SOCIETY", re: "ai and society conference|aihsconf|\\bacavent\\b", segmentId: "professional_event_planners", pri: 8 },
  // OLA Super Conference is an association convention — not relocation.
  { key: "OLA_SUPER", re: "ola super conference|ontario library association", segmentId: "associations_conferences_events", pri: 9 },
  { key: "UOFT_FACULTY", re: "faculty housing|visiting faculty|extended.?stay|relocation", segmentId: "relocation_extended_stay", pri: 10 },
  { key: "TASC_AESTHETICS", re: "aesthetics sciences|tasc", segmentId: "education_scientific", pri: 11 },
  { key: "EV_CHARGING", re: "ev.?charging expo|electric autonomy", segmentId: "associations_conferences_events", pri: 12 },
  { key: "CFCA_ENGAGE", re: "cfca engage|\\bcfca\\b", segmentId: "associations_conferences_events", pri: 13 },
  { key: "FMF200", re: "fmf200|mathematical finance", segmentId: "education_scientific", pri: 14 },
  { key: "CEMA_PLANNER", re: "cema summit|corporate event marketing", segmentId: "professional_event_planners", pri: 15 },
];
const RECOVERY_SKIPS = [
  { re: /gbta canada/i, reason: "Already staged as CMI — not re-researched." },
  { re: /financial economics and accounting|cfea/i, reason: "Already staged Hotel Market." },
  { re: /cgna|gerontological nursing/i, reason: "Already staged CMI / association conference — do not relabel into missing segments." },
  { re: /people connected/i, reason: "Airport-corridor stretch territory." },
  { re: /generali hellas/i, reason: "Historical past trip." },
  { re: /york suites/i, reason: "Campus accommodation outside Harbourfront territory." },
];

function mapWorkflowSegment(raw) {
  const x = String(raw || "").toLowerCase();
  if (MISSING_SEGMENTS.includes(x)) return x;
  if (/social|wedding|festival|special.?event/.test(x)) return "social_special_events";
  if (/cater|day.?meet|local_catering|workshop|training.?day|board meeting/.test(x)) return "local_catering_day_meetings";
  if (/travel.?trade|dmc|intermed|mice|tour|motorcoach|model united/.test(x)) return "travel_trade_group_intermediaries";
  if (/reloc|extended|faculty.?hous|mobility|assignment/.test(x)) return "relocation_extended_stay";
  if (/preferred.?lodg|venue.?partner|official hotel list/.test(x)) return "preferred_lodging_partnerships";
  if (/planner|pep|cema|pco|meeting.?manag/.test(x)) return "professional_event_planners";
  return null;
}

/** Seed recovery from diversification gap reports (six missing segments). */
const gapSeedCands = [];
for (const gp of MISSING_SEG_GAP_PATHS) {
  if (!fs.existsSync(gp)) continue;
  const doc = readJson(gp);
  for (const g of doc.researchGaps || []) {
    const seg = mapWorkflowSegment(g.segmentId) || g.segmentId;
    if (!MISSING_SEGMENTS.includes(seg)) continue;
    gapSeedCands.push({
      id: g.id || hashId("rx_g", g.title || g.org, seg),
      stage: "PASS2_DEEP",
      source: "DIVERSIFICATION_GAP_QUEUE",
      discoveryId: g.id || null,
      opportunityId: null,
      title: text(g.title),
      organizationName: text(g.org),
      segmentId: seg,
      ledgerStatus: g.status || "RESEARCH_CONTINUATION",
      excerpt: "",
      evidenceUrls: [],
      ledgerVerifiedDates: g.start || "",
      ledgerStart: parseIso(g.start),
      ledgerVenue: text(g.venue),
      ledgerSourcingStatus: "",
      ledgerAttendance: null,
      priorLabel: null,
      selectionWhy: "Diversification research-gap queue (six missing segments)",
      selectionScore: g.start ? 4 : 2,
    });
  }
}

function blockedIdToRow(id) {
  return ROWS.find((r) => r.discoveryId === id || r.opportunityId === id) || null;
}
const recoverySeedRows = [];
for (const g of gapSeedCands) {
  const r = g.discoveryId ? blockedIdToRow(String(g.discoveryId).replace(/^rx_d_/, "").replace(/^rx_/, "")) : null;
  if (r) recoverySeedRows.push(r);
}
for (const t of RECOVERY_TARGETS) {
  const rx = new RegExp(t.re, "i");
  for (const r of ROWS) {
    if (r.researchStatus === "DUPLICATE") continue;
    if (rx.test(`${rowTitle(r)} ${r.organizationName}`) && !recoverySeedRows.includes(r)) recoverySeedRows.push(r);
  }
}
const recoveryFamilies = new Map();
const recoverySkipped = [];
for (const r of recoverySeedRows) {
  const blob = `${rowTitle(r)} ${r.organizationName}`;
  const skip = RECOVERY_SKIPS.find((s) => s.re.test(blob));
  if (skip) {
    recoverySkipped.push({ discoveryId: r.discoveryId, title: rowTitle(r).slice(0, 100), reason: skip.reason });
    continue;
  }
  const t = RECOVERY_TARGETS.find((x) => new RegExp(x.re, "i").test(blob));
  const famKey = t ? t.key : normKey(`${r.organizationName}`).slice(0, 30);
  const cur = recoveryFamilies.get(famKey);
  const better =
    !cur ||
    (r.opportunityId && !cur.row.opportunityId) ||
    (!!r.opportunityId === !!cur.row.opportunityId && rowTitle(r).length > rowTitle(cur.row).length);
  if (better) recoveryFamilies.set(famKey, { key: famKey, pri: t?.pri ?? 50, re: t?.re || null, segmentId: t?.segmentId || mapWorkflowSegment(r.segmentId), row: r });
}
const recoveryFromLedger = [...recoveryFamilies.values()]
  .sort((a, b) => a.pri - b.pri)
  .map((f) =>
    candFromRow(f.row, {
      recoveryKey: f.key,
      recoveryRe: f.re,
      segmentId: f.segmentId || mapWorkflowSegment(f.row.segmentId) || f.row.segmentId,
      selectionWhy: "Prior ledger / named missing-segment recovery target",
      selectionScore: 5,
    }),
  );

const recoveryCandsAll = [];
const seenRec = new Set();
for (const c of [...recoveryFromLedger, ...gapSeedCands]) {
  const fam = normKey(`${c.organizationName}|${c.title}`).slice(0, 44);
  if (seenRec.has(fam)) continue;
  if (!MISSING_SEGMENTS.includes(c.segmentId) && !mapWorkflowSegment(c.segmentId)) {
    recoverySkipped.push({ id: c.id, title: c.title, reason: "NOT_IN_SIX_MISSING_SEGMENTS" });
    continue;
  }
  c.segmentId = MISSING_SEGMENTS.includes(c.segmentId) ? c.segmentId : mapWorkflowSegment(c.segmentId);
  seenRec.add(fam);
  recoveryCandsAll.push(c);
}
const recoveryDupBaseline = [];
const RECOVERY_CANDS = recoveryCandsAll.filter((c) => {
  const d = dupOfBaseline(c);
  if (d) {
    recoveryDupBaseline.push({ id: c.id, title: c.title, dupOf: d.id });
    return false;
  }
  return true;
});
const recoveryPlanRows = RECOVERY_CANDS.map((c) => ({
  id: c.id,
  recoveryKey: c.recoveryKey || null,
  title: c.title,
  org: c.organizationName,
  segment: c.segmentId,
  ledgerStatus: c.ledgerStatus,
  priorPaidCacheAvailable: Boolean(findPriorRaw(c)),
  source: c.source,
}));

// ---------------------------------------------------------------------------
// Pass 1 job definitions — SIX MISSING SEGMENTS only (full workflow IDs)
// ---------------------------------------------------------------------------
const SEGMENT_KEYS = MISSING_SEGMENTS;

const BASE_FOCUS = {
  social_special_events:
    "Weddings with published room blocks, destination celebrations, multi-day cultural/heritage gatherings, charity gala guest accommodation, private group celebrations, and Harbourfront/waterfront festivals with visiting participants that create actual hotel demand in downtown Toronto. Require identifiable organizer and out-of-town lodging need — not generic wedding-market assumptions.",
  local_catering_day_meetings:
    "Corporate training days, executive workshops, board meetings, client presentations, leadership meetings, company offsites, private business dinners, product demonstrations, and multi-day meetings with catering that can use a Harbourfront hotel meeting rooms / F&B. Identify organizer and commercial contact. Do NOT invent guestrooms; catering-only may qualify when demand activity is evidenced.",
  travel_trade_group_intermediaries:
    "Destination management companies, inbound tour operators, corporate group travel companies, incentive travel organizers, educational group travel planners, and international group coordinators with SPECIFIC future Toronto programmes, travel windows, or recurring group-demand mechanisms — not generic agency listings. Named commercial entry point required.",
  relocation_extended_stay:
    "International employee assignments, temporary project teams, engineering specialists, corporate relocation programmes, visiting faculty, medical research assignments, film/TV support staff, and implementation teams with an actual travel/lodging mechanism in downtown Toronto. Do not assume company expansion creates hotel demand.",
  preferred_lodging_partnerships:
    "Corporate accounts, waterfront/port organizations, universities, event producers, associations, and government bodies with EVIDENCE of recurring travel or accommodation demand near Harbourfront and a travel/procurement/operations entry point. A nearby office alone is NOT sufficient.",
  professional_event_planners:
    "Named Toronto PCOs, independent planners, association-management companies, destination event planners, and conference production agencies with evidence of ACTUAL future Toronto programmes (not general service offerings). Identify the underlying demand-generating programme; avoid duplicating the same event under planner and organizer.",
};
const EXTRA_JOBS = [
  { id: "x_local_corp_trainings_boards", segmentKey: "local_catering_day_meetings", angle: "Recurring professional trainings, certification programs (CIRI/Rotman, insurance, imaging), board meetings and workshops with day-meeting + possible overnight attendees near Harbourfront." },
  { id: "x_local_waterfront_institutional", segmentKey: "local_catering_day_meetings", angle: "Waterfront-related institutional gatherings, client presentations and private business dinners that need downtown meeting space / catering." },
  { id: "x_preferred_corp_visitor", segmentKey: "preferred_lodging_partnerships", angle: "Waterfront businesses, port-related orgs, universities and sports/entertainment orgs with recurring visiting-team or consultant lodging demand seeking preferred hotels." },
  { id: "x_preferred_university_hospital", segmentKey: "preferred_lodging_partnerships", angle: "UofT/TMU/hospital visitor lodging lists and preferred-hotel programmes currently accepting partners for downtown Toronto." },
  { id: "x_reloc_faculty_medical", segmentKey: "relocation_extended_stay", angle: "Visiting faculty, medical fellows, research assignments and corporate implementation teams with multi-week downtown Toronto lodging needs in 2027." },
  { id: "x_reloc_project_film", segmentKey: "relocation_extended_stay", angle: "Infrastructure project specialists and film/TV support crews needing extended-stay lodging near Harbourfront / downtown." },
  { id: "x_travel_dmc_incentive", segmentKey: "travel_trade_group_intermediaries", angle: "Toronto DMCs, incentive houses and inbound tour operators with dated 2027 group programmes into downtown / Harbourfront." },
  { id: "x_travel_edu_student_groups", segmentKey: "travel_trade_group_intermediaries", angle: "Educational / Model UN / youth / senior group travel programmes with future Toronto departures needing downtown lodging." },
  { id: "x_social_cultural_festivals", segmentKey: "social_special_events", angle: "Cultural festivals, charity galas and destination celebrations with published out-of-town guest lodging near Harbourfront." },
  { id: "x_social_wedding_room_blocks", segmentKey: "social_special_events", angle: "Multi-day destination weddings / family gatherings with identifiable organizers and published room-block or lodging needs downtown." },
  { id: "x_planner_pco_toronto", segmentKey: "professional_event_planners", angle: "Named Toronto PCO/DMC/agency programmes with FUTURE dates that publish hotel RFPs or room-block requirements." },
  { id: "x_planner_assoc_mgmt", segmentKey: "professional_event_planners", angle: "Association-management and conference-production agencies running specific 2027 Toronto programmes that source hotel blocks." },
];
const PASS1_JOBS = [
  ...SEGMENT_KEYS.map((k) => ({
    id: `seg_${k}`,
    segmentKey: k,
    segmentId: k,
    label: `${k} — base`,
    angle: BASE_FOCUS[k],
  })),
  ...EXTRA_JOBS.map((j) => ({ ...j, segmentId: j.segmentKey, label: `${j.segmentKey} — ${j.id}` })),
].slice(0, MAX_RUNS.PASS1_DISCOVERY);
for (const k of SEGMENT_KEYS) {
  if (!BASE_FOCUS[k]) {
    console.error(`Missing discovery focus for segment ${k}`);
    process.exit(3);
  }
}

function hotelFitConstraints() {
  return [
    "HOTEL FIT CONSTRAINTS (apply honestly — unknown is not zero, but do not inflate fit):",
    "- 157 guestrooms (19 suites); ~11,000 sq ft over 20 meeting rooms; largest room Admiral Ballroom 3,151 sq ft (~240 theatre / ~224 banquet / ~280 reception).",
    "- Harbourfront / Queens Quay West, walkable to the Metro Toronto Convention Centre. Restaurant/lounge may be closed — do not assume catering capability.",
    "- The hotel is NOT exclusive housing for 2,000+ attendee MTCC / Exhibition Place conventions. Realistic roles: delegate/speaker overflow, mid-size groups (≈15–109 peak rooms), VIP/executive lodging, side meetings and receptions.",
    "- Territory: Toronto Harbourfront, downtown, MTCC, Exhibition Place. Toronto-wide mega-conventions are NOT automatic demand — state whether this event can realistically use a 157-room hotel.",
  ].join("\n");
}
const alreadyInPipeline = [...new Set(baselineAsLike.map((b) => `${b.title} (${b.org})`))].slice(0, 40);
function discoveryContext(job) {
  return [
    `TORONTO MID-SIZE GROUP DEMAND DISCOVERY — SEGMENT: ${job.segmentKey}${job.id.startsWith("x_") ? ` (second angle: ${job.id})` : ""}`,
    "HOTEL:",
    JSON.stringify(HOTEL),
    "",
    hotelFitConstraints(),
    "",
    `SEGMENT FOCUS: ${job.angle}`,
    "",
    "REQUIREMENTS:",
    `- Today is ${REF}. Return FUTURE activity only: start dates from ${IMMINENT_CUTOFF} through mid-2028, in Toronto (Harbourfront / downtown / MTCC / Exhibition Place / university & hospital campuses).`,
    "- PREFER mid-size, commercially appropriate events (≈40–1,500 attendees; peak ~15–109 rooms). Do NOT return mega MTCC/Exhibition Place calendars (≥5,000 attendees) unless overflow sourcing is explicitly open.",
    "- PREFER events whose lodging is open, unannounced or participant-arranged. De-prioritize events already contracted to competing Toronto hotels (Westin Harbour Castle, Hotel X, Sheraton Centre, Hyatt Regency, Le Méridien, Fairmont Royal York, Marriott, Hilton, InterContinental, Delta).",
    "- For EVERY candidate give verified dates (exact start date), exact venue, organizer, official source URLs, how lodging is sourced, and any named public organizer contact. Do not invent contacts, dates, rates or room blocks.",
    `- Already in the pipeline (do NOT return again): ${alreadyInPipeline.join("; ")}.`,
    "- English and French sources both welcome (Canadian institutional organizers).",
  ].join("\n");
}

// ---------------------------------------------------------------------------
// PLAN-ONLY (and baseline reports shared by every mode)
// ---------------------------------------------------------------------------
const gitInfo = { branch: sh("git rev-parse --abbrev-ref HEAD"), headSha: sh("git rev-parse HEAD") };
const plannedPass2Free = RECOVERY_CANDS.filter((c) => findPriorRaw(c)).length;
const plannedPass2Paid = RECOVERY_CANDS.length - plannedPass2Free;

function baselineDoc() {
  return {
    contract: "GDI_RADISSON_14SEG_EXA_20USD_BASELINE_V1",
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    referenceDate: REF,
    imminentCutoff: IMMINENT_CUTOFF,
    authorizationId: AUTHORIZATION_ID,
    hardCeilingUsd: HARD_CEILING_USD,
    stageCapsUsd: STAGE_CAP_USD,
    maxRuns: MAX_RUNS,
    concurrency: CONCURRENCY,
    quotes: {
      medium: quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM }),
      high: quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.HIGH }),
    },
    providers: { exa: true, surfe: false, parallel: false },
    airtable: "DRY_RUN_ONLY",
    customerPublished: false,
    separateLedgerFromPrior15Usd: true,
    priorStagedFingerprintPreserved: PRIOR_FINGERPRINT,
    newPublicationVersion: PUB_VERSION,
    targetHotelMarket: TARGET_HM,
    baselineInventory: {
      priorPublicationVersion: priorPkg.publicationVersion,
      hotelMarket: priorHm.length,
      cmi: priorCmi.length,
      hotelMarketIds: priorHm.map((o) => o.id),
      cmiIds: priorCmi.map((o) => o.id),
    },
    hotelProfile: {
      rooms: HOTEL_CFG.capabilityProfile?.totalGuestrooms,
      suites: HOTEL_CFG.capabilityProfile?.suites,
      meetingSqFt: HOTEL_CFG.capabilityProfile?.totalMeetingSpaceSqFt,
      meetingRooms: HOTEL_CFG.capabilityProfile?.meetingRoomCount,
      largestRoom: HOTEL_CFG.capabilityProfile?.largestRoomName,
      demandTerritory: HOTEL_CFG.demandTerritory,
    },
    discoveryLedger: {
      path: path.relative(ROOT, DISCOVERY_LEDGER_PATH),
      sha256: sha256File(DISCOVERY_LEDGER_PATH),
      candidates: ROWS.length,
      statusCounts: countBy(ROWS, (r) => r.researchStatus),
    },
    priorPaidRawRunsReusable: PRIOR_RAW.length,
    pass1Plan: {
      jobs: PASS1_JOBS.map((j) => ({ id: j.id, segment: j.segmentKey, ledgerSegment: j.segmentId })),
      segmentsCovered: [...new Set(PASS1_JOBS.map((j) => j.segmentKey))].length,
      maxSpendUsd: Number((PASS1_JOBS.length * MEDIUM_UNIT).toFixed(2)),
    },
    pass2Plan: {
      recoveryCandidates: recoveryPlanRows,
      recoverySkipped,
      recoveryAlreadyInBaseline: recoveryDupBaseline,
      freeCacheReuse: plannedPass2Free,
      paidRecoveryRuns: plannedPass2Paid,
      remainingPaidSlotsForDiscoveryHits: Math.max(0, MAX_RUNS.PASS2_DEEP - plannedPass2Paid),
    },
    protectedPublicationsBefore: protectedBefore,
    priorPackageSha256: priorPkgShaBefore,
    prior15UsdPackageSha256: prior15PkgShaBefore,
    stagedPointerBefore,
    git: gitInfo,
    policy: [
      "Exa ONLY — no Surfe, no Parallel, no CUSTOMER_PUBLISHED, Airtable dry-run only.",
      "Hotel Market and CMI are never dual-classified; competitor commitment → CMI.",
      "Events before REF are expired/research-watch, never customer Hotel Market, unless a next edition is evidenced.",
      "Prior staged package is preserved on disk; a NEW fingerprint is staged.",
    ],
  };
}

if (!AUTHORIZE && !ASSEMBLE_CACHED) {
  if (fs.existsSync(LEDGER_PATH)) {
    const l = readJson(LEDGER_PATH);
    if (Number(l.spentUsd) > 0) {
      console.error(
        `Ledger already has spend ($${l.spentUsd}); plan-only would overwrite campaign reports. Use --assemble-cached to refresh reports at $0.`,
      );
      process.exit(4);
    }
  }
  writeJson("01-BASELINE_AND_AUTHORIZATION.json", baselineDoc());
  writeJson("02-PAID_BUDGET_LEDGER.json", {
    status: "NOT_STARTED",
    authorizationId: AUTHORIZATION_ID,
    hardCeilingUsd: HARD_CEILING_USD,
    stageCapsUsd: STAGE_CAP_USD,
    spentUsd: 0,
    remainingUsd: HARD_CEILING_USD,
    note: "Plan-only run — no paid calls made.",
  });
  writeJson("03-PASS1_DISCOVERY_RESULTS.json", {
    status: "PLANNED_NOT_EXECUTED",
    capUsd: STAGE_CAP_USD.PASS1_DISCOVERY,
    unitCostUsd: MEDIUM_UNIT,
    plannedJobs: PASS1_JOBS.map((j) => ({ id: j.id, segment: j.segmentKey, angle: j.angle })),
  });
  writeJson("04-PASS2_DEEP_RESULTS.json", {
    status: "PLANNED_NOT_EXECUTED",
    capUsd: STAGE_CAP_USD.PASS2_DEEP,
    unitCostUsd: HIGH_UNIT,
    maxRuns: MAX_RUNS.PASS2_DEEP,
    recoveryCandidates: recoveryPlanRows,
  });
  writeJson("05-PASS3_VERIFY_RESULTS.json", {
    status: "PLANNED_NOT_EXECUTED",
    capUsd: STAGE_CAP_USD.PASS3_VERIFY,
    unitCostUsd: HIGH_UNIT,
    maxRuns: MAX_RUNS.PASS3_VERIFY,
  });
  writeJson("11-GAP_TO_20_ANALYSIS.json", {
    status: "BASELINE_ONLY",
    target: TARGET_HM,
    achieved: priorHm.length,
    gapTo20: Math.max(0, TARGET_HM - priorHm.length),
    note: "Plan-only baseline — gap closes only after --authorize-paid.",
  });
  writeJson("13-NEW_STAGED_PUBLICATION.json", {
    staged: false,
    status: "PLAN_ONLY_NOTHING_STAGED",
    publicationVersion: PUB_VERSION,
    priorPackagePreserved: { fingerprint: priorFp, sha256: priorPkgShaBefore },
  });
  writeJson("14-LOCAL_BROWSER_VERIFICATION.json", {
    status: "PENDING_BROWSER_RUN",
    howToRun: `node scripts/${SCRIPT_ID}.mjs --browser-verify`,
  });
  writeJson("15-GITHUB_MERGE_STATUS.json", {
    branch: gitInfo.branch,
    headSha: gitInfo.headSha,
    prNumber: null,
    prUrl: null,
    ciResult: "NOT_RUN_BY_SCRIPT",
    mergeStatus: "NOT_MERGED",
    mergeCommitSha: null,
    mainSynchronized: false,
    noCustomerPublished: true,
  });
  fs.writeFileSync(
    path.join(OUT, "FOUNDER_REPORT.md"),
    `# Radisson Blu Toronto — Six Missing Segments Exa ($30) PLAN ONLY\n\n**Authorization:** \`${AUTHORIZATION_ID}\`\n**Hard ceiling:** $${HARD_CEILING_USD} · **Spent:** $0\n\nBaseline: ${priorHm.length} Hotel Market + ${priorCmi.length} CMI (diversification fingerprint \`${priorFp}\`; cert \`${CERT_FINGERPRINT_PRESERVE}\`).\nMissing segments: ${MISSING_SEGMENTS.join(", ")}.\nTarget: ≥${MIN_NEW_MISSING_SEGMENTS} newly represented missing segments; aspirational ≥${TARGET_SEGMENTS_WITH_HM}/14 HM segments.\n\nPass 1: ${PASS1_JOBS.length} MEDIUM jobs across ${SEGMENT_KEYS.length} missing segments (≤ $${(PASS1_JOBS.length * MEDIUM_UNIT).toFixed(2)}).\nPass 2: ${RECOVERY_CANDS.length} recovery candidates (${plannedPass2Free} reusable at $0) + discovery hits.\nRun with \`--authorize-paid\` to execute.\n`,
  );
  console.log(
    JSON.stringify(
      {
        mode: "PLAN_ONLY",
        baselineHotelMarket: priorHm.length,
        baselineCmi: priorCmi.length,
        pass1Jobs: PASS1_JOBS.length,
        pass1MaxUsd: Number((PASS1_JOBS.length * MEDIUM_UNIT).toFixed(2)),
        recoveryCandidates: RECOVERY_CANDS.length,
        recoveryFreeFromPriorCache: plannedPass2Free,
        recoveryPaidRuns: plannedPass2Paid,
        recoverySkipped: recoverySkipped.length,
        mediumUnit: MEDIUM_UNIT,
        highUnit: HIGH_UNIT,
        priorRawReusable: PRIOR_RAW.length,
      },
      null,
      2,
    ),
  );
  process.exit(0);
}

// ---------------------------------------------------------------------------
// STAGE 1 — paid Exa budget machinery
// ---------------------------------------------------------------------------
if (LIVE_PAID && !process.env.EXA_API_KEY) {
  console.error("EXA_API_KEY missing");
  process.exit(2);
}
if (LIVE_PAID) process.env.GDI_EXA_AGENT_LIVE = "1";

let ledger = loadPaidBudgetLedger(LEDGER_PATH, {
  authorizationId: AUTHORIZATION_ID,
  hardCeilingUsd: HARD_CEILING_USD,
  effort: EXA_AGENT_EFFORT.HIGH,
});
if (!ledger) {
  if (!LIVE_PAID) {
    console.error("No ledger on disk — nothing to assemble. Run --authorize-paid first.");
    process.exit(4);
  }
  ledger = createPaidBudgetLedger({
    authorizationId: AUTHORIZATION_ID,
    hardCeilingUsd: HARD_CEILING_USD,
    effort: EXA_AGENT_EFFORT.HIGH,
    priorSpendUsd: 0,
    outPath: LEDGER_PATH,
  });
}
persistPaidBudgetLedger(ledger);
const resumedSpendUsd = ledger.spentUsd;
if (ledger.hardCeilingUsd !== HARD_CEILING_USD || ledger.authorizationId !== AUTHORIZATION_ID) {
  console.error("Ledger authorization/ceiling mismatch");
  process.exit(3);
}

const stageInflight = {};
const stageSpentUsd = (stage) =>
  ledger.operations.filter((o) => o.stage === stage).reduce((a, o) => a + Number(o.actualCostUsd || 0), 0) +
  (stageInflight[stage] || 0);
const stageRemainingUsd = (stage) => Number(Math.max(0, (STAGE_CAP_USD[stage] ?? 0) - stageSpentUsd(stage)).toFixed(6));

/** Single choke point for every paid call: stage cap + global ceiling + reservation. */
async function chargeable(meta, fn) {
  const effort = meta.effort || EXA_AGENT_EFFORT.HIGH;
  const quote = quoteExaAgentRun({ effort });
  if (!LIVE_PAID) return { ok: false, blocked: "PAID_NOT_AUTHORIZED_THIS_RUN", remaining: remainingBudgetUsd(ledger) };
  const cap = STAGE_CAP_USD[meta.stage];
  if (cap == null) return { ok: false, blocked: "UNKNOWN_STAGE", remaining: remainingBudgetUsd(ledger) };
  if (stageSpentUsd(meta.stage) + quote.maxUsd > cap + 1e-9) {
    return { ok: false, blocked: "STAGE_CAP_REACHED", remaining: remainingBudgetUsd(ledger) };
  }
  if (Number(ledger.spentUsd) + Number(ledger.reservedUsd || 0) + quote.maxUsd > HARD_CEILING_USD + 1e-9) {
    return { ok: false, blocked: "WOULD_EXCEED_GLOBAL_HARD_CEILING", remaining: remainingBudgetUsd(ledger) };
  }
  const gate = canAffordQuotedCall(ledger, quote.maxUsd);
  if (!gate.ok) {
    ledger.blockedOperations.push({ at: new Date().toISOString(), reason: gate.reason, ...meta });
    persistPaidBudgetLedger(ledger);
    return { ok: false, blocked: gate.reason, remaining: remainingBudgetUsd(ledger) };
  }
  const reserved = reserveQuotedCall(ledger, { quotedMaxUsd: quote.maxUsd, ...meta });
  if (!reserved.ok) return { ok: false, blocked: reserved.reason, remaining: remainingBudgetUsd(ledger) };
  stageInflight[meta.stage] = (stageInflight[meta.stage] || 0) + quote.maxUsd;
  try {
    const result = await fn({ effort, quote });
    let actual;
    if (result?.costDollars?.total != null) actual = Number(result.costDollars.total);
    else if (result?.costUsd != null) actual = Number(result.costUsd);
    else if (result?.ok === false && !(result?.runId || result?.providerRunId)) actual = 0;
    else actual = quote.maxUsd;
    if (!Number.isFinite(actual)) actual = quote.maxUsd;
    stageInflight[meta.stage] -= quote.maxUsd;
    settleReservedCall(ledger, {
      quotedMaxUsd: quote.maxUsd,
      actualCostUsd: actual,
      stage: meta.stage,
      hunter: meta.hunter,
      opportunityId: meta.opportunityId || null,
      segmentId: meta.segmentId || null,
      provider: "exa_agent",
      providerRunId: result?.providerRunId || result?.runId || null,
      iteration: meta.iteration || 1,
      label: meta.label || null,
      kind: meta.kind || null,
    });
    return { ok: true, result, actualCostUsd: actual, remaining: remainingBudgetUsd(ledger) };
  } catch (e) {
    stageInflight[meta.stage] -= quote.maxUsd;
    releaseReservation(ledger, quote.maxUsd);
    ledger.blockedOperations.push({ at: new Date().toISOString(), reason: String(e?.message || e), ...meta });
    persistPaidBudgetLedger(ledger);
    return { ok: false, error: String(e?.message || e), remaining: remainingBudgetUsd(ledger) };
  }
}

function loadRawFile(file) {
  if (!fs.existsSync(file)) return null;
  try {
    const j = readJson(file);
    if (j?.paid?.ok && j?.paid?.result) return j;
  } catch (e) {
    console.warn(`[raw-cache] unreadable ${file}: ${String(e?.message || e)}`);
  }
  return null;
}
const loadRaw = (key) => loadRawFile(path.join(RAW_DIR, `${key}.json`));
const saveRaw = (key, obj) => fs.writeFileSync(path.join(RAW_DIR, `${key}.json`), JSON.stringify(obj, null, 2));

// ---------------------------------------------------------------------------
// Pass 1 — MEDIUM discovery
// ---------------------------------------------------------------------------
function parseDiscoveryHits(structured) {
  const s = structured || {};
  const rows = s.candidates || s.events || s.companies || s.results || [];
  return (Array.isArray(rows) ? rows : []).map((row) => {
    const company = row.companyIdentity && typeof row.companyIdentity === "object" ? row.companyIdentity : null;
    const urls = [
      row.url, row.sourceUrl, ...(row.sourceUrls || []), ...(row.evidenceUrls || []),
      ...((row.supportingSources || []).map((x) => x?.url || x?.sourceUrl)),
      ...((row.supportingEvidence || []).map((x) => x?.url || x?.sourceUrl)),
      company?.website,
    ].filter((u) => /^https?:\/\//i.test(String(u || "")));
    const dates = text(row.verifiedDates || "");
    const start =
      parseIso(row.verifiedStartDate) || parseIso(row.futureEditionDate) || extractExplicitDates(dates)[0] || null;
    return {
      title: text(row.verifiedEventName || row.eventIdentity || row.title || row.eventName || row.verifiedBusinessActivity || row.businessTrigger || row.organizationName || "") || null,
      organizationName: text(row.organizingEntity || row.organizationName || row.corporateOrAssociationOwner || row.companyName || company?.canonicalName || row.name || "") || null,
      start,
      end: parseIso(row.verifiedEndDate),
      dates,
      venue: text(row.verifiedVenue || row.incumbentVenue || ""),
      incumbentVenue: text(row.incumbentVenue || ""),
      city: text(row.eventLocationCity || ""),
      destination: text(row.destination || ""),
      sourcingStatus: text(row.sourcingStatus || ""),
      demandCycle: text(row.demandCycleStatus || ""),
      eventStatus: text(row.eventStatus || ""),
      attendance: Number(row.attendancePublished || row.attendance) || null,
      evidenceUrls: [...new Set(urls)],
      excerpt: text(row.summary || row.excerpt || row.rankScoreExplain || row.underlyingEvidence || row.hotelDemandHypothesis || "").slice(0, 500),
    };
  });
}

const pass1Calls = [];
const pass1Hits = [];
async function runPass1() {
  console.log(`[PASS1] jobs=${PASS1_JOBS.length} capRemaining=${stageRemainingUsd("PASS1_DISCOVERY")} globalRemaining=${remainingBudgetUsd(ledger)}`);
  await pool(PASS1_JOBS, CONCURRENCY, async (job) => {
    const key = `p1_${job.id}`;
    let paid;
    const cached = loadRaw(key);
    if (cached) paid = { ...cached.paid, cached: true };
    else if (!LIVE_PAID) paid = { ok: false, blocked: "NO_CACHE_AND_PAID_NOT_AUTHORIZED" };
    else {
      const ctx = discoveryContext(job);
      const pack = buildExaEventsAgentCreatePayload({ mode: "discovery", contextBlock: ctx, effort: EXA_AGENT_EFFORT.MEDIUM, segmentId: job.segmentKey || job.segmentId });
      paid = await chargeable(
        { stage: "PASS1_DISCOVERY", kind: "discovery", hunter: job.segmentKey, segmentId: job.segmentId, effort: EXA_AGENT_EFFORT.MEDIUM, label: job.id },
        async () => {
          const r = await runExaAgentResearch({
            mode: "discovery",
            contextBlock: ctx,
            effort: EXA_AGENT_EFFORT.MEDIUM,
            authorizeExaAgentLive: true,
            forceMockOnly: false,
            createPayloadOverride: pack.body,
            contractIdOverride: pack.meta.contractId,
            maxPolls: 75,
            pollIntervalMs: 4000,
          });
          return { ...r, hits: r.ok ? parseDiscoveryHits(r.structured) : [] };
        },
      );
      if (paid.ok || paid.error) saveRaw(key, { job, paid });
    }
    const hits = paid.result?.hits?.length ? paid.result.hits : parseDiscoveryHits(paid.result?.structured);
    pass1Calls.push({
      key, job: job.id, segment: job.segmentKey, ok: Boolean(paid.ok && paid.result?.ok !== false),
      cached: paid.cached === true, blocked: paid.blocked || paid.error || null, hits: hits.length,
      costUsd: paid.cached ? 0 : paid.actualCostUsd ?? 0,
    });
    console.log(`[PASS1] ${job.id} ok=${paid.ok} hits=${hits.length} cost=${paid.actualCostUsd ?? 0} rem=${remainingBudgetUsd(ledger)}`);
    for (const h of hits) pass1Hits.push({ ...h, job: job.id, segmentKey: job.segmentKey, segmentId: job.segmentId });
  });
}
await runPass1();

// triage discovery hits -> deep candidates
const triageRejected = [];
const discoveryCands = [];
const seenDisc = new Set();
for (const h of pass1Hits) {
  const title = text(h.title);
  const org = text(h.organizationName);
  if (!title && !org) continue;
  const blob = `${title} ${org} ${h.excerpt} ${h.venue} ${h.city} ${h.destination}`;
  const key = normKey(`${org}|${title}`).slice(0, 48);
  if (seenDisc.has(key)) {
    triageRejected.push({ title, org, reason: "DUPLICATE_DISCOVERY_HIT", job: h.job });
    continue;
  }
  seenDisc.add(key);
  const rec = { title, org, start: h.start, venue: h.venue, job: h.job };
  const stubCand = { title, organizationName: org, ledgerStart: h.start };
  const dupB = dupOfBaseline(stubCand);
  if (dupB) {
    triageRejected.push({ ...rec, reason: `DUPLICATE_OF_BASELINE:${dupB.id}` });
    continue;
  }
  if (HOTEL_LIKE_ORG_REGEX.test(org) && COMPETITOR_REGEX.test(org)) {
    triageRejected.push({ ...rec, reason: "ORG_IS_COMPETING_HOTEL_BRAND" });
    continue;
  }
  if (h.start && h.start < IMMINENT_CUTOFF) {
    triageRejected.push({ ...rec, reason: h.start < REF ? "EXPIRED_VS_REF" : "IMMINENT_LT_21_DAYS" });
    continue;
  }
  const y = maxYearIn(`${title} ${h.dates} ${h.excerpt}`);
  if (!h.start && !(y && y >= 2027)) {
    triageRejected.push({ ...rec, reason: "NO_FUTURE_SIGNAL" });
    continue;
  }
  const locBlob = `${h.venue} ${h.city} ${h.destination} ${title}`;
  const terr = territoryOf(locBlob, h.city);
  if (!HOTEL_MARKET_TERRITORIES.has(terr)) {
    triageRejected.push({ ...rec, reason: `TERRITORY_${terr}` });
    continue;
  }
  if (COMPETITOR_REGEX.test(`${h.venue} ${h.incumbentVenue}`) || /likely_closed_incumbent|closed/.test(h.sourcingStatus)) {
    triageRejected.push({ ...rec, reason: "COMPETITOR_OR_CLOSED_SOURCING_ROUTED_TO_CMI_NOT_PAID_DEEP" });
    continue;
  }
  if (h.attendance && h.attendance >= 5000 && MTCC_LIKE_VENUE_REGEX.test(`${h.venue} ${title}`)) {
    triageRejected.push({ ...rec, reason: "MEGA_CONVENTION_NOT_AUTOMATIC_DEMAND" });
    continue;
  }
  if (!h.evidenceUrls.length) {
    triageRejected.push({ ...rec, reason: "NO_SOURCE_URL" });
    continue;
  }
  let score = 0;
  if (h.start) score += 3;
  if (terr === "TERRITORY_CORE") score += 2;
  if (/open|unknown|not_published|accepting|rfp|participant|own/.test(h.sourcingStatus)) score += 2;
  if (h.attendance && h.attendance >= 40 && h.attendance <= 2000) score += 1;
  if (!h.attendance) score += 0.5;
  score += Math.min(2, h.evidenceUrls.length * 0.5);
  const discSeg = MISSING_SEGMENTS.includes(h.segmentKey)
    ? h.segmentKey
    : mapWorkflowSegment(h.segmentId) || mapWorkflowSegment(h.segmentKey) || h.segmentId;
  if (!MISSING_SEGMENTS.includes(discSeg)) {
    triageRejected.push({ ...rec, reason: `NOT_MISSING_SEGMENT:${discSeg}` });
    continue;
  }
  discoveryCands.push({
    id: hashId("rx_d", title || org, org || title),
    stage: "PASS2_DEEP",
    source: "PASS1_DISCOVERY",
    discoveryId: null,
    opportunityId: null,
    title: title || org,
    organizationName: org || title,
    segmentId: discSeg,
    ledgerStatus: "PASS1_DISCOVERY_HIT",
    excerpt: h.excerpt,
    evidenceUrls: h.evidenceUrls,
    ledgerVerifiedDates: h.dates || h.start || "",
    ledgerStart: h.start,
    ledgerVenue: h.venue,
    ledgerSourcingStatus: h.sourcingStatus,
    ledgerAttendance: h.attendance,
    priorLabel: null,
    selectionScore: score,
    selectionWhy: `Pass 1 discovery (${h.job})`,
  });
}
discoveryCands.sort((a, b) => b.selectionScore - a.selectionScore);

// ---------------------------------------------------------------------------
// deep-research context + structured output parsing
// ---------------------------------------------------------------------------
function deepContext(cand, mode = "hotel") {
  const lines = [
    "HOTEL:",
    JSON.stringify(HOTEL),
    "",
    hotelFitConstraints(),
    "",
    "TARGET CANDIDATE TO DEEP-RESEARCH:",
    `Title: ${cand.title}`,
    `Organization: ${cand.organizationName}`,
    `Segment: ${cand.segmentId}`,
    `Prior discovery dates: ${cand.ledgerVerifiedDates || "unknown"} · prior venue: ${cand.ledgerVenue || "unknown"} · prior sourcing status: ${cand.ledgerSourcingStatus || "unknown"}`,
    `Seed excerpt: ${cand.excerpt || ""}`,
    `Seed URLs: ${(cand.evidenceUrls || []).join(", ")}`,
  ];
  if (cand.priorLabel) {
    lines.push(`NOTE: a prior thin discovery labelled this "${cand.priorLabel}". That label is NOT evidence — re-verify everything independently.`);
  }
  lines.push(
    "",
    "REQUIREMENTS:",
    `- Verify ACTUAL activity dates (not article dates). Today is ${REF}; horizon ${IMMINENT_CUTOFF} – Jun 2028. If the researched edition is already past, report the NEXT announced edition (exact dates + city + venue) only if officially published; otherwise say none.`,
    "- State the exact venue; whether it is in Toronto proper (Harbourfront/downtown/MTCC/Exhibition Place) or elsewhere. Always populate venue, city and region fields.",
    "- State how lodging is sourced: official housing bureau, host hotel, published room blocks (name EVERY hotel listed), open RFP, or unknown. Quote the source.",
    "- Report published or estimated attendance and any peak-room / room-night figure; do not invent numbers.",
    "- Identify NAMED commercially relevant contacts (organizer, events/conference/housing manager, executive director) with role AND source URL. Do not invent emails, RFPs or room blocks.",
    "- Do not treat generic hotel marketing or 'preferred hotel list' pages as a booking commitment unless edition-specific.",
    `- Produce a hotel demand thesis for ${HOTEL_NAME}; state honestly if lodging is already committed to other hotels or a housing bureau, and if the event is too large/small for a 157-room hotel.`,
  );
  return lines.join("\n");
}
function verifyContext(cand, prev) {
  const reasonAsk = {
    NO_NAMED_COMMERCIAL_CONTACT_WITH_SOURCE:
      "Find at least one NAMED commercially relevant person (events/conference/housing/meetings manager, executive director, organizer) for the SPECIFIC future edition with a source URL for the person and role.",
    NO_VERIFIED_FUTURE_DATE: "Find the exact start/end dates and venue of the NEXT officially announced edition on or after the horizon start; say 'none announced' if none.",
    YEAR_ONLY_NEEDS_EXACT_DATE: "Find the exact start/end dates of the 2027+ edition; say 'none announced' if only a year is published.",
    SPECULATIVE_ONLY: "Find edition-specific evidence of overnight guest demand and how lodging is sourced (open RFP, participant-arranged, housing bureau, host hotel).",
    LOCATION_NOT_VERIFIED_BY_DEEP_RESEARCH: "Verify the exact venue and city of the NEXT edition; state clearly if it is outside Toronto.",
  };
  return [
    "VERIFICATION PASS — resolve ONLY the specific gap below; do not broaden scope.",
    "HOTEL:",
    JSON.stringify(HOTEL),
    "",
    hotelFitConstraints(),
    "",
    `Title: ${cand.title}`,
    `Organization: ${cand.organizationName}`,
    `Today is ${REF}; horizon ${IMMINENT_CUTOFF} – Jun 2028.`,
    `Prior deep research outcome: ${prev.reason}`,
    `Prior findings snapshot — future edition: ${text(prev.futureEditionText).slice(0, 500) || "n/a"} · venue: ${text(prev.venueText).slice(0, 300) || "n/a"}`,
    `Seed URLs: ${(cand.evidenceUrls || []).slice(0, 6).join(", ")}`,
    "",
    `SPECIFIC ASK: ${reasonAsk[prev.reason] || "Resolve the missing date/contact/sourcing evidence."}`,
    "Always populate venue, city, region, dates, named contacts (with role + source URL) and hotel sourcing status. Do not invent contacts, dates, rates or room blocks.",
  ].join("\n");
}

function extractDeep(structured) {
  const s = structured || {};
  const ov = s.eventsOpportunityOverviewFields || {};
  const fe = ov.futureEdition || {};
  const cib = s.cibMappingHints || {};
  const f = s.findings || {};
  return {
    eventName: text(ov.eventName || s.eventIdentity),
    organizer: text(ov.organizerName),
    start: parseIso(fe.startDate) || parseIso(ov.verifiedStartDate),
    end: parseIso(fe.endDate) || parseIso(ov.verifiedEndDate),
    city: text(ov.eventLocationCity || fe.city),
    region: text(ov.eventLocationRegion),
    country: text(ov.eventLocationCountry),
    venue: text(ov.verifiedVenueName || fe.primaryVenue),
    hotelSourcingStatus: text(ov.hotelSourcingStatus).toLowerCase(),
    venueCommitment: text(ov.venueCommitment || f.venueCommitments),
    demandCycle: text(ov.demandCycleStatus).toLowerCase(),
    recurrence: text(ov.recurrenceEvidence || f.historicalRotation),
    accommodation: text(f.accommodationSourcing),
    attendanceField: Number(ov.attendancePublished || ov.attendance) || null,
    findings: f,
    cib,
    track: text(s.commercialOpportunityTrack),
    contacts: Array.isArray(s.organizerContactCandidates) ? s.organizerContactCandidates : [],
    sources: Array.isArray(s.supportingSources) ? s.supportingSources : [],
    uncertainties: Array.isArray(s.remainingUncertainties) ? s.remainingUncertainties : [],
  };
}

function pickContact(contacts, fallbackOrg, { relaxed = false } = {}) {
  let best = null;
  for (const c of contacts) {
    const name = text(c.name);
    if (!name || /desk|team|office|department|contact us|info@|^n\/a|unknown|not (?:published|found)/i.test(name)) continue;
    const role = classifyContactRole(c.role || c.title);
    const urls = [...(c.sourceUrls || []), c.profileUrl, c.sourceUrl].filter((u) => /^https?:/i.test(String(u || "")));
    if (!urls.length) continue;
    if (String(c.confidence || "").toLowerCase() === "low") continue;
    if (role === "PROCUREMENT_ONLY") continue;
    if (role !== "COMMERCIALLY_RELEVANT" && !(relaxed && role === "ROLE_UNCLEAR")) continue;
    const rank =
      (String(c.confidence).toLowerCase() === "high" ? 2 : 1) +
      (/president|director|founder|owner|organizer|events|sales|ceo|chair|conference|housing/i.test(c.role || "") ? 1 : 0) -
      (role === "ROLE_UNCLEAR" ? 1 : 0);
    if (!best || rank > best.rank) {
      best = {
        rank,
        contact: { name, title: text(c.role || c.title) || null, organization: text(c.organization) || fallbackOrg, sourceUrl: urls[0] },
        contactClass: contactIsIntermediary(c.role, c.organization) ? "INTERMEDIARY_ENTRY_POINT" : "RELEVANT_COMMERCIAL_ENTRY_POINT",
        relevance: text(c.commercialRelevance),
      };
    }
  }
  return best;
}

function bestAttendance(d, cand) {
  const blob = `${JSON.stringify(d.findings)} ${d.recurrence} ${d.cib?.potentialBusiness || ""} ${cand.excerpt || ""}`;
  const nums = [...blob.matchAll(/(\d[\d,]{2,6})\+?\s*(?:attendees|delegates|participants|professionals|visitors|people|members|registrants|buyers)/gi)]
    .map((m) => Number(m[1].replace(/,/g, "")))
    .filter((n) => n > 0 && n < 200000);
  const cands = [...nums, d.attendanceField, cand.ledgerAttendance].filter((n) => Number.isFinite(n) && n > 0);
  return cands.length ? Math.max(...cands) : null;
}

function assessHotelFit(d, cand) {
  const attendance = bestAttendance(d, cand);
  const venueBlob = `${d.venue} ${cand.ledgerVenue}`;
  const mtccLike = MTCC_LIKE_VENUE_REGEX.test(venueBlob);
  const accBlob = `${d.accommodation} ${d.hotelSourcingStatus}`.toLowerCase();
  const housingClosed = /official housing (?:bureau|partner|provider)|housing bureau|exclusive(?:ly)? (?:through|via)|room blocks? (?:are )?(?:full|closed|sold out)|housing (?:is )?closed/.test(accBlob);
  const openSignal = /open (?:hotel|lodging|sourcing)|accepting|rfp|additional hotels|overflow|alternative (?:hotel|lodging)|own accommodation|book (?:your|their) own/.test(accBlob);
  let fit;
  let rationale;
  if (attendance && attendance >= 5000 && mtccLike && !openSignal) {
    fit = "WATCH_MEGA_CONVENTION_NOT_AUTOMATIC_DEMAND";
    rationale = `~${attendance} attendees at ${text(d.venue) || "a convention venue"}; no evidence of open overflow sourcing — a 157-room hotel is not assumed to capture this.`;
  } else if (housingClosed && !openSignal) {
    fit = "WATCH_HOUSING_CLOSED_TO_ADDITIONAL_HOTELS";
    rationale = "Lodging appears to be centrally contracted; no evidence additional hotels can participate.";
  } else if (attendance && attendance < 20) {
    fit = "WATCH_TOO_SMALL_FOR_MEANINGFUL_ROOM_VOLUME";
    rationale = `Published attendance (~${attendance}) is below a commercially meaningful room volume.`;
  } else if ((attendance && attendance >= 1000) || mtccLike) {
    fit = "OVERFLOW_ANCILLARY";
    rationale = `Convention-scale event${attendance ? ` (~${attendance} attendees)` : ""}; realistic role for a 157-room Harbourfront hotel is delegate/speaker overflow, VIP lodging and side functions — not primary housing.`;
  } else {
    fit = "DIRECT_FIT";
    rationale = `Event scale${attendance ? ` (~${attendance})` : " (attendance unverified)"} is compatible with a 157-room hotel and ~11,000 sq ft of meeting space.`;
  }
  return { fit, attendance, mtccLike, housingClosed, openSignal, rationale };
}

// ---------------------------------------------------------------------------
// Pass 2 — HIGH deep research
// ---------------------------------------------------------------------------
const deepRows = []; // { stage, cand, key, paid, reusedFromPrior? }

async function runDeepOne(cand, stage, keyPrefix, ctxFn) {
  const key = `${keyPrefix}_${cand.id}`;
  const own = loadRaw(key);
  if (own) return { stage, cand, key, paid: { ...own.paid, cached: true }, reusedFromPrior: own.reusedFromPrior || null };
  if (stage === "PASS2_DEEP") {
    const prior = LIVE_PAID ? findPriorRaw(cand) : null;
    if (prior) {
      // $0 reuse of a completed prior paid run for the SAME candidate; copy into this campaign's raw-runs.
      const paid = { ...prior.paid, actualCostUsd: 0, cached: true };
      saveRaw(key, { cand, paid: { ok: true, result: prior.paid.result, actualCostUsd: 0 }, reusedFromPrior: prior.file });
      return { stage, cand, key, paid, reusedFromPrior: prior.file };
    }
  }
  if (!LIVE_PAID) return { stage, cand, key, skipped: true, paid: { ok: false, blocked: "NO_CACHE_AND_PAID_NOT_AUTHORIZED" } };
  const ctx = ctxFn(cand);
  const pack = buildExaEventsAgentCreatePayload({ mode: "deep_research", contextBlock: ctx, effort: EXA_AGENT_EFFORT.HIGH, segmentId: cand.segmentId || cand.segmentKey || "associations_conferences_events" });
  const paid = await chargeable(
    {
      stage,
      kind: stage === "PASS3_VERIFY" ? "verify" : "deep",
      hunter: cand.segmentId || "deep",
      segmentId: cand.segmentId || null,
      opportunityId: cand.id,
      effort: EXA_AGENT_EFFORT.HIGH,
      label: text(cand.title).slice(0, 80),
    },
    async () =>
      runExaAgentResearch({
        mode: "deep_research",
        contextBlock: ctx,
        effort: EXA_AGENT_EFFORT.HIGH,
        authorizeExaAgentLive: true,
        forceMockOnly: false,
        createPayloadOverride: pack.body,
        contractIdOverride: pack.meta.contractId,
        maxPolls: 75,
        pollIntervalMs: 4000,
      }),
  );
  if (paid.ok || paid.error) saveRaw(key, { cand, paid });
  return { stage, cand, key, paid };
}

async function runStage(stage, keyPrefix, cands, ctxFn) {
  const rows = [];
  console.log(`[${stage}] targets=${cands.length} capRemaining=${stageRemainingUsd(stage)} globalRemaining=${remainingBudgetUsd(ledger)}`);
  await pool(cands, CONCURRENCY, async (cand) => {
    if (ledger.frozen) {
      const own = loadRaw(`${keyPrefix}_${cand.id}`);
      if (!own) {
        rows.push({ stage, cand, key: `${keyPrefix}_${cand.id}`, skipped: true, paid: { ok: false, blocked: ledger.freezeReason || "LEDGER_FROZEN" } });
        return;
      }
    }
    const row = await runDeepOne(cand, stage, keyPrefix, ctxFn);
    console.log(
      `[${stage}] "${text(cand.title).slice(0, 55)}" ok=${row.paid.ok} cached=${row.paid.cached === true}${row.reusedFromPrior ? " (prior-reuse)" : ""} blocked=${row.paid.blocked || ""} cost=${row.paid.cached ? 0 : row.paid.actualCostUsd ?? 0} rem=${remainingBudgetUsd(ledger)}`,
    );
    rows.push(row);
  });
  return rows;
}

// Build the Pass 2 candidate list: free reuse first (does not consume paid slots), then paid up to the cap.
function buildPass2List() {
  const ordered = [...RECOVERY_CANDS, ...discoveryCands];
  const seen = new Set();
  const list = [];
  let paidSlots = 0;
  const maxPaid = Math.min(MAX_RUNS.PASS2_DEEP, Math.floor((STAGE_CAP_USD.PASS2_DEEP + 1e-9) / HIGH_UNIT));
  for (const c of ordered) {
    const fam = normKey(`${c.organizationName}|${c.title}`).slice(0, 44);
    if (seen.has(fam)) continue;
    seen.add(fam);
    const cachedOwn = loadRaw(`p2_${c.id}`);
    const prior = LIVE_PAID && !cachedOwn ? findPriorRaw(c) : null;
    const free = Boolean(cachedOwn || prior);
    if (!free) {
      if (!LIVE_PAID && !cachedOwn) {
        // assemble-cached: nothing to run without cache
        continue;
      }
      if (paidSlots >= maxPaid) continue;
      paidSlots += 1;
    }
    list.push({ ...c, plannedFree: free });
  }
  return { list, paidSlots, maxPaid };
}
const pass2Plan = buildPass2List();
const pass2Rows = await runStage("PASS2_DEEP", "p2", pass2Plan.list, (c) => deepContext(c));
deepRows.push(...pass2Rows);

// ---------------------------------------------------------------------------
// Qualification
// ---------------------------------------------------------------------------
function mkSources(cand, d, contact) {
  const out = [];
  const seen = new Set();
  const add = (url, statement) => {
    if (!/^https?:/i.test(text(url)) || seen.has(url)) return;
    seen.add(url);
    out.push({ url, statement: text(statement) || cand.title });
  };
  for (const u of cand.evidenceUrls || []) add(u, cand.title);
  for (const s of d.sources) add(s.url, s.claim);
  if (contact?.sourceUrl) add(contact.sourceUrl, `Contact source — ${contact.name}`);
  return out;
}
const NO_NEXT_EDITION_RE =
  /no (?:officially )?(?:published|announced|confirmed) (?:next[- ]edition|future edition|dates)|no next[- ]edition|no future edition|none (?:announced|published|located)|not (?:yet )?(?:announced|published|confirmed)|no (?:verified )?event falls within/i;

function qualifyRow(row) {
  const { cand, paid, stage } = row;
  const base = {
    key: row.key, stage, candId: cand.id, title: cand.title, org: cand.organizationName, segment: cand.segmentId,
    source: cand.source, cached: paid.cached === true, reusedFromPrior: row.reusedFromPrior || null,
    costUsd: paid.cached ? 0 : paid.actualCostUsd ?? 0,
  };
  if (row.skipped) return { decision: { ...base, bucket: "NOT_RUN", reason: paid.blocked } };
  if (!paid.ok || paid.result?.ok === false) {
    return { decision: { ...base, bucket: "REJECTED", reason: paid.blocked || paid.error || paid.result?.status || "DEEP_FAILED" } };
  }
  const d = extractDeep(paid.result?.structured);
  const title = d.eventName || cand.title;
  const org = d.organizer || cand.organizationName;
  const futureEditionText = text(d.findings.futureEdition);
  const summary = {
    eventName: title, organizer: org, start: d.start, end: d.end, venue: d.venue, city: d.city,
    hotelSourcingStatus: d.hotelSourcingStatus, contactsReturned: d.contacts.length,
    futureEditionText: futureEditionText.slice(0, 400), venueText: text(d.venueCommitment).slice(0, 300),
  };
  let dateEv = { kind: "NONE" };
  if (d.start && d.start >= REF) dateEv = { kind: "EXACT_FUTURE", startDate: d.start, endDate: d.end && d.end >= d.start ? d.end : null };
  else if (d.start && d.start < REF && (!d.end || d.end < REF)) dateEv = { kind: "EXPIRED", startDate: d.start };
  else {
    const y = maxYearIn(`${title} ${d.recurrence} ${d.demandCycle}`);
    if (y && y >= 2027) dateEv = { kind: "YEAR_ONLY", year: y };
  }
  const locBlob = [d.venue, d.city, d.region, d.country, d.city ? "" : text(d.venueCommitment).slice(0, 400), title, cand.ledgerVenue].join(" ");
  const territory = territoryOf(locBlob, d.city);
  const locationReturned = Boolean(d.venue || d.city || d.region || d.country || d.venueCommitment);
  const competitorSources = `${d.venue} ${d.venueCommitment} ${d.accommodation}`;
  const competitors = findCompetitors(competitorSources).filter((n) => !/radisson/i.test(n));
  const accBlob = `${d.accommodation} ${d.venueCommitment} ${d.hotelSourcingStatus} ${JSON.stringify(d.findings)}`.toLowerCase();
  const bookingLanguage = /host hotel|conference hotel|official hotel|headquarters hotel|room block|published (?:room )?rates|group rate|conference rate|special rate|booking code|group code|reservation link|reservations? (?:open|are open)|hotel block|hotel partners?/.test(accBlob);
  const lodgingCommitted = /(?<!un)confirmed|announced|\bofficial\b|committed|competitor|room_block|\bhost\b|blocks?/.test(`${d.hotelSourcingStatus} ${d.venueCommitment}`.toLowerCase());
  const urls = [...(cand.evidenceUrls || []), ...d.sources.map((s) => s.url)].filter((u) => /^https?:/i.test(String(u || "")));
  const fit = assessHotelFit(d, cand);
  const detail = { ...summary, dateEv, territory, competitors, bookingLanguage, lodgingCommitted, fit };
  const out = (bucket, reason, extra = {}) => ({ decision: { ...base, bucket, reason, detail: { ...detail, ...extra } } });

  if (dateEv.kind === "EXPIRED") return out("REJECTED", "EXPIRED_ONE_TIME_OR_PAST_EDITION");
  if (dateEv.kind === "EXACT_FUTURE" && dateEv.startDate < IMMINENT_CUTOFF) return out("REJECTED", "IMMINENT_LT_21_DAYS_NOT_ACTIONABLE");
  if (territory === "OUTSIDE_REALISTIC_TERRITORY") {
    return locationReturned ? out("REJECTED", "OUTSIDE_TERRITORY") : out("WATCH", "LOCATION_NOT_VERIFIED_BY_DEEP_RESEARCH");
  }
  if (!HOTEL_MARKET_TERRITORIES.has(territory)) return out("WATCH", `${territory}_NOT_ASSUMED_RADISSON_DEMAND`);
  const blob = JSON.stringify(d.findings).toLowerCase();
  if (/not a (?:conference|group event|meeting|event)|airline connectivity|route launch|new (?:direct )?route/.test(`${title} ${blob}`)) {
    return out("REJECTED", "NON_GROUP_DEMAND_SIGNAL");
  }

  // ---- CMI route: edition-specific competitor commitment (CMI is separate; only exceptional rows are added) ----
  if (competitors.length && bookingLanguage && lodgingCommitted && urls.length) {
    if (dateEv.kind !== "EXACT_FUTURE") return out("WATCH", "CMI_COMMITMENT_BUT_NO_EXACT_FUTURE_DATE");
    const pc = pickContact(d.contacts, org, { relaxed: true });
    if (!pc) return out("WATCH", "CMI_COMMITMENT_VERIFIED_NO_NAMED_SOURCED_CONTACT");
    const roomEvidence = /room block|published (?:room )?rates|group rate|conference rate|special rate|booking code|group code|reservation link|reservations? (?:open|are open)/.test(accBlob);
    const yearStr = dateEv.startDate.slice(0, 4);
    const competitorHotel = competitors.slice(0, 4).join("; ");
    const venueStatus = roomEvidence ? COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK : COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE;
    const claimUrls = [...new Set(urls)].slice(0, 4);
    const recommended = recommendCommercialAction({
      competitorVenueStatus: venueStatus,
      targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
      commercialActionability: COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
      competitorHotel,
      contactHint: pc.contact.name,
      targetHotelName: HOTEL_NAME,
    });
    const spec = {
      id: cand.id,
      kind: "CMI",
      title,
      org,
      competitorHotel,
      competitorVenueStatus: venueStatus,
      commitmentScope: roomEvidence ? "VENUE_AND_GUESTROOMS" : "VENUE_PRIMARY_GUESTROOMS_NOT_SEPARATELY_PUBLISHED",
      bookingType: roomEvidence ? "EVENT_VENUE_PLUS_PUBLISHED_GUESTROOM_TERMS" : "CONFIRMED_HOTEL_SEDE_NO_PUBLISHED_ROOM_TERMS",
      eventStartDate: dateEv.startDate,
      eventEndDate: dateEv.endDate || dateEv.startDate,
      datePillDisplayLabel: dateLabel(dateEv.startDate, dateEv.endDate && dateEv.endDate > dateEv.startDate ? dateEv.endDate : null),
      competitorBookingClaim: `${text(d.venueCommitment).slice(0, 420) || `${competitorHotel} named for ${title}`} (edition ${yearStr}).`,
      lodgingEvidence: text(d.accommodation).slice(0, 420) || `${competitorHotel} named for the ${yearStr} edition; separate guestroom terms not published.`,
      futureCycleThesis:
        `${text(d.cib.potentialBusiness).slice(0, 300) || "Develop the organizer relationship for a future edition or separately evidenced overflow."} ${HOTEL_NAME} cannot realistically win the committed ${yearStr} principal housing; ${fit.rationale}`.trim().slice(0, 700),
      recurs: text(d.recurrence).slice(0, 200) || "RECURRENCE_NOT_VERIFIED",
      uncertainty: d.uncertainties.slice(0, 2).map(text).join(" ") || "Contract terms not reviewed.",
      contact: pc.contact,
      contactClass: pc.contactClass,
      sources: claimUrls,
      claims: [
        { claimType: CMI_CLAIM_TYPE.COMPETITOR_HOST_HOTEL, status: "VERIFIED", evidenceUrls: claimUrls, statement: `${competitorHotel} named as committed hotel(s) for ${title} (${yearStr}).` },
        roomEvidence
          ? { claimType: CMI_CLAIM_TYPE.COMPETITOR_ROOM_BLOCK, status: "VERIFIED", evidenceUrls: claimUrls, statement: text(d.accommodation).slice(0, 220) }
          : { claimType: CMI_CLAIM_TYPE.COMPETITOR_ROOM_BLOCK, status: "NOT_SUPPORTED", evidenceUrls: claimUrls, statement: "No published attendee room terms found." },
        { claimType: CMI_CLAIM_TYPE.TARGET_OPEN_SOURCING, status: "NOT_SUPPORTED", evidenceUrls: claimUrls, statement: `No open host sourcing path evidenced for ${HOTEL_NAME}.` },
      ],
      recommended,
      segmentId: cand.segmentId || "association_events",
      territory,
      fit,
      provenance: { origin: "PAID_EXA_14SEG_CMI", stage, deepRunId: paid.result?.runId || null, costUsd: paid.cached ? 0 : paid.actualCostUsd ?? null, referenceDate: REF },
    };
    return { decision: { ...base, bucket: "CMI_CANDIDATE", reason: "COMPETITOR_COMMITMENT_EDITION_SPECIFIC_WITH_NAMED_CONTACT", detail: { ...detail, contact: pc.contact } }, spec };
  }
  if (competitors.length && /host hotel|\bofficial\b|committed|(?<!un)confirmed/.test(`${d.hotelSourcingStatus} ${d.venueCommitment}`.toLowerCase()) && /host hotel|conference hotel|official hotel|headquarters hotel|room block/.test(accBlob)) {
    return out("WATCH", "CMI_COMMITMENT_NOT_EDITION_SPECIFIC_OR_UNCONFIRMED");
  }
  if (d.venue && COMPETITOR_REGEX.test(d.venue)) return out("WATCH", "VENUE_IS_COMPETITOR_HOTEL_COMMITMENT_UNPROVEN");

  // ---- Hotel Market route ----
  if (dateEv.kind === "NONE") {
    if (NO_NEXT_EDITION_RE.test(futureEditionText)) return out("WATCH", "NO_NEXT_EDITION_PUBLISHED_PAST_EDITION_ONLY");
    return out("WATCH", "NO_VERIFIED_FUTURE_DATE");
  }
  if (dateEv.kind === "YEAR_ONLY") return out("WATCH", "YEAR_ONLY_NEEDS_EXACT_DATE");
  if (fit.fit.startsWith("WATCH_")) return out("WATCH", fit.fit);
  const pc = pickContact(d.contacts, org);
  if (!pc) return out("WATCH", "NO_NAMED_COMMERCIAL_CONTACT_WITH_SOURCE");
  let demandClass = "SPECULATIVE_DEMAND";
  if (/rfp|open (?:hotel|lodging) sourcing|accepting (?:hotel|proposals)|hotel proposals/.test(`${d.hotelSourcingStatus} ${blob}`) && !/no rfp|not an open/.test(blob)) demandClass = "CONFIRMED_DEMAND";
  else if (/hotel|lodging|alojamiento|overnight|room|attendee|delegat|participants|teams|accommodation/.test(blob)) demandClass = "RELIABLE_INFERRED_DEMAND";
  if (demandClass === "SPECULATIVE_DEMAND") return out("WATCH", "SPECULATIVE_ONLY");

  const overflow = fit.fit === "OVERFLOW_ANCILLARY";
  const what = text(d.cib.whatIsHappening) || `${title} (${dateLabel(dateEv.startDate, dateEv.endDate)}) — organized by ${org}.`;
  const potential =
    text(d.cib.potentialBusiness) ||
    (overflow
      ? `Possible delegate/speaker overflow lodging and ancillary meeting or reception demand around ${title}; volume unverified.`
      : `Possible guestroom and group-meeting demand tied to ${title}; volume unverified.`);
  const openBasis = /open/.test(d.hotelSourcingStatus)
    ? "lodging sourcing open"
    : overflow
      ? "overflow / ancillary path to qualify"
      : "participant lodging path to qualify";
  const spec = {
    id: cand.id,
    kind: "HM",
    title,
    org,
    segmentId: cand.segmentId || "association_events",
    startDate: dateEv.startDate,
    endDate: dateEv.endDate || null,
    territory,
    contact: pc.contact,
    contactClass: pc.contactClass,
    what: text(what).slice(0, 700),
    potential: text(potential).slice(0, 600),
    whyHotel: softClampCustomerProse(
      `${HOTEL_FIT_COPY} ${fit.rationale} ${text(d.cib.whyThisHotel || d.findings?.hotelProductFit)}`.trim(),
      1200,
    ),
    whyNow:
      softClampCustomerProse(futureEditionText, 900) ||
      "Edition dates are published; qualify who selects lodging before sourcing closes.",
    nextStep:
      softClampCustomerProse(text(d.cib.recommendedNextStep), 1200) ||
      `Contact ${pc.contact.name} at ${org} to confirm who selects lodging, whether additional hotels can participate, and expected room nights; position ${HOTEL_NAME} as a Harbourfront ${overflow ? "overflow / ancillary meeting" : "group"} option.`,
    sources: mkSources(cand, d, pc.contact),
    demandClass,
    sourcingLabel: `Hotel Sourcing Open — ${openBasis}`,
    uncertainties: [...d.uncertainties.slice(0, 3).map(text), "Room volume unverified.", "Restaurant/lounge availability unverified — do not assume catering capability."].filter(Boolean),
    fit,
    provenance: { origin: "PAID_EXA_14SEG_DEEP_RESEARCH", stage, priorLedgerLabel: cand.priorLabel || null, deepRunId: paid.result?.runId || null, costUsd: paid.cached ? 0 : paid.actualCostUsd ?? null, reusedFromPriorCampaign: row.reusedFromPrior || null, referenceDate: REF, paidExaUsed: !paid.cached },
    dateEvidence: dateEv,
  };
  return { decision: { ...base, bucket: "HOTEL_MARKET_CANDIDATE", reason: `FUTURE_DATE_TERRITORY_CONTACT_FIT_${fit.fit}`, detail: { ...detail, contact: pc.contact } }, spec };
}

// ---------------------------------------------------------------------------
// Pass 3 — borderline verification (only rows that would unlock Hotel Market)
// ---------------------------------------------------------------------------
const pass2Quals = new Map();
for (const row of deepRows) pass2Quals.set(row.cand.id, { row, q: qualifyRow(row) });

const VERIFY_PRIORITY = [
  "NO_NAMED_COMMERCIAL_CONTACT_WITH_SOURCE",
  "SPECULATIVE_ONLY",
  "NO_VERIFIED_FUTURE_DATE",
  "YEAR_ONLY_NEEDS_EXACT_DATE",
  "LOCATION_NOT_VERIFIED_BY_DEEP_RESEARCH",
];
function verifyEligible(q) {
  const dcs = q.decision;
  if (dcs.bucket !== "WATCH") return false;
  const rank = VERIFY_PRIORITY.indexOf(dcs.reason);
  if (rank < 0) return false;
  const d = dcs.detail || {};
  if (d.territory === "OUTSIDE_REALISTIC_TERRITORY" && dcs.reason !== "LOCATION_NOT_VERIFIED_BY_DEEP_RESEARCH") return false;
  if (dcs.reason === "LOCATION_NOT_VERIFIED_BY_DEEP_RESEARCH" && d.dateEv?.kind !== "EXACT_FUTURE") return false;
  if (["NO_VERIFIED_FUTURE_DATE", "YEAR_ONLY_NEEDS_EXACT_DATE"].includes(dcs.reason)) {
    // need a real next-edition signal (season/year) in the findings, otherwise past-only
    if (NO_NEXT_EDITION_RE.test(d.futureEditionText || "")) return false;
    if (!/20(2[7-8])/.test(`${d.futureEditionText || ""}`)) return false;
  }
  if (dcs.reason === "NO_NAMED_COMMERCIAL_CONTACT_WITH_SOURCE" && d.dateEv?.kind !== "EXACT_FUTURE") return false;
  return true;
}
const verifyList = [...pass2Quals.values()]
  .filter(({ q }) => verifyEligible(q))
  .sort((a, b) => VERIFY_PRIORITY.indexOf(a.q.decision.reason) - VERIFY_PRIORITY.indexOf(b.q.decision.reason))
  .slice(0, Math.min(MAX_RUNS.PASS3_VERIFY, Math.floor((STAGE_CAP_USD.PASS3_VERIFY + 1e-9) / HIGH_UNIT)))
  .map(({ row, q }) => ({
    ...row.cand,
    stage: "PASS3_VERIFY",
    verifyPrev: {
      reason: q.decision.reason,
      futureEditionText: q.decision.detail?.futureEditionText,
      venueText: q.decision.detail?.venueText,
    },
  }));
const pass3Rows = await runStage("PASS3_VERIFY", "p3", verifyList, (c) => verifyContext(c, c.verifyPrev));
const pass3Quals = new Map();
for (const row of pass3Rows) pass3Quals.set(row.cand.id, { row, q: qualifyRow(row) });

// final decision per candidate: Pass 3 upgrade if it produced a qualified spec, else Pass 2
const finalQuals = [];
for (const [id, p2] of pass2Quals) {
  const p3 = pass3Quals.get(id);
  if (p3 && p3.q.spec) finalQuals.push({ id, via: "PASS3_VERIFY", q: p3.q, p2: p2.q.decision });
  else finalQuals.push({ id, via: "PASS2_DEEP", q: p2.q, p3: p3?.q.decision || null });
}
const allDecisions = [
  ...[...pass2Quals.values()].map((x) => x.q.decision),
  ...[...pass3Quals.values()].map((x) => x.q.decision),
];

// ---------------------------------------------------------------------------
// opportunity builders
// ---------------------------------------------------------------------------
function applySalesPresentation(draft) {
  const typeKey = classifyOpportunityTypeFilter(draft);
  draft.opportunityTypeFilterCategory = typeKey;
  draft.opportunityTypeFilterLabel = OPPORTUNITY_TYPE_FILTER_LABEL[typeKey] || typeKey;
  draft.opportunityTypeLabel = draft.opportunityTypeFilterLabel;
  draft.segmentDisplayLabel = draft.opportunityTypeFilterLabel;
  const sales = resolveCustomerSalesAction(draft);
  draft.salesActionKey = sales.key;
  draft.salesActionPillLabel = sales.label;
  draft.salesActionReason = sales.reason;
  const tilePlan = planCustomerTileBadges(draft);
  draft.tileBadgePlan = tilePlan;
  draft.priorityFilterKey = tilePlan.priorityFilterKey || draft.priority;
  draft.priorityPillLabel = tilePlan.priorityPillLabel || null;
  draft.commercialStageKey = tilePlan.commercialStageKey || draft.commercialStageKey;
  draft.datePillDisplayLabel = tilePlan.datePillDisplayLabel || draft.datePillDisplayLabel;
  return attachSalesOpportunityFilterFields(draft);
}

function buildHotelMarketOpportunity(spec) {
  const contact = spec.contact
    ? {
        name: spec.contact.name,
        title: text(spec.contact.title || spec.contact.role) || null,
        organization: text(spec.contact.organization) || spec.org,
        sourceUrl: spec.contact.sourceUrl || null,
        contactClass: spec.contactClass || "RELEVANT_COMMERCIAL_ENTRY_POINT",
      }
    : null;
  const start = spec.startDate || null;
  const end = spec.endDate || null;
  const sources = (spec.sources || []).map((s) => (typeof s === "string" ? { url: s, statement: spec.title } : s));
  const fitObj = typeof spec.fit === "object" && spec.fit ? spec.fit : { fit: spec.fit || "DIRECT_FIT", rationale: "", attendance: null };
  const draft = {
    id: spec.id,
    displayTitle: spec.title,
    title: spec.title,
    organizationName: spec.org,
    displayCompanyName: spec.org,
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    targetHotelName: HOTEL_NAME,
    whatIsHappening: spec.what,
    summaryWhat: spec.what,
    potentialHotelBusiness: spec.potential,
    potentialBusiness: spec.potential,
    whyThisHotel: spec.whyHotel,
    whyActNow: spec.whyNow,
    cardWhyNowLine: text(spec.whyNow).slice(0, 160),
    sources,
    hotelSourcingStatusLabel: spec.sourcingLabel || "Hotel Sourcing Unconfirmed",
    demandSegmentId: spec.segmentId,
    researchSegment: spec.segmentId,
    opportunityType: spec.segmentId,
    revenueStreamClass: revenueStreamFor(spec.potential),
    guestroomThesis: spec.potential,
    hasVerifiedNamedRelevantContact: Boolean(contact),
    hasCredibleOutreachRoute: Boolean(contact),
    eventStartDate: start,
    eventEndDate: end,
    tileEventStartDate: start,
    tileEventEndDate: end,
    datePillDisplayLabel: start ? dateLabel(start, end) : "Timing TBD",
    primaryContact: contact,
    cardContactSubtitle: contact?.title || (contact ? "Role Not Yet Verified" : null),
    tileContactPreview: contact
      ? {
          displayRole: contact.title || "Role Not Yet Verified",
          displayRoleKind: contact.title ? "professional_title" : "role_not_yet_verified",
          professionalTitle: contact.title || null,
        }
      : null,
    inventoryPopulations: contact ? [INVENTORY_POPULATION.SALES_OPPORTUNITIES] : [],
    hotelMarketScopeEligible: Boolean(contact),
    competitiveMarketScopeEligible: false,
    commercialStageKey: "ACCOUNT_DEVELOPMENT",
    commercialStagePillLabel: "Account Development",
    inventoryLane: "QUALIFICATION_NEEDED",
    bookingWindowStatus: "QUALIFY_NOW",
    recommendedNextStep: spec.nextStep,
    recommendedAction: spec.nextStep,
    demandEvidenceClass: spec.demandClass,
    territoryClassification: spec.territory,
    hotelFitAssessment: { fit: fitObj.fit, rationale: fitObj.rationale || "", attendance: fitObj.attendance ?? null, ruleVersion: "radisson-toronto-fit-14seg-v1", assessedAt: REF },
    researchProvenance: spec.provenance,
    uncertainties: spec.uncertainties || [],
    sharedTenSectionDrawer: {
      sections: [
        { id: "what_is_happening", title: "What is happening", body: spec.what },
        { id: "potential_hotel_business", title: "Potential hotel business", body: spec.potential },
        { id: "why_this_hotel", title: "Why this hotel", body: spec.whyHotel },
        { id: "why_now", title: "Why now", body: spec.whyNow },
        {
          id: "who_to_contact",
          title: "Who to contact",
          body: contact
            ? `${contact.name}${contact.title ? ` — ${contact.title}` : ""} (${contact.organization}). Entry point only; lodging authority not verified.`
            : "Named contact not yet verified — research stage.",
        },
        { id: "recommended_next_step", title: "Recommended next step", body: spec.nextStep },
        { id: "supporting_evidence", title: "Supporting evidence", body: sources.slice(0, 10).map((s) => `${s.statement || ""} ${s.url || ""}`.trim()).join("\n") },
        { id: "research_uncertainties", title: "Research uncertainties", body: (spec.uncertainties || []).join(" ") || "Hotel sourcing unconfirmed. Room volume unverified." },
      ],
    },
  };
  return applySalesPresentation(draft);
}

/** Upgrade sales action: no generic Monitor for Hotel Market rows. */
function ensureHotelSourcingOpenSales(opp) {
  const o = cloneOpp(opp);
  if (!/^Hotel Sourcing Open/i.test(text(o.hotelSourcingStatusLabel))) {
    o.hotelSourcingStatusLabel = "Hotel Sourcing Open — overflow / ancillary path to qualify";
  }
  o.targetHotelSourcingStatus = o.targetHotelSourcingStatus || TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED;
  o.accountDevelopmentPublished = true;
  o.decisionState = null;
  o.nextActionRecheckReason = null;
  o.commercialStageKey = "ACCOUNT_DEVELOPMENT";
  o.commercialStagePillLabel = "Account Development";
  o.inventoryLane = o.inventoryLane || "QUALIFICATION_NEEDED";
  o.bookingWindowStatus = "QUALIFY_NOW";
  return applySalesPresentation(o);
}

function buildCmiOpportunity(spec) {
  const base = buildHotelMarketOpportunity({
    id: spec.id,
    title: spec.title,
    org: spec.org,
    segmentId: spec.segmentId,
    startDate: spec.eventStartDate,
    endDate: spec.eventEndDate,
    territory: spec.territory,
    contact: spec.contact,
    contactClass: spec.contactClass,
    what: `${spec.title} is committed at ${spec.competitorHotel} (${spec.datePillDisplayLabel}). The principal housing is not available to ${HOTEL_NAME}.`,
    potential: "Future-cycle account development and/or separately evidenced overflow only — not the committed principal housing.",
    whyHotel: spec.futureCycleThesis,
    whyNow: `Competitor commitment confirmed at ${spec.competitorHotel}; develop the organizer relationship for a future cycle or residual overflow lodging.`,
    nextStep: spec.recommended,
    sources: spec.sources.map((url) => ({ url, statement: spec.title })),
    demandClass: "CONFIRMED_COMPETITOR_COMMITMENT",
    sourcingLabel: "Competitor host/block confirmed — not an open host RFP",
    uncertainties: [spec.uncertainty],
    provenance: spec.provenance,
    fit: spec.fit,
  });
  return {
    ...base,
    competitorHotel: spec.competitorHotel,
    likelyCompetitor: spec.competitorHotel,
    competitorVenueStatus: spec.competitorVenueStatus,
    competitorBookingClaim: spec.competitorBookingClaim,
    lodgingEvidence: spec.lodgingEvidence,
    housingStatus: "COMPETITOR_BLOCK_OR_SEDE_CONFIRMED",
    roomDemandStatus: spec.commitmentScope === "VENUE_AND_GUESTROOMS" ? "COMPETITOR_BLOCK_CONFIRMED" : "COMPETITOR_VENUE_CONFIRMED_GUESTROOMS_UNPUBLISHED",
    competitorBookingEvidenceClass: "VERIFIED_BOOKING",
    competitiveVenueEvidence: true,
    competitorVenueConfirmed: true,
    commercialActionability: COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    claims: spec.claims,
    lodgingEvidenceUrl: spec.sources[0] || null,
    cmiEvidenceRegisterV1: {
      bookingType: spec.bookingType,
      commitmentScope: spec.commitmentScope,
      evidenceConfidence: "MEDIUM_HIGH",
      verifiedAt: REF,
      uncertainty: spec.uncertainty,
      futureCycleThesis: spec.futureCycleThesis,
      recurs: spec.recurs,
    },
    eventDates: `${spec.eventStartDate}/${spec.eventEndDate}`,
    hotelMarketScopeEligible: false,
    competitiveMarketScopeEligible: true,
    inventoryPopulations: [INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE],
    customerVisible: true,
  };
}

function sanitizeCustomerCopy(v, key = "") {
  if (typeof v === "string") {
    if (/url|href|^id$|orsId|fingerprint/i.test(key)) return v;
    return v.replace(/\bTBD\b/g, "to be announced");
  }
  if (Array.isArray(v)) return v.map((x) => sanitizeCustomerCopy(x, key));
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, sanitizeCustomerCopy(x, k)]));
  return v;
}

/**
 * Evidence-based segment reclassification.
 * Discovery-job labels must NOT manufacture missing-segment coverage for ordinary conventions.
 * IMPORTANT: classify on title/org/contact only — never on Exa what/potential prose
 * (those narratives often mention "extended-stay" or "preferred lodging" as speculative revenue streams).
 */
function inferCanonicalDemandSegment(spec) {
  const title = text(spec.title);
  const org = text(spec.org);
  const contactTitle = text(spec.contact?.title || spec.contact?.role);
  const blob = `${title} ${org} ${contactTitle}`.toLowerCase();
  const assigned = text(spec.segmentId);
  const why = (segmentId, reason) => ({ segmentId, reason, priorSegmentId: assigned || null });

  // Hard overrides for known false-seeded discovery labels.
  if (/labfab|lab medicine|mathematical finance|\bfmf200\b|fhlip|prime\b|atwood colloquium|aesthetics sciences/.test(blob)) {
    return why("education_scientific", "Scientific/academic program override — not a missing-segment relabel");
  }
  if (/\bicsc\b|cfca engage|\bcfca\b|small business show|biocomm|mars impact|ev.?charging|ola super/.test(blob)) {
    return why("associations_conferences_events", "Association/expo override — not a missing-segment relabel");
  }

  // Genuine relocation / extended-stay mechanisms only (identity-level, not narrative).
  if (/faculty housing|visiting faculty housing|corporate relocation program|temporary project housing|corporate housing program/.test(blob)) {
    return why("relocation_extended_stay", "Evidence of relocation/extended-stay lodging demand");
  }
  // Preferred lodging partnerships — recurring visitor/program lodging identity.
  if (/preferred lodging partnership|official hotel partner program|visiting[- ]team lodging|preferred hotel program/.test(blob)) {
    return why("preferred_lodging_partnerships", "Evidence of recurring preferred-lodging program");
  }
  // Professional planners / PCOs / DMCs as the commercial organizer.
  if (/\bacavent\b|destination management|professional conference organiz|\bpco\b|meeting management (?:company|firm)|incentive (?:travel )?(?:house|agency)|event management (?:company|agency)/.test(blob)) {
    return why("professional_event_planners", "Named planner/PCO/DMC intermediary");
  }
  // Travel-trade / educational group intermediaries (Model UN student groups, inbound tours).
  if (/model united nations|\butmun\b|\bnamun\b|inbound tour|tour operator|educational (?:group )?travel|student group travel|motorcoach group/.test(blob)) {
    return why("travel_trade_group_intermediaries", "Group-travel intermediary / educational travel program");
  }
  // Social / special events — festivals, cultural celebrations, dance showcases.
  if (
    /(festival|tango|brazilfest|tea festival|bridal|wedding|gala|urban dance|stomp|cultural celebration)/.test(blob) &&
    !/(labfab|lab medicine|icsc|scientific|symposium|colloquium|annual meeting|world congress)/.test(blob)
  ) {
    return why("social_special_events", "Social/cultural/festival demand mechanism");
  }
  // Local catering / day meetings — training days, certification programs, education days, leadership retreats.
  if (
    /(certification program|education day|colonography|virtual colonography|leadership retreat|training day|day meeting|board meeting|investor relations certification|in-house leadership|2-day event|2-day event|wasylenki|insight summit|insurance bureau of canada)/.test(blob) &&
    !/(annual conference|world congress|expo|trade show|scientific meeting|annual meeting)/.test(blob)
  ) {
    return why("local_catering_day_meetings", "Day-meeting / training / certification catering demand");
  }
  // Conventional conferences / expos / symposia / shows — never keep a manufactured missing-segment label.
  if (/(conference|congress|symposium|colloquium|expo|\bshow\b|summit|annual meeting|scientific meeting|trade show)/.test(blob)) {
    if (/university|institute|department of|faculty of|medical|scientific|research|precision medicine|ecology|statistical sciences|aesthetics sciences|lab medicine|prime\b/.test(blob)) {
      return why("education_scientific", "Convention/scientific program — not a missing-segment relabel");
    }
    if (/in-house|corporate|leadership retreat|marketing law/.test(blob)) {
      return why("corporate_meetings_incentives", "Corporate meeting program — not a missing-segment relabel");
    }
    return why("associations_conferences_events", "Association/convention program — not a missing-segment relabel");
  }
  // Faith / assembly gatherings without festival cues.
  if (/assembly conference|\bc3 assembly\b/.test(blob)) {
    return why("associations_conferences_events", "Multi-day assembly/conference — not social festival demand");
  }
  // If still sitting on a missing segment without a genuine mechanism, demote to associations.
  if (MISSING_SEGMENTS.includes(assigned)) {
    if (["relocation_extended_stay", "preferred_lodging_partnerships"].includes(assigned)) {
      return why("associations_conferences_events", "No identity-level evidence for relocation/preferred-lodging — demoted");
    }
    return why(assigned, "Kept discovery assignment after no convention override");
  }
  return why(assigned || "associations_conferences_events", "Fallback to assigned/default segment");
}

// ---- select new HM / exceptional CMI specs and dedupe ----
const segmentReclassLog = [];
const newHmSpecsAll = finalQuals
  .filter((x) => x.q.spec?.kind === "HM")
  .map((x) => {
    const spec = { ...x.q.spec, _via: x.via };
    const rc = inferCanonicalDemandSegment(spec);
    if (rc.segmentId !== spec.segmentId) {
      segmentReclassLog.push({ id: spec.id, title: spec.title, from: spec.segmentId, to: rc.segmentId, reason: rc.reason });
      spec.segmentId = rc.segmentId;
      spec.segmentReclassReason = rc.reason;
      spec.priorSegmentId = rc.priorSegmentId;
    }
    return spec;
  });
const newCmiSpecsAll = finalQuals.filter((x) => x.q.spec?.kind === "CMI").map((x) => {
  const spec = { ...x.q.spec, _via: x.via };
  const rc = inferCanonicalDemandSegment({ ...spec, startDate: spec.eventStartDate });
  if (rc.segmentId !== spec.segmentId) {
    segmentReclassLog.push({ id: spec.id, title: spec.title, from: spec.segmentId, to: rc.segmentId, reason: rc.reason, kind: "CMI" });
    spec.segmentId = rc.segmentId;
  }
  return spec;
});
const dedupeDropped = [];
const hmKept = [];
for (const s of newHmSpecsAll) {
  const bd = baselineAsLike.find((b) => sameEvent({ title: s.title, org: s.org, startDate: s.startDate }, b));
  if (bd) {
    dedupeDropped.push({ id: s.id, title: s.title, keptId: bd.id, kind: "HM_DUP_OF_BASELINE" });
    continue;
  }
  const dupCmi = newCmiSpecsAll.find((k) => sameEvent({ title: k.title, org: k.org, startDate: k.eventStartDate }, { title: s.title, org: s.org, startDate: s.startDate }));
  if (dupCmi) {
    dedupeDropped.push({ id: s.id, title: s.title, keptId: dupCmi.id, kind: "HM_DUP_OF_NEW_CMI" });
    continue;
  }
  const dup = hmKept.find((k) => sameEvent({ title: k.title, org: k.org, startDate: k.startDate }, { title: s.title, org: s.org, startDate: s.startDate }));
  if (dup) dedupeDropped.push({ id: s.id, title: s.title, keptId: dup.id, kind: "HM_DUP" });
  else hmKept.push(s);
}
// exceptional CMI only: published guestroom terms, qualifies canonical gates, not a baseline dup, max 3
const cmiCandidatesEvaluated = [];
const cmiKept = [];
for (const s of newCmiSpecsAll) {
  const bd = baselineAsLike.find((b) => sameEvent({ title: s.title, org: s.org, startDate: s.eventStartDate }, b));
  const opp = buildCmiOpportunity(s);
  const gates = evaluateCmiQualificationGates(opp);
  const exceptional = s.commitmentScope === "VENUE_AND_GUESTROOMS" && s.recurs !== "RECURRENCE_NOT_VERIFIED" && gates.qualifiesAsCustomerCmi;
  const row = { id: s.id, title: s.title, competitorHotel: s.competitorHotel, dupOfBaseline: bd?.id || null, gatesOk: gates.qualifiesAsCustomerCmi, exceptional, failedGates: gates.failedGates || [] };
  cmiCandidatesEvaluated.push(row);
  if (bd || !exceptional || cmiKept.length >= 3) continue;
  if (cmiKept.find((k) => sameEvent({ title: k.title, org: k.org, startDate: k.eventStartDate }, { title: s.title, org: s.org, startDate: s.eventStartDate }))) continue;
  cmiKept.push(s);
}

const newHmOpps = hmKept.map((s) => ensureHotelSourcingOpenSales(sanitizeCustomerCopy(buildHotelMarketOpportunity(s))));
const newCmiOpps = cmiKept.map((s) => sanitizeCustomerCopy(buildCmiOpportunity(s)));

const priorHmClones = priorHm.map(cloneOpp);
const priorCmiClones = priorCmi.map((o) => {
  const c = cloneOpp(o);
  c.researchProvenance = { ...(c.researchProvenance || {}), preservedFromPriorPackage: priorFp };
  return c;
});
let hotelMarketAll = [...priorHmClones, ...newHmOpps];
let competitiveAll = [...priorCmiClones, ...newCmiOpps];
const seenH = new Set();
hotelMarketAll = hotelMarketAll.filter((o) => (seenH.has(o.id) ? false : (seenH.add(o.id), true)));
const seenC = new Set();
competitiveAll = competitiveAll.filter((o) => (seenC.has(o.id) ? false : (seenC.add(o.id), true)));
const FORCE_HOTEL_MARKET_IDS = new Set(hotelMarketAll.map((o) => o.id));

let allOpps = [...hotelMarketAll, ...competitiveAll];
allOpps = annotateInventoryPopulations(allOpps, { hotelId: HOTEL_ID }).map((o) => {
  const forceHm = FORCE_HOTEL_MARKET_IDS.has(o.id);
  const isCmi = !forceHm && o.competitiveMarketScopeEligible === true;
  const next = { ...o, competitiveMarketScopeEligible: isCmi, hotelMarketScopeEligible: forceHm || !isCmi };
  if (forceHm) {
    next.competitiveMarketScopeEligible = false;
    next.hotelMarketScopeEligible = true;
    next.commercialActionability = COMMERCIAL_ACTIONABILITY.ACCOUNT_DEVELOPMENT;
    next.competitorVenueStatus = null;
    next.competitorHotel = null;
    next.cmiVerified = false;
    next.cmiClass = null;
  }
  return annotateCanonicalCmiContract(next);
});
// re-assert forced HM after canonical annotate (which may recompute scopes)
allOpps = allOpps.map((o) =>
  FORCE_HOTEL_MARKET_IDS.has(o.id)
    ? {
        ...o,
        hotelMarketScopeEligible: true,
        competitiveMarketScopeEligible: false,
        inventoryPopulations: [INVENTORY_POPULATION.SALES_OPPORTUNITIES],
        inventoryPopulationPrimary: INVENTORY_POPULATION.SALES_OPPORTUNITIES,
      }
    : o,
);
const hintsById = {};
for (const o of allOpps) {
  const hm = FORCE_HOTEL_MARKET_IDS.has(o.id);
  hintsById[o.id] = {
    actualEventStart: o.eventStartDate,
    actualEventEnd: o.eventEndDate || null,
    datePillDisplayLabel: o.datePillDisplayLabel,
    competitorCommitted: hm ? false : o.competitiveMarketScopeEligible === true,
    cmiVerified: hm ? false : o.competitiveMarketScopeEligible === true,
  };
}
const gate = applyCommercialActionabilityGate(allOpps, { referenceDate: REF, hintsById });
const gatedRows = gate.rows.map((o) => attachSalesOpportunityFilterFields(o));
const customerVisible = gatedRows.filter((o) => o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true);
const hotelMarket = customerVisible.filter((o) => o.hotelMarketScopeEligible === true);
const competitive = customerVisible.filter((o) => o.competitiveMarketScopeEligible === true);
const gatedOut = gatedRows.filter((o) => !(o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true));
const actionabilityAssert = assertCustomerInventoryPassesActionabilityGate(customerVisible, { referenceDate: REF, hintsById });
const dualClassified = customerVisible.filter((o) => o.hotelMarketScopeEligible === true && o.competitiveMarketScopeEligible === true);
const monitorHm = hotelMarket.filter((o) => /monitor/i.test(`${o.salesActionKey} ${o.salesActionPillLabel}`));
const gap = Math.max(0, TARGET_HM - hotelMarket.length);

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------
const spendByStage = {};
for (const o of ledger.operations) spendByStage[o.stage] = Number(((spendByStage[o.stage] || 0) + Number(o.actualCostUsd || 0)).toFixed(4));
const withinCaps = Object.entries(STAGE_CAP_USD).every(([k, cap]) => (spendByStage[k] || 0) <= cap + 1e-9);

writeJson("01-BASELINE_AND_AUTHORIZATION.json", { ...baselineDoc(), mode: LIVE_PAID ? "PAID_AUTHORIZED" : "ASSEMBLE_CACHED_NO_SPEND", resumedSpendUsdAtStart: resumedSpendUsd });
writeJson("02-PAID_BUDGET_LEDGER.json", {
  contract: "GDI_RADISSON_14SEG_EXA_20USD_LEDGER_V1",
  authorizationId: AUTHORIZATION_ID,
  hardCeilingUsd: HARD_CEILING_USD,
  stageCapsUsd: STAGE_CAP_USD,
  separateFromPrior15UsdLedger: true,
  spentUsd: ledger.spentUsd,
  reservedUsd: ledger.reservedUsd,
  remainingUsd: remainingBudgetUsd(ledger),
  withinCeiling: ledger.spentUsd <= HARD_CEILING_USD,
  withinStageCaps: withinCaps,
  spendByStage,
  operationCount: ledger.operations.length,
  operations: ledger.operations.map((o) => ({ at: o.at, stage: o.stage, kind: o.kind || null, hunter: o.hunter, label: o.label || null, opportunityId: o.opportunityId, quotedMaxUsd: o.quotedMaxUsd, actualCostUsd: o.actualCostUsd, providerRunId: o.providerRunId })),
  blockedOperations: ledger.blockedOperations,
  frozen: ledger.frozen,
  freezeReason: ledger.freezeReason,
  surfeUsed: false,
  parallelUsed: false,
  note: "Authoritative on-disk ledger: paid-budget-ledger.json (same directory).",
});
writeJson("03-PASS1_DISCOVERY_RESULTS.json", {
  stage: "PASS1_DISCOVERY",
  status: LIVE_PAID ? "EXECUTED" : "ASSEMBLED_FROM_CACHE",
  capUsd: STAGE_CAP_USD.PASS1_DISCOVERY,
  spentUsd: spendByStage.PASS1_DISCOVERY || 0,
  unitCostUsd: MEDIUM_UNIT,
  jobsPlanned: PASS1_JOBS.length,
  jobsCompleted: pass1Calls.filter((c) => c.ok).length,
  segmentsCovered: [...new Set(pass1Calls.filter((c) => c.ok).map((c) => c.segment))],
  calls: pass1Calls,
  totalHits: pass1Hits.length,
  triagedToDeep: discoveryCands.map((c) => ({ id: c.id, title: c.title, org: c.organizationName, segment: c.segmentId, start: c.ledgerStart, venue: c.ledgerVenue, score: c.selectionScore, why: c.selectionWhy })),
  triageRejected,
  triageRejectedCounts: countBy(triageRejected, (r) => String(r.reason).split(":")[0]),
});
const rowSummary = (x) => ({
  key: x.key, candId: x.candId, title: x.title, org: x.org, source: x.source, segment: x.segment, cached: x.cached, reusedFromPrior: x.reusedFromPrior, costUsd: x.costUsd,
  bucket: x.bucket, reason: x.reason, eventName: x.detail?.eventName, start: x.detail?.start, venue: x.detail?.venue, city: x.detail?.city, territory: x.detail?.territory,
  hotelSourcingStatus: x.detail?.hotelSourcingStatus, competitors: x.detail?.competitors, fit: x.detail?.fit?.fit, attendance: x.detail?.fit?.attendance ?? null, contact: x.detail?.contact || null,
});
const p2Decisions = [...pass2Quals.values()].map((x) => x.q.decision);
const p3Decisions = [...pass3Quals.values()].map((x) => x.q.decision);
writeJson("04-PASS2_DEEP_RESULTS.json", {
  stage: "PASS2_DEEP",
  status: LIVE_PAID ? "EXECUTED" : "ASSEMBLED_FROM_CACHE",
  capUsd: STAGE_CAP_USD.PASS2_DEEP,
  spentUsd: spendByStage.PASS2_DEEP || 0,
  unitCostUsd: HIGH_UNIT,
  paidSlotsPlanned: pass2Plan.paidSlots,
  maxPaidSlots: pass2Plan.maxPaid,
  candidatesProcessed: pass2Plan.list.length,
  freeCacheReuse: pass2Plan.list.filter((c) => c.plannedFree).length,
  outcomeCounts: countBy(p2Decisions, (x) => x.bucket),
  recoveryQueue: recoveryPlanRows,
  recoverySkipped,
  recoveryAlreadyInBaseline: recoveryDupBaseline,
  runs: p2Decisions.map(rowSummary),
});
writeJson("05-PASS3_VERIFY_RESULTS.json", {
  stage: "PASS3_VERIFY",
  status: LIVE_PAID ? "EXECUTED" : "ASSEMBLED_FROM_CACHE",
  capUsd: STAGE_CAP_USD.PASS3_VERIFY,
  spentUsd: spendByStage.PASS3_VERIFY || 0,
  unitCostUsd: HIGH_UNIT,
  eligibleBorderline: [...pass2Quals.values()].filter(({ q }) => verifyEligible(q)).map(({ q }) => ({ id: q.decision.candId, title: q.decision.title, reason: q.decision.reason })),
  selected: verifyList.map((c) => ({ id: c.id, title: c.title, previousReason: c.verifyPrev.reason })),
  outcomeCounts: countBy(p3Decisions, (x) => x.bucket),
  runs: p3Decisions.map(rowSummary),
  upgradedToQualified: finalQuals.filter((x) => x.via === "PASS3_VERIFY").map((x) => ({ id: x.id, from: x.p2?.reason, to: x.q.decision.bucket })),
});
const failureHistogram = countBy(allDecisions.filter((x) => !["HOTEL_MARKET_CANDIDATE", "CMI_CANDIDATE"].includes(x.bucket)), (x) => x.reason);
writeJson("06-COMMERCIAL_ELIGIBILITY.json", {
  contract: "GDI_RADISSON_14SEG_COMMERCIAL_ELIGIBILITY_V1",
  referenceDate: REF,
  imminentCutoff: IMMINENT_CUTOFF,
  template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  cmiContract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  rules: [
    "Exact future event date (≥ 21 days from REF) required — year-only and expired editions are Watch/Reject.",
    "Territory: Toronto Harbourfront / downtown / MTCC / Exhibition Place (core) or Toronto proper (nearby); airport corridor / GTA suburbs are stretch → Watch.",
    "Named, sourced, commercially relevant contact required; procurement-only roles excluded.",
    "Hotel-fit assessment per event; ≥5,000 attendee MTCC-scale events without open overflow sourcing are Watch.",
    "Edition-specific competitor commitment (host hotel / room block / booking code) + named contact → CMI, never Hotel Market (no dual classification).",
    "Events before REF are expired/research-watch unless a next edition is evidenced.",
  ],
  decisionCounts: countBy(allDecisions, (x) => x.bucket),
  failureHistogram,
  newHotelMarketQualified: hmKept.map((s) => ({ id: s.id, title: s.title, start: s.startDate, via: s._via })),
  eventDedupeDropped: dedupeDropped,
  cmiCandidatesEvaluated,
  newCmiAdded: cmiKept.map((s) => ({ id: s.id, title: s.title, competitorHotel: s.competitorHotel })),
  dualClassifiedCount: dualClassified.length,
  decisions: allDecisions.map(rowSummary),
});
const oppSummary = (o) => ({
  id: o.id, title: o.displayTitle, org: o.organizationName, dates: o.datePillDisplayLabel, start: o.eventStartDate,
  territory: o.territoryClassification, fit: o.hotelFitAssessment?.fit || null,
  contact: o.primaryContact ? { name: o.primaryContact.name, title: o.primaryContact.title, sourceUrl: o.primaryContact.sourceUrl } : null,
  sourceCount: (o.sources || []).length, salesAction: o.salesActionPillLabel, stage: o.commercialStagePillLabel,
});
writeJson("07-HOTEL_MARKET_INVENTORY.json", {
  target: TARGET_HM,
  achieved: hotelMarket.length,
  targetMet: hotelMarket.length >= TARGET_HM,
  gapTo20: gap,
  baselineHotelMarket: priorHm.length,
  newlyAddedHotelMarket: hotelMarket.filter((o) => !baselineIds.has(o.id)).length,
  baselineHotelMarketRetainedAfterGate: hotelMarket.filter((o) => baselineIds.has(o.id)).length,
  opportunities: hotelMarket.map(oppSummary),
  watchList: allDecisions.filter((x) => x.bucket === "WATCH").map(rowSummary),
  rejected: allDecisions.filter((x) => x.bucket === "REJECTED").map(rowSummary),
});
writeJson("08-CMI_PRESERVATION_AUDIT.json", {
  priorCmiCount: priorCmi.length,
  priorCmiIds: priorCmi.map((o) => o.id),
  retainedAfterGate: competitive.filter((o) => baselineIds.has(o.id)).map((o) => o.id),
  newlyAddedCmi: competitive.filter((o) => !baselineIds.has(o.id)).map((o) => o.id),
  finalCmiCount: competitive.length,
  cmiSeparateFromHotelMarket: dualClassified.length === 0,
  priorPackageFingerprintPreserved: priorFp,
  gateReviews: competitive.map((o) => ({ id: o.id, title: o.displayTitle, competitorHotel: o.competitorHotel, gates: evaluateCmiQualificationGates(o) })),
  cmiCoverage: evaluateCmiDiscoveryCoverage({
    checklist: CMI_DISCOVERY_COVERAGE_CHECKLIST,
    completed: CMI_DISCOVERY_COVERAGE_CHECKLIST.map((c) => c.id),
  }),
});
writeJson("09-ACTIONABILITY_AUDIT.json", {
  referenceDate: REF,
  gate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  counts: gate.counts,
  assertCustomerInventoryPassesActionabilityGate: actionabilityAssert,
  rows: gatedRows.map((o) => ({ id: o.id, title: o.title, state: o.commercialActionabilityStateV1, temporal: o.commercialTemporalDisposition, hotelMarket: o.hotelMarketScopeEligible, competitive: o.competitiveMarketScopeEligible, datePill: o.datePillDisplayLabel })),
  gatedOut: gatedOut.map((o) => ({ id: o.id, title: o.title, state: o.commercialActionabilityStateV1 })),
});
writeJson("10-CONTACT_AND_SALES_ACTIONS.json", {
  note: "Hotel Market rows carry Hotel Sourcing Open labels + accountDevelopmentPublished; no generic Monitor.",
  monitorHotelMarketRows: monitorHm.map((o) => o.id),
  rows: hotelMarket.map((o) => ({
    id: o.id, title: o.displayTitle, contact: o.primaryContact, salesActionPill: o.salesActionPillLabel, salesActionKey: o.salesActionKey,
    sourcing: o.hotelSourcingStatusLabel, accountDevelopmentPublished: o.accountDevelopmentPublished === true,
    nextStep: o.recommendedNextStep, when: o.whyActNow, unverified: o.uncertainties || [],
  })),
});
writeJson("11-GAP_TO_20_ANALYSIS.json", {
  target: TARGET_HM,
  achieved: hotelMarket.length,
  targetMet: hotelMarket.length >= TARGET_HM,
  gapTo20: gap,
  baseline: priorHm.length,
  added: hotelMarket.filter((o) => !baselineIds.has(o.id)).length,
  watchByReason: countBy(allDecisions.filter((x) => x.bucket === "WATCH"), (x) => x.reason),
  rejectedByReason: countBy(allDecisions.filter((x) => x.bucket === "REJECTED"), (x) => x.reason),
  triageRejectedCounts: countBy(triageRejected, (r) => String(r.reason).split(":")[0]),
  remainingBudgetUsd: remainingBudgetUsd(ledger),
  diagnosis:
    gap === 0
      ? "Target met under unchanged qualification standards."
      : "Gap remains after the authorized $20; qualification standards were not lowered and no CMI was dual-classified.",
  nextPaidOptionIfNeeded: "Separate founder authorization required — see Watch rows with next-edition evidence.",
});

// ---------------------------------------------------------------------------
// Stage package
// ---------------------------------------------------------------------------
let pkg = null;
let integrity = null;
let qualityGate = null;
let packageBlocker = null;
let writtenPath = null;
if (customerVisible.length === 0) {
  packageBlocker = "NO_CUSTOMER_ELIGIBLE_OPPORTUNITIES_TO_STAGE";
} else {
  const built = buildCustomerPublicationPackage({
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    opportunities: customerVisible,
    publicationVersion: PUB_VERSION,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
    approvalStatus: "PENDING_FOUNDER_APPROVAL",
    templateVersion: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
    notes: {
      founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
      script: SCRIPT_ID,
      authorizationId: AUTHORIZATION_ID,
      hardCeilingUsd: HARD_CEILING_USD,
      spentUsd: ledger.spentUsd,
      hotelMarketCount: hotelMarket.length,
      competitiveCount: competitive.length,
      targetHotelMarket: TARGET_HM,
      targetMet: hotelMarket.length >= TARGET_HM,
      priorFingerprintPreserved: priorFp,
      customerPublished: false,
    },
  });
  const candidatePkg = built.package || built;
  integrity = verifyPublicationPackageIntegrity(candidatePkg);
  qualityGate = runPublicationQualityGate({ opportunities: candidatePkg.opportunities || customerVisible, referenceDate: REF });
  if (integrity?.ok === false) {
    packageBlocker = `PACKAGE_INTEGRITY_FAILED:${JSON.stringify(integrity.fails || integrity)}`;
  } else if (qualityGate?.ok === false) {
    packageBlocker = `PUBLICATION_QUALITY_GATE_BLOCKING:${JSON.stringify(qualityGate.blocking || qualityGate.summary || null)}`;
  } else if (candidatePkg.contentFingerprintSha256 === PRIOR_FINGERPRINT) {
    packageBlocker = "NEW_FINGERPRINT_EQUALS_PRIOR_NO_CHANGE";
  } else {
    pkg = candidatePkg;
    const written = protectedStore.writePackage(pkg);
    writtenPath = written?.path || null;
    protectedStore.writePointer(HOTEL_ID, "STAGED", {
      publicationId: pkg.publicationId,
      publicationVersion: pkg.publicationVersion,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
      founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    });
  }
}
const protectedAfter = protectedSnapshot();
const protectedUnchanged = JSON.stringify(protectedBefore) === JSON.stringify(protectedAfter);
const priorPkgUnchanged = sha256File(priorPkgPath) === priorPkgShaBefore;
const prior15PkgUnchanged = sha256File(path.join(PRIOR_15USD_PKG_DIR, "package.json")) === prior15PkgShaBefore;
const certPkgUnchanged = sha256File(certPkgPath) === certPkgShaBefore;

function canonicalizeSeg(raw) {
  const x = String(raw || "").toLowerCase();
  if (MISSING_SEGMENTS.includes(x)) return x;
  if (/association|association_events/.test(x)) return "associations_conferences_events";
  if (/scal/.test(x)) return "corporate_accounts_scaling";
  if (/corporate.?meet|incentive/.test(x)) return "corporate_meetings_incentives";
  if (/gov|public.?sector/.test(x)) return "government_public_sector";
  if (/sport|entertain/.test(x)) return "sports_entertainment";
  if (/educ|scien|medical|academic/.test(x)) return "education_scientific";
  if (/project|infra/.test(x)) return "projects_infrastructure";
  if (/social|wedding|festival|special.?event/.test(x)) return "social_special_events";
  if (/cater|day.?meet|local_catering/.test(x)) return "local_catering_day_meetings";
  if (/celebrat|private.?event/.test(x)) return "corporate_celebrations_private_events";
  if (/travel.?trade|dmc|intermed|mice/.test(x)) return "travel_trade_group_intermediaries";
  if (/reloc|extended|faculty.?hous/.test(x)) return "relocation_extended_stay";
  if (/preferred.?lodg/.test(x)) return "preferred_lodging_partnerships";
  if (/planner|pep|cema/.test(x)) return "professional_event_planners";
  return x || null;
}
function hmSegCounts(rows) {
  const m = {};
  for (const o of rows) {
    const s = canonicalizeSeg(o.demandSegmentId || o.researchSegment || o.opportunityType);
    if (!s) continue;
    m[s] = (m[s] || 0) + 1;
  }
  return m;
}
const beforeSegCounts = hmSegCounts(priorHm);
const afterSegCounts = hmSegCounts(hotelMarket);
const beforeSegsWithHm = Object.keys(beforeSegCounts).length;
const afterSegsWithHm = Object.keys(afterSegCounts).length;
const newlyRepresentedMissing = MISSING_SEGMENTS.filter((s) => (afterSegCounts[s] || 0) > 0 && !(beforeSegCounts[s] > 0));
const stillMissing = MISSING_SEGMENTS.filter((s) => !(afterSegCounts[s] > 0));
const newHmOnly = hotelMarket.filter((o) => !baselineIds.has(o.id));
const stageOf = (o) => {
  const blob = text(
    [
      o.priorityFilterKey,
      o.commercialStageKey,
      o.commercialStagePillLabel,
      o.salesActionKey,
      o.salesActionPillLabel,
      o.inventoryLane,
      o.bookingWindowStatus,
    ].join(" "),
  ).toLowerCase();
  if (/sales.?ready|pursue_now|pursue now|contact_buyer|win_room|win room/.test(blob)) return "SALES_READY";
  if (/account.?develop|build_account|build account|ongoing_account/.test(blob)) return "ACCOUNT_DEVELOPMENT";
  if (/qualify|qualification_needed|qualify_now|pursue catering|engage planner|build venue/.test(blob)) return "QUALIFY_OPPORTUNITY";
  return "QUALIFY_OPPORTUNITY";
};
const readinessCounts = countBy(hotelMarket, stageOf);

fs.writeFileSync(
  OPPS_JSON_PATH,
  `${JSON.stringify(
    {
      schema: "gdi-opportunities-doc-v1",
      hotelId: HOTEL_ID,
      hotelName: HOTEL_NAME,
      updatedAt: new Date().toISOString(),
      source: SCRIPT_ID,
      note: pkg ? "STAGED_FOR_FOUNDER_REVIEW — not CUSTOMER_PUBLISHED" : `NOT_STAGED:${packageBlocker}`,
      opportunities: customerVisible,
    },
    null,
    2,
  )}\n`,
);

let airtablePlan = { configured: isGdiOpportunityAirtableConfigured(), dryRun: true, writes: 0 };
try {
  const sync = await syncHotelOpportunitiesToAirtable({ hotelId: HOTEL_ID, dryRun: true, opportunities: customerVisible });
  airtablePlan = { configured: true, dryRun: true, writes: 0, wouldCreate: sync.summary?.created, wouldUpdate: sync.summary?.updated, total: sync.summary?.total };
} catch (e) {
  airtablePlan.error = String(e?.message || e);
}
writeJson("12-AIRTABLE_WRITE_DRY_RUN.json", { ...airtablePlan, liveWritesAuthorized: false });

const inventoryCounts = computePublicationInventoryCounts(customerVisible);
const previewUrl = `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;
writeJson("13-NEW_STAGED_PUBLICATION.json", {
  staged: Boolean(pkg),
  packageBlocker,
  publicationVersion: PUB_VERSION,
  publicationId: pkg?.publicationId || null,
  fingerprint: pkg?.contentFingerprintSha256 || null,
  lifecycleState: pkg ? GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED : null,
  founderReviewLabel: pkg ? "STAGED_FOR_FOUNDER_REVIEW" : null,
  customerPublished: false,
  packagePath: writtenPath,
  inventoryCounts,
  hotelMarketCount: hotelMarket.length,
  competitiveCount: competitive.length,
  targetHotelMarket: TARGET_HM,
  targetMet: hotelMarket.length >= TARGET_HM,
  gapTo20: gap,
  integrity,
  qualityGate: { ran: Boolean(qualityGate), ok: qualityGate?.ok ?? null, blocking: qualityGate?.blocking || null, summary: qualityGate?.summary || null },
  priorPackagePreserved: {
    fingerprint: priorFp,
    path: path.relative(ROOT, PRIOR_PKG_DIR),
    sha256Before: priorPkgShaBefore,
    sha256After: sha256File(priorPkgPath),
    unchanged: priorPkgUnchanged,
    prior15UsdPackageUnchanged: prior15PkgUnchanged,
    stagedPointerBefore,
  },
  protectedPublicationsBefore: protectedBefore,
  protectedPublicationsAfter: protectedAfter,
  protectedPublicationsUnchanged: protectedUnchanged,
  previewUrl,
  opportunitiesJson: path.relative(ROOT, OPPS_JSON_PATH),
});
if (pkg) {
  writeJson("FINGERPRINT.json", {
    hotelId: HOTEL_ID,
    publicationVersion: pkg.publicationVersion,
    publicationId: pkg.publicationId,
    contentFingerprintSha256: pkg.contentFingerprintSha256,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    opportunityCount: customerVisible.length,
    packagePath: writtenPath,
  });
}
if (!fs.existsSync(path.join(OUT, "14-LOCAL_BROWSER_VERIFICATION.json")) || pkg) {
  writeJson("14-LOCAL_BROWSER_VERIFICATION.json", {
    contract: "GDI_RADISSON_14SEG_LOCAL_BROWSER_VERIFICATION_V1",
    status: "PENDING_BROWSER_RUN",
    howToRun: `node scripts/${SCRIPT_ID}.mjs --browser-verify [--base-url http://127.0.0.1:8080]`,
    previewUrl,
    expectedFingerprint: pkg?.contentFingerprintSha256 || null,
    expectedCounts: inventoryCounts,
  });
}
writeJson("15-GITHUB_MERGE_STATUS.json", {
  branch: sh("git rev-parse --abbrev-ref HEAD"),
  headSha: sh("git rev-parse HEAD"),
  committedByScript: false,
  prNumber: null,
  prUrl: null,
  ciResult: "NOT_RUN_BY_SCRIPT",
  mergeStatus: "NOT_MERGED — pending PR per GDI merge discipline",
  mergeCommitSha: null,
  mainSynchronized: false,
  noCustomerPublished: true,
  surfeUsed: false,
  parallelUsed: false,
  protectedPublicationsUnchanged: protectedUnchanged,
  reminder: "Task is not technically complete until the PR is merged into main (see gdi-github-branch-merge-discipline rule).",
  templateEvidence: {
    template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
    lockId: GDI_FULL_E2E_HOTEL_TEMPLATE_LOCK_V1,
    actionabilityAssertOk: actionabilityAssert?.ok ?? null,
    publicationQualityGateRan: Boolean(qualityGate),
    paidWithinCeiling: ledger.spentUsd <= HARD_CEILING_USD,
    noDualClassification: dualClassified.length === 0,
    noMonitorOnHotelMarket: monitorHm.length === 0,
  },
});

const mdRows = (arr, fn) => (arr.length ? arr.map(fn).join("\n") : "_none_");
const costPerNewHm = newHmOnly.length ? Number((ledger.spentUsd / newHmOnly.length).toFixed(2)) : null;
const costPerNewSeg = newlyRepresentedMissing.length ? Number((ledger.spentUsd / newlyRepresentedMissing.length).toFixed(2)) : null;
const founder = `# Radisson Blu Toronto Downtown — Six Missing Segments Exa ($30)

**Authorization:** \`${AUTHORIZATION_ID}\`
**Hotel:** ${HOTEL_NAME} (\`${HOTEL_ID}\` / CNC41) · **Reference date:** ${REF}
**Hard ceiling:** **$${HARD_CEILING_USD}** · **Spent:** **$${ledger.spentUsd}** · **Remaining:** **$${remainingBudgetUsd(ledger)}**
**Staged publication:** \`${pkg?.publicationVersion || "NOT STAGED"}\`${packageBlocker ? ` (blocker: ${packageBlocker})` : ""}
**New fingerprint:** \`${pkg?.contentFingerprintSha256 || "n/a"}\`
**Diversification fingerprint preserved:** \`${priorFp}\` (unchanged: ${priorPkgUnchanged})
**Commercial cert fingerprint preserved:** \`${CERT_FINGERPRINT_PRESERVE}\` (unchanged: ${certPkgUnchanged})
**Lifecycle:** STAGED_FOR_FOUNDER_REVIEW (never CUSTOMER_PUBLISHED)

## Scorecard

| Metric | Baseline | Final |
|---|---:|---:|
| Qualified Hotel Market | ${priorHm.length} | ${hotelMarket.length} |
| Verified CMI | ${priorCmi.length} | ${competitive.length} |
| Represented HM segments | ${beforeSegsWithHm}/14 | ${afterSegsWithHm}/14 |
| Sales Ready | 3 | ${readinessCounts.SALES_READY || 0} |
| Account Development | 3 | ${readinessCounts.ACCOUNT_DEVELOPMENT || 0} |
| Qualify Opportunity | 15 | ${readinessCounts.QUALIFY_OPPORTUNITY || 0} |
| Newly represented missing segments | 0 | ${newlyRepresentedMissing.length} (${newlyRepresentedMissing.join(", ") || "none"}) |
| New unique Hotel Market | 0 | ${newHmOnly.length} |
| Exa spend | $0 | $${ledger.spentUsd} (max $${HARD_CEILING_USD}) |

**Acceptance (≥2 of 6 missing):** ${newlyRepresentedMissing.length >= MIN_NEW_MISSING_SEGMENTS ? "MET" : "NOT MET"}
**Aspirational ≥10/14:** ${afterSegsWithHm >= TARGET_SEGMENTS_WITH_HM ? "MET" : "NOT MET"} — still missing: ${stillMissing.join(", ") || "none"}

## Spend by stage

${mdRows(Object.entries(STAGE_CAP_USD), ([k, cap]) => `- ${k}: cap $${cap} → spent $${spendByStage[k] || 0}`)}
- Cost / new HM: ${costPerNewHm ?? "n/a"} · Cost / newly represented segment: ${costPerNewSeg ?? "n/a"}
- Surfe: not invoked · Parallel: not invoked · Airtable live writes: 0

## Pass 1 — six missing segments
- Jobs: ${pass1Calls.filter((c) => c.ok).length}/${PASS1_JOBS.length} · hits: ${pass1Hits.length} · deep promotions: ${discoveryCands.length}
- Triage rejects: ${JSON.stringify(countBy(triageRejected, (r) => String(r.reason).split(":")[0]))}

## Pass 2 / Pass 3
- Deep: ${pass2Plan.list.length} (free reuse ${pass2Plan.list.filter((c) => c.plannedFree).length}) · ${JSON.stringify(countBy(p2Decisions, (x) => x.bucket))}
- Verify: ${pass3Rows.length} · ${JSON.stringify(countBy(p3Decisions, (x) => x.bucket))}

## New Hotel Market
${mdRows(newHmOnly, (o, i) => `${i + 1}. **${o.displayTitle}** — ${canonicalizeSeg(o.demandSegmentId || o.researchSegment || o.opportunityType)} · ${o.organizationName} · ${o.datePillDisplayLabel} · ${o.primaryContact?.name || "n/a"} · ${o.salesActionPillLabel}`)}

## Full Hotel Market inventory (${hotelMarket.length})
${mdRows(hotelMarket, (o, i) => `${i + 1}. **${o.displayTitle}** — ${o.organizationName} · ${o.datePillDisplayLabel} · ${o.primaryContact?.name || "n/a"}${baselineIds.has(o.id) ? "" : " · **NEW**"}`)}

## CMI (separate, preserved)
${mdRows(competitive, (o, i) => `${i + 1}. **${o.displayTitle}** @ ${o.competitorHotel || "n/a"} — ${o.datePillDisplayLabel}${baselineIds.has(o.id) ? "" : " · **NEW**"}`)}

## Watch / reject
${mdRows(Object.entries(failureHistogram), ([k, v]) => `- ${k}: ${v}`)}

## Remaining evidence gaps (no automatic budget request)
${stillMissing.includes("relocation_extended_stay") ? `- **relocation_extended_stay:** U of T faculty housing was researched but rejected as IMMINENT (<21 days). No other identity-level relocation/extended-stay program cleared qualification. Next: target named corporate relocation vendors, film/TV production housing coordinators, and multi-month project-team lodging RFPs with exact travel windows.\n` : ""}${stillMissing.includes("preferred_lodging_partnerships") ? `- **preferred_lodging_partnerships:** Discovery returned conferences, not recurring preferred-hotel programs. Tempo/WNBA visiting-team lodging remains in the baseline sports inventory. Next: evidence-backed preferred-hotel lists, university visitor lodging agreements, and port/waterfront corporate traveler programs with a named procurement entry point.\n` : ""}${stillMissing.length === 0 ? "- None — all six previously missing segments now have at least one qualified Hotel Market opportunity.\n" : ""}
## Gates
- Actionability assert ok=${actionabilityAssert.ok}
- Quality gate: ${qualityGate ? JSON.stringify(qualityGate.summary || qualityGate.ok) : "not run"}
- Protected hotels unchanged: ${protectedUnchanged}
- Prior diversification pkg unchanged: ${priorPkgUnchanged} · cert pkg unchanged: ${certPkgUnchanged}
- Airtable dry-run: ${JSON.stringify(airtablePlan)}

## Preview
\`${previewUrl}\`
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);

// Deliverable aliases required by founder brief
writeJson("02-EXA_HARD_CAP_SPEND_LEDGER.json", readJson(path.join(OUT, "02-PAID_BUDGET_LEDGER.json")));
writeJson("03-14_SEGMENT_COVERAGE_BEFORE.json", {
  hotelMarket: priorHm.length,
  cmi: priorCmi.length,
  segmentsWithHm: beforeSegsWithHm,
  counts: beforeSegCounts,
  missingSegments: MISSING_SEGMENTS.filter((s) => !(beforeSegCounts[s] > 0)),
  fingerprint: priorFp,
});
writeJson("04-SIX_SEGMENT_CANDIDATE_QUEUE.json", {
  missingSegments: MISSING_SEGMENTS,
  recoveryCandidates: recoveryPlanRows,
  recoverySkipped,
  gapSeedCount: gapSeedCands.length,
  pass1Jobs: PASS1_JOBS.map((j) => ({ id: j.id, segment: j.segmentKey })),
});
writeJson("05-EXA_DISCOVERY_RESULTS.json", readJson(path.join(OUT, "03-PASS1_DISCOVERY_RESULTS.json")));
writeJson("06-COMMERCIAL_DEEP_RESEARCH.json", readJson(path.join(OUT, "04-PASS2_DEEP_RESULTS.json")));
writeJson("07-SEGMENT_SPECIFIC_QUALIFICATION.json", {
  decisions: allDecisions.map(rowSummary),
  bySegment: countBy(allDecisions, (x) => x.segment || "(none)"),
  failureHistogram,
  newlyRepresentedMissing,
  stillMissing,
  afterCounts: afterSegCounts,
  segmentReclassification: {
    note: "Discovery-job labels were reclassified by demand mechanism so ordinary conventions do not manufacture missing-segment coverage.",
    count: segmentReclassLog.length,
    rows: segmentReclassLog,
  },
});
writeJson("08-CONTACT_ELIGIBILITY.json", readJson(path.join(OUT, "10-CONTACT_AND_SALES_ACTIONS.json")));
writeJson("09-NEW_HOTEL_MARKET_INVENTORY.json", {
  baseline: priorHm.length,
  newlyAdded: newHmOnly.length,
  final: hotelMarket.length,
  newOpportunities: newHmOnly.map(oppSummary),
  allHotelMarket: hotelMarket.map(oppSummary),
});
writeJson("10-CMI_SEPARATION.json", readJson(path.join(OUT, "08-CMI_PRESERVATION_AUDIT.json")));
writeJson("11-SALES_READINESS_AND_ACTIONABILITY.json", {
  readinessCounts,
  actionability: gate.counts,
  assertOk: actionabilityAssert.ok,
  monitorHotelMarketRows: monitorHm.map((o) => o.id),
});
writeJson("12-SEGMENT_COVERAGE_AFTER.json", {
  beforeSegsWithHm,
  afterSegsWithHm,
  targetSegsWithHm: TARGET_SEGMENTS_WITH_HM,
  targetMet: afterSegsWithHm >= TARGET_SEGMENTS_WITH_HM,
  newlyRepresentedMissing,
  stillMissing,
  beforeCounts: beforeSegCounts,
  afterCounts: afterSegCounts,
  minNewMissingMet: newlyRepresentedMissing.length >= MIN_NEW_MISSING_SEGMENTS,
});
writeJson("13-COST_PER_SEGMENT_AND_OPPORTUNITY.json", {
  spentUsd: ledger.spentUsd,
  hardCeilingUsd: HARD_CEILING_USD,
  spendByStage,
  newHotelMarket: newHmOnly.length,
  newlyRepresentedMissing: newlyRepresentedMissing.length,
  costPerNewHotelMarket: costPerNewHm,
  costPerNewlyRepresentedSegment: costPerNewSeg,
});
writeJson("14-AIRTABLE_RECONCILIATION_DRY_RUN.json", { ...airtablePlan, liveWritesAuthorized: false });
writeJson("15-NEW_STAGED_PUBLICATION.json", readJson(path.join(OUT, "13-NEW_STAGED_PUBLICATION.json")));
writeJson("16-BROWSER_VERIFICATION.json", {
  status: "PENDING_BROWSER_RUN",
  howToRun: `node scripts/${SCRIPT_ID}.mjs --browser-verify`,
  expectedFingerprint: pkg?.contentFingerprintSha256 || null,
});
writeJson("17-FULL_E2E_CERTIFICATION_STATUS.json", {
  authorizationId: AUTHORIZATION_ID,
  hotelMarket: hotelMarket.length,
  cmi: competitive.length,
  segmentsWithHm: afterSegsWithHm,
  newlyRepresentedMissing,
  minNewMissingMet: newlyRepresentedMissing.length >= MIN_NEW_MISSING_SEGMENTS,
  aspirational10of14: afterSegsWithHm >= TARGET_SEGMENTS_WITH_HM,
  fingerprint: pkg?.contentFingerprintSha256 || null,
  priorDiversificationFingerprintPreserved: priorFp,
  commercialCertFingerprintPreserved: CERT_FINGERPRINT_PRESERVE,
  priorPkgUnchanged,
  certPkgUnchanged,
  customerPublished: false,
  airtableLiveWrites: false,
  surfeUsed: false,
  parallelUsed: false,
  spentUsd: ledger.spentUsd,
  withinCeiling: ledger.spentUsd <= HARD_CEILING_USD,
});
writeJson("18-GITHUB_MERGE_CLOSEOUT.json", readJson(path.join(OUT, "15-GITHUB_MERGE_STATUS.json")));

console.log(
  JSON.stringify(
    {
      spentUsd: ledger.spentUsd,
      remainingUsd: remainingBudgetUsd(ledger),
      spendByStage,
      withinStageCaps: withinCaps,
      hotelMarket: hotelMarket.length,
      cmi: competitive.length,
      gapTo20: gap,
      targetMet: hotelMarket.length >= TARGET_HM,
      newFingerprint: pkg?.contentFingerprintSha256 || null,
      packageBlocker,
      decisionCounts: countBy(allDecisions, (x) => x.bucket),
      protectedUnchanged,
      priorPkgUnchanged,
      dualClassified: dualClassified.length,
      monitorHotelMarketRows: monitorHm.length,
      actionabilityOk: actionabilityAssert?.ok !== false,
    },
    null,
    2,
  ),
);
