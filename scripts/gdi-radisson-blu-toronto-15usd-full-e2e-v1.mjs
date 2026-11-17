#!/usr/bin/env node
/**
 * Radisson Blu Toronto Downtown — Full E2E with a HARD $15 incremental paid Exa ceiling.
 *
 * Authorization: FOUNDER_APPROVE_RADISSON_BLU_TORONTO_PAID_FULL_E2E_15USD_20261115
 *   - Exa ONLY (no Surfe, no Parallel). Max USD $15 incremental.
 *   - No CUSTOMER_PUBLISHED. No Airtable live writes (dry-run only).
 *   - Bethesda / Sheraton / Wyndham publications are never touched (pointer + file hash proof).
 *
 * Batches (HIGH deep_research = $0.50 / run; MEDIUM discovery = ~$0.10 / run):
 *   BATCH_A  cap $4  — named events (ODA ASM27, OWWA/WEAO, ISCT, ESA, GBTA Canada) + up to 3 strongest queued
 *   BATCH_B  cap $6  — the 12 unique prior AWAITING_FOUNDER_PROMOTION opportunityIds (never auto-promoted)
 *   BATCH_C  cap $5  — CMI packaging (OWWA/WEAO competitor hotels, GBTA@Westin Harbour Castle, ledger
 *                      competitor-venue events) + targeted Toronto competitor-committed MEDIUM discovery
 *
 * Modes:
 *   node scripts/gdi-radisson-blu-toronto-15usd-full-e2e-v1.mjs                   # baseline + plan only ($0)
 *   node scripts/gdi-radisson-blu-toronto-15usd-full-e2e-v1.mjs --authorize-paid  # paid Exa batches A/B/C + stage
 *   node scripts/gdi-radisson-blu-toronto-15usd-full-e2e-v1.mjs --assemble-cached # re-qualify from raw-runs cache, $0
 *   node scripts/gdi-radisson-blu-toronto-15usd-full-e2e-v1.mjs --browser-verify  # local preview check (14-*.json)
 *
 * Re-runs resume the on-disk ledger and reuse completed raw runs (a stored run is never re-paid).
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

// ---------------------------------------------------------------------------
// constants
// ---------------------------------------------------------------------------
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recH9h3McgMNFQEla";
const HOTEL_NAME = "Radisson Blu Toronto Downtown";
const REF = "2026-11-15";
const HARD_CEILING_USD = 15;
const AUTHORIZATION_ID =
  "FOUNDER_APPROVE_RADISSON_BLU_TORONTO_PAID_FULL_E2E_15USD_20261115";
const SCRIPT_ID = "gdi-radisson-blu-toronto-15usd-full-e2e-v1";
const OUT = path.join(ROOT, "reports", "gdi-radisson-blu-toronto-paid-full-e2e-15usd-v1");
const RAW_DIR = path.join(OUT, "raw-runs");
const LEDGER_PATH = path.join(OUT, "paid-budget-ledger.json");
const HOTEL_DATA = path.join(ROOT, "data/group-demand-intelligence/hotels", HOTEL_ID);
const DISCOVERY_LEDGER_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/discovery-ledgers",
  HOTEL_ID,
  "four-hotel-14-workflow-full-e2e-v1.json",
);
const BASELINE_REPORT_DIR = path.join(ROOT, "reports/gdi-radisson-blu-toronto-full-e2e-v1");
const PUB_VERSION = `gdi_pub_${HOTEL_ID}_20261115_radisson_blu_toronto_15usd_full_e2e_v1`;
const PROTECTED_STORE_ROOT = path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages");
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

/** Per-stage USD caps inside the single $15 hard ceiling. */
const STAGE_CAP_USD = { BATCH_A: 4, BATCH_B: 6, BATCH_C: 5 };
const MAX_DEEP_RUNS = { BATCH_A: 8, BATCH_B: 12, BATCH_C: 10 };
const CONCURRENCY = 2;
const HIGH_UNIT = Number(quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.HIGH }).maxUsd);
const MEDIUM_UNIT = Number(quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM }).maxUsd);

if (HOTEL_ID !== "recH9h3McgMNFQEla") {
  console.error("Unexpected hotel id");
  process.exit(3);
}
if (Object.values(STAGE_CAP_USD).reduce((a, b) => a + b, 0) > HARD_CEILING_USD + 1e-9) {
  console.error("Stage caps exceed hard ceiling");
  process.exit(3);
}

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
  demandTerritory: {
    label: "Toronto — Harbourfront / Queens Quay / downtown / MTCC / Exhibition Place",
  },
  positioning:
    "157-room Radisson Blu on Toronto's Harbourfront (Queens Quay West); ~11,000 sq ft over 20 meeting rooms; walkable to the Metro Toronto Convention Centre; restaurant/lounge status must be verified before catering claims",
};

const HOTEL_FIT_COPY =
  "Radisson Blu Toronto Downtown is a 157-room Harbourfront hotel at 249 Queens Quay West with about 11,000 sq ft across 20 meeting rooms (Admiral Ballroom 3,151 sq ft, roughly 240 theatre / 224 banquet), a walk from the Metro Toronto Convention Centre — a credible overflow, mid-size group and ancillary meeting option, not exclusive housing for 2,000+ attendee convention shows.";

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
function findCompetitors(blob) {
  const s = String(blob || "");
  return COMPETITOR_CATALOG.filter(([, rx]) => rx.test(s)).map(([name]) => name);
}

const HOTEL_LIKE_ORG_REGEX =
  /\b(?:hotel|hotels|resort|resorts|hospitality|hyatt|marriott|hilton|sheraton|westin|fairmont|intercontinental|radisson|delta hotels|four seasons)\b/i;

const MTCC_LIKE_VENUE_REGEX =
  /metro(?:politan)? toronto convention|\bmtcc\b|exhibition place|enercare|beanfield|national trade cent(?:re|er)|scotiabank arena|rogers centre/i;

const MONTHS = {
  jan: 1, january: 1, janvier: 1, feb: 2, february: 2, fevrier: 2, mar: 3, march: 3, mars: 3,
  apr: 4, april: 4, avril: 4, may: 5, mai: 5, jun: 6, june: 6, juin: 6, jul: 7, july: 7, juillet: 7,
  aug: 8, august: 8, aout: 8, sep: 9, sept: 9, september: 9, septembre: 9, oct: 10, october: 10,
  octobre: 10, nov: 11, november: 11, novembre: 11, dec: 12, december: 12, decembre: 12,
};
const MONTH_RE =
  "jan(?:uary)?|janvier|feb(?:ruary)?|fevrier|mar(?:ch)?|mars|apr(?:il)?|avril|may|mai|jun(?:e)?|juin|jul(?:y)?|juillet|aug(?:ust)?|aout|sep(?:t(?:ember)?)?|septembre|oct(?:ober)?|octobre|nov(?:ember)?|novembre|dec(?:ember)?|decembre";

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
function sha256File(p) {
  return fs.existsSync(p) ? crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex") : null;
}
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
const monthNum = (w) => MONTHS[String(w || "").toLowerCase().replace(/\.$/, "")] || null;
const countBy = (arr, fn) => {
  const m = {};
  for (const x of arr) {
    const k = fn(x) ?? "(none)";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
};

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
function dateLabel(start, end) {
  if (!start) return "Dates TBD";
  const f = (d) =>
    new Date(`${d}T12:00:00Z`).toLocaleDateString("en-US", {
      month: "short", day: "numeric", year: "numeric", timeZone: "UTC",
    });
  return end && end !== start ? `${f(start)} – ${f(end)}` : f(start);
}
const addDays = (d, n) => new Date(Date.parse(`${d}T12:00:00Z`) + n * 86400000).toISOString().slice(0, 10);
const daysBetween = (a, b) => Math.round((Date.parse(`${b}T12:00:00Z`) - Date.parse(`${a}T12:00:00Z`)) / 86400000);
/** Events starting inside 21 days cannot realistically be sourced — excluded, not padded. */
const IMMINENT_CUTOFF = addDays(REF, 21);

/** Toronto-specific territory: Harbourfront/downtown/MTCC/Exhibition Place core; generic Toronto nearby. */
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
  /catering|banquet|meeting|conference|training|congress|convention|seminar|reception/i.test(potential || "")
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

// ---------------------------------------------------------------------------
// --browser-verify (standalone; writes 14-LOCAL_BROWSER_VERIFICATION.json)
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
    contract: "GDI_RADISSON_LOCAL_BROWSER_VERIFICATION_V1",
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
    const t = setTimeout(() => ctl.abort(), 6000);
    const r = await fetch(apiUrl, { signal: ctl.signal });
    clearTimeout(t);
    result.checks.apiHttpStatus = r.status;
    const api = await r.json().catch(() => null);
    result.checks.apiFingerprint = api?.publication?.contentFingerprintSha256 || null;
    result.checks.fingerprintMatch = result.checks.apiFingerprint === meta.fingerprint;
    result.checks.inventoryCounts = api?.publication?.inventoryCounts || null;
    const ops = Array.isArray(api?.opportunities) ? api.opportunities : [];
    result.checks.apiOpportunityCount = ops.length;
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
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  const shotDir = path.join(OUT, "screenshots");
  fs.mkdirSync(shotDir, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
    // GDI preview hydrates after API; wait for opportunity cards (not the loading toast).
    await page.waitForSelector(
      'article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]',
      { timeout: 90000 },
    );
    await page.waitForTimeout(1500);
    result.checks.dom = await page.evaluate(() => {
      const body = document.body?.innerText || "";
      const cards = [
        ...document.querySelectorAll(
          'article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]',
        ),
      ];
      const titles = cards.map((c) => (c.innerText || "").split("\n")[0].trim().slice(0, 120));
      return {
        cards: cards.length,
        titles,
        mentionsHotel: /Radisson Blu Toronto/i.test(body),
        scopeAll14: /All\s*\(14\)/i.test(body),
        scopeHotel3: /Hotel\s*\(3\)/i.test(body),
        scopeCompetitive11: /Competitive\s*\(11\)/i.test(body),
        providerLeak: /\bExa\b|Surfe|Parallel|Airtable/.test(body),
        stillLoading: /Loading Group Demand Intelligence/i.test(body),
      };
    });
    await page.screenshot({ path: path.join(shotDir, "01-radisson-staged-overview.png"), fullPage: true });
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
// STAGE 0 — zero-cost baseline from the recovered discovery ledger
// ---------------------------------------------------------------------------
fs.mkdirSync(RAW_DIR, { recursive: true });

const ledgerDoc = readJson(DISCOVERY_LEDGER_PATH);
const ROWS = ledgerDoc.candidates || [];
const HOTEL_CFG = readJson(path.join(ROOT, "config/group-demand-intelligence/hotels", `${HOTEL_ID}.json`));

function rowSF(r) {
  return r.structuredExaCandidateFields || {};
}
function rowTitle(r) {
  const s = rowSF(r);
  return text(r.programmeName || s.verifiedEventName || s.eventIdentity || s.programmeName || r.originalResultTitle || "");
}
function rowVenue(r) {
  const s = rowSF(r);
  return text(s.verifiedVenue || s.incumbentVenue || "");
}
function rowStart(r) {
  const s = rowSF(r);
  return parseIso(s.verifiedStartDate) || parseIso(s.futureEditionDate) || extractExplicitDates(`${s.verifiedDates || ""}`)[0] || null;
}
function rowUrls(r) {
  const s = rowSF(r);
  return [
    ...(s.evidenceUrls || []),
    ...(s.supportingSources || []).map((x) => x?.url),
    ...(r.originalSourceUrls || []),
  ].filter((u) => /^https?:\/\//i.test(String(u || "")));
}
function priorLabelOf(r) {
  const ev = (r.disposition?.evidence || []).find((e) => /^acceptance=/.test(e));
  return ev ? ev.replace(/^acceptance=/, "") : null;
}
function candFromRow(r, batch) {
  const s = rowSF(r);
  const start = rowStart(r);
  const pfx = batch === "BATCH_C" ? "rbc" : "rb";
  const id = r.opportunityId ? `${pfx}_${r.opportunityId}` : `${pfx}_${r.discoveryId}`;
  return {
    id,
    batch,
    discoveryId: r.discoveryId,
    opportunityId: r.opportunityId || null,
    title: rowTitle(r) || text(r.organizationName),
    organizationName: text(r.organizationName || s.organizationName || s.organizingEntity),
    segmentId: r.segmentId,
    ledgerStatus: r.researchStatus,
    excerpt: text(s.demandMechanismSummary || s.underlyingEvidence || s.rankScoreExplain || r.originalResultExcerpt).slice(0, 500),
    evidenceUrls: [...new Set(rowUrls(r))],
    ledgerVerifiedDates: text(s.verifiedDates || ""),
    ledgerStart: start,
    ledgerVenue: rowVenue(r),
    ledgerSourcingStatus: text(s.sourcingStatus || s.hotelSourcingStatus || ""),
    ledgerAttendance: Number(s.attendancePublished) || null,
    priorLabel: priorLabelOf(r),
  };
}
const rowIsPast = (r) => {
  const st = rowStart(r);
  return st ? st < IMMINENT_CUTOFF : false;
};
const rowCompetitorVenue = (r) => COMPETITOR_REGEX.test(`${rowVenue(r)} ${text(rowSF(r).incumbentVenue)}`);
const rowKey = (r) => `${normKey(r.organizationName).slice(0, 24)}|${normKey(rowTitle(r)).slice(0, 28)}`;

function bestRow(pred, statuses = ["QUEUED_FOR_DEEP_RESEARCH", "RESEARCH_CONTINUATION", "AWAITING_FOUNDER_PROMOTION"]) {
  const hit = ROWS.filter((r) => pred(r) && statuses.includes(r.researchStatus));
  hit.sort((a, b) => {
    const sa = (rowStart(a) ? 2 : 0) + (rowVenue(a) ? 1 : 0) + (a.researchStatus === "QUEUED_FOR_DEEP_RESEARCH" ? 1 : 0);
    const sb = (rowStart(b) ? 2 : 0) + (rowVenue(b) ? 1 : 0) + (b.researchStatus === "QUEUED_FOR_DEEP_RESEARCH" ? 1 : 0);
    return sb - sa;
  });
  return hit[0] || null;
}

// ----- Batch A selection -----
const NAMED_A = [
  ["ODA_ASM27", (r) => /ontario dental/i.test(r.organizationName) && /asm27|spring meeting/i.test(rowTitle(r))],
  ["OWWA_WEAO_2027", (r) => /owwa|ontario water works/i.test(r.organizationName) && /2027/.test(rowTitle(r) + rowStart(r))],
  ["ISCT_2027", (r) => /cell\s*&\s*gene|isct/i.test(`${r.organizationName} ${rowTitle(r)}`) && /2027/.test(rowTitle(r) + rowStart(r))],
  ["ESA_2027", (r) => /ecological society of america/i.test(r.organizationName)],
  ["GBTA_CANADA_2027", (r) => /gbta canada/i.test(r.organizationName) && /2027/.test(rowTitle(r) + rowStart(r))],
];
const namedARows = NAMED_A.map(([key, pred]) => ({ key, row: bestRow(pred) })).filter((x) => x.row);
const namedAIds = new Set(namedARows.map((x) => rowKey(x.row)));
const namedACands = namedARows.map(({ key, row }) => ({ ...candFromRow(row, "BATCH_A"), namedKey: key }));

function scoreAssocEdu(r) {
  const st = rowStart(r);
  const s = rowSF(r);
  let sc = 0;
  if (st && st >= IMMINENT_CUTOFF) sc += 4;
  if (MTCC_LIKE_VENUE_REGEX.test(rowVenue(r))) sc += 2;
  if (/toronto/i.test(`${s.eventLocationCity || ""} ${s.destination || ""}`)) sc += 1;
  if (Number(s.attendancePublished)) sc += 1;
  if (/2027|2028/.test(rowTitle(r))) sc += 2;
  if (r.segmentId === "association_events") sc += 1;
  if (Number(s.attendancePublished) >= 5000) sc -= 1; // mega-convention: fit is conditional
  if (/2026|2025/.test(rowTitle(r)) && !st) sc -= 3; // likely already past
  return sc;
}
const extraACandidates = ROWS.filter(
  (r) =>
    r.researchStatus === "QUEUED_FOR_DEEP_RESEARCH" &&
    ["association_events", "education_scientific"].includes(r.segmentId) &&
    !namedAIds.has(rowKey(r)) &&
    !rowCompetitorVenue(r) && // competitor-venue events are routed to Batch C (CMI)
    !rowIsPast(r) &&
    !/^(GBTA|Ontario Dental|Ecological|OWWA|International Society for Cell)/i.test(r.organizationName),
)
  .map((r) => ({ r, sc: scoreAssocEdu(r) }))
  .sort((a, b) => b.sc - a.sc);
const seenAKeys = new Set();
const extraA = [];
for (const { r, sc } of extraACandidates) {
  const k = rowKey(r);
  if (seenAKeys.has(k)) continue;
  seenAKeys.add(k);
  extraA.push({ ...candFromRow(r, "BATCH_A"), namedKey: null, selectionScore: sc });
  if (extraA.length >= 3) break;
}
const BATCH_A_CANDS = [...namedACands, ...extraA].slice(0, MAX_DEEP_RUNS.BATCH_A);

// ----- Batch B selection: 12 unique prior AWAITING_FOUNDER_PROMOTION opportunityIds -----
const awaitingRows = ROWS.filter((r) => r.researchStatus === "AWAITING_FOUNDER_PROMOTION");
const awaitingById = new Map();
for (const r of awaitingRows) {
  const id = r.opportunityId;
  if (!id) continue;
  if (!awaitingById.has(id)) awaitingById.set(id, []);
  awaitingById.get(id).push(r);
}
const BATCH_B_CANDS = [...awaitingById.entries()].slice(0, MAX_DEEP_RUNS.BATCH_B).map(([oppId, rows]) => {
  const rep = rows[0];
  return {
    ...candFromRow(rep, "BATCH_B"),
    linkedLedgerProgrammes: [...new Set(rows.map((x) => `${text(x.organizationName)} — ${rowTitle(x)}`))],
    linkedLedgerRowCount: rows.length,
    priorLabel: priorLabelOf(rep),
    opportunityId: oppId,
  };
});

// ----- Batch C known CMI candidates -----
const CORE_C_PREDS = [
  ["OWWA_WEAO_2027_CMI", (r) => /owwa|ontario water works/i.test(r.organizationName) && /2027/.test(rowTitle(r) + rowStart(r)), "OWWA/WEAO competitor-hotel room blocks (Hotel X, Sheraton Centre, Hyatt Regency, Le Méridien)"],
  ["GBTA_CANADA_2027_WESTIN_CMI", (r) => /gbta canada/i.test(r.organizationName) && /2027/.test(rowTitle(r) + rowStart(r)), "GBTA Canada 2027 @ The Westin Harbour Castle"],
];
const coreCRows = CORE_C_PREDS.map(([key, pred, why]) => ({ key, why, row: bestRow(pred) })).filter((x) => x.row);
const coreCKeys = new Set(coreCRows.map((x) => rowKey(x.row)));
const coreCCands = coreCRows.map(({ key, why, row }) => ({ ...candFromRow(row, "BATCH_C"), namedKey: key, selectionWhy: why }));
const batchCExclude = new Set([...namedAIds, ...BATCH_B_CANDS.map((c) => rowKey(ROWS.find((r) => r.discoveryId === c.discoveryId)))]);
const knownCmiRows = ROWS.filter(
  (r) =>
    ["QUEUED_FOR_DEEP_RESEARCH", "RESEARCH_CONTINUATION"].includes(r.researchStatus) &&
    rowCompetitorVenue(r) &&
    !rowIsPast(r) &&
    !coreCKeys.has(rowKey(r)) &&
    !batchCExclude.has(rowKey(r)),
);
const seenCKeys = new Set();
const knownCmiCands = [];
for (const r of knownCmiRows.sort((a, b) => (rowStart(a) || "9").localeCompare(rowStart(b) || "9"))) {
  const k = rowKey(r);
  if (seenCKeys.has(k)) continue;
  seenCKeys.add(k);
  knownCmiCands.push({
    ...candFromRow(r, "BATCH_C"),
    namedKey: null,
    selectionWhy: `Ledger shows competitor venue/hotel: ${rowVenue(r)}`,
  });
}
const BATCH_C_KNOWN = [...coreCCands, ...knownCmiCands].slice(0, MAX_DEEP_RUNS.BATCH_C);

function planRow(c) {
  return {
    id: c.id,
    namedKey: c.namedKey || null,
    title: c.title,
    org: c.organizationName,
    segment: c.segmentId,
    ledgerStatus: c.ledgerStatus,
    ledgerDates: c.ledgerVerifiedDates || null,
    ledgerVenue: c.ledgerVenue || null,
    priorLabel: c.priorLabel || null,
    selectionScore: c.selectionScore ?? null,
    selectionWhy: c.selectionWhy || null,
    linkedLedgerProgrammes: c.linkedLedgerProgrammes || undefined,
  };
}

const protectedStore = createGdiPublicationPackageStore({ root: PROTECTED_STORE_ROOT });
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
const priorOppsPath = path.join(HOTEL_DATA, "opportunities.json");
const priorOppsCount = fs.existsSync(priorOppsPath)
  ? (readJson(priorOppsPath).opportunities || []).length
  : 0;

writeJson("01-BASELINE_AND_RECOVERY.json", {
  contract: "GDI_RADISSON_PAID_15USD_BASELINE_V1",
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  referenceDate: REF,
  authorizationId: AUTHORIZATION_ID,
  hardCeilingUsd: HARD_CEILING_USD,
  surfe: "OFF",
  airtable: "DRY_RUN_ONLY",
  customerPublished: false,
  hotelProfile: {
    rooms: HOTEL_CFG.capabilityProfile?.totalGuestrooms,
    suites: HOTEL_CFG.capabilityProfile?.suites,
    meetingSqFt: HOTEL_CFG.capabilityProfile?.totalMeetingSpaceSqFt,
    meetingRooms: HOTEL_CFG.capabilityProfile?.meetingRoomCount,
    largestRoom: HOTEL_CFG.capabilityProfile?.largestRoomName,
    classification: HOTEL_CFG.capabilityProfile?.classification,
    capabilityNote: HOTEL_CFG.capabilityProfile?.notes,
    demandTerritory: HOTEL_CFG.demandTerritory,
  },
  baseline: {
    discoveryLedgerPath: path.relative(ROOT, DISCOVERY_LEDGER_PATH),
    discoveryLedgerSha256: sha256File(DISCOVERY_LEDGER_PATH),
    candidateCount: ROWS.length,
    statusCounts: countBy(ROWS, (r) => r.researchStatus),
    segmentCounts: countBy(ROWS, (r) => r.segmentId),
    awaitingPromotionRows: awaitingRows.length,
    awaitingPromotionUniqueIds: awaitingById.size,
    deepArtifactsOnDisk: ROWS.filter((r) => r.deepArtifactPath).length,
    zeroCostRecoveryReportDir: path.relative(ROOT, BASELINE_REPORT_DIR),
    zeroCostRecoveryReportPresent: fs.existsSync(path.join(BASELINE_REPORT_DIR, "FOUNDER_REPORT.md")),
    priorOpportunitiesJsonCount: priorOppsCount,
  },
  selection: {
    batchA: BATCH_A_CANDS.map(planRow),
    batchB: BATCH_B_CANDS.map(planRow),
    batchCKnown: BATCH_C_KNOWN.map(planRow),
    namedEventsUnresolved: NAMED_A.map(([k]) => k).filter((k) => !namedARows.some((x) => x.key === k)),
  },
  protectedPublicationsBefore: protectedBefore,
  git: { branch: sh("git rev-parse --abbrev-ref HEAD"), headSha: sh("git rev-parse HEAD") },
  policy: [
    "Prior AWAITING_FOUNDER_PROMOTION / Sales Ready labels are NOT auto-promoted — each must be re-qualified from fresh deep research.",
    "Hotel fit is assessed per event: 157 rooms = overflow / mid-group / ancillary, never exclusive housing for 2,000+ MTCC shows.",
  ],
});

const plannedSpendMaxUsd =
  BATCH_A_CANDS.length * HIGH_UNIT + BATCH_B_CANDS.length * HIGH_UNIT + BATCH_C_KNOWN.length * HIGH_UNIT;
console.log(
  `[baseline] ledger=${ROWS.length} awaitingUnique=${awaitingById.size} A=${BATCH_A_CANDS.length} B=${BATCH_B_CANDS.length} C-known=${BATCH_C_KNOWN.length} plannedMaxUsd(deep only)=${plannedSpendMaxUsd}`,
);

if (!AUTHORIZE && !ASSEMBLE_CACHED) {
  const planDoc = (batch, cands) => ({
    batch,
    status: "PLANNED_NOT_EXECUTED",
    capUsd: STAGE_CAP_USD[batch],
    unitCostUsd: HIGH_UNIT,
    maxRuns: MAX_DEEP_RUNS[batch],
    plannedRuns: cands.length,
    plannedMaxUsd: Number((cands.length * HIGH_UNIT).toFixed(2)),
    targets: cands.map(planRow),
  });
  writeJson("02-PAID_BUDGET_LEDGER.json", {
    status: fs.existsSync(LEDGER_PATH) ? "EXISTING_LEDGER_ON_DISK" : "NOT_STARTED",
    authorizationId: AUTHORIZATION_ID,
    hardCeilingUsd: HARD_CEILING_USD,
    stageCapsUsd: STAGE_CAP_USD,
    ledger: fs.existsSync(LEDGER_PATH) ? readJson(LEDGER_PATH) : null,
  });
  writeJson("03-BATCH_A_RESULTS.json", planDoc("BATCH_A", BATCH_A_CANDS));
  writeJson("04-BATCH_B_RESULTS.json", planDoc("BATCH_B", BATCH_B_CANDS));
  writeJson("05-BATCH_C_CMI_RESULTS.json", planDoc("BATCH_C", BATCH_C_KNOWN));
  console.log("[baseline-only] Re-run with --authorize-paid to spend within the $15 ceiling (Exa only).");
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

let ledger =
  loadPaidBudgetLedger(LEDGER_PATH, { authorizationId: AUTHORIZATION_ID, hardCeilingUsd: HARD_CEILING_USD, effort: EXA_AGENT_EFFORT.HIGH }) ||
  createPaidBudgetLedger({
    authorizationId: AUTHORIZATION_ID,
    hardCeilingUsd: HARD_CEILING_USD,
    effort: EXA_AGENT_EFFORT.HIGH,
    priorSpendUsd: 0,
    outPath: LEDGER_PATH,
  });
persistPaidBudgetLedger(ledger);
const resumedSpendUsd = ledger.spentUsd;
if (ledger.hardCeilingUsd !== HARD_CEILING_USD) {
  console.error("Ledger ceiling mismatch");
  process.exit(3);
}

const stageInflight = {};
function stageSpentUsd(stage) {
  const done = ledger.operations
    .filter((o) => o.stage === stage)
    .reduce((a, o) => a + Number(o.actualCostUsd || 0), 0);
  return done + (stageInflight[stage] || 0);
}
const stageRemainingUsd = (stage) => Number(Math.max(0, (STAGE_CAP_USD[stage] ?? 0) - stageSpentUsd(stage)).toFixed(6));

/** Single choke point for every paid call: stage cap + global ceiling + reservation. Synchronous check→reserve. */
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

/** Reuse completed raw runs on disk so a re-run never re-pays (includes charged-but-failed runs). */
function loadRaw(key) {
  const f = path.join(RAW_DIR, `${key}.json`);
  if (!fs.existsSync(f)) return null;
  try {
    const j = readJson(f);
    if (j?.paid?.ok && j?.paid?.result) return j;
  } catch (e) {
    console.warn(`[raw-cache] unreadable ${key}: ${String(e?.message || e)}`);
  }
  return null;
}
const saveRaw = (key, obj) => fs.writeFileSync(path.join(RAW_DIR, `${key}.json`), JSON.stringify(obj, null, 2));

writeJson("00-RESEARCH_AUTHORIZATION_AND_BUDGET.json", {
  authorizationId: AUTHORIZATION_ID,
  hardCeilingUsd: HARD_CEILING_USD,
  incrementalOnly: true,
  providers: { exa: true, parallel: false, surfe: false, surfeAutoInvoke: false },
  quotes: { medium: quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM }), high: quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.HIGH }) },
  stageCapsUsd: STAGE_CAP_USD,
  maxDeepRuns: MAX_DEEP_RUNS,
  concurrency: CONCURRENCY,
  resumedSpendUsdAtStart: resumedSpendUsd,
  mode: LIVE_PAID ? "PAID_AUTHORIZED" : "ASSEMBLE_CACHED_NO_SPEND",
  constraints: [
    "No Surfe", "No Parallel", "No CUSTOMER_PUBLISHED", "Airtable dry-run only (0 writes)",
    "Bethesda / Sheraton / Wyndham publications unchanged",
  ],
  referenceDate: REF,
  horizon: "2026-12 through 2028-06",
});

// ---------------------------------------------------------------------------
// deep-research context + structured output parsing
// ---------------------------------------------------------------------------
function hotelFitConstraints() {
  return [
    "HOTEL FIT CONSTRAINTS (apply honestly — unknown is not zero, but do not inflate fit):",
    "- 157 guestrooms (19 suites); ~11,000 sq ft over 20 meeting rooms; largest room Admiral Ballroom 3,151 sq ft (~240 theatre / ~224 banquet / ~280 reception).",
    "- Harbourfront / Queens Quay West, walkable to the Metro Toronto Convention Centre. Restaurant/lounge may be closed — do not assume catering capability.",
    "- The hotel is NOT exclusive housing for 2,000+ attendee MTCC / Exhibition Place conventions. Realistic roles: delegate/speaker overflow, mid-size groups (≈15–109 peak rooms), VIP/executive lodging, side meetings and receptions.",
    "- Territory: Toronto Harbourfront, downtown, MTCC, Exhibition Place. Toronto-wide mega-conventions are NOT automatic demand — state whether this event can realistically use a 157-room hotel.",
  ].join("\n");
}
function deepContext(cand, mode) {
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
  if (cand.linkedLedgerProgrammes?.length > 1) {
    lines.push(`Other ledger rows sharing this id (context only — research the primary target): ${cand.linkedLedgerProgrammes.join(" | ")}`);
  }
  if (cand.priorLabel) {
    lines.push(`NOTE: a prior thin discovery labelled this "${cand.priorLabel}". That label is NOT evidence — re-verify everything independently.`);
  }
  lines.push(
    "",
    "REQUIREMENTS:",
    `- Verify ACTUAL activity dates (not article dates). Today is ${REF}; horizon Dec 2026 – Jun 2028. If the researched edition is already past, report the NEXT announced edition (dates + city) only if officially published; otherwise say none.`,
    "- State the exact venue; whether it is in Toronto proper (Harbourfront/downtown/MTCC/Exhibition Place) or elsewhere.",
    "- State how lodging is sourced: official housing bureau, host hotel, published room blocks (name EVERY hotel listed), open RFP, or unknown. Quote the source.",
    "- Report published or estimated attendance and any peak-room / room-night figure; do not invent numbers.",
    "- Identify NAMED commercially relevant contacts (organizer, events/conference/housing manager, executive director) with role AND source URL. Do not invent emails, RFPs or room blocks.",
    "- Do not treat generic hotel marketing or 'preferred hotel list' pages as a booking commitment unless edition-specific.",
  );
  if (mode === "cmi") {
    lines.push(
      "- COMPETITIVE INTELLIGENCE FOCUS: identify which competing Toronto hotel(s) are committed for this future edition (host hotel, event venue, published room block, group rate/booking code), the type of commitment, edition-specific evidence URLs, whether the event rotates/recurs in Toronto, and the organizer contact for future-cycle account development. Competing set examples: Westin Harbour Castle, Hotel X, Sheraton Centre, Hyatt Regency Toronto, Le Méridien Toronto Pinnacle, Fairmont Royal York, Marriott City Centre, Hilton Toronto, InterContinental Toronto Centre.",
      "- Also state honestly whether Radisson Blu Toronto Downtown has any realistic overflow / ancillary role for the committed edition.",
    );
  } else {
    lines.push(`- Produce a hotel demand thesis for ${HOTEL_NAME}; state honestly if lodging is already committed to other hotels or a housing bureau, and if the event is too large/small for a 157-room hotel.`);
  }
  return lines.join("\n");
}

function extractDeep(structured) {
  const s = structured || {};
  const ov = s.eventsOpportunityOverviewFields || {};
  const fe = ov.futureEdition || {};
  const cib = s.cibMappingHints || {};
  const f = s.findings || {};
  const start = parseIso(fe.startDate) || parseIso(ov.verifiedStartDate);
  const end = parseIso(fe.endDate) || parseIso(ov.verifiedEndDate);
  return {
    eventName: text(ov.eventName || s.eventIdentity),
    organizer: text(ov.organizerName),
    start,
    end,
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

/** Named, sourced, commercially relevant contact. `relaxed` accepts unclear (non-procurement) roles for CMI entry points. */
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

/** Hotel-fit assessment — Toronto-wide mega-conventions are not automatic demand for a 157-room hotel. */
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
// STAGE 2 — run deep research batches
// ---------------------------------------------------------------------------
const deepRows = []; // { batch, mode, cand, key, paid, skipped? }
const keyPrefix = { BATCH_A: "deepA", BATCH_B: "deepB", BATCH_C: "cmiC" };

async function runDeep(cand, batch, mode) {
  const key = `${keyPrefix[batch]}_${cand.id}`;
  const cached = loadRaw(key);
  if (cached) return { batch, mode, cand, key, paid: { ...cached.paid, cached: true } };
  if (!LIVE_PAID) {
    return { batch, mode, cand, key, skipped: true, paid: { ok: false, blocked: "NO_CACHE_AND_PAID_NOT_AUTHORIZED" } };
  }
  const ctx = deepContext(cand, mode);
  const pack = buildExaEventsAgentCreatePayload({ mode: "deep_research", contextBlock: ctx, effort: EXA_AGENT_EFFORT.HIGH, segmentId: cand.segmentId || cand.segmentKey || "associations_conferences_events" });
  const paid = await chargeable(
    {
      stage: batch,
      kind: "deep",
      hunter: cand.segmentId || "cmi",
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
  // Only persist when a charge/run actually happened (blocked-before-call attempts are retried on re-run).
  if (paid.ok || paid.error) saveRaw(key, { cand, paid });
  return { batch, mode, cand, key, paid };
}

async function runBatch(batch, cands, mode) {
  const list = cands.slice(0, MAX_DEEP_RUNS[batch]);
  const rows = [];
  console.log(`[${batch}] targets=${list.length} capRemaining=${stageRemainingUsd(batch)} globalRemaining=${remainingBudgetUsd(ledger)}`);
  await pool(list, CONCURRENCY, async (cand) => {
    if (ledger.frozen) {
      rows.push({ batch, mode, cand, key: `${keyPrefix[batch]}_${cand.id}`, paid: { ok: false, blocked: ledger.freezeReason || "LEDGER_FROZEN" } });
      return;
    }
    const row = await runDeep(cand, batch, mode);
    console.log(`[${batch}] "${text(cand.title).slice(0, 55)}" ok=${row.paid.ok} cached=${row.paid.cached === true} blocked=${row.paid.blocked || ""} cost=${row.paid.actualCostUsd ?? 0} rem=${remainingBudgetUsd(ledger)}`);
    rows.push(row);
  });
  deepRows.push(...rows);
  return rows;
}

await runBatch("BATCH_A", BATCH_A_CANDS, "hotel");
await runBatch("BATCH_B", BATCH_B_CANDS, "hotel");

// ----- Batch C: MEDIUM discovery first (only if cap allows after reserving known deep), then HIGH deep -----
function parseDiscoveryHits(structured) {
  const s = structured || {};
  const rows = s.candidates || s.events || s.companies || s.results || [];
  return (Array.isArray(rows) ? rows : []).map((row) => {
    const company = row.companyIdentity && typeof row.companyIdentity === "object" ? row.companyIdentity : null;
    const urls = [
      row.url, row.sourceUrl, ...(row.sourceUrls || []), ...(row.evidenceUrls || []),
      ...((row.supportingSources || []).map((x) => x?.url || x?.sourceUrl)),
      ...((row.supportingEvidence || []).map((x) => x?.url || x?.sourceUrl)),
      ...((row.evidenceClaims || []).map((x) => x?.url || x?.sourceUrl)),
      company?.website,
    ].filter((u) => /^https?:\/\//i.test(String(u || "")));
    return {
      title: text(row.title || row.verifiedEventName || row.eventIdentity || row.eventName || row.verifiedBusinessActivity || row.businessTrigger || row.organizationName || "") || null,
      organizationName: text(row.organizationName || row.organizingEntity || row.corporateOrAssociationOwner || row.companyName || row.associationName || company?.canonicalName || row.name || "") || null,
      url: urls[0] || null,
      evidenceUrls: [...new Set(urls)],
      excerpt: text(row.summary || row.excerpt || row.rationale || row.rankScoreExplain || row.hotelDemandHypothesis || row.hotelRelevanceHypothesis || "").slice(0, 500),
    };
  });
}

const CMI_DISCOVERY_JOBS = [
  {
    id: "harbourfront_mtcc_cluster",
    label: "Harbourfront / MTCC-adjacent competitors",
    hotels: ["The Westin Harbour Castle", "Hotel X Toronto", "Fairmont Royal York", "InterContinental Toronto Centre", "Toronto Marriott City Centre Hotel", "Hilton Toronto"],
  },
  {
    id: "downtown_core_cluster",
    label: "Downtown core convention-hotel competitors",
    hotels: ["Sheraton Centre Toronto", "Hyatt Regency Toronto", "Le Méridien Toronto Pinnacle Hotel", "Delta Hotels Toronto", "Toronto Marriott Downtown Eaton Centre Hotel", "Omni King Edward Hotel"],
  },
];
function cmiDiscoveryContext(job) {
  return [
    "COMPETITOR-COMMITTED FUTURE GROUP/EVENT BUSINESS DISCOVERY (Competitive Market Intelligence) — TORONTO",
    `Target hotel (do NOT list as the venue): ${HOTEL_NAME}, 157 rooms, ~11,000 sq ft meeting space, 249 Queens Quay West, Harbourfront, Toronto, Canada.`,
    `Competing hotels to search: ${job.hotels.join("; ")}.`,
    `Find FUTURE (Dec 2026 – mid 2028; today is ${REF}) association, corporate, government, sports-team, education or incentive events in Toronto that are ALREADY COMMITTED to one of these named hotels: official host/conference hotel, official event page naming the hotel for that edition, published group rate or booking code, or a published room block.`,
    "Exclude generic hotel marketing, past events, 'preferred hotel list' pages without edition-specific commitment, and events outside Toronto. English and French sources (Canadian institutional organizers) both welcome.",
    "For every candidate return the competing hotel, event name, organizer, verified dates, official source URLs and the named organizer contact if public. Do not invent contacts, rates or room blocks.",
  ].join("\n");
}

const discoveryCalls = [];
const discoveryHits = [];
const knownCDeepReserveUsd = BATCH_C_KNOWN.length * HIGH_UNIT;
const discoveryAffordable = Math.max(0, Math.floor((STAGE_CAP_USD.BATCH_C - knownCDeepReserveUsd + 1e-9) / MEDIUM_UNIT));
const discoveryJobsPlanned = CMI_DISCOVERY_JOBS.slice(0, Math.min(CMI_DISCOVERY_JOBS.length, discoveryAffordable));
console.log(`[BATCH_C] known deep=${BATCH_C_KNOWN.length} reserveUsd=${knownCDeepReserveUsd} discoveryJobs=${discoveryJobsPlanned.length}`);
await pool(discoveryJobsPlanned, CONCURRENCY, async (job) => {
  const key = `discC_${job.id}`;
  let paid;
  const cached = loadRaw(key);
  if (cached) {
    paid = { ...cached.paid, cached: true };
  } else if (!LIVE_PAID) {
    paid = { ok: false, blocked: "NO_CACHE_AND_PAID_NOT_AUTHORIZED" };
  } else {
    const ctx = cmiDiscoveryContext(job);
    const pack = buildExaEventsAgentCreatePayload({ mode: "discovery", contextBlock: ctx, effort: EXA_AGENT_EFFORT.MEDIUM, segmentId: job.segmentKey || job.segmentId || "associations_conferences_events" });
    paid = await chargeable(
      { stage: "BATCH_C", kind: "discovery", hunter: "cmi_competitor_booked_business", segmentId: "association_events", effort: EXA_AGENT_EFFORT.MEDIUM, label: job.id },
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
    if (paid.ok || paid.error) saveRaw(key, { paid });
  }
  const hits = paid.result?.hits || parseDiscoveryHits(paid.result?.structured);
  discoveryCalls.push({ key, job: job.id, ok: paid.ok && paid.result?.ok !== false, cached: paid.cached === true, blocked: paid.blocked || paid.error || null, hits: hits.length, costUsd: paid.actualCostUsd ?? 0 });
  console.log(`[BATCH_C:discovery] ${key} ok=${paid.ok} hits=${hits.length} cost=${paid.actualCostUsd ?? 0} rem=${remainingBudgetUsd(ledger)}`);
  for (const h of hits) discoveryHits.push({ ...h, job: job.id });
});

// triage discovery hits -> extra CMI deep candidates (speculative, ranked behind known candidates)
const knownKeys = new Set([
  ...BATCH_A_CANDS.map((c) => normKey(`${c.organizationName}|${c.title}`).slice(0, 40)),
  ...BATCH_B_CANDS.map((c) => normKey(`${c.organizationName}|${c.title}`).slice(0, 40)),
  ...BATCH_C_KNOWN.map((c) => normKey(`${c.organizationName}|${c.title}`).slice(0, 40)),
]);
const discoveryTriageRejected = [];
const discoveryDerived = [];
const seenDisc = new Set();
for (const h of discoveryHits) {
  const title = text(h.title);
  const org = text(h.organizationName);
  if (!title && !org) continue;
  const blob = `${title} ${h.excerpt} ${(h.evidenceUrls || []).join(" ")}`;
  const key = normKey(`${org}|${title}`).slice(0, 40);
  const dates = extractExplicitDates(blob).filter((d) => d >= IMMINENT_CUTOFF);
  const y = maxYearIn(blob);
  if (knownKeys.has(key)) {
    discoveryTriageRejected.push({ title, org, reason: "ALREADY_IN_KNOWN_QUEUE" });
    continue;
  }
  if (!COMPETITOR_REGEX.test(blob)) {
    discoveryTriageRejected.push({ title, org, reason: "NO_COMPETING_HOTEL_NAMED" });
    continue;
  }
  if (!(dates.length || (y && y >= 2027))) {
    discoveryTriageRejected.push({ title, org, reason: "NO_FUTURE_SIGNAL" });
    continue;
  }
  if (HOTEL_LIKE_ORG_REGEX.test(org) && COMPETITOR_REGEX.test(org)) {
    discoveryTriageRejected.push({ title, org, reason: "ORG_IS_COMPETING_HOTEL_BRAND" });
    continue;
  }
  const id = hashId("rbd", title || org, org || title);
  if (seenDisc.has(id)) continue;
  seenDisc.add(id);
  discoveryDerived.push({
    id,
    batch: "BATCH_C",
    title: title || org,
    organizationName: org || title,
    segmentId: "association_events",
    ledgerStatus: "PAID_DISCOVERY_C",
    excerpt: h.excerpt,
    evidenceUrls: h.evidenceUrls || [],
    ledgerVerifiedDates: dates[0] || "",
    ledgerStart: dates[0] || null,
    ledgerVenue: "",
    ledgerSourcingStatus: "",
    ledgerAttendance: null,
    priorLabel: null,
    selectionWhy: `Paid MEDIUM discovery (${h.job})`,
    selectionScore: (dates.length ? 3 : 1) + (territoryOf(blob) !== "OUTSIDE_REALISTIC_TERRITORY" ? 2 : -3),
  });
}
discoveryDerived.sort((a, b) => b.selectionScore - a.selectionScore);
await runBatch("BATCH_C", BATCH_C_KNOWN, "cmi");
const slotsLeft = Math.max(0, MAX_DEEP_RUNS.BATCH_C - BATCH_C_KNOWN.length);
const extraC = discoveryDerived.filter((c) => c.selectionScore > 0).slice(0, slotsLeft);
if (extraC.length) await runBatch("BATCH_C", extraC, "cmi");
const BATCH_C_ALL = [...BATCH_C_KNOWN, ...extraC];

// ---------------------------------------------------------------------------
// STAGE 3 — qualification (Hotel Market vs CMI vs Watch vs Reject)
// ---------------------------------------------------------------------------
const decisions = []; // per deep row
const hotelSpecs = [];
const cmiSpecs = [];

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

function qualifyRow(row) {
  const { cand, paid, batch } = row;
  const base = { key: row.key, batch, candId: cand.id, title: cand.title, org: cand.organizationName, mode: row.mode, cached: paid.cached === true, costUsd: paid.actualCostUsd ?? 0 };
  if (row.skipped) return { ...base, bucket: "NOT_RUN", reason: paid.blocked };
  if (!paid.ok || paid.result?.ok === false) {
    return { ...base, bucket: "REJECTED", reason: paid.blocked || paid.error || paid.result?.status || "DEEP_FAILED" };
  }
  const d = extractDeep(paid.result?.structured);
  const title = d.eventName || cand.title;
  const org = d.organizer || cand.organizationName;
  const summary = {
    eventName: title, organizer: org, start: d.start, end: d.end, venue: d.venue, city: d.city,
    hotelSourcingStatus: d.hotelSourcingStatus, contactsReturned: d.contacts.length,
  };
  // date
  let dateEv = { kind: "NONE" };
  if (d.start && d.start >= REF) dateEv = { kind: "EXACT_FUTURE", startDate: d.start, endDate: d.end && d.end >= d.start ? d.end : null };
  else if (d.start && d.start < REF && (!d.end || d.end < REF)) dateEv = { kind: "EXPIRED", startDate: d.start };
  else {
    const y = maxYearIn(`${title} ${d.recurrence} ${d.demandCycle}`);
    if (y && y >= 2027) dateEv = { kind: "YEAR_ONLY", year: y };
  }
  const locBlob = [d.venue, d.city, d.region, d.country, title, cand.ledgerVenue].join(" ");
  const territory = territoryOf(locBlob, d.city);
  const locationReturned = Boolean(d.venue || d.city || d.region || d.country);
  const competitorSources = `${d.venue} ${d.venueCommitment} ${d.accommodation} ${row.mode === "cmi" ? JSON.stringify(d.findings) : ""}`;
  const competitors = findCompetitors(competitorSources).filter((n) => !/radisson/i.test(n));
  const accBlob = `${d.accommodation} ${d.venueCommitment} ${d.hotelSourcingStatus} ${JSON.stringify(d.findings)}`.toLowerCase();
  const bookingLanguage = /host hotel|conference hotel|official hotel|headquarters hotel|room block|published (?:room )?rates|group rate|conference rate|special rate|booking code|group code|reservation link|reservations? (?:open|are open)|hotel block|hotel partners?/.test(accBlob);
  const lodgingCommitted = /(?<!un)confirmed|announced|\bofficial\b|committed|competitor|room_block|\bhost\b|blocks?/.test(`${d.hotelSourcingStatus} ${d.venueCommitment}`.toLowerCase());
  const urls = [...(cand.evidenceUrls || []), ...d.sources.map((s) => s.url)].filter((u) => /^https?:/i.test(String(u || "")));
  const fit = assessHotelFit(d, cand);

  const detail = { ...summary, dateEv, territory, competitors, bookingLanguage, lodgingCommitted, fit };

  if (dateEv.kind === "EXPIRED") return { ...base, bucket: "REJECTED", reason: "EXPIRED_ONE_TIME_OR_PAST_EDITION", detail };
  if (dateEv.kind === "EXACT_FUTURE" && dateEv.startDate < IMMINENT_CUTOFF) {
    return { ...base, bucket: "REJECTED", reason: "IMMINENT_LT_21_DAYS_NOT_ACTIONABLE", detail };
  }
  if (territory === "OUTSIDE_REALISTIC_TERRITORY") {
    return locationReturned
      ? { ...base, bucket: "REJECTED", reason: "OUTSIDE_TERRITORY", detail }
      : { ...base, bucket: "WATCH", reason: "LOCATION_NOT_VERIFIED_BY_DEEP_RESEARCH", detail };
  }
  if (!HOTEL_MARKET_TERRITORIES.has(territory)) {
    return { ...base, bucket: "WATCH", reason: `${territory}_NOT_ASSUMED_RADISSON_DEMAND`, detail };
  }
  const blob = JSON.stringify(d.findings).toLowerCase();
  if (/not a (?:conference|group event|meeting|event)|airline connectivity|route launch|new (?:direct )?route/.test(`${title} ${blob}`)) {
    return { ...base, bucket: "REJECTED", reason: "NON_GROUP_DEMAND_SIGNAL", detail };
  }

  // ---- CMI route: edition-specific competitor commitment ----
  if (competitors.length && bookingLanguage && lodgingCommitted && urls.length) {
    if (dateEv.kind !== "EXACT_FUTURE") return { ...base, bucket: "WATCH", reason: "CMI_COMMITMENT_BUT_NO_EXACT_FUTURE_DATE", detail };
    const pc = pickContact(d.contacts, org, { relaxed: true });
    if (!pc) return { ...base, bucket: "WATCH", reason: "CMI_COMMITMENT_VERIFIED_NO_NAMED_SOURCED_CONTACT", detail };
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
        `${text(d.cib.potentialBusiness).slice(0, 300) || `Develop the organizer relationship for a future edition or separately evidenced overflow.`} ${HOTEL_NAME} cannot realistically win the committed ${yearStr} principal housing; ${fit.rationale}`.trim().slice(0, 700),
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
      provenance: { origin: "PAID_EXA_CMI_STAGE_4A", batch, deepRunId: paid.result?.runId || null, costUsd: paid.actualCostUsd ?? null, referenceDate: REF },
    };
    cmiSpecs.push({ ...spec, _row: row.key });
    return { ...base, bucket: "CMI_CANDIDATE", reason: "COMPETITOR_COMMITMENT_EDITION_SPECIFIC_WITH_NAMED_CONTACT", detail: { ...detail, contact: pc.contact } };
  }
  if (competitors.length && /host hotel|\bofficial\b|committed|(?<!un)confirmed/.test(accBlob) && row.mode === "cmi") {
    return { ...base, bucket: "WATCH", reason: "CMI_COMMITMENT_NOT_EDITION_SPECIFIC_OR_UNCONFIRMED", detail };
  }
  // Venue itself a competitor hotel but not bookable evidence -> watch, never hotel-market
  if (d.venue && COMPETITOR_REGEX.test(d.venue)) {
    return { ...base, bucket: "WATCH", reason: "VENUE_IS_COMPETITOR_HOTEL_COMMITMENT_UNPROVEN", detail };
  }

  // ---- Hotel Market route ----
  if (dateEv.kind === "NONE") return { ...base, bucket: "WATCH", reason: "NO_VERIFIED_FUTURE_DATE", detail };
  if (dateEv.kind === "YEAR_ONLY") return { ...base, bucket: "WATCH", reason: "YEAR_ONLY_NEEDS_EXACT_DATE", detail };
  if (fit.fit.startsWith("WATCH_")) return { ...base, bucket: "WATCH", reason: fit.fit, detail };
  const pc = pickContact(d.contacts, org);
  if (!pc) return { ...base, bucket: "WATCH", reason: "NO_NAMED_COMMERCIAL_CONTACT_WITH_SOURCE", detail };
  let demandClass = "SPECULATIVE_DEMAND";
  if (/rfp|open (?:hotel|lodging) sourcing|accepting (?:hotel|proposals)|hotel proposals/.test(`${d.hotelSourcingStatus} ${blob}`) && !/no rfp|not an open/.test(blob)) demandClass = "CONFIRMED_DEMAND";
  else if (/hotel|lodging|alojamiento|overnight|room|attendee|delegat|participants|teams|accommodation/.test(blob)) demandClass = "RELIABLE_INFERRED_DEMAND";
  if (demandClass === "SPECULATIVE_DEMAND") return { ...base, bucket: "WATCH", reason: "SPECULATIVE_ONLY", detail };

  const overflow = fit.fit === "OVERFLOW_ANCILLARY";
  const what = text(d.cib.whatIsHappening) || `${title} (${dateLabel(dateEv.startDate, dateEv.endDate)}) — organized by ${org}.`;
  const potential =
    text(d.cib.potentialBusiness) ||
    (overflow
      ? `Possible delegate/speaker overflow lodging and ancillary meeting or reception demand around ${title}; volume unverified.`
      : `Possible guestroom and group-meeting demand tied to ${title}; volume unverified.`);
  const spec = {
    id: cand.id,
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
      softClampCustomerProse(
        text(d.findings.futureEdition),
        900,
      ) || "Edition dates are published; qualify who selects lodging before sourcing closes.",
    nextStep:
      softClampCustomerProse(text(d.cib.recommendedNextStep), 1200) ||
      `Contact ${pc.contact.name} at ${org} to confirm who selects lodging, whether additional hotels can participate, and expected room nights; position ${HOTEL_NAME} as a Harbourfront ${overflow ? "overflow / ancillary meeting" : "group"} option.`,
    sources: mkSources(cand, d, pc.contact),
    demandClass,
    sourcingLabel: /open/.test(d.hotelSourcingStatus) ? "Hotel Sourcing Open" : "Hotel Sourcing Unconfirmed",
    uncertainties: [
      ...d.uncertainties.slice(0, 3).map(text),
      "Room volume unverified.",
      "Restaurant/lounge availability unverified — do not assume catering capability.",
    ].filter(Boolean),
    fit,
    provenance: { origin: "PAID_EXA_DEEP_RESEARCH", batch, priorLedgerLabel: cand.priorLabel || null, deepRunId: paid.result?.runId || null, costUsd: paid.actualCostUsd ?? null, referenceDate: REF },
    dateEvidence: dateEv,
  };
  hotelSpecs.push({ ...spec, _row: row.key });
  return { ...base, bucket: "HOTEL_MARKET_CANDIDATE", reason: `FUTURE_DATE_TERRITORY_CONTACT_FIT_${fit.fit}`, detail: { ...detail, contact: pc.contact } };
}

for (const row of deepRows) decisions.push(qualifyRow(row));
const decisionByKey = new Map(decisions.map((x) => [x.key, x]));

// ---- event-level dedupe (CMI preferred over hotel-market for the same event) ----
const titleTokens = (t) => new Set(normKey(t).split(" ").filter((w) => w.length > 3));
function sameEvent(a, b) {
  const sa = a.eventStartDate || a.startDate;
  const sb = b.eventStartDate || b.startDate;
  if (!sa || !sb || sa !== sb) return false;
  if (normKey(a.org) === normKey(b.org)) return true;
  const ta = titleTokens(a.title);
  const tb = titleTokens(b.title);
  const inter = [...ta].filter((x) => tb.has(x)).length;
  return inter >= 2 && inter / Math.max(1, Math.min(ta.size, tb.size)) >= 0.5;
}
const dedupeDropped = [];
const cmiKept = [];
// prefer Batch C (CMI-focused) rows first, then more roomEvidence
const cmiOrder = [...cmiSpecs].sort((a, b) => (b.provenance.batch === "BATCH_C") - (a.provenance.batch === "BATCH_C") || (b.commitmentScope === "VENUE_AND_GUESTROOMS") - (a.commitmentScope === "VENUE_AND_GUESTROOMS"));
for (const s of cmiOrder) {
  const dup = cmiKept.find((k) => sameEvent(k, s));
  if (dup) dedupeDropped.push({ id: s.id, title: s.title, keptId: dup.id, kind: "CMI_DUP" });
  else cmiKept.push(s);
}
const hotelKept = [];
for (const s of hotelSpecs) {
  const dupCmi = cmiKept.find((k) => sameEvent(k, s));
  if (dupCmi) {
    dedupeDropped.push({ id: s.id, title: s.title, keptId: dupCmi.id, kind: "HOTEL_DUP_OF_CMI" });
    continue;
  }
  const dup = hotelKept.find((k) => sameEvent(k, s));
  if (dup) dedupeDropped.push({ id: s.id, title: s.title, keptId: dup.id, kind: "HOTEL_DUP" });
  else hotelKept.push(s);
}

// ---------------------------------------------------------------------------
// opportunity builders (same contract fields as Wyndham)
// ---------------------------------------------------------------------------
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
  const pill = start ? dateLabel(start, end) : "Timing TBD";
  const sources = spec.sources || [];
  let draft = {
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
    cardWhyNowLine: spec.whyNow.slice(0, 160),
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
    datePillDisplayLabel: pill,
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
    hotelFitAssessment: spec.fit ? { fit: spec.fit.fit, rationale: spec.fit.rationale, attendance: spec.fit.attendance, ruleVersion: "radisson-toronto-fit-v1", assessedAt: REF } : undefined,
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
        {
          id: "supporting_evidence",
          title: "Supporting evidence",
          body: sources.slice(0, 10).map((s) => `${s.statement || ""} ${s.url || ""}`.trim()).join("\n"),
        },
        {
          id: "research_uncertainties",
          title: "Research uncertainties",
          body: (spec.uncertainties || []).join(" ") || "Hotel sourcing unconfirmed. Room volume unverified.",
        },
      ],
    },
  };
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

const cmiOppsRaw = cmiKept.map(buildCmiOpportunity);
const cmiGateReviews = cmiOppsRaw.map((o) => ({ id: o.id, title: o.title, competitorHotel: o.competitorHotel, gates: evaluateCmiQualificationGates(o) }));
const cmiQualifiedIds = new Set(cmiGateReviews.filter((r) => r.gates.qualifiesAsCustomerCmi).map((r) => r.id));
const cmiFailedGateRows = cmiGateReviews.filter((r) => !r.gates.qualifiesAsCustomerCmi);
const hotelOppsRaw = hotelKept.map(buildHotelMarketOpportunity);
const cmiOppsQualified = cmiOppsRaw.filter((o) => cmiQualifiedIds.has(o.id));

const hintsById = {};
for (const s of hotelKept) {
  if (s.startDate) hintsById[s.id] = { actualEventStart: s.startDate, actualEventEnd: s.endDate || null, datePillDisplayLabel: dateLabel(s.startDate, s.endDate) };
}
for (const o of cmiOppsQualified) {
  hintsById[o.id] = {
    competitorCommitted: true,
    cmiVerified: true,
    actualEventStart: o.eventStartDate,
    actualEventEnd: o.eventEndDate,
    datePillDisplayLabel: o.datePillDisplayLabel,
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
let allOpps = [...hotelOppsRaw, ...cmiOppsQualified].map((o) => sanitizeCustomerCopy(o));
allOpps = annotateInventoryPopulations(allOpps, { hotelId: HOTEL_ID }).map((o) => {
  const isCmi = cmiQualifiedIds.has(o.id);
  return annotateCanonicalCmiContract({
    ...o,
    competitiveMarketScopeEligible: isCmi,
    hotelMarketScopeEligible: isCmi ? false : o.hotelMarketScopeEligible,
  });
});
const gate = applyCommercialActionabilityGate(allOpps, { referenceDate: REF, hintsById });
const gatedRows = gate.rows.map((o) => attachSalesOpportunityFilterFields(o));
const customerVisible = gatedRows.filter((o) => o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true);
const hotelMarket = customerVisible.filter((o) => o.hotelMarketScopeEligible === true);
const competitive = customerVisible.filter((o) => o.competitiveMarketScopeEligible === true);
const gatedOut = gatedRows.filter((o) => !(o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true));
const actionabilityAssert = assertCustomerInventoryPassesActionabilityGate(customerVisible, { referenceDate: REF, hintsById });

// ---------------------------------------------------------------------------
// reports 02–11
// ---------------------------------------------------------------------------
const ops = ledger.operations;
const spendByStage = {};
for (const o of ops) spendByStage[o.stage] = Number(((spendByStage[o.stage] || 0) + Number(o.actualCostUsd || 0)).toFixed(4));

writeJson("02-PAID_BUDGET_LEDGER.json", {
  contract: "GDI_RADISSON_PAID_15USD_LEDGER_V1",
  authorizationId: AUTHORIZATION_ID,
  hardCeilingUsd: HARD_CEILING_USD,
  stageCapsUsd: STAGE_CAP_USD,
  priorResumedSpendUsd: resumedSpendUsd,
  spentUsd: ledger.spentUsd,
  reservedUsd: ledger.reservedUsd,
  remainingUsd: remainingBudgetUsd(ledger),
  withinCeiling: ledger.spentUsd <= HARD_CEILING_USD,
  spendByStage,
  withinStageCaps: Object.entries(STAGE_CAP_USD).every(([k, cap]) => (spendByStage[k] || 0) <= cap + 1e-9),
  operationCount: ops.length,
  operations: ops.map((o) => ({ at: o.at, stage: o.stage, kind: o.kind || null, hunter: o.hunter, label: o.label || null, opportunityId: o.opportunityId, quotedMaxUsd: o.quotedMaxUsd, actualCostUsd: o.actualCostUsd, providerRunId: o.providerRunId })),
  blockedOperations: ledger.blockedOperations,
  frozen: ledger.frozen,
  freezeReason: ledger.freezeReason,
  surfeUsed: false,
  parallelUsed: false,
  note: "Authoritative on-disk ledger: paid-budget-ledger.json (same directory).",
});

const rowSummary = (x) => ({
  key: x.key, candId: x.candId, title: x.title, org: x.org, cached: x.cached, costUsd: x.costUsd,
  bucket: x.bucket, reason: x.reason,
  eventName: x.detail?.eventName, start: x.detail?.start, venue: x.detail?.venue, territory: x.detail?.territory,
  hotelSourcingStatus: x.detail?.hotelSourcingStatus, competitors: x.detail?.competitors, fit: x.detail?.fit?.fit,
  attendance: x.detail?.fit?.attendance ?? null, contact: x.detail?.contact || null,
});
const batchDoc = (batch, cands, extra = {}) => {
  const ds = decisions.filter((x) => x.batch === batch);
  return {
    batch,
    status: LIVE_PAID ? "EXECUTED" : "ASSEMBLED_FROM_CACHE",
    capUsd: STAGE_CAP_USD[batch],
    spentUsd: spendByStage[batch] || 0,
    unitCostUsd: HIGH_UNIT,
    plannedRuns: cands.length,
    runsCompleted: ds.filter((x) => x.bucket !== "NOT_RUN").length,
    outcomeCounts: countBy(ds, (x) => x.bucket),
    targets: cands.map(planRow),
    runs: ds.map(rowSummary),
    ...extra,
  };
};
writeJson("03-BATCH_A_RESULTS.json", batchDoc("BATCH_A", BATCH_A_CANDS));
writeJson("04-BATCH_B_RESULTS.json", batchDoc("BATCH_B", BATCH_B_CANDS, { note: "Prior Sales Ready labels were NOT auto-promoted; every id re-qualified from fresh deep research." }));
writeJson("05-BATCH_C_CMI_RESULTS.json", batchDoc("BATCH_C", BATCH_C_ALL, {
  discovery: { jobsPlanned: discoveryJobsPlanned.map((j) => j.id), calls: discoveryCalls, hitCount: discoveryHits.length, derivedCandidates: discoveryDerived.map(planRow), deepRunSlotsUsedForDerived: extraC.length, triageRejected: discoveryTriageRejected },
  competitiveSet: COMPETITOR_CATALOG.map(([n]) => n),
}));

const failureHistogram = countBy(decisions.filter((x) => x.bucket !== "HOTEL_MARKET_CANDIDATE" && x.bucket !== "CMI_CANDIDATE"), (x) => x.reason);
writeJson("06-COMMERCIAL_ELIGIBILITY.json", {
  contract: "GDI_RADISSON_COMMERCIAL_ELIGIBILITY_V1",
  referenceDate: REF,
  imminentCutoff: IMMINENT_CUTOFF,
  rules: [
    "Exact future event date (≥ 21 days out) required — year-only and expired editions are Watch/Reject.",
    "Territory: Toronto Harbourfront / downtown / MTCC / Exhibition Place (core) or Toronto proper (nearby); airport corridor / GTA suburbs are stretch → Watch.",
    "Named, sourced, commercially relevant contact required; procurement-only roles excluded.",
    "Hotel-fit assessment per event; ≥5,000 attendee MTCC-scale events without open overflow sourcing are Watch, not automatic demand.",
    "Edition-specific competitor commitment (host hotel / room block / booking code) + named contact → CMI, never Hotel Market.",
    "Prior thin-discovery Sales Ready labels are not evidence.",
  ],
  decisionCounts: countBy(decisions, (x) => x.bucket),
  failureHistogram,
  eventDedupeDropped: dedupeDropped,
  decisions: decisions.map(rowSummary),
});

const reconRows = BATCH_B_CANDS.map((c) => {
  const dec = decisions.find((x) => x.candId === c.id && x.batch === "BATCH_B");
  const finalOpp = customerVisible.find((o) => o.id === c.id);
  return {
    opportunityId: c.opportunityId,
    title: c.title,
    organizationName: c.organizationName,
    segment: c.segmentId,
    linkedLedgerRows: c.linkedLedgerRowCount,
    linkedProgrammes: c.linkedLedgerProgrammes,
    priorLedgerStatus: "AWAITING_FOUNDER_PROMOTION",
    priorAcceptanceLabel: c.priorLabel,
    autoPromoted: false,
    batchBDeepOutcome: dec?.bucket || "NOT_RUN",
    batchBReason: dec?.reason || null,
    batchBCostUsd: dec?.costUsd ?? 0,
    stagedNow: Boolean(finalOpp),
    stagedAs: finalOpp ? (finalOpp.competitiveMarketScopeEligible ? "CMI" : "HOTEL_MARKET_ACCOUNT_DEVELOPMENT") : null,
    detail: dec?.detail ? { start: dec.detail.start, venue: dec.detail.venue, territory: dec.detail.territory, fit: dec.detail.fit?.fit, contact: dec.detail.contact || null } : null,
  };
});
writeJson("07-HISTORICAL_SALES_READY_RECONCILIATION.json", {
  contract: "GDI_RADISSON_PRIOR_SR_RECONCILIATION_V2",
  priorUniqueIds: reconRows.length,
  priorLabelsAutoPromoted: 0,
  outcomeCounts: countBy(reconRows, (r) => r.batchBDeepOutcome),
  stagedNowFromPrior: reconRows.filter((r) => r.stagedNow).length,
  note: "A prior SALES_READY label never carries forward; only fresh evidence passing the same gates as every other candidate is staged (as Account Development).",
  rows: reconRows,
});

writeJson("08-ACTIONABILITY_AUDIT.json", {
  referenceDate: REF,
  gate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  counts: gate.counts,
  assertCustomerInventoryPassesActionabilityGate: actionabilityAssert,
  rows: gatedRows.map((o) => ({ id: o.id, title: o.title, state: o.commercialActionabilityStateV1, temporal: o.commercialTemporalDisposition, hotelMarket: o.hotelMarketScopeEligible, competitive: o.competitiveMarketScopeEligible, datePill: o.datePillDisplayLabel })),
  gatedOut: gatedOut.map((o) => ({ id: o.id, title: o.title, state: o.commercialActionabilityStateV1 })),
});

const oppSummary = (o) => ({
  id: o.id, title: o.displayTitle, org: o.organizationName, dates: o.datePillDisplayLabel, start: o.eventStartDate,
  territory: o.territoryClassification, fit: o.hotelFitAssessment?.fit || null,
  contact: o.primaryContact ? { name: o.primaryContact.name, title: o.primaryContact.title, sourceUrl: o.primaryContact.sourceUrl } : null,
  sourceCount: (o.sources || []).length, salesAction: o.salesActionPillLabel, stage: o.commercialStagePillLabel,
});
writeJson("09-HOTEL_MARKET_INVENTORY.json", {
  hotelMarketCount: hotelMarket.length,
  salesReadyCount: hotelMarket.filter((o) => /sales ready/i.test(o.commercialStagePillLabel || "")).length,
  opportunities: hotelMarket.map(oppSummary),
  watchList: decisions.filter((x) => x.bucket === "WATCH").map(rowSummary),
  rejected: decisions.filter((x) => x.bucket === "REJECTED").map(rowSummary),
});

const cmiAttempted = discoveryCalls.some((c) => c.ok) || decisions.some((x) => x.batch === "BATCH_C" && x.bucket !== "NOT_RUN");
const cmiDeepRan = decisions.some((x) => x.batch === "BATCH_C" && !["NOT_RUN", "REJECTED"].includes(x.bucket));
const cmiRejectedCount = decisions.filter((x) => x.batch === "BATCH_C" && ["WATCH", "REJECTED"].includes(x.bucket)).length + cmiFailedGateRows.length;
const completedChecklist = [
  "ESTABLISH_RELEVANT_COMPETITIVE_HOTEL_SET",
  cmiAttempted && "SEARCH_FUTURE_EVENTS_BOOKED_AT_COMPETING_HOTELS",
  cmiAttempted && "INVESTIGATE_COMPETITOR_PROPERTY_MEETING_CALENDARS",
  (discoveryHits.length > 0 || cmiDeepRan) && "IDENTIFY_ORGANIZATIONS_HOLDING_EVENTS_THERE",
  cmiDeepRan && "VERIFY_ACTUAL_BOOKING_RELATIONSHIPS",
  cmiDeepRan && "IDENTIFY_RECURRING_EVENT_CYCLES",
  cmiDeepRan && "LOCATE_RELEVANT_DECISION_MAKERS_OR_ENTRY_POINTS",
  cmiDeepRan && "DETERMINE_REALISTIC_NEXT_CYCLE_PURSUIT",
  "DEDUPLICATE_AGAINST_EXISTING_GDI_OPPORTUNITIES",
  "PERSIST_CONFIRMED_AND_REJECTED_EVIDENCE",
].filter(Boolean);
const cmiCoverage = evaluateCmiDiscoveryCoverage({
  researchAttempted: cmiAttempted,
  evidenceFindingsDocumented: true,
  confirmedCompetitorBookingCount: cmiQualifiedIds.size,
  rejectedEvidenceCount: cmiRejectedCount,
  completedChecklistItems: completedChecklist,
});
writeJson("10-CMI_INVENTORY.json", {
  contract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  competitiveSet: COMPETITOR_CATALOG.map(([n]) => n),
  guardrails: [
    "Competitive-set membership is not an opportunity.",
    "Only edition-specific committed host/venue/block evidence plus a named contact qualifies.",
    "Hotel-program listings and 'preferred hotel' pages never promote to CMI.",
  ],
  qualifiedCmiCount: cmiQualifiedIds.size,
  qualifiedCmi: cmiOppsQualified.map((o) => ({ ...oppSummary(o), competitorHotel: o.competitorHotel, commitmentScope: o.cmiEvidenceRegisterV1?.commitmentScope })),
  gateReviews: cmiGateReviews,
  failedCanonicalGates: cmiFailedGateRows.map((r) => ({ id: r.id, title: r.title, failedGates: r.gates.failedGates })),
  cmiCandidatesBeforeDedupe: cmiSpecs.length,
  watchedOrRejectedCmiEvidence: decisions.filter((x) => x.batch === "BATCH_C" && ["WATCH", "REJECTED"].includes(x.bucket)).map(rowSummary),
  checklistRequired: CMI_DISCOVERY_COVERAGE_CHECKLIST,
  completedChecklist,
  coverage: cmiCoverage,
});

const needDateRows = customerVisible.map((o) => {
  const start = o.eventStartDate;
  return {
    id: o.id,
    title: o.displayTitle,
    scope: o.competitiveMarketScopeEligible ? "CMI" : "HOTEL_MARKET",
    eventStart: start,
    eventEnd: o.eventEndDate || null,
    daysUntilStart: start ? daysBetween(REF, start) : null,
    revenueStreamClass: o.revenueStreamClass,
    heuristicQualifyByDate: start ? addDays(start, -150) : null,
    heuristicNote: "Internal timing heuristic (start − 150 days), not a published organizer deadline.",
    publishedSourcingDeadline: null,
  };
});
writeJson("11-NEED_DATES_AND_SVG.json", {
  referenceDate: REF,
  revenueStreamCounts: countBy(needDateRows, (r) => r.revenueStreamClass),
  rows: needDateRows,
  watchNeedDateHints: decisions.filter((x) => x.bucket === "WATCH" && x.detail?.dateEv?.startDate).map((x) => ({ key: x.key, title: x.title, reason: x.reason, start: x.detail.dateEv.startDate })),
});

// ---------------------------------------------------------------------------
// STAGE 4 — package, opportunities.json, Airtable dry-run
// ---------------------------------------------------------------------------
const store = createGdiPublicationPackageStore({ root: PROTECTED_STORE_ROOT });
let pkg = null;
let integrity = null;
let writtenPath = null;
let qualityGate = null;
let packageBlocker = null;
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
  } else {
    pkg = candidatePkg;
    const written = store.writePackage(pkg);
    writtenPath = written?.path || null;
    store.writePointer(HOTEL_ID, "STAGED", {
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

fs.writeFileSync(
  priorOppsPath,
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
  totalStaged: customerVisible.length,
  integrity,
  qualityGate: { ran: Boolean(qualityGate), ok: qualityGate?.ok ?? null, blocking: qualityGate?.blocking || null, summary: qualityGate?.summary || null },
  protectedPublicationsBefore: protectedBefore,
  protectedPublicationsAfter: protectedAfter,
  protectedPublicationsUnchanged: protectedUnchanged,
  previewUrl,
  opportunitiesJson: path.relative(ROOT, priorOppsPath),
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
writeJson("14-LOCAL_BROWSER_VERIFICATION.json", {
  contract: "GDI_RADISSON_LOCAL_BROWSER_VERIFICATION_V1",
  status: "PENDING_BROWSER_RUN",
  howToRun: `node scripts/${SCRIPT_ID}.mjs --browser-verify [--base-url http://127.0.0.1:8080]`,
  previewUrl,
  expectedFingerprint: pkg?.contentFingerprintSha256 || null,
  expectedCount: customerVisible.length,
  note: "If the preview server is not running the verifier records FAIL_SERVER_DOWN honestly.",
});

const templateEvidence = {
  template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  lockId: GDI_FULL_E2E_HOTEL_TEMPLATE_LOCK_V1,
  hotelConfigPresent: Boolean(HOTEL_CFG?.capabilityProfile),
  cmiCanonicalContractApplied: true,
  cmiDiscoveryStageExecutedOrCached: cmiAttempted,
  cmiCoverageGateOk: cmiCoverage.coverageComplete === true || cmiCoverage.zeroConfirmedCmiAcceptable === true,
  commercialActionabilityGateApplied: true,
  commercialActionabilityAssertOk: actionabilityAssert?.ok ?? null,
  publicationQualityGateRan: Boolean(qualityGate),
  publicationStagedNotPublished: Boolean(pkg),
  noSurfe: true,
  airtableDryRunOnly: true,
  protectedPublicationsUnchanged: protectedUnchanged,
  paidWithinCeiling: ledger.spentUsd <= HARD_CEILING_USD,
  note: "Run-level template compliance record. The full main-merge template certificate (evaluateFullE2eHotelTemplateCertificate) is issued separately after merge.",
};
writeJson("15-TEMPLATE_CERTIFICATION.json", templateEvidence);
writeJson("16-GITHUB_MERGE_STATUS.json", {
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
  protectedPublicationsUnchanged: protectedUnchanged,
  reminder: "Task is not technically complete until the PR is merged into main (see gdi-github-branch-merge-discipline rule).",
});

// ---------------------------------------------------------------------------
// FOUNDER_REPORT.md
// ---------------------------------------------------------------------------
const mdRows = (arr, fn) => (arr.length ? arr.map(fn).join("\n") : "_none_");
const founder = `# Radisson Blu Toronto Downtown — Full E2E ($15 ceiling)

**Authorization:** \`${AUTHORIZATION_ID}\`
**Canonical hotel:** \`${HOTEL_ID}\` · **Reference date:** ${REF}
**Hard ceiling:** **$${HARD_CEILING_USD}** · **Spent:** **$${ledger.spentUsd}** · **Remaining:** **$${remainingBudgetUsd(ledger)}**
**Staged publication:** \`${pkg?.publicationVersion || "NOT STAGED"}\`${packageBlocker ? ` (blocker: ${packageBlocker})` : ""}
**Fingerprint:** \`${pkg?.contentFingerprintSha256 || "n/a"}\`
**Lifecycle:** STAGED_FOR_FOUNDER_REVIEW (never CUSTOMER_PUBLISHED)

## Result

| Metric | Count |
|---|---:|
| Hotel Market (Account Development) staged | **${hotelMarket.length}** |
| Competitive Market Intelligence staged | **${competitive.length}** |
| Total staged | ${customerVisible.length} |
| Sales Ready | 0 (every row is Account Development — contacts are entry points, lodging authority unverified) |
| Prior Sales Ready labels auto-promoted | 0 |
| Deep runs executed | ${decisions.filter((x) => x.bucket !== "NOT_RUN").length} |
| Watch / Reject | ${decisions.filter((x) => x.bucket === "WATCH").length} / ${decisions.filter((x) => x.bucket === "REJECTED").length} |

## Spend by batch (cap → spent)

${mdRows(Object.entries(STAGE_CAP_USD), ([k, cap]) => `- ${k}: cap $${cap} → spent $${spendByStage[k] || 0}`)}
- Surfe: not invoked · Parallel: not invoked · Airtable writes: 0

## Hotel Market opportunities
${mdRows(hotelMarket, (o, i) => `${i + 1}. **${o.displayTitle}** — ${o.organizationName} · ${o.datePillDisplayLabel} · ${o.primaryContact?.name || "n/a"} (${o.primaryContact?.title || "role n/a"}) · fit: ${o.hotelFitAssessment?.fit || "n/a"}`)}

## CMI (Stage 4a)
- Qualified CMI: **${cmiQualifiedIds.size}** · coverage complete: ${cmiCoverage.coverageComplete} · zero-CMI acceptable: ${cmiCoverage.zeroConfirmedCmiAcceptable}
${mdRows(competitive, (o, i) => `${i + 1}. **${o.displayTitle}** @ ${o.competitorHotel} — ${o.datePillDisplayLabel} · ${o.primaryContact?.name || "n/a"}`)}

## Prior Sales Ready reconciliation (12 unique ids)
${mdRows(reconRows, (r) => `- ${r.title}: ${r.batchBDeepOutcome}${r.batchBReason ? ` (${r.batchBReason})` : ""}${r.stagedNow ? ` → staged as ${r.stagedAs}` : ""}`)}

## Watch items (not staged)
${mdRows(decisions.filter((x) => x.bucket === "WATCH"), (x) => `- ${x.title} [${x.batch}] — ${x.reason}`)}

## Gates
- Commercial actionability: ${gate.counts.activeHotelMarket} hotel / ${gate.counts.activeCompetitive} competitive / ${gate.counts.researchWatch} research-watch; assert ok=${actionabilityAssert.ok}
- Publication quality gate: ${qualityGate ? JSON.stringify(qualityGate.summary || qualityGate.ok) : "not run"}
- Bethesda / Sheraton / Wyndham publications unchanged: ${protectedUnchanged}
- Airtable: dry-run only (${JSON.stringify(airtablePlan)})

## Preview
\`${previewUrl}\` — verify with \`node scripts/${SCRIPT_ID}.mjs --browser-verify\`

## Notes
- Hotel fit: 157 rooms / ~11,000 sq ft. MTCC-scale shows are assessed as overflow/ancillary at best; ≥5,000-attendee events without open overflow sourcing stay on Watch.
- Charged-but-failed Exa runs are cached in raw-runs/ and not re-paid; delete the specific raw file to force a retry.
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);

console.log(
  JSON.stringify(
    {
      spentUsd: ledger.spentUsd,
      remainingUsd: remainingBudgetUsd(ledger),
      spendByStage,
      hotelMarketCount: hotelMarket.length,
      cmiCount: competitive.length,
      totalStaged: customerVisible.length,
      decisionCounts: countBy(decisions, (x) => x.bucket),
      fingerprint: pkg?.contentFingerprintSha256 || null,
      publicationVersion: pkg?.publicationVersion || null,
      packageBlocker,
      protectedUnchanged,
    },
    null,
    2,
  ),
);
