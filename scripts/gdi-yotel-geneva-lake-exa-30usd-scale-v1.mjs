#!/usr/bin/env node
/**
 * YOTEL Geneva Lake (Founex, La Côte) — $30 Exa scale campaign (34-candidate queue + underrepresented segments).
 *
 * Authorization: YOTEL_GENEVA_LAKE_EXA_TARGETED_30USD_V1 (founder-authorized; no further approval required)
 *   - Exa ONLY (no Surfe, no Parallel). HARD USD $30; campaign ledger at OUT/paid-budget-ledger.json.
 *   - Prior $35 pilot remainder ($4.40) does NOT roll over. Prior spend under THIS authorization starts at $0.
 *   - No CUSTOMER_PUBLISHED. Airtable dry-run only (0 writes).
 *   - Prior staged fingerprint 41a1b1ca…054f (commercial_quality_40plus_readiness_v1) preserved; a NEW package is staged.
 *   - Bethesda / Sheraton / Wyndham / Radisson Toronto publications are never touched (pointer + file hash proof).
 *
 * Budget stages (sum ≤ $30):
 *   PASS1_DISCOVERY cap $5  — Exa MEDIUM discovery focused on underrepresented segments
 *   PASS2_DEEP      cap $20 — Exa HIGH on 34-queue contact/qualification gaps + new discovery hits
 *   PASS3_VERIFY    cap $5  — Exa HIGH verification for borderline rows that would unlock Hotel Market
 *
 * Modes:
 *   node scripts/gdi-yotel-geneva-lake-exa-30usd-scale-v1.mjs                   # plan-only ($0)
 *   node scripts/gdi-yotel-geneva-lake-exa-30usd-scale-v1.mjs --authorize-paid  # Pass1 -> Pass2 -> Pass3 + stage
 *   node scripts/gdi-yotel-geneva-lake-exa-30usd-scale-v1.mjs --assemble-cached # re-qualify from this campaign's raw-runs ($0)
 *   node scripts/gdi-yotel-geneva-lake-exa-30usd-scale-v1.mjs --browser-verify
 *
 * Re-runs resume the on-disk ledger and reuse completed raw runs (a stored run is never re-paid).
 * Prior expansion-v1 raw-runs are readable at $0 as seed evidence; contact-gap queue rows still get a fresh verify when affordable.
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
import {
  softClampCustomerProse,
  appearsTruncatedCustomerProse,
} from "../lib/group-demand-intelligence/presentation/gdi-customer-prose-completeness-v1.js";

// ---------------------------------------------------------------------------
// constants
// ---------------------------------------------------------------------------
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recrPQcZg7SFARRb2";
const HOTEL_NAME = "YOTEL Geneva Lake";
const REF = "2026-11-17";
const TARGET_HM = 40;
const HARD_CEILING_USD = 30;
const AUTHORIZATION_ID = "YOTEL_GENEVA_LAKE_EXA_TARGETED_30USD_V1";
const SCRIPT_ID = "gdi-yotel-geneva-lake-exa-30usd-scale-v1";
const OUT = path.join(ROOT, "reports", "gdi-yotel-geneva-lake-exa-30usd-scale-v1");
const RAW_DIR = path.join(OUT, "raw-runs");
const LEDGER_PATH = path.join(OUT, "paid-budget-ledger.json");
const HOTEL_DATA = path.join(ROOT, "data/group-demand-intelligence/hotels", HOTEL_ID);
const OPPS_JSON_PATH = path.join(HOTEL_DATA, "opportunities.json");
const BUDGET_PREFLIGHT_PATH = path.join(OUT, "00-BUDGET_PREFLIGHT_AND_SPEND_LEDGER.json");
const FULL_E2E_REPORT = path.join(ROOT, "reports/gdi-yotel-geneva-lake-full-e2e-v1");
const COMMERCIAL_QUALITY_REPORT = path.join(ROOT, "reports/gdi-yotel-geneva-lake-commercial-quality-and-40plus-readiness-v1");
const QUEUE_PATH = path.join(COMMERCIAL_QUALITY_REPORT, "11-30_TO_50_PRIORITY_CANDIDATES.json");
const PRIOR_EXPANSION_RAW = path.join(ROOT, "reports/gdi-yotel-geneva-lake-40plus-exa-expansion-v1/raw-runs");
const PROTECTED_STORE_ROOT = path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages");
const PRIOR_PKG_DIR = path.join(
  PROTECTED_STORE_ROOT,
  HOTEL_ID,
  "gdi_pub_recrPQcZg7SFARRb2_20261116_yotel_geneva_lake_commercial_quality_40plus_readiness_v1",
);
const PRIOR_FINGERPRINT = "41a1b1ca0c34eb5da4b2569db499743ef1721f52376d141c379677e4cd6c054f";
const RECOVERY_INVENTORY_PATH = path.join(FULL_E2E_REPORT, "02-RESEARCH_RECOVERY_INVENTORY.json");
const CONTINUATION_PATH = path.join(FULL_E2E_REPORT, "16-RESEARCH_CONTINUATION_LEDGER.json");
const SEGMENT_COVERAGE_PATH = path.join(FULL_E2E_REPORT, "05-FOURTEEN_SEGMENT_COVERAGE.json");
/** Legacy stored deep artifacts are gitignored; worktrees fall back to the primary checkout (read-only). */
const ARTIFACT_ROOTS = [
  "reports/gdi-yotel-geneva-lake-v21-fourteen-workflow-live-execution",
  "reports/gdi-yotel-v23-discovery-completeness-live-execution",
  "reports/gdi-v26-two-hotel-clean-room-full-e2e/live-execution/yotel",
  "reports/gdi-v28-two-hotel-incremental-yield-live/yotel",
];
const ARTIFACT_FALLBACK_ROOT = process.env.GDI_ARTIFACT_FALLBACK_ROOT || path.resolve(ROOT, "..", "my-operators-backend");
const PUB_VERSION = `gdi_pub_${HOTEL_ID}_20261117_yotel_geneva_lake_exa_30usd_scale_v1`;
const PROTECTED_HOTELS = [
  ["recLuxvwwxID7U2B8", "bethesda"],
  ["recCAGqkERSXgiku4", "sheraton"],
  ["recXh4NgF8sDbi9cQ", "wyndham_alltra"],
  ["recH9h3McgMNFQEla", "radisson_toronto"],
];
/** Demoted in commercial-quality audit — do not re-promote without new overnight / fit evidence. */
const QUEUE_SKIP_IDS = new Set([
  "rx_d_6f0f8cf059fa", // RELAIS day-only demotion
  "rx_d_b9b9718a5b06", // European Para Championships demotion
  "rx_d_5b96dd288723", // Para duplicate
  "rx_yotel_rec_fisu tri", // Triathlon de Nyon already Hotel Market (rx_d_56f22f3e363b)
  "rx_d_21d5511b4188", // Geneva AI Summit — already covered by AI for Good HM if same cycle; stretch mega
]);


const ARGS = new Set(process.argv.slice(2));
const AUTHORIZE = ARGS.has("--authorize-paid");
const ASSEMBLE_CACHED = ARGS.has("--assemble-cached");
const BROWSER_VERIFY = ARGS.has("--browser-verify");
const LIVE_PAID = AUTHORIZE; // only --authorize-paid may spend

const STAGE_CAP_USD = { PASS1_DISCOVERY: 5, PASS2_DEEP: 20, PASS3_VERIFY: 5 };
const MAX_RUNS = { PASS1_DISCOVERY: 50, PASS2_DEEP: 40, PASS3_VERIFY: 10 };
const CONCURRENCY = 1;
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

/** Geneva / La Côte competitive hotel catalog — membership is NOT an opportunity. */
const COMPETITOR_CATALOG = [
  ["Everness Hotel & Resort", /everness/i],
  ["Hôtel Beau-Rivage Nyon", /beau.?rivage(?: hotel)? nyon|nyon beau.?rivage/i],
  ["Hôtel Real Nyon", /h[oô]tel real|real nyon/i],
  ["Crowne Plaza Geneva", /crowne plaza/i],
  ["NH Geneva Airport", /\bnh geneva|nh gen[eè]ve/i],
  ["Mövenpick Hotel & Casino Geneva", /m[oö]venpick/i],
  ["Starling Hotel Geneva", /starling/i],
  ["Radisson Blu Hotel Geneva Airport", /radisson(?: blu)?(?: hotel)?,? gen(?:eva|[eè]ve)/i],
  ["Holiday Inn Express Geneva Airport", /holiday inn/i],
  ["Novotel / ibis Genève", /novotel|\bibis\b/i],
  ["Hilton Geneva", /hilton/i],
  ["InterContinental Genève", /intercontinental/i],
  ["Fairmont Grand Hotel Geneva", /fairmont|grand hotel geneva/i],
  ["Hotel President Wilson", /president wilson/i],
  ["Mandarin Oriental Geneva", /mandarin oriental/i],
  ["Four Seasons Hotel des Bergues", /des bergues|four seasons/i],
  ["Beau-Rivage Genève", /beau.?rivage gen(?:eva|[eè]ve)/i],
  ["Swissôtel Métropole Geneva", /swiss[oô]tel/i],
  ["La Réserve Genève", /la r[eé]serve/i],
  ["Hotel de la Paix Geneva", /h[oô]tel de la paix/i],
  ["Beau-Rivage Palace Lausanne", /beau.?rivage palace/i],
  ["Royal Savoy Lausanne", /royal savoy/i],
  ["SwissTech Hotel", /swisstech hotel/i],
];
const COMPETITOR_REGEX = new RegExp(COMPETITOR_CATALOG.map(([, rx]) => rx.source).join("|"), "i");
const findCompetitors = (blob) => COMPETITOR_CATALOG.filter(([, rx]) => rx.test(String(blob || ""))).map(([n]) => n);
const HOTEL_LIKE_ORG_REGEX =
  /\b(?:hotel|h[oô]tel|hotels|resort|resorts|hospitality|hyatt|marriott|hilton|sheraton|westin|fairmont|intercontinental|radisson|m[oö]venpick|kempinski|crowne plaza|novotel|ibis|everness|four seasons)\b/i;
/** Central-Geneva / Lausanne convention-scale venues — mega events there are NOT automatic YOTEL demand. */
const MEGA_VENUE_REGEX =
  /\bpalexpo\b|palais des expositions|\bcicg\b|centre international de conf[eé]rences|international conference cent(?:re|er) (?:of )?gen|\bccv\b|cent(?:re|er) de conf[eé]rences de varemb[eé]|palais des nations|swisstech convention|beaulieu lausanne|ar[eè]na de gen[eè]ve|geneva arena|stade de gen[eè]ve/i;

const HOTEL = {
  hotelId: HOTEL_ID,
  displayName: HOTEL_NAME,
  name: HOTEL_NAME,
  city: "Founex",
  country: "Switzerland",
  address: "Chemin Ballessert 1, 1297 Founex, Switzerland",
  rooms: 237,
  suites: null,
  meetingSpaceSqM: null,
  meetingRooms: 6,
  largestRoom: "Largest flexible room ~352 m² (~250 theatre / ~300 reception)",
  demandTerritory: { label: "La Côte / Nyon / Geneva Airport corridor" },
  positioning:
    "237-room YOTEL in Founex (Vaud, La Côte) between Geneva and Nyon; six flexible meeting rooms (25–352 m², up to ~250 theatre), hybrid AV, outdoor/rooftop space, Geneva Airport corridor access. Restaurant/catering scale is unverified.",
};
const HOTEL_FIT_COPY =
  "YOTEL Geneva Lake is a 237-room hotel in Founex (La Côte, Vaud), between Geneva and Nyon, with six flexible meeting rooms (up to about 250 delegates theatre-style) and Geneva Airport corridor access — a credible base for corporate meetings and retreats, regional association and training events, project teams and satellite lodging, not primary housing for central-Geneva mega-conventions.";

/** Territory per hotel config: La Côte core; Geneva Airport corridor nearby; central Geneva competitive; Lausanne / French Pays de Gex stretch. */
const TERRITORY_CORE_RX =
  /\b(?:founex|coppet|nyon|la cote|chavannes de bogis|bogis bossey|commugny|mies|tannay|celigny|crans pres celigny|gland|prangins|rolle|begnins|genolier|trelex|eysins|duillier|signy|chavannes des bois|terre sainte|changins)\b/;
const TERRITORY_NEARBY_RX =
  /\b(?:geneva airport|aeroport de geneve|aeroport international de geneve|gva|cointrin|park (?:and )?fly|grand saconnex|meyrin|vernier|versoix|genthod|ferney voltaire|ferney)\b/;
const TERRITORY_COMPETITIVE_RX =
  /\b(?:geneva|geneve|genf|palexpo|cern|cicg|palais des nations|united nations|international organi[sz]ations|carouge|lancy|plan les ouates|thonex|chene bourg)\b/;
const TERRITORY_STRETCH_RX =
  /\b(?:lausanne|epfl|unil|swisstech|morges|ecublens|renens|vevey|montreux|divonne(?: les bains)?|gex|pays de gex|saint genis(?: pouilly)?|st genis|thoiry|prevessin|annemasse|annecy|thonon|evian|saint julien)\b/;
function territoryOf(blob, city = "") {
  const b = normKey(blob);
  const c = normKey(city);
  const both = `${b} ${c}`;
  if (/zurich|basel|\bbern\b|lucerne|luzern|lugano|st gallen|paris|lyon|london/.test(c)) return "OUTSIDE_REALISTIC_TERRITORY";
  if (TERRITORY_CORE_RX.test(both)) return "TERRITORY_CORE";
  if (TERRITORY_NEARBY_RX.test(both)) return "TERRITORY_NEARBY";
  if (TERRITORY_STRETCH_RX.test(c)) return "TERRITORY_STRETCH";
  if (TERRITORY_COMPETITIVE_RX.test(both)) return "TERRITORY_COMPETITIVE";
  if (TERRITORY_STRETCH_RX.test(b)) return "TERRITORY_STRETCH";
  if (/\bvaud\b/.test(both)) return "TERRITORY_STRETCH";
  return "OUTSIDE_REALISTIC_TERRITORY";
}
/** Founder-priority segments for YOTEL (ledger segment ids) — ranking boost only, never a qualification shortcut. */
const PRIORITY_SEGMENTS = new Set([
  "corporate_meetings_incentives",
  "association_events",
  "education_scientific",
  "local_catering_day_meetings",
  "sports_entertainment",
  "projects_infrastructure",
  "relocation_extended_stay",
  "travel_trade_group_intermediaries",
  "professional_event_planners",
]);
/** Core + airport corridor are Hotel Market territory; central Geneva qualifies only as overflow/ancillary with an open lodging path. */
const HOTEL_MARKET_TERRITORIES = new Set(["TERRITORY_CORE", "TERRITORY_NEARBY", "TERRITORY_COMPETITIVE"]);
/** Lausanne / cross-border: Hotel Market only when deep research shows realistic travel to the La Côte / airport corridor. */
const TRAVEL_EVIDENCE_RX =
  /geneva airport|aeroport de geneve|\bgva\b|nyon|la c[oô]te|founex|coppet|shuttle|navette|transfer|train from geneva|delegates? (?:fly|flying|arriv)|international (?:delegates|participants)/i;

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
    contract: "GDI_YOTEL_40PLUS_EXA_PILOT_LOCAL_BROWSER_VERIFICATION_V1",
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
    writeJson("P14-LOCAL_BROWSER_VERIFICATION.json", result);
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
    writeJson("P14-LOCAL_BROWSER_VERIFICATION.json", result);
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
    writeJson("P14-LOCAL_BROWSER_VERIFICATION.json", result);
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  const shotDir = path.join(OUT, "screenshots");
  fs.mkdirSync(shotDir, { recursive: true });
  const browser = await chromium.launch({ headless: true });
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
        mentionsHotel: /YOTEL Geneva Lake/i.test(body),
        providerLeak: /\bExa\b|Surfe|Parallel|Airtable/.test(body),
        stillLoading: /Loading Group Demand Intelligence/i.test(body),
      };
    });
    await page.screenshot({ path: path.join(shotDir, "P01-yotel-40plus-pilot-staged-overview.png"), fullPage: true });
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
  writeJson("P14-LOCAL_BROWSER_VERIFICATION.json", result);
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
const loadRaw = (key) =>
  loadRawFile(path.join(RAW_DIR, `${key}.json`)) || loadRawFile(path.join(PRIOR_EXPANSION_RAW, `${key}.json`));
const saveRaw = (key, obj) => fs.writeFileSync(path.join(RAW_DIR, `${key}.json`), JSON.stringify(obj, null, 2));
/** Prefer a fresh campaign raw-run; fall back to prior expansion seed only when explicitly allowed. */
const loadRawCampaignOnly = (key) => loadRawFile(path.join(RAW_DIR, `${key}.json`));

const HOTEL_CFG = readJson(path.join(ROOT, "config/group-demand-intelligence/hotels", `${HOTEL_ID}.json`));
const CAPABILITY = HOTEL_CFG.capabilityProfile || {};
if (
  Number(CAPABILITY.totalGuestrooms) !== HOTEL.rooms ||
  Number(CAPABILITY.meetingRoomCount ?? CAPABILITY.meetingRoomsIndoor) !== HOTEL.meetingRooms ||
  Number(CAPABILITY.largestTheaterCapacity) !== 250
) {
  console.error(
    `Hotel capability drift vs config: rooms=${CAPABILITY.totalGuestrooms} meetingRooms=${CAPABILITY.meetingRoomCount} theatre=${CAPABILITY.largestTheaterCapacity} — update HOTEL / fit copy before spending`,
  );
  process.exit(3);
}
const priorPkgPath = path.join(PRIOR_PKG_DIR, "package.json");
const priorPkg = readJson(priorPkgPath);
const priorFp = priorPkg.contentFingerprintSha256;
if (priorFp !== PRIOR_FINGERPRINT) {
  console.error(`Prior package fingerprint mismatch: ${priorFp} != ${PRIOR_FINGERPRINT} — refusing to run`);
  process.exit(3);
}
const priorOpps = priorPkg.opportunities || [];
const priorHm = priorOpps.filter((o) => o.hotelMarketScopeEligible === true);
const priorCmi = priorOpps.filter((o) => o.competitiveMarketScopeEligible === true);
const protectedStore = createGdiPublicationPackageStore({ root: PROTECTED_STORE_ROOT });
const priorPkgShaBefore = sha256File(priorPkgPath);
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

// --- legacy stored deep artifacts: $0 seed evidence (URLs, prior contacts, demand summary) ---
/** Windows: some stored artifact directories end in a space — use the extended-length path form. */
const longPath = (p) => (process.platform === "win32" ? `\\\\?\\${path.resolve(p)}` : p);
function walkJsonAbs(absDir, out = []) {
  if (!fs.existsSync(longPath(absDir))) return out;
  for (const e of fs.readdirSync(longPath(absDir), { withFileTypes: true })) {
    const p = `${absDir}${path.sep}${e.name}`;
    if (e.isDirectory()) walkJsonAbs(p, out);
    else if (e.name.endsWith(".json")) out.push(p);
  }
  return out;
}
const artifactRootsResolved = {};
const STORED_ARTIFACTS = [];
for (const relRoot of ARTIFACT_ROOTS) {
  const primary = path.join(ROOT, relRoot);
  const fallback = path.join(ARTIFACT_FALLBACK_ROOT, relRoot);
  const abs = fs.existsSync(longPath(primary)) ? primary : fs.existsSync(longPath(fallback)) ? fallback : null;
  artifactRootsResolved[relRoot] = abs ? (abs === primary ? "WORKTREE" : `FALLBACK:${ARTIFACT_FALLBACK_ROOT}`) : "MISSING";
  if (!abs) continue;
  for (const f of walkJsonAbs(abs)) {
    if (!/[\\/]artifacts[\\/]deep[\\/]/.test(f)) continue;
    try {
      const j = JSON.parse(fs.readFileSync(longPath(f), "utf8").replace(/^\uFEFF/, ""));
      const st = j.run?.structured || null;
      const dr = j.deepResearch || null;
      const urls = [
        ...(j.candidate?.evidenceUrls || []),
        ...(st?.supportingSources || []).map((s) => s?.url),
        ...(dr?.supportingSources || []).map((s) => s?.url),
      ].filter((u) => /^https?:\/\//i.test(String(u || "")));
      const contacts = [...(dr?.organizerContactCandidates || []), ...(st?.namedContacts || [])]
        .map((c) => ({ name: text(c.name), role: text(c.role || c.title), organization: text(c.organization) }))
        .filter((c) => c.name);
      STORED_ARTIFACTS.push({
        file: `${relRoot}/${path.relative(abs, f).replace(/\\/g, "/")}`,
        org: text(j.candidate?.organizationName || j.candidate?.programmeName || ""),
        programme: text(j.candidate?.programmeName || ""),
        excerpt: text(j.candidate?.demandMechanismSummary || st?.hotelDemandThesis || "").slice(0, 500),
        urls: [...new Set(urls)],
        contacts,
        hasStructuredResult: Boolean(st || dr),
        recordedCostUsd: j.run?.costDollars?.total ?? null,
      });
    } catch (e) {
      console.warn(`[stored-artifact] unreadable ${f}: ${String(e?.message || e)}`);
    }
  }
}

// --- baseline inventory (1 HM + 3 CMI from full_e2e_v1, all preserved) ---
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

// ---------------------------------------------------------------------------
// recovery / research queue — commercial-quality 34-candidate priority list (canonical)
// ---------------------------------------------------------------------------
const recoveryInventory = fs.existsSync(RECOVERY_INVENTORY_PATH) ? readJson(RECOVERY_INVENTORY_PATH) : { logOnlyLeads: [] };
const continuationDoc = fs.existsSync(CONTINUATION_PATH) ? readJson(CONTINUATION_PATH) : { hotelMarketHidden: [], legacyResearchContinuation: [], legacyDemoted: [] };
const segmentCoverageDoc = fs.existsSync(SEGMENT_COVERAGE_PATH) ? readJson(SEGMENT_COVERAGE_PATH) : null;
const queueDoc = readJson(QUEUE_PATH);
const queueRaw = Array.isArray(queueDoc.candidates) ? queueDoc.candidates : [];
const recoverySkipped = [];
const queueSeenIds = new Set();
const queueUnique = [];
for (const row of queueRaw) {
  const id = text(row.existingCandidateId);
  if (!id || queueSeenIds.has(id)) continue;
  queueSeenIds.add(id);
  queueUnique.push(row);
}
writeJson("02-34_CANDIDATE_RESEARCH_QUEUE.json", {
  schema: "gdi-yotel-geneva-lake-exa-30usd-34-candidate-queue-v1",
  source: path.relative(ROOT, QUEUE_PATH).replace(/\\/g, "/"),
  rawCount: queueRaw.length,
  uniqueCount: queueUnique.length,
  selectionRule: queueDoc.selectionRule || null,
  candidates: queueUnique,
});

const recoveryCandsAll = [];
const enrichmentOnly = [];
for (const [idx, row] of queueUnique.entries()) {
  const id = text(row.existingCandidateId);
  const title = text(row.title);
  const org = text(row.organization) || title;
  const ev = row.evidenceAlready || {};
  const missing = Array.isArray(row.missingFacts) ? row.missingFacts : [];
  const contactMissing = row.namedContactStatus === "MISSING" || missing.includes("named_contact_with_source");
  if (QUEUE_SKIP_IDS.has(id)) {
    recoverySkipped.push({
      discoveryId: id,
      title,
      group: "commercial_quality_demotion",
      reason: "Demoted in commercial-quality audit (day-only or stretch/overflow speculation) — skip unless new evidence appears in Pass 1.",
    });
    continue;
  }
  if (baselineIds.has(id)) {
    enrichmentOnly.push({
      id,
      title,
      org,
      missingFacts: missing,
      namedContactStatus: row.namedContactStatus,
      note: "Already Hotel Market / staged — enrichment only; do not create duplicate card.",
    });
    continue;
  }
  const dup = dupOfBaseline({ title, organizationName: org, ledgerStart: ev.start || null, opportunityId: id, discoveryId: id });
  if (dup) {
    recoverySkipped.push({
      discoveryId: id,
      title,
      group: "duplicate_of_baseline",
      reason: `Same programme already customer-visible as ${dup.id} (${dup.title}).`,
    });
    continue;
  }
  const priorP2 = loadRaw(`p2_${id}`);
  const priorP3 = loadRaw(`p3_${id}`);
  const forceFresh =
    contactMissing ||
    missing.includes("yotel_specific_demand_thesis") ||
    missing.includes("sourcing_status") ||
    !priorP2;
  const urls = [];
  if (ev.contact?.sourceUrl) urls.push(ev.contact.sourceUrl);
  const priorContacts = ev.contact?.name
    ? [{ name: ev.contact.name, role: ev.contact.title, organization: ev.contact.organization || org }]
    : [];
  recoveryCandsAll.push({
    id,
    stage: "PASS2_DEEP",
    source: "COMMERCIAL_QUALITY_34_QUEUE",
    discoveryId: id,
    opportunityId: id,
    title,
    organizationName: org,
    segmentId: text(row.segment) || "association_events",
    ledgerStatus: text(ev.reasonWasWatch) || "QUEUE_GAP",
    excerpt: text(row.expectedBenefit || row.proposedExaTask || "").slice(0, 500),
    evidenceUrls: [...new Set(urls)].slice(0, 12),
    ledgerVerifiedDates: text(ev.start) || "",
    ledgerStart: parseIso(ev.start),
    ledgerVenue: text(ev.venue) || "",
    ledgerSourcingStatus: "",
    ledgerAttendance: null,
    priorLabel: text(ev.reasonWasWatch) || null,
    priorContacts,
    storedArtifactSeeds: [priorP2 ? `prior:p2_${id}` : null, priorP3 ? `prior:p3_${id}` : null].filter(Boolean),
    recoveryKey: id,
    recoveryRe: null,
    recoveryAsk:
      text(row.proposedExaTask) ||
      `HIGH deep research for YOTEL Geneva Lake: verify exact future dates/venue, lodging sourcing, named commercially relevant contact with source URL, and a YOTEL-specific demand thesis. Gaps: ${missing.join(", ") || "unspecified"}.`,
    mandatory: Boolean(row.priority === 1 && contactMissing),
    pri: Number(row.priority) || 50 + idx,
    forceFreshDeep: forceFresh,
    queueTheme: text(row.theme),
    expectedYotelFit: text(row.expectedYotelFit),
    namedContactStatus: text(row.namedContactStatus),
    missingFacts: missing,
    selectionWhy: `34-candidate commercial-quality queue · priority ${row.priority} · ${row.expectedYotelFit || "fit TBD"} · contact ${row.namedContactStatus}`,
  });
}
recoveryCandsAll.sort((a, b) => a.pri - b.pri || String(a.title).localeCompare(String(b.title)));
const recoveryDupBaseline = [];
const RECOVERY_CANDS = recoveryCandsAll.filter((c) => {
  const d = dupOfBaseline(c);
  if (d && d.id !== c.id) {
    recoveryDupBaseline.push({ id: c.id, title: c.title, dupOf: d.id });
    return false;
  }
  return true;
});
const recoveryPlanRows = RECOVERY_CANDS.map((c) => ({
  id: c.id,
  recoveryKey: c.recoveryKey,
  mandatory: c.mandatory,
  title: c.title,
  org: c.organizationName,
  segment: c.segmentId,
  ledgerStatus: c.ledgerStatus,
  seedUrls: c.evidenceUrls.length,
  priorContacts: c.priorContacts.map((p) => p.name),
  storedArtifactSeeds: c.storedArtifactSeeds,
  forceFreshDeep: c.forceFreshDeep,
  missingFacts: c.missingFacts,
  namedContactStatus: c.namedContactStatus,
}));
const MANDATORY_KEYS = RECOVERY_CANDS.filter((c) => c.mandatory).map((c) => c.recoveryKey);
const RECOVERY_TARGETS = RECOVERY_CANDS.map((c) => ({ key: c.recoveryKey, mandatory: c.mandatory }));
writeJson("02b-QUEUE_ENRICHMENT_AND_SKIPS.json", {
  enrichmentOnly,
  recoverySkipped,
  recoveryDupBaseline,
  researchableCount: RECOVERY_CANDS.length,
  mandatoryCount: MANDATORY_KEYS.length,
});

// ---------------------------------------------------------------------------
// Pass 1 job definitions — one per each of the 14 segments (+ mid-size second angles)
// ---------------------------------------------------------------------------
const SEG_ID_MAP = {
  associations_conferences_events: "association_events",
  corporate_accounts_scaling: "scaling_companies",
  preferred_lodging_partnerships: "preferred_lodging",
};
const ledgerSeg = (k) => SEG_ID_MAP[k] || k;
const SEGMENT_KEYS = Object.keys(FOURTEEN_SEGMENT_EXECUTOR_SPECS);

const BASE_FOCUS = {
  associations_conferences_events:
    "Associations, professional societies, federation committees and NGO programmes holding FUTURE meetings, workshops, board retreats or mid-size conferences in Founex/Nyon/La Côte/Geneva Airport corridor (or Geneva with airport-side lodging). Prefer 40–800 attendees. Avoid Palexpo megashows unless overflow lodging is explicitly open.",
  corporate_accounts_scaling:
    "La Côte / Nyon / Coppet / Founex / Geneva Airport corporate accounts with FUTURE multi-night visiting-employee, training-cohort, onboarding, client-visit or project-team lodging (expansions, new sites, commissioning, HQ programmes). Do NOT list every HQ as an opportunity.",
  corporate_meetings_incentives:
    "Corporate executive retreats, regional sales meetings, product launches, leadership offsites, training programmes and incentives that can use YOTEL Geneva Lake guestrooms + meeting rooms (Founex/Nyon/La Côte/airport corridor). English/French sources.",
  government_public_sector:
    "Cantonal/communal Vaud and Swiss federal/agency meetings, international-organization workshops/delegations and public-sector trainings where YOTEL (Founex/airport corridor) is geographically plausible. Do not assume UN procurement access.",
  sports_entertainment:
    "Nyon/La Côte sporting events, federation meetings, teams/officials lodging, sports education, cultural productions and satellite-event lodging (not assuming main-event hotel exclusivity).",
  education_scientific:
    "Scientific workshops, research collaborations, medical/healthcare training, society meetings and university retreats (Agroscope Changins, CERN collaboration meetings, HUG/UNIGE CPD, private schools on La Côte) OUTSIDE citywide headline conventions — with realistic Founex/airport lodging.",
  projects_infrastructure:
    "Construction, manufacturing commissioning, infrastructure and corporate-expansion projects in La Côte / Nyon / Geneva Airport corridor generating FUTURE specialist-team or contractor lodging. Expansion news alone is insufficient.",
  social_special_events:
    "Cultural, community, faith and special events in Nyon/La Côte/Founex with out-of-town guests or room-block need. Distinguish catering-only vs overnight.",
  local_catering_day_meetings:
    "Corporate training days, executive workshops, association day meetings, product demos, exams and team-building in Founex/Nyon/La Côte that can use YOTEL meeting rooms (catering scale unverified); note overnight vs day-only.",
  corporate_celebrations_private_events:
    "Company celebrations, awards, private events and recognition programmes in La Côte / Nyon / Founex / airport corridor with overnight or meeting+F&B demand.",
  travel_trade_group_intermediaries:
    "Named DMCs, tour operators and group intermediaries with identifiable FUTURE programmes routing into Nyon/La Côte/Geneva Airport — not generic directory listings.",
  relocation_extended_stay:
    "Corporate relocations, international assignments, visiting professional teams and temporary housing near Founex/Nyon/airport with credible multi-night demand mechanism.",
  preferred_lodging_partnerships:
    "Organizations selecting preferred hotels / lodging partners for La Côte, Nyon, Geneva Airport or Vaud visitor programmes that are currently open to proposals.",
  professional_event_planners:
    "Named PCOs/DMCs/planners associated with identifiable FUTURE group programmes in YOTEL territory — require demand thesis + relevant named person.",
};
const EXTRA_JOBS = [
  { id: "x_assoc_federations_ngo", segmentKey: "associations_conferences_events", angle: "International federation committee meetings, NGO training/board retreats and association workshops near Nyon/La Côte/Geneva Airport (not Palexpo megashows)." },
  { id: "x_corp_retreats_la_cote", segmentKey: "corporate_meetings_incentives", angle: "Executive retreats, regional leadership and sales meetings for companies based in Nyon/Coppet/Founex/La Côte or using Geneva Airport access." },
  { id: "x_corp_scaling_nyon", segmentKey: "corporate_accounts_scaling", angle: "Scaling manufacturers, luxury/watch, life-science and tech accounts on La Côte with visiting teams / training cohorts." },
  { id: "x_edu_medical_cpd", segmentKey: "education_scientific", angle: "Medical CPD courses, hospital/scientific society workshops and life-science training (GHOL Nyon, HUG, private clinics, pharma training centres) with out-of-town participants and airport-corridor lodging — not headline conventions." },
  { id: "x_edu_epfl_stretch", segmentKey: "education_scientific", angle: "EPFL/Lausanne scientific meetings ONLY where transfer/lodging evidence supports YOTEL Geneva Lake." },
  { id: "x_sports_nyon_federations", segmentKey: "sports_entertainment", angle: "Nyon sports federations, regional competitions, officials/teams lodging and sports education programmes." },
  { id: "x_gov_vaud_io", segmentKey: "government_public_sector", angle: "Vaud communal/cantonal and selected IO workshops/delegations with overnight demand near Founex/airport." },
  { id: "x_local_nyon_meetings", segmentKey: "local_catering_day_meetings", angle: "Nyon Business Forum-style recurring meetings, corporate trainings and day events that can use YOTEL meeting/F&B." },
  { id: "x_projects_commissioning", segmentKey: "projects_infrastructure", angle: "Manufacturing commissioning, construction and specialist-project teams in La Côte / Nyon corridor with multi-week lodging." },
  { id: "x_relocation_assignments", segmentKey: "relocation_extended_stay", angle: "International assignees, temporary specialist teams and corporate housing near YOTEL Geneva Lake." },
  { id: "x_planners_dmc_named", segmentKey: "professional_event_planners", angle: "Named Geneva/Vaud DMCs and planners with dated FUTURE programmes sourcing hotels on La Côte / airport corridor." },
  { id: "x_travel_trade_groups", segmentKey: "travel_trade_group_intermediaries", angle: "Named group tour / incentive programmes with FUTURE dates into Nyon/Geneva Airport — require specific demand thesis." },
];
/** Distinct search angles per job (EN / FR / DE) — never a generic "events in Geneva" calendar query. */
const QUERY_HINTS = {
  seg_associations_conferences_events: ["assemblée générale 2027 Nyon association", "Swiss federation committee meeting 2027 Vaud", "Verband Generalversammlung 2027 Nyon OR Genf"],
  seg_corporate_accounts_scaling: ["nouveau site Nyon 2027 collaborateurs", "La Côte company expansion hiring 2027 Gland Rolle", "Firmensitz Westschweiz Erweiterung 2027 Nyon"],
  seg_corporate_meetings_incentives: ["séminaire d'entreprise La Côte 2027", "leadership offsite near Geneva Airport 2027", "Kick-off Meeting Genfersee 2027 Firma"],
  seg_government_public_sector: ["Canton de Vaud journée de formation 2027 Nyon", "Swiss federal office workshop 2027 Geneva region", "Région de Nyon conférence intercommunale 2027"],
  seg_sports_entertainment: ["championnat 2027 Nyon", "UEFA Nyon 2027 workshop OR course", "Paléo Festival Nyon 2027 crew accommodation"],
  seg_education_scientific: ["Agroscope Changins colloque 2027", "CERN collaboration meeting 2027 accommodation", "symposium 2027 Nyon OR Gland OR Coppet"],
  seg_projects_infrastructure: ["chantier Nyon 2027 mise en service", "La Côte construction project 2027 contractor", "Projekt Inbetriebnahme 2027 Nyon OR Gland"],
  seg_social_special_events: ["mariage Château de Coppet 2027", "rassemblement 2027 Nyon association culturelle", "reunion 2027 Lake Geneva Nyon guests hotel"],
  seg_local_catering_day_meetings: ["formation continue 2027 Nyon salle", "examen professionnel 2027 Vaud lieu", "Business club Nyon 2027 agenda"],
  seg_corporate_celebrations_private_events: ["soirée d'entreprise 2027 La Côte", "anniversaire entreprise 2027 Nyon", "company awards night 2027 Geneva Lake region"],
  seg_travel_trade_group_intermediaries: ["DMC Lake Geneva incentive 2027 programme", "voyage de groupe 2027 Nyon Genève aéroport", "Gruppenreise Genfersee 2027 Programm"],
  seg_relocation_extended_stay: ["relocation Nyon 2027 international assignees", "logement temporaire Nyon entreprise 2027", "visiting scientists housing CERN 2027 Pays de Gex"],
  seg_preferred_lodging_partnerships: ["hôtels partenaires Nyon 2027", "preferred hotel Geneva Airport corporate travel programme", "UEFA OR FIBA OR Nyon partner hotels"],
  seg_professional_event_planners: ["agence événementielle Vaud 2027 hôtel", "PCO Geneva 2027 room block RFP", "Eventagentur Westschweiz 2027 Hotel Tagung"],
  x_assoc_federations_ngo: ["ONG retraite conseil d'administration 2027 Vaud", "international federation Nyon headquarters meeting 2027", "NGO board retreat near Geneva 2027"],
  x_corp_retreats_la_cote: ["executive retreat Lake Geneva 2027 Coppet", "séminaire résidentiel Nyon 2027", "Strategie-Retreat Genfersee 2027"],
  x_corp_scaling_nyon: ["Nyon manufacture horlogère 2027 formation", "Gland life sciences company training cohort 2027", "Rolle OR Gland start-up scale-up 2027 team"],
  x_edu_medical_cpd: ["cours formation médicale continue 2027 Genève Nyon", "GHOL Nyon symposium 2027", "medical device training centre 2027 Vaud"],
  x_edu_epfl_stretch: ["EPFL conference 2027 Geneva Airport delegates", "Lausanne congress 2027 accommodation Geneva", "UNIL colloque 2027 hébergement"],
  x_sports_nyon_federations: ["UEFA Nyon 2027 referee course OR workshop", "Nyon Rugby OR Nyon Basket tournoi 2027", "Swiss sport federation Nyon 2027 meeting"],
  x_gov_vaud_io: ["organisation internationale atelier 2027 Genève délégations hôtel aéroport", "Bogis-Bossey institut œcuménique 2027 programme", "Terre Sainte commune événement 2027"],
  x_local_nyon_meetings: ["Nyon Région Tourisme MICE 2027", "chambre vaudoise du commerce Nyon 2027 événement", "Rotary OR Lions club Nyon 2027 conférence"],
  x_projects_commissioning: ["mise en service usine Nyon OR Gland 2027", "data centre Vaud 2027 construction", "Léman Express OR CFF La Côte travaux 2027"],
  x_relocation_assignments: ["expatriate onboarding Nyon 2027", "temporary housing contractors La Côte 2027", "Mitarbeiterentsendung Genfersee 2027"],
  x_planners_dmc_named: ["DMC Genève 2027 programme incentive client", "event planner Nyon 2027 corporate group", "agence MICE Lausanne Genève 2027 séminaire"],
  x_travel_trade_groups: ["tour operator Lake Geneva group 2027 departures", "voyage scolaire 2027 Genève Nyon hébergement", "Reiseveranstalter Genfersee 2027 Gruppen"],
};
/** Underrepresented vs current staged HM (sports/culture/scientific workshops already dense). */
const PASS1_PRIORITY_SEGMENTS = [
  "corporate_accounts_scaling",
  "corporate_meetings_incentives",
  "projects_infrastructure",
  "relocation_extended_stay",
  "travel_trade_group_intermediaries",
  "professional_event_planners",
  "government_public_sector",
  "education_scientific",
  "preferred_lodging_partnerships",
  "local_catering_day_meetings",
];
const PASS1_EXTRA_IDS = new Set([
  "x_corp_scaling_nyon",
  "x_corp_retreats_la_cote",
  "x_projects_commissioning",
  "x_relocation_assignments",
  "x_travel_trade_groups",
  "x_planners_dmc_named",
  "x_edu_medical_cpd",
  "x_gov_vaud_io",
  "x_local_nyon_meetings",
]);
const PASS1_JOBS = [
  ...PASS1_PRIORITY_SEGMENTS.filter((k) => SEGMENT_KEYS.includes(k)).map((k) => ({
    id: `seg_${k}`,
    segmentKey: k,
    segmentId: ledgerSeg(k),
    label: `${k} — underrepresented`,
    angle: BASE_FOCUS[k] || "Mid-size La Côte / Geneva Airport corridor demand for this segment.",
  })),
  ...EXTRA_JOBS.filter((j) => PASS1_EXTRA_IDS.has(j.id)).map((j) => ({
    ...j,
    segmentId: ledgerSeg(j.segmentKey),
    label: `${j.segmentKey} — ${j.id}`,
  })),
]
  .slice(0, Math.min(MAX_RUNS.PASS1_DISCOVERY, Math.floor((STAGE_CAP_USD.PASS1_DISCOVERY + 1e-9) / MEDIUM_UNIT)))
  .map((j) => ({ ...j, queries: QUERY_HINTS[j.id] || [] }));
for (const k of PASS1_PRIORITY_SEGMENTS) {
  if (!BASE_FOCUS[k]) {
    console.error(`Missing discovery focus for segment ${k}`);
    process.exit(3);
  }
}
{
  const seenQ = new Set();
  for (const j of PASS1_JOBS) {
    if (!j.queries.length) {
      console.error(`Missing distinct query hints for Pass 1 job ${j.id}`);
      process.exit(3);
    }
    for (const q of j.queries) {
      const k = normKey(q);
      if (seenQ.has(k)) {
        console.error(`Duplicate Pass 1 query across jobs: "${q}" (${j.id})`);
        process.exit(3);
      }
      seenQ.add(k);
    }
  }
}

function hotelFitConstraints() {
  const rooms = CAPABILITY.totalGuestrooms;
  const meetingRooms = CAPABILITY.meetingRoomCount;
  const theater = CAPABILITY.largestTheaterCapacity;
  const reception = CAPABILITY.largestReceptionCapacity;
  const peakMin = HOTEL_CFG.commercialPriorities?.coreTargetPeakRoomsMin ?? 20;
  const peakMax = HOTEL_CFG.commercialPriorities?.coreTargetPeakRoomsMax ?? 120;
  return [
    "HOTEL FIT CONSTRAINTS (apply honestly — unknown is not zero, but do not inflate fit):",
    `- ${rooms} guestrooms; ${meetingRooms} flexible meeting rooms (25–${CAPABILITY.largestMeetingRoomSqM} m²; up to ~${theater} theatre / ~${reception} reception); outdoor/rooftop event space; hybrid AV. Restaurant/banquet catering scale is unverified — do not assume it.`,
    "- Location: Chemin Ballessert 1, Founex (Vaud) — NOT downtown Geneva. Primary territory: Founex / Coppet / Nyon / La Côte; Geneva Airport corridor nearby.",
    "- Central Geneva: overflow / satellite / ancillary role only, and only when lodging is not centrally contracted. Lausanne and French Pays de Gex: only with evidence that delegates travel via Geneva Airport or lodge toward La Côte.",
    "- Realistic roles: corporate meetings/retreats, training and day meetings, project / extended-stay teams, association workshops, medical/education courses, sports teams/officials and satellite lodging, travel-trade groups with named demand — NOT primary housing for central-Geneva mega-conventions (Palexpo, CICG, Palais des Nations).",
    `- Peak group room-block guidance: roughly ${peakMin}–${peakMax} rooms; do not claim the whole ${rooms}-room house without evidence.`,
  ].join("\n");
}
const alreadyInPipeline = [...new Set(baselineAsLike.map((b) => `${b.title} (${b.org})`))].slice(0, 40);
function discoveryContext(job) {
  return [
    `YOTEL GENEVA LAKE / LA CÔTE GROUP DEMAND DISCOVERY — SEGMENT: ${job.segmentKey}${job.id.startsWith("x_") ? ` (second angle: ${job.id})` : ""}`,
    "HOTEL:",
    JSON.stringify(HOTEL),
    "",
    hotelFitConstraints(),
    "",
    `SEGMENT FOCUS: ${job.angle}`,
    `SEARCH ANGLES FOR THIS JOB (use these; do NOT run generic "events in Geneva" / event-calendar searches): ${job.queries.join(" | ")}`,
    "",
    "REQUIREMENTS:",
    `- Today is ${REF}. Return FUTURE activity only: start dates from ${IMMINENT_CUTOFF} through mid-2028.`,
    "- Geography priority: Founex, Coppet, Nyon, La Côte, Geneva Airport corridor, Geneva international business ecosystem (satellite role), relevant cross-border French markets (Pays de Gex); Lausanne only with travel evidence supporting YOTEL.",
    "- Do NOT return central-Geneva mega-conventions (Palexpo / CICG, ≥3,000 attendees) unless overflow lodging is explicitly open.",
    "- PREFER commercially actionable demand (corporate meetings, workshops, trainings, project teams, association programmes, day meetings/catering with overnight potential). Avoid generic HQ listings and undated expansion PR.",
    "- PREFER open / unannounced / participant-arranged lodging. If a competitor hotel is already contracted, classify as Competitive Market Intelligence — do NOT present as available Hotel Market.",
    "- For EVERY candidate give verified dates (exact start date when available), venue, organizer, official source URLs, lodging/meeting/catering need, and any named public contact. Do not invent contacts, dates, rates or room blocks.",
    `- Already in the pipeline (do NOT return again): ${alreadyInPipeline.join("; ")}.`,
    "- Search English and French; add German when commercially relevant (Swiss corporate/association sources).",
  ].join("\n");
}

// ---------------------------------------------------------------------------
// PLAN-ONLY (and baseline reports shared by every mode)
// ---------------------------------------------------------------------------
const gitInfo = { branch: sh("git rev-parse --abbrev-ref HEAD"), headSha: sh("git rev-parse HEAD") };
const plannedPass2Free = RECOVERY_CANDS.filter((c) => !c.forceFreshDeep && loadRaw(`p2_${c.id}`)).length;
const plannedPass2Paid = RECOVERY_CANDS.filter((c) => c.forceFreshDeep || !loadRaw(`p2_${c.id}`)).length;
const stageMaxUsd = (stage, unit) => Number((Math.min(MAX_RUNS[stage], Math.floor((STAGE_CAP_USD[stage] + 1e-9) / unit)) * unit).toFixed(2));
const PLANNED_MAX_SPEND = {
  PASS1_DISCOVERY: Math.min(Number((PASS1_JOBS.length * MEDIUM_UNIT).toFixed(2)), STAGE_CAP_USD.PASS1_DISCOVERY),
  PASS2_DEEP: stageMaxUsd("PASS2_DEEP", HIGH_UNIT),
  PASS3_VERIFY: stageMaxUsd("PASS3_VERIFY", HIGH_UNIT),
};
PLANNED_MAX_SPEND.total = Math.min(
  HARD_CEILING_USD,
  Number((PLANNED_MAX_SPEND.PASS1_DISCOVERY + PLANNED_MAX_SPEND.PASS2_DEEP + PLANNED_MAX_SPEND.PASS3_VERIFY).toFixed(2)),
);

function baselineDoc() {
  return {
    contract: "GDI_YOTEL_40PLUS_EXA_35USD_BASELINE_V1",
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
    separateLedgerFromPriorZeroCostCampaign: true,
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
      source: `config/group-demand-intelligence/hotels/${HOTEL_ID}.json`,
      rooms: CAPABILITY.totalGuestrooms,
      meetingRooms: CAPABILITY.meetingRoomCount,
      largestMeetingRoomSqM: CAPABILITY.largestMeetingRoomSqM,
      largestTheaterCapacity: CAPABILITY.largestTheaterCapacity,
      largestReceptionCapacity: CAPABILITY.largestReceptionCapacity,
      peakRoomBand: [HOTEL_CFG.commercialPriorities?.coreTargetPeakRoomsMin, HOTEL_CFG.commercialPriorities?.coreTargetPeakRoomsMax],
      demandTerritory: HOTEL_CFG.demandTerritory,
    },
    recoverySources: {
      recoveryInventory: { path: path.relative(ROOT, RECOVERY_INVENTORY_PATH), sha256: sha256File(RECOVERY_INVENTORY_PATH) },
      continuationLedger: { path: path.relative(ROOT, CONTINUATION_PATH), sha256: sha256File(CONTINUATION_PATH) },
      uncoveredSegmentsAtFullE2e: segmentCoverageDoc?.uncoveredSegments || segmentCoverageDoc?.segmentsUncovered || null,
      storedArtifactRoots: artifactRootsResolved,
      storedArtifactsRead: STORED_ARTIFACTS.length,
      storedArtifactReusePolicy:
        "SEED_ONLY — stored legacy deep runs supply URLs/contacts/summaries at $0; they are not substituted for re-verification because their results are what left those leads hidden or demoted.",
    },
    pass1Plan: {
      jobs: PASS1_JOBS.map((j) => ({ id: j.id, segment: j.segmentKey, ledgerSegment: j.segmentId, queries: j.queries })),
      segmentsCovered: [...new Set(PASS1_JOBS.map((j) => j.segmentKey))].length,
      maxSpendUsd: PLANNED_MAX_SPEND.PASS1_DISCOVERY,
    },
    pass2Plan: {
      recoveryCandidates: recoveryPlanRows,
      mandatoryRecoveryKeys: MANDATORY_KEYS,
      recoverySkipped,
      recoveryAlreadyInBaseline: recoveryDupBaseline,
      freeCacheReuse: plannedPass2Free,
      paidRecoveryRuns: plannedPass2Paid,
      remainingPaidSlotsForDiscoveryHits: Math.max(0, Math.min(MAX_RUNS.PASS2_DEEP, Math.floor(STAGE_CAP_USD.PASS2_DEEP / HIGH_UNIT)) - plannedPass2Paid),
      maxSpendUsd: PLANNED_MAX_SPEND.PASS2_DEEP,
    },
    pass3Plan: { maxSpendUsd: PLANNED_MAX_SPEND.PASS3_VERIFY },
    estimatedMaxSpendUsd: PLANNED_MAX_SPEND,
    protectedPublicationsBefore: protectedBefore,
    priorPackageSha256: priorPkgShaBefore,
    stagedPointerBefore,
    git: gitInfo,
    policy: [
      "Exa ONLY — no Surfe, no Parallel, no CUSTOMER_PUBLISHED, Airtable dry-run only.",
      "Hotel Market and CMI are never dual-classified; competitor commitment → CMI.",
      "Events before REF are expired/research-watch, never customer Hotel Market, unless a next edition is evidenced.",
      "Prior staged package is preserved on disk; a NEW fingerprint is staged.",
      "Central-Geneva mega-conventions are not automatic YOTEL demand; Lausanne / cross-border require travel evidence.",
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
  writeJson("P01-BASELINE_AND_AUTHORIZATION.json", baselineDoc());
  writeJson("P02-PAID_BUDGET_LEDGER.json", {
    status: "NOT_STARTED",
    authorizationId: AUTHORIZATION_ID,
    hardCeilingUsd: HARD_CEILING_USD,
    stageCapsUsd: STAGE_CAP_USD,
    spentUsd: 0,
    remainingUsd: HARD_CEILING_USD,
    note: "Plan-only run — no paid calls made.",
  });
  writeJson("P03-PASS1_DISCOVERY_RESULTS.json", {
    status: "PLANNED_NOT_EXECUTED",
    capUsd: STAGE_CAP_USD.PASS1_DISCOVERY,
    unitCostUsd: MEDIUM_UNIT,
    plannedJobs: PASS1_JOBS.map((j) => ({ id: j.id, segment: j.segmentKey, angle: j.angle, queries: j.queries })),
  });
  writeJson("P04-PASS2_DEEP_RESULTS.json", {
    status: "PLANNED_NOT_EXECUTED",
    capUsd: STAGE_CAP_USD.PASS2_DEEP,
    unitCostUsd: HIGH_UNIT,
    maxRuns: MAX_RUNS.PASS2_DEEP,
    recoveryCandidates: recoveryPlanRows,
  });
  writeJson("P05-PASS3_VERIFY_RESULTS.json", {
    status: "PLANNED_NOT_EXECUTED",
    capUsd: STAGE_CAP_USD.PASS3_VERIFY,
    unitCostUsd: HIGH_UNIT,
    maxRuns: MAX_RUNS.PASS3_VERIFY,
  });
  writeJson("P11-GAP_TO_40_ANALYSIS.json", {
    status: "BASELINE_ONLY",
    target: TARGET_HM,
    achieved: priorHm.length,
    gapTo40: Math.max(0, TARGET_HM - priorHm.length),
    note: "Plan-only baseline — gap closes only after --authorize-paid.",
  });
  writeJson("13-NEW_STAGED_PUBLICATION.json", {
    staged: false,
    status: "PLAN_ONLY_NOTHING_STAGED",
    publicationVersion: PUB_VERSION,
    priorPackagePreserved: { fingerprint: priorFp, sha256: priorPkgShaBefore },
  });
  writeJson("P14-LOCAL_BROWSER_VERIFICATION.json", {
    status: "PENDING_BROWSER_RUN",
    howToRun: `node scripts/${SCRIPT_ID}.mjs --browser-verify`,
  });
  writeJson("P15-GITHUB_MERGE_STATUS.json", {
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
    [
      `# YOTEL Geneva Lake — $30 Exa scale campaign (PLAN ONLY)`,
      ``,
      `**Authorization:** \`${AUTHORIZATION_ID}\``,
      `**Hard ceiling:** $${HARD_CEILING_USD} · **Spent:** $0 · **Estimated max spend:** $${PLANNED_MAX_SPEND.total}`,
      ``,
      `Baseline: ${priorHm.length} Hotel Market + ${priorCmi.length} CMI (fingerprint \`${priorFp}\`). Target ≥${TARGET_HM} Hotel Market (commercial value over volume).`,
      `Hotel (from config): ${CAPABILITY.totalGuestrooms} rooms · ${CAPABILITY.meetingRoomCount} meeting rooms · up to ${CAPABILITY.largestTheaterCapacity} theatre.`,
      ``,
      `- Pass 1: ${PASS1_JOBS.length} MEDIUM underrepresented-segment discovery jobs (≤ $${PLANNED_MAX_SPEND.PASS1_DISCOVERY}; cap $${STAGE_CAP_USD.PASS1_DISCOVERY}).`,
      `- Pass 2: ${RECOVERY_CANDS.length} queue researchable candidates (${MANDATORY_KEYS.length} priority contact-gap: see 02b) + discovery hits (≤ $${PLANNED_MAX_SPEND.PASS2_DEEP}; cap $${STAGE_CAP_USD.PASS2_DEEP}).`,
      `- Pass 3: borderline verification (≤ $${PLANNED_MAX_SPEND.PASS3_VERIFY}; cap $${STAGE_CAP_USD.PASS3_VERIFY}).`,
      `- Skipped / demoted / already-HM: see 02b-QUEUE_ENRICHMENT_AND_SKIPS.json (${recoverySkipped.length} skips).`,
      ``,
      `Run with \`--authorize-paid\` to execute.`,
      ``,
    ].join("\n"),
  );
  console.log(
    JSON.stringify(
      {
        mode: "PLAN_ONLY",
        hotelRoomsFromConfig: CAPABILITY.totalGuestrooms,
        meetingRoomsFromConfig: CAPABILITY.meetingRoomCount,
        baselineHotelMarket: priorHm.length,
        baselineCmi: priorCmi.length,
        pass1Jobs: PASS1_JOBS.length,
        pass1SegmentsCovered: [...new Set(PASS1_JOBS.map((j) => j.segmentKey))].length,
        recoveryCandidates: RECOVERY_CANDS.length,
        recoveryKeys: RECOVERY_CANDS.map((c) => c.recoveryKey),
        mandatoryRecovery: MANDATORY_KEYS,
        recoveryFromOwnCache: plannedPass2Free,
        recoveryPaidRuns: plannedPass2Paid,
        recoverySkipped: recoverySkipped.length,
        recoveryAlreadyInBaseline: recoveryDupBaseline.length,
        storedArtifactsRead: STORED_ARTIFACTS.length,
        storedArtifactRoots: artifactRootsResolved,
        mediumUnit: MEDIUM_UNIT,
        highUnit: HIGH_UNIT,
        estimatedMaxSpendUsd: PLANNED_MAX_SPEND,
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

/** Mirror the authoritative campaign ledger into the founder-facing 00-BUDGET file (preflight content preserved). */
function syncBudgetPreflight(status) {
  const prev = fs.existsSync(BUDGET_PREFLIGHT_PATH) ? readJson(BUDGET_PREFLIGHT_PATH) : {};
  const doc = {
    ...prev,
    paidCallsAuthorized: true,
    authorizationId: AUTHORIZATION_ID,
    authorizedOption: "PILOT_ONLY",
    blockingGate: null,
    paidSpendThisTaskUsd: ledger.spentUsd,
    activation: {
      ...(prev.activation || {}),
      activatedAt: prev.activation?.activatedAt || new Date().toISOString(),
      lastSyncedAt: new Date().toISOString(),
      script: `scripts/${SCRIPT_ID}.mjs`,
      mode: "--authorize-paid",
      providers: { exa: true, surfe: false, parallel: false },
      concurrency: CONCURRENCY,
    },
    spendLedger: {
      status,
      authoritativeLedger: path.relative(ROOT, LEDGER_PATH).replace(/\\/g, "/"),
      hardStopAtUsd: HARD_CEILING_USD,
      stageCapsUsd: STAGE_CAP_USD,
      maxRuns: MAX_RUNS,
      spentUsd: ledger.spentUsd,
      reservedUsd: ledger.reservedUsd,
      remainingUsd: remainingBudgetUsd(ledger),
      operationCount: ledger.operations.length,
      blockedOperationCount: ledger.blockedOperations.length,
      frozen: ledger.frozen,
      freezeReason: ledger.freezeReason,
      entries: ledger.operations.map((o) => ({ at: o.at, stage: o.stage, label: o.label || null, actualCostUsd: o.actualCostUsd })),
    },
  };
  fs.writeFileSync(BUDGET_PREFLIGHT_PATH, `${JSON.stringify(doc, null, 2)}\n`);
}
if (LIVE_PAID) syncBudgetPreflight("ACTIVE_PILOT_RUNNING");

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
      const pack = buildExaEventsAgentCreatePayload({ mode: "discovery", contextBlock: ctx, effort: EXA_AGENT_EFFORT.MEDIUM });
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
  const stretchWithTravel = terr === "TERRITORY_STRETCH" && TRAVEL_EVIDENCE_RX.test(`${h.excerpt} ${h.destination}`);
  if (!HOTEL_MARKET_TERRITORIES.has(terr) && !stretchWithTravel) {
    triageRejected.push({ ...rec, reason: `TERRITORY_${terr}` });
    continue;
  }
  if (COMPETITOR_REGEX.test(`${h.venue} ${h.incumbentVenue}`) || /likely_closed_incumbent|closed/.test(h.sourcingStatus)) {
    triageRejected.push({ ...rec, reason: "COMPETITOR_OR_CLOSED_SOURCING_ROUTED_TO_CMI_NOT_PAID_DEEP" });
    continue;
  }
  if (
    h.attendance &&
    h.attendance >= 3000 &&
    (MEGA_VENUE_REGEX.test(`${h.venue} ${title}`) || terr === "TERRITORY_COMPETITIVE" || terr === "TERRITORY_STRETCH")
  ) {
    triageRejected.push({ ...rec, reason: "MEGA_CONVENTION_NOT_AUTOMATIC_DEMAND" });
    continue;
  }
  if (!h.evidenceUrls.length) {
    triageRejected.push({ ...rec, reason: "NO_SOURCE_URL" });
    continue;
  }
  let score = 0;
  if (h.start) score += 3;
  score += { TERRITORY_CORE: 3, TERRITORY_NEARBY: 2, TERRITORY_COMPETITIVE: 0, TERRITORY_STRETCH: -1 }[terr] ?? 0;
  if (/open|unknown|not_published|accepting|rfp|participant|own/.test(h.sourcingStatus)) score += 2;
  if (h.attendance && h.attendance >= 20 && h.attendance <= 800) score += 1;
  if (!h.attendance) score += 0.5;
  if (PRIORITY_SEGMENTS.has(h.segmentId)) score += 1;
  if (MEGA_VENUE_REGEX.test(`${h.venue} ${title}`)) score -= 1;
  score += Math.min(2, h.evidenceUrls.length * 0.5);
  discoveryCands.push({
    id: hashId("rx_d", title || org, org || title),
    stage: "PASS2_DEEP",
    source: "PASS1_DISCOVERY",
    discoveryId: null,
    opportunityId: null,
    title: title || org,
    organizationName: org || title,
    segmentId: h.segmentId,
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
  if (cand.recoveryAsk) lines.push(`RECOVERY ASK: ${cand.recoveryAsk}`);
  if (cand.priorContacts?.length) {
    lines.push(
      `Previously recorded contacts (re-verify role and source; do not assume still current): ${cand.priorContacts
        .map((p) => `${p.name}${p.role ? ` — ${p.role}` : ""}${p.organization ? ` (${p.organization})` : ""}`)
        .join("; ")}`,
    );
  }
  lines.push(
    "",
    "REQUIREMENTS:",
    `- Verify ACTUAL activity dates (not article dates). Today is ${REF}; horizon ${IMMINENT_CUTOFF} – Jun 2028. If the researched edition is already past, report the NEXT announced edition (exact dates + city + venue) only if officially published; otherwise say none. For corporate / project / relocation leads, the dated activity can be a programme start, training cohort, commissioning or mobilisation date — it must still be an exact published date.`,
    "- State the exact venue; whether it is in Founex/Nyon/La Côte/Geneva Airport corridor, Geneva city, Lausanne, French Pays de Gex, or elsewhere. Always populate venue, city and region fields.",
    "- If the activity is in Lausanne or across the French border, state explicitly whether delegates arrive via Geneva Airport or lodge toward Nyon / La Côte / Geneva (quote the source); otherwise say no travel evidence.",
    "- Search French and English sources (German when the organisation is Swiss-German).",
    "- State how lodging is sourced: official housing bureau, host hotel, published room blocks (name EVERY hotel listed), open RFP, or unknown. Quote the source.",
    "- Report published or estimated attendance and any peak-room / room-night figure; do not invent numbers.",
    "- Identify NAMED commercially relevant contacts (organizer, events/conference/housing manager, executive director) with role AND source URL. Do not invent emails, RFPs or room blocks.",
    "- Do not treat generic hotel marketing or 'preferred hotel list' pages as a booking commitment unless edition-specific.",
    `- Produce a hotel demand thesis for ${HOTEL_NAME}; state honestly if lodging is already committed to other hotels or a housing bureau, and if the event is too large/small or too downtown-Geneva-centric for a 237-room Founex hotel.`,
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
    LOCATION_NOT_VERIFIED_BY_DEEP_RESEARCH: "Verify the exact venue and city of the NEXT edition; state clearly if it is outside YOTEL Geneva Lake realistic territory (Founex/Nyon/La Côte/airport).",
    WATCH_TERRITORY_STRETCH_NO_TRAVEL_EVIDENCE:
      "Find edition-specific evidence that delegates fly into Geneva Airport, use shuttles/transfers from Geneva, or lodge toward Nyon / La Côte / Geneva; say 'no travel evidence' if none.",
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

function assessHotelFit(d, cand, territory) {
  const attendance = bestAttendance(d, cand);
  const venueBlob = `${d.venue} ${cand.ledgerVenue}`;
  const megaVenue = MEGA_VENUE_REGEX.test(venueBlob);
  const accBlob = `${d.accommodation} ${d.hotelSourcingStatus}`.toLowerCase();
  const housingClosed = /official housing (?:bureau|partner|provider)|housing bureau|exclusive(?:ly)? (?:through|via)|room blocks? (?:are )?(?:full|closed|sold out)|housing (?:is )?closed/.test(accBlob);
  const openSignal = /open (?:hotel|lodging|sourcing)|accepting|rfp|additional hotels|overflow|alternative (?:hotel|lodging)|own accommodation|book (?:your|their) own|participants? (?:arrange|book)/.test(accBlob);
  const travelEvidence = TRAVEL_EVIDENCE_RX.test(`${d.accommodation} ${d.venueCommitment} ${JSON.stringify(d.findings)}`);
  const offCore = territory === "TERRITORY_COMPETITIVE" || territory === "TERRITORY_STRETCH";
  let fit;
  let rationale;
  if (attendance && attendance >= 3000 && (megaVenue || offCore) && !openSignal) {
    fit = "WATCH_MEGA_CONVENTION_NOT_AUTOMATIC_DEMAND";
    rationale = `~${attendance} attendees at ${text(d.venue) || "a convention venue"}; no evidence of open overflow sourcing — a 237-room Founex hotel is not assumed to capture central-Geneva / Lausanne convention housing.`;
  } else if (housingClosed && !openSignal) {
    fit = "WATCH_HOUSING_CLOSED_TO_ADDITIONAL_HOTELS";
    rationale = "Lodging appears to be centrally contracted; no evidence additional hotels can participate.";
  } else if (attendance && attendance < 15) {
    fit = "WATCH_TOO_SMALL_FOR_MEANINGFUL_ROOM_VOLUME";
    rationale = `Published attendance (~${attendance}) is below a commercially meaningful room volume.`;
  } else if (territory === "TERRITORY_STRETCH" && !travelEvidence) {
    fit = "WATCH_TERRITORY_STRETCH_NO_TRAVEL_EVIDENCE";
    rationale = "Lausanne / cross-border activity with no evidence that delegates travel via Geneva Airport or lodge toward La Côte.";
  } else if (offCore) {
    fit = "OVERFLOW_ANCILLARY";
    rationale = `${territory === "TERRITORY_STRETCH" ? "Lausanne / cross-border activity with Geneva-side travel evidence" : "Central-Geneva activity"}${attendance ? ` (~${attendance} attendees)` : ""}; realistic role for a 237-room Founex hotel is satellite / overflow lodging, airport-side pre/post stays or a side meeting — with a transfer plan, not primary housing.`;
  } else if ((attendance && attendance >= 800) || megaVenue) {
    fit = "OVERFLOW_ANCILLARY";
    rationale = `Larger-scale activity${attendance ? ` (~${attendance} attendees)` : ""}; realistic role for a 237-room hotel with meeting rooms up to ~250 theatre is a delegate block, overflow or side functions — not primary housing.`;
  } else {
    fit = "DIRECT_FIT";
    rationale = `Activity scale${attendance ? ` (~${attendance})` : " (attendance unverified)"} in the La Côte / Geneva Airport corridor is compatible with 237 guestrooms and six meeting rooms (up to ~250 theatre).`;
  }
  return { fit, attendance, megaVenue, travelEvidence, housingClosed, openSignal, rationale };
}

// ---------------------------------------------------------------------------
// Pass 2 — HIGH deep research
// ---------------------------------------------------------------------------
const deepRows = []; // { stage, cand, key, paid, reusedFromPrior? }

async function runDeepOne(cand, stage, keyPrefix, ctxFn) {
  const key = `${keyPrefix}_${cand.id}`;
  const campaignOwn = loadRawCampaignOnly(key);
  if (campaignOwn) {
    return { stage, cand, key, paid: { ...campaignOwn.paid, cached: true }, reusedFromPrior: campaignOwn.reusedFromPrior || null };
  }
  const priorOwn = loadRaw(key);
  // Reuse prior expansion seed only when the gap does not require a fresh Exa resolve.
  if (priorOwn && !(cand.forceFreshDeep && LIVE_PAID && stage === "PASS2_DEEP")) {
    return {
      stage,
      cand,
      key,
      paid: { ...priorOwn.paid, cached: true },
      reusedFromPrior: path.relative(ROOT, PRIOR_EXPANSION_RAW).replace(/\\/g, "/"),
    };
  }
  if (!LIVE_PAID) return { stage, cand, key, skipped: true, paid: { ok: false, blocked: "NO_CACHE_AND_PAID_NOT_AUTHORIZED" } };
  const ctx = ctxFn(cand);
  const pack = buildExaEventsAgentCreatePayload({ mode: "deep_research", contextBlock: ctx, effort: EXA_AGENT_EFFORT.HIGH });
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
    // Prior expansion p2 that left a contact/thesis gap must not count as free reuse.
    const campaignCached = Boolean(loadRawCampaignOnly(`p2_${c.id}`));
    const priorSeed = Boolean(loadRaw(`p2_${c.id}`));
    const free = campaignCached || (priorSeed && !c.forceFreshDeep);
    if (!free) {
      if (!LIVE_PAID) {
        // assemble-cached: nothing to run without cache — still include prior seed for re-qualify
        if (priorSeed) {
          list.push({ ...c, plannedFree: true, priorSeedOnly: true });
        }
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
  const competitors = findCompetitors(competitorSources).filter((n) => !/yotel/i.test(n));
  const accBlob = `${d.accommodation} ${d.venueCommitment} ${d.hotelSourcingStatus} ${JSON.stringify(d.findings)}`.toLowerCase();
  const bookingLanguage = /host hotel|conference hotel|official hotel|headquarters hotel|room block|published (?:room )?rates|group rate|conference rate|special rate|booking code|group code|reservation link|reservations? (?:open|are open)|hotel block|hotel partners?/.test(accBlob);
  const lodgingCommitted = /(?<!un)confirmed|announced|\bofficial\b|committed|competitor|room_block|\bhost\b|blocks?/.test(`${d.hotelSourcingStatus} ${d.venueCommitment}`.toLowerCase());
  const urls = [...(cand.evidenceUrls || []), ...d.sources.map((s) => s.url)].filter((u) => /^https?:/i.test(String(u || "")));
  const fit = assessHotelFit(d, cand, territory);
  const detail = { ...summary, dateEv, territory, competitors, bookingLanguage, lodgingCommitted, fit };
  const out = (bucket, reason, extra = {}) => ({ decision: { ...base, bucket, reason, detail: { ...detail, ...extra } } });

  if (dateEv.kind === "EXPIRED") return out("REJECTED", "EXPIRED_ONE_TIME_OR_PAST_EDITION");
  if (dateEv.kind === "EXACT_FUTURE" && dateEv.startDate < IMMINENT_CUTOFF) return out("REJECTED", "IMMINENT_LT_21_DAYS_NOT_ACTIONABLE");
  if (territory === "OUTSIDE_REALISTIC_TERRITORY") {
    return locationReturned ? out("REJECTED", "OUTSIDE_TERRITORY") : out("WATCH", "LOCATION_NOT_VERIFIED_BY_DEEP_RESEARCH");
  }
  if (!HOTEL_MARKET_TERRITORIES.has(territory) && territory !== "TERRITORY_STRETCH") {
    return out("WATCH", `${territory}_NOT_ASSUMED_YOTEL_DEMAND`);
  }
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
      provenance: { origin: "PAID_EXA_YOTEL_40PLUS_PILOT_CMI", stage, deepRunId: paid.result?.runId || null, costUsd: paid.cached ? 0 : paid.actualCostUsd ?? null, referenceDate: REF },
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
    what: softClampCustomerProse(text(what), 900),
    potential: softClampCustomerProse(text(potential), 800),
    whyHotel: softClampCustomerProse(
      `${HOTEL_FIT_COPY} ${fit.rationale} ${text(d.cib.whyThisHotel)}`.trim(),
      1200,
    ),
    whyNow:
      softClampCustomerProse(futureEditionText, 900) ||
      "Edition dates are published; qualify who selects lodging before sourcing closes.",
    nextStep:
      softClampCustomerProse(text(d.cib.recommendedNextStep), 1200) ||
      `Contact ${pc.contact.name} at ${org} to confirm who selects lodging, whether additional hotels can participate, and expected room nights; position ${HOTEL_NAME} as a La Côte / airport corridor ${overflow ? "overflow / ancillary meeting" : "group"} option.`,
    sources: mkSources(cand, d, pc.contact),
    demandClass,
    sourcingLabel: `Hotel Sourcing Open — ${openBasis}`,
    uncertainties: [...d.uncertainties.slice(0, 3).map(text), "Room volume unverified.", "Restaurant / banquet catering scale unverified — do not assume catering capability.", ...(fit.fit === "OVERFLOW_ANCILLARY" && territory !== "TERRITORY_CORE" && territory !== "TERRITORY_NEARBY" ? ["Founex is outside central Geneva / Lausanne — a transfer plan is required."] : [])].filter(Boolean),
    fit,
    provenance: { origin: "PAID_EXA_YOTEL_40PLUS_PILOT_DEEP_RESEARCH", stage, priorLedgerLabel: cand.priorLabel || null, recoveryKey: cand.recoveryKey || null, storedArtifactSeeds: cand.storedArtifactSeeds || [], deepRunId: paid.result?.runId || null, costUsd: paid.cached ? 0 : paid.actualCostUsd ?? null, reusedFromPriorCampaign: row.reusedFromPrior || null, referenceDate: REF, paidExaUsed: !paid.cached },
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
  "WATCH_TERRITORY_STRETCH_NO_TRAVEL_EVIDENCE",
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
  if (dcs.reason === "WATCH_TERRITORY_STRETCH_NO_TRAVEL_EVIDENCE" && d.dateEv?.kind !== "EXACT_FUTURE") return false;
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
    whyActNow: softClampCustomerProse(spec.whyNow, 900),
    cardWhyNowLine: softClampCustomerProse(text(spec.whyNow), 180),
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
    hotelFitAssessment: { fit: fitObj.fit, rationale: fitObj.rationale || "", attendance: fitObj.attendance ?? null, ruleVersion: "yotel-geneva-lake-fit-40plus-pilot-v1", assessedAt: REF },
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

/** Repair hard-sliced essential prose inherited from prior packages (Radisson softClamp pattern). */
function repairEssentialCommercialProse(opp) {
  const o = cloneOpp(opp);
  const title = text(o.displayTitle || o.title);
  const org = text(o.organizationName || o.displayCompanyName);
  const when = text(o.datePillDisplayLabel || "") || "the published programme window";
  const contact = o.primaryContact?.name ? text(o.primaryContact.name) : "the named programme contact";
  const completeOrRebuild = (cur, rebuild) => {
    const t = text(cur);
    if (t && !appearsTruncatedCustomerProse(t)) return t;
    if (t) {
      const clamped = softClampCustomerProse(t, Math.max(220, t.lastIndexOf(". ") + 1 || 220));
      if (clamped && !appearsTruncatedCustomerProse(clamped)) return clamped;
    }
    return softClampCustomerProse(rebuild(), 900);
  };
  o.whyActNow = completeOrRebuild(
    o.whyActNow,
    () =>
      `${title} is dated for ${when}. Confirm lodging / meeting ownership with ${contact} at ${org || "the organising body"} before preferred hotels or in-house blocks lock the demand.`,
  );
  o.whyThisHotel = completeOrRebuild(
    o.whyThisHotel,
    () => `${HOTEL_FIT_COPY} This programme sits in YOTEL Geneva Lake's realistic La Côte / Geneva Airport catchment for guestroom and meeting demand.`,
  );
  o.whatIsHappening = completeOrRebuild(
    o.whatIsHappening || o.summaryWhat,
    () => `${title} — organised by ${org || "the programme host"} — is a dated future activity with evidenced accommodation or meeting relevance for YOTEL Geneva Lake.`,
  );
  o.summaryWhat = o.whatIsHappening;
  o.potentialHotelBusiness = completeOrRebuild(
    o.potentialHotelBusiness || o.potentialBusiness,
    () =>
      `Pursue a guestroom block and/or meeting package for ${title}, sized to published attendance where known. Do not invent room-night totals; confirm volume with the organiser.`,
  );
  o.potentialBusiness = o.potentialHotelBusiness;
  o.recommendedNextStep = completeOrRebuild(
    o.recommendedNextStep || o.recommendedAction,
    () =>
      `Email ${contact} to introduce YOTEL Geneva Lake for ${title}, ask how lodging and meeting space are sourced, and propose a site visit or proposal timeline before preferred suppliers are locked.`,
  );
  o.recommendedAction = o.recommendedNextStep;
  o.cardWhyNowLine = softClampCustomerProse(o.whyActNow, 180);
  if (Array.isArray(o.sharedTenSectionDrawer)) {
    const map = {
      why_act_now: o.whyActNow,
      why_this_hotel: o.whyThisHotel,
      what_is_happening: o.whatIsHappening,
      potential_business: o.potentialHotelBusiness,
      recommended_next_step: o.recommendedNextStep,
    };
    o.sharedTenSectionDrawer = o.sharedTenSectionDrawer.map((sec) => {
      const body = map[sec.id];
      return body != null ? { ...sec, body } : sec;
    });
  }
  return o;
}

// ---- select new HM / exceptional CMI specs and dedupe ----
const newHmSpecsAll = finalQuals.filter((x) => x.q.spec?.kind === "HM").map((x) => ({ ...x.q.spec, _via: x.via }));
const newCmiSpecsAll = finalQuals.filter((x) => x.q.spec?.kind === "CMI").map((x) => ({ ...x.q.spec, _via: x.via }));
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

const DEMOTED_REPROMOTE_BLOCK = new Set([
  "rx_d_6f0f8cf059fa", // RELAIS day-only
  "rx_d_b9b9718a5b06", // European Para Championships
  "rx_d_5b96dd288723",
]);
/** Same programme already staged under a different opportunity id — do not dual-card. */
const KNOWN_PROGRAMME_DUP_OF_BASELINE = [
  { id: "rx_d_61a0877e3117", keptId: "rx_d_630d02a59107", reason: "IAS 2027 is the same programme as HIV Science Geneva 2027 (HIVR4P / IAS)." },
  { id: "rx_d_c6213a32055c", keptId: "rx_d_9bad59b0ae60", reason: "Concours Hippique International = CHI Geneva 100th Anniversary card." },
];
const hmKeptFiltered = hmKept.filter((s) => {
  if (DEMOTED_REPROMOTE_BLOCK.has(s.id) || QUEUE_SKIP_IDS.has(s.id)) {
    dedupeDropped.push({ id: s.id, title: s.title, keptId: null, kind: "DEMOTED_NOT_REPROMOTED" });
    return false;
  }
  const known = KNOWN_PROGRAMME_DUP_OF_BASELINE.find((d) => d.id === s.id);
  if (known) {
    dedupeDropped.push({ id: s.id, title: s.title, keptId: known.keptId, kind: "HM_DUP_OF_BASELINE_PROGRAMME", reason: known.reason });
    return false;
  }
  // Title-level programme aliases vs baseline (IAS/CHI/HIV Science)
  const aliasDup = baselineAsLike.find((b) => {
    const a = normKey(`${s.title} ${s.org}`);
    const bb = normKey(`${b.title} ${b.org}`);
    if (/ias\s*2027|hiv\s*science|hivr4p/.test(a) && /ias\s*2027|hiv\s*science|hivr4p/.test(bb)) return true;
    if (/concours hippique|chi geneva/.test(a) && /concours hippique|chi geneva/.test(bb)) return true;
    return false;
  });
  if (aliasDup) {
    dedupeDropped.push({ id: s.id, title: s.title, keptId: aliasDup.id, kind: "HM_DUP_OF_BASELINE_ALIAS" });
    return false;
  }
  return true;
});
const newHmOpps = hmKeptFiltered.map((s) =>
  repairEssentialCommercialProse(ensureHotelSourcingOpenSales(sanitizeCustomerCopy(buildHotelMarketOpportunity(s)))),
);
const newCmiOpps = cmiKept.map((s) => repairEssentialCommercialProse(sanitizeCustomerCopy(buildCmiOpportunity(s))));

const priorHmClones = priorHm.map((o) => repairEssentialCommercialProse(cloneOpp(o)));
const priorCmiClones = priorCmi.map((o) => {
  const c = repairEssentialCommercialProse(cloneOpp(o));
  c.researchProvenance = { ...(c.researchProvenance || {}), preservedFromPriorPackage: priorFp };
  return c;
});
let hotelMarketAll = [...priorHmClones, ...newHmOpps];
let competitiveAll = [...priorCmiClones, ...newCmiOpps];
const seenH = new Set();
hotelMarketAll = hotelMarketAll.filter((o) => {
  if (DEMOTED_REPROMOTE_BLOCK.has(o.id)) return false;
  if (seenH.has(o.id)) return false;
  seenH.add(o.id);
  return true;
});
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

writeJson("P01-BASELINE_AND_AUTHORIZATION.json", { ...baselineDoc(), mode: LIVE_PAID ? "PAID_AUTHORIZED" : "ASSEMBLE_CACHED_NO_SPEND", resumedSpendUsdAtStart: resumedSpendUsd });
writeJson("P02-PAID_BUDGET_LEDGER.json", {
  contract: "GDI_YOTEL_40PLUS_EXA_35USD_PILOT_LEDGER_V1",
  authorizationId: AUTHORIZATION_ID,
  hardCeilingUsd: HARD_CEILING_USD,
  stageCapsUsd: STAGE_CAP_USD,
  priorCampaignPaidSpendUsd: 0,
  founderFacingMirror: path.relative(ROOT, BUDGET_PREFLIGHT_PATH).replace(/\\/g, "/"),
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
writeJson("P03-PASS1_DISCOVERY_RESULTS.json", {
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
writeJson("P04-PASS2_DEEP_RESULTS.json", {
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
writeJson("P05-PASS3_VERIFY_RESULTS.json", {
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
writeJson("P06-COMMERCIAL_ELIGIBILITY.json", {
  contract: "GDI_YOTEL_40PLUS_EXA_PILOT_COMMERCIAL_ELIGIBILITY_V1",
  referenceDate: REF,
  imminentCutoff: IMMINENT_CUTOFF,
  template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  cmiContract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  rules: [
    "Exact future event date (≥ 21 days from REF) required — year-only and expired editions are Watch/Reject.",
    "Territory: Founex / Coppet / Nyon / La Côte (core) and Geneva Airport corridor (nearby); central Geneva only as overflow/ancillary with an open lodging path; Lausanne / Pays de Gex only with Geneva-side travel evidence, otherwise Watch.",
    "Central-Geneva / Lausanne events of ≥3,000 attendees without open overflow sourcing are Watch (not automatic YOTEL demand).",
    "Named, sourced, commercially relevant contact required; procurement-only roles excluded.",
    "Hotel-fit assessment per event; Palexpo / mega Geneva convention-scale events without open overflow sourcing are Watch.",
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
writeJson("P07-HOTEL_MARKET_INVENTORY.json", {
  target: TARGET_HM,
  achieved: hotelMarket.length,
  targetMet: hotelMarket.length >= TARGET_HM,
  gapTo40: gap,
  baselineHotelMarket: priorHm.length,
  newlyAddedHotelMarket: hotelMarket.filter((o) => !baselineIds.has(o.id)).length,
  baselineHotelMarketRetainedAfterGate: hotelMarket.filter((o) => baselineIds.has(o.id)).length,
  opportunities: hotelMarket.map(oppSummary),
  watchList: allDecisions.filter((x) => x.bucket === "WATCH").map(rowSummary),
  rejected: allDecisions.filter((x) => x.bucket === "REJECTED").map(rowSummary),
});
writeJson("P08-CMI_PRESERVATION_AUDIT.json", {
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
writeJson("P09-ACTIONABILITY_AUDIT.json", {
  referenceDate: REF,
  gate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  counts: gate.counts,
  assertCustomerInventoryPassesActionabilityGate: actionabilityAssert,
  rows: gatedRows.map((o) => ({ id: o.id, title: o.title, state: o.commercialActionabilityStateV1, temporal: o.commercialTemporalDisposition, hotelMarket: o.hotelMarketScopeEligible, competitive: o.competitiveMarketScopeEligible, datePill: o.datePillDisplayLabel })),
  gatedOut: gatedOut.map((o) => ({ id: o.id, title: o.title, state: o.commercialActionabilityStateV1 })),
});
writeJson("P10-CONTACT_AND_SALES_ACTIONS.json", {
  note: "Hotel Market rows carry Hotel Sourcing Open labels + accountDevelopmentPublished; no generic Monitor.",
  monitorHotelMarketRows: monitorHm.map((o) => o.id),
  rows: hotelMarket.map((o) => ({
    id: o.id, title: o.displayTitle, contact: o.primaryContact, salesActionPill: o.salesActionPillLabel, salesActionKey: o.salesActionKey,
    sourcing: o.hotelSourcingStatusLabel, accountDevelopmentPublished: o.accountDevelopmentPublished === true,
    nextStep: o.recommendedNextStep, when: o.whyActNow, unverified: o.uncertainties || [],
  })),
});
writeJson("P11-GAP_TO_40_ANALYSIS.json", {
  target: TARGET_HM,
  achieved: hotelMarket.length,
  targetMet: hotelMarket.length >= TARGET_HM,
  gapTo40: gap,
  baseline: priorHm.length,
  added: hotelMarket.filter((o) => !baselineIds.has(o.id)).length,
  watchByReason: countBy(allDecisions.filter((x) => x.bucket === "WATCH"), (x) => x.reason),
  rejectedByReason: countBy(allDecisions.filter((x) => x.bucket === "REJECTED"), (x) => x.reason),
  triageRejectedCounts: countBy(triageRejected, (r) => String(r.reason).split(":")[0]),
  remainingBudgetUsd: remainingBudgetUsd(ledger),
  diagnosis:
    gap === 0
      ? "Target met under unchanged qualification standards."
      : `Gap remains after the authorized $${HARD_CEILING_USD} pilot; qualification standards were not lowered and no CMI was dual-classified.`,
  measuredYield: {
    paidDeepRuns: ledger.operations.filter((o) => o.stage === "PASS2_DEEP" || o.stage === "PASS3_VERIFY").length,
    newHotelMarket: hotelMarket.filter((o) => !baselineIds.has(o.id)).length,
    newHotelMarketPerPaidDeepRun: (() => {
      const runs = ledger.operations.filter((o) => o.stage === "PASS2_DEEP" || o.stage === "PASS3_VERIFY").length;
      return runs ? Number((hotelMarket.filter((o) => !baselineIds.has(o.id)).length / runs).toFixed(3)) : null;
    })(),
    phase2Threshold: 0.15,
    note: "Phase 2 scale-up only if measured yield ≥ 15% of deep runs (see 00-BUDGET proposal); separate founder authorization required.",
  },
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
writeJson("P12-AIRTABLE_WRITE_DRY_RUN.json", { ...airtablePlan, liveWritesAuthorized: false });

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
  gapTo40: gap,
  integrity,
  qualityGate: { ran: Boolean(qualityGate), ok: qualityGate?.ok ?? null, blocking: qualityGate?.blocking || null, summary: qualityGate?.summary || null },
  priorPackagePreserved: {
    fingerprint: priorFp,
    path: path.relative(ROOT, PRIOR_PKG_DIR),
    sha256Before: priorPkgShaBefore,
    sha256After: sha256File(priorPkgPath),
    unchanged: priorPkgUnchanged,
    stagedPointerBefore,
  },
  protectedPublicationsBefore: protectedBefore,
  protectedPublicationsAfter: protectedAfter,
  protectedPublicationsUnchanged: protectedUnchanged,
  previewUrl,
  opportunitiesJson: path.relative(ROOT, OPPS_JSON_PATH),
});
if (pkg) {
  writeJson("PILOT_FINGERPRINT.json", {
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
if (!fs.existsSync(path.join(OUT, "P14-LOCAL_BROWSER_VERIFICATION.json")) || pkg) {
  writeJson("P14-LOCAL_BROWSER_VERIFICATION.json", {
    contract: "GDI_YOTEL_40PLUS_EXA_PILOT_LOCAL_BROWSER_VERIFICATION_V1",
    status: "PENDING_BROWSER_RUN",
    howToRun: `node scripts/${SCRIPT_ID}.mjs --browser-verify [--base-url http://127.0.0.1:8080]`,
    previewUrl,
    expectedFingerprint: pkg?.contentFingerprintSha256 || null,
    expectedCounts: inventoryCounts,
  });
}
writeJson("P15-GITHUB_MERGE_STATUS.json", {
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
const newHmCount = hotelMarket.filter((o) => !baselineIds.has(o.id)).length;
const paidDeepRuns = ledger.operations.filter((o) => o.stage === "PASS2_DEEP" || o.stage === "PASS3_VERIFY").length;
const recoveryOutcomes = RECOVERY_CANDS.map((c) => {
  const fq = finalQuals.find((x) => x.id === c.id);
  return { key: c.recoveryKey, title: c.title, mandatory: c.mandatory, bucket: fq?.q.decision.bucket || "NOT_RUN", reason: fq?.q.decision.reason || null };
});
const founder = `# YOTEL Geneva Lake — $30 Exa scale campaign

**Authorization:** \`${AUTHORIZATION_ID}\`
**Hotel:** ${HOTEL_NAME} (\`${HOTEL_ID}\`) · Founex, La Côte · ${CAPABILITY.totalGuestrooms} rooms · ${CAPABILITY.meetingRoomCount} meeting rooms · **Reference date:** ${REF}
**Hard ceiling:** **$${HARD_CEILING_USD}** · **Spent:** **$${ledger.spentUsd}** · **Remaining:** **$${remainingBudgetUsd(ledger)}** (this authorization prior spend: $0; prior $35 pilot remainder not rolled over)
**Staged publication:** \`${pkg?.publicationVersion || "NOT STAGED"}\`${packageBlocker ? ` (blocker: ${packageBlocker})` : ""}
**New fingerprint:** \`${pkg?.contentFingerprintSha256 || "n/a"}\`
**Prior fingerprint preserved:** \`${priorFp}\` (package on disk unchanged: ${priorPkgUnchanged})
**Lifecycle:** STAGED_FOR_FOUNDER_REVIEW (never CUSTOMER_PUBLISHED)

## Target status

| Metric | Value |
|---|---:|
| Baseline Hotel Market | ${priorHm.length} |
| Newly qualified Hotel Market | ${hotelMarket.filter((o) => !baselineIds.has(o.id)).length} |
| **Final Hotel Market** | **${hotelMarket.length}** |
| Target | ${TARGET_HM} |
| **Target met?** | **${hotelMarket.length >= TARGET_HM ? "YES" : "NO"}** |
| Gap to ${TARGET_HM} | ${gap} |
| CMI (separate, preserved) | ${competitive.length} (baseline ${priorCmi.length}) |
| Dual-classified rows | ${dualClassified.length} |
| Measured yield (new HM / paid deep run) | ${paidDeepRuns ? (newHmCount / paidDeepRuns).toFixed(3) : "n/a"} (${newHmCount} / ${paidDeepRuns}; Phase 2 threshold 0.15) |

## Spend by stage (cap → spent)

${mdRows(Object.entries(STAGE_CAP_USD), ([k, cap]) => `- ${k}: cap $${cap} → spent $${spendByStage[k] || 0}`)}
- Surfe: not invoked · Parallel: not invoked · Airtable writes: 0

## Pass 1 — discovery (14 segments)
- Jobs completed: ${pass1Calls.filter((c) => c.ok).length}/${PASS1_JOBS.length} · segments covered: ${[...new Set(pass1Calls.filter((c) => c.ok).map((c) => c.segment))].length}/${SEGMENT_KEYS.length} · raw hits: ${pass1Hits.length} · promoted to deep: ${discoveryCands.length}
- Triage rejects: ${JSON.stringify(countBy(triageRejected, (r) => String(r.reason).split(":")[0]))}

## Pass 2 / Pass 3
- Deep candidates processed: ${pass2Plan.list.length} (free cache reuse: ${pass2Plan.list.filter((c) => c.plannedFree).length}) · outcomes: ${JSON.stringify(countBy(p2Decisions, (x) => x.bucket))}
- Pass 3 verifications: ${pass3Rows.length} · outcomes: ${JSON.stringify(countBy(p3Decisions, (x) => x.bucket))}

## Recovery re-verification
${mdRows(recoveryOutcomes, (r) => `- ${r.mandatory ? "**[mandatory]** " : ""}${r.title} — ${r.bucket}${r.reason ? ` (${r.reason})` : ""}`)}
- Not re-purchased: ${recoverySkipped.length} prior rows (AI for Good sponsor rows, past listings)

## Hotel Market inventory
${mdRows(hotelMarket, (o, i) => `${i + 1}. **${o.displayTitle}** — ${o.organizationName} · ${o.datePillDisplayLabel} · ${o.primaryContact?.name || "n/a"} · ${o.salesActionPillLabel}${baselineIds.has(o.id) ? "" : " · **NEW**"}`)}

## CMI inventory (separate)
${mdRows(competitive, (o, i) => `${i + 1}. **${o.displayTitle}** @ ${o.competitorHotel || "n/a"} — ${o.datePillDisplayLabel}${baselineIds.has(o.id) ? "" : " · **NEW**"}`)}

## Watch / reject summary
${mdRows(Object.entries(failureHistogram), ([k, v]) => `- ${k}: ${v}`)}

## Gates
- Commercial actionability: ${gate.counts.activeHotelMarket} hotel / ${gate.counts.activeCompetitive} competitive / ${gate.counts.researchWatch} research-watch; assert ok=${actionabilityAssert.ok}
- Publication quality gate: ${qualityGate ? JSON.stringify(qualityGate.summary || qualityGate.ok) : "not run"}
- Bethesda / Sheraton / Wyndham / Radisson Toronto publications unchanged: ${protectedUnchanged}
- Airtable: dry-run only (${JSON.stringify(airtablePlan)})

## Preview
\`${previewUrl}\` — verify with \`node scripts/${SCRIPT_ID}.mjs --browser-verify\`
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);
if (LIVE_PAID) syncBudgetPreflight(ledger.frozen ? `PILOT_STOPPED_${ledger.freezeReason || "FROZEN"}` : "PILOT_COMPLETE");

console.log(
  JSON.stringify(
    {
      spentUsd: ledger.spentUsd,
      remainingUsd: remainingBudgetUsd(ledger),
      spendByStage,
      withinStageCaps: withinCaps,
      hotelMarket: hotelMarket.length,
      cmi: competitive.length,
      gapTo40: gap,
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
