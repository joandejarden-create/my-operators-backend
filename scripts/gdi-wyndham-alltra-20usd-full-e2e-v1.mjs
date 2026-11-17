#!/usr/bin/env node
/**
 * Wyndham Alltra Punta Cana — Full E2E with a HARD $20 incremental paid Exa ceiling.
 *
 * Authorization: FOUNDER_APPROVE_WYNDHAM_ALLTRA_PUNTA_CANA_PAID_FULL_E2E_20USD_20261114
 *   - Exa only (no Surfe auto, no Parallel). Max $20 incremental.
 *   - No CUSTOMER_PUBLISHED. No Bethesda / Sheraton publication mutation.
 *   - Airtable dry-run only.
 *
 * Modes:
 *   node scripts/gdi-wyndham-alltra-20usd-full-e2e-v1.mjs                  # zero-cost reconcile only
 *   node scripts/gdi-wyndham-alltra-20usd-full-e2e-v1.mjs --authorize-paid # zero-cost + paid Exa + stage package
 *
 * Re-runs resume the on-disk ledger and reuse completed raw runs (never re-pay a stored run).
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import os from "node:os";
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
  COMMERCIAL_ACTIONABILITY_STATE,
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
import { GDI_FULL_E2E_HOTEL_TEMPLATE_V1 } from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
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
import { createExaLiveDiscoveryProvider } from "../lib/group-demand-intelligence/research/exa-agent/exa-live-discovery-provider-bridge-v1.js";
import { buildExaEventsAgentCreatePayload } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-events-contracts-v1.js";
import { resolveExaAgentRoutingForSegment } from "../lib/group-demand-intelligence/research/exa-agent/gdi-segment-aware-exa-agent-routing-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recXh4NgF8sDbi9cQ";
const FORBIDDEN_DUP_ID = "reccax9nT5J90Sc0Y";
const HOTEL_NAME = "Wyndham Alltra Punta Cana";
const REF = "2026-11-14";
const HARD_CEILING_USD = 20;
const AUTHORIZATION_ID =
  "FOUNDER_APPROVE_WYNDHAM_ALLTRA_PUNTA_CANA_PAID_FULL_E2E_20USD_20261114";
const SCRIPT_ID = "gdi-wyndham-alltra-20usd-full-e2e-v1";
const ASSEMBLE_TEST = process.argv.includes("--assemble-test");
const OUT = ASSEMBLE_TEST
  ? path.join(os.tmpdir(), "wy-assemble-test")
  : path.join(ROOT, "reports", SCRIPT_ID);
const HOTEL_DATA = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
);
const ORS_DIR = path.join(HOTEL_DATA, "opportunity-research-state");
const SEG_DIR = path.join(HOTEL_DATA, "full-e2e-fourteen-segment-state");
const LEDGER_PATH = path.join(OUT, "paid-budget-ledger.json");
const RAW_DIR = path.join(OUT, "raw-runs");
const ARGS = new Set(process.argv.slice(2));
const AUTHORIZE = ARGS.has("--authorize-paid");
const PUB_VERSION = `gdi_pub_${HOTEL_ID}_20261114_wyndham_alltra_20usd_full_e2e_v1`;

/** Per-stage USD caps inside the single $20 hard ceiling. */
const STAGE_CAP_USD = {
  DISCOVERY: 2.4,
  CMI_DISCOVERY: 0.8,
  CMI_DEEP: 5.0,
  DEEP_RESEARCH: HARD_CEILING_USD, // bounded only by the global ceiling
};
const MAX_HOTEL_DEEP_RUNS = 30;
const MAX_CMI_DEEP_RUNS = 10;
const STOP_AFTER_CONSECUTIVE_NO_YIELD = 10;

const HOTEL = {
  hotelId: HOTEL_ID,
  displayName: HOTEL_NAME,
  name: HOTEL_NAME,
  city: "Punta Cana",
  country: "Dominican Republic",
  demandTerritory: {
    label:
      "Punta Cana / Uvero Alto / Bávaro — Dominican Republic all-inclusive incentives, weddings, DMC and leisure groups, sports",
  },
  rooms: 620,
  positioning:
    "620-room all-inclusive resort on Uvero Alto Beach; 3 meeting venues (380 m²), ~300 conference / 200 banquet, outdoor event space, 20 F&B venues; Aimbridge LATAM managed",
};

const HOTEL_FIT_COPY =
  "Wyndham Alltra Punta Cana is a 620-room all-inclusive resort on Uvero Alto Beach with three meeting venues (≈380 m², up to ~300 conference / ~200 banquet), outdoor event space and 20 food-and-beverage venues — a credible single-base option for destination incentive, wedding, association and leisure groups that want an all-inclusive format.";

const SEGMENTS = [
  "corporate_meetings_incentives",
  "social_special_events",
  "travel_trade_group_intermediaries",
  "sports_entertainment",
  "associations_conferences_events",
  "corporate_celebrations_private_events",
  "professional_event_planners",
  "corporate_accounts_scaling",
];

/** EN + ES discovery queries per segment (≤240 chars each; geography embedded). */
const GEO_EN =
  "Punta Cana OR Uvero Alto OR Bávaro Dominican Republic all-inclusive resort";
const GEO_ES =
  "Punta Cana, Uvero Alto o Bávaro, República Dominicana, resort todo incluido";
const SEGMENT_QUERIES = {
  corporate_meetings_incentives: {
    en: [
      `Corporate incentive trips, sales conventions and reward programs 2027 2028 ${GEO_EN} companies planning group travel`,
      `DMC-run corporate incentive group programs 2027 2028 Dominican Republic Punta Cana named companies hotel sourcing`,
      `Annual company retreat OR kickoff OR President's Club Punta Cana 2027 2028 future edition`,
    ],
    es: [
      `viaje de incentivo corporativo, convención de ventas o premio a vendedores Punta Cana República Dominicana 2027 2028 empresa`,
      `programa de incentivos corporativos DMC Punta Cana 2027 2028 empresas grupos hoteles todo incluido`,
      `reunión anual de empresa, retiro corporativo o kickoff en Punta Cana 2027 2028 próxima edición`,
    ],
  },
  social_special_events: {
    en: [
      `Destination wedding groups, wedding expos and reunions ${GEO_EN} 2027 2028 named wedding company or planner`,
      `Large family reunion OR milestone celebration OR quinceañera group Punta Cana 2027 2028 organizer`,
      `Destination wedding showcase or wedding planner annual event Punta Cana Dominican Republic 2027 hotel partners`,
    ],
    es: [
      `bodas destino, expo bodas y reuniones familiares ${GEO_ES} 2027 2028 organizador o wedding planner`,
      `reunión familiar grande, aniversario o celebración especial grupo Punta Cana 2027 2028 organizador`,
      `evento anual de wedding planners o expo de bodas destino República Dominicana 2027 hoteles`,
    ],
  },
  travel_trade_group_intermediaries: {
    en: [
      `Group travel organizers, tour operators, wholesalers and DMCs booking all-inclusive group blocks Punta Cana 2027 2028`,
      `Leisure group buyout OR multi-room group program Punta Cana Uvero Alto 2027 named travel company OR club OR church group OR alumni`,
      `Travel trade familiarization OR sales mission OR group travel summit Dominican Republic 2027 2028 hotels all-inclusive`,
    ],
    es: [
      `organizadores de viajes en grupo, operadores turísticos, mayoristas y DMC bloques todo incluido Punta Cana 2027 2028`,
      `grupo de ocio buyout o programa multi-habitación Punta Cana Uvero Alto 2027 agencia o club o grupo`,
      `viaje de familiarización o misión de ventas o cumbre de turismo grupal República Dominicana 2027 2028 hoteles`,
    ],
  },
  sports_entertainment: {
    en: [
      `Sports tournaments, training camps, festivals and entertainment events in ${GEO_EN} 2027 2028 organizer lodging teams fans`,
      `Youth OR amateur baseball OR volleyball OR soccer OR golf tournament Punta Cana 2027 2028 hotel accommodation packages`,
      `Music festival OR wellness festival OR fitness retreat Punta Cana 2027 2028 destination organizer hotel partner`,
    ],
    es: [
      `torneos deportivos, campamentos, festivales y eventos de entretenimiento Punta Cana 2027 2028 organizador alojamiento equipos`,
      `torneo juvenil o amateur de béisbol, voleibol, fútbol o golf en Punta Cana 2027 2028 paquetes de hotel`,
      `festival de música o bienestar o retiro de fitness Punta Cana 2027 2028 organizador hotel`,
    ],
  },
  associations_conferences_events: {
    en: [
      `Association conferences, congresses and annual meetings in Punta Cana OR Dominican Republic 2027 2028 official event site dates host hotel`,
      `Medical OR professional OR trade association destination meeting Punta Cana 2027 2028 attendees hotel`,
      `Regional OR international congress Dominican Republic east coast all-inclusive resort 2027 2028 organizing committee`,
    ],
    es: [
      `congresos, convenciones y asambleas de asociaciones en Punta Cana o República Dominicana 2027 2028 sitio oficial fechas hotel sede`,
      `congreso médico, profesional o gremial en Punta Cana 2027 2028 asistentes hotel sede`,
      `congreso regional o internacional República Dominicana Punta Cana resort todo incluido 2027 2028 comité organizador`,
    ],
  },
  corporate_celebrations_private_events: {
    en: [
      `Corporate anniversary galas, awards nights and private company celebrations Punta Cana Dominican Republic 2027 2028 organizer`,
      `Company-wide celebration OR customer appreciation trip OR dealer meeting Punta Cana 2027 2028 named company`,
      `Private corporate events OR family-office retreats at Dominican Republic all-inclusive resorts 2027 2028`,
    ],
    es: [
      `gala de aniversario corporativo, premiaciones y celebraciones privadas de empresas Punta Cana 2027 2028 organizador`,
      `celebración de toda la empresa, viaje de agradecimiento a clientes o reunión de distribuidores Punta Cana 2027 2028`,
      `eventos privados corporativos y retiros en resorts todo incluido República Dominicana 2027 2028`,
    ],
  },
  professional_event_planners: {
    en: [
      `Professional event planners, PCOs and DMCs in Punta Cana and Dominican Republic managing group events 2027 2028 named firm hotel selection`,
      `Meeting planner OR incentive house organizing Punta Cana program 2027 2028 site selection Dominican Republic`,
      `Destination wedding and corporate event production companies Punta Cana named principals hotel partnerships`,
    ],
    es: [
      `planificadores profesionales de eventos, PCO y DMC en Punta Cana y República Dominicana grupos 2027 2028 empresa selección de hotel`,
      `organizador de reuniones o casa de incentivos programa Punta Cana 2027 2028 selección de sede`,
      `empresas de producción de bodas destino y eventos corporativos Punta Cana socios hoteleros`,
    ],
  },
  corporate_accounts_scaling: {
    en: [
      `Companies expanding operations in Punta Cana OR Dominican Republic east region 2026 2027 with visiting teams training travel hotel needs`,
      `Free trade zone OR resort developer OR airline expansion Punta Cana 2027 corporate travel meetings procurement`,
      `Multinational opening Dominican Republic office OR contact center 2026 2027 staff training offsite lodging`,
    ],
    es: [
      `empresas en expansión en Punta Cana o región este de República Dominicana 2026 2027 equipos visitantes capacitación hospedaje`,
      `zona franca, desarrolladores turísticos o aerolíneas en expansión Punta Cana 2027 viajes corporativos reuniones`,
      `multinacional abre oficina o centro de contacto en República Dominicana 2026 2027 capacitación alojamiento`,
    ],
  },
};

/**
 * CMI Stage 4a — competing Punta Cana / Bávaro / Cap Cana / Uvero Alto AI resorts.
 * Competitive-set membership ≠ automatic CMI opportunity membership.
 */
const COMPETITIVE_CLUSTERS = [
  {
    id: "uvero_alto_macao_miches",
    label: "Uvero Alto / Macao / Miches — direct beachfront AI peers",
    inclusion: "PRIMARY_COMPSET",
    hotels: [
      "Excellence Punta Cana",
      "Excellence El Carmen",
      "Dreams Macao Beach Punta Cana",
      "Zoetry Agua Punta Cana",
      "Majestic Elegance Punta Cana",
      "Majestic Mirage Punta Cana",
      "Iberostar Waves Dominicana",
      "Secrets Cap Cana",
      "Nickelodeon Hotels & Resorts Punta Cana",
      "Club Med Punta Cana",
    ],
  },
  {
    id: "bavaro_punta_cana",
    label: "Bávaro / Punta Cana large AI resorts",
    inclusion: "PRIMARY_COMPSET",
    hotels: [
      "Barceló Bávaro Grand Resort",
      "Hard Rock Hotel & Casino Punta Cana",
      "Lopesan Costa Bávaro",
      "TRS Turquesa",
      "Grand Palladium Punta Cana",
      "Meliá Punta Cana Beach Resort",
      "Paradisus Palma Real",
      "Royalton Punta Cana",
      "Riu Palace Punta Cana",
      "Bahia Principe Grand Bávaro",
    ],
  },
  {
    id: "cap_cana_puntacana_resort",
    label: "Cap Cana / Puntacana Resort — competitive alternatives (not automatic Alltra demand)",
    inclusion: "SECONDARY_COMPSET",
    hotels: [
      "Eden Roc Cap Cana",
      "Sanctuary Cap Cana",
      "Hyatt Ziva Cap Cana",
      "Hyatt Zilara Cap Cana",
      "Margaritaville Island Reserve Cap Cana",
      "Moon Palace Punta Cana",
      "The Westin Puntacana Resort & Club",
      "Tortuga Bay Puntacana",
    ],
  },
];

const COMPETITOR_REGEX =
  /hard rock|barcel[oó]|iberostar|excellence|secrets|dreams|zoetry|hyatt|meli[aá]|paradisus|majestic|royalton|riu\b|lopesan|trs\b|eden roc|sanctuary|margaritaville|breathless|nickelodeon|moon palace|palace resort|club med|occidental|palladium|bahia principe|westin|\bw punta cana|tortuga bay|puntacana resort|four points|sheraton|hilton|marriott|ritz|live aqua|sunscape|catalonia|ocean (?:blue|el faro)|sirenis|impressive|be live|viva (?:dominicus|wyndham)|dominicus|sensatori|now (?:onyx|larimar)|jewel|zilara|ziva|ritz-carlton|luxury collection|casa mirall[aá]|yanuna/i;

const HOTEL_LIKE_ORG_REGEX =
  /\b(?:hotel|hotels|resort|resorts|hospitality|hyatt|marriott|hilton|iberostar|barcel[oó]|meli[aá]|riu|secrets|dreams|excellence|palladium|royalton|majestic|lopesan|bahia principe|sheraton|westin|aimbridge|wyndham)\b/i;

const MONTHS = {
  jan: 1, january: 1, enero: 1, ene: 1,
  feb: 2, february: 2, febrero: 2,
  mar: 3, march: 3, marzo: 3,
  apr: 4, april: 4, abril: 4, abr: 4,
  may: 5, mayo: 5,
  jun: 6, june: 6, junio: 6,
  jul: 7, july: 7, julio: 7,
  aug: 8, august: 8, agosto: 8, ago: 8,
  sep: 9, sept: 9, september: 9, septiembre: 9, setiembre: 9,
  oct: 10, october: 10, octubre: 10,
  nov: 11, november: 11, noviembre: 11,
  dec: 12, december: 12, diciembre: 12, dic: 12,
};
const MONTH_RE =
  "jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre";

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8").replace(/^\uFEFF/, ""));
}
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
function normKey(s) {
  return text(s)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function hashId(prefix, a, b) {
  const h = crypto
    .createHash("sha256")
    .update(`${normKey(a)}|${normKey(b)}`)
    .digest("hex")
    .slice(0, 12);
  return `${prefix}_${h}`;
}
function iso(y, m, d) {
  return `${String(y).padStart(4, "0")}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}
function parseIso(s) {
  const m = text(s).match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}
function monthNum(w) {
  return MONTHS[String(w || "").toLowerCase().replace(/\.$/, "")] || null;
}

/** Extract explicit calendar dates (start of ranges) from text / URLs. */
function extractExplicitDates(blob) {
  const s = String(blob || "");
  const out = [];
  const push = (y, m, d) => {
    if (!y || !m || !d) return;
    if (m < 1 || m > 12 || d < 1 || d > 31) return;
    out.push(iso(y, m, d));
  };
  for (const m of s.matchAll(/\b(20\d{2})-(\d{2})-(\d{2})\b/g)) {
    push(Number(m[1]), Number(m[2]), Number(m[3]));
  }
  const reEn = new RegExp(
    `\\b(${MONTH_RE})\\.?\\s+(\\d{1,2})(?:st|nd|rd|th)?(?:\\s*(?:–|-|to)\\s*(?:(?:${MONTH_RE})\\.?\\s+)?\\d{1,2})?,?\\s+(20\\d{2})`,
    "gi",
  );
  for (const m of s.matchAll(reEn)) push(Number(m[3]), monthNum(m[1]), Number(m[2]));
  const reEn2 = new RegExp(
    `\\b(\\d{1,2})(?:\\s*(?:–|-)\\s*\\d{1,2})?\\s+(${MONTH_RE})\\.?,?\\s+(20\\d{2})`,
    "gi",
  );
  for (const m of s.matchAll(reEn2)) push(Number(m[3]), monthNum(m[2]), Number(m[1]));
  const reEs = new RegExp(
    `\\b(\\d{1,2})(?:\\s*(?:–|-|al|y)\\s*\\d{1,2})?\\s+de\\s+(${MONTH_RE})\\s+(?:de\\s+)?(20\\d{2})`,
    "gi",
  );
  for (const m of s.matchAll(reEs)) push(Number(m[3]), monthNum(m[2]), Number(m[1]));
  const reUrl = new RegExp(`(${MONTH_RE})-(\\d{1,2})(?:-\\d{1,2})?-(20\\d{2})`, "gi");
  for (const m of s.matchAll(reUrl)) push(Number(m[3]), monthNum(m[1]), Number(m[2]));
  return [...new Set(out)].sort();
}
function maxYearIn(s) {
  const ys = [...String(s || "").matchAll(/\b(20[2-3]\d)\b/g)].map((m) => Number(m[1]));
  return ys.length ? Math.max(...ys) : null;
}
function dateLabel(start, end) {
  if (!start) return "Dates TBD";
  const f = (d) =>
    new Date(`${d}T12:00:00Z`).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      timeZone: "UTC",
    });
  if (end && end !== start) return `${f(start)} – ${f(end)}`;
  return f(start);
}

const HOTEL_CFG = readJson(
  path.join(ROOT, "config/group-demand-intelligence/hotels", `${HOTEL_ID}.json`),
);
const TERRITORY_KW = HOTEL_CFG.demandTerritory?.classificationKeywords || {};
function territoryOf(blob) {
  const b = ` ${normKey(blob)} `;
  const has = (arr) =>
    (arr || []).some((k) => {
      const nk = normKey(k);
      return nk && b.includes(` ${nk} `);
    });
  const coreKw = [...(TERRITORY_KW.TERRITORY_CORE || []), "uvero alto"];
  const nearKw = [...(TERRITORY_KW.TERRITORY_NEARBY || []), "puntacana", "bavaro", "veron", "higuey", "la altagracia"];
  if (has(coreKw)) return "TERRITORY_CORE";
  if (has(TERRITORY_KW.TERRITORY_COMPETITIVE)) return "TERRITORY_COMPETITIVE";
  if (has(nearKw)) return "TERRITORY_NEARBY";
  if (has(TERRITORY_KW.TERRITORY_STRETCH)) return "TERRITORY_STRETCH";
  if (/dominican|republica dominicana/.test(normKey(blob))) return "TERRITORY_STRETCH";
  return "OUTSIDE_REALISTIC_TERRITORY";
}
const HOTEL_MARKET_TERRITORIES = new Set(["TERRITORY_CORE", "TERRITORY_NEARBY"]);
/** Events starting inside 21 days cannot realistically be sourced — excluded, not padded. */
const IMMINENT_CUTOFF = new Date(Date.parse(`${REF}T12:00:00Z`) + 21 * 86400000)
  .toISOString()
  .slice(0, 10);

/** Contact role classification — procurement-only roles are not event buyers. */
function classifyContactRole(title, org = "") {
  const t = normKey(`${title}`);
  if (!t) return "NO_ROLE";
  const procurement =
    /procure|purchas|compras|contrataciones|buyer|supply|suministros|almacen|facility|logistics and purchasing|purchasing and logistics/.test(t);
  const eventish =
    /event|evento|meeting|reunion|travel|viaje|incentive|tournament|torneo|convention|congress|congreso|conference host|organizer|organizador|producer|wedding|boda/.test(t);
  if (procurement && !eventish) return "PROCUREMENT_ONLY";
  const commercial =
    /event|evento|meeting|reunion|travel|viaje|incentive|sales|ventas|commercial|comercial|organizer|organizador|tournament|producer|program|project manager|proyecto|president|presidente|founder|fundador|ceo|owner|coordinator|coordinador|executive director|director ejecutivo|chair|secretary|secretar|chief operating|\bcoo\b|vice president|vicepresident|hospitality|host|committee|comite|director/.test(
      t,
    );
  if (commercial) return "COMMERCIALLY_RELEVANT";
  return "ROLE_UNCLEAR";
}
function contactIsIntermediary(title, org) {
  return /\b(dmc|pco|planner|wedding|incentive|tour operator|travel agency|wholesaler|events? (?:company|agency)|agency|production)\b/i.test(
    `${title} ${org}`,
  );
}

function revenueStreamFor(potential) {
  return /catering|banquet|meeting|conference|training|congress|convention|seminar/i.test(
    potential || "",
  )
    ? "COMBINED_GUESTROOMS_EVENT_FNB"
    : "GUESTROOMS_ONLY";
}

// ---------------------------------------------------------------------------
// STAGE 0 — ZERO-COST BASELINE
// ---------------------------------------------------------------------------
fs.mkdirSync(OUT, { recursive: true });
fs.mkdirSync(RAW_DIR, { recursive: true });

if (HOTEL_ID === FORBIDDEN_DUP_ID) {
  console.error("Forbidden duplicate HPC id");
  process.exit(3);
}

const orsFiles = fs.readdirSync(ORS_DIR).filter((f) => f.endsWith(".json")).sort();
const ors = orsFiles.map((f) => ({ file: f, ...readJson(path.join(ORS_DIR, f)) }));
const dossiers = ors.filter((d) => !String(d.id || d.opportunityId || d.file).startsWith("aimbridge_"));
const seeds = ors.filter((d) => String(d.id || d.opportunityId || d.file).startsWith("aimbridge_"));

const segFiles = fs.readdirSync(SEG_DIR).filter((f) => f.endsWith(".json")).sort();
const segDocs = segFiles.map((f) => readJson(path.join(SEG_DIR, f)));
const segCandidateUnion = new Set();
for (const s of segDocs) for (const id of s.candidateIds || []) segCandidateUnion.add(String(id));

const priorHotelOppsPath = path.join(HOTEL_DATA, "opportunities.json");
const priorOppsFileExisted = fs.existsSync(priorHotelOppsPath);

function countBy(arr, fn) {
  const m = {};
  for (const x of arr) {
    const k = fn(x) ?? "(none)";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

const baseline = {
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  referenceDate: REF,
  forbiddenDuplicateHpcId: FORBIDDEN_DUP_ID,
  orsDossierCount: ors.length,
  orsEventAndHistDossiers: dossiers.length,
  aimbridgeInventorySeeds: seeds.length,
  segmentStateFiles: segDocs.length,
  segmentCandidateIdUnion: segCandidateUnion.size,
  segmentStatus: segDocs.map((s) => ({
    segmentId: s.segmentId,
    status: s.status,
    completionDecision: s.completionDecision,
    candidateIds: (s.candidateIds || []).length,
    completedCandidateIds: (s.completedCandidateIds || []).length,
    unresolvedCandidateIds: (s.unresolvedCandidateIds || []).length,
  })),
  dossierInternalDisposition: countBy(
    dossiers,
    (d) => d.commercialQualificationV41?.v3?.internalDisposition || d.disposition || null,
  ),
  dossierActivityStatus: countBy(dossiers, (d) => d.activityStatus || null),
  dossiersWithVerifiedNamedContactFlag: dossiers.filter((d) => d.hasVerifiedNamedRelevantContact).length,
  dossiersWithPrimaryContact: dossiers.filter((d) => d.primaryContact?.name).length,
  seedDispositions: countBy(seeds, (d) => d.disposition || null),
  priorOpportunitiesJsonExisted: priorOppsFileExisted,
  customerVisibleOpportunitiesBeforeRun: 0,
  priorPaidSpendOnThisHotelUsd: dossiers.reduce((a, d) => a + Number(d.researchCostUsd || 0), 0),
  note: "Recorded before any paid spend in this run. $0.10 per dossier figure is historical ORS cost, not part of the $20 ceiling.",
};
writeJson("01-ZERO_COST_BASELINE.json", baseline);

// ----- seeds reconciliation (category channels are NOT prospects) -----
const seedRows = seeds.map((s) => {
  const named = (s.contacts || []).filter((c) => c?.name);
  const ownTeam = named.some((c) =>
    /wyndham alltra|aimbridge/i.test(`${c.organization || ""}`),
  );
  const vendorRep = named.some((c) =>
    /cara global/i.test(`${c.organization || ""}`),
  );
  let decision;
  let reason;
  if (/reject_cap_cana/i.test(s.id || s.file)) {
    decision = "REJECTED_GUARDRAIL";
    reason = "Cap Cana auto-overflow rejected — Cap Cana demand is never assumed to be Alltra demand.";
  } else if (/cvent_need_window/i.test(s.id || s.file)) {
    decision = "NEED_WINDOW_HINT_ONLY";
    reason = "Public venue need-date window is a prioritization hint, not an opportunity.";
  } else if (ownTeam) {
    decision = "PROPERTY_OWN_CHANNEL_NOT_A_PROSPECT";
    reason =
      "Named contact is the hotel's own group-sales lead; this is a capture channel, not an external buyer.";
  } else if (vendorRep) {
    decision = "HOTEL_REPRESENTATION_VENDOR_NOT_A_PROSPECT";
    reason =
      "Named intermediary is the hotel's own UK/EU sales representation — not demand.";
  } else if (/^aimbridge_14seg_/.test(s.id || s.file)) {
    decision = "CATEGORY_CHANNEL_NO_NAMED_BUYER";
    reason = "Segment placeholder seed with no named organization or buyer.";
  } else {
    decision = "CATEGORY_CHANNEL_NO_NAMED_BUYER";
    reason = text(s.adAudit?.gaps?.join(", ")) || "Category channel without a named buyer.";
  }
  return {
    id: s.id || s.opportunityId || s.file,
    title: s.title,
    priorDisposition: s.disposition,
    namedContactStatus: s.namedContactStatus,
    decision,
    promotedToCustomer: false,
    reason,
  };
});
writeJson("03-SEED_RECONCILIATION.json", {
  seedCount: seedRows.length,
  promotedCount: 0,
  rows: seedRows,
  note: "Many seeds are category channels. None promoted: promotion requires an evidence-grounded named external buyer or intermediary with a future/ongoing program.",
});

// ----- zero-cost promotion of ORS dossiers -----
const existingKeys = new Set();
const existingTitleKeys = [];
const existingOrgYear = new Set();
for (const d of dossiers) {
  const title = d.title || d.programmeName || "";
  const org = d.organizationName || "";
  existingKeys.add(normKey(`${title}|${org}`));
  if (normKey(title)) existingTitleKeys.push(normKey(title));
}

function dossierDateEvidence(d) {
  const urls = (d.evidence || []).map((e) => e.url).concat(d.seedUrls || []);
  const stmts = (d.evidence || []).map((e) => e.statement);
  const primary = [d.title, d.programmeName, d.activityHeadline, ...urls].join(" \n ");
  const explicit = extractExplicitDates(primary).filter((x) => x >= REF);
  if (explicit.length) {
    return { kind: "EXACT_FUTURE", startDate: explicit[0], basis: "explicit_date_in_title_headline_or_url" };
  }
  const od = parseIso(d.originalActivityDate);
  const evidenceBlob = [d.title, d.programmeName, d.activityHeadline, ...urls, ...stmts].join(" ");
  if (od && od >= "2027-01-01" && evidenceBlob.includes(od.slice(0, 4))) {
    return { kind: "EXACT_FUTURE", startDate: od, basis: "ors_original_activity_date_2027_plus_year_corroborated" };
  }
  const y = maxYearIn([d.title, d.programmeName].join(" "));
  if (y && y >= 2027) {
    const corroborated = [...urls, ...stmts].some((x) => String(x || "").includes(String(y)));
    if (corroborated) {
      return { kind: "YEAR_VERIFIED_FUTURE", year: y, basis: "title_year_corroborated_by_evidence" };
    }
  }
  return { kind: "NONE", basis: "no_reliable_future_event_date_in_dossier" };
}

function zeroCostGate(d) {
  const fails = [];
  const id = d.id || d.opportunityId;
  const title = text(d.title || d.programmeName);
  const org = text(d.organizationName);
  if (!title) fails.push("NO_EVENT_TITLE");
  if (HOTEL_LIKE_ORG_REGEX.test(org) && !/association|asociaci|sociedad|congress|congreso/i.test(org))
    fails.push("ORG_IS_HOTEL_OR_RESORT_BRAND");
  const v3 = d.commercialQualificationV41?.v3 || {};
  if (["EXCLUDED"].includes(v3.commercialStage) || d.exaAgentResearchDisposition?.disposition === "BLOCKED_IDENTITY_OR_EVIDENCE")
    fails.push("EXCLUDED_OR_IDENTITY_BLOCKED");
  if (d.exaAgentResearchDisposition?.disposition === "NOT_COMMERCIALLY_ATTRACTIVE")
    fails.push("NOT_COMMERCIALLY_ATTRACTIVE");
  if (d.activityStatus === "HISTORICAL_ACCOUNT_DEVELOPMENT") fails.push("HISTORICAL_ACTIVITY");
  const dateEv = dossierDateEvidence(d);
  if (dateEv.kind === "NONE") fails.push("NO_VERIFIED_FUTURE_DATE");
  if (dateEv.kind === "YEAR_VERIFIED_FUTURE") fails.push("YEAR_ONLY_NEEDS_EXACT_DATE");
  if (dateEv.kind === "EXACT_FUTURE") {
    if (dateEv.startDate < IMMINENT_CUTOFF) fails.push("IMMINENT_LT_21_DAYS_NOT_ACTIONABLE");
    const ty = maxYearIn(title);
    if (ty && ty < Number(dateEv.startDate.slice(0, 4))) {
      fails.push("TITLE_YEAR_MISMATCH_NEEDS_VERIFICATION");
    }
  }
  const locBlob = [d.title, d.programmeName, d.organizationName, d.activityLocation, d.localConnection].join(" ");
  const territory = territoryOf(locBlob);
  if (!HOTEL_MARKET_TERRITORIES.has(territory)) fails.push(`TERRITORY_${territory}`);
  // contact
  const cands = [d.primaryContact, d.secondaryContact].filter((c) => c?.name);
  let contact = null;
  let contactRole = "NO_CONTACT";
  for (const c of cands) {
    const role = classifyContactRole(c.title || c.role, c.organization);
    if (role === "COMMERCIALLY_RELEVANT" && text(c.sourceUrl)) {
      contact = c;
      contactRole = role;
      break;
    }
    if (contactRole === "NO_CONTACT") contactRole = role;
  }
  if (!contact) fails.push(`CONTACT_${contactRole}`);
  return { id, title, org, dateEv, territory, contact, fails, contactRole };
}

const zcEvaluations = dossiers.map(zeroCostGate);
const zcPass = zcEvaluations.filter((e) => e.fails.length === 0);
// dedupe same event across disc_/hist_ dossiers: keep best-evidenced
const zcByKey = new Map();
for (const e of zcPass) {
  const startYear = e.dateEv.startDate?.slice(0, 4) || String(e.dateEv.year);
  const key = `${normKey(e.title).slice(0, 40)}|${startYear}`;
  const prev = zcByKey.get(key);
  const score = (x) => (x.contact?.sourceUrl ? 1 : 0) + (x.dateEv.kind === "EXACT_FUTURE" ? 2 : 0) + (x.id.startsWith("disc_") ? 0.5 : 0);
  if (!prev || score(e) > score(prev)) zcByKey.set(key, e);
}
const zcPromotable = [...zcByKey.values()];

function mkSources(d, contact) {
  const seen = new Set();
  const out = [];
  for (const e of d.evidence || []) {
    if (!/^https?:/i.test(text(e.url)) || seen.has(e.url)) continue;
    seen.add(e.url);
    out.push({ url: e.url, statement: text(e.statement) || text(d.title) });
  }
  for (const u of d.seedUrls || []) {
    if (/^https?:/i.test(text(u)) && !seen.has(u)) {
      seen.add(u);
      out.push({ url: u, statement: text(d.title) });
    }
  }
  if (contact?.sourceUrl && !seen.has(contact.sourceUrl)) {
    out.push({ url: contact.sourceUrl, statement: `Contact source — ${contact.name}` });
  }
  return out;
}

/** Shared hotel-market opportunity builder (zero-cost ORS + paid deep). */
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
  const pill =
    start ? dateLabel(start, end) : spec.year ? `${spec.year} · dates TBD` : "Timing TBD";
  const what = spec.what;
  const potential = spec.potential;
  const whyHotel = spec.whyHotel;
  const whyNow = spec.whyNow;
  const nextStep = spec.nextStep;
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
    whatIsHappening: what,
    summaryWhat: what,
    potentialHotelBusiness: potential,
    potentialBusiness: potential,
    whyThisHotel: whyHotel,
    whyActNow: whyNow,
    cardWhyNowLine: whyNow.slice(0, 160),
    sources,
    hotelSourcingStatusLabel: spec.sourcingLabel || "Hotel Sourcing Unconfirmed",
    demandSegmentId: spec.segmentId,
    researchSegment: spec.segmentId,
    opportunityType: spec.segmentId,
    revenueStreamClass: revenueStreamFor(potential),
    guestroomThesis: potential,
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
    recommendedNextStep: nextStep,
    recommendedAction: nextStep,
    demandEvidenceClass: spec.demandClass,
    territoryClassification: spec.territory,
    researchProvenance: spec.provenance,
    uncertainties: spec.uncertainties || [],
    sharedTenSectionDrawer: {
      sections: [
        { id: "what_is_happening", title: "What is happening", body: what },
        { id: "potential_hotel_business", title: "Potential hotel business", body: potential },
        { id: "why_this_hotel", title: "Why this hotel", body: whyHotel },
        { id: "why_now", title: "Why now", body: whyNow },
        {
          id: "who_to_contact",
          title: "Who to contact",
          body: contact
            ? `${contact.name}${contact.title ? ` — ${contact.title}` : ""} (${contact.organization}). Entry point only; lodging authority not verified.`
            : "Named contact not yet verified — research stage.",
        },
        { id: "recommended_next_step", title: "Recommended next step", body: nextStep },
        {
          id: "supporting_evidence",
          title: "Supporting evidence",
          body: sources
            .slice(0, 10)
            .map((s) => `${s.statement || ""} ${s.url || ""}`.trim())
            .join("\n"),
        },
        {
          id: "research_uncertainties",
          title: "Research uncertainties",
          body:
            (spec.uncertainties || []).join(" ") ||
            "Hotel sourcing unconfirmed. Room volume unverified.",
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

function zcSpecFromDossier(e) {
  const d = dossiers.find((x) => (x.id || x.opportunityId) === e.id);
  const contact = e.contact;
  const start = e.dateEv.startDate || null;
  const lbl = start ? dateLabel(start, null) : `${e.dateEv.year}`;
  const locClause = text((d.activityLocation || "").split(/[;.]/)[0]);
  const what = `${e.org} has published ${e.title}${start ? ` for ${lbl}` : ` for ${e.dateEv.year} (exact dates not verified)`}${locClause ? ` — ${locClause}` : ""}.`;
  const potentialRaw = text(d.potentialHotelBusiness);
  const potential =
    potentialRaw.slice(0, 520) ||
    `Possible guestroom and group-meeting demand from ${e.title}; volume and sourcing unverified.`;
  const whyHotel = `${HOTEL_FIT_COPY} Fit is conditional on the organizer keeping lodging open to additional properties; no room block or RFP is evidenced.`;
  const whyNow = start
    ? `Edition dates are published (${lbl}); qualify who selects lodging before sourcing for the edition is finalized.`
    : `${e.dateEv.year} edition is announced; qualify who selects lodging before sourcing is finalized.`;
  const nextStep = `Contact ${contact.name}${contact.title ? ` (${text(contact.title).slice(0, 90)})` : ""} at ${e.org}; ask who selects lodging for the edition, whether hotel selection is still open, and expected room nights. Position ${HOTEL_NAME} as an all-inclusive Uvero Alto option.`;
  return {
    id: `wy_zc_${e.id}`,
    title: e.title,
    org: e.org,
    segmentId: d.researchSegment || "associations_conferences_events",
    startDate: start,
    year: e.dateEv.year || null,
    territory: e.territory,
    contact,
    contactClass: contactIsIntermediary(contact.title, contact.organization)
      ? "INTERMEDIARY_ENTRY_POINT"
      : "RELEVANT_COMMERCIAL_ENTRY_POINT",
    what,
    potential,
    whyHotel,
    whyNow,
    nextStep,
    sources: mkSources(d, contact),
    demandClass: "RELIABLE_INFERRED_DEMAND",
    uncertainties: [
      "Hotel sourcing unconfirmed — the organizer may already have a host property; verify before outreach.",
      "Room volume unverified.",
      `Date basis: ${e.dateEv.basis}.`,
    ],
    provenance: {
      origin: "ZERO_COST_ORS_RECONCILIATION",
      orsId: e.id,
      paidSpendUsd: 0,
      referenceDate: REF,
    },
    dateEvidence: e.dateEv,
  };
}

const zcSpecs = zcPromotable.map(zcSpecFromDossier);

// Near-misses: everything passes except exact-date verification -> deep-research priority.
const NEAR_MISS_FAILS = new Set([
  "YEAR_ONLY_NEEDS_EXACT_DATE",
  "TITLE_YEAR_MISMATCH_NEEDS_VERIFICATION",
  "NO_VERIFIED_FUTURE_DATE",
]);
const nearMissByKey = new Map();
for (const e of zcEvaluations) {
  if (!e.fails.length || !e.fails.every((f) => NEAR_MISS_FAILS.has(f))) continue;
  const d = dossiers.find((x) => (x.id || x.opportunityId) === e.id);
  const titleYear = maxYearIn([d.title, d.programmeName].join(" "));
  const hasFutureTitle = (titleYear && titleYear >= 2027) || e.fails.includes("TITLE_YEAR_MISMATCH_NEEDS_VERIFICATION");
  if (!hasFutureTitle) continue;
  const key = normKey(e.title).slice(0, 40);
  if (nearMissByKey.has(key)) continue;
  nearMissByKey.set(key, {
    id: `wy_nm_${e.id}`,
    fromDossier: true,
    orsId: e.id,
    title: e.title,
    organizationName: e.org,
    segmentId: d.researchSegment || "associations_conferences_events",
    excerpt: text(d.potentialHotelBusiness).slice(0, 300),
    evidenceUrls: (d.evidence || []).map((x) => x.url).concat(d.seedUrls || []).filter((u) => /^https?:/i.test(String(u || ""))),
    score: 6,
    nearMissFails: e.fails,
  });
}
const nearMissCandidates = [...nearMissByKey.values()];
console.log(`[zero-cost] near-miss deep candidates=${nearMissCandidates.length}`);
const failureHistogram = {};
for (const e of zcEvaluations) {
  for (const f of e.fails) failureHistogram[f] = (failureHistogram[f] || 0) + 1;
}
writeJson("02-ZERO_COST_PROMOTION.json", {
  evaluated: zcEvaluations.length,
  passedGateBeforeDedupe: zcPass.length,
  promotableAfterDedupe: zcPromotable.length,
  failureHistogram,
  promoted: zcSpecs.map((s) => ({
    id: s.id,
    orsId: s.provenance.orsId,
    title: s.title,
    org: s.org,
    segment: s.segmentId,
    dateEvidence: s.dateEvidence,
    territory: s.territory,
    contact: { name: s.contact.name, title: s.contact.title, sourceUrl: s.contact.sourceUrl },
  })),
  rejectedSample: zcEvaluations
    .filter((e) => e.fails.length)
    .slice(0, 400)
    .map((e) => ({ id: e.id, title: e.title, org: e.org, fails: e.fails })),
  policy: [
    "Future/ongoing date must be explicit in title/headline/evidence URL or ORS activity date (2027+) corroborated by evidence year.",
    "Territory must be Uvero Alto/Punta Cana/Bávaro (Cap Cana is competitive, never assumed Alltra demand).",
    "Contact must be named, commercially relevant (organizer/president/events/sales), sourced; procurement-only roles excluded.",
    "Hotel/resort brands excluded as prospects; category channels and own-team seeds never promoted.",
  ],
});
console.log(
  `[zero-cost] ORS=${ors.length} dossiers=${dossiers.length} seeds=${seeds.length} segments=${segDocs.length} promotable=${zcSpecs.length}`,
);

if (!AUTHORIZE && !ASSEMBLE_TEST) {
  console.log("[zero-cost-only] Re-run with --authorize-paid to spend within the $20 ceiling.");
  console.log(JSON.stringify({ promotable: zcSpecs.map((s) => s.title) }, null, 2));
  process.exit(0);
}

// ---------------------------------------------------------------------------
// STAGE 1+ — PAID EXA (hard ceiling)
// ---------------------------------------------------------------------------
if (!ASSEMBLE_TEST && !process.env.EXA_API_KEY) {
  console.error("EXA_API_KEY missing");
  process.exit(2);
}
process.env.GDI_EXA_AGENT_LIVE = "1";

let ledger =
  loadPaidBudgetLedger(LEDGER_PATH, {
    authorizationId: AUTHORIZATION_ID,
    hardCeilingUsd: HARD_CEILING_USD,
  }) ||
  createPaidBudgetLedger({
    authorizationId: AUTHORIZATION_ID,
    hardCeilingUsd: HARD_CEILING_USD,
    effort: EXA_AGENT_EFFORT.MEDIUM,
    priorSpendUsd: 0,
    outPath: LEDGER_PATH,
  });
persistPaidBudgetLedger(ledger);
const resumedSpendUsd = ledger.spentUsd;

writeJson("00-RESEARCH_AUTHORIZATION_AND_BUDGET.json", {
  authorizationId: AUTHORIZATION_ID,
  hardCeilingUsd: HARD_CEILING_USD,
  incrementalOnly: true,
  providers: { exa: true, parallel: false, surfe: false, surfeAutoInvoke: false },
  quotes: {
    medium: quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM }),
    high: quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.HIGH }),
  },
  stageCapsUsd: STAGE_CAP_USD,
  resumedSpendUsdAtStart: resumedSpendUsd,
  constraints: [
    "No Surfe auto",
    "No CUSTOMER_PUBLISHED",
    "No Bethesda/Sheraton publication mutation",
    "Airtable dry-run only",
    "Never use forbidden duplicate HPC reccax9nT5J90Sc0Y",
  ],
  referenceDate: REF,
  horizon: "2026-11 through 2028-05",
});

const stageInflight = {};
function stageSpentUsd(stage) {
  const done = ledger.operations
    .filter((o) => o.stage === stage)
    .reduce((a, o) => a + Number(o.actualCostUsd || 0), 0);
  return done + (stageInflight[stage] || 0);
}

async function chargeable(meta, fn) {
  const effort = meta.effort || EXA_AGENT_EFFORT.MEDIUM;
  const quote = quoteExaAgentRun({ effort });
  const cap = STAGE_CAP_USD[meta.stage] ?? HARD_CEILING_USD;
  if (stageSpentUsd(meta.stage) + quote.maxUsd > cap + 1e-9) {
    return { ok: false, blocked: "STAGE_CAP_REACHED", remaining: remainingBudgetUsd(ledger) };
  }
  const gate = canAffordQuotedCall(ledger, quote.maxUsd);
  if (!gate.ok) {
    ledger.blockedOperations.push({ at: new Date().toISOString(), reason: gate.reason, ...meta });
    persistPaidBudgetLedger(ledger);
    return { ok: false, blocked: gate.reason, remaining: remainingBudgetUsd(ledger) };
  }
  const reserved = reserveQuotedCall(ledger, { quotedMaxUsd: quote.maxUsd, ...meta });
  if (!reserved.ok) {
    return { ok: false, blocked: reserved.reason, remaining: remainingBudgetUsd(ledger) };
  }
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
    });
    return { ok: true, result, actualCostUsd: actual, remaining: remainingBudgetUsd(ledger) };
  } catch (e) {
    stageInflight[meta.stage] -= quote.maxUsd;
    releaseReservation(ledger, quote.maxUsd);
    ledger.blockedOperations.push({
      at: new Date().toISOString(),
      reason: String(e?.message || e),
      ...meta,
    });
    persistPaidBudgetLedger(ledger);
    return { ok: false, error: String(e?.message || e), remaining: remainingBudgetUsd(ledger) };
  }
}

/** Reuse completed raw runs on disk so a re-run never re-pays. */
function loadRaw(key) {
  const f = path.join(RAW_DIR, `${key}.json`);
  if (!fs.existsSync(f)) return null;
  try {
    const j = readJson(f);
    if (j?.paid?.ok && j?.paid?.result) return j;
  } catch {
    /* ignore */
  }
  return null;
}
function saveRaw(key, obj) {
  fs.writeFileSync(path.join(RAW_DIR, `${key}.json`), JSON.stringify(obj, null, 2));
}

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

function parseDiscoveryHits(structured) {
  const s = structured || {};
  const rows = s.candidates || s.events || s.companies || s.results || [];
  return (Array.isArray(rows) ? rows : []).map((row) => {
    const company =
      row.companyIdentity && typeof row.companyIdentity === "object" ? row.companyIdentity : null;
    const urls = [
      row.url,
      row.sourceUrl,
      ...(row.sourceUrls || []),
      ...(row.evidenceUrls || []),
      ...((row.supportingSources || []).map((x) => x?.url || x?.sourceUrl)),
      ...((row.supportingEvidence || []).map((x) => x?.url || x?.sourceUrl)),
      ...((row.evidenceClaims || []).map((x) => x?.url || x?.sourceUrl)),
      company?.website,
      company?.officialWebsite,
    ].filter((u) => /^https?:\/\//i.test(String(u || "")));
    return {
      title:
        text(
          row.title || row.verifiedEventName || row.eventIdentity || row.eventName ||
            row.verifiedBusinessActivity || row.businessTrigger || row.organizationName || "",
        ) || null,
      organizationName:
        text(
          row.organizationName || row.organizingEntity || row.corporateOrAssociationOwner ||
            row.companyName || row.associationName || company?.canonicalName ||
            company?.legalOrTradingName || row.name || "",
        ) || null,
      url: urls[0] || null,
      evidenceUrls: [...new Set(urls)],
      excerpt: text(
        row.summary || row.excerpt || row.rationale || row.rankScoreExplain ||
          row.hotelDemandHypothesis || row.hotelRelevanceHypothesis || "",
      ).slice(0, 500),
      raw: row,
    };
  });
}

// ---- Stage 1: discovery (EN + ES) ----
const hunterCoverage = SEGMENTS.map((seg) => {
  const routing = resolveExaAgentRoutingForSegment(seg, { stage: "DISCOVERY" });
  return {
    segmentId: seg,
    agentFamily: routing.agentFamily,
    paidExaAllowed: routing.paidExaAllowed,
    support: routing.support,
  };
});
writeJson("04a-HUNTER_EXECUTION_COVERAGE.json", { hunters: hunterCoverage });

const discoveryProvider = createExaLiveDiscoveryProvider({
  effort: EXA_AGENT_EFFORT.MEDIUM,
  authorizePaidDiscovery: true,
  authorizeExaAgentLive: true,
  forceMockOnly: false,
});
const discoveryHits = [];
const discoveryCalls = [];
const discoveryJobs = [];
for (const segmentId of ASSEMBLE_TEST ? [] : SEGMENTS) {
  const routing = resolveExaAgentRoutingForSegment(segmentId, { stage: "DISCOVERY" });
  if (!routing.paidExaAllowed) continue;
  for (const lang of ["en", "es"]) {
    discoveryJobs.push({ segmentId, lang });
  }
}
console.log("[discovery] jobs=", discoveryJobs.length);
await pool(discoveryJobs, 4, async ({ segmentId, lang }) => {
  const key = `discovery_${segmentId}_${lang}`;
  let paid;
  const cached = loadRaw(key);
  if (cached) {
    paid = { ...cached.paid, cached: true };
  } else {
    const queries = SEGMENT_QUERIES[segmentId][lang].map((q, i) => ({
      id: `${segmentId}_${lang}_${i + 1}`,
      intent: "future_demand",
      query: q,
    }));
    paid = await chargeable(
      { stage: "DISCOVERY", hunter: segmentId, segmentId, effort: EXA_AGENT_EFFORT.MEDIUM, label: lang },
      () => discoveryProvider.search({ queries, hotel: HOTEL, hotelId: HOTEL_ID, segmentId }),
    );
    saveRaw(key, { paid });
  }
  const result = paid.result || {};
  const hits = result.hits || [];
  discoveryCalls.push({
    key,
    segmentId,
    lang,
    ok: paid.ok,
    cached: paid.cached === true,
    blocked: paid.blocked || paid.error || result.blockedReason || null,
    hits: hits.length,
    costUsd: paid.actualCostUsd ?? 0,
  });
  console.log(`[discovery] ${key} ok=${paid.ok} hits=${hits.length} cost=${paid.actualCostUsd ?? 0} rem=${remainingBudgetUsd(ledger)}`);
  for (const h of hits) {
    discoveryHits.push({ ...h, segmentId, lang, providerRunId: result.providerRunId || null });
  }
});
writeJson("04-PAID_DISCOVERY.json", {
  calls: discoveryCalls,
  hitCount: discoveryHits.length,
  hits: discoveryHits.map((h) => ({
    title: h.title,
    org: h.organizationName,
    segment: h.segmentId,
    lang: h.lang,
    url: h.url,
    evidenceUrls: h.evidenceUrls,
    excerpt: h.excerpt,
  })),
});

// ---- Stage 2: CMI Stage 4a (competitor-committed future business) ----
const cmiDiscoveryCalls = [];
const cmiHits = [];
const cmiJobs = [];
for (const cluster of ASSEMBLE_TEST ? [] : COMPETITIVE_CLUSTERS) {
  for (const lang of ["en", "es"]) cmiJobs.push({ cluster, lang });
}
function cmiContext(cluster, lang) {
  const hotels = cluster.hotels.join("; ");
  return [
    "COMPETITOR-COMMITTED FUTURE GROUP/EVENT BUSINESS DISCOVERY (Competitive Market Intelligence)",
    `Target hotel (do NOT list as the venue): ${HOTEL_NAME}, 620-room all-inclusive, Uvero Alto Beach, Punta Cana, Dominican Republic.`,
    `Competing resorts to search: ${hotels}.`,
    "Find FUTURE (2027–mid 2028) group, incentive, wedding-expo, association, sports-team or corporate events that are ALREADY COMMITTED to one of these named resorts: official host hotel / conference hotel / official event page naming the resort / published group rate or booking code / team-lodging announcement.",
    "Include only items where an official or edition-specific source names the competing resort for that future edition. Exclude generic resort marketing, past events, and 'preferred hotel list' pages without edition-specific commitment.",
    `Language: ${lang === "es" ? "prioritize Spanish-language sources (eventos, congresos, bodas, torneos), also English" : "prioritize English-language sources, also Spanish"}.`,
    "For every candidate return the competing resort name, event name, organizer, verified dates, official source URLs, and the named organizer contact if public. Do not invent contacts, rates or room blocks.",
  ].join("\n");
}
console.log("[cmi-discovery] jobs=", cmiJobs.length);
await pool(cmiJobs, 3, async ({ cluster, lang }) => {
  const key = `cmi_discovery_${cluster.id}_${lang}`;
  let paid;
  const cached = loadRaw(key);
  if (cached) {
    paid = { ...cached.paid, cached: true };
  } else {
    const ctx = cmiContext(cluster, lang);
    const pack = buildExaEventsAgentCreatePayload({
      mode: "discovery",
      contextBlock: ctx,
      effort: EXA_AGENT_EFFORT.MEDIUM,
      segmentId: cluster.segmentId || "associations_conferences_events",
    });
    paid = await chargeable(
      {
        stage: "CMI_DISCOVERY",
        hunter: "cmi_competitor_booked_business",
        segmentId: "associations_conferences_events",
        effort: EXA_AGENT_EFFORT.MEDIUM,
        label: `${cluster.id}:${lang}`,
      },
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
    saveRaw(key, { paid });
  }
  const hits = paid.result?.hits || parseDiscoveryHits(paid.result?.structured);
  cmiDiscoveryCalls.push({
    key,
    cluster: cluster.id,
    lang,
    ok: paid.ok && paid.result?.ok !== false,
    cached: paid.cached === true,
    status: paid.result?.status || paid.blocked || paid.error || null,
    hits: hits.length,
    costUsd: paid.actualCostUsd ?? 0,
  });
  console.log(`[cmi-discovery] ${key} ok=${paid.ok} hits=${hits.length} cost=${paid.actualCostUsd ?? 0} rem=${remainingBudgetUsd(ledger)}`);
  for (const h of hits) cmiHits.push({ ...h, cluster: cluster.id, lang });
});

// ---- candidate triage ----
function isDuplicateOfExisting(title, org) {
  const key = normKey(`${title}|${org}`);
  if (existingKeys.has(key)) return true;
  const nt = normKey(title);
  if (!nt || nt.length < 14) return false;
  return existingTitleKeys.some(
    (ht) => ht && ht.length >= 14 && (ht.includes(nt.slice(0, 26)) || nt.includes(ht.slice(0, 26))),
  );
}
function futureSignal(h) {
  const blob = `${h.title || ""} ${h.excerpt || ""} ${(h.evidenceUrls || []).join(" ")}`;
  const y = maxYearIn(blob);
  if (y && y >= 2027) return true;
  const dates = extractExplicitDates(blob).filter((d) => d >= REF);
  if (dates.length) return true;
  return false;
}
function candScore(h) {
  const blob = `${h.title || ""} ${h.excerpt || ""} ${h.organizationName || ""}`;
  let s = 0;
  const t = territoryOf(blob);
  s += t === "TERRITORY_CORE" ? 4 : t === "TERRITORY_NEARBY" ? 3 : t === "TERRITORY_STRETCH" ? 1 : t === "TERRITORY_COMPETITIVE" ? 0 : -3;
  if (maxYearIn(blob) >= 2027) s += 2;
  if ((h.evidenceUrls || []).length >= 2) s += 1;
  if (HOTEL_LIKE_ORG_REGEX.test(h.organizationName || "")) s -= 2;
  return s;
}

const seenCand = new Set();
const triageRejected = [];
const hotelCandidates = [];
for (const h of discoveryHits) {
  const title = text(h.title);
  const org = text(h.organizationName);
  if (!title && !org) continue;
  if (isDuplicateOfExisting(title || org, org)) {
    triageRejected.push({ title, org, reason: "DUPLICATE_OF_ORS_DOSSIER" });
    continue;
  }
  if (!futureSignal(h)) {
    triageRejected.push({ title, org, reason: "NO_FUTURE_SIGNAL" });
    continue;
  }
  if (HOTEL_LIKE_ORG_REGEX.test(org) && COMPETITOR_REGEX.test(org)) {
    triageRejected.push({ title, org, reason: "ORG_IS_COMPETING_HOTEL_BRAND" });
    continue;
  }
  const id = hashId("wy_pd", title || org, org || title);
  if (seenCand.has(id)) continue;
  seenCand.add(id);
  hotelCandidates.push({ id, ...h, title: title || org, organizationName: org || title, score: candScore(h) });
}
// round-robin across segments, best score first
const bySeg = new Map();
for (const c of hotelCandidates.sort((a, b) => b.score - a.score)) {
  if (!bySeg.has(c.segmentId)) bySeg.set(c.segmentId, []);
  bySeg.get(c.segmentId).push(c);
}
const hotelDeepQueue = [];
let progressed = true;
while (progressed) {
  progressed = false;
  for (const seg of SEGMENTS) {
    const q = bySeg.get(seg);
    if (q && q.length) {
      hotelDeepQueue.push(q.shift());
      progressed = true;
    }
  }
}

const cmiSeen = new Set();
const cmiCandidates = [];
for (const h of cmiHits) {
  const title = text(h.title);
  const org = text(h.organizationName);
  if (!title && !org) continue;
  const blob = `${title} ${h.excerpt} ${(h.evidenceUrls || []).join(" ")}`;
  if (!COMPETITOR_REGEX.test(blob)) {
    triageRejected.push({ title, org, reason: "CMI_NO_COMPETITOR_NAMED", stage: "CMI" });
    continue;
  }
  if (!futureSignal(h)) {
    triageRejected.push({ title, org, reason: "CMI_NO_FUTURE_SIGNAL", stage: "CMI" });
    continue;
  }
  const id = hashId("wy_cmi", title || org, org || title);
  if (cmiSeen.has(id)) continue;
  cmiSeen.add(id);
  cmiCandidates.push({ id, ...h, title: title || org, organizationName: org || title, score: candScore(h) });
}
cmiCandidates.sort((a, b) => b.score - a.score);
console.log(
  `[triage] hotelCandidates=${hotelDeepQueue.length} cmiCandidates=${cmiCandidates.length} rejected=${triageRejected.length}`,
);

// ---- deep research helpers ----
function deepContext(cand, mode) {
  const lines = [
    "HOTEL:",
    JSON.stringify(HOTEL),
    "",
    "TERRITORY RULES:",
    "- Core: Uvero Alto / Punta Cana / Bávaro. Cap Cana, La Romana and Bayahibe are COMPETITIVE alternatives — do not assume Cap Cana events are demand for this hotel.",
    "- The hotel is all-inclusive; group business is guestroom-led with modest meeting space (≈380 m², ~300 conference).",
    "",
    "TARGET CANDIDATE TO DEEP-RESEARCH:",
    `Title: ${cand.title}`,
    `Organization: ${cand.organizationName}`,
    `Segment: ${cand.segmentId || "associations_conferences_events"}`,
    `Seed excerpt: ${cand.excerpt || ""}`,
    `Seed URLs: ${(cand.evidenceUrls || []).join(", ")}`,
    "",
    "REQUIREMENTS:",
    "- Verify ACTUAL activity dates (not article dates). Horizon Nov 2026 – May 2028. Reject expired one-time past events.",
    "- State the event venue / host hotel if announced, and whether lodging sourcing is open, closed or unknown.",
    "- Identify named commercially relevant contacts with role + source URL (organizer, planner, DMC, executive). Do not invent emails, RFPs or room blocks.",
    "- Do not treat generic resort marketing or 'preferred hotel list' pages as a booking commitment.",
  ];
  if (mode === "cmi") {
    lines.push(
      "- COMPETITIVE INTELLIGENCE FOCUS: identify which competing hotel/resort has been committed for this future edition, the type of commitment (event venue, guestrooms, group block), edition-specific evidence URLs, whether recurring/rotating, and the organizer contact for future-cycle account development.",
    );
  } else {
    lines.push(
      `- Produce a hotel demand thesis for ${HOTEL_NAME}; state honestly if the demand is already committed to another hotel.`,
    );
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
    findings: f,
    cib,
    track: text(s.commercialOpportunityTrack),
    contacts: Array.isArray(s.organizerContactCandidates) ? s.organizerContactCandidates : [],
    sources: Array.isArray(s.supportingSources) ? s.supportingSources : [],
    uncertainties: Array.isArray(s.remainingUncertainties) ? s.remainingUncertainties : [],
    followUps: Array.isArray(s.suggestedOrsFollowUps) ? s.suggestedOrsFollowUps : [],
  };
}

function pickContact(contacts, fallbackOrg) {
  let best = null;
  for (const c of contacts) {
    const name = text(c.name);
    if (!name || /desk|team|office|department|contact us|info@|^n\/a/i.test(name)) continue;
    const role = classifyContactRole(c.role || c.title, c.organization || fallbackOrg);
    const urls = [...(c.sourceUrls || []), c.profileUrl, c.sourceUrl].filter((u) => /^https?:/i.test(String(u || "")));
    if (!urls.length) continue;
    if (String(c.confidence || "").toLowerCase() === "low") continue;
    if (role !== "COMMERCIALLY_RELEVANT") continue;
    const rank = (String(c.confidence).toLowerCase() === "high" ? 2 : 1) + (/president|director|founder|owner|organizer|events|sales|ceo|chair/i.test(c.role || "") ? 1 : 0);
    if (!best || rank > best.rank) {
      best = {
        rank,
        contact: {
          name,
          title: text(c.role || c.title) || null,
          organization: text(c.organization) || fallbackOrg,
          sourceUrl: urls[0],
        },
        contactClass: contactIsIntermediary(c.role, c.organization)
          ? "INTERMEDIARY_ENTRY_POINT"
          : "RELEVANT_COMMERCIAL_ENTRY_POINT",
        relevance: text(c.commercialRelevance),
      };
    }
  }
  return best;
}

const deepRuns = [];
const deepKey = (cand, stage) => `${stage === "CMI_DEEP" ? "cmi_deep" : "deep"}_${cand.id}`;
const isCachedDeep = (cand, stage) => !!loadRaw(deepKey(cand, stage));
async function runDeep(cand, stage, mode) {
  const key = `${stage === "CMI_DEEP" ? "cmi_deep" : "deep"}_${cand.id}`;
  const cached = loadRaw(key);
  if (cached) return { cand, paid: { ...cached.paid, cached: true } };
  const ctx = deepContext(cand, mode);
  const pack = buildExaEventsAgentCreatePayload({
    mode: "deep_research",
    contextBlock: ctx,
    effort: EXA_AGENT_EFFORT.HIGH,
    segmentId: cand.segmentId || cand.segmentKey || "associations_conferences_events",
  });
  const paid = await chargeable(
    {
      stage,
      hunter: cand.segmentId || "cmi",
      segmentId: cand.segmentId || null,
      opportunityId: cand.id,
      effort: EXA_AGENT_EFFORT.HIGH,
      label: cand.title?.slice(0, 80),
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
  saveRaw(key, { cand: { ...cand, raw: undefined }, paid });
  return { cand, paid };
}

// ---- Stage 3: CMI deep ----
const cmiDeepResults = [];
const cmiDeepList = ASSEMBLE_TEST ? [] : cmiCandidates.slice(0, MAX_CMI_DEEP_RUNS);
console.log("[cmi-deep] candidates=", cmiDeepList.length);
await pool(cmiDeepList, 3, async (cand) => {
  if (remainingBudgetUsd(ledger) < 0.5 && !isCachedDeep(cand, "CMI_DEEP")) return null;
  const row = await runDeep(cand, "CMI_DEEP", "cmi");
  console.log(`[cmi-deep] ${cand.id} "${cand.title.slice(0, 50)}" ok=${row.paid.ok} cost=${row.paid.actualCostUsd ?? 0} rem=${remainingBudgetUsd(ledger)}`);
  cmiDeepResults.push(row);
  return row;
});

// ---- Stage 4: hotel-market deep ----
const hotelDeepResults = [];
let consecutiveNoYield = 0;
let stoppedEarlyReason = null;
let yieldQualified = 0;
function quickQualifies(row) {
  if (!row.paid.ok || row.paid.result?.ok === false) return false;
  const d = extractDeep(row.paid.result?.structured);
  if (!d.start && !/2027|2028/.test(`${d.eventName} ${d.recurrence} ${d.track}`)) return false;
  if (!pickContact(d.contacts, d.organizer || row.cand.organizationName)) return false;
  const venueIsCompetitor = d.venue && COMPETITOR_REGEX.test(d.venue);
  return !venueIsCompetitor;
}
const MAX_NEAR_MISS_DEEP = 8;
const hotelDeepList = ASSEMBLE_TEST
  ? []
  : (() => {
      const all = [];
      const seenQ = new Set();
      for (const c of [...nearMissCandidates.slice(0, MAX_NEAR_MISS_DEEP), ...hotelDeepQueue]) {
        if (seenQ.has(c.id)) continue;
        seenQ.add(c.id);
        all.push(c);
      }
      // Already-paid (cached) runs first: discovery hit order can vary between resumes.
      const paidAlready = all.filter((c) => isCachedDeep(c, "DEEP_RESEARCH"));
      const fresh = all.filter((c) => !isCachedDeep(c, "DEEP_RESEARCH"));
      return [...paidAlready, ...fresh].slice(0, MAX_HOTEL_DEEP_RUNS);
    })();
console.log("[deep] queue=", hotelDeepList.length, "remaining=", remainingBudgetUsd(ledger));
// process in waves of 3 to evaluate marginal yield between waves
for (let i = 0; i < hotelDeepList.length; i += 3) {
  if (
    remainingBudgetUsd(ledger) < 0.5 &&
    !hotelDeepList.slice(i, i + 3).some((c) => isCachedDeep(c, "DEEP_RESEARCH"))
  ) {
    stoppedEarlyReason = "BUDGET_FLOOR_REACHED";
    break;
  }
  if (consecutiveNoYield >= STOP_AFTER_CONSECUTIVE_NO_YIELD) {
    stoppedEarlyReason = `MARGINAL_YIELD_POOR_${consecutiveNoYield}_CONSECUTIVE_NON_QUALIFYING`;
    break;
  }
  const wave = hotelDeepList.slice(i, i + 3);
  const rows = await Promise.all(
    wave.map(async (cand) => {
      if (remainingBudgetUsd(ledger) < 0.5 && !isCachedDeep(cand, "DEEP_RESEARCH")) {
        console.log(`[deep] SKIP_NO_BUDGET_NO_CACHE ${cand.id}`);
        return null;
      }
      return runDeep(cand, "DEEP_RESEARCH", "hotel");
    }),
  );
  for (const row of rows) {
    if (!row) continue;
    console.log(`[deep] ${row.cand.id} "${row.cand.title.slice(0, 50)}" ok=${row.paid.ok} cost=${row.paid.actualCostUsd ?? 0} rem=${remainingBudgetUsd(ledger)}`);
    hotelDeepResults.push(row);
    if (quickQualifies(row)) {
      consecutiveNoYield = 0;
      yieldQualified += 1;
    } else {
      consecutiveNoYield += 1;
    }
  }
}
console.log("[deep] done", hotelDeepResults.length, "stopReason=", stoppedEarlyReason, "spent=", ledger.spentUsd);

// ---------------------------------------------------------------------------
// STAGE 5 — qualification
// ---------------------------------------------------------------------------
const rejected = [];
const researchWatch = [];
const paidHotelSpecs = [];

for (const row of hotelDeepResults) {
  const { cand, paid } = row;
  if (!paid.ok || paid.result?.ok === false) {
    rejected.push({ id: cand.id, title: cand.title, reason: paid.blocked || paid.error || paid.result?.status || "DEEP_FAILED" });
    continue;
  }
  const d = extractDeep(paid.result?.structured);
  const title = d.eventName || cand.title;
  const org = d.organizer || cand.organizationName;
  if (!cand.fromDossier && isDuplicateOfExisting(title, org)) {
    rejected.push({ id: cand.id, title, reason: "DUPLICATE_OF_ORS_DOSSIER" });
    continue;
  }
  const loc = [d.venue, d.city, d.region, d.country, cand.excerpt, title].join(" ");
  const territory = territoryOf(loc);
  // date
  let dateEv = { kind: "NONE" };
  if (d.start && d.start >= REF) dateEv = { kind: "EXACT_FUTURE", startDate: d.start, endDate: d.end && d.end >= d.start ? d.end : null };
  else if (d.start && d.start < REF && (!d.end || d.end < REF)) dateEv = { kind: "EXPIRED", startDate: d.start };
  else {
    const y = maxYearIn(`${title} ${d.recurrence} ${d.demandCycle}`);
    if (y && y >= 2027) dateEv = { kind: "YEAR_VERIFIED_FUTURE", year: y };
  }
  if (dateEv.kind === "EXACT_FUTURE" && dateEv.startDate < IMMINENT_CUTOFF) {
    rejected.push({ id: cand.id, title, reason: "IMMINENT_LT_21_DAYS_NOT_ACTIONABLE", start: d.start });
    continue;
  }
  if (dateEv.kind === "EXPIRED") {
    rejected.push({ id: cand.id, title, reason: "EXPIRED_ONE_TIME", start: d.start });
    continue;
  }
  if (dateEv.kind === "NONE") {
    researchWatch.push({ id: cand.id, title, org, reason: "NO_VERIFIED_FUTURE_DATE" });
    continue;
  }
  if (territory === "OUTSIDE_REALISTIC_TERRITORY") {
    rejected.push({ id: cand.id, title, reason: "OUTSIDE_TERRITORY", location: `${d.city}, ${d.country}` });
    continue;
  }
  if (!HOTEL_MARKET_TERRITORIES.has(territory)) {
    researchWatch.push({ id: cand.id, title, org, reason: `${territory}_NOT_ASSUMED_ALLTRA_DEMAND`, venue: d.venue });
    continue;
  }
  const venueIsCompetitor = d.venue && COMPETITOR_REGEX.test(d.venue) && !/alltra/i.test(d.venue);
  if (venueIsCompetitor) {
    researchWatch.push({
      id: cand.id,
      title,
      org,
      reason: "VENUE_COMMITTED_TO_COMPETITOR_ROUTED_TO_CMI_REVIEW",
      venue: d.venue,
    });
    cmiDeepResults.push({ cand, paid, routedFromHotelDeep: true });
    continue;
  }
  const pc = pickContact(d.contacts, org);
  if (!pc) {
    researchWatch.push({ id: cand.id, title, org, reason: "NO_NAMED_COMMERCIAL_CONTACT_WITH_SOURCE" });
    continue;
  }
  const blob = JSON.stringify(d.findings).toLowerCase();
  if (
    /not a (?:conference|group event|meeting|event)|airline connectivity|route launch|new (?:direct )?route|direct route|weekly (?:direct )?(?:flights|services)/i.test(
      `${title} ${blob}`,
    )
  ) {
    rejected.push({ id: cand.id, title, reason: "NON_GROUP_DEMAND_SIGNAL_AIRLINE_OR_INFRASTRUCTURE" });
    continue;
  }
  let demandClass = "SPECULATIVE_DEMAND";
  if (/rfp|open (?:hotel|lodging) sourcing|accepting (?:hotel|proposals)|hotel proposals/.test(`${d.hotelSourcingStatus} ${blob}`) && !/no rfp|not an open/.test(blob)) {
    demandClass = "CONFIRMED_DEMAND";
  } else if (/hotel|lodging|alojamiento|overnight|room|attendee|delegat|participants|teams/.test(blob)) {
    demandClass = "RELIABLE_INFERRED_DEMAND";
  }
  if (demandClass === "SPECULATIVE_DEMAND") {
    researchWatch.push({ id: cand.id, title, org, reason: "SPECULATIVE_ONLY" });
    continue;
  }
  const sources = [
    ...(cand.evidenceUrls || []).map((url) => ({ url, statement: title })),
    ...d.sources.map((s) => ({ url: s.url, statement: text(s.claim) || title })),
  ]
    .filter((s) => /^https?:/i.test(text(s.url)))
    .filter((s, i, a) => a.findIndex((x) => x.url === s.url) === i);
  const what =
    text(d.cib.whatIsHappening) ||
    `${title}${dateEv.startDate ? ` (${dateLabel(dateEv.startDate, dateEv.endDate)})` : ""} — organized by ${org}.`;
  const potential =
    text(d.cib.potentialBusiness) ||
    `Possible guestroom and meeting demand tied to ${title}; volume unverified.`;
  const whyHotel = `${HOTEL_FIT_COPY} ${text(d.cib.whyThisHotel).slice(0, 360)}`.trim();
  const whyNow =
    text(d.findings.futureEdition).slice(0, 260) ||
    `Edition dates published; qualify lodging ownership before sourcing closes.`;
  const nextStep =
    text(d.cib.recommendedNextStep) ||
    `Contact ${pc.contact.name} at ${org} to confirm who selects lodging and whether hotel selection is open.`;
  paidHotelSpecs.push({
    id: cand.id,
    title,
    org,
    segmentId: cand.segmentId || "associations_conferences_events",
    startDate: dateEv.startDate || null,
    endDate: dateEv.endDate || null,
    year: dateEv.year || null,
    territory,
    contact: pc.contact,
    contactClass: pc.contactClass,
    what: text(what).slice(0, 700),
    potential: text(potential).slice(0, 600),
    whyHotel: text(whyHotel).slice(0, 900),
    whyNow: text(whyNow).slice(0, 400),
    nextStep: text(nextStep).slice(0, 600),
    sources,
    demandClass,
    sourcingLabel:
      d.hotelSourcingStatus && /open/.test(d.hotelSourcingStatus)
        ? "Hotel Sourcing Open"
        : "Hotel Sourcing Unconfirmed",
    uncertainties: [
      ...d.uncertainties.slice(0, 3).map(text),
      "Room volume unverified.",
    ].filter(Boolean),
    provenance: {
      origin: "PAID_EXA_DISCOVERY_PLUS_DEEP_RESEARCH",
      discoveryRunId: cand.providerRunId || null,
      deepRunId: paid.result?.runId || null,
      segment: cand.segmentId,
      costUsd: paid.actualCostUsd ?? null,
      referenceDate: REF,
    },
    dateEvidence: dateEv,
  });
}

// ---- CMI qualification via canonical gates ----
const cmiSpecs = [];
const cmiRejected = [];
for (const row of cmiDeepResults) {
  const { cand, paid } = row;
  if (!paid?.ok || paid.result?.ok === false) {
    cmiRejected.push({ id: cand.id, title: cand.title, reason: paid?.blocked || paid?.error || "DEEP_FAILED" });
    continue;
  }
  const d = extractDeep(paid.result?.structured);
  const title = d.eventName || cand.title;
  const org = d.organizer || cand.organizationName;
  const venueBlob = `${d.venue} ${d.venueCommitment}`;
  const competitorMatch = venueBlob.match(COMPETITOR_REGEX);
  if (!competitorMatch || /alltra/i.test(d.venue)) {
    cmiRejected.push({ id: cand.id, title, reason: "NO_COMPETING_RESORT_NAMED_AS_COMMITTED", venue: d.venue });
    continue;
  }
  const competitorHotel =
    text(d.venue && COMPETITOR_REGEX.test(d.venue) ? d.venue : competitorMatch[0]);
  if (!d.start || d.start < REF) {
    cmiRejected.push({ id: cand.id, title, reason: d.start ? "EXPIRED_OR_PAST_EDITION" : "NO_VERIFIED_FUTURE_DATE", start: d.start });
    continue;
  }
  const accBlob = `${d.accommodation} ${d.venueCommitment} ${JSON.stringify(d.findings)}`.toLowerCase();
  const bookingLanguage = /host hotel|conference hotel|official hotel|headquarters hotel|sede|room block|published (?:room )?rates|group rate|booking code|reservation link|confirmed venue|hotel sede/.test(accBlob);
  const confirmedVenue = /venue_confirmed|hotel_confirmed|confirmed/.test(d.hotelSourcingStatus) || /confirmed|announced|official/.test(d.venueCommitment.toLowerCase());
  const urls = [
    ...(cand.evidenceUrls || []),
    ...d.sources.map((s) => s.url),
  ].filter((u) => /^https?:/i.test(String(u || "")));
  const yearStr = d.start.slice(0, 4);
  const roomEvidence = /room block|published (?:room )?rates|group rate|booking code|reservation link|tarifa/.test(accBlob);
  if (!(confirmedVenue && bookingLanguage && urls.length)) {
    cmiRejected.push({ id: cand.id, title, reason: "COMMITMENT_NOT_EDITION_SPECIFIC_OR_UNCONFIRMED", venue: d.venue, hotelSourcingStatus: d.hotelSourcingStatus });
    continue;
  }
  const pc = pickContact(d.contacts, org);
  const fallbackIntermediary = !pc && d.contacts.find((c) => text(c.name));
  const contact = pc?.contact || (fallbackIntermediary
    ? { name: text(fallbackIntermediary.name), title: text(fallbackIntermediary.role) || null, organization: text(fallbackIntermediary.organization) || org, sourceUrl: (fallbackIntermediary.sourceUrls || [])[0] || urls[0] }
    : null);
  if (!contact) {
    cmiRejected.push({ id: cand.id, title, reason: "NO_CONTACT_OR_INTERMEDIARY", venue: d.venue });
    continue;
  }
  const commitmentScope = roomEvidence ? "VENUE_AND_GUESTROOMS" : "VENUE_PRIMARY_GUESTROOMS_NOT_SEPARATELY_PUBLISHED";
  const venueStatus = roomEvidence
    ? COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK
    : COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE;
  const recommended = recommendCommercialAction({
    competitorVenueStatus: venueStatus,
    targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
    commercialActionability: COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
    competitorHotel,
    contactHint: contact.name,
    targetHotelName: HOTEL_NAME,
  });
  const claimUrls = urls.slice(0, 4);
  cmiSpecs.push({
    id: cand.id,
    title,
    org,
    competitorHotel,
    competitorVenueStatus: venueStatus,
    commitmentScope,
    bookingType: roomEvidence ? "EVENT_VENUE_PLUS_PUBLISHED_GUESTROOM_TERMS" : "CONFIRMED_HOTEL_SEDE_NO_PUBLISHED_ROOM_TERMS",
    eventStartDate: d.start,
    eventEndDate: d.end && d.end >= d.start ? d.end : d.start,
    datePillDisplayLabel: dateLabel(d.start, d.end && d.end > d.start ? d.end : null),
    competitorBookingClaim: `${text(d.venueCommitment).slice(0, 420) || `${competitorHotel} named for ${title}`} (edition ${yearStr}).`,
    lodgingEvidence: text(d.accommodation).slice(0, 420) || `${competitorHotel} named for the ${yearStr} edition; separate guestroom terms not published.`,
    futureCycleThesis:
      text(d.cib.potentialBusiness).slice(0, 360) ||
      `Develop the organizer relationship for a future edition or separately evidenced overflow; ${HOTEL_NAME} cannot realistically win the committed ${yearStr} main booking.`,
    recurs: text(d.recurrence).slice(0, 200) || "RECURRENCE_NOT_VERIFIED",
    uncertainty: d.uncertainties.slice(0, 2).map(text).join(" ") || "Contract terms not reviewed.",
    contact,
    contactClass: pc?.contactClass || "INTERMEDIARY_ENTRY_POINT",
    sources: claimUrls,
    claims: [
      { claimType: CMI_CLAIM_TYPE.COMPETITOR_HOST_HOTEL, status: "VERIFIED", evidenceUrls: claimUrls, statement: `${competitorHotel} named as the committed venue/host for ${title} (${yearStr}).` },
      roomEvidence
        ? { claimType: CMI_CLAIM_TYPE.COMPETITOR_ROOM_BLOCK, status: "VERIFIED", evidenceUrls: claimUrls, statement: text(d.accommodation).slice(0, 220) }
        : { claimType: CMI_CLAIM_TYPE.COMPETITOR_ROOM_BLOCK, status: "NOT_SUPPORTED", evidenceUrls: claimUrls, statement: "No published attendee room terms found." },
      { claimType: CMI_CLAIM_TYPE.TARGET_OPEN_SOURCING, status: "NOT_SUPPORTED", evidenceUrls: claimUrls, statement: `No open host sourcing path evidenced for ${HOTEL_NAME}.` },
    ],
    recommended,
    segmentId: cand.segmentId || "associations_conferences_events",
    provenance: {
      origin: "PAID_EXA_CMI_STAGE_4A",
      deepRunId: paid.result?.runId || null,
      costUsd: paid.actualCostUsd ?? null,
      referenceDate: REF,
    },
  });
}

function buildCmiOpportunity(spec) {
  const base = buildHotelMarketOpportunity({
    id: spec.id,
    title: spec.title,
    org: spec.org,
    segmentId: spec.segmentId,
    startDate: spec.eventStartDate,
    endDate: spec.eventEndDate,
    territory: "TERRITORY_NEARBY",
    contact: spec.contact,
    contactClass: spec.contactClass,
    what: `${spec.title} is committed at ${spec.competitorHotel} (${spec.datePillDisplayLabel}). The main booking is not available to ${HOTEL_NAME}.`,
    potential: "Future-cycle account development and/or separately evidenced overflow only — not the committed main booking.",
    whyHotel: spec.futureCycleThesis,
    whyNow: `Competitor booking confirmed at ${spec.competitorHotel}; develop the organizer relationship for a future cycle or residual lodging.`,
    nextStep: spec.recommended,
    sources: spec.sources.map((url) => ({ url, statement: spec.title })),
    demandClass: "CONFIRMED_COMPETITOR_COMMITMENT",
    sourcingLabel: "Competitor host/block confirmed — not an open host RFP",
    uncertainties: [spec.uncertainty],
    provenance: spec.provenance,
  });
  return {
    ...base,
    competitorHotel: spec.competitorHotel,
    likelyCompetitor: spec.competitorHotel,
    competitorVenueStatus: spec.competitorVenueStatus,
    competitorBookingClaim: spec.competitorBookingClaim,
    lodgingEvidence: spec.lodgingEvidence,
    housingStatus: "COMPETITOR_BLOCK_OR_SEDE_CONFIRMED",
    roomDemandStatus:
      spec.commitmentScope === "VENUE_AND_GUESTROOMS"
        ? "COMPETITOR_BLOCK_CONFIRMED"
        : "COMPETITOR_VENUE_CONFIRMED_GUESTROOMS_UNPUBLISHED",
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

const cmiOppsRaw = cmiSpecs.map(buildCmiOpportunity);
const cmiGateReviews = cmiOppsRaw.map((o) => ({
  id: o.id,
  title: o.title,
  competitorHotel: o.competitorHotel,
  gates: evaluateCmiQualificationGates(o),
}));
const cmiQualifiedIds = new Set(
  cmiGateReviews.filter((r) => r.gates.qualifiesAsCustomerCmi).map((r) => r.id),
);
const cmiFailedGateRows = cmiGateReviews.filter((r) => !r.gates.qualifiesAsCustomerCmi);
// Same organization + same start + same competitor venue = one booking, not two cards.
const cmiDuplicatesDropped = [];
{
  const seen = new Set();
  for (const o of cmiOppsRaw) {
    if (!cmiQualifiedIds.has(o.id)) continue;
    const k = `${normKey(o.organizationName)}|${o.eventStartDate}|${normKey(o.competitorHotel || "")}`;
    if (seen.has(k)) {
      cmiQualifiedIds.delete(o.id);
      cmiDuplicatesDropped.push({ id: o.id, title: o.title, duplicateKey: k });
      continue;
    }
    seen.add(k);
  }
}

// ---- assemble opportunities: zero-cost + paid hotel + CMI ----
const hotelSpecsAll = [];
const seenSpecKeys = new Set();
// paid deep supersedes zero-cost on the same event
for (const s of [...paidHotelSpecs, ...zcSpecs]) {
  const k = `${normKey(s.title).slice(0, 40)}|${(s.startDate || String(s.year || "")).slice(0, 4)}`;
  if (seenSpecKeys.has(k)) continue;
  seenSpecKeys.add(k);
  hotelSpecsAll.push(s);
}
const hotelOppsRaw = hotelSpecsAll.map(buildHotelMarketOpportunity);
const cmiOppsQualified = cmiOppsRaw.filter((o) => cmiQualifiedIds.has(o.id));

const hintsById = {};
for (const s of hotelSpecsAll) {
  const id = s.id;
  if (s.dateEvidence?.kind === "EXACT_FUTURE" || s.startDate) {
    hintsById[id] = {
      actualEventStart: s.startDate,
      actualEventEnd: s.endDate || null,
      datePillDisplayLabel: dateLabel(s.startDate, s.endDate),
    };
  } else if (s.year) {
    hintsById[id] = {
      dispositionOverride: "FUTURE_CYCLE_VERIFIED",
      datePillDisplayLabel: `${s.year} · dates TBD`,
      reason: "year_verified_in_evidence_exact_dates_unpublished",
    };
  }
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
  if (v && typeof v === "object") {
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, sanitizeCustomerCopy(x, k)]));
  }
  return v;
}
let allOpps = [...hotelOppsRaw, ...cmiOppsQualified].map((o) => sanitizeCustomerCopy(o));
allOpps = annotateInventoryPopulations(allOpps, { hotelId: HOTEL_ID }).map((o) => {
  const isCmi = cmiQualifiedIds.has(o.id);
  return annotateCanonicalCmiContract({
    ...o,
    // soft competitor-name associations must never inflate Competitive
    competitiveMarketScopeEligible: isCmi ? true : false,
    hotelMarketScopeEligible: isCmi ? false : o.hotelMarketScopeEligible,
  });
});
const gate = applyCommercialActionabilityGate(allOpps, {
  referenceDate: REF,
  hintsById,
});
const gatedRows = gate.rows.map((o) => attachSalesOpportunityFilterFields(o));
const customerVisible = gatedRows.filter(
  (o) => o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true,
);
const hotelMarket = customerVisible.filter((o) => o.hotelMarketScopeEligible === true);
const competitive = customerVisible.filter((o) => o.competitiveMarketScopeEligible === true);
const gatedOut = gatedRows.filter(
  (o) => !(o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true),
);
const actionabilityAssert = assertCustomerInventoryPassesActionabilityGate(customerVisible, {
  referenceDate: REF,
  hintsById,
});

writeJson("05-CMI_STAGE_4A.json", {
  contract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  competitiveSet: COMPETITIVE_CLUSTERS,
  guardrails: [
    "Competitive-set membership is not an opportunity.",
    "Cap Cana peers are competitive/nearby alternatives — never automatic Alltra overflow.",
    "Only edition-specific committed venue/block evidence qualifies; hotel-program listings do not.",
  ],
  discoveryCalls: cmiDiscoveryCalls,
  discoveryHitCount: cmiHits.length,
  candidatesDeepResearched: cmiDeepResults.map((r) => ({
    id: r.cand.id,
    title: r.cand.title,
    ok: r.paid?.ok,
    costUsd: r.paid?.actualCostUsd ?? null,
    routedFromHotelDeep: r.routedFromHotelDeep === true,
  })),
  qualifiedCmiCount: cmiQualifiedIds.size,
  qualifiedCmi: cmiOppsQualified.map((o) => ({
    id: o.id,
    title: o.title,
    competitorHotel: o.competitorHotel,
    dates: o.datePillDisplayLabel,
    competitiveMarketScopeEligible: true,
    hotelMarketScopeEligible: false,
  })),
  failedCanonicalGates: cmiFailedGateRows.map((r) => ({ id: r.id, title: r.title, failedGates: r.gates.failedGates })),
  rejectedEvidence: cmiRejected,
  triageRejected: triageRejected.filter((t) => t.stage === "CMI"),
});

writeJson("06-DEEP_RESEARCH_RESULTS.json", {
  hotelDeepRuns: hotelDeepResults.length,
  yieldQualifiedQuick: yieldQualified,
  stoppedEarlyReason,
  qualifiedPaidHotelSpecs: paidHotelSpecs.map((s) => ({
    id: s.id,
    title: s.title,
    org: s.org,
    segment: s.segmentId,
    dateEvidence: s.dateEvidence,
    territory: s.territory,
    contact: s.contact,
    demandClass: s.demandClass,
  })),
  researchWatch,
  rejected,
  triageRejectedCount: triageRejected.length,
  triageRejectedSample: triageRejected.slice(0, 200),
});

writeJson("07-COMMERCIAL_ACTIONABILITY.json", {
  referenceDate: REF,
  gate: "gdi-commercial-actionability-gate-v1",
  counts: gate.counts,
  assertCustomerInventoryPassesActionabilityGate: actionabilityAssert,
  rows: gatedRows.map((o) => ({
    id: o.id,
    title: o.title,
    state: o.commercialActionabilityStateV1,
    temporal: o.commercialTemporalDisposition,
    hotelMarket: o.hotelMarketScopeEligible,
    competitive: o.competitiveMarketScopeEligible,
    datePill: o.datePillDisplayLabel,
  })),
  gatedOut: gatedOut.map((o) => ({ id: o.id, title: o.title, state: o.commercialActionabilityStateV1 })),
});

// ---------------------------------------------------------------------------
// STAGE 6 — package, opportunities.json, Airtable dry run
// ---------------------------------------------------------------------------
const PROTECTED_STORE_ROOT = path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages");
const store = createGdiPublicationPackageStore({
  root: ASSEMBLE_TEST ? path.join(OUT, "_store") : PROTECTED_STORE_ROOT,
});
const protectedStore = createGdiPublicationPackageStore({ root: PROTECTED_STORE_ROOT });
const protectedBefore = {};
for (const [hid, label] of [["recLuxvwwxID7U2B8", "bethesda"], ["recCAGqkERSXgiku4", "sheraton"]]) {
  const cur = protectedStore.readPointer(hid, "CURRENT");
  const stg = protectedStore.readPointer(hid, "STAGED");
  protectedBefore[label] = {
    current: cur?.contentFingerprintSha256 || null,
    staged: stg?.contentFingerprintSha256 || null,
  };
}

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
  pkg = built.package || built;
  integrity = verifyPublicationPackageIntegrity(pkg);
  if (integrity?.ok === false) {
    packageBlocker = `PACKAGE_INTEGRITY_FAILED:${JSON.stringify(integrity.fails || integrity)}`;
    pkg = null;
  } else {
    const written = store.writePackage(pkg);
    writtenPath = written?.path || null;
    store.writePointer(HOTEL_ID, "STAGED", {
      publicationId: pkg.publicationId,
      publicationVersion: pkg.publicationVersion,
      contentFingerprintSha256: pkg.contentFingerprintSha256,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
      founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    });
    qualityGate = runPublicationQualityGate({
      opportunities: pkg.opportunities || customerVisible,
      referenceDate: REF,
    });
  }
}
const protectedAfter = {};
for (const [hid, label] of [["recLuxvwwxID7U2B8", "bethesda"], ["recCAGqkERSXgiku4", "sheraton"]]) {
  const cur = protectedStore.readPointer(hid, "CURRENT");
  const stg = protectedStore.readPointer(hid, "STAGED");
  protectedAfter[label] = {
    current: cur?.contentFingerprintSha256 || null,
    staged: stg?.contentFingerprintSha256 || null,
  };
}
const protectedUnchanged = JSON.stringify(protectedBefore) === JSON.stringify(protectedAfter);

fs.writeFileSync(
  ASSEMBLE_TEST ? path.join(OUT, "opportunities.test.json") : path.join(HOTEL_DATA, "opportunities.json"),
  `${JSON.stringify(
    {
      schema: "gdi-opportunities-doc-v1",
      hotelId: HOTEL_ID,
      updatedAt: new Date().toISOString(),
      source: SCRIPT_ID,
      opportunities: customerVisible,
    },
    null,
    2,
  )}\n`,
);

let airtablePlan = { configured: isGdiOpportunityAirtableConfigured(), dryRun: true, writes: 0 };
try {
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    dryRun: true,
    opportunities: customerVisible,
  });
  airtablePlan = {
    configured: true,
    dryRun: true,
    writes: 0,
    wouldCreate: sync.summary?.created,
    wouldUpdate: sync.summary?.updated,
    total: sync.summary?.total,
  };
} catch (e) {
  airtablePlan.error = String(e?.message || e);
}
writeJson("09-AIRTABLE_DRY_RUN.json", airtablePlan);

const inventoryCounts = computePublicationInventoryCounts(customerVisible);
writeJson("10-QUALITY_GATE.json", {
  ran: Boolean(qualityGate),
  blocking: qualityGate?.blocking || null,
  summary: qualityGate?.summary || null,
  ok: qualityGate?.ok ?? null,
  packageBlocker,
});
writeJson("11-INVENTORY_COUNTS.json", {
  publicationVersion: PUB_VERSION,
  inventoryCounts,
  hotelMarketCount: hotelMarket.length,
  competitiveCount: competitive.length,
  allCount: customerVisible.length,
  zeroCostPromoted: hotelSpecsAll.filter((s) => s.provenance.origin === "ZERO_COST_ORS_RECONCILIATION").length,
  paidPromoted: hotelSpecsAll.filter((s) => s.provenance.origin !== "ZERO_COST_ORS_RECONCILIATION").length,
  target: 15,
  targetMet: hotelMarket.length >= 15,
  shortfall: Math.max(0, 15 - hotelMarket.length),
  honestShortfallExplanation:
    hotelMarket.length >= 15
      ? null
      : "Shortfall is real: rows were included only when a future/ongoing date, in-territory location, a named commercially relevant sourced contact and non-committed sourcing were evidenced. Category channels, own-team seeds, procurement-only contacts, Cap Cana/competitor-committed and expired items were excluded rather than padded.",
  protectedPublicationsUnchanged: protectedUnchanged,
});

// spend itemization
const ops = ledger.operations;
const spendByStage = {};
for (const o of ops) {
  spendByStage[o.stage] = Number(((spendByStage[o.stage] || 0) + Number(o.actualCostUsd || 0)).toFixed(4));
}
writeJson("12-SPEND_ITEMIZATION.json", {
  authorizationId: AUTHORIZATION_ID,
  hardCeilingUsd: HARD_CEILING_USD,
  spentUsd: ledger.spentUsd,
  remainingUsd: remainingBudgetUsd(ledger),
  withinCeiling: ledger.spentUsd <= HARD_CEILING_USD,
  spendByStage,
  operationCount: ops.length,
  operations: ops.map((o) => ({
    at: o.at,
    stage: o.stage,
    hunter: o.hunter,
    segmentId: o.segmentId,
    label: o.label || null,
    quotedMaxUsd: o.quotedMaxUsd,
    actualCostUsd: o.actualCostUsd,
    providerRunId: o.providerRunId,
  })),
  blockedOperations: ledger.blockedOperations,
  frozen: ledger.frozen,
  freezeReason: ledger.freezeReason,
  costPerHotelMarketOpportunity:
    hotelMarket.length ? Number((ledger.spentUsd / hotelMarket.length).toFixed(4)) : null,
  surfeUsed: false,
});

// CMI coverage
const cmiAttempted = cmiDiscoveryCalls.some((c) => c.ok);
const cmiDeepRan = cmiDeepResults.some((r) => r.paid?.ok);
const completedChecklist = [
  "ESTABLISH_RELEVANT_COMPETITIVE_HOTEL_SET",
  cmiAttempted && "SEARCH_FUTURE_EVENTS_BOOKED_AT_COMPETING_HOTELS",
  cmiAttempted && "INVESTIGATE_COMPETITOR_PROPERTY_MEETING_CALENDARS",
  cmiHits.length > 0 && "IDENTIFY_ORGANIZATIONS_HOLDING_EVENTS_THERE",
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
  rejectedEvidenceCount: cmiRejected.length + cmiFailedGateRows.length,
  completedChecklistItems: completedChecklist,
});
writeJson("08-CMI_COVERAGE.json", {
  cmiCount: cmiQualifiedIds.size,
  checklistRequired: CMI_DISCOVERY_COVERAGE_CHECKLIST,
  completedChecklist,
  coverage: cmiCoverage,
  zeroCmiDocumentedCoverage:
    cmiQualifiedIds.size === 0
      ? {
          acceptable: cmiCoverage.zeroConfirmedCmiAcceptable,
          explanation:
            "Public all-inclusive resort group bookings rarely publish edition-specific host-hotel/room-block evidence. Competitor-named hits were investigated and rejected where commitment was not edition-specific, unconfirmed, expired, or lacked a named contact/intermediary.",
        }
      : null,
});

const gitSha = sh("git rev-parse HEAD");
const gitBranch = sh("git rev-parse --abbrev-ref HEAD");
writeJson("13-GITHUB_STATUS.json", {
  branch: gitBranch,
  headSha: gitSha,
  noCustomerPublished: true,
  noBethesdaMutation: protectedUnchanged,
  noSheratonMutation: protectedUnchanged,
  surfeUsed: false,
  commitedByScript: false,
});
writeJson("14-LOCAL_BROWSER_VERIFICATION.json", {
  status: "PENDING_BROWSER_SCRIPT",
  previewUrl: `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`,
  expectedFingerprint: pkg?.contentFingerprintSha256 || null,
  expectedCount: customerVisible.length,
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

// ---------------------------------------------------------------------------
// FOUNDER_REPORT.md
// ---------------------------------------------------------------------------
const rows = (arr, fn) => arr.map(fn).join("\n");
const founder = `# Wyndham Alltra Punta Cana — Full E2E ($20 ceiling)

**Authorization:** \`${AUTHORIZATION_ID}\`
**Canonical hotel:** \`${HOTEL_ID}\` (forbidden duplicate \`${FORBIDDEN_DUP_ID}\` never used)
**Hard ceiling:** **$${HARD_CEILING_USD}** · **Spent:** **$${ledger.spentUsd}** · **Remaining:** **$${remainingBudgetUsd(ledger)}**
**Staged publication:** \`${pkg?.publicationVersion || "NOT STAGED"}\`
**Fingerprint:** \`${pkg?.contentFingerprintSha256 || "n/a"}\`
**Lifecycle:** STAGED_FOR_FOUNDER_REVIEW (not CUSTOMER_PUBLISHED)

## Result

| Metric | Count |
|---|---:|
| Hotel Market opportunities (customer eligible) | **${hotelMarket.length}** |
| Competitive Market Intelligence (CMI) | **${competitive.length}** |
| Total staged | ${customerVisible.length} |
| Target (Hotel Market) | 15 |
| Target met | ${hotelMarket.length >= 15 ? "YES" : "NO — honest shortfall"} |
| Shortfall | ${Math.max(0, 15 - hotelMarket.length)} |

${hotelMarket.length >= 15 ? "" : `**Shortfall note:** ${"Rows were included only with an evidenced future/ongoing date, in-territory location, named sourced commercial contact and non-committed sourcing. Category channels, own-team seeds, procurement-only contacts, Cap Cana/competitor-committed and expired items were excluded rather than padded."}\n`}
## Zero-cost baseline (before any paid spend)

- ORS dossiers: **${ors.length}** (events/hist: ${dossiers.length}, Aimbridge inventory seeds: ${seeds.length})
- 14 segment state files: **${segDocs.length}**, unique segment candidate IDs: **${segCandidateUnion.size}**, all segments PARTIAL / not complete before this run
- Customer-visible opportunities before run: **0** (no opportunities.json existed)
- Zero-cost promotable (evidence-grounded): **${zcSpecs.length}**
- Seeds promoted: **0** (category channels, own sales team, hotel's UK/EU representation vendor, need-window hint, Cap Cana overflow guardrail)

## Paid research (Exa only)

${rows(Object.entries(spendByStage), ([k, v]) => `- ${k}: $${v}`)}
- Operations: ${ops.length}
- Hotel deep-research runs: ${hotelDeepResults.length} · CMI deep runs: ${cmiDeepResults.length}
- Early stop: ${stoppedEarlyReason || "none (queue exhausted or budget used)"}
- Surfe: not invoked. Parallel: not invoked.

## Hotel Market opportunities
${rows(hotelMarket, (o, i) => `${i + 1}. **${o.displayTitle}** — ${o.organizationName} · ${o.datePillDisplayLabel} · ${o.primaryContact?.name || "n/a"} (${o.primaryContact?.title || "role n/a"})`)}

## CMI (Stage 4a)
- CMI count: **${cmiQualifiedIds.size}**
- Discovery calls: ${cmiDiscoveryCalls.length} · hits: ${cmiHits.length} · deep: ${cmiDeepResults.length}
- Coverage complete: ${cmiCoverage.coverageComplete} · zero-CMI acceptable: ${cmiCoverage.zeroConfirmedCmiAcceptable}
${rows(competitive, (o, i) => `${i + 1}. **${o.displayTitle}** @ ${o.competitorHotel} — ${o.datePillDisplayLabel}`)}

## Gates
- Commercial actionability gate: ${gate.counts.activeHotelMarket} hotel / ${gate.counts.activeCompetitive} competitive / ${gate.counts.researchWatch} research-watch; assert ok=${actionabilityAssert.ok}
- Publication quality gate: ${qualityGate ? JSON.stringify(qualityGate.summary || qualityGate.ok) : "not run"}
- Bethesda & Sheraton publication pointers unchanged: ${protectedUnchanged}
- Airtable: dry-run only (${JSON.stringify(airtablePlan)})

## Preview
\`http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged\`

## Notes
- No CUSTOMER_PUBLISHED. No Surfe. Airtable writes: 0.
- Contacts are entry points; lodging authority is not verified. Sales Ready: 0 (all Account Development).
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);

console.log(
  JSON.stringify(
    {
      spentUsd: ledger.spentUsd,
      remainingUsd: remainingBudgetUsd(ledger),
      hotelMarketCount: hotelMarket.length,
      cmiCount: competitive.length,
      totalStaged: customerVisible.length,
      fingerprint: pkg?.contentFingerprintSha256 || null,
      publicationVersion: pkg?.publicationVersion || null,
      packageBlocker,
      protectedUnchanged,
      stoppedEarlyReason,
    },
    null,
    2,
  ),
);
