#!/usr/bin/env node
/**
 * Sheraton Guadalajara Expo — provisional need dates + SVG + CMI remediation.
 * Stages a NEW founder preview package. Does NOT CUSTOMER_PUBLISH.
 *
 *   node scripts/gdi-sheraton-need-dates-svg-cmi-v1.mjs
 *   node scripts/gdi-sheraton-need-dates-svg-cmi-v1.mjs --apply-need-periods
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
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
import { attachOpportunityRevenueStreamIcons } from "../lib/group-demand-intelligence/opportunity-revenue-stream-icons-v1.js";
import {
  saveMarketEstimatedNeedPeriods,
  deriveHotelNeedDateConfigStatus,
  HOTEL_NEED_DATE_CONFIG_STATUS,
  NEED_PERIOD_PROVENANCE,
  applyNeedDateMatchProvenanceGuard,
} from "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js";
import { attachHotelNeedDateTilePreviewsSync } from "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js";
import { matchInventoryToNeedPeriods } from "../lib/group-demand-intelligence/need-date-demand/need-period-inventory-match-v1.js";
import {
  listNeedPeriodsFromAirtable,
  saveNeedPeriodChangeToAirtable,
  upsertNeedPeriodOpportunityMatch,
} from "../lib/group-demand-intelligence/need-date-demand/need-date-airtable-store-v3.js";
import {
  NEED_PERIOD_CHANGE_TYPE,
  NEED_PERIOD_STATUS,
  NEED_REVENUE_TYPE,
} from "../lib/group-demand-intelligence/need-date-demand/need-date-demand-contract-v1.js";
import { GDI_FULL_E2E_HOTEL_TEMPLATE_V1 } from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import { INVENTORY_POPULATION } from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recCAGqkERSXgiku4";
const HOTEL_NAME = "Sheraton Guadalajara Expo";
const SOURCE_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261114_expiration_gate_cleanup_v1";
const SOURCE_FP =
  "967d63ff5fef0319a6f94aa726202a7c01e1d98d4130ba07206a67cd7913f746";
const ARCHIVED_EXA_FP =
  "3c5f09487037d509126e99a27a7d896c9321506e5e787c07254adfb9f29ea8e3";
const OUT = path.join(ROOT, "reports/gdi-sheraton-need-dates-svg-cmi-v1");
const APPLY = process.argv.includes("--apply-need-periods");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, obj) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

fs.mkdirSync(OUT, { recursive: true });

// ---------------------------------------------------------------------------
// Provisional need calendar (planning hypotheses — not hotel occupancy)
// ---------------------------------------------------------------------------
const PROVISIONAL_PERIODS = [
  {
    needPeriodId: "ndp_sheraton_gdl_prov_2027_01_shoulder",
    periodLabel: "January 2027 post-holiday shoulder (midweeks)",
    startDate: "2027-01-05",
    endDate: "2027-01-23",
    calendarYear: 2027,
    revenueNeed: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.CORPORATE_TRANSIENT_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
    ],
    priority: "MEDIUM",
    confidence: 0.55,
    why: "Guadalajara leisure/corporate demand typically softens after holidays before spring Expo/business calendars firm up; midweeks historically quieter than FIL/Fiestas peaks.",
    evidence: [
      "https://revenuenaire.com/locations/guadalajara/",
      "https://travel.usnews.com/Guadalajara_Mexico/When_To_Visit/",
    ],
    mustConfirm:
      "Whether Sheraton sales sees soft midweeks in early January 2027 after any GNP / inbound group holds.",
  },
  {
    needPeriodId: "ndp_sheraton_gdl_prov_2027_02_weekend",
    periodLabel: "February 2027 weekend leisure soft window",
    startDate: "2027-02-05",
    endDate: "2027-02-15",
    calendarYear: 2027,
    revenueNeed: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.LOCAL_CATERING,
      NEED_REVENUE_TYPE.BANQUETS,
    ],
    priority: "LOW",
    confidence: 0.48,
    why: "Shoulder leisure weekends between January business return and March–May Expo strength; useful for local catering / short social groups — not evidence of empty hotel.",
    evidence: [
      "https://revenuenaire.com/locations/guadalajara/",
      "https://travel.usnews.com/Guadalajara_Mexico/When_To_Visit/",
    ],
    mustConfirm:
      "Mary Kay / other inbound programmes may fill Feb — hotel must confirm whether this window is still soft.",
  },
  {
    needPeriodId: "ndp_sheraton_gdl_prov_2027_06_rain_shoulder",
    periodLabel: "June 2027 rainy-season soft midweeks",
    startDate: "2027-06-08",
    endDate: "2027-06-26",
    calendarYear: 2027,
    revenueNeed: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
      NEED_REVENUE_TYPE.LOCAL_CATERING,
    ],
    priority: "HIGH",
    confidence: 0.62,
    why: "June–August is Guadalajara's typical low leisure/corporate season (rains). Window avoids assuming whole-month weakness; major medical congresses (e.g. dermatology 2027) may override — protect those weeks.",
    evidence: [
      "https://revenuenaire.com/locations/guadalajara/",
      "https://travel.usnews.com/Guadalajara_Mexico/When_To_Visit/",
      "https://www.eleconomista.com.mx/estados/itb-americas-impulsa-expectativas-turisticas-jalisco-20261110-843234.html",
    ],
    mustConfirm:
      "Exact Dermatología / Fisioterapia / other Expo holds in June 2027 and whether Sheraton already has contracted group.",
  },
  {
    needPeriodId: "ndp_sheraton_gdl_prov_2027_07_soft",
    periodLabel: "July 2027 rainy midweeks",
    startDate: "2027-07-06",
    endDate: "2027-07-24",
    calendarYear: 2027,
    revenueNeed: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.CORPORATE_TRANSIENT_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
    ],
    priority: "HIGH",
    confidence: 0.64,
    why: "Core rainy-season soft period for city hotels; school-holiday leisure softness and weaker midweek corporate. Prefer midweeks; weekends may still see domestic leisure.",
    evidence: [
      "https://revenuenaire.com/locations/guadalajara/",
      "https://travel.usnews.com/Guadalajara_Mexico/When_To_Visit/",
    ],
    mustConfirm: "Any Expo Guadalajara holds or contracted room blocks for July 2027.",
  },
  {
    needPeriodId: "ndp_sheraton_gdl_prov_2027_08_soft",
    periodLabel: "August 2027 soft period",
    startDate: "2027-08-03",
    endDate: "2027-08-28",
    calendarYear: 2027,
    revenueNeed: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
      NEED_REVENUE_TYPE.LOCAL_CATERING,
      NEED_REVENUE_TYPE.BANQUETS,
    ],
    priority: "HIGH",
    confidence: 0.66,
    why: "Late summer rains + school calendar typically softest for Guadalajara city hotels; strong candidate for incremental group/meeting/catering prospecting if Expo calendar is clear.",
    evidence: [
      "https://revenuenaire.com/locations/guadalajara/",
      "https://travel.usnews.com/Guadalajara_Mexico/When_To_Visit/",
    ],
    mustConfirm: "Expo calendar clearance and any Aimbridge/Sheraton internal pickup forecasts for Aug 2027.",
  },
  {
    needPeriodId: "ndp_sheraton_gdl_prov_2027_09_early",
    periodLabel: "Early September 2027 pre-peak bridge",
    startDate: "2027-09-01",
    endDate: "2027-09-12",
    calendarYear: 2027,
    revenueNeed: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
    ],
    priority: "MEDIUM",
    confidence: 0.52,
    why: "Bridge before Independence / Fiestas de Octubre strength; short window only — not full September.",
    evidence: ["https://revenuenaire.com/locations/guadalajara/"],
    mustConfirm: "Whether early Sep already holds corporate or Expo-related demand.",
  },
  {
    needPeriodId: "ndp_sheraton_gdl_prov_2028_01_shoulder",
    periodLabel: "January 2028 post-holiday shoulder",
    startDate: "2028-01-04",
    endDate: "2028-01-22",
    calendarYear: 2028,
    revenueNeed: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.CORPORATE_TRANSIENT_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
    ],
    priority: "MEDIUM",
    confidence: 0.5,
    why: "Repeat of typical post-holiday shoulder into 2028 planning horizon; provisional until hotel RM confirms.",
    evidence: [
      "https://revenuenaire.com/locations/guadalajara/",
      "https://travel.usnews.com/Guadalajara_Mexico/When_To_Visit/",
    ],
    mustConfirm: "2028 Expo / corporate calendar and hotel forecast for early January.",
  },
  {
    needPeriodId: "ndp_sheraton_gdl_prov_2028_07_soft",
    periodLabel: "July 2028 rainy midweeks",
    startDate: "2028-07-05",
    endDate: "2028-07-23",
    calendarYear: 2028,
    revenueNeed: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
      NEED_REVENUE_TYPE.LOCAL_CATERING,
    ],
    priority: "HIGH",
    confidence: 0.58,
    why: "Forward rainy-season soft hypothesis for 18-month research horizon; same caveats as 2027 — Expo holds can override.",
    evidence: [
      "https://revenuenaire.com/locations/guadalajara/",
      "https://travel.usnews.com/Guadalajara_Mexico/When_To_Visit/",
    ],
    mustConfirm: "2028 Expo Guadalajara holds overlapping July.",
  },
  {
    needPeriodId: "ndp_sheraton_gdl_prov_2028_08_soft",
    periodLabel: "August 2028 soft period",
    startDate: "2028-08-02",
    endDate: "2028-08-27",
    calendarYear: 2028,
    revenueNeed: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
      NEED_REVENUE_TYPE.LOCAL_CATERING,
      NEED_REVENUE_TYPE.BANQUETS,
    ],
    priority: "HIGH",
    confidence: 0.58,
    why: "Forward soft-period planning for Aug 2028; provisional internal scenario only.",
    evidence: [
      "https://revenuenaire.com/locations/guadalajara/",
      "https://travel.usnews.com/Guadalajara_Mexico/When_To_Visit/",
    ],
    mustConfirm: "Hotel sales/RM confirmation before treating as need-date priority.",
  },
];

const COMPETITIVE_SET = [
  {
    name: "Hilton Guadalajara (WTC / Expo adjacency)",
    rationale:
      "AAA full-service neighbour to Expo Guadalajara / World Trade Center; direct group and meetings competitor for Expo-adjacent demand.",
    evidence: [
      "https://www.gdltours.com/gran_turismo_hotels_guadalajara.html",
      "https://expoguadalajara.mx/en/allied-hotels/",
    ],
    inclusion: "PRIMARY_COMPSET",
  },
  {
    name: "The Westin Guadalajara",
    rationale:
      "Listed ~150 m / 2 mins from Expo on Expo Guadalajara allied hotels; upscale Expo-adjacent lodging competitor.",
    evidence: ["https://expoguadalajara.mx/en/allied-hotels/"],
    inclusion: "PRIMARY_COMPSET",
  },
  {
    name: "Real Inn Expo (Camino Real Expo corridor)",
    rationale:
      "Expo Guadalajara allied hotel ~150 m; direct corridor competitor for exhibitor/attendee lodging.",
    evidence: ["https://expoguadalajara.mx/en/allied-hotels/"],
    inclusion: "PRIMARY_COMPSET",
  },
  {
    name: "Barceló Guadalajara",
    rationale:
      "Large full-service property opposite Expo Guadalajara with substantial meeting inventory; competing for congress/overflow blocks.",
    evidence: [
      "https://www.cvent.com/venues/guadalajara/hotel/barcelo-guadalajara/venue-a1f52a65-810d-497f-a648-57c899db8dab",
    ],
    inclusion: "PRIMARY_COMPSET",
  },
  {
    name: "AC Hotel Guadalajara Expo",
    rationale:
      "Marriott select-service ~5 minutes from Expo; overlapping Bonvoy / Expo-corridor transient and small-group demand.",
    evidence: [
      "https://www.marriott.com/en-us/hotels/gdlar-ac-hotel-guadalajara-expo-mexico/overview/",
    ],
    inclusion: "SECONDARY_COMPSET",
  },
  {
    name: "Crowne Plaza Guadalajara (López Mateos / Plaza del Sol)",
    rationale:
      "Plaza del Sol corridor full-service alternative near Expo approach roads.",
    evidence: [
      "https://www.gdltours.com/gran_turismo_hotels_guadalajara.html",
    ],
    inclusion: "SECONDARY_COMPSET",
  },
];

// ---------------------------------------------------------------------------
// Phase 1 — baseline
// ---------------------------------------------------------------------------
const store = createGdiPublicationPackageStore({
  root: path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  ),
});
const listed = store.listVersions(HOTEL_ID);
const source = store.loadPackage(HOTEL_ID, SOURCE_VERSION);
if (!source.ok || source.package.contentFingerprintSha256 !== SOURCE_FP) {
  console.error("Source staged package mismatch — refuse to proceed");
  process.exit(3);
}
const ops0 = source.package.opportunities.map((o) => ({ ...o }));

let liveApi = { error: null };
try {
  const r = await fetch(
    `http://127.0.0.1:8080/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities?publicationPackage=1&surface=customer&preview=staged`,
  );
  const j = await r.json();
  liveApi = {
    count: j.count,
    fingerprint: j.publication?.contentFingerprintSha256 || null,
    version: j.publication?.publicationVersion || null,
    hotel: j.publication?.inventoryCounts?.hotel ?? null,
    competitive: j.publication?.inventoryCounts?.competitive ?? null,
    needDateStatus: j.hotelNeedDateConfigV1?.status || null,
    needDateLabel: j.hotelNeedDateConfigV1?.statusLabel || null,
  };
} catch (e) {
  liveApi = { error: String(e?.message || e) };
}

const baselineSvg = {
  withRevenueStreamClass: ops0.filter((o) => o.revenueStreamClass).length,
  withFrozenIcons: ops0.filter((o) => o.revenueStreamIcons).length,
  guestActiveFrozen: ops0.filter(
    (o) => o.revenueStreamIcons?.guestrooms === "active",
  ).length,
  classHistogram: {},
};
for (const o of ops0) {
  const c = o.revenueStreamClass || "MISSING";
  baselineSvg.classHistogram[c] = (baselineSvg.classHistogram[c] || 0) + 1;
}

writeJson("01-BASELINE_PUBLICATION_AND_UI.json", {
  asOf: new Date().toISOString(),
  hotelId: HOTEL_ID,
  template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  explanation: {
    hotel0_vs_hotel62:
      "Hotel (0) / empty Sales chips typically indicates a package missing inventoryPopulations or a UI loading CURRENT/empty package. Hotel (62) / Competitive (0) matches archived Exa historical-recovery candidate fingerprint 3c5f0948… — NOT the current STAGED pointer.",
    currentStaged:
      "Live STAGED at script start is expiration-gate cleanup fingerprint 967d63ff… (43 opportunities). This run does not silently fall back to 3c5f0948….",
  },
  archivedExaHistoricalRecoveryFingerprint: ARCHIVED_EXA_FP,
  sourcePackage: {
    version: SOURCE_VERSION,
    fingerprint: SOURCE_FP,
    opportunityCount: ops0.length,
    inventoryCounts: computePublicationInventoryCounts(ops0),
  },
  liveApiBefore: liveApi,
  allPackageVersions: listed,
  opportunityRegister: ops0.map((o) => ({
    id: o.id,
    title: o.title,
    revenueStreamClass: o.revenueStreamClass || null,
    revenueStreamIcons: o.revenueStreamIcons || null,
    salesKeys: o.salesOpportunityQuickFilterKeys || [],
    hotelEligible: o.hotelMarketScopeEligible !== false,
    competitiveEligible: o.competitiveMarketScopeEligible === true,
    needDateMatchV1: o.needDateMatchV1 || null,
    eventStart: o.eventStartDate || o.tileEventStartDate || null,
  })),
  svgBaseline: baselineSvg,
});

writeJson("02-SEASONALITY_AND_NEED_PERIOD_EVIDENCE.json", {
  hotelId: HOTEL_ID,
  note: "Provisional planning evidence only — not hotel-confirmed occupancy gaps.",
  marketSeasonality: {
    softest: "June–August rainy season (leisure + corporate soften; Expo holds can override)",
    peak: "October–December (Fiestas de Octubre, FIL, holidays)",
    shoulder: "January–February post-holiday / pre-spring Expo strength",
    sources: [
      "https://revenuenaire.com/locations/guadalajara/",
      "https://travel.usnews.com/Guadalajara_Mexico/When_To_Visit/",
      "https://www.c-mw.net/guadalajara-poised-for-the-global-stage/",
      "https://www.eleconomista.com.mx/estados/itb-americas-impulsa-expectativas-turisticas-jalisco-20261110-843234.html",
    ],
  },
  caveats: [
    "Few recorded conventions ≠ low occupancy.",
    "Not every week in a soft month is equally weak.",
    "Major Expo / medical congress holds can fill rainy-season weeks.",
  ],
  commercialProfileNeedDatesStatus: "UNKNOWN — hotel has not supplied confirmed need dates",
});

writeJson("03-PROVISIONAL_NEED_PERIOD_PLAN.json", {
  hotelId: HOTEL_ID,
  classification: "PROVISIONAL_INTERNAL_PLANNING_SCENARIO",
  hotelConfirmed: false,
  periods: PROVISIONAL_PERIODS,
});

// ---------------------------------------------------------------------------
// Persist market estimates (local) + Airtable TEST_SCENARIO
// ---------------------------------------------------------------------------
const marketPeriods = PROVISIONAL_PERIODS.map((p) => ({
  estimateId: p.needPeriodId,
  needPeriodId: p.needPeriodId,
  hotelId: HOTEL_ID,
  periodLabel: p.periodLabel,
  startDate: p.startDate,
  endDate: p.endDate,
  calendarYear: p.calendarYear,
  revenueNeed: p.revenueNeed,
  revenueNeedType: p.revenueNeed[0],
  priority: p.priority,
  status: "Active",
  provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
  evidenceSummary: `${p.why} Evidence: ${p.evidence.join("; ")}. Must confirm: ${p.mustConfirm}`,
  sources: p.evidence,
  confidence: p.confidence,
  uncertainty:
    "Provisional planning hypothesis — hotel has not confirmed occupancy need.",
  hotelConfirmationStatus: "PENDING",
  lifecycleStatus: "ACTIVE",
  customerVisible: false,
  publishable: false,
  isTestScenario: false,
  source: "PROVISIONAL_INTERNAL_PLANNING_SCENARIO_SHERATON_GDL_V1",
}));

const marketDoc = saveMarketEstimatedNeedPeriods(HOTEL_ID, marketPeriods);
const hotelNeedConfig = deriveHotelNeedDateConfigStatus({
  hotelId: HOTEL_ID,
  hotelConfirmedPeriods: [],
  marketEstimatedPeriods: marketPeriods,
});

let existingAirtable = { periods: [], error: null };
try {
  existingAirtable = await listNeedPeriodsFromAirtable(HOTEL_ID);
} catch (e) {
  existingAirtable = { periods: [], error: String(e?.message || e) };
}

const airtablePlan = [];
for (const p of PROVISIONAL_PERIODS) {
  const existing = (existingAirtable.periods || []).find(
    (x) => x.needPeriodId === p.needPeriodId,
  );
  const patch = {
    needPeriodId: p.needPeriodId,
    hotelId: HOTEL_ID,
    periodLabel: p.periodLabel,
    startDate: p.startDate,
    endDate: p.endDate,
    revenueNeed: p.revenueNeed,
    priority: p.priority,
    status: NEED_PERIOD_STATUS.TEST_SCENARIO,
    isTestScenario: true,
    source: "PROVISIONAL_INTERNAL_PLANNING_SCENARIO_SHERATON_GDL_V1",
    researchState: "MATCHING_EXISTING",
    hotelSalesNotes: [
      "PROVISIONAL planning scenario — NOT hotel-confirmed occupancy.",
      p.why,
      `Evidence: ${p.evidence.join(" | ")}`,
      `Must confirm: ${p.mustConfirm}`,
      `Confidence: ${p.confidence}`,
      "Do not populate Last Confirmed By Hotel.",
    ].join("\n"),
    idempotencyKey: `sheraton-prov-v1:${p.needPeriodId}`,
    changeReason:
      "Founder-authorized Sheraton-only provisional need-period scenario",
  };
  airtablePlan.push({
    needPeriodId: p.needPeriodId,
    action: existing ? "SKIP_EXISTING_IDEMPOTENT" : "CREATE_TEST_SCENARIO",
    existingStatus: existing?.status || null,
    existingIsTest: existing?.isTestScenario ?? null,
    wouldOverwriteConfirmed:
      existing &&
      existing.isTestScenario !== true &&
      String(existing.status) !== "TEST_SCENARIO" &&
      String(existing.status) !== "Test Scenario",
    patch,
  });
}

writeJson("03-PROVISIONAL_NEED_PERIOD_PLAN.json", {
  hotelId: HOTEL_ID,
  classification: "PROVISIONAL_INTERNAL_PLANNING_SCENARIO",
  hotelConfirmed: false,
  hotelNeedConfig,
  marketEstimatePath: marketDoc && "saved",
  periods: PROVISIONAL_PERIODS,
  airtableDryRun: airtablePlan,
});

const writeReceipts = {
  dryRun: !APPLY,
  applied: [],
  skipped: [],
  errors: [],
  existingError: existingAirtable.error || null,
};

if (APPLY) {
  for (const row of airtablePlan) {
    if (row.wouldOverwriteConfirmed) {
      writeReceipts.skipped.push({
        id: row.needPeriodId,
        reason: "refuse_overwrite_non_test_period",
      });
      continue;
    }
    if (row.action === "SKIP_EXISTING_IDEMPOTENT" && row.existingIsTest === true) {
      writeReceipts.skipped.push({
        id: row.needPeriodId,
        reason: "already_present_test_scenario",
      });
      continue;
    }
    try {
      const r = await saveNeedPeriodChangeToAirtable({
        hotelId: HOTEL_ID,
        changeType: NEED_PERIOD_CHANGE_TYPE.ADD,
        patch: row.patch,
        actorSource: "founder_provisional_scenario",
        actorUser: { id: "founder", email: "founder@dealality.test" },
        reason: row.patch.changeReason,
        env: { ...process.env, GDI_NEED_DATES_AIRTABLE_LIVE: "1" },
      });
      writeReceipts.applied.push({
        needPeriodId: row.needPeriodId,
        resultNeedPeriodId: r?.change?.needPeriodId || null,
        ok: true,
      });
    } catch (e) {
      writeReceipts.errors.push({
        needPeriodId: row.needPeriodId,
        error: String(e?.message || e),
      });
    }
  }
  try {
    const again = await listNeedPeriodsFromAirtable(HOTEL_ID);
    writeReceipts.readback = (again.periods || [])
      .filter((p) => String(p.needPeriodId || "").startsWith("ndp_sheraton_gdl_prov_"))
      .map((p) => ({
        needPeriodId: p.needPeriodId,
        status: p.status,
        isTestScenario: p.isTestScenario,
        source: p.source,
        startDate: p.startDate,
        endDate: p.endDate,
        lastConfirmedByHotel: p.lastConfirmedByHotel || null,
      }));
  } catch (e) {
    writeReceipts.readbackError = String(e?.message || e);
  }
} else {
  writeReceipts.note =
    "Dry-run only. Re-run with --apply-need-periods after founder review of airtableDryRun.";
}

writeJson("04-AIRTABLE_NEED_PERIOD_WRITE_RECEIPTS.json", writeReceipts);

// ---------------------------------------------------------------------------
// Need-Date Match against provisional market estimates
// ---------------------------------------------------------------------------
const matchablePeriods = marketPeriods.map((p) => ({
  ...p,
  needPeriodId: p.needPeriodId,
  status: "Active",
  provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
  needPeriodProvenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
  isTestScenario: false,
}));

const inventoryMatch = matchInventoryToNeedPeriods(matchablePeriods, ops0);
const withNeed = attachHotelNeedDateTilePreviewsSync(HOTEL_ID, ops0, {
  needPeriods: matchablePeriods,
  allowMarketEstimatedPeriods: true,
  hotelName: HOTEL_NAME,
  hotelNeedDateConfig: {
    hotelId: HOTEL_ID,
    status: HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED,
    statusLabel: hotelNeedConfig.statusLabel,
    confirmedPeriodCount: 0,
    estimatedPeriodCount: marketPeriods.length,
  },
});

const matchResults = [];
const matchPersist = { attempted: 0, ok: 0, errors: [] };
for (const o of withNeed) {
  const m = o.needDateMatchV1;
  if (!m) continue;
  const guarded = applyNeedDateMatchProvenanceGuard(
    {
      indicatorState: m.tileIndicatorState || m.timingOverlapStatus,
      tooltipText: m.whyThisMatches,
      accessibleLabel: m.needPeriodLabel,
      verifiedHotelNeedDateMatch: false,
    },
    NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
  );
  matchResults.push({
    opportunityId: o.id,
    title: o.title,
    needPeriodLabel: m.needPeriodLabel,
    timingOverlapStatus: m.timingOverlapStatus,
    provisional: true,
    verifiedHotelNeedDateMatch: false,
    guarded,
    whyThisMatches: m.whyThisMatches || null,
    suggestedSalesAction: m.suggestedSalesAction || null,
  });
  if (APPLY && Array.isArray(o.needDateAssociations)) {
    for (const assoc of o.needDateAssociations) {
      matchPersist.attempted += 1;
      try {
        await upsertNeedPeriodOpportunityMatch({
          hotelId: HOTEL_ID,
          needPeriodId: assoc.needPeriod?.needPeriodId,
          opportunityId: o.id,
          matchingEvidence: {
            provisional: true,
            title: o.title,
            timing: assoc.timing?.overlapStatus,
            provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
          },
          timingOverlap: assoc.timing?.overlapStatus || null,
          revenueRelevance: assoc.association?.matchedRevenue || null,
          matchClass: "Existing Match",
          env: { ...process.env, GDI_NEED_DATES_AIRTABLE_LIVE: "1" },
        });
        matchPersist.ok += 1;
      } catch (e) {
        matchPersist.errors.push(String(e?.message || e));
      }
    }
  }
}

writeJson("05-NEED_DATE_MATCH_RESULTS.json", {
  inventoryMatchSummary: {
    matchCount: inventoryMatch.matches?.length || 0,
    existingMatchCount: inventoryMatch.existingMatchCount || 0,
  },
  opportunityMatches: matchResults,
  airtableMatchPersist: matchPersist,
  note: "All matches are provisional — never Hotel Confirmed.",
});

// ---------------------------------------------------------------------------
// SVG audit + correction
// ---------------------------------------------------------------------------
const svgAudit = [];
const svgCorrections = [];
let ops = withNeed.map((o) => {
  const before = {
    class: o.revenueStreamClass || null,
    icons: o.revenueStreamIcons || null,
    salesKeys: o.salesOpportunityQuickFilterKeys || [],
  };
  let next = { ...o };
  // Ensure sales keys present for icon restore
  next = attachSalesOpportunityFilterFields(next);
  const attached = attachOpportunityRevenueStreamIcons({
    ...next,
    // Feed thesis from commercial prose already on the row
    guestroomThesis:
      next.guestroomThesis ||
      (Array.isArray(next.salesOpportunityQuickFilterKeys) &&
      next.salesOpportunityQuickFilterKeys.includes("GUEST_ROOMS")
        ? text(next.potentialHotelBusiness) ||
          text(next.potentialGuestroomDemand) ||
          "Guestroom demand supported by Sales Opportunities GUEST_ROOMS classification and hotel-fit thesis."
        : null),
    eventFnbThesis:
      next.eventFnbThesis ||
      (Array.isArray(next.salesOpportunityQuickFilterKeys) &&
      (next.salesOpportunityQuickFilterKeys.includes("LOCAL_CATERING") ||
        next.salesOpportunityQuickFilterKeys.includes("BOTH"))
        ? text(next.potentialMeetingDemand) ||
          text(next.potentialCateringDemand) ||
          "Meetings/catering demand supported by Sales Opportunities LOCAL_CATERING/BOTH classification."
        : null),
  });
  // Prefer class+sales-key restore when attach still muted
  let icons = attached.revenueStreamIcons;
  let cls = attached.revenueStreamClass;
  if (
    (!icons ||
      (icons.guestrooms === "muted" && icons.eventFnb === "muted")) &&
    next.revenueStreamClass &&
    next.revenueStreamClass !== "REVENUE_STREAM_UNCONFIRMED"
  ) {
    const keys = new Set(
      (next.salesOpportunityQuickFilterKeys || []).map((k) =>
        String(k).toUpperCase(),
      ),
    );
    const frozen = String(next.revenueStreamClass).toUpperCase();
    const guest =
      (frozen.includes("GUESTROOM") || frozen.includes("COMBINED")) &&
      (keys.has("GUEST_ROOMS") || keys.has("BOTH"))
        ? "active"
        : "muted";
    const event =
      (frozen.includes("EVENT") ||
        frozen.includes("COMBINED") ||
        frozen.includes("FNB")) &&
      (keys.has("LOCAL_CATERING") || keys.has("BOTH"))
        ? "active"
        : "muted";
    // GUESTROOMS_ONLY
    if (frozen === "GUESTROOMS_ONLY" && keys.has("GUEST_ROOMS")) {
      icons = { guestrooms: "active", eventFnb: "muted" };
      cls = "GUESTROOMS_ONLY";
    } else if (
      frozen === "COMBINED_GUESTROOMS_EVENT_FNB" &&
      keys.has("GUEST_ROOMS") &&
      (keys.has("LOCAL_CATERING") || keys.has("BOTH"))
    ) {
      icons = { guestrooms: "active", eventFnb: "active" };
      cls = "COMBINED_GUESTROOMS_EVENT_FNB";
    } else if (
      frozen === "EVENT_FNB_ONLY" &&
      (keys.has("LOCAL_CATERING") || keys.has("BOTH"))
    ) {
      icons = { guestrooms: "muted", eventFnb: "active" };
      cls = "EVENT_FNB_ONLY";
    } else {
      icons = { guestrooms: guest, eventFnb: event };
    }
  }
  svgAudit.push({
    id: o.id,
    title: o.title,
    before,
    after: { class: cls, icons },
    salesKeys: next.salesOpportunityQuickFilterKeys || [],
  });
  if (
    JSON.stringify(before.icons) !== JSON.stringify(icons) ||
    before.class !== cls
  ) {
    svgCorrections.push({
      id: o.id,
      from: before,
      to: { class: cls, icons },
      reason:
        "Restore evidence-backed revenueStreamClass + Sales Opportunities keys after package thesis scrub muted SVGs",
    });
  }
  return {
    ...attached,
    ...next,
    revenueStreamClass: cls,
    revenueStreamIcons: icons,
    revenueStreamIconsVersion: "GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1",
    guestroomThesis: attached.guestroomThesis || next.guestroomThesis || null,
    eventFnbThesis: attached.eventFnbThesis || next.eventFnbThesis || null,
    needDateMatchV1: next.needDateMatchV1
      ? {
          ...next.needDateMatchV1,
          provisionalNeedDateMatch: true,
          verifiedHotelNeedDateMatch: false,
          periodProvenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
          estimatedLabel:
            "Provisional need period — awaiting hotel confirmation",
        }
      : null,
  };
});

writeJson("06-SVG_SEMANTIC_AUDIT.json", {
  rootCause:
    "Packages retained revenueStreamClass and Sales Opportunities keys but dropped revenueStreamIcons + thesis prose; read-time consistency guard then demoted all icons to muted/UNCONFIRMED.",
  count: svgAudit.length,
  rows: svgAudit,
});
writeJson("07-SVG_CORRECTIONS.json", {
  corrected: svgCorrections.length,
  rows: svgCorrections,
});

// ---------------------------------------------------------------------------
// CMI audit — clear unjustified competitive flags
// ---------------------------------------------------------------------------
writeJson("08-COMPETITIVE_HOTEL_SET.json", {
  hotelId: HOTEL_ID,
  status: "ESTABLISHED_FROM_PUBLIC_SOURCES",
  competitiveSet: COMPETITIVE_SET,
  note: "Competitive set is geography/positioning reference — not automatic CMI opportunity membership.",
});

const cmiAudit = [];
const namedCompetitorRe =
  /\b(hilton guadalajara|westin guadalajara|barcel[oó] guadalajara|real inn expo|camino real expo|crowne plaza guadalajara|fiesta americana|presidente intercontinental|hyatt regency guadalajara|ac hotel guadalajara)\b/i;

ops = ops.map((o) => {
  const blob = [
    o.title,
    o.whatIsHappening,
    o.potentialHotelBusiness,
    o.whyThisHotel,
    o.summaryWhat,
    JSON.stringify(o.sharedTenSectionDrawer || {}),
  ]
    .map(text)
    .join("\n");
  const namedHit = namedCompetitorRe.test(blob);
  // Boilerplate "competitor-blocked" in whyThisHotel is NOT CMI evidence.
  const boilerplateOnly =
    /competitor-blocked|versus venue-hosted or competitor/i.test(blob) &&
    !namedHit;
  const prior = o.competitiveMarketScopeEligible === true;
  let classLabel = "NOT_CMI";
  let eligible = false;
  let reason =
    "No verified competitor-hosted booking, preferred hotel relationship, or named competing-hotel programme meeting CMI eligibility.";
  if (namedHit) {
    classLabel = "NEEDS_FURTHER_VERIFICATION";
    eligible = false;
    reason =
      "Named competitor hotel string present but not verified as confirmed competitor-hosted demand for a future cycle — flagged for founder review, not auto-CMI.";
  }
  if (prior && !eligible) {
    reason +=
      " Cleared prior competitiveMarketScopeEligible=true that lacked CMI-standard evidence (paid Discovery Expo capture ≠ Competitive Market Intelligence).";
  }
  cmiAudit.push({
    opportunityId: o.id,
    title: o.title,
    priorCompetitiveEligible: prior,
    namedCompetitorMention: namedHit,
    boilerplateOnly,
    cmiClass: classLabel,
    competitiveMarketScopeEligible: eligible,
    reason,
  });
  const pops = new Set(
    (Array.isArray(o.inventoryPopulations) ? o.inventoryPopulations : []).filter(
      Boolean,
    ),
  );
  pops.add(INVENTORY_POPULATION.SALES_OPPORTUNITIES || "SALES_OPPORTUNITIES");
  pops.delete("COMPETITIVE_MARKET_INTELLIGENCE");
  pops.delete("COMPETITIVE");
  return {
    ...o,
    competitiveMarketScopeEligible: false,
    hotelMarketScopeEligible: true,
    inventoryPopulations: [...pops],
    inventoryPopulationPrimary: "SALES_OPPORTUNITIES",
    cmiClass: classLabel,
    cmiAuditReason: reason,
  };
});

writeJson("09-CMI_OPPORTUNITY_AUDIT.json", {
  priorCompetitiveCount: ops0.filter((o) => o.competitiveMarketScopeEligible)
    .length,
  afterCompetitiveCount: ops.filter((o) => o.competitiveMarketScopeEligible)
    .length,
  standard:
    "CMI requires verified competitor-hosted demand, recurring competitor relationship, or evidence-supported future competitive pursuit — not Expo-corridor hotel capture alone.",
  rows: cmiAudit,
});

writeJson("10-MARKET_SCOPE_CHANGES.json", {
  before: computePublicationInventoryCounts(ops0),
  after: computePublicationInventoryCounts(ops),
  note: "Competitive count may be zero after honest CMI audit; Hotel Market Scope remains the Expo-corridor sales inventory.",
});

// ---------------------------------------------------------------------------
// Stage new publication package
// ---------------------------------------------------------------------------
const publicationVersion = `gdi_pub_${HOTEL_ID}_20261114_need_dates_svg_cmi_v1`;
const built = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  publicationVersion,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  opportunities: ops,
  templateVersion: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  notes: {
    purpose:
      "Provisional need dates + SVG revenue-stream repair + CMI reclassification for Sheraton founder preview",
    sourceFingerprint: SOURCE_FP,
    provisionalNeedPeriods: PROVISIONAL_PERIODS.length,
    notCustomerPublished: true,
  },
});

const pkg = built.package || built;
const gate = runPublicationQualityGate(pkg);
const integrity = verifyPublicationPackageIntegrity(pkg);
const written = store.writePackage(pkg);
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  supersedes: {
    publicationVersion: SOURCE_VERSION,
    contentFingerprintSha256: SOURCE_FP,
    preservesExaHistoricalRecovery: ARCHIVED_EXA_FP,
  },
});

writeJson("12-NEW_STAGED_PUBLICATION.json", {
  publicationVersion,
  fingerprint: pkg.contentFingerprintSha256,
  opportunityCount: pkg.opportunities?.length,
  inventoryCounts: computePublicationInventoryCounts(pkg.opportunities || []),
  qualityGate: { ok: gate?.ok !== false, summary: gate?.summary || gate },
  integrity: { ok: integrity?.ok !== false },
  packagePath: written?.path || null,
  preservedFingerprints: {
    expirationCleanup: SOURCE_FP,
    exaHistoricalRecovery: ARCHIVED_EXA_FP,
  },
});

writeJson("11-AIRTABLE_READBACK_AND_INTEGRITY.json", {
  needPeriodWrites: writeReceipts,
  opportunityAirtableUpsert:
    "Deferred — founder previously required separate upsert authorization for core opportunity registry; this run persists provisional need periods/matches only when --apply-need-periods is set.",
  packageIntegrity: integrity,
});

writeJson("14-REGRESSION_TESTS.json", {
  svgPositive: {
    expectGuestActiveWhenGuestRoomsKey: ops.filter(
      (o) =>
        (o.salesOpportunityQuickFilterKeys || []).includes("GUEST_ROOMS") &&
        o.revenueStreamIcons?.guestrooms === "active",
    ).length,
  },
  svgNegative: {
    expectNoInventedCateringWithoutKey: ops.filter(
      (o) =>
        !(o.salesOpportunityQuickFilterKeys || []).includes("LOCAL_CATERING") &&
        !(o.salesOpportunityQuickFilterKeys || []).includes("BOTH") &&
        o.revenueStreamIcons?.eventFnb === "active",
    ).length,
  },
  provisionalNeverHotelConfirmed: matchResults.every(
    (m) => m.verifiedHotelNeedDateMatch === false,
  ),
  competitiveNotForcedNonzero:
    computePublicationInventoryCounts(ops).competitive === 0,
});

const gh = {
  branch: sh("git rev-parse --abbrev-ref HEAD"),
  commit: sh("git rev-parse HEAD"),
  status: sh("git status --porcelain"),
};
writeJson("15-GITHUB_STATUS.json", gh);

fs.writeFileSync(
  path.join(OUT, "16-REMAINING_HOTEL_VALIDATION_QUESTIONS.md"),
  `# Remaining hotel validation questions — ${HOTEL_NAME}

1. Which months/windows does Sheraton sales/RM confirm as true need periods for 2027–2028?
2. Do any June–August 2027 Expo / medical congress holds already fill the provisional soft windows?
3. Confirm or reject each provisional period in Airtable (promote from Test Scenario → Active only after hotel confirmation).
4. Are any competitor hotels (Hilton / Westin / Barceló / Real Inn Expo / Crowne Plaza) preferred hosts for recurring accounts in the current inventory?
5. Which opportunities should stay customer-visible vs research-watch after RM review of timing?

Provisional periods are planning hypotheses — not evidence of actual occupancy shortfalls.
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      fingerprint: pkg.contentFingerprintSha256,
      version: publicationVersion,
      applyNeedPeriods: APPLY,
      matches: matchResults.length,
      svgCorrected: svgCorrections.length,
      competitiveAfter: computePublicationInventoryCounts(ops).competitive,
      out: OUT,
    },
    null,
    2,
  ),
);
