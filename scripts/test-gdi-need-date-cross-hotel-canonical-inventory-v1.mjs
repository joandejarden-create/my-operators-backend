#!/usr/bin/env node
/**
 * Cross-hotel regression: enumerate every canonical GDI-eligible hotel.
 * Does NOT hardcode a hotel-count denominator (e.g. 27).
 * Fails if any newly onboarded hotel lacks an explicit need-date status.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  listCanonicalGdiEligibleHotels,
  auditHotelLayerModelV2,
  GDI_RESEARCH_STATUS,
} from "../lib/group-demand-intelligence/need-date-demand/gdi-hotel-layer-model-v2.js";
import {
  HOTEL_NEED_DATE_CONFIG_STATUS,
  resolveHotelNeedDateConfiguration,
  loadAirtableNeedPeriodMirror,
} from "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js";
import {
  resolveAuthoritativeNeedDatePeriods,
  assertSourceContractInvariants,
  isTestScenarioPeriod,
} from "../lib/group-demand-intelligence/need-date-demand/gdi-need-date-source-contract-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-need-dates-27-hotel-integration-certification"
);
fs.mkdirSync(OUT, { recursive: true });

const failures = [];
const checks = [];
function check(id, ok, detail) {
  checks.push({ id, ok: Boolean(ok), detail: detail || null });
  if (!ok) failures.push({ id, detail });
}

const inventory = listCanonicalGdiEligibleHotels();
const audit = auditHotelLayerModelV2();

check("inventory_non_empty", inventory.length > 0, { count: inventory.length });
check(
  "inventory_ids_are_rec",
  inventory.every((h) => String(h.hotelId).startsWith("rec")),
  { sample: inventory.slice(0, 3).map((h) => h.hotelId) }
);
check(
  "no_hardcoded_27_denominator",
  inventory.length !== 27 || audit.counts.inventoryHotels === inventory.length,
  {
    note: "Count may change after onboarding; do not freeze at 27",
    count: inventory.length,
  }
);

const requiredStatuses = new Set(Object.values(HOTEL_NEED_DATE_CONFIG_STATUS));
const hotelRows = [];

for (const hotel of inventory) {
  const cfg = resolveHotelNeedDateConfiguration(hotel.hotelId);
  const resolved = resolveAuthoritativeNeedDatePeriods(hotel.hotelId, {
    persistConflicts: false,
  });
  const invariants = assertSourceContractInvariants(resolved);
  const row = audit.hotels.find((h) => h.hotelId === hotel.hotelId);

  check(
    `need_date_status_present:${hotel.hotelId}`,
    Boolean(cfg.status) && requiredStatuses.has(cfg.status),
    { hotelName: hotel.hotelName, status: cfg.status }
  );
  check(
    `research_status_present:${hotel.hotelId}`,
    Boolean(row?.gdiResearchStatus) &&
      Object.values(GDI_RESEARCH_STATUS).includes(row.gdiResearchStatus),
    { status: row?.gdiResearchStatus }
  );
  check(
    `stable_identity:${hotel.hotelId}`,
    hotel.hotelId === cfg.hotelId && hotel.hotelName.length > 0,
    { hotelName: hotel.hotelName }
  );
  check(
    `source_contract_ok:${hotel.hotelId}`,
    invariants.ok,
    { failures: invariants.failures }
  );
  check(
    `no_test_scenario_confirmed:${hotel.hotelId}`,
    !(resolved.periods || []).some(isTestScenarioPeriod),
    {
      confirmed: (resolved.periods || []).map((p) => p.needPeriodId),
    }
  );
  check(
    `svg_status_icon:${hotel.hotelId}`,
    Boolean(cfg.icon) && Boolean(cfg.colorClass),
    { icon: cfg.icon, colorClass: cfg.colorClass }
  );

  hotelRows.push({
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName,
    city: hotel.city,
    country: hotel.country,
    gdiOnboardingStatus: hotel.onboardingStatus,
    gdiResearchStatus: row?.gdiResearchStatus,
    needDateStatus: cfg.status,
    hotelConfirmedPeriods: cfg.counts.hotelConfirmedPeriods,
    marketEstimatedPeriods: cfg.counts.marketEstimatedPeriods,
    precedenceSource: resolved.precedenceSource,
    opportunityCount: row?.gdiOpportunityCount ?? 0,
  });
}

// Anchor hotels
const bethesda = resolveHotelNeedDateConfiguration("recLuxvwwxID7U2B8");
const bethesdaMirror = loadAirtableNeedPeriodMirror("recLuxvwwxID7U2B8");
check(
  "bethesda_eight_airtable_periods",
  bethesdaMirror.present &&
    bethesdaMirror.periods.length === 8 &&
    bethesda.status === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED &&
    bethesda.counts.hotelConfirmedPeriods === 8
);
check(
  "bethesda_unique_months",
  new Set(bethesdaMirror.periods.map((p) => `${p.startDate}|${p.endDate}`)).size ===
    8
);

const waterstone = resolveHotelNeedDateConfiguration("recgMYovrrZDJMqzX");
const waterstoneMirror = loadAirtableNeedPeriodMirror("recgMYovrrZDJMqzX");
check(
  "waterstone_test_not_confirmed",
  waterstone.counts.hotelConfirmedPeriods === 0 &&
    (waterstoneMirror.excludedNonAuthoritative || []).length >= 1
);
check(
  "waterstone_not_hotel_confirmed_status",
  waterstone.status !== HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED
);

const spice = resolveHotelNeedDateConfiguration("recKRJjcPnb4tVDDS");
check(
  "spice_not_established",
  spice.status === HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED &&
    spice.counts.hotelConfirmedPeriods === 0
);

for (const id of [
  "recRkGGNKslJptFIL",
  "recterKmNPdntCm0v",
  "recIEcSWuhSCoNLFN",
]) {
  check(
    `recovered_in_inventory:${id}`,
    inventory.some((h) => h.hotelId === id)
  );
  check(
    `recovered_has_need_date_status:${id}`,
    Boolean(resolveHotelNeedDateConfiguration(id).status)
  );
}

const uiSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8"
);
check("legend_present", /Need Dates|need-date-config|gdiNeedDateLegend/i.test(uiSrc));
check(
  "hotel_status_svgs",
  /gdiSvgCalendarCheck/.test(uiSrc) &&
    /gdiSvgCalendarClock/.test(uiSrc) &&
    /gdiSvgCalendarSearch/.test(uiSrc)
);

const report = {
  ok: failures.length === 0,
  version: "GDI_NEED_DATES_CROSS_HOTEL_CANONICAL_INVENTORY_V1",
  generatedAt: new Date().toISOString(),
  inventoryCount: inventory.length,
  denominator: "listCanonicalGdiEligibleHotels",
  notDenominator: "listGdiSelectableHotels",
  layerCounts: audit.counts,
  checks,
  failures,
  hotels: hotelRows,
};

fs.writeFileSync(
  path.join(OUT, "cross-hotel-canonical-inventory-test.json"),
  JSON.stringify(report, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(OUT, "canonical-hotel-need-date-status-table.json"),
  JSON.stringify(
    {
      version: report.version,
      inventoryCount: inventory.length,
      generatedAt: report.generatedAt,
      hotels: hotelRows,
    },
    null,
    2
  ) + "\n"
);

console.log(
  JSON.stringify(
    {
      ok: report.ok,
      inventoryCount: inventory.length,
      failureCount: failures.length,
      failures: failures.slice(0, 20),
      out: OUT,
    },
    null,
    2
  )
);
process.exit(report.ok ? 0 : 1);
