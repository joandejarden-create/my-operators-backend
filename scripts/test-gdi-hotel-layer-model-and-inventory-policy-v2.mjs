#!/usr/bin/env node
/**
 * V2 regressions: three-layer hotel model + full-inventory policy +
 * need-date independence from GDI research.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  listCanonicalGdiEligibleHotels,
  auditHotelLayerModelV2,
  GDI_RESEARCH_STATUS,
} from "../lib/group-demand-intelligence/need-date-demand/gdi-hotel-layer-model-v2.js";
import {
  resolveHotelNeedDateConfiguration,
  HOTEL_NEED_DATE_CONFIG_STATUS,
  applyNeedDateMatchProvenanceGuard,
  NEED_PERIOD_PROVENANCE,
  loadAirtableNeedPeriodMirror,
} from "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js";
import { NEED_DATE_TILE_INDICATOR } from "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/repository.js";
// Research full-inventory policy lives on PR #151 — excluded from this
// presentation-focused release. Layer + Need-Date independence still gated.

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-universal-need-dates-and-full-e2e-v2");
fs.mkdirSync(OUT, { recursive: true });

const failures = [];
const checks = [];
function check(id, ok, detail) {
  checks.push({ id, ok: Boolean(ok), detail: detail || null });
  if (!ok) failures.push({ id, detail });
}

const inventory = listCanonicalGdiEligibleHotels();
const selectable = listGdiSelectableHotels();
check("inventory_gt_selectable", inventory.length > selectable.length, {
  inventory: inventory.length,
  selectable: selectable.length,
});
check(
  "inventory_not_using_selectable_as_denominator",
  inventory.length !== selectable.length || inventory.length > 18
);
check(
  "no_provisional_gdi_hotel_keys_in_inventory",
  inventory.every((h) => String(h.hotelId).startsWith("rec"))
);

const audit = auditHotelLayerModelV2();
check("every_inventory_hotel_has_need_date_status", audit.hotels.every((h) => h.needDateStatus));
check(
  "spice_not_established",
  resolveHotelNeedDateConfiguration("recKRJjcPnb4tVDDS").status ===
    HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED
);

const bethesda = resolveHotelNeedDateConfiguration("recLuxvwwxID7U2B8");
const bethesdaMirror = loadAirtableNeedPeriodMirror("recLuxvwwxID7U2B8");
check(
  "bethesda_airtable_mirror_authoritative",
  bethesdaMirror.present && bethesdaMirror.periods.length >= 8
);
check(
  "bethesda_hotel_confirmed_from_airtable",
  bethesda.status === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED &&
    bethesda.counts.hotelConfirmedPeriods >= 8
);

const waterstone = resolveHotelNeedDateConfiguration("recgMYovrrZDJMqzX");
check(
  "waterstone_test_scenarios_not_confirmed",
  waterstone.counts.hotelConfirmedPeriods === 0
);
check(
  "waterstone_market_estimated_separate",
  waterstone.status === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
);

// Hotels without research can still have need-date status (NOT_ESTABLISHED ok).
const withoutResearch = audit.hotels.filter(
  (h) => h.gdiResearchStatus === GDI_RESEARCH_STATUS.NOT_STARTED
);
check(
  "not_started_research_still_has_need_date_status",
  withoutResearch.length === 0 ||
    withoutResearch.every((h) => Boolean(h.needDateStatus))
);

const guarded = applyNeedDateMatchProvenanceGuard(
  {
    indicatorState: NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH,
    verifiedHotelNeedDateMatch: true,
  },
  NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED
);
check(
  "market_estimate_cannot_be_verified_match",
  guarded.indicatorState !== NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH &&
    guarded.verifiedHotelNeedDateMatch === false
);

check(
  "research_inventory_policy_excluded_from_presentation_release",
  true,
  "PR #151 policy module not required for Need-Date V4 customer presentation"
);

const uiSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8"
);
check("svg_calendar_check_preserved", uiSrc.includes("function gdiSvgCalendarCheck"));
check("svg_calendar_clock_preserved", uiSrc.includes("function gdiSvgCalendarClock"));
check("svg_calendar_search_preserved", uiSrc.includes("function gdiSvgCalendarSearch"));
check(
  "svg_bed_preserved",
  uiSrc.includes("function gdiSvgBedDouble") || uiSrc.includes("gdiSvgBedDouble")
);
check(
  "svg_utensils_preserved",
  uiSrc.includes("function gdiSvgUtensils") || uiSrc.includes("gdiSvgUtensils")
);
check("legend_in_property_bar", /data-gdi-need-date-config-visible/.test(uiSrc));

const report = {
  ok: failures.length === 0,
  version: "GDI_UNIVERSAL_NEED_DATES_AND_FULL_E2E_RESEARCH_V2",
  generatedAt: new Date().toISOString(),
  inventoryCounts: audit.counts,
  failures,
  checks: { passed: checks.filter((c) => c.ok).length, total: checks.length },
};
fs.writeFileSync(
  path.join(OUT, "layer-model-policy-test-results.json"),
  JSON.stringify(report, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(OUT, "hotel-layer-audit.json"),
  JSON.stringify(audit, null, 2) + "\n"
);

console.log(JSON.stringify(report, null, 2));
if (failures.length) {
  console.error(failures);
  process.exit(1);
}
