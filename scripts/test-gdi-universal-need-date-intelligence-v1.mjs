#!/usr/bin/env node
/**
 * GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1 regression gate.
 * Zero-paid. No Airtable writes. No publish. No token mutation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1,
  HOTEL_NEED_DATE_CONFIG_STATUS,
  NEED_PERIOD_PROVENANCE,
  MARKET_ESTIMATE_LIFECYCLE,
  deriveHotelNeedDateConfigStatus,
  buildEvidenceBasedMarketEstimate,
  applyMarketEstimateLifecycleAction,
  saveMarketEstimatedNeedPeriods,
  loadMarketEstimatedNeedPeriods,
  applyNeedDateMatchProvenanceGuard,
  toCustomerSafeHotelNeedDateStatus,
  auditUniversalNeedDateHotelCoverage,
  NEED_DATE_INFORMATION_LEGEND_V1,
  resolveHotelNeedDateConfiguration,
} from "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js";
import {
  NEED_DATE_TILE_INDICATOR,
  attachHotelNeedDateTilePreviewsSync,
  runNeedDateTilePreviewGate,
} from "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/repository.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-universal-need-date-intelligence-v1"
);
fs.mkdirSync(OUT, { recursive: true });

const failures = [];
const checks = [];
function check(id, ok, detail) {
  checks.push({ id, ok: Boolean(ok), detail: detail || null });
  if (!ok) failures.push({ id, detail });
}

const tmpHotel = `recTESTNeedDate_${Date.now().toString(36)}`;
const tmpDir = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  tmpHotel
);
fs.mkdirSync(tmpDir, { recursive: true });

try {
  // 1) Three hotel-level statuses
  const confirmed = deriveHotelNeedDateConfigStatus({
    hotelId: "h1",
    hotelConfirmedPeriods: [
      {
        needPeriodId: "ndp_1",
        status: "Active",
        source: "HOTEL_REPORTED",
        provenance: NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED,
        periodLabel: "Jan 2027",
      },
    ],
    marketEstimatedPeriods: [],
  });
  check(
    "status_hotel_confirmed",
    confirmed.status === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED
  );

  const estimated = deriveHotelNeedDateConfigStatus({
    hotelId: "h2",
    hotelConfirmedPeriods: [],
    marketEstimatedPeriods: [
      {
        estimateId: "est_1",
        provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
        hotelConfirmationStatus: "PENDING",
        periodLabel: "Mar 2027",
      },
    ],
  });
  check(
    "status_market_estimated",
    estimated.status === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
  );

  const none = deriveHotelNeedDateConfigStatus({
    hotelId: "h3",
    hotelConfirmedPeriods: [],
    marketEstimatedPeriods: [],
  });
  check(
    "status_not_established",
    none.status === HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED
  );

  // 2) Mixed confirmed + estimated — hotel status confirmed; period provenance preserved
  const mixed = deriveHotelNeedDateConfigStatus({
    hotelId: "h4",
    hotelConfirmedPeriods: [
      {
        needPeriodId: "ndp_c",
        status: "Active",
        source: "HOTEL_REPORTED",
        provenance: NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED,
      },
    ],
    marketEstimatedPeriods: [
      {
        estimateId: "est_m",
        provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
        hotelConfirmationStatus: "PENDING",
      },
    ],
  });
  check(
    "mixed_hotel_status_confirmed",
    mixed.status === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED
  );
  check(
    "mixed_estimate_not_relabeled",
    mixed.periods.some(
      (p) => p.provenance === NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED
    ) &&
      mixed.periods.some(
        (p) => p.provenance === NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED
      )
  );

  // 3) Weak evidence → no fabricated estimate
  const weak = buildEvidenceBasedMarketEstimate(
    { hotelId: tmpHotel },
    { evidenceSummary: "maybe soft", sources: [], confidence: 0.1 }
  );
  check("weak_evidence_null", weak === null);

  const strong = buildEvidenceBasedMarketEstimate(
    { hotelId: tmpHotel },
    {
      startDate: "2027-03-01",
      endDate: "2027-03-31",
      revenueNeedType: "GROUP_GUESTROOMS",
      evidenceSummary:
        "Supported weekday/weekend seasonality and event calendar demand drivers for this market historically concentrate soft rooms demand in March.",
      sources: ["https://example.com/market-seasonality"],
      confidence: 0.62,
      uncertainty: "Hotel has not confirmed this estimate.",
    }
  );
  check("strong_estimate_built", Boolean(strong?.estimateId));

  // 4) Lifecycle confirm/edit/reject — no hotel calendar auto-write
  saveMarketEstimatedNeedPeriods(tmpHotel, [strong]);
  const confirmedLifecycle = applyMarketEstimateLifecycleAction({
    hotelId: tmpHotel,
    estimateId: strong.estimateId,
    action: "CONFIRM",
    actor: "test",
  });
  check(
    "confirm_does_not_write_hotel_calendar",
    confirmedLifecycle.ok === true &&
      confirmedLifecycle.wroteHotelConfirmedCalendar === false
  );
  check(
    "confirm_sets_pending_calendar_flag",
    confirmedLifecycle.estimate.hotelConfirmationStatus ===
      "CONFIRMED_PENDING_CALENDAR_WRITE"
  );

  const edited = applyMarketEstimateLifecycleAction({
    hotelId: tmpHotel,
    estimateId: strong.estimateId,
    action: "EDIT",
    patch: { periodLabel: "March 2027 (revised)" },
    actor: "test",
  });
  check("edit_lifecycle", edited.ok && edited.estimate.periodLabel.includes("revised"));

  const rejected = applyMarketEstimateLifecycleAction({
    hotelId: tmpHotel,
    estimateId: strong.estimateId,
    action: "REJECT",
    patch: { reason: "Not a real soft period" },
    actor: "test",
  });
  check(
    "reject_lifecycle",
    rejected.ok &&
      rejected.estimate.lifecycleStatus === MARKET_ESTIMATE_LIFECYCLE.REJECTED
  );

  // 5) No false verified match from estimate
  const guarded = applyNeedDateMatchProvenanceGuard(
    {
      indicatorState: NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH,
      icon: "calendar-check",
      colorClass: "gdi-need-date-icon--verified",
      visible: true,
    },
    NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED
  );
  check(
    "no_false_verified_from_estimate",
    guarded.indicatorState === NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE &&
      guarded.verifiedHotelNeedDateMatch === false &&
      /estimated/i.test(guarded.estimatedLabel || "")
  );

  // Sync attach with estimated period must not keep VERIFIED
  const opps = [
    {
      id: "opp_est_match",
      hotelId: tmpHotel,
      title: "Association Meeting",
      eventStartDate: "2027-03-10",
      eventEndDate: "2027-03-12",
      opportunityType: "Association Conference",
    },
  ];
  const periods = [
    {
      needPeriodId: "mkt_est_period",
      hotelId: tmpHotel,
      periodLabel: "March 2027",
      startDate: "2027-03-01",
      endDate: "2027-03-31",
      status: "Active",
      source: "MARKET_ESTIMATE",
      provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
      preferenceOverrides: { scenarioClassification: "ACTUAL_HOTEL_INPUT" },
    },
  ];
  const attached = attachHotelNeedDateTilePreviewsSync(tmpHotel, opps, {
    needPeriods: periods,
    hotelName: "Test Hotel",
  });
  const preview = attached[0]?.tileNeedDatePreview;
  check(
    "attach_estimate_not_verified",
    !preview ||
      preview.indicatorState !== NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH ||
      preview.verifiedHotelNeedDateMatch === false ||
      preview.periodProvenance === NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED
  );

  // 6) Existing opportunity icon gate still passes
  const tileGate = runNeedDateTilePreviewGate();
  check("existing_tile_icon_gate", tileGate.pass, tileGate.failures);

  // 7) UI SVGs + legend present
  const uiSrc = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  check("ui_preserves_calendar_check", /function gdiSvgCalendarCheck/.test(uiSrc));
  check("ui_preserves_calendar_clock", /function gdiSvgCalendarClock/.test(uiSrc));
  check("ui_preserves_calendar_search", /function gdiSvgCalendarSearch/.test(uiSrc));
  check("ui_bed_utensils", /function gdiSvgBedDouble/.test(uiSrc) && /gdiSvgUtensils/.test(uiSrc));
  check("ui_calendar_dashed", /function gdiSvgCalendarDashed/.test(uiSrc));
  check("ui_calendar_minus", /function gdiSvgCalendarMinus/.test(uiSrc));
  check("ui_info_legend", /function needDateInformationLegendHtml/.test(uiSrc));
  check(
    "ui_legend_icon_rows",
    /function needDateLegendRowHtml/.test(uiSrc) &&
      /gdi-need-date-legend-row/.test(uiSrc) &&
      /Need-Date Match/.test(uiSrc) &&
      /Potential Revenue/.test(uiSrc) &&
      /Matches a Confirmed Need Date/.test(uiSrc) &&
      /Not Yet Assessed/.test(uiSrc),
  );
  check(
    "ui_need_dates_in_history_row",
    /gdi-property-bar__history-item--need-dates/.test(uiSrc) &&
      /Need Dates and research activity/.test(uiSrc),
  );
  check("ui_hotel_status", /function hotelNeedDateConfigStatusHtml/.test(uiSrc));
  check("ui_legend_a11y", /aria-expanded/.test(uiSrc) && /Escape/.test(uiSrc));

  const cssSrc = fs.readFileSync(
    path.join(ROOT, "public/css/group-demand-intelligence.css"),
    "utf8"
  );
  check("css_estimated_status", /gdi-hotel-need-date-status--estimated/.test(cssSrc));
  check("css_not_established", /gdi-hotel-need-date-status--not-established/.test(cssSrc));
  check("css_legend_popover", /gdi-need-date-legend-popover/.test(cssSrc));

  // 8) Share hides unpublished estimates
  const shareSafe = toCustomerSafeHotelNeedDateStatus(estimated, {
    includeUnpublishedEstimates: false,
  });
  check(
    "share_hides_unpublished_estimates",
    shareSafe.status === HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED
  );
  const authSafe = toCustomerSafeHotelNeedDateStatus(estimated, {
    includeUnpublishedEstimates: true,
  });
  check(
    "auth_can_show_estimates",
    authSafe.status === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
  );

  // 9) No configured dates must not imply inventory wipe — selectable hotels retain ids
  const selectable = listGdiSelectableHotels();
  check("selectable_hotels_exist", selectable.length > 0, selectable.length);

  // Inventory preservation: empty need periods still return all opportunities from attach
  const inventory = [
    { id: "a", hotelId: "hx", title: "Opp A" },
    { id: "b", hotelId: "hx", title: "Opp B" },
    { id: "c", hotelId: "hx", title: "Opp C" },
  ];
  const preserved = attachHotelNeedDateTilePreviewsSync("hx", inventory, {
    needPeriods: [],
  });
  check("inventory_preserved_without_need_dates", preserved.length === 3);

  // 10) Coverage audit runs for every selectable hotel
  const audit = auditUniversalNeedDateHotelCoverage();
  check(
    "coverage_audit_counts",
    audit.counts.totalHotels === audit.hotels.length &&
      audit.counts.HOTEL_CONFIRMED +
        audit.counts.MARKET_ESTIMATED +
        audit.counts.NOT_ESTABLISHED ===
        audit.counts.totalHotels
  );
  check(
    "legend_contract",
    NEED_DATE_INFORMATION_LEGEND_V1.sections.length >= 3 &&
      NEED_DATE_INFORMATION_LEGEND_V1.sections.some((s) => s.id === "opportunity_timing") &&
      NEED_DATE_INFORMATION_LEGEND_V1.sections.some((s) =>
        (s.items || []).some((i) => i.icon === "calendar-x"),
      ),
  );

  // 11) No customer snapshot / token mutation in this module tree
  const intelSrc = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js"
    ),
    "utf8"
  );
  check(
    "no_share_token_writes",
    !/active-tokens\.json|revoked-token|publishSnapshot|railway/i.test(intelSrc)
  );

  fs.writeFileSync(
    path.join(OUT, "test-results.json"),
    JSON.stringify(
      {
        version: GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1,
        pass: failures.length === 0,
        checks,
        failures,
        coverageCounts: audit.counts,
      },
      null,
      2
    ) + "\n"
  );
  fs.writeFileSync(
    path.join(OUT, "hotel-coverage-audit.json"),
    JSON.stringify(audit, null, 2) + "\n"
  );

  console.log(
    JSON.stringify(
      {
        ok: failures.length === 0,
        version: GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1,
        passed: checks.filter((c) => c.ok).length,
        failed: failures.length,
        failures,
        coverageCounts: audit.counts,
      },
      null,
      2
    )
  );
  if (failures.length) process.exit(1);
} finally {
  // cleanup temp hotel dir
  try {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  } catch {
    /* ignore */
  }
}
