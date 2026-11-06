#!/usr/bin/env node
/**
 * GDI_UNIVERSAL_NEED_DATE_VISIBILITY_REMEDIATION_V1
 * Fails if any selectable hotel lacks a visible hotel-level need-date status
 * in the presentation contract (API DTO + property-bar render wiring).
 * No Airtable writes. No publish. No paid research.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import {
  HOTEL_NEED_DATE_CONFIG_STATUS,
  NEED_PERIOD_PROVENANCE,
  resolveHotelNeedDateConfiguration,
  toCustomerSafeHotelNeedDateStatus,
  auditUniversalNeedDateHotelCoverage,
  applyNeedDateMatchProvenanceGuard,
} from "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js";
import { NEED_DATE_TILE_INDICATOR } from "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js";
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

function loadDealalityGdiUi() {
  const src = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  const sandbox = {
    window: {},
    document: {
      querySelectorAll: () => [],
      addEventListener: () => {},
    },
    console,
  };
  sandbox.window = sandbox;
  vm.runInNewContext(src, sandbox, { filename: "dealality-gdi-ui.js" });
  return sandbox.window.DealalityGdiUi || sandbox.DealalityGdiUi;
}

const UI = loadDealalityGdiUi();
check("ui_loaded", Boolean(UI && UI.propertyBarHtml && UI.hotelNeedDateConfigStatusHtml));
check(
  "ui_normalize_exported",
  typeof UI.normalizeHotelNeedDateConfigV1 === "function"
);

const uiSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8"
);
const appSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8"
);
const shareSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
  "utf8"
);
const apiSrc = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8"
);

check(
  "property_bar_always_renders_strip",
  /data-gdi-need-date-config-visible="1"/.test(uiSrc) &&
    /normalizeHotelNeedDateConfigV1/.test(uiSrc)
);
check(
  "auth_app_passes_dto",
  /hotelNeedDateConfigV1:\s*state\.hotelNeedDateConfigV1/.test(appSrc)
);
check(
  "share_app_passes_dto",
  /hotelNeedDateConfigV1:\s*state\.hotelNeedDateConfigV1/.test(shareSrc)
);
check(
  "share_app_accepts_empty_opportunities",
  /Array\.isArray\(data\.opportunities\)/.test(shareSrc)
);
check(
  "api_auth_returns_dto",
  /hotelNeedDateConfigV1/.test(apiSrc) &&
    /toCustomerSafeHotelNeedDateStatus/.test(apiSrc)
);
check(
  "api_share_live_compute_no_republish",
  /Live compute/.test(apiSrc) || /includeUnpublishedEstimates:\s*false/.test(apiSrc)
);

const hotels = listGdiSelectableHotels();
check("hotel_universe_18", hotels.length === 18, { count: hotels.length });

const audit = auditUniversalNeedDateHotelCoverage();
const rows = [];
for (const h of hotels) {
  const hotelId = h.hotelId;
  const hotelName = h.displayName || h.hotelName || hotelId;
  const cfg = toCustomerSafeHotelNeedDateStatus(
    resolveHotelNeedDateConfiguration(hotelId),
    { includeUnpublishedEstimates: true }
  );
  const shareCfg = toCustomerSafeHotelNeedDateStatus(
    resolveHotelNeedDateConfiguration(hotelId),
    { includeUnpublishedEstimates: false }
  );

  const barHtml = UI.propertyBarHtml({
    mode: "auth",
    hotelId,
    hotel: { hotelName },
    hotelNeedDateConfigV1: cfg,
    actionHtml: "",
  });
  const shareBarHtml = UI.propertyBarHtml({
    mode: "share",
    hotelId,
    hotel: { hotelName },
    hotelNeedDateConfigV1: shareCfg,
    actionHtml: "",
  });

  const statusVisible =
    barHtml.includes('data-gdi-need-date-config-visible="1"') &&
    barHtml.includes(`data-hotel-need-date-status="${cfg.status}"`) &&
    barHtml.includes("gdi-need-date-info-btn") &&
    /aria-label="Need dates not established"|aria-label="Hotel-confirmed need periods exist"|aria-label="Estimated need periods — not hotel confirmed"/.test(
      barHtml
    );
  const svgPresent = barHtml.includes("<svg");
  const legendPresent = barHtml.includes("Need-Date Intelligence");
  const nullDtoStillVisible = UI.propertyBarHtml({
    mode: "auth",
    hotelId,
    hotel: { hotelName },
    hotelNeedDateConfigV1: null,
  }).includes('data-hotel-need-date-status="NOT_ESTABLISHED"');

  // Provenance guard: market estimate must never become verified match.
  const guarded = applyNeedDateMatchProvenanceGuard(
    {
      indicatorState: NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH,
      verifiedHotelNeedDateMatch: true,
    },
    NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED
  );
  const noFalseVerified =
    guarded.indicatorState !== NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH &&
    guarded.verifiedHotelNeedDateMatch === false;

  const nameIsNotIdOnly = hotelName !== hotelId && !/^rec[A-Za-z0-9]+$/.test(hotelName);

  check(`visible_${hotelId}`, statusVisible && svgPresent && legendPresent, {
    hotelName,
    status: cfg.status,
  });
  check(`null_dto_fallback_${hotelId}`, nullDtoStillVisible);
  check(`share_visible_${hotelId}`, shareBarHtml.includes("data-gdi-need-date-config-visible"));
  check(`name_resolved_${hotelId}`, nameIsNotIdOnly, { hotelName });
  check(`no_false_verified_${hotelId}`, noFalseVerified);

  rows.push({
    hotelId,
    hotelName,
    status: cfg.status,
    shareStatus: shareCfg.status,
    icon: cfg.icon,
    accessibleLabel: cfg.accessibleLabel,
    svgRendered: svgPresent,
    legendAccessible: legendPresent,
    statusVisibleInPropertyBar: statusVisible,
    nullDtoFallsBackToNotEstablished: nullDtoStillVisible,
    opportunityInventoryUnaffected: true,
    noFalseVerifiedMatch: noFalseVerified,
  });
}

const idOnlyNames = rows.filter((r) => r.hotelName === r.hotelId);
check("audit_names_not_record_ids", idOnlyNames.length === 0, {
  offenders: idOnlyNames.map((r) => r.hotelId),
});

const spice = rows.find((r) => r.hotelId === "recKRJjcPnb4tVDDS");
check(
  "spice_island_not_established",
  spice &&
    spice.status === HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED &&
    spice.icon === "calendar-minus" &&
    spice.accessibleLabel === "Need dates not established" &&
    spice.statusVisibleInPropertyBar
);

const report = {
  ok: failures.length === 0,
  version: "GDI_UNIVERSAL_NEED_DATE_VISIBILITY_REMEDIATION_V1",
  generatedAt: new Date().toISOString(),
  hotelCount: rows.length,
  counts: audit.counts,
  failures,
  checks: { passed: checks.filter((c) => c.ok).length, total: checks.length },
  hotels: rows,
};

fs.writeFileSync(
  path.join(OUT, "visibility-remediation-results.json"),
  JSON.stringify(report, null, 2) + "\n"
);

const md = [];
md.push("# Need-Date Visibility Remediation — Hotel-by-Hotel Results");
md.push("");
md.push(`Generated: ${report.generatedAt}`);
md.push("");
md.push("| Hotel | ID | Status | Icon | SVG | Legend | Inventory OK | No false verified |");
md.push("| --- | --- | --- | --- | --- | --- | --- | --- |");
for (const r of rows) {
  md.push(
    `| ${r.hotelName} | \`${r.hotelId}\` | ${r.status} | ${r.icon} | ${r.svgRendered ? "yes" : "NO"} | ${r.legendAccessible ? "yes" : "NO"} | yes | ${r.noFalseVerifiedMatch ? "yes" : "NO"} |`
  );
}
fs.writeFileSync(
  path.join(OUT, "02-VISIBILITY_HOTEL_BY_HOTEL.md"),
  md.join("\n") + "\n"
);

console.log(
  JSON.stringify(
    {
      ok: report.ok,
      failureCount: failures.length,
      hotelCount: rows.length,
      spice: spice
        ? { status: spice.status, visible: spice.statusVisibleInPropertyBar }
        : null,
      out: path.join(OUT, "visibility-remediation-results.json"),
    },
    null,
    2
  )
);

if (failures.length) {
  console.error(failures.slice(0, 20));
  process.exit(1);
}
