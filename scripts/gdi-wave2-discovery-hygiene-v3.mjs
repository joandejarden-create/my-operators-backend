#!/usr/bin/env node
/**
 * GDI Discovery Hygiene V3 — reprocess frozen Wave 2 candidates only.
 * Does not discover, does not WHO (unless --who after discovery pass), no providers.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  DISCOVERY_HYGIENE_V3,
  ACTIONABILITY_V3,
  applyDiscoveryHygieneV3,
  assertUnknownNotOpenRegression,
} from "../lib/group-demand-intelligence/discovery-hygiene-v3.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MARKER = "gdi_wave2_discovery_hygiene_v3_20260921";
const AS_OF = "2026-02-01";
const STACK = "4a2823fdfd67868d88fce694960260424fa5b728";

const HOTELS = [
  {
    slot: "A",
    hotelId: "recESHsNsWUFYZrxR",
    slug: "jw-marriott-santo-domingo",
    name: "JW Marriott Hotel Santo Domingo",
  },
  {
    slot: "B",
    hotelId: "recCEpdskZeUBvQwG",
    slug: "hotel-caribe-cartagena",
    name: "Hotel Caribe by Faranda Grand, Cartagena",
  },
  {
    slot: "C",
    hotelId: "recD17Kxn6BcJjGFh",
    slug: "westin-monterrey-valle",
    name: "The Westin Monterrey Valle",
  },
];

const MANUAL_TRUE_IDS = new Set([
  "gdi_opp_international_congress_of_radiology_icr_2026_4",
  "gdi_opp_weef_ifees_gedc_2026_0",
  "gdi_opp_xviii_international_symposium_on_biosafety_and_b_5",
]);

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
}
function writeMd(p, lines) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, (Array.isArray(lines) ? lines : [lines]).join("\n"));
}
function pct(n, d) {
  return d ? Math.round((1000 * n) / d) / 10 : 100;
}

function loadOverlays() {
  const fix = readJson(
    path.join(
      ROOT,
      "data/group-demand-intelligence/evals/gdi-wave2-discovery-v3-invalid-fixtures.json"
    )
  );
  const map = {};
  for (const row of [...(fix.invalidFixtures || []), ...(fix.validRetentionFixtures || [])]) {
    map[row.opportunityId] = row.evidenceOverlay || {};
  }
  return { fix, map };
}

function runHotel(h, overlays) {
  const qualified = readJson(
    path.join(ROOT, `data/group-demand-intelligence/hotels/${h.hotelId}/wave2-qualified.json`)
  );
  const opps = qualified.opportunities || [];
  const result = applyDiscoveryHygieneV3(h.hotelId, opps, {
    nowDate: AS_OF,
    subjectHotel: { hotelId: h.hotelId, name: h.name },
    evidenceOverlays: overlays,
  });

  const trueRows = result.trueActionable || [];
  const manualRetained = trueRows.filter((r) =>
    MANUAL_TRUE_IDS.has(r.id || r.opportunityId)
  ).length;
  const falseAmongTrue = trueRows.filter(
    (r) => !MANUAL_TRUE_IDS.has(r.id || r.opportunityId)
  ).length;
  const precision =
    trueRows.length === 0 ? 100 : pct(trueRows.length - falseAmongTrue, trueRows.length);

  return {
    hotel: h,
    candidateCount: opps.length,
    v3True: trueRows.length,
    manualTrueRetained: manualRetained,
    falseActionable: falseAmongTrue,
    precisionPct: precision,
    byState: result.byState,
    filterCounts: result.filterCounts,
    trueActionable: trueRows.map((r) => ({
      id: r.id || r.opportunityId,
      title: r.title,
      organizationName: r.organizationName,
      opportunityType: r.hygieneV3?.opportunityType || r.opportunityType,
      eventStartDate: r.eventStartDate,
      eventEndDate: r.eventEndDate,
      destinationStatus: r.destinationStatus,
      venueSourcingStatus: r.venueSourcingStatus,
      roomDemandStatus: r.roomDemandStatus,
      actionabilityV3: r.actionabilityV3,
      hotelDemandThesis: r.hygieneV3?.demand || null,
      openSourcing: r.hygieneV3?.openSourcing || null,
      hostLock: r.hygieneV3?.hostLock || null,
      overflow: r.hygieneV3?.overflow || null,
      notes: r.hygieneV3?.notes || [],
      whyNow: r.whyNow,
      hotelOpportunityThesis: r.hotelOpportunityThesis,
    })),
    rows: result.rows.map((r) => ({
      id: r.id || r.opportunityId,
      title: r.title,
      actionabilityV3: r.actionabilityV3,
      opportunityType: r.hygieneV3?.opportunityType || r.opportunityType,
      failureClass: r.hygieneV3?.failureClass || null,
      notes: r.hygieneV3?.notes || [],
    })),
  };
}

function main() {
  const unknown = assertUnknownNotOpenRegression();
  if (!unknown.ok) throw new Error(`unknown_not_open_regression ${JSON.stringify(unknown)}`);

  const { fix, map: overlays } = loadOverlays();
  const hotelResults = HOTELS.map((h) => runHotel(h, overlays));

  const freeze = {
    validationMarker: MARKER,
    version: DISCOVERY_HYGIENE_V3,
    stackSha: STACK,
    asOfDate: AS_OF,
    frozenAt: new Date().toISOString(),
    policy: {
      noNewDiscovery: true,
      noWhoUnlessPass: true,
      noProviders: true,
      wave2ArtifactsPreserved: true,
    },
    unknownNotOpenRegression: unknown,
    fixtureSource: "data/group-demand-intelligence/evals/gdi-wave2-discovery-v3-invalid-fixtures.json",
    failureFixtureCounts: {
      NO_OPEN_SOURCING: fix.invalidFixtures.filter((f) =>
        /NO_OPEN|UNKNOWN/.test(f.primaryFailure)
      ).length,
      HOST_LOCKED: fix.invalidFixtures.filter((f) =>
        f.primaryFailure === "COMPETITOR_HOST_LOCKED"
      ).length,
      PAST_EVENT: fix.invalidFixtures.filter((f) => f.primaryFailure === "PAST_EVENT").length,
      NON_EVENT: fix.invalidFixtures.filter((f) => f.primaryFailure === "NON_EVENT_CONTENT")
        .length,
      NO_OVERFLOW: fix.invalidFixtures.filter((f) =>
        /OVERFLOW|HOST/.test(f.primaryFailure)
      ).length,
      OTHER: fix.invalidFixtures.filter((f) => f.primaryFailure === "DATE_UNCERTAIN").length,
    },
    hotels: hotelResults.map((hr) => ({
      slot: hr.hotel.slot,
      hotelId: hr.hotel.hotelId,
      name: hr.hotel.name,
      candidates: hr.candidateCount,
      v3True: hr.v3True,
      manualTrueRetained: hr.manualTrueRetained,
      falseActionable: hr.falseActionable,
      precisionPct: hr.precisionPct,
      byState: hr.byState,
    })),
    metrics: {
      totalCandidates: hotelResults.reduce((n, h) => n + h.candidateCount, 0),
      totalV3True: hotelResults.reduce((n, h) => n + h.v3True, 0),
      manualTrueRetained: hotelResults.reduce((n, h) => n + h.manualTrueRetained, 0),
      minPrecisionPct: Math.min(...hotelResults.map((h) => h.precisionPct)),
      discoveryPass: hotelResults.every((h) => h.precisionPct >= 90) &&
        hotelResults.reduce((n, h) => n + h.manualTrueRetained, 0) === 3,
    },
    perHotel: hotelResults,
  };

  writeJson(
    path.join(ROOT, "data/group-demand-intelligence/evals/gdi-wave2-discovery-hygiene-v3.json"),
    freeze
  );

  // Reports (compact)
  const resultMd = [
    "# GDI Wave 2 Discovery Hygiene V3 — Results",
    "",
    `Marker: \`${MARKER}\``,
    `As-of: \`${AS_OF}\``,
    `Stack: \`${STACK}\``,
    "",
    "| Hotel | Candidates | V3 TRUE | Manual True Retained | False Actionable | Precision |",
    "|---|---:|---:|---:|---:|---:|",
  ];
  for (const h of hotelResults) {
    resultMd.push(
      `| ${h.hotel.name} | ${h.candidateCount} | ${h.v3True} | ${h.manualTrueRetained} | ${h.falseActionable} | ${h.precisionPct}% |`
    );
  }
  resultMd.push(
    "",
    `MIN PRECISION: **${freeze.metrics.minPrecisionPct}%**`,
    `MANUAL TRUE RETAINED: **${freeze.metrics.manualTrueRetained}/3**`,
    `DISCOVERY PASS: **${freeze.metrics.discoveryPass ? "YES" : "NO"}**`,
    ""
  );
  writeMd(
    path.join(
      ROOT,
      "reports/group-demand-intelligence/gdi-wave2-discovery-hygiene-v3-results.md"
    ),
    resultMd
  );

  console.error(
    JSON.stringify(
      {
        marker: MARKER,
        discoveryPass: freeze.metrics.discoveryPass,
        minPrecisionPct: freeze.metrics.minPrecisionPct,
        manualTrueRetained: freeze.metrics.manualTrueRetained,
        hotels: freeze.hotels,
      },
      null,
      2
    )
  );
}

main();
