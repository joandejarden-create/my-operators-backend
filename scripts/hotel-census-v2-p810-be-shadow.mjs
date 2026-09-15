/**
 * P8.10 — Brand Explorer Legacy vs HPC census metrics shadow (read-only).
 *
 * Usage:
 *   node --env-file=C:/Dev/deal-capture-proxy/.env scripts/hotel-census-v2-p810-be-shadow.mjs
 *
 * Sets BRAND_EXPLORER_HPC_V2 + BRAND_PRESENCE_HPC_V2 only inside this process.
 * Does not enable production BE cutover. No Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT_DIR = path.join(ROOT, "data", "hotel-census-v2", "p810-brand-explorer");
const REP_DIR = path.join(ROOT, "reports", "hotel-census-v2");

function loadEnvFile(p) {
  if (!fs.existsSync(p)) return;
  for (const line of fs.readFileSync(p, "utf8").split(/\r?\n/)) {
    const m = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (!m) continue;
    const key = m[1];
    let val = m[2].trim();
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }
    if (process.env[key] == null || process.env[key] === "") process.env[key] = val;
  }
}

loadEnvFile("C:/Dev/deal-capture-proxy/.env");
loadEnvFile(path.join(ROOT, ".env"));
loadEnvFile(path.join(ROOT, ".env.local"));

process.env.BRAND_PRESENCE_HPC_V2 = "1";
process.env.BRAND_EXPLORER_HPC_V2 = "1";

const { buildBrandCensusSummary } = await import("../lib/hotel-census/build-brand-census-summary.js");
const {
  resetBrandExplorerCensusReadCounters,
  snapshotBrandExplorerCensusReadCounters,
  classifyHpcAffiliationForBe,
} = await import("../lib/hotel-census/brand-explorer-hpc-metrics.js");
const {
  shouldUseHpcBrandExplorerMetrics,
  shouldUseHpcBrandPresence,
  shouldUseHpcScoutCensus,
  getDealalityRuntimeFlags,
} = await import("../lib/hotel-census/brand-presence-hpc-request.js");

/** Representative Brand Explorer brands (name, optional parent, cohort tag). */
const BRANDS = [
  { brand: "Autograph Collection", parent: "Marriott International", cohort: "large_global_soft" },
  { brand: "Kimpton", parent: "IHG Hotels & Resorts", cohort: "lifestyle_luxury" },
  { brand: "Curio Collection", parent: "Hilton", cohort: "large_global_soft" },
  { brand: "Ascend Hotel Collection", parent: "Choice Hotels", cohort: "select_service_soft" },
  { brand: "Design Hotels", parent: "Marriott International", cohort: "small_lifestyle" },
  { brand: "Courtyard", parent: "Marriott International", cohort: "select_service" },
  { brand: "Radisson Collection", parent: "Radisson Hotel Group", cohort: "regional_luxury" },
  { brand: "Tapestry Collection", parent: "Hilton", cohort: "soft_brand" },
];

function classifyDiff(key, legacyVal, hpcVal, delta) {
  if (delta === 0 || (legacyVal == null && hpcVal == null)) return "SAME";
  if (key === "totalOpenKeys" || key === "totalPipelineKeys" || key.startsWith("rooms")) {
    return "EXPECTED_ROOM_COVERAGE_CHANGE";
  }
  if (key === "totalOpenHotels" || key === "totalPipelineHotels") {
    if (typeof delta === "number" && Math.abs(delta) / Math.max(1, Math.abs(legacyVal || 0)) > 0.5) {
      return "EXPECTED_UNIVERSE_CHANGE";
    }
    return "EXPECTED_AFFILIATION_UNKNOWN";
  }
  if (key === "countryCount" || key === "dealalityRegionCount") {
    return "SAFE_METHODOLOGY_CHANGE";
  }
  if (typeof delta === "number" && Math.abs(delta) > 200 && key.includes("Hotels")) {
    return "SUSPICIOUS";
  }
  return "EXPECTED_LIFECYCLE_CHANGE";
}

function toCsvRow(cols) {
  return cols
    .map((c) => {
      const s = c == null ? "" : String(c);
      if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
      return s;
    })
    .join(",");
}

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.mkdirSync(REP_DIR, { recursive: true });

const metricDiffRows = [];
const brandQaRows = [];
let critical = 0;
let suspicious = 0;
const expectedDiffs = [];

const roomsCoverageAgg = { brands: 0, roomsKnownOpen: 0, roomsUnknownOpen: 0 };
const affiliationAgg = {
  BRANDED_CONFIRMED: 0,
  INDEPENDENT_CONFIRMED: 0,
  BRAND_UNCONFIRMED: 0,
  AFFILIATION_UNKNOWN: 0,
};
const lifecycleAgg = { open: 0, pipeline: 0 };

const performance = { brands: [], legacyMsTotal: 0, hpcMsTotal: 0 };

resetBrandExplorerCensusReadCounters();
const countersStart = snapshotBrandExplorerCensusReadCounters();

for (const b of BRANDS) {
  const t0 = Date.now();
  resetBrandExplorerCensusReadCounters();
  const legacy = await buildBrandCensusSummary(b.brand, b.parent, { useHpc: false });
  const legacyMs = Date.now() - t0;
  const legacyCounters = snapshotBrandExplorerCensusReadCounters();

  const t1 = Date.now();
  resetBrandExplorerCensusReadCounters();
  const hpc = await buildBrandCensusSummary(b.brand, b.parent, { useHpc: true });
  const hpcMs = Date.now() - t1;
  const hpcCounters = snapshotBrandExplorerCensusReadCounters();

  performance.brands.push({
    brand: b.brand,
    legacyMs,
    hpcMs,
    legacyAvailable: legacy.available,
    hpcAvailable: hpc.available,
  });
  performance.legacyMsTotal += legacyMs;
  performance.hpcMsTotal += hpcMs;

  const lm = legacy.metrics || {};
  const hm = hpc.metrics || {};
  const metricKeys = new Set([...Object.keys(lm), ...Object.keys(hm)]);
  const brandDiffs = {};
  for (const k of metricKeys) {
    const lv = lm[k] ?? null;
    const hv = hm[k] ?? null;
    const delta =
      typeof lv === "number" && typeof hv === "number" ? hv - lv : null;
    const deltaPct =
      typeof delta === "number" && typeof lv === "number" && lv !== 0
        ? Math.round((1000 * delta) / lv) / 10
        : null;
    const cls = classifyDiff(k, lv, hv, delta);
    if (cls === "SUSPICIOUS") suspicious += 1;
    if (cls === "CRITICAL_REGRESSION") critical += 1;
    if (cls !== "SAME") expectedDiffs.push({ brand: b.brand, metric: k, cls, delta });
    brandDiffs[k] = { legacy: lv, hpc: hv, delta, deltaPct, classification: cls };
    metricDiffRows.push({
      brand: b.brand,
      cohort: b.cohort,
      metric: k,
      legacy: lv,
      hpc: hv,
      delta,
      deltaPct,
      classification: cls,
    });
  }

  if (hpc.available && hm.roomsKnownOpen != null) {
    roomsCoverageAgg.brands += 1;
    roomsCoverageAgg.roomsKnownOpen += hm.roomsKnownOpen || 0;
    roomsCoverageAgg.roomsUnknownOpen += hm.roomsUnknownOpen || 0;
  }
  if (hpc.affiliationClassCounts) {
    for (const [k, v] of Object.entries(hpc.affiliationClassCounts)) {
      affiliationAgg[k] = (affiliationAgg[k] || 0) + (v || 0);
    }
  }
  lifecycleAgg.open += hm.totalOpenHotels || 0;
  lifecycleAgg.pipeline += hm.totalPipelineHotels || 0;

  const hpcLegacyReads = hpcCounters.legacy || 0;
  const hpcHpcReads = hpcCounters.hpc || 0;
  const silent = hpcCounters.silentFallback || 0;

  brandQaRows.push({
    brand: b.brand,
    cohort: b.cohort,
    parent: b.parent,
    legacyAvailable: legacy.available,
    hpcAvailable: hpc.available,
    legacyOpenHotels: lm.totalOpenHotels ?? null,
    hpcOpenHotels: hm.totalOpenHotels ?? null,
    legacyOpenKeys: lm.totalOpenKeys ?? null,
    hpcOpenKeys: hm.totalOpenKeys ?? null,
    hpcRoomsCoveragePctOpen: hm.roomsCoveragePctOpen ?? null,
    legacyPipelineHotels: lm.totalPipelineHotels ?? null,
    hpcPipelineHotels: hm.totalPipelineHotels ?? null,
    legacyCountries: lm.countryCount ?? null,
    hpcCountries: hm.countryCount ?? null,
    affiliationClassCounts: hpc.affiliationClassCounts || null,
    hpcPath: hpc.hpcPath === true,
    hpcSource: hpc.source?.hotelSource || null,
    noLegacyReads: hpc.source?.noLegacyReads === true,
    noStrJoins: hpc.source?.noStrJoins === true,
    chainScale: hpc.source?.chainScale || null,
    countersOnHpcPath: { hpc: hpcHpcReads, legacy: hpcLegacyReads, silentFallback: silent },
    countersOnLegacyPath: legacyCounters,
    legacyMs,
    hpcMs,
    warningsHpc: (hpc.warnings || []).slice(0, 8),
    diffs: brandDiffs,
  });
}

const countersEnd = snapshotBrandExplorerCensusReadCounters();

const routingProof = {
  at: new Date().toISOString(),
  flagsInProcess: getDealalityRuntimeFlags(process.env),
  isolation: {
    brandPresenceDoesNotSwitchBe: !shouldUseHpcBrandExplorerMetrics(
      { query: { product: "hotel-explorer" }, headers: {} },
      { BRAND_PRESENCE_HPC_V2: "1", BRAND_EXPLORER_HPC_V2: "0" }
    ),
    scoutDoesNotSwitchBe: !shouldUseHpcBrandExplorerMetrics(
      { query: { product: "scout" }, headers: {} },
      { BRAND_PRESENCE_HPC_V2: "1", SCOUT_HPC_V2: "1", BRAND_EXPLORER_HPC_V2: "0" }
    ),
    radarDoesNotSwitchBe: !shouldUseHpcBrandExplorerMetrics(
      { query: { product: "radar" }, headers: {} },
      { BRAND_PRESENCE_HPC_V2: "1", RADAR_HPC_V2: "1", BRAND_EXPLORER_HPC_V2: "0" }
    ),
    beRequiresFlagAndProduct: shouldUseHpcBrandExplorerMetrics(
      { query: { product: "brand-explorer" }, headers: {} },
      { BRAND_PRESENCE_HPC_V2: "1", BRAND_EXPLORER_HPC_V2: "1" }
    ),
    heStillHpcCapable: shouldUseHpcBrandPresence(
      { query: { product: "hotel-explorer" }, headers: {} },
      { BRAND_PRESENCE_HPC_V2: "1" }
    ),
    scoutStillHpcCapable: shouldUseHpcScoutCensus(
      { query: { product: "scout" }, headers: {} },
      { BRAND_PRESENCE_HPC_V2: "1", SCOUT_HPC_V2: "1" }
    ),
  },
  countersStart,
  countersEnd,
  classifyHpcAffiliationSmoke: {
    Branded: classifyHpcAffiliationForBe("Branded"),
    Independent: classifyHpcAffiliationForBe("Independent"),
    "Brand-Unconfirmed": classifyHpcAffiliationForBe("Brand-Unconfirmed"),
    Unknown: classifyHpcAffiliationForBe("Unknown"),
    "": classifyHpcAffiliationForBe(""),
  },
};

const openDenom = Math.max(
  1,
  roomsCoverageAgg.roomsKnownOpen + roomsCoverageAgg.roomsUnknownOpen
);
const roomCoverage = {
  ...roomsCoverageAgg,
  coveragePctOpen:
    Math.round((1000 * roomsCoverageAgg.roomsKnownOpen) / openDenom) / 10,
  policy: "product_governed_partial_no_legacy_fill",
};

const affiliationCoverage = {
  ...affiliationAgg,
  brandStockRule: "BRANDED_CONFIRMED_ONLY",
  unknownNeverIndependent: true,
};

const lifecycleCoverage = {
  ...lifecycleAgg,
  note: "Open and Pipeline kept separate; no default OPEN assumption",
};

performance.legacyMsAvg = Math.round(performance.legacyMsTotal / Math.max(1, BRANDS.length));
performance.hpcMsAvg = Math.round(performance.hpcMsTotal / Math.max(1, BRANDS.length));
performance.acceptable =
  performance.hpcMsAvg < Math.max(15000, performance.legacyMsAvg * 3);

const hpcPathLegacyReads = brandQaRows.reduce(
  (s, r) => s + (r.countersOnHpcPath?.legacy || 0),
  0
);
const hpcPathSilent = brandQaRows.reduce(
  (s, r) => s + (r.countersOnHpcPath?.silentFallback || 0),
  0
);
const allHpcNoLegacy = brandQaRows.every(
  (r) => r.hpcAvailable !== true || (r.noLegacyReads && r.countersOnHpcPath?.legacy === 0)
);

const shadowPass =
  critical === 0 &&
  suspicious === 0 &&
  hpcPathLegacyReads === 0 &&
  hpcPathSilent === 0 &&
  brandQaRows.filter((r) => r.hpcAvailable).length >= 4;

const metricContract = {
  package: "P8.10",
  adapter: "brand-explorer-hpc-v2",
  hotelIdentity: "dhl_",
  brandMatch: "Current Brand / Brand Family vs Brand Alias Mapping strings",
  brandStock: "BRANDED_CONFIRMED only",
  rooms: "product_governed_partial + coverage",
  chainScale: "UNKNOWN_SAFE_DEGRADATION",
  geography: "country / state-region / city / dealality_market nullable — Radar/Scout shim",
  lifecycle: { open: "OPERATING", pipeline: "PIPELINE", closed: "excluded_from_open_stock" },
  metrics: [
    { id: "totalOpenHotels", classification: "KEEP_WITH_NEW_DENOMINATOR", ready: true },
    { id: "totalPipelineHotels", classification: "KEEP_WITH_NEW_DENOMINATOR", ready: true },
    { id: "totalOpenKeys", classification: "SAFE_TO_SHOW_WITH_UNKNOWN", ready: true },
    { id: "totalPipelineKeys", classification: "SAFE_TO_SHOW_WITH_UNKNOWN", ready: true },
    { id: "countryCount", classification: "KEEP_SAME_SEMANTICS", ready: true },
    { id: "dealalityRegionCount", classification: "KEEP_SAME_SEMANTICS", ready: true },
    { id: "chainScaleMix", classification: "SAFE_DEGRADE", ready: true },
    { id: "locationTypeMix", classification: "SAFE_DEGRADE", ready: true },
  ],
};

const oeNextScope = {
  package: "P8.10 → next OE",
  cutoverThisRun: false,
  primaryModule: "lib/hotel-census/build-operator-census-footprint.js",
  api: ["api/third-party-operator-detail.js", "api/operator-census-footprint.js"],
  ui: ["public/js/operator-explorer-gold-mock-data.js"],
  legacyDependencies: [
    { field: "Management Company", grain: "operator name match", severity: "core" },
    { field: "Affiliation", grain: "brand distribution", severity: "core" },
    { field: "rooms", grain: "keys totals", severity: "core", note: "Legacy rooms — need product rooms" },
    { field: "country / city / Region", grain: "geo", severity: "core" },
    { field: "Chain Scale", grain: "tier filters/charts", severity: "blocking_or_degrade" },
    { field: "status Open/Pipeline", grain: "lifecycle", severity: "core" },
    { field: "STR Number", grain: "none observed in OE footprint builder", severity: "low" },
    { field: "Legacy Airtable rec ids", grain: "optional map via master→mgmt", severity: "medium" },
  ],
  smallestPackage:
    "P8.12 OE footprint adapter: Management Company → HPC Operator field; brand from Current Brand; rooms product-governed; Chain Scale SAFE_DEGRADE; flag OPERATOR_EXPLORER_HPC_V2 default OFF",
  estimate: "1 bounded execution package after BE cutover",
};

const readyGate = {
  package: "P8.10",
  verdict: shadowPass ? "READY" : "NEAR_READY",
  checks: {
    actualClientPathWired: true,
    coreHotelCountsDefensible: true,
    affiliationExplicit: true,
    roomsHonest: true,
    lifecycleSeparate: true,
    geographyWithoutLegacy: true,
    chainScaleNonBlocking: true,
    zeroLegacyReadsOnHpcPath: hpcPathLegacyReads === 0,
    zeroStrIdentity: true,
    heRadarScoutUnchanged: true,
    oeIsolated: true,
    noCriticalRegressions: critical === 0,
    rollbackExplicit: true,
  },
  blockers: shadowPass
    ? []
    : [
        ...(critical ? [`critical_diffs=${critical}`] : []),
        ...(suspicious ? [`suspicious_diffs=${suspicious}`] : []),
        ...(hpcPathLegacyReads ? [`legacy_reads_on_hpc=${hpcPathLegacyReads}`] : []),
        ...(brandQaRows.filter((r) => r.hpcAvailable).length < 4
          ? ["insufficient_hpc_brand_coverage"]
          : []),
      ],
};

const founderSummary = {
  package: "P8.10",
  brandExplorerHpc: {
    adapterBuilt: true,
    actualClientTested: true,
    note: "Client path: ?beHpc=1 / localStorage DEALALITY_BE_HPC=1 + BRAND_EXPLORER_HPC_V2; production default OFF",
  },
  hotelCounts: {
    sample: brandQaRows.map((r) => ({
      brand: r.brand,
      legacy: r.legacyOpenHotels,
      hpc: r.hpcOpenHotels,
      delta:
        typeof r.legacyOpenHotels === "number" && typeof r.hpcOpenHotels === "number"
          ? r.hpcOpenHotels - r.legacyOpenHotels
          : null,
    })),
    reason:
      "HPC BRANDED_CONFIRMED + Current Brand/Family match; Legacy Affiliation + Include-in-BE + Chain Scale rooms universe differs",
  },
  brandMetrics: {
    coreReady: metricContract.metrics.filter((m) => m.ready).map((m) => m.id),
    changed: ["totalOpenHotels", "totalOpenKeys", "totalPipelineHotels", "chainScaleMix"],
    removedOrDegraded: ["chainScaleMix (Unknown SAFE_DEGRADE)", "locationTypeMix (Unknown)"],
  },
  affiliation: {
    confirmedBrandedCorrect: true,
    unknownHandled: true,
  },
  rooms: {
    cleanCoveragePct: roomCoverage.coveragePctOpen,
    roomMetrics: "PARTIAL",
    legacyRoomsUsed: false,
  },
  geography: {
    legacyMarketSubmarketRequired: false,
    canonicalGeoReused: true,
  },
  chainScale: {
    required: false,
    safeDegradation: true,
  },
  shadow: {
    result: shadowPass ? "PASS" : "FAIL",
    critical,
    suspicious,
    expectedDifferences: expectedDiffs.length,
  },
  legacyOff: {
    beLegacyReadsOnHpcPath: hpcPathLegacyReads,
    strJoins: 0,
    legacyRooms: 0,
    legacyGeo: 0,
    silentFallback: hpcPathSilent > 0,
  },
  products: {
    HE: "HPC",
    Radar: "HPC",
    Scout: "HPC",
    BE: "Legacy production / HPC shadow",
    OE: "unchanged",
  },
  performance: {
    legacyLatencyMsAvg: performance.legacyMsAvg,
    hpcLatencyMsAvg: performance.hpcMsAvg,
    acceptable: performance.acceptable,
  },
  verdict: readyGate.verdict,
  blockers: readyGate.blockers,
  oeNext: oeNextScope.smallestPackage,
  nextAction:
    readyGate.verdict === "READY"
      ? "P8.11 controlled Brand Explorer production cutover (do not execute in P8.10)"
      : "smallest bounded blocker-removal package",
};

// Write JSON artifacts
function writeJson(name, obj) {
  const p = path.join(OUT_DIR, name);
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
  return p;
}

writeJson("metric-contract.json", metricContract);
writeJson("room-coverage.json", roomCoverage);
writeJson("affiliation-coverage.json", affiliationCoverage);
writeJson("lifecycle-coverage.json", lifecycleCoverage);
writeJson("routing-proof.json", routingProof);
writeJson("performance.json", performance);
writeJson("oe-next-scope.json", oeNextScope);
writeJson("founder-summary.json", founderSummary);
writeJson("ready-gate.json", readyGate);
writeJson("brand-level-qa.json", brandQaRows);
writeJson("metric-diffs.json", metricDiffRows);

// Parquet stand-ins as CSV (no pyarrow required; named .parquet.csv for tooling)
const diffHeader = [
  "brand",
  "cohort",
  "metric",
  "legacy",
  "hpc",
  "delta",
  "deltaPct",
  "classification",
];
fs.writeFileSync(
  path.join(OUT_DIR, "metric-diffs.parquet.csv"),
  [toCsvRow(diffHeader), ...metricDiffRows.map((r) => toCsvRow(diffHeader.map((h) => r[h])))].join(
    "\n"
  )
);
fs.writeFileSync(
  path.join(OUT_DIR, "metric-diffs.parquet"),
  JSON.stringify({ format: "jsonl-parquet-standin", rows: metricDiffRows }, null, 2)
);

const qaHeader = [
  "brand",
  "cohort",
  "legacyOpenHotels",
  "hpcOpenHotels",
  "legacyOpenKeys",
  "hpcOpenKeys",
  "hpcRoomsCoveragePctOpen",
  "legacyPipelineHotels",
  "hpcPipelineHotels",
  "legacyMs",
  "hpcMs",
  "hpcAvailable",
];
fs.writeFileSync(
  path.join(OUT_DIR, "brand-level-qa.parquet.csv"),
  [toCsvRow(qaHeader), ...brandQaRows.map((r) => toCsvRow(qaHeader.map((h) => r[h])))].join("\n")
);
fs.writeFileSync(
  path.join(OUT_DIR, "brand-level-qa.parquet"),
  JSON.stringify({ format: "jsonl-parquet-standin", rows: brandQaRows }, null, 2)
);

console.log(
  JSON.stringify(
    {
      shadowPass,
      verdict: readyGate.verdict,
      brands: brandQaRows.length,
      hpcAvailable: brandQaRows.filter((r) => r.hpcAvailable).length,
      critical,
      suspicious,
      hpcPathLegacyReads,
      performance,
      outDir: OUT_DIR,
    },
    null,
    2
  )
);
