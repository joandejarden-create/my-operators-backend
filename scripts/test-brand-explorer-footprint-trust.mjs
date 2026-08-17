/**
 * Phase 1D/1E — footprint display trust (handler + client helper parity).
 *
 * Usage:
 *   BRAND_EXPLORER_CENSUS_METRICS=1 node scripts/test-brand-explorer-footprint-trust.mjs
 */
import "../load-env.js";
import { readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import vm from "vm";
import {
  footprintTrustModel as trustNode,
  useCensusSummary,
} from "../lib/brand-explorer-footprint-trust.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

function loadCensusMetricsHelper() {
  const path = join(__dirname, "..", "public", "js", "brand-explorer-census-metrics.js");
  const sandbox = { window: {}, globalThis: {} };
  sandbox.globalThis = sandbox.window;
  vm.runInNewContext(readFileSync(path, "utf8"), sandbox);
  return sandbox.window.BrandExplorerCensusMetrics;
}

function mockRes() {
  const out = { statusCode: 200, body: null };
  return {
    setHeader() {},
    status(code) {
      out.statusCode = code;
      return this;
    },
    json(payload) {
      out.body = payload;
      return this;
    },
    getOut() {
      return out;
    },
  };
}

async function fetchBrand(brandId) {
  process.env.BRAND_EXPLORER_CENSUS_METRICS = "1";
  const { getBrandLibraryBrandById } = await import("../api/brand-library.js");
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId } }, res);
  const out = res.getOut();
  if (out.statusCode !== 200 || !out.body?.success) {
    throw new Error(`${brandId}: HTTP ${out.statusCode} ${JSON.stringify(out.body)}`);
  }
  return out.body.brand;
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

function assertTrustParity(brand, M) {
  const nodeT = trustNode(brand);
  const browserT = M.footprintTrustModel(brand);
  assert(
    nodeT.sourceUsed === browserT.sourceUsed,
    `trust parity: node ${nodeT.sourceUsed} vs browser ${browserT.sourceUsed}`
  );
}

function syntheticBrand(overrides) {
  return {
    name: "Synthetic Test Brand",
    parentCompany: "Choice Hotels International",
    footprint: {
      totalExistingHotels: 57,
      totalExistingRooms: 4000,
      regionalDistribution: { AM: { hotels: 57, rooms: 4000 } },
      formValues: {},
      ...(overrides.footprint || {}),
    },
    censusSummary: overrides.censusSummary,
    explorerHeroVerification: overrides.explorerHeroVerification,
    explorerHeroDataSource: overrides.explorerHeroDataSource,
  };
}

function runSyntheticCases(M) {
  const cases = [
    {
      label: "census wins over MVP Placeholder",
      brand: syntheticBrand({
        footprint: {
          verification: { status: "Placeholder" },
        },
        censusSummary: {
          available: true,
          fallbackRecommended: false,
          metrics: { totalOpenHotels: 10, totalOpenKeys: 1000, countryCount: 2 },
        },
      }),
      expectSource: "census",
      expectLabel: "Based on current Dealality census records.",
    },
    {
      label: "census fallback + MVP Verified",
      brand: syntheticBrand({
        footprint: {
          verification: { status: "Verified", source: "FDD 2024" },
        },
        censusSummary: { available: true, fallbackRecommended: true, metrics: { totalOpenHotels: 0 } },
      }),
      expectSource: "mvp-footprint",
      expectLabel: "Based on verified brand setup footprint data.",
    },
    {
      label: "census fallback + MVP Estimated",
      brand: syntheticBrand({
        footprint: {
          verification: { status: "Estimated" },
        },
        censusSummary: { available: true, fallbackRecommended: true },
      }),
      expectSource: "mvp-footprint",
      expectLabel: "Based on estimated brand setup footprint data.",
    },
    {
      label: "census fallback + MVP Placeholder",
      brand: syntheticBrand({
        footprint: {
          verification: { status: "Placeholder" },
        },
        censusSummary: { available: true, fallbackRecommended: true },
      }),
      expectSource: "unverified",
      expectLabel: "Portfolio data being verified.",
      expectShowMetrics: true,
      expectDisplaySource: "mvp-footprint",
    },
    {
      label: "census fallback + MVP Needs Review",
      brand: syntheticBrand({
        footprint: {
          verification: { status: "Needs Review" },
        },
        censusSummary: { available: true, fallbackRecommended: true },
      }),
      expectSource: "unverified",
      expectLabel: "Portfolio data being verified.",
      expectShowMetrics: true,
      expectDisplaySource: "mvp-footprint",
    },
    {
      label: "Footprint Figures As Of on zero-census fallback",
      brand: syntheticBrand({
        footprint: {
          verification: { figuresAsOf: "2025-01-01" },
        },
        censusSummary: {
          available: true,
          fallbackRecommended: true,
          metrics: { totalOpenHotels: 0 },
        },
      }),
      expectSource: "mvp-footprint",
      expectLabel: "Based on brand setup footprint data.",
    },
    {
      label: "legacy Figures as of blocked when census fallback has zero open hotels",
      brand: syntheticBrand({
        footprint: {
          formValues: { figuresAsOf: "2024-12-31" },
          verification: undefined,
        },
        censusSummary: {
          available: true,
          fallbackRecommended: true,
          metrics: { totalOpenHotels: 0 },
        },
      }),
      expectSource: "unverified",
      expectLabel: "Portfolio data being verified.",
      expectShowMetrics: true,
      expectDisplaySource: "mvp-footprint",
    },
    {
      label: "no verification fields — legacy unverified fallback",
      brand: syntheticBrand({
        footprint: { verification: undefined },
        censusSummary: { available: true, fallbackRecommended: true },
      }),
      expectSource: "unverified",
      expectLabel: "Portfolio data being verified.",
      expectShowMetrics: true,
      expectDisplaySource: "mvp-footprint",
    },
  ];

  for (const c of cases) {
    const trust = trustNode(c.brand);
    const disp = M.footprintDisplayModel(c.brand);
    assert(trust.sourceUsed === c.expectSource, `${c.label}: source ${trust.sourceUsed}`);
    assert(
      trust.displaySourceLabel === c.expectLabel,
      `${c.label}: label got "${trust.displaySourceLabel}"`
    );
    assert(disp.sourceUsed === (c.expectDisplaySource || c.expectSource), `${c.label}: display source`);
    if (c.expectShowMetrics === true) {
      assert(disp.showVerifiedMetrics === true, `${c.label}: should show footprint metrics`);
    }
    if (c.expectShowMetrics === false) {
      assert(disp.showVerifiedMetrics === false, `${c.label}: should hide footprint metrics`);
    }
    assertTrustParity(c.brand, M);
    console.log(`PASS (synthetic): ${c.label} → ${c.expectSource}`);
  }

  const pipeBrand = syntheticBrand({
    footprint: {
      totalExistingHotels: 51,
      totalExistingRooms: 5000,
      totalNewBuildHotels: 51,
      totalNewBuildRooms: 5000,
      regionalDistribution: {
        AM: { hotels: 30, rooms: 3000, pipelineHotels: 3, pipelineRooms: 300 },
        EU: { hotels: 8, rooms: 750, pipelineHotels: 1, pipelineRooms: 75 },
        CALA: { hotels: 5, rooms: 500, pipelineHotels: 1, pipelineRooms: 50 },
        MEA: { hotels: 3, rooms: 250, pipelineHotels: 3, pipelineRooms: 25 },
        APAC: { hotels: 5, rooms: 500, pipelineHotels: 1, pipelineRooms: 50 },
      },
    },
  });
  const pipe = M.footprintPipelineTotals(pipeBrand.footprint);
  assert(pipe.hotels === 9, `pipeline hotels expected 9 got ${pipe.hotels}`);
  assert(pipe.rooms === 500, `pipeline rooms expected 500 got ${pipe.rooms}`);
  console.log("PASS (synthetic): regional pipeline totals beat portfolio new-build sum");

  const censusNoDist = syntheticBrand({
    footprint: {
      totalExistingHotels: 10,
      totalExistingRooms: 1000,
      regionalDistribution: {
        AM: { hotels: 10, rooms: 1000, pipelineHotels: 1, pipelineRooms: 100 },
      },
    },
    censusSummary: {
      available: true,
      fallbackRecommended: false,
      metrics: { totalOpenHotels: 12, totalOpenKeys: 1200, totalPipelineHotels: 2, totalPipelineKeys: 200, countryCount: 3 },
      breakdowns: { dealalityRegion: [], country: [] },
    },
  });
  const censusDisp = M.footprintDisplayModel(censusNoDist);
  assert(censusDisp.showVerifiedMetrics === true, "census without breakdown still shows metrics");
  assert(
    Object.keys(censusDisp.fp.regionalDistribution || {}).length === 1,
    "census without breakdown falls back to MVP regional distribution"
  );
  assert(
    !censusDisp.censusBreakdownNotice,
    "no breakdown notice when MVP regional fills the gap"
  );
  console.log("PASS (synthetic): census missing distribution uses MVP regional rows");
}

async function main() {
  const M = loadCensusMetricsHelper();

  runSyntheticCases(M);

  const apiCases = [
    {
      name: "Comfort Inn & Suites",
      expectSource: "census",
      expectHotels: 47,
      expectKeys: 5799,
    },
    {
      name: "Radisson Blu (Choice)",
      expectSource: "census",
      expectHotels: 5,
      expectKeys: 887,
    },
    {
      name: "Clarion",
      expectSource: "census",
      expectHotels: 5,
      expectKeys: 502,
    },
    {
      name: "Country Inn & Suites by Radisson",
      expectSource: "census",
      expectHotels: 1,
      expectKeys: 92,
    },
    {
      name: "Clarion Pointe",
      expectSource: "unverified",
      expectHotels: null,
      expectKeys: null,
    },
    {
      name: "Radisson Collection  (Choice)",
      expectSource: "unverified",
      expectHotels: null,
      expectKeys: null,
    },
  ];

  for (const c of apiCases) {
    const brand = await fetchBrand(c.name);
    const disp = M.footprintDisplayModel(brand);
    const trust = M.footprintTrustModel(brand);
    assertTrustParity(brand, M);

    assert(
      disp.sourceUsed === c.expectSource,
      `${c.name}: sourceUsed expected ${c.expectSource}, got ${disp.sourceUsed}`
    );
    assert(
      trust.sourceUsed === c.expectSource,
      `${c.name}: trust.sourceUsed expected ${c.expectSource}, got ${trust.sourceUsed}`
    );

    if (c.expectSource === "census") {
      assert(disp.showVerifiedMetrics === true, `${c.name}: should show verified metrics`);
      assert(disp.useCensus === true, `${c.name}: useCensus should be true`);
      assert(useCensusSummary(brand), `${c.name}: census should win`);
      if (brand.footprint?.verification?.status === "Placeholder") {
        assert(
          disp.fp.totalExistingHotels === c.expectHotels,
          `${c.name}: census must beat Placeholder MVP status`
        );
      } else {
        assert(
          disp.fp.totalExistingHotels === c.expectHotels,
          `${c.name}: hotels expected ${c.expectHotels}, got ${disp.fp.totalExistingHotels}`
        );
        assert(
          disp.fp.totalExistingRooms === c.expectKeys,
          `${c.name}: keys expected ${c.expectKeys}, got ${disp.fp.totalExistingRooms}`
        );
      }
    } else {
      assert(disp.showVerifiedMetrics === false, `${c.name}: should not show verified metrics`);
      assert(
        disp.fp.totalExistingHotels == null || disp.fp.totalExistingHotels === 0,
        `${c.name}: display hotels should be empty, got ${disp.fp.totalExistingHotels}`
      );
      assert(brand.footprint != null, `${c.name}: raw MVP footprint should remain on brand object`);
    }

    console.log(
      `PASS (api): ${c.name} → ${disp.sourceUsed} (display hotels=${disp.fp.totalExistingHotels}, verification=${brand.footprint?.verification?.status ?? "—"})`
    );
  }

  const censusPlaceholder = syntheticBrand({
    footprint: { verification: { status: "Placeholder" } },
    censusSummary: {
      available: true,
      fallbackRecommended: false,
      metrics: { totalOpenHotels: 99, totalOpenKeys: 9900, countryCount: 3 },
    },
  });
  const cpDisp = M.footprintDisplayModel(censusPlaceholder);
  assert(cpDisp.sourceUsed === "census", "census vs Placeholder status");
  assert(cpDisp.fp.totalExistingHotels === 99, "census metrics used");
  console.log("PASS: census available beats MVP Placeholder status");

  const offBrand = await (async () => {
    process.env.BRAND_EXPLORER_CENSUS_METRICS = "0";
    const { getBrandLibraryBrandById } = await import("../api/brand-library.js");
    const res = mockRes();
    await getBrandLibraryBrandById({ query: { brandId: "Clarion Pointe" } }, res);
    return res.getOut().body.brand;
  })();

  assert(offBrand.censusSummary === undefined, "flag off must omit censusSummary");
  const offDisp = M.footprintDisplayModel(offBrand);
  console.log(
    `PASS: flag off Clarion Pointe → ${offDisp.sourceUsed} (no censusSummary; MVP rules only)`
  );
}

main().catch((e) => {
  console.error("FAIL:", e.message);
  process.exit(1);
});
