/**
 * Tests brand.censusSummary on GET /api/brand-library/brand (handler-level).
 *
 * Usage:
 *   node scripts/test-brand-library-census-summary.mjs
 *   BRAND_EXPLORER_CENSUS_METRICS=1 node scripts/test-brand-library-census-summary.mjs
 */
import "../load-env.js";

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

async function fetchBrand(flagValue, brandId) {
  const prev = process.env.BRAND_EXPLORER_CENSUS_METRICS;
  if (flagValue === null) delete process.env.BRAND_EXPLORER_CENSUS_METRICS;
  else process.env.BRAND_EXPLORER_CENSUS_METRICS = flagValue;

  const { getBrandLibraryBrandById } = await import("../api/brand-library.js");
  const req = { query: { brandId } };
  const res = mockRes();
  await getBrandLibraryBrandById(req, res);
  const out = res.getOut();

  if (flagValue === null) delete process.env.BRAND_EXPLORER_CENSUS_METRICS;
  else process.env.BRAND_EXPLORER_CENSUS_METRICS = prev;

  return out;
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

async function main() {
  const brandId = "Courtyard by Marriott";

  const off = await fetchBrand("0", brandId);
  assert(off.statusCode === 200 && off.body?.success, "flag off request failed");
  assert(
    off.body.brand.censusSummary === undefined,
    "flag off must not include censusSummary"
  );
  console.log("PASS: flag off — no censusSummary");

  process.env.BRAND_EXPLORER_CENSUS_METRICS = "1";
  const onCourtyard = await fetchBrand("1", brandId);
  assert(onCourtyard.statusCode === 200 && onCourtyard.body?.success, "Courtyard request failed");
  const b = onCourtyard.body.brand;
  const cs = b.censusSummary;
  assert(cs, "censusSummary missing when flag on");
  assert(cs.available === true, "censusSummary should be available");
  assert(cs.fallbackRecommended === false, "Courtyard fallbackRecommended should be false");
  assert(cs.metrics.totalOpenHotels === 50, `open hotels expected 50 got ${cs.metrics.totalOpenHotels}`);
  assert(cs.metrics.totalOpenKeys === 8088, `open keys expected 8088 got ${cs.metrics.totalOpenKeys}`);
  assert(
    cs.metrics.countryCount === 24,
    `countries expected 24 (open + pipeline) got ${cs.metrics.countryCount}`
  );
  assert(
    !(cs.warnings || []).some((w) => w.startsWith("PARENT_COMPANY_ALIAS_MISMATCH")),
    `unexpected parent mismatch warnings: ${JSON.stringify(cs.warnings)}`
  );
  assert(b.footprint != null, "footprint should remain present");
  assert(
    cs.alias.affiliationMatchers.includes("Courtyard"),
    "matchers should include Courtyard"
  );
  const brCourtyard = cs.breakdowns.country.find((c) => c.label === "Mexico");
  assert(
    brCourtyard && brCourtyard.pipelineHotels > 0,
    "Courtyard country breakdown should include pipelineHotels"
  );
  console.log("PASS: Courtyard by Marriott censusSummary");

  const onIbis = await fetchBrand("1", "ibis Styles");
  const ibis = onIbis.body.brand.censusSummary;
  assert(ibis.metrics.totalOpenHotels === 57, `ibis open hotels expected 57 got ${ibis.metrics.totalOpenHotels}`);
  assert(
    ibis.metrics.totalPipelineHotels === 23,
    `ibis pipeline hotels expected 23 got ${ibis.metrics.totalPipelineHotels}`
  );
  const brIbis = ibis.breakdowns.country.find((c) => c.label === "Brazil");
  assert(
    brIbis && brIbis.pipelineHotels === 18,
    `ibis Brazil pipeline hotels expected 18 got ${brIbis && brIbis.pipelineHotels}`
  );
  const unknownLoc = ibis.breakdowns.locationType.find((c) => c.label === "Unknown");
  assert(
    unknownLoc && unknownLoc.pipelineHotels === 23,
    "ibis pipeline without location should roll up to Unknown archetype"
  );
  const cala = ibis.breakdowns.dealalityRegion.find((c) => c.label === "Caribbean & Latin America");
  assert(
    cala && cala.pipelineHotels === 23,
    `ibis CALA region pipeline expected 23 got ${cala && cala.pipelineHotels}`
  );
  console.log("PASS: ibis Styles census pipeline breakdowns");

  const onAc = await fetchBrand("1", "AC Hotels by Marriott");
  const ac = onAc.body.brand.censusSummary;
  assert(ac.metrics.totalOpenHotels === 18, `AC open hotels expected 18 got ${ac.metrics.totalOpenHotels}`);
  assert(ac.metrics.totalOpenKeys === 2866, `AC keys expected 2866 got ${ac.metrics.totalOpenKeys}`);
  assert(
    ac.metrics.countryCount === 11,
    `AC countries expected 11 (open + pipeline) got ${ac.metrics.countryCount}`
  );
  assert(ac.fallbackRecommended === false, "AC fallbackRecommended should be false");
  assert(
    !(ac.warnings || []).some((w) => w.startsWith("PARENT_COMPANY_ALIAS_MISMATCH")),
    `AC unexpected parent mismatch: ${JSON.stringify(ac.warnings)}`
  );
  console.log("PASS: AC Hotels by Marriott censusSummary");

  const cambria = await fetchBrand("1", "Cambria Hotels");
  assert(cambria.statusCode === 200, "Cambria Hotels should load from MVP");
  const fcs = cambria.body.brand.censusSummary;
  assert(fcs, "censusSummary should attach even when fallback");
  assert(fcs.fallbackRecommended === true, "brand without alias mapping should recommend fallback");
  assert(cambria.body.brand.footprint != null, "footprint should remain when census fallback");
  console.log("PASS: Cambria Hotels (no alias rows) census fallback");

  console.log("\nSample excerpt (Courtyard):");
  console.log(
    JSON.stringify(
      {
        name: b.name,
        parentCompany: b.parentCompany,
        footprintKeys: Object.keys(b.footprint || {}),
        censusSummary: {
          available: cs.available,
          fallbackRecommended: cs.fallbackRecommended,
          metrics: cs.metrics,
          alias: cs.alias,
          match: cs.match,
          warnings: cs.warnings,
        },
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error("FAIL:", e.message);
  process.exit(1);
});
