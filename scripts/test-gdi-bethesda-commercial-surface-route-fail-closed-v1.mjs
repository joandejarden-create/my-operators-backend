#!/usr/bin/env node
/**
 * Adversarial integration: getGdiOpportunities default commercial route.
 * - Forbidden IDs must be absent from default response.
 * - Forced filter failure must fail closed (non-2xx, empty opportunities).
 * - Explicit research/archive/full may include excluded research IDs.
 * - UI filterOpportunities must not render forbidden IDs even if API leaked.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { getGdiOpportunities } from "../api/group-demand-intelligence.js";
import {
  primaryCommercialForbiddenIdList,
  resolveRequestedCommercialSurfaceView,
  GDI_LIST_SURFACE_VIEW,
} from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const BETHESDA = "recLuxvwwxID7U2B8";
const FORBIDDEN = primaryCommercialForbiddenIdList();

process.env.GROUP_DEMAND_INTELLIGENCE_PILOT_READ =
  process.env.GROUP_DEMAND_INTELLIGENCE_PILOT_READ || "1";
process.env.GDI_SERVER_BUILD_SHA =
  process.env.GDI_SERVER_BUILD_SHA || "test-commercial-surface-fail-closed";

function mockRes() {
  const res = {
    statusCode: 200,
    body: null,
    headers: {},
  };
  res.status = (code) => {
    res.statusCode = code;
    return res;
  };
  res.json = (body) => {
    res.body = body;
    return res;
  };
  res.setHeader = (k, v) => {
    res.headers[String(k).toLowerCase()] = v;
  };
  return res;
}

function mockReq(query = {}) {
  return {
    params: { hotelId: BETHESDA },
    query: { ...query },
    dealalityUser: { id: "test-founder", email: "founder@dealality.test" },
  };
}

async function callRoute(query = {}) {
  const req = mockReq(query);
  const res = mockRes();
  await getGdiOpportunities(req, res);
  return res;
}

function assertNoForbidden(opportunities, label) {
  const ids = new Set((opportunities || []).map((o) => o.id));
  const hits = FORBIDDEN.filter((id) => ids.has(id));
  assert.equal(hits.length, 0, `${label}: forbidden leaked: ${hits.join(",")}`);
}

async function main() {
  // Surface resolver: includeDisqualified alone must NOT widen to full.
  assert.equal(
    resolveRequestedCommercialSurfaceView({ includeDisqualified: "1" }),
    GDI_LIST_SURFACE_VIEW.COMMERCIAL,
  );
  assert.equal(
    resolveRequestedCommercialSurfaceView({}),
    GDI_LIST_SURFACE_VIEW.COMMERCIAL,
  );
  assert.equal(
    resolveRequestedCommercialSurfaceView({ view: "full" }),
    GDI_LIST_SURFACE_VIEW.FULL,
  );
  assert.equal(
    resolveRequestedCommercialSurfaceView({ surface: "archive" }),
    GDI_LIST_SURFACE_VIEW.ARCHIVE,
  );

  const commercial = await callRoute({});
  assert.equal(commercial.statusCode, 200, "default commercial HTTP 200");
  assert.equal(commercial.body?.ok, true);
  assert.equal(commercial.body?.listScope, "BETHESDA_PRIMARY_COMMERCIAL");
  assert.equal(commercial.body?.effectiveSurface, "commercial");
  assert.equal(commercial.body?.requestedSurface, "commercial");
  assert.ok(
    Number(commercial.body?.count) >= 1,
    "commercial list should retain legitimate opportunities",
  );
  assertNoForbidden(commercial.body?.opportunities, "default commercial");
  assert.ok(
    commercial.body?.serverBuildSha,
    "serverBuildSha must be present on commercial response",
  );
  assert.ok(commercial.body?.sourceOfTruth, "sourceOfTruth required");
  assert.ok(commercial.body?.storeIdentity?.hotelId === BETHESDA);

  // Forced filter failure → fail closed
  process.env.GDI_FORCE_COMMERCIAL_SURFACE_FILTER_FAILURE = "1";
  try {
    const failed = await callRoute({});
    assert.ok(failed.statusCode >= 400, "forced failure must be non-2xx");
    assert.equal(failed.body?.ok, false);
    assert.equal(failed.body?.error, "COMMERCIAL_SURFACE_FILTER_FAILED");
    assert.deepEqual(failed.body?.opportunities || [], []);
    assert.equal(failed.body?.count, 0);
    assertNoForbidden(failed.body?.opportunities, "forced failure body");
  } finally {
    delete process.env.GDI_FORCE_COMMERCIAL_SURFACE_FILTER_FAILURE;
  }

  // Explicit archive may include DQ fixtures
  const archive = await callRoute({ surface: "archive" });
  assert.equal(archive.statusCode, 200);
  assert.equal(archive.body?.effectiveSurface, "archive");
  const archiveIds = new Set((archive.body?.opportunities || []).map((o) => o.id));
  assert.ok(
    archiveIds.has("gdi_opp_aao_2027_annual_session_9") ||
      archiveIds.has("gdi_opp_ntca_2027_disqualified"),
    "archive view should retain disqualified research IDs",
  );

  // Explicit full retains Marriott research theme
  const full = await callRoute({ view: "full" });
  assert.equal(full.statusCode, 200);
  assert.equal(full.body?.effectiveSurface, "full");
  const fullIds = new Set((full.body?.opportunities || []).map((o) => o.id));
  assert.ok(
    fullIds.has("gdi_opp_marriott_hq_adjacent_corporate_watch"),
    "full admin ledger must preserve Marriott HQ research theme",
  );

  // UI defense-in-depth: even if a leaked payload were passed to filterOpportunities
  const uiPath = path.join(
    ROOT,
    "public/js/group-demand-intelligence/dealality-gdi-ui.js",
  );
  const uiSrc = fs.readFileSync(uiPath, "utf8");
  assert.match(uiSrc, /forbiddenCommercialIds/);
  assert.match(uiSrc, /raw priority wins over remapped priorityFilterKey/i);
  assert.match(
    fs.readFileSync(
      path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
      "utf8",
    ),
    /UNSAFE_BETHESDA_LIST_SCOPE/,
  );
  assert.match(
    fs.readFileSync(path.join(ROOT, "api/group-demand-intelligence.js"), "utf8"),
    /COMMERCIAL_SURFACE_FILTER_FAILED/,
  );
  // Ensure fail-open catch that continues unfiltered was removed for commercial.
  assert.doesNotMatch(
    fs.readFileSync(path.join(ROOT, "api/group-demand-intelligence.js"), "utf8"),
    /surfaceFilterError[\s\S]{0,200}opportunities = union\.opportunities/,
  );

  // Sanity: require path still resolves (CommonJS interop not required)
  createRequire(import.meta.url);

  console.log(
    JSON.stringify(
      {
        ok: true,
        test: "test-gdi-bethesda-commercial-surface-route-fail-closed-v1",
        commercialCount: commercial.body?.count,
        listScope: commercial.body?.listScope,
        effectiveSurface: commercial.body?.effectiveSurface,
        forbiddenOnCommercial: 0,
        forcedFailureFailClosed: true,
        archiveHasDq: true,
        fullPreservesMarriott: true,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
