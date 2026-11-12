#!/usr/bin/env node
/**
 * Permanent GDI invariant:
 * EVERY list-visible opportunity must resolve to a correct detail body.
 *
 * Exercises getGdiOpportunities + getGdiOpportunityDetail for Bethesda default
 * commercial list, plus archive/full admin views and fail cases.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getGdiOpportunities,
  getGdiOpportunityDetail,
} from "../api/group-demand-intelligence.js";
import {
  isSyntheticGdiTestOrFixtureOpportunity,
  mayOpenAuthOpportunityDetail,
  resolveHotelOpportunityById,
} from "../lib/group-demand-intelligence/gdi-opportunity-id-resolution-v1.js";
import { isGdiTestOrFixtureOpportunity } from "../lib/group-demand-intelligence/customer-visibility.js";
import { loadHotelOpportunityDoc } from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { primaryCommercialForbiddenIdList } from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const BETHESDA = "recLuxvwwxID7U2B8";
const RIC = "gdi_opp_regulatory_information_conference_20260930";
const FORBIDDEN = new Set(primaryCommercialForbiddenIdList());

process.env.GROUP_DEMAND_INTELLIGENCE_PILOT_READ =
  process.env.GROUP_DEMAND_INTELLIGENCE_PILOT_READ || "1";

function mockRes() {
  const res = { statusCode: 200, body: null, headers: {} };
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

function mockReq({ hotelId = BETHESDA, opportunityId = null, query = {} } = {}) {
  return {
    params: {
      hotelId,
      ...(opportunityId ? { opportunityId } : {}),
    },
    query: { ...query },
    dealalityUser: { id: "test-founder", email: "founder@dealality.test" },
  };
}

async function callList(query = {}) {
  const res = mockRes();
  await getGdiOpportunities(mockReq({ query }), res);
  return res;
}

async function callDetail(opportunityId, hotelId = BETHESDA) {
  const res = mockRes();
  await getGdiOpportunityDetail(mockReq({ hotelId, opportunityId }), res);
  return res;
}

async function main() {
  // Unit: unpublished commercial ≠ synthetic fixture
  const ricStore = loadHotelOpportunityDoc(BETHESDA).opportunities.find(
    (o) => o.id === RIC,
  );
  assert.ok(ricStore, "RIC must exist in canonical store");
  assert.equal(ricStore.customerVisible, false);
  assert.equal(isSyntheticGdiTestOrFixtureOpportunity(ricStore), false);
  assert.equal(isGdiTestOrFixtureOpportunity(ricStore), true); // share still blocked
  assert.equal(mayOpenAuthOpportunityDetail(ricStore, { hotelId: BETHESDA }).ok, true);

  const resolved = resolveHotelOpportunityById(
    BETHESDA,
    RIC,
    loadHotelOpportunityDoc(BETHESDA).opportunities,
  );
  assert.equal(resolved.opportunity?.id, RIC);

  // Default commercial list
  const list = await callList({});
  assert.equal(list.statusCode, 200);
  assert.equal(list.body?.listScope, "BETHESDA_PRIMARY_COMMERCIAL");
  const opportunities = list.body?.opportunities || [];
  assert.ok(opportunities.length >= 1, "commercial list non-empty");
  assert.ok(
    opportunities.some((o) => o.id === RIC),
    "RIC must remain on commercial list",
  );
  for (const o of opportunities) {
    assert.ok(!FORBIDDEN.has(o.id), `forbidden id on commercial list: ${o.id}`);
  }

  const matrix = [];
  let okCount = 0;
  let failCount = 0;
  for (const row of opportunities) {
    const id = row.id;
    const detail = await callDetail(id);
    const dOpp = detail.body?.opportunity;
    const titleOk =
      !dOpp ||
      !row.title ||
      String(dOpp.title || "")
        .toLowerCase()
        .includes(String(row.title).slice(0, 12).toLowerCase()) ||
      String(row.title)
        .toLowerCase()
        .includes(String(dOpp.title || "").slice(0, 12).toLowerCase());
    const identityOk =
      detail.statusCode === 200 &&
      dOpp &&
      (dOpp.id === id || dOpp.opportunityId === id);
    const hotelOk =
      !dOpp?.hotelId || dOpp.hotelId === BETHESDA || !dOpp.hotelRecordId;
    const pass = identityOk && titleOk && hotelOk && detail.body?.ok === true;
    if (pass) okCount += 1;
    else failCount += 1;
    matrix.push({
      id,
      listTitle: row.title,
      status: detail.statusCode,
      detailId: dOpp?.id || null,
      pass,
      error: detail.body?.error || null,
    });
  }

  assert.equal(failCount, 0, `list-to-detail failures: ${JSON.stringify(matrix.filter((m) => !m.pass), null, 2)}`);
  assert.equal(okCount, opportunities.length);

  // Explicit missing id
  const missing = await callDetail("gdi_opp_definitely_does_not_exist_zzzz");
  assert.equal(missing.statusCode, 404);
  assert.equal(missing.body?.error, "opportunity_not_found");
  assert.match(String(missing.body?.message || ""), /could not be opened/i);

  // Cross-hotel: same id against wrong hotel should not return Bethesda body
  // (resolver is hotel-scoped via store path)
  const cross = await callDetail(RIC, "recSOMEOTHERHOTEL999");
  assert.ok(
    cross.statusCode === 404 ||
      !cross.body?.opportunity ||
      cross.body?.opportunity?.id !== RIC ||
      String(cross.body?.opportunity?.hotelId || "") === "recSOMEOTHERHOTEL999",
    "cross-hotel must not serve Bethesda RIC as another hotel's detail",
  );

  // Archive/full still open their IDs when requested
  const full = await callList({ view: "full" });
  assert.equal(full.statusCode, 200);
  const marriott = (full.body?.opportunities || []).find(
    (o) => o.id === "gdi_opp_marriott_hq_adjacent_corporate_watch",
  );
  assert.ok(marriott, "full view retains Marriott research theme");
  const marriottDetail = await callDetail(marriott.id);
  assert.equal(marriottDetail.statusCode, 200, "admin may open Marriott theme detail");

  // UI must not surface raw opportunity_not_found alone
  const uiCtrl = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/gdi-universal-presentation-controller.js"),
    "utf8",
  );
  assert.match(uiCtrl, /could not be opened/i);

  const reportDir = path.join(
    ROOT,
    "reports/gdi-list-to-detail-integrity-v1",
  );
  fs.mkdirSync(reportDir, { recursive: true });
  const report = {
    ok: true,
    hotelId: BETHESDA,
    commercialListCount: opportunities.length,
    detailRequestsTested: matrix.length,
    successfulDetailRequests: okCount,
    failedDetailRequests: failCount,
    ricPass: matrix.find((m) => m.id === RIC)?.pass === true,
    matrix,
  };
  fs.writeFileSync(
    path.join(reportDir, "00-bethesda-list-to-detail-matrix.json"),
    JSON.stringify(report, null, 2),
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        test: "test-gdi-list-to-detail-integrity-v1",
        commercialListCount: opportunities.length,
        detailRequestsTested: matrix.length,
        successfulDetailRequests: okCount,
        failedDetailRequests: failCount,
        ricPass: true,
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
