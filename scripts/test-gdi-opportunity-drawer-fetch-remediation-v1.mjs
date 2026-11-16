#!/usr/bin/env node
/**
 * Permanent GDI Full E2E gate: staged list ↔ detail publication contract.
 *
 *   npm run test:gdi-opportunity-drawer-fetch-remediation-v1
 *
 * - Every staged-visible Radisson opportunity detail resolves from the same STAGED package.
 * - Founder preview=staged is propagated on auth detail URLs.
 * - Drawer open does not block on decision/pursuit (bounded enrichment).
 * - Customer (non-staged) sessions are not silently granted staged-only inventory via live fallthrough.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getGdiOpportunities,
  getGdiOpportunityDetail,
} from "../api/group-demand-intelligence.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recH9h3McgMNFQEla";
const FHLIP = "rx_d_2400ebe4c2bb";
const EXPECTED_FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";

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

function mockReq({ hotelId = HOTEL, opportunityId = null, query = {} } = {}) {
  return {
    params: {
      hotelId,
      ...(opportunityId ? { opportunityId } : {}),
    },
    query: { ...query },
    dealalityUser: { id: "test-founder", email: "founder@dealality.test" },
  };
}

async function callList(query) {
  const res = mockRes();
  await getGdiOpportunities(mockReq({ query }), res);
  return res;
}

async function callDetail(opportunityId, query) {
  const res = mockRes();
  await getGdiOpportunityDetail(
    mockReq({ opportunityId, query }),
    res,
  );
  return res;
}

let failed = 0;
function check(name, fn) {
  try {
    const out = fn();
    if (out && typeof out.then === "function") {
      return out
        .then(() => console.log(`PASS ${name}`))
        .catch((err) => {
          failed += 1;
          console.error(`FAIL ${name}: ${err.message}`);
        });
    }
    console.log(`PASS ${name}`);
    return Promise.resolve();
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err.message}`);
    return Promise.resolve();
  }
}

const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const pcJs = fs.readFileSync(
  path.join(
    ROOT,
    "public/js/group-demand-intelligence/gdi-universal-presentation-controller.js",
  ),
  "utf8",
);

await check("detail_url_propagates_preview_staged", () => {
  assert.match(appJs, /path \+= "&preview=staged"/);
  assert.match(appJs, /wantsExplicitStagedPreview\(\)/);
});

await check("drawer_detail_not_blocked_by_decision_pursuit", () => {
  assert.match(appJs, /raceTimeout/);
  assert.match(appJs, /STALE_DETAIL_RESPONSE/);
  assert.match(appJs, /GDI_NETWORK_FETCH_FAILED/);
  // Detail must resolve before optional enrichments.
  assert.match(appJs, /return api\(url\)\.then\(function \(data\)/);
});

await check("friendly_network_error_no_raw_failed_to_fetch", () => {
  assert.match(
    pcJs,
    /failed to fetch\|networkerror\|load failed\|abort/i,
  );
  assert.match(pcJs, /STALE_DETAIL_RESPONSE/);
});

const list = await callList({
  publicationPackage: "1",
  surface: "customer",
  preview: "staged",
});
await check("radisson_staged_list_66", () => {
  assert.equal(list.statusCode, 200);
  assert.equal(list.body?.ok, true);
  assert.equal(list.body?.publication?.previewSource, "STAGED");
  assert.equal(list.body?.publication?.contentFingerprintSha256, EXPECTED_FP);
  assert.equal((list.body?.opportunities || []).length, 66);
});

const opps = list.body?.opportunities || [];
const hm = opps.filter((o) => o.hotelMarketScopeEligible === true);
const cmi = opps.filter((o) => o.competitiveMarketScopeEligible === true);

await check("radisson_inventory_split_50_16", () => {
  assert.equal(hm.length, 50);
  assert.equal(cmi.length, 16);
});

const detailFails = [];
const fps = new Set();
for (const o of opps) {
  const detail = await callDetail(o.id, {
    publicationPackage: "1",
    surface: "customer",
    preview: "staged",
  });
  const got = detail.body?.opportunity;
  const fp = detail.body?.publication?.contentFingerprintSha256;
  if (fp) fps.add(fp);
  if (
    detail.statusCode !== 200 ||
    detail.body?.ok !== true ||
    !got ||
    String(got.id) !== String(o.id) ||
    fp !== EXPECTED_FP ||
    String(detail.headers["x-gdi-detail-source"] || "") !== "PUBLICATION_PACKAGE"
  ) {
    detailFails.push({
      id: o.id,
      title: o.title || o.displayTitle,
      status: detail.statusCode,
      error: detail.body?.error || detail.body?.message,
      gotId: got?.id || null,
      fp,
      source: detail.headers["x-gdi-detail-source"] || null,
    });
  }
}

await check("all_66_staged_details_open", () => {
  assert.equal(detailFails.length, 0, JSON.stringify(detailFails.slice(0, 5)));
  assert.deepEqual([...fps], [EXPECTED_FP]);
});

await check("fhlip_2027_detail_opens", async () => {
  const detail = await callDetail(FHLIP, {
    publicationPackage: "1",
    surface: "customer",
    preview: "staged",
  });
  assert.equal(detail.statusCode, 200);
  assert.equal(detail.body?.opportunity?.id, FHLIP);
  assert.match(
    String(
      detail.body?.opportunity?.displayTitle ||
        detail.body?.opportunity?.title ||
        "",
    ),
    /FHLIP/i,
  );
  assert.equal(
    detail.body?.publication?.contentFingerprintSha256,
    EXPECTED_FP,
  );
});

await check("live_path_cannot_open_staged_only_fhlip", async () => {
  const detail = await callDetail(FHLIP, { live: "1" });
  assert.equal(detail.statusCode, 404);
  assert.equal(detail.body?.ok, false);
});

await check("customer_package_without_staged_flag_still_resolves_via_fallback", async () => {
  // No CURRENT for Radisson → resolver falls back to STAGED without leaking live.
  const detail = await callDetail(FHLIP, {
    publicationPackage: "1",
    surface: "customer",
  });
  assert.equal(detail.statusCode, 200);
  assert.equal(detail.body?.opportunity?.id, FHLIP);
  assert.equal(detail.body?.publication?.previewSource, "STAGED");
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log(
  JSON.stringify(
    {
      ok: true,
      hotelId: HOTEL,
      fingerprint: EXPECTED_FP,
      all: opps.length,
      hotelMarket: hm.length,
      cmi: cmi.length,
      detailFails: detailFails.length,
      fhlipId: FHLIP,
    },
    null,
    2,
  ),
);
console.log("\nOK test:gdi-opportunity-drawer-fetch-remediation-v1");
