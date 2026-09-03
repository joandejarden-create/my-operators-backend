#!/usr/bin/env node
/**
 * PRODUCTION_CORE_TRENDS_MATCH_VERIFIED_LOCAL
 * npm run test:production-core-trends-match-verified-local-v1
 */

import { createHash } from "crypto";
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import { ADP_CERTIFIED_PROPERTY_IDS } from "../lib/ai-demand-positioning/contracts/adp-certified-property-cohort-v1.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";

export const PRODUCTION_CORE_TRENDS_MATCH_VERIFIED_LOCAL =
  "PRODUCTION_CORE_TRENDS_MATCH_VERIFIED_LOCAL";

const EXPECTED_CACHE = "adp-v76-20260903-trends-p2";
const OUT_DIR = join(process.cwd(), "reports/ai-demand-positioning/core-trends-period2-wiring");

function sha(s) {
  return createHash("sha256").update(s).digest("hex").slice(0, 16);
}

function near(a, b, eps = 0.15) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= eps;
}

function loadInventory() {
  const p = join(
    process.cwd(),
    "reports/client-share-links/PRODUCTION_CLIENT_SHARE_LINK_INVENTORY_2026-09-03T04-34-03.json"
  );
  return JSON.parse(readFileSync(p, "utf8"));
}

async function fetchProd(link) {
  const base = link.shareUrl.split("/owner-ai-demand-share.html")[0];
  const share = new URL(link.shareUrl).searchParams.get("share");
  const url = `${base}/api/ai-demand-positioning/property/${encodeURIComponent(link.propertyId)}/report?share=${encodeURIComponent(share)}&_cb=${Date.now()}`;
  const res = await fetch(url, { headers: { "cache-control": "no-cache", pragma: "no-cache" } });
  const text = await res.text();
  let j = null;
  try {
    j = JSON.parse(text);
  } catch {
    /* ignore */
  }
  return { status: res.status, cacheControl: res.headers.get("cache-control"), payload: j };
}

async function main() {
  const inv = loadInventory();
  const htmlRes = await fetch(`${inv.publicBase}/owner-ai-demand-share.html?_cb=${Date.now()}`, {
    headers: { "cache-control": "no-cache" },
  });
  const html = await htmlRes.text();
  const jsCacheBust = (html.match(/ai-demand-positioning\.js\?v=([^"']+)/) || [])[1] || null;
  const cacheToken = existsSync("data/ai-demand-positioning/published/_cache_token.json")
    ? JSON.parse(readFileSync("data/ai-demand-positioning/published/_cache_token.json", "utf8")).token
    : null;

  const rows = [];
  let pass = true;
  for (const propertyId of ADP_CERTIFIED_PROPERTY_IDS) {
    const localReport = await getPublishedOwnerReport(propertyId);
    const local = localReport.payload;
    const link = (inv.adpLinks || []).find((l) => l.propertyId === propertyId);
    if (!link) {
      pass = false;
      rows.push({ propertyId, ok: false, error: "missing_share_link" });
      continue;
    }
    const prod = await fetchProd(link);
    const p = prod.payload;
    const localTrends = local?.trends || [];
    const prodTrends = p?.trends || [];
    const localPeriod = local?.period?.periodId;
    const prodPeriod = p?.period?.periodId || p?.periodId;
    const localPrior = local?.executiveMetrics?.currentVsPrior?.priorComparablePeriodId || null;
    const prodPrior = p?.executiveMetrics?.currentVsPrior?.priorComparablePeriodId || null;
    const metricMatch =
      localTrends.length === prodTrends.length &&
      localTrends.every((t, i) => {
        const u = prodTrends[i];
        return (
          t.periodId === u?.periodId &&
          near(t.considerationRate, u?.considerationRate) &&
          near(t.scenarioPresenceRate, u?.scenarioPresenceRate) &&
          near(t.propertyRealityCoverage, u?.propertyRealityCoverage)
        );
      });
    const rowPass =
      prod.status === 200 &&
      localPeriod === prodPeriod &&
      localTrends.length === 2 &&
      prodTrends.length === 2 &&
      localPrior === prodPrior &&
      Boolean(prodPrior) &&
      metricMatch &&
      jsCacheBust === EXPECTED_CACHE &&
      cacheToken === EXPECTED_CACHE &&
      !/20260820/.test(String(prodPeriod || ""));

    if (!rowPass) pass = false;
    rows.push({
      propertyId,
      pass: rowPass,
      localPeriod,
      prodPeriod,
      localTrendsLen: localTrends.length,
      prodTrendsLen: prodTrends.length,
      localTrendDates: localTrends.map((t) => String(t.date || "").slice(0, 10)),
      prodTrendDates: prodTrends.map((t) => String(t.date || "").slice(0, 10)),
      localPrior,
      prodPrior,
      metricMatch,
      localCr: local?.executiveMetrics?.considerationRate?.rate ?? null,
      prodCr: p?.executiveMetrics?.considerationRate?.rate ?? null,
      jsCacheBust,
      cacheToken,
      cacheControl: prod.cacheControl,
      localPayloadHash: sha(
        JSON.stringify({
          period: localPeriod,
          trends: localTrends,
          cvp: local?.executiveMetrics?.currentVsPrior,
        })
      ),
      prodPayloadHash: sha(
        JSON.stringify({
          period: prodPeriod,
          trends: prodTrends,
          cvp: p?.executiveMetrics?.currentVsPrior,
        })
      ),
    });
  }

  const result = {
    gate: PRODUCTION_CORE_TRENDS_MATCH_VERIFIED_LOCAL,
    [PRODUCTION_CORE_TRENDS_MATCH_VERIFIED_LOCAL]: pass ? "PASS" : "FAIL",
    expectedCache: EXPECTED_CACHE,
    jsCacheBust,
    cacheToken,
    publicBase: inv.publicBase,
    properties: rows,
    stamp: new Date().toISOString(),
  };

  mkdirSync(OUT_DIR, { recursive: true });
  const outPath = join(OUT_DIR, `production-core-trends-parity-${Date.now()}.json`);
  writeFileSync(outPath, JSON.stringify(result, null, 2));
  console.log(JSON.stringify({ ...result, outPath }, null, 2));
  if (!pass) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
