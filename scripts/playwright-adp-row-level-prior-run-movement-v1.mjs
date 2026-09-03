#!/usr/bin/env node
/**
 * Playwright: Core + BPP row-level Δ vs Prior Run + rank movement.
 * Local:  npm run playwright:adp-row-level-prior-run-movement-v1
 * Prod:   npm run playwright:production-adp-row-level-prior-run-movement-v1
 *
 * Uses existing production share inventory when --production.
 * Local default: http://127.0.0.1:PORT with share tokens from inventory if available,
 * otherwise skips share and expects authenticated session is not required for share URLs.
 */

import { chromium } from "playwright";
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { loadCustomerPublishedBrandPortfolio } from "../api/ai-demand-positioning.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";

const production = process.argv.includes("--production");
const WIDTHS = [1440, 1024, 390];
const OUT = join(process.cwd(), "reports/ai-demand-positioning/row-level-prior-run-movement");
const PROD_HOST = "https://my-operators-backend-production.up.railway.app";
const LOCAL_HOST = process.env.ADP_LOCAL_BASE || "http://127.0.0.1:8080";

const INV_CANDIDATES = [
  "reports/client-share-links/PRODUCTION_CLIENT_SHARE_LINK_INVENTORY_2026-09-03T04-34-03.json",
];

function loadInventory() {
  for (const rel of INV_CANDIDATES) {
    if (!existsSync(rel)) continue;
    const j = JSON.parse(readFileSync(rel, "utf8"));
    const links = j.adpLinks || j.links || [];
    if (links.length) return { ...j, links };
  }
  return null;
}

function ppLike(text) {
  return /[+\-−]?\d+(?:\.\d+)?\s*pp|NEW|EXITED|RETURNED|0\.0\s*pp/i.test(String(text || ""));
}

function rankMoveLike(text) {
  return /#\d+(\s*[↑↓]\d+)?|#\d+\s+(NEW|RETURNED|EXITED)/i.test(String(text || "").trim());
}

async function auditPage(page, label, link) {
  await page.waitForFunction(
    () => document.querySelectorAll("#adpCompTableBody tr").length > 0,
    { timeout: 90000 }
  );

  const share = (() => {
    try {
      return new URL(page.url()).searchParams.get("share");
    } catch {
      return null;
    }
  })();

  const api = await page.evaluate(
    async ({ propertyId, share }) => {
      const qs = share
        ? `?share=${encodeURIComponent(share)}&_cb=${Date.now()}`
        : `?_cb=${Date.now()}`;
      const res = await fetch(
        `/api/ai-demand-positioning/property/${encodeURIComponent(propertyId)}/report${qs}`,
        { cache: "no-store" }
      );
      const j = await res.json();
      const overall =
        j.competitiveRankingByTerritory?.byTerritory?.overall?.displayRows ||
        j.payload?.competitiveRankingByTerritory?.byTerritory?.overall?.displayRows ||
        [];
      const intents = Object.values(j.intentPresenceIndex || j.payload?.intentPresenceIndex || {});
      const bppRows =
        j.brandPortfolioPosition?.ranking?.rows ||
        j.payload?.brandPortfolioPosition?.ranking?.rows ||
        [];
      return {
        ok: j.ok !== false && overall.length > 0,
        status: res.status,
        overallDeltas: overall.filter((r) => r.deltaDisplay && r.deltaDisplay !== "—").length,
        overallRankMoves: overall.filter((r) => /[↑↓]|NEW|RETURNED|EXITED/.test(String(r.rankDisplay || ""))).length,
        intentDeltas: intents.filter((r) => r.deltaDisplay && r.deltaDisplay !== "—").length,
        bppDeltas: bppRows.filter((r) => r.deltaDisplay && r.deltaDisplay !== "—").length,
        sampleOverall: overall.slice(0, 3).map((r) => ({
          name: r.name,
          deltaDisplay: r.deltaDisplay,
          rankDisplay: r.rankDisplay,
        })),
        sampleIntent: intents.slice(0, 3).map((r) => ({
          territory: r.territory,
          deltaDisplay: r.deltaDisplay,
        })),
        sampleBpp: bppRows.slice(0, 3).map((r) => ({
          name: r.name,
          deltaDisplay: r.deltaDisplay,
          rankLabel: r.rankLabel,
        })),
        jsSrc: document.querySelector('script[src*="ai-demand-positioning.js"]')?.src || null,
      };
    },
    { propertyId: link.propertyId, share }
  );

  // Competitive overview DOM
  const competitive = await page.evaluate(() => {
    const rows = Array.from(document.querySelectorAll("#adpCompTable tbody tr"));
    const parsed = rows
      .map((tr) => {
        const tds = Array.from(tr.querySelectorAll("td"));
        if (tds.length < 4) return null;
        return {
          rank: (tds[0]?.innerText || "").trim(),
          name: (tds[1]?.innerText || "").trim().slice(0, 80),
          presence: (tds[2]?.innerText || "").trim(),
          delta: (tds[3]?.innerText || "").trim(),
        };
      })
      .filter(Boolean);
    return { rowCount: parsed.length, rows: parsed.slice(0, 12) };
  });

  const intent = await page.evaluate(() => {
    const rows = Array.from(document.querySelectorAll("#adpIntentTableContainer tbody tr"));
    return rows.slice(0, 10).map((tr) => {
      const tds = Array.from(tr.querySelectorAll("td"));
      return {
        territory: (tds[0]?.innerText || "").trim(),
        presence: (tds[1]?.innerText || "").trim(),
        delta: (tds[4]?.innerText || "").trim(),
      };
    });
  });

  const bpp = await page.evaluate(() => {
    const rows = Array.from(document.querySelectorAll("#adpBrandPortfolioTableBody tr"));
    if (!rows.length) return { ready: false, rows: [] };
    return {
      ready: true,
      rows: rows.slice(0, 8).map((tr) => {
        const tds = Array.from(tr.querySelectorAll("td"));
        return {
          rank: (tds[0]?.innerText || "").trim(),
          name: (tds[1]?.innerText || "").trim().slice(0, 80),
          presence: (tds[2]?.innerText || "").trim(),
          delta: (tds[3]?.innerText || "").trim(),
        };
      }),
    };
  });

  const compDeltasPopulated = competitive.rows.filter((r) => ppLike(r.delta)).length;
  const intentDeltasPopulated = intent.filter((r) => ppLike(r.delta)).length;
  const rankMoves = competitive.rows.filter((r) => /[↑↓]|NEW|RETURNED|EXITED/.test(r.rank)).length;
  const bppDeltas = bpp.rows.filter((r) => ppLike(r.delta)).length;
  const bppRankMoves = bpp.rows.filter((r) => /[↑↓]|NEW|RETURNED|EXITED/.test(r.rank)).length;

  const pass =
    api.ok &&
    api.overallDeltas >= 3 &&
    api.intentDeltas >= 3 &&
    api.bppDeltas >= 3 &&
    competitive.rows.length > 0 &&
    compDeltasPopulated >= Math.min(3, competitive.rows.length) &&
    intentDeltasPopulated >= Math.min(3, intent.length || 3) &&
    (!bpp.ready || bppDeltas >= Math.min(3, bpp.rows.length));

  return {
    label,
    pass,
    api,
    competitive: {
      rowCount: competitive.rowCount,
      deltasPopulated: compDeltasPopulated,
      rankMoves,
      sample: competitive.rows.slice(0, 5),
    },
    intent: {
      rowCount: intent.length,
      deltasPopulated: intentDeltasPopulated,
      sample: intent.slice(0, 5),
    },
    bpp: {
      ready: bpp.ready,
      deltasPopulated: bppDeltas,
      rankMoves: bppRankMoves,
      sample: bpp.rows.slice(0, 5),
    },
  };
}

async function buildLocalEnrichedReport(propertyId) {
  const result = await getPublishedOwnerReport(propertyId);
  if (!result?.ok) throw new Error(`local report failed for ${propertyId}`);
  const profile = loadPropertyProfile(propertyId);
  const brandPortfolioPosition = loadCustomerPublishedBrandPortfolio(propertyId);
  const corePayload = result.payload || {};
  return {
    ...corePayload,
    ok: true,
    propertyId,
    property: {
      ...(corePayload.property || {}),
      propertyId,
      name: profile?.name || corePayload.property?.name,
      label: profile?.name || corePayload.property?.name,
    },
    brandPortfolioPosition,
  };
}

async function installLocalShareMocks(page, propertyId) {
  const report = await buildLocalEnrichedReport(propertyId);
  const profile = loadPropertyProfile(propertyId);
  const label = profile?.name || propertyId;

  await page.addInitScript(() => {
    // Prevent Memberstack waitForLogin from blocking share-surface fetch on local mock.
    window.DealalityMemberstackAuth = undefined;
    window.authFetch = function (url, opts) {
      return fetch(url, opts);
    };
  });

  await page.route("**/api/ai-demand-positioning/share/resolve**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        propertyId,
        property: { propertyId, label, name: label },
      }),
    });
  });

  await page.route("**/api/ai-demand-positioning/publication-meta**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        bpp: {
          customerPublished: true,
          publicationVersion: "bpp-customer-v1.1-20260902-p2",
        },
      }),
    });
  });

  await page.route("**/api/ai-demand-positioning/property/**/report**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(report),
    });
  });
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  const inv = loadInventory();
  if (!inv?.links?.length) {
    throw new Error("Share inventory missing — cannot run UI audit");
  }

  const host = production ? PROD_HOST : LOCAL_HOST;
  const links = inv.links.filter((l) => l.propertyId && (l.shareUrl || l.token));

  const browser = await chromium.launch({ headless: true });
  const results = {
    stamp: new Date().toISOString(),
    production,
    host,
    widths: WIDTHS,
    audits: [],
  };

  try {
    for (const link of links) {
      for (const width of WIDTHS) {
        const page = await browser.newPage({ viewport: { width, height: 900 } });
        try {
          let shareUrl;
          if (production) {
            shareUrl = link.shareUrl.startsWith("http")
              ? link.shareUrl
              : `${PROD_HOST}${link.shareUrl}`;
          } else {
            // Local: serve static share page + mock signed APIs with enriched payloads
            // (local process often lacks ADP_SHARE_CAPABILITY_SECRET).
            await installLocalShareMocks(page, link.propertyId);
            shareUrl = `${LOCAL_HOST}/owner-ai-demand-share.html?share=adps_local_row_movement_mock&property=${encodeURIComponent(link.propertyId)}`;
          }

          await page.goto(shareUrl, { waitUntil: "domcontentloaded", timeout: 120000 });
          try {
            page.once("dialog", (d) => d.dismiss());
          } catch {
            /* ignore */
          }
          const audit = await auditPage(page, `${link.propertyId}@${width}`, link);
          audit.propertyId = link.propertyId;
          audit.width = width;
          audit.shareUrl = shareUrl;
          results.audits.push(audit);
          console.log(
            `${audit.pass ? "PASS" : "FAIL"} ${link.propertyId} ${width} ` +
              `compΔ=${audit.competitive.deltasPopulated} intentΔ=${audit.intent.deltasPopulated} ` +
              `bppΔ=${audit.bpp.deltasPopulated} apiΔ=${audit.api?.overallDeltas}`
          );
        } catch (err) {
          results.audits.push({
            propertyId: link.propertyId,
            width,
            pass: false,
            error: err.message,
          });
          console.error(`FAIL ${link.propertyId} ${width}: ${err.message}`);
        } finally {
          await page.close();
        }
      }
    }
  } finally {
    await browser.close();
  }

  const pass = results.audits.length > 0 && results.audits.every((a) => a.pass);
  results.pass = pass;
  results.gate = pass
    ? production
      ? "LOCAL_PRODUCTION_ROW_MOVEMENT_PARITY"
      : "CUSTOMER_ROW_MOVEMENT_UI_LOCAL"
    : "FAIL";

  const outPath = join(
    OUT,
    production ? "playwright-production-row-movement-v1.json" : "playwright-local-row-movement-v1.json"
  );
  writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log(JSON.stringify({ ok: pass, outPath, failCount: results.audits.filter((a) => !a.pass).length }, null, 2));
  if (!pass) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
