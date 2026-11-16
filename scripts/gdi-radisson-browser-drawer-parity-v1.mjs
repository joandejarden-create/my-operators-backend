#!/usr/bin/env node
/**
 * Open all 66 Radisson drawers on local STAGED founder preview.
 * Optional external CURRENT pass when GDI_RADISSON_SHARE_TOKEN is set
 * (token never written to reports).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { appearsTruncatedCustomerProse } from "../lib/group-demand-intelligence/presentation/gdi-customer-prose-completeness-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-radisson-production-drawer-content-remediation-v1",
);
const HOTEL = "recH9h3McgMNFQEla";
const LOCAL_URL = `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL}`;
const PROD =
  process.env.GDI_PROD_HOST ||
  "https://my-operators-backend-production.up.railway.app";
const SHARE_TOKEN = String(process.env.GDI_RADISSON_SHARE_TOKEN || "").trim();

function text(v) {
  return String(v == null ? "" : v).trim();
}

async function auditSurface(page, label) {
  await page.waitForSelector(".brand-card--gdi-opp", { timeout: 60000 });
  await page.waitForTimeout(1200);
  const cardCount = await page.locator(".brand-card--gdi-opp").count();
  const drawers = [];
  for (let i = 0; i < cardCount; i++) {
    const card = page.locator(".brand-card--gdi-opp").nth(i);
    const title = text(
      await card.locator(".brand-card__name").first().innerText().catch(() => ""),
    );
    await card.scrollIntoViewIfNeeded();
    await card.click();
    await page.waitForTimeout(350);
    const info = await page.evaluate(() => {
      const drawer = document.querySelector(
        "#gdiDrawer, .gdi-drawer, [data-gdi-drawer], [role='dialog']",
      );
      const body = drawer || document.body;
      const t = body?.innerText || "";
      const section = (labelRe) => {
        const re = new RegExp(
          labelRe + "[\\s\\S]{0,40}?\\n([\\s\\S]{20,900}?)(?=\\n[A-Z][A-Za-z ].{0,40}\\n|$)",
          "i",
        );
        const m = t.match(re);
        return (m && m[1] ? m[1] : "").replace(/\s+/g, " ").trim();
      };
      return {
        drawerFound: Boolean(drawer),
        hasPotential: /Potential business|Potential Hotel Business/i.test(t),
        hasWhyHotel: /Why this hotel/i.test(t),
        hasNext: /Recommended next step|Next sales action/i.test(t),
        hasContact: /Who to contact|Contact/i.test(t),
        duplicateTitle: (() => {
          const lines = t
            .split(/\n+/)
            .map((x) => x.trim())
            .filter(Boolean);
          return lines.length >= 2 && lines[0] && lines[0] === lines[1];
        })(),
        whyHotel: section("Why this hotel"),
        potential: section("Potential (hotel )?business"),
        next: section("Recommended next step|Next sales action"),
        snippet: t.slice(0, 240),
      };
    });
    const truncFields = [];
    if (appearsTruncatedCustomerProse(info.whyHotel)) truncFields.push("whyThisHotel");
    if (appearsTruncatedCustomerProse(info.potential))
      truncFields.push("potentialHotelBusiness");
    if (appearsTruncatedCustomerProse(info.next))
      truncFields.push("recommendedNextStep");
    drawers.push({
      index: i,
      title,
      drawerFound: info.drawerFound,
      hasPotential: info.hasPotential,
      hasWhyHotel: info.hasWhyHotel,
      hasNext: info.hasNext,
      hasContact: info.hasContact,
      duplicateTitle: info.duplicateTitle,
      truncFields,
    });
    await page.keyboard.press("Escape");
    await page.waitForTimeout(80);
  }
  const shotDir = path.join(OUT, "screenshots");
  fs.mkdirSync(shotDir, { recursive: true });
  await page.screenshot({
    path: path.join(shotDir, `${label}-all-cards.png`),
    fullPage: true,
  });
  return {
    label,
    cardCount,
    drawersOpened: drawers.length,
    allDrawersFound: drawers.every((d) => d.drawerFound),
    truncationHits: drawers.filter((d) => d.truncFields.length).length,
    duplicateTitleHits: drawers.filter((d) => d.duplicateTitle).length,
    missingCommercialSection: drawers.filter(
      (d) => !(d.hasPotential && d.hasWhyHotel && d.hasNext && d.hasContact),
    ).length,
    drawers,
  };
}

async function apiDetailScan(base, previewStaged) {
  const listUrl = previewStaged
    ? `${base}/api/group-demand-intelligence/hotels/${HOTEL}/opportunities?publicationPackage=1&surface=customer&preview=staged`
    : `${base}/api/group-demand-intelligence/hotels/${HOTEL}/opportunities?publicationPackage=1&surface=customer`;
  const list = await fetch(listUrl).then((r) => r.json());
  const opps = list.opportunities || [];
  const rows = [];
  for (const o of opps) {
    const detailUrl = previewStaged
      ? `${base}/api/group-demand-intelligence/hotels/${HOTEL}/opportunities/${encodeURIComponent(o.id)}?publicationPackage=1&surface=customer&preview=staged`
      : `${base}/api/group-demand-intelligence/hotels/${HOTEL}/opportunities/${encodeURIComponent(o.id)}?publicationPackage=1&surface=customer`;
    const res = await fetch(detailUrl);
    const json = await res.json().catch(() => ({}));
    rows.push({
      id: o.id,
      status: res.status,
      ok: res.ok && json.ok !== false,
      previewSource: json.publication?.previewSource || null,
      fingerprint:
        json.publication?.contentFingerprintSha256 ||
        json.opportunity?._contentFingerprintSha256 ||
        null,
    });
  }
  return {
    listFingerprint: list.publication?.contentFingerprintSha256 || null,
    previewSource: list.publication?.previewSource || null,
    count: opps.length,
    allDetailOk: rows.every((r) => r.ok && r.status === 200),
    rows,
  };
}

const browser = await chromium.launch({ headless: true });
const localPage = await browser.newPage({ viewport: { width: 1440, height: 900 } });
await localPage.goto(LOCAL_URL, { waitUntil: "networkidle", timeout: 90000 });
const localBrowser = await auditSurface(localPage, "local-staged");
const localApi = await apiDetailScan("http://127.0.0.1:8080", true);

let externalBrowser = null;
if (SHARE_TOKEN) {
  const extPage = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const shareUrl = `${PROD}/group-demand-intelligence-share.html?hotelId=${HOTEL}`;
  await extPage.addInitScript((token) => {
    try {
      sessionStorage.setItem("gdiShareBearer", token);
      localStorage.setItem("gdiShareBearer", token);
    } catch (_) {
      /* ignore */
    }
  }, SHARE_TOKEN);
  // Prefer Authorization header injection via route if page reads query/hash token patterns.
  await extPage.route("**/api/group-demand-intelligence/share/**", async (route) => {
    const headers = {
      ...route.request().headers(),
      authorization: `Bearer ${SHARE_TOKEN}`,
    };
    await route.continue({ headers });
  });
  await extPage.goto(shareUrl, { waitUntil: "networkidle", timeout: 90000 });
  externalBrowser = await auditSurface(extPage, "external-current");
}

await browser.close();

const report = {
  schema: "gdi-radisson-browser-drawer-parity-v1",
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL,
  local: {
    url: LOCAL_URL,
    browser: {
      cardCount: localBrowser.cardCount,
      drawersOpened: localBrowser.drawersOpened,
      allDrawersFound: localBrowser.allDrawersFound,
      truncationHits: localBrowser.truncationHits,
      duplicateTitleHits: localBrowser.duplicateTitleHits,
      missingCommercialSection: localBrowser.missingCommercialSection,
    },
    api: {
      count: localApi.count,
      previewSource: localApi.previewSource,
      listFingerprint: localApi.listFingerprint,
      allDetailOk: localApi.allDetailOk,
    },
  },
  external: externalBrowser
    ? {
        browser: {
          cardCount: externalBrowser.cardCount,
          drawersOpened: externalBrowser.drawersOpened,
          allDrawersFound: externalBrowser.allDrawersFound,
          truncationHits: externalBrowser.truncationHits,
          duplicateTitleHits: externalBrowser.duplicateTitleHits,
          missingCommercialSection: externalBrowser.missingCommercialSection,
        },
        note: "Live CURRENT package until founder publishes staged fingerprint",
      }
    : {
        skipped: true,
        reason: "GDI_RADISSON_SHARE_TOKEN not set in environment (token never logged)",
      },
  expectedStagedFingerprint:
    "f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d",
  verdict: {
    localStagedApiFingerprintMatch:
      localApi.listFingerprint ===
      "f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d",
    localAll66DrawersOpened:
      localBrowser.drawersOpened === 66 && localBrowser.allDrawersFound,
    localNoTruncations: localBrowser.truncationHits === 0,
    localApiAllDetailOk: localApi.allDetailOk === true,
  },
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "10-BROWSER_LOCAL_EXTERNAL_66.json"),
  JSON.stringify(report, null, 2),
);
console.log(JSON.stringify(report.verdict, null, 2));
console.log(`Wrote ${path.join(OUT, "10-BROWSER_LOCAL_EXTERNAL_66.json")}`);

if (
  !(
    report.verdict.localStagedApiFingerprintMatch &&
    report.verdict.localAll66DrawersOpened &&
    report.verdict.localNoTruncations &&
    report.verdict.localApiAllDetailOk
  )
) {
  process.exit(1);
}
