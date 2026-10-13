/**
 * GDI external restore visual + acceptance smoke.
 * Usage:
 *   node scripts/gdi-external-restore-smoke.mjs --phase before
 *   node scripts/gdi-external-restore-smoke.mjs --phase restored
 *   node scripts/gdi-external-restore-smoke.mjs --phase admin
 */
import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";
import { resolveExternalClientLinks } from "../lib/admin/report-external-client-links-v1.js";

const OUT = path.resolve("reports/gdi-external-restore-v1");
const SHOTS = path.join(OUT, "screenshots");
fs.mkdirSync(SHOTS, { recursive: true });

const phase = (process.argv.includes("--phase")
  ? process.argv[process.argv.indexOf("--phase") + 1]
  : "restored"
).toLowerCase();

const HOST =
  process.env.GDI_PUBLIC_HOST ||
  "https://my-operators-backend-production.up.railway.app";

function maskUrl(u) {
  if (!u) return null;
  try {
    const x = new URL(u);
    const s = x.searchParams.get("share") || "";
    const mid = s.length > 40 ? `${s.slice(0, 20)}…${s.slice(-12)}` : s;
    x.searchParams.set("share", mid);
    return `${x.toString()} (len=${u.length})`;
  } catch {
    return String(u).slice(0, 80);
  }
}

async function probeShare(page, url, shotName) {
  const apiHits = [];
  page.on("response", (res) => {
    const u = res.url();
    if (u.includes("/api/group-demand-intelligence/share/")) {
      apiHits.push({ status: res.status(), url: u.split("?")[0] });
    }
  });

  await page.goto(url, { waitUntil: "networkidle", timeout: 90000 });
  await page.waitForTimeout(2500);

  const text = await page.locator("body").innerText();
  const hasDemandReportTab = /Demand\s*Report/i.test(text) &&
    (await page.locator('button[role="tab"], .section-nav-item').filter({ hasText: /Demand/i }).count()) > 0;
  const hasViewPdf = (await page.locator("#gdiShareViewPdf, button:has-text('View PDF')").count()) > 0;
  const hasDownloadPdf = (await page.locator("#gdiShareDownloadPdf, button:has-text('Download PDF')").count()) > 0;
  const hasReset = (await page.locator("#gdiResetViewBtn, button:has-text('Reset View')").count()) > 0;
  const hasMainTab = (await page.locator(".section-nav-item, button[role='tab']").filter({ hasText: /Group/i }).count()) > 0;
  const hasCards = (await page.locator("[data-open], .gdi-opp-card, article.gdi-card").count()) > 0;
  const hasReadOnly = /read[-\s]?only/i.test(text);
  const hasDisclaimer = /Share brief for review only/i.test(text);
  const pdfApiHit = apiHits.some((h) => /pdf-report|report-pdf/i.test(h.url));

  await page.screenshot({
    path: path.join(SHOTS, shotName),
    fullPage: true,
  });

  // Open first opportunity if present
  const first = page.locator("[data-open]").first();
  let detailOpen = false;
  if (await first.count()) {
    await first.click();
    await page.waitForTimeout(800);
    detailOpen = await page.locator("#gdiShareDrawer[open], dialog#gdiShareDrawer").count() > 0
      || await page.locator("#gdiShareDrawerBody").innerText().then((t) => t.trim().length > 20).catch(() => false);
    await page.screenshot({
      path: path.join(SHOTS, shotName.replace(".png", "-detail.png")),
      fullPage: true,
    });
  }

  return {
    hasDemandReportTab,
    hasViewPdf,
    hasDownloadPdf,
    hasReset,
    hasMainTab,
    hasCards,
    hasReadOnly,
    hasDisclaimer,
    detailOpen,
    pdfApiHit,
    apiHits: apiHits.slice(0, 20),
    shotName,
  };
}

async function main() {
  const links = resolveExternalClientLinks("recLuxvwwxID7U2B8", {
    createIfMissing: false,
  });
  const shareUrl = links?.gdi?.url;
  if (!shareUrl) throw new Error("Bethesda GDI share URL missing");
  if (!String(links.gdi.tokenId).includes("47c25d74")) {
    throw new Error(`Bethesda token mutated: ${links.gdi.tokenId}`);
  }

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

  let result;
  if (phase === "before") {
    result = await probeShare(page, shareUrl, "BEFORE_BAD_CURRENT.png");
  } else if (phase === "lastgood") {
    // Local restored server expected at LOCAL_BASE
    const local =
      process.env.LOCAL_BASE || "http://127.0.0.1:3847";
    const u = new URL(shareUrl);
    const localUrl = `${local}${u.pathname}${u.search}`;
    result = await probeShare(page, localUrl, "LAST_GOOD_REFERENCE.png");
  } else if (phase === "restored") {
    result = await probeShare(page, shareUrl, "RESTORED_PRODUCTION.png");
  } else if (phase === "admin") {
    // Public HTML markers only — Admin requires Memberstack; verify static + API route presence via fetch
    const adminPage = await page.goto(`${HOST}/app/admin/ai-demand-admin.html`, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });
    const html = await page.content();
    result = {
      adminHtmlLoaded: adminPage.ok(),
      hasGdiReportsCopy: /GDI Reports|gdi-reports|Generate PDF|Report Archive/i.test(html),
      note: "Full Generate/View/Download requires Admin auth; route presence checked separately",
    };
    await page.screenshot({
      path: path.join(SHOTS, "ADMIN_PAGE.png"),
      fullPage: true,
    });
  } else {
    throw new Error(`Unknown phase ${phase}`);
  }

  await browser.close();

  const out = {
    phase,
    at: new Date().toISOString(),
    host: HOST,
    tokenId: links.gdi.tokenId,
    shareUrlMasked: maskUrl(shareUrl),
    result,
  };
  const outPath = path.join(OUT, `smoke-${phase}.json`);
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  console.log(JSON.stringify(out, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
