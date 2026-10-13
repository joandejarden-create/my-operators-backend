import { chromium } from "playwright";
import { resolveExternalClientLinks } from "../lib/admin/report-external-client-links-v1.js";
import fs from "node:fs";

const tokens = JSON.parse(
  fs.readFileSync(
    "config/client-share/gdi-share-registry/active-tokens.json",
    "utf8"
  )
).tokens;
const active = Object.values(tokens).filter(
  (t) => t.status === "ACTIVE" && !t.revokedAt
);
const find = (re) => active.find((t) => re.test(String(t.label || "")));
const names = [
  ["Hilton New York Times Square", /Hilton.*Times Square/i],
  ["Renaissance New York Times Square", /Renaissance.*Times Square/i],
  ["Waterstone", /Waterstone/i],
  ["Cambridge Beaches", /Cambridge Beaches/i],
  ["NOW NOW NOHO", /NOW NOW NOHO|NOHO/i],
];

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1280, height: 800 } });
const results = [];
for (const [name, re] of names) {
  const row = find(re);
  if (!row) {
    results.push({ name, skipped: true, reason: "no_active_share" });
    continue;
  }
  const r = resolveExternalClientLinks(row.hotelId, { createIfMissing: false });
  if (!r?.gdi?.url) {
    results.push({ name, skipped: true, reason: "no_url" });
    continue;
  }
  await page.goto(r.gdi.url, { waitUntil: "networkidle", timeout: 90000 });
  await page.waitForTimeout(1500);
  const text = await page.locator("body").innerText();
  const demandNav =
    (await page
      .locator(".section-nav-item,button[role=tab]")
      .filter({ hasText: /Demand/i })
      .count()) > 0;
  results.push({
    name,
    hotelId: row.hotelId,
    hasDemandReportTab: /Demand\s*Report/i.test(text) && demandNav,
    hasViewPdf:
      (await page.locator("#gdiShareViewPdf,button:has-text('View PDF')").count()) >
      0,
    hasDownloadPdf:
      (await page
        .locator("#gdiShareDownloadPdf,button:has-text('Download PDF')")
        .count()) > 0,
    hasReset:
      (await page.locator("#gdiResetViewBtn,button:has-text('Reset View')").count()) >
      0,
    hasMainTab:
      (await page
        .locator(".section-nav-item,button[role=tab]")
        .filter({ hasText: /Group/i })
        .count()) > 0,
  });
}
await browser.close();
fs.writeFileSync(
  "reports/gdi-external-restore-v1/other-shares-browser.json",
  JSON.stringify(results, null, 2)
);
console.log(JSON.stringify(results, null, 2));
