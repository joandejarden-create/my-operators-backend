#!/usr/bin/env node
import { chromium } from "playwright";

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
await page.goto(
  "http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recCAGqkERSXgiku4&preview=staged",
  { waitUntil: "networkidle", timeout: 90000 },
);
await page.waitForSelector(".brand-card--gdi-opp", { timeout: 30000 });
await page.waitForTimeout(2000);

const rows = await page.evaluate(() => {
  return Array.from(document.querySelectorAll(".brand-card--gdi-opp")).map(
    (card) => {
      const title = String(
        card.querySelector(".brand-card__name")?.innerText || "",
      ).trim();
      const pills = Array.from(
        card.querySelectorAll(".gdi-tile-pill, .gdi-pill"),
      ).map((p) => String(p.innerText || "").trim());
      const text = String(card.innerText || "");
      return {
        title: title.slice(0, 70),
        pills,
        hasJan12: /Jan 12,? 2026|2026-01-12/i.test(text),
        hasNov10: /Nov 10,? 2026/i.test(text),
        hasNov4: /Nov 4,? 2026/i.test(text),
        hasJul2: /Jul 2,? 2026|July 2,? 2026/i.test(text),
        hasOngoing: /Ongoing/i.test(text),
        has2027Tbd: /2027 Dates TBD/i.test(text),
        hasDatesTbd: /Dates TBD/i.test(text),
      };
    },
  );
});

const focus = rows.filter((r) =>
  /Marat|Convenio|Macrolibramiento|Atlas|Foxconn|Megafactory|FIL|Notariado|CEO Summit|Endocrin|IMIN|Smart Tech|CICOM|Sinergia/i.test(
    r.title,
  ),
);
const contaminatedVisible = rows.filter(
  (r) => r.hasJan12 || r.hasNov10 || r.hasNov4 || r.hasJul2,
);

console.log(
  JSON.stringify(
    {
      total: rows.length,
      focus,
      contaminatedVisibleCount: contaminatedVisible.length,
      contaminatedVisible: contaminatedVisible.slice(0, 15),
    },
    null,
    2,
  ),
);

await browser.close();
if (contaminatedVisible.length) process.exit(2);
