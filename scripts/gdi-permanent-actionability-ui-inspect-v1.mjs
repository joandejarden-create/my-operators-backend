#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-permanent-commercial-actionability-gate-v1",
);

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
await page.goto(
  "http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recCAGqkERSXgiku4&preview=staged",
  { waitUntil: "networkidle", timeout: 90000 },
);
await page.waitForTimeout(6000);

const s = await page.evaluate(() => {
  const classes = {};
  for (const n of document.querySelectorAll("[class]")) {
    for (const c of String(n.className).split(/\s+/)) {
      if (/gdi|card|opp|tile|row|list/i.test(c)) {
        classes[c] = (classes[c] || 0) + 1;
      }
    }
  }
  const body = document.body?.innerText || "";
  const dataIds = Array.from(
    document.querySelectorAll("[data-opportunity-id]"),
  ).map((n) => n.getAttribute("data-opportunity-id"));
  return {
    classCounts: Object.entries(classes)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 50),
    dataOpportunityIds: dataIds.slice(0, 50),
    dataOpportunityCount: dataIds.length,
    hasNotariado: /Notariado/i.test(body),
    hasFIL: /Feria Internacional del Libro|FIL Guadalajara/i.test(body),
    hasFoxconn: /Foxconn|AI-Server|Megafactory/i.test(body),
    hasIMIN: /Semana IMIN|Hard Rock/i.test(body),
    hasSMNE: /Endocrinolog/i.test(body),
    hasCEO: /CEO Summit 2026/i.test(body),
    hasSmartTech: /Smart Technology Expo 2026/i.test(body),
    hasCICOM: /Ciclo de Conferencias M[eé]dicas|CICOM/i.test(body),
    bodyLen: body.length,
    sample: body.slice(0, 2500),
  };
});

await page.screenshot({
  path: path.join(OUT, "screenshots", "05-ui-full.png"),
  fullPage: true,
});
await browser.close();

fs.writeFileSync(
  path.join(OUT, "_ui_inspect.json"),
  `${JSON.stringify(s, null, 2)}\n`,
);
console.log(
  JSON.stringify(
    {
      dataOpportunityCount: s.dataOpportunityCount,
      hasFIL: s.hasFIL,
      hasFoxconn: s.hasFoxconn,
      hasNotariado: s.hasNotariado,
      hasSMNE: s.hasSMNE,
      hasCEO: s.hasCEO,
      topClasses: s.classCounts.slice(0, 15),
    },
    null,
    2,
  ),
);
