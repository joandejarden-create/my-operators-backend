#!/usr/bin/env node
/**
 * Browser regression: Bethesda Premier Cup drawer must render Hotel Need-Date Match
 * on the live external share (actual DOM, not API-only).
 *
 *   GDI_PARITY_SHARE_TOKEN=... node scripts/test-gdi-premier-cup-need-date-drawer-dom-v1.mjs
 *   Optional: GDI_PARITY_PRODUCTION_URL
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = path.join(
  ROOT,
  "reports/gdi-bethesda-rendered-parity-root-cause-v1",
);
fs.mkdirSync(path.join(outDir, "screenshots"), { recursive: true });

const share =
  process.env.GDI_PARITY_SHARE_TOKEN ||
  (fs.readFileSync(path.join(ROOT, ".env"), "utf8").match(
    /^GDI_BETHESDA_PRODUCTION_SHARE_TOKEN=(.+)$/m,
 ) || [])[1]?.trim();
assert.ok(share, "GDI_PARITY_SHARE_TOKEN or .env token required");

const base = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");
const url = `${base}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`;

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const result = {
  ok: false,
  url: base + "/group-demand-intelligence-share.html?share=<token>",
  sections: [],
  needDateSectionPresent: false,
  needDateText: null,
  errors: [],
};

try {
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120000 });
  await page.waitForSelector("text=Bethesda Premier Cup", { timeout: 120000 });
  await page.locator("text=Bethesda Premier Cup").first().click();
  await page.waitForTimeout(2000);

  // Prefer detail API readiness then DOM.
  await page.waitForSelector(
    '[role="dialog"] h3, .gdi-drawer-body h3, dialog h3',
    { timeout: 30000 },
  );

  const extracted = await page.evaluate(() => {
    const root =
      document.querySelector("[role='dialog']") ||
      document.querySelector("dialog[open]") ||
      document.querySelector(".gdi-drawer-body")?.closest("dialog") ||
      document.querySelector(".gdi-drawer-body");
    if (!root) return { sections: [], text: "", needDateEl: null };
    const headings = [...root.querySelectorAll("h3")].map((h) =>
      (h.textContent || "").trim(),
    );
    const needEl = root.querySelector(
      '[data-section-id="hotel_need_date_match"]',
    );
    return {
      sections: headings,
      text: (root.innerText || "").slice(0, 4000),
      needDateEl: needEl
        ? {
            present: true,
            html: needEl.outerHTML.slice(0, 1500),
            text: (needEl.innerText || "").slice(0, 1200),
          }
        : { present: false },
    };
  });

  result.sections = extracted.sections;
  result.needDateSectionPresent = Boolean(extracted.needDateEl?.present);
  result.needDateText = extracted.needDateEl?.text || null;
  result.overviewIndex = extracted.sections.findIndex((s) =>
    /opportunity overview/i.test(s),
  );
  result.needDateIndex = extracted.sections.findIndex((s) =>
    /need-date match/i.test(s),
  );
  result.whatIndex = extracted.sections.findIndex((s) =>
    /what is happening/i.test(s),
  );

  await page.screenshot({
    path: path.join(outDir, "screenshots/premier-cup-drawer-dom.png"),
    fullPage: false,
  });

  assert.ok(
    result.needDateSectionPresent,
    "Hotel Need-Date Match section missing from rendered drawer DOM",
  );
  assert.ok(
    result.needDateIndex > result.overviewIndex,
    "Need-Date Match must follow Opportunity overview",
  );
  assert.ok(
    result.whatIndex < 0 || result.needDateIndex < result.whatIndex,
    "Need-Date Match must precede What is happening when both present",
  );
  assert.match(
    String(result.needDateText || ""),
    /November 2026/i,
    "Need-Date Match body must include November 2026",
  );

  result.ok = true;
} catch (e) {
  result.errors.push(String(e?.stack || e));
} finally {
  await browser.close();
}

fs.writeFileSync(
  path.join(outDir, "03-PREMIER-CUP-ROOT-CAUSE.json"),
  JSON.stringify(
    {
      ...result,
      rootCausePendingDeploy: !result.ok,
      diagnosedCause:
        "getGdiShareOpportunityDetail ignored publication package; drawer fetched live detail without needDateMatchV1",
    },
    null,
    2,
  ),
);

console.log(JSON.stringify(result, null, 2));
process.exit(result.ok ? 0 : 2);
