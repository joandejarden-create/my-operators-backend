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
  page.on("pageerror", (err) => result.errors.push(`pageerror:${err.message}`));
  page.on("console", (msg) => {
    if (msg.type() === "error") result.errors.push(`console:${msg.text()}`);
  });

  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120000 });
  await page.waitForSelector("text=Bethesda Premier Cup", { timeout: 120000 });

  // Open via data-open card control when present (more reliable than text click).
  const card = page
    .locator(
      '[data-open]:has-text("Bethesda Premier Cup"), button:has-text("Bethesda Premier Cup"), a:has-text("Bethesda Premier Cup")',
    )
    .first();
  await card.click({ timeout: 30000 });

  // Target #gdiShareDrawer only — never need-date legend / other role=dialog nodes.
  const drawer = page.locator("#gdiShareDrawer");
  await page.waitForFunction(() => {
    const d = document.querySelector("#gdiShareDrawer");
    if (!d || !d.open) return false;
    const body = d.querySelector("#gdiShareDrawerBody, .gdi-drawer-body");
    const text = (body && body.innerText) || "";
    return text.length > 80 && !/prose is not defined/i.test(text);
  }, null, { timeout: 60000 });

  // Scroll full drawer so below-fold sections enter layout/text extraction.
  await drawer.evaluate((el) => {
    const body = el.querySelector("#gdiShareDrawerBody, .gdi-drawer-body") || el;
    body.scrollTop = body.scrollHeight;
  });
  await page.waitForTimeout(500);
  await drawer.evaluate((el) => {
    const body = el.querySelector("#gdiShareDrawerBody, .gdi-drawer-body") || el;
    body.scrollTop = 0;
  });

  const extracted = await page.evaluate(() => {
    const root = document.querySelector("#gdiShareDrawer");
    if (!root || !root.open) {
      return { sections: [], text: "", needDateEl: null, drawerOpen: false };
    }
    const body =
      root.querySelector("#gdiShareDrawerBody, .gdi-drawer-body") || root;
    const headings = [
      ...body.querySelectorAll("h3, .gdi-section > h3, [data-section-id] > h3"),
    ]
      .map((h) => (h.textContent || "").replace(/\s+/g, " ").trim())
      .filter(Boolean);
    const needEl = body.querySelector(
      '[data-section-id="hotel_need_date_match"]',
    );
    return {
      drawerOpen: true,
      sections: headings,
      text: (body.innerText || "").slice(0, 6000),
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
  const nd = String(result.needDateText || "");
  assert.match(nd, /November 2026/i, "Hotel need period November 2026");
  assert.match(
    nd,
    /2026-11-13|Nov(?:ember)?\s*13/i,
    "Opportunity timing / verified demand start",
  );
  assert.match(
    nd,
    /Confirmed period overlap|overlap/i,
    "Date-match classification",
  );
  assert.match(
    nd,
    /Qualify capture for November 2026|confirm remaining inventory/i,
    "Sales verification / next-step guidance",
  );
  assert.ok(
    !/prose is not defined/i.test(String(extracted.text || "")),
    "Drawer must not crash on prose()",
  );

  result.ok = true;
} catch (e) {
  result.errors.push(String(e?.stack || e));
} finally {
  await browser.close();
}

const prior = fs.existsSync(path.join(outDir, "03-PREMIER-CUP-ROOT-CAUSE.json"))
  ? JSON.parse(
      fs.readFileSync(path.join(outDir, "03-PREMIER-CUP-ROOT-CAUSE.json"), "utf8"),
    )
  : {};
fs.writeFileSync(
  path.join(outDir, "03-PREMIER-CUP-ROOT-CAUSE.json"),
  JSON.stringify(
    {
      ...prior,
      browserDomVerification: result,
      rootCausePendingDeploy: !result.ok,
      diagnosedCause:
        prior.diagnosedCause ||
        "getGdiShareOpportunityDetail ignored publication package; drawer fetched live detail without needDateMatchV1",
      secondaryCrash:
        "whoShouldSalesContactHtml called undefined prose() after package detail began returning full DTOs",
      secondaryFixCommit: "54fd044273a3c193ae3a73205efff16a52dd43ce",
    },
    null,
    2,
  ),
);

console.log(JSON.stringify(result, null, 2));
process.exit(result.ok ? 0 : 2);
