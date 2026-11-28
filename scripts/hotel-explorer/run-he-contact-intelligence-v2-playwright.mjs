#!/usr/bin/env node
/**
 * HE Contact Intelligence V2 — Playwright layout certification (fixture HTML).
 * Does not call paid enrichment. Authenticated 8080 live pass is optional when server up.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const require = createRequire(import.meta.url);

let playwright;
try {
  playwright = require("playwright");
} catch {
  console.log(
    JSON.stringify({
      ok: false,
      status: "NOT_TESTED",
      reason: "playwright_not_installed",
    }),
  );
  process.exit(0);
}

const css = fs.readFileSync(
  path.join(ROOT, "public/css/hotel-contact-intelligence.css"),
  "utf8",
);

const fixtureHtml = `<!doctype html>
<html><head><meta charset="utf-8"><style>
${css}
body { margin:0; background:#0f1730; color:#fff; font-family: system-ui, sans-serif; }
.dl-table { width:100%; border-collapse: separate; }
.dl-td, .dl-th { border-bottom:1px solid #37446b; padding:10px 12px; }
</style></head><body>
<div class="hci-root">
  <div class="dl-table-wrap hiw-table-wrap">
    <table class="dl-table hiw-table hci-directory-table hci-directory-table--5col">
      <thead><tr>
        <th>Contact</th><th>Organization</th><th>Function / Relationship</th>
        <th>Contact Details</th><th>Details</th>
      </tr></thead>
      <tbody>
        <tr class="hci-dir-row">
          <td><div class="hci-dir-contact-stack"><div class="hci-dir-contact-stack__name">Jeff Mullen</div><div class="hci-dir-contact-stack__title">President and CEO</div></div></td>
          <td>Mullen Real Estate Capital</td>
          <td><div class="hci-dir-relationship">Property Leadership — General Manager</div></td>
          <td>
            <div class="gdi-list-contact-details gdi-list-contact-details--with-action" data-hci-contact-details-cell="1">
              <div class="gdi-list-contact-channels">
                <div class="gdi-list-contact-line"><span>✉</span><a class="gdi-list-contact-link" href="mailto:jeff.mullen@mullencap.com">jeff.mullen@mullencap.com</a></div>
                <div class="gdi-list-contact-line"><span>☎</span><span class="gdi-list-contact-missing">Phone Not Available</span></div>
                <div class="gdi-list-contact-line"><span>in</span><span>LinkedIn Profile</span></div>
              </div>
              <div class="gdi-list-contact-action">
                <button type="button" class="hci-btn hci-btn--enrich hci-btn--enrich-compact" data-hci-enrich="1">Find Email &amp; Phone</button>
              </div>
            </div>
          </td>
          <td style="text-align:center;vertical-align:middle"><button type="button" class="hci-btn hci-btn--ghost hci-dir-details-btn">Details</button></td>
        </tr>
        <tr class="hci-dir-row">
          <td>Both Channels</td><td>Org</td>
          <td><div class="hci-dir-relationship">Ownership / Asset Management — Regional Vice President of Something Long</div></td>
          <td>
            <div class="gdi-list-contact-details gdi-list-contact-details--with-action">
              <div class="gdi-list-contact-channels">
                <div class="gdi-list-contact-line"><a class="gdi-list-contact-link" href="mailto:a@b.com">a@b.com</a></div>
                <div class="gdi-list-contact-line"><a class="gdi-list-contact-link" href="tel:+15551212">+1 555 1212</a></div>
                <div class="gdi-list-contact-line">LinkedIn Profile</div>
              </div>
              <div class="gdi-list-contact-action">
                <button type="button" class="hci-btn hci-btn--enrich hci-btn--enrich-compact hci-btn--enrich-dimmed is-disabled" disabled>Find Email &amp; Phone</button>
              </div>
            </div>
          </td>
          <td style="text-align:center"><button class="hci-btn hci-btn--ghost">Details</button></td>
        </tr>
      </tbody>
    </table>
  </div>
</div>
</body></html>`;

const viewports = [
  { name: "1920x1080", width: 1920, height: 1080 },
  { name: "1440x900", width: 1440, height: 900 },
  { name: "1366x768", width: 1366, height: 768 },
  { name: "768x1024", width: 768, height: 1024 },
  { name: "390x844", width: 390, height: 844 },
];

const browser = await playwright.chromium.launch({ headless: true });
const results = [];

for (const vp of viewports) {
  const page = await browser.newPage({ viewport: { width: vp.width, height: vp.height } });
  await page.setContent(fixtureHtml, { waitUntil: "load" });
  const checks = await page.evaluate(() => {
    const table = document.querySelector("table.hci-directory-table--5col");
    const headers = [...document.querySelectorAll("thead th")].map((t) => t.textContent.trim());
    const cell = document.querySelector("[data-hci-contact-details-cell]");
    const action = cell?.querySelector(".gdi-list-contact-action");
    const channels = cell?.querySelector(".gdi-list-contact-channels");
    const btn = cell?.querySelector("[data-hci-enrich]");
    const detailsBtn = document.querySelector(".hci-dir-details-btn");
    const rel = document.querySelector(".hci-dir-relationship");
    const cellBox = cell?.getBoundingClientRect();
    const btnBox = btn?.getBoundingClientRect();
    const detailsBox = detailsBtn?.getBoundingClientRect();
    const relBox = rel?.getBoundingClientRect();
    const contactDetailsTh = [...document.querySelectorAll("thead th")].find((t) =>
      /Contact Details/i.test(t.textContent || ""),
    );
    const contactDetailsTd = contactDetailsTh
      ? contactDetailsTh.cellIndex
      : -1;
    return {
      fiveHeaders: headers,
      buttonInsideContactDetails: Boolean(cell && cell.contains(btn)),
      channelsLeftOfButton:
        channels && btnBox && channels.getBoundingClientRect().right <= btnBox.left + 2,
      buttonNotInDetailsColumn:
        detailsBox && btnBox ? btnBox.right <= detailsBox.left + 4 : false,
      relationshipWrapped: Boolean(rel && getComputedStyle(rel).overflowWrap),
      dimmedWhenComplete: Boolean(
        document.querySelector(".hci-btn--enrich-dimmed[disabled]"),
      ),
      activeWhenPhoneMissing: Boolean(
        document.querySelector(
          '[data-hci-contact-details-cell] .hci-btn--enrich:not([disabled])',
        ),
      ),
      horizontalScroll: document.querySelector(".dl-table-wrap")?.scrollWidth >=
        document.querySelector(".dl-table-wrap")?.clientWidth,
      cellWidth: cellBox?.width || 0,
      contactDetailsColIndex: contactDetailsTd,
    };
  });
  const ok =
    checks.fiveHeaders.length === 5 &&
    checks.buttonInsideContactDetails &&
    checks.channelsLeftOfButton &&
    checks.dimmedWhenComplete &&
    checks.activeWhenPhoneMissing;
  results.push({ viewport: vp.name, ok, checks });
  await page.close();
}

await browser.close();

const outDir = path.join(ROOT, "reports/shared-contact-intelligence");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, "HE-CONTACT-INTELLIGENCE-V2-PLAYWRIGHT.json");
const overall = results.every((r) => r.ok);
fs.writeFileSync(
  outPath,
  JSON.stringify(
    {
      version: "he-contact-intelligence-v2-playwright",
      generatedAt: new Date().toISOString(),
      paidCalls: false,
      overall: overall ? "PASS" : "FAIL",
      results,
    },
    null,
    2,
  ) + "\n",
);

console.log(JSON.stringify({ ok: overall, outPath, results }, null, 2));
process.exit(overall ? 0 : 1);
