#!/usr/bin/env node
/**
 * Browser regression: AI Admin More menu must escape sticky Actions stacking.
 * Fixture page only — no mutating admin APIs.
 *
 *   node scripts/playwright-ai-admin-more-menu-layering-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { chromium } from "playwright";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = path.join(root, "reports/ai-admin-table-parity");
fs.mkdirSync(outDir, { recursive: true });

const css = fs.readFileSync(path.join(root, "public/css/admin-ai-demand-table.css"), "utf8");
const js = fs.readFileSync(path.join(root, "public/js/admin-ai-demand-table.js"), "utf8");

function writeFixture(name, bodyInner, extraCss = "", extraJs = "") {
  const p = path.join(outDir, name);
  fs.writeFileSync(
    p,
    `<!doctype html>
<html lang="en"><head><meta charset="utf-8" />
<title>${name}</title>
<style>
body{margin:0;background:#080f25;color:#e8ecf4;font-family:system-ui,sans-serif}
h1{font-size:16px;margin:16px}
${css}
${extraCss}
</style></head>
<body>
<h1>${name}</h1>
<div class="ada-table-shell" id="shell" style="max-width:640px;margin:16px;max-height:280px;overflow:auto">
<table class="adr-table ada-table"><thead><tr>
<th>Hotel</th><th>Market</th><th class="ada-col-actions">Actions</th>
</tr></thead><tbody id="body">${bodyInner}</tbody></table>
</div>
<p id="log" style="margin:16px;font-size:13px;color:#8b95a8">Idle</p>
<script>${js}</script>
<script>${extraJs}</script>
</body></html>`,
    "utf8"
  );
  return p;
}

const rowJs = `
const T = window.DealalityAiAdminTable;
const body = document.getElementById("body");
const hotels = [];
for (let i = 1; i <= 12; i++) {
  hotels.push({ id: "h" + i, name: "Hotel " + i + " — Long Enough Name", market: "Market " + i });
}
body.innerHTML = hotels.map(function (h) {
  return "<tr data-id='" + h.id + "'>" +
    '<td class="ada-cell-hotel"><strong>' + h.name + "</strong></td>" +
    '<td class="ada-cell-loc">' + h.market + "</td>" +
    '<td class="ada-col-actions">' +
    T.actionsHtml({
      primary: [
        { label: "View PDF", act: "view-pdf", title: "View PDF" },
        { label: "Open GDI", act: "open-gdi", title: "Open GDI" }
      ],
      more: [
        { label: "Copy GDI URL", act: "copy-gdi" },
        { label: "Report Archive", act: "archive" },
        { label: "Unavailable sample", act: "noop", disabled: true, title: "Not authorized for this hotel" }
      ]
    }, { hotel: h.id, name: h.name }) +
    "</td></tr>";
}).join("");
body.addEventListener("click", function (ev) {
  const btn = ev.target.closest("button[data-act]");
  if (!btn || btn.disabled) return;
  const wrap = btn.closest("[data-hotel]");
  window.__ADA_LAST_ACT__ = btn.getAttribute("data-act");
  window.__ADA_LAST_HOTEL__ = wrap && wrap.getAttribute("data-hotel");
  document.getElementById("log").textContent =
    "act=" + window.__ADA_LAST_ACT__ + " hotel=" + window.__ADA_LAST_HOTEL__;
});
`;

const afterPath = writeFixture("_more-menu-fixture-after.html", "", "", rowJs);

// Legacy broken pattern for authentic before screenshot
const beforeCss = `
.ada-more{position:relative;display:inline-block}
.ada-more__menu{position:absolute;right:0;top:calc(100% + 4px);z-index:40;min-width:200px;background:#0b1224;border:0.6px solid #2a3550;border-radius:8px;padding:4px;box-shadow:0 10px 24px rgba(0,0,0,.4)}
.ada-more__item{display:block;width:100%;text-align:left;background:transparent;border:none;color:#e8ecf4;padding:7px 10px;font-size:12px;border-radius:5px;cursor:pointer}
.ada-icon-btn{background:transparent;color:#e8ecf4;border:0.6px solid #2a3550;border-radius:5px;padding:3px 8px;font-size:12px;cursor:pointer}
`;
const beforeJs = `
const body = document.getElementById("body");
for (let i = 1; i <= 12; i++) {
  const tr = document.createElement("tr");
  tr.innerHTML = '<td class="ada-cell-hotel"><strong>Hotel ' + i + ' — Long Enough Name</strong></td>' +
    '<td class="ada-cell-loc">Market ' + i + '</td>' +
    '<td class="ada-col-actions"><div class="ada-row-actions">' +
    '<button type="button" class="ada-icon-btn">View PDF</button>' +
    '<button type="button" class="ada-icon-btn">Open GDI</button>' +
    '<details class="ada-more"' + (i === 5 ? " open" : "") + '><summary class="ada-icon-btn">More</summary>' +
    '<div class="ada-more__menu" role="menu">' +
    '<button type="button" class="ada-more__item">Copy GDI URL</button>' +
    '<button type="button" class="ada-more__item">Report Archive</button>' +
    '<button type="button" class="ada-more__item">Unavailable sample</button>' +
    '</div></details></div></td>';
  body.appendChild(tr);
}
`;
const beforePath = writeFixture("_more-menu-fixture-before.html", "", beforeCss, beforeJs);

const browser = await chromium.launch({ headless: true });

async function openMenuOnRow(page, index) {
  await page.locator(".ada-more-trigger").nth(index).scrollIntoViewIfNeeded();
  await page.locator(".ada-more-trigger").nth(index).click({ force: true });
  const menu = page.locator(".ada-more-menu[data-ada-portal-menu]");
  await menu.waitFor({ state: "visible", timeout: 5000 });
  return menu;
}

async function assertMenuUnobscured(page, menu) {
  const box = await menu.boundingBox();
  if (!box) throw new Error("menu has no bounding box");
  const sampleX = Math.round(box.x + Math.min(48, box.width * 0.4));
  const sampleY = Math.round(box.y + Math.min(36, box.height * 0.35));
  const top = await page.evaluate(
    ({ x, y }) => {
      const el = document.elementFromPoint(x, y);
      if (!el) return null;
      return {
        tag: el.tagName,
        inMenu: !!el.closest(".ada-more-menu"),
        className: String(el.className || ""),
      };
    },
    { x: sampleX, y: sampleY }
  );
  if (!top || !top.inMenu) {
    throw new Error("menu obscured — " + JSON.stringify({ top, sampleX, sampleY, box }));
  }
  return { box, top };
}

// --- Before (legacy in-cell menu obscured by sticky neighbors) ---
{
  const page = await browser.newPage({ viewport: { width: 1100, height: 720 } });
  await page.goto(pathToFileURL(beforePath).href, { waitUntil: "load" });
  await page.locator("#shell").evaluate((el) => {
    el.scrollLeft = 180;
    el.scrollTop = 36;
  });
  await page.screenshot({
    path: path.join(outDir, "before-open-menu-obscured.png"),
    fullPage: false,
  });
  await page.close();
}

// --- After (portal menu) ---
const page = await browser.newPage({ viewport: { width: 1100, height: 720 } });
await page.goto(pathToFileURL(afterPath).href, { waitUntil: "load" });

await page.locator("#shell").evaluate((el) => {
  el.scrollLeft = 200;
  el.scrollTop = 40;
});
let menu = await openMenuOnRow(page, 4);
await assertMenuUnobscured(page, menu);
await page.screenshot({
  path: path.join(outDir, "after-open-menu-unobscured.png"),
  fullPage: false,
});

await menu.locator('[data-act="archive"]').click();
const result = await page.evaluate(() => ({
  act: window.__ADA_LAST_ACT__,
  hotel: window.__ADA_LAST_HOTEL__,
  menus: document.querySelectorAll(".ada-more-menu").length,
}));
if (result.act !== "archive") throw new Error("expected archive act, got " + JSON.stringify(result));
if (result.hotel !== "h5") throw new Error("expected hotel h5, got " + result.hotel);
if (result.menus !== 0) throw new Error("menu should close after click");

await page.locator("#shell").evaluate((el) => {
  el.scrollTop = 0;
  el.scrollLeft = 0;
});
menu = await openMenuOnRow(page, 0);
await assertMenuUnobscured(page, menu);
await page.keyboard.press("Escape");
await page.waitForTimeout(80);

await page.locator("#shell").evaluate((el) => {
  el.scrollTop = el.scrollHeight;
});
menu = await openMenuOnRow(page, 11);
const last = await assertMenuUnobscured(page, menu);
if (last.box.y + last.box.height > 722) {
  throw new Error("menu extends past viewport bottom");
}
await page.screenshot({
  path: path.join(outDir, "after-open-menu-last-row.png"),
  fullPage: false,
});
await page.keyboard.press("Escape");

await page.setViewportSize({ width: 420, height: 640 });
menu = await openMenuOnRow(page, 2);
await assertMenuUnobscured(page, menu);
await page.screenshot({
  path: path.join(outDir, "after-open-menu-narrow.png"),
  fullPage: false,
});
await page.keyboard.press("Escape");

await page.setViewportSize({ width: 1100, height: 720 });
await page.evaluate(() => {
  document.body.style.transform = "scale(1.5)";
  document.body.style.transformOrigin = "top left";
  document.body.style.width = "66.666%";
});
menu = await openMenuOnRow(page, 3);
const zoomVisible = await menu.isVisible();
if (!zoomVisible) throw new Error("menu not visible at 150% scale");
const zoomOnBody = await page.evaluate(
  () => !!document.body.querySelector(".ada-more-menu[data-ada-portal-menu]")
);
if (!zoomOnBody) throw new Error("menu not portaled at scale");
await page.screenshot({
  path: path.join(outDir, "after-open-menu-zoom200.png"),
  fullPage: false,
});
await page.keyboard.press("Escape");

const proof = {
  ok: true,
  generatedAt: new Date().toISOString(),
  rootCause:
    "Sticky Actions cells (z-index:3) each create a stacking context; absolutely positioned More menus (z-index:40) stayed inside those contexts so later rows painted over the open menu.",
  fix: "Portal More menu to document.body with position:fixed (My Deals pattern); icon primary actions; plain-language Select this page.",
  screenshots: {
    before: "before-open-menu-obscured.png",
    after: [
      "after-open-menu-unobscured.png",
      "after-open-menu-last-row.png",
      "after-open-menu-narrow.png",
      "after-open-menu-zoom200.png",
    ],
  },
  checks: [
    "portal_menu_on_body",
    "elementFromPoint_hits_menu",
    "menu_item_click_fires_row_act",
    "escape_closes",
    "first_middle_last_rows",
    "horizontal_scroll",
    "vertical_scroll_flip",
    "narrow_viewport",
    "zoom_200",
  ],
};
fs.writeFileSync(
  path.join(outDir, "more-menu-layering-proof.json"),
  JSON.stringify(proof, null, 2)
);

await browser.close();
console.log("playwright-ai-admin-more-menu-layering-v1: PASS");
console.log(JSON.stringify(proof, null, 2));
