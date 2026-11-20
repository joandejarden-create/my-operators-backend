#!/usr/bin/env node
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const puppeteer = require("puppeteer");
const outDir = join(process.cwd(), "reports/brand-explorer/phase2h-a-renderer-parity/content-preservation-v1");
mkdirSync(outDir, { recursive: true });

const TABS = [
  "atelier-overview",
  "atelier-value-owners",
  "atelier-ops",
  "atelier-standards-owner",
  "atelier-commercial",
  "atelier-economics",
  "atelier-loyalty",
  "atelier-footprint",
  "atelier-materials",
  "atelier-insight",
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function dig(browser, url, mode) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 2000 });
  await page.goto(url, { waitUntil: "networkidle0", timeout: 120000 });
  await sleep(1200);
  const out = {};
  for (const id of TABS) {
    if (mode === "product") {
      await page.evaluate((tabId) => {
        const btn = document.querySelector(`.section-nav-item[data-atelier-tab="${tabId}"]`);
        if (btn) btn.click();
      }, id);
    } else {
      await page.evaluate((tabId) => {
        const btn = document.querySelector(`[data-becs-tab="${tabId}"]`);
        if (btn) btn.click();
      }, id);
    }
    await sleep(700);
    out[id] = await page.evaluate((m) => {
      let root;
      if (m === "product") {
        root =
          document.querySelector(".be-atelier-tab-panel.active") ||
          document.querySelector("#beAtelierExplorer");
      } else if (m === "parity") {
        const mounts = [
          ...document.querySelectorAll('[data-gate1-side="A-compare-left"] .becs-product-mount'),
        ];
        root = document.createElement("div");
        mounts.forEach((node) => root.appendChild(node.cloneNode(true)));
      } else {
        const mounts = [
          ...document.querySelectorAll("#becsPairedDoc .becs-col-left .becs-product-mount"),
        ];
        root = document.createElement("div");
        mounts.forEach((node) => root.appendChild(node.cloneNode(true)));
      }
      const text = (root.innerText || "").replace(/\s+/g, " ").trim();
      const clipped = [];
      const all = [
        ...document.querySelectorAll(
          m === "product"
            ? ".be-atelier-tab-panel.active *"
            : "#becsPairedDoc .becs-col-left .becs-product-mount *",
        ),
      ];
      for (const el of all.slice(0, 400)) {
        const cs = getComputedStyle(el);
        if (
          (cs.overflow === "hidden" || cs.overflowY === "hidden") &&
          el.scrollHeight > el.clientHeight + 4 &&
          (el.innerText || "").trim().length > 40
        ) {
          clipped.push({
            tag: el.tagName,
            className: String(el.className || "").slice(0, 80),
            scrollHeight: el.scrollHeight,
            clientHeight: el.clientHeight,
            sample: (el.innerText || "").replace(/\s+/g, " ").trim().slice(0, 120),
          });
        }
      }
      return {
        textLen: text.length,
        sections: root.querySelectorAll(".oe-section").length,
        titles: [...root.querySelectorAll(".oe-section-title")].map((e) => e.textContent.trim()),
        bullets: root.querySelectorAll("ul li").length,
        cards: root.querySelectorAll(
          ".scenario-card, .proof-point-card, .explorer-detail-card, .diff-card, .info-card",
        ).length,
        imgs: root.querySelectorAll("img").length,
        tables: root.querySelectorAll("table").length,
        emptyLabel: !!root.querySelector(".becs-card-empty"),
        clippedCount: clipped.length,
        clipped: clipped.slice(0, 8),
        textSample: text.slice(0, 280),
      };
    }, mode);
  }
  await page.close();
  return out;
}

const chrome =
  process.env.PUPPETEER_EXECUTABLE_PATH ||
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
const browser = await puppeteer.launch({
  headless: "new",
  executablePath: chrome,
  args: ["--no-sandbox"],
});

const product = await dig(
  browser,
  "http://127.0.0.1:8082/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=curio-gate1-renderer-parity-payload.json",
  "product",
);
const parity = await dig(
  browser,
  "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1",
  "parity",
);
const normal = await dig(
  browser,
  "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1",
  "normal",
);
await browser.close();

const rows = TABS.map((id) => ({
  tab: id,
  product: product[id],
  parity: parity[id],
  normal: normal[id],
  parityDeltaText: (parity[id]?.textLen || 0) - (product[id]?.textLen || 0),
  normalDeltaText: (normal[id]?.textLen || 0) - (product[id]?.textLen || 0),
  parityLostTitles: (product[id]?.titles || []).filter((t) => !(parity[id]?.titles || []).includes(t)),
  normalLostTitles: (product[id]?.titles || []).filter((t) => !(normal[id]?.titles || []).includes(t)),
}));

writeFileSync(join(outDir, "02-content-dig.json"), JSON.stringify({ rows }, null, 2));
console.log(
  JSON.stringify(
    rows.map((r) => ({
      tab: r.tab,
      p: r.product.textLen,
      parity: r.parity.textLen,
      normal: r.normal.textLen,
      parityDelta: r.parityDeltaText,
      normalDelta: r.normalDeltaText,
      pCards: r.product.cards,
      nCards: r.normal.cards,
      parityCards: r.parity.cards,
      normalEmpty: r.normal.emptyLabel,
      normalLostTitles: r.normalLostTitles,
      parityClipped: r.parity.clippedCount,
      normalClipped: r.normal.clippedCount,
    })),
    null,
    2,
  ),
);
