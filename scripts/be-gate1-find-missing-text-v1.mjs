#!/usr/bin/env node
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const puppeteer = require("puppeteer");
const outDir = join(process.cwd(), "reports/brand-explorer/phase2h-a-renderer-parity/content-preservation-v1");
mkdirSync(outDir, { recursive: true });

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function tokenize(text) {
  return text
    .replace(/\s+/g, " ")
    .trim()
    .split(/(?<=[.!?])\s+|\n+/)
    .map((s) => s.trim())
    .filter((s) => s.length >= 24);
}

const chrome =
  process.env.PUPPETEER_EXECUTABLE_PATH ||
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
const browser = await puppeteer.launch({
  headless: "new",
  executablePath: chrome,
  args: ["--no-sandbox"],
});

const productPage = await browser.newPage();
await productPage.goto(
  "http://127.0.0.1:8082/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=curio-gate1-renderer-parity-payload.json",
  { waitUntil: "networkidle0", timeout: 120000 },
);
await sleep(1000);

const comparePage = await browser.newPage();
await comparePage.goto(
  "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1",
  { waitUntil: "networkidle0", timeout: 120000 },
);
await sleep(1200);

const tabs = [
  "atelier-overview",
  "atelier-value-owners",
  "atelier-ops",
  "atelier-standards-owner",
  "atelier-economics",
  "atelier-materials",
];
const report = {};

for (const tabId of tabs) {
  await productPage.evaluate((id) => {
    document.querySelector(`.section-nav-item[data-atelier-tab="${id}"]`)?.click();
  }, tabId);
  await comparePage.evaluate((id) => {
    document.querySelector(`[data-becs-tab="${id}"]`)?.click();
  }, tabId);
  await sleep(700);

  const productText = await productPage.evaluate(() => {
    const root = document.querySelector(".be-atelier-tab-panel.active");
    return (root?.innerText || "").replace(/\s+/g, " ").trim();
  });
  const compareText = await comparePage.evaluate(() => {
    const mounts = [
      ...document.querySelectorAll('[data-gate1-side="A-compare-left"] .becs-product-mount'),
    ];
    return mounts
      .map((m) => m.innerText || "")
      .join(" ")
      .replace(/\s+/g, " ")
      .trim();
  });

  // Also compare raw HTML from renderer API on compare page
  const htmlCompare = await comparePage.evaluate((id) => {
    const api = window.BrandExplorerAtelierFromApi;
    const brand = window.__BE_GATE1_COMPARE__
      ? null
      : null;
    // use mounted brand via parity state — re-render from leftBrand not exposed; use DOM mounts
    const mounts = [
      ...document.querySelectorAll('[data-gate1-side="A-compare-left"] .becs-product-mount'),
    ];
    const joined = mounts.map((m) => m.innerHTML).join("");
    // Reconstruct lossless check against full tab if available
    return {
      mountHtmlLen: joined.length,
      mountCount: mounts.length,
    };
  }, tabId);

  const productTokens = tokenize(productText);
  const compareBlob = ` ${compareText} `;
  const missing = productTokens.filter((t) => !compareBlob.includes(t));

  report[tabId] = {
    productTextLen: productText.length,
    compareTextLen: compareText.length,
    delta: compareText.length - productText.length,
    missingTokenCount: missing.length,
    missingTokens: missing.slice(0, 30),
    htmlCompare,
  };
}

await browser.close();
writeFileSync(join(outDir, "03-missing-tokens.json"), JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    Object.fromEntries(
      Object.entries(report).map(([k, v]) => [
        k,
        { delta: v.delta, missingTokenCount: v.missingTokenCount, sample: v.missingTokens.slice(0, 5) },
      ]),
    ),
    null,
    2,
  ),
);
