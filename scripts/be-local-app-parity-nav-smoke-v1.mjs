#!/usr/bin/env node
import { createRequire } from "module";
import { existsSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";

const require = createRequire(import.meta.url);
const puppeteer = require("puppeteer");
const exe = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
const out = "reports/brand-explorer/local-app-parity-v1/screenshots";
mkdirSync(out, { recursive: true });

const browser = await puppeteer.launch({
  headless: "new",
  executablePath: existsSync(exe) ? exe : undefined,
  args: ["--no-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto(
  "http://127.0.0.1:8080/brand-explorer-combined.html?id=receQkxgjlezsc1xg&beInternalPreview=1",
  { waitUntil: "networkidle2", timeout: 90000 },
);
await new Promise((r) => setTimeout(r, 2500));

const before = await page.evaluate(() => ({
  active: document.querySelector(".section-nav-item.active")?.getAttribute("data-atelier-tab"),
  jumpAttr: !!document.querySelector("[data-be-jump-atelier-tab]"),
}));

const clicked = await page.evaluate(() => {
  const candidates = [...document.querySelectorAll("a,button,[role=button],.section-nav-item,[data-be-jump-atelier-tab]")];
  const el = candidates.find((e) =>
    /view recent openings|recent openings|recent momentum/i.test(e.textContent || ""),
  );
  if (!el) return { ok: false, reason: "control_not_found" };
  el.click();
  return { ok: true, text: (el.textContent || "").trim().slice(0, 80), jump: el.getAttribute("data-be-jump-atelier-tab") };
});
await new Promise((r) => setTimeout(r, 900));
const after = await page.evaluate(() => ({
  active: document.querySelector(".section-nav-item.active")?.getAttribute("data-atelier-tab"),
  panel: document.querySelector(".be-atelier-tab-panel.active")?.getAttribute("data-atelier-panel"),
}));
await page.screenshot({ path: join(out, "app-combined-after-recent-openings.png"), fullPage: false });

await page.evaluate(() =>
  document.querySelector('.section-nav-item[data-atelier-tab="atelier-footprint"]')?.click(),
);
await new Promise((r) => setTimeout(r, 700));
const fp = await page.evaluate(() => {
  const root = document.querySelector(".be-atelier-tab-panel.active") || document.body;
  const text = root.innerText || "";
  return {
    hasOpen: /\bOpen\b/i.test(text),
    hasPipeline: /Pipeline/i.test(text),
    hasRegion: /Region/i.test(text),
    hasCountry: /Country/i.test(text),
    hasArchetype: /Archetype|Asset/i.test(text),
    snippet: text.slice(0, 700),
  };
});
await page.screenshot({ path: join(out, "app-combined-footprint.png"), fullPage: false });

await page.goto("http://127.0.0.1:8080/brand-explorer-compare.html?beInternalPreview=1", {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await new Promise((r) => setTimeout(r, 2000));
const compare = await page.evaluate(() => ({
  title: document.title,
  hasPaired: !!document.querySelector("#becsPairedDoc, .becs-shell, [data-becs-root]"),
  tabCount: document.querySelectorAll("[data-becs-tab]").length,
}));
await page.screenshot({ path: join(out, "app-compare-studio.png"), fullPage: false });

// Preview compare for side-by-side evidence
await page.goto("http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1", {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await new Promise((r) => setTimeout(r, 2000));
await page.screenshot({ path: join(out, "preview-compare-studio.png"), fullPage: false });

const result = { before, clicked, after, fp, compare };
writeFileSync(
  "reports/brand-explorer/local-app-parity-v1/nav-smoke.json",
  JSON.stringify(result, null, 2),
);
console.log(JSON.stringify(result, null, 2));
await browser.close();
