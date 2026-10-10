#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";

const ROOT = process.cwd();
const urls = JSON.parse(fs.readFileSync(path.join(ROOT, "reports/hilton-development-share/2026-10-02/LOCAL_SHARE_URLS.json"), "utf8"));
const outDir = path.join(ROOT, "reports/hilton-development-share/2026-10-02/screenshots");
fs.mkdirSync(outDir, { recursive: true });

const pages = [
  { key: "radar", url: urls.urls.radar, waitMs: 14000 },
  { key: "brand-ai", url: urls.urls.brandAi, waitMs: 16000 },
  { key: "brand-explorer", url: urls.urls.brandExplorer, waitMs: 16000 },
  { key: "gdi", url: urls.urls.gdi, waitMs: 16000 },
];

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
const results = [];

for (const p of pages) {
  const page = await context.newPage();
  const consoleErrors = [];
  page.on("console", (msg) => { if (msg.type() === "error") consoleErrors.push(msg.text().slice(0, 240)); });
  page.on("pageerror", (err) => consoleErrors.push(String(err?.message || err).slice(0, 240)));
  try {
    const resp = await page.goto(p.url, { waitUntil: "domcontentloaded", timeout: 60000 });
    const status = resp ? resp.status() : null;
    await page.waitForTimeout(p.waitMs);
    const finalUrl = page.url();
    const title = await page.title();
    const bodyText = await page.evaluate(() => (document.body && document.body.innerText) || "");
    const loginWall = /sign in|log in|memberstack|create an account/i.test(bodyText) && /password/i.test(bodyText);
    const hasEditControls = await page.evaluate(() => /\b(Regenerate|Rerun|Admin|Delete|Enrich)\b/i.test((document.body && document.body.innerText) || ""));
    const redirectedToSignIn = /sign-?in|\/login|memberstack/i.test(finalUrl);
    const shot = path.join(outDir, p.key + "-desktop.png");
    await page.screenshot({ path: shot, fullPage: false });
    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(800);
    const shotM = path.join(outDir, p.key + "-mobile.png");
    await page.screenshot({ path: shotM, fullPage: false });
    results.push({ key: p.key, ok: status === 200 && !redirectedToSignIn && !loginWall, status, finalUrl, title, loginWall, hasEditControls, redirectedToSignIn, consoleErrorCount: consoleErrors.length, consoleErrors: consoleErrors.slice(0, 8), screenshotDesktop: shot, screenshotMobile: shotM, bodySnippet: bodyText.replace(/\s+/g, " ").slice(0, 220) });
  } catch (err) {
    results.push({ key: p.key, ok: false, error: String(err?.message || err).slice(0, 400) });
  }
  await page.close();
}
await browser.close();
const summary = { checkedAt: new Date().toISOString(), allOk: results.every((r) => r.ok), results };
fs.writeFileSync(path.join(ROOT, "reports/hilton-development-share/2026-10-02/LOGGED_OUT_QA.json"), JSON.stringify(summary, null, 2));
console.log(JSON.stringify(summary, null, 2));
process.exit(summary.allOk ? 0 : 1);
