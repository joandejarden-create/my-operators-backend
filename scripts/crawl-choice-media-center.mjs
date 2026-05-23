/**
 * Crawl media.choicehotels.com press kits (our-brands + per-brand pages).
 * node scripts/crawl-choice-media-center.mjs
 * node scripts/crawl-choice-media-center.mjs --limit 3
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { chromium } from "playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BASE = "https://media.choicehotels.com";
const OUT_DIR = path.join(ROOT, "fixtures", "choice-media-center-text");

const limitIdx = process.argv.indexOf("--limit");
const LIMIT = limitIdx >= 0 ? parseInt(process.argv[limitIdx + 1], 10) : 0;

async function extractPageText(page) {
  return page.evaluate(() => {
    const raw = document.body.innerText || "";
    const lines = raw
      .split(/\n+/)
      .map((l) => l.replace(/\s+/g, " ").trim())
      .filter((l) => l.length > 1);
    const pdfLinks = [...document.querySelectorAll('a[href*="/download/"], a[href$=".pdf"]')]
      .map((a) => `[PDF] ${(a.innerText || a.href).trim()} → ${a.href}`)
      .filter((l) => l.length > 10);
    return [...new Set([...lines, ...pdfLinks])].join("\n");
  });
}

async function loadPressKitPage(page, url) {
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
  await page.waitForLoadState("networkidle", { timeout: 45000 }).catch(() => {});
  await page
    .waitForFunction(
      () => (document.body.innerText || "").length > 800,
      { timeout: 30000 }
    )
    .catch(() => {});
  const expand = page.getByRole("button", { name: /continue reading/i }).first();
  if (await expand.isVisible().catch(() => false)) {
    await expand.click().catch(() => {});
    await page.waitForTimeout(1000);
  }
}

async function discoverBrandUrls(page) {
  await page.goto(`${BASE}/our-brands`, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(2000);
  return page.evaluate((base) => {
    const urls = new Set();
    for (const a of document.querySelectorAll("a[href]")) {
      let href = a.getAttribute("href") || "";
      if (href.startsWith("/")) href = base + href;
      if (!href.startsWith(base)) continue;
      const path = new URL(href).pathname.replace(/\/$/, "");
      if (
        path === "/our-brands" ||
        path.includes("press-release") ||
        path.includes("download") ||
        path.includes("#") ||
        /\/(our-|media-|in-the|multimedia|awards|leadership|history|company|contacts|home)/i.test(path)
      )
        continue;
      if (
        path.split("/").filter(Boolean).length === 1 &&
        (/press-kit|suites|everhome|radisson|ascend|cambria|clarion|comfort|quality|sleep|econo|rodeway|woodspring|suburban|mainstay|privileges/i.test(path) ||
          path.endsWith("-suites"))
      )
        urls.add(href.split("#")[0]);
    }
    return [...urls].sort();
  }, BASE);
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  });
  const page = await context.newPage();
  let urls = await discoverBrandUrls(page);
  console.log(`Discovered ${urls.length} press-kit URLs`);
  if (LIMIT > 0) urls = urls.slice(0, LIMIT);

  const manifest = [];
  for (const url of urls) {
    const slug = new URL(url).pathname.replace(/^\//, "") || "home";
    console.log(`Crawl ${url}`);
    const p = await context.newPage();
    try {
      await loadPressKitPage(p, url);
      const text = await extractPageText(p);
      const outFile = path.join(OUT_DIR, `${slug}.txt`);
      fs.writeFileSync(outFile, `# URL: ${url}\n\n${text}`, "utf8");
      manifest.push({ url, slug, chars: text.length });
    } catch (e) {
      console.warn(`FAIL ${url}: ${e.message}`);
    } finally {
      await p.close();
    }
  }
  fs.writeFileSync(path.join(OUT_DIR, "manifest.json"), JSON.stringify(manifest, null, 2));
  await browser.close();
  console.log(`Done → ${OUT_DIR}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
