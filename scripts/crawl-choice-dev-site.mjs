/**
 * Crawl choicehotelsdevelopment.com public routes with Playwright and save text.
 * node scripts/crawl-choice-dev-site.mjs
 * node scripts/crawl-choice-dev-site.mjs --limit 5
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { chromium } from "playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BASE = "https://www.choicehotelsdevelopment.com";
const ROUTES_FILE = path.join(ROOT, "fixtures", "choice-dev-site-routes.json");
const OUT_DIR = path.join(ROOT, "fixtures", "choice-dev-site-text");

const limitArg = process.argv.find((a) => a.startsWith("--limit"));
const LIMIT = limitArg ? parseInt(process.argv[process.argv.indexOf(limitArg) + 1], 10) : 0;

function slugFromPath(p) {
  return p.replace(/^\//, "").replace(/\//g, "__") || "home";
}

async function extractPageText(page) {
  return page.evaluate(() => {
    const skip = new Set(["SCRIPT", "STYLE", "NOSCRIPT", "SVG"]);
    const parts = [];
    const walk = (el) => {
      if (!el || skip.has(el.tagName)) return;
      if (el.tagName === "A" && el.href) {
        const t = (el.innerText || "").trim();
        if (t) parts.push(`[LINK] ${t} → ${el.href}`);
      }
      for (const child of el.childNodes) {
        if (child.nodeType === Node.TEXT_NODE) {
          const t = child.textContent.replace(/\s+/g, " ").trim();
          if (t.length > 1) parts.push(t);
        } else if (child.nodeType === Node.ELEMENT_NODE) walk(child);
      }
    };
    walk(document.body);
    return [...new Set(parts)].join("\n");
  });
}

async function main() {
  let routes = JSON.parse(fs.readFileSync(ROUTES_FILE, "utf8"));
  if (LIMIT > 0) routes = routes.slice(0, LIMIT);
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  });

  const manifest = [];
  const errors = [];

  for (const r of routes) {
    const url = BASE + r.path;
    const slug = slugFromPath(r.path);
    console.log(`Crawl ${url}`);
    const page = await context.newPage();
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
      await page.waitForLoadState("load", { timeout: 30000 }).catch(() => {});
      await page
        .waitForFunction(
          () => document.body && document.body.innerText.trim().length > 200,
          { timeout: 45000 }
        )
        .catch(() => {});
      await page.waitForTimeout(1500);
      const text = await extractPageText(page);
      const outFile = path.join(OUT_DIR, `${slug}.txt`);
      const header = `# ${r.label}\n# URL: ${url}\n# devName: ${r.devName}\n\n`;
      fs.writeFileSync(outFile, header + text, "utf8");
      manifest.push({
        label: r.label,
        path: r.path,
        url,
        outFile: path.relative(ROOT, outFile),
        charCount: text.length,
      });
    } catch (e) {
      errors.push({ url, error: e.message });
      console.error(`  FAIL: ${e.message}`);
    } finally {
      await page.close();
    }
  }

  await browser.close();
  fs.writeFileSync(
    path.join(OUT_DIR, "manifest.json"),
    JSON.stringify({ crawledAt: new Date().toISOString(), pages: manifest, errors }, null, 2),
    "utf8"
  );
  console.log(`\nDone: ${manifest.length} pages, ${errors.length} errors → ${OUT_DIR}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
