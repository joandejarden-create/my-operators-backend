/**
 * Crawl Choice Privileges consumer pages on choicehotels.com.
 * node scripts/crawl-choice-privileges-web.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { chromium } from "playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "fixtures", "choice-privileges-web");

const PAGES = [
  { slug: "benefits", path: "/choice-privileges/benefits" },
  { slug: "earn-points", path: "/choice-privileges/earn-points" },
  { slug: "redeem-points", path: "/choice-privileges/redeem-points" },
  { slug: "partners", path: "/choice-privileges/partners" },
];

const BASE = "https://www.choicehotels.com";

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  });

  for (const { slug, path: p } of PAGES) {
    const url = BASE + p;
    const page = await context.newPage();
    console.log(`Crawl ${url}`);
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
    await page.waitForLoadState("networkidle", { timeout: 45000 }).catch(() => {});
    await page
      .waitForFunction(() => (document.body.innerText || "").length > 500, {
        timeout: 30000,
      })
      .catch(() => {});
    const text = await page.evaluate(() => document.body.innerText || "");
    const md = `# Choice Privileges — ${slug}\nSource: ${url}\n\n${text.trim()}\n`;
    fs.writeFileSync(path.join(OUT, `${slug}.txt`), text, "utf8");
    fs.writeFileSync(path.join(OUT, `${slug}.md`), md, "utf8");
    await page.close();
  }

  const combined = PAGES.map(({ slug }) => {
    const body = fs.readFileSync(path.join(OUT, `${slug}.txt`), "utf8");
    return `\n\n---\n# ${slug}\n\n${body}`;
  }).join("");
  fs.writeFileSync(path.join(OUT, "all-pages.txt"), combined.trim(), "utf8");
  await browser.close();
  console.log(`Done → ${OUT}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
