/**
 * Scrape CALA / LATAM news article links from choicehotelsdevelopment.com
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { chromium } from "playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PAGES = [
  "https://www.choicehotelsdevelopment.com/international/cala",
  "https://www.choicehotelsdevelopment.com/news-latam",
];

async function extractNews(page) {
  return page.evaluate(() => {
    /** @type {{ date: string; title: string; href: string; snippet: string }[]} */
    const items = [];
    const seen = new Set();

    const allLinks = [...document.querySelectorAll("a[href]")];
    for (const a of allLinks) {
      const href = a.href || "";
      const text = (a.innerText || "").trim();
      if (!href || seen.has(href)) continue;
      if (
        /media\.choicehotels\.com/i.test(href) ||
        (/choicehotelsdevelopment\.com/i.test(href) &&
          (/\/news/i.test(href) || /article|post|happenings|press/i.test(href)))
      ) {
        seen.add(href);
        items.push({ date: "", title: text.slice(0, 200), href, snippet: "" });
      }
    }

    // Common news card patterns
    const cards = document.querySelectorAll(
      "article, [class*='news'], [class*='News'], [class*='card'], [class*='Card']"
    );
    for (const card of cards) {
      const titleEl =
        card.querySelector("h1,h2,h3,h4,[class*='title'],[class*='Title']") || card;
      const title = (titleEl.innerText || "").trim().split("\n")[0].slice(0, 300);
      const link = card.querySelector("a[href]");
      const href = link?.href || "";
      if (!title || title.length < 20 || !href) continue;
      if (seen.has(href)) continue;
      const dateMatch = (card.innerText || "").match(
        /\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+20\d{2}\b/i
      );
      seen.add(href);
      items.push({
        date: dateMatch ? dateMatch[0] : "",
        title,
        href,
        snippet: (card.innerText || "").replace(/\s+/g, " ").trim().slice(0, 400),
      });
    }

    // Headings followed by read-more links
    const headings = [...document.querySelectorAll("h2, h3, h4")];
    for (const h of headings) {
      const title = (h.innerText || "").trim();
      if (title.length < 25) continue;
      let el = h.parentElement;
      for (let i = 0; i < 5 && el; i++) {
        const a = el.querySelector("a[href]");
        if (a?.href && /read more|continue reading/i.test(a.innerText || "")) {
          if (!seen.has(a.href)) {
            seen.add(a.href);
            const dateMatch = (el.innerText || "").match(
              /\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+20\d{2}\b/i
            );
            items.push({
              date: dateMatch ? dateMatch[0] : "",
              title,
              href: a.href,
              snippet: (el.innerText || "").replace(/\s+/g, " ").trim().slice(0, 400),
            });
          }
          break;
        }
        el = el.parentElement;
      }
    }

    return items;
  });
}

async function resolveMediaUrl(page, articleUrl) {
  await page.goto(articleUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(2000);
  return page.evaluate(() => {
    const media = [...document.querySelectorAll("a[href]")]
      .map((a) => a.href)
      .filter((h) => /^https:\/\/media\.choicehotels\.com\//i.test(h));
    const text = (document.body?.innerText || "").replace(/\s+/g, " ").trim();
    const dateMatch = text.match(
      /\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+20\d{2}\b/i
    );
    const paras = text.split(/\n+/).filter((p) => p.length > 40);
    return {
      mediaUrls: [...new Set(media)],
      date: dateMatch ? dateMatch[0] : "",
      excerpt: paras.slice(0, 3).join(" ").slice(0, 500),
      title: document.title || "",
    };
  });
}

async function main() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
  });
  const page = await context.newPage();
  const all = [];

  for (const url of PAGES) {
    console.log("Crawl", url);
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForTimeout(3000);
    const items = await extractNews(page);
    console.log(`  ${items.length} link(s)`);
    all.push(...items.map((i) => ({ ...i, sourcePage: url })));
  }

  const outPath = path.join(ROOT, "fixtures", "choice-cala-news-scrape.json");
  const enriched = [];
  const seenHref = new Set();

  for (const item of all) {
    if (!item.href || seenHref.has(item.href)) continue;
    seenHref.add(item.href);
    if (/media\.choicehotels\.com/i.test(item.href)) {
      enriched.push({ ...item, mediaUrl: item.href });
      continue;
    }
    if (!/choicehotelsdevelopment\.com/i.test(item.href)) continue;
    try {
      console.log("  Article", item.href);
      const detail = await resolveMediaUrl(page, item.href);
      enriched.push({
        ...item,
        mediaUrls: detail.mediaUrls,
        mediaUrl: detail.mediaUrls[0] || "",
        resolvedDate: detail.date,
        excerpt: detail.excerpt || item.snippet,
        pageTitle: detail.title,
      });
    } catch (err) {
      enriched.push({ ...item, error: String(err.message || err) });
    }
  }

  fs.writeFileSync(outPath, JSON.stringify({ scrapedAt: new Date().toISOString(), items: enriched }, null, 2));
  console.log("Wrote", outPath, enriched.length, "items");
  await browser.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
