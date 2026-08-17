/**
 * Probe state FDD portals for IHG (Holiday Hospitality Franchising, LLC) filings.
 * Run: node scripts/probe-state-fdd-portals-ihg.mjs
 */
import puppeteer from "puppeteer";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "ihg-state-fdd-discovery.json");

const SEARCH_TERMS = [
  "Holiday Hospitality",
  "Holiday Inn",
  "Holiday Inn Express",
  "InterContinental",
  "Crowne Plaza",
  "Kimpton",
  "Hotel Indigo",
  "Staybridge",
  "Candlewood",
  "Even Hotels",
  "Avid",
  "Atwell",
  "Voco",
  "Garner",
  "Six Senses",
  "Regent",
  "Vignette",
  "IHG",
];

async function searchWisconsin(page, term) {
  await page.goto("https://apps.dfi.wi.gov/apps/FranchiseSearch/MainSearch.aspx", {
    waitUntil: "networkidle2",
    timeout: 120000,
  });
  await page.waitForSelector('input[id*="Name"]', { timeout: 30000 });
  const inputSel = 'input[id*="Name"]';
  await page.evaluate((sel) => {
    const el = document.querySelector(sel);
    if (el) el.value = "";
  }, inputSel);
  await page.type(inputSel, term, { delay: 30 });
  await Promise.all([
    page.waitForNavigation({ waitUntil: "networkidle2", timeout: 60000 }).catch(() => {}),
    page.click('input[type="submit"], input[value="Search"], button[type="submit"]').catch(async () => {
      await page.keyboard.press("Enter");
    }),
  ]);
  await new Promise((r) => setTimeout(r, 2000));

  const results = await page.evaluate(() => {
    const rows = [...document.querySelectorAll("table tr")];
    const out = [];
    for (const tr of rows) {
      const cells = [...tr.querySelectorAll("td")].map((td) => td.textContent?.trim() || "");
      if (cells.length >= 2) {
        const link = tr.querySelector("a[href]");
        out.push({
          cells,
          href: link?.href || null,
          text: link?.textContent?.trim() || cells.join(" | "),
        });
      }
    }
    const countMatch = document.body.innerText.match(/Results Count:\s*(\d+)/i);
    return { count: countMatch ? Number(countMatch[1]) : out.length, rows: out.slice(0, 20) };
  });

  return { term, ...results };
}

async function getWisconsinFddLinks(page, detailUrl) {
  await page.goto(detailUrl, { waitUntil: "networkidle2", timeout: 120000 });
  await new Promise((r) => setTimeout(r, 1500));
  return page.evaluate(() => {
    const links = [...document.querySelectorAll("a[href]")]
      .map((a) => ({ href: a.href, text: a.textContent?.trim() || "" }))
      .filter((l) => /\.pdf|fdd|disclosure|download|Document/i.test(`${l.href} ${l.text}`));
    const bodySnippet = document.body.innerText.slice(0, 4000);
    return { links, bodySnippet };
  });
}

async function searchMinnesota(page, term) {
  const url = new URL("https://cards.web.commerce.state.mn.us/franchise-registrations");
  url.searchParams.set("doSearch", "true");
  url.searchParams.set("franchisor", term);
  url.searchParams.set("documentType", "Clean FDD");
  await page.goto(url.toString(), { waitUntil: "networkidle2", timeout: 120000 });
  await new Promise((r) => setTimeout(r, 3000));
  return page.evaluate(() => {
    const cards = [...document.querySelectorAll(".card, .result, tr, li, article")];
    const hits = [];
    for (const el of cards) {
      const text = el.textContent?.replace(/\s+/g, " ").trim() || "";
      if (/holiday|ihg|intercontinental|kimpton|crowne|staybridge|candlewood|hotel indigo|even hotel|avid|atwell|voco|garner|six senses|regent|vignette/i.test(text)) {
        const link = el.querySelector("a[href]");
        hits.push({ text: text.slice(0, 300), href: link?.href || null });
      }
    }
    const allLinks = [...document.querySelectorAll("a[href]")]
      .map((a) => ({ href: a.href, text: a.textContent?.trim() || "" }))
      .filter((l) => /holiday|ihg|intercontinental|kimpton|crowne|staybridge|candlewood|hotel indigo|even|avid|atwell|voco|garner|six senses|regent|vignette/i.test(`${l.href} ${l.text}`));
    return { hits: hits.slice(0, 15), links: allLinks.slice(0, 20), pageText: document.body.innerText.slice(0, 2000) };
  });
}

async function searchIndiana(page, term) {
  await page.goto("https://securities.sos.in.gov/public-portfolio-search/", {
    waitUntil: "networkidle2",
    timeout: 120000,
  });
  await new Promise((r) => setTimeout(r, 2000));
  // Select Franchise registration type
  await page.select('select', "Franchise").catch(() => {});
  const nameInput = await page.$('input[type="text"], input[name*="Name"], input[id*="Name"]');
  if (nameInput) {
    await nameInput.click({ clickCount: 3 });
    await nameInput.type(term, { delay: 30 });
  }
  await page.click('button[type="submit"], input[type="submit"], button:has-text("Search")').catch(async () => {
    await page.keyboard.press("Enter");
  });
  await new Promise((r) => setTimeout(r, 4000));
  return page.evaluate(() => {
    const rows = [...document.querySelectorAll("table tr, .result-row, [class*='result']")];
    const out = [];
    for (const row of rows) {
      const text = row.textContent?.replace(/\s+/g, " ").trim() || "";
      if (text.length > 10) {
        const link = row.querySelector("a[href]");
        out.push({ text: text.slice(0, 400), href: link?.href || null });
      }
    }
    const links = [...document.querySelectorAll("a[href]")]
      .map((a) => ({ href: a.href, text: a.textContent?.trim() || "" }))
      .filter((l) => /portfolio|download|pdf|fdd|document/i.test(`${l.href} ${l.text}`));
    return { rows: out.slice(0, 20), links: links.slice(0, 20), pageText: document.body.innerText.slice(0, 2500) };
  });
}

async function main() {
  const report = {
    generatedAt: new Date().toISOString(),
    franchisorLegalName: "Holiday Hospitality Franchising, LLC",
    wisconsin: [],
    minnesota: [],
    indiana: [],
    wisconsinDetails: [],
  };

  const browser = await puppeteer.launch({ headless: "new" });
  const page = await browser.newPage();
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  );

  console.log("=== Wisconsin ===");
  for (const term of SEARCH_TERMS) {
    try {
      const res = await searchWisconsin(page, term);
      console.log(term, "->", res.count, "results");
      if (res.count > 0) report.wisconsin.push(res);
    } catch (err) {
      console.warn("WI fail", term, err.message);
      report.wisconsin.push({ term, error: err.message });
    }
    await new Promise((r) => setTimeout(r, 1000));
  }

  // Drill into first WI hits for FDD download links
  const wiDetailUrls = new Set();
  for (const block of report.wisconsin) {
    for (const row of block.rows || []) {
      if (row.href && /FranchiseSearch/i.test(row.href)) wiDetailUrls.add(row.href);
    }
  }
  console.log("WI detail pages to probe:", wiDetailUrls.size);
  for (const url of [...wiDetailUrls].slice(0, 15)) {
    try {
      const detail = await getWisconsinFddLinks(page, url);
      report.wisconsinDetails.push({ url, ...detail });
      console.log("Detail", url, detail.links.length, "doc links");
    } catch (err) {
      report.wisconsinDetails.push({ url, error: err.message });
    }
  }

  console.log("\n=== Minnesota ===");
  for (const term of ["Holiday Hospitality", "Holiday Inn", "InterContinental", "Crowne Plaza", "Kimpton"]) {
    try {
      const res = await searchMinnesota(page, term);
      console.log(term, "->", res.links.length, "links,", res.hits.length, "hits");
      report.minnesota.push({ term, ...res });
    } catch (err) {
      console.warn("MN fail", term, err.message);
      report.minnesota.push({ term, error: err.message });
    }
  }

  console.log("\n=== Indiana ===");
  for (const term of ["Holiday Hospitality", "Holiday Inn", "InterContinental", "Crowne Plaza", "Kimpton"]) {
    try {
      const res = await searchIndiana(page, term);
      console.log(term, "->", res.rows.length, "rows");
      report.indiana.push({ term, ...res });
    } catch (err) {
      console.warn("IN fail", term, err.message);
      report.indiana.push({ term, error: err.message });
    }
  }

  await browser.close();
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log("\nWrote", OUT);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
