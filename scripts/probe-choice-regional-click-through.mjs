#!/usr/bin/env node
/**
 * Navigate property via regional page link click (same-tab session).
 */
import puppeteer from "puppeteer";
import { parseChoiceAmenitiesFromHtml } from "../lib/choice-hotel-content-fetch.js";

const regionalUrl =
  "https://www.choicehotels.com/en-uk/mexico/regional-hotels?placeId=ChIJU1NoiDs6BIQREZgJa760ZO0";
const propertyId = (process.argv[2] || "mx077").toLowerCase();

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
});

try {
  const page = await browser.newPage();
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
  });
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT  10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
  );

  await page.goto(regionalUrl, { waitUntil: "networkidle2", timeout: 120000 });
  const href = await page.evaluate((pid) => {
    const a = [...document.querySelectorAll("a[href]")].find((el) =>
      el.getAttribute("href")?.toLowerCase().includes(`/${pid}`)
    );
    return a?.getAttribute("href") || "";
  }, propertyId);
  console.log("link:", href);
  if (!href) {
    console.log("no link found");
    process.exit(0);
  }

  await Promise.all([
    page.waitForNavigation({ waitUntil: "networkidle2", timeout: 120000 }),
    page.click(`a[href*="${propertyId}"]`),
  ]);

  const html = await page.content();
  console.log("url:", page.url());
  console.log("title:", await page.title());
  console.log("len:", html.length, "denied:", /access denied/i.test(html));
  const parsed = parseChoiceAmenitiesFromHtml(html);
  console.log("amenities:", parsed.amenities.length, parsed.amenities.join("; "));
} finally {
  await browser.close();
}
