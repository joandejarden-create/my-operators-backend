#!/usr/bin/env node
/**
 * Warm-session Puppeteer probe: regional browse → property page (Akamai bypass attempt).
 */
import puppeteer from "puppeteer";
import { parseChoiceAmenitiesFromHtml } from "../lib/choice-hotel-content-fetch.js";

const propertyUrl =
  process.argv[2] ||
  "https://www.choicehotels.com/chihuahua/chihuahua/comfort-inn-hotels/mx077";
const regionalUrl =
  process.argv[3] ||
  "https://www.choicehotels.com/en-uk/mexico/regional-hotels?placeId=ChIJU1NoiDs6BIQREZgJa760ZO0";

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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
  );

  console.log("1) Home…");
  await page.goto("https://www.choicehotels.com/", {
    waitUntil: "networkidle2",
    timeout: 120000,
  });
  console.log("   title:", await page.title(), "len:", (await page.content()).length);

  console.log("2) Regional…");
  await page.goto(regionalUrl, { waitUntil: "networkidle2", timeout: 120000 });
  const regionalHtml = await page.content();
  console.log("   title:", await page.title(), "len:", regionalHtml.length);
  console.log("   hotels:", (regionalHtml.match(/"@type":"Hotel"/g) || []).length);

  console.log("3) Property…");
  await page.goto(propertyUrl, { waitUntil: "networkidle2", timeout: 120000 });
  const html = await page.content();
  console.log("   title:", await page.title(), "len:", html.length);
  console.log("   denied:", /access denied/i.test(html));

  const parsed = parseChoiceAmenitiesFromHtml(html);
  console.log("   amenities:", parsed.amenities.length, parsed.amenities.join("; "));
} finally {
  await browser.close();
}
