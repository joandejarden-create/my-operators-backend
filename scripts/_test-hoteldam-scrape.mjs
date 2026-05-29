import puppeteer from "puppeteer";

const url = process.argv[2] || "https://www.choicehotels.com/colombia/medellin/radisson-individuals-hotels/cb030";
const browser = await puppeteer.launch({ headless: "new" });
const page = await browser.newPage();
await page.setUserAgent(
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
);
await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
await page.waitForTimeout(4000);
const html = await page.content();
const found = [
  ...html.matchAll(
    /https:\/\/www\.choicehotels\.com\/hoteldam\/[^"'\\s]+?\.(?:jpg|jpeg|png|webp)/gi
  ),
].map((m) => m[0]);
const ext = found.filter((u) => /exterior/i.test(u));
console.log("total", found.length, "exterior", ext.length);
console.log(ext[0] || found[0] || "(none)");
await browser.close();
