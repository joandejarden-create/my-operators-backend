import puppeteer from "puppeteer";

const url = process.argv[2] || "https://www.choicehotels.com/colombia/medellin/radisson-individuals-hotels/cb030";
const hits = new Set();
const browser = await puppeteer.launch({ headless: "new" });
const page = await browser.newPage();
page.on("response", (res) => {
  const u = res.url();
  if (/hoteldam/i.test(u) && /\.(jpe?g|png|webp)/i.test(u)) hits.add(u);
});
await page.setUserAgent(
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
);
await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
await page.waitForTimeout(5000);
console.log([...hits].slice(0, 10));
await browser.close();
