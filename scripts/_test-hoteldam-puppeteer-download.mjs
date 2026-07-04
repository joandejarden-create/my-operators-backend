import puppeteer from "puppeteer";

const url =
  process.argv[2] ||
  "https://www.choicehotels.com/hoteldam/aw/aw007/images/2048/AW007Exterior5_1.JPG";

const browser = await puppeteer.launch({ headless: "new" });
const page = await browser.newPage();
await page.setUserAgent(
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
);
const res = await page.goto(url, { waitUntil: "networkidle0", timeout: 60000 });
const buf = await res.buffer();
console.log(res.status(), buf.length, res.headers()["content-type"]);
await browser.close();
