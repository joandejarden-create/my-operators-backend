import fs from "fs";
import path from "path";
import puppeteer from "puppeteer";

const out = path.resolve("tmp-hero-video-shots/video-modal.png");
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1280, height: 800, deviceScaleFactor: 1 });
await page.goto("https://www.dealality.com/old-home?v=" + Date.now(), {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await page.click("#hero-video-poster");
await page.waitForSelector("#oh-video-modal.is-open #oh-video-player", {
  timeout: 15000,
});
await new Promise((r) => setTimeout(r, 1200));
const dialog = await page.$("#oh-video-dialog");
if (dialog) {
  await dialog.screenshot({ path: out });
} else {
  await page.screenshot({ path: out });
}
console.log("saved", out);
await browser.close();
