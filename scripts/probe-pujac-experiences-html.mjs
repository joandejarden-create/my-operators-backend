#!/usr/bin/env node
import puppeteer from "puppeteer";
import { writeFileSync } from "node:fs";

const url = "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/experiences/";
const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
await new Promise((r) => setTimeout(r, 5000));
const html = await page.content();
writeFileSync("reports/pujac-experiences-full.html", html);

const patterns = [
  /Connect to the city in a new way[\s\S]{0,1200}/i,
  /"amenit[^"]*"\s*:\s*\[[\s\S]{0,3000}?\]/gi,
  /Free high-speed internet/gi,
  /Mobility accessible rooms/gi,
  /Meeting event space/gi,
];
for (const p of patterns) {
  const m = html.match(p);
  console.log(String(p), m ? "HIT" : "miss", m?.[0]?.slice(0, 200) || "");
}
console.log("html len", html.length, "next_data", html.includes("__NEXT_DATA__"));
await browser.close();
