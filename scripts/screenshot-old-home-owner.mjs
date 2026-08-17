import { chromium } from 'playwright';
import path from 'path';
import fs from 'fs';

const outDir = path.resolve('tmp-screenshots-old-home');
fs.mkdirSync(outDir, { recursive: true });

const browser = await chromium.launch({ headless: true });

async function shot(name, url, width, height, waitText) {
  const page = await browser.newPage({ viewport: { width, height } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e)));
  page.on('console', (msg) => {
    if (msg.type() === 'error') errors.push(msg.text());
  });
  await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });
  if (waitText) {
    await page.getByText(waitText, { exact: false }).first().waitFor({ timeout: 20000 });
  }
  await page.waitForTimeout(800);
  const file = path.join(outDir, name);
  await page.screenshot({ path: file, fullPage: false });
  const bodyText = await page.locator('body').innerText();
  console.log(JSON.stringify({ name, url, width, hasHero: bodyText.includes('Do not let the first conversation'), hasProcess: bodyText.includes('structured process, supported by hospitality'), hasCta: bodyText.includes('Your hotel may have more than one credible future'), errors: errors.slice(0, 8) }));
  await page.close();
  return file;
}

await shot('desktop-hero.png', 'https://www.dealality.com/old-home', 1440, 900, 'For Hotel Owners and Developers');
await shot('desktop-process.png', 'https://www.dealality.com/old-home#how-it-works', 1440, 900, 'A structured process, supported by hospitality judgment');
await shot('desktop-cta.png', 'https://www.dealality.com/old-home#cta', 1440, 900, 'Your hotel may have more than one credible future');
await shot('mobile-hero.png', 'https://www.dealality.com/old-home', 390, 844, 'For Hotel Owners and Developers');

// Opportunity review — may still 404 on Railway
try {
  await shot('mobile-opportunity-form.png', 'https://my-operators-backend-production.up.railway.app/opportunity-review', 390, 844, 'Discuss Your Hotel Opportunity');
} catch (e) {
  console.log(JSON.stringify({ opportunityForm: 'blocked', error: String(e).slice(0, 200) }));
}

await browser.close();
