import { chromium } from 'playwright';
import fs from 'fs';
import path from 'path';

const outDir = 'docs/old-home-problem-phase1b-snapshots';
fs.mkdirSync(outDir, { recursive: true });
const url = 'http://localhost:8765/old-home-problem-phase1b-preview.html';

const viewports = [
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'tablet', width: 768, height: 1024 },
  { name: 'mobile', width: 390, height: 844 },
];

const browser = await chromium.launch({ headless: true });
for (const vp of viewports) {
  const page = await browser.newPage({ viewport: { width: vp.width, height: vp.height } });
  await page.goto(url, { waitUntil: 'networkidle' });
  // Hide kickers for public-looking shots (same CSS clip; also force display none for clarity)
  await page.addStyleTag({ content: '.oh-p1b-kicker{display:none!important}' });
  const file = path.join(outDir, `${vp.name}.png`);
  await page.screenshot({ path: file, fullPage: true });
  console.log('wrote', file);
  await page.close();
}
await browser.close();
