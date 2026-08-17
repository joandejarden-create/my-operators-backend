import puppeteer from 'puppeteer';

const url = process.argv[2] || 'https://www.dealality.com/old-home?f15=1';

const browser = await puppeteer.launch({
  headless: 'new',
  args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
});
try {
  const page = await browser.newPage();
  page.setDefaultTimeout(45000);
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForFunction(
    () =>
      !!document.querySelector('.dmp-problem-p') &&
      !!document.querySelector('.dmp-manual-label') &&
      !!document.querySelector('#dealality-manual-process'),
    { timeout: 30000 }
  );
  await new Promise((r) => setTimeout(r, 1000));
  const result = await page.evaluate(() => {
    const problem = document.querySelector('.dmp-problem-p');
    const label = document.querySelector('.dmp-manual-label');
    const links = [...document.querySelectorAll('link[rel="stylesheet"]')].map((l) => l.href);
    return {
      problemColor: problem ? getComputedStyle(problem).color : null,
      labelColor: label ? getComputedStyle(label).color : null,
      f15CssHref: links.find((h) => h.includes('v20260801f15.css')) || null,
      hasSection: !!document.querySelector('#dealality-manual-process'),
      dataDmpVersion: document.querySelector('#dealality-manual-process')?.getAttribute('data-dmp-version') || null,
    };
  });
  console.log(JSON.stringify(result, null, 2));
} finally {
  await browser.close();
}
