import puppeteer from "puppeteer";

const url = "https://www.dealality.com/old-home?f17=1";
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 1100 });
await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });

// Wait for connectors ready (draw JS)
await page.waitForFunction(
  () => {
    const el = document.getElementById("dealality-manual-process");
    return el && el.classList.contains("is-connectors-ready");
  },
  { timeout: 45000 }
);

const result = await page.evaluate(() => {
  const cssHrefs = [...document.querySelectorAll('link[rel="stylesheet"]')]
    .map((l) => l.href)
    .filter((h) => /manual-process/i.test(h));
  const scripts = [...document.querySelectorAll("script[src]")]
    .map((s) => s.src)
    .filter((h) => /manual-process/i.test(h));
  const root = document.getElementById("dealality-manual-process");
  const journey = root?.querySelector(".dmp-journey-row");
  const problems = root?.querySelector(".dmp-problems");
  const paths = [...(root?.querySelectorAll("path.dmp-line-in") || [])];
  let lowestPathBottom = null;
  for (const p of paths) {
    const b = p.getBoundingClientRect().bottom;
    if (lowestPathBottom == null || b > lowestPathBottom) lowestPathBottom = b;
  }
  const journeyBottom = journey ? journey.getBoundingClientRect().bottom : null;
  const problemsTop = problems ? problems.getBoundingClientRect().top : null;
  return {
    cssHrefs,
    scripts,
    hasF17Css: cssHrefs.some((h) => /v20260801f17\.css/.test(h)),
    hasF17BootOrDraw: scripts.some((h) =>
      /v20260801f17\.(boot\.)?js|boot\.v20260801f17\.js/.test(h)
    ),
    isConnectorsReady: !!(root && root.classList.contains("is-connectors-ready")),
    dataVersion: root?.getAttribute("data-dmp-version") || null,
    pathCount: paths.length,
    lowestPathBottom,
    journeyBottom,
    problemsTop,
    pathWithinJourney:
      lowestPathBottom != null && journeyBottom != null
        ? lowestPathBottom <= journeyBottom + 2
        : null,
    pathDoesNotReachProblems:
      lowestPathBottom != null && problemsTop != null
        ? lowestPathBottom < problemsTop
        : null,
  };
});

console.log(JSON.stringify({ url, ...result }, null, 2));
await browser.close();
