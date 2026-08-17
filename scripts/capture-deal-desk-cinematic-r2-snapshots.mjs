import fs from "fs";
import path from "path";
import puppeteer from "puppeteer";

const outDir = path.join(
  process.cwd(),
  "docs/old-home-problem-deal-desk-snapshots-cinematic"
);
fs.mkdirSync(outDir, { recursive: true });

const base =
  "http://127.0.0.1:8788/docs/old-home-problem-deal-desk-preview.html";

const shots = [
  {
    name: "r2-workstreams-1440",
    url: `${base}?dealDeskState=workstreams`,
    width: 1440,
    height: 1100,
    clip: "desk",
  },
  {
    name: "r2-workstreams-1280",
    url: `${base}?dealDeskState=workstreams`,
    width: 1280,
    height: 1100,
    clip: "desk",
  },
  {
    name: "r2-artifacts-1440",
    url: `${base}?dealDeskState=artifacts`,
    width: 1440,
    height: 1100,
    clip: "desk",
  },
  {
    name: "r2-artifacts-1280",
    url: `${base}?dealDeskState=artifacts`,
    width: 1280,
    height: 1100,
    clip: "desk",
  },
  {
    name: "r2-tablet-workstreams-900",
    url: `${base}?dealDeskState=workstreams`,
    width: 900,
    height: 1200,
    clip: "desk",
  },
  {
    name: "r2-tablet-artifacts-900",
    url: `${base}?dealDeskState=artifacts`,
    width: 900,
    height: 1200,
    clip: "desk",
  },
  {
    name: "r2-capital-partner-closeup",
    url: `${base}?dealDeskState=artifacts`,
    width: 1280,
    height: 1100,
    clip: "capital",
  },
  {
    name: "r2-mobile-artifacts-390",
    url: `${base}?dealDeskState=artifacts`,
    width: 390,
    height: 1100,
    clip: "desk",
  },
  {
    name: "r2-comparison-1440",
    url: `${base}?dealDeskState=comparison`,
    width: 1440,
    height: 1100,
    clip: "desk",
  },
  {
    name: "r2-outcome-1440",
    url: `${base}?dealDeskState=outcome`,
    width: 1440,
    height: 1100,
    clip: "desk",
  },
];

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});

try {
  for (const shot of shots) {
    const page = await browser.newPage();
    await page.setViewport({
      width: shot.width,
      height: shot.height,
      deviceScaleFactor: 1,
    });
    await page.goto(shot.url, { waitUntil: "networkidle0", timeout: 60000 });
    await page.waitForSelector(".dealality-problem-desk", { timeout: 15000 });

    const selector =
      shot.clip === "capital"
        ? ".dpd-lane--capital"
        : ".dealality-problem-desk";
    const el = await page.$(selector);
    const file = path.join(outDir, `${shot.name}.png`);
    await el.screenshot({ path: file });

    const metrics = await page.evaluate(() => {
      const root = document.querySelector(".dealality-problem-desk");
      const docs = [...document.querySelectorAll(".dpd-doc--primary")];
      const lanes = [...document.querySelectorAll(".dpd-lane")];
      const capital = document.querySelector(".dpd-lane--capital");
      const rs = (n) =>
        n
          ? {
              w: Math.round(n.getBoundingClientRect().width),
              h: Math.round(n.getBoundingClientRect().height),
            }
          : null;
      return {
        state: root?.getAttribute("data-story-state"),
        lanes: lanes.length,
        laneNames: lanes.map((l) =>
          l.querySelector(".dpd-lane-name")?.textContent?.trim()
        ),
        primaryDocAvgH:
          docs.length === 0
            ? 0
            : Math.round(
                docs.reduce((a, d) => a + d.getBoundingClientRect().height, 0) /
                  docs.length
              ),
        capital: rs(capital),
        hasEllipsisRule: getComputedStyle(
          document.querySelector(".dpd-doc-cap") || document.body
        ).textOverflow,
      };
    });

    console.log(
      JSON.stringify({
        file: path.basename(file),
        viewport: { w: shot.width, h: shot.height },
        ...metrics,
      })
    );
    await page.close();
  }
} finally {
  await browser.close();
}
