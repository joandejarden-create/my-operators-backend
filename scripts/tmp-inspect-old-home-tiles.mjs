import puppeteer from "puppeteer";
import fs from "fs";

const out = "tmp-screenshots-tiles";
fs.mkdirSync(out, { recursive: true });
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto("https://www.dealality.com/old-home", {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await new Promise((r) => setTimeout(r, 2500));

const sections = await page.evaluate(() => {
  const ids = [
    "insights",
    "testimonials",
    "trust",
    "platform-features",
    "pricing",
    "modules",
    "perspectives",
  ];
  return ids.map((id) => {
    const el = document.getElementById(id);
    if (!el) return { id, found: false };
    const r = el.getBoundingClientRect();
    const arts = el.querySelectorAll("article");
    return {
      id,
      found: true,
      top: Math.round(r.top + scrollY),
      h: Math.round(r.height),
      articles: arts.length,
      headline: (el.querySelector("h2") || {}).textContent?.slice(0, 90),
    };
  });
});
console.log(JSON.stringify(sections, null, 2));

for (const id of ["insights", "testimonials", "platform-features", "pricing"]) {
  const handle = await page.$(`#${id}`);
  if (!handle) continue;
  await page.evaluate((sid) => {
    document.getElementById(sid)?.scrollIntoView({ block: "center" });
  }, id);
  await new Promise((r) => setTimeout(r, 700));
  await page.screenshot({ path: `${out}/${id}.png` });
}

await page.evaluate(() =>
  document.getElementById("insights")?.scrollIntoView({ block: "center" })
);
await new Promise((r) => setTimeout(r, 500));

async function insightsMetrics(label) {
  const metrics = await page.evaluate(() => {
    const track = document.getElementById("insights-grid");
    if (!track) return null;
    const tr = track.getBoundingClientRect();
    const cards = [...track.querySelectorAll("article")].map((c, i) => {
      const r = c.getBoundingClientRect();
      const visible = Math.max(
        0,
        Math.min(r.right, tr.right) - Math.max(r.left, tr.left)
      );
      const more = c.querySelector("[id$='-more'], a");
      const title = c.querySelector("h3");
      return {
        i,
        id: c.id,
        w: Math.round(r.width),
        visiblePx: Math.round(visible),
        pct: Math.round((100 * visible) / Math.max(1, r.width)),
        cardBottom: Math.round(r.bottom),
        titleBottom: title ? Math.round(title.getBoundingClientRect().bottom) : null,
        moreBottom: more ? Math.round(more.getBoundingClientRect().bottom) : null,
        trackBottom: Math.round(tr.bottom),
      };
    });
    const controls = document.getElementById("insights-controls");
    const lead = document.getElementById("insights-lead");
    return {
      scrollLeft: track.scrollLeft,
      clientW: track.clientWidth,
      scrollW: track.scrollWidth,
      insVisible: getComputedStyle(track).getPropertyValue("--ins-visible").trim(),
      leadBottom: lead ? Math.round(lead.getBoundingClientRect().bottom) : null,
      trackTop: Math.round(tr.top),
      controlsTop: controls
        ? Math.round(controls.getBoundingClientRect().top)
        : null,
      cards,
    };
  });
  console.log(label, JSON.stringify(metrics, null, 2));
  return metrics;
}

await insightsMetrics("insights-before");
const next = await page.$("#insights-next");
if (next) {
  await next.click();
  await new Promise((r) => setTimeout(r, 900));
  await page.screenshot({ path: `${out}/insights-after-click1.png` });
  await insightsMetrics("insights-after-1");
  await next.click();
  await new Promise((r) => setTimeout(r, 900));
  await page.screenshot({ path: `${out}/insights-after-click2.png` });
  await insightsMetrics("insights-after-2");
}

const tmetrics = await page.evaluate(() => {
  const root =
    document.getElementById("testimonials") || document.getElementById("trust");
  if (!root) return null;
  const active =
    root.querySelector("[data-slide].is-active") ||
    root.querySelector("[data-slide]");
  const arts = active
    ? [...active.querySelectorAll("article")].map((a) => {
        const s = getComputedStyle(a);
        const r = a.getBoundingClientRect();
        const p = a.querySelector("p");
        const bq = a.querySelector("blockquote");
        const pr = p ? p.getBoundingClientRect() : null;
        const br = bq ? bq.getBoundingClientRect() : null;
        return {
          display: s.display,
          visibility: s.visibility,
          h: Math.round(r.height),
          top: Math.round(r.top),
          bottom: Math.round(r.bottom),
          pBottom: pr ? Math.round(pr.bottom) : null,
          bqBottom: br ? Math.round(br.bottom) : null,
          gapBqToP: pr && br ? Math.round(pr.top - br.bottom) : null,
        };
      })
    : [];
  return {
    slides: root.querySelectorAll("[data-slide]").length,
    activeArts: arts,
    dots: root.querySelectorAll("#testimonials-dots button").length,
  };
});
console.log("testimonials", JSON.stringify(tmetrics, null, 2));

await page.setViewport({ width: 768, height: 900 });
await page.evaluate(() =>
  document.getElementById("insights")?.scrollIntoView({ block: "center" })
);
await new Promise((r) => setTimeout(r, 700));
await page.screenshot({ path: `${out}/insights-tablet.png` });
await insightsMetrics("insights-tablet");

await page.setViewport({ width: 390, height: 844 });
await page.evaluate(() =>
  document.getElementById("insights")?.scrollIntoView({ block: "center" })
);
await new Promise((r) => setTimeout(r, 700));
await page.screenshot({ path: `${out}/insights-mobile.png` });
await insightsMetrics("insights-mobile");

await browser.close();
