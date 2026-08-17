/**
 * Capture cinematic-v1 visual QA screenshots using the same CDN CSS + CDN hotel
 * payload pushed to Webflow HtmlEmbed (hybrid @import method).
 */
import fs from "fs";
import path from "path";
import http from "http";
import puppeteer from "puppeteer";

const root = process.cwd();
const outDir = path.join(root, "docs/old-home-problem-deal-desk-snapshots-cinematic-webflow");
fs.mkdirSync(outDir, { recursive: true });

const hybrid = fs.readFileSync(
  path.join(root, "docs/old-home-problem-deal-desk-embed-cinematic-v1-hybrid.html"),
  "utf8"
);

const pageHtml = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Deal Desk cinematic-v1 Webflow parity</title>
<style>
  html,body{margin:0;background:#0b1220;font-family:Inter,system-ui,sans-serif}
  #about{padding:48px 24px;max-width:1200px;margin:0 auto}
  .native{color:#e8eefc;margin:0 0 28px}
  .native .eyebrow{letter-spacing:.12em;text-transform:uppercase;font-size:12px;opacity:.7;margin:0 0 8px}
  .native h2{font-size:34px;line-height:1.15;margin:0 0 12px;font-weight:650}
  .native .lead{opacity:.78;max-width:42rem;margin:0;line-height:1.5}
  .chapters{display:flex;gap:10px;flex-wrap:wrap;margin:18px 0 0}
  .chapters span{border:1px solid rgba(255,255,255,.18);padding:6px 10px;border-radius:999px;font-size:12px;opacity:.75}
</style>
</head>
<body>
<section id="about" data-oh-problem="deal-desk">
  <div class="native" aria-hidden="true">
    <p class="eyebrow">Native eyebrow (Webflow — not in embed)</p>
    <h2>Native headline preserved</h2>
    <p class="lead">Native supporting paragraph preserved outside the embed.</p>
    <div class="chapters"><span>Fragmented</span><span>Responses</span><span>Upside</span></div>
  </div>
  ${hybrid}
</section>
<script>
  // Local review only — not shipped to Webflow. Mirrors ?dealDeskState= query mechanism.
  (function(){
    const desk=document.querySelector('[data-dealality-problem-desk]');
    if(!desk) return;
    const q=new URLSearchParams(location.search).get('dealDeskState');
    if(q) desk.setAttribute('data-story-state', q);
  })();
</script>
</body>
</html>`;

const previewPath = path.join(outDir, "parity-preview.html");
fs.writeFileSync(previewPath, pageHtml, "utf8");

const server = http.createServer((req, res) => {
  const u = new URL(req.url, "http://127.0.0.1");
  if (u.pathname === "/" || u.pathname === "/parity-preview.html") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(pageHtml);
    return;
  }
  res.writeHead(404);
  res.end("not found");
});

await new Promise((r) => server.listen(8791, "127.0.0.1", r));

const shots = [
  { name: "01-opportunity-1440", state: "opportunity", width: 1440, height: 900, zoom: 1 },
  { name: "02-workstreams-1440", state: "workstreams", width: 1440, height: 900, zoom: 1 },
  { name: "03-workstreams-1280", state: "workstreams", width: 1280, height: 900, zoom: 1 },
  { name: "04-artifacts-1440", state: "artifacts", width: 1440, height: 900, zoom: 1 },
  { name: "05-artifacts-1280", state: "artifacts", width: 1280, height: 900, zoom: 1 },
  { name: "06-comparison-1440", state: "comparison", width: 1440, height: 900, zoom: 1 },
  { name: "07-momentum-1440", state: "momentum", width: 1440, height: 900, zoom: 1 },
  { name: "08-outcome-1440", state: "outcome", width: 1440, height: 900, zoom: 1 },
  { name: "09-tablet-workstreams-900", state: "workstreams", width: 900, height: 1200, zoom: 1 },
  { name: "10-tablet-artifacts-900", state: "artifacts", width: 900, height: 1200, zoom: 1 },
  { name: "11-mobile-opportunity-390", state: "opportunity", width: 390, height: 844, zoom: 1 },
  { name: "12-mobile-artifacts-390", state: "artifacts", width: 390, height: 844, zoom: 1 },
  { name: "13-zoom-150-workstreams", state: "workstreams", width: 1440, height: 900, zoom: 1.5 },
  { name: "14-zoom-200-artifacts", state: "artifacts", width: 1440, height: 900, zoom: 2 },
];

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});

const notes = [];
for (const shot of shots) {
  const page = await browser.newPage();
  await page.setViewport({
    width: shot.width,
    height: shot.height,
    deviceScaleFactor: 1,
  });
  const url = `http://127.0.0.1:8791/parity-preview.html?dealDeskState=${shot.state}`;
  await page.goto(url, { waitUntil: "networkidle0", timeout: 60000 });
  await page.evaluate((z) => {
    document.body.style.zoom = String(z);
  }, shot.zoom);
  await new Promise((r) => setTimeout(r, 400));

  const metrics = await page.evaluate(() => {
    const desk = document.querySelector("[data-dealality-problem-desk]");
    const img = document.querySelector(".dpd-hotel-img");
    const lanes = [...document.querySelectorAll(".dpd-lane-name")].map((n) => n.textContent.trim());
    const caps = [...document.querySelectorAll(".dpd-doc--primary .dpd-doc-cap")].map((n) =>
      n.textContent.trim()
    );
    const depthUnlabeled = [...document.querySelectorAll(".dpd-doc--depth")].every(
      (el) => !el.querySelector(".dpd-doc-cap")
    );
    const compareRows = [...document.querySelectorAll(".dpd-rl")].map((n) => n.textContent.trim());
    const hasEllipsis = [...document.querySelectorAll(".dpd-lane-name, .dpd-doc-cap, .dpd-st-txt")].some(
      (el) => {
        const s = getComputedStyle(el);
        return s.textOverflow === "ellipsis" && el.scrollWidth > el.clientWidth + 1;
      }
    );
    const scrollW = document.documentElement.scrollWidth;
    const clientW = document.documentElement.clientWidth;
    return {
      state: desk?.getAttribute("data-story-state"),
      visual: desk?.getAttribute("data-visual"),
      hotelNatural: img?.naturalWidth || 0,
      hotelComplete: !!img?.complete,
      lanes,
      primaryCaps: caps,
      depthUnlabeled,
      compareRows,
      hasEllipsis,
      horizontalOverflow: scrollW > clientW + 1,
      scrollW,
      clientW,
    };
  });

  const file = path.join(outDir, `${shot.name}.png`);
  await page.screenshot({ path: file, fullPage: false });
  notes.push({ shot: shot.name, file, metrics });
  await page.close();
}

await browser.close();
server.close();

fs.writeFileSync(path.join(outDir, "qa-notes.json"), JSON.stringify(notes, null, 2));
console.log(JSON.stringify({ outDir, count: notes.length, sample: notes[0]?.metrics }, null, 2));
