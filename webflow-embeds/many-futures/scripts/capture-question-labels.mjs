import { chromium } from "playwright";
import { createServer } from "http";
import {
  readFileSync,
  existsSync,
  mkdirSync,
  writeFileSync,
  copyFileSync,
} from "fs";
import { join, extname, dirname } from "path";
import { fileURLToPath } from "url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const outDir = "/opt/cursor/artifacts/many-futures/question-labels";
const repoOut = join(root, "visual-review/question-labels");
mkdirSync(outDir, { recursive: true });
mkdirSync(repoOut, { recursive: true });

const mime = {
  ".html": "text/html",
  ".css": "text/css",
  ".js": "application/javascript",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".webp": "image/webp",
};

const server = createServer((req, res) => {
  const u = decodeURIComponent((req.url || "/").split("?")[0]);
  const p = join(root, u === "/" ? "preview.html" : u);
  if (!existsSync(p)) {
    res.writeHead(404);
    res.end();
    return;
  }
  res.writeHead(200, { "Content-Type": mime[extname(p)] || "application/octet-stream" });
  res.end(readFileSync(p));
});

await new Promise((r) => server.listen(0, "127.0.0.1", r));
const port = server.address().port;
const browser = await chromium.launch({
  executablePath: "/usr/local/bin/google-chrome",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});

const IDS = [
  "rebrand",
  "operators",
  "affiliation",
  "residences",
  "confidential",
  "market",
  "actions",
  "proposals",
  "clarify",
];

const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
await page.goto(`http://127.0.0.1:${port}/preview.html`, {
  waitUntil: "networkidle",
});
await page.waitForSelector("#dealality-many-futures");

const titles = await page.$$eval(".mf-q-title", (els) =>
  els.map((el) => el.textContent.trim())
);
const decisionTitles = await page.$$eval(".mf-decision-title", (els) =>
  els.map((el) => el.textContent.trim())
);

const rail = page.locator(".mf-rail-sticky, .mf-questions").first();
await rail.screenshot({ path: join(outDir, "desktop-rail-all-questions.png") });

const rootEl = page.locator("#dealality-many-futures");
await rootEl.screenshot({ path: join(outDir, "desktop-rebrand-full.png") });

for (const id of ["affiliation", "residences", "proposals", "clarify"]) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(200);
  await rootEl.screenshot({ path: join(outDir, `desktop-${id}.png`) });
}

const hotelH = await page.locator(".mf-hotel").evaluate((el) => el.getBoundingClientRect().height);
const qH = await page.locator(".mf-questions").evaluate((el) => el.getBoundingClientRect().height);

const qa = {
  titles,
  decisionTitles,
  hotelHeight: Math.round(hotelH),
  questionsHeight: Math.round(qH),
  heightDelta: Math.round(Math.abs(hotelH - qH)),
};

writeFileSync(join(outDir, "qa.json"), JSON.stringify(qa, null, 2) + "\n");
writeFileSync(
  join(repoOut, "REVIEW.md"),
  `# Question labels — local review

**Webflow not updated. Not published.**

## Goal
Make each rail question as scannable as “Independent or affiliated?” so users quickly identify where to focus.

## Titles

| # | Label |
|---|-------|
${titles.map((t, i) => `| ${String(i + 1).padStart(2, "0")} | ${t} |`).join("\n")}

## QA
- Hotel / questions height: ${Math.round(hotelH)} / ${Math.round(qH)} (Δ ${Math.round(Math.abs(hotelH - qH))})
- Decision titles match rail titles
`
);

for (const f of [
  "desktop-rail-all-questions.png",
  "desktop-rebrand-full.png",
  "desktop-affiliation.png",
  "desktop-residences.png",
  "desktop-proposals.png",
  "desktop-clarify.png",
  "qa.json",
]) {
  copyFileSync(join(outDir, f), join(repoOut, f));
}

console.log(JSON.stringify(qa, null, 2));
await browser.close();
server.close();
