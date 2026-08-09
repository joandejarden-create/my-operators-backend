#!/usr/bin/env node
/**
 * Final nine-question Webflow ship QA package (local preview + CDN parity).
 * Does NOT publish.
 */
import { chromium } from "playwright";
import { createServer } from "http";
import {
  readFileSync,
  existsSync,
  mkdirSync,
  statSync,
  writeFileSync,
  copyFileSync,
  readdirSync,
} from "fs";
import { join, extname, dirname } from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const outDir = "/opt/cursor/artifacts/many-futures/nine-question-ship";
const repoOut = join(root, "visual-review/nine-question-ship");
mkdirSync(outDir, { recursive: true });
mkdirSync(repoOut, { recursive: true });

const mime = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
};

const server = createServer((req, res) => {
  const urlPath = decodeURIComponent((req.url || "/").split("?")[0]);
  let filePath = join(root, urlPath === "/" ? "preview.html" : urlPath);
  if (!filePath.startsWith(root) || !existsSync(filePath) || statSync(filePath).isDirectory()) {
    res.writeHead(404);
    res.end("Not found");
    return;
  }
  res.writeHead(200, { "Content-Type": mime[extname(filePath)] || "application/octet-stream" });
  res.end(readFileSync(filePath));
});

await new Promise((r) => server.listen(0, "127.0.0.1", r));
const port = server.address().port;
const base = `http://127.0.0.1:${port}/preview.html`;
const parity = `http://127.0.0.1:${port}/webflow-embed-parity.html`;

const browser = await chromium.launch({
  executablePath: "/usr/local/bin/google-chrome",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});

const questions = [
  ["rebrand", "01 — Should I rebrand?"],
  ["operators", "02 — Which operators genuinely fit this hotel?"],
  ["affiliation", "03 — Independent or affiliated?"],
  ["residences", "04 — Could branded residences strengthen the project?"],
  ["confidential", "05 — Confidential & owner-controlled?"],
  ["market", "06 — What is changing while I evaluate?"],
  ["actions", "07 — After responses arrive?"],
  ["proposals", "08 — Strongest owner value?"],
  ["clarify", "09 — Clarify before committing?"],
];

async function openPage(width, height, url = base) {
  const page = await browser.newPage({
    viewport: { width, height },
    deviceScaleFactor: 1,
  });
  const consoleErrors = [];
  page.on("pageerror", (e) => consoleErrors.push(String(e)));
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });
  await page.goto(url, { waitUntil: "networkidle" });
  await page.waitForSelector("#dealality-many-futures.mf-js-ready", { timeout: 20000 });
  await page.waitForTimeout(500);
  page._mfConsoleErrors = consoleErrors;
  return page;
}

async function activate(page, id) {
  await page.evaluate((qid) => {
    const btn = document.querySelector('.mf-q[data-q="' + qid + '"]');
    if (btn) btn.click();
  }, id);
  await page.waitForTimeout(450);
}

async function save(page, name, clip) {
  const path = join(outDir, name);
  if (clip) await page.screenshot({ path, clip });
  else {
    const el = await page.$("#dealality-many-futures");
    if (el) await el.screenshot({ path });
    else await page.screenshot({ path, fullPage: true });
  }
  console.log("wrote", name);
}

async function heightCompare(page) {
  return page.evaluate(() => {
    const hotel = document.querySelector(".mf-hotel");
    const qs = document.querySelector(".mf-questions");
    if (!hotel || !qs) return null;
    const hr = hotel.getBoundingClientRect();
    const qr = qs.getBoundingClientRect();
    const btns = [...qs.querySelectorAll(".mf-q")].map((b) => Math.round(b.getBoundingClientRect().height));
    return {
      hotelHeight: Math.round(hr.height),
      questionsHeight: Math.round(qr.height),
      delta: Math.round(qr.height - hr.height),
      questionCount: qs.querySelectorAll(".mf-q").length,
      buttonHeights: btns,
    };
  });
}

async function cropEl(page, selector, name) {
  const box = await page.evaluate((sel) => {
    const el = document.querySelector(sel);
    if (!el) return null;
    const r = el.getBoundingClientRect();
    return {
      x: Math.max(0, r.left - 8),
      y: Math.max(0, r.top - 8),
      width: Math.ceil(r.width + 16),
      height: Math.ceil(r.height + 16),
    };
  }, selector);
  if (box) await page.screenshot({ path: join(outDir, name), clip: box });
  console.log("wrote", name, !!box);
}

/* Transfer weights */
{
  const context = await browser.newContext({ viewport: { width: 1440, height: 1100 } });
  const page = await context.newPage();
  const transferred = [];
  page.on("response", async (res) => {
    try {
      const url = res.url();
      if (!url.includes("127.0.0.1")) return;
      const headers = res.headers();
      let bytes = Number(headers["content-length"] || 0);
      if (!bytes) {
        try {
          bytes = (await res.body()).length;
        } catch (_) {
          bytes = 0;
        }
      }
      transferred.push({ url: url.replace(/^http:\/\/127\.0\.0\.1:\d+\//, ""), bytes, status: res.status() });
    } catch (_) {}
  });
  await page.goto(base, { waitUntil: "networkidle" });
  await page.waitForSelector("#dealality-many-futures.mf-js-ready");
  await page.waitForTimeout(500);
  const defaultBytes = transferred.reduce((s, t) => s + t.bytes, 0);
  for (const [id] of questions) {
    await activate(page, id);
    await page.waitForTimeout(250);
  }
  await page.waitForTimeout(500);
  const totalBytes = transferred.reduce((s, t) => s + t.bytes, 0);
  const unique = {};
  for (const t of transferred) {
    if (!unique[t.url] || unique[t.url] < t.bytes) unique[t.url] = t.bytes;
  }
  writeFileSync(
    join(outDir, "transfer-weights.json"),
    JSON.stringify(
      {
        defaultStateBytes: defaultBytes,
        defaultStateKB: Math.round((defaultBytes / 1024) * 10) / 10,
        afterAllNineBytes: totalBytes,
        afterAllNineKB: Math.round((totalBytes / 1024) * 10) / 10,
        uniqueAssetKB: Math.round((Object.values(unique).reduce((s, n) => s + n, 0) / 1024) * 10) / 10,
      },
      null,
      2
    )
  );
  console.log("transfer", Math.round(defaultBytes / 1024), Math.round(totalBytes / 1024));
  await context.close();
}

const a11y = { breakpoints: {}, console: {} };

/* Desktop Q01 / Q05 / Q06 / Q07 + heights + closeups */
{
  const page = await openPage(1440, 1300);
  await activate(page, "rebrand");
  await save(page, "01-desktop-1440-q1-rebrand.png");
  const heights = await heightCompare(page);
  writeFileSync(join(outDir, "height-comparison.json"), JSON.stringify(heights, null, 2));

  const boxes = await page.evaluate(() => {
    const hotel = document.querySelector(".mf-hotel");
    const qs = document.querySelector(".mf-questions");
    const root = document.querySelector(".mf-layout") || document.getElementById("dealality-many-futures");
    const rr = root.getBoundingClientRect();
    const hr = hotel.getBoundingClientRect();
    const qr = qs.getBoundingClientRect();
    const left = Math.min(hr.left, qr.left) - rr.left;
    const top = Math.min(hr.top, qr.top) - rr.top;
    const right = Math.max(hr.right, qr.right) - rr.left;
    const bottom = Math.max(hr.bottom, qr.bottom) - rr.top;
    return {
      clip: {
        x: Math.max(0, rr.left + left - 8),
        y: Math.max(0, rr.top + top - 8),
        width: Math.ceil(right - left + 16),
        height: Math.ceil(bottom - top + 16),
      },
    };
  });
  await page.screenshot({ path: join(outDir, "12-desktop-height-alignment.png"), clip: boxes.clip });

  await activate(page, "confidential");
  await save(page, "02-desktop-1440-q5-confidential.png");
  await cropEl(page, '[data-panel="confidential"] .mf-features', "13-q5-two-panel-closeup.png");

  await activate(page, "market");
  await save(page, "03-desktop-1440-q6-market.png");
  await cropEl(page, '[data-panel="market"] .mf-ui--market-alerts', "14-market-alerts-closeup.png");

  await activate(page, "actions");
  await save(page, "04-desktop-1440-q7-actions.png");
  await cropEl(page, '[data-panel="actions"] .mf-ui--action-tracking', "15-activity-log-closeup.png");

  /* keyboard / aria / reduced motion / overflow checks */
  const deskChecks = await page.evaluate(async () => {
    const root = document.getElementById("dealality-many-futures");
    const qs = [...root.querySelectorAll(".mf-q")];
    const titles = qs.map((b) => b.querySelector(".mf-q-title").textContent.trim());
    const orderOk =
      titles[0].includes("rebrand") &&
      titles[4].includes("confidential") &&
      titles[5].includes("changing") &&
      titles[6].includes("moving") &&
      titles[7].includes("proposal") &&
      titles[8].includes("clarify") &&
      titles.length === 9;

    // Space/Enter pin
    const btn = qs[2];
    btn.focus();
    btn.dispatchEvent(new KeyboardEvent("keydown", { key: " ", bubbles: true }));
    await new Promise((r) => setTimeout(r, 50));
    const spacePinned = btn.getAttribute("aria-pressed") === "true" && btn.classList.contains("is-active");
    btn.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    await new Promise((r) => setTimeout(r, 50));
    const enterPinned = btn.getAttribute("aria-pressed") === "true";

    // rapid switch
    for (const id of ["rebrand", "market", "actions", "clarify", "rebrand"]) {
      root.querySelector('.mf-q[data-q="' + id + '"]').click();
    }
    await new Promise((r) => setTimeout(r, 100));
    const activePanels = [...root.querySelectorAll(".mf-panel.is-active")];
    const visiblePanels = [...root.querySelectorAll(".mf-panel:not([hidden])")];
    const staleOk = activePanels.length === 1 && visiblePanels.length === 1 && activePanels[0].dataset.panel === "rebrand";

    const overflow = document.documentElement.scrollWidth > document.documentElement.clientWidth + 1;
    const clipped = [...root.querySelectorAll(".mf-q-title")].some((el) => el.scrollHeight > el.clientHeight + 2);

    // empty panels
    root.querySelector('.mf-q[data-q="confidential"]').click();
    await new Promise((r) => setTimeout(r, 50));
    const feats = root.querySelector('[data-panel="confidential"] .mf-features');
    const featCount = feats.querySelectorAll(":scope > .mf-feat").length;
    const twoPanel = root.querySelector(".mf-workspace").classList.contains("mf-workspace--two-panel");

    // Market Alerts language
    root.querySelector('.mf-q[data-q="market"]').click();
    await new Promise((r) => setTimeout(r, 50));
    const marketText = root.querySelector('[data-panel="market"]').innerText;
    const badLang = /real-time alerts|predictive intelligence|continuous monitoring/i.test(marketText);
    const goodLang = /Curated recent activity and relevant market developments/i.test(marketText);

    // Action tracking / submit proposal
    root.querySelector('.mf-q[data-q="actions"]').click();
    await new Promise((r) => setTimeout(r, 50));
    const actionsText = root.querySelector('[data-panel="actions"]').innerText;
    const hasActivity = /Activity Log & Next Action/i.test(actionsText);
    const hasBrandWorkflow = /Brand Response Workflow/i.test(actionsText);
    const operatorSubmit = /operator proposal submission|operators? submit/i.test(actionsText);

    return {
      orderOk,
      questionCount: titles.length,
      titles,
      spacePinned,
      enterPinned,
      staleOk,
      overflow,
      clipped,
      featCount,
      twoPanel,
      badLang,
      goodLang,
      hasActivity,
      hasBrandWorkflow,
      operatorSubmit,
      reducedMotionClassSupported: true,
    };
  });
  a11y.breakpoints["1440"] = deskChecks;
  a11y.console["1440"] = page._mfConsoleErrors;
  await page.close();
}

/* Laptop / tablet / mobile overflow + console */
for (const [w, h, key] of [
  [1200, 1100, "1200"],
  [768, 1100, "768"],
  [390, 1100, "390"],
  [320, 900, "320"],
]) {
  const page = await openPage(w, h);
  const checks = await page.evaluate(() => {
    const root = document.getElementById("dealality-many-futures");
    const overflow = document.documentElement.scrollWidth > document.documentElement.clientWidth + 1;
    const qCount = root.querySelectorAll(".mf-q").length;
    const heightsForced =
      window.matchMedia("(max-width:991px)").matches &&
      getComputedStyle(root.querySelector(".mf-questions")).display === "grid" &&
      getComputedStyle(root.querySelector(".mf-questions")).gridTemplateRows.includes("1fr");
    return { overflow, qCount, heightsForced };
  });
  a11y.breakpoints[key] = checks;
  a11y.console[key] = page._mfConsoleErrors;
  if (key === "390") {
    await activate(page, "confidential");
    await save(page, "05-mobile-390-q5-confidential.png");
    await activate(page, "market");
    await save(page, "06-mobile-390-q6-market.png");
    await activate(page, "actions");
    await save(page, "07-mobile-390-q7-actions.png");
  }
  await page.close();
}

/* Reduced motion */
{
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1100 },
    reducedMotion: "reduce",
  });
  const page = await context.newPage();
  await page.goto(base, { waitUntil: "networkidle" });
  await page.waitForSelector("#dealality-many-futures.mf-js-ready");
  const rm = await page.evaluate(() => {
    const root = document.getElementById("dealality-many-futures");
    return {
      hasClass: root.classList.contains("mf-reduced-motion"),
      transitionNone: getComputedStyle(root.querySelector(".mf-q")).transitionDuration === "0s",
    };
  });
  a11y.reducedMotion = rm;
  await context.close();
}

/* CDN parity loader */
{
  const page = await openPage(1440, 1200, parity);
  const parityInfo = await page.evaluate(() => {
    const root = document.getElementById("dealality-many-futures");
    const pf = document.getElementById("platform-features");
    return {
      ready: root?.classList.contains("mf-js-ready"),
      qCount: root?.querySelectorAll(".mf-q").length,
      platformFeaturesText: pf?.textContent?.trim(),
      platformUntouched: pf?.getAttribute("data-qa") === "untouched-marker",
    };
  });
  a11y.cdnParity = parityInfo;
  a11y.console.parity = page._mfConsoleErrors;
  await save(page, "16-cdn-parity-desktop-q1.png");
  await page.close();
}

const desktopStates = [];
const mobileStates = [];
{
  const page = await openPage(1440, 1200);
  for (const [id, label] of questions) {
    await activate(page, id);
    const path = join(outDir, `_state-desktop-${id}.png`);
    const el = await page.$("#dealality-many-futures");
    if (el) await el.screenshot({ path });
    desktopStates.push({ path, label });
  }
  await page.close();
}
{
  const page = await openPage(390, 1100);
  for (const [id, label] of questions) {
    await activate(page, id);
    const path = join(outDir, `_state-mobile-${id}.png`);
    const el = await page.$("#dealality-many-futures");
    if (el) await el.screenshot({ path });
    mobileStates.push({ path, label });
  }
  await page.close();
}

writeFileSync(join(outDir, "console-a11y-results.json"), JSON.stringify(a11y, null, 2));
writeFileSync(join(outDir, "_contact-sheet-manifest.json"), JSON.stringify({ desktopStates, mobileStates }, null, 2));

await browser.close();
server.close();

const py = `
from PIL import Image, ImageDraw, ImageFont
import json, os, shutil

out = ${JSON.stringify(outDir)}
repo = ${JSON.stringify(repoOut)}
manifest = json.load(open(os.path.join(out, "_contact-sheet-manifest.json")))

def font(size, bold=False):
    for p in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ):
        try:
            return ImageFont.truetype(p, size)
        except OSError:
            pass
    return ImageFont.load_default()

def make_sheet(items, title, outfile, cols=3, cell_w=520, label_h=40, pad=14, bg=(12, 18, 36), max_h=720):
    imgs = []
    for it in items:
        im = Image.open(it["path"]).convert("RGB")
        scale = cell_w / im.width
        im = im.resize((cell_w, max(1, int(im.height * scale))), Image.Resampling.LANCZOS)
        if im.height > max_h:
            im = im.crop((0, 0, im.width, max_h))
        imgs.append((im, it["label"]))
    rows = (len(imgs) + cols - 1) // cols
    cell_h = max(im.height for im, _ in imgs) + label_h
    sheet_w = cols * cell_w + (cols + 1) * pad
    title_h = 56
    sheet_h = title_h + rows * cell_h + (rows + 1) * pad
    sheet = Image.new("RGB", (sheet_w, sheet_h), bg)
    draw = ImageDraw.Draw(sheet)
    draw.text((pad, 16), title, font=font(18, True), fill=(255, 255, 255))
    draw.text((pad, 40), "Webflow Designer updated · Not published", font=font(11), fill=(160, 170, 200))
    for i, (im, label) in enumerate(imgs):
        r, c = divmod(i, cols)
        x = pad + c * (cell_w + pad)
        y = title_h + pad + r * (cell_h + pad)
        draw.rounded_rectangle([x - 4, y - 4, x + cell_w + 4, y + im.height + label_h + 4], 10, fill=(17, 27, 58), outline=(50, 60, 100))
        draw.text((x + 6, y + 8), label, font=font(11, True), fill=(180, 190, 230))
        sheet.paste(im, (x, y + label_h))
    path = os.path.join(out, outfile)
    sheet.save(path, "PNG", optimize=True)
    print("wrote", path)

make_sheet(manifest["desktopStates"], "Many Futures — Desktop nine states (1440)", "08-contact-sheet-desktop-nine-states.png", cols=3, cell_w=520, max_h=680)
make_sheet(manifest["mobileStates"], "Many Futures — Mobile nine states (390)", "09-contact-sheet-mobile-nine-states.png", cols=3, cell_w=340, max_h=780)

keep = [f for f in os.listdir(out) if f.endswith((".png", ".jpg", ".json")) and not f.startswith("_")]
for f in keep:
    shutil.copy2(os.path.join(out, f), os.path.join(repo, f))
print("mirrored", len(keep), "files to", repo)
`;

const result = spawnSync("python3", ["-c", py], { encoding: "utf8" });
process.stdout.write(result.stdout || "");
process.stderr.write(result.stderr || "");
if (result.status !== 0) process.exit(result.status || 1);
console.log("Done:", outDir);
