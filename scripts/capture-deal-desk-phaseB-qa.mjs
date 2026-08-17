/**
 * Phase B Deal Desk QA captures (parity browser).
 */
import fs from "fs";
import path from "path";
import http from "http";
import { spawn } from "child_process";
import puppeteer from "puppeteer";

const root = process.cwd();
const outDir = path.join(
  root,
  "docs/old-home-problem-deal-desk-snapshots-phaseB"
);
fs.mkdirSync(outDir, { recursive: true });

// Ensure preview is fresh
await new Promise((resolve, reject) => {
  const child = spawn("node", ["scripts/build-deal-desk-cinematic-preview.mjs"], {
    cwd: root,
    stdio: "inherit",
  });
  child.on("exit", (code) => (code === 0 ? resolve() : reject(new Error("build failed"))));
});

const previewHtml = fs.readFileSync(
  path.join(root, "docs/old-home-problem-deal-desk-preview.html"),
  "utf8"
);

const server = http.createServer((req, res) => {
  const u = new URL(req.url, "http://127.0.0.1");
  if (u.pathname.startsWith("/public/")) {
    const file = path.join(root, u.pathname.slice(1));
    if (!fs.existsSync(file)) {
      res.writeHead(404);
      res.end("missing");
      return;
    }
    const ext = path.extname(file);
    const type =
      ext === ".jpg" || ext === ".jpeg"
        ? "image/jpeg"
        : ext === ".css"
          ? "text/css"
          : ext === ".js"
            ? "text/javascript"
            : "application/octet-stream";
    res.writeHead(200, { "Content-Type": type });
    res.end(fs.readFileSync(file));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(previewHtml);
});

await new Promise((r) => server.listen(8792, "127.0.0.1", r));

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});

async function shot(name, opts) {
  const page = await browser.newPage();
  const width = opts.width || 1440;
  const height = opts.height || 900;
  await page.setViewport({ width, height, deviceScaleFactor: 1 });
  if (opts.reduced) {
    await page.emulateMediaFeatures([
      { name: "prefers-reduced-motion", value: "reduce" },
    ]);
  }
  const q = opts.query || "";
  await page.goto(`http://127.0.0.1:8792/${q}`, {
    waitUntil: "networkidle0",
    timeout: 60000,
  });
  if (opts.zoom) {
    await page.evaluate((z) => {
      document.body.style.zoom = String(z);
    }, opts.zoom);
  }
  if (opts.waitMs) await new Promise((r) => setTimeout(r, opts.waitMs));
  if (opts.forceState) {
    await page.evaluate((state) => {
      if (window.DealalityDealDesk) window.DealalityDealDesk.setState(state);
    }, opts.forceState);
    await new Promise((r) => setTimeout(r, opts.afterStateMs || 400));
  }
  if (opts.scrollAbout) {
    await page.evaluate(() => {
      const about = document.querySelector("#about");
      if (about) about.scrollIntoView({ block: "center" });
    });
    await new Promise((r) => setTimeout(r, 500));
  }
  if (opts.clickReplay) {
    await page.waitForSelector("[data-deal-desk-replay]:not([hidden])", {
      timeout: 20000,
    });
    await page.click("[data-deal-desk-replay]");
    await new Promise((r) => setTimeout(r, opts.afterReplayMs || 800));
  }
  if (opts.waitComplete) {
    await page.waitForFunction(
      () =>
        document
          .querySelector("#about")
          ?.getAttribute("data-deal-desk-complete") === "true",
      { timeout: 25000 }
    );
    await new Promise((r) => setTimeout(r, 400));
  }

  const metrics = await page.evaluate(() => {
    const about = document.querySelector("#about");
    const desk = document.querySelector("[data-dealality-problem-desk]");
    const pvls = document.querySelectorAll("#oh-pvl");
    const replay = document.querySelector("[data-deal-desk-replay]");
    return {
      state: desk?.getAttribute("data-story-state"),
      initialized: about?.getAttribute("data-deal-desk-initialized"),
      complete: about?.getAttribute("data-deal-desk-complete"),
      motion: about?.getAttribute("data-deal-desk-motion"),
      pvlCount: pvls.length,
      pvlAboutAttr: document.documentElement.getAttribute("data-oh-pvl-about"),
      replayHidden: replay ? !!replay.hidden : null,
      chapter: [
        ...document.querySelectorAll("[data-problem-chapter].is-active"),
      ].map((n) => n.getAttribute("data-problem-chapter")),
      hOverflow:
        document.documentElement.scrollWidth >
        document.documentElement.clientWidth + 1,
    };
  });

  const file = path.join(outDir, `${name}.png`);
  await page.screenshot({ path: file, fullPage: false });
  await page.close();
  return { name, file, metrics };
}

const notes = [];

// Force scroll into view + start by scrolling about to center then waiting through timeline
async function autoplayShot(name, width, height, waitComplete, extra) {
  const page = await browser.newPage();
  await page.setViewport({
    width: width || 1440,
    height: height || 900,
    deviceScaleFactor: 1,
  });
  if (extra?.reduced) {
    await page.emulateMediaFeatures([
      { name: "prefers-reduced-motion", value: "reduce" },
    ]);
  }
  await page.goto("http://127.0.0.1:8792/", {
    waitUntil: "networkidle0",
    timeout: 60000,
  });
  await page.evaluate(() => {
    document.querySelector("#about")?.scrollIntoView({ block: "center" });
  });
  // Trigger IO by small scroll nudge
  await page.evaluate(() => window.scrollBy(0, 1));
  await new Promise((r) => setTimeout(r, 600));

  if (extra?.forceState) {
    await page.evaluate((s) => window.DealalityDealDesk?.setState(s), extra.forceState);
    await new Promise((r) => setTimeout(r, 500));
  } else if (waitComplete) {
    await page.waitForFunction(
      () =>
        document
          .querySelector("#about")
          ?.getAttribute("data-deal-desk-complete") === "true",
      { timeout: 25000 }
    );
  } else if (extra?.waitMs) {
    await new Promise((r) => setTimeout(r, extra.waitMs));
  }

  if (extra?.zoom) {
    await page.evaluate((z) => {
      document.body.style.zoom = String(z);
    }, extra.zoom);
    await new Promise((r) => setTimeout(r, 200));
  }

  if (extra?.clickReplay) {
    await page.waitForSelector("[data-deal-desk-replay]:not([hidden])", {
      timeout: 5000,
    });
    await page.click("[data-deal-desk-replay]");
    await new Promise((r) => setTimeout(r, extra.afterReplayMs || 1200));
  }

  if (extra?.leaveAbout) {
    await page.evaluate(() => window.scrollTo(0, 0));
    await new Promise((r) => setTimeout(r, 400));
  }

  const metrics = await page.evaluate(() => {
    const about = document.querySelector("#about");
    const desk = document.querySelector("[data-dealality-problem-desk]");
    return {
      state: desk?.getAttribute("data-story-state"),
      complete: about?.getAttribute("data-deal-desk-complete"),
      pvlCount: document.querySelectorAll("#oh-pvl").length,
      pvlAboutAttr: document.documentElement.getAttribute("data-oh-pvl-about"),
      replayHidden: document.querySelector("[data-deal-desk-replay]")?.hidden,
      chapter: [
        ...document.querySelectorAll("[data-problem-chapter].is-active"),
      ].map((n) => n.getAttribute("data-problem-chapter")),
      hOverflow:
        document.documentElement.scrollWidth >
        document.documentElement.clientWidth + 1,
      errors: window.__dpdErrors || [],
    };
  });

  const file = path.join(outDir, `${name}.png`);
  await page.screenshot({ path: file, fullPage: false });
  await page.close();
  notes.push({ name, file, metrics });
}

// State freezes for deterministic shots
for (const state of [
  "opportunity",
  "workstreams",
  "artifacts",
  "comparison",
  "momentum",
  "outcome",
]) {
  notes.push(
    await shot(`01-${state}-1440`, {
      width: 1440,
      height: 900,
      query: `?dealDeskState=${state}`,
      scrollAbout: true,
      waitMs: 500,
    })
  );
}

await autoplayShot("07-replay-reset", 1440, 900, true, {
  clickReplay: true,
  afterReplayMs: 1600,
});

await autoplayShot("08-tablet-900", 900, 1200, false, {
  forceState: "workstreams",
});

await autoplayShot("09-mobile-390", 390, 844, false, {
  forceState: "artifacts",
});

await autoplayShot("10-reduced-motion", 1440, 900, false, {
  reduced: true,
  waitMs: 800,
});

await autoplayShot("11-zoom-150", 1440, 900, false, {
  forceState: "comparison",
  zoom: 1.5,
});

await autoplayShot("12-zoom-200", 1440, 900, false, {
  forceState: "outcome",
  zoom: 2,
});

await autoplayShot("13-pvl-hidden-about", 1440, 900, false, {
  forceState: "opportunity",
  waitMs: 600,
});

await autoplayShot("14-pvl-restored-top", 1440, 900, false, {
  forceState: "opportunity",
  leaveAbout: true,
});

// Console / dedupe check page
{
  const page = await browser.newPage();
  const errors = [];
  page.on("pageerror", (e) => errors.push(String(e)));
  page.on("console", (msg) => {
    if (msg.type() === "error") errors.push(msg.text());
  });
  await page.setViewport({ width: 1440, height: 900 });
  await page.goto("http://127.0.0.1:8792/", { waitUntil: "networkidle0" });
  await page.evaluate(() => {
    document.querySelector("#about")?.scrollIntoView({ block: "center" });
    window.scrollBy(0, 1);
  });
  await page.waitForFunction(
    () =>
      document.querySelector("#about")?.getAttribute("data-deal-desk-complete") ===
      "true",
    { timeout: 25000 }
  );
  // replay twice for timer leak smoke
  await page.click("[data-deal-desk-replay]");
  await new Promise((r) => setTimeout(r, 500));
  await page.waitForFunction(
    () =>
      document.querySelector("#about")?.getAttribute("data-deal-desk-complete") ===
      "true",
    { timeout: 25000 }
  );
  await page.click("[data-deal-desk-replay]");
  await new Promise((r) => setTimeout(r, 500));
  await page.waitForFunction(
    () =>
      document.querySelector("#about")?.getAttribute("data-deal-desk-complete") ===
      "true",
    { timeout: 25000 }
  );
  const final = await page.evaluate(() => ({
    pvlCount: document.querySelectorAll("#oh-pvl").length,
    initialized: document
      .querySelector("#about")
      ?.getAttribute("data-deal-desk-initialized"),
    state: document
      .querySelector("[data-dealality-problem-desk]")
      ?.getAttribute("data-story-state"),
  }));
  notes.push({ name: "15-replay-twice-console", metrics: { ...final, errors } });
  await page.close();
}

await browser.close();
server.close();

fs.writeFileSync(path.join(outDir, "qa-notes.json"), JSON.stringify(notes, null, 2));
console.log(JSON.stringify({ outDir, count: notes.length, last: notes[notes.length - 1] }, null, 2));
