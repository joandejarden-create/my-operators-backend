/**
 * Capture desktop + mobile QA shots for the floating video launcher.
 * Usage: node scripts/screenshot-platform-video-launcher.mjs
 */
import fs from "fs";
import path from "path";
import puppeteer from "puppeteer";

const outDir = path.resolve(
  "public/marketing/qa-shots/platform-video-launcher"
);
fs.mkdirSync(outDir, { recursive: true });

const url = "https://www.dealality.com/old-home?pvlqa=" + Date.now();

async function forceReveal(page) {
  await page.evaluate(() => {
    try {
      sessionStorage.removeItem("dl_platform_video_launcher_dismissed_v3");
      sessionStorage.removeItem("dl_platform_video_launcher_dismissed_v2");
    } catch (_e) {}
  });
  await page.reload({ waitUntil: "networkidle2", timeout: 90000 });
  await page.waitForFunction(
    () => {
      const el = document.getElementById("oh-pvl");
      return el && el.classList.contains("is-visible");
    },
    { timeout: 20000 }
  ).catch(async () => {
    // Force reveal if timer hasn't fired yet (page may cache old delay).
    await page.evaluate(() => {
      const el = document.getElementById("oh-pvl");
      if (!el) return;
      el.removeAttribute("hidden");
      el.setAttribute("data-state", "collapsed");
      el.classList.add("is-visible");
    });
  });
  await new Promise((r) => setTimeout(r, 600));
}

async function shot(page, name) {
  const file = path.join(outDir, name);
  await page.screenshot({ path: file, fullPage: false });
  console.log("wrote", file);
}

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});

try {
  // Desktop collapsed + expanded
  const desk = await browser.newPage();
  await desk.setViewport({ width: 1440, height: 900, deviceScaleFactor: 1 });
  await desk.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
  await forceReveal(desk);
  await shot(desk, "desktop-collapsed.png");

  await desk.click("#oh-pvl-open");
  await desk.waitForSelector("#oh-pvl[data-state='expanded']", {
    timeout: 10000,
  });
  await new Promise((r) => setTimeout(r, 800));
  await shot(desk, "desktop-expanded.png");
  await desk.close();

  // Mobile collapsed + modal
  const mob = await browser.newPage();
  await mob.setViewport({ width: 390, height: 844, deviceScaleFactor: 2 });
  await mob.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
  await forceReveal(mob);
  await shot(mob, "mobile-collapsed.png");

  await mob.click("#oh-pvl-open");
  await mob.waitForSelector("#oh-pvl-modal.is-open", { timeout: 10000 });
  await new Promise((r) => setTimeout(r, 800));
  await shot(mob, "mobile-expanded.png");
  await mob.close();

  // Copy check
  const check = await browser.newPage();
  await check.setViewport({ width: 1440, height: 900 });
  await check.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
  await forceReveal(check);
  const meta = await check.evaluate(() => {
    const title = document.querySelector(".oh-pvl-title")?.textContent || "";
    const sub = document.querySelector(".oh-pvl-sub")?.textContent || "";
    const dur = document.querySelector(".oh-pvl-duration")?.textContent || "";
    const w = document.getElementById("oh-pvl")?.getBoundingClientRect().width;
    document.getElementById("oh-pvl-open")?.click();
    return new Promise((resolve) => {
      setTimeout(() => {
        const panel = document.getElementById("oh-pvl-panel");
        const panelW = panel?.getBoundingClientRect().width;
        const panelTitle =
          document.getElementById("oh-pvl-panel-title")?.textContent || "";
        const hasTrack = !!document.querySelector("#oh-pvl-player track");
        const video = document.getElementById("oh-pvl-player");
        resolve({
          title,
          sub,
          dur,
          collapsedWidth: Math.round(w || 0),
          panelWidth: Math.round(panelW || 0),
          panelTitle,
          hasTrack,
          videoMounted: !!video,
          state: document.getElementById("oh-pvl")?.getAttribute("data-state"),
        });
      }, 500);
    });
  });
  fs.writeFileSync(
    path.join(outDir, "qa-check.json"),
    JSON.stringify(meta, null, 2)
  );
  console.log(meta);
  await check.close();
} finally {
  await browser.close();
}
