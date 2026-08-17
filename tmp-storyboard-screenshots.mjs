import { spawn } from "child_process";
import fs from "fs";
import path from "path";

const outDir = path.resolve("tmp-storyboard-shots");
fs.mkdirSync(outDir, { recursive: true });

const candidates = [
  process.env.CHROME_PATH,
  "C:\\\\Program Files\\\\Google\\\\Chrome\\\\Application\\\\chrome.exe",
  "C:\\\\Program Files (x86)\\\\Google\\\\Chrome\\\\Application\\\\chrome.exe",
  "C:\\\\Program Files\\\\Microsoft\\\\Edge\\\\Application\\\\msedge.exe",
].filter(Boolean);

let chrome = null;
for (const c of candidates) {
  if (fs.existsSync(c)) {
    chrome = c;
    break;
  }
}
if (!chrome) {
  console.error("No Chrome/Edge found");
  process.exit(1);
}

  const url = "https://www.dealality.com/old-home?v=sbshotsb" + Date.now();
const html = `<!doctype html><html><body>
<script type="module">
import puppeteer from 'https://esm.sh/puppeteer-core@22.15.0';
</script>
</body></html>`;

// Use CDP via chrome --remote-debugging and a tiny node CDP client would be heavy.
// Prefer puppeteer-core if installed locally.
async function main() {
  let puppeteer;
  try {
    puppeteer = await import("puppeteer-core");
  } catch {
    console.error("puppeteer-core missing");
    process.exit(2);
  }
  const browser = await puppeteer.default.launch({
    executablePath: chrome,
    headless: "new",
    args: ["--window-size=1440,1200", "--force-device-scale-factor=1"],
    defaultViewport: { width: 1440, height: 1100 },
  });
  const page = await browser.newPage();
  await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
  await page.waitForSelector("#about", { timeout: 60000 });
  await page.evaluate(() => {
    try {
      sessionStorage.removeItem("oh_problem_storyboard_played_v1");
    } catch {}
  });
  await page.reload({ waitUntil: "networkidle2", timeout: 120000 });
  await page.waitForSelector(".oh-problem-sb", { timeout: 60000 });
  await page.evaluate(() => {
    document.getElementById("about")?.scrollIntoView({ block: "center" });
  });
  await new Promise((r) => setTimeout(r, 800));

  async function shot(name, frame) {
    await page.evaluate((f) => {
      const sb = document.querySelector(".oh-problem-sb");
      if (!sb) return;
      sb.classList.remove("is-complete", "is-reduced");
      sb.setAttribute("data-frame", String(f));
      const panels = [
        document.getElementById("about-point-1"),
        document.getElementById("about-point-2"),
        document.getElementById("about-point-3"),
      ];
      const map = [0, 1, 1, 2, 3, 3];
      const active = map[f - 1] || 0;
      panels.forEach((el, i) => {
        if (!el) return;
        el.classList.toggle("is-active", i + 1 === active);
        el.classList.toggle("is-done", active > 0 && i + 1 < active);
      });
      if (f === 6) sb.classList.add("is-complete");
    }, frame);
    await new Promise((r) => setTimeout(r, 400));
    const el = await page.$("#about");
    await el.screenshot({ path: path.join(outDir, name) });
    console.log("wrote", name);
  }

  await shot("frame1.png", 1);
  await shot("fragmentation.png", 3);
  await shot("comparison-fail.png", 4);
  await shot("final.png", 6);

  await page.setViewport({ width: 390, height: 900 });
  await page.evaluate(() => {
    document.getElementById("about")?.scrollIntoView({ block: "start" });
  });
  await shot("mobile-final.png", 6);

  await page.setViewport({ width: 1440, height: 1100 });
  await page.emulateMediaFeatures([
    { name: "prefers-reduced-motion", value: "reduce" },
  ]);
  await page.reload({ waitUntil: "networkidle2", timeout: 120000 });
  await page.waitForSelector(".oh-problem-sb", { timeout: 60000 });
  await page.evaluate(() => {
    document.getElementById("about")?.scrollIntoView({ block: "center" });
  });
  await new Promise((r) => setTimeout(r, 800));
  const el = await page.$("#about");
  await el.screenshot({ path: path.join(outDir, "reduced-motion.png") });
  console.log("wrote reduced-motion.png");

  await browser.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
