#!/usr/bin/env node
/**
 * Phase B visual-review package:
 * primary breakpoint shots + desktop/mobile six-state contact sheets.
 */
import { chromium } from "playwright";
import { createServer } from "http";
import {
  readFileSync,
  existsSync,
  mkdirSync,
  statSync,
  writeFileSync,
} from "fs";
import { join, extname, dirname } from "path";
import { fileURLToPath } from "url";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
// Prefer system sharp/PIL via python for contact sheets if playwright-only
const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const outDir = "/opt/cursor/artifacts/many-futures/phase-b-visual-review";
mkdirSync(outDir, { recursive: true });

const mime = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
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

const browser = await chromium.launch({
  executablePath: "/usr/local/bin/google-chrome",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});

const questions = [
  ["rebrand", "01 — Should I rebrand?"],
  ["operators", "02 — Which operators genuinely fit this hotel?"],
  ["affiliation", "03 — Independent or affiliated?"],
  ["residences", "04 — Could branded residences strengthen the project?"],
  ["proposals", "05 — Which proposal creates the strongest owner value?"],
  ["clarify", "06 — What should I clarify before committing?"],
];

async function openPage(width, height) {
  const page = await browser.newPage({
    viewport: { width, height },
    deviceScaleFactor: 1,
  });
  await page.goto(base, { waitUntil: "networkidle" });
  await page.waitForSelector("#dealality-many-futures.mf-js-ready");
  await page.waitForTimeout(350);
  return page;
}

async function activate(page, id) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(400);
}

// Primary required screenshots
{
  const page = await openPage(1440, 1100);
  await activate(page, "rebrand");
  await page.screenshot({
    path: join(outDir, "01-desktop-1440-q1-rebrand.png"),
    fullPage: true,
  });
  await activate(page, "operators");
  await page.screenshot({
    path: join(outDir, "02-desktop-1440-q2-operators.png"),
    fullPage: true,
  });
  await activate(page, "proposals");
  await page.screenshot({
    path: join(outDir, "03-desktop-1440-q5-proposals.png"),
    fullPage: true,
  });
  await page.close();
}

{
  const page = await openPage(768, 1024);
  await activate(page, "rebrand");
  await page.screenshot({
    path: join(outDir, "04-tablet-768-q1-rebrand.png"),
    fullPage: true,
  });
  await page.close();
}

{
  const page = await openPage(390, 900);
  await activate(page, "rebrand");
  await page.screenshot({
    path: join(outDir, "05-mobile-390-q1-rebrand.png"),
    fullPage: true,
  });
  await activate(page, "proposals");
  await page.screenshot({
    path: join(outDir, "06-mobile-390-q5-proposals.png"),
    fullPage: true,
  });
  await page.close();
}

// Capture all six desktop + mobile states for contact sheets
const desktopStates = [];
const mobileStates = [];

{
  const page = await openPage(1440, 1100);
  for (const [id, label] of questions) {
    await activate(page, id);
    const path = join(outDir, `_state-desktop-${id}.png`);
    await page.screenshot({ path, fullPage: true });
    desktopStates.push({ path, label });
    console.log("desktop state", id);
  }
  await page.close();
}

{
  const page = await openPage(390, 900);
  for (const [id, label] of questions) {
    await activate(page, id);
    const path = join(outDir, `_state-mobile-${id}.png`);
    await page.screenshot({ path, fullPage: true });
    mobileStates.push({ path, label });
    console.log("mobile state", id);
  }
  await page.close();
}

await browser.close();
server.close();

// Build contact sheets with Pillow
writeFileSync(
  join(outDir, "_contact-sheet-manifest.json"),
  JSON.stringify({ desktopStates, mobileStates }, null, 2)
);

const { spawnSync } = await import("child_process");
const py = `
from PIL import Image, ImageDraw, ImageFont
import json, os

out = ${JSON.stringify(outDir)}
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

def make_sheet(items, title, outfile, cols=3, cell_w=720, label_h=42, pad=18, bg=(12, 18, 36)):
    imgs = []
    for it in items:
        im = Image.open(it["path"]).convert("RGB")
        # scale to cell width
        scale = cell_w / im.width
        im = im.resize((cell_w, max(1, int(im.height * scale))), Image.Resampling.LANCZOS)
        # cap height so sheets stay manageable
        max_h = 900
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
    draw.text((pad, 16), title, font=font(22, True), fill=(255, 255, 255))
    draw.text((pad, 40), "Review only — not for Webflow", font=font(12), fill=(160, 170, 200))

    for i, (im, label) in enumerate(imgs):
        r, c = divmod(i, cols)
        x = pad + c * (cell_w + pad)
        y = title_h + pad + r * (cell_h + pad)
        # card
        draw.rounded_rectangle([x - 4, y - 4, x + cell_w + 4, y + im.height + label_h + 4], 10, fill=(17, 27, 58), outline=(50, 60, 100))
        draw.text((x + 6, y + 8), label, font=font(13, True), fill=(180, 190, 230))
        sheet.paste(im, (x, y + label_h))
    path = os.path.join(out, outfile)
    sheet.save(path, "PNG", optimize=True)
    print("wrote", path, sheet.size)

make_sheet(manifest["desktopStates"], "Many Futures Phase B — Desktop question states (1440)", "contact-sheet-desktop-six-states.png", cols=3, cell_w=640)
make_sheet(manifest["mobileStates"], "Many Futures Phase B — Mobile question states (390)", "contact-sheet-mobile-six-states.png", cols=3, cell_w=360)
`;

const result = spawnSync("python3", ["-c", py], { encoding: "utf8" });
process.stdout.write(result.stdout || "");
process.stderr.write(result.stderr || "");
if (result.status !== 0) process.exit(result.status || 1);

console.log("Visual review package:", outDir);
