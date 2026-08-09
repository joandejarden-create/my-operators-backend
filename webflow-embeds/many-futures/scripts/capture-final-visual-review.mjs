#!/usr/bin/env node
/**
 * Final Phase B visual-review package (operator Smart Matching correction).
 * Writes to /opt/cursor/artifacts and repo visual-review/ for PR visibility.
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
const outDir = "/opt/cursor/artifacts/many-futures/phase-b-final-review";
const repoOut = join(root, "visual-review");
mkdirSync(outDir, { recursive: true });
mkdirSync(repoOut, { recursive: true });

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

async function save(page, name) {
  const path = join(outDir, name);
  await page.screenshot({ path, fullPage: true });
  console.log("wrote", name);
}

{
  const page = await openPage(1440, 1100);
  await activate(page, "rebrand");
  await save(page, "01-desktop-1440-q1-rebrand.png");
  await activate(page, "operators");
  await save(page, "02-desktop-1440-q2-operators.png");
  await activate(page, "proposals");
  await save(page, "03-desktop-1440-q5-proposals.png");
  await page.close();
}

{
  const page = await openPage(768, 1024);
  await activate(page, "rebrand");
  await save(page, "04-tablet-768-q1-rebrand.png");
  await page.close();
}

{
  const page = await openPage(390, 900);
  await activate(page, "rebrand");
  await save(page, "05-mobile-390-q1-rebrand.png");
  await activate(page, "operators");
  await save(page, "06-mobile-390-q2-operators.png");
  await activate(page, "proposals");
  await save(page, "07-mobile-390-q5-proposals.png");
  await page.close();
}

const desktopStates = [];
const mobileStates = [];

{
  const page = await openPage(1440, 1100);
  for (const [id, label] of questions) {
    await activate(page, id);
    const path = join(outDir, `_state-desktop-${id}.png`);
    await page.screenshot({ path, fullPage: true });
    desktopStates.push({ path, label });
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
  }
  await page.close();
}

await browser.close();
server.close();

writeFileSync(
  join(outDir, "_contact-sheet-manifest.json"),
  JSON.stringify({ desktopStates, mobileStates }, null, 2)
);

const py = `
from PIL import Image, ImageDraw, ImageFont
import json, os, shutil

out = ${JSON.stringify(outDir)}
repo = ${JSON.stringify(repoOut)}
features = ${JSON.stringify(join(root, "assets", "features"))}
hotels = ${JSON.stringify(join(root, "assets", "hotel-candidates"))}
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

def make_sheet(items, title, outfile, cols=3, cell_w=640, label_h=42, pad=18, bg=(12, 18, 36), max_h=900):
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
    draw.text((pad, 16), title, font=font(20, True), fill=(255, 255, 255))
    draw.text((pad, 40), "Review only — not for Webflow", font=font(12), fill=(160, 170, 200))
    for i, (im, label) in enumerate(imgs):
        r, c = divmod(i, cols)
        x = pad + c * (cell_w + pad)
        y = title_h + pad + r * (cell_h + pad)
        draw.rounded_rectangle([x - 4, y - 4, x + cell_w + 4, y + im.height + label_h + 4], 10, fill=(17, 27, 58), outline=(50, 60, 100))
        draw.text((x + 6, y + 8), label, font=font(12, True), fill=(180, 190, 230))
        sheet.paste(im, (x, y + label_h))
    path = os.path.join(out, outfile)
    sheet.save(path, "PNG", optimize=True)
    print("wrote", path)

make_sheet(manifest["desktopStates"], "Many Futures Phase B — Desktop question states (1440)", "08-contact-sheet-desktop-six-states.png", cols=3, cell_w=640)
make_sheet(manifest["mobileStates"], "Many Futures Phase B — Mobile question states (390)", "09-contact-sheet-mobile-six-states.png", cols=3, cell_w=360)

# Close-ups of every simplified reconstruction
recon = [
    "smart-matching-operators-desktop.png",
    "smart-matching-operators-mobile.png",
    "deal-readiness-desktop.png",
    "deal-readiness-mobile.png",
    "clause-library-desktop.png",
    "clause-library-mobile.png",
    "financial-term-library-desktop.png",
    "financial-term-library-mobile.png",
    "submit-proposal-desktop.png",
    "submit-proposal-mobile.png",
]
for name in recon:
    src = os.path.join(features, name)
    dst = os.path.join(out, "10-recon-" + name)
    shutil.copy2(src, dst)
    print("copied recon", name)

# Hotel candidates for review
for name in (
    "hotel-candidate-1-recommended.jpg",
    "hotel-candidate-2-strong-alt.jpg",
    "hotel-candidate-3-secondary.jpg",
):
    src = os.path.join(hotels, name)
    dst = os.path.join(out, "11-" + name)
    shutil.copy2(src, dst)
    print("copied hotel", name)

# Mirror primary deliverables into repo visual-review/
keep = [f for f in os.listdir(out) if f.endswith((".png", ".jpg")) and not f.startswith("_")]
for f in keep:
    shutil.copy2(os.path.join(out, f), os.path.join(repo, f))
print("mirrored", len(keep), "files to", repo)
`;

const result = spawnSync("python3", ["-c", py], { encoding: "utf8" });
process.stdout.write(result.stdout || "");
process.stderr.write(result.stderr || "");
if (result.status !== 0) process.exit(result.status || 1);
console.log("Done:", outDir);
