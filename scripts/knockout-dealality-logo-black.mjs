/**
 * dealality-logo.png ships with a solid black matte. Knock out dark pixels to alpha
 * so the logo sits on the navy hero without a rectangle. Backs up once to
 * dealality-logo-baked-original.png (only created if missing).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import sharp from "sharp";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const target = path.join(root, "public", "assets", "dealality-logo.png");
const backup = path.join(root, "public", "assets", "dealality-logo-baked-original.png");

const SUM_THRESH = Number(process.env.LOGO_KNOCKOUT_SUM ?? "135");

if (!fs.existsSync(target)) {
  console.error("Missing file:", target);
  process.exit(1);
}
if (!fs.existsSync(backup)) {
  fs.copyFileSync(target, backup);
  console.log("Backed up original to", backup);
}

const { data, info } = await sharp(target).ensureAlpha().raw().toBuffer({ resolveWithObject: true });
const { width, height, channels } = info;
if (channels !== 4) {
  throw new Error(`Expected RGBA, got ${channels} channels`);
}

const out = Buffer.from(data);
for (let i = 0; i < out.length; i += 4) {
  const r = out[i];
  const g = out[i + 1];
  const b = out[i + 2];
  if (r + g + b < SUM_THRESH) {
    out[i + 3] = 0;
  }
}

const tmp = target + ".tmp.png";
await sharp(out, { raw: { width, height, channels: 4 } })
  .png({ compressionLevel: 9 })
  .toFile(tmp);
fs.renameSync(tmp, target);
console.log("Wrote transparent logo:", target, `(sum < ${SUM_THRESH} -> alpha 0)`);
