import fs from "fs";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const path =
  "C:/Users/joand/.cursor/projects/c-Users-joand-OneDrive-Documents-deal-capture-proxy/assets/c__Users_joand_AppData_Roaming_Cursor_User_workspaceStorage_f9da20dda2b4f4147468be6368bd6411_images_image-674c24ef-2483-490f-8f74-1148093a197c.png";

let sharp;
try {
  sharp = require("sharp");
} catch {
  console.log("no sharp");
  process.exit(1);
}

const meta = await sharp(path).metadata();
console.log(meta);
const { data, info } = await sharp(path).raw().ensureAlpha().toBuffer({
  resolveWithObject: true,
});
const w = info.width;

function sample(x, y) {
  const i = (y * w + x) * 4;
  return [data[i], data[i + 1], data[i + 2]];
}

const counts = new Map();
for (let y = 70; y < 130; y++) {
  for (let x = 30; x < 320; x++) {
    const [r, g, b] = sample(x, y);
    if (r + g + b < 90) continue;
    if (r > 220 && g > 220 && b > 220) continue;
    const key = `${r},${g},${b}`;
    counts.set(key, (counts.get(key) || 0) + 1);
  }
}

[...counts.entries()]
  .sort((a, b) => b[1] - a[1])
  .slice(0, 25)
  .forEach(([k, v]) => console.log(v, k));
