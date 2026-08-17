const sharp = require("sharp");
const path = require("path");
const fs = require("fs");

const src = path.join(
  "public",
  "marketing",
  "assets",
  "globe-texture-3.jpg"
);
const dest = path.join(
  "public",
  "marketing",
  "assets",
  "dealality-globe-texture.jpg"
);

// Dealality palette targets
// oceans ~ #080F25, land highlights ~ #6C72FF / #8B90FF, mid ~ #343259
(async () => {
  const img = sharp(src);
  const { data, info } = await img
    .ensureAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });

  const out = Buffer.alloc(data.length);
  for (let i = 0; i < data.length; i += 4) {
    const r = data[i];
    const g = data[i + 1];
    const b = data[i + 2];
    const a = data[i + 3];
    const lum = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
    const sat = Math.max(r, g, b) - Math.min(r, g, b);

    // Dark / ocean → near #080F25
    // Mid/bright land → purple-cyan Dealality
    let nr, ng, nb;
    if (lum < 0.22) {
      nr = 8 + lum * 40;
      ng = 15 + lum * 50;
      nb = 37 + lum * 70;
    } else if (lum < 0.45) {
      // deep purple land
      const t = (lum - 0.22) / 0.23;
      nr = 52 + t * 40; // ~#343259 → #6C72FF-ish
      ng = 50 + t * 55;
      nb = 89 + t * 90;
    } else {
      // bright cyan-lavender highlights
      const t = Math.min(1, (lum - 0.45) / 0.55);
      nr = 108 + t * 50;
      ng = 114 + t * 70;
      nb = 255 - t * 20;
      // slight desat boost toward brand blue
      if (sat < 40) {
        nr = nr * 0.85 + 108 * 0.15;
        ng = ng * 0.85 + 114 * 0.15;
        nb = nb * 0.7 + 255 * 0.3;
      }
    }

    out[i] = Math.max(0, Math.min(255, Math.round(nr)));
    out[i + 1] = Math.max(0, Math.min(255, Math.round(ng)));
    out[i + 2] = Math.max(0, Math.min(255, Math.round(nb)));
    out[i + 3] = a;
  }

  await sharp(out, {
    raw: { width: info.width, height: info.height, channels: 4 },
  })
    .jpeg({ quality: 82, mozjpeg: true })
    .toFile(dest);

  const st = fs.statSync(dest);
  console.log("wrote", dest, st.size, info.width, "x", info.height);
})();
