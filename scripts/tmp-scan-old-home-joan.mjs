import fs from "fs";
import path from "path";
import os from "os";

const htmlPath = path.join(os.tmpdir(), "old-home.html");
const html = fs.readFileSync(htmlPath, "utf8");

const needles = [
  "working both sides",
  "nearly 30 years",
  "Joan Dejarden is the Founder",
  "oh-testimonial-bio",
  "testimonials.v2026",
  "asset-boot",
  "quote-tiles",
  "setInterval",
];

for (const n of needles) {
  const i = html.indexOf(n);
  console.log(n, i);
  if (i >= 0) console.log(html.slice(Math.max(0, i - 60), i + 220).replace(/\s+/g, " "));
}

const scripts = [...html.matchAll(/src=("([^"]+)"|'([^']+)')/gi)]
  .map((m) => m[2] || m[3])
  .filter((u) => /testimonial|asset-boot|quote-tiles|old-home/i.test(u));
console.log("SCRIPTS", [...new Set(scripts)]);
