import fs from "fs";
import os from "os";
const html = fs.readFileSync(os.tmpdir() + "/old-home.html", "utf8");
const m = [...html.matchAll(/testimonials[^"'<\s]*/gi)].map((x) => x[0]);
console.log("matches", [...new Set(m)]);
console.log("bootguard", /BootGuard|asset-boot|testimonials\.v2026/i.test(html));
const idx = html.indexOf("oldhomebootguard");
console.log("idx", idx);
if (idx >= 0) console.log(html.slice(idx, idx + 200));
const hosted = [...html.matchAll(/cdn\.prod\.website-files\.com\/68108c29063eeb5d1bd7ae4a\/[^"'\\\s]+/g)].map((x) => x[0]);
console.log(
  "cdn",
  [...new Set(hosted)].filter((u) => /testimonial|asset-boot|bootguard|w19/i.test(u))
);
