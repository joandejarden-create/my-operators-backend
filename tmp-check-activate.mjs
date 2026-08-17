import fs from "fs";

const h = fs.readFileSync("tmp-old-home-live2.html", "utf8");
const start = h.indexOf('var dot1=document.getElementById("modules-dot-1")');
console.log(h.slice(start, start + 1200));

// Check freeform head CSS for anything hiding tabs or panels
const css = fs.readFileSync(
  "public/marketing/dealality-old-home-freeform-head.v20260729w15.css",
  "utf8"
);
const lines = css.split("\n");
for (let i = 0; i < lines.length; i++) {
  if (/modules-tab|modules-panel|modules-badge|modules-dot|aria-hidden|\[hidden\]/.test(lines[i])) {
    console.log(i + 1, lines[i].slice(0, 200));
  }
}
