import fs from "fs";
const h = fs.readFileSync("tmp-old-home-live-now.html", "utf8");
const marker = "<!-- Google Tag Manager (noscript) -->";
const i = h.lastIndexOf(marker);
if (i < 0) {
  console.log("marker not found");
  process.exit(1);
}
const end = h.indexOf("</body>", i);
const footer = h.slice(i, end);
fs.writeFileSync("tmp-restore-site-footer.html", footer);
console.log("chars", footer.length);
console.log(footer.slice(0, 200));
console.log("---tail---");
console.log(footer.slice(-300));
