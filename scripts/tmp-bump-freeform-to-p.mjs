import fs from "fs";

const headPath = "tmp-old-home-freeform-head-patched.txt";
let head = fs.readFileSync(headPath, "utf8");
const oldCss = "6a689b2648471629f7f717af_dealality-old-home-dark.v20260728o.css";
const newCss = "6a689e5d959f53c9070f6a53_dealality-old-home-dark.v20260728p.css";
if (!head.includes(oldCss)) {
  // already on another version — try n or current
  const m = head.match(/dealality-old-home-dark\.v20260728[a-z]\.css/);
  if (!m) throw new Error("css ref not found");
  head = head.replace(m[0], "dealality-old-home-dark.v20260728p.css");
  head = head.replace(/6a689[a-z0-9]+_dealality-old-home-dark\.v20260728p\.css/, newCss);
} else {
  head = head.split(oldCss).join(newCss);
}
fs.writeFileSync("tmp-old-home-freeform-head-p.txt", head);
fs.writeFileSync("tmp-old-home-freeform-head-p.json", JSON.stringify({ content: head }));
console.log(JSON.stringify({ length: head.length, hasNew: head.includes(newCss) }));
