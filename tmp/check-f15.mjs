import fs from "fs";
const code = fs.readFileSync("docs/_dmp_embed_inline.html", "utf8");
console.log(JSON.stringify({
  f15: code.includes("v20260801f15.css"),
  f14: code.includes("v20260801f14.css"),
  chars: code.length,
}));
