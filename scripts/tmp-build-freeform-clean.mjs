import fs from "fs";

let css = fs.readFileSync("public/marketing/dealality-old-home-premium.css", "utf8");
css = css
  .replace(/\/\*[\s\S]*?\*\//g, "")
  .replace(/\s+/g, " ")
  .replace(/\s*([{}:;,])\s*/g, "$1")
  .trim();

const head = [
  '<link rel="preconnect" href="https://fonts.googleapis.com">',
  '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>',
  '<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">',
  `<style>${css}</style>`,
].join("");

const foot =
  '<script>(function(){var b=document.getElementById("nmenu");var m=document.getElementById("mnav");if(!b||!m)return;b.addEventListener("click",function(e){e.preventDefault();var open=m.hasAttribute("hidden");if(open){m.removeAttribute("hidden");b.setAttribute("aria-expanded","true");}else{m.setAttribute("hidden","");b.setAttribute("aria-expanded","false");}});})();</script>';

fs.writeFileSync("tmp-premium-head.html", head);
fs.writeFileSync("tmp-premium-foot.html", foot);
fs.writeFileSync(
  "tmp-freeform-payload.json",
  JSON.stringify({ head, foot })
);
console.log({ head: head.length, foot: foot.length, cssOk: !css.includes("}}@keyframes") });
