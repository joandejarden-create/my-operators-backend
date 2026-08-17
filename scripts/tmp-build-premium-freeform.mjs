import fs from "fs";

const j = JSON.parse(fs.readFileSync("tmp-premium-min.json", "utf8"));
const head = [
  '<link rel="preconnect" href="https://fonts.googleapis.com">',
  '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>',
  '<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">',
  `<style>${j.css}body{background:#0b1020;margin:0}#dc-page{min-height:100%}</style>`,
].join("");
const foot =
  '<script>(function(){var b=document.getElementById("nmenu");var m=document.getElementById("mnav");if(!b||!m)return;b.addEventListener("click",function(){var o=m.classList.toggle("is-open");m.hidden=!o;b.setAttribute("aria-expanded",o?"true":"false");});})();</script>';
fs.writeFileSync("tmp-premium-head.html", head);
fs.writeFileSync("tmp-premium-foot.html", foot);
fs.writeFileSync("tmp-premium-body.html", j.html);
console.log(head.length, foot.length, j.html.length);
