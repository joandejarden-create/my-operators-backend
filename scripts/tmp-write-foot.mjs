import fs from "fs";

const foot =
  '<script>(function(){var b=document.getElementById("nmenu");var m=document.getElementById("mnav");if(!b||!m)return;m.classList.remove("is-open");b.setAttribute("aria-expanded","false");b.addEventListener("click",function(e){e.preventDefault();var open=!m.classList.contains("is-open");m.classList.toggle("is-open",open);b.setAttribute("aria-expanded",open?"true":"false");});})();</script>';

fs.writeFileSync("tmp-premium-foot.html", foot);
const head = fs.readFileSync("tmp-premium-head.html", "utf8");
fs.writeFileSync("tmp-freeform-payload.json", JSON.stringify({ head, foot }));
console.log({ head: head.length, foot: foot.length });
