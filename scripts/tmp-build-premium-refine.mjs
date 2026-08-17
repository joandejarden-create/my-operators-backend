import fs from "fs";

let html = fs.readFileSync("public/marketing/dealality-old-home-premium.html", "utf8");
let css = fs.readFileSync("public/marketing/dealality-old-home-premium.css", "utf8");

html = html.replace(/\r\n/g, "\n").replace(/>\s+</g, "><").trim();
css = css
  .replace(/\/\*[\s\S]*?\*\//g, "")
  .replace(/\s+/g, " ")
  .replace(/\s*([{}:;,])\s*/g, "$1")
  .trim();

const wrapped = `<div id="dc-premium">${html}</div>`;

const head = [
  '<link rel="preconnect" href="https://fonts.googleapis.com">',
  '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>',
  '<link href="https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">',
  `<style>${css}html,body{height:auto!important;min-height:0!important;margin:0;padding:0;background:#080F25}#dc-page{height:auto!important;min-height:0!important;background:#080F25}</style>`,
].join("");

const foot = `<script>(function(){
var b=document.getElementById("nmenu");
var m=document.getElementById("mnav");
if(b&&m){
  b.addEventListener("click",function(e){
    e.preventDefault();
    var open=m.classList.toggle("is-open");
    b.setAttribute("aria-expanded",open?"true":"false");
    if(open){m.removeAttribute("hidden");}else{m.setAttribute("hidden","");}
  });
  m.querySelectorAll("a").forEach(function(a){
    a.addEventListener("click",function(){
      m.classList.remove("is-open");
      m.setAttribute("hidden","");
      b.setAttribute("aria-expanded","false");
    });
  });
}
var tabs=document.querySelectorAll("#features-tabs a, #features-tabs button");
var panels=["fpanel-1","fpanel-2","fpanel-3","fpanel-4","fpanel-5"];
function showPanel(id){
  panels.forEach(function(pid){
    var el=document.getElementById(pid);
    if(!el)return;
    if(pid===id){el.removeAttribute("hidden");}
    else{el.setAttribute("hidden","");}
  });
}
tabs.forEach(function(tab){
  tab.addEventListener("click",function(e){
    e.preventDefault();
    tabs.forEach(function(t){t.setAttribute("aria-selected","false");});
    tab.setAttribute("aria-selected","true");
    showPanel(tab.getAttribute("data-panel"));
  });
});
var track=document.getElementById("insights-grid");
var prev=document.getElementById("insights-prev");
var next=document.getElementById("insights-next");
function cardStep(){
  var card=track&&track.querySelector("article");
  if(!card)return 360;
  var styles=window.getComputedStyle(track);
  var gap=parseFloat(styles.columnGap||styles.gap)||24;
  return card.getBoundingClientRect().width+gap;
}
function syncInsightsNav(){
  if(!track||!prev||!next)return;
  var max=Math.max(0,track.scrollWidth-track.clientWidth-2);
  prev.disabled=track.scrollLeft<=2;
  next.disabled=track.scrollLeft>=max;
}
if(track&&prev&&next){
  prev.addEventListener("click",function(){track.scrollBy({left:-cardStep(),behavior:"smooth"});});
  next.addEventListener("click",function(){track.scrollBy({left:cardStep(),behavior:"smooth"});});
  track.addEventListener("scroll",syncInsightsNav,{passive:true});
  window.addEventListener("resize",syncInsightsNav);
  syncInsightsNav();
}
})();</script>`;

fs.writeFileSync("tmp-premium-min.json", JSON.stringify({ html: wrapped, css }));
fs.writeFileSync("tmp-premium-head.html", head);
fs.writeFileSync("tmp-premium-foot.html", foot);
fs.writeFileSync("tmp-premium-body.html", wrapped);

console.log(
  JSON.stringify({
    html: html.length,
    css: css.length,
    wrapped: wrapped.length,
    head: head.length,
    foot: foot.length,
  })
);
