import fs from "fs";

// Patch Old Home footer freeform carousel nav to be resilient.
// Expects current footer from MCP saved to tmp-old-home-footer-live.html OR we write from stdin.

const footer = fs.readFileSync("tmp-old-home-footer-live.html", "utf8");

const oldCarousel = `function cardStep(){
  if(!track)return 420;
  var card=track.querySelector("article,#ins-1,#ins-2,#ins-3");
  if(!card)return 420;
  var styles=window.getComputedStyle(track);
  var gap=parseFloat(styles.columnGap||styles.gap)||24;
  return Math.round(card.getBoundingClientRect().width+gap);
}
function setNavState(el,disabled){
  if(!el)return;
  el.setAttribute("aria-disabled",disabled?"true":"false");
  el.classList.toggle("is-disabled",disabled);
  if(disabled){el.setAttribute("tabindex","-1");}
  else{el.removeAttribute("tabindex");}
}
function syncInsightsNav(){
  if(!track||!prev||!next)return;
  var max=Math.max(0,track.scrollWidth-track.clientWidth-2);
  setNavState(prev,track.scrollLeft<=2);
  setNavState(next,track.scrollLeft>=max);
}
function scrollInsights(dir,e){
  if(e){e.preventDefault();e.stopPropagation();}
  if(!track)return;
  if(dir<0&&prev&&prev.getAttribute("aria-disabled")==="true")return;
  if(dir>0&&next&&next.getAttribute("aria-disabled")==="true")return;
  track.scrollBy({left:dir*cardStep(),behavior:"smooth"});
}
if(track&&prev&&next){
  prev.addEventListener("click",function(e){scrollInsights(-1,e);});
  next.addEventListener("click",function(e){scrollInsights(1,e);});
  track.addEventListener("scroll",syncInsightsNav,{passive:true});
  window.addEventListener("resize",syncInsightsNav);
  syncInsightsNav();
}`;

const newCarousel = `function cardStep(){
  if(!track)return 420;
  var card=track.querySelector("article,#ins-1,#ins-2,#ins-3");
  if(!card)return 420;
  var styles=window.getComputedStyle(track);
  var gap=parseFloat(styles.columnGap||styles.gap)||24;
  return Math.max(280,Math.round(card.getBoundingClientRect().width+gap));
}
function scrollMax(){
  if(!track)return 0;
  return Math.max(0,track.scrollWidth-track.clientWidth-2);
}
function setNavState(el,disabled){
  if(!el)return;
  el.setAttribute("aria-disabled",disabled?"true":"false");
  el.classList.toggle("is-disabled",disabled);
  if(disabled){el.setAttribute("tabindex","-1");}
  else{el.removeAttribute("tabindex");}
}
function syncInsightsNav(){
  if(!track||!prev||!next)return;
  var max=scrollMax();
  // If layout hasn't settled yet, keep next enabled when more cards than one viewport.
  if(max<=0&&track.children&&track.children.length>1){
    setNavState(prev,true);
    setNavState(next,false);
    return;
  }
  setNavState(prev,track.scrollLeft<=2);
  setNavState(next,track.scrollLeft>=max);
}
function scrollInsights(dir,e){
  if(e){e.preventDefault();e.stopPropagation();}
  if(!track)return;
  var max=scrollMax();
  if(dir<0&&track.scrollLeft<=2)return;
  if(dir>0&&max>0&&track.scrollLeft>=max)return;
  // Always attempt scroll by card step even if max was briefly 0 after layout.
  var step=cardStep();
  if(max<=0){
    track.scrollBy({left:dir*step,behavior:"smooth"});
    setTimeout(syncInsightsNav,80);
    setTimeout(syncInsightsNav,320);
    return;
  }
  track.scrollBy({left:dir*step,behavior:"smooth"});
}
if(track&&prev&&next){
  prev.setAttribute("href","#insights");
  next.setAttribute("href","#insights");
  prev.addEventListener("click",function(e){scrollInsights(-1,e);});
  next.addEventListener("click",function(e){scrollInsights(1,e);});
  track.addEventListener("scroll",syncInsightsNav,{passive:true});
  window.addEventListener("resize",syncInsightsNav);
  window.addEventListener("load",syncInsightsNav);
  syncInsightsNav();
  setTimeout(syncInsightsNav,100);
  setTimeout(syncInsightsNav,400);
  setTimeout(syncInsightsNav,1000);
  if(typeof ResizeObserver==="function"){
    try{new ResizeObserver(function(){syncInsightsNav();}).observe(track);}catch(err){}
  }
}`;

if (!footer.includes(oldCarousel)) {
  console.error("old carousel block not found");
  process.exit(1);
}
const next = footer.replace(oldCarousel, newCarousel);
fs.writeFileSync("tmp-old-home-footer-live.html", next);
fs.writeFileSync(
  "tmp-old-home-footer-with-cta-reader.json",
  JSON.stringify({ content: next })
);
console.log("patched footer", next.length);
console.log("has scrollMax", next.includes("scrollMax"));
console.log("has ResizeObserver", next.includes("ResizeObserver"));
