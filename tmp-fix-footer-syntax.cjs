const fs = require("fs");

// Load footer from MCP dump saved by writing the get result
// We'll reconstruct from live bad script + remaining scripts from a template approach

const bad = fs.readFileSync("tmp-bad-script-27.js", "utf8");
// Fix the broken translateY string: "px"); -> "px)";
let fixed = bad.replace(
  'rot.style.transform="translateY("+((c-ri)*h)+"px");',
  'rot.style.transform="translateY("+((c-ri)*h)+"px)";'
);

if (fixed === bad) {
  // try escaped variants
  fixed = bad.replace(
    /rot\.style\.transform="translateY\("\+\(\(c-ri\)\*h\)\+"px"\);/,
    'rot.style.transform="translateY("+((c-ri)*h)+"px)";'
  );
}

if (fixed === bad) throw new Error("fix pattern not found");

// Harden insights carousel: event delegation + re-query track on click
const oldInsightsBlock = `if(track&&prev&&next){
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

const newInsightsBlock = `function bindInsights(){
  track=document.getElementById("insights-grid");
  prev=document.getElementById("insights-prev");
  next=document.getElementById("insights-next");
  if(!track||!prev||!next)return false;
  prev.setAttribute("href","#insights");
  next.setAttribute("href","#insights");
  if(!track.getAttribute("data-oh-ins-bound")){
    track.setAttribute("data-oh-ins-bound","1");
    track.addEventListener("scroll",syncInsightsNav,{passive:true});
    if(typeof ResizeObserver==="function"){
      try{new ResizeObserver(function(){syncInsightsNav();}).observe(track);}catch(err){}
    }
  }
  syncInsightsNav();
  return true;
}
document.addEventListener("click",function(e){
  var t=e.target&&e.target.closest?e.target.closest("#insights-prev,#insights-next"):null;
  if(!t)return;
  e.preventDefault();
  e.stopPropagation();
  bindInsights();
  scrollInsights(t.id==="insights-prev"?-1:1,e);
  setTimeout(syncInsightsNav,80);
  setTimeout(syncInsightsNav,320);
},true);
window.addEventListener("resize",function(){bindInsights();});
window.addEventListener("load",function(){bindInsights();});
bindInsights();
setTimeout(bindInsights,100);
setTimeout(bindInsights,400);
setTimeout(bindInsights,1000);`;

if (!fixed.includes(oldInsightsBlock)) {
  console.log("old insights block not exact — writing fixed syntax only");
} else {
  fixed = fixed.replace(oldInsightsBlock, newInsightsBlock);
}

// Validate syntax
try {
  // eslint-disable-next-line no-new-func
  new Function(fixed);
  console.log("syntax OK");
} catch (e) {
  console.error("still broken", e.message);
  process.exit(1);
}

fs.writeFileSync("tmp-fixed-script-27.js", fixed);

// Build full footer: need other two scripts from MCP.
// Read from live HTML after script 27.
(async () => {
  const https = require("https");
  const html = await new Promise((res, rej) => {
    https
      .get("https://www.dealality.com/old-home", (r) => {
        let d = "";
        r.on("data", (c) => (d += c));
        r.on("end", () => res(d));
      })
      .on("error", rej);
  });
  const scripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)].map(
    (m) => m[1]
  );
  // Find fsw and article reader scripts
  const fsw = scripts.find((s) => s.includes("fsw-email"));
  const reader = scripts.find((s) => s.includes("oh-article-reader"));
  if (!fsw || !reader) throw new Error("missing companion scripts");

  const footer =
    "<script>" +
    fixed +
    "</script>\n<script>" +
    fsw +
    "</script>\n<script>" +
    reader +
    "</script>\n";

  // validate each
  [fixed, fsw, reader].forEach((s, i) => {
    try {
      new Function(s);
    } catch (e) {
      console.error("script", i, e.message);
      process.exit(1);
    }
  });

  fs.writeFileSync("tmp-old-home-footer-fixed.txt", footer);
  console.log("footer bytes", footer.length);
  console.log("has px); broken?", footer.includes('+"px");'));
  console.log("has px); fixed?", footer.includes('+"px)";'));
  console.log("has delegation", footer.includes("data-oh-ins-bound"));
})();
