import fs from "fs";

// Rebuild body with HTML (not SVG) hero visual — WHTML-friendly
let html = fs.readFileSync("public/marketing/dealality-old-home-premium.html", "utf8");
html = html.replace(/\r\n/g, "\n").replace(/>\s+</g, "><").trim();

// Replace SVG block with HTML branching system (IDs preserved for CSS)
const htmlVisual = `<aside id="hero-visual" aria-label="Dealality decision path visualization"><div id="hero-svg" role="img" aria-label="One hotel opportunity branches into strategic paths, then converges through structured evaluation into one informed decision"><p id="hv-label">OWNER DECISION PATH</p><div id="hv-s1">One hotel opportunity</div><div id="hv-branch" aria-hidden="true"><span id="hv-b1"></span><span id="hv-b2"></span><span id="hv-b3"></span></div><div id="hv-s2"><span id="hv-p1">Brand path</span><span id="hv-p2">Operator path</span><span id="hv-p3">Conversion path</span></div><div id="hv-merge" aria-hidden="true"><span id="hv-m1"></span><span id="hv-m2"></span><span id="hv-m3"></span></div><div id="hv-s3">Structured evaluation</div><div id="hv-stem" aria-hidden="true"></div><div id="hv-s4">One informed decision</div></div></aside>`;

html = html.replace(/<aside id="hero-visual"[\s\S]*?<\/aside>/, htmlVisual);

const wrapped = `<div id="dc-premium">${html}</div>`;
fs.writeFileSync("tmp-premium-body.html", wrapped);

let css = fs.readFileSync("public/marketing/dealality-old-home-premium.css", "utf8");
// Append HTML hero visual styles (replace SVG-oriented rules)
const heroVisualCss = `
#hero-svg{display:flex;flex-direction:column;align-items:center;gap:.55rem;padding:1.25rem 1.1rem 1.4rem;border-radius:18px;background:#121a2e;border:1px solid rgba(255,255,255,.08);min-height:320px}
#hv-label{margin:0 0 .35rem;align-self:flex-start;font-size:.68rem;letter-spacing:.14em;color:rgba(232,236,245,.45);font-weight:600}
#hv-s1,#hv-s3{padding:.7rem 1rem;border-radius:10px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.12);font-size:.82rem;font-weight:600;text-align:center;color:#e8ecf5;opacity:0;animation:hv-fade .5s ease forwards}
#hv-s1{animation-delay:.1s}
#hv-s3{animation-delay:.95s;background:rgba(196,165,116,.08);border-color:rgba(196,165,116,.4);width:min(100%,260px)}
#hv-s2{display:grid;grid-template-columns:repeat(3,1fr);gap:.45rem;width:100%;opacity:0;animation:hv-fade .55s ease .45s forwards}
#hv-p1,#hv-p2,#hv-p3{display:flex;align-items:center;justify-content:center;padding:.65rem .4rem;border-radius:9px;background:rgba(255,255,255,.035);border:1px solid rgba(196,165,116,.35);font-size:.72rem;text-align:center;color:#e8ecf5;line-height:1.25}
#hv-branch,#hv-merge{display:grid;grid-template-columns:repeat(3,1fr);width:100%;height:18px;opacity:0;animation:hv-fade .45s ease .35s forwards}
#hv-merge{animation-delay:.8s}
#hv-b1,#hv-b2,#hv-b3,#hv-m1,#hv-m2,#hv-m3{position:relative}
#hv-b1::before,#hv-b2::before,#hv-b3::before,#hv-m1::before,#hv-m2::before,#hv-m3::before{content:"";position:absolute;left:50%;top:0;width:1px;height:100%;background:linear-gradient(180deg,#c4a574,rgba(232,236,245,.35))}
#hv-stem{width:1px;height:18px;background:linear-gradient(180deg,#c4a574,rgba(232,236,245,.4));opacity:0;animation:hv-fade .45s ease 1.25s forwards}
#hv-s4{padding:.65rem 1.2rem;border-radius:999px;background:#c4a574;color:#0b1020;font-size:.82rem;font-weight:700;opacity:0;animation:hv-fade .55s ease 1.4s forwards}
@media (prefers-reduced-motion:reduce){#hv-s1,#hv-s2,#hv-s3,#hv-s4,#hv-branch,#hv-merge,#hv-stem{animation:none!important;opacity:1!important}}
`;

// Remove old SVG path animation rules that no longer apply cleanly
css = css
  .replace(/#hero-svg\{[^}]*\}/, "")
  .replace(/#hv-paths-out path[\s\S]*?#hv-path-final path\{[^}]*\}/, "")
  .replace(/#hv-s1\{[^}]*\}/, "")
  .replace(/#hv-s2,#hv-paths-out\{[^}]*\}/, "")
  .replace(/#hv-paths-out path\{[^}]*\}/, "")
  .replace(/#hv-s3,#hv-paths-in\{[^}]*\}/, "")
  .replace(/#hv-paths-in path\{[^}]*\}/, "")
  .replace(/#hv-s4,#hv-path-final\{[^}]*\}/, "")
  .replace(/#hv-path-final path\{[^}]*\}/, "")
  .replace(/@keyframes hv-draw\{[^}]*\}/, "")
  .replace(/@media \(prefers-reduced-motion:reduce\)\{[^}]*\}/, "");

css = css + heroVisualCss;

const minCss = css
  .replace(/\/\*[\s\S]*?\*\//g, "")
  .replace(/\s+/g, " ")
  .replace(/\s*([{}:;,])\s*/g, "$1")
  .trim();

const head = [
  '<link rel="preconnect" href="https://fonts.googleapis.com">',
  '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>',
  '<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">',
  `<style>${minCss}html,body{height:auto!important;min-height:0!important;margin:0;padding:0;background:#0b1020}#dc-page{height:auto!important;min-height:0!important}</style>`,
].join("");

const foot =
  '<script>(function(){var b=document.getElementById("nmenu");var m=document.getElementById("mnav");if(!b||!m)return;b.addEventListener("click",function(e){e.preventDefault();var open=m.hasAttribute("hidden");if(open){m.removeAttribute("hidden");b.setAttribute("aria-expanded","true");}else{m.setAttribute("hidden","");b.setAttribute("aria-expanded","false");}});})();</script>';

fs.writeFileSync("public/marketing/dealality-old-home-premium.html", html);
fs.writeFileSync("public/marketing/dealality-old-home-premium.css", css);
fs.writeFileSync("tmp-premium-body.html", wrapped);
fs.writeFileSync("tmp-premium-head.html", head);
fs.writeFileSync("tmp-premium-foot.html", foot);
console.log({ body: wrapped.length, head: head.length, foot: foot.length });
