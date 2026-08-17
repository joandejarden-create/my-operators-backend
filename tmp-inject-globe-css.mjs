import fs from "fs";

const headPath = "tmp-old-home-head-globe.txt";
let t = fs.readFileSync(headPath, "utf8");
t = t.replace(
  "#hero-globe,#hero-signals{display:none!important}",
  "#hero-signals{display:none!important}"
);

const css = `
/* Hero globe — subtle full-bleed background (not a column) */
#hero,.oh-hero{position:relative!important;overflow:hidden!important}
#hero-globe{position:absolute!important;inset:0!important;z-index:0!important;display:block!important;pointer-events:none!important;overflow:hidden!important;opacity:.5}
#hero-globe-container{position:absolute!important;top:-18%!important;right:-28%!important;bottom:-28%!important;left:8%!important;width:auto!important;height:auto!important}
#oh-globe-canvas{display:block!important;width:100%!important;height:100%!important}
#hero-globe-list{display:none!important}
#hero::after{content:""!important;position:absolute!important;inset:0!important;z-index:0!important;pointer-events:none!important;background:radial-gradient(ellipse 70% 80% at 18% 42%,rgba(8,15,37,.88) 0%,rgba(8,15,37,.55) 42%,rgba(8,15,37,.22) 68%,rgba(8,15,37,.08) 100%),linear-gradient(90deg,rgba(8,15,37,.72) 0%,rgba(8,15,37,.28) 48%,rgba(8,15,37,.12) 100%)}
#hero-inner{position:relative!important;z-index:1!important}
@media(max-width:960px){
#hero-globe{opacity:.34}
#hero-globe-container{top:-8%!important;right:-40%!important;bottom:-22%!important;left:-10%!important}
#hero::after{background:radial-gradient(ellipse 90% 70% at 50% 30%,rgba(8,15,37,.82) 0%,rgba(8,15,37,.55) 50%,rgba(8,15,37,.28) 100%)}
}
@media(prefers-reduced-motion:reduce){#hero-globe{opacity:.28}}
`;

const marker = "/* Hero — copy left / video poster right */";
if (!t.includes("Hero globe — subtle")) {
  t = t.replace(marker, css + "\n" + marker);
}
fs.writeFileSync(headPath, t);
console.log({
  hasGlobeCss: t.includes("Hero globe — subtle"),
  hiddenBoth: t.includes("#hero-globe,#hero-signals{display:none"),
});

// Local premium CSS
const cssPath = "public/marketing/dealality-old-home-premium.css";
let c = fs.readFileSync(cssPath, "utf8");
c = c.replace(
  "#hero-globe,#hero-signals{display:none!important}",
  "#hero-signals{display:none!important}"
);
if (!c.includes("Hero globe — subtle")) {
  c = c.replace(
    "/* Hero — copy left / video poster right */",
    css.replace(/!important/g, "") + "\n/* Hero — copy left / video poster right */"
  );
  // keep important on local for consistency with ID overrides — re-apply with importants
  // simpler: insert the same important block
}
fs.writeFileSync(cssPath, c);

// Ensure local CSS has the important version
let c2 = fs.readFileSync(cssPath, "utf8");
if (!c2.includes("#hero-globe{position:absolute")) {
  c2 = c2.replace(
    "/* Hero — copy left / video poster right */",
    css + "\n/* Hero — copy left / video poster right */"
  );
  fs.writeFileSync(cssPath, c2);
}
console.log("local", fs.readFileSync(cssPath, "utf8").includes("#hero-globe{position:absolute"));
