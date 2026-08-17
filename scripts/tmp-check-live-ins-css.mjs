const cssUrl =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68c0c23946c27237e50206_dealality-old-home-dark.v20260728af.css";
const css = await (await fetch(cssUrl)).text();
for (const key of [
  "#insights-grid",
  "#insights-prev",
  "#ins-1,#ins-2",
  "overflow-x:auto",
  "pointer-events:none",
  "--ins-visible",
]) {
  console.log(key, css.includes(key));
}
const m = css.match(/#insights-grid\{[^}]+\}/);
console.log("grid rule", m && m[0]);
const n = css.match(/#insights-prev,#insights-next\{[^}]+\}/);
console.log("nav rule", n && n[0].slice(0, 300));
const c = css.match(/#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6\{[^}]+\}/);
console.log("card rule", c && c[0].slice(0, 350));
