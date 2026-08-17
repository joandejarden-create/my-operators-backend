import fs from "fs";
import crypto from "crypto";

const src = "tmp-old-home-dark.v20260728p.css";
const out = "tmp-old-home-dark.v20260728q.css";
let css = fs.readFileSync(src, "utf8");

const replacements = [
  [
    `#insights-grid{order:1;display:flex;gap:2.5rem;overflow-x:auto;overflow-y:hidden;scroll-snap-type:x mandatory;scroll-behavior:smooth;-webkit-overflow-scrolling:touch;padding:0 0 1.5rem;scrollbar-width:thin;scrollbar-color:#6C72FF rgba(255,255,255,.08)}`,
    `#insights-grid{order:1;display:flex;gap:2.5rem;overflow-x:auto;overflow-y:hidden;scroll-snap-type:x mandatory;scroll-behavior:smooth;-webkit-overflow-scrolling:touch;padding:0 0 .75rem;scrollbar-width:none;-ms-overflow-style:none}`,
  ],
  [
    `#insights-controls{order:2;display:flex;justify-content:center;align-items:center;gap:.85rem;margin:1.85rem 0 0;padding:0}`,
    `#insights-controls{order:2;display:flex;justify-content:center;align-items:center;gap:.85rem;margin:1.5rem 0 0;padding:0}`,
  ],
  [
    `#insights-prev,#insights-next{width:46px;height:46px;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.04);color:#fff;font-size:1.45rem;line-height:1;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;text-decoration:none!important;transition:background .25s ease,border-color .25s ease,opacity .25s ease,transform .25s ease,box-shadow .25s ease}`,
    `#insights-prev,#insights-next{width:46px;height:46px;border-radius:999px;border:1px solid rgba(108,114,255,.55);background:rgba(108,114,255,.16);color:#fff;font-size:1.45rem;line-height:1;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;text-decoration:none!important;box-shadow:0 0 0 1px rgba(108,114,255,.12),0 8px 22px rgba(108,114,255,.14);transition:background .25s ease,border-color .25s ease,opacity .25s ease,transform .25s ease,box-shadow .25s ease,color .25s ease,filter .25s ease}`,
  ],
  [
    `#insights-prev:hover,#insights-next:hover{background:rgba(108,114,255,.18);border-color:rgba(108,114,255,.45);color:#fff;transform:translateY(-1px);box-shadow:0 8px 22px rgba(108,114,255,.18)}`,
    `#insights-prev:hover,#insights-next:hover{background:rgba(108,114,255,.28);border-color:#6C72FF;color:#fff;transform:translateY(-1px);box-shadow:0 10px 26px rgba(108,114,255,.22);filter:brightness(1.06)}`,
  ],
  [
    `#insights-prev.is-disabled,#insights-next.is-disabled,#insights-prev[aria-disabled="true"],#insights-next[aria-disabled="true"]{opacity:.35;cursor:default;pointer-events:none}`,
    `#insights-prev.is-disabled,#insights-next.is-disabled,#insights-prev[aria-disabled="true"],#insights-next[aria-disabled="true"]{opacity:1;cursor:default;pointer-events:none;transform:none;filter:none;color:rgba(255,255,255,.28);border-color:rgba(255,255,255,.1);background:rgba(255,255,255,.03);box-shadow:none}`,
  ],
  [
    `#insights-grid::-webkit-scrollbar{height:6px}`,
    `#insights-grid::-webkit-scrollbar{display:none;width:0;height:0}`,
  ],
];

const missing = [];
for (const [from, to] of replacements) {
  if (!css.includes(from)) missing.push(from.slice(0, 100));
  else css = css.split(from).join(to);
}

// Drop leftover track/thumb rules if still present
css = css.replace(
  /#insights-grid::-webkit-scrollbar-track\{[^}]+\}/g,
  ""
);
css = css.replace(
  /#insights-grid::-webkit-scrollbar-thumb\{[^}]+\}/g,
  ""
);

fs.writeFileSync(out, css);
const hash = crypto.createHash("md5").update(fs.readFileSync(out)).digest("hex");
console.log(JSON.stringify({ out, bytes: css.length, hash, missingCount: missing.length, missing }));
