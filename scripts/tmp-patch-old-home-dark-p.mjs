import fs from "fs";
import crypto from "crypto";

const src = "tmp-old-home-dark.v20260728o.css";
const out = "tmp-old-home-dark.v20260728p.css";
let css = fs.readFileSync(src, "utf8");

const replacements = [
  [
    `#insights-grid{order:1;display:flex;gap:1.5rem;overflow-x:auto;overflow-y:hidden;scroll-snap-type:x mandatory;scroll-behavior:smooth;-webkit-overflow-scrolling:touch;padding:0 0 .35rem;scrollbar-width:thin;scrollbar-color:#6C72FF rgba(255,255,255,.08)}`,
    `#insights-grid{order:1;display:flex;gap:2.5rem;overflow-x:auto;overflow-y:hidden;scroll-snap-type:x mandatory;scroll-behavior:smooth;-webkit-overflow-scrolling:touch;padding:0 0 1.5rem;scrollbar-width:thin;scrollbar-color:#6C72FF rgba(255,255,255,.08)}`,
  ],
  [
    `#insights-controls{order:2;display:flex;justify-content:center;align-items:center;gap:.7rem;margin:1.35rem 0 0;padding:0}`,
    `#insights-controls{order:2;display:flex;justify-content:center;align-items:center;gap:.85rem;margin:1.85rem 0 0;padding:0}`,
  ],
  [
    `#insights-prev,#insights-next{width:46px;height:46px;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.04);color:#fff;font-size:1.45rem;line-height:1;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;text-decoration:none!important;transition:background .2s,border-color .2s,opacity .2s}`,
    `#insights-prev,#insights-next{width:46px;height:46px;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.04);color:#fff;font-size:1.45rem;line-height:1;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;text-decoration:none!important;transition:background .25s ease,border-color .25s ease,opacity .25s ease,transform .25s ease,box-shadow .25s ease}`,
  ],
  [
    `#insights-prev:hover,#insights-next:hover{background:rgba(108,114,255,.18);border-color:rgba(108,114,255,.45);color:#fff}`,
    `#insights-prev:hover,#insights-next:hover{background:rgba(108,114,255,.18);border-color:rgba(108,114,255,.45);color:#fff;transform:translateY(-1px);box-shadow:0 8px 22px rgba(108,114,255,.18)}`,
  ],
  [
    `#insights-grid::-webkit-scrollbar{height:8px}`,
    `#insights-grid::-webkit-scrollbar{height:6px}`,
  ],
  [
    `#insights-grid::-webkit-scrollbar-thumb{background:#6C72FF;border-radius:999px}`,
    `#insights-grid::-webkit-scrollbar-thumb{background:linear-gradient(90deg,#6C72FF,#8B90FF);border-radius:999px}`,
  ],
  [
    `#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6{position:relative;display:flex;flex-direction:column;flex:0 0 min(480px,85vw);max-width:520px;min-width:320px;scroll-snap-align:start;min-height:100%}`,
    `#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6{position:relative;display:flex;flex-direction:column;flex:0 0 min(480px,85vw);max-width:520px;min-width:320px;scroll-snap-align:start;min-height:100%;transition:transform .35s ease,box-shadow .35s ease}`,
  ],
  [
    `#ins-1::after,#ins-2::after,#ins-3::after,#ins-4::after,#ins-5::after{content:"";position:absolute;top:3%;bottom:3%;right:-0.75rem;width:1px;background-image:linear-gradient(180deg,transparent,#b7a2fc 50%,transparent);opacity:.9;pointer-events:none;transform-origin:center center;animation:dc-ins-divider-in .85s ease both}`,
    `#ins-1::after,#ins-2::after,#ins-3::after,#ins-4::after,#ins-5::after{content:"";position:absolute;top:3%;bottom:3%;right:-1.25rem;width:1px;background-image:linear-gradient(180deg,transparent,#b7a2fc 50%,transparent);opacity:.55;pointer-events:none;transform-origin:center center;animation:dc-ins-divider-in .85s ease both}`,
  ],
  [
    `#ins-1-img-wrap,#ins-2-img-wrap,#ins-3-img-wrap,#ins-4-img-wrap,#ins-5-img-wrap,#ins-6-img-wrap{display:block;overflow:hidden;margin:0 0 16px;border-radius:12px;background:#0D1530;line-height:0;font-size:0;position:relative}`,
    `#ins-1-img-wrap,#ins-2-img-wrap,#ins-3-img-wrap,#ins-4-img-wrap,#ins-5-img-wrap,#ins-6-img-wrap{display:block;overflow:hidden;margin:0 0 16px;border-radius:12px;background:#0D1530;line-height:0;font-size:0;position:relative;box-shadow:0 0 0 1px rgba(255,255,255,.06)}`,
  ],
  [
    `transition:transform .35s ease}`,
    `transition:transform .45s cubic-bezier(.22,1,.36,1)}`,
  ],
  [
    `#ins-1-img-wrap:hover img,#ins-2-img-wrap:hover img,#ins-3-img-wrap:hover img,#ins-4-img-wrap:hover img,#ins-5-img-wrap:hover img,#ins-6-img-wrap:hover img{transform:scale(1.04)}`,
    `#ins-1:hover,#ins-2:hover,#ins-3:hover,#ins-4:hover,#ins-5:hover,#ins-6:hover{transform:translateY(-4px)} #ins-1-img-wrap:hover img,#ins-2-img-wrap:hover img,#ins-3-img-wrap:hover img,#ins-4-img-wrap:hover img,#ins-5-img-wrap:hover img,#ins-6-img-wrap:hover img{transform:scale(1.045)}`,
  ],
  [
    `#ins-1-title a,#ins-2-title a,#ins-3-title a,#ins-4-title a,#ins-5-title a,#ins-6-title a{color:#fff}`,
    `#ins-1-title a,#ins-2-title a,#ins-3-title a,#ins-4-title a,#ins-5-title a,#ins-6-title a{color:#fff;transition:color .2s ease}`,
  ],
  [
    `#ins-1-more,#ins-2-more,#ins-3-more,#ins-4-more,#ins-5-more,#ins-6-more{align-self:flex-start;display:inline-flex;align-items:center;justify-content:center;padding:0 20px;height:40px;margin-top:auto;border-radius:8px;border:1.5px solid rgba(108,114,255,.28);background:transparent;color:rgba(255,255,255,.88);font-size:.9rem;font-weight:600}`,
    `#ins-1-more,#ins-2-more,#ins-3-more,#ins-4-more,#ins-5-more,#ins-6-more{align-self:flex-start;display:inline-flex;align-items:center;justify-content:center;padding:0 20px;height:40px;margin-top:auto;margin-bottom:.35rem;border-radius:8px;border:1.5px solid rgba(108,114,255,.28);background:transparent;color:rgba(255,255,255,.88);font-size:.9rem;font-weight:600;transition:color .25s ease,border-color .25s ease,background .25s ease,transform .25s ease,box-shadow .25s ease}`,
  ],
  [
    `#ins-1-more:hover,#ins-2-more:hover,#ins-3-more:hover,#ins-4-more:hover,#ins-5-more:hover,#ins-6-more:hover{color:#fff;border-color:#6C72FF;background:rgba(108,114,255,.12)}`,
    `#ins-1-more:hover,#ins-2-more:hover,#ins-3-more:hover,#ins-4-more:hover,#ins-5-more:hover,#ins-6-more:hover{color:#fff;border-color:#6C72FF;background:rgba(108,114,255,.12);transform:translateY(-1px);box-shadow:0 8px 20px rgba(108,114,255,.16)}`,
  ],
];

const missing = [];
for (const [from, to] of replacements) {
  if (!css.includes(from)) missing.push(from.slice(0, 90));
  else css = css.split(from).join(to);
}

// Add reduced-motion guard once near FAQ if not present
if (!css.includes("prefers-reduced-motion:reduce){ #ins-1")) {
  css += ` @media (prefers-reduced-motion:reduce){ #ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,#ins-1-img-wrap img,#ins-2-img-wrap img,#ins-3-img-wrap img,#ins-4-img-wrap img,#ins-5-img-wrap img,#ins-6-img-wrap img,#ins-1-more,#ins-2-more,#ins-3-more,#ins-4-more,#ins-5-more,#ins-6-more,#insights-prev,#insights-next{transition:none!important;transform:none!important} }`;
}

fs.writeFileSync(out, css);
const hash = crypto.createHash("md5").update(fs.readFileSync(out)).digest("hex");
console.log(JSON.stringify({ out, bytes: css.length, hash, missingCount: missing.length, missing }));
