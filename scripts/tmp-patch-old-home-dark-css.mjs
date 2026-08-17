import fs from "fs";
import crypto from "crypto";

const src = "tmp-old-home-dark.v20260728n.css";
const out = "tmp-old-home-dark.v20260728o.css";
let css = fs.readFileSync(src, "utf8");

const replacements = [
  [
    `#insights-h2{margin:0 auto 18px;max-width:min(40rem,100%);font-family:"Plus Jakarta Sans","Inter Tight",system-ui,sans-serif;font-size:clamp(26px,3.5vw,44px);line-height:1.15;font-weight:800;color:#fff;letter-spacing:-.03em;text-transform:none;text-wrap:balance}`,
    `#insights-h2{margin:0 auto 18px;max-width:min(54rem,100%);font-family:"Plus Jakarta Sans","Inter Tight",system-ui,sans-serif;font-size:clamp(26px,3.5vw,44px);line-height:1.15;font-weight:800;color:#fff;letter-spacing:-.03em;text-transform:none;white-space:normal;text-wrap:pretty}`,
  ],
  [
    `#ins-1,#ins-2,#ins-3{position:relative;display:flex;flex-direction:column;flex:0 0 min(420px,78vw);max-width:440px;min-width:280px;scroll-snap-align:start;min-height:100%}`,
    `#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6{position:relative;display:flex;flex-direction:column;flex:0 0 min(480px,85vw);max-width:520px;min-width:320px;scroll-snap-align:start;min-height:100%}`,
  ],
  [
    `#ins-1::after,#ins-2::after{content:"";position:absolute;top:3%;bottom:3%;right:-0.75rem;width:1px;background-image:linear-gradient(180deg,transparent,#b7a2fc 50%,transparent);opacity:.9;pointer-events:none;transform-origin:center center;animation:dc-ins-divider-in .85s ease both}`,
    `#ins-1::after,#ins-2::after,#ins-3::after,#ins-4::after,#ins-5::after{content:"";position:absolute;top:3%;bottom:3%;right:-0.75rem;width:1px;background-image:linear-gradient(180deg,transparent,#b7a2fc 50%,transparent);opacity:.9;pointer-events:none;transform-origin:center center;animation:dc-ins-divider-in .85s ease both}`,
  ],
  [
    `@media (max-width:640px){ #ins-1::after,#ins-2::after{display:none} }`,
    `@media (max-width:640px){ #ins-1::after,#ins-2::after,#ins-3::after,#ins-4::after,#ins-5::after{display:none} }`,
  ],
  [
    `#ins-1-img-wrap,#ins-2-img-wrap,#ins-3-img-wrap{display:block;overflow:hidden;margin:0 0 16px;border-radius:12px;background:#0D1530;line-height:0;font-size:0;position:relative}`,
    `#ins-1-img-wrap,#ins-2-img-wrap,#ins-3-img-wrap,#ins-4-img-wrap,#ins-5-img-wrap,#ins-6-img-wrap{display:block;overflow:hidden;margin:0 0 16px;border-radius:12px;background:#0D1530;line-height:0;font-size:0;position:relative}`,
  ],
  [
    `#ins-1-img-wrap img,#ins-2-img-wrap img,#ins-3-img-wrap img,#ins-1-img,#ins-2-img,#ins-3-img{width:100%!important;max-width:none!important;height:auto!important;aspect-ratio:16/10;object-fit:cover!important;object-position:center center!important;border-radius:12px;display:block!important;margin:0!important;padding:0!important;border:0!important;vertical-align:top;transition:transform .35s ease}`,
    `#ins-1-img-wrap img,#ins-2-img-wrap img,#ins-3-img-wrap img,#ins-4-img-wrap img,#ins-5-img-wrap img,#ins-6-img-wrap img,#ins-1-img,#ins-2-img,#ins-3-img,#ins-4-img,#ins-5-img,#ins-6-img{width:100%!important;max-width:none!important;height:auto!important;aspect-ratio:16/10;object-fit:cover!important;object-position:center center!important;border-radius:12px;display:block!important;margin:0!important;padding:0!important;border:0!important;vertical-align:top;transition:transform .35s ease}`,
  ],
  [
    `#ins-1-img-wrap:hover img,#ins-2-img-wrap:hover img,#ins-3-img-wrap:hover img{transform:scale(1.04)}`,
    `#ins-1-img-wrap:hover img,#ins-2-img-wrap:hover img,#ins-3-img-wrap:hover img,#ins-4-img-wrap:hover img,#ins-5-img-wrap:hover img,#ins-6-img-wrap:hover img{transform:scale(1.04)}`,
  ],
  [
    `#ins-1-meta,#ins-2-meta,#ins-3-meta{display:flex;align-items:center;gap:.55rem;margin:0 0 8px;font-size:.95rem;line-height:1.4}`,
    `#ins-1-meta,#ins-2-meta,#ins-3-meta,#ins-4-meta,#ins-5-meta,#ins-6-meta{display:flex;align-items:center;gap:.55rem;margin:0 0 8px;font-size:.95rem;line-height:1.4}`,
  ],
  [
    `#ins-1-cat,#ins-2-cat,#ins-3-cat{color:#8B90FF;font-weight:600}`,
    `#ins-1-cat,#ins-2-cat,#ins-3-cat,#ins-4-cat,#ins-5-cat,#ins-6-cat{color:#8B90FF;font-weight:600}`,
  ],
  [
    `#ins-1-dot,#ins-2-dot,#ins-3-dot{width:4px;height:4px;border-radius:50%;background:rgba(255,255,255,.45);flex:0 0 auto}`,
    `#ins-1-dot,#ins-2-dot,#ins-3-dot,#ins-4-dot,#ins-5-dot,#ins-6-dot{width:4px;height:4px;border-radius:50%;background:rgba(255,255,255,.45);flex:0 0 auto}`,
  ],
  [
    `#ins-1-date,#ins-2-date,#ins-3-date{color:rgba(255,255,255,.55)}`,
    `#ins-1-date,#ins-2-date,#ins-3-date,#ins-4-date,#ins-5-date,#ins-6-date{color:rgba(255,255,255,.55)}`,
  ],
  [
    `#ins-1-title,#ins-2-title,#ins-3-title{margin:0 0 22px;font-family:"Plus Jakarta Sans","Inter Tight",system-ui,sans-serif;font-size:1.2rem;line-height:1.35;font-weight:700;letter-spacing:-.02em;color:#fff}`,
    `#ins-1-title,#ins-2-title,#ins-3-title,#ins-4-title,#ins-5-title,#ins-6-title{margin:0 0 22px;font-family:"Plus Jakarta Sans","Inter Tight",system-ui,sans-serif;font-size:1.2rem;line-height:1.35;font-weight:700;letter-spacing:-.02em;color:#fff}`,
  ],
  [
    `#ins-1-title a,#ins-2-title a,#ins-3-title a{color:#fff}`,
    `#ins-1-title a,#ins-2-title a,#ins-3-title a,#ins-4-title a,#ins-5-title a,#ins-6-title a{color:#fff}`,
  ],
  [
    `#ins-1-title a:hover,#ins-2-title a:hover,#ins-3-title a:hover{color:#8B90FF}`,
    `#ins-1-title a:hover,#ins-2-title a:hover,#ins-3-title a:hover,#ins-4-title a:hover,#ins-5-title a:hover,#ins-6-title a:hover{color:#8B90FF}`,
  ],
  [
    `#ins-1-more,#ins-2-more,#ins-3-more{align-self:flex-start;display:inline-flex;align-items:center;justify-content:center;padding:0 20px;height:40px;margin-top:auto;border-radius:8px;border:1.5px solid rgba(108,114,255,.28);background:transparent;color:rgba(255,255,255,.88);font-size:.9rem;font-weight:600}`,
    `#ins-1-more,#ins-2-more,#ins-3-more,#ins-4-more,#ins-5-more,#ins-6-more{align-self:flex-start;display:inline-flex;align-items:center;justify-content:center;padding:0 20px;height:40px;margin-top:auto;border-radius:8px;border:1.5px solid rgba(108,114,255,.28);background:transparent;color:rgba(255,255,255,.88);font-size:.9rem;font-weight:600}`,
  ],
  [
    `#ins-1-more:hover,#ins-2-more:hover,#ins-3-more:hover{color:#fff;border-color:#6C72FF;background:rgba(108,114,255,.12)}`,
    `#ins-1-more:hover,#ins-2-more:hover,#ins-3-more:hover,#ins-4-more:hover,#ins-5-more:hover,#ins-6-more:hover{color:#fff;border-color:#6C72FF;background:rgba(108,114,255,.12)}`,
  ],
  [
    `@media (max-width:960px){ #insights{padding:72px 1.25rem} #insights-h2{max-width:min(36rem,100%)} #insights-lead{margin-bottom:1.25rem} #ins-1,#ins-2,#ins-3{flex-basis:min(320px,88vw)} }`,
    `@media (max-width:960px){ #insights{padding:72px 1.25rem} #insights-h2{max-width:min(42rem,100%)} #insights-lead{margin-bottom:1.25rem} #ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6{flex-basis:min(340px,88vw)} }`,
  ],
];

let missing = [];
for (const [from, to] of replacements) {
  if (!css.includes(from)) missing.push(from.slice(0, 80));
  else css = css.split(from).join(to);
}

fs.writeFileSync(out, css);
const hash = crypto.createHash("md5").update(fs.readFileSync(out)).digest("hex");
console.log(JSON.stringify({ out, bytes: css.length, hash, missingCount: missing.length, missing }));
