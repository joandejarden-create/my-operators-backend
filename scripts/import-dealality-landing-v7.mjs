import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const src = path.join(process.env.USERPROFILE || "", "Downloads", "dealality-landing-v7.html");
const dest = path.join(__dirname, "..", "public", "marketing", "dealality-landing-v7.html");

const shotMap = {
  "Deal Brief": { src: "screenshots/deal-brief.png", width: 1024, height: 637 },
  "Brand Explorer": { src: "screenshots/brand-explorer.png", width: 1024, height: 673 },
  "Operator Explorer": { src: "screenshots/operator-track-record.png", width: 1024, height: 670 },
  "Match Brands": { src: "screenshots/matched-brands.png", width: 1024, height: 517 },
  "Compare Terms": { src: "screenshots/deal-compare.png", width: 1024, height: 463 },
  "Operator Profile": { src: "screenshots/operator-profile.png", width: 1024, height: 670 },
  "Franchise Fee Estimator": { src: "screenshots/fee-estimator.png", width: 1024, height: 510 },
};

let html = fs.readFileSync(src, "utf8");

for (const [label, meta] of Object.entries(shotMap)) {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(
    `<span class="sfu">dealality\\.com — ${escaped}</span></div><div class="sfb"[^>]*><img src="data:image/[^"]+"(?:\\s[^>]*)?>`,
    "g"
  );
  html = html.replace(
    re,
    `<span class="sfu">dealality.com — ${label}</span></div><div class="sfb"><img src="${meta.src}" width="${meta.width}" height="${meta.height}" alt="" loading="lazy" class="mkt-shot-img">`
  );
}

const replacements = [
  ["#persona{padding:0 48px 96px;max-width:1240px;margin:0 auto}", "#persona{padding:52px 48px 96px;max-width:1240px;margin:0 auto}"],
  [".plbl{text-align:center;font-size:12px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--mu);margin-bottom:22px}", ".sec-kicker{text-align:center;font-size:12px;font-weight:700;letter-spacing:.04em;color:var(--pl);margin-bottom:22px}"],
  [".prow{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}", ".prow{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;align-items:stretch}"],
  [".pc{background:var(--bg2);border:1px solid var(--bo);border-radius:12px;padding:30px;display:block;text-decoration:none;transition:border-color .3s,box-shadow .3s,background .3s}", ".pc{background:var(--bg2);border:1px solid var(--bo);border-radius:12px;padding:30px;display:flex;flex-direction:column;height:100%;text-decoration:none;transition:border-color .3s,box-shadow .3s,background .3s}"],
  [".pcbody{font-size:14px;color:var(--se);line-height:1.65;margin-bottom:16px}", ".pcbody{font-size:14px;color:var(--se);line-height:1.65;flex:1}"],
  [".pccta{font-size:12.5px;font-weight:700}", ".pccta{font-size:12.5px;font-weight:700;margin-top:auto;padding-top:16px}"],
  [".pgrid{display:grid;grid-template-columns:repeat(3,1fr);background:var(--bo);border:1px solid var(--bo);border-radius:12px;overflow:hidden;gap:1px;margin-top:56px}", ".pgrid{display:grid;grid-template-columns:repeat(3,1fr);align-items:stretch;background:var(--bo);border:1px solid var(--bo);border-radius:12px;overflow:hidden;gap:1px;margin-top:56px}"],
  [".pcol{background:var(--bg2);padding:34px 30px;transition:background .3s}", ".pcol{display:flex;flex-direction:column;height:100%;background:var(--bg2);padding:34px 30px;transition:background .3s}"],
  [".pb{font-size:14.5px;color:var(--se);line-height:1.68;margin-bottom:18px}", ".pb{flex:1;font-size:14.5px;color:var(--se);line-height:1.68;margin-bottom:0}"],
  [".pr{font-size:13px;color:rgba(215,142,44,.65);font-weight:600;padding-top:14px;border-top:1px solid var(--bo)}", ".pr{margin-top:auto;font-size:13px;color:rgba(215,142,44,.65);font-weight:600;padding-top:16px;border-top:1px solid var(--bo)}"],
  ['<p class="plbl">Who Are You?</p>', '<p class="sec-kicker">Who are you?</p>'],
  ['<div class="ph">Incomplete Information. No Structured Comparison.</div>', '<div class="ph">Incomplete Information.<br>No Structured Comparison.</div>'],
  ['<div class="ph">Misaligned Outreach. Misqualified Leads.</div>', '<div class="ph">Misaligned Outreach.<br>Misqualified Leads.</div>'],
  ['<div class="ph">Looped In Late. Limited Context.</div>', '<div class="ph">Looped in Late.<br>Limited Context.</div>'],
  ["#persona{padding:0 20px 68px}", "#persona{padding:40px 20px 68px}"],
  [".sfb img{width:100%;display:block}", ".sfb img{width:100%;max-width:100%;height:auto;display:block}"],
];

for (const [from, to] of replacements) {
  if (!html.includes(from)) {
    console.warn("Missing expected snippet:", from.slice(0, 80));
  }
  html = html.split(from).join(to);
}

fs.writeFileSync(dest, html, "utf8");
console.log(`Wrote ${dest} (${html.length} bytes)`);
