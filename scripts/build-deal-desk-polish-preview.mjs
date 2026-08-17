import fs from "fs";
import path from "path";

const root = process.cwd();
const css = fs.readFileSync(path.join(root, "public/marketing/old-home-problem-deal-desk.v1.css"), "utf8");
const embed = fs.readFileSync(path.join(root, "public/marketing/old-home-problem-deal-desk.v1.html"), "utf8");

const preview = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Deal Desk Polish v2 Preview</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@450;550;650;700&display=swap" rel="stylesheet"/>
<style>
body{margin:0;background:#070b18;font-family:"DM Sans",system-ui,sans-serif;color:#fff}
#about{padding:48px 20px 72px;max-width:1280px;margin:0 auto}
.preview-note{font-size:.75rem;color:rgba(255,255,255,.4);margin:0 0 1.25rem}
.oh-problem-header{text-align:center;margin:0 auto 1.5rem;max-width:46rem}
.oh-problem-eyebrow{display:inline-flex;align-items:center;gap:.55rem;margin:0 0 1rem;padding:.35rem .85rem .35rem .35rem;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(8,15,37,.92)}
.oh-problem-eyebrow-label{background:#343259;padding:.3rem .7rem;border-radius:10px;font-weight:600}
.oh-problem-eyebrow-detail{color:rgba(255,255,255,.7);font-size:.9rem}
.oh-problem-title{font-size:clamp(1.35rem,3vw,2rem);line-height:1.25;margin:0 0 1rem;font-weight:650}
.oh-problem-lead{margin:0 auto;color:rgba(255,255,255,.62);line-height:1.55;max-width:42rem}
.oh-problem-chapters{display:flex;gap:.55rem;justify-content:center;flex-wrap:wrap;margin:1.25rem 0 1.5rem}
.oh-problem-chapter{padding:.45rem .8rem;border:1px solid rgba(255,255,255,.12);border-radius:10px;font-size:.85rem}
.oh-problem-chapter.is-active{border-color:rgba(155,138,251,.45);box-shadow:0 0 0 1px rgba(155,138,251,.12)}
</style>
<style id="oh-deal-desk">${css}</style>
</head>
<body>
<section id="about" data-oh-problem="deal-desk" class="oh-problem">
  <p class="preview-note">Local preview of Deal Desk polish-v2 (static; not published). No animation.</p>
  <header class="oh-problem-header">
    <div class="oh-problem-eyebrow"><span class="oh-problem-eyebrow-label">The Problem</span><span class="oh-problem-eyebrow-detail">Manual. Fragmented. Hard to Compare.</span></div>
    <h2 class="oh-problem-title">One Hotel Opportunity Can Have Many Possible Futures. Most Are Never Evaluated Together.</h2>
    <p class="oh-problem-lead">Owners still manage brand, operator, capital, conversion, and strategic decisions across separate conversations, files, and assumptions. The process becomes difficult to compare long before the full potential of the opportunity is clear.</p>
  </header>
  <div class="oh-problem-chapters">
    <div class="oh-problem-chapter is-active">01 Fragmented Outreach</div>
    <div class="oh-problem-chapter">02 Inconsistent Responses</div>
    <div class="oh-problem-chapter">03 Missed Upside</div>
  </div>
  ${embed}
</section>
</body>
</html>
`;

const out = path.join(root, "docs/old-home-problem-deal-desk-preview.html");
fs.writeFileSync(out, preview);
fs.writeFileSync(path.join(root, "docs/old-home-problem-deal-desk-embed.html"), embed);
console.log(JSON.stringify({ out, previewBytes: Buffer.byteLength(preview), embedBytes: Buffer.byteLength(embed) }));
