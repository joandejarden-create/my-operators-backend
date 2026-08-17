import fs from 'fs';

const css = fs.readFileSync('public/marketing/old-home-problem-deal-desk.v1.css', 'utf8');
const html = fs.readFileSync('public/marketing/old-home-problem-deal-desk.v1.html', 'utf8');
const preview = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Deal Desk Phase A Preview</title>
<style>
body{margin:0;background:#070b18;font-family:Inter,system-ui,sans-serif;color:#fff}
#about{padding:48px 20px 64px;max-width:1280px;margin:0 auto}
.preview-note{font-size:.75rem;color:rgba(255,255,255,.4);margin:0 0 1.25rem}
.oh-problem-header{text-align:center;margin:0 auto 1.5rem;max-width:46rem}
.oh-problem-eyebrow{display:inline-flex;align-items:center;gap:.55rem;margin:0 0 1rem;padding:.35rem .85rem .35rem .35rem;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(8,15,37,.92)}
.oh-problem-eyebrow-label{background:#343259;padding:.3rem .7rem;border-radius:10px;font-weight:600}
.oh-problem-eyebrow-detail{color:rgba(255,255,255,.7);font-size:.9rem}
.oh-problem-title{font-size:clamp(1.35rem,3vw,2rem);line-height:1.25;margin:0 0 1rem;font-weight:650}
.oh-problem-lead{margin:0 auto;color:rgba(255,255,255,.62);line-height:1.55;max-width:42rem}
.oh-problem-chapters{display:flex;gap:.55rem;justify-content:center;flex-wrap:wrap;margin:0 0 1.5rem}
.oh-problem-chapter{padding:.45rem .8rem;border:1px solid rgba(255,255,255,.12);border-radius:10px;font-size:.85rem}
.oh-problem-chapter.is-active{border-color:rgba(155,138,251,.45);box-shadow:0 0 0 1px rgba(155,138,251,.12)}
</style>
<style id="oh-deal-desk">${css}</style>
</head>
<body>
<section id="about" data-oh-problem="deal-desk">
  <p class="preview-note">Local preview of Deal Desk Phase A (Designer draft; not published).</p>
  <div class="oh-problem-header">
    <div class="oh-problem-eyebrow">
      <span class="oh-problem-eyebrow-label">The Problem</span>
      <span class="oh-problem-eyebrow-detail">Manual. Fragmented. Hard to Compare.</span>
    </div>
    <h2 class="oh-problem-title">One Hotel Opportunity Can Have Many Possible Futures.<br/>Most Are Never Evaluated Together.</h2>
    <p class="oh-problem-lead">Owners still manage brand, operator, capital, conversion, and strategic decisions across separate conversations, files, and assumptions. The process becomes difficult to compare long before the full potential of the opportunity is clear.</p>
  </div>
  <div class="oh-problem-chapters" aria-label="Problem chapters">
    <div class="oh-problem-chapter is-active" data-problem-chapter="fragmented"><span>01</span> Fragmented Outreach</div>
    <div class="oh-problem-chapter" data-problem-chapter="responses"><span>02</span> Inconsistent Responses</div>
    <div class="oh-problem-chapter" data-problem-chapter="upside"><span>03</span> Missed Upside</div>
  </div>
  ${html}
</section>
</body>
</html>`;
fs.writeFileSync('docs/old-home-problem-deal-desk-preview.html', preview);
console.log('wrote preview', preview.length);
