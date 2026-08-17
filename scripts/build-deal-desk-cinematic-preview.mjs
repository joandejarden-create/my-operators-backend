import fs from "fs";
import path from "path";

const root = process.cwd();
const html = fs.readFileSync(
  path.join(root, "public/marketing/old-home-problem-deal-desk.v1.html"),
  "utf8"
);
const css = fs.readFileSync(
  path.join(root, "public/marketing/old-home-problem-deal-desk.v1.css"),
  "utf8"
);
const js = fs.readFileSync(
  path.join(root, "public/marketing/old-home-problem-deal-desk.v1.js"),
  "utf8"
);

const hotelSrc =
  "../public/marketing/deal-desk-assets/coastal-hotel-480.jpg";
const htmlLocal = html.replace(
  /src="deal-desk-assets\/coastal-hotel-480\.jpg"/g,
  `src="${hotelSrc}"`
);

const out = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Deal Desk Cinematic Phase B Preview</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@450;550;650;700&display=swap" rel="stylesheet"/>
<style>
body{margin:0;background:#070b18;font-family:"DM Sans",system-ui,sans-serif;color:#fff}
#about{padding:40px 20px 72px;max-width:1320px;margin:0 auto}
.preview-note{font-size:.72rem;color:rgba(255,255,255,.38);margin:0 0 1rem}
.oh-problem-header{text-align:center;margin:0 auto 1.35rem;max-width:46rem}
.oh-problem-eyebrow{display:inline-flex;align-items:center;gap:.55rem;margin:0 0 1rem;padding:.35rem .85rem .35rem .35rem;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(8,15,37,.92)}
.oh-problem-eyebrow-label{background:#343259;padding:.3rem .7rem;border-radius:10px;font-weight:600}
.oh-problem-eyebrow-detail{color:rgba(255,255,255,.7);font-size:.9rem}
.oh-problem-title{font-size:clamp(1.35rem,3vw,2rem);line-height:1.25;margin:0 0 1rem;font-weight:650}
.oh-problem-lead{margin:0 auto;color:rgba(255,255,255,.62);line-height:1.55;max-width:42rem}
.oh-problem-chapters{display:flex;gap:.55rem;justify-content:center;flex-wrap:wrap;margin:1.15rem 0 1.35rem}
.oh-problem-chapter{padding:.45rem .8rem;border:1px solid rgba(255,255,255,.12);border-radius:10px;font-size:.85rem}
.oh-problem-chapter.is-active{border-color:rgba(155,138,251,.45);box-shadow:0 0 0 1px rgba(155,138,251,.12)}
.oh-problem-stage{display:block}
/* Mock duplicate PVL for Phase B QA */
#oh-pvl{position:fixed;right:20px;bottom:24px;z-index:1400;width:320px;padding:12px 14px;border-radius:14px;background:#0b1228;border:1px solid rgba(255,255,255,.12);box-shadow:0 16px 40px rgba(0,0,0,.4)}
#oh-pvl strong{display:block;margin-bottom:4px}
#oh-pvl span{font-size:.82rem;color:rgba(255,255,255,.7)}
</style>
<style id="oh-deal-desk">${css}</style>
</head>
<body>
<section id="about" data-oh-problem="deal-desk">
  <p class="preview-note">Phase B preview · autoplay on ~33% visible · ?dealDeskState= for freeze · reduced-motion respected</p>
  <div class="oh-problem-header">
    <div class="oh-problem-eyebrow"><span class="oh-problem-eyebrow-label">The Problem</span><span class="oh-problem-eyebrow-detail">Fragmented deal evaluation</span></div>
    <h2 class="oh-problem-title">One opportunity. Four separate workstreams. Incomplete comparison.</h2>
    <p class="oh-problem-lead">Owners gather responses from brands, operators, advisors, and capital partners — then struggle to compare them on the same basis.</p>
    <div class="oh-problem-chapters" aria-label="Problem chapters">
      <span class="oh-problem-chapter is-active" data-problem-chapter="fragmented">Fragmented Outreach</span>
      <span class="oh-problem-chapter" data-problem-chapter="responses">Inconsistent Responses</span>
      <span class="oh-problem-chapter" data-problem-chapter="upside">Missed Upside</span>
    </div>
  </div>
  <div class="oh-problem-stage" aria-label="Deal Desk storyboard" role="group">
  ${htmlLocal}
  </div>
</section>
<!-- Intentional duplicate for dedupe QA (mirrors published bug) -->
<div id="oh-pvl"><strong>Why Dealality</strong><span>See the problem it solves—and how the platform works. 1:19</span></div>
<div id="oh-pvl"><strong>Why Dealality</strong><span>Duplicate launcher (should be removed)</span></div>
<script>${js}</script>
</body>
</html>
`;

fs.writeFileSync(
  path.join(root, "docs/old-home-problem-deal-desk-preview.html"),
  out
);
console.log("Wrote docs/old-home-problem-deal-desk-preview.html", out.length);
