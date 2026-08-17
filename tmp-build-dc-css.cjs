/**
 * Build dc-* scoped visual CSS from Railway v9 standalone styles,
 * plus FAQ 2-col insights layout helpers.
 */
const fs = require('fs');

const html = fs.readFileSync('public/marketing/dealality-landing-v9-standalone.html', 'utf8');
const css = html.match(/<style>([\s\S]*?)<\/style>/)[1];

// Map unprefixed v9 classes → Webflow dc-* classes used on Old Home.
const classMap = {
  page: 'dc-page',
  nav: 'dc-nav',
  mnav: 'dc-mnav',
  nmenu: 'dc-nmenu',
  logo: 'dc-logo',
  nl: 'dc-nl',
  nr: 'dc-nr',
  nbg: 'dc-nbg',
  nbn: 'dc-nbn',
  si: 'dc-si',
  ey: 'dc-ey',
  eyg: 'dc-eyg',
  h2: 'dc-h2',
  lead: 'dc-lead',
  dvb: 'dc-dvb',
  hero: 'dc-hero',
  'hero-grid': 'dc-hero-grid',
  'hero-mesh': 'dc-hero-mesh',
  'hero-readability': 'dc-hero-readability',
  hbadge: 'dc-badge',
  puls: 'dc-puls',
  h1wrap: 'dc-h1wrap',
  hstatic: 'dc-hstatic',
  hrwrap: 'dc-hrwrap',
  hrinner: 'dc-hrinner',
  hrword: 'dc-hrword',
  hsub: 'dc-hsub',
  hctas: 'dc-ctas',
  bp2: 'dc-bp2',
  bs2: 'dc-bs2',
  proofbar: 'dc-proofbar',
  pgrid: 'dc-pgrid',
  pcol: 'dc-pcol',
  pwho: 'dc-pwho',
  ph: 'dc-ph',
  pb: 'dc-pb',
  pr: 'dc-pr',
  fbtabs: 'dc-stage-tabs',
  fbt: 'dc-stage-tab',
  'fbt-ico': 'dc-fbt-ico',
  'fbt-label': 'dc-fbt-label',
  fbprogress: 'dc-fbprogress',
  'fbprogress-fill': 'dc-fbprogress-fill',
  fbpause: 'dc-fbtour-pause',
  fbbody: 'dc-fbbody',
  fbp: 'dc-stage-panel',
  fbp2: 'dc-fbp2',
  'fb-side': 'dc-fb-side',
  fbpt: 'dc-fbpt',
  fbnote: 'dc-fbnote',
  fbck: 'dc-fbck',
  'fbi-d': 'dc-fbi-d',
  'fbi-p': 'dc-fbi-p',
  'fb-outcome': 'dc-fb-outcome',
  'stage-demo': 'dc-stage-demo',
  'dr-wrap': 'dc-dr-wrap',
  'dr-ring': 'dc-dr-ring',
  'dr-center': 'dc-dr-center',
  'dr-val': 'dc-dr-val',
  'dr-sub': 'dc-dr-sub',
  'dr-metrics': 'dc-dr-metrics',
  drm: 'dc-drm',
  'drm-l': 'dc-drm-l',
  'drm-track': 'dc-drm-track',
  'drm-bar': 'dc-drm-bar',
  'drm-v': 'dc-drm-v',
  'dr-bg': 'dc-dr-bg',
  'dr-fill': 'dc-dr-fill',
  audtabs: 'dc-aud-tabs',
  audt: 'dc-aud-tab',
  'aud-ico': 'dc-aud-ico',
  audtag: 'dc-aud-tag',
  audbody: 'dc-aud-body',
  bi: 'dc-bi',
  betabox: 'dc-betabox',
  bfull: 'dc-bfull',
  rc: 'dc-rc',
  bout: 'dc-bout',
  ptag: 'dc-ptag',
  pcard: 'dc-pcard',
  wgrid: 'dc-wgrid',
  'founder-block': 'dc-founder',
  'founder-av': 'dc-founder-img',
  wq: 'dc-wq',
  wa: 'dc-wa',
  wcards: 'dc-wcards',
  wc: 'dc-wc',
  wcl: 'dc-wcl',
  'faq-list': 'dc-faq-list',
  'faq-item': 'dc-faq-item',
  'faq-q': 'dc-faq-q',
  'faq-q-t': 'dc-faq-q-t',
  'faq-ico': 'dc-faq-ico',
  'faq-a': 'dc-faq-a',
  'faq-ai': 'dc-faq-ai',
  'bg-blob': 'dc-bg-blob',
  dg: 'dc-dg',
  'cta-in': 'dc-cta-in',
  ctah: 'dc-ctah',
  ctasub: 'dc-ctasub',
  ctapair: 'dc-ctapair',
  ctafine: 'dc-ctafine',
  ft: 'dc-ft',
  ftop: 'dc-ftop',
  fbrand: 'dc-fbrand',
  ftag: 'dc-ftag',
  fcolh: 'dc-fcolh',
  flinks: 'dc-flinks',
  'vs-wrap': 'dc-vs-wrap',
  'vs-row': 'dc-vs-row',
  'vs-cell': 'dc-vs-cell',
  'vs-mid': 'dc-vs-mid',
  'vs-ico': 'dc-vs-ico',
  shot: 'dc-shot',
  'posbanner': 'dc-posbanner',
};

// Longest keys first so replacements don't partial-match.
const keys = Object.keys(classMap).sort((a, b) => b.length - a.length);

function mapSelectorChunk(sel) {
  let out = sel;
  for (const k of keys) {
    const re = new RegExp(`\\.${k.replace(/-/g, '\\-')}(?![a-z0-9_-])`, 'g');
    out = out.replace(re, `.${classMap[k]}`);
  }
  // section ids stay the same (#faq, #how, etc.)
  return out;
}

// Naive CSS rewrite: split on `{` / `}` carefully enough for landing CSS (no nested @media content issues if we process rules).
function rewriteCss(src) {
  // Also map body → .dc-page
  let s = src
    .replace(/\bbody\s*\{/g, '.dc-page{')
    .replace(/html\s*\{[^}]*\}/g, '')
    .replace(/\*,\*::before,\*::after\{[^}]*\}/g, '');

  // Replace class names in selector portions (before `{`)
  s = s.replace(/(^|})([^{}@]+)\{/g, (m, brace, sels) => {
    return `${brace}${mapSelectorChunk(sels)}{`;
  });
  // @media blocks: also rewrite inner selectors via same pass already handled by global replace above... 
  // Actually the regex only hits top-level. Handle @media by rewriting whole stylesheet class tokens:
  return s;
}

// Safer: replace .classname tokens globally with word boundaries
let out = css
  .replace(/\bbody\s*\{/g, '.dc-page{')
  .replace(/html\{[^}]*\}/g, '')
  .replace(/\*,\*::before,\*::after\{box-sizing:border-box;margin:0;padding:0\}/g, '');

for (const k of keys) {
  const re = new RegExp(`\\.${k.replace(/-/g, '\\-')}(?![a-z0-9_-])`, 'g');
  out = out.replace(re, `.${classMap[k]}`);
}

// Extra FAQ 2-col layout (insights to the right of FAQ)
const extra = `
/* FAQ + Approach insights side-by-side */
.dc-faq-layout{display:grid;grid-template-columns:minmax(0,1.15fr) minmax(260px,.85fr);gap:40px;align-items:start;margin-top:40px}
.dc-faq-layout .dc-faq-list{margin-top:0}
.dc-faq-side{position:sticky;top:88px}
.dc-faq-side .dc-wcards{display:grid;grid-template-columns:1fr;gap:12px}
.dc-faq-side .dc-wc{background:var(--bg2);border:1px solid var(--bo);border-radius:10px;padding:18px 16px}
.dc-section{position:relative;padding:88px 48px;overflow:hidden}
.dc-section > .dc-si{position:relative;z-index:1;max-width:1200px;margin:0 auto}
.dc-hero{position:relative;overflow:hidden;min-height:86vh;padding:120px 48px 80px;display:flex;align-items:center}
.dc-cta{position:relative;overflow:hidden;text-align:center;padding:96px 48px}
.dc-dg{position:absolute;inset:0;pointer-events:none;background-image:linear-gradient(45deg,rgba(108,114,255,.025) 1px,transparent 1px),linear-gradient(-45deg,rgba(108,114,255,.025) 1px,transparent 1px);background-size:56px 56px;z-index:0}
.dc-bg-blob{position:absolute;pointer-events:none;z-index:0;user-select:none;width:min(640px,88vw);height:auto;opacity:.75;top:50%;left:50%;transform:translate(-50%,-50%)}
.dc-hero-mesh{position:absolute;inset:0;z-index:0;width:100%;height:100%;pointer-events:none;display:block}
.dc-fbt-ico{width:54px;height:54px;border-radius:50%;border:2px solid rgba(108,114,255,.22);background:var(--bg2);display:inline-flex;align-items:center;justify-content:center;color:rgba(139,144,255,.42);margin:0 auto 10px}
.dc-fbt-ico svg{width:20px;height:20px;fill:none;stroke:currentColor;stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round}
.dc-stage-tab.dc-stage-tab-on .dc-fbt-ico,.dc-stage-tab-on .dc-fbt-ico{border-color:var(--purple);background:linear-gradient(145deg,rgba(108,114,255,.22),rgba(108,114,255,.08));color:var(--pl);box-shadow:0 0 0 4px rgba(108,114,255,.12),0 8px 28px rgba(108,114,255,.18)}
.dc-stage-tab{display:flex;flex-direction:column;align-items:center}
.dc-fbprogress{height:2px;background:var(--bo);border-radius:2px;margin:6px 8.33% 12px;overflow:hidden}
.dc-fbprogress-fill{height:100%;width:16.66%;background:linear-gradient(90deg,var(--gold),var(--purple));border-radius:2px;transition:width .5s var(--ease)}
.dc-fbtour-pause{background:none;border:none;color:var(--mu);font-size:11px;font-weight:600;cursor:pointer;padding:4px 0;font-family:inherit}
.dc-dr-wrap{display:grid;grid-template-columns:auto 1fr;gap:20px;align-items:center;background:var(--bg3);border:1px solid var(--bp);border-radius:12px;padding:22px 20px;margin-top:16px}
.dc-dr-ring{position:relative;width:112px;height:112px;flex-shrink:0}
.dc-dr-ring svg{transform:rotate(-90deg)}
.dc-dr-bg{fill:none;stroke:rgba(255,255,255,.08);stroke-width:10}
.dc-dr-fill{fill:none;stroke:url(#rg);stroke-width:10;stroke-linecap:round;stroke-dasharray:364;stroke-dashoffset:364;transition:stroke-dashoffset 1.4s var(--ease)}
.dc-dr-center{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center}
.dc-dr-val{font-size:24px;font-weight:800;color:#fff}
.dc-dr-sub{font-size:11px;color:var(--mu);letter-spacing:.04em;text-transform:uppercase}
.dc-drm{background:var(--bg2);border:1px solid var(--bo);border-radius:8px;padding:11px 12px;margin-bottom:8px}
.dc-drm-l{font-size:12px;font-weight:600;color:var(--mu);margin-bottom:7px}
.dc-drm-track{height:4px;background:rgba(255,255,255,.07);border-radius:100px;overflow:hidden;margin-bottom:6px}
.dc-drm-bar{height:100%;width:0%;border-radius:100px;background:linear-gradient(90deg,var(--purple),var(--pl));transition:width 2s var(--ease)}
.dc-drm-v{font-size:14px;font-weight:700;color:#fff}
.dc-fbp2{display:grid;grid-template-columns:1fr 1fr;gap:20px;align-items:start}
.dc-aud-ico{width:24px;height:24px;margin-bottom:12px;display:block;color:var(--pl)}
.dc-aud-ico--o{color:var(--gold)}.dc-aud-ico--b{color:var(--pl)}.dc-aud-ico--p{color:rgba(250,243,225,.55)}
.dc-faq-q{display:flex;align-items:center;justify-content:space-between;gap:16px;cursor:pointer}
.dc-faq-ico{width:22px;height:22px;border-radius:50%;border:1.5px solid var(--bp);display:inline-flex;align-items:center;justify-content:center;flex-shrink:0;color:var(--pl)}
.dc-wgrid{display:grid;grid-template-columns:1fr;gap:28px;align-items:start;margin-top:8px}
.dc-founder{display:flex;gap:20px;align-items:flex-start}
.dc-founder-img{width:80px;height:80px;border-radius:50%;flex-shrink:0;object-fit:cover;object-position:center 18%;border:1px solid var(--bp);box-shadow:0 6px 24px rgba(0,0,0,.4)}
#problem,#how,#faq{background:var(--bg2)}
#proofbar{background:var(--bg3)}
@media(max-width:960px){
  .dc-faq-layout,.dc-fbp2,.dc-wgrid{grid-template-columns:1fr}
  .dc-faq-side{position:static}
  .dc-section,.dc-cta,.dc-hero{padding-left:20px;padding-right:20px}
}
`;

const finalCss = `/* auto-generated from dealality-landing-v9-standalone.html — do not hand-edit */\n${out}\n${extra}\n`;

fs.writeFileSync('public/marketing/dealality-v9-webflow-dc.css', finalCss);
console.log('wrote', finalCss.length, 'bytes');

// Compact head link + critical extras (fonts already in page)
const headLink = `<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=Lora:ital,wght@0,400;0,500;1,400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://my-operators-backend-production.up.railway.app/marketing/dealality-v9-webflow-dc.css?v=20260727a">
<style id="dc-v9-critical">
:root{--bg:#080F25;--bg2:#0D1530;--bg3:#111B3A;--purple:#6C72FF;--pl:#8B90FF;--gold:#D78E2C;--bo:rgba(255,255,255,.07);--bp:rgba(108,114,255,.28);--se:rgba(255,255,255,.62);--mu:rgba(255,255,255,.38);--ease:cubic-bezier(.22,1,.36,1);--font:'Plus Jakarta Sans',sans-serif;--serif:'Lora',serif}
.dc-page{font-family:var(--font);background:var(--bg);color:#fff}
.dc-faq-layout{display:grid;grid-template-columns:minmax(0,1.15fr) minmax(260px,.85fr);gap:40px;align-items:start;margin-top:40px}
.dc-wgrid{display:grid;grid-template-columns:1fr;gap:28px}
.dc-wcards{display:grid;grid-template-columns:1fr;gap:12px}
.dc-wc{background:var(--bg2);border:1px solid var(--bo);border-radius:10px;padding:18px 16px}
.dc-wcl strong{display:block;color:#fff;margin-bottom:5px}
.dc-wcl{font-size:13px;color:var(--se);line-height:1.6}
.dc-stage-panel{display:none}.dc-stage-panel-on{display:block}
.dc-aud-panel{display:none}.dc-aud-panel-on{display:block}
.dc-faq-a{display:none}.dc-faq-a-open{display:block}
.dc-mnav{display:none}.dc-mnav-open{display:block}
.dc-overview{display:none}.dc-overview-open{display:block}
.dc-h1wrap{display:inline-flex;align-items:center;justify-content:center;gap:.32em;--hr-lh:clamp(32px,4vw,56px);--hr-slots:5}
.dc-hrwrap{display:inline-block;height:calc(var(--hr-lh)*var(--hr-slots));overflow:hidden}
.dc-hrinner{display:flex;flex-direction:column}
.dc-hrword{display:flex;align-items:center;height:var(--hr-lh);font-family:var(--serif);font-style:italic;color:var(--pl);opacity:.08}
.dc-hrword.on{font-family:var(--font);font-style:normal;font-weight:800;color:var(--gold);opacity:1}
@media(max-width:960px){.dc-faq-layout{grid-template-columns:1fr}}
</style>`;

fs.writeFileSync('tmp-old-home-head-visual.html', headLink);
console.log('head link length', headLink.length);
