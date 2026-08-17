const fs = require('fs');

const h = fs.readFileSync(
  'public/marketing/dealality-landing-v9-standalone.html',
  'utf8'
);

const styleStart = h.indexOf('<style>') + '<style>'.length;
const styleEnd = h.indexOf('</style>');
const bodyStart = h.indexOf('<body');
const bodyOpen = h.indexOf('>', bodyStart) + 1;
const bodyEnd = h.lastIndexOf('</body>');

const css = h.slice(styleStart, styleEnd).trim();
let body = h.slice(bodyOpen, bodyEnd).trim();
body = body.replace(/<script[\s\S]*?<\/script>/gi, '').trim();

// Extract scripts separately for footer freeform
const scripts = [];
const sre = /<script\b([^>]*)>([\s\S]*?)<\/script>/gi;
let m;
while ((m = sre.exec(h))) {
  const attrs = m[1] || '';
  if (/\bsrc\s*=/.test(attrs) && /gtag|googletagmanager/i.test(attrs)) continue;
  if (/\bsrc\s*=/.test(attrs)) {
    scripts.push({ type: 'src', attrs: attrs.trim(), code: '' });
  } else {
    const code = (m[2] || '').trim();
    if (code && !/gtag|dataLayer/i.test(code)) scripts.push({ type: 'inline', code });
  }
}

const fonts = [
  '<link rel="preconnect" href="https://fonts.googleapis.com">',
  '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>',
  '<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=Lora:ital,wght@0,400;0,500;1,400&display=swap" rel="stylesheet">',
].join('\n');

const head = `${fonts}\n<style id="dc-v9">${css}</style>`;

fs.writeFileSync('tmp-v9-css.css', css);
fs.writeFileSync('tmp-v9-body.html', body);
fs.writeFileSync('tmp-v9-head.html', head);
fs.writeFileSync(
  'tmp-v9-scripts.json',
  JSON.stringify(
    scripts.map((s) => ({
      type: s.type,
      codeLen: (s.code || '').length,
      preview: (s.code || '').slice(0, 80),
    })),
    null,
    2
  )
);

// Split body into HtmlEmbed-sized chunks (~20k) at section boundaries
const markers = [];
const mre = /<(?:nav|section|footer|div class="mnav"|div class="hero-overview)\b/g;
while ((m = mre.exec(body))) markers.push(m.index);
if (!markers.length || markers[0] !== 0) markers.unshift(0);
markers.push(body.length);

const parts = [];
for (let i = 0; i < markers.length - 1; i++) {
  const chunk = body.slice(markers[i], markers[i + 1]).trim();
  if (chunk) parts.push(chunk);
}

const MAX = 18000;
const embeds = [];
let cur = '';
for (const p of parts) {
  if (cur && cur.length + p.length + 1 > MAX) {
    embeds.push(cur);
    cur = p;
  } else {
    cur = cur ? `${cur}\n${p}` : p;
  }
}
if (cur) embeds.push(cur);

embeds.forEach((e, i) => {
  const n = String(i + 1).padStart(2, '0');
  fs.writeFileSync(`tmp-v9-embed-${n}.html`, e);
  console.log('embed', n, e.length);
});

console.log(
  JSON.stringify(
    {
      css: css.length,
      body: body.length,
      head: head.length,
      embeds: embeds.length,
      hasNav: body.includes('<nav'),
      hasHero: body.includes('commercial fit'),
      scripts: scripts.length,
    },
    null,
    2
  )
);
