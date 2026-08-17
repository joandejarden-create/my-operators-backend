const fs = require('fs');

const css = fs
  .readFileSync('public/css/dealality-old-home-landing.css', 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '')
  .replace(/\s+/g, ' ')
  .replace(/\s*([{}:;,])\s*/g, '$1')
  .trim();

let body = fs
  .readFileSync('tmp-landing-body.html', 'utf8')
  .replace(/<!--[\s\S]*?-->/g, '')
  .replace(/>\s+</g, '><')
  .replace(/\s+/g, ' ')
  .trim();

// Split body into ~8kb root-wrapped chunks for WHTML
const starts = [];
const re = /<(?:nav|header|section|footer)\b/g;
let m;
while ((m = re.exec(body))) starts.push(m.index);
starts.push(body.length);

const blocks = [];
for (let i = 0; i < starts.length - 1; i++) {
  blocks.push(body.slice(starts[i], starts[i + 1]).trim());
}

const MAX = 8000;
const chunks = [];
let cur = '';
for (const b of blocks) {
  if (cur && cur.length + b.length + 1 > MAX) {
    chunks.push(cur);
    cur = b;
  } else {
    cur = cur ? cur + b : b;
  }
}
if (cur) chunks.push(cur);

for (const f of fs.readdirSync('.')) {
  if (/^tmp-whtml-fix-\d+\.html$/.test(f)) fs.unlinkSync(f);
}

chunks.forEach((c, i) => {
  const n = String(i + 1).padStart(2, '0');
  const html = `<div class="dc-landing-chunk">${c}</div>`;
  fs.writeFileSync(`tmp-whtml-fix-${n}.html`, html);
  console.log(n, html.length);
});

fs.writeFileSync('tmp-whtml-fix-css.css', css);
console.log('css', css.length, 'chunks', chunks.length);
