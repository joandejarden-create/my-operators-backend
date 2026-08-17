const fs = require('fs');

const style = fs.readFileSync('tmp-landing-style.css', 'utf8');
const bodyInner = fs.readFileSync('tmp-landing-body.html', 'utf8');

let css = style
  .replace(/^(\s*)body\s*\{/m, '$1.dc-landing {')
  .replace(/^(\s*)html\s*\{[^}]*\}\s*/m, '')
  .replace(/^(\s*)\*\s*,\s*\*::before\s*,\s*\*::after\s*\{[^}]*\}\s*/m, '');

css = `
.dc-landing {
  font-family: 'Mona Sans Variable', 'Mona Sans', -apple-system, BlinkMacSystemFont, sans-serif;
  color: #d1dbf9;
  background-color: #080f25;
  min-height: 100vh;
}
.dc-landing img { max-width: 100%; height: auto; }
.dc-landing section {
  padding: 80px 24px;
  max-width: 100%;
  margin: 0 auto;
  scroll-margin-top: 70px;
  border-top: 1px solid rgba(255,255,255,0.06);
  box-sizing: border-box;
}
${css}
`.trim();

fs.writeFileSync('tmp-landing-scoped.css', css);
fs.writeFileSync('tmp-landing-scoped.html', `<div class="dc-landing">\n${bodyInner}\n</div>`);

const starts = [];
const re = /<(?:nav|header|section|footer)\b/g;
let m;
while ((m = re.exec(bodyInner))) starts.push(m.index);
starts.push(bodyInner.length);

const blocks = [];
for (let i = 0; i < starts.length - 1; i++) {
  blocks.push(bodyInner.slice(starts[i], starts[i + 1]).trim());
}

const MAX = 9500;
const chunks = [];
let cur = '';
for (const b of blocks) {
  if (cur && cur.length + b.length + 1 > MAX) {
    chunks.push(cur);
    cur = b;
  } else {
    cur = cur ? `${cur}\n${b}` : b;
  }
}
if (cur) chunks.push(cur);

for (const f of fs.readdirSync('.')) {
  if (/^tmp-landing-chunk-\d+\.html$/.test(f)) fs.unlinkSync(f);
}

chunks.forEach((c, i) => {
  const name = String(i + 1).padStart(2, '0');
  fs.writeFileSync(`tmp-landing-chunk-${name}.html`, c);
  console.log(name, c.length);
});
console.log('css', css.length, 'chunks', chunks.length);

const full = fs.readFileSync('public/index.html', 'utf8');
const scripts = [];
const sre = /<script\b([^>]*)>([\s\S]*?)<\/script>/gi;
while ((m = sre.exec(full))) {
  if (/\bsrc\s*=/.test(m[1] || '')) continue;
  const code = (m[2] || '').trim();
  if (code) scripts.push(code);
}
fs.writeFileSync('tmp-landing-scripts.js', scripts.join('\n\n'));
console.log('scripts', scripts.length, scripts.reduce((n, s) => n + s.length, 0));
