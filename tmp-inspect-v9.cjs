const fs = require('fs');
const s = fs.readFileSync('public/marketing/dealality-landing-v9-standalone.html', 'utf8');
console.log('total', s.length);
const ids = [...s.matchAll(/id="([^"]+)"/g)].map((m) => m[1]);
console.log([...new Set(ids)].join('\n'));
for (const f of [
  'tmp-v9-embed-01.html',
  'tmp-v9-embed-02.html',
  'tmp-v9-embed-03.html',
  'tmp-v9-embed-04.html',
  'tmp-v9-css.css',
  'public/marketing/dealality-v9-old-home.css',
]) {
  if (fs.existsSync(f)) console.log(f, fs.statSync(f).size);
}
