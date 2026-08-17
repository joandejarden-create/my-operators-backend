const fs = require('fs');
const body = fs.readFileSync('tmp-landing-body.html', 'utf8');

// Skip nav/hero already inserted — start at what-is
const start = body.indexOf('<section class="what-is"');
const rest = start >= 0 ? body.slice(start) : body;

const starts = [];
const re = /<section\b/g;
let m;
while ((m = re.exec(rest))) starts.push(m.index);
starts.push(rest.length);

const sections = [];
for (let i = 0; i < starts.length - 1; i++) {
  sections.push(rest.slice(starts[i], starts[i + 1]).trim());
}

// Also capture footer after last section
const footerIdx = rest.lastIndexOf('<footer');
let footer = '';
if (footerIdx > starts[starts.length - 2]) {
  // last section may include footer; check
}

const MAX = 4500;
const chunks = [];
let cur = '';
for (const s of sections) {
  if (cur && cur.length + s.length + 1 > MAX) {
    chunks.push(cur);
    cur = s;
  } else {
    cur = cur ? `${cur}\n${s}` : s;
  }
}
if (cur) chunks.push(cur);

// Ensure footer included
if (!chunks.join('').includes('<footer') && body.includes('<footer')) {
  const f = body.slice(body.indexOf('<footer')).trim();
  // strip scripts already removed
  if (chunks[chunks.length - 1].length + f.length < MAX) {
    chunks[chunks.length - 1] += `\n${f}`;
  } else {
    chunks.push(f);
  }
}

for (const file of fs.readdirSync('.')) {
  if (/^tmp-whtml-\d+\.html$/.test(file)) fs.unlinkSync(file);
}

chunks.forEach((c, i) => {
  const wrapped = `<div class="dc-landing-chunk dc-landing-chunk-${i + 2}">\n${c}\n</div>`;
  const name = String(i + 2).padStart(2, '0');
  fs.writeFileSync(`tmp-whtml-${name}.html`, wrapped);
  console.log(name, wrapped.length);
});
console.log('count', chunks.length);
