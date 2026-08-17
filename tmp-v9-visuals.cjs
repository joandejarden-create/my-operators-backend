const fs = require('fs');
const h = fs.readFileSync('public/marketing/dealality-landing-v9-standalone.html', 'utf8');

// CSS for .si and nearby faq layout
const styleEnd = h.indexOf('</style>');
const style = h.slice(0, styleEnd);
for (const sel of ['.si{', '#faq{', '.bg-blob', '.dg{', '.icon', '.pico', '.wcl', '.fbi-', 'svg.icon', '.fbt-ico', '.hero-mesh', '--bg', '.posbanner']) {
  const i = style.indexOf(sel);
  if (i >= 0) console.log('\n---', sel, '---\n', style.slice(i, i + 400));
}

// Why section
const why = h.indexOf('id="why"');
console.log('\n=== WHY ===\n', h.slice(why, why + 3500));

// How prepare side
const prep = h.indexOf('fb-side');
console.log('\n=== FB-SIDE occurrences ===', (h.match(/fb-side/g) || []).length);
console.log(h.slice(prep - 200, prep + 2500));
