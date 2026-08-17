const fs = require('fs');
const h = fs.readFileSync('public/marketing/dealality-landing-v9-standalone.html', 'utf8');

// Deal readiness block
const i = h.indexOf('id="drwrap"');
console.log(h.slice(i - 400, i + 1200));

// Problem icons - look for svg near For Owners
const p = h.indexOf('id="problem"');
console.log('\n=== PROBLEM START ===\n', h.slice(p, p + 2000));

// Audience icons
const a = h.indexOf('id="audiences"');
console.log('\n=== AUD ICONS ===\n');
const aud = h.slice(a, a + 2500);
const svgs = [...aud.matchAll(/<svg[\s\S]*?<\/svg>/g)];
svgs.slice(0, 3).forEach((m, idx) => console.log(idx, m[0].slice(0, 200)));

// Check bg blob path
const blob = fs.existsSync('public/marketing/assets/sales-home-bg-blob.svg');
const blob2 = fs.existsSync('public/assets/sales-home-bg-blob.svg');
console.log('blob local', blob, blob2);
console.log(fs.readdirSync('public/marketing').slice(0, 40).join('\n'));
