const fs = require('fs');
const crypto = require('crypto');

const css = fs.readFileSync('public/marketing/dealality-old-home-premium.css', 'utf8');
const start = css.indexOf('#logos{display:none}');
const end = css.indexOf('#about{padding:5rem');
let block = css.slice(start, end).trim();
block = block
  .split(/\n/)
  .map((l) => {
    if (!l.includes(':') || l.trim().startsWith('@') || l.trim() === '}') return l;
    return l.replace(/:([^;{]+);/g, (m, v) => (v.includes('!important') ? m : ':' + v.trim() + '!important;'));
  })
  .join('\n');
fs.writeFileSync('tmp-perspectives.css', '/* Perspectives */\n' + block + '\n');
const b = fs.readFileSync('tmp-perspectives.css');
console.log('css', crypto.createHash('md5').update(b).digest('hex'), b.length);

const html = fs.readFileSync('public/marketing/dealality-old-home-premium.html', 'utf8');
const s = html.indexOf('<section id="perspectives"');
const e = html.indexOf('</section>', s) + '</section>'.length;
const compact = html.slice(s, e).replace(/>\s+</g, '><').replace(/\n/g, '');
fs.writeFileSync('tmp-perspectives-whtml.html', compact);
console.log('html', compact.length);

const js = fs.readFileSync('tmp-perspectives-tabs.js');
console.log(
  'js',
  crypto.createHash('md5').update(js).digest('hex'),
  js.length,
  'sha256-' + crypto.createHash('sha256').update(js).digest('base64')
);
