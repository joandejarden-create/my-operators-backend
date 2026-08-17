import fs from 'fs';

const raw = fs.readFileSync('docs/_dd_head.html', 'utf8').replace(/\r\n/g, '\n');
const ttEnd = raw.indexOf('</style>') + 8;
const ohTt = raw.slice(0, ttEnd);

let dd = raw.slice(raw.indexOf('<style id="oh-deal-desk">'));
// Drop duplicated long-prefix selectors; keep .dealality-problem-desk only
dd = dd.replace(/#about\[data-oh-problem="deal-desk"\]\s*\.dealality-problem-desk\s*,\s*/g, '');
dd = dd.replace(/#about\[data-oh-problem="deal-desk"\]\s*\.dealality-problem-desk/g, '.dealality-problem-desk');
dd = dd
  .replace(/\/\*[\s\S]*?\*\//g, '')
  .replace(/\s+/g, ' ')
  .replace(/\s*([{}:;,>~+])\s*/g, '$1')
  .replace(/;}/g, '}')
  .trim();

const out = ohTt.replace(/\r\n/g, '\n') + '\n' + dd + (dd.endsWith('\n') ? '' : '\n');
fs.writeFileSync('docs/_dd_head_compact.html', out);
console.log(
  JSON.stringify({
    outLen: out.length,
    hasOhTt: out.includes('id="oh-tt"'),
    hasDealDesk: out.includes('id="oh-deal-desk"'),
    hasAboutPrefix: out.includes('data-oh-problem'),
    hasDpd: out.includes('.dpd-desk'),
    has767: out.includes('767px'),
    hasP1b: out.includes('oh-p1b'),
  })
);
