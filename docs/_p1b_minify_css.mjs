import fs from 'fs';
const src = fs.readFileSync('docs/old-home-problem-phase1b-visual-css.css', 'utf8');
const min = src
  .replace(/\/\*[\s\S]*?\*\//g, '')
  .replace(/\s+/g, ' ')
  .replace(/\s*([{}:;,])\s*/g, '$1')
  .replace(/;}/g, '}')
  .trim();
fs.writeFileSync('docs/old-home-problem-phase1b-visual-css.min.css', min);
console.log('css_len', min.length);
console.log(min.slice(0, 120));
