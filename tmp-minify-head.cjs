const fs = require('fs');
let css = fs.readFileSync('tmp-landing-scoped.css', 'utf8');
// Light minify
css = css
  .replace(/\/\*[\s\S]*?\*\//g, '')
  .replace(/\s+/g, ' ')
  .replace(/\s*([{}:;,])\s*/g, '$1')
  .trim();
const head = `<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fontsource-variable/mona-sans@5.0.0/index.min.css">
<style id="dc-landing-styles">${css}</style>`;
fs.writeFileSync('tmp-landing-head-min.html', head);
console.log('min css', css.length, 'head', head.length);
