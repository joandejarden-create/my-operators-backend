const fs = require('fs');
const css = fs.readFileSync('C:/Dev/deal-capture-proxy/docs/_dd_head.html', 'utf8').replace(/\r\n/g, '\n');
let out = css.replace(/\/\*[\s\S]*?\*\//g, '');
out = out
  .replace(/[ \t]+/g, ' ')
  .replace(/\n+/g, '\n')
  .replace(/ \{\n/g, '{')
  .replace(/\n\}/g, '}')
  .replace(/;\n/g, ';')
  .replace(/,\n/g, ',')
  .trim();
fs.writeFileSync('C:/Dev/deal-capture-proxy/docs/_dd_head_min.html', out);
console.log(JSON.stringify({
  orig: css.length,
  min: out.length,
  hasOhTt: out.includes('id="oh-tt"'),
  hasDealDesk: out.includes('id="oh-deal-desk"'),
  hasP1b: out.includes('oh-p1b'),
}));
