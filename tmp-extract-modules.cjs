const fs = require('fs');
const html = fs.readFileSync('public/marketing/dealality-old-home-premium.html', 'utf8');
const start = html.indexOf('<section id="modules"');
const end = html.indexOf('</section>', start) + '</section>'.length;
fs.writeFileSync('tmp-modules-section.html', html.slice(start, end));
console.log('len', end - start);

const css = fs.readFileSync('public/marketing/dealality-old-home-premium.css', 'utf8');
const cStart = css.indexOf('#modules{padding:100px');
const cEnd = css.indexOf('#trust{');
let block = css.slice(cStart, cEnd);
// make important overrides for freeform
const lines = block.split(/\n/).map((line) => {
  if (!line.includes('{') && !line.includes('}') && line.includes(':') && !line.includes('!important')) {
    return line.replace(/;(\s*)$/, '!important;$1');
  }
  if (line.includes('{') && line.includes('}') && line.includes(':')) {
    return line.replace(/:([^;{}]+);/g, ':$1!important;');
  }
  return line.replace(/:([^;{]+);/g, (m, v) => (v.includes('!important') ? m : `:${v}!important;`));
});
fs.writeFileSync(
  'tmp-benefits-tabs-css.txt',
  '/* Benefits dual tabs */\n' +
    lines.join('\n').trim() +
    '\n#modules-panel-platform[hidden]{display:none!important}\n@media(max-width:960px){#modules-grid,#modules-grid-platform{grid-template-columns:1fr 1fr!important;gap:1.1rem!important}}\n@media(max-width:640px){#modules-grid,#modules-grid-platform{grid-template-columns:1fr!important}#modules-tab-outcomes,#modules-tab-platform{font-size:.9rem!important;padding:0 10px!important}}\n'
);
console.log('css written', fs.statSync('tmp-benefits-tabs-css.txt').size);
