const fs = require('fs');
const css = fs.readFileSync('tmp-v9-full-css.css', 'utf8');
const MAX = 10000;
const parts = [];
let i = 0;
while (i < css.length) {
  let end = Math.min(i + MAX, css.length);
  if (end < css.length) {
    // break at newline / rule end
    const nl = css.lastIndexOf('\n', end);
    if (nl > i + MAX * 0.5) end = nl + 1;
  }
  parts.push(css.slice(i, end));
  i = end;
}
parts.forEach((p, idx) => {
  const html = `<style id="dc-v9-css-${idx + 1}">\n${p}\n</style>`;
  fs.writeFileSync(`tmp-v9-full-style-${String(idx + 1).padStart(2, '0')}.html`, html);
  console.log('style', idx + 1, html.length);
});
console.log('parts', parts.length);
