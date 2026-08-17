import fs from 'fs';

let html = fs.readFileSync('docs/_dd_head_compact.html', 'utf8');
// Collapse duplicated identical selectors "A,A" -> "A"
html = html.replace(/([^{},;]+),\1(?=[{,])/g, '$1');
fs.writeFileSync('docs/_dd_head_compact.html', html);
console.log(html.length);
