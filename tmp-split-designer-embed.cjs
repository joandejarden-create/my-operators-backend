const fs = require('fs');
const raw = fs.readFileSync('tmp-v9-designer-embed.html', 'utf8');
const cssMatch = raw.match(/<style>([\s\S]*?)<\/style>/);
const css = (cssMatch ? cssMatch[1] : '').trim();
const html = raw.replace(/<style>[\s\S]*?<\/style>/, '').trim();
fs.writeFileSync('tmp-v9-designer-only.html', html);
fs.writeFileSync('tmp-v9-designer-only.css', css);
console.log({ html: html.length, css: css.length });
