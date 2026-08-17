const fs = require('fs');
const h = fs.readFileSync('public/index.html', 'utf8');
const bodyStart = h.indexOf('<body');
const bodyEnd = h.lastIndexOf('</body>');
const styleStart = h.indexOf('<style>') + '<style>'.length;
const styleEnd = h.indexOf('</style>');
const style = h.slice(styleStart, styleEnd).trim();
const bodyOpenEnd = h.indexOf('>', bodyStart) + 1;
let bodyInner = h.slice(bodyOpenEnd, bodyEnd).trim();
// Drop scripts for WHTML insert; keep structure/content
bodyInner = bodyInner.replace(/<script[\s\S]*?<\/script>/gi, '');
fs.writeFileSync('tmp-landing-body.html', bodyInner);
fs.writeFileSync('tmp-landing-style.css', style);
console.log(JSON.stringify({
  htmlLen: h.length,
  styleLen: style.length,
  bodyLen: bodyInner.length,
  ids: [...bodyInner.matchAll(/id="([^"]+)"/g)].map((m) => m[1]),
  topTags: [...bodyInner.matchAll(/<(nav|section|footer|header|div)\b[^>]{0,80}/g)]
    .slice(0, 30)
    .map((m) => m[0]),
}, null, 2));
