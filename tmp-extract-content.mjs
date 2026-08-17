import fs from 'fs';
const p = JSON.parse(fs.readFileSync('tmp-old-home-head-mcp-payload.json', 'utf8'));
const c = p.actions[0].set_page_freeform_code.content;
fs.writeFileSync('tmp-content-only.txt', c, 'utf8');
console.log(
  [c.length, c.startsWith('<link rel="preconnect"'), c.includes('DashDark-style 4-col footer')].join('|')
);
