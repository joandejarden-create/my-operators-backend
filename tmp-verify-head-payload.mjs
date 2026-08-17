import fs from 'fs';
const p = JSON.parse(fs.readFileSync('tmp-old-home-head-mcp-payload.json', 'utf8'));
const c = p.actions[0].set_page_freeform_code.content;
const out = {
  ok: c.length === 21949,
  length: c.length,
  starts: c.startsWith('<link rel="preconnect"'),
  marker: c.includes('DashDark-style 4-col footer'),
  ends: c.trimEnd().endsWith('</style>'),
};
fs.writeFileSync('tmp-old-home-head-verify.json', JSON.stringify(out, null, 2));
console.log(JSON.stringify(out));
