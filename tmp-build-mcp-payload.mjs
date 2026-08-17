import fs from 'fs';
const content = fs.readFileSync('tmp-old-home-head-patched.txt', 'utf8');
const args = {
  actions: [
    {
      label: 'set_old_home_head',
      set_page_freeform_code: {
        page_id: '68108c2a063eeb5d1bd7ae90',
        location: 'head',
        content,
      },
    },
  ],
  context:
    'Restores Old Home page HEAD freeform custom code with DashDark footer CSS appended before style close.',
};
fs.writeFileSync('tmp-old-home-head-mcp-payload.json', JSON.stringify(args), 'utf8');
console.log(
  JSON.stringify({
    length: content.length,
    starts: content.startsWith('<link rel="preconnect"'),
    marker: content.includes('DashDark-style 4-col footer'),
  })
);
