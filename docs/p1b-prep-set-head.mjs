import fs from 'fs';

const content = fs.readFileSync('docs/p1b-mcp-head-content.txt', 'utf8');
const payload = {
  actions: [
    {
      label: 'set-head-p1b-css',
      set_page_freeform_code: {
        page_id: '68108c2a063eeb5d1bd7ae90',
        location: 'head',
        content,
      },
    },
  ],
  context:
    'Sets Old Home freeform head with testimonials oh-tt plus Phase 1B oh-p1b CSS.',
};
fs.writeFileSync('docs/p1b-mcp-set-head.json', JSON.stringify(payload));
console.log(
  JSON.stringify({
    head_len: content.length,
    payload_bytes: Buffer.byteLength(JSON.stringify(payload)),
    has_oh_tt: content.includes('id="oh-tt"'),
    has_oh_p1b: content.includes('id="oh-p1b"'),
  })
);
