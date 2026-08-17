import fs from 'fs';
import crypto from 'crypto';

const content = fs.readFileSync(
  'c:/Users/joand/OneDrive/Documents/deal-capture-proxy/tmp-old-home-head-patched.txt',
  'utf8'
);

const args = {
  actions: [
    {
      label: 'set_old_home_head_restore',
      set_page_freeform_code: {
        page_id: '68108c2a063eeb5d1bd7ae90',
        location: 'head',
        content,
      },
    },
  ],
  context:
    'Restores patched Old Home head freeform code with DashDark footer CSS after accidental placeholder write.',
};

fs.writeFileSync(
  'c:/Users/joand/OneDrive/Documents/deal-capture-proxy/tmp-mcp-invoke.json',
  JSON.stringify(args)
);

console.log(
  JSON.stringify({
    len: content.length,
    sha256: crypto.createHash('sha256').update(content).digest('hex'),
    marker: content.includes('DashDark-style 4-col footer'),
  })
);
