const fs = require('fs');
const content = fs.readFileSync('C:/Dev/deal-capture-proxy/docs/_dd_head.html', 'utf8').replace(/\r\n/g, '\n');
const payload = {
  context: 'Replacing Old Home freeform head with oh-tt plus oh-deal-desk CSS, removing oh-p1b.',
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
};
fs.writeFileSync('C:/Dev/deal-capture-proxy/docs/_mcp_head_args.json', JSON.stringify(payload));
console.log(JSON.stringify({ len: content.length, payloadBytes: Buffer.byteLength(JSON.stringify(payload)) }));
