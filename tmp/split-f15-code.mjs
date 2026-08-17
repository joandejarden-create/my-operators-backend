import fs from 'fs';

const args = JSON.parse(fs.readFileSync('tmp/dmp-set-embed-f15.json', 'utf8'));
const code = args.actions[0].set_settings.operations[0].settings[0].static_text.value;

// Split code into 3 chunks for reassembling in CallMcpTool if needed
const n = 3;
const size = Math.ceil(code.length / n);
for (let i = 0; i < n; i++) {
  const part = code.slice(i * size, (i + 1) * size);
  fs.writeFileSync(`tmp/dmp-code-chunk-${i}.txt`, part);
  console.log('chunk', i, part.length);
}

// Also write a verification get payload
const getPayload = {
  siteId: args.siteId,
  pageId: args.pageId,
  context: 'Verifies Manual Process HtmlEmbed code contains f15 CSS after set.',
  actions: [
    {
      label: 'verify-embed',
      get_settings: {
        type: 'query_settings',
        element_id: {
          component: '68108c2a063eeb5d1bd7ae90',
          element: 'a64ef2f7-2f5f-ab92-9711-5f43f9eeb3fa',
        },
        queries: [{ label: 'code', key: 'code' }],
      },
    },
  ],
};
fs.writeFileSync('tmp/dmp-verify-embed.json', JSON.stringify(getPayload));
console.log('verify ready');
