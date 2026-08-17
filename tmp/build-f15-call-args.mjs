import fs from 'fs';

const sot = fs.readFileSync('docs/_dmp_embed_inline.html', 'utf8');
const args = {
  siteId: '68108c29063eeb5d1bd7ae4a',
  pageId: '68108c2a063eeb5d1bd7ae90',
  context:
    'Updates Manual Process CSS: muted problem body again; Email/Spreadsheets/Conversations white.',
  actions: [
    {
      label: 'set-embed-f15',
      set_settings: {
        operations: [
          {
            label: 'code',
            element_id: {
              component: '68108c2a063eeb5d1bd7ae90',
              element: 'a64ef2f7-2f5f-ab92-9711-5f43f9eeb3fa',
            },
            settings: [
              {
                key: 'code',
                static_text: { value: sot },
              },
            ],
          },
        ],
      },
    },
  ],
};

fs.writeFileSync('tmp/dmp-f15-from-transcript-pattern.json', JSON.stringify(args));
console.log(
  JSON.stringify({
    wrote: true,
    bytes: fs.statSync('tmp/dmp-f15-from-transcript-pattern.json').size,
    hasF15: sot.includes('v20260801f15.css'),
    hasVersion: sot.includes('1.1.37'),
    codeLen: sot.length,
  })
);
