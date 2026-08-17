import fs from 'fs';

const css = fs.readFileSync('docs/old-home-problem-phase1b-visual-css.min.css', 'utf8');
const code = `<style id="oh-p1b">${css}</style>`;
const args = {
  siteId: '68108c29063eeb5d1bd7ae4a',
  pageId: '68108c2a063eeb5d1bd7ae90',
  context:
    'Writes Phase 1B visual storyboard CSS into the Old Home problem stage CodeBlock.',
  actions: [
    {
      label: 'set-p1b-css',
      set_settings: {
        operations: [
          {
            label: 'code',
            element_id: {
              component: '68108c2a063eeb5d1bd7ae90',
              element: '9d852fca-b819-568d-6535-30c71f9389c7',
            },
            settings: [{ key: 'code', static_text: { value: code } }],
          },
        ],
      },
    },
  ],
};
fs.writeFileSync('docs/_p1b_set_codeblock.json', JSON.stringify(args));
console.log(JSON.stringify({ codeLen: code.length, argsLen: Buffer.byteLength(JSON.stringify(args)) }));
