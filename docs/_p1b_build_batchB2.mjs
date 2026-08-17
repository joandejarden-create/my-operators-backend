import fs from 'fs';

const scenes = [4, 5, 6].map((n) =>
  fs
    .readFileSync(`docs/old-home-problem-phase1b-scene${n}.html`, 'utf8')
    .replace(/\r\n/g, '\n')
    .trim()
    .replace(/[\u201C\u201D]/g, '"')
);

const parent = {
  component: '68108c2a063eeb5d1bd7ae90',
  element: '1da347bd-f52c-dadf-da10-6906df4da740',
};

const args = {
  siteId: '68108c29063eeb5d1bd7ae4a',
  pageId: '68108c2a063eeb5d1bd7ae90',
  context:
    'Inserts Phase 1B visual scenes 4-6 into Old Home problem stage as static HTML.',
  actions: scenes.map((html, i) => ({
    build_label: `p1b-scene${i + 4}`,
    parent_element_id: parent,
    creation_position: 'append',
    html,
    return_element_info: true,
  })),
};

fs.writeFileSync('docs/_p1b_html_batchB2.json', JSON.stringify(args));
console.log(Buffer.byteLength(JSON.stringify(args)));
