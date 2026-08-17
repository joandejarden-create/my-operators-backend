import fs from 'fs';

const scenes = [1, 2, 3, 4, 5, 6].map((n) =>
  fs.readFileSync(`docs/old-home-problem-phase1b-scene${n}.html`, 'utf8').replace(/\r\n/g, '\n').trim()
);

const parent = {
  component: '68108c2a063eeb5d1bd7ae90',
  element: '1da347bd-f52c-dadf-da10-6906df4da740',
};

const siteId = '68108c29063eeb5d1bd7ae4a';
const pageId = '68108c2a063eeb5d1bd7ae90';

const mk = (label, list) => ({
  siteId,
  pageId,
  context: `Inserts Phase 1B visual ${label} into Old Home problem stage (HTML only; CSS in page head).`,
  actions: list.map((html, i) => ({
    build_label: `p1b-scene-${label}-${i + 1}`,
    parent_element_id: parent,
    creation_position: 'append',
    html,
    return_element_info: true,
  })),
});

const batchA = mk('batchA', scenes.slice(0, 3));
const batchB = mk('batchB', scenes.slice(3));
fs.writeFileSync('docs/_p1b_html_batchA.json', JSON.stringify(batchA));
fs.writeFileSync('docs/_p1b_html_batchB.json', JSON.stringify(batchB));
console.log(
  JSON.stringify({
    A: Buffer.byteLength(JSON.stringify(batchA)),
    B: Buffer.byteLength(JSON.stringify(batchB)),
  })
);
