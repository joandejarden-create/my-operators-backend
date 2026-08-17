import fs from 'fs';

const css = fs.readFileSync('docs/old-home-problem-phase1b-visual-css.min.css', 'utf8');
const scenes = [1, 2, 3, 4, 5, 6].map((n) =>
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

const siteId = '68108c29063eeb5d1bd7ae4a';
const pageId = '68108c2a063eeb5d1bd7ae90';

function batch(label, actions) {
  return {
    siteId,
    pageId,
    context: `Phase 1B render fix: inserts ${label} with Designer Style classes via WHTML CSS.`,
    actions,
  };
}

const a = batch('scene 1 with CSS', [
  {
    build_label: 'fix-scene1',
    parent_element_id: parent,
    creation_position: 'append',
    html: scenes[0],
    css,
    return_element_info: true,
  },
]);

const b = batch('scenes 2-3', [
  {
    build_label: 'fix-scene2',
    parent_element_id: parent,
    creation_position: 'append',
    html: scenes[1],
    return_element_info: true,
  },
  {
    build_label: 'fix-scene3',
    parent_element_id: parent,
    creation_position: 'append',
    html: scenes[2],
    return_element_info: true,
  },
]);

const c = batch('scenes 4-6', [
  {
    build_label: 'fix-scene4',
    parent_element_id: parent,
    creation_position: 'append',
    html: scenes[3],
    return_element_info: true,
  },
  {
    build_label: 'fix-scene5',
    parent_element_id: parent,
    creation_position: 'append',
    html: scenes[4],
    return_element_info: true,
  },
  {
    build_label: 'fix-scene6',
    parent_element_id: parent,
    creation_position: 'append',
    html: scenes[5],
    return_element_info: true,
  },
]);

fs.writeFileSync('docs/_p1b_fix_batchA.json', JSON.stringify(a));
fs.writeFileSync('docs/_p1b_fix_batchB.json', JSON.stringify(b));
fs.writeFileSync('docs/_p1b_fix_batchC.json', JSON.stringify(c));

// Page head freeform: testimonials + oh-p1b (backup for published/preview)
const testimonials = fs.readFileSync('docs/_p1b_existing_testimonials_head.txt', 'utf8').trimEnd() + '\n';
const head = `${testimonials}<style id="oh-p1b">${css}</style>\n`;
fs.writeFileSync(
  'docs/_p1b_fix_set_head.json',
  JSON.stringify({
    actions: [
      {
        label: 'set-head-p1b-css',
        set_page_freeform_code: {
          page_id: pageId,
          location: 'head',
          content: head,
        },
      },
    ],
    context:
      'Moves Phase 1B CSS into Old Home page head custom code so styles are not visible page content.',
  })
);

console.log(
  JSON.stringify({
    cssLen: css.length,
    A: Buffer.byteLength(JSON.stringify(a)),
    B: Buffer.byteLength(JSON.stringify(b)),
    C: Buffer.byteLength(JSON.stringify(c)),
    head: Buffer.byteLength(head),
  })
);
