import fs from 'fs';

const css = fs
  .readFileSync('docs/old-home-problem-phase1b-visual-css.css', 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '')
  .replace(/\s+/g, ' ')
  .trim();

if (/[<>]/.test(css) || /:(before|after)\b/.test(css) || /::(before|after)\b/.test(css)) {
  console.error('Forbidden CSS tokens remain');
  process.exit(1);
}

fs.writeFileSync('docs/old-home-problem-phase1b-visual-css.min.css', css);

const scenes = [1, 2, 3, 4, 5, 6].map((n) =>
  fs.readFileSync(`docs/old-home-problem-phase1b-scene${n}.html`, 'utf8').replace(/\r\n/g, '\n').trim()
);

const parent = {
  component: '68108c2a063eeb5d1bd7ae90',
  element: '1da347bd-f52c-dadf-da10-6906df4da740',
};

const siteId = '68108c29063eeb5d1bd7ae4a';
const pageId = '68108c2a063eeb5d1bd7ae90';

const actions = scenes.map((html, i) => ({
  build_label: `p1b-scene${i + 1}`,
  parent_element_id: parent,
  creation_position: 'append',
  html,
  ...(i === 0 ? { css } : {}),
  return_element_info: true,
}));

// Max 5 actions per call — split into 2 batches
const batchA = {
  siteId,
  pageId,
  context: 'Inserts Phase 1B visual scenes 1-3 with shared CSS into Old Home problem stage.',
  actions: actions.slice(0, 3),
};
const batchB = {
  siteId,
  pageId,
  context: 'Inserts Phase 1B visual scenes 4-6 into Old Home problem stage.',
  actions: actions.slice(3),
};

fs.writeFileSync('docs/_p1b_batchA.json', JSON.stringify(batchA));
fs.writeFileSync('docs/_p1b_batchB.json', JSON.stringify(batchB));
console.log(
  JSON.stringify({
    cssLen: css.length,
    batchA: Buffer.byteLength(JSON.stringify(batchA)),
    batchB: Buffer.byteLength(JSON.stringify(batchB)),
    sceneLens: scenes.map((s) => s.length),
  })
);
