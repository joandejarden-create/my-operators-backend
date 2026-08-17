import fs from 'fs';

const css = fs.readFileSync('docs/old-home-problem-phase1b-visual-css.min.css', 'utf8');
const tt = fs.readFileSync('docs/_p1b_existing_testimonials_head.txt', 'utf8');
const head = `${tt.trim()}\n<style id="oh-p1b">\n${css}\n</style>\n`;
fs.writeFileSync('docs/_p1b_merged_head_fix.html', head);
console.log('head_len', head.length);

const parent = {
  component: '68108c2a063eeb5d1bd7ae90',
  element: '1da347bd-f52c-dadf-da10-6906df4da740',
};

const actions = [];
for (const n of [1, 2, 3, 4, 5, 6]) {
  const html = fs.readFileSync(`docs/old-home-problem-phase1b-scene${n}.html`, 'utf8').trim();
  actions.push({
    build_label: `p1b-fix-scene-${n}`,
    parent_element_id: parent,
    creation_position: 'append',
    html,
    css: n === 1 ? css : undefined,
    return_element_info: true,
  });
}

// Batch A: scenes 1-3 (css on 1)
const batchA = actions.slice(0, 3).map((a, i) => {
  if (i === 0) return a;
  const { css: _c, ...rest } = a;
  return { ...rest, css }; // include css on all for safety
});
const batchB = actions.slice(3).map((a) => ({ ...a, css }));

fs.writeFileSync('docs/_p1b_fix_whtml_A.json', JSON.stringify(batchA));
fs.writeFileSync('docs/_p1b_fix_whtml_B.json', JSON.stringify(batchB));
fs.writeFileSync('docs/_p1b_fix_head_content.txt', head);
console.log('batchA', batchA.length, 'chars', JSON.stringify(batchA).length);
console.log('batchB', batchB.length, 'chars', JSON.stringify(batchB).length);
