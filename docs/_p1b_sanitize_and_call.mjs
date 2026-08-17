import fs from 'fs';

const p = JSON.parse(fs.readFileSync('docs/_p1b_action1.json', 'utf8'));
let css = p.actions[0].css;
css = css.split('content:""').join('content:none');
if (/[<>]/.test(css)) {
  console.error('angle brackets remain');
  process.exit(1);
}
p.actions[0].css = css;
p.context = 'Inserts Phase 1B scene 1 before scene 2 with shared visual CSS.';
fs.writeFileSync('docs/_p1b_action1b.json', JSON.stringify(p));
console.log(
  JSON.stringify({
    ok: true,
    cssLen: css.length,
    contentNone: (css.match(/content:none/g) || []).length,
    emptyLeft: css.includes('content:""'),
  })
);
