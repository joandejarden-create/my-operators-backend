import fs from 'fs';

const a = JSON.parse(fs.readFileSync('docs/_p1b_form_whtml_A.json', 'utf8'));
const b = JSON.parse(fs.readFileSync('docs/_p1b_form_whtml_B.json', 'utf8'));
const c = JSON.parse(fs.readFileSync('docs/_p1b_form_whtml_C.json', 'utf8'));
const head = fs.readFileSync('docs/_p1b_merged_head_fix.html', 'utf8');

console.log(
  JSON.stringify({
    A: a.map((x) => ({ label: x.build_label, html: x.html.length, css: x.css.length })),
    B: b.map((x) => ({ label: x.build_label, html: x.html.length, css: x.css.length })),
    C: c.map((x) => ({ label: x.build_label, html: x.html.length, css: x.css.length })),
    head: head.length,
  })
);

// Write single-action invoke files for reliable MCP arg loading
for (const [name, batch] of [
  ['A', a],
  ['B', b],
  ['C', c],
]) {
  fs.writeFileSync(`docs/_p1b_mcp_invoke_${name}.json`, JSON.stringify(batch));
}
fs.writeFileSync('docs/_p1b_mcp_head_content.txt', head);
console.log('wrote invoke files');
