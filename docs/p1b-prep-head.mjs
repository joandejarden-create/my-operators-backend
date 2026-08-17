import fs from 'fs';

const head = fs.readFileSync('docs/_p1b_merged_head_form.html', 'utf8');
fs.writeFileSync('docs/p1b-mcp-head-content.txt', head);
console.log(
  JSON.stringify({
    head_len: head.length,
    has_oh_tt: head.includes('id="oh-tt"'),
    has_oh_p1b: head.includes('id="oh-p1b"'),
  })
);
