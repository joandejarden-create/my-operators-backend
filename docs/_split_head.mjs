import fs from 'fs';

const content = fs.readFileSync('docs/_dd_head.html', 'utf8').replace(/\r\n/g, '\n');
const i = content.indexOf('<style id="oh-deal-desk">');
const ohTt = content.slice(0, i);
const ohDd = content.slice(i);
fs.writeFileSync('docs/_dd_head_part_tt.html', ohTt);
fs.writeFileSync('docs/_dd_head_part_dd.html', ohDd);
console.log(JSON.stringify({ tt: ohTt.length, dd: ohDd.length, total: content.length }));
