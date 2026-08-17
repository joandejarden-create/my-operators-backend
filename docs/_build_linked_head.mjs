import fs from 'fs';

const raw = fs.readFileSync('docs/_dd_head.html', 'utf8').replace(/\r\n/g, '\n');
const ohTt = raw.match(/<style id="oh-tt">[\s\S]*?<\/style>/)[0];
const link =
  '<link id="oh-deal-desk" rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bcad5d2a0492c49da9bd1_oh-deal-desk-phase-a.css">';
const content = `${ohTt}\n${link}\n`;
fs.writeFileSync('docs/_dd_head_linked.html', content);
console.log(
  JSON.stringify({
    len: content.length,
    hasTt: content.includes('oh-tt'),
    hasDd: content.includes('oh-deal-desk'),
    hasP1b: content.includes('oh-p1b'),
  })
);
