import fs from 'fs';

const raw = fs.readFileSync('docs/_dd_head.html', 'utf8').replace(/\r\n/g, '\n');

function minifyCssBlock(css) {
  return css
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/\s+/g, ' ')
    .replace(/\s*([{}:;,>~+])\s*/g, '$1')
    .replace(/;}/g, '}')
    .trim();
}

const re = /<style(\s[^>]*)?>([\s\S]*?)<\/style>/gi;
let out = '';
let m;
while ((m = re.exec(raw))) {
  const attrs = m[1] || '';
  out += `<style${attrs}>` + minifyCssBlock(m[2]) + `</style>\n`;
}

fs.writeFileSync('docs/_dd_head_fullmin.html', out);
console.log(
  JSON.stringify({
    outLen: out.length,
    hasOhTt: out.includes('id="oh-tt"'),
    hasDealDesk: out.includes('id="oh-deal-desk"'),
    hasP1b: out.includes('oh-p1b'),
    hasDpdDesk: out.includes('.dpd-desk'),
    hasMedia767: out.includes('767px'),
  })
);
