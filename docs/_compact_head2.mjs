import fs from 'fs';

function minifyCssInStyleTags(html) {
  return html.replace(/<style(\s[^>]*)?>([\s\S]*?)<\/style>/gi, (_, attrs = '', css) => {
    const min = css
      .replace(/\/\*[\s\S]*?\*\//g, '')
      .replace(/\s+/g, ' ')
      .replace(/\s*([{}:;,>~+])\s*/g, '$1')
      .replace(/;}/g, '}')
      .trim();
    return `<style${attrs}>${min}</style>`;
  });
}

let raw = fs.readFileSync('docs/_dd_head.html', 'utf8').replace(/\r\n/g, '\n');
raw = raw.replace(/#about\[data-oh-problem="deal-desk"\]\s*\.dealality-problem-desk\s*,\s*/g, '');
raw = raw.replace(/#about\[data-oh-problem="deal-desk"\]\s*\.dealality-problem-desk/g, '.dealality-problem-desk');
const out = minifyCssInStyleTags(raw) + '\n';
fs.writeFileSync('docs/_dd_head_compact.html', out);
console.log(JSON.stringify({
  outLen: out.length,
  hasOhTt: out.includes('id="oh-tt"'),
  hasDealDesk: out.includes('id="oh-deal-desk"'),
  hasP1b: out.includes('oh-p1b'),
  hasDpd: out.includes('.dpd-desk'),
  has767: out.includes('767px'),
}));
