const fs = require('fs');

const cssRaw = fs.readFileSync('public/css/dealality-old-home-landing.css', 'utf8');
const body = fs.readFileSync('tmp-landing-body.html', 'utf8');
const scripts = fs.existsSync('tmp-landing-scripts.js')
  ? fs.readFileSync('tmp-landing-scripts.js', 'utf8')
  : '';

const minCss = cssRaw
  .replace(/\/\*[\s\S]*?\*\//g, '')
  .replace(/\s+/g, ' ')
  .replace(/\s*([{}:;,])\s*/g, '$1')
  .trim();

const head = [
  '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fontsource-variable/mona-sans@5.0.0/index.min.css">',
  `<style id="dc-landing-styles">${minCss}</style>`,
].join('\n');

// Embed: full markup + behavior scripts. CSS lives in page head freeform.
const embedHtml = `${body}\n<script>\n${scripts}\n</script>\n`;

fs.writeFileSync('tmp-landing-head-full.html', head);
fs.writeFileSync('tmp-landing-embed-body.html', embedHtml);

console.log(
  JSON.stringify(
    {
      headLen: head.length,
      embedLen: embedHtml.length,
      cssLen: minCss.length,
      bodyLen: body.length,
    },
    null,
    2
  )
);
