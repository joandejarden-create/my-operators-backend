const fs = require('fs');

const cssRaw = fs.readFileSync('public/css/dealality-old-home-landing.css', 'utf8');
let body = fs.readFileSync('tmp-landing-body.html', 'utf8');
const scripts = fs.existsSync('tmp-landing-scripts.js')
  ? fs.readFileSync('tmp-landing-scripts.js', 'utf8')
  : '';

const minCss = cssRaw
  .replace(/\/\*[\s\S]*?\*\//g, '')
  .replace(/\s+/g, ' ')
  .replace(/\s*([{}:;,])\s*/g, '$1')
  .trim();

body = body
  .replace(/<!--[\s\S]*?-->/g, '')
  .replace(/>\s+</g, '><')
  .replace(/\s+/g, ' ')
  .trim();

const cssEmbed = [
  '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fontsource-variable/mona-sans@5.0.0/index.min.css">',
  `<style id="dc-landing-styles">${minCss}</style>`,
].join('');

const htmlEmbed = `<div class="dc-landing" id="dc-landing-root">${body}</div><script>${scripts}</script>`;

fs.writeFileSync('tmp-embed-css.html', cssEmbed);
fs.writeFileSync('tmp-embed-html.html', htmlEmbed);

console.log(
  JSON.stringify(
    {
      cssEmbed: cssEmbed.length,
      htmlEmbed: htmlEmbed.length,
      total: cssEmbed.length + htmlEmbed.length,
    },
    null,
    2
  )
);
