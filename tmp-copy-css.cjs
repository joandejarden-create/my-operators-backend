const fs = require('fs');
const css = fs.readFileSync('tmp-landing-scoped.css', 'utf8');
fs.mkdirSync('public/css', { recursive: true });
fs.writeFileSync('public/css/dealality-old-home-landing.css', css);

const head = [
  '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fontsource-variable/mona-sans@5.0.0/index.min.css">',
  '<link rel="stylesheet" href="https://my-operators-backend-staging.up.railway.app/css/dealality-old-home-landing.css">',
  '<style>.dc-landing{background:#080f25;min-height:100vh;color:#d1dbf9;font-family:\"Mona Sans Variable\",\"Mona Sans\",sans-serif}</style>',
].join('\n');
fs.writeFileSync('tmp-landing-head-link.html', head);
console.log('css', css.length, 'head', head.length);
