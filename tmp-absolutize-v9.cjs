const fs = require('fs');
const BASE = 'https://my-operators-backend-production.up.railway.app';
const WF_LOGO =
  'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png';

function absolutize(html) {
  return html
    .replace(/src="\/assets\/dealality-logo\.png"/g, `src="${WF_LOGO}"`)
    .replace(/src="\/(?!\/)/g, `src="${BASE}/`)
    .replace(/href="\/(?!\/|#)/g, `href="${BASE}/`)
    .replace(/url\(\/(?!\/)/g, `url(${BASE}/`);
}

for (const n of ['01', '02', '03', '04']) {
  const p = `tmp-v9-embed-${n}.html`;
  if (!fs.existsSync(p)) continue;
  const out = absolutize(fs.readFileSync(p, 'utf8'));
  fs.writeFileSync(`tmp-v9-embed-${n}-abs.html`, out);
  console.log(n, out.length);
}

const head = fs.readFileSync('tmp-v9-head-link.html', 'utf8');
const footerRaw = fs.readFileSync('tmp-v9-footer.html', 'utf8');
// Fix relative API paths in scripts if any
const footer = footerRaw
  .replace(/['"]\/api\//g, `'${BASE}/api/`)
  .replace(/['"]\/assets\//g, `'${BASE}/assets/`);
fs.writeFileSync('tmp-v9-footer-abs.html', footer);
console.log('footer', footer.length);
