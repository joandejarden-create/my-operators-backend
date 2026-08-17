const fs = require('fs');

const BASE = 'https://my-operators-backend-production.up.railway.app';
const WF_LOGO =
  'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png';

const h = fs.readFileSync(
  'public/marketing/dealality-landing-v9-standalone.html',
  'utf8'
);
const bodyOpen = h.indexOf('>', h.indexOf('<body')) + 1;
const bodyEnd = h.lastIndexOf('</body>');
let body = h.slice(bodyOpen, bodyEnd).trim();

// Drop analytics only; keep page behavior scripts
body = body.replace(
  /<script async src="https:\/\/www\.googletagmanager\.com\/gtag\/js[^"]*"><\/script>\s*/gi,
  ''
);
body = body.replace(
  /<script>\s*window\.dataLayer[\s\S]*?gtag\('config'[\s\S]*?<\/script>\s*/gi,
  ''
);

body = body
  .replace(/src="\/assets\/dealality-logo\.png"/g, `src="${WF_LOGO}"`)
  .replace(/src="\/(?!\/)/g, `src="${BASE}/`)
  .replace(/href="\/(?!\/|#)/g, `href="${BASE}/`)
  .replace(/url\(\/(?!\/)/g, `url(${BASE}/`)
  .replace(/['"]\/api\//g, `"${BASE}/api/`);

const boot = `/*! Dealality v9 Old Home boot — same-document inject for Clarity */
(function () {
  var ROOT_ID = 'dc-v9-root';
  var html = ${JSON.stringify(body)};
  function runScripts(root) {
    var list = root.querySelectorAll('script');
    list.forEach(function (old) {
      var s = document.createElement('script');
      if (old.src) {
        s.src = old.src;
        s.async = old.async;
      } else {
        s.text = old.textContent;
      }
      old.parentNode.replaceChild(s, old);
    });
  }
  function go() {
    var root = document.getElementById(ROOT_ID);
    if (!root) {
      root = document.createElement('div');
      root.id = ROOT_ID;
      document.body.insertBefore(root, document.body.firstChild);
    }
    root.innerHTML = html;
    runScripts(root);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', go);
  else go();
})();
`;

fs.writeFileSync('public/marketing/dealality-v9-old-home-boot.js', boot);
console.log(
  JSON.stringify(
    {
      boot: boot.length,
      body: body.length,
      hasNav: /<nav\b/.test(body),
      hasScripts: /<script\b/.test(body),
      hasCommercial: body.includes('commercial fit'),
    },
    null,
    2
  )
);
