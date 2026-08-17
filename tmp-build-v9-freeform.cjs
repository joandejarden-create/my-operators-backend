const fs = require('fs');
const h = fs.readFileSync(
  'public/marketing/dealality-landing-v9-standalone.html',
  'utf8'
);

const scripts = [];
const sre = /<script\b([^>]*)>([\s\S]*?)<\/script>/gi;
let m;
while ((m = sre.exec(h))) {
  const attrs = m[1] || '';
  if (/\bsrc\s*=/.test(attrs)) continue;
  const code = (m[2] || '').trim();
  if (!code) continue;
  if (/gtag|dataLayer|G-8ZW8FDHBV2/.test(code)) continue;
  scripts.push(code);
}

const footer = `<script>\n${scripts.join('\n\n')}\n</script>`;
fs.writeFileSync('tmp-v9-footer.html', footer);
console.log({ count: scripts.length, footer: footer.length });

const head = [
  '<link rel="preconnect" href="https://fonts.googleapis.com">',
  '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>',
  '<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=Lora:ital,wght@0,400;0,500;1,400&display=swap" rel="stylesheet">',
  '<link rel="stylesheet" href="https://my-operators-backend-production.up.railway.app/marketing/dealality-v9-old-home.css">',
  '<style>html,body{margin:0;background:#080F25;font-family:\"Plus Jakarta Sans\",sans-serif;color:#fff}</style>',
].join('\n');
fs.writeFileSync('tmp-v9-head-link.html', head);
console.log({ head: head.length });
