const fs = require('fs');
const css = fs.readFileSync('tmp-landing-scoped.css', 'utf8');
const keep = [
  '.dc-landing',
  '.nav',
  '.hero',
  '.btn',
  '.what-is',
  '.problem',
  '.section-header',
  '.feature',
  '.how-',
  '.team-',
  '.faq',
  '.footer',
  '.cta-',
  '.values-',
  '.vision',
  '.impact',
  '.trust',
  '.why-we',
  '.stat-',
  '.pillar',
  '.solution',
  ':root',
  '@media',
  '@keyframes',
];
const rules = css.split('}');
const out = [];
for (const r of rules) {
  if (!r.trim()) continue;
  const t = `${r.trim()}}`;
  if (keep.some((k) => t.includes(k))) out.push(t);
}
const filtered = out.join('\n').replace(/\s+/g, ' ').trim();
const head = [
  '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fontsource-variable/mona-sans@5.0.0/index.min.css">',
  `<style id="dc-landing-styles">${filtered}</style>`,
  '<link rel="stylesheet" href="https://my-operators-backend-staging.up.railway.app/css/dealality-old-home-landing.css">',
].join('\n');
fs.writeFileSync('tmp-landing-head-critical.html', head);
console.log({ filtered: filtered.length, head: head.length });
