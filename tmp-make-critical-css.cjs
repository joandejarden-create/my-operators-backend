const fs = require('fs');
const css = fs.readFileSync('tmp-whtml-fix-css.css', 'utf8');

// Prefer layout/visual selectors for Designer fidelity
const prefer = [
  '.dc-landing',
  ':root',
  '.nav',
  '.hero',
  '.btn',
  '.what-is',
  '.section-header',
  '.problem',
  '.solution',
  '.pillar',
  '.how-',
  '.feature',
  '.trust',
  '.why-we',
  '.values',
  '.vision',
  '.mission',
  '.impact',
  '.team',
  '.join-team',
  '.cta',
  '.faq',
  '.footer',
  '.stat-',
  '.back-to-top',
  '.chat',
  '@media',
  '@keyframes',
];

const parts = css.split('}');
const kept = [];
for (const part of parts) {
  if (!part.trim()) continue;
  const rule = `${part}}`;
  if (prefer.some((p) => rule.includes(p))) kept.push(rule.trim());
}
let out = kept.join('');
// Cap ~12kb for MCP reliability
if (out.length > 12000) out = out.slice(0, 12000);
// ensure we don't cut mid-rule
const lastBrace = out.lastIndexOf('}');
out = out.slice(0, lastBrace + 1);

fs.writeFileSync('tmp-whtml-fix-css-critical.css', out);
console.log({ full: css.length, critical: out.length });
