const fs = require('fs');
const h = fs.readFileSync('public/marketing/dealality-landing-v9-standalone.html', 'utf8');

const faqIdx = h.indexOf('id="faq"');
console.log('=== FAQ SECTION ===');
console.log(h.slice(faqIdx, faqIdx + 5500));

console.log('\n=== IMAGES ===');
const imgs = [...h.matchAll(/src="([^"]+)"/g)].map((m) => m[1]);
console.log([...new Set(imgs)].join('\n'));

console.log('\n=== FAQ CSS ===');
const cssMatch = h.match(/#faq\{[\s\S]*?\}/);
const faqCss = h.match(/\.faq[^{]*\{[^}]*\}/g) || [];
console.log(faqCss.join('\n'));
const more = h.match(/\.fgrid|\.faq-side|\.insight|\.dr-wrap[\s\S]{0,80}/g);
console.log('more', more && more.slice(0, 20));

// Find class names around FAQ
const faqBlock = h.slice(faqIdx, faqIdx + 6000);
const classes = [...faqBlock.matchAll(/class="([^"]+)"/g)].map((m) => m[1]);
console.log('\n=== FAQ classes ===');
console.log([...new Set(classes)].join('\n'));
