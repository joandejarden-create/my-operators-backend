const fs = require('fs');
const h = fs.readFileSync('public/marketing/dealality-landing-v9-standalone.html', 'utf8');

// Full aud CSS block
const start = h.indexOf('.audtabs{');
const end = h.indexOf('.audbody', start);
console.log(h.slice(start, end + 800));

// brands + partners panels
const brands = h.indexOf('id="brands"');
console.log('\n\n===== BRANDS =====\n', h.slice(brands, brands + 2200));
const partners = h.indexOf('id="partners"');
console.log('\n\n===== PARTNERS =====\n', h.slice(partners, partners + 2200));

// faq aside?
console.log('\nfaq has approach?', /Approach Insights/i.test(h));
console.log('faq-aside?', /faq-aside|faq-side|faq-grid/i.test(h));
