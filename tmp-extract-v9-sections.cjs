const fs = require('fs');
const h = fs.readFileSync('public/marketing/dealality-landing-v9-standalone.html', 'utf8');

function sliceId(id, n = 5000) {
  const i = h.indexOf(`id="${id}"`);
  console.log(`\n===== ${id} @ ${i} =====`);
  console.log(h.slice(i, i + n));
}

sliceId('audiences', 7000);
sliceId('why', 4000);
sliceId('faq', 2500);

// CSS for aud / wgrid
for (const needle of ['.audtabs', '.audt', '.wgrid', '.wcards', '.wc{', '.founder', '.beta-box', '.og{', '.ptags']) {
  const i = h.indexOf(needle);
  if (i >= 0) console.log('\nCSS', needle, h.slice(i, i + 400));
}
