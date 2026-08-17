const fs = require('fs');
const foot = fs.readFileSync('tmp-old-home-footer-with-tabs-js.txt', 'utf8');
// Verify CDN script present
if (!foot.includes('6a69058f60171c2764fb0e62_dealality-old-home-benefits-tabs.v20260728.js')) {
  console.error('missing js tag');
  process.exit(1);
}
fs.writeFileSync(
  'tmp-foot-set-args.json',
  JSON.stringify({
    actions: [
      {
        label: 'set-foot',
        set_page_freeform_code: {
          page_id: '68108c2a063eeb5d1bd7ae90',
          location: 'footer',
          content: foot,
        },
      },
    ],
    context: 'Append Benefits dual-tab script tag to old-home freeform footer.',
  })
);
console.log('ok', foot.length, foot.slice(-120));
