import fs from 'fs';

const a = JSON.parse(fs.readFileSync('tmp-set-footer-iframe-args.json', 'utf8'));
const cdnOnly = {
  actions: [
    {
      label: 'set-old-home-footer-hero-iframe',
      set_page_freeform_code: {
        page_id: '68108c2a063eeb5d1bd7ae90',
        location: 'footer',
        content:
          '<script src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69fe92f7d4f50fb6112eb9_old-home-footer-oh-20260729b.js"></script>',
      },
    },
  ],
  context: a.context,
};
fs.writeFileSync('tmp-footer-cdn-only-args.json', JSON.stringify(cdnOnly));
console.log('cdnOnly', cdnOnly.actions[0].set_page_freeform_code.content.length);
console.log('exact', a.actions[0].set_page_freeform_code.content.length);
