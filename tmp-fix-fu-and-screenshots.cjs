const fs = require('fs');

// Fix style-01: force .fu visible
let s1 = fs.readFileSync('tmp-v9-full-style-01.html', 'utf8');
if (!s1.includes('.fu{opacity:1!important')) {
  s1 = s1.replace(
    '</style>',
    '.fu{opacity:1!important;transform:none!important}.fu.vis{opacity:1!important;transform:none!important}\n</style>'
  );
  fs.writeFileSync('tmp-v9-full-style-01.html', s1);
}
console.log('style1', s1.length);

// Fix how screenshots to absolute Railway URLs
const RAIL = 'https://my-operators-backend-production.up.railway.app/marketing/';
let how = fs.readFileSync('tmp-v9-full-embed-02-min.html', 'utf8');
how = how.replace(/src="screenshots\//g, `src="${RAIL}screenshots/`);
fs.writeFileSync('tmp-v9-full-embed-02-min.html', how);
console.log('how', how.length, how.includes(RAIL + 'screenshots/'));
