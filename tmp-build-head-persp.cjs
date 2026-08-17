const fs = require('fs');
// Live head captured in prior MCP get (benefits-tabs b)
let head = fs.readFileSync('tmp-old-home-head-with-tabs-link-b.txt', 'utf8');
const link =
  '<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css">';
if (!head.includes('perspectives.v20260728.css')) {
  head = head.replace(
    'dealality-old-home-benefits-tabs.v20260728b.css">',
    'dealality-old-home-benefits-tabs.v20260728b.css">\n' + link
  );
}
fs.writeFileSync('tmp-old-home-head-with-persp.txt', head);
console.log(head.includes('perspectives.v20260728.css'), head.length);
