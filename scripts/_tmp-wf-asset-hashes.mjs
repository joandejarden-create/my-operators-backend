const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const https = require('https');
const http = require('http');

const SITE = '68108c29063eeb5d1bd7ae4a';
const ROOT = 'C:/Dev/deal-capture-proxy/public/marketing';

function md5(buf){ return crypto.createHash('md5').update(buf).digest('hex'); }
function sri(buf){ return 'sha256-' + crypto.createHash('sha256').update(buf).digest('base64'); }

const files = process.argv.slice(2);
if (!files.length) { console.error('usage: node upload.js file...'); process.exit(1); }

for (const f of files) {
  const fp = path.isAbsolute(f) ? f : path.join(ROOT, f);
  const buf = fs.readFileSync(fp);
  const name = path.basename(fp);
  console.log(JSON.stringify({ file: name, md5: md5(buf), sri: sri(buf), size: buf.length, path: fp }));
}
