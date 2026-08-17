import fs from 'fs';
import crypto from 'crypto';
import https from 'https';
import http from 'http';

const raw = fs.readFileSync('docs/_dd_head.html', 'utf8').replace(/\r\n/g, '\n');
const m = raw.match(/<style id="oh-deal-desk">([\s\S]*?)<\/style>/);
if (!m) throw new Error('oh-deal-desk block missing');
const css = m[1].trim() + '\n';
fs.writeFileSync('docs/_oh_deal_desk_only.css', css);
const md5 = crypto.createHash('md5').update(Buffer.from(css, 'utf8')).digest('hex');
console.log(JSON.stringify({ cssLen: css.length, md5 }));
