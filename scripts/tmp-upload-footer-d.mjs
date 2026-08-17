import fs from 'fs';
import crypto from 'crypto';
import https from 'https';
import { URL } from 'url';

const filePath = process.argv[2];
if (!filePath) {
  console.error('Usage: node scripts/tmp-upload-footer-d.mjs <file>');
  process.exit(1);
}
const file = fs.readFileSync(filePath);
const md5 = crypto.createHash('md5').update(file).digest('hex');
console.log('md5', md5, 'bytes', file.length, 'path', filePath);

const uploadUrl = process.env.WF_UPLOAD_URL;
const d = JSON.parse(process.env.WF_UPLOAD_DETAILS);
const fileName = filePath.split(/[/\\]/).pop();
const contentType = d.contentType || 'application/octet-stream';

const boundary = '----WebKitFormBoundary' + crypto.randomBytes(16).toString('hex');
const parts = [];
function addField(name, val) {
  parts.push(Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="${name}"\r\n\r\n${val}\r\n`));
}
addField('acl', d.acl);
addField('bucket', d.bucket);
addField('X-Amz-Algorithm', d.xAmzAlgorithm);
addField('X-Amz-Credential', d.xAmzCredential);
addField('X-Amz-Date', d.xAmzDate);
addField('key', d.key);
addField('Policy', d.policy);
addField('X-Amz-Signature', d.xAmzSignature);
addField('success_action_status', d.successActionStatus);
addField('Content-Type', contentType);
addField('Cache-Control', d.cacheControl);
parts.push(
  Buffer.from(
    `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${fileName}"\r\nContent-Type: ${contentType}\r\n\r\n`
  )
);
parts.push(file);
parts.push(Buffer.from(`\r\n--${boundary}--\r\n`));
const body = Buffer.concat(parts);

const u = new URL(uploadUrl);
const req = https.request(
  {
    hostname: u.hostname,
    path: u.pathname,
    method: 'POST',
    headers: {
      'Content-Type': `multipart/form-data; boundary=${boundary}`,
      'Content-Length': body.length,
    },
  },
  (res) => {
    let data = '';
    res.on('data', (c) => (data += c));
    res.on('end', () => {
      console.log('status', res.statusCode);
      console.log(data.slice(0, 800));
      process.exit(res.statusCode === 201 ? 0 : 1);
    });
  }
);
req.on('error', (e) => {
  console.error(e);
  process.exit(1);
});
req.write(body);
req.end();
