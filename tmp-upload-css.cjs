const fs = require('fs');
const path = require('path');

async function upload(filePath, uploadUrl, uploadDetails, fileName, contentType) {
  const fileData = fs.readFileSync(filePath);
  const form = new FormData();
  form.append('acl', uploadDetails.acl);
  form.append('bucket', uploadDetails.bucket);
  form.append('X-Amz-Algorithm', uploadDetails.xAmzAlgorithm);
  form.append('X-Amz-Credential', uploadDetails.xAmzCredential);
  form.append('X-Amz-Date', uploadDetails.xAmzDate);
  form.append('key', uploadDetails.key);
  form.append('Policy', uploadDetails.policy);
  form.append('X-Amz-Signature', uploadDetails.xAmzSignature);
  form.append('success_action_status', uploadDetails.successActionStatus);
  form.append('Content-Type', uploadDetails.contentType || contentType);
  form.append('Cache-Control', uploadDetails.cacheControl);
  form.append('file', new Blob([fileData], { type: contentType }), fileName);
  const res = await fetch(uploadUrl, { method: 'POST', body: form });
  const text = await res.text();
  console.log(fileName, res.status, text.slice(0, 200));
  if (res.status !== 201) process.exit(1);
}

const details = JSON.parse(fs.readFileSync('tmp-css-upload-details.json', 'utf8'));
upload(
  'tmp-benefits-tabs.css',
  details.uploadUrl,
  details.uploadDetails,
  'dealality-old-home-benefits-tabs.v20260728.css',
  'text/css'
).catch((e) => {
  console.error(e);
  process.exit(1);
});
