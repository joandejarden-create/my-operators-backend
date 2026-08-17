const fs = require('fs');

async function upload(filePath, uploadUrl, uploadDetails, fileName) {
  const fileData = fs.readFileSync(filePath);
  const form = new FormData();
  const u = uploadDetails;
  form.append('acl', u.acl);
  form.append('bucket', u.bucket);
  form.append('X-Amz-Algorithm', u.xAmzAlgorithm);
  form.append('X-Amz-Credential', u.xAmzCredential);
  form.append('X-Amz-Date', u.xAmzDate);
  form.append('key', u.key);
  form.append('Policy', u.policy);
  form.append('X-Amz-Signature', u.xAmzSignature);
  form.append('success_action_status', u.successActionStatus);
  form.append('Content-Type', u.contentType);
  form.append('Cache-Control', u.cacheControl);
  form.append('file', new Blob([fileData], { type: u.contentType }), fileName);
  const res = await fetch(uploadUrl, { method: 'POST', body: form });
  console.log(fileName, res.status);
  if (res.status !== 201) {
    console.log(await res.text());
    process.exit(1);
  }
}

const js = {
  uploadUrl: 'https://webflow-prod-assets.s3.amazonaws.com/',
  uploadDetails: {
    acl: 'public-read',
    bucket: 'webflow-prod-assets',
    xAmzAlgorithm: 'AWS4-HMAC-SHA256',
    xAmzCredential: 'AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request',
    xAmzDate: '20260728T194459Z',
    key: '68108c29063eeb5d1bd7ae4a/6a6906bb27597c4a7d4f1433_dealality-old-home-benefits-tabs.v20260728b.js',
    policy:
      'eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQyMDo0NDo1OVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI4L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOFQxOTQ0NTlaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2OTA2YmIyNzU5N2M0YTdkNGYxNDMzX2RlYWxhbGl0eS1vbGQtaG9tZS1iZW5lZml0cy10YWJzLnYyMDI2MDcyOGIuanMifV19',
    xAmzSignature: 'cde02898d8b0e623305bc69e0427953c4a1149efcf326e9fd6f280ebee53d109',
    successActionStatus: '201',
    contentType: 'application/javascript',
    cacheControl: 'max-age=31536000',
  },
};

upload(
  'tmp-benefits-tabs.js',
  js.uploadUrl,
  js.uploadDetails,
  'dealality-old-home-benefits-tabs.v20260728b.js'
).catch((e) => {
  console.error(e);
  process.exit(1);
});
