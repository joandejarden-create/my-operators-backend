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

async function main() {
  await upload(
    'tmp-benefits-tabs.js',
    'https://webflow-prod-assets.s3.amazonaws.com/',
    {
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
    'dealality-old-home-benefits-tabs.v20260728b.js'
  );

  await upload(
    'tmp-benefits-tabs.css',
    'https://webflow-prod-assets.s3.amazonaws.com/',
    {
      acl: 'public-read',
      bucket: 'webflow-prod-assets',
      xAmzAlgorithm: 'AWS4-HMAC-SHA256',
      xAmzCredential: 'AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request',
      xAmzDate: '20260728T194520Z',
      key: '68108c29063eeb5d1bd7ae4a/6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css',
      policy:
        'eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQyMDo0NToyMFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjgvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI4VDE5NDUyMFoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTY5MDZkMDJjZmEzYjEzNDQ2YTMyMzZfZGVhbGFsaXR5LW9sZC1ob21lLWJlbmVmaXRzLXRhYnMudjIwMjYwNzI4Yi5jc3MifV19',
      xAmzSignature: 'cf41a3a8a824e388941343d5a12dc96640016672602ae1b4226a222dc5857ea2',
      successActionStatus: '201',
      contentType: 'text/css',
      cacheControl: 'max-age=31536000',
    },
    'dealality-old-home-benefits-tabs.v20260728b.css'
  );

  let head = fs.readFileSync('tmp-old-home-head-with-tabs-link.txt', 'utf8');
  head = head.replace(
    '6a6905579687cbd43649cf04_dealality-old-home-benefits-tabs.v20260728.css',
    '6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css'
  );
  fs.writeFileSync('tmp-old-home-head-with-tabs-link-b.txt', head);
  console.log('head ready', head.includes('v20260728b.css'));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
