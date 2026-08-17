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
    'tmp-perspectives.css',
    'https://webflow-prod-assets.s3.amazonaws.com/',
    {
      acl: 'public-read',
      bucket: 'webflow-prod-assets',
      xAmzAlgorithm: 'AWS4-HMAC-SHA256',
      xAmzCredential: 'AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request',
      xAmzDate: '20260728T205659Z',
      key: '68108c29063eeb5d1bd7ae4a/6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css',
      policy:
        'eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQyMTo1Njo1OVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjgvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI4VDIwNTY1OVoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTY5MTc5YjBjZTcyYzlmZGVkNDE0NTRfZGVhbGFsaXR5LW9sZC1ob21lLXBlcnNwZWN0aXZlcy52MjAyNjA3MjguY3NzIn1dfQ==',
      xAmzSignature: '265cd719811fe15b8e47d35e84be96d77877a309839c5cfb85bda2765064df80',
      successActionStatus: '201',
      contentType: 'text/css',
      cacheControl: 'max-age=31536000',
    },
    'dealality-old-home-perspectives.v20260728.css'
  );

  await upload(
    'tmp-perspectives-tabs.js',
    'https://webflow-prod-assets.s3.amazonaws.com/',
    {
      acl: 'public-read',
      bucket: 'webflow-prod-assets',
      xAmzAlgorithm: 'AWS4-HMAC-SHA256',
      xAmzCredential: 'AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request',
      xAmzDate: '20260728T205659Z',
      key: '68108c29063eeb5d1bd7ae4a/6a69179be2f68782cb20483c_dealality-old-home-perspectives.v20260728.js',
      policy:
        'eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQyMTo1Njo1OVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI4L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOFQyMDU2NTlaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2OTE3OWJlMmY2ODc4MmNiMjA0ODNjX2RlYWxhbGl0eS1vbGQtaG9tZS1wZXJzcGVjdGl2ZXMudjIwMjYwNzI4LmpzIn1dfQ==',
      xAmzSignature: '1c22e2613fafb6b662ecab18d7839e9e560c091b580e6a16c929ce81454b030f',
      successActionStatus: '201',
      contentType: 'application/javascript',
      cacheControl: 'max-age=31536000',
    },
    'dealality-old-home-perspectives.v20260728.js'
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
