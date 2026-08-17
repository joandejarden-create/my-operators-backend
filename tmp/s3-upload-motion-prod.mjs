import fs from "fs";

const uploads = [
  {
    local: "public/marketing/old-home-motion.prod.v20260801a.css",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    details: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T101333Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6dc6cd556592892613ec4d_old-home-motion.prod.v20260801a.css",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxMToxMzozM1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA4MDEvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwODAxVDEwMTMzM1oifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZkYzZjZDU1NjU5Mjg5MjYxM2VjNGRfb2xkLWhvbWUtbW90aW9uLnByb2QudjIwMjYwODAxYS5jc3MifV19",
      xAmzSignature: "a2f1228517192149663809c3f3758334cd1c9b48c3f1fd285619f80d80ce6875",
      successActionStatus: "201",
      contentType: "text/css",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6dc6cd556592892613ec4d_old-home-motion.prod.v20260801a.css",
  },
  {
    local: "public/marketing/old-home-motion.prod.v20260801a.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    details: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T101333Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6dc6cdb0fe6b6f28efa2bd_old-home-motion.prod.v20260801a.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxMToxMzozM1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxMDEzMzNaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZGM2Y2RiMGZlNmI2ZjI4ZWZhMmJkX29sZC1ob21lLW1vdGlvbi5wcm9kLnYyMDI2MDgwMWEuanMifV19",
      xAmzSignature: "61a0aa43abdc450befecbfdf0287a18e4852538af3c5551790c0b4ce9ddc40b6",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6dc6cdb0fe6b6f28efa2bd_old-home-motion.prod.v20260801a.js",
  },
];

for (const u of uploads) {
  const bytes = fs.readFileSync(u.local);
  const form = new FormData();
  const d = u.details;
  form.append("acl", d.acl);
  form.append("bucket", d.bucket);
  form.append("X-Amz-Algorithm", d.xAmzAlgorithm);
  form.append("X-Amz-Credential", d.xAmzCredential);
  form.append("X-Amz-Date", d.xAmzDate);
  form.append("key", d.key);
  form.append("Policy", d.policy);
  form.append("X-Amz-Signature", d.xAmzSignature);
  form.append("success_action_status", d.successActionStatus);
  form.append("Content-Type", d.contentType);
  form.append("Cache-Control", d.cacheControl);
  form.append("file", new Blob([bytes], { type: d.contentType }), u.local.split("/").pop());
  const res = await fetch(u.uploadUrl, { method: "POST", body: form });
  const text = await res.text();
  console.log(u.local, res.status, text.slice(0, 120));
  if (res.status !== 201) process.exit(1);
}
console.log("OK");
