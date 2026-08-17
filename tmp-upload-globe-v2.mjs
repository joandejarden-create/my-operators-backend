import fs from "fs";
import axios from "axios";
import FormData from "form-data";

async function upload(filePath, details, filename, contentType) {
  const fileData = fs.readFileSync(filePath);
  const form = new FormData();
  form.append("acl", details.acl);
  form.append("bucket", details.bucket);
  form.append("X-Amz-Algorithm", details.xAmzAlgorithm);
  form.append("X-Amz-Credential", details.xAmzCredential);
  form.append("X-Amz-Date", details.xAmzDate);
  form.append("key", details.key);
  form.append("Policy", details.policy);
  form.append("X-Amz-Signature", details.xAmzSignature);
  form.append("success_action_status", details.successActionStatus);
  form.append("Content-Type", details.contentType);
  form.append("Cache-Control", details.cacheControl);
  form.append("file", fileData, { filename, contentType });
  const res = await axios.post("https://webflow-prod-assets.s3.amazonaws.com/", form, {
    headers: form.getHeaders(),
    maxBodyLength: Infinity,
    validateStatus: () => true,
  });
  console.log(filename, res.status);
  if (res.status !== 201) {
    console.log(String(res.data).slice(0, 300));
    process.exit(1);
  }
}

await upload(
  "public/marketing/dealality-old-home-freeform-head.v20260729b.css",
  {
    acl: "public-read",
    bucket: "webflow-prod-assets",
    xAmzAlgorithm: "AWS4-HMAC-SHA256",
    xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request",
    xAmzDate: "20260728T222421Z",
    key: "68108c29063eeb5d1bd7ae4a/6a692c15b4e79633f7ad9c16_dealality-old-home-freeform-head.v20260729b.css",
    policy:
      "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQyMzoyNDoyMVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjgvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI4VDIyMjQyMVoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTY5MmMxNWI0ZTc5NjMzZjdhZDljMTZfZGVhbGFsaXR5LW9sZC1ob21lLWZyZWVmb3JtLWhlYWQudjIwMjYwNzI5Yi5jc3MifV19",
    xAmzSignature:
      "e9b3005cafb79e2b3d93e8fde20e5ce3f6d9d2c564a0bbbcb60b691237ca0eed",
    successActionStatus: "201",
    contentType: "text/css",
    cacheControl: "max-age=31536000",
  },
  "dealality-old-home-freeform-head.v20260729b.css",
  "text/css"
);

await upload(
  "public/marketing/dealality-old-home-hero-globe-bg.js",
  {
    acl: "public-read",
    bucket: "webflow-prod-assets",
    xAmzAlgorithm: "AWS4-HMAC-SHA256",
    xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request",
    xAmzDate: "20260728T222422Z",
    key: "68108c29063eeb5d1bd7ae4a/6a692c1644b6ca466a071564_dealality-old-home-hero-globe-bg.v20260729b.js",
    policy:
      "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQyMzoyNDoyMloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI4L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOFQyMjI0MjJaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2OTJjMTY0NGI2Y2E0NjZhMDcxNTY0X2RlYWxhbGl0eS1vbGQtaG9tZS1oZXJvLWdsb2JlLWJnLnYyMDI2MDcyOWIuanMifV19",
    xAmzSignature:
      "ffa5d3d5e4e3161f2608f156755da88908e4dee1f8b853c599ffac9b14887dee",
    successActionStatus: "201",
    contentType: "application/javascript",
    cacheControl: "max-age=31536000",
  },
  "dealality-old-home-hero-globe-bg.v20260729b.js",
  "application/javascript"
);

console.log("uploads ok");
